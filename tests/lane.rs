//! The lane, end to end: a client speaks the protocol the browser speaks.

use futures_util::{SinkExt, StreamExt};
use mlp_core::media::pdu::{Carrier, Fragment, FragmentClass, FragmentFlags, Priority};
use mlp_core::media::receiver::{MediaReceiverConfig, MediaReceiverMachine};
use mlp_core::media::{MediaAck, MediaFeedback};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

// The test drives mlp-server's own wire module for the bodies it shares, the
// way the browser drives its generated bindings: the bytes on the socket are
// the contract, and both sides hold it. Only the decoders are called here;
// the encoders are the server's, dead to this translation unit by design.
#[allow(dead_code)]
#[path = "../src/wire.rs"]
mod wire;

/// Encode one CONTROL fragment: URGENT, FIN, one object.
fn control_fragment(payload: Vec<u8>) -> Vec<u8> {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let object_id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Fragment::new(
        FragmentClass::Control,
        FragmentFlags::URGENT.union(FragmentFlags::FIN),
        Priority::CONTROL,
        object_id,
        0,
        1,
        mlp_core::media::pdu::HeaderExt::new(),
        payload,
    )
    .unwrap()
    .encode()
}

/// Encode one FEEDBACK fragment carrying the receiver's report.
fn feedback_fragment(feedback: &MediaFeedback) -> Vec<u8> {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let object_id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let payload = encode_feedback_body(feedback);
    Fragment::new(
        FragmentClass::Feedback,
        FragmentFlags::FIN,
        Priority::REPORT,
        object_id,
        0,
        1,
        mlp_core::media::pdu::HeaderExt::new(),
        payload,
    )
    .unwrap()
    .encode()
}

// ── the client's own protobuf encoders (the mirror of the server's) ─────────

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn tag(out: &mut Vec<u8>, number: u64, wire: u64) {
    put_varint(out, (number << 3) | wire);
}

/// A `MediaHello` as the oneof's field 1.
fn hello_body() -> Vec<u8> {
    let mut hello = Vec::new();
    tag(&mut hello, 1, 0);
    put_varint(&mut hello, 1); // protocol_version
    tag(&mut hello, 3, 2); // session_id
    put_varint(&mut hello, 4);
    hello.extend_from_slice(b"test");
    tag(&mut hello, 4, 0); // max_fragment_payload
    put_varint(&mut hello, 65_536);
    tag(&mut hello, 9, 0); // encoder_max_fps
    put_varint(&mut hello, 30);

    let mut out = Vec::new();
    tag(&mut out, 1, 2);
    put_varint(&mut out, hello.len() as u64);
    out.extend_from_slice(&hello);
    out
}

/// A `MediaResume` as the oneof's field 3.
fn resume_body(token: &str, applied: u64) -> Vec<u8> {
    let mut resume = Vec::new();
    tag(&mut resume, 1, 2); // resume_token
    put_varint(&mut resume, token.len() as u64);
    resume.extend_from_slice(token.as_bytes());
    tag(&mut resume, 2, 0); // applied_update_id
    put_varint(&mut resume, applied);

    let mut out = Vec::new();
    tag(&mut out, 3, 2);
    put_varint(&mut out, resume.len() as u64);
    out.extend_from_slice(&resume);
    out
}

/// A `MediaClose` as the oneof's field 7.
fn close_body(park: bool) -> Vec<u8> {
    let mut close = Vec::new();
    if park {
        tag(&mut close, 3, 0);
        close.push(1);
    }
    let mut out = Vec::new();
    tag(&mut out, 7, 2);
    put_varint(&mut out, close.len() as u64);
    out.extend_from_slice(&close);
    out
}

/// A `MediaFeedback` body: rtt, the ack (fields 1, 10.1, 10.2), lost_objects.
fn encode_feedback_body(feedback: &MediaFeedback) -> Vec<u8> {
    let mut body = Vec::new();
    tag(&mut body, 1, 0);
    put_varint(&mut body, u64::from(feedback.rtt_ms.max(1)));
    tag(&mut body, 6, 0);
    put_varint(&mut body, feedback.lost_objects);
    if let Some(ack) = &feedback.ack {
        let mut ack_body = Vec::new();
        tag(&mut ack_body, 1, 0);
        put_varint(&mut ack_body, ack.highest_applied_update_id);
        tag(&mut ack_body, 2, 0);
        put_varint(&mut ack_body, ack.ack_bitmap);
        tag(&mut ack_body, 6, 0);
        ack_body.push(u8::from(ack.converged));
        tag(&mut body, 10, 2);
        put_varint(&mut body, ack_body.len() as u64);
        body.extend_from_slice(&ack_body);
    }
    body
}

// ── the test ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_lane_speaks_the_protocol_end_to_end() {
    // mlp-server is a binary crate; the test runs it as a child process so
    // the whole server — listeners, sessions, park table — is under test.
    let lane_port = pick_port();
    let metrics_port = pick_port();
    let mut server = std::process::Command::new(env!("CARGO_BIN_EXE_mlp-server"))
        .env("MLP_PORT", lane_port.to_string())
        .env("MLP_METRICS_PORT", metrics_port.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("the server starts");
    // The listener needs a beat.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let result = run_lane(lane_port, metrics_port).await;
    let _ = server.kill().wait();
    result.expect("the lane held the contract");
}

fn pick_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn run_lane(lane_port: u16, metrics_port: u16) -> Result<(), String> {
    use tokio_tungstenite::client_async;

    // Connect under the protocol's own subprotocol.
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", lane_port))
        .await
        .map_err(|e| e.to_string())?;
    let mut request = format!("ws://127.0.0.1:{lane_port}/")
        .into_client_request()
        .map_err(|e| e.to_string())?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static("mlp-v1"),
    );
    let (ws, _resp) = client_async(request, stream).await.map_err(|e| e.to_string())?;
    let (mut sink, mut source) = ws.split();

    // Hello → HelloAck.
    sink.send(Message::Binary(control_fragment(hello_body())))
        .await
        .map_err(|e| e.to_string())?;
    let mut media_session_id = String::new();
    let mut hello_ack_seen = false;
    let mut media_fragments = 0usize;
    let mut receiver = MediaReceiverMachine::new(MediaReceiverConfig::for_carrier(Carrier::Ws), std::time::Instant::now());
    let mut applied = 0u64;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut park_token: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
            .await
            .ok()
            .and_then(|m| m)
            .ok_or("the lane went silent")?;
        let Message::Binary(bytes) = message.map_err(|e| e.to_string())? else {
            return Err("a text frame on the media lane".into());
        };
        let Ok((fragment, consumed)) = Fragment::parse(&bytes, Carrier::Ws) else {
            return Err("the server sent bytes that are not a fragment".into());
        };
        if consumed != bytes.len() {
            return Err("trailing bytes".into());
        }
        match fragment.header.class {
            FragmentClass::Control => {
                if let Some(wire::InboundControl::Hello(_)) =
                    wire::InboundControl::decode(&fragment.payload)
                {
                    return Err("the server never says hello".into());
                }
                // The HelloAck travels as the oneof's field 2; decode it with
                // the server's own encoder walked backwards: find field 2.
                if let Some(ack) = decode_hello_ack(&fragment.payload) {
                    media_session_id = ack;
                    hello_ack_seen = true;
                }
            }
            FragmentClass::Media => {
                media_fragments += 1;
                for event in receiver.on_message(&bytes, std::time::Instant::now()) {
                    if let mlp_core::media::MediaReceiveEvent::Object(object) = event
                        && let Some(update_id) = object.ext.update_id() {
                            receiver.note_applied(update_id);
                            applied = applied.max(update_id);
                        }
                }
            }
            _ => {}
        }

        if hello_ack_seen && media_fragments >= 3 {
            break;
        }
    }
    if !hello_ack_seen {
        return Err("no hello ack".into());
    }
    if media_fragments < 3 {
        return Err(format!("only {media_fragments} media fragments"));
    }

    // Feedback: the receiver reports what it applied.
    let ack = MediaAck {
        highest_applied_update_id: applied,
        ack_bitmap: 1,
        chain_epoch: receiver.epoch(),
        applied_cell_count: 15,
        stale_cells: 0,
        converged: true,
        render_latency_ms: 1.0,
        has_render_latency_ms: true,
        echo_of_input_seq: 0,
        ack_epoch_ms: 0,
    };
    let feedback = MediaFeedback {
        rtt_ms: 20,
        rtt_min_ms: 20,
        jitter_ms: 0,
        received_bytes: 0,
        received_objects: 0,
        lost_objects: 0,
        duplicated_objects: 0,
        reordered_objects: 0,
        window_ms: 100,
        ack: Some(ack),
        probes: Vec::new(),
        buffered_amount_bytes: 0,
        display_fps: 30,
        render_ms_p50: 1,
        render_ms_p95: 2,
        input_queue_depth: 0,
        input_submitted: 0,
        input_expired: 0,
        path_switches: 0,
        active_path: mlp_core::media::MediaPath::Ws,
        app_version: String::new(),
    };
    sink.send(Message::Binary(feedback_fragment(&feedback)))
        .await
        .map_err(|e| e.to_string())?;

    // Park: close with park=true, and the lane answers park before it ends.
    sink.send(Message::Binary(control_fragment(close_body(true))))
        .await
        .map_err(|e| e.to_string())?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
            .await
            .ok()
            .and_then(|m| m)
            .ok_or("the lane closed without answering the park")?;
        match message.map_err(|e| e.to_string())? {
            Message::Close(_) => break,
            Message::Binary(bytes) => {
                if let Ok((fragment, _)) = Fragment::parse(&bytes, Carrier::Ws)
                    && fragment.header.class == FragmentClass::Control
                        && let Some(close) = decode_close(&fragment.payload)
                            && close.park {
                                park_token = Some(media_session_id.clone());
                                break;
                            }
            }
            _ => {}
        }
    }
    let token = park_token.ok_or("the park was not answered")?;

    // Resume: a fresh lane, the same token, accepted.
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", lane_port))
        .await
        .map_err(|e| e.to_string())?;
    let mut request = format!("ws://127.0.0.1:{lane_port}/")
        .into_client_request()
        .map_err(|e| e.to_string())?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static("mlp-v1"),
    );
    let (ws, _resp) = client_async(request, stream).await.map_err(|e| e.to_string())?;
    let (mut sink, mut source) = ws.split();
    sink.send(Message::Binary(control_fragment(resume_body(&token, applied))))
        .await
        .map_err(|e| e.to_string())?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut resumed = false;
    while std::time::Instant::now() < deadline {
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
            .await
            .ok()
            .and_then(|m| m);
        let Some(Ok(Message::Binary(bytes))) = message else { break };
        if let Ok((fragment, _)) = Fragment::parse(&bytes, Carrier::Ws)
            && fragment.header.class == FragmentClass::Control
                && let Some(accepted) = decode_resume_ack(&fragment.payload) {
                    resumed = accepted;
                    break;
                }
    }
    if !resumed {
        return Err("the resume was not accepted".into());
    }

    // Metrics: the counters the lane fed.
    let mut http = tokio::net::TcpStream::connect(("127.0.0.1", metrics_port))
        .await
        .map_err(|e| e.to_string())?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    http.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|e| e.to_string())?;
    let mut page = String::new();
    http.read_to_string(&mut page).await.map_err(|e| e.to_string())?;
    if !page.contains("mlp_fragments_sent_total") || !page.contains("mlp_updates_applied_total 1") {
        return Err(format!("the metrics page is wrong: {page}"));
    }
    Ok(())
}

/// Decode the HelloAck's media_session_id (oneof field 2 → field 2).
fn decode_hello_ack(payload: &[u8]) -> Option<String> {
    let mut at = 0;
    while at < payload.len() {
        let key = varint_u64(payload, &mut at)?;
        let number = key >> 3;
        let wire = key & 7;
        if number == 2 && wire == 2 {
            let len = varint_u64(payload, &mut at)? as usize;
            let body = payload.get(at..at + len)?;
            // MediaHelloAck.media_session_id is its field 2.
            let mut body_at = 0;
            while body_at < body.len() {
                let key = varint_u64(body, &mut body_at)?;
                let number = key >> 3;
                let wire = key & 7;
                if number == 2 && wire == 2 {
                    let len = varint_u64(body, &mut body_at)? as usize;
                    let value = body.get(body_at..body_at + len)?;
                    return Some(String::from_utf8_lossy(value).into_owned());
                }
                skip_field(wire, body, &mut body_at)?;
            }
            return None;
        }
        skip_field(wire, payload, &mut at)?;
    }
    None
}

/// Decode the ResumeAck's accepted flag (oneof field 4 → field 1).
fn decode_resume_ack(payload: &[u8]) -> Option<bool> {
    let mut at = 0;
    while at < payload.len() {
        let key = varint_u64(payload, &mut at)?;
        let number = key >> 3;
        let wire = key & 7;
        if number == 4 && wire == 2 {
            let len = varint_u64(payload, &mut at)? as usize;
            let body = payload.get(at..at + len)?;
            let mut body_at = 0;
            while body_at < body.len() {
                let key = varint_u64(body, &mut body_at)?;
                let number = key >> 3;
                let wire = key & 7;
                if number == 1 && wire == 0 {
                    return Some(varint_u64(body, &mut body_at)? != 0);
                }
                skip_field(wire, body, &mut body_at)?;
            }
            return None;
        }
        skip_field(wire, payload, &mut at)?;
    }
    None
}

/// Decode a Close's park flag (oneof field 7 → field 3).
fn decode_close(payload: &[u8]) -> Option<ClosePark> {
    let mut at = 0;
    while at < payload.len() {
        let key = varint_u64(payload, &mut at)?;
        let number = key >> 3;
        let wire = key & 7;
        if number == 7 && wire == 2 {
            let len = varint_u64(payload, &mut at)? as usize;
            let body = payload.get(at..at + len)?;
            let mut body_at = 0;
            let mut park = false;
            while body_at < body.len() {
                let key = varint_u64(body, &mut body_at)?;
                let number = key >> 3;
                let wire = key & 7;
                if number == 3 && wire == 0 {
                    park = varint_u64(body, &mut body_at)? != 0;
                } else {
                    skip_field(wire, body, &mut body_at)?;
                }
            }
            return Some(ClosePark { park });
        }
        skip_field(wire, payload, &mut at)?;
    }
    None
}

struct ClosePark {
    park: bool,
}

fn varint_u64(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    while *at < bytes.len() {
        let byte = bytes[*at];
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

fn skip_field(wire: u64, bytes: &[u8], at: &mut usize) -> Option<()> {
    match wire {
        0 => {
            varint_u64(bytes, at)?;
        }
        1 => *at += 8,
        2 => {
            let len = varint_u64(bytes, at)? as usize;
            *at += len;
        }
        5 => *at += 4,
        _ => return None,
    }
    bytes.get(*at).map(|_| ())
}
