//! The protobuf bodies of the CONTROL and FEEDBACK classes (MLP spec §6.3),
//! hand-encoded to the schema's own field numbers (`mlp-proto`, `mlp.v1`).
//!
//! The fragment header and its TLVs are not protobuf — `mlp-core::media::pdu`
//! owns those bytes. This module owns only the class bodies the server
//! speaks: hello/hello_ack, resume/resume_ack, lane_state, anchor_plan,
//! close, error, and the receiver's feedback with its ack and probe echoes.

use mlp_core::media::{
    MediaAck, MediaAnchorPlan, MediaClose, MediaCloseCode, MediaErrorBody, MediaFeedback,
    MediaHello, MediaHelloAck, MediaLaneState, MediaResume, MediaResumeAck,
    ProbeEcho,
};

// ── varint plumbing ─────────────────────────────────────────────────────────

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

fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
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

/// A decoded field: (tag, wire type).
struct Field(u64, u64);

fn field(bytes: &[u8], at: &mut usize) -> Option<Field> {
    let key = varint(bytes, at)?;
    Some(Field(key >> 3, key & 7))
}

fn len_delimited<'a>(bytes: &'a [u8], at: &mut usize) -> Option<&'a [u8]> {
    let len = varint(bytes, at)? as usize;
    let end = at.checked_add(len)?;
    if end > bytes.len() {
        return None;
    }
    let slice = &bytes[*at..end];
    *at = end;
    Some(slice)
}

fn skip(wire: u64, bytes: &[u8], at: &mut usize) -> Option<()> {
    match wire {
        0 => {
            varint(bytes, at)?;
        }
        1 => *at += 8,
        2 => {
            len_delimited(bytes, at)?;
        }
        5 => *at += 4,
        _ => return None,
    }
    bytes.get(*at).map(|_| ())
}

fn tag(out: &mut Vec<u8>, number: u64, wire: u64) {
    put_varint(out, (number << 3) | wire);
}

fn put_u64_field(out: &mut Vec<u8>, number: u64, value: u64) {
    if value != 0 {
        tag(out, number, 0);
        put_varint(out, value);
    }
}

fn put_u32_field(out: &mut Vec<u8>, number: u64, value: u32) {
    put_u64_field(out, number, u64::from(value));
}

fn put_bool_field(out: &mut Vec<u8>, number: u64, value: bool) {
    if value {
        tag(out, number, 0);
        out.push(1);
    }
}

fn put_str_field(out: &mut Vec<u8>, number: u64, value: &str) {
    if !value.is_empty() {
        tag(out, number, 2);
        put_varint(out, value.len() as u64);
        out.extend_from_slice(value.as_bytes());
    }
}

fn put_msg_field(out: &mut Vec<u8>, number: u64, body: &[u8]) {
    tag(out, number, 2);
    put_varint(out, body.len() as u64);
    out.extend_from_slice(body);
}

fn u64_of(bytes: &[u8], at: &mut usize) -> Option<u64> {
    varint(bytes, at)
}

fn u32_of(bytes: &[u8], at: &mut usize) -> Option<u32> {
    u64_of(bytes, at).map(|v| v as u32)
}

fn f64_of(bytes: &[u8], at: &mut usize) -> Option<f64> {
    if *at + 8 > bytes.len() {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[*at..*at + 8]);
    *at += 8;
    Some(f64::from_le_bytes(buf))
}

fn str_of(bytes: &[u8], at: &mut usize) -> Option<String> {
    let raw = len_delimited(bytes, at)?;
    Some(String::from_utf8_lossy(raw).into_owned())
}

// ── MediaHello (fields 1-10) ─────────────────────────────────────────────────

/// Decode a `MediaHello` from a CONTROL body's `hello` field.
pub fn decode_hello(body: &[u8]) -> Option<MediaHello> {
    let mut hello = MediaHello {
        protocol_version: 0,
        media_session_id: String::new(),
        session_id: String::new(),
        max_fragment_payload: 0,
        cell_size: 0,
        cell_cols: 0,
        cell_rows: 0,
        capabilities: Vec::new(),
        encoder_max_fps: 0,
        start_epoch_ms: 0,
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 0) => hello.protocol_version = u32_of(body, &mut at)?,
            (2, 2) => hello.media_session_id = str_of(body, &mut at)?,
            (3, 2) => hello.session_id = str_of(body, &mut at)?,
            (4, 0) => hello.max_fragment_payload = u32_of(body, &mut at)?,
            (5, 0) => hello.cell_size = u32_of(body, &mut at)?,
            (6, 0) => hello.cell_cols = u32_of(body, &mut at)?,
            (7, 0) => hello.cell_rows = u32_of(body, &mut at)?,
            (8, 2) => hello.capabilities.push(str_of(body, &mut at)?),
            (9, 0) => hello.encoder_max_fps = u32_of(body, &mut at)?,
            (10, 0) => hello.start_epoch_ms = u64_of(body, &mut at)?,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(hello)
}

/// Encode a `MediaHelloAck` as a CONTROL body's `hello_ack` field.
pub fn encode_hello_ack(out: &mut Vec<u8>, ack: &MediaHelloAck) {
    let mut body = Vec::new();
    put_u32_field(&mut body, 1, ack.protocol_version);
    put_str_field(&mut body, 2, &ack.media_session_id);
    put_u32_field(&mut body, 3, ack.accepted_fragment_payload);
    put_u32_field(&mut body, 4, ack.cell_size);
    put_u32_field(&mut body, 5, ack.cell_cols);
    put_u32_field(&mut body, 6, ack.cell_rows);
    for capability in &ack.capabilities {
        tag(&mut body, 7, 2);
        put_varint(&mut body, capability.len() as u64);
        body.extend_from_slice(capability.as_bytes());
    }
    put_u32_field(&mut body, 8, ack.resume_ttl_seconds);
    put_bool_field(&mut body, 9, ack.media_supported);
    if let Some(reason) = &ack.media_unsupported_reason {
        put_str_field(&mut body, 10, reason.as_str());
    }
    put_msg_field(out, 2, &body);
}

// ── MediaResume (fields 1-9) ─────────────────────────────────────────────────

/// Decode a `MediaResume` from a CONTROL body's `resume` field.
pub fn decode_resume(body: &[u8]) -> Option<MediaResume> {
    let mut resume = MediaResume {
        resume_token: String::new(),
        applied_update_id: 0,
        chain_epoch: 0,
        ack_bitmap: 0,
        input_seq_next: 0,
        pressed_keys: Vec::new(),
        pressed_buttons: Vec::new(),
        active_path: mlp_core::media::MediaPath::Ws,
        dc_transport_id: String::new(),
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 2) => resume.resume_token = str_of(body, &mut at)?,
            (2, 0) => resume.applied_update_id = u64_of(body, &mut at)?,
            (3, 0) => resume.chain_epoch = u32_of(body, &mut at)?,
            (4, 0) => resume.ack_bitmap = u64_of(body, &mut at)?,
            (5, 0) => resume.input_seq_next = u64_of(body, &mut at)?,
            (6, 0) => resume.pressed_keys.push(u32_of(body, &mut at)?),
            (7, 0) => resume.pressed_buttons.push(u32_of(body, &mut at)?),
            (8, 0) => {
                resume.active_path = match u32_of(body, &mut at)? {
                    2 => mlp_core::media::MediaPath::DcDirect,
                    3 => mlp_core::media::MediaPath::DcTurn,
                    4 => mlp_core::media::MediaPath::Quic,
                    _ => mlp_core::media::MediaPath::Ws,
                };
            }
            (9, 2) => resume.dc_transport_id = str_of(body, &mut at)?,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(resume)
}

/// Encode a `MediaResumeAck` as a CONTROL body's `resume_ack` field.
pub fn encode_resume_ack(out: &mut Vec<u8>, ack: &MediaResumeAck) {
    let mut body = Vec::new();
    put_bool_field(&mut body, 1, ack.accepted);
    put_str_field(&mut body, 2, ack.reason.as_str());
    put_u64_field(&mut body, 3, ack.next_update_id);
    put_u64_field(&mut body, 4, ack.pending_cells);
    put_u32_field(&mut body, 5, ack.pace_bps);
    put_u32_field(&mut body, 6, ack.budget_bytes);
    put_u32_field(&mut body, 7, ack.target_fps);
    put_bool_field(&mut body, 8, ack.anchor_plan_started);
    put_msg_field(out, 4, &body);
}

// ── the oneof's other arms ──────────────────────────────────────────────────

/// Encode a `MediaLaneState` as a CONTROL body's `lane_state` field.
pub fn encode_lane_state(out: &mut Vec<u8>, state: &MediaLaneState) {
    let mut body = Vec::new();
    put_u32_field(&mut body, 1, state.state.as_u32());
    put_u64_field(&mut body, 2, state.since_epoch_ms);
    put_str_field(&mut body, 3, &state.detail);
    put_msg_field(out, 5, &body);
}

/// Encode a `MediaAnchorPlan` as a CONTROL body's `anchor_plan` field.
pub fn encode_anchor_plan(out: &mut Vec<u8>, plan: &MediaAnchorPlan) {
    let mut body = Vec::new();
    put_u32_field(&mut body, 1, plan.plan_id);
    put_u32_field(&mut body, 2, plan.total_updates_estimate);
    put_u32_field(&mut body, 3, plan.total_cells);
    put_u32_field(&mut body, 4, u32::from(plan.priority));
    put_msg_field(out, 6, &body);
}

/// Encode a `MediaClose` as a CONTROL body's `close` field.
pub fn encode_close(out: &mut Vec<u8>, close: &MediaClose) {
    let mut body = Vec::new();
    put_u32_field(&mut body, 1, close.code.as_u32());
    put_str_field(&mut body, 2, &close.reason);
    put_bool_field(&mut body, 3, close.park);
    put_msg_field(out, 7, &body);
}

/// Encode a `MediaError` as a CONTROL body's `error` field.
pub fn encode_error(out: &mut Vec<u8>, error: &MediaErrorBody) {
    let mut body = Vec::new();
    put_u32_field(&mut body, 1, error.code);
    put_u32_field(&mut body, 2, error.object_id);
    put_str_field(&mut body, 3, &error.detail);
    put_msg_field(out, 8, &body);
}

/// The CONTROL-class bodies the server sends, pre-wrapped for one fragment.
#[derive(Debug, Clone, PartialEq)]
pub enum ControlBody {
    HelloAck(MediaHelloAck),
    ResumeAck(MediaResumeAck),
    LaneState(MediaLaneState),
    AnchorPlan(MediaAnchorPlan),
    Close(MediaClose),
    Error(MediaErrorBody),
}

impl ControlBody {
    /// The oneof-wrapped protobuf bytes for a CONTROL fragment's payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::HelloAck(ack) => encode_hello_ack(&mut out, ack),
            Self::ResumeAck(ack) => encode_resume_ack(&mut out, ack),
            Self::LaneState(state) => encode_lane_state(&mut out, state),
            Self::AnchorPlan(plan) => encode_anchor_plan(&mut out, plan),
            Self::Close(close) => encode_close(&mut out, close),
            Self::Error(error) => encode_error(&mut out, error),
        }
        out
    }
}

/// The CONTROL-class bodies the server receives.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundControl {
    Hello(MediaHello),
    Resume(MediaResume),
    Close(MediaClose),
    Error(MediaErrorBody),
}

impl InboundControl {
    /// Decode a CONTROL fragment's oneof payload. Unknown arms are `None`:
    /// a receiver never speaks them.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let mut at = 0;
        while at < payload.len() {
            let Field(number, wire) = field(payload, &mut at)?;
            if wire != 2 {
                skip(wire, payload, &mut at)?;
                continue;
            }
            let body = len_delimited(payload, &mut at)?;
            return match number {
                1 => decode_hello(body).map(Self::Hello),
                3 => decode_resume(body).map(Self::Resume),
                7 => decode_close(body).map(Self::Close),
                8 => decode_error(body).map(Self::Error),
                _ => continue,
            };
        }
        None
    }
}

fn decode_close(body: &[u8]) -> Option<MediaClose> {
    let mut close = MediaClose {
        code: MediaCloseCode::Normal,
        reason: String::new(),
        park: false,
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 0) => close.code = MediaCloseCode::from_u32(u32_of(body, &mut at)?)?,
            (2, 2) => close.reason = str_of(body, &mut at)?,
            (3, 0) => close.park = u64_of(body, &mut at)? != 0,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(close)
}

fn decode_error(body: &[u8]) -> Option<MediaErrorBody> {
    let mut error = MediaErrorBody {
        code: 0,
        object_id: 0,
        detail: String::new(),
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 0) => error.code = u32_of(body, &mut at)?,
            (2, 0) => error.object_id = u32_of(body, &mut at)?,
            (3, 2) => error.detail = str_of(body, &mut at)?,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(error)
}

// ── MediaFeedback (fields 1-21) with its nested MediaAck ────────────────────

fn decode_ack(body: &[u8]) -> Option<MediaAck> {
    let mut ack = MediaAck {
        highest_applied_update_id: 0,
        ack_bitmap: 0,
        chain_epoch: 0,
        applied_cell_count: 0,
        stale_cells: 0,
        converged: false,
        render_latency_ms: 0.0,
        has_render_latency_ms: false,
        echo_of_input_seq: 0,
        ack_epoch_ms: 0,
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 0) => ack.highest_applied_update_id = u64_of(body, &mut at)?,
            (2, 0) => ack.ack_bitmap = u64_of(body, &mut at)?,
            (3, 0) => ack.chain_epoch = u32_of(body, &mut at)?,
            (4, 0) => ack.applied_cell_count = u32_of(body, &mut at)?,
            (5, 0) => ack.stale_cells = u32_of(body, &mut at)?,
            (6, 0) => ack.converged = u64_of(body, &mut at)? != 0,
            (7, 1) => ack.render_latency_ms = f64_of(body, &mut at)?,
            (8, 0) => ack.has_render_latency_ms = u64_of(body, &mut at)? != 0,
            (9, 0) => ack.echo_of_input_seq = u64_of(body, &mut at)?,
            (10, 0) => ack.ack_epoch_ms = u64_of(body, &mut at)?,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(ack)
}

fn decode_probe_echo(body: &[u8]) -> Option<ProbeEcho> {
    let mut echo = ProbeEcho {
        object_id: 0,
        send_epoch_ms: 0,
        echo_epoch_ms: 0,
        payload_bytes: 0,
    };
    let mut at = 0;
    while at < body.len() {
        let Field(number, wire) = field(body, &mut at)?;
        match (number, wire) {
            (1, 0) => echo.object_id = u32_of(body, &mut at)?,
            (2, 0) => echo.send_epoch_ms = u64_of(body, &mut at)?,
            (3, 0) => echo.echo_epoch_ms = u64_of(body, &mut at)?,
            (4, 0) => echo.payload_bytes = u32_of(body, &mut at)?,
            _ => skip(wire, body, &mut at)?,
        }
    }
    Some(echo)
}

/// Decode a FEEDBACK-class body into the receiver's report.
pub fn decode_feedback(payload: &[u8]) -> Option<MediaFeedback> {
    let mut feedback = MediaFeedback {
        rtt_ms: 0,
        rtt_min_ms: 0,
        jitter_ms: 0,
        received_bytes: 0,
        received_objects: 0,
        lost_objects: 0,
        duplicated_objects: 0,
        reordered_objects: 0,
        window_ms: 0,
        ack: None,
        probes: Vec::new(),
        buffered_amount_bytes: 0,
        display_fps: 0,
        render_ms_p50: 0,
        render_ms_p95: 0,
        input_queue_depth: 0,
        input_submitted: 0,
        input_expired: 0,
        path_switches: 0,
        active_path: mlp_core::media::MediaPath::Ws,
        app_version: String::new(),
    };
    let mut at = 0;
    while at < payload.len() {
        let Field(number, wire) = field(payload, &mut at)?;
        match (number, wire) {
            (1, 0) => feedback.rtt_ms = u32_of(payload, &mut at)?,
            (2, 0) => feedback.rtt_min_ms = u32_of(payload, &mut at)?,
            (3, 0) => feedback.jitter_ms = u32_of(payload, &mut at)?,
            (4, 0) => feedback.received_bytes = u64_of(payload, &mut at)?,
            (5, 0) => feedback.received_objects = u64_of(payload, &mut at)?,
            (6, 0) => feedback.lost_objects = u64_of(payload, &mut at)?,
            (7, 0) => feedback.duplicated_objects = u64_of(payload, &mut at)?,
            (8, 0) => feedback.reordered_objects = u64_of(payload, &mut at)?,
            (9, 0) => feedback.window_ms = u64_of(payload, &mut at)?,
            (10, 2) => feedback.ack = Some(decode_ack(len_delimited(payload, &mut at)?)?),
            (11, 2) => feedback
                .probes
                .push(decode_probe_echo(len_delimited(payload, &mut at)?)?),
            (12, 0) => feedback.buffered_amount_bytes = u32_of(payload, &mut at)?,
            (13, 0) => feedback.display_fps = u32_of(payload, &mut at)?,
            (14, 0) => feedback.render_ms_p50 = u32_of(payload, &mut at)?,
            (15, 0) => feedback.render_ms_p95 = u32_of(payload, &mut at)?,
            (16, 0) => feedback.input_queue_depth = u32_of(payload, &mut at)?,
            (17, 0) => feedback.input_submitted = u64_of(payload, &mut at)?,
            (18, 0) => feedback.input_expired = u64_of(payload, &mut at)?,
            (19, 0) => feedback.path_switches = u64_of(payload, &mut at)?,
            (20, 0) => {
                feedback.active_path = match u32_of(payload, &mut at)? {
                    2 => mlp_core::media::MediaPath::DcDirect,
                    3 => mlp_core::media::MediaPath::DcTurn,
                    4 => mlp_core::media::MediaPath::Quic,
                    _ => mlp_core::media::MediaPath::Ws,
                };
            }
            (21, 2) => feedback.app_version = str_of(payload, &mut at)?,
            _ => skip(wire, payload, &mut at)?,
        }
    }
    Some(feedback)
}
