//! One media-lane session: the WebSocket, the two obligations of a sender,
//! and the park table the session's continuity lives in.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mlp_core::media::cells::CellGrid;
use mlp_core::media::pdu::{Carrier, Fragment, FragmentClass, FragmentFlags, Priority, MEDIA_PROTOCOL_VERSION};
use mlp_core::media::resume::{ParkEntry, ParkPolicy, ParkTable, ResumeRejectReason};
use mlp_core::media::metrics::PrometheusTelemetry;
use mlp_core::media::sender::{
    EnqueueOutcome, MediaSenderConfig, MediaSenderEvent, MediaSenderMachine, UpdateRequest,
};
use mlp_core::media::{
    MediaAnchorPlan, MediaClose, MediaCloseCode, MediaHelloAck, MediaLaneState, MediaResumeAck,
};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;

use crate::wire::{self, ControlBody, InboundControl};

/// The media lane's WebSocket subprotocol (MLP spec §6.1).
pub const MEDIA_LANE_SUBPROTOCOL: &str = "mlp-v1";
/// How often the session's sender obligations run.
const TICK: Duration = Duration::from_millis(10);
/// The synthetic surface's geometry: 320x180 at 64 px cells — 5x3, 15 cells.
fn grid() -> CellGrid {
    CellGrid::with_cell_size(320, 180, 64)
}
/// The park/resume window the hello ack advertises.
const RESUME_TTL_SECONDS: u32 = 300;

/// The session ids: one counter, opaque to the peer.
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

/// The shared state every session sees.
pub struct Shared {
    pub park_table: Mutex<ParkTable>,
    pub telemetry: PrometheusTelemetry,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            park_table: Mutex::new(ParkTable::new(256)),
            telemetry: PrometheusTelemetry::default(),
        })
    }
}

/// Accept the lane only under the protocol's own subprotocol.
///
/// The handshake callback's error type is tungstenite's own, and its size
/// is that crate's choice, not this server's.
#[allow(clippy::result_large_err)]
fn check_subprotocol(request: &Request, response: Response) -> Result<Response, ErrorResponse> {
    let protocols = request.headers().get("Sec-WebSocket-Protocol");
    let agreed = protocols
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(',').any(|p| p.trim() == MEDIA_LANE_SUBPROTOCOL))
        .unwrap_or(false);
    if agreed {
        let mut response = response;
        response.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            header_value(MEDIA_LANE_SUBPROTOCOL),
        );
        Ok(response)
    } else {
        Err(ErrorResponse::new(None))
    }
}


fn header_value(value: &str) -> tokio_tungstenite::tungstenite::http::HeaderValue {
    tokio_tungstenite::tungstenite::http::HeaderValue::from_str(value)
        .expect("the subprotocol is a valid header value")
}

/// Run one lane to its close.
pub async fn serve(stream: TcpStream, shared: Arc<Shared>) {
    let ws = match accept_hdr_async(stream, check_subprotocol).await {
        Ok(ws) => ws,
        Err(_) => return,
    };
    let (mut sink, mut source) = ws.split();
    let mut session = Session::new(shared);

    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            message = source.next() => {
                match message {
                    Some(Ok(Message::Binary(bytes))) => {
                        if !session.on_bytes(&bytes, &mut sink).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(_))) => {
                        // A text message is a framing breach (MLP spec §6.3):
                        // close 4400, not a string on the canvas.
                        let frame = tokio_tungstenite::tungstenite::protocol::frame::CloseFrame {
                            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(4400),
                            reason: "text is not a fragment".into(),
                        };
                        let _ = sink.send(Message::Close(Some(frame))).await;
                        break;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            _ = ticker.tick() => {
                if !session.tick(&mut sink).await {
                    break;
                }
            }
        }
    }
    session.finish(&mut sink).await;
}

/// The per-lane state: the sender machine, the synthetic capture, the
/// emitted-cell record the repair cells need.
struct Session {
    shared: Arc<Shared>,
    media_session_id: String,
    control_session_id: String,
    attached: bool,
    sender: Option<MediaSenderMachine>,
    frame: u64,
    capture_dirty: Vec<u32>,
    emitted: Vec<u64>,
    last_emit: Instant,
    converged: bool,
    parked: bool,
    closed: bool,
}

impl Session {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            media_session_id: String::new(),
            control_session_id: String::new(),
            attached: false,
            sender: None,
            frame: 0,
            capture_dirty: Vec::new(),
            emitted: vec![0; grid().cells() as usize],
            last_emit: Instant::now(),
            converged: false,
            parked: false,
            closed: false,
        }
    }

    /// One inbound WebSocket message. Returns false when the lane is done.
    async fn on_bytes<S>(&mut self, bytes: &[u8], sink: &mut S) -> bool
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        self.shared.telemetry.record_fragment_received(bytes.len() as u64);
        let Ok((fragment, consumed)) = Fragment::parse(bytes, Carrier::Ws) else {
            self.send_control(sink, ControlBody::Error(error_body(7, 0, "the fragment did not parse"))).await;
            return true;
        };
        if consumed != bytes.len() {
            self.send_control(sink, ControlBody::Error(error_body(7, 0, "trailing bytes after the fragment"))).await;
            return true;
        }
        match fragment.header.class {
            FragmentClass::Control => self.on_control(&fragment, sink).await,
            FragmentClass::Feedback => {
                self.on_feedback(&fragment, sink).await;
                true
            }
            _ => true, // the receiver speaks control and feedback; media up
                       // the lane is not this server's shape
        }
    }

    async fn on_control<S>(&mut self, fragment: &Fragment, sink: &mut S) -> bool
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        let Some(control) = InboundControl::decode(&fragment.payload) else {
            return true;
        };
        match control {
            InboundControl::Hello(hello) => {
                if self.attached {
                    return true;
                }
                self.control_session_id = hello.session_id.clone();
                self.attach(None, sink).await;
                true
            }
            InboundControl::Resume(resume) => {
                if self.attached {
                    return true;
                }
                self.control_session_id = resume.resume_token.clone();
                let mut table = self.shared.park_table.lock().await;
                let entry = table.resume(&resume.resume_token);
                drop(table);
                match entry {
                    Some(_) => {
                        self.attach(Some(resume.resume_token.clone()), sink).await;
                        let sender = self.sender.as_ref().expect("just attached");
                        // A resume onto held state re-bases: the anchor plan
                        // carries what the receiver's snapshot lost.
                        let ack = MediaResumeAck {
                            accepted: true,
                            reason: ResumeRejectReason::Ok,
                            next_update_id: sender.next_update_id(),
                            pending_cells: sender.accounting().pending_count() as u64,
                            pace_bps: sender.estimate().pace_bps as u32,
                            budget_bytes: sender.estimate().max_object_bytes,
                            target_fps: 30,
                            anchor_plan_started: true,
                        };
                        if let Some(sender) = self.sender.as_mut() {
                            sender.request_plan(mlp_core::media::scheduler::PlanStartReason::Resume);
                        }
                        self.send_control(sink, ControlBody::ResumeAck(ack)).await;
                    }
                    None => {
                        self.attach(None, sink).await;
                        let ack = MediaResumeAck {
                            accepted: false,
                            reason: ResumeRejectReason::UnknownSession,
                            next_update_id: self.sender.as_ref().map(|s| s.next_update_id()).unwrap_or(1),
                            pending_cells: 0,
                            pace_bps: 0,
                            budget_bytes: 0,
                            target_fps: 0,
                            anchor_plan_started: false,
                        };
                        self.send_control(sink, ControlBody::ResumeAck(ack)).await;
                    }
                }
                true
            }
            InboundControl::Close(close) => {
                if close.park && !self.media_session_id.is_empty() {
                    self.parked = true;
                    let mut table = self.shared.park_table.lock().await;
                    let policy = ParkPolicy {
                        park_ttl_seconds: u64::from(RESUME_TTL_SECONDS),
                        resume_ttl_seconds: u64::from(RESUME_TTL_SECONDS),
                        max_parked_sessions: 256,
                    };
                    table.park(
                        ParkEntry {
                            media_session_id: self.media_session_id.clone(),
                            session_id: self.control_session_id.clone(),
                            viewer_id: String::new(),
                            host_id: String::new(),
                            parked_at_secs: start_secs(),
                            epoch: self
                                .sender
                                .as_ref()
                                .map(|s| s.accounting().grid().cells())
                                .unwrap_or(0),
                        },
                        start_secs(),
                        &policy,
                    );
                    drop(table);
                    self.send_control(sink, ControlBody::Close(MediaClose {
                        code: MediaCloseCode::Park,
                        reason: "parked; bring the token back".into(),
                        park: true,
                    })).await;
                } else {
                    self.send_control(sink, ControlBody::Close(MediaClose {
                        code: MediaCloseCode::Normal,
                        reason: "closing".into(),
                        park: false,
                    })).await;
                }
                self.closed = true;
                false
            }
            InboundControl::Error(_) => true,
        }
    }

    async fn on_feedback<S>(&mut self, fragment: &Fragment, sink: &mut S)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        let Some(feedback) = wire::decode_feedback(&fragment.payload) else {
            return;
        };
        if feedback.lost_objects > 0 {
            self.shared.telemetry.record_loss();
        }
        if let Some(ack) = &feedback.ack {
            self.shared.telemetry.record_update_applied();
            if ack.converged && !self.converged {
                self.converged = true;
                self.shared.telemetry.record_convergence();
            }
        }
        let Some(sender) = self.sender.as_mut() else {
            return;
        };
        let now = Instant::now();
        let events = sender.on_feedback(feedback, now);
        self.drain_events(events, sink).await;
    }

    async fn attach<S>(&mut self, resumed_token: Option<String>, sink: &mut S)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        let now = Instant::now();
        let media_session_id = resumed_token.unwrap_or_else(|| {
            format!("m-{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed))
        });
        let mut sender = MediaSenderMachine::new(MediaSenderConfig::new(Carrier::Ws, grid()), grid(), now);
        // The first thing a fresh receiver needs is the whole surface: an
        // anchor plan, not a delta against nothing.
        sender.request_plan(mlp_core::media::scheduler::PlanStartReason::Pending);
        self.sender = Some(sender);
        self.attached = true;
        self.media_session_id = media_session_id.clone();
        self.capture_dirty = (0..grid().cells()).collect();
        let ack = MediaHelloAck {
            protocol_version: MEDIA_PROTOCOL_VERSION as u32,
            media_session_id,
            accepted_fragment_payload: Carrier::Ws.fragment_payload_max() as u32,
            cell_size: grid().cell_size(),
            cell_cols: grid().cols(),
            cell_rows: grid().rows(),
            capabilities: vec!["anchor".into(), "patch".into(), "cursor".into()],
            resume_ttl_seconds: RESUME_TTL_SECONDS,
            media_supported: true,
            media_unsupported_reason: None,
        };
        self.send_control(sink, ControlBody::HelloAck(ack)).await;
    }

    /// The sender's obligations: timers, the capture's updates, fragments.
    async fn tick<S>(&mut self, sink: &mut S) -> bool
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        let Some(sender) = self.sender.as_mut() else {
            return true;
        };
        let now = Instant::now();
        for event in sender.handle_timeout_events(now) {
            if let MediaSenderEvent::ProbeDue { payload_bytes, .. } = event {
                let probe = mlp_core::media::MediaProbe {
                    probe_id: 0,
                    payload_bytes,
                    send_epoch_ms: 0,
                };
                let payload = vec![0u8; payload_bytes as usize];
                sender.enqueue_probe(&probe, payload, now);
            }
        }
        let events = sender.poll_lane(now);
        self.drain_events(events, sink).await;

        // The synthetic capture: a band of cells changes every 100 ms.
        if now.duration_since(self.last_emit) >= Duration::from_millis(100) {
            self.last_emit = now;
            self.frame += 1;
            let cells = grid().cells();
            let head = (self.frame % u64::from(cells)) as u32;
            for offset in 0..3u32 {
                let cell = (head + offset) % cells;
                if !self.capture_dirty.contains(&cell) {
                    self.capture_dirty.push(cell);
                }
            }
        }

        self.emit_update();
        self.release_fragments(sink).await;
        !self.closed
    }

    fn emit_update(&mut self) {
        let Some(sender) = self.sender.as_mut() else {
            return;
        };
        let now = Instant::now();
        let pending = sender.accounting().pending_count();
        if self.capture_dirty.is_empty() && pending == 0 {
            if let Some(idle) = sender.poll_idle(now) {
                let update_id = sender.next_update_id();
                if matches!(sender.enqueue_update(idle, now), EnqueueOutcome::Accepted { .. }) {
                    let _ = update_id;
                }
            }
            return;
        }
        let fresh = self.capture_dirty.clone();
        let refresh: Vec<u32> = self
            .emitted
            .iter()
            .enumerate()
            .filter(|(_, update)| **update == 0)
            .map(|(index, _)| index as u32)
            .take(64)
            .collect();
        let decision = sender.plan_next_update(&fresh, &refresh, now);
        if decision.cells.is_empty() {
            return;
        }
        let update_id = sender.next_update_id();
        // The synthetic source's cost: four bytes per cell, opaque to the
        // protocol (a codec owns the media body, not the lane).
        let mut payload = Vec::with_capacity(decision.cells.len() * 4);
        for cell in &decision.cells {
            payload.extend_from_slice(&u32::to_le_bytes(*cell ^ (update_id as u32)));
        }
        let request = UpdateRequest {
            parts: vec![payload],
            cells: decision.cells.clone(),
            anchor: decision.anchor,
            kind: decision.kind,
            reflects_input_seq: 0,
            update_bytes: 0,
            stream_id: 0,
            path_id: 0,
            echo_critical: false,
            deadline_epoch_ms: None,
        };
        if let EnqueueOutcome::Accepted { .. } = sender.enqueue_update(request, now) {
            for cell in &decision.cells {
                self.emitted[*cell as usize] = update_id;
            }
            self.capture_dirty.retain(|dirty| !decision.cells.contains(dirty));
        }
    }

    async fn release_fragments<S>(&mut self, sink: &mut S)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        let Some(sender) = self.sender.as_mut() else {
            return;
        };
        let now = Instant::now();
        while let Some(fragment) = sender.poll_fragment(now) {
            self.shared
                .telemetry
                .record_fragment_sent(fragment.bytes.len() as u64);
            if sink.send(Message::Binary(fragment.bytes)).await.is_err() {
                return;
            }
        }
    }

    async fn drain_events<S>(&mut self, events: Vec<MediaSenderEvent>, sink: &mut S)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        for event in events {
            match event {
                MediaSenderEvent::LaneStateChanged(state) => {
                    let object = MediaLaneState {
                        state,
                        since_epoch_ms: 0,
                        detail: String::new(),
                    };
                    self.send_control(sink, ControlBody::LaneState(object)).await;
                }
                MediaSenderEvent::PlanStarted { plan_id, cells, updates_estimate, .. } => {
                    let plan = MediaAnchorPlan {
                        plan_id,
                        total_updates_estimate: updates_estimate,
                        total_cells: cells,
                        priority: 4,
                    };
                    self.send_control(sink, ControlBody::AnchorPlan(plan)).await;
                }
                MediaSenderEvent::CloseRequested { .. } => {
                    self.closed = true;
                }
                _ => {}
            }
        }
    }

    async fn send_control<S>(&mut self, sink: &mut S, body: ControlBody)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        if let Some(bytes) = encode_control(&body) {
            let _ = sink.send(Message::Binary(bytes)).await;
        }
    }

    async fn finish<S>(&mut self, _sink: &mut S)
    where
        S: SinkExt<Message> + Unpin,
        S::Error: std::fmt::Debug,
    {
        if !self.parked && !self.media_session_id.is_empty() {
            let mut table = self.shared.park_table.lock().await;
            table.end(&self.media_session_id);
        }
    }
}

fn error_body(code: u32, object_id: u32, detail: &str) -> mlp_core::media::MediaErrorBody {
    mlp_core::media::MediaErrorBody {
        code,
        object_id,
        detail: detail.to_string(),
    }
}

/// One control object is one fragment: CONTROL, URGENT, FIN (INV-1).
fn encode_control(body: &ControlBody) -> Option<Vec<u8>> {
    let payload = body.encode();
    let object_id = NEXT_SESSION.fetch_add(1, Ordering::Relaxed) as u32;
    let ext = mlp_core::media::pdu::HeaderExt::new();
    let fragment = Fragment::new(
        FragmentClass::Control,
        FragmentFlags::URGENT.union(FragmentFlags::FIN),
        Priority::CONTROL,
        object_id,
        0,
        1,
        ext,
        payload,
    )
    .ok()?;
    Some(fragment.encode())
}

fn start_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
