//! mlp-server: reference WebSocket server for MLP.

use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::accept_async;

use mlp_core::media::cells::CellGrid;
use mlp_core::media::pdu::{Carrier, Fragment, MEDIA_PROTOCOL_VERSION};
use mlp_core::media::sender::{MediaSenderConfig, MediaSenderMachine};
use mlp_core::media::receiver::{MediaReceiverConfig, MediaReceiverMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionState {
    Handshaking,
    Active,
    Parked,
    Closing,
    Closed,
}

#[allow(dead_code)]
struct Session {
    id: u64,
    state: SessionState,
    sender: MediaSenderMachine,
    receiver: MediaReceiverMachine,
    grid: CellGrid,
}

impl Session {
    fn new(id: u64) -> Self {
        let grid = CellGrid::with_cell_size(320, 240, 64);
        let now = std::time::Instant::now();
        Self {
            id,
            state: SessionState::Handshaking,
            sender: MediaSenderMachine::new(MediaSenderConfig::new(Carrier::Ws, grid), grid, now),
            receiver: MediaReceiverMachine::new(MediaReceiverConfig::for_carrier(Carrier::Ws), now),
            grid,
        }
    }
}

type Sessions = Arc<Mutex<HashMap<u64, Session>>>;

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await.expect("bind failed");
    println!("mlp-server listening on {}", addr);

    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let next_id = Arc::new(Mutex::new(1u64));

    loop {
        if let Ok((stream, peer)) = listener.accept().await {
            let sessions = sessions.clone();
            let next_id = next_id.clone();
            tokio::spawn(handle_connection(stream, peer, sessions, next_id));
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    sessions: Sessions,
    next_id: Arc<Mutex<u64>>,
) {
    println!("new connection from {}", peer);
    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("ws accept error: {}", e);
            return;
        }
    };
    let (mut sink, mut source) = ws.split();

    // Create session.
    let sid = {
        let mut id = next_id.lock().await;
        let v = *id;
        *id += 1;
        v
    };
    let mut session = Session::new(sid);
    println!("session {} created for {}", sid, peer);

    // Send handshake (limits).
    let handshake = format!(
        "{{\"session_id\":{},\"protocol_version\":{},\"status\":\"ok\"}}",
        sid, MEDIA_PROTOCOL_VERSION
    );
    let _ = sink.send(Message::Text(handshake.into())).await;
    session.state = SessionState::Active;

    // Main loop.
    while let Some(msg) = source.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok((frag, _)) = Fragment::parse(&data, Carrier::Ws) {
                    println!(
                        "session {}: fragment class={:?} object_id={}",
                        sid, frag.header.class, frag.header.object_id
                    );
                    // Echo back.
                    let _ = sink.send(Message::Binary(data)).await;
                } else {
                    let _ = sink.send(Message::Binary(data)).await;
                }
            }
            Ok(Message::Text(text)) => {
                println!("session {}: text: {}", sid, text);
                if text.contains("close") {
                    session.state = SessionState::Closing;
                    let _ = sink.send(Message::Text("{\"status\":\"closing\"}".into())).await;
                    break;
                }
                if text.contains("park") {
                    session.state = SessionState::Parked;
                    let _ = sink.send(Message::Text("{\"status\":\"parked\"}".into())).await;
                } else if text.contains("resume") {
                    session.state = SessionState::Active;
                    let _ = sink.send(Message::Text("{\"status\":\"active\"}".into())).await;
                } else {
                    let _ = sink.send(Message::Text(text)).await;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("session {} ws error: {}", sid, e);
                break;
            }
        }
    }

    session.state = SessionState::Closed;
    sessions.lock().await.insert(sid, session);
    println!("session {} closed: {}", sid, peer);
}
