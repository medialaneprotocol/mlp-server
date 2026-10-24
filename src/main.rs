//! mlp-server: reference WebSocket server for MLP.

use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::accept_async;

use mlp_core::media::cells::CellGrid;
use mlp_core::media::pdu::Carrier;
use mlp_core::media::sender::{MediaSenderConfig, MediaSenderMachine};

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await.expect("bind failed");
    println!("mlp-server listening on {}", addr);

    loop {
        if let Ok((stream, peer)) = listener.accept().await {
            tokio::spawn(handle_connection(stream, peer));
        }
    }
}

async fn handle_connection(stream: TcpStream, peer: SocketAddr) {
    println!("new connection from {}", peer);
    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("ws accept error: {}", e);
            return;
        }
    };
    let (mut sink, mut source) = ws.split();

    let grid = CellGrid::with_cell_size(320, 240, 64);
    let now = std::time::Instant::now();
    let sender = MediaSenderMachine::new(
        MediaSenderConfig::new(Carrier::Ws, grid),
        grid,
        now,
    );
    let _ = sender;

    // Echo loop: receive fragments, send acks.
    while let Some(msg) = source.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                // Parse fragment, send back echo (simplified).
                let _ = sink.send(Message::Binary(data)).await;
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("ws error: {}", e);
                break;
            }
        }
    }
    println!("connection closed: {}", peer);
}
