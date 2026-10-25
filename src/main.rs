//! mlp-server: the reference WebSocket host for the Media Lane Protocol.
//!
//! One process, two listeners: the media lane speaks `mlp-v1` WebSocket
//! fragments (MLP_PORT, default 8080), and a plain HTTP listener serves the
//! core telemetry sink's eight counters (MLP_METRICS_PORT, default 9090).

mod session;
mod wire;

use std::sync::Arc;

use session::Shared;

#[tokio::main]
async fn main() {
    let lane_port: u16 = std::env::var("MLP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);
    let metrics_port: u16 = std::env::var("MLP_METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);

    let shared = Shared::new();
    tokio::spawn(serve_metrics(metrics_port, shared.clone()));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", lane_port))
        .await
        .expect("the media lane's port is bindable");
    println!("mlp-server: media lane on :{lane_port} (mlp-v1), metrics on :{metrics_port}");

    loop {
        if let Ok((stream, _peer)) = listener.accept().await {
            let shared = shared.clone();
            tokio::spawn(session::serve(stream, shared));
        }
    }
}

/// GET /metrics renders the telemetry sink; anything else is a 404.
async fn serve_metrics(port: u16, shared: Arc<Shared>) {
    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => listener,
        Err(_) => return,
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let read = match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.read(&mut buf),
            )
            .await
            {
                Ok(Ok(read)) => read,
                _ => return,
            };
            let head = String::from_utf8_lossy(&buf[..read]);
            let is_metrics = head.starts_with("GET /metrics") || head.starts_with("get /metrics");
            let (status, body) = if is_metrics {
                ("200 OK", shared.telemetry.render())
            } else {
                ("404 Not Found", String::new())
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}
