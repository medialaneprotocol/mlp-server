# mlp-server

Reference WebSocket server for the Media Lane Protocol (MLP).

Uses mlp-core for protocol logic. Listens on port 8080.

## Build

```sh
cargo build --release
./target/release/mlp-server
```

## Docker

```sh
docker build -t mlp-server .
docker run -p 8080:8080 mlp-server
```
