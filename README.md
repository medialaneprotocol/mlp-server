# mlp-server

The reference WebSocket host for the Media Lane Protocol (MLP).

One process, two listeners:

- **The media lane** (`MLP_PORT`, default 8080): a WebSocket under the
  protocol's own `mlp-v1` subprotocol. Binary messages are MLP fragments,
  nothing else — a text frame closes the lane 4400.
- **Telemetry** (`MLP_METRICS_PORT`, default 9090): `GET /metrics` renders
  the core telemetry sink's eight Prometheus counters.

## What a lane does

The server drives `mlp-core`'s real sender machine per session — the same
machine the harness verifies and the conformance corpus records:

- **Attach**: the peer's `MediaHello` gets a `MediaHelloAck` with the
  session's media id, the agreed fragment cap, the cell grid and the
  resume window. A `MediaResume` with a parked token re-attaches the held
  state: a `MediaResumeAck`, a fresh anchor plan, and the cells the
  receiver's snapshot lost.
- **Media**: a synthetic 320x180 capture (5x3 repair cells) changes on a
  100 ms cadence; the scheduler plans each update, the pacer releases its
  fragments, and every fragment leaves as the bytes `mlp-core` encoded.
- **Feedback**: the receiver's `MediaFeedback` (with its `MediaAck` and
  probe echoes) feeds the estimator, the repair accounting and the lane
  state; lane transitions and anchor plans travel back as control objects.
- **Continuity**: `MediaClose{park}` parks the session in the park table
  under its TTL; a later lane resumes it by token. A close without park
  ends the record.
- **Limits**: the wire's own — classes `0..=4`, the reserved flag bit,
  media's reserved priorities, the fragment caps — are enforced by the
  parse itself, and a malformed object gets a typed `MediaError` back.

The control and feedback bodies are protobuf, hand-encoded to the schema's
own field numbers (`mlp-proto`, `mlp.v1`) — the same bytes the browser's
generated bindings make.

## Build & test

```sh
cargo test        # the end-to-end lane test: hello, media, feedback, park, resume, metrics
cargo build --release
```

## Docker

```sh
docker build -t mlp-server .
docker run -p 8080:8080 -p 9090:9090 mlp-server
```
