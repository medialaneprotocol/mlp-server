FROM rust:1.88 AS build
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM debian:trixie-slim
COPY --from=build /app/target/release/mlp-server /usr/local/bin/mlp-server
EXPOSE 8080 9090
ENTRYPOINT ["mlp-server"]
