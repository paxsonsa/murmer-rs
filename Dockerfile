# Multi-stage build for the cluster_chat example
FROM rust:1.85-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release -p murmer-examples --bin cluster_chat

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/cluster_chat /usr/local/bin/cluster_chat

ENTRYPOINT ["cluster_chat"]
