# Multi-stage build for the cluster_chat example + the murmer CLI.
# iroh's transitive deps (vergen-gitcl) require a recent rustc, so track the
# latest stable. Pin the bookworm variant so the build glibc matches the
# debian:bookworm-slim runtime below (a trixie-based image links a newer glibc
# than bookworm provides).
FROM rust:1-slim-bookworm AS builder

# git is needed by vergen-gitcl (reads build version info from the repo).
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
# cluster_chat is the demo node; the murmer CLI generates keys / resolves ids.
RUN cargo build --release -p murmer-examples --bin cluster_chat \
 && cargo build --release -p murmer --features cli --bin murmer

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/cluster_chat /usr/local/bin/cluster_chat
COPY --from=builder /app/target/release/murmer /usr/local/bin/murmer
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
