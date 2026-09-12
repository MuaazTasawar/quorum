# ---- Build stage ----
FROM rust:1-slim-bookworm AS builder
WORKDIR /build

# Copy the whole workspace - all crates are needed since `node` depends on
# raft-core/storage/transport via path dependencies.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p node

# ---- Runtime stage ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/node /usr/local/bin/node

# Raft RPC port (internal to the docker network, not published to the host)
# and metrics/dashboard API port (published per-service in docker-compose.yml).
EXPOSE 7000 8000

ENTRYPOINT ["/usr/local/bin/node"]
