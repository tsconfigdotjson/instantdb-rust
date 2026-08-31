# Builder and runtime must stay on the same Debian release (glibc compat).
FROM rust:1.98-slim-bookworm AS chef
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Dependency-only compile; this layer is reused until the dep set changes.
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p instant-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates postgresql-client && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/instant-server /usr/local/bin/instant-server
COPY LEGACY/server/resources/migrations /migrations
COPY scripts/apply-migrations.sh /apply-migrations.sh
ENV PORT=8888
EXPOSE 8888
CMD ["instant-server"]
