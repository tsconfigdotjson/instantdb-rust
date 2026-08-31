FROM rust:1.98-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
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
