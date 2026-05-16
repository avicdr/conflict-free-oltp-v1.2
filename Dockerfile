FROM rust:1.77-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin crdtdb-daemon

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/crdtdb-daemon /usr/local/bin/crdtdb-daemon
ENV PEER_ID=peer_a
ENV BIND_ADDR=0.0.0.0:8080
ENV DB_PATH=/data
VOLUME /data
EXPOSE 8080
CMD ["crdtdb-daemon"]
