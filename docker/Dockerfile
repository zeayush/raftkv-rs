FROM rust:1.78-slim AS builder

WORKDIR /app
COPY . .

# Build in release mode.
RUN apt-get update && apt-get install -y protobuf-compiler clang libclang-dev && \
    cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/raftkv /usr/local/bin/raftkv

EXPOSE 7001 6379

ENTRYPOINT ["raftkv"]
CMD ["--config", "/config/config.toml"]
