FROM rust:bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/api-gate /app/api-gate
COPY --from=builder /build/gates/api/dev.json /app/dev.json

ENV ENV=dev
ENV RUST_LOG=info
EXPOSE 443

CMD ["/app/api-gate"]
