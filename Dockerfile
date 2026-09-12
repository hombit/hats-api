FROM rust:1.98.1-slim-trixie AS build

RUN apt-get update \
    && apt-get install --no-install-recommends -y cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin hats

LABEL org.opencontainers.image.source="https://github.com/hombit/hats-api" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="hats-api" \
      org.opencontainers.image.description="Query HATS catalogs over HTTP"

COPY --from=build /build/target/release/hats-api /usr/local/bin/hats-api
USER hats

EXPOSE 80
ENTRYPOINT ["/usr/local/bin/hats-api"]
