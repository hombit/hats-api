# Compile without adding a Rust toolchain to the runtime image.
FROM rust:trixie AS build

WORKDIR /build
# Dependencies on their own layer: DataFusion is a multi-minute build, and this
# layer only has to be redone when Cargo.toml or Cargo.lock changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
# The placeholder main.rs was already compiled, so make sure the real one looks
# newer than the artifact cargo would otherwise reuse.
RUN touch src/main.rs && cargo build --release --locked

FROM debian:trixie-slim AS runtime

# ca-certificates is not optional here: every request this service makes is
# HTTPS to an object store, and rustls reads the system roots.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home hats
COPY --from=build /build/target/release/hats-api /usr/local/bin/hats-api
USER hats
# The binary defaults to 127.0.0.1, which is right on a laptop and useless in a
# container.
ENV HATS_API_LISTEN_ADDR=0.0.0.0:80
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1/api/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/hats-api"]
