# Compile without adding a Rust toolchain to the runtime image.
#
# Pinned rather than `rust:trixie`, so the image built from a git tag is the image that
# tag builds again a year later. Dependabot bumps it.
FROM rust:1.98.1-slim-trixie AS build

# The slim image carries the toolchain, gcc and libc6-dev; `aws-lc-sys`, which rustls
# pulls in, needs cmake to build its C.
RUN apt-get update \
    && apt-get install --no-install-recommends -y cmake \
    && rm -rf /var/lib/apt/lists/*

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
# HTTPS to an object store, and rustls reads the system roots. Nothing else is
# installed — the image holds one statically-configured binary and its roots.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin hats

# `source` is what links the GHCR package to this repository; CI overwrites these with
# the revision and version it built.
LABEL org.opencontainers.image.source="https://github.com/hombit/hats-api" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="hats-api" \
      org.opencontainers.image.description="Query HATS catalogs over HTTP"

COPY --from=build /build/target/release/hats-api /usr/local/bin/hats-api
USER hats
# 0.0.0.0:80 and "any s3 endpoint, no local files" are the built-in defaults, so the
# image needs no config file. Mount one and point HATS_API_CONFIG at it to narrow
# what the service may read, or to give it a local directory to read from.
#
# There is no HEALTHCHECK: the port and the API prefix are both configuration, so an
# instruction baked in here would name the wrong URL for any deployment that changed
# either, and it would need a copy of curl in the image to ask. Probe
# `GET {api.prefix}/health` from wherever the deployment is described.
EXPOSE 80
ENTRYPOINT ["/usr/local/bin/hats-api"]
