# Base images are pinned by exact version and digest. To update, pick the new
# tag, run `docker buildx imagetools inspect <image>:<tag>` and paste its
# digest. The Rust version matches wasm/build.sh.
FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src
COPY ui ./ui
# `src/js.rs` embeds the checked-in QuickJS WASI runner, and `src/view.rs` and
# `src/search.rs` the shared JS wrappers, at compile time.
RUN mkdir -p wasm
COPY wasm/qjs-runner.wasm ./wasm/qjs-runner.wasm
COPY wasm/js ./wasm/js
RUN cargo build --release --locked

# debian:bookworm-slim, by digest (the tag is a moving target).
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home douchdb \
    && mkdir -p /data \
    && chown douchdb:douchdb /data

COPY --from=build /src/target/release/douchdb /usr/local/bin/douchdb

USER douchdb
EXPOSE 5984
VOLUME ["/data"]
ENTRYPOINT ["douchdb"]
