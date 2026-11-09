# Base images are pinned by exact version and digest. To update, pick the new
# tag, run `docker buildx imagetools inspect <image>:<tag>` and paste its
# digest. The Rust version matches rust-toolchain.toml (wasm/build.sh keeps its
# own pin: the checked-in wasm bytes depend on it).
FROM rust:1.99.1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src
COPY ui ./ui
# `src/js.rs` embeds the checked-in QuickJS WASI runner, and `src/view.rs` and
# `src/search.rs` the shared JS wrappers, at compile time; `src/js.rs` also
# includes the output limits it shares with the runner.
RUN mkdir -p wasm
COPY wasm/qjs-runner.wasm ./wasm/qjs-runner.wasm
COPY wasm/js ./wasm/js
COPY wasm/limits.rs ./wasm/limits.rs
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
