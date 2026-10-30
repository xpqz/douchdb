FROM rust:1-bookworm AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ui ./ui
RUN cargo build --release --locked

FROM debian:bookworm-slim

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
