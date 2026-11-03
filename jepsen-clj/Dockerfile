FROM clojure:tools-deps-bookworm

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

# The Jepsen runner controls the douchdb node containers through the mounted
# Docker socket. Use the static client so the image does not need a daemon.
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-29.9.2.tgz \
        -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

WORKDIR /work
