#!/bin/sh
# Rebuild wasm/qjs-runner.wasm in Docker with the pinned toolchain from
# wasm/README.md, then print its checksum. Needs only Docker; run from anywhere.
# The build is pinned to linux/amd64 because the arm64 and x86_64 WASI SDK
# builds give different wasm bytes; CI rebuilds on x86_64 and compares.
set -eu

IMAGE=rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663
WASI_SDK_VERSION=24.0
SDK_ARM64_SHA256=ae6c1417ea161e54bc54c0a168976af57a0c6e53078857886057a71a0d928646
SDK_X86_64_SHA256=c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5

wasm_dir=$(cd "$(dirname "$0")" && pwd)
volume=qjs-runner-target-$$
trap 'docker volume rm -f "$volume" >/dev/null' EXIT

docker run --rm --platform linux/amd64 \
  -v "$wasm_dir":/work \
  -v "$volume":/target \
  -e CARGO_TARGET_DIR=/target \
  -e SDK_VERSION="$WASI_SDK_VERSION" \
  -e SDK_ARM64_SHA256="$SDK_ARM64_SHA256" \
  -e SDK_X86_64_SHA256="$SDK_X86_64_SHA256" \
  -w /work/qjs-runner \
  "$IMAGE" sh -eu -c '
    case "$(uname -m)" in
      aarch64) arch=arm64; sha=$SDK_ARM64_SHA256 ;;
      x86_64) arch=x86_64; sha=$SDK_X86_64_SHA256 ;;
      *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
    esac
    archive=wasi-sdk-$SDK_VERSION-$arch-linux.tar.gz
    curl -fsSL -o "/tmp/$archive" \
      "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${SDK_VERSION%.*}/$archive"
    echo "$sha  /tmp/$archive" | sha256sum -c -
    mkdir /opt/wasi-sdk
    tar -xzf "/tmp/$archive" -C /opt/wasi-sdk --strip-components=1
    rustup target add wasm32-wasip1
    WASI_SDK=/opt/wasi-sdk cargo build --locked --release --target wasm32-wasip1
    cp /target/wasm32-wasip1/release/qjs-runner.wasm /work/qjs-runner.wasm
  '

(cd "$wasm_dir" && shasum -a 256 qjs-runner.wasm 2>/dev/null || sha256sum qjs-runner.wasm)
