#!/usr/bin/env python3
"""Minimal OTLP/HTTP capture endpoint for local OpenTelemetry demos.

This is not a collector: it accepts OTLP protobuf POSTs, writes each payload to
a file, and returns 200. Use `strings`/`protoc --decode` or point a real
collector at douchdb when you need decoded telemetry.

    python3 scripts/otel_capture.py --port 4318 --out target/otel-capture

Then run douchdb with `--otel-endpoint http://127.0.0.1:4318`.
"""

import argparse
import pathlib
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - http.server API
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        name = self.path.strip("/").replace("/", "-") or "root"
        target = self.server.out / f"{time.time_ns()}-{name}.bin"
        target.write_bytes(body)
        print(f"{self.path}  {len(body)} bytes  -> {target}", flush=True)
        self.send_response(200)
        self.send_header("content-type", "application/x-protobuf")
        self.end_headers()

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4318)
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("target/otel-capture"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.out = args.out
    print(f"capturing OTLP/HTTP on http://127.0.0.1:{args.port} into {args.out}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
