#!/usr/bin/env python3
"""Make a local S3 mock behave like S3 on the wire: keep-alive connections and latency.

Put this in front of moto (or any S3-compatible server) and point `AWS_ENDPOINT` at it.
Two things change for the client:

  * **Connections stay open between requests**, as S3's do. moto closes its connections,
    so a client that reuses a pooled connection in a way that cannot work — a connection
    opened on one async runtime, sent on from another while the first is idle — passes
    against moto and hangs against S3 until its request times out. Through this proxy it
    hangs here too.
  * **Every request waits `--delay-ms` first**, so a run's request count costs what it
    would against a real store: a client that reads one file after another is as slow
    here as it would be there.

It listens on 127.0.0.1 unless `--host` says otherwise: a catalog running in a container
(examples/catalog) writes its metadata through it, so it needs `--host 0.0.0.0` there. The
proxy has no authentication of its own; never expose it beyond a test network.

It also counts requests by kind (HEAD, GET, ranged GET, PUT, POST, DELETE) and the most it
saw in flight at once. Control endpoints (not forwarded):

    GET  /__stats          the counts, as JSON
    POST /__reset          zero the counts
    POST /__delay?ms=N     change the delay

Usage:

    python examples/merge/s3_proxy.py --listen 5556 --upstream 5555 --delay-ms 2
"""

import argparse
import collections
import http.client
import http.server
import json
import threading
import time
import urllib.parse

HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "proxy-connection", "te",
              "trailer", "upgrade"}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--listen", type=int, required=True, help="port to listen on")
    ap.add_argument("--host", default="127.0.0.1",
                    help="address to listen on (0.0.0.0 when a catalog in a container writes through it)")
    ap.add_argument("--upstream", type=int, required=True, help="port of the S3 mock (127.0.0.1)")
    ap.add_argument("--delay-ms", type=float, default=0, help="added to every request")
    a = ap.parse_args()

    state = {"delay": a.delay_ms / 1000, "in_flight": 0, "max_in_flight": 0}
    counts = collections.Counter()
    lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive, as S3

        def log_message(self, *args):
            pass

        def read_body(self):
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                body = b""
                while True:
                    size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                    if size == 0:
                        self.rfile.readline()
                        return body
                    body += self.rfile.read(size)
                    self.rfile.readline()
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else None

        def reply(self, data):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def control(self):
            url = urllib.parse.urlparse(self.path)
            with lock:
                if url.path == "/__stats":
                    self.reply(json.dumps({**counts, "max_in_flight": state["max_in_flight"]}).encode())
                elif url.path == "/__reset":
                    counts.clear()
                    state["max_in_flight"] = 0
                    self.reply(b"{}")
                elif url.path == "/__delay":
                    state["delay"] = float(urllib.parse.parse_qs(url.query)["ms"][0]) / 1000
                    self.reply(b"{}")
                else:
                    return False
            return True

        def forward(self):
            if self.path.startswith("/__") and self.control():
                return
            body = self.read_body()
            kind = "GET range" if self.command == "GET" and "Range" in self.headers else self.command
            with lock:
                counts[kind] += 1
                state["in_flight"] += 1
                state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
            try:
                time.sleep(state["delay"])
                upstream = http.client.HTTPConnection("127.0.0.1", a.upstream, timeout=300)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
                upstream.request(self.command, self.path, body=body, headers=headers)
                resp = upstream.getresponse()
                data = resp.read()
                self.send_response(resp.status, resp.reason)
                length = "0"
                for k, v in resp.getheaders():
                    if k.lower() == "content-length":
                        length = v
                    elif k.lower() not in HOP_BY_HOP:
                        self.send_header(k, v)
                # A HEAD answer carries the object's length but no body.
                self.send_header("Content-Length", length if self.command == "HEAD" else str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)
                upstream.close()
            finally:
                with lock:
                    state["in_flight"] -= 1

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward

    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

    Server((a.host, a.listen), Handler).serve_forever()


if __name__ == "__main__":
    main()
