"""Read-only UI acceptance server (Release 29 UI consolidation).

Serves ``api/ui`` on an ISOLATED port and proxies ``/v1/*`` to the running
backend **GET-only**. It exists so browser acceptance of a presentation change
can never mutate production state: the proxy is the boundary, not a promise.

Guarantees, enforced here rather than assumed:

* Only ``GET`` and ``HEAD`` reach the backend. Every other method is answered
  ``405`` locally and is never forwarded.
* An allow-list of path prefixes bounds what can be proxied at all; anything
  else is ``403``.
* Nothing is cached, so the acceptance run always sees the file on disk.

Usage::

    python scripts/ui_acceptance_server.py --port 8099 --upstream http://127.0.0.1:8001

The API key is taken from ``PAPER_TRADER_SERVICE_API_KEY`` and injected into the
proxied request, so the acceptance browser never needs the credential.
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
UI_DIR = REPO_ROOT / "api" / "ui"

# Only these prefixes may be proxied at all. A path outside the list is refused
# locally — the acceptance harness has no business reaching anything else.
ALLOWED_PREFIXES = ("/v1/",)

# Read-only methods. Everything else is refused BEFORE any network call.
ALLOWED_METHODS = ("GET", "HEAD")

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
}


class _Handler(BaseHTTPRequestHandler):
    upstream = "http://127.0.0.1:8001"
    api_key = ""
    protocol_version = "HTTP/1.1"

    # ---- refusal helpers ------------------------------------------------ #
    def _refuse(self, code: int, message: str) -> None:
        body = message.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # quieter output
        sys.stderr.write("[ui-acceptance] " + (fmt % args) + "\n")

    # ---- every mutating verb is refused locally ------------------------- #
    def _reject_mutation(self) -> None:
        self._refuse(405, "UI_ACCEPTANCE_READ_ONLY: %s is not permitted by the "
                          "acceptance proxy. No request was forwarded." % self.command)

    do_POST = _reject_mutation
    do_PUT = _reject_mutation
    do_PATCH = _reject_mutation
    do_DELETE = _reject_mutation
    do_OPTIONS = _reject_mutation

    # ---- static + proxied reads ----------------------------------------- #
    def do_HEAD(self) -> None:
        self.do_GET(head_only=True)

    def do_GET(self, head_only: bool = False) -> None:
        path = self.path.split("#", 1)[0]
        if path == "/" or path == "":
            self.send_response(302)
            self.send_header("Location", "/ui/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path.startswith("/ui"):
            return self._serve_static(path, head_only)
        if any(path.startswith(p) for p in ALLOWED_PREFIXES):
            return self._proxy(path, head_only)
        self._refuse(403, "UI_ACCEPTANCE_PATH_NOT_ALLOWED: %s" % path)

    def _serve_static(self, path: str, head_only: bool) -> None:
        rel = path[len("/ui"):].split("?", 1)[0].lstrip("/") or "index.html"
        target = (UI_DIR / rel).resolve()
        try:
            target.relative_to(UI_DIR.resolve())
        except ValueError:
            return self._refuse(403, "UI_ACCEPTANCE_PATH_ESCAPE")
        if target.is_dir():
            target = target / "index.html"
        if not target.is_file():
            return self._refuse(404, "not found: %s" % rel)
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type",
                         _CONTENT_TYPES.get(target.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    def _proxy(self, path: str, head_only: bool) -> None:
        if self.command not in ALLOWED_METHODS:
            return self._reject_mutation()
        url = self.upstream.rstrip("/") + path
        req = urllib.request.Request(url, method="GET")
        if self.api_key:
            req.add_header("X-API-Key", self.api_key)
        req.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                data = r.read()
                status = r.status
                ctype = r.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as e:
            data = e.read() or b"{}"
            status = e.code
            ctype = e.headers.get("Content-Type", "application/json")
        except Exception as e:  # upstream unreachable
            body = ('{"error":"upstream_unreachable","detail":%r}' % str(e)).encode()
            data, status, ctype = body, 502, "application/json"
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(data)


def serve(port: int, upstream: str, api_key: str) -> ThreadingHTTPServer:
    _Handler.upstream = upstream
    _Handler.api_key = api_key
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--upstream", default="http://127.0.0.1:8001")
    args = ap.parse_args()
    key = os.environ.get("PAPER_TRADER_SERVICE_API_KEY", "")
    httpd = serve(args.port, args.upstream, key)
    print("UI_ACCEPTANCE_SERVER_READY http://127.0.0.1:%d/ui/ "
          "(read-only proxy to %s; GET/HEAD only)" % (args.port, args.upstream),
          flush=True)
    try:
        while True:
            threading.Event().wait(3600)
    except KeyboardInterrupt:
        httpd.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
