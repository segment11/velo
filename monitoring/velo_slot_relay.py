#!/usr/bin/env python3
"""
Velo metrics relay for Prometheus.

Velo parses its HTTP query string positionally, with no '=' signs
(e.g. `?metrics` or `?manage&slot&0&view-metrics`). Prometheus always
URL-encodes scrape params as `key=value` and sorts them, so a direct
Prometheus scrape of Velo returns 404. This tiny multi-target exporter
bridges the gap: Prometheus scrapes the relay with normal params, and the
relay fetches the exact positional URL Velo expects.

Prometheus scrape params (all optional except target):
  target=HOST:PORT   the Velo instance to scrape (required)
  slot=<n>|last      per-slot metrics via `?manage&slot&<n>&view-metrics`;
                     `last` is resolved to slotNumber-1 from global metrics.
                     Omit `slot` entirely for the global `?metrics` payload.
  auth=user:pass     optional HTTP basic auth forwarded to Velo.

The relay strips Velo's own `{slot="N"}` label from per-slot output so the
`slot` label is owned by the Prometheus job (via relabel of __param_slot).
"""

import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import base64

LISTEN_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9200

_SLOT_LABEL_RE = re.compile(r'\{slot="[^"]*"\}')
_SLOT_NUMBER_RE = re.compile(r'^global_slot_number\S*\s+([0-9.eE+]+)', re.MULTILINE)


def _fetch(target, query, auth=None, timeout=5):
    """GET http://<target>/?<query> and return the body text."""
    url = "http://{}/?{}".format(target, query)
    req = Request(url)
    if auth:
        token = base64.b64encode(auth.encode()).decode()
        req.add_header("Authorization", "Basic " + token)
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _resolve_last_slot(target, auth):
    """Read global_slot_number from the global metrics to find the last slot index."""
    body = _fetch(target, "metrics", auth)
    m = _SLOT_NUMBER_RE.search(body)
    if not m:
        raise ValueError("global_slot_number not found in global metrics")
    n = int(float(m.group(1)))
    return max(0, n - 1)


def build_payload(params):
    target = params.get("target", [None])[0]
    if not target:
        raise ValueError("missing 'target' param")
    auth = params.get("auth", [None])[0]
    slot = params.get("slot", [None])[0]

    if slot is None:
        # global scrape, pass through untouched
        return _fetch(target, "metrics", auth)

    idx = _resolve_last_slot(target, auth) if slot == "last" else int(slot)
    body = _fetch(target, "manage&slot&{}&view-metrics".format(idx), auth)
    # drop Velo's own slot label; the Prometheus job owns the slot label
    return _SLOT_LABEL_RE.sub("", body)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path not in ("/metrics", "/"):
            self.send_error(404, "use /metrics?target=host:port[&slot=N|last]")
            return
        params = parse_qs(parsed.query)
        try:
            payload = build_payload(params)
        except (HTTPError, URLError) as e:
            self.send_error(502, "velo fetch failed: {}".format(e))
            return
        except Exception as e:  # noqa: BLE001 - surface config errors to the scraper
            self.send_error(400, str(e))
            return
        data = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        pass  # keep the relay quiet


def main():
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print("velo_slot_relay listening on :{}".format(LISTEN_PORT), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
