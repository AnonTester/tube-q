"""
mitmproxy addon: for configured domains, routes the request through
FlareSolverr (bypassing Cloudflare bot-protection) instead of fetching it
directly; every other domain is left untouched (mitmproxy proceeds with its
normal passthrough when `request()` doesn't set flow.response).

FlareSolverr's own URL is infrastructure-level config, set via the
FLARESOLVERR_URL env var on this container (matching the FlareProxy tool
this replaced). The domain list + apply_to_all flag are read from a small
JSON file on a shared volume and reloaded periodically, so Tube-Q's Settings
UI can update them without restarting this container. Expected shape:
    {"domains": ["example.com", "example.org"], "apply_to_all": false}

Cookies returned by FlareSolverr for one request (e.g. the initial page
fetch, which solves the Cloudflare challenge) are remembered per-domain and
replayed on subsequent requests to that domain -- this is what lets a
follow-up AJAX/API call made by the same extractor also get through, since
Cloudflare's challenge cookie carries over.

FlareSolverr's browser-driven fetch wraps a raw JSON response in an HTML
document (Chrome's built-in JSON viewer markup) rather than returning it
as-is -- confirmed empirically against a real API endpoint. That wrapper is
detected and unwrapped so yt-dlp's _download_json() gets real JSON back
instead of failing to parse HTML.
"""
import html
import json
import os
import re
import time

import requests
from mitmproxy import http

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191/v1")
CONFIG_PATH = os.environ.get("FLARESOLVERR_CONFIG_PATH", "/config/flaresolverr_proxy.json")
RELOAD_INTERVAL = 10  # seconds
FLARESOLVERR_TIMEOUT_MS = 60000

_JSON_VIEWER_RE = re.compile(
    r'^\s*<html[^>]*><head>.*?</head><body><pre>(?P<json>.*)</pre>.*</body></html>\s*$',
    re.DOTALL,
)


def _unwrap_json_viewer(body_text):
    """If FlareSolverr's browser rendered the response through Chrome's raw
    JSON viewer, extract and unescape the actual JSON text; otherwise return
    the body unchanged."""
    m = _JSON_VIEWER_RE.match(body_text)
    if not m:
        return body_text, "text/html; charset=utf-8"
    return html.unescape(m.group("json")), "application/json; charset=utf-8"


class FlareSolverrAddon:
    def __init__(self):
        self._config = {"domains": [], "apply_to_all": False}
        self._config_mtime = None
        self._last_reload_check = 0.0
        self._cookies = {}  # domain -> {name: value}
        self._reload_config(force=True)
        print(f"[flaresolverr-addon] using FlareSolverr at {FLARESOLVERR_URL}, "
              f"config file {CONFIG_PATH}")

    def _reload_config(self, force=False):
        now = time.time()
        if not force and (now - self._last_reload_check) < RELOAD_INTERVAL:
            return
        self._last_reload_check = now
        try:
            mtime = os.path.getmtime(CONFIG_PATH)
            if not force and mtime == self._config_mtime:
                return
            with open(CONFIG_PATH) as f:
                self._config = json.load(f)
            self._config_mtime = mtime
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[flaresolverr-addon] failed to reload config: {e}")

    def _domain_matches(self, host):
        if self._config.get("apply_to_all"):
            return True
        for d in self._config.get("domains") or []:
            d = (d or "").strip().lower()
            if not d:
                continue
            if host == d or host.endswith("." + d):
                return True
        return False

    def request(self, flow: http.HTTPFlow):
        self._reload_config()
        host = (flow.request.host or "").lower()
        if not self._domain_matches(host):
            return

        method = flow.request.method.upper()
        url = flow.request.pretty_url
        cmd = "request.post" if method == "POST" else "request.get"

        known = dict(self._cookies.get(host, {}))
        for name, value in flow.request.cookies.items():
            known.setdefault(name, value)
        cookies_payload = [{"name": n, "value": v} for n, v in known.items()]

        payload = {
            "cmd": cmd,
            "url": url,
            "maxTimeout": FLARESOLVERR_TIMEOUT_MS,
            "cookies": cookies_payload,
        }
        if method == "POST":
            payload["postData"] = flow.request.text or ""

        try:
            resp = requests.post(FLARESOLVERR_URL, json=payload, timeout=70)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(data.get("message") or "unknown flaresolverr error")
            solution = data.get("solution") or {}
        except Exception as e:
            flow.response = http.Response.make(
                502,
                f"flaresolverr-addon: {e}".encode("utf-8", errors="replace"),
                {"Content-Type": "text/plain; charset=utf-8"},
            )
            return

        domain_cookies = self._cookies.setdefault(host, {})
        for c in solution.get("cookies") or []:
            name = c.get("name")
            if name:
                domain_cookies[name] = c.get("value", "")

        body_text, content_type = _unwrap_json_viewer(solution.get("response") or "")
        status = int(solution.get("status") or 200)
        flow.response = http.Response.make(
            status,
            body_text.encode("utf-8", errors="replace"),
            {"Content-Type": content_type},
        )


addons = [FlareSolverrAddon()]
