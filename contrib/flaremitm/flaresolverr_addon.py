"""
mitmproxy addon: for configured domains, gets past Cloudflare bot-protection
by letting FlareSolverr solve the challenge, instead of fetching directly;
every other domain is left untouched (mitmproxy proceeds with its normal
passthrough when `request()` doesn't set flow.response).

FlareSolverr's own URL is infrastructure-level config, set via the
FLARESOLVERR_URL env var on this container (matching the FlareProxy tool
this replaced). The domain list + apply_to_all flag are read from a small
JSON file on a shared volume and reloaded periodically, so Tube-Q's Settings
UI can update them without restarting this container. Expected shape:
    {"domains": ["example.com", "example.org"], "apply_to_all": false}

Per configured host, the first request is fetched through FlareSolverr (its
browser solves the challenge). FlareSolverr returns the rendered page, which
is fine for HTML/JSON but not for anything else: redirects are followed
inside the browser and a binary body (e.g. a video served from the same
domain via a redirect to a CDN) comes back wrapped in a small HTML player
page instead of the real bytes. So the clearance cookies and user agent
FlareSolverr obtained are remembered per host, and every later request to
that host is passed through to the origin with them injected -- redirects
and binary downloads then behave like a normal browser session, and
large bodies are streamed rather than buffered. If a passed-through request
is challenged again (cookie expired / rejected), the stored clearance is
dropped and that request is re-solved through FlareSolverr.

FlareSolverr's browser-driven fetch also wraps a raw JSON response in an HTML
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


# A browser that navigates straight to a media file renders it inside a tiny
# player document (<html><head>..</head><body><video ...><source src=...>)
# instead of handing over the bytes.
_MEDIA_DOCUMENT_RE = re.compile(
    r'^\s*<html[^>]*>\s*<head>.*?</head>\s*<body[^>]*>\s*<(?:video|audio)\b',
    re.DOTALL | re.IGNORECASE,
)
_MEDIA_SOURCE_RE = re.compile(r'<(?:video|audio|source)\b[^>]*?\bsrc="([^"]+)"', re.IGNORECASE)
MEDIA_REDIRECT_LIMIT = 3  # per URL, within MEDIA_REDIRECT_WINDOW seconds
MEDIA_REDIRECT_WINDOW = 120


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
        self._cookies = {}  # host -> {name: value}
        self._user_agents = {}  # host -> user agent the clearance was issued to
        self._media_redirects = {}  # url -> [timestamps of redirects sent back to the client]
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

    def _is_challenge(self, response):
        """Cloudflare marks interstitial/challenge responses with a
        cf-mitigated header; a bare 403/503 from Cloudflare is treated the
        same way (e.g. a block page for an expired clearance cookie)."""
        if (response.headers.get("cf-mitigated") or "").lower() == "challenge":
            return True
        server = (response.headers.get("server") or "").lower()
        return response.status_code in (403, 503) and server == "cloudflare"

    def _allow_media_redirect(self, url):
        """Loop guard for the solve-then-redirect above: if the redirected
        request keeps getting challenged, stop instead of redirecting forever."""
        now = time.time()
        recent = [t for t in self._media_redirects.get(url, []) if now - t < MEDIA_REDIRECT_WINDOW]
        if len(recent) >= MEDIA_REDIRECT_LIMIT:
            self._media_redirects[url] = recent
            return False
        recent.append(now)
        self._media_redirects[url] = recent
        return True

    def _solve(self, flow: http.HTTPFlow):
        """Fetch the flow's request through FlareSolverr and answer it with
        the result, remembering the clearance cookies + user agent for
        passthrough of later requests to the same host."""
        host = (flow.request.host or "").lower()
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
        if solution.get("userAgent"):
            self._user_agents[host] = solution["userAgent"]

        raw_body = solution.get("response") or ""
        if _MEDIA_DOCUMENT_RE.match(raw_body):
            # The URL is a media download, not a page: FlareSolverr's browser
            # followed the site's redirect and can only give us its player
            # wrapper, never the bytes. The wrapper does name the file's real
            # location, so answer with that redirect -- what the origin itself
            # would have sent. (FlareSolverr only returns cookies for the page
            # it ended on, i.e. the media host, so there is no clearance for
            # the original host to pass through with here.)
            src = _MEDIA_SOURCE_RE.search(raw_body)
            target = html.unescape(src.group(1)) if src else (solution.get("url") or "")
            if target and target != url and self._allow_media_redirect(url):
                flow.response = http.Response.make(
                    307, b"", {"Location": target, "Cache-Control": "no-store"})
            else:
                flow.response = http.Response.make(
                    502,
                    b"flaresolverr-addon: media request could not be resolved; "
                    b"refusing to return FlareSolverr's player page as the file",
                    {"Content-Type": "text/plain; charset=utf-8"},
                )
            return

        body_text, content_type = _unwrap_json_viewer(raw_body)
        status = int(solution.get("status") or 200)
        flow.response = http.Response.make(
            status,
            body_text.encode("utf-8", errors="replace"),
            {"Content-Type": content_type},
        )

    def request(self, flow: http.HTTPFlow):
        self._reload_config()
        host = (flow.request.host or "").lower()
        if not self._domain_matches(host):
            return

        user_agent = self._user_agents.get(host)
        cookies = self._cookies.get(host)
        if user_agent and cookies:
            # Already cleared for this host: let the request through to the
            # origin as-is, carrying the clearance (cookies + the user agent
            # they were issued to).
            flow.request.headers["User-Agent"] = user_agent
            for name, value in cookies.items():
                if name not in flow.request.cookies:
                    flow.request.cookies[name] = value
            flow.metadata["flaresolverr_passthrough"] = True
            return

        self._solve(flow)

    def responseheaders(self, flow: http.HTTPFlow):
        # Responses from the origin can be large downloads (the media file
        # itself, or a CDN it redirects to) -- stream them through instead of
        # letting mitmproxy buffer the whole body first, which would stall the
        # client until the entire file has arrived. The one exception is a
        # challenge response to a passed-through request: that stays buffered
        # so response() can replace it.
        if flow.metadata.get("flaresolverr_passthrough") and self._is_challenge(flow.response):
            return
        flow.response.stream = True

    def response(self, flow: http.HTTPFlow):
        if not flow.metadata.get("flaresolverr_passthrough"):
            return
        if not self._is_challenge(flow.response):
            return
        # The stored clearance no longer works: drop it and re-solve this
        # request through FlareSolverr.
        host = (flow.request.host or "").lower()
        self._cookies.pop(host, None)
        self._user_agents.pop(host, None)
        self._solve(flow)


addons = [FlareSolverrAddon()]
