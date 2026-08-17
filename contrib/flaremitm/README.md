# FlareMITM

A small HTTP(S) proxy that transparently routes requests for specific
Cloudflare-protected domains through FlareSolverr, while passing every
other request straight through untouched.

## Why this exists

FlareSolverr is not itself a usable proxy -- it exposes a request/response
API (`POST /v1` with `{"cmd": "request.get", "url": "..."}`), not a
CONNECT-tunnel-capable HTTP proxy a client can just point `--proxy` at. This
container bridges that gap using mitmproxy (a mature, well-tested
MITM-capable proxy engine) with a small custom addon
(`flaresolverr_addon.py`) that:

- Terminates TLS itself for configured domains (mitmproxy's self-signed CA;
  works without any client-side cert trust setup as long as the client
  doesn't validate certificates -- e.g. yt-dlp with `--no-check-certificates`,
  already the default in Tube-Q's config) and swaps the real network fetch
  for a FlareSolverr-resolved one.
- Leaves every other domain alone (mitmproxy's normal passthrough) --
  in particular this means the actual video/CDN download, which typically
  happens on a *different*, non-Cloudflare-protected domain, is not slowed
  down or routed through FlareSolverr at all.
- Carries cookies across requests to the same domain. This matters because
  some sites only send a challenge on the *first* request (the page fetch);
  a follow-up AJAX/API call the page's own JS would make needs the same
  challenge cookie to also get through. Confirmed necessary for sites whose
  extractor does a page-fetch, then a separate follow-up API call to resolve
  the actual video URLs -- both requests need to go through FlareSolverr
  with the same cookies for it to work end to end.
- Unwraps FlareSolverr's response when it's actually JSON: when a real
  browser navigates directly to a JSON endpoint, it gets wrapped in the
  browser's own "raw JSON viewer" HTML (`<html>...<body><pre>{...}</pre>...`).
  FlareSolverr returns that as-is. The addon detects and strips this wrapper
  so JSON API responses come back as real JSON instead of HTML-wrapped JSON
  that breaks a JSON parser expecting a bare payload.

## What it does NOT fix

FlareSolverr (and therefore this proxy) only solves the *ordinary*,
auto-resolving Cloudflare JS challenge/interstitial. Some sites run their
own *interactive* captcha gate on top of that (e.g. a button that only then
renders a captcha widget requiring an actual solve) -- FlareSolverr's
non-interactive mode can't click through that, so this proxy can't either.
It'll just return the captcha gate page as-is, with no useful cookie to
extract.

## Configuration

Two separate config channels, matching what's infrastructure-level
(container env vars, set once at deploy time) versus what changes often
(the domain list, driven by Tube-Q's Settings UI):

- `FLARESOLVERR_URL` env var (default `http://flaresolverr:8191/v1`) --
  where the addon reaches FlareSolverr's own API.
- `/config/flaresolverr_proxy.json` (path overridable via the
  `FLARESOLVERR_CONFIG_PATH` env var), polled every 10s so changes apply
  without restarting the container:
  ```json
  {
    "domains": ["example.com", "example.org"],
    "apply_to_all": false
  }
  ```
  - `domains`: exact hostname or parent-domain match (a request to
    `m.example.com` matches an `example.com` entry).
  - `apply_to_all`: if true, every request goes through FlareSolverr
    regardless of `domains` -- not recommended outside of testing, since it
    adds FlareSolverr's browser-fetch latency (~1-5s per request) to
    everything, including domains that don't need it.

  Tube-Q's Settings UI ("FlareSolverr" tab) writes this file so it stays in
  sync with what's configured there -- see the "FlareSolverr" section of the
  main Tube-Q README for how to point Tube-Q's own config at it.

## Using it

Point any HTTP(S) client's proxy setting at this container's published port
(e.g. `http://<host>:<port>`). No special target-URL scheme trick needed --
this is a real proxy with proper CONNECT/TLS handling, so normal `https://`
target URLs work as expected.

### Tube-Q

Handled automatically: when a normal yt-dlp attempt fails for a
FlareSolverr-configured domain, Tube-Q retries once with
`--proxy http://<flaremitm-host>:<port>` added to that domain's yt-dlp
config before falling through to the existing JDownloader2 handoff. See
`run_yt_dlp_for_item` in `tube-q.py`.

### JDownloader2

Not automatic -- the My.JDownloader REST API has no per-link/per-package
proxy override, so this is a one-time manual setup in JDownloader2 itself:

1. Open JDownloader2's own settings → **Settings → Connection Manager →
   Proxy**.
2. Add a new proxy rule:
   - **Type**: HTTP
   - **Host / Port**: this container's address
   - **Rule / hostmask**: scope it to the same domains configured above
     (e.g. `*.example.com`) rather than applying it globally, for the same
     latency reason `apply_to_all` isn't recommended.
3. Save. JDownloader2's crawler will now route matching hosts through
   FlareSolverr automatically from then on -- including whenever Tube-Q's
   existing "Send to JD2" fallback hands it one of these URLs.

## Deploying

`docker-compose.yml` in this directory is a minimal standalone example
(FlareSolverr + this proxy). Adjust image names/ports/volumes to fit
however your FlareSolverr instance is actually deployed -- the compose file
here assumes both containers are on the same Docker network and reachable
by service name.

Mount the shared config file read-only into this container at whatever path
`FLARESOLVERR_CONFIG_PATH` points to, and have Tube-Q (or whatever else you
use to drive it) write to that same host path.

## Troubleshooting

- `docker logs <container>` -- the addon prints a line whenever it fails to
  reload its config file, and mitmproxy itself logs each request/response.
- A `502` response body starting with `flaresolverr-addon:` means the addon
  reached FlareSolverr but got back an error or a bad response -- the rest
  of the message is FlareSolverr's own error text.
- If a configured domain is still failing, verify with a direct call first
  to rule out FlareSolverr itself before suspecting this proxy:
  `curl -X POST http://<flaresolverr-host>:8191/v1 -H "Content-Type: application/json" -d '{"cmd":"request.get","url":"https://the-url","maxTimeout":60000}'`
