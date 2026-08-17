# Tube-Q – yt-dlp Tube Download Queue

![Tube-Q Logo](logo.png)

**Tube-Q** is a self-hosted web app that lets you manage and monitor multiple video downloads using [**yt-dlp**](https://github.com/yt-dlp/yt-dlp).

You can queue downloads, check progress in real time, and even send video urls directly from your browser to it using the External Application Launcher extension.

The first versions were bash scripts, but this has now been converted into a python application.

---

## 🌟 Overview

Tube-Q runs a small web server (default port **7090**) that gives you a dashboard for managing downloads.
It works on Windows, macOS, and Linux — and can also run easily inside Docker.

Main features:

- Web UI for adding, managing, and monitoring downloads
- Supports all `yt-dlp`-compatible sources (YouTube, SoundCloud, etc.)
- Real-time progress updates (speed, ETA, percentage)
- Pause, resume, or retry failed downloads
- Auto-save queue and history
- Separate global and per-domain concurrency limits (defaults: global `10`, per-domain `2`)
- Supports domain-specific settings
- Check for new yt-dlp version and update function
- Easy browser integration (send current page or link to Tube-Q)
- No account or login needed — just open your browser to use it
- Optional JDownloader2 backup: offload failed downloads to a My.JDownloader-linked JDownloader2 instance

---

## ⚙️ Installation

### 1. Clone or download Tube-Q

```bash
git clone https://github.com/AnonTester/tube-q.git
cd tube-q
```

### 2. Install Python requirements

You need **Python 3.9+**.

It is strongly suggested to use a virtual environment for the requirements

On debian/ubuntu systems install the python-venv package for your python version. For python 3.12:

```bash
sudo apt install python3.12-venv
```

Then create the virtual environment and activate it:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the required python packages:
```bash
pip install -r requirements.txt
```

Also make sure the latest `yt-dlp` is installed:

```bash
pip install -U yt-dlp
```

To deactivate the virtual environment:
```bash
deactivate
```

You may also need to install ffmpeg for merging separate video and audio streams, convert containers and other post-processing tasks:

```bash
sudo apt install ffmpeg     # Linux
```


### 3. Start Tube-Q

enable virtual environment:
```bash
cd tube-q
source .venv/bin/activate
```
then start tube-q

```bash
python3 tube-q.py
```

Then open your browser and go to:

👉 **http://localhost:7090**

Test with a youtube video like this educational 133MB video from Ze Frank:
"True Facts: The Science of the Praying Mantis"
https://www.youtube.com/watch?v=zScP-6v2hxU

to exit/deactivate the virtual environment:
```bash
deactivate
```

---

## 🐳 Run with Docker (Optional)

If you prefer Docker, Tube-Q includes ready-to-use files.

Adjust the `docker-compose.yml` file for your purposes and configuration.
The docker-compose file is set for Intel/AMD GPU accelleration by default. Swap to NVIDIA if required.

Volumes:
 * /app/conf - stores configuration files and yt-dlp updated binary
 * /downloads - to save downloaded videos

`ffmpeg`/`ffprobe` are installed in the container image by default.
Avoid bind-mounting single host binaries into `/usr/bin` because shared-library versions can mismatch.

Optional binary override (advanced): mount a full compatible toolchain directory and point env vars to it.

```bash
docker-compose up -d
```

Tube-Q will be available at **http://localhost:7090**.

---

## 🧰 Configuration

You can use the Settings page in the web UI to change defaults or create the configuration files yourself.

Make sure to at least define the default yt-dlp output filename template including the target download folder (docker uses /download).

This example stores downloads in /download with a subfolder of the domain and the filename will be the title followed by the resolution height if available:
```commandline
-o /download/%(webpage_url_domain)s/%(title)s.%(height&{}p.|)s%(ext)s
```
Full description of the possible output template options can be found on the [yt-dlp github page](https://github.com/yt-dlp/yt-dlp#output-template)

Domain-specific yt-dlp arguments (different output paths, post-processing,
proxies, etc. per site) are configured entirely through **Settings → Domain
Options** in the web UI, stored in `domain_overrides` in `config.json`. This
is the single place for per-domain config -- there's no separate per-domain
`.conf` file convention to maintain outside of Settings.

Tube-Q stores its settings in:

```conf/config.json```

and yt-dlp default configuration in:

```conf/default.conf```

Example Tube-Q defaults:

```yaml
{
  "port": 7090,
  "concurrent_downloads_global": 10,
  "concurrent_downloads_per_domain": 2,
  "yt_dlp_path": "conf/yt-dlp",
  "yt_dlp_global_args": [],
  "start_paused": false,
  "new_urls_paused": false,
  "download_favicons": true,
  "domain_overrides": {},
}
```


## 🧰 JDownloader2 backup

Tube-Q can hand off downloads that yt-dlp fails on to a [JDownloader2](https://jdownloader.org/) instance linked to your [My.JDownloader](https://my.jdownloader.org/) account, via the official My.JDownloader API.

To set this up, open **Settings → JDownloader2**:

- **Enable JDownloader2 backup** – turns the integration on.
- **Automatically send failed downloads to JDownloader2** – when checked, any URL that yt-dlp fails to download is automatically sent to JDownloader2's link collector and the download is started there.
- **Preferred resolution** – when a link resolves to several resolutions (e.g. a page offering 480p/720p/1080p/2160p versions of the same video), only the version matching this preference is kept; the rest are discarded from JDownloader2's link collector before the download starts. Choices are `all` (no filtering, every resolution found is downloaded), `lowest`, `480p`, `720p`, `1080p`, `2160p`, and `highest` (default). For a fixed-resolution choice, if that exact resolution isn't available the closest one below it is used, falling back to the lowest available if all found resolutions are higher.
- **My.JDownloader Email / Password** – the credentials for your My.JDownloader account.
- **Test connection / List devices** – connects to the My.JDownloader API with the entered credentials and lists the JDownloader instances linked to your account.
- **Target JDownloader device** – select which linked JDownloader instance should receive the links.

Once enabled and configured, a **"Send all Errors to JDownloader2"** option appears in the queue's hamburger menu whenever there are entries with an error status, letting you offload all current errors to JDownloader2 on demand. Each individual failed item also gets its own **"Send to JD2"** button next to its Retry button for offloading that single download.

When links are sent to JDownloader2, its link collector is used to resolve the page first; any resulting files that aren't recognized video formats (e.g. `.html`, `.js`, `.gif`, `.jpg`) are discarded, and only the actual video files matching the preferred resolution are moved to the download list and started.

Unlike a plain hand-off, Tube-Q keeps tracking the item after sending it to
JDownloader2 rather than dropping it from the queue: it shows a distinct
"sent to JDownloader2" status with live progress (polled from the
JDownloader2 device), and once the download there finishes, Tube-Q
automatically runs the same post-processing that domain's yt-dlp config
would have applied to a normal download (output path/filename template,
recoding, any configured `--exec` post-processing steps) against the
downloaded file. If JDownloader2's download fails or the file can't be
located afterward, the item is marked as an error like any other failed
download instead of silently disappearing.

Your My.JDownloader email and password are stored in `conf/config.json` alongside the rest of the Tube-Q configuration.

---

## 🧰 FlareSolverr fallback (Cloudflare-protected domains)

Some sites front their pages with Cloudflare's bot-protection challenge,
which a plain HTTP client like yt-dlp (or JDownloader2's crawler) can't get
past on its own, even with a valid cookie jar and browser-like headers.
[FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) solves that
challenge using a real headless browser; Tube-Q can automatically retry a
failed download through a FlareSolverr-backed proxy before giving up (or
falling through to JDownloader2).

This only helps with the *ordinary*, auto-resolving Cloudflare challenge.
Sites that layer their own interactive captcha on top of that (one that
needs an actual click/solve, not just a JS challenge) aren't fixable this
way.

To set this up, open **Settings → FlareSolverr**:

- **Enable FlareSolverr retry** – turns the integration on.
- **Proxy address** – where the FlareSolverr-backed proxy is reachable
  (e.g. `http://192.168.1.10:8192`). See `contrib/flaremitm/` in this repo
  for a ready-to-build proxy that bridges FlareSolverr into something
  yt-dlp can actually point `--proxy` at (FlareSolverr itself only exposes
  a request/response API, not a proxy interface).
- **Domains to retry through FlareSolverr** – comma-separated list of
  domains that should get this treatment. Only domains you list here (or
  "apply to all", see below) trigger the retry, so this doesn't add latency
  to sites that don't need it.
- **Apply to all domains** – skips the domain list and routes every retry
  attempt through FlareSolverr. Not recommended outside of testing, since
  every FlareSolverr-backed request has real added latency compared to a
  normal fetch.

When a normal yt-dlp attempt fails for a domain configured here, Tube-Q
retries once through the proxy (keeping normal fast-path performance for
every other domain and for the first attempt on any domain). If that retry
also fails, the item falls through to the existing JDownloader2 handoff
(if enabled) exactly as it would for any other failure.

JDownloader2 doesn't automatically benefit from this -- see
`contrib/flaremitm/README.md` for how to point JDownloader2's own proxy
settings at the same proxy for the domains that need it.

---

## 🔗 Sending URLs to Tube-Q

Use the web UI or send directly from your browser.

### 🧩 External Application Launcher browser Add-on

The "External Application Launcher" addon extension allows you to define customizable toolbar button and context menu items that can execute external executables by passing command-line arguments of your choice.

**Settings for external application launcher**
This assumes curl is present on the system and Tube-Q runs locally on default port. Adjust as necessary.
```commandline
Display Name: Download with Tube-Q
Executable Name: curl
Arguments: 'http://localhost:7090/add' -X POST -H 'Content-Type: text/plain' --data-raw '[HREF]'
Enable Toolbar button with 'Page Context' and 'Link Context'
```

A configuration file 'external-application-button-preferences.json' that can be imported in the extension settings is available in the contrib directory.

It is possible to use keyboard shortcuts to run the external command instead of pressing the toolbar button:
On Chromium browsers:
  open "chrome://extensions/shortcuts" in a browser tab and assign a custom shortcut for this extension.
On Firefox:
  open "about:addons" in a browser tab, click on the gear icon, and select the "Manage Extension Shortcuts" button.


  * [Homepage](https://webextension.org/listing/external-application-button.html) /  [github](https://github.com/andy-portmen/external-application-button)
  * [Chrome Store](https://chrome.google.com/webstore/detail/external-application-butt/bifmfjgpgndemajpeeoiopbeilbaifdo)
  * [Firefox Add-ons](https://addons.mozilla.org/addon/external-application/)
  * [Opera Addons](https://addons.opera.com/en/extensions/details/external-application-button/)


### Command line example

Example command (on Linux/Mac):

```bash
curl -s -X POST http://localhost:7090/add \
     -H "Content-Type: text/plain" \
     -d "%URL%"
```
or using wget:
```bash
wget --method=POST --header="Content-Type: text/plain" \
     --body-data="%URL%" \
     http://localhost:7090/add
```


When on any video page, just right-click on a video link or on the background of a video page and "Download with Tube-Q" and it'll queue automatically.



---

## 🧠 Quick Summary

| Action | Command                                                                                                 |
|---------|---------------------------------------------------------------------------------------------------------|
| Start Tube-Q | `python3 tube-q.py`                                                                                     |
| Access web UI | [http://localhost:7090](http://localhost:7090)                                                          |
| Add URL | `curl -X POST http://localhost:7090/add -d '{"url":"<videopage>"}' -H "Content-Type: application/json"` |
| Run with Docker | `docker-compose up -d`                                                                                  |
| Send from browser | Use external application launcher add-on                                                                |

---

# 🧾 License

MIT License © 2025.
