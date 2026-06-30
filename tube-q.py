#!/usr/bin/env python3
"""
Tube-Q : yt-dlp Tube Download Queue
"""
APP_VERSION = "1.17.1"
APP_GITHUB_REPO = "https://github.com/AnonTester/tube-q"
APP_GITHUB_COMMITS_API = APP_GITHUB_REPO.replace("https://github.com/", "https://api.github.com/repos/") + "/commits?per_page=1"

import asyncio
import copy
import json
import hashlib
import hmac
import base64
import re
import time
import datetime
import signal
import subprocess
import sys
import os
import platform
import stat
import urllib.request
from pathlib import Path
from urllib.parse import urlparse, quote
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List, Set

import aiohttp
import yarl
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

# === Paths & default config ===
APP_ROOT = Path(__file__).parent.resolve()
CONF_DIR = APP_ROOT / "conf"
CONF_DIR.mkdir(exist_ok=True)
(CONF_DIR / "domains").mkdir(exist_ok=True)
FAVICON_DIR = CONF_DIR / "favicons"
FAVICON_DIR.mkdir(exist_ok=True)
LOGS_DIR = CONF_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
URL_ATTEMPTS_LOG_PATH = LOGS_DIR / "url_attempts.log"

YT_DLP_BINARY = "yt-dlp"

DEFAULT_CONFIG = {
    "port": 7090,
    "concurrent_downloads_global": 10,
    "concurrent_downloads_per_domain": 2,
    "yt_dlp_path": str(CONF_DIR / "yt-dlp"),  # alternative binary path (if present)
    "queue_file": str(CONF_DIR / "queue.json"),
    "history_file": str(CONF_DIR / "history.json"),
    "yt_dlp_config_folder": str(CONF_DIR),
    "yt_dlp_global_args": [],
    "start_paused": False,
    "new_urls_paused": False,
    "download_favicons": True,
    # domain_overrides is a mapping of "comma-separated-domains" -> "ytdlp args string"
    "domain_overrides": {},
    "last_version_check": 0,
    "yt_dlp_latest": None,
    "last_tubeq_version_check": 0,
    "tubeq_latest": None,
    "tubeq_latest_checked_app_version": None,
    # JDownloader2 (My.JDownloader) backup/offload settings
    "jdownloader": {
        "enabled": False,
        "email": "",
        "password": "",
        "device_id": "",
        "device_name": "",
        "auto_send_errors": False,
        "resolution_preference": "highest"
    }
}
CONFIG_PATH = CONF_DIR / "config.json"
if not CONFIG_PATH.exists():
    CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2))
CONFIG = json.loads(CONFIG_PATH.read_text())
CONFIG["jdownloader"] = {**DEFAULT_CONFIG["jdownloader"], **(CONFIG.get("jdownloader") or {})}


def _to_pos_int(value: Any, fallback: int) -> int:
    try:
        n = int(value)
    except Exception:
        n = int(fallback)
    return n if n >= 1 else int(fallback if int(fallback) >= 1 else 1)


# load some runtime vars from config
QUEUE_PATH = Path(CONFIG.get("queue_file"))
HISTORY_PATH = Path(CONFIG.get("history_file"))
# unified queue state file (single JSON map: id -> item)
YT_DLP_PATH = Path(CONFIG.get("yt_dlp_path", str(CONF_DIR / "yt-dlp")))
YT_DLP_GLOBAL_ARGS = CONFIG.get("yt_dlp_global_args", []) or []
CONCURRENT_DOWNLOADS_GLOBAL = _to_pos_int(
    CONFIG.get("concurrent_downloads_global", CONFIG.get("concurrent_downloads", 10)),
    10
)
CONCURRENT_DOWNLOADS_PER_DOMAIN = _to_pos_int(CONFIG.get("concurrent_downloads_per_domain", 2), 2)
START_PAUSED = bool(CONFIG.get("start_paused", False))
NEW_URLS_PAUSED = bool(CONFIG.get("new_urls_paused", False))
DOWNLOAD_FAVICONS = bool(CONFIG.get("download_favicons", True))
YTDLP_CONFIG_FOLDER = Path(CONFIG.get("yt_dlp_config_folder", str(CONF_DIR)))
DOMAIN_OVERRIDES = CONFIG.get("domain_overrides", {}) or {}

# ensure queue files exist
for p in [QUEUE_PATH, HISTORY_PATH]:
    if not p.exists():
        # queue.json will be a dict (id->item). history remains a list.
        if p == QUEUE_PATH:
            p.write_text(json.dumps({}, indent=2))
        else:
            p.write_text("[]")


# === Helpers & state ===
def make_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:12]


def get_domain(url: str) -> str:
    """
    Extracts the root (registered) domain from a URL.
    """

    multipart_tlds = {
        'co.uk', 'org.uk', 'gov.uk',
        'ac.uk', 'co.in', 'co.jp', 'com.au',
        'net.au', 'org.au', 'co.nz', 'com.sg',
        'co.za', 'com.br', 'com.mx', 'com.tr'
    }

    try:
        # Add a scheme if missing so urlparse works
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url

        parsed = urlparse(url)
        hostname = parsed.hostname or ''
        parts = hostname.split('.')

        if len(parts) < 2:
            return hostname  # e.g., localhost or invalid domain

        # Join last two parts as base (e.g., example.com)
        last_two = '.'.join(parts[-2:])

        # Check if last two parts form a known multipart TLD
        last_three = '.'.join(parts[-3:])
        if last_two in multipart_tlds:
            # e.g., google.co.uk (3 parts needed)
            return '.'.join(parts[-3:])
        elif last_three in multipart_tlds:
            # In rare case, TLD has 3 parts
            return '.'.join(parts[-4:])
        else:
            return last_two
    except Exception:
        return ""

def load_json_map(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}

# Unified in-memory state: id -> item dict
QUEUE_STATE: Dict[str, Any] = {}

try:
    QUEUE_STATE = json.loads(QUEUE_PATH.read_text())
    if not isinstance(QUEUE_STATE, dict):
        # older format (list) — convert into dict keyed by id if simple list detected.
        try:
            old_list = json.loads(QUEUE_PATH.read_text())
            QUEUE_STATE = {it['id']: it for it in old_list if isinstance(it, dict) and it.get('id')}
        except Exception:
            QUEUE_STATE = {}
except Exception:
    QUEUE_STATE = {}

try:
    HISTORY: List[str] = json.loads(HISTORY_PATH.read_text())
except Exception:
    HISTORY = []

DOWNLOADS: Dict[str, Dict[str, Any]] = {}

# detect stalled items from previous crash or restart
for item in QUEUE_STATE.values():
    status = item.get("status")
    if status in ("downloading", "postprocessing"):
        item["status"] = "stalled"
    elif status == "processing":
        # Legacy transient status: keep it queued.
        item["status"] = "queued"
    item.pop("processing", None)
#save_all_state()

pause_all_flag = START_PAUSED
subscribers: List[asyncio.Queue] = []
yt_dlp_version: Optional[str] = None

# parse post processing patterns
POSTPROC_INDICATORS = ["[Merger]", "[ExtractAudio]", "[Postprocessor]", "[ffmpeg]", "Merging formats", "[Exec]"]


# === Check if in docker ===
def is_docker():
    cgroup = Path('/proc/self/cgroup')
    return Path('/.dockerenv').is_file() or (cgroup.is_file() and 'docker' in cgroup.read_text())


# === Favicons ===
async def fetch_favicon(domain: str) -> str:
    if not domain:
        return "_generic.ico"
    fname = f"{domain}.ico"
    path = FAVICON_DIR / fname
    if path.exists():
        return fname
    try:
        url = f"https://{domain}/favicon.ico"
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    path.write_bytes(data)
                    return fname
    except Exception:
        pass
    return "_generic.ico"


async def ensure_favicon_for_url(url: str) -> str:
    if not DOWNLOAD_FAVICONS:
        return "_generic.ico"
    domain = get_domain(url)
    if not domain:
        return "_generic.ico"
    return await fetch_favicon(domain)


# === JDownloader2 (My.JDownloader) integration ===
JD_API_URL = "https://api.jdownloader.org"
JD_APP_KEY = "Tube-Q"

# extensions kept when sending links to JDownloader2; everything else
# (html, js, css, images, subtitles, etc. picked up by generic page crawling)
# is removed from the link grabber before starting the download.
JD_VIDEO_EXTENSIONS = {
    "mp4", "m4v", "mkv", "webm", "avi", "mov", "wmv", "flv", "f4v",
    "mpg", "mpeg", "mpe", "m2v", "m2ts", "mts", "ts", "3gp", "3g2",
    "ogv", "vob", "divx", "asf", "rm", "rmvb", "m3u8",
}

# resolution preference choices offered in the JDownloader2 settings tab; "all"
# disables filtering (legacy behavior: every resolution found gets downloaded)
JD_RESOLUTION_OPTIONS = ["all", "lowest", "480p", "720p", "1080p", "2160p", "highest"]
JD_DEFAULT_RESOLUTION_PREFERENCE = "highest"

# recognized resolution tokens that may appear in a crawled link's filename,
# mapped to a comparable pixel height
_JD_RES_HEIGHTS = {
    "2160p": 2160, "1440p": 1440, "1080p": 1080, "720p": 720, "480p": 480,
    "360p": 360, "240p": 240, "144p": 144,
    "4k": 2160, "uhd": 2160, "fhd": 1080, "hd": 720, "sd": 480,
}
_JD_RES_TOKEN_RE = re.compile(
    r'(?i)(?:^|[\s_\-.\(\[])(2160p|1440p|1080p|720p|480p|360p|240p|144p|4k|uhd|fhd|hd|sd)(?:$|[\s_\-.\)\]])'
)


def _jd_link_resolution(name: str) -> Optional[int]:
    """Pixel height parsed from a resolution token in a filename, or None if absent."""
    m = _JD_RES_TOKEN_RE.search(name or "")
    return _JD_RES_HEIGHTS.get(m.group(1).lower()) if m else None


def _jd_link_base_name(name: str) -> str:
    """Filename with extension and resolution token stripped, used to group same-video
    resolution variants that were crawled as separate link grabber entries."""
    stem = name or ""
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]
    stem = _JD_RES_TOKEN_RE.sub(" ", stem)
    return re.sub(r'[\s_\-.]+', ' ', stem).strip().lower()


def select_resolution_discards(links: List[Dict[str, Any]], pref: str) -> List[str]:
    """Given linkgrabber link entries that already passed the video-extension filter,
    return the uuids to discard so only the link matching the preferred resolution
    remains per group of same-video resolution variants (same package, same base
    filename). Links whose resolution can't be parsed, or that are the only entry
    in their group, are always kept since we have no reliable signal to filter on."""
    if pref == "all" or not links:
        return []

    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for link in links:
        key = (link.get("packageUUID"), _jd_link_base_name(link.get("name") or ""))
        groups.setdefault(key, []).append(link)

    discard_ids = []
    for group in groups.values():
        if len(group) < 2:
            continue
        heights = [(link, _jd_link_resolution(link.get("name") or "")) for link in group]
        if any(h is None for _, h in heights):
            # can't reliably tell these apart; leave the whole group untouched
            continue
        if pref == "lowest":
            target_height = min(h for _, h in heights)
        elif pref == "highest":
            target_height = max(h for _, h in heights)
        else:
            target = _JD_RES_HEIGHTS.get(pref)
            if target is None:
                continue
            le = [h for _, h in heights if h <= target]
            target_height = max(le) if le else min(h for _, h in heights)
        keep_uuid = min(
            (link for link, h in heights if h == target_height),
            key=lambda l: l.get("name") or ""
        )["uuid"]
        discard_ids.extend(link["uuid"] for link, _ in heights if link["uuid"] != keep_uuid)
    return discard_ids


class JDownloaderError(Exception):
    """Raised when the My.JDownloader API returns an error or cannot be reached."""


def _jd_secret(email: str, password: str, domain: str) -> bytes:
    h = hashlib.sha256()
    h.update(email.lower().encode("utf-8") + password.encode("utf-8") + domain.lower().encode("utf-8"))
    return h.digest()


def _jd_encrypt(token: bytes, data: str) -> str:
    iv, key = token[:len(token) // 2], token[len(token) // 2:]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return base64.b64encode(cipher.encrypt(pad(data.encode("utf-8"), AES.block_size))).decode("utf-8")


def _jd_decrypt(token: bytes, data: str) -> bytes:
    iv, key = token[:len(token) // 2], token[len(token) // 2:]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(base64.b64decode(data)), AES.block_size)


def _jd_error_message(text: str) -> str:
    try:
        err = json.loads(text)
        return str(err.get("type") or err.get("src") or text[:200])
    except Exception:
        return text[:200]


class JDownloaderClient:
    """Minimal async client for the My.JDownloader API (https://my.jdownloader.org)."""

    def __init__(self, email: str, password: str):
        self.email = (email or "").strip().lower()
        self.password = password or ""
        self._login_secret = _jd_secret(self.email, self.password, "server")
        self._device_secret = _jd_secret(self.email, self.password, "device")
        self._server_token: Optional[bytes] = None
        self._device_token: Optional[bytes] = None
        self._session_token: Optional[str] = None
        self._rid = int(time.time() * 1000)
        self._session: Optional[aiohttp.ClientSession] = None

    def _next_rid(self) -> int:
        rid = int(time.time() * 1000)
        if rid <= self._rid:
            rid = self._rid + 1
        self._rid = rid
        return rid

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session_token:
            try:
                await self._get("/my/disconnect", [("sessiontoken", self._session_token)])
            except Exception:
                pass
        await self._session.close()
        self._session = None

    async def _get(self, path: str, params: List[tuple]) -> Dict[str, Any]:
        rid = self._next_rid()
        parts = [path + "?"] + [f"{k}={quote(str(v), safe='')}" for k, v in params] + [f"rid={rid}"]
        query = parts[0] + "&".join(parts[1:])
        sign_key = self._server_token or self._login_secret
        signature = hmac.new(sign_key, query.encode("utf-8"), hashlib.sha256).hexdigest()
        query += f"&signature={signature}"
        async with self._session.get(yarl.URL(JD_API_URL + query, encoded=True)) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise JDownloaderError(_jd_error_message(text))
        decrypted = _jd_decrypt(sign_key, text)
        return json.loads(decrypted.decode("utf-8"))

    async def _device_action(self, device_id: str, path: str, params: Optional[List[Any]]):
        rid = self._next_rid()
        body = {
            "apiVer": 1,
            "url": path,
            "params": [json.dumps(p) for p in params] if params is not None else None,
            "rid": rid,
        }
        encrypted = _jd_encrypt(self._device_token, json.dumps(body))
        url = f"{JD_API_URL}/t_{self._session_token}_{device_id}{path}"
        headers = {"Content-Type": "application/aesjson-jd; charset=utf-8"}
        async with self._session.post(url, data=encrypted, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise JDownloaderError(_jd_error_message(text))
        decrypted = _jd_decrypt(self._device_token, text)
        return json.loads(decrypted.decode("utf-8")).get("data")

    async def connect(self):
        data = await self._get("/my/connect", [("email", self.email), ("appkey", JD_APP_KEY)])
        self._session_token = data["sessiontoken"]
        session_bytes = bytes.fromhex(self._session_token)
        self._server_token = hashlib.sha256(self._login_secret + session_bytes).digest()
        self._device_token = hashlib.sha256(self._device_secret + session_bytes).digest()

    async def list_devices(self) -> List[Dict[str, Any]]:
        data = await self._get("/my/listdevices", [("sessiontoken", self._session_token)])
        return data.get("list", [])

    async def send_links(self, device_id: str, urls: List[str], package_name: str = "Tube-Q",
                          resolution_pref: str = JD_DEFAULT_RESOLUTION_PREFERENCE):
        # snapshot the current link grabber contents so we can identify which
        # links result from this addLinks call, regardless of how JDownloader
        # groups/names the resulting package(s)
        before = await self._device_action(device_id, "/linkgrabberv2/queryLinks", [{
            "maxResults": -1, "startAt": 0,
        }]) or []
        before_ids = {link["uuid"] for link in before}

        params = {
            "autostart": False,
            "links": "\n".join(urls),
            "packageName": package_name,
            "extractPassword": None,
            "priority": "DEFAULT",
            "downloadPassword": None,
            "destinationFolder": None,
            "overwritePackagizerRules": False,
        }
        await self._device_action(device_id, "/linkgrabberv2/addLinks", [params])

        # wait for the link grabber to finish resolving/crawling the links
        for _ in range(20):
            await asyncio.sleep(0.5)
            try:
                collecting = await self._device_action(device_id, "/linkgrabberv2/isCollecting", None)
            except JDownloaderError:
                break
            if not collecting:
                break

        after = await self._device_action(device_id, "/linkgrabberv2/queryLinks", [{
            "maxResults": -1, "startAt": 0, "packageUUID": True,
        }]) or []
        new_links = [link for link in after if link["uuid"] not in before_ids]
        if not new_links:
            return

        # only keep recognized video files; discard everything else (html,
        # js, css, images, subtitles, etc. picked up by generic page crawling)
        keep_ids = []
        discard_ids = []
        for link in new_links:
            name = (link.get("name") or "").lower()
            ext = name.rsplit(".", 1)[-1] if "." in name else ""
            if ext in JD_VIDEO_EXTENSIONS:
                keep_ids.append(link["uuid"])
            else:
                discard_ids.append(link["uuid"])

        # when a video is offered at several resolutions, the generic crawler adds
        # one link per resolution into the same package; keep only the one matching
        # the configured preference instead of downloading every resolution
        if keep_ids:
            kept_links = [link for link in new_links if link["uuid"] in keep_ids]
            resolution_discards = select_resolution_discards(kept_links, resolution_pref)
            if resolution_discards:
                discard_ids.extend(resolution_discards)
                keep_ids = [uid for uid in keep_ids if uid not in set(resolution_discards)]

        if discard_ids:
            await self._device_action(device_id, "/linkgrabberv2/removeLinks", [discard_ids, []])

        if keep_ids:
            await self._device_action(device_id, "/linkgrabberv2/moveToDownloadlist", [keep_ids, []])
            await self._device_action(device_id, "/downloadcontroller/start", None)


async def send_queue_ids_to_jdownloader(ids: List[str]) -> Dict[str, Any]:
    """Send the given queue items' URLs to JDownloader2's link collector, start the
    download there, and remove them from the Tube-Q queue. Raises JDownloaderError
    if JDownloader2 isn't enabled/configured, or any error raised by JDownloaderClient."""
    jd_cfg = CONFIG.get('jdownloader') or {}
    if not jd_cfg.get('enabled'):
        raise JDownloaderError('JDownloader2 backup is not enabled')
    email = jd_cfg.get('email') or ''
    password = jd_cfg.get('password') or ''
    device_id = jd_cfg.get('device_id') or ''
    if not email or not password or not device_id:
        raise JDownloaderError('JDownloader2 is not fully configured')

    ids = [id_ for id_ in ids if QUEUE_STATE.get(id_, {}).get('url')]
    if not ids:
        return {'sent': 0, 'removed': []}
    urls = [QUEUE_STATE[id_]['url'] for id_ in ids]
    resolution_pref = jd_cfg.get('resolution_preference') or JD_DEFAULT_RESOLUTION_PREFERENCE
    if resolution_pref not in JD_RESOLUTION_OPTIONS:
        resolution_pref = JD_DEFAULT_RESOLUTION_PREFERENCE

    async with JDownloaderClient(email, password) as client:
        await client.connect()
        await client.send_links(device_id, urls, resolution_pref=resolution_pref)

    removed = []
    for id_ in ids:
        it = QUEUE_STATE.pop(id_, None)
        if it is None:
            continue
        append_url_attempt(it.get('url', ''), "sent_to_jdownloader2", uid=id_)
        lf = LOGS_DIR / f"{id_}.log"
        if lf.exists():
            try:
                lf.unlink()
            except Exception:
                pass
        removed.append(id_)
    await persist_and_publish()
    return {'sent': len(urls), 'removed': removed}


# === Persistence ===
def save_all_state():
    try:
        # write unified queue state map
        QUEUE_PATH.write_text(json.dumps(QUEUE_STATE, indent=2))
    except Exception:
        pass
    try:
        HISTORY_PATH.write_text(json.dumps(HISTORY, indent=2))
    except Exception:
        pass
    # also persist main config domain overrides and path if changed
    try:
        cfg = json.loads(CONFIG_PATH.read_text())
        cfg["domain_overrides"] = DOMAIN_OVERRIDES
        cfg["yt_dlp_global_args"] = YT_DLP_GLOBAL_ARGS
        cfg["yt_dlp_path"] = str(YT_DLP_PATH)
        CONFIG_PATH.write_text(json.dumps(cfg, indent=2))
    except Exception:
        pass


def append_url_attempt(url: str, outcome: str, source: str = "api", uid: Optional[str] = None):
    ts = datetime.datetime.now().isoformat(timespec="seconds")
    safe_url = (url or "").replace("\n", " ").strip()
    safe_outcome = (outcome or "").replace("\n", " ").strip()
    safe_source = (source or "").replace("\n", " ").strip()
    safe_uid = uid or "-"
    line = f"{ts}\t{safe_source}\t{safe_outcome}\t{safe_uid}\t{safe_url}\n"
    try:
        with URL_ATTEMPTS_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


async def persist_and_publish():
    save_all_state()
    await publish_state()

# choose domain config
def choose_config_for_url(url: str) -> Optional[str]:
    domain = get_domain(url)
    if not domain:
        return None
    # 1) domain_overrides in CONFIG -- supports keys like "youtube.com, youtu.be"
    for key, args in (DOMAIN_OVERRIDES or {}).items():
        domains = [d.strip() for d in key.split(',') if d.strip()]
        if domain in domains:
            # write args to a temp config file in config folder so yt-dlp can use it
            tmp = YTDLP_CONFIG_FOLDER / f"_override_{hashlib.sha1(key.encode()).hexdigest()[:8]}.conf"
            try:
                tmp.write_text(args)
                return str(tmp)
            except Exception:
                return None
    # 2) legacy per-domain conf files in conf/domains
    domain_conf = YTDLP_CONFIG_FOLDER / "domains" / f"{domain}.conf"
    default_conf = YTDLP_CONFIG_FOLDER / "default.conf"
    if domain_conf.exists():
        return str(domain_conf)
    if default_conf.exists():
        return str(default_conf)
    return None


# === yt-dlp version check (daily) ===
def get_local_ytdlp_version():
    global yt_dlp_version
    if yt_dlp_version is not None:
        return yt_dlp_version
    # Prefer local bundled binary if configured
    try:
        # try running the configured YT_DLP path or binary name
        bin_path = str(YT_DLP_PATH) if YT_DLP_PATH.exists() else YT_DLP_BINARY
        out = subprocess.check_output([bin_path, "--version"], stderr=subprocess.STDOUT, text=True).strip()
        return out
    except Exception:
        # fallback to module if installed
        try:
            import yt_dlp
            return getattr(yt_dlp.version, '__version__', 'unknown')
        except Exception:
            return "unknown"


def check_latest_ytdlp_version_once_daily():
    now = time.time()
    last = CONFIG.get("last_version_check", 0)
    if (now - last) < 86400 and CONFIG.get("yt_dlp_latest"):
        return CONFIG.get("yt_dlp_latest")
    try:
        with urllib.request.urlopen('https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest', timeout=8) as resp:
            data = json.loads(resp.read().decode())
            tag = data.get('tag_name', '')
            latest = tag.lstrip('v')
            CONFIG["yt_dlp_latest"] = latest
            CONFIG["last_version_check"] = int(now)
            CONFIG_PATH.write_text(json.dumps(CONFIG, indent=2))
            return latest
    except Exception:
        return CONFIG.get("yt_dlp_latest")


def _extract_tube_q_version_from_commit_message(message: Any) -> Optional[str]:
    msg = str(message or "").strip()
    if not msg:
        return None
    m = re.search(r"\bTube-Q\s+v(\d+(?:\.\d+){0,3})\b", msg, re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def check_latest_tubeq_version_once_daily() -> Optional[str]:
    now = time.time()
    last = CONFIG.get("last_tubeq_version_check", 0)
    # the cached "latest" value was computed while a (possibly older or newer)
    # build of Tube-Q was running; conf/config.json survives container
    # rebuilds, so a redeploy can leave a stale cached value in place for up
    # to 24h. Force a fresh check whenever the running APP_VERSION differs
    # from the version that was active when we last checked.
    same_build = CONFIG.get("tubeq_latest_checked_app_version") == APP_VERSION
    if same_build and (now - last) < 86400 and CONFIG.get("tubeq_latest"):
        return CONFIG.get("tubeq_latest")
    try:
        with urllib.request.urlopen(APP_GITHUB_COMMITS_API, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        latest = None
        if isinstance(data, list) and data:
            message = (((data[0] or {}).get("commit") or {}).get("message"))
            latest = _extract_tube_q_version_from_commit_message(message)
        if latest:
            CONFIG["tubeq_latest"] = latest
        CONFIG["last_tubeq_version_check"] = int(now)
        CONFIG["tubeq_latest_checked_app_version"] = APP_VERSION
        CONFIG_PATH.write_text(json.dumps(CONFIG, indent=2))
        return CONFIG.get("tubeq_latest")
    except Exception:
        return CONFIG.get("tubeq_latest")


def _parse_version_parts(version: Optional[str]) -> Optional[tuple]:
    if version is None:
        return None
    raw = str(version).strip()
    if not raw or raw.lower() == "unknown":
        return None
    nums = re.findall(r"\d+", raw)
    if not nums:
        return None
    try:
        return tuple(int(n) for n in nums)
    except Exception:
        return None


def is_update_available(local_version: Optional[str], latest_version: Optional[str]) -> bool:
    local_raw = (local_version or "").strip()
    latest_raw = (latest_version or "").strip()
    if not local_raw or local_raw.lower() == "unknown" or not latest_raw:
        return False
    if local_raw == latest_raw:
        return False

    local_parts = _parse_version_parts(local_raw)
    latest_parts = _parse_version_parts(latest_raw)
    if local_parts is not None and latest_parts is not None:
        length = max(len(local_parts), len(latest_parts))
        local_norm = local_parts + (0,) * (length - len(local_parts))
        latest_norm = latest_parts + (0,) * (length - len(latest_parts))
        return latest_norm > local_norm

    # Fallback for non-standard version strings.
    return latest_raw != local_raw


def is_strict_newer_version(local_version: Optional[str], latest_version: Optional[str]) -> bool:
    local_parts = _parse_version_parts(local_version)
    latest_parts = _parse_version_parts(latest_version)
    if local_parts is None or latest_parts is None:
        return False
    length = max(len(local_parts), len(latest_parts))
    local_norm = local_parts + (0,) * (length - len(local_parts))
    latest_norm = latest_parts + (0,) * (length - len(latest_parts))
    return latest_norm > local_norm


LOCAL_YTDLP_VERSION = get_local_ytdlp_version()
LATEST_YTDLP_VERSION = check_latest_ytdlp_version_once_daily()
UPDATE_AVAILABLE = is_update_available(LOCAL_YTDLP_VERSION, LATEST_YTDLP_VERSION)
LATEST_TUBEQ_VERSION = check_latest_tubeq_version_once_daily()
TUBEQ_UPDATE_AVAILABLE = is_strict_newer_version(APP_VERSION, LATEST_TUBEQ_VERSION)


def kill_process_tree(proc):
    """Kill a running yt-dlp subprocess together with any children it spawned
    (e.g. ffmpeg during merging/postprocessing, or an external downloader).
    proc is started with start_new_session=True so its pid is also its
    process group id; killing the group prevents the child from continuing
    the download/merge after the parent yt-dlp process has been killed."""
    if proc is None or proc.returncode is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# === yt-dlp runner with logging ===
async def run_yt_dlp_for_item(item: Dict[str, Any]):
    id_ = item["id"]
    url = item["url"]
    favicon = await ensure_favicon_for_url(url)
    item["favicon"] = favicon
    log_path = LOGS_DIR / f"{id_}.log"

    # mark item as actively downloading in unified state
    item["status"] = "downloading"
    progress = {"percent": 0.0, "eta": None, "speed": None, "status": "downloading", "detail": None}
    # Seed progress immediately so clients can render a bar right away.
    item["progress"] = progress
    # store process placeholder; will set actual process handle after creation
    DOWNLOADS[id_] = {"item": item, "progress": progress, "process": None}
    QUEUE_STATE[id_] = item
    await persist_and_publish()

    enforced_wait_logged = False
    last_progress_emit_ts = 0.0
    last_progress_emit_status = ""
    last_progress_emit_pct = 0.0
    last_progress_emit_detail = ""

    async def maybe_publish_progress(force: bool = False):
        nonlocal last_progress_emit_ts, last_progress_emit_status, last_progress_emit_pct, last_progress_emit_detail
        # Always keep in-memory state current so full snapshots remain accurate.
        try:
            DOWNLOADS[id_]["progress"] = progress
        except Exception:
            DOWNLOADS[id_] = {"item": item, "progress": progress, "process": None}
        QUEUE_STATE.get(id_, {}).update({"progress": progress})

        now = time.monotonic()
        status_now = str(progress.get("status") or "")
        pct_now = float(progress.get("percent") or 0.0)
        detail_now = str(progress.get("detail") or "")

        should_emit = force
        if not should_emit and status_now != last_progress_emit_status:
            should_emit = True
        if not should_emit and abs(pct_now - last_progress_emit_pct) >= 1.0:
            should_emit = True
        if not should_emit and (now - last_progress_emit_ts) >= 0.75:
            should_emit = True
        if not should_emit and status_now == "postprocessing" and detail_now != last_progress_emit_detail and (
                now - last_progress_emit_ts) >= 1.5:
            should_emit = True

        if not should_emit:
            return

        last_progress_emit_ts = now
        last_progress_emit_status = status_now
        last_progress_emit_pct = pct_now
        last_progress_emit_detail = detail_now
        await publish_item_update(id_, progress=progress)

    with log_path.open("ab") as lf:

        lf.write(("Log start: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n").encode('UTF-8'))
        lf.write(("URL: " + url + "\n").encode('UTF-8'))

        cfg = choose_config_for_url(url)
        # prefer configured local binary path if it exists, otherwise binary name
        bin_exec = str(YT_DLP_PATH) if YT_DLP_PATH.exists() else YT_DLP_BINARY
        ytdlp_args = [bin_exec, "--newline"]
        if YT_DLP_GLOBAL_ARGS:
            ytdlp_args += YT_DLP_GLOBAL_ARGS
        if cfg:
            ytdlp_args += ["--config-location", cfg, url]
        elif is_docker():
            # default output path: domain-based dir under /downloads for docker; user can change via domain conf
            domain = get_domain(url) or "misc"
            out_template = f"/downloads/{domain}/%(title)s.%(ext)s"
            ytdlp_args += ["-o", out_template, url]
        else:
            # default output template without path
            domain = get_domain(url) or "misc"
            out_template = f"{domain}/%(title)s.%(ext)s"
            ytdlp_args += ["-o", out_template, url]

        lf.write(f"ARGS: {ytdlp_args}\n".encode('UTF-8'))
        if cfg:
            try:
                cfgcontent = Path(cfg).read_text(encoding='UTF-8', errors=None)
            except Exception as e:
                cfgcontent = f"[error reading config: {e}]"
            lf.write(f"cfg:\n{cfgcontent}\n".encode('UTF-8'))
        else:
            lf.write(b"cfg: [no config used]\n")
        lf.write("--------\n".encode('UTF-8'))

        # ensure yt-dlp output is flushed line-by-line
        env = dict(**os.environ)
        env["PYTHONUNBUFFERED"] = "1"

        proc = await asyncio.create_subprocess_exec(
            *ytdlp_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 32,  # smaller buffer to reduce latency
            env=env,
            # own process group so kill_process_tree() can also reach child
            # processes yt-dlp spawns (ffmpeg merging/postprocessing, external
            # downloaders, ...) on cancellation, not just the yt-dlp process itself
            start_new_session=True
        )

        # store running process so it can be cancelled externally
        try:
            DOWNLOADS[id_]["process"] = proc
        except Exception:
            DOWNLOADS[id_] = {"item": item, "progress": progress, "process": proc}

        try:
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    break
                await asyncio.sleep(0)  # yield to event-loop for smoother updates
                try:
                    lf.write(raw)
                    lf.flush()
                except Exception:
                    pass
                line = raw.decode(errors="ignore").strip()
                # detect enforced waiting from yt-dlp
                if ("[download]" in line and "seconds" in line and "sleeping" in line.lower()) or re.search(
                        r"Waiting\s+\d+\s+seconds", line, re.I):
                    progress["status"] = "waiting"
                    progress["detail"] = "enforced waiting on external site"
                    if not enforced_wait_logged:
                        lf.write(b"[info] Enforced waiting detected\n")
                        enforced_wait_logged = True
                    await maybe_publish_progress()
                    continue

                # parse download progress lines
                if line.startswith("[download]"):
                    # extract the percentage from "X% of ..."
                    m_pct = re.search(r"\[download\]\s+(\d{1,3}(?:\.\d+)?)%\s+of", line)
                    if m_pct:
                        try:
                            pct = float(m_pct.group(1))
                        except ValueError:
                            pct = progress.get("percent", 0.0)
                        progress["percent"] = round(pct, 2)

                    # extract speed and ETA if available, tolerate missing ETA
                    m_sp = re.search(r"at\s+([0-9.]+[KkMGT]?i?B/s|[0-9.]+[KkMGT]?B/s)", line)
                    progress["speed"] = m_sp.group(1) if m_sp else None

                    m_eta = re.search(r"ETA\s+([0-9:]+|--:--:--)", line)
                    progress["eta"] = m_eta.group(1) if m_eta else None

                    progress["status"] = "downloading"
                    progress["detail"] = line
                    await maybe_publish_progress()
                    continue

                # detect postprocessing phase
                if any(ind in line for ind in POSTPROC_INDICATORS):
                    progress["status"] = "postprocessing"
                    progress["detail"] = line
                    await maybe_publish_progress()
                    continue

        except Exception as e:
            progress["status"] = "error"
            progress["detail"] = f"progress parser error: {e}"
            await maybe_publish_progress(force=True)

        rc = await proc.wait()
        # cleanup running record
        DOWNLOADS.pop(id_, None)

        # read last line(s) of log to provide last output line
        last_line = None
        try:
            txt = log_path.read_text(errors='ignore').strip().splitlines()
            if txt:
                last_line = txt[-1]
        except Exception:
            last_line = None

        # treat rc==0 as success. Also treat "already been downloaded" message as success
        log_lower = ""
        try:
            log_lower = (log_path.read_text(errors='ignore') or "").lower()
        except Exception:
            log_lower = ""

        if rc == 0 or ("already downloaded" in (last_line or "").lower()) or ("already been downloaded" in log_lower):
            item["status"] = "completed"
            item["progress"]["status"] = ""
            item["progress"]["detail"] = ""
            item["completed_at"] = int(time.time())
            if url not in HISTORY:
                HISTORY.append(url)
        else:
            item["status"] = "error"
            item["error"] = f"yt-dlp exit code {rc}"
            item["last_output"] = last_line

        # ensure unified state contains final item
        QUEUE_STATE[id_] = item
        await persist_and_publish()
        lf.write(("Log end: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n").encode('UTF-8'))
        lf.write("--------\n".encode('UTF-8'))

    if item.get("status") == "error":
        jd_cfg = CONFIG.get("jdownloader") or {}
        if jd_cfg.get("enabled") and jd_cfg.get("auto_send_errors"):
            try:
                await send_queue_ids_to_jdownloader([id_])
            except Exception as e:
                print(f"[jdownloader] auto-send for {id_} failed: {e}")


# === queue processor ===
async def queue_processor():
    active_global = 0
    active_by_domain: Dict[str, int] = {}
    scheduled_ids: Set[str] = set()

    def domain_key_for_item(item: Dict[str, Any]) -> str:
        domain = (item or {}).get("domain") or get_domain((item or {}).get("url", "")) or "_unknown"
        return str(domain).lower()

    while True:
        if pause_all_flag:
            await asyncio.sleep(0.5)
            continue

        if active_global >= CONCURRENT_DOWNLOADS_GLOBAL:
            await asyncio.sleep(0.2)
            continue

        # pick next queued item (not paused), constrained by per-domain concurrency
        next_item = None
        next_domain = None
        for it in list(QUEUE_STATE.values()):
            item_id = it.get("id")
            if not item_id or item_id in scheduled_ids:
                continue
            if it.get("status") in (None, "queued") and not it.get("paused", False):
                domain = domain_key_for_item(it)
                if active_by_domain.get(domain, 0) >= CONCURRENT_DOWNLOADS_PER_DOMAIN:
                    continue
                next_item = it
                next_domain = domain
                break

        if not next_item:
            await asyncio.sleep(0.4)
            continue

        item_id = next_item.get("id")
        if not item_id or not next_domain:
            await asyncio.sleep(0.1)
            continue

        scheduled_ids.add(item_id)
        active_global += 1
        active_by_domain[next_domain] = active_by_domain.get(next_domain, 0) + 1

        async def run_and_release(item: Dict[str, Any], domain: str):
            nonlocal active_global
            try:
                current = QUEUE_STATE.get(item.get("id"))
                if current and current.get("status") in (None, "queued") and not current.get("paused", False) and not pause_all_flag:
                    await run_yt_dlp_for_item(current)
                # do not remove from QUEUE_STATE — state remains for history / UI; entry kept with final status
            finally:
                if item.get("id"):
                    scheduled_ids.discard(item.get("id"))
                active_global = max(0, active_global - 1)
                left = active_by_domain.get(domain, 0) - 1
                if left <= 0:
                    active_by_domain.pop(domain, None)
                else:
                    active_by_domain[domain] = left

        asyncio.create_task(run_and_release(next_item, next_domain))
        await asyncio.sleep(0.05)


# === version retrieval (used in SSE payload) ===
def get_yt_dlp_version() -> str:
    try:
        bin_exec = str(YT_DLP_PATH) if YT_DLP_PATH.exists() else YT_DLP_BINARY
        out = subprocess.check_output([bin_exec, "--version"], stderr=subprocess.STDOUT, text=True).strip()
        return out
    except Exception:
        return "unknown"


# === SSE publishing ===
async def _publish_json_payload(payload: Dict[str, Any]):
    data = json.dumps(payload)
    for q in list(subscribers):
        try:
            await q.put(data)
        except Exception:
            pass


async def publish_state():
    try:
        queue_list = list(QUEUE_STATE.values())
    except Exception:
        queue_list = []

    payload = {
        "queue": queue_list,
        "pause_all": pause_all_flag
    }
    await _publish_json_payload(payload)


async def publish_item_update(id_: str, progress: Optional[Dict[str, Any]] = None):
    payload: Dict[str, Any] = {"type": "item_update", "id": id_}
    if progress is not None:
        payload["progress"] = progress
    await _publish_json_payload(payload)


async def periodic_state_publisher():
    while True:
        await publish_state()
        await asyncio.sleep(5)


# === FastAPI app & routes ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(queue_processor())
    asyncio.create_task(periodic_state_publisher())
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/favicons", StaticFiles(directory=str(FAVICON_DIR)), name="favicons")

# templates/index.html (single-file SPA)
TEMPLATES_DIR = APP_ROOT / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
INDEX_HTML_PATH = TEMPLATES_DIR / "index.html"

INDEX_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tube-Q</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link rel="icon" type="image/ico" href="favicon.ico">
  <link rel="apple-touch-icon" href="apple-touch-icon.png" />
  <meta name="mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <style>
    :root{--bg:#f7f7f8;--fg:#111;--card:#fff;--muted:#666;--accent:#3b82f6}
    @media (prefers-color-scheme: dark){:root{--bg:#0b0b0d;--fg:#e6e6e6;--card:#212529;--muted:#999;--accent:#3b82f6}}
    body{margin:0;font-family:system-ui;background:var(--bg);color:var(--fg);height:100vh;display:flex;flex-direction:column}
    .container {width: 100%;max-width: 1200px;margin-left: auto;margin-right: auto;flex: 1;display: flex;flex-direction: column;padding: 18px;box-sizing: border-box;}
    .header{display:flex;align-items:center;margin-bottom:8px}
    .header h1{margin:0;font-size:1.25rem}
    .card{background:var(--card);border-radius:10px;padding:12px;margin-bottom:12px;box-shadow:0 6px 18px rgba(0,0,0,0.06)}
    textarea,input[type="text"],input[type="password"],select { width: calc(100% - 16px); max-height:60px; padding:8px; border-radius:6px; border:1px solid #ddd; box-sizing:border-box; white-space: pre; overflow-wrap: normal; overflow: auto}
    .row{display:flex;gap:8px;align-items:center}
    .tabs{display:flex;gap:8px;margin-bottom:8px;flex-wrap:wrap;min-height:38px;align-items:center}
    .tab{padding:6px 10px;border-radius:8px;background:transparent;border:1px solid rgba(0,0,0,0.06);cursor:pointer}
    .tab.active, .tab.active:hover {background:var(--accent);color:white}
    .tab:hover { background: rgba(0,0,0,0.25); }
    .pill{background:#111;color:#fff;border-radius:999px;padding:2px 8px;font-size:0.8rem;margin-left:6px}
    .box{flex:1;overflow:auto;padding:8px;background:var(--card);border-radius:8px;min-height:160px}
    .list{list-style:none;margin:0;padding:0}
    .item{padding:8px;border-bottom:1px solid rgba(255,255,255,0.22);display:flex;align-items:center;gap:10px}
    .url-text { font-size: 0.95rem; word-break: break-all; color: var(--fg); flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    img.favicon{width:18px;height:18px;object-fit:contain;border-radius:3px}
    .small{font-size:0.85rem;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
    .smallnote{font-size:0.85rem;color:var(--muted);}
    .progress{display:block;height:8px;background:rgba(127,127,127,0.28);border-radius:6px;overflow:hidden;width:100%;margin:0;}
    .progress > div {height: 100%;background: var(--accent);transition: width 0.2s linear;}
    .fill{height:100%;background:var(--accent);width:0%}
    .spinner{width:12px;height:12px;border-radius:50%;border:2px solid rgba(0,0,0,0.1);border-top-color:var(--accent);animation:spin 1s linear infinite;display:inline-block;margin-right:6px}
    @keyframes spin{to{transform:rotate(360deg)}}
    button{padding:6px 10px;border-radius:6px;border:0;background:var(--accent);color:white;cursor:pointer}
    button.ghost{background:transparent;border:1px solid rgba(0,0,0,0.06);color:var(--muted)}
    .mobile-icon-btn{font-weight:700;line-height:1}
    .mobile-icon-btn.icon-retry{color:#16a34a}
    .mobile-icon-btn.icon-jd2{color:#2563eb}
    .mobile-icon-btn.icon-log{color:var(--fg)}
    .mobile-icon-btn.icon-remove{color:#dc2626}
    .muted{color:var(--muted)}
    .status-icon{width:12px;height:12px;border-radius:3px;display:inline-block}
    .status-paused{background:orange}
    .status-downloading{background:#3b82f6;}
    .status-complete{background:green}
    .status-error{background:red}
    .status-duplicate{background:gray}
    .status-stalled{background:purple}
    .ghost.active{background:var(--accent);color:white;border-color:transparent}
    #settingsModal {height: 90dvh; max-height: 90vh;}
    #domain_default_conf {max-height:180px;}
    /* Toasts */
    #toasts{position:fixed;right:18px;bottom:18px;z-index:9999;display:flex;flex-direction:column;gap:8px}
    .toast{background:#222;color:#fff;padding:8px 12px;border-radius:8px;opacity:0.95}
    @media (prefers-color-scheme: light){ .toast{background:#eee;color:#000} }

    /* Styled toast variants */
    .toast.success { background: #ccffcc; border: 1px solid #009900; color: #004400; }
    .toast.error { background: #ffcccc; border: 1px solid #990000; color: #440000; }
    .toast.warning { background: #ffe5b3; border: 1px solid #cc7a00; color: #663c00; }

    /* Modal (settings + update confirm) */
    .overlay{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,0.4);z-index:10000;overscroll-behavior: contain;}
    .modal{width:90%;max-height: 90dvh;background:var(--card);border-radius:8px;overflow:auto;position:relative;padding:12px;box-sizing:border-box;border: 1px solid;overscroll-behavior: contain;}
    .modal .modal-header{display:flex;justify-content:space-between;align-items:center}
    .modal .modal-content{margin-top:12px;min-height:60px;max-height:calc(80vh - 120px);overflow:auto;overscroll-behavior: contain;}
    .modal .btn-row{display:flex;gap:8px;justify-content:flex-end;margin-top:6px}
    .modal button{min-width:40px}

    /*
    .modal {
      height: 80dvh;
      max-height: 80vh; /* fallback for older browsers */
    }
    */
    .settings-modal {
      height: 80dvh;
      max-height: 80vh; /* fallback for older browsers */
    }

    /* footer fixed at bottom */
    footer{padding:8px 12px;text-align:center;font-size:0.9rem;color:var(--muted);border-top:1px solid rgba(0,0,0,0.06);position:fixed;left:0;right:0;bottom:0;background:var(--bg)}
    .bulk-bar{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap}
    .top-right-controls{margin-left:auto;display:flex;gap:8px}
    .hidden{display:none !important}
    /* queue action buttons on the right side of tabs */
    .queue-actions { margin-left: auto; display: flex; align-items: center; gap: 8px; }
    .queue-toggle-btn { background: var(--accent); color: #fff; border: none; border-radius: 6px; padding: 6px 10px; font-size: 1.05rem; line-height: 1; min-width: 38px; }
    .queue-toggle-btn.state-paused { background: #16a34a; }
    .queue-toggle-btn:disabled { opacity: 0.7; cursor: wait; }
    /* hamburger menu dropdown for queue actions */
    .hamburger-container { position: relative; }
    .hamburger-btn { background: var(--accent); color: white; border: none; border-radius: 6px; padding: 6px 10px; font-size: 1.2rem; cursor: pointer; }
    .hamburger-menu { position: absolute; right: 0; top: 100%; background: var(--card); box-shadow: 0 4px 12px rgba(0,0,0,0.2); border-radius: 8px; border: 1px solid; display: none; flex-direction: column; min-width: 180px; z-index: 999; }
    .hamburger-menu button { background: none; border: none; text-align: left; padding: 8px 12px; width: 100%; color: var(--fg); cursor: pointer; }
    .hamburger-menu button:hover { background: rgba(0,0,0,0.25); }

    /* Default (desktop) layout fix: keep action buttons on right side */
    .item {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    /* Desktop layout: left-group | status text | url | right buttons */
    .item .top-row,
    .item .bottom-row { display: contents; }

    .item .left-group {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
      min-width: 100px;
    }

    .item .status-col {
      flex-shrink: 0;
      width: 120px; /* fixed width for all rows */
      text-align: left;
      color: var(--muted);
      font-size: 0.9rem;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .item .url-text {
      flex: 1;
      text-align: left;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .item .meta-col {
      display: flex;
      flex-direction: column;
      gap: 4px;
      min-width: 0;
      flex: 1;
    }

    .item .right-group {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 6px;
      margin-left: auto;
      flex-shrink: 0;
      text-align: right;
    }

    /* Mobile layout override */
    @media (max-width:600px){
      /* Allow URLs to wrap up to 2 lines */
      .url-text{
        white-space: normal;
        overflow-wrap: break-word;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
      }

      /* Compact favicon */
      img.favicon{
        width: 16px;
        height: 16px;
        vertical-align:bottom;
        padding-bottom:2px;
      }
      /* .tab > span, .tab span { display: none; } */
      .tabs .tab { font-size: 0.8rem; padding: 4px 6px; }
      .actions button { font-size: 0.8rem; padding: 4px 6px; }

      /* Split into stacked layout */
      .item {
        display: flex;
        flex-direction: column;
        align-items: stretch;
      }

      .item .top-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        width: 100%;
        flex-wrap: nowrap;
      }

      .item .left-group {
        display: flex;
        align-items: center;
        gap: 6px;
      }

      .item .right-group {
        display: flex;
        align-items: center;
        gap: 4px;
        justify-content: flex-end;
      }

      .item .bottom-row {
        display: flex;
        flex-direction: column;
        gap: 4px;
        width: 100%;
        margin-top: 4px;
        text-align: left; /* keep URL/status left-aligned */
        align-items: stretch;
      }

      .item .meta-col {
        width: 100%;
      }

      .item button {
        padding: 3px 5px;
        font-size: 0.74rem;
      }

      .item button.mobile-icon-btn {
        background: transparent;
        border: 1px solid rgba(127,127,127,0.45);
        color: var(--fg);
        min-width: 24px;
        padding: 2px 4px;
        font-size: 0.95rem;
      }

      .item .progress {
        width: 100%;
        min-width: 0;
        margin: 0;
      }

      .url-text {
        white-space: normal;
        overflow-wrap: break-word;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        text-align: left;
      }
    }
  </style>
</head>
<body>
<div class="container">
    <div class="header">
        <img src="/logo.png" alt="Tube-Q" border=0 style="height:3.2rem; max-height:12vw; object-fit:contain;">
        <div class="top-right-controls">
            <button id="openSettings" class="ghost">&#9881;&#65039; Settings</button>
        </div>
    </div>

    <div class="card">
        <form id="addForm">
            <label for="urls">Add URLs (one per line)</label>
            <textarea id="urls" placeholder="https://..."></textarea>
            <div style="margin-top:8px" class="row">
                <button type="submit">Add</button>
                <label style="margin-left:8px"><input type="checkbox" id="addPaused"> Add paused</label>
            </div>
        </form>
    </div>

    <div class="card" style="display:flex;flex-direction:column;flex:1;min-height:200px;">
        <div class="tabs" id="tabs">
            <div class="tab active hidden" data-tab="all">All <span class="pill" id="cnt-all">0</span></div>
            <div class="tab hidden" data-tab="queued">Queued <span class="pill" id="cnt-queued">0</span></div>
            <div class="tab hidden" data-tab="downloading">Downloading <span class="pill" id="cnt-downloading">0</span>
            </div>
            <div class="tab hidden" data-tab="completed">Completed <span class="pill" id="cnt-completed">0</span></div>
            <div class="tab hidden" data-tab="errors">Errors <span class="pill" id="cnt-errors">0</span></div>
            <div class="tab hidden" data-tab="duplicates">Duplicates <span class="pill" id="cnt-duplicates">0</span>
            </div>
            <div class="queue-actions">
                <button id="pauseAllToggle" class="queue-toggle-btn" title="Pause queue" aria-label="Pause queue">&#9208;</button>
                <div class="hamburger-container hidden" id="hamburgerContainer">
                    <button class="hamburger-btn" id="hamburgerBtn">&equiv;</button>
                    <div class="hamburger-menu" id="hamburgerMenu">
                        <button id="removeCompleted">Remove Completed</button>
                        <button id="copyErrors">Copy all Errors</button>
                        <button id="copyDupes">Copy all Duplicates</button>
                        <button id="removeAllErrors">Remove all Errors</button>
                        <button id="removeAllDupes">Remove all Duplicates</button>
                        <button id="retryAllErrors">Retry all Errors</button>
                        <button id="retryAllDupes">Re-download all Duplicates</button>
                        <button id="sendErrorsToJD">Send all Errors to JDownloader2</button>
                    </div>
                </div>
            </div>
        </div>

        <div class="bulk-bar" id="bulkBar">
            <div id="selectControls" class="hidden">
                <button id="selAll" class="ghost">Select all</button>
                <button id="selInvert" class="ghost">Invert selection</button>
                <button id="selNone" class="ghost">Select none</button>
            </div>
            <div>
                <button id="bulkRemove" class="ghost hidden">Remove selected</button>
            </div>
            <div>
                <button id="bulkRetry" class="ghost hidden">Retry selected</button>
            </div>
            <div>
                <button id="bulkPause" class="ghost hidden">Pause selected</button>
            </div>
            <div>
                <button id="bulkResume" class="ghost hidden">Resume selected</button>
            </div>
        </div>

        <div class="box" id="mainBox">
            <ul id="items" class="list"></ul>
        </div>
    </div>

    <footer id="footer">
        <span id="footer-app"><a id="appLink" href="https://github.com/AnonTester/tube-q" target="_blank"
                                  style="color:var(--accent);text-decoration:none;">Tube-Q</a> v<span id="appVer">loading...</span></span>
        <span id="appUpdateArea"></span>
        &nbsp;|&nbsp;
        <a id="ytdlpLink" href="https://github.com/yt-dlp/yt-dlp" target="_blank"
           style="color:var(--accent);text-decoration:none;">yt-dlp</a>
        <span id="ytdlpVer">loading...</span>
        <span id="updateArea"></span>
    </footer>
</div>

<!-- Toast container -->
<div id="toasts"></div>

<!-- Log Modal -->
<div id="modalOverlay" class="overlay">
    <div class="modal" id="modal">
        <div class="modal-header">
            <h3 id="modalTitle">Log</h3>
            <button id="modalClose">&#10005;</button>
        </div>
        <div class="modal-content">
            <pre class="log" id="modalContent">Loading...</pre>
        </div>
    </div>
</div>

<!-- Settings Modal -->
<div id="settingsOverlay" class="overlay">
    <div class="modal" id="settingsModal">
        <div class="modal-header">
            <h3>Settings</h3>
            <button id="settingsClose">&#10005;</button>
        </div>
        <div class="modal-content" id="settingsContentWrapper">
            <div style="display:flex;gap:8px;margin-bottom:8px;">
                <button id="settingsTabGeneral" class="ghost">General</button>
                <button id="settingsTabDomains" class="ghost">Domain Options</button>
                <button id="settingsTabJDownloader" class="ghost">JDownloader2</button>
            </div>
            <div id="settingsContent"></div>
            <div class="btn-row">
                <button id="saveSettings">Save</button>
            </div>
        </div>
    </div>
</div>

<!-- Update Confirmation Modal -->
<div id="updateOverlay" class="overlay">
    <div class="modal" id="updateModal">
        <div class="modal-header">
            <h3>Updated yt-dlp version available</h3>
            <button id="updateClose">&#10005;</button>
        </div>
        <div class="modal-content">
            <p>New version: <span id="latestVerText"></span></p>
            <p>Current version: <span id="currentVerText"></span></p>
            <p>Should the latest yt-dlp binary be downloaded and installed?</p>
            <div class="btn-row">
                <button class="ghost btn-cancel" id="updateCancel">Cancel</button>
                <button id="updateConfirm">Update</button>
            </div>
        </div>
    </div>
</div>

<script>
    let sse;
    let reconnectTimer = null;
    let sseSuspended = false;
    let currentTab = 'all';
    let stateCache = { queue: [] };
    let pauseAllCache = false;
    let pauseAllPending = false;
    let jdownloaderConfigured = false;

    function setPauseAllButton(paused) {
        pauseAllCache = !!paused;
        const btn = document.getElementById('pauseAllToggle');
        if (!btn) return;
        if (pauseAllCache) {
            btn.innerHTML = '&#9658;';
            btn.title = 'Resume queue';
            btn.setAttribute('aria-label', 'Resume queue');
            btn.classList.add('state-paused');
        } else {
            btn.innerHTML = '&#9208;';
            btn.title = 'Pause queue';
            btn.setAttribute('aria-label', 'Pause queue');
            btn.classList.remove('state-paused');
        }
    }

    async function togglePauseAll() {
        if (pauseAllPending) return;
        const btn = document.getElementById('pauseAllToggle');
        const previous = pauseAllCache;
        const target = !previous;
        pauseAllPending = true;
        if (btn) btn.disabled = true;
        setPauseAllButton(target);
        try {
            const r = await fetch('/pause_all', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({paused: target})
            });
            if (!r.ok) throw new Error('request failed');
            const j = await r.json();
            setPauseAllButton(!!j.pause_all);
            showToastStyled(j.pause_all ? 'Queue paused (running downloads continue)' : 'Queue resumed');
        } catch (e) {
            setPauseAllButton(previous);
            showToastStyled('Failed to update pause-all state', 'error');
        } finally {
            pauseAllPending = false;
            if (btn) btn.disabled = false;
        }
    }

    function detachSSE(clearReconnect = true) {
        if (clearReconnect && reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
        if (sse) {
            try {
                sse.close();
            } catch (e) {
            }
            sse = null;
        }
    }

    function attachSSE() {
        if (sseSuspended || document.hidden) return;
        if (sse) {
            try {
                sse.close();
            } catch (e) {
            }
        }
        sse = new EventSource('/events');
        sse.onmessage = e => {
            try {
                const st = JSON.parse(e.data);
                if (st && st.type === 'item_update') {
                    applyItemUpdateMessage(st);
                    return;
                }
                handleState(st);
            } catch (err) {
                console.error('SSE JSON error', err, e.data);
            }
        };
        sse.onerror = err => { /* console.warn('SSE error - reconnecting', err); */
            if (sseSuspended || document.hidden) {
                detachSSE();
                return;
            }
            showToastStyled('SSE error - reconnecting', 'warning');
            try {
                sse.close();
            } catch (e) {
            }
            if (reconnectTimer) clearTimeout(reconnectTimer);
            reconnectTimer = setTimeout(attachSSE, 3000);
        };
    }

    document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
            sseSuspended = true;
            detachSSE();
            return;
        }
        sseSuspended = false;
        // Force next SSE payload to be treated as a full refresh.
        stateCache = null;
        attachSSE();
    });

    window.addEventListener('beforeunload', () => {
        detachSSE();
    });

    if (!document.hidden) attachSSE();

    function showToastStyled(msg, type = 'success', timeout = 4000) {
        const t = document.createElement('div');
        t.className = `toast ${type}`;
        t.textContent = msg;
        document.getElementById('toasts').appendChild(t);
        setTimeout(() => {
            t.style.opacity = 0;
            setTimeout(() => t.remove(), 500);
        }, timeout);
    }

    const modalOverlay = document.getElementById('modalOverlay');
    const modalContent = document.getElementById('modalContent');
    const modalClose = document.getElementById('modalClose');
    let liveLogES = null;

    function openLog(id) {
        // static (one-off) log viewer
        modalContent.textContent = 'Loading...';
        modalOverlay.style.display = 'flex';
        fetch('/log/' + id).then(r => {
            if (!r.ok) throw new Error('no log');
            return r.text()
        }).then(txt => {
            modalContent.textContent = txt;
        }).catch(e => {
            modalContent.textContent = 'Error loading log';
        });
    }

    function openLiveLog(id) {
        // live tailing log via SSE
        modalContent.textContent = 'Connecting...';
        modalOverlay.style.display = 'flex';
        // close any previous live stream
        if (liveLogES) {
            try {
                liveLogES.close();
            } catch (e) {
            }
            liveLogES = null;
        }
        liveLogES = new EventSource('/log/stream/' + id);
        liveLogES.onmessage = e => {
            try {
                const payload = JSON.parse(e.data);
                const chunk = payload && payload.chunk ? payload.chunk : '';
                if (!chunk) return;
                // preserve scroll position if user is at bottom
                const wasAtBottom = (modalContent.scrollTop + modalContent.clientHeight >= modalContent.scrollHeight - 8);
                // append safely
                modalContent.textContent = modalContent.textContent + chunk;
                // keep scroll at bottom only if user was at bottom before update
                if (wasAtBottom) {
                    modalContent.scrollTop = modalContent.scrollHeight;
                }
            } catch (err) {
                console.error('live log parse error', err);
            }
        };
        liveLogES.onerror = e => {
            // keep the modal visible and show an error marker
            console.warn('Live log SSE error', e);
        };
    }

    function closeModal() {
        modalOverlay.style.display = 'none';
        modalContent.textContent = '';
        if (liveLogES) {
            try {
                liveLogES.close();
            } catch (e) {
            }
            liveLogES = null;
        }
    }

    modalClose.onclick = closeModal;
    modalOverlay.addEventListener('click', e => {
        if (e.target === modalOverlay) closeModal();
    });
    window.addEventListener('keydown', e => {
        if (e.key === 'Escape') closeModal();
    });

    function countsFromState(st) {
        // Single-array payload: st.queue === [ ...items... ]
        const counts = {all: 0, queued: 0, downloading: 0, completed: 0, errors: 0, duplicates: 0};
        const arr = Array.isArray(st.queue) ? st.queue : [];
        arr.forEach(it => {
            counts.all += 1;
            const s = itemStatus(it);
            if (s === 'queued' || s === 'paused') counts.queued += 1;
            if (s === 'downloading' || s === 'postprocessing') counts.downloading += 1;
            if (s === 'completed') counts.completed += 1;
            if (s === 'error' || s === 'stalled' || s === 'cancelled') counts.errors += 1;
            if (s === 'duplicate') counts.duplicates += 1;
        });
        return counts;
    }

    function itemStatus(it) {
        if (!it) return 'queued';
        if (it.status) return it.status === 'processing' ? 'queued' : it.status; // map legacy transient status
        if (it.error) return 'error';
        if (it.progress || it.in_downloads) return (it.progress && it.progress.status === 'postprocessing') ? 'postprocessing' : 'downloading';
        if (it.completed_at) return 'completed';
        if (it.duplicate || it.status === 'duplicate') return 'duplicate';
        if (it.stalled) return 'stalled';
        if (it.paused) return 'paused';
        return 'queued';
    }

    function isMobileViewport() {
        const vw = Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0);
        return vw > 0 && vw <= 600;
    }

    function statusSortRank(status) {
        if (status === 'downloading' || status === 'postprocessing' || status === 'waiting') return 0;
        if (status === 'queued' || status === 'paused') return 1;
        if (status === 'error' || status === 'stalled' || status === 'cancelled') return 2;
        if (status === 'duplicate') return 3;
        if (status === 'completed') return 4;
        return 1;
    }

    function sortQueueItems(items) {
        return (Array.isArray(items) ? items.slice() : []).sort((a, b) => {
            const sa = itemStatus(a);
            const sb = itemStatus(b);
            const ra = statusSortRank(sa);
            const rb = statusSortRank(sb);
            if (ra !== rb) return ra - rb;
            if (ra === 4) {
                const byCompleted = (b.completed_at || 0) - (a.completed_at || 0);
                if (byCompleted !== 0) return byCompleted;
            }
            const byAdded = (b.added_at || 0) - (a.added_at || 0);
            if (byAdded !== 0) return byAdded;
            return String(a.id || '').localeCompare(String(b.id || ''));
        });
    }

    function statusIconMeta(status) {
        if (status === 'paused') return {className: 'status-icon status-paused', title: 'paused', inlineStyle: ''};
        if (status === 'downloading' || status === 'postprocessing') {
            return {className: 'status-icon status-downloading', title: 'downloading/postprocessing', inlineStyle: ''};
        }
        if (status === 'completed') return {className: 'status-icon status-complete', title: 'completed', inlineStyle: ''};
        if (status === 'error') return {className: 'status-icon status-error', title: 'error', inlineStyle: ''};
        if (status === 'duplicate') return {className: 'status-icon status-duplicate', title: 'duplicate', inlineStyle: ''};
        if (status === 'stalled') return {className: 'status-icon status-stalled', title: 'stalled', inlineStyle: ''};
        return {className: 'status-icon', title: 'queued', inlineStyle: 'background:#999'};
    }

    function statusIcon(status) {
        const meta = statusIconMeta(status);
        const style = meta.inlineStyle ? ` style="${meta.inlineStyle}"` : '';
        return `<span class="${meta.className}" title="${meta.title}"${style}></span>`;
    }

    function truncateSingleLine(str, maxChars) {
        if (!str) return '';
        if (str.length <= maxChars) return str;
        return str.slice(0, maxChars - 1) + '…';
    }

    function buildRightButtonsHTML(id, status, mobile) {
        let rightButtons = '';
        if (status === 'paused') rightButtons += `<button onclick="resumeQueued('${id}')">Resume</button> `;
        if (status === 'error' || status === 'stalled' || status === 'duplicate') {
            rightButtons += mobile
                ? `<button class="mobile-icon-btn icon-retry" title="Retry" aria-label="Retry" onclick="retryItem('${id}')">↻</button> `
                : `<button onclick="retryItem('${id}')">Retry</button> `;
        }
        if (status === 'error' && jdownloaderConfigured) {
            rightButtons += mobile
                ? `<button class="mobile-icon-btn icon-jd2" title="Send to JD2" aria-label="Send to JD2" onclick="sendToJDItem('${id}')">📤</button> `
                : `<button onclick="sendToJDItem('${id}')">Send to JD2</button> `;
        }
        if (status === 'completed' || status === 'error' || status === 'stalled') {
            rightButtons += mobile
                ? `<button class="mobile-icon-btn icon-log" title="Log" aria-label="Log" onclick="openLog('${id}')">📄</button> `
                : `<button onclick="openLog('${id}')">Log</button> `;
        }
        if (status === 'downloading' || status === 'postprocessing') {
            rightButtons += `<button onclick="openLiveLog('${id}')">Live Log</button> `;
            rightButtons += `<button onclick="cancelItem('${id}')">Cancel</button>`;
        }
        if (status !== 'downloading' && status !== 'postprocessing') {
            rightButtons += mobile
                ? `<button class="mobile-icon-btn icon-remove" title="Remove" aria-label="Remove" onclick="removeEntry('${id}')">✖</button>`
                : `<button onclick="removeEntry('${id}')">Remove</button>`;
        }
        return rightButtons;
    }

    function getItemRenderModel(it, status) {
        const activeProgress = status === 'downloading' || status === 'postprocessing' || (it.progress && it.progress.status === 'waiting');
        const rawPct = Number(it.progress && it.progress.percent != null ? it.progress.percent : 0);
        const pct = Number.isFinite(rawPct) ? Math.max(0, Math.min(100, rawPct)) : 0;
        const pctText = it.progress ? `${Math.round(pct)}%` : '';
        const etaText = it.progress && it.progress.eta ? `ETA: ${it.progress.eta}` : '';

        let smallText = '';
        if (it.error && it.last_output) {
            smallText = truncateSingleLine(it.last_output, 120);
        } else if (it.progress && it.progress.status === 'postprocessing' && it.progress.detail) {
            // show current post-processing log line to indicate what is being worked on
            smallText = truncateSingleLine(it.progress.detail, 120);
        } else if (status === 'downloading' || status === 'postprocessing' || (it.progress && it.progress.status === 'waiting')) {
            const progressLine = [status];
            if (pctText) progressLine.push(pctText);
            if (etaText) progressLine.push(etaText);
            smallText = progressLine.join(' • ');
        } else {
            smallText = status;
        }
        return {
            pct,
            showProgress: Boolean(it.progress || activeProgress),
            smallText,
        };
    }

    function buildItemHTML(it) {
        const status = itemStatus(it);
        const favicon = it.favicon || '_generic.ico';
        const id = it.id;
        const url = it.url;
        const mobile = isMobileViewport();
        const rightButtons = buildRightButtonsHTML(id, status, mobile);
        const m = getItemRenderModel(it, status);
        const progressHTML = m.showProgress ? `<div class="progress"><div class="fill" style="width:${m.pct}%"></div></div>` : '';
        const smallText = `<div class="small">${escapeHtml(m.smallText)}</div>`;

        if (!mobile) {
            // desktop interface
            return `
          <div class="top-row">
            <div class="left-group">
              <input type="checkbox" class="sel" data-id="${id}">
              ${statusIcon(status)}
              <img class="favicon" src="/favicons/${favicon}">
            </div>
          </div>
          <div class="bottom-row">
            <div class="meta-col">
              <div class="url-text"><strong>${escapeHtml(url)}</strong></div>
              ${smallText}
              ${progressHTML}
            </div>
            <div class="right-group">${rightButtons}</div>
          </div>`;
        } else {
            // mobile interface
            return `
          <div class="top-row">
            <div class="left-group">
              <input type="checkbox" class="sel" data-id="${id}">
              ${statusIcon(status)}
              <img class="favicon" src="/favicons/${favicon}">
            </div>
            <div class="right-group">${rightButtons}</div>
          </div>
          <div class="bottom-row">
            <div class="meta-col">
              <div class="url-text"><strong>${escapeHtml(url)}</strong></div>
              ${smallText}
              ${progressHTML}
            </div>
          </div>`;
        }

    }

    function renderItemRow(li, it, opts = {}) {
        const preserveChecked = opts.preserveChecked !== false;
        const forceFull = !!opts.forceFull;
        const status = itemStatus(it);
        const id = String((it && it.id) || '');
        const url = String((it && it.url) || '');
        const favicon = it.favicon || '_generic.ico';
        const mobile = isMobileViewport();
        const layoutMarker = mobile ? '1' : '0';
        const previousChecked = preserveChecked ? Boolean(li.querySelector('.sel')?.checked) : false;
        const m = getItemRenderModel(it, status);

        const canPatch = !forceFull
            && li.getAttribute('data-layout-mobile') === layoutMarker
            && li.querySelector('.top-row')
            && li.querySelector('.meta-col')
            && li.querySelector('.right-group')
            && li.querySelector('.sel')
            && li.querySelector('.status-icon')
            && li.querySelector('.favicon')
            && li.querySelector('.url-text strong');

        if (!canPatch) {
            li.innerHTML = buildItemHTML(it);
        } else {
            const iconEl = li.querySelector('.status-icon');
            const iconMeta = statusIconMeta(status);
            iconEl.className = iconMeta.className;
            iconEl.title = iconMeta.title;
            iconEl.style.cssText = iconMeta.inlineStyle || '';

            const faviconEl = li.querySelector('.favicon');
            if (faviconEl.getAttribute('src') !== `/favicons/${favicon}`) {
                faviconEl.setAttribute('src', `/favicons/${favicon}`);
            }

            const urlStrong = li.querySelector('.url-text strong');
            if (urlStrong && urlStrong.textContent !== url) urlStrong.textContent = url;

            const metaCol = li.querySelector('.meta-col');
            let smallEl = li.querySelector('.meta-col .small');
            if (!smallEl) {
                smallEl = document.createElement('div');
                smallEl.className = 'small';
                metaCol.appendChild(smallEl);
            }
            if (smallEl.textContent !== m.smallText) smallEl.textContent = m.smallText;

            let progressEl = li.querySelector('.meta-col .progress');
            if (m.showProgress) {
                if (!progressEl) {
                    progressEl = document.createElement('div');
                    progressEl.className = 'progress';
                    progressEl.innerHTML = '<div class="fill"></div>';
                    metaCol.appendChild(progressEl);
                }
                let fillEl = progressEl.querySelector('.fill');
                if (!fillEl) {
                    fillEl = document.createElement('div');
                    fillEl.className = 'fill';
                    progressEl.innerHTML = '';
                    progressEl.appendChild(fillEl);
                }
                fillEl.style.width = `${m.pct}%`;
            } else if (progressEl) {
                progressEl.remove();
            }

            const rightGroup = li.querySelector('.right-group');
            const rightButtons = buildRightButtonsHTML(id, status, mobile);
            if (rightGroup.innerHTML !== rightButtons) rightGroup.innerHTML = rightButtons;
        }

        li.setAttribute('data-id', id);
        li.setAttribute('data-layout-mobile', layoutMarker);
        const sel = li.querySelector('.sel');
        if (sel) {
            sel.setAttribute('data-id', id);
            sel.checked = previousChecked;
            if (!sel._attached) {
                sel.addEventListener('change', refreshBulkBar);
                sel._attached = true;
            }
        }
    }

    // very small html escape helper for injected text
    function escapeHtml(s) {
        return String(s).replace(/[&<>"]/g, function (c) {
            return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c];
        });
    }

    function getQueueScrollAnchor() {
        const box = document.getElementById('mainBox');
        if (!box) return null;
        const atBottom = (box.scrollTop + box.clientHeight >= box.scrollHeight - 2);
        if (atBottom) return {atBottom: true};
        const boxTop = box.getBoundingClientRect().top;
        const rows = Array.from(document.querySelectorAll('#items .item'));
        for (const row of rows) {
            if (row.style.display === 'none') continue;
            const rect = row.getBoundingClientRect();
            if (rect.bottom > boxTop) {
                return {
                    atBottom: false,
                    id: row.getAttribute('data-id'),
                    offsetTop: rect.top - boxTop,
                    scrollTop: box.scrollTop
                };
            }
        }
        return {atBottom: false, id: null, offsetTop: 0, scrollTop: box.scrollTop};
    }

    function restoreQueueScrollAnchor(anchor) {
        if (!anchor) return;
        const box = document.getElementById('mainBox');
        if (!box) return;
        if (anchor.atBottom) {
            box.scrollTop = box.scrollHeight;
            return;
        }
        if (anchor.id) {
            const rows = Array.from(document.querySelectorAll('#items .item'));
            const row = rows.find(x => x.getAttribute('data-id') === anchor.id);
            if (row && row.style.display !== 'none') {
                const boxTop = box.getBoundingClientRect().top;
                const newOffsetTop = row.getBoundingClientRect().top - boxTop;
                box.scrollTop += (newOffsetTop - anchor.offsetTop);
                return;
            }
        }
        box.scrollTop = anchor.scrollTop || 0;
    }

    function fullRender(st) {
        stateCache = st;
        const itemsEl = document.getElementById('items');
        itemsEl.innerHTML = '';
        const all = sortQueueItems(Array.isArray(st.queue) ? st.queue : []);

        all.forEach(it => {
            const li = document.createElement('li');
            li.className = 'item';
            li.setAttribute('data-id', it.id);
            renderItemRow(li, it, {preserveChecked: false, forceFull: true});
            itemsEl.appendChild(li);
        });

        const c = countsFromState(st);
        document.getElementById('cnt-all').innerText = c.all;
        document.getElementById('cnt-queued').innerText = c.queued;
        document.getElementById('cnt-downloading').innerText = c.downloading;
        document.getElementById('cnt-completed').innerText = c.completed;
        document.getElementById('cnt-errors').innerText = c.errors;
        document.getElementById('cnt-duplicates').innerText = c.duplicates;

        updateTabsVisibility(c);
        // Re-apply active-tab filtering whenever statuses change.
        filterView();
        attachSelectionHandlers();
    }

    function incrementalUpdate(st) {
        // Update only changed items in the list for minimal DOM churn.
        const scrollAnchor = getQueueScrollAnchor();
        const prevMap = {};
        const prev = stateCache && Array.isArray(stateCache.queue) ? stateCache.queue : [];
        prev.forEach(it => { if (it && it.id) prevMap[String(it.id)] = it; });

        const now = Array.isArray(st.queue) ? st.queue : [];
        const sortedNow = sortQueueItems(now);
        const desiredIds = sortedNow
            .map(it => String((it && it.id) || ''))
            .filter(Boolean);
        const nowIds = new Set(desiredIds);
        const itemsEl = document.getElementById('items');
        let hasStructuralChanges = false;

        const existingById = new Map();
        Array.from(itemsEl.querySelectorAll('.item')).forEach(el => {
            const id = el.getAttribute('data-id');
            if (id) existingById.set(id, el);
        });

        // remove stale elements that no longer exist
        Array.from(existingById.entries()).forEach(([id, el]) => {
            if (!nowIds.has(id)) {
                el.remove();
                existingById.delete(id);
                hasStructuralChanges = true;
            }
        });

        // update existing elements and create new ones
        sortedNow.forEach(it => {
            const id = String((it && it.id) || '');
            if (!id) return;
            let li = existingById.get(id);
            if (li) {
                const prevIt = prevMap[id];
                // shallow JSON compare (good enough for UI updates)
                if (!prevIt || JSON.stringify(prevIt) !== JSON.stringify(it)) {
                    renderItemRow(li, it);
                }
            } else {
                li = document.createElement('li');
                li.className = 'item';
                li.setAttribute('data-id', id);
                renderItemRow(li, it, {preserveChecked: false, forceFull: true});
                existingById.set(id, li);
                hasStructuralChanges = true;
            }
        });

        // only reorder if structure/order actually changed
        const currentOrder = Array.from(itemsEl.querySelectorAll('.item'))
            .map(el => el.getAttribute('data-id'))
            .filter(Boolean);
        const orderChanged = hasStructuralChanges
            || currentOrder.length !== desiredIds.length
            || desiredIds.some((id, idx) => currentOrder[idx] !== id);
        if (orderChanged) {
            const frag = document.createDocumentFragment();
            desiredIds.forEach(id => {
                const li = existingById.get(id);
                if (li) frag.appendChild(li);
            });
            itemsEl.appendChild(frag);
        }

        const c = countsFromState(st);
        document.getElementById('cnt-all').innerText = c.all;
        document.getElementById('cnt-queued').innerText = c.queued;
        document.getElementById('cnt-downloading').innerText = c.downloading;
        document.getElementById('cnt-completed').innerText = c.completed;
        document.getElementById('cnt-errors').innerText = c.errors;
        document.getElementById('cnt-duplicates').innerText = c.duplicates;

        // Ensure filter/selection logic uses the newest queue state.
        stateCache = st;
        updateTabsVisibility(c);
        // Re-apply active-tab filtering whenever statuses change.
        filterView();
        attachSelectionHandlers();
        if (orderChanged) restoreQueueScrollAnchor(scrollAnchor);
    }

    function handleState(st) {
        if (st && typeof st.pause_all !== 'undefined') setPauseAllButton(!!st.pause_all);
        if (!stateCache) {
            fullRender(st);
            stateCache = st;
            return;
        }
        incrementalUpdate(st);
    }

    function applyItemUpdateMessage(msg) {
        if (!msg || !msg.id) return;
        if (!stateCache || !Array.isArray(stateCache.queue)) return;
        const it = stateCache.queue.find(x => x && x.id === msg.id);
        if (!it) return;

        let changed = false;
        if (Object.prototype.hasOwnProperty.call(msg, 'progress')) {
            const oldProgress = JSON.stringify(it.progress || null);
            const newProgress = JSON.stringify(msg.progress || null);
            if (oldProgress !== newProgress) {
                it.progress = msg.progress;
                changed = true;
            }
        }
        if (Object.prototype.hasOwnProperty.call(msg, 'status')) {
            const oldStatus = String(it.status || '');
            const newStatus = String(msg.status || '');
            if (oldStatus !== newStatus) {
                it.status = msg.status;
                changed = true;
            }
        }
        if (Object.prototype.hasOwnProperty.call(msg, 'pause_all')) setPauseAllButton(!!msg.pause_all);
        if (!changed) return;

        const li = document.querySelector(`#items [data-id="${msg.id}"]`);
        if (!li) return;
        const wasChecked = Boolean(li.querySelector('.sel')?.checked);
        renderItemRow(li, it);
        const status = itemStatus(it);
        li.style.display = isStatusVisibleInCurrentTab(status) ? 'flex' : 'none';
        if (wasChecked) refreshBulkBar();
    }

    function updateTabsVisibility(counts) {
        // always show 'all'
        const tabs = document.querySelectorAll('#tabs .tab');
        tabs.forEach(tab => {
            const t = tab.getAttribute('data-tab');
            if (t === 'all') {
                if (counts.all === 0) {
                    tab.classList.add('hidden');
                    tab.style.display = 'none';
                } else {
                    tab.classList.remove('hidden');
                    tab.style.display = 'inline-flex';
                }
                return;
            }
            const cnt = counts[t] || 0;
            if (cnt === 0) {
                tab.classList.add('hidden');
                tab.style.display = 'none';
            } else {
                tab.classList.remove('hidden');
                tab.style.display = 'inline-flex';
            }
        });
        // ensure currentTab still exists
        if (document.querySelector(`#tabs .tab[data-tab="${currentTab}"]`)?.classList.contains('hidden')) {
            // switch back to all
            document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
            const allTab = document.querySelector('.tab[data-tab="all"]');
            if (allTab) allTab.classList.add('active');
            currentTab = 'all';
            filterView();
        }
    }

    function attachSelectionHandlers() {
        // attach change listeners for checkboxes
        const boxes = document.querySelectorAll('#items .sel');
        boxes.forEach(b => {
            if (!b._attached) {
                b.addEventListener('change', refreshBulkBar);
                b._attached = true;
            }
        });
        refreshBulkBar();
    }

    function refreshBulkBar() {
        const selectedBoxes = Array.from(document.querySelectorAll('#items .sel'));
        const selected = selectedBoxes.filter(b => b.checked).map(b => b.getAttribute('data-id'));
        const anySelected = selected.length > 0;

        // enable select controls when there's at least one selection (per request)
        document.getElementById('selectControls').classList.toggle('hidden', !anySelected);

        // Determine statuses of selected items
        let anyPaused = false, anyQueued = false, anyErrorOrDup = false;
        selected.forEach(id => {
            const it = findItemInCache(id);
            if (!it) return;
            const s = itemStatus(it);
            if (s === 'paused') anyPaused = true;
            if (s === 'queued' || s === 'paused' || s === 'stalled') anyQueued = true;
            if (s === 'error' || s === 'duplicate' || s === 'stalled') anyErrorOrDup = true;
        });

        // Which bulk buttons to show depends on selection + active tab
        document.getElementById('bulkRemove').classList.toggle('hidden', !anySelected);
        // bulkRetry only when selection includes errors/dupes and currentTab in allowed list
        const retryAllowedTab = (currentTab === 'all' || currentTab === 'errors' || currentTab === 'duplicates');
        document.getElementById('bulkRetry').classList.toggle('hidden', !(anyErrorOrDup && retryAllowedTab));
        // resume only when any paused selected
        document.getElementById('bulkResume').classList.toggle('hidden', !anyPaused);
        // pause only when any queued selected (i.e., not paused/downloading)
        document.getElementById('bulkPause').classList.toggle('hidden', !anyQueued);

        const counts = {
            completed: parseInt(document.getElementById('cnt-completed').innerText || '0', 10),
            errors: parseInt(document.getElementById('cnt-errors').innerText || '0', 10),
            duplicates: parseInt(document.getElementById('cnt-duplicates').innerText || '0', 10)
        };
        updateHamburgerMenu(counts);
     }

    // selection helpers
    function selectAllVisible(){
        const visibleBoxes = Array.from(document.querySelectorAll('#items .item')).filter(li => li.style.display !== 'none')
            .map(li => li.querySelector('.sel')).filter(Boolean);
        visibleBoxes.forEach(b => b.checked = true);
        refreshBulkBar();
    }
    function invertSelection(){
        const visibleBoxes = Array.from(document.querySelectorAll('#items .item')).filter(li => li.style.display !== 'none')
            .map(li => li.querySelector('.sel')).filter(Boolean);
        visibleBoxes.forEach(b => b.checked = !b.checked);
        refreshBulkBar();
    }
    function selectNone(){
        Array.from(document.querySelectorAll('#items .sel')).forEach(b => b.checked = false);
        refreshBulkBar();
    }

    // wire select control buttons
    document.getElementById('selAll')?.addEventListener('click', selectAllVisible);
    document.getElementById('selInvert')?.addEventListener('click', invertSelection);
    document.getElementById('selNone')?.addEventListener('click', selectNone);
    document.getElementById('pauseAllToggle')?.addEventListener('click', togglePauseAll);
    setPauseAllButton(false);

    function updateHamburgerMenu(counts){
        const hamCont = document.getElementById('hamburgerContainer');
        if(!hamCont) return;

        // individual buttons
        document.getElementById('removeCompleted').style.display = counts.completed > 0 ? 'block' : 'none';
        document.getElementById('retryAllErrors').style.display = counts.errors > 0 ? 'block' : 'none';
        document.getElementById('removeAllErrors').style.display = counts.errors > 0 ? 'block' : 'none';
        document.getElementById('copyErrors').style.display = counts.errors > 0 ? 'block' : 'none';
        const hasDupes = counts.duplicates > 0;
        document.getElementById('retryAllDupes').style.display = hasDupes ? 'block' : 'none';
        document.getElementById('removeAllDupes').style.display = hasDupes ? 'block' : 'none';
        document.getElementById('copyDupes').style.display = hasDupes ? 'block' : 'none';
        document.getElementById('sendErrorsToJD').style.display = (counts.errors > 0 && jdownloaderConfigured) ? 'block' : 'none';

        // remove hamburger if no options are visible
        const anyVisible = counts.errors > 0 || counts.duplicates > 0 || counts.completed > 0;
        hamCont.classList.toggle('hidden', !anyVisible);
    }

    // tab click handling
    document.getElementById('tabs').addEventListener('click', e => {
        const t = e.target.closest('.tab');
        if (!t) return;
        document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        currentTab = t.getAttribute('data-tab');
        filterView();
    });

    function isStatusVisibleInCurrentTab(status) {
        if (currentTab === 'all') return true;
        if (currentTab === 'queued') return status === 'paused' || status === 'queued';
        if (currentTab === 'downloading') return status === 'downloading' || status === 'postprocessing';
        if (currentTab === 'completed') return status === 'completed';
        if (currentTab === 'errors') return status === 'error' || status === 'stalled' || status === 'cancelled';
        if (currentTab === 'duplicates') return status === 'duplicate';
        return false;
    }

    function filterView() {
        const allItems = Array.from(document.querySelectorAll('#items .item'));
        allItems.forEach(li => {
            const id = li.getAttribute('data-id');
            const it = findItemInCache(id);
            if (!it) { li.style.display = 'none'; return; }
            const status = itemStatus(it);
            li.style.display = isStatusVisibleInCurrentTab(status) ? 'flex' : 'none';
        });
    }

    function findItemInCache(id) {
        if (!stateCache || !Array.isArray(stateCache.queue)) return null;
        return stateCache.queue.find(x => x && x.id === id) || null;
    }

    fetch('/version').then(r => r.json()).then(j => {
        const localVer = j.yt_dlp_version || 'unknown';
        const latestVer = j.latest_ytdlp || '';
        const latestAppVer = j.latest_tubeq || '';
        document.getElementById('ytdlpVer').innerText = localVer;
        document.getElementById('appVer').innerText = j.app_version || 'unknown';
        renderTubeQUpdateArea(Boolean(j.tubeq_update_available), latestAppVer);
        document.getElementById('currentVerText').innerText = localVer;
        document.getElementById('latestVerText').innerText = latestVer;
        // render update link area if needed
        renderUpdateArea(Boolean(j.update_available), latestVer);
    });

    fetch('/status').then(r => r.json()).then(j => {
        jdownloaderConfigured = Boolean(j.jdownloader_configured);
        refreshBulkBar();
        // items may have already rendered before this resolved; re-render
        // error rows so the "Send to JD2" button reflects the current state
        if (jdownloaderConfigured && stateCache && Array.isArray(stateCache.queue)) {
            stateCache.queue.forEach(it => {
                if (it.status === 'error') {
                    const el = document.querySelector(`#items [data-id="${it.id}"]`);
                    if (el) renderItemRow(el, it);
                }
            });
        }
    });

    function renderTubeQUpdateArea(available, latest) {
        const el = document.getElementById('appUpdateArea');
        if (!el) return;
        if (!available || !latest) {
            el.innerHTML = '';
            return;
        }
        el.innerHTML = `&nbsp;|&nbsp; <span style="color:yellow;">update available (${escapeHtml(latest)})</span>`;
    }

    function renderUpdateArea(available, latest) {
        const el = document.getElementById('updateArea');
        if (!available) {
            el.innerHTML = '';
            return;
        }
        el.innerHTML = `&nbsp;|&nbsp; <a id="updateLink" href="#" style="color:yellow;text-decoration:none;">update available (${latest})</a>`;
        document.getElementById('latestVerText').innerText = latest;
        document.getElementById('updateLink').addEventListener('click', (e) => {
            e.preventDefault();
            // show modal with latest version
            document.getElementById('updateOverlay').style.display = 'flex';
        });
    }

    // Add form
    document.getElementById('addForm').onsubmit = async e => {
        e.preventDefault();
        const text = document.getElementById('urls').value.trim();
        const paused = document.getElementById('addPaused').checked;
        if (!text) return;
        const res = await fetch('/add?paused=' + (paused ? 'true' : 'false'), {
            method: 'POST',
            headers: {'Content-Type': 'text/plain'},
            body: text
        });
        const j = await res.json();
        showToastStyled(`Added ${j.added} URLs — duplicates ${j.duplicates}`, 'success');
        document.getElementById('urls').value = '';
    };

    async function copyToClipboard(textToCopy) {
        if (navigator.clipboard && window.isSecureContext) {
            await navigator.clipboard.writeText(textToCopy);
        } else {
            const textArea = document.createElement("textarea");
            textArea.value = textToCopy;
            textArea.style.position = "absolute";
            textArea.style.left = "-999999px";
            document.body.prepend(textArea);
            textArea.select();

            try {
                document.execCommand('copy');
            } catch (error) {
                console.error(error);
            } finally {
                textArea.remove();
            }
        }
    }

    // copy all errors/duplicates
    document.getElementById('copyErrors').onclick = async () => {
        const r = await fetch('/dump_errors');
        const j = await r.json();
        const s = j.urls.join('\n');
        await copyToClipboard(s);
        showToastStyled('Copied errors to clipboard');
    }
    document.getElementById('copyDupes').onclick = async () => {
        const r = await fetch('/dump_duplicates');
        const j = await r.json();
        const s = j.urls.join('\n');
        await copyToClipboard(s);
        showToastStyled('Copied duplicates to clipboard');
    }

    async function getSelectedIds() {
        return Array.from(document.querySelectorAll('#items .sel:checked')).map(i => i.getAttribute('data-id'));
    }
    async function bulkRemove() {
        const ids = await getSelectedIds();
        if (ids.length === 0) return showToastStyled('No selection', 'error');
        openConfirmModal('Remove selected entries and logs?', async () => {
            // optimistic remove from UI:
            ids.forEach(id => {
                const el = document.querySelector(`#items [data-id="${id}"]`);
                if (el) el.remove();
                if (stateCache && Array.isArray(stateCache.queue)) stateCache.queue = stateCache.queue.filter(x => x.id !== id);
            });
            filterView();
            try {
                await fetch('/bulk_remove', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ids})});
                showToastStyled('Removed selected');
            } catch (e) {
                console.warn('bulk remove failed', e);
                showToastStyled('Removal failed', 'error');
            }
        });
        return;
    }
    async function bulkRetry() {
        const ids = await getSelectedIds();
        if (ids.length === 0) return showToastStyled('No selection', 'error');
        // optimistic update
        ids.forEach(id => {
            const it = findItemInCache(id);
            if (it) { it.status = 'queued'; it.paused = false; }
            const el = document.querySelector(`#items [data-id="${id}"]`);
            if (el && it) renderItemRow(el, it);
        });
        filterView();
        try {
            await fetch('/bulk_retry', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ids})});
            showToastStyled('Retry queued');
        } catch (e) {
            console.warn('bulk retry failed', e);
            showToastStyled('Retry failed', 'error');
        }
    }
    async function bulkPause() {
        const ids = await getSelectedIds();
        if (ids.length === 0) return showToastStyled('No selection', 'error');
        // optimistic
        ids.forEach(id => {
            const it = findItemInCache(id); if (it) { it.paused = true; it.status = 'paused'; const el = document.querySelector(`#items [data-id="${id}"]`); if (el) renderItemRow(el, it); }
        });
        filterView();
        try {
            await fetch('/bulk_pause_resume', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ids, pause: true})});
            showToastStyled('Paused selected');
        } catch (e) {
            console.warn('bulk pause failed', e);
            showToastStyled('Pause failed', 'error');
        }
    }
    async function bulkResume() {
        const ids = await getSelectedIds();
        if (ids.length === 0) return showToastStyled('No selection', 'error');
        ids.forEach(id => {
            const it = findItemInCache(id); if (it) { it.paused = false; it.status = 'queued'; const el = document.querySelector(`#items [data-id="${id}"]`); if (el) renderItemRow(el, it); }
        });
        filterView();
        try {
            await fetch('/bulk_pause_resume', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ids, pause: false})});
            showToastStyled('Resumed selected');
        } catch (e) {
            console.warn('bulk resume failed', e);
            showToastStyled('Resume failed', 'error');
        }
    }

    document.getElementById('bulkRemove').onclick = bulkRemove;
    document.getElementById('bulkRetry').onclick = bulkRetry;
    document.getElementById('bulkPause').onclick = bulkPause;
    document.getElementById('bulkResume').onclick = bulkResume;

    document.getElementById('removeAllErrors').onclick = () => {
        openConfirmModal('Remove all errors?', async () => {
            await fetch('/clear', {method: 'POST', headers: { "Content-Type": "application/json" }, body: ('{"statuses":["error"]}') });
            showToastStyled('Removed all errors');
        });
    };

    document.getElementById('removeCompleted').onclick = () => {
        fetch('/clear', {method: 'POST', headers: {"Content-Type": "application/json"}, body: ('{"statuses":["completed"]}') })
            .then(() => showToastStyled('Removed all completed'))
            .catch(() => showToastStyled('Failed to remove completed', 'error'));
    };

    document.getElementById('removeAllDupes').onclick = () => {
        fetch('/clear', {method: 'POST', headers: {"Content-Type": "application/json"}, body: ('{"statuses":["duplicate"]}') })
            .then(() => showToastStyled('Removed all duplicates'))
            .catch(() => showToastStyled('Failed to remove duplicates', 'error'));
    };



    document.getElementById('retryAllErrors').onclick = () => {
        openConfirmModal('Retry all errors?', async () => {
            await fetch('/retry_all_errors', {method: 'POST'});
            showToastStyled('Retried all errors');
        });
    };

    document.getElementById('retryAllDupes').onclick = () => {
        openConfirmModal('Retry all duplicates?', async () => {
            await fetch('/retry_all_dupes', {method: 'POST'});
            showToastStyled('Retried all duplicates');
        });
    };

    document.getElementById('sendErrorsToJD').onclick = () => {
        openConfirmModal('Send all errors to JDownloader2 and remove them from this queue?', async () => {
            try {
                const r = await fetch('/jdownloader/send_errors', {method: 'POST'});
                const j = await r.json();
                if (!r.ok) {
                    showToastStyled('JDownloader2: ' + (j.error || 'failed'), 'error');
                    return;
                }
                showToastStyled(`Sent ${j.sent} error(s) to JDownloader2`);
            } catch (e) {
                showToastStyled('JDownloader2: request failed', 'error');
            }
        });
    };


    async function removeEntry(id) {
        // Optimistically remove from UI immediately
        const el = document.querySelector(`#items [data-id="${id}"]`);
        if (el) el.remove();
        if (stateCache && Array.isArray(stateCache.queue)) {
            stateCache.queue = stateCache.queue.filter(x => x.id !== id);
        }
        filterView();
        showToastStyled('Removed');
        // Notify server
        try { await fetch('/remove_entry/' + id, {method: 'POST'}); } catch (e) { console.warn('remove failed', e); }
    }

    function openConfirmModal(message, onConfirm) {
        const overlay = document.createElement('div');
        overlay.className = 'overlay';
        overlay.style.display = 'flex';
        overlay.innerHTML = `
        <div class="modal">
          <div class="modal-header"><h3>Confirm</h3></div>
          <div class="modal-content"><p>${message}</p></div>
          <div class="btn-row">
            <button class="ghost" id="cancelBtn">Cancel</button>
            <button id="okBtn">OK</button>
          </div>
        </div>`;
        document.body.appendChild(overlay);
        overlay.querySelector('#cancelBtn').onclick = () => overlay.remove();
        overlay.querySelector('#okBtn').onclick = () => {
            overlay.remove();
            if (typeof onConfirm === 'function') {
                // Safely handle both async and sync callbacks
                const result = onConfirm();
                if (result && typeof result.then === 'function') {
                    result.catch(err => console.error('Confirm action failed:', err));
                }
            }
        };
    }

    async function resumeQueued(id) {
        // Optimistically update status locally
        const it = findItemInCache(id);
        if (it) {
            it.paused = false;
            it.status = 'queued';
            const el = document.querySelector(`#items [data-id="${id}"]`);
            if (el) renderItemRow(el, it);
        }
        filterView();
        showToastStyled('Resumed');
        try { await fetch('/resume/' + id, {method: 'POST'}); } catch (e) { console.warn('resume failed', e); }
    }

    async function retryItem(id) {
        // Optimistically set status to queued locally to make UI reflect change
        const it = findItemInCache(id);
        if (it) {
            delete it.error;
            delete it.last_output;
            it.paused = false;
            it.status = 'queued';
            it.added_at = Math.floor(Date.now() / 1000);
            const el = document.querySelector(`#items [data-id="${id}"]`);
            if (el) renderItemRow(el, it);
        }
        filterView();
        showToastStyled('Retry queued');
        try { await fetch('/retry_error/' + id, {method: 'POST'}); } catch (e) { console.warn('retry failed', e); }
    }

    async function sendToJDItem(id) {
        try {
            const r = await fetch('/jdownloader/send/' + id, {method: 'POST'});
            const j = await r.json();
            if (!r.ok) {
                showToastStyled('JDownloader2: ' + (j.error || 'failed'), 'error');
                return;
            }
            showToastStyled('Sent to JDownloader2');
        } catch (e) {
            showToastStyled('JDownloader2: request failed', 'error');
        }
    }

    async function cancelItem(id) {
        // Optimistically set status to queued locally to make UI reflect change
        const it = findItemInCache(id);
        if (it) {
            delete it.error;
            delete it.last_output;
            it.paused = true;
            it.status = 'queued';
            it.added_at = Math.floor(Date.now() / 1000);
            const el = document.querySelector(`#items [data-id="${id}"]`);
            if (el) renderItemRow(el, it);
        }
        filterView();
        showToastStyled('Cancel queued');
        try { await fetch('/cancel/' + id, {method: 'POST'}); } catch (e) { console.warn('cancel failed', e); }
    }

    // Settings UI
    const openSettingsBtn = document.getElementById('openSettings');
    const settingsOverlay = document.getElementById('settingsOverlay');
    const settingsModal = document.getElementById('settingsModal');
    const settingsClose = document.getElementById('settingsClose');
    const settingsContent = document.getElementById('settingsContent');
    const settingsContentWrapper = document.getElementById('settingsContentWrapper');
    const settingsTabGeneral = document.getElementById('settingsTabGeneral');
    const settingsTabDomains = document.getElementById('settingsTabDomains');
    const settingsTabJDownloader = document.getElementById('settingsTabJDownloader');
    const saveSettingsBtn = document.getElementById('saveSettings');

    let settingsCache = null;   // will hold the last fetched settings from server
    let settingsDirty = false;  // tracks unsaved changes

    function markDirty() {
        settingsDirty = true;
    }

    function setActiveSettingsTab(tabBtn) {
        settingsTabGeneral.classList.remove('active');
        settingsTabDomains.classList.remove('active');
        settingsTabJDownloader.classList.remove('active');
        tabBtn.classList.add('active');
    }

    // helper to attach change listeners inside modal to detect unsaved changes
    function attachDirtyListeners(container) {
        container.querySelectorAll('input, textarea, select').forEach(el => {
            if (!el._dirtyAttached) {
                el.addEventListener('input', markDirty);
                el._dirtyAttached = true;
            }
        });
    }

    openSettingsBtn.onclick = async () => {
        settingsOverlay.style.display = 'flex';
        // set stable height to avoid resizing when swapping content
        settingsContent.style.minHeight = '30vh';
        // load current config
        const r = await fetch('/settings');
        const cfg = await r.json();
        settingsCache = cfg;
        settingsDirty = false;
        renderSettingsGeneral(cfg);
        setActiveSettingsTab(settingsTabGeneral);
    };


    settingsClose.onclick = () => {
        if (settingsDirty) {
            openConfirmModal('Settings have unsaved changes. Save now? OK = Save, Cancel = Discard', async () => {
                saveSettingsBtn.click();
                return;
            });
            settingsOverlay.style.display = 'none';
            settingsDirty = false;
            return;
        }
        settingsOverlay.style.display = 'none';
    };

    settingsTabGeneral.onclick = async () => {
        setActiveSettingsTab(settingsTabGeneral);
        // reuse cached config if available
        const cfg = settingsCache || (await (await fetch('/settings')).json());
        renderSettingsGeneral(cfg);
    };

    settingsTabDomains.onclick = async () => {
        setActiveSettingsTab(settingsTabDomains);
        const cfg = settingsCache || (await (await fetch('/settings')).json());
        renderSettingsDomains(cfg);
    };

    settingsTabJDownloader.onclick = async () => {
        setActiveSettingsTab(settingsTabJDownloader);
        const cfg = settingsCache || (await (await fetch('/settings')).json());
        renderSettingsJDownloader(cfg);
    };

    function IsNumeric(val) {
        return Number(parseFloat(val)) == val;
    }

    function renderSettingsGeneral(cfg) {
        settingsContent.innerHTML = '';
        const container = document.createElement('div');
        // show fields with nice labels and merged yt-dlp path+binary
        const fields = [
            {key: 'port', label: 'Web UI Port (1–65535)'},
            {key: 'concurrent_downloads_global', label: 'Concurrent Downloads Global (>=1)'},
            {key: 'concurrent_downloads_per_domain', label: 'Concurrent Downloads Per Domain (>=1)'},
            {key: 'yt_dlp_path', label: 'yt-dlp Path (includes binary)'},
            {key: 'yt_dlp_global_args', label: 'yt-dlp Global Arguments'},
            {key: 'start_paused', label: 'Start paused'},
            {key: 'new_urls_paused', label: 'New URLs added paused'},
            {key: 'download_favicons', label: 'Download favicons'}
        ];

        fields.forEach(f => {
            const row = document.createElement('div');
            row.style.marginBottom = '6px';
            const label = document.createElement('label');
            label.textContent = f.label;
            label.style.fontWeight = '500';
            label.style.display = 'flex';
            label.style.alignItems = 'center';
            label.style.gap = '6px';

            let input;
            const val = cfg[f.key];
            if (typeof val === 'boolean') {
                input = document.createElement('input');
                input.type = 'checkbox';
                input.checked = !!val;
                input.className = 'toggle';
                label.prepend(input);
                row.appendChild(label);
                container.appendChild(row);
                return;
            } else if (Array.isArray(val)) {
                input = document.createElement('textarea');
                input.value = (val || []).join('\n');
                input.rows = 3;
            } else {
                input = document.createElement('input');
                input.type = 'text';
                input.value = val === undefined ? '' : String(val);
            }
            input.id = 'cfg_' + f.key;
            row.appendChild(label);
            row.appendChild(input);
            container.appendChild(row);
        });
        settingsContent.appendChild(container);

        // attach dirty listeners
        attachDirtyListeners(settingsContent);

        fetch('/status').then(r => r.json()).then(status => {
            const inDocker = status.in_docker;
            // Hide port field in settings if running inside Docker
            if (inDocker) {
                const portRow = document.querySelector('#cfg_port')?.closest('div');
                if (portRow) portRow.style.display = 'none';
            }
        });
    }

    function renderSettingsDomains(cfg) {
        settingsContent.innerHTML = '';
        const container = document.createElement('div');

        // default config block (permanent, non-deletable)
        const defaultBlock = document.createElement('div');
        defaultBlock.style.border = '1px solid #eee';
        defaultBlock.style.padding = '8px';
        defaultBlock.style.marginTop = '8px';
        const defaultLabel = document.createElement('div');
        defaultLabel.style.fontWeight = '600';
        defaultLabel.textContent = 'yt-dlp default config (applies when no domain-specific config exists)';
        const defaultTextarea = document.createElement('textarea');
        defaultTextarea.style.height = '180px';
        // attempt to load existing default.conf via fetching the server settings cache
        const defaultPathContents = (function () {
            // populate with contents from cfg if we stored it previously
            // server will not normally send full file contents in /settings, so try to fetch default.conf
            return '';
        })();
        // try to read default.conf from conf/default.conf by asking a special endpoint (not present) - fallback to empty
        // load potential default from cfg._default_conf (if server provided it in /settings)
        if (cfg._default_conf) defaultTextarea.value = cfg._default_conf;
        defaultTextarea.id = 'domain_default_conf';
        defaultBlock.appendChild(defaultLabel);
        defaultBlock.appendChild(defaultTextarea);
        container.appendChild(defaultBlock);

        const info = document.createElement('div');
        info.className = 'smallnote';
        info.style.marginTop = '8px';
        info.textContent = 'Configure domain-specific yt-dlp args. Use comma-separated domains as the key. Example: \"youtube.com, youtu.be\" &#8594; \"--format best\"';
        container.appendChild(info);

        const list = document.createElement('div');
        list.id = 'domainList';
        list.style.marginTop = '8px';
        const overrides = cfg.domain_overrides || {};
        Object.keys(overrides).forEach(k => {
            const block = createDomainBlock(k, overrides[k]);
            list.appendChild(block);
        });
        const addBtn = document.createElement('button');
        addBtn.textContent = 'Add domain override';
        addBtn.style.marginTop = '8px';
        addBtn.onclick = () => {
            const b = createDomainBlock('', '');
            list.appendChild(b);
            attachDirtyListeners(list);
        };
        container.appendChild(list);
        container.appendChild(addBtn);
        settingsContent.appendChild(container);

        // attach dirty listeners
        attachDirtyListeners(settingsContent);
    }

    function createDomainBlock(domains, args) {
        const wrapper = document.createElement('div');
        wrapper.style.border = '1px solid #eee';
        wrapper.style.padding = '8px';
        wrapper.style.marginTop = '8px';
        const dIn = document.createElement('input');
        dIn.type = 'text';
        dIn.value = domains;
        dIn.placeholder = 'comma separated domains';
        dIn.style.width = 'calc(100% - 16px)';
        const aIn = document.createElement('textarea');
        aIn.rows = 3;
        aIn.value = args;
        aIn.placeholder = 'yt-dlp args free text';
        aIn.style.width = 'calc(100% - 16px)';
        const del = document.createElement('button');
        del.textContent = 'Delete';
        del.onclick = () => {
            wrapper.remove();
            markDirty();
        };
        wrapper.appendChild(dIn);
        wrapper.appendChild(aIn);
        wrapper.appendChild(del);
        return wrapper;
    }

    function renderSettingsJDownloader(cfg) {
        settingsContent.innerHTML = '';
        const container = document.createElement('div');
        const jd = cfg.jdownloader || {};

        function checkboxRow(id, text, checked) {
            const row = document.createElement('div');
            row.style.marginBottom = '6px';
            const label = document.createElement('label');
            label.style.display = 'flex';
            label.style.alignItems = 'center';
            label.style.gap = '6px';
            label.style.fontWeight = '500';
            const input = document.createElement('input');
            input.type = 'checkbox';
            input.id = id;
            input.className = 'toggle';
            input.checked = !!checked;
            label.appendChild(input);
            label.appendChild(document.createTextNode(text));
            row.appendChild(label);
            container.appendChild(row);
            return input;
        }

        function textRow(id, labelText, value, type) {
            const row = document.createElement('div');
            row.style.marginBottom = '6px';
            const label = document.createElement('label');
            label.textContent = labelText;
            label.style.fontWeight = '500';
            label.style.display = 'block';
            const input = document.createElement('input');
            input.type = type || 'text';
            input.id = id;
            input.value = value || '';
            row.appendChild(label);
            row.appendChild(input);
            container.appendChild(row);
            return input;
        }

        function selectRow(id, labelText, options, selectedValue) {
            const row = document.createElement('div');
            row.style.marginBottom = '6px';
            const label = document.createElement('label');
            label.textContent = labelText;
            label.style.fontWeight = '500';
            label.style.display = 'block';
            const select = document.createElement('select');
            select.id = id;
            options.forEach(([value, text]) => {
                const opt = document.createElement('option');
                opt.value = value;
                opt.textContent = text;
                if (value === selectedValue) opt.selected = true;
                select.appendChild(opt);
            });
            row.appendChild(label);
            row.appendChild(select);
            container.appendChild(row);
            return select;
        }

        checkboxRow('jd_enabled', 'Enable JDownloader2 backup', jd.enabled);
        checkboxRow('jd_auto_send_errors', 'Automatically send failed downloads to JDownloader2', jd.auto_send_errors);
        selectRow('jd_resolution_preference', 'Preferred resolution (for links offering multiple resolutions)', [
            ['all', 'All resolutions (download every version)'],
            ['lowest', 'Lowest available'],
            ['480p', '480p'],
            ['720p', '720p'],
            ['1080p', '1080p'],
            ['2160p', '2160p'],
            ['highest', 'Highest available'],
        ], jd.resolution_preference || 'highest');
        const emailInput = textRow('jd_email', 'My.JDownloader Email', jd.email, 'text');
        const passInput = textRow('jd_password', 'My.JDownloader Password', jd.password, 'password');

        // test connection / list devices
        const testRow = document.createElement('div');
        testRow.style.marginBottom = '6px';
        const testBtn = document.createElement('button');
        testBtn.type = 'button';
        testBtn.id = 'jd_test_btn';
        testBtn.textContent = 'Test connection / List devices';
        const statusMsg = document.createElement('span');
        statusMsg.id = 'jd_status_msg';
        statusMsg.className = 'smallnote';
        statusMsg.style.marginLeft = '8px';
        testRow.appendChild(testBtn);
        testRow.appendChild(statusMsg);
        container.appendChild(testRow);

        // target device select
        const deviceRow = document.createElement('div');
        deviceRow.style.marginTop = '6px';
        const deviceLabel = document.createElement('label');
        deviceLabel.textContent = 'Target JDownloader device';
        deviceLabel.style.fontWeight = '500';
        deviceLabel.style.display = 'block';
        const deviceSelect = document.createElement('select');
        deviceSelect.id = 'jd_device_id';
        if (jd.device_id) {
            const opt = document.createElement('option');
            opt.value = jd.device_id;
            opt.textContent = jd.device_name || jd.device_id;
            opt.dataset.name = jd.device_name || '';
            opt.selected = true;
            deviceSelect.appendChild(opt);
        }
        deviceRow.appendChild(deviceLabel);
        deviceRow.appendChild(deviceSelect);
        container.appendChild(deviceRow);

        settingsContent.appendChild(container);
        attachDirtyListeners(settingsContent);

        testBtn.onclick = async () => {
            const email = emailInput.value.trim();
            const password = passInput.value;
            if (!email || !password) {
                statusMsg.textContent = 'Enter email and password first';
                statusMsg.style.color = 'crimson';
                return;
            }
            statusMsg.textContent = 'Connecting...';
            statusMsg.style.color = '';
            testBtn.disabled = true;
            try {
                const r = await fetch('/jdownloader/devices', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({email, password})
                });
                const j = await r.json();
                if (!r.ok) {
                    statusMsg.textContent = j.error || 'Connection failed';
                    statusMsg.style.color = 'crimson';
                    return;
                }
                const devices = j.devices || [];
                const prevSelected = deviceSelect.value;
                deviceSelect.innerHTML = '';
                devices.forEach(d => {
                    const opt = document.createElement('option');
                    opt.value = d.id;
                    opt.textContent = d.name || d.id;
                    opt.dataset.name = d.name || '';
                    deviceSelect.appendChild(opt);
                });
                const toSelect = prevSelected || jd.device_id;
                if (toSelect) {
                    const match = Array.from(deviceSelect.options).find(o => o.value === toSelect);
                    if (match) match.selected = true;
                }
                statusMsg.textContent = devices.length ? `Connected. Found ${devices.length} device(s).` : 'Connected, but no devices found.';
                statusMsg.style.color = devices.length ? 'green' : 'crimson';
                markDirty();
            } catch (e) {
                statusMsg.textContent = 'Connection failed: ' + e;
                statusMsg.style.color = 'crimson';
            } finally {
                testBtn.disabled = false;
            }
        };
    }

    saveSettingsBtn.onclick = async () => {
        // gather general
        const newCfg = {};
        const fields = ['port', 'concurrent_downloads_global', 'concurrent_downloads_per_domain', 'yt_dlp_global_args', 'start_paused', 'new_urls_paused', 'download_favicons', 'yt_dlp_path'];
        fields.forEach(f => {
            const el = document.getElementById('cfg_' + f);
            if (!el) return;
            if (el.type === 'checkbox') newCfg[f] = el.checked;
            else if (el.tagName.toLowerCase() === 'textarea') newCfg[f] = el.value.split('\n').map(s => s.trim()).filter(Boolean);
            else if (f === 'port' || f === 'concurrent_downloads_global' || f === 'concurrent_downloads_per_domain') newCfg[f] = parseInt(el.value, 10) || 0;
            else newCfg[f] = el.value;
        });

        // gather domains if domainList exists; otherwise preserve existing overrides from cache
        const domMap = {};
        const domainListEl = document.querySelector('#domainList');
        if (domainListEl) {
            const domainBlocks = Array.from(document.querySelectorAll('#domainList > div'));
            domainBlocks.forEach(b => {
                const inputs = b.querySelectorAll('input, textarea');
                if (inputs.length >= 2) {
                    const k = inputs[0].value.trim();
                    const v = inputs[1].value.trim();
                    if (k) domMap[k] = v;
                }
            });
        } else {
            // preserve previously loaded domain overrides (if any)
            if (settingsCache && settingsCache.domain_overrides) {
                Object.assign(domMap, settingsCache.domain_overrides);
            }
        }

        // gather default.conf textarea (if present)
        const defaultConfEl = document.getElementById('domain_default_conf');
        const defaultConfText = defaultConfEl ? defaultConfEl.value : null;

        const payload = {general: newCfg, domain_overrides: domMap, default_conf: defaultConfText};

        // gather jdownloader settings if that tab has been rendered
        const jdEnabledEl = document.getElementById('jd_enabled');
        if (jdEnabledEl) {
            const deviceSelectEl = document.getElementById('jd_device_id');
            const selectedOption = deviceSelectEl ? deviceSelectEl.selectedOptions[0] : null;
            payload.jdownloader = {
                enabled: jdEnabledEl.checked,
                auto_send_errors: document.getElementById('jd_auto_send_errors').checked,
                resolution_preference: document.getElementById('jd_resolution_preference').value,
                email: document.getElementById('jd_email').value.trim(),
                password: document.getElementById('jd_password').value,
                device_id: selectedOption ? selectedOption.value : '',
                device_name: selectedOption ? (selectedOption.dataset.name || selectedOption.textContent || '') : '',
            };
        }
        const r = await fetch('/save_settings', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        if (r.ok) {
            showToastStyled('Settings saved');
            settingsOverlay.style.display = 'none';
            settingsDirty = false;
            setTimeout(() => location.reload(), 500);
        } else {
            showToastStyled('Failed saving settings', 'error');
        }
    };

    // track changes inside settings overlay to set dirty flag when user types or deletes
    settingsOverlay.addEventListener('input', () => {
        settingsDirty = true;
    });

    // prevent body scroll while settings modal is open (fix overscroll/wheel passing to background)
    openSettingsBtn.addEventListener('click', () => {
        document.body.style.overflow = 'hidden';
    });
    settingsClose.addEventListener('click', () => {
        document.body.style.overflow = '';
    });
    settingsOverlay.addEventListener('click', e => {
        if (e.target === settingsOverlay) {
            document.body.style.overflow = '';
        }
    });

    // hamburger menu toggling
    const hamBtn = document.getElementById('hamburgerBtn');
    const hamMenu = document.getElementById('hamburgerMenu');
    const closeHamburgerMenu = () => {
        if (hamMenu) hamMenu.style.display = 'none';
    };
    if (hamBtn) {
        hamBtn.addEventListener('click', () => {
            hamMenu.style.display = hamMenu.style.display === 'flex' ? 'none' : 'flex';
        });
        // close menu once any action button is chosen (helps mobile/touch interaction)
        hamMenu.addEventListener('click', (e) => {
            if (e.target && e.target.tagName === 'BUTTON') closeHamburgerMenu();
        });
        // close if clicked outside
        document.addEventListener('click', (e) => {
            if (!hamBtn.contains(e.target) && !hamMenu.contains(e.target)) {
                closeHamburgerMenu();
            }
        });
    }

    // warn before page unload if settings are dirty
    window.addEventListener('beforeunload', (e) => {
        if (settingsDirty) {
            e.preventDefault();
            e.returnValue = '';
        }
    });

    // attach SSE and initial filter
    attachSSE();
    setTimeout(() => filterView(), 300);

    // Update modal wiring
    document.getElementById('updateClose').onclick = () => document.getElementById('updateOverlay').style.display = 'none';
    document.getElementById('updateCancel').onclick = () => document.getElementById('updateOverlay').style.display = 'none';
    document.getElementById('updateConfirm').onclick = async () => {
        document.getElementById('updateOverlay').style.display = 'none';
        // replace link with downloading text + spinner
        const area = document.getElementById('updateArea');
        if (area) area.innerHTML = `&nbsp;|&nbsp; Downloading update... <span class="spinner"></span>`;
        try {
            const r = await fetch('/update_ytdlp', {method: 'POST'});
            const j = await r.json();
            if (j.status === 'success') {
                showToastStyled('yt-dlp updated to version ' + (j.version || 'unknown'));
                setTimeout(() => location.reload(), 1200);
            } else {
                showToastStyled('Update failed: ' + (j.message || 'unknown error'), 'error');
                setTimeout(() => location.reload(), 1200);
            }
        } catch (e) {
            showToastStyled('Download failed: ' + e, 'error');
            setTimeout(() => location.reload(), 1200);
        }
    };

</script>
</body>
</html>
"""

# write index.html (overwrite on upgrade)
INDEX_HTML_PATH.write_text(INDEX_HTML)


# === FastAPI routes ===

@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML_PATH.read_text()


@app.head("/", include_in_schema=False)
async def index_head():
    return PlainTextResponse("", status_code=200)


@app.get("/favicon.ico")
async def app_favicon():
    return FileResponse("favicon.ico")


@app.get("/apple-touch-icon.png")
async def app_apple_touch_icon():
    return FileResponse("apple-touch-icon.png")


@app.get("/logo.png")
async def app_logo():
    return FileResponse("logo.png")


@app.get('/status', response_class=JSONResponse)
def status():
    jd_cfg = CONFIG.get("jdownloader") or {}
    jd_configured = bool(
        jd_cfg.get("enabled") and jd_cfg.get("email") and jd_cfg.get("password") and jd_cfg.get("device_id")
    )
    return {
        "in_docker": is_docker(),
        "update_available": UPDATE_AVAILABLE,
        "pause_all": pause_all_flag,
        "jdownloader_configured": jd_configured,
    }


@app.post('/pause_all', response_class=JSONResponse)
async def set_pause_all(body: Optional[Dict[str, Any]] = None):
    global pause_all_flag
    body = body or {}
    if "paused" in body:
        pause_all_flag = bool(body.get("paused"))
    else:
        pause_all_flag = not pause_all_flag
    await publish_state()
    return {"pause_all": pause_all_flag}


@app.get("/events")
async def events():
    q: asyncio.Queue = asyncio.Queue()
    subscribers.append(q)

    # immediately send initial state on connect
    initial_state = json.dumps({
        "queue": list(QUEUE_STATE.values()),
        "pause_all": pause_all_flag
    })

    async def event_generator():
        try:
            yield {"event": "message", "data": initial_state}
            while True:
                data = await q.get()
                yield {"event": "message", "data": data}
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        finally:
            try:
                subscribers.remove(q)
            except Exception:
                pass

    return EventSourceResponse(event_generator())


# log fetch endpoint
@app.get('/log/{id_}')
async def get_log(id_: str):
    logfile = LOGS_DIR / f"{id_}.log"
    if not logfile.exists():
        raise HTTPException(404, 'log not found')
    return PlainTextResponse(logfile.read_text(errors='ignore'))


# log stream endpoint
@app.get('/log/stream/{id_}')
async def stream_log(id_: str):
    """
    SSE endpoint that streams new lines from the given log file.
    Sends the full current file initially then yields appended text as it's written.
    """
    logfile = LOGS_DIR / f"{id_}.log"
    if not logfile.exists():
        raise HTTPException(404, 'log not found')

    async def generator():
        try:
            # send full file initially
            try:
                txt = logfile.read_text(errors='ignore')
            except Exception:
                txt = ''
            if txt:
                yield {"event": "message", "data": json.dumps({"chunk": txt})}

            # tail new content
            pos = logfile.stat().st_size
            while True:
                try:
                    current_size = logfile.stat().st_size
                    if current_size > pos:
                        with logfile.open('r', errors='ignore') as fh:
                            fh.seek(pos)
                            new = fh.read()
                            pos = fh.tell()
                            if new:
                                yield {"event": "message", "data": json.dumps({"chunk": new})}
                    await asyncio.sleep(0.4)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await asyncio.sleep(0.8)
        finally:
            return

    return EventSourceResponse(generator())


@app.post("/clear")
async def clear_statuses(body: Dict[str, Any]):
    statuses = body.get("statuses", [])
    if not statuses:
        raise HTTPException(400, "no statuses specified")
    removed = []
    for id_, item in list(QUEUE_STATE.items()):
        if item.get("status") in statuses:
            QUEUE_STATE.pop(id_, None)
            lf = LOGS_DIR / f"{id_}.log"
            if lf.exists():
                try:
                    lf.unlink()
                except Exception:
                    pass
            removed.append(id_)
    await persist_and_publish()
    return {"cleared": removed, "statuses": statuses}


@app.post("/bulk_pause_resume")
async def bulk_pause_resume(body: Dict[str, Any]):
    ids = body.get("ids", [])
    pause = bool(body.get("pause", True))
    for id_ in ids:
        it = QUEUE_STATE.get(id_)
        if not it:
            continue
        it["paused"] = pause
        it["status"] = "paused" if pause else "queued"
        QUEUE_STATE[id_] = it
    await persist_and_publish()
    return {"updated": ids, "selected entries paused": pause}


@app.post('/retry_all_errors')
async def retry_all_errors():
    # Re-queue any QUEUE_STATE items that are in an error-like state
    requeued = []
    for id_, it in list(QUEUE_STATE.items()):
        if it.get('status') in ('error', 'stalled', 'cancelled'):
            it.pop('error', None)
            it.pop('last_output', None)
            it['paused'] = False
            it['status'] = 'queued'
            it['added_at'] = int(time.time())
            QUEUE_STATE[id_] = it
            requeued.append(id_)
    await persist_and_publish()
    return JSONResponse({'all errors re-queued': requeued})

@app.post('/retry_all_dupes')
async def retry_all_dupes():
    # Re-queue any QUEUE_STATE items that are duplicates
    requeued = []
    for id_, it in list(QUEUE_STATE.items()):
        if it.get('status') in ('duplicate'):
            it.pop('error', None)
            it.pop('last_output', None)
            it['paused'] = False
            it['status'] = 'queued'
            it['added_at'] = int(time.time())
            QUEUE_STATE[id_] = it
            requeued.append(id_)
    await persist_and_publish()
    return JSONResponse({'all duplicates re-queued': requeued})

# settings endpoints
@app.get('/settings')
async def get_settings():
    """Return full settings, including contents of default.conf if present."""
    data = CONFIG.copy()
    # normalize concurrency keys for UI/backward-compat
    data['concurrent_downloads_global'] = _to_pos_int(
        data.get('concurrent_downloads_global', data.get('concurrent_downloads', 10)),
        10
    )
    data['concurrent_downloads_per_domain'] = _to_pos_int(data.get('concurrent_downloads_per_domain', 2), 2)
    data['jdownloader'] = {**DEFAULT_CONFIG['jdownloader'], **(data.get('jdownloader') or {})}
    try:
        default_conf_path = YTDLP_CONFIG_FOLDER / "default.conf"
        if default_conf_path.exists():
            data["_default_conf"] = default_conf_path.read_text()
        else:
            data["_default_conf"] = ""
    except Exception:
        data["_default_conf"] = ""
    return JSONResponse(data)


@app.post('/save_settings')
async def save_settings(body: Dict[str, Any]):
    gen = body.get('general', {})
    doms = body.get('domain_overrides', {})
    default_conf_text = body.get('default_conf', "")
    try:
        raw = json.loads(CONFIG_PATH.read_text())
    except Exception:
        raw = DEFAULT_CONFIG.copy()
    # apply general
    for k, v in gen.items():
        raw[k] = v
    raw['concurrent_downloads_global'] = _to_pos_int(
        raw.get('concurrent_downloads_global', raw.get('concurrent_downloads', 10)),
        10
    )
    raw['concurrent_downloads_per_domain'] = _to_pos_int(raw.get('concurrent_downloads_per_domain', 2), 2)
    # keep legacy key in sync for older installations/tools
    raw['concurrent_downloads'] = raw['concurrent_downloads_global']
    # apply domain overrides only if provided (do not wipe existing if not provided)
    # doms will be {} if client explicitly sent an empty map; that's expected.
    if 'domain_overrides' in body:
        raw['domain_overrides'] = doms
    # apply jdownloader settings only if provided (preserve existing otherwise)
    if 'jdownloader' in body:
        jd = body.get('jdownloader') or {}
        resolution_pref = str(jd.get('resolution_preference', '') or JD_DEFAULT_RESOLUTION_PREFERENCE)
        if resolution_pref not in JD_RESOLUTION_OPTIONS:
            resolution_pref = JD_DEFAULT_RESOLUTION_PREFERENCE
        raw['jdownloader'] = {
            'enabled': bool(jd.get('enabled', False)),
            'email': str(jd.get('email', '') or '').strip(),
            'password': str(jd.get('password', '') or ''),
            'device_id': str(jd.get('device_id', '') or ''),
            'device_name': str(jd.get('device_name', '') or ''),
            'auto_send_errors': bool(jd.get('auto_send_errors', False)),
            'resolution_preference': resolution_pref,
        }
    # persist the default.conf if provided (this makes the UI default section behave like conf/default.conf)
    try:
        if default_conf_text != "":
            try:
                default_conf_path = YTDLP_CONFIG_FOLDER / "default.conf"
                # if empty text -> remove default.conf file
                if default_conf_text.strip() == "":
                    if default_conf_path.exists():
                        default_conf_path.unlink()
                else:
                    default_conf_path.write_text(default_conf_text)
            except Exception:
                pass

        # persist full config JSON
        CONFIG_PATH.write_text(json.dumps(raw, indent=2))
        # update runtime vars
        global YT_DLP_BINARY, YT_DLP_GLOBAL_ARGS, CONCURRENT_DOWNLOADS_GLOBAL, CONCURRENT_DOWNLOADS_PER_DOMAIN, START_PAUSED, NEW_URLS_PAUSED, DOWNLOAD_FAVICONS, DOMAIN_OVERRIDES, YT_DLP_PATH, CONFIG
        YT_DLP_GLOBAL_ARGS = raw.get('yt_dlp_global_args', YT_DLP_GLOBAL_ARGS) or []
        CONCURRENT_DOWNLOADS_GLOBAL = _to_pos_int(
            raw.get('concurrent_downloads_global', raw.get('concurrent_downloads', CONCURRENT_DOWNLOADS_GLOBAL)),
            CONCURRENT_DOWNLOADS_GLOBAL
        )
        CONCURRENT_DOWNLOADS_PER_DOMAIN = _to_pos_int(
            raw.get('concurrent_downloads_per_domain', CONCURRENT_DOWNLOADS_PER_DOMAIN),
            CONCURRENT_DOWNLOADS_PER_DOMAIN
        )
        START_PAUSED = bool(raw.get('start_paused', START_PAUSED))
        NEW_URLS_PAUSED = bool(raw.get('new_urls_paused', NEW_URLS_PAUSED))
        DOWNLOAD_FAVICONS = bool(raw.get('download_favicons', DOWNLOAD_FAVICONS))
        DOMAIN_OVERRIDES = raw.get('domain_overrides', {}) or {}
        YT_DLP_PATH = Path(raw.get('yt_dlp_path', str(YT_DLP_PATH)))
        # save in-memory CONFIG
        CONFIG = raw
        await persist_and_publish()
        return JSONResponse({'saved': True})
    except Exception as e:
        return JSONResponse({'saved': False, 'error': str(e)}, status_code=500)


# === JDownloader2 endpoints ===
@app.post('/jdownloader/devices')
async def jdownloader_devices(body: Dict[str, Any]):
    """Connect to My.JDownloader with the given credentials and return the account's devices."""
    email = (body.get('email') or '').strip()
    password = body.get('password') or ''
    if not email or not password:
        return JSONResponse({'error': 'Email and password are required'}, status_code=400)
    try:
        async with JDownloaderClient(email, password) as client:
            await client.connect()
            devices = await client.list_devices()
        return JSONResponse({'devices': [
            {'id': d.get('id'), 'name': d.get('name'), 'type': d.get('type')} for d in devices
        ]})
    except JDownloaderError as e:
        return JSONResponse({'error': str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=502)


@app.post('/jdownloader/send_errors')
async def jdownloader_send_errors():
    """Send all entries with an error status to JDownloader2's link collector,
    start the download there, and remove them from the Tube-Q queue."""
    error_ids = [id_ for id_, it in QUEUE_STATE.items()
                  if it.get('status') == 'error' and it.get('url')]
    try:
        result = await send_queue_ids_to_jdownloader(error_ids)
    except JDownloaderError as e:
        return JSONResponse({'error': str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=502)
    return JSONResponse(result)


@app.post('/jdownloader/send/{id_}')
async def jdownloader_send_one(id_: str):
    """Send a single error item to JDownloader2's link collector, start the
    download there, and remove it from the Tube-Q queue."""
    it = QUEUE_STATE.get(id_)
    if not it or it.get('status') != 'error' or not it.get('url'):
        raise HTTPException(404, 'not found')
    try:
        result = await send_queue_ids_to_jdownloader([id_])
    except JDownloaderError as e:
        return JSONResponse({'error': str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=502)
    return JSONResponse(result)


# add endpoint
@app.post('/add')
async def add_urls(request: Request, paused: Optional[bool] = Query(False)):
    body = await request.body()
    text = body.decode('utf-8', errors='ignore')
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    added = 0
    duplicates = 0
    for url in lines:
        uid = make_id(url)
        # if in queue, ignore
        if any(item.get('url') == url for item in QUEUE_STATE.values()):
            duplicates += 1
            append_url_attempt(url, "duplicate_in_queue", uid=uid)
            continue
        # if in history, add as duplicate for option to redownload it
        if url in HISTORY:
            duplicates += 1
            # do not await favicon fetch here (fast return). set placeholder; fetch async
            QUEUE_STATE[uid] = {"id": uid, "url": url, "favicon": "_generic.ico", "status": "duplicate", "added_at": int(time.time())}
            append_url_attempt(url, "duplicate_in_history", uid=uid)
            # spawn favicon fetch in background
            if DOWNLOAD_FAVICONS:
                asyncio.create_task(_bg_fetch_favicon_and_update(uid, url, 'duplicates'))
            continue
        # new item: add to regular queue, placeholder favicon for immediate response
        item = {"id": uid, "url": url, "domain": get_domain(url), 'favicon': "_generic.ico",
                "added_at": int(time.time()), "paused": bool(paused or NEW_URLS_PAUSED), "status": "queued",}
        QUEUE_STATE[uid] = item
        append_url_attempt(url, "queued_paused" if item.get("paused") else "queued", uid=uid)
        # spawn favicon fetch in background without blocking response
        if DOWNLOAD_FAVICONS:
            asyncio.create_task(_bg_fetch_favicon_and_update(uid, url, 'queue'))
        added += 1
    await persist_and_publish()
    return JSONResponse({'added': added, 'duplicates': duplicates})


# background favicon updater (fire-and-forget)
async def _bg_fetch_favicon_and_update(uid: str, url: str, target: str):
    try:
        favicon = await ensure_favicon_for_url(url)
        # update unified QUEUE_STATE in-place
        it = QUEUE_STATE.get(uid)
        if it:
            it['favicon'] = favicon
            QUEUE_STATE[uid] = it
        elif target == 'duplicates':
            # duplicates are represented as QUEUE_STATE entries with status='duplicate'
            du = QUEUE_STATE.get(uid)
            if du:
                du['favicon'] = favicon
                QUEUE_STATE[uid] = du
        await persist_and_publish()
    except Exception:
        # ignore background errors
        pass


# remove entry
@app.post('/remove_entry/{id_}')
async def remove_entry(id_: str):
    removed = False
    # remove from unified state
    if id_ in QUEUE_STATE:
        try:
            QUEUE_STATE.pop(id_)
            removed = True
        except Exception:
            removed = False
    # stop running process if present
    rr = DOWNLOADS.pop(id_, None)
    if rr and rr.get("process"):
        kill_process_tree(rr["process"])
    logf = LOGS_DIR / f"{id_}.log"
    if logf.exists():
        try:
            logf.unlink()
        except:
            pass
    await persist_and_publish()
    return JSONResponse({'removed': removed})


# resume queued
@app.post('/resume/{id_}')
async def resume_item(id_: str):
    it = QUEUE_STATE.get(id_)
    if not it:
        raise HTTPException(404, 'not found')
    it['paused'] = False
    # set queued status if was stalled/paused
    if it.get('status') in ('stalled', 'paused', None):
        it['status'] = 'queued'
    QUEUE_STATE[id_] = it
    await persist_and_publish()
    return JSONResponse({'resumed': True})


# retry error
@app.post('/retry_error/{id_}')
async def retry_error(id_: str):
    it = QUEUE_STATE.get(id_)
    if not it:
        # maybe it's in DOWNLOADS or was a transient: try to pop and re-queue
        entry = None
        rr = DOWNLOADS.pop(id_, None)
        if rr:
            entry = rr.get("item")
        if not entry:
            raise HTTPException(404, 'not found')
        it = entry
    # clear error info and re-queue
    it.pop('error', None)
    it.pop('last_output', None)
    it['paused'] = False
    it['status'] = 'queued'
    it['added_at'] = int(time.time())
    QUEUE_STATE[it['id']] = it
    await persist_and_publish()
    return JSONResponse({'queued': True})


# bulk actions
@app.post('/bulk_remove')
async def bulk_remove(body: Dict[str, Any]):
    ids = body.get('ids', [])
    for id_ in ids:
        if id_ in QUEUE_STATE:
            try:
                QUEUE_STATE.pop(id_)
            except:
                pass
        lf = LOGS_DIR / f"{id_}.log"
        if lf.exists():
            try:
                lf.unlink()
            except:
                pass
        rr = DOWNLOADS.pop(id_, None)
        if rr and rr.get("process"):
            kill_process_tree(rr["process"])
    await persist_and_publish()
    return JSONResponse({'removed': ids})


@app.post('/bulk_retry')
async def bulk_retry(body: Dict[str, Any]):
    ids = body.get('ids', [])
    for id_ in ids:
        it = QUEUE_STATE.get(id_)
        if not it:
            rr = DOWNLOADS.pop(id_, None)
            if rr:
                it = rr.get("item")
        if not it:
            # nothing to retry
            continue
        it.pop('error', None)
        it.pop('last_output', None)
        it['paused'] = False
        it['status'] = 'queued'
        it['added_at'] = int(time.time())
        QUEUE_STATE[it['id']] = it
    await persist_and_publish()
    return JSONResponse({'retried': ids})



@app.post('/cancel/{id_}')
async def cancel_download(id_: str):
    # cancel a currently running download
    rr = DOWNLOADS.get(id_)
    if not rr:
        raise HTTPException(404, 'not running')
    proc = rr.get("process")
    if proc:
        try:
            kill_process_tree(proc)
        except Exception as e:
            raise HTTPException(500, f'failed to kill process: {e}')
    # mark as cancelled in unified state
    it = QUEUE_STATE.get(id_) or (rr.get("item") if rr else None)
    if it:
        it['status'] = 'cancelled'
        it['error'] = 'cancelled by user'
        QUEUE_STATE[id_] = it
    # remove download record
    DOWNLOADS.pop(id_, None)
    await persist_and_publish()
    return JSONResponse({'cancelled': id_})


@app.get('/dump_errors')
async def dump_errors():
    urls = [it.get("url") for it in QUEUE_STATE.values() if it.get("status") == "error" or it.get("status") == "stalled" or it.get("status") == "cancelled"]
    return JSONResponse({"urls": urls})


@app.get('/dump_duplicates')
async def dump_duplicates():
    urls = [it.get("url") for it in QUEUE_STATE.values() if it.get("status") == "duplicate"]
    return JSONResponse({'urls': urls})


@app.get('/version')
async def version():
    global LOCAL_YTDLP_VERSION, LATEST_YTDLP_VERSION, UPDATE_AVAILABLE, LATEST_TUBEQ_VERSION, TUBEQ_UPDATE_AVAILABLE
    # these do blocking subprocess/HTTP I/O (GitHub API calls can each take
    # seconds); run them off the event loop so a cache-miss here doesn't
    # stall every other request (SSE progress, queue actions, ...) for
    # everyone connected while both checks run
    local_version = await asyncio.to_thread(get_yt_dlp_version)
    latest_version = await asyncio.to_thread(check_latest_ytdlp_version_once_daily)
    latest_tubeq = await asyncio.to_thread(check_latest_tubeq_version_once_daily)
    LOCAL_YTDLP_VERSION = local_version
    LATEST_YTDLP_VERSION = latest_version
    UPDATE_AVAILABLE = is_update_available(local_version, latest_version)
    LATEST_TUBEQ_VERSION = latest_tubeq
    TUBEQ_UPDATE_AVAILABLE = is_strict_newer_version(APP_VERSION, latest_tubeq)
    return JSONResponse({'yt_dlp_version': local_version, 'app_version': APP_VERSION,
                         'latest_ytdlp': latest_version,
                         'update_available': UPDATE_AVAILABLE,
                         'latest_tubeq': latest_tubeq,
                         'tubeq_update_available': TUBEQ_UPDATE_AVAILABLE})


@app.get('/health')
async def health_check():
    return JSONResponse(content={"status": "healthy"}, status_code=200)


# update endpoint: download latest platform-specific binary and replace (backup to .old)
@app.post('/update_ytdlp')
async def update_ytdlp():
    try:
        system = platform.system().lower()
        binary_name = 'yt-dlp.exe' if 'windows' in system else 'yt-dlp'
        url = f'https://github.com/yt-dlp/yt-dlp/releases/latest/download/{binary_name}'
        dest = CONF_DIR / binary_name
        backup = CONF_DIR / (binary_name + '.old')

        # remove existing backup
        if backup.exists():
            try:
                backup.unlink()
            except Exception:
                pass
        # backup current binary
        if dest.exists():
            try:
                dest.rename(backup)
            except Exception:
                # best effort: try to remove then move
                try:
                    dest.unlink()
                except Exception:
                    pass
        # download new binary
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read()
            dest.write_bytes(data)

        # make executable on unix
        try:
            dest.chmod(dest.stat().st_mode | stat.S_IEXEC)
        except Exception:
            pass

        # update config path and persist
        CONFIG['yt_dlp_path'] = str(dest)
        CONFIG['yt_dlp_latest'] = check_latest_ytdlp_version_once_daily()
        CONFIG_PATH.write_text(json.dumps(CONFIG, indent=2))

        # update in-memory path
        global YT_DLP_PATH, LOCAL_YTDLP_VERSION, LATEST_YTDLP_VERSION, UPDATE_AVAILABLE
        YT_DLP_PATH = Path(CONFIG['yt_dlp_path'])
        LOCAL_YTDLP_VERSION = get_local_ytdlp_version()
        LATEST_YTDLP_VERSION = check_latest_ytdlp_version_once_daily()
        UPDATE_AVAILABLE = is_update_available(LOCAL_YTDLP_VERSION, LATEST_YTDLP_VERSION)

        await publish_state()
        return JSONResponse({'status': 'success', 'version': LATEST_YTDLP_VERSION})
    except Exception as e:
        # on error, try to restore backup if exists and dest is missing/corrupt
        try:
            if 'backup' in locals() and backup.exists():
                backup.rename(dest)
        except Exception:
            pass
        return JSONResponse({'status': 'error', 'message': str(e)}, status_code=500)


# CLI add mode
def cli_add_mode(arg_text: str):
    urls = [l.strip() for l in arg_text.splitlines() if l.strip()]
    if not urls:
        print('no urls')
        return
    try:
        q = json.loads(QUEUE_PATH.read_text())
        if not isinstance(q, dict):
            q = {}
    except Exception:
        q = {}
    try:
        h = json.loads(HISTORY_PATH.read_text())
        if not isinstance(h, list):
            h = []
    except Exception:
        h = []
    added = 0
    for url in urls:
        uid = make_id(url)
        if any((it or {}).get("url") == url for it in q.values()):
            print('duplicate:', url)
            append_url_attempt(url, "duplicate_in_queue", source="cli", uid=uid)
            continue
        if url in h:
            print('duplicate:', url)
            append_url_attempt(url, "duplicate_in_history", source="cli", uid=uid)
            continue
        q[uid] = (
            {'id': uid, 'url': url, 'domain': get_domain(url), 'added_at': int(time.time()), 'paused': NEW_URLS_PAUSED,
             'favicon': '_generic.ico'})
        append_url_attempt(url, "queued_paused" if NEW_URLS_PAUSED else "queued", source="cli", uid=uid)
        added += 1
    QUEUE_PATH.write_text(json.dumps(q, indent=2))
    print('added', added)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--add', '-a', help='Add URL(s) (newline separated) and exit', default=None)
    args = parser.parse_args()
    if args.add:
        cli_add_mode(args.add)
        sys.exit(0)
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    log_config = copy.deepcopy(LOGGING_CONFIG)
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s %(levelprefix)s %(message)s"
    log_config["formatters"]["default"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    log_config["formatters"]["access"]["fmt"] = '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"

    print(f"-=-=-=-=-=-=-=-=-=-=-=")
    print(f"Tube-Q v{APP_VERSION}")
    print(f"-=-=-=-=-=-=-=-=-=-=-=")
    uvicorn.run(app, host='0.0.0.0', port=int(CONFIG.get('port', 7090)), log_config=log_config)
