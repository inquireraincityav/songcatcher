#!/usr/bin/env python3
"""Songcatcher daemon.

Local audio-fetching service:
  - HTTP API on 127.0.0.1:7878 (menu bar + CLI talk to it)
  - Optional Telegram bot long-poller (mobile dispatch)
  - SQLite-backed priority queue with SIGSTOP/SIGCONT preemption
  - yt-dlp + ffmpeg pipeline producing 320 kbps MP3s in ~/Desktop/Music
"""

from __future__ import annotations

import dataclasses
import enum
import fcntl
import heapq
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


# --- Paths & constants --------------------------------------------------------

HOME = Path(os.environ["HOME"])
APP_DIR = HOME / "Library" / "Application Support" / "Songcatcher"
LOG_DIR = HOME / "Library" / "Logs" / "Songcatcher"
MUSIC_DIR = HOME / "Desktop" / "Music"
WORK_DIR = APP_DIR / "work"
DB_PATH = APP_DIR / "state.db"
TELEGRAM_CONFIG_PATH = APP_DIR / "telegram.json"

HTTP_HOST = "127.0.0.1"
HTTP_PORT = 7878
WATCHDOG_RESUME_TIMEOUT = 30  # seconds
PREEMPTION_POLL_INTERVAL = 0.5


# --- Logging ------------------------------------------------------------------

for d in (APP_DIR, LOG_DIR, MUSIC_DIR, WORK_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "daemon.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("songcatcher")

jobs_log_path = LOG_DIR / "jobs.log"


def log_job_event(event: dict):
    event["ts"] = time.time()
    with jobs_log_path.open("a") as f:
        f.write(json.dumps(event) + "\n")


# --- Job model ----------------------------------------------------------------


class JobState(str, enum.Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclasses.dataclass
class Job:
    id: str
    input: str
    important: bool
    enqueued_at: float
    state: JobState = JobState.QUEUED
    title: Optional[str] = None
    output_path: Optional[str] = None
    error: Optional[str] = None
    telegram_chat_id: Optional[int] = None
    telegram_message_id: Optional[int] = None
    # Playlist fan-out: parent jobs expand into children. Children inherit context.
    is_playlist_parent: bool = False
    parent_id: Optional[str] = None
    playlist_index: Optional[int] = None  # 1-based position within parent playlist
    playlist_total: Optional[int] = None  # total track count for status display
    output_dir: Optional[str] = None  # override of MUSIC_DIR for this job's final file
    split_as_set: bool = False  # treat input as DJ set: download whole then split into tracks

    @property
    def priority_key(self) -> tuple:
        return (0 if self.important else 1, self.enqueued_at)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "input": self.input,
            "important": self.important,
            "enqueued_at": self.enqueued_at,
            "state": self.state.value,
            "title": self.title,
            "output_path": self.output_path,
            "error": self.error,
            "is_playlist_parent": self.is_playlist_parent,
            "parent_id": self.parent_id,
            "playlist_index": self.playlist_index,
            "playlist_total": self.playlist_total,
            "split_as_set": self.split_as_set,
        }


# --- Persistence --------------------------------------------------------------


class Persistence:
    SCHEMA_COLUMNS = [
        ("id", "TEXT PRIMARY KEY"),
        ("input", "TEXT NOT NULL"),
        ("important", "INTEGER NOT NULL"),
        ("enqueued_at", "REAL NOT NULL"),
        ("state", "TEXT NOT NULL"),
        ("title", "TEXT"),
        ("output_path", "TEXT"),
        ("error", "TEXT"),
        ("telegram_chat_id", "INTEGER"),
        ("telegram_message_id", "INTEGER"),
        ("is_playlist_parent", "INTEGER DEFAULT 0"),
        ("parent_id", "TEXT"),
        ("playlist_index", "INTEGER"),
        ("playlist_total", "INTEGER"),
        ("output_dir", "TEXT"),
        ("split_as_set", "INTEGER DEFAULT 0"),
    ]

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        col_defs = ", ".join(f"{c} {t}" for c, t in self.SCHEMA_COLUMNS)
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS jobs ({col_defs})")
        # Idempotent migrations for older DBs missing new columns
        existing = {row[1] for row in self._conn.execute("PRAGMA table_info(jobs)")}
        for col, typ in self.SCHEMA_COLUMNS:
            if col not in existing and col != "id":
                try:
                    self._conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typ}")
                except sqlite3.OperationalError as e:
                    log.warning(f"ALTER TABLE failed for {col}: {e}")
        self._conn.commit()

    def upsert(self, job: Job):
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO jobs (id, input, important, enqueued_at, state, title,
                                  output_path, error, telegram_chat_id, telegram_message_id,
                                  is_playlist_parent, parent_id, playlist_index,
                                  playlist_total, output_dir, split_as_set)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    state=excluded.state,
                    title=excluded.title,
                    output_path=excluded.output_path,
                    error=excluded.error
                """,
                (
                    job.id,
                    job.input,
                    int(job.important),
                    job.enqueued_at,
                    job.state.value,
                    job.title,
                    job.output_path,
                    job.error,
                    job.telegram_chat_id,
                    job.telegram_message_id,
                    int(job.is_playlist_parent),
                    job.parent_id,
                    job.playlist_index,
                    job.playlist_total,
                    job.output_dir,
                    int(job.split_as_set),
                ),
            )
            self._conn.commit()

    def load_unfinished(self) -> list[Job]:
        with self._lock:
            cur = self._conn.execute(
                """SELECT id, input, important, enqueued_at, state, title, output_path,
                          error, telegram_chat_id, telegram_message_id,
                          is_playlist_parent, parent_id, playlist_index,
                          playlist_total, output_dir, split_as_set
                   FROM jobs
                   WHERE state IN ('queued', 'downloading', 'paused')"""
            )
            rows = cur.fetchall()
        jobs = []
        for r in rows:
            job = Job(
                id=r[0],
                input=r[1],
                important=bool(r[2]),
                enqueued_at=r[3],
                state=JobState.QUEUED,  # reset in-flight back to queued on restart
                title=r[5],
                output_path=r[6],
                error=r[7],
                telegram_chat_id=r[8],
                telegram_message_id=r[9],
                is_playlist_parent=bool(r[10]),
                parent_id=r[11],
                playlist_index=r[12],
                playlist_total=r[13],
                output_dir=r[14],
                split_as_set=bool(r[15]),
            )
            jobs.append(job)
        return jobs


# --- Priority queue -----------------------------------------------------------


class JobQueue:
    """Min-heap priority queue keyed by (priority, enqueued_at)."""

    def __init__(self, persistence: Persistence):
        self._persistence = persistence
        self._heap: list[tuple[tuple, str]] = []
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        for job in persistence.load_unfinished():
            self._jobs[job.id] = job
            heapq.heappush(self._heap, (job.priority_key, job.id))
        if self._heap:
            self._has_work.set()

    def add(self, job: Job):
        with self._lock:
            self._jobs[job.id] = job
            heapq.heappush(self._heap, (job.priority_key, job.id))
            self._has_work.set()
        self._persistence.upsert(job)

    def pop_next(self) -> Optional[Job]:
        with self._lock:
            while self._heap:
                _, jid = heapq.heappop(self._heap)
                job = self._jobs.get(jid)
                if job and job.state == JobState.QUEUED:
                    return job
            self._has_work.clear()
            return None

    def peek_important_pending(self, exclude_id: str) -> Optional[Job]:
        """Return any QUEUED important job other than the in-flight, without removing it."""
        with self._lock:
            for _, jid in self._heap:
                if jid == exclude_id:
                    continue
                job = self._jobs.get(jid)
                if job and job.important and job.state == JobState.QUEUED:
                    return job
            return None

    def remove(self, job_id: str) -> bool:
        """Remove a job from the heap if present and queued."""
        with self._lock:
            for i, (_, jid) in enumerate(self._heap):
                if jid == job_id:
                    self._heap.pop(i)
                    heapq.heapify(self._heap)
                    job = self._jobs.get(jid)
                    if job:
                        job.state = JobState.CANCELLED
                        self._persistence.upsert(job)
                    return True
            return False

    def requeue_head(self, job: Job):
        """Re-add a job that was in-flight, at the head of its priority lane."""
        with self._lock:
            # Bump enqueued_at slightly into the past so it sorts ahead of newer items.
            job.enqueued_at = min(
                job.enqueued_at,
                min((k[1] for k, _ in self._heap), default=time.time()) - 0.001,
            )
            job.state = JobState.QUEUED
            self._jobs[job.id] = job
            heapq.heappush(self._heap, (job.priority_key, job.id))
            self._has_work.set()
        self._persistence.upsert(job)

    def snapshot(self) -> list[Job]:
        with self._lock:
            return [
                self._jobs[jid] for _, jid in sorted(self._heap) if jid in self._jobs
            ]

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def wait_for_work(self, timeout: float = 1.0) -> bool:
        return self._has_work.wait(timeout)


# --- Input classification & metadata resolution -------------------------------


SPOTIFY_TRACK_RE = re.compile(r"open\.spotify\.com/(?:intl-[a-z]{2}/)?track/")
SPOTIFY_OTHER_RE = re.compile(
    r"open\.spotify\.com/(?:intl-[a-z]{2}/)?(?P<kind>playlist|album|episode|show|artist)/"
)
SPOTIFY_PLAYLIST_RE = re.compile(
    r"open\.spotify\.com/(?:intl-[a-z]{2}/)?playlist/([A-Za-z0-9]+)"
)
SPOTIFY_ALBUM_RE = re.compile(
    r"open\.spotify\.com/(?:intl-[a-z]{2}/)?album/([A-Za-z0-9]+)"
)
YT_PLAYLIST_ONLY_RE = re.compile(
    r"(?:music\.|www\.)?youtube\.com/playlist\?", re.IGNORECASE
)
# yt-dlp-native playlist-like URLs (YouTube playlist, SoundCloud sets, Bandcamp albums)
YT_DLP_PLAYLIST_RE = re.compile(
    r"("
    r"youtube\.com/playlist\?"
    r"|soundcloud\.com/[^/]+/sets/"
    r"|bandcamp\.com/album/"
    r")",
    re.IGNORECASE,
)


def is_url(text: str) -> bool:
    try:
        p = urlparse(text.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def is_spotify_track(text: str) -> bool:
    return bool(SPOTIFY_TRACK_RE.search(text))


def spotify_unsupported_kind(text: str) -> Optional[str]:
    m = SPOTIFY_OTHER_RE.search(text)
    return m.group("kind") if m else None


def resolve_spotify_query(url: str) -> Optional[str]:
    """Build a 'Title Artist' search query from a Spotify track URL."""
    # Normalize intl-xx variants so oEmbed accepts the URL
    url_clean = re.sub(
        r"open\.spotify\.com/intl-[a-z]{2}/", "open.spotify.com/", url
    )
    try:
        req = Request(
            f"https://open.spotify.com/oembed?url={url_clean}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(req, timeout=10) as resp:
            oembed = json.loads(resp.read())
        title = oembed.get("title", "")
    except Exception as e:
        log.warning(f"Spotify oEmbed failed: {e}")
        return None

    artist = None
    try:
        req = Request(url_clean, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        m = re.search(
            r'<meta name="music:musician_description" content="([^"]+)"', html
        )
        if m:
            artist = m.group(1)
    except Exception as e:
        log.warning(f"Spotify HTML scrape failed: {e}")

    if title and artist:
        return f"{title} {artist}"
    return title or None


def classify_input(user_input: str) -> str:
    """Return a yt-dlp target for a single-track input: a URL or a ytsearch1: query.

    Raises RuntimeError with a user-friendly message for unsupported inputs.
    Caller is responsible for detecting playlists before calling this.
    """
    text = user_input.strip()

    # Spotify: track → resolve via oEmbed; other unsupported kinds → friendly rejection
    if is_spotify_track(text):
        query = resolve_spotify_query(text)
        if not query:
            raise RuntimeError("Could not resolve Spotify track metadata")
        return f"ytsearch1:{query}"

    kind = spotify_unsupported_kind(text)
    if kind in ("episode", "show"):
        raise RuntimeError("Podcasts and shows aren't supported — only music tracks.")
    if kind == "artist":
        raise RuntimeError(
            "Artist pages have no specific track — send a track URL instead."
        )

    if is_url(text):
        return text
    return f"ytsearch1:{text}"


# --- Playlist detection and expansion -----------------------------------------


def is_playlist_input(text: str) -> bool:
    """True if the input should be expanded into multiple track jobs."""
    t = text.strip()
    if SPOTIFY_PLAYLIST_RE.search(t) or SPOTIFY_ALBUM_RE.search(t):
        return True
    if YT_DLP_PLAYLIST_RE.search(t):
        return True
    return False


@dataclasses.dataclass
class PlaylistEntry:
    """One item inside an expanded playlist. Either url OR query is set."""

    title: str  # display name for prefix and logs
    url: Optional[str] = None  # direct URL (YouTube/SoundCloud/Bandcamp)
    query: Optional[str] = None  # ytsearch1: query (Spotify track→YT)

    def as_input(self) -> str:
        return self.url if self.url else (self.query or self.title)


def expand_spotify_playlist(playlist_url: str) -> tuple[str, list[PlaylistEntry]]:
    """Scrape the Spotify embed page for the track list."""
    m = SPOTIFY_PLAYLIST_RE.search(playlist_url) or SPOTIFY_ALBUM_RE.search(playlist_url)
    if not m:
        raise RuntimeError("Not a recognized Spotify playlist/album URL")
    kind = "playlist" if SPOTIFY_PLAYLIST_RE.search(playlist_url) else "album"
    pid = m.group(1)
    embed_url = f"https://open.spotify.com/embed/{kind}/{pid}"
    req = Request(embed_url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as resp:
        html = resp.read().decode("utf-8", errors="ignore")
    nd = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html, re.DOTALL
    )
    if not nd:
        raise RuntimeError("Could not parse Spotify embed page (missing __NEXT_DATA__)")
    data = json.loads(nd.group(1))
    entity = (
        data.get("props", {})
        .get("pageProps", {})
        .get("state", {})
        .get("data", {})
        .get("entity", {})
    )
    name = entity.get("name") or "Spotify Playlist"
    tracks = entity.get("trackList") or []
    entries: list[PlaylistEntry] = []
    for t in tracks:
        title = t.get("title") or ""
        artist = t.get("subtitle") or ""
        if not title:
            continue
        query = f"{title} {artist}".strip()
        entries.append(
            PlaylistEntry(
                title=f"{title} - {artist}" if artist else title,
                query=query,  # classify_input will add ytsearch1: prefix
            )
        )
    if not entries:
        raise RuntimeError(f"Spotify {kind} '{name}' has no readable tracks")
    return name, entries


def expand_ytdlp_playlist(playlist_url: str) -> tuple[str, list[PlaylistEntry]]:
    """Use yt-dlp --flat-playlist -J to enumerate playlist entries."""
    cmd = [
        "yt-dlp",
        "-J",
        "--no-warnings",
        "--flat-playlist",
        playlist_url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp playlist probe failed: {result.stderr.strip()[:200]}")
    info = json.loads(result.stdout)
    name = info.get("title") or info.get("playlist_title") or "Untitled Playlist"
    raw_entries = info.get("entries") or []
    entries: list[PlaylistEntry] = []
    for e in raw_entries:
        if not e:
            continue
        url = e.get("url") or e.get("webpage_url")
        title = e.get("title") or "Untitled"
        if url:
            entries.append(PlaylistEntry(title=title, url=url))
    if not entries:
        raise RuntimeError(f"Playlist '{name}' has no entries")
    return name, entries


def expand_playlist(playlist_url: str) -> tuple[str, list[PlaylistEntry]]:
    """Dispatch to the right expander based on URL type."""
    t = playlist_url.strip()
    if SPOTIFY_PLAYLIST_RE.search(t) or SPOTIFY_ALBUM_RE.search(t):
        return expand_spotify_playlist(t)
    if YT_DLP_PLAYLIST_RE.search(t):
        return expand_ytdlp_playlist(t)
    raise RuntimeError("Not a recognized playlist URL")


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[/\\:*?"<>|\x00-\x1f]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return (name[:120] or "untitled").rstrip(". ")


def probe_info(target: str) -> dict:
    """Run yt-dlp -J once and return the canonical entry dict.

    Single source of truth for metadata: title, duration, chapters, description.
    Raises with a friendly message for live streams.
    """
    cmd = [
        "yt-dlp",
        "-J",
        "--no-warnings",
        "--no-playlist",
        "--default-search",
        "ytsearch1:",
        target,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    if result.returncode != 0:
        raise RuntimeError(f"Metadata probe failed: {result.stderr.strip()[:200]}")
    info = json.loads(result.stdout)
    if info.get("_type") == "playlist":
        entries = info.get("entries") or []
        if not entries:
            raise RuntimeError("Search returned no results")
        info = entries[0]
    if info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming"):
        raise RuntimeError(
            "Live streams and scheduled premieres can't be downloaded as MP3 files. "
            "Wait until the stream ends and the recording is published."
        )
    return info


def probe_title(target: str) -> str:
    """Pull a clean filename-ready title from the metadata probe."""
    info = probe_info(target)
    title = info.get("title") or info.get("fulltitle") or "untitled"
    artist = info.get("artist") or info.get("uploader")
    if artist and artist.lower() not in title.lower():
        return f"{title} - {artist}"
    return title


def unique_path(base_dir: Path, filename: str) -> Path:
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    candidate = base_dir / filename
    counter = 2
    while candidate.exists():
        candidate = base_dir / f"{stem} ({counter}){suffix}"
        counter += 1
    return candidate


# --- DJ-set splitting helpers -------------------------------------------------


SHAZAM_HELPER = APP_DIR / "shazam_helper.py"
SHAZAM_VENV_PYTHON = APP_DIR / "shazam_venv" / "bin" / "python"
SHAZAM_WINDOW_SEC = 30.0
SHAZAM_HOP_SEC = 30.0
SHAZAM_RATE_LIMIT_SLEEP = 1.0  # between calls, to avoid hammering Shazam
UNIDENTIFIED_MIN_GAP_SEC = 45.0  # gaps shorter than this aren't worth a file

TRACKLISTS_BASE = "https://www.1001tracklists.com"
TRACKLISTS_CACHE_DIR = APP_DIR / "cache" / "1001tracklists"
TRACKLISTS_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)
TRACKLISTS_MIN_REQUEST_INTERVAL = 3.0  # seconds between hits to 1001tracklists.com
_tracklists_last_request = 0.0
_tracklists_request_lock = threading.Lock()

# Timestamp like "0:00", "01:23", "1:23:45" — required at line start (after optional brackets)
TIMESTAMP_LINE_RE = re.compile(
    r"^\s*[\[\(]?\s*(?P<time>(?:\d+:)?\d{1,2}:\d{2})\s*[\]\)]?[\s\-\–\—\|.:]*(?P<title>.+?)\s*$"
)

# Numbered tracklist line like "01. Artist - Title" or "1) Artist - Title"
# Captures the position number and the remaining text as the track entry.
NUMBERED_TRACK_RE = re.compile(
    r"^\s*(?P<pos>\d{1,3})\s*[.):\]]\s*(?P<entry>\S.{1,}?)\s*$"
)


def parse_timestamp(t: str) -> int:
    """Convert 'H:MM:SS' or 'M:SS' to total seconds."""
    parts = t.split(":")
    if len(parts) == 2:
        m, s = parts
        return int(m) * 60 + int(s)
    h, m, s = parts
    return int(h) * 3600 + int(m) * 60 + int(s)


@dataclasses.dataclass
class Segment:
    start: float
    end: float
    title: str
    artist: Optional[str] = None
    confidence: str = "unknown"  # "chapter" | "description" | "shazam" | "unknown"

    def display_title(self) -> str:
        if self.artist:
            return f"{self.title} - {self.artist}"
        return self.title


_POSITION_PREFIX_RE = re.compile(r"^\s*\d+\)\.?\s*")


_TITLE_ARTIST_SEP_RE = re.compile(r"\s+[-–—]\s+")  # hyphen, en dash, or em dash
# Parentheticals that almost always belong on the TITLE side:
_TITLE_MARKERS_RE = re.compile(
    r"\([^)]*?\b("
    r"mix|remix|edit|version|dub|remaster(?:ed)?|"
    r"instrumental|extended|acoustic|live|original|"
    r"radio edit|club mix|vocal mix|"
    # DJ-edit nomenclature common on DJ-pool and white-label uploads
    r"mashup|bootleg|vip|rework|flip|refix|reedit|re-edit|"
    r"intro edit|outro edit|short edit|clean|dirty"
    r")\b[^)]*?\)",
    re.IGNORECASE,
)
# Markers that follow the ARTIST ("featuring …"):
_FEAT_RE = re.compile(r"\b(?:feat|ft|featuring)\.?\s+", re.IGNORECASE)


def _split_title_artist(raw: str) -> tuple[str, Optional[str]]:
    """Split a 'Title - Artist' or 'Artist - Title' string into separate fields.

    Accepts hyphen, en dash (–), or em dash (—). Uses content markers to decide
    which side is which:
      - '(Remix)' / '(Remastered)' / '(Original Mix)' etc. → that side is TITLE
      - 'feat.' / 'ft.' → that side is ARTIST
    If we have NO strong signal, we DO NOT split — the whole string becomes the
    title and artist stays None. This avoids guessing wrong on uploads where the
    convention is reversed (e.g. some DJs use "Title – Artist", most use "Artist
    – Title"). Position prefixes ('1).') are stripped either way.
    Returns (title, artist), artist may be None.
    """
    raw = raw.strip()
    parts = _TITLE_ARTIST_SEP_RE.split(raw, maxsplit=1)
    if len(parts) < 2:
        return _POSITION_PREFIX_RE.sub("", raw).strip(), None
    left = _POSITION_PREFIX_RE.sub("", parts[0]).strip()
    right = _POSITION_PREFIX_RE.sub("", parts[1]).strip()

    left_marker = bool(_TITLE_MARKERS_RE.search(left))
    right_marker = bool(_TITLE_MARKERS_RE.search(right))
    left_feat = bool(_FEAT_RE.search(left))
    right_feat = bool(_FEAT_RE.search(right))

    score = 0
    if left_marker:
        score += 1
    if right_marker:
        score -= 1
    if left_feat:
        score -= 1  # feat. on left → left is artist
    if right_feat:
        score += 1  # feat. on right → right is artist
    if score > 0:
        return left, right  # Title – Artist
    if score < 0:
        return right, left  # Artist – Title
    # Ambiguous: keep the full string as the title, no artist guess.
    return _POSITION_PREFIX_RE.sub("", raw).strip(), None


def extract_chapter_segments(info: dict) -> list[Segment]:
    """Pull tracklist from yt-dlp chapter markers."""
    chapters = info.get("chapters") or []
    out: list[Segment] = []
    for ch in chapters:
        title_raw = (ch.get("title") or "").strip()
        if not title_raw:
            continue
        start = float(ch.get("start_time") or 0)
        end = float(ch.get("end_time") or 0)
        if end <= start:
            continue
        title, artist = _split_title_artist(title_raw)
        out.append(
            Segment(start=start, end=end, title=title, artist=artist, confidence="chapter")
        )
    return out


SET_TITLE_HINTS = re.compile(
    r"\b("
    r"dj\s*set|live\s*set|live\s*@|"
    r"mixtape|megamix|dj\s*mix|mix\s*compilation|"
    r"essential\s*mix|boiler\s*room|cercle\s*\b"
    r")\b",
    re.IGNORECASE,
)


def should_auto_set_split(info: dict) -> bool:
    """Decide whether to auto-trigger set-splitting based on metadata signals.

    Conservative defaults to avoid chopping a single long song:
      - 4+ chapter markers AND duration >= 20 min → yes
      - 4+ description-timestamp lines AND duration >= 20 min → yes
      - Title strongly indicates a DJ set ("DJ Set", "Mixtape", etc.)
        AND duration >= 20 min → yes
      - Otherwise → no (caller treats as a single track)
    """
    duration = float(info.get("duration") or 0)
    if duration < 20 * 60:
        return False
    chapters = info.get("chapters") or []
    if len(chapters) >= 4:
        # Reject section markers (e.g. "Intro / Verse / Chorus / Bridge")
        titles = [(c.get("title") or "").lower() for c in chapters]
        section_words = {"intro", "outro", "verse", "chorus", "bridge", "hook", "break"}
        if not all(any(w in t for w in section_words) for t in titles):
            return True
    desc_segs = extract_description_segments(info.get("description") or "", duration)
    if len(desc_segs) >= 4:
        return True
    title = info.get("title") or ""
    if SET_TITLE_HINTS.search(title):
        return True
    return False


def extract_description_numbered_tracklist(
    description: str, duration: float
) -> list[Segment]:
    """Parse a numbered description tracklist (no timestamps).

    Matches lines like '01. Artist - Title' or '1) Artist - Title'. Filters out
    sub-entries like 'Mashed with …' that don't start with a number. Boundaries
    are unknown — segments get uniformly distributed across `duration` so the
    mix-slice fallback in _run_set_job has approximate positions. The real value
    of this parser is feeding the originals-first sourcing path with track names.
    """
    if not description or duration <= 0:
        return []
    found: list[tuple[int, str]] = []
    for line in description.splitlines():
        m = NUMBERED_TRACK_RE.match(line)
        if not m:
            continue
        pos = int(m.group("pos"))
        entry = m.group("entry").strip()
        # Reject obvious non-tracks: phone numbers, dates, links
        if entry.startswith(("http://", "https://")) or len(entry) < 4:
            continue
        # Reject sub-entries that are just "Mashed with …" / "Featuring …" etc.
        if re.match(r"^(?:mashed|featuring|feat\.?|ft\.?|with)\b", entry, re.IGNORECASE):
            continue
        found.append((pos, entry))
    if len(found) < 3:
        return []
    # Sort by position number (descriptions often number them sequentially)
    found.sort(key=lambda x: x[0])
    # Reject if positions aren't a roughly monotonic 1..N sequence
    positions = [p for p, _ in found]
    if positions != sorted(set(positions)) or positions[0] > 2 or positions[-1] > len(found) + 5:
        return []
    # Distribute uniformly across the mix duration
    n = len(found)
    segment_len = duration / n
    out: list[Segment] = []
    for i, (_pos, entry) in enumerate(found):
        start = i * segment_len
        end = (i + 1) * segment_len
        title, artist = _split_title_artist(entry)
        out.append(
            Segment(
                start=start,
                end=end,
                title=title,
                artist=artist,
                confidence="description_numbered",
            )
        )
    return out


def extract_description_segments(
    description: str, duration: float
) -> list[Segment]:
    """Parse a YouTube description for `MM:SS Title` lines."""
    if not description or duration <= 0:
        return []
    matches: list[tuple[int, str]] = []
    for line in description.splitlines():
        m = TIMESTAMP_LINE_RE.match(line)
        if not m:
            continue
        try:
            seconds = parse_timestamp(m.group("time"))
        except ValueError:
            continue
        if seconds < 0 or seconds > duration:
            continue
        title = m.group("title").strip(" -–—|.:")
        if len(title) < 2:
            continue
        matches.append((seconds, title))
    if len(matches) < 2:
        return []
    # Must be monotonically increasing (mostly) — discard otherwise
    sorted_matches = sorted(matches, key=lambda x: x[0])
    if [m[0] for m in matches] != [m[0] for m in sorted_matches]:
        matches = sorted_matches
    # Dedupe consecutive same-timestamp entries
    deduped = []
    for ts, title in matches:
        if not deduped or deduped[-1][0] != ts:
            deduped.append((ts, title))
    if len(deduped) < 2:
        return []
    out: list[Segment] = []
    for i, (start, raw_title) in enumerate(deduped):
        end = deduped[i + 1][0] if i + 1 < len(deduped) else duration
        title, artist = _split_title_artist(raw_title)
        out.append(
            Segment(
                start=float(start),
                end=float(end),
                title=title,
                artist=artist,
                confidence="description",
            )
        )
    return out


def _tracklists_rate_limited_get(url: str, method: str = "GET", data: Optional[bytes] = None) -> Optional[str]:
    """Throttled fetch of a 1001tracklists URL. Returns HTML text, or None on failure.

    Honors a global min-interval rate limit so we don't hammer the site.
    Uses a realistic browser User-Agent (the site's robots.txt blocks `CPython`).
    """
    global _tracklists_last_request
    with _tracklists_request_lock:
        elapsed = time.time() - _tracklists_last_request
        if elapsed < TRACKLISTS_MIN_REQUEST_INTERVAL:
            time.sleep(TRACKLISTS_MIN_REQUEST_INTERVAL - elapsed)
        _tracklists_last_request = time.time()
    try:
        req = Request(
            url,
            data=data,
            method=method,
            headers={
                "User-Agent": TRACKLISTS_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "identity",
            },
        )
        with urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        if "Cloudflare" in body and "Checking your browser" in body:
            log.warning("1001tracklists: CloudFlare challenge served, scrape blocked")
            return None
        return body
    except Exception as e:
        log.warning(f"1001tracklists fetch failed: {e}")
        return None


def _normalize_search_query(title: str) -> str:
    """Strip noisy words from a set title before searching 1001tracklists."""
    # Remove common YouTube-style noise
    noise = re.compile(
        r"\b(official|hd|hq|4k|video|audio|full set|live|mix|set|"
        r"feat\.?|ft\.?|prod\.?|edition|version)\b",
        re.IGNORECASE,
    )
    cleaned = noise.sub(" ", title)
    cleaned = re.sub(r"[\[\](){}|]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def find_1001_tracklist(source_title: str) -> Optional[str]:
    """Search 1001tracklists for a tracklist matching the set's title.

    Returns the full tracklist URL on a probable hit, else None.
    """
    query = _normalize_search_query(source_title)
    if len(query) < 5:
        return None
    # 1001tracklists's search uses a POST to /search/result.php
    data = (
        f"main_search={query.replace(' ', '+')}"
        f"&search_selection=9"  # 9 = Tracklists
    ).encode("utf-8")
    html = _tracklists_rate_limited_get(
        f"{TRACKLISTS_BASE}/search/result.php", method="POST", data=data
    )
    if not html:
        return None
    # First /tracklist/<id>/<slug>.html href in the response body
    m = re.search(r'href="(/tracklist/[A-Za-z0-9_]+/[^"]+)"', html)
    if not m:
        log.info(f"1001tracklists: no results for query '{query}'")
        return None
    return f"{TRACKLISTS_BASE}{m.group(1)}"


def _parse_1001_tracklist_html(html: str, duration: float) -> list[Segment]:
    """Parse a 1001tracklists tracklist page for segments with cue times.

    The site's HTML changes frequently. We try multiple patterns; brittle by
    nature. Returns [] on no match so the caller can fall through to Shazam.
    """
    segments: list[Segment] = []

    # Strategy 1: JSON-LD structured data (most stable when present)
    for ld_match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.+?)</script>',
        html,
        re.DOTALL,
    ):
        try:
            data = json.loads(ld_match.group(1).strip())
        except json.JSONDecodeError:
            continue
        items = data.get("track") or data.get("itemListElement") or []
        if not isinstance(items, list):
            continue
        # JSON-LD often has track names + maybe positions but rarely cue times
        # — fall through to regex-extracted times below
        # (We don't return early; we collect titles to merge with times.)

    # Strategy 2: tlpItem rows with cue times
    # Pattern: ... data-trno="N" ... [HH:MM[:SS]] ... <span class="trackValue">Artist - Track</span>
    item_re = re.compile(
        r'data-trno="(?P<idx>\d+)"[^<]*?'
        r'(?:.*?\[?(?P<time>\d+:\d{1,2}(?::\d{1,2})?)\]?)?'
        r'.*?(?:trackValue|track_v|tlT)[^>]*>\s*(?P<title>[^<]+)',
        re.DOTALL,
    )
    matches = list(item_re.finditer(html))
    if matches:
        cues: list[tuple[Optional[float], str]] = []
        for m in matches:
            ts_raw = m.group("time")
            ts = None
            if ts_raw:
                try:
                    ts = float(parse_timestamp(ts_raw))
                except ValueError:
                    ts = None
            title_raw = re.sub(r"\s+", " ", m.group("title")).strip()
            if title_raw:
                cues.append((ts, title_raw))
        # Fill in missing timestamps by linear interpolation only if we have
        # at least two anchors; otherwise drop unanchored cues.
        anchored = [(i, ts, title) for i, (ts, title) in enumerate(cues) if ts is not None]
        if len(anchored) >= 2:
            cleaned: list[tuple[float, str]] = []
            for i, (ts, title) in enumerate(cues):
                if ts is not None:
                    cleaned.append((ts, title))
            for i, (start, raw_title) in enumerate(cleaned):
                end = cleaned[i + 1][0] if i + 1 < len(cleaned) else duration
                if end <= start:
                    continue
                title, artist = _split_title_artist(raw_title)
                segments.append(
                    Segment(
                        start=float(start),
                        end=float(end),
                        title=title,
                        artist=artist,
                        confidence="1001tracklists",
                    )
                )
    return segments


def fetch_1001_tracklist(tracklist_url: str, duration: float) -> list[Segment]:
    """Fetch a tracklist page (with disk cache) and parse it into Segments."""
    TRACKLISTS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    import hashlib

    cache_key = hashlib.sha256(tracklist_url.encode()).hexdigest()[:24]
    cache_file = TRACKLISTS_CACHE_DIR / f"{cache_key}.html"

    html: Optional[str] = None
    if cache_file.exists():
        try:
            html = cache_file.read_text(encoding="utf-8")
            log.info(f"1001tracklists: cache hit {cache_key}")
        except OSError:
            html = None
    if html is None:
        html = _tracklists_rate_limited_get(tracklist_url)
        if html:
            try:
                cache_file.write_text(html, encoding="utf-8")
            except OSError:
                pass
    if not html:
        return []
    segments = _parse_1001_tracklist_html(html, duration)
    log.info(
        f"1001tracklists: {tracklist_url} → {len(segments)} segments"
    )
    return segments


def shazam_sample(audio_path: Path, start: float, duration: float) -> Optional[dict]:
    """Invoke the shazam helper subprocess. Returns its parsed JSON, or None on error."""
    if not SHAZAM_HELPER.exists() or not SHAZAM_VENV_PYTHON.exists():
        return None
    try:
        result = subprocess.run(
            [
                str(SHAZAM_VENV_PYTHON),
                str(SHAZAM_HELPER),
                str(audio_path),
                f"{start:.2f}",
                f"{duration:.2f}",
            ],
            capture_output=True,
            text=True,
            timeout=45,
        )
    except subprocess.TimeoutExpired:
        log.warning(f"Shazam timeout at {start:.0f}s")
        return None
    if result.returncode != 0:
        log.warning(f"Shazam helper exit {result.returncode} at {start:.0f}s: {result.stdout.strip()[:200]}")
        return None
    try:
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return None


def shazam_scan(
    audio_path: Path, total_duration: float, stop_event: Optional[threading.Event] = None
) -> list[Segment]:
    """Slide Shazam recognition across the audio and group consecutive matches into segments."""
    samples: list[tuple[float, str, str, str]] = []  # (start, title, artist, key)
    pos = 0.0
    while pos + SHAZAM_WINDOW_SEC <= total_duration:
        if stop_event and stop_event.is_set():
            break
        result = shazam_sample(audio_path, pos, SHAZAM_WINDOW_SEC)
        if result and result.get("matched"):
            samples.append(
                (
                    pos,
                    result.get("title") or "Unknown",
                    result.get("artist") or "",
                    result.get("shazam_id") or "",
                )
            )
            log.info(
                f"Shazam @ {pos:7.1f}s → {result.get('title')} — {result.get('artist')}"
            )
        else:
            log.debug(f"Shazam @ {pos:7.1f}s → no match")
        pos += SHAZAM_HOP_SEC
        time.sleep(SHAZAM_RATE_LIMIT_SLEEP)

    # Group consecutive samples with the same Shazam key into segments
    segments: list[Segment] = []
    i = 0
    while i < len(samples):
        start, title, artist, key = samples[i]
        end_pos = start + SHAZAM_HOP_SEC
        j = i + 1
        while j < len(samples) and samples[j][3] == key:
            end_pos = samples[j][0] + SHAZAM_HOP_SEC
            j += 1
        end = min(end_pos, total_duration)
        if end - start >= 60:  # ignore single-window flukes (<1 min runs)
            segments.append(
                Segment(
                    start=start,
                    end=end,
                    title=title,
                    artist=artist or None,
                    confidence="shazam",
                )
            )
        i = j
    return segments


def fill_unidentified_gaps(
    segments: list[Segment], total_duration: float
) -> list[tuple[float, float]]:
    """Return gaps between known segments (and at start/end) ≥ UNIDENTIFIED_MIN_GAP_SEC."""
    if total_duration <= 0:
        return []
    gaps: list[tuple[float, float]] = []
    sorted_segs = sorted(segments, key=lambda s: s.start)
    cur = 0.0
    for seg in sorted_segs:
        if seg.start - cur >= UNIDENTIFIED_MIN_GAP_SEC:
            gaps.append((cur, seg.start))
        cur = max(cur, seg.end)
    if total_duration - cur >= UNIDENTIFIED_MIN_GAP_SEC:
        gaps.append((cur, total_duration))
    return gaps


SLICE_BUFFER_SEC = 5.0  # trim this much off each end of a mix-sliced segment
                        # to dodge crossfades, EQ ramps, and DJ chatter.

# Reject ytsearch1 hits whose duration falls outside this band — guards against
# 30-second previews and 2-hour wrong-match videos.
ORIGINAL_MIN_DURATION_SEC = 60.0
ORIGINAL_MAX_DURATION_SEC = 15 * 60.0

# Titles that aren't worth a YouTube search — too generic to disambiguate.
_GENERIC_TITLE_RE = re.compile(
    r"^\s*("
    r"untitled(?:\s+chapter)?(?:\s+\d+)?|"
    r"chapter\s*\d+|track\s*\d+|part\s*\d+|"
    r"intro|outro|interlude|break(?:down)?|drop|"
    r"id|unknown(?:\s+id)?|\?+"
    r")\s*$",
    re.IGNORECASE,
)


def _is_searchable_segment(seg: "Segment") -> bool:
    """True if the segment has enough title/artist info to look up the original."""
    if seg.artist:
        return True  # any artist name → worth a try
    if not seg.title or len(seg.title.strip()) < 4:
        return False
    if _GENERIC_TITLE_RE.match(seg.title):
        return False
    return True


def ffmpeg_split_stream_copy(src: Path, start: float, end: float, dest: Path):
    """Cut [start, end] from src into dest, stream-copying MP3 frames (no re-encode)."""
    cmd = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        f"{start:.3f}",
        "-to",
        f"{end:.3f}",
        "-i",
        str(src),
        "-c",
        "copy",
        "-map_metadata",
        "-1",
        str(dest),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg split failed: {result.stderr.strip()[:200]}")


# Search prefixes for the original-track lookup. Tried in order; first
# usable hit wins. SoundCloud catches underground/white-label tracks that
# YouTube doesn't surface.
ORIGINAL_SEARCH_PREFIXES: tuple[tuple[str, str], ...] = (
    ("ytsearch1:", "youtube"),
    ("scsearch1:", "soundcloud"),
)

# Words inside a query's parentheses that are too generic to use as a match signal.
_GENERIC_MODIFIER_WORDS = {
    "mix", "original", "extended", "radio", "edit", "version",
    "clean", "dirty", "album", "single", "official",
    "feat", "ft", "featuring", "with",
    "the", "and", "a", "an", "of",
}


def _extract_modifier_words(query: str) -> list[str]:
    """Pull specific / non-generic tokens from any parenthetical in the query.

    Example:
      "Amr Diab Nour el Ein (Yas Cepeda HABIBI HOUSE Remix)"
      → ['yas', 'cepeda', 'habibi', 'house', 'remix']
    Returns [] when the query has no parentheticals or only generic ones.
    """
    words: list[str] = []
    for m in re.finditer(r"\(([^)]+)\)", query):
        for tok in re.findall(r"\w+", m.group(1).lower()):
            if len(tok) >= 3 and tok not in _GENERIC_MODIFIER_WORDS:
                words.append(tok)
    # Preserve order but de-dupe
    seen: set[str] = set()
    out = []
    for w in words:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def _result_matches_modifier(
    result_title: str, result_uploader: str, modifier_words: list[str]
) -> bool:
    """True if the result seems to be the specific edit implied by modifier_words.

    We accept the match when at least half of the modifier tokens appear in the
    result's title OR uploader. That handles cases where the DJ's name shows up
    as the channel rather than in the video title.
    """
    if not modifier_words:
        return True
    haystack = f"{result_title} {result_uploader}".lower()
    hits = sum(1 for w in modifier_words if w in haystack)
    return hits >= max(1, (len(modifier_words) + 1) // 2)


def _try_original_from(prefix: str, source_name: str, query: str, dest: Path) -> bool:
    """Try one search-prefix path. Returns True if a usable file landed at `dest`."""
    target = f"{prefix}{query.strip()}"
    try:
        info = probe_info(target)
    except Exception as e:
        log.info(f"Original probe failed ({source_name}) for {query!r}: {e}")
        return False

    duration = float(info.get("duration") or 0)
    if not (ORIGINAL_MIN_DURATION_SEC <= duration <= ORIGINAL_MAX_DURATION_SEC):
        log.info(
            f"Original rejected ({source_name}) for {query!r}: "
            f"duration {duration:.0f}s outside "
            f"[{ORIGINAL_MIN_DURATION_SEC:.0f}, {ORIGINAL_MAX_DURATION_SEC:.0f}]"
        )
        return False

    # Verify the returned video is the *specific edit* the query implied.
    # This catches the common failure mode where the query asks for
    # "(Bedouin Private Remix)" and the top hit is the original studio track.
    modifier_words = _extract_modifier_words(query)
    if modifier_words:
        result_title = info.get("title") or ""
        result_uploader = info.get("uploader") or ""
        if not _result_matches_modifier(result_title, result_uploader, modifier_words):
            log.info(
                f"Original rejected ({source_name}) for {query!r}: "
                f"result {result_title!r} by {result_uploader!r} doesn't match "
                f"modifier tokens {modifier_words}"
            )
            return False

    stem = f"_original_{uuid.uuid4().hex[:8]}"
    tmpl = str(WORK_DIR / f"{stem}.%(ext)s")
    cmd = [
        "yt-dlp", "--no-warnings", "--no-playlist",
        "-x", "--audio-format", "mp3", "--audio-quality", "320K",
        "--postprocessor-args", "ffmpeg:-b:a 320k -minrate 320k -maxrate 320k",
        "-o", tmpl, target,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    produced = WORK_DIR / f"{stem}.mp3"
    if result.returncode != 0 or not produced.exists() or produced.stat().st_size == 0:
        log.warning(
            f"Original download failed ({source_name}) for {query!r}: "
            f"{result.stderr.strip()[:200]}"
        )
        if produced.exists():
            try:
                produced.unlink()
            except OSError:
                pass
        return False

    if not _verify_bitrate(produced):
        log.warning(
            f"Original bitrate check failed ({source_name}) for {query!r} "
            f"— SoundCloud often serves <320 kbps; will fall through"
            if source_name == "soundcloud" else
            f"Original bitrate check failed ({source_name}) for {query!r}"
        )
        # SoundCloud commonly serves at 128 kbps so don't reject for that source;
        # accept what we got rather than fall back to a mix slice.
        if source_name != "soundcloud":
            try:
                produced.unlink()
            except OSError:
                pass
            return False

    os.replace(produced, dest)
    return True


def download_original_track(query: str, dest: Path) -> Optional[str]:
    """Try each search source in order; return the source name on success, None on failure."""
    for prefix, source_name in ORIGINAL_SEARCH_PREFIXES:
        if _try_original_from(prefix, source_name, query, dest):
            return source_name
    return None


def _fmt_hms(seconds: float) -> str:
    """Format seconds as HH-MM-SS for use in filenames."""
    s = int(round(seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}-{m:02d}-{s:02d}"


def tag_mp3(
    path: Path,
    title: str,
    artist: Optional[str],
    album: str,
    track_num: int,
    total_tracks: int,
):
    """Write ID3v2.3 tags to an MP3 using mutagen."""
    try:
        from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK, ID3NoHeaderError
    except ImportError:
        log.warning("mutagen not installed; skipping ID3 tagging")
        return
    try:
        try:
            tags = ID3(str(path))
        except ID3NoHeaderError:
            tags = ID3()
        tags["TIT2"] = TIT2(encoding=3, text=title)
        if artist:
            tags["TPE1"] = TPE1(encoding=3, text=artist)
        tags["TALB"] = TALB(encoding=3, text=album)
        tags["TRCK"] = TRCK(encoding=3, text=f"{track_num}/{total_tracks}")
        tags.save(str(path), v2_version=3)
    except Exception as e:
        log.warning(f"ID3 tag write failed for {path.name}: {e}")


# --- Job execution ------------------------------------------------------------


class JobRunner:
    """Owns the worker thread and the active subprocess (so preemption can reach it)."""

    def __init__(self, queue: JobQueue, persistence: Persistence, status_cb=None):
        self.queue = queue
        self.persistence = persistence
        self.status_cb = status_cb  # called with a Job for status updates (telegram)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._current_job: Optional[Job] = None
        self._current_proc: Optional[subprocess.Popen] = None
        self._proc_lock = threading.Lock()

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _notify(self, job: Job):
        self.persistence.upsert(job)
        if self.status_cb:
            try:
                self.status_cb(job)
            except Exception as e:
                log.warning(f"status_cb error: {e}")

    def _loop(self):
        while not self._stop.is_set():
            if not self.queue.wait_for_work(timeout=1.0):
                continue
            job = self.queue.pop_next()
            if not job:
                continue
            self._current_job = job
            try:
                self._run_job(job)
            except Exception as e:
                log.exception(f"Job {job.id} crashed: {e}")
                job.state = JobState.FAILED
                job.error = str(e)
                self._notify(job)
                self._cleanup_work_files(job)
            finally:
                self._current_job = None

    def _cleanup_work_files(self, job: Job):
        """Remove any partial/failed output for this job so retries don't reuse stale data."""
        if not job.title:
            return
        sanitized = sanitize_filename(job.title)
        for stale in WORK_DIR.glob(f"{sanitized}.*"):
            try:
                stale.unlink()
                log.info(f"Cleaned up stale work file: {stale.name}")
            except OSError as e:
                log.warning(f"Could not remove {stale}: {e}")

    def _run_job(self, job: Job):
        log.info(
            f"Starting job {job.id}: {job.input!r} important={job.important} set={job.split_as_set}"
        )

        # Playlist parent: expand into child jobs and finish (no probe needed).
        if (
            not job.parent_id
            and not job.split_as_set
            and is_playlist_input(job.input)
        ):
            self._run_playlist_parent(job)
            return

        # Auto-detect set splitting for top-level URL/search inputs. Skip when
        # the user already passed --set, for playlist children, and for Spotify
        # track URLs (those go through oEmbed and don't have set chapters).
        if (
            not job.split_as_set
            and not job.parent_id
            and not is_spotify_track(job.input)
        ):
            try:
                probe_target = classify_input(job.input)
                info = probe_info(probe_target)
                if should_auto_set_split(info):
                    log.info(
                        f"Auto-set detected for {job.id} "
                        f"(chapters={len(info.get('chapters') or [])}, "
                        f"duration={info.get('duration')}s) — enabling split mode"
                    )
                    job.split_as_set = True
            except Exception as e:
                log.warning(f"Auto-set probe failed (non-fatal): {e}")

        # DJ set: download whole source then split into tracks.
        if job.split_as_set and not job.parent_id:
            self._run_set_job(job)
            return

        job.state = JobState.DOWNLOADING
        self._notify(job)
        log_job_event({"event": "start", "job": job.to_dict()})

        target = classify_input(job.input)
        job.title = probe_title(target)
        self._notify(job)

        sanitized = sanitize_filename(job.title)
        work_template = str(WORK_DIR / f"{sanitized}.%(ext)s")

        cmd = [
            "yt-dlp",
            "--no-warnings",
            "--no-playlist",
            "--continue",
            "--default-search",
            "ytsearch1:",
            "-x",
            "--audio-format",
            "mp3",
            "--audio-quality",
            "320K",  # yt-dlp interprets K-suffixed values as CBR kbps
            "--postprocessor-args",
            "ffmpeg:-b:a 320k -minrate 320k -maxrate 320k",
            "-o",
            work_template,
            target,
        ]

        success = self._run_with_preemption(cmd, job)
        if not success:
            # Either failed terminally or was preempted+requeued
            return

        produced = WORK_DIR / f"{sanitized}.mp3"
        if not produced.exists() or produced.stat().st_size == 0:
            raise RuntimeError(f"Expected output {produced} missing or empty")

        if not _verify_bitrate(produced):
            raise RuntimeError(f"Output bitrate below 320 kbps")

        target_dir = Path(job.output_dir) if job.output_dir else MUSIC_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        filename = self._final_filename(job, sanitized)
        final = unique_path(target_dir, filename)
        os.replace(produced, final)
        job.output_path = str(final)
        job.state = JobState.DONE
        self._notify(job)
        log_job_event({"event": "done", "job": job.to_dict()})
        log.info(f"Job {job.id} saved to {final}")

    def _final_filename(self, job: Job, sanitized_title: str) -> str:
        """Build the final filename, prefixing with playlist index when applicable."""
        if job.playlist_index and job.playlist_total:
            width = max(2, len(str(job.playlist_total)))
            prefix = f"{job.playlist_index:0{width}d} - "
            return f"{prefix}{sanitized_title}.mp3"
        return f"{sanitized_title}.mp3"

    def _run_set_job(self, job: Job):
        """Download a DJ set and split into individual MP3s.

        Cheap cascade runs BEFORE any audio download:
          1. YouTube chapter markers (titles + boundaries)
          2. Description timestamp lines (titles + boundaries)
          3. Description numbered tracklist (titles only)
          4. 1001tracklists.com lookup (titles + boundaries)

        Only if all of the above fail OR we need a mix-slice fallback do we
        actually download the source mix; Shazam runs only after the download.

        Per-segment sourcing: YouTube original → SoundCloud original → mix slice
        (only if mix exists) → _missing/<title>.txt placeholder.
        """
        log.info(f"DJ-set job {job.id}: {job.input!r}")
        job.state = JobState.DOWNLOADING
        self._notify(job)
        log_job_event({"event": "set_start", "job": job.to_dict()})

        target = classify_input(job.input)
        info = probe_info(target)
        set_name = info.get("title") or "Untitled Set"
        sanitized_set_name = sanitize_filename(set_name)
        duration = float(info.get("duration") or 0)
        if duration < 60:
            raise RuntimeError("Source is too short to be a DJ set (<60 s).")

        job.title = set_name
        self._notify(job)

        # --- Cheap cascade: no audio download required ---
        description = info.get("description") or ""
        segments: list[Segment] = extract_chapter_segments(info)
        cascade_source = "chapters" if segments else None

        if not segments:
            segments = extract_description_segments(description, duration)
            if segments:
                cascade_source = "description_timestamps"
                log.info(f"Set {job.id}: parsed {len(segments)} segments from description timestamps")

        if not segments:
            segments = extract_description_numbered_tracklist(description, duration)
            if segments:
                cascade_source = "description_numbered"
                log.info(
                    f"Set {job.id}: parsed {len(segments)} segments from "
                    f"description numbered list (no real boundaries)"
                )

        if not segments:
            log.info(f"Set {job.id}: trying 1001tracklists lookup")
            tl_url = find_1001_tracklist(set_name)
            if tl_url:
                log.info(f"Set {job.id}: 1001tracklists match → {tl_url}")
                segments = fetch_1001_tracklist(tl_url, duration)
                if segments:
                    cascade_source = "1001tracklists"

        if segments:
            log.info(
                f"Set {job.id}: cascade source={cascade_source}, {len(segments)} segments"
            )

        # --- Decide whether to download the mix ---
        # Skip the mix when we have description-numbered (uniform boundaries are
        # useless for slicing — better to leave un-findable tracks as placeholders).
        # Always download when we'll need Shazam.
        needs_mix_for_shazam = not segments
        needs_mix_for_slice_fallback = (
            segments is not None and cascade_source != "description_numbered"
        )
        source_mp3: Optional[Path] = None
        if needs_mix_for_shazam or needs_mix_for_slice_fallback:
            log.info(f"Set {job.id}: downloading source mix for slice fallback / Shazam")
            source_template = str(WORK_DIR / f"_set_{job.id}.%(ext)s")
            source_mp3 = WORK_DIR / f"_set_{job.id}.mp3"
            dl_cmd = [
                "yt-dlp", "--no-warnings", "--no-playlist", "--continue",
                "--default-search", "ytsearch1:",
                "-x", "--audio-format", "mp3", "--audio-quality", "320K",
                "--postprocessor-args", "ffmpeg:-b:a 320k -minrate 320k -maxrate 320k",
                "-o", source_template, target,
            ]
            success = self._run_with_preemption(dl_cmd, job)
            if not success:
                return
            if not source_mp3.exists() or source_mp3.stat().st_size == 0:
                raise RuntimeError(f"Source MP3 missing after download: {source_mp3}")
        else:
            log.info(
                f"Set {job.id}: skipping mix download — description tracklist found, "
                f"sourcing each track individually"
            )

        # Shazam (only when we have the mix and no segments yet)
        if not segments:
            assert source_mp3 is not None
            log.info(
                f"Set {job.id}: running ShazamIO scan "
                f"({duration:.0f} s @ {SHAZAM_WINDOW_SEC:.0f} s window)"
            )
            segments = shazam_scan(source_mp3, duration, stop_event=self._stop)
            cascade_source = "shazam"
            log.info(f"Set {job.id}: Shazam identified {len(segments)} segments")

        # --- Build output folder, then source each track ---
        set_dir = unique_path(MUSIC_DIR, sanitized_set_name)
        set_dir.mkdir(parents=True, exist_ok=False)
        unident_dir = set_dir / "_unidentified"
        missing_dir = set_dir / "_missing"

        gaps = fill_unidentified_gaps(segments, duration) if source_mp3 else []
        if not segments and not gaps:
            if source_mp3:
                full = set_dir / f"{sanitized_set_name}.mp3"
                os.replace(source_mp3, full)
                job.output_path = str(set_dir)
                job.state = JobState.DONE
                self._notify(job)
                log.warning(f"Set {job.id}: no segments identified; saved full mix to {full}")
                return
            raise RuntimeError("Cascade returned no segments and no mix is available")

        width = max(2, len(str(len(segments))))
        sourcing_counts = {"youtube": 0, "soundcloud": 0, "slice": 0, "missing": 0}
        for i, seg in enumerate(segments, start=1):
            track_title = seg.display_title()
            fname = f"{i:0{width}d} - {sanitize_filename(track_title)}.mp3"
            dest = unique_path(set_dir, fname)

            query = (
                f"{seg.artist} {seg.title}".strip()
                if seg.artist
                else seg.title
            )
            source = None
            if _is_searchable_segment(seg):
                source = download_original_track(query, dest)

            if source:
                sourcing_counts[source] = sourcing_counts.get(source, 0) + 1
                log.info(f"Set {job.id}: wrote {dest.name} (original from {source})")
            elif source_mp3 is not None and seg.end > seg.start:
                # Mix-slice fallback with crossfade buffer
                seg_duration = seg.end - seg.start
                buf = SLICE_BUFFER_SEC if seg_duration > 4 * SLICE_BUFFER_SEC else 0.0
                ffmpeg_split_stream_copy(
                    source_mp3, seg.start + buf, seg.end - buf, dest
                )
                sourcing_counts["slice"] += 1
                log.info(
                    f"Set {job.id}: wrote {dest.name} (mix slice "
                    f"{seg.start + buf:.0f}-{seg.end - buf:.0f} s)"
                )
            else:
                # No mix and no original found → write placeholder text file
                missing_dir.mkdir(parents=True, exist_ok=True)
                placeholder = missing_dir / f"{i:0{width}d} - {sanitize_filename(track_title)}.txt"
                placeholder.write_text(
                    f"Track not findable on YouTube or SoundCloud.\n\n"
                    f"Title:  {seg.title}\n"
                    f"Artist: {seg.artist or '(unknown)'}\n"
                    f"Source: {cascade_source}\n",
                    encoding="utf-8",
                )
                sourcing_counts["missing"] += 1
                log.warning(f"Set {job.id}: wrote placeholder {placeholder.name}")
                continue

            tag_mp3(
                dest,
                title=seg.title,
                artist=seg.artist,
                album=set_name,
                track_num=i,
                total_tracks=len(segments),
            )
        log.info(
            f"Set {job.id}: sourcing summary — "
            f"{sourcing_counts['youtube']} from YouTube, "
            f"{sourcing_counts['soundcloud']} from SoundCloud, "
            f"{sourcing_counts['slice']} mix slices, "
            f"{sourcing_counts['missing']} placeholders"
        )

        # Write unidentified gaps (only when we have the mix to slice from)
        if gaps and source_mp3 is not None:
            unident_dir.mkdir(parents=True, exist_ok=True)
            for start, end in gaps:
                fname = f"{_fmt_hms(start)}_{_fmt_hms(end)}.mp3"
                dest = unident_dir / fname
                try:
                    ffmpeg_split_stream_copy(source_mp3, start, end, dest)
                    log.info(f"Set {job.id}: wrote unidentified {dest.name}")
                except Exception as e:
                    log.warning(f"Failed unidentified split {start}-{end}: {e}")

        # Clean up the source mix from work dir (only if we ever downloaded it)
        if source_mp3 is not None:
            try:
                source_mp3.unlink()
            except OSError:
                pass

        job.output_path = str(set_dir)
        job.state = JobState.DONE
        self._notify(job)
        log_job_event(
            {
                "event": "set_done",
                "job_id": job.id,
                "segments": len(segments),
                "unidentified": len(gaps),
            }
        )
        log.info(
            f"Set {job.id} → {set_dir} ({len(segments)} tracks, {len(gaps)} unidentified)"
        )

    def _run_playlist_parent(self, job: Job):
        """Expand a playlist URL into child jobs and mark parent done."""
        log.info(f"Expanding playlist {job.id}: {job.input!r}")
        job.is_playlist_parent = True
        job.state = JobState.DOWNLOADING  # use DOWNLOADING as a generic 'working' state
        self._notify(job)

        playlist_name, entries = expand_playlist(job.input)
        sanitized_name = sanitize_filename(playlist_name)
        job.title = playlist_name
        job.playlist_total = len(entries)
        self._notify(job)

        playlist_dir = unique_path(MUSIC_DIR, sanitized_name)  # treat dirname like a file collision
        playlist_dir.mkdir(parents=True, exist_ok=False)
        log.info(f"Playlist '{playlist_name}' → {playlist_dir} ({len(entries)} tracks)")

        # Enqueue one child per entry. Use slightly increasing enqueued_at so
        # priority order preserves playlist order within the same lane.
        base_ts = time.time()
        for idx, entry in enumerate(entries, start=1):
            child = Job(
                id=uuid.uuid4().hex[:8],
                input=entry.as_input(),
                important=job.important,
                enqueued_at=base_ts + idx * 0.001,
                parent_id=job.id,
                playlist_index=idx,
                playlist_total=len(entries),
                output_dir=str(playlist_dir),
                telegram_chat_id=job.telegram_chat_id,
            )
            self.queue.add(child)

        job.output_path = str(playlist_dir)
        job.state = JobState.DONE
        self._notify(job)
        log_job_event(
            {"event": "playlist_dispatched", "parent_id": job.id, "count": len(entries)}
        )

    def _run_with_preemption(self, cmd: list[str], job: Job) -> bool:
        """Spawn yt-dlp; pause for important jobs that arrive mid-flight.

        Returns True on success, False if the job was requeued (we'll see it again).
        Raises on terminal failure.
        """
        log.info(f"Spawning: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,  # own process group for SIGSTOP/SIGCONT
            bufsize=1,
        )
        with self._proc_lock:
            self._current_proc = proc

        output_lines: list[str] = []
        try:
            while True:
                # Drain a line non-blockingly (approx — Popen has no async stdout in stdlib).
                # We use poll() + a short read with select-like pattern via readline timeout
                # via threading. Simpler: spawn a reader thread that pushes to a queue.
                # For brevity we use a periodic poll + try to readline with a small sleep.
                rc = proc.poll()
                line = _try_readline(proc, output_lines)
                if line:
                    log.debug(f"[yt-dlp] {line.rstrip()}")

                # Check for preemption
                important = self.queue.peek_important_pending(job.id)
                if important and proc.poll() is None:
                    self._handle_preemption(job, proc)
                    # After preemption, either the original resumed cleanly (proc still running)
                    # or we requeued it (we return False).
                    if self._proc_was_requeued(proc):
                        return False
                    continue

                if rc is not None:
                    # Drain remaining output
                    remaining = proc.stdout.read() if proc.stdout else ""
                    if remaining:
                        output_lines.append(remaining)
                    break
                if not line:
                    time.sleep(PREEMPTION_POLL_INTERVAL)

            if proc.returncode != 0:
                tail = "".join(output_lines[-20:])
                raise RuntimeError(f"yt-dlp exited {proc.returncode}: {tail[-400:]}")
            return True
        finally:
            with self._proc_lock:
                self._current_proc = None

    def _proc_was_requeued(self, proc: subprocess.Popen) -> bool:
        """A sentinel: if the proc was killed during preemption and the job is back in queue."""
        return proc.poll() is not None and proc.returncode in (-signal.SIGKILL, -9)

    def _handle_preemption(self, original_job: Job, original_proc: subprocess.Popen):
        log.info(f"Preempting job {original_job.id} for important job")
        pgid = os.getpgid(original_proc.pid)

        # Pause
        try:
            os.killpg(pgid, signal.SIGSTOP)
        except ProcessLookupError:
            return  # already finished
        original_job.state = JobState.PAUSED
        self._notify(original_job)

        # Run the important job synchronously
        important = self.queue.pop_next()
        if not important or not important.important:
            # Race: it got cancelled or another worker grabbed it. Resume original.
            os.killpg(pgid, signal.SIGCONT)
            original_job.state = JobState.DOWNLOADING
            self._notify(original_job)
            return

        log.info(f"Running important job {important.id} while {original_job.id} is paused")
        # Temporarily clear current_proc so the important job's recursive _run_job
        # can write its own. _run_job sets it.
        try:
            self._run_job(important)
        except Exception as e:
            log.exception(f"Important job {important.id} crashed: {e}")
            important.state = JobState.FAILED
            important.error = str(e)
            self._notify(important)

        # Resume original
        with self._proc_lock:
            self._current_proc = original_proc
            self._current_job = original_job

        log.info(f"Resuming job {original_job.id}")
        try:
            os.killpg(pgid, signal.SIGCONT)
        except ProcessLookupError:
            return
        original_job.state = JobState.DOWNLOADING
        self._notify(original_job)

        # Watchdog: 30s to see signs of life (output or exit)
        watchdog_start = time.time()
        last_output_line_count = -1
        cur_output_lines: list[str] = []
        while time.time() - watchdog_start < WATCHDOG_RESUME_TIMEOUT:
            if original_proc.poll() is not None:
                # Exited (clean or not). The outer loop will handle return code.
                return
            line = _try_readline(original_proc, cur_output_lines)
            if line:
                return  # progress detected
            time.sleep(PREEMPTION_POLL_INTERVAL)

        # No signs of life — assume bad resume. Kill, clean partial, requeue.
        log.warning(f"Resume watchdog tripped on {original_job.id}; requeueing")
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        # Clean up partial files for this job
        if original_job.title:
            sanitized = sanitize_filename(original_job.title)
            for stale in WORK_DIR.glob(f"{sanitized}.*"):
                try:
                    stale.unlink()
                except OSError:
                    pass
        # Bump priority forward in the normal lane (re-enqueue at head of normals).
        original_job.enqueued_at = time.time() - 1000  # very negative
        self.queue.requeue_head(original_job)
        log_job_event(
            {"event": "preemption_requeue", "job_id": original_job.id}
        )


def _try_readline(proc: subprocess.Popen, sink: list[str]) -> str:
    """Try to read available output without blocking. Returns '' if none ready."""
    if proc.stdout is None:
        return ""
    fd = proc.stdout.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if not (flags & os.O_NONBLOCK):
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    try:
        chunk = proc.stdout.read(4096)
    except (BlockingIOError, OSError):
        return ""
    if not chunk:
        return ""
    sink.append(chunk)
    return chunk


def _verify_bitrate(path: Path) -> bool:
    """Use ffprobe to confirm the file is MP3 at >=320 kbps."""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_name,bit_rate",
        "-of",
        "json",
        str(path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        data = json.loads(result.stdout)
        stream = (data.get("streams") or [{}])[0]
        codec = stream.get("codec_name")
        bit_rate = int(stream.get("bit_rate") or 0)
        log.info(f"ffprobe: codec={codec} bit_rate={bit_rate}")
        return codec == "mp3" and bit_rate >= 315000  # small slack for CBR rounding
    except Exception as e:
        log.warning(f"ffprobe failed: {e}")
        return False


# --- HTTP API -----------------------------------------------------------------


class EnqueueRequest(BaseModel):
    input: str
    important: bool = False
    split_as_set: bool = False


def build_http_app(queue: JobQueue, runner: JobRunner) -> FastAPI:
    app = FastAPI(title="Songcatcher")

    @app.post("/enqueue")
    def enqueue(req: EnqueueRequest):
        if not req.input.strip():
            raise HTTPException(400, "input required")
        job = Job(
            id=uuid.uuid4().hex[:8],
            input=req.input.strip(),
            important=req.important,
            enqueued_at=time.time(),
            split_as_set=req.split_as_set,
        )
        queue.add(job)
        log.info(
            f"Enqueued {job.id}: {job.input!r} important={job.important} set={job.split_as_set}"
        )
        return {"job_id": job.id, "important": job.important, "split_as_set": job.split_as_set}

    @app.get("/queue")
    def get_queue():
        snap = queue.snapshot()
        return {
            "in_flight": runner._current_job.to_dict() if runner._current_job else None,
            "pending": [j.to_dict() for j in snap if j.state == JobState.QUEUED],
        }

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = queue.get(job_id)
        if not job:
            raise HTTPException(404, "not found")
        return job.to_dict()

    @app.delete("/jobs/{job_id}")
    def cancel_job(job_id: str):
        if queue.remove(job_id):
            return {"cancelled": job_id}
        raise HTTPException(404, "not found or already running")

    @app.post("/shutdown")
    def shutdown():
        os.kill(os.getpid(), signal.SIGTERM)
        return {"shutting_down": True}

    return app


# --- Telegram bot (raw HTTP long-polling) -------------------------------------


def load_telegram_config() -> Optional[dict]:
    if not TELEGRAM_CONFIG_PATH.exists():
        return None
    try:
        return json.loads(TELEGRAM_CONFIG_PATH.read_text())
    except Exception as e:
        log.warning(f"Invalid telegram config: {e}")
        return None


class TelegramClient:
    """Minimal Bot API client: long-polling for updates, plain sendMessage for replies."""

    API = "https://api.telegram.org"

    def __init__(self, token: str, allowed_ids: set[int]):
        self.token = token
        self.allowed_ids = allowed_ids
        self._offset = 0
        self._send_lock = threading.Lock()

    def _url(self, method: str) -> str:
        return f"{self.API}/bot{self.token}/{method}"

    def _post(self, method: str, payload: dict, timeout: float = 35.0) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = Request(
            self._url(method),
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())

    def send_message(self, chat_id: int, text: str):
        try:
            with self._send_lock:
                self._post(
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                    },
                    timeout=10,
                )
        except Exception as e:
            log.warning(f"Telegram sendMessage failed: {e}")

    def send_document(self, chat_id: int, path: Path, caption: str = ""):
        # Multipart upload via stdlib
        import email.mime.multipart
        import email.mime.base
        import email.encoders
        import mimetypes

        boundary = uuid.uuid4().hex
        body = bytearray()

        def part(name: str, value: str):
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            )
            body.extend(value.encode("utf-8"))
            body.extend(b"\r\n")

        part("chat_id", str(chat_id))
        if caption:
            part("caption", caption)
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(
            f'Content-Disposition: form-data; name="document"; filename="{path.name}"\r\n'.encode()
        )
        body.extend(b"Content-Type: audio/mpeg\r\n\r\n")
        body.extend(path.read_bytes())
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode())

        req = Request(
            self._url("sendDocument"),
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            with self._send_lock:
                with urlopen(req, timeout=120) as resp:
                    json.loads(resp.read())
        except Exception as e:
            log.warning(f"Telegram sendDocument failed: {e}")

    def get_updates(self) -> list[dict]:
        """Blocks up to ~30 s waiting for updates."""
        try:
            resp = self._post(
                "getUpdates",
                {"offset": self._offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=35,
            )
        except Exception as e:
            log.warning(f"getUpdates failed: {e}")
            time.sleep(2)
            return []
        if not resp.get("ok"):
            log.warning(f"getUpdates not ok: {resp}")
            time.sleep(2)
            return []
        updates = resp.get("result", [])
        if updates:
            self._offset = max(u["update_id"] for u in updates) + 1
        return updates

    def authorized(self, update: dict) -> bool:
        msg = update.get("message") or {}
        user = msg.get("from") or {}
        return user.get("id") in self.allowed_ids


def telegram_status_callback(client: TelegramClient):
    """Returns a fn that posts status updates to the originating Telegram chat."""

    def position_tag(job: Job) -> str:
        if job.playlist_index and job.playlist_total:
            return f"[{job.playlist_index}/{job.playlist_total}] "
        return ""

    def cb(job: Job):
        if not job.telegram_chat_id:
            return
        tag = position_tag(job)
        if job.is_playlist_parent:
            if job.state == JobState.DOWNLOADING:
                text = f"📃 Expanding playlist: {job.input}"
            elif job.state == JobState.DONE:
                text = (
                    f"📃 Playlist *{job.title}* — {job.playlist_total} tracks queued.\n"
                    f"Folder: `{Path(job.output_path).name}/`"
                )
            elif job.state == JobState.FAILED:
                text = f"❌ Playlist failed: {job.error}"
            else:
                return
        elif job.state == JobState.QUEUED:
            # Skip per-track queued chatter for playlist children to avoid spam
            if job.parent_id:
                return
            text = f"⏳ Queued #{job.id}: {job.input}"
        elif job.state == JobState.DOWNLOADING:
            # Skip per-track downloading updates for playlist children (still get DONE)
            if job.parent_id:
                return
            text = f"⬇️ {tag}Downloading: {job.title or job.input}"
        elif job.state == JobState.PAUSED:
            text = f"⏸️ {tag}Paused #{job.id} (important job in front)"
        elif job.state == JobState.DONE:
            text = f"✅ {tag}Saved: `{Path(job.output_path).name}`"
        elif job.state == JobState.FAILED:
            text = f"❌ {tag}Failed #{job.id}: {job.error}"
        elif job.state == JobState.CANCELLED:
            text = f"🚫 {tag}Cancelled #{job.id}"
        else:
            return
        client.send_message(job.telegram_chat_id, text)

    return cb


def start_telegram_bot(queue: JobQueue, runner: JobRunner) -> Optional[TelegramClient]:
    cfg = load_telegram_config()
    if not cfg or not cfg.get("token"):
        log.info("No Telegram config; bot disabled")
        return None
    allowed_ids = set(int(x) for x in cfg.get("allowed_user_ids", []))
    if not allowed_ids:
        log.warning("No allowed_user_ids in telegram.json; bot will accept nobody")
    client = TelegramClient(cfg["token"], allowed_ids)

    def handle_text(update: dict):
        if not client.authorized(update):
            return
        msg = update["message"]
        text = (msg.get("text") or "").strip()
        if not text:
            return
        chat_id = msg["chat"]["id"]

        if text.startswith("/queue"):
            snap = queue.snapshot()
            in_flight = runner._current_job
            lines = []
            if in_flight:
                lines.append(
                    f"▶️ {in_flight.id} ({in_flight.state.value}): {in_flight.title or in_flight.input}"
                )
            for j in snap:
                if j.state == JobState.QUEUED:
                    star = "⭐ " if j.important else ""
                    lines.append(f"• {star}{j.id}: {j.input[:60]}")
            client.send_message(chat_id, "\n".join(lines) or "Idle.")
            return

        if text.startswith("/cancel"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                client.send_message(chat_id, "Usage: /cancel <job_id>")
                return
            if queue.remove(parts[1].strip()):
                client.send_message(chat_id, f"Cancelled #{parts[1].strip()}")
            else:
                client.send_message(chat_id, f"No pending job #{parts[1].strip()}")
            return

        if text.startswith("/start") or text.startswith("/help"):
            client.send_message(
                chat_id,
                "Send a URL or 'song artist' to enqueue.\n"
                "Prefix with `!` or `important ` to jump the queue.\n"
                "/queue — show pending\n"
                "/cancel <id> — cancel a pending job",
            )
            return

        if text.startswith("/"):
            return  # ignore unknown commands

        important = False
        split_as_set = False
        lowered = text.lower()
        # Parse prefixes in any order: !important, !set, "important", "set"
        while True:
            lowered = text.lower()
            if text.startswith("!"):
                rest = text[1:].lstrip()
                rest_lower = rest.lower()
                if rest_lower.startswith("set "):
                    split_as_set = True
                    text = rest[4:].strip()
                    continue
                # bare ! means important
                important = True
                text = rest
                continue
            if lowered.startswith("important "):
                important = True
                text = text[len("important "):].strip()
                continue
            if lowered.startswith("set "):
                split_as_set = True
                text = text[4:].strip()
                continue
            break

        if not text:
            client.send_message(chat_id, "Empty input.")
            return

        # If the message has a URL embedded in extra text ("check this https://..."),
        # extract just the URL so yt-dlp doesn't try to search for the whole sentence.
        url_match = re.search(r"https?://\S+", text)
        if url_match and url_match.group(0) != text:
            text = url_match.group(0).rstrip(".,;)\"'")

        job = Job(
            id=uuid.uuid4().hex[:8],
            input=text,
            important=important,
            enqueued_at=time.time(),
            telegram_chat_id=chat_id,
            split_as_set=split_as_set,
        )
        queue.add(job)
        flags = []
        if important:
            flags.append("⚡️important")
        if split_as_set:
            flags.append("✂️ split as DJ set")
        flag_text = " (" + ", ".join(flags) + ")" if flags else ""
        client.send_message(chat_id, f"Queued #{job.id}{flag_text}")

    def poll_loop():
        log.info(f"Telegram polling started; whitelisted: {sorted(allowed_ids)}")
        while True:
            try:
                updates = client.get_updates()
                for u in updates:
                    try:
                        handle_text(u)
                    except Exception as e:
                        log.exception(f"Telegram handler error: {e}")
            except Exception as e:
                log.exception(f"Telegram poll loop error: {e}")
                time.sleep(2)

    threading.Thread(target=poll_loop, daemon=True, name="telegram-bot").start()
    return client


# --- Main ---------------------------------------------------------------------


def main():
    log.info(f"Songcatcher starting; MUSIC_DIR={MUSIC_DIR}")
    persistence = Persistence(DB_PATH)
    queue = JobQueue(persistence)
    runner = JobRunner(queue, persistence)
    runner.start()

    telegram_client = start_telegram_bot(queue, runner)
    if telegram_client:
        runner.status_cb = telegram_status_callback(telegram_client)

    app = build_http_app(queue, runner)
    config = uvicorn.Config(app, host=HTTP_HOST, port=HTTP_PORT, log_level="warning")
    server = uvicorn.Server(config)

    def handle_sig(signum, frame):
        log.info(f"Signal {signum}; shutting down")
        runner.stop()
        server.should_exit = True

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    server.run()
    log.info("Songcatcher exited")


if __name__ == "__main__":
    main()
