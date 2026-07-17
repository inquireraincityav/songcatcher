#!/usr/bin/env python3
"""Standalone Shazam recognition helper.

Runs in its own venv (shazamio + pydantic v1) to avoid clashing with the
daemon's FastAPI/pydantic v2 deps. The daemon shells out to this script
for each fingerprint sample.

Usage:
    shazam_helper.py <audio_path> <start_seconds> <duration_seconds>

Outputs a single JSON object on stdout:
    {"matched": true, "title": "...", "artist": "...", "shazam_id": "..."}
    {"matched": false, "reason": "no_match"}
    {"matched": false, "reason": "error", "error": "..."}
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile

try:
    from shazamio import Shazam
except ImportError as e:
    print(json.dumps({"matched": False, "reason": "error", "error": f"import: {e}"}))
    sys.exit(1)


def extract_chunk(src: str, start: float, duration: float) -> str:
    """Use ffmpeg to write [start, start+duration] of src to a temp WAV file."""
    tmp = tempfile.NamedTemporaryFile(suffix=".m4a", delete=False)
    tmp.close()
    cmd = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        str(start),
        "-t",
        str(duration),
        "-i",
        src,
        "-vn",
        "-ac",
        "1",
        "-ar",
        "44100",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        tmp.name,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if res.returncode != 0 or not os.path.exists(tmp.name) or os.path.getsize(tmp.name) == 0:
        os.unlink(tmp.name)
        raise RuntimeError(f"ffmpeg chunk extract failed: {res.stderr.strip()[:200]}")
    return tmp.name


async def recognize(path: str) -> dict:
    sh = Shazam()
    raw = await sh.recognize(path)
    track = (raw or {}).get("track") or {}
    if not track:
        return {"matched": False, "reason": "no_match"}
    return {
        "matched": True,
        "title": track.get("title") or "",
        "artist": track.get("subtitle") or "",
        "shazam_id": str(track.get("key") or ""),
    }


def main():
    if len(sys.argv) != 4:
        print(json.dumps({"matched": False, "reason": "error", "error": "bad args"}))
        sys.exit(2)
    src, start, duration = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    chunk = None
    try:
        chunk = extract_chunk(src, start, duration)
        result = asyncio.run(recognize(chunk))
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"matched": False, "reason": "error", "error": str(e)}))
        sys.exit(1)
    finally:
        if chunk and os.path.exists(chunk):
            try:
                os.unlink(chunk)
            except OSError:
                pass


if __name__ == "__main__":
    main()
