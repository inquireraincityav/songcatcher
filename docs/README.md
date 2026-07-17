# Songcatcher

Local audio-fetching daemon on this Mac. Send a link or `song artist` query from
the menu bar, the CLI, or Telegram on your phone — get back a 320 kbps MP3 in
`~/Desktop/Music` named after the track's metadata title.

## What it does

- Accepts YouTube, SoundCloud, Bandcamp, and other yt-dlp-supported URLs
- Accepts Spotify track URLs (resolves title+artist via oEmbed, sources audio from YouTube)
- Accepts plain text search ("Hotel California Eagles")
- Accepts playlist URLs: YouTube playlists, Spotify playlists/albums, SoundCloud sets, Bandcamp albums — fans out into per-track downloads under `~/Desktop/Music/<Playlist Name>/`
- Accepts DJ sets via `--set` flag — downloads the full mix, splits into individual tracks using chapter markers, in-description tracklists, or ShazamIO fingerprinting (in that priority order)
- Saves 320 kbps CBR MP3 to `~/Desktop/Music/<Title - Artist>.mp3`
- Priority queue: `important` requests jump the line and pause the in-flight job

## Install

```sh
cd songcatcher/install
./install.sh
```

The installer:
- Installs `ffmpeg`, `yt-dlp`, `SwiftBar` (via Homebrew) if missing
- Creates a Python venv at `~/Library/Application Support/Songcatcher/venv`
- Copies the daemon, launchd plist, SwiftBar plugin, and CLI into place
- Walks you through Telegram bot registration (optional but useful)
- Loads the launchd LaunchAgent so the daemon starts at login

## Usage

### CLI

```sh
songcatcher "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
songcatcher "Hotel California Eagles"
songcatcher --important "https://open.spotify.com/track/4u7EnebtmKWzUH433cf5Qv"

# Playlists (auto-detected — no flag needed)
songcatcher "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
songcatcher "https://www.youtube.com/playlist?list=PLrAl6rYAS4HrAGqcrnQUOh4tdwXLJgXc4"

# DJ sets — opt-in splitting
songcatcher --set "https://www.youtube.com/watch?v=gWH6sOCtd2A"

# Combine flags
songcatcher --important --set "https://example.com/mix"

songcatcher --queue
songcatcher --cancel <job_id>
songcatcher --status <job_id>
```

### Menu bar (SwiftBar)

A "♪" icon shows in your menu bar:
- Click → menu with current job status, pending queue, and a "New job…" prompt
- The "New job…" dialog has **Queue** (normal) and **Important** buttons

### Telegram (mobile)

The default bot name suggested by the installer is **SJAIstudio**. After running
`./install.sh --register-telegram` (which walks you through BotFather), message
your bot:

- Any text → enqueued
- A URL → enqueued
- Prefix with `!` or `important ` → marked important, jumps the queue
- Prefix with `!set ` or `set ` → DJ-set split mode
- Combine: `!set https://...` → important + split
- `/queue` → list pending
- `/cancel <id>` → cancel a pending job
- `/help` → reminder

The bot replies with status updates ("⏳ Queued", "⬇️ Downloading", "✅ Saved").

## Priority & preemption

Normal jobs run FIFO. When an `important` request arrives mid-flight:
1. The in-flight `yt-dlp` process tree is paused (`SIGSTOP` to its process group).
2. The important job runs to completion.
3. The original is resumed (`SIGCONT`).
4. If the resumed process doesn't make progress within 30 s (e.g., the suspended
   TCP connection died), it's killed cleanly, partial files are removed, and
   the job is re-queued at the head of the normal-priority lane.

In short: **"important jumps the queue; the current job pauses and resumes if
possible, otherwise restarts after the important one."**

## Playlists

YouTube/SoundCloud/Bandcamp playlist URLs and Spotify playlists/albums are
auto-detected: the daemon enumerates the track list, creates
`~/Desktop/Music/<Playlist Name>/`, and enqueues one job per track named
`01 - <Title>.mp3`, `02 - …`, etc. An `important` playlist makes every child
track jump the queue together; pending normals wait.

Spotify playlist resolution scrapes the public embed page (`__NEXT_DATA__`)
for the track list, then sources each track from YouTube. No Spotify API
credentials required.

## DJ sets

`--set` triggers split-into-tracks mode:
1. Daemon downloads the full source as a 320 kbps MP3.
2. Tries to find a tracklist (in priority order):
   a. **YouTube chapter markers** (high confidence; modern DJ podcasts use these).
   b. **Description timestamps** (`0:00 Artist - Track`, `[12:34] Track`, etc.).
   c. **1001tracklists.com lookup** — searches the site by your set's title
      (stripped of noise words), fetches the first matching tracklist page,
      and parses cue times. Rate-limited (1 request / 3 s) and disk-cached, so
      we never re-fetch the same set. The scraper is brittle by nature — if
      the site's HTML shifts, this step gracefully falls through to Shazam.
      You are responsible for your own ToS compliance with the site.
   d. **ShazamIO fingerprinting** — slides a 30-second window across the audio,
      hits Shazam's public recognition endpoint per window, groups consecutive
      matches into segments. Slow (~1 min of recognition per 1 min of audio)
      and only finds chart/commercial music — underground and remixes mostly
      come back as gaps.
3. Splits the mix with ffmpeg stream-copy (no quality loss).
4. Tags each MP3 with ID3 (title, artist, album = set name, track number).
5. Writes named tracks to `~/Desktop/Music/<Set Name>/01 - Title - Artist.mp3`.
6. Gaps of ≥45 seconds with no identification go to
   `~/Desktop/Music/<Set Name>/_unidentified/HH-MM-SS_HH-MM-SS.mp3` so you can
   listen, identify by hand, and rename.

The tracklist parser assumes the standard "Artist - Title" convention for
description lines. If a particular tracklist uses "Title - Artist", the artist
and title fields end up swapped. Edit tags manually with a tool like
[MP3Tag](https://www.mp3tag.de/) if it matters.

## File layout (after install)

```
~/Library/Application Support/Songcatcher/
  songcatcherd.py             # daemon
  shazam_helper.py            # standalone Shazam recognition subprocess
  venv/                       # Daemon Python venv (FastAPI, Uvicorn, Pydantic, mutagen)
  shazam_venv/                # Isolated venv for shazamio (pydantic v1 dep)
  state.db                    # SQLite queue + history
  telegram.json               # bot token + whitelisted Telegram user IDs (chmod 600)
  work/                       # partial downloads + DJ-set sources during processing
~/Library/LaunchAgents/
  com.thehub.songcatcher.plist
~/Library/Logs/Songcatcher/
  daemon.log                  # app log
  daemon.stdout.log           # launchd stdout
  daemon.stderr.log           # launchd stderr
  jobs.log                    # one JSON record per job event
~/Library/Application Support/SwiftBar/Plugins/
  songcatcher.5s.sh
~/bin/songcatcher             # CLI
~/Desktop/Music/              # output
```

## HTTP API

The daemon binds to `127.0.0.1:7878` only (loopback). No auth needed because the
port isn't exposed off-machine.

- `POST /enqueue` — `{"input": "<url-or-query>", "important": false}`
- `GET /queue`
- `GET /jobs/<id>`
- `DELETE /jobs/<id>`
- `POST /shutdown`

## Troubleshooting

- **Daemon not responding** — `tail -100 ~/Library/Logs/Songcatcher/daemon.stderr.log`
- **Reload daemon** — `launchctl unload && launchctl load ~/Library/LaunchAgents/com.thehub.songcatcher.plist`
- **Telegram not receiving** — `cat ~/Library/Application\ Support/Songcatcher/telegram.json` (token + IDs present?). Re-run `./install.sh --register-telegram --force`.
- **SwiftBar item missing** — open SwiftBar.app → "Refresh All".
- **Bitrate check failing** — re-encoding fallback. yt-dlp's `--audio-quality 0` + `-b:a 320k` postprocessor flag forces CBR 320; if you see <320, the source may be very low quality and ffmpeg refused to upsample.

## Remote access from outside your home network

Telegram works from anywhere with no setup. The HTTP API is loopback-only by
design — if you want to hit it from another machine on your LAN, you'll need to
adjust `HTTP_HOST` in `songcatcherd.py` to `"0.0.0.0"` and add auth. Don't
expose it to the public internet.

## Legal

yt-dlp can fetch most non-DRM sources directly. Recording copyrighted audio you
don't own a license to may violate streaming-service ToS and copyright law
depending on jurisdiction. The tool itself is dual-use; the responsibility for
what you point it at is yours.

## Uninstall

```sh
./install.sh --uninstall
```

Leaves your music files and app data alone; remove
`~/Library/Application Support/Songcatcher` by hand if you want a clean wipe.
