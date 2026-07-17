#!/bin/bash
# install-recording.sh — opt-in fallback for true playback recording (rarely needed).
#
# Most inputs are handled by yt-dlp direct-download. The recording path is for
# DRM-protected sources yt-dlp can't reach (Apple Music, DRM'd streams). It
# requires installing BlackHole and manually setting up a Multi-Output Device
# in macOS so you can still hear playback while it's being captured.

set -euo pipefail

bold()  { printf "\033[1m%s\033[0m\n" "$1"; }
yellow(){ printf "\033[33m%s\033[0m\n" "$1"; }
green() { printf "\033[32m%s\033[0m\n" "$1"; }

bold "Installing BlackHole 2ch via Homebrew cask…"
brew install --cask blackhole-2ch

cat <<'EOF'

BlackHole installed. You must now create a Multi-Output Device by hand so audio
plays through both your speakers AND BlackHole (so the recorder hears it).

  1. Open "Audio MIDI Setup" (cmd-space → "Audio MIDI Setup").
  2. Click the "+" in the lower-left → "Create Multi-Output Device".
  3. Tick BOTH "Built-in Output" (or your headphones) AND "BlackHole 2ch".
  4. Right-click the new device → "Use This Device For Sound Output".
  5. In the same window, set Master Device = your speakers/headphones, and
     check "Drift Correction" on BlackHole 2ch.

When you're done, this script enables the recording fallback in the daemon.

EOF
read -r -p "Press Enter when the Multi-Output Device is set up… " _

touch "$HOME/Desktop/Music/.recording_enabled"
green "Recording fallback enabled. The daemon will use it when yt-dlp can't fetch a source directly."
yellow "Note: the daemon code for the recording fallback is scaffolded but not fully wired yet — first time you hit a source that needs it, we'll finish that path."
