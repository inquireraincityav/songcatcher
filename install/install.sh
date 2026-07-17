#!/bin/bash
# Songcatcher installer.
#
# Sets up the daemon under ~/Library/Application Support/Songcatcher,
# installs the launchd LaunchAgent, the SwiftBar plugin, and the CLI wrapper.
# Walks you through Telegram bot registration on first run.
#
# Usage:
#   ./install.sh                  full install
#   ./install.sh --register-telegram   re-run only the Telegram registration step
#   ./install.sh --uninstall      remove everything

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

APP_DIR="$HOME/Library/Application Support/Songcatcher"
LOG_DIR="$HOME/Library/Logs/Songcatcher"
MUSIC_DIR="$HOME/Desktop/Music"
LAUNCHAGENT_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCHAGENT_DIR/com.thehub.songcatcher.plist"
SWIFTBAR_DIR="$HOME/Library/Application Support/SwiftBar/Plugins"
CLI_LINK="$HOME/bin/songcatcher"

bold()  { printf "\033[1m%s\033[0m\n" "$1"; }
green() { printf "\033[32m%s\033[0m\n" "$1"; }
yellow(){ printf "\033[33m%s\033[0m\n" "$1"; }
red()   { printf "\033[31m%s\033[0m\n" "$1"; }

check_brew() {
    if ! command -v brew >/dev/null 2>&1; then
        red "Homebrew not found. Install from https://brew.sh and re-run."
        exit 1
    fi
}

check_required_bins() {
    for tool in ffmpeg yt-dlp; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            yellow "$tool not on PATH; installing via Homebrew..."
            brew install "$tool"
        fi
    done
}

setup_dirs() {
    bold "Creating directories…"
    mkdir -p "$APP_DIR" "$LOG_DIR" "$MUSIC_DIR" "$APP_DIR/work" "$LAUNCHAGENT_DIR" "$HOME/bin"
}

setup_venv() {
    bold "Creating Python venv at $APP_DIR/venv…"
    if [[ ! -d "$APP_DIR/venv" ]]; then
        /usr/bin/python3 -m venv "$APP_DIR/venv" 2>/dev/null \
            || python3 -m venv "$APP_DIR/venv"
    fi
    "$APP_DIR/venv/bin/pip" install --quiet --upgrade pip
    "$APP_DIR/venv/bin/pip" install --quiet \
        "fastapi>=0.110" "uvicorn>=0.27" "pydantic>=2.7" "mutagen>=1.47"
    green "Daemon Python deps installed."
}

setup_shazam_venv() {
    # shazamio pulls in pydantic v1, so it gets its own venv. The daemon shells
    # out to shazam_helper.py running in this venv when fingerprinting is needed.
    bold "Creating Shazam venv at $APP_DIR/shazam_venv…"
    if [[ ! -d "$APP_DIR/shazam_venv" ]]; then
        /usr/bin/python3 -m venv "$APP_DIR/shazam_venv" 2>/dev/null \
            || python3 -m venv "$APP_DIR/shazam_venv"
    fi
    "$APP_DIR/shazam_venv/bin/pip" install --quiet --upgrade pip 2>/dev/null || true
    "$APP_DIR/shazam_venv/bin/pip" install --quiet shazamio || {
        yellow "shazamio install failed — DJ-set fingerprinting fallback will be disabled."
        yellow "Chapter-marker and description-tracklist splitting still work without it."
        return 0
    }
    green "shazamio installed (isolated venv)."
}

copy_files() {
    bold "Copying daemon, plist, plugin, CLI…"
    cp "$PROJECT_DIR/daemon/songcatcherd.py" "$APP_DIR/songcatcherd.py"
    cp "$PROJECT_DIR/daemon/shazam_helper.py" "$APP_DIR/shazam_helper.py"
    chmod +x "$APP_DIR/shazam_helper.py"
    sed \
        -e "s|__APP_DIR__|$APP_DIR|g" \
        -e "s|__LOG_DIR__|$LOG_DIR|g" \
        "$PROJECT_DIR/launchd/com.thehub.songcatcher.plist" > "$PLIST_PATH"
    mkdir -p "$SWIFTBAR_DIR"
    cp "$PROJECT_DIR/swiftbar/songcatcher.5s.sh" "$SWIFTBAR_DIR/songcatcher.5s.sh"
    chmod +x "$SWIFTBAR_DIR/songcatcher.5s.sh"
    cp "$PROJECT_DIR/cli/songcatcher" "$CLI_LINK"
    chmod +x "$CLI_LINK"
}

install_swiftbar() {
    if [[ ! -d "/Applications/SwiftBar.app" ]]; then
        yellow "SwiftBar.app not found; installing via Homebrew cask…"
        brew install --cask swiftbar
    fi
}

register_telegram() {
    bold "Telegram bot registration"
    if [[ -f "$APP_DIR/telegram.json" ]] && [[ "${1:-}" != "--force" ]]; then
        green "telegram.json already exists. Use --register-telegram --force to redo."
        return 0
    fi
    cat <<EOF

1. Open Telegram and chat with @BotFather.
2. Send: /newbot
3. When BotFather asks for the bot name, send:  SJAIstudio
4. When BotFather asks for the username, send any unused username ending in "bot"
   (e.g. SJAIstudio_bot or SJAIstudio_<yourinitials>_bot).
5. Copy the bot token BotFather returns.

If you already have a bot and want to rename it: send /setname to BotFather,
pick the bot, and send  SJAIstudio  as the new name.

EOF
    read -r -p "Paste bot token: " TOKEN
    if [[ -z "$TOKEN" ]]; then
        red "No token; skipping Telegram setup."
        return 0
    fi

    cat <<EOF

5. Now open your bot in Telegram (BotFather gave you a link).
6. Send ANY message to your bot from your phone.

When you're done, press Enter to continue.
EOF
    read -r _

    yellow "Polling for your user ID..."
    response=$(curl -s "https://api.telegram.org/bot${TOKEN}/getUpdates")
    user_ids=$(/usr/bin/python3 -c "
import json, sys
d = json.loads(sys.argv[1])
seen = set()
for u in d.get('result', []):
    m = u.get('message') or {}
    f = m.get('from') or {}
    uid = f.get('id')
    if uid:
        name = (f.get('first_name','') + ' ' + f.get('last_name','')).strip() or f.get('username','?')
        seen.add((uid, name))
for uid, name in sorted(seen):
    print(f'{uid}\t{name}')
" "$response")

    if [[ -z "$user_ids" ]]; then
        red "No messages seen. Send a message to your bot, then re-run: $0 --register-telegram"
        return 1
    fi

    echo "Found user(s) who messaged your bot:"
    echo
    printf '   %-15s %s\n' "USER ID" "NAME"
    echo "$user_ids" | awk -F'\t' '{ printf "   %-15s %s\n", $1, $2 }'
    echo
    yellow "Paste the numeric USER ID (the digits in the left column),"
    yellow "not the name. Multiple IDs: comma-separated, e.g. 12345,67890"
    while true; do
        read -r -p "Allowed user ID(s): " ALLOWED
        if [[ -z "$ALLOWED" ]]; then
            red "No IDs entered; aborting."
            return 1
        fi
        if [[ "$ALLOWED" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
            break
        fi
        red "That doesn't look like numeric IDs. Use only digits (and commas for multiple)."
    done

    /usr/bin/python3 - <<PY
import json, os
ids = [int(x.strip()) for x in "$ALLOWED".split(',') if x.strip()]
cfg = {"token": "$TOKEN", "allowed_user_ids": ids}
path = os.path.expanduser("$APP_DIR/telegram.json")
with open(path, "w") as f:
    json.dump(cfg, f, indent=2)
os.chmod(path, 0o600)
print("Wrote", path)
PY
    green "Telegram registration complete."
}

load_launchd() {
    bold "Loading launchd agent…"
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    launchctl load "$PLIST_PATH"
    sleep 2
    if launchctl list | grep -q com.thehub.songcatcher; then
        green "Daemon loaded."
    else
        red "Daemon failed to load. Check $LOG_DIR/daemon.stderr.log"
        return 1
    fi
}

health_check() {
    bold "Health check…"
    sleep 1
    if curl -s --max-time 3 "http://127.0.0.1:7878/queue" >/dev/null; then
        green "Daemon responding on 127.0.0.1:7878"
    else
        yellow "Daemon not responding yet — give it a few seconds. Check $LOG_DIR/daemon.stderr.log"
    fi
}

uninstall() {
    bold "Uninstalling Songcatcher…"
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    rm -f "$PLIST_PATH" "$CLI_LINK" "$SWIFTBAR_DIR/songcatcher.5s.sh"
    yellow "App data preserved at $APP_DIR (delete manually if desired)."
    yellow "Music files preserved at $MUSIC_DIR."
    green "Uninstalled."
}

case "${1:-}" in
    --uninstall)
        uninstall
        ;;
    --register-telegram)
        shift
        register_telegram "${1:-}"
        # Restart daemon to pick up the new config
        launchctl unload "$PLIST_PATH" 2>/dev/null || true
        launchctl load "$PLIST_PATH" 2>/dev/null || true
        ;;
    "")
        check_brew
        check_required_bins
        setup_dirs
        setup_venv
        setup_shazam_venv
        copy_files
        install_swiftbar
        register_telegram || true
        load_launchd
        health_check
        echo
        green "Done. Try: songcatcher 'https://www.youtube.com/watch?v=dQw4w9WgXcQ'"
        ;;
    *)
        red "Unknown arg: $1"
        echo "Usage: $0 [--register-telegram [--force] | --uninstall]"
        exit 1
        ;;
esac
