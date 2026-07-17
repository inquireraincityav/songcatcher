#!/bin/bash
# SwiftBar plugin for Songcatcher.
# Refreshes every 5 s. Shows in-flight job + pending queue, with a "New job…" prompt.
#
# <bitbar.title>Songcatcher</bitbar.title>
# <bitbar.version>0.1</bitbar.version>
# <bitbar.author>thehub</bitbar.author>
# <bitbar.desc>Local audio-fetching daemon</bitbar.desc>

API="http://127.0.0.1:7878"
SELF="$0"

# Sub-commands: prompt the user and POST a new job.
case "$1" in
    new)
        result=$(/usr/bin/osascript <<'OSA'
try
    set dialogResult to display dialog "Link or song:" default answer "" with title "Songcatcher" buttons {"Cancel", "Important", "Queue"} default button "Queue"
    set userInput to text returned of dialogResult
    set buttonPicked to button returned of dialogResult
    return userInput & "||" & buttonPicked
on error
    return ""
end try
OSA
)
        [[ -z "$result" ]] && exit 0
        input="${result%||*}"
        button="${result##*||}"
        [[ -z "$input" ]] && exit 0
        important="false"
        [[ "$button" == "Important" ]] && important="true"
        # Build JSON safely
        payload=$(/usr/bin/python3 -c "import json,sys; print(json.dumps({'input': sys.argv[1], 'important': sys.argv[2]=='true'}))" "$input" "$important")
        /usr/bin/curl -s -X POST "$API/enqueue" \
            -H 'Content-Type: application/json' \
            -d "$payload" > /dev/null
        exit 0
        ;;
    cancel)
        [[ -z "$2" ]] && exit 0
        /usr/bin/curl -s -X DELETE "$API/jobs/$2" > /dev/null
        exit 0
        ;;
    open-music)
        /usr/bin/open "$HOME/Desktop/Music"
        exit 0
        ;;
    open-log)
        /usr/bin/open "$HOME/Library/Logs/Songcatcher/daemon.log"
        exit 0
        ;;
esac

# Default: render menu bar.
queue_json=$(/usr/bin/curl -s --max-time 2 "$API/queue" 2>/dev/null || echo '{}')

if [[ -z "$queue_json" || "$queue_json" == "{}" ]]; then
    echo "♪ ?"
    echo "---"
    echo "Daemon unreachable | color=red"
    echo "Open log | bash='$SELF' param1=open-log terminal=false"
    exit 0
fi

# Use python for safe JSON parsing
parsed=$(/usr/bin/python3 - "$queue_json" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
inf = d.get("in_flight")
pending = d.get("pending") or []
if inf:
    title = (inf.get("title") or inf.get("input") or "")[:40]
    state = inf.get("state", "?")
    print(f"STATUS:♪ {state} — {title}")
else:
    print("STATUS:♪ idle")
print(f"COUNT:{len(pending)}")
for j in pending[:8]:
    star = "⭐ " if j.get("important") else ""
    txt = (j.get("title") or j.get("input") or "")[:48]
    print(f"JOB:{j['id']}|{star}{txt}")
PY
)

status_line=$(echo "$parsed" | grep '^STATUS:' | head -1 | sed 's/^STATUS://')
count=$(echo "$parsed" | grep '^COUNT:' | head -1 | sed 's/^COUNT://')
jobs=$(echo "$parsed" | grep '^JOB:' | sed 's/^JOB://')

# Header: status + count if any pending
if [[ -n "$count" && "$count" -gt 0 ]]; then
    echo "$status_line (+$count)"
else
    echo "$status_line"
fi

echo "---"
echo "New job… | bash='$SELF' param1=new terminal=false refresh=true"
echo "---"

if [[ -n "$count" && "$count" -gt 0 ]]; then
    echo "Pending ($count):"
    IFS=$'\n'
    for job_line in $jobs; do
        jid="${job_line%%|*}"
        rest="${job_line#*|}"
        echo "$rest | size=11"
        echo "-- Cancel #$jid | bash='$SELF' param1=cancel param2=$jid terminal=false refresh=true"
    done
    unset IFS
    echo "---"
fi

echo "Open Music folder | bash='$SELF' param1=open-music terminal=false"
echo "Open log | bash='$SELF' param1=open-log terminal=false"
echo "Refresh | refresh=true"
