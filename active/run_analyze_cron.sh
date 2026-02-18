#!/usr/bin/env bash
set -euo pipefail

unset BASH_ENV
unset ENV

ROOT="/media/sf_masaos_mov"
SCRIPT="/media/sf_REC/scripts/analyze_y2_events.py"
LOCK="/tmp/masao_analyze.lock"
LOG="/media/sf_REC/logs/analyze_cron.log"

mkdir -p /media/sf_REC/logs

flock -n "$LOCK" /usr/bin/env -i PATH=/usr/bin:/bin /usr/bin/bash -c '
  set -euo pipefail

  ROOT="$1"
  SCRIPT="$2"
  LOG="$3"

  echo "[ENV] ROOT=$ROOT SCRIPT=$SCRIPT BASH=${BASH-} VERSION=${BASH_VERSION-}" >> "$LOG"

  session_dir="$(
    find "$ROOT" -maxdepth 1 -type d -name "20*" \
      -exec test -f "{}/proxy_360.mp4" \; \
      -exec test ! -e "{}/logs/.analyze_done" \; \
      -print 2>/dev/null \
    | sort | head -n 1
  )"

  [[ -z "$session_dir" ]] && exit 0

  s1=$(stat -c "%s" "$session_dir/proxy_360.mp4" || echo 0)
  sleep 1
  s2=$(stat -c "%s" "$session_dir/proxy_360.mp4" || echo 0)

  if [[ "$s1" -ne "$s2" ]]; then
    echo "[SKIP] growing: $session_dir" >> "$LOG"
    exit 0
  fi

  echo "[RUN] $session_dir" >> "$LOG"
  python3 "$SCRIPT" --session-dir "$session_dir" >> "$LOG" 2>&1

  mkdir -p "$session_dir/logs"
  date +"%F %T" > "$session_dir/logs/.analyze_done"

  echo "[DONE] $session_dir" >> "$LOG"
' _ "$ROOT" "$SCRIPT" "$LOG"
