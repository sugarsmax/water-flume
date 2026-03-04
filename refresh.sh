#!/usr/bin/env bash
# Fetch latest Flume water data, regenerate the chart, and push to GitHub.
# Designed to be run by cron. Logs to refresh.log in the same directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="/Users/maxfiep/.python-venvs/pdms-shared/bin/python"
LOG="$SCRIPT_DIR/refresh.log"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

log() {
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG"
}

log "=== refresh started ==="

if $DRY_RUN; then
  log "Dry-run mode — no API calls or git operations."
  "$PYTHON" "$SCRIPT_DIR/flume_client_20260303.py" --monthly --dry-run
  exit 0
fi

# Fetch monthly data and regenerate index.html
"$PYTHON" "$SCRIPT_DIR/flume_client_20260303.py" --monthly 2>&1 | tee -a "$LOG"

# Also append today's daily data
"$PYTHON" "$SCRIPT_DIR/flume_client_20260303.py" 2>&1 | tee -a "$LOG"

# Commit and push if anything changed
cd "$SCRIPT_DIR"

if git diff --quiet && git diff --staged --quiet; then
  log "Nothing changed — skipping commit."
else
  git add water_usage.csv water_usage_monthly.csv index.html
  git commit -m "Auto-update: $(date -u '+%Y-%m-%d')"
  git push
  log "Pushed to GitHub."
fi

log "=== refresh complete ==="
