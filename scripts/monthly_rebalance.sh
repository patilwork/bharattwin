#!/bin/bash
# BharatTwin Phase A — monthly paper-track orchestrator (PAPER ONLY).
# Scores last month's book, forms the new momentum+value book with the Tier-1.2
# trend overlay, and emits order tickets for the USER to place. Places NO live orders.
#
# Crontab (first trading day of the month, ~post-close; path resolves relative to
# this script, so just point cron at it):
#   30 18 1-4 * * /Users/abhishekpatil/Developer/bharattwin/scripts/monthly_rebalance.sh >> /Users/abhishekpatil/Developer/bharattwin/logs/monthly.log 2>&1
#   (1-4 + a weekday guard below approximates "first business day"; refine as needed.)
#
# SECRETS live in <repo>/.env (gitignored). DATABASE_URL / DAWN_URL required.
# Optionally pass live Kite LTPs by writing them to the JSON paths below before the run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env"
  set +a
fi

: "${DATABASE_URL:=postgresql://localhost:5432/bharattwin}"
: "${DAWN_URL:=postgresql://localhost:5432/dawn}"
export DATABASE_URL DAWN_URL

# Prefer the repo venv if present, else system python3.
PY="$REPO_ROOT/.venv/bin/python"
[ -x "$PY" ] || PY="python3"

mkdir -p "$REPO_ROOT/logs"

echo "============================================"
echo "BharatTwin Phase A monthly rebalance — $(date)"
echo "============================================"

# Step 0: refresh the Dawn price panel from NSE bhavcopy so the signal is current
# (turns the data-freshness "gate" into a real fetch). Non-fatal if it fails —
# the orchestrator still runs and flags staleness.
echo "Refreshing Dawn price panel ..."
"$PY" scripts/refresh_dawn_prices.py || echo "WARN: price refresh failed; continuing on existing panel"

# Step 0.5: refresh the Dawn FUNDAMENTALS panel (NSE integrated filing) so the VALUE
# leg picks up new quarterly reports (e.g. Q1 FY27). skip_fresh_days keeps it cheap
# mid-quarter and only fetches new filings during earnings season. Slow (NSE API per
# symbol), non-fatal. PIT lag (90d) means new quarters reach the signal ~1 rebalance later.
echo "Refreshing Dawn fundamentals (NSE integrated filing) ..."
"$PY" scripts/refresh_dawn_fundamentals.py --skip-fresh-days 90 || echo "WARN: fundamentals refresh failed; continuing on existing fundamentals"

# --dry-run by default is NOT used here: the scheduled job records the paper book.
# To preview without writing, run manually with --dry-run.
# Live Kite LTPs (optional): if scripts fetched them to these files, pass them through.
EXTRA=()
[ -f "$REPO_ROOT/logs/entry_ltp.json" ] && EXTRA+=(--live-prices "$REPO_ROOT/logs/entry_ltp.json")
[ -f "$REPO_ROOT/logs/exit_ltp.json" ]  && EXTRA+=(--exit-prices "$REPO_ROOT/logs/exit_ltp.json")

"$PY" scripts/orchestrate_monthly.py ${EXTRA[@]+"${EXTRA[@]}"}

echo "Done at $(date)"
