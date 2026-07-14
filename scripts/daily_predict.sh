#!/bin/bash
# BharatTwin Daily Prediction Pipeline
# Run on trading days (schedule to your preferred pre-market / post-close slot).
#
# Crontab (path is resolved relative to this script, so just point at it):
#   30 2 * * 1-5 /Users/abhishekpatil/Developer/bharattwin/scripts/daily_predict.sh >> /Users/abhishekpatil/Developer/bharattwin/logs/daily.log 2>&1
#
# SECRETS ARE NOT STORED IN THIS FILE. Put them in <repo>/.env (gitignored):
#   LLM_PROVIDER=sarvam
#   SARVAM_API_KEY=...        # rotate at console.sarvam.ai — never commit
#   DATABASE_URL=postgresql://bharattwin:devpassword@localhost:5432/bharattwin

set -euo pipefail

# Resolve repo root relative to this script — robust to checkout location.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Load config/secrets from .env if present (gitignored — real keys never committed).
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env"
  set +a
fi

: "${LLM_PROVIDER:=sarvam}"
export LLM_PROVIDER

if [ -z "${SARVAM_API_KEY:-}" ] && [ -z "${ANTHROPIC_API_KEY:-}" ] && [ -z "${OPENAI_API_KEY:-}" ]; then
  echo "ERROR: no LLM API key set (SARVAM_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY)." >&2
  echo "       Add one to $REPO_ROOT/.env" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/logs"

DATE=$(python3 -c "
from datetime import date, timedelta
d = date.today()
# If weekend, use Friday
if d.weekday() == 5: d -= timedelta(1)
if d.weekday() == 6: d -= timedelta(2)
print(d)
")

echo "============================================"
echo "BharatTwin Daily Pipeline — $DATE"
echo "$(date)"
echo "============================================"

# Step 1: Run pipeline (build state + run agents)
echo "Running pipeline for $DATE..."
python3 -m src.pipeline "$DATE" --mode api --with-event 2>&1

# Step 2: Score yesterday's prediction (if exists)
YESTERDAY=$(python3 -c "
from datetime import date, timedelta
d = date.today() - timedelta(1)
if d.weekday() == 5: d -= timedelta(1)
if d.weekday() == 6: d -= timedelta(2)
print(d)
")
echo "Scoring $YESTERDAY..."
python3 -m src.scoring "$YESTERDAY" 2>&1 || echo "No prediction to score for $YESTERDAY"

echo "Done at $(date)"
