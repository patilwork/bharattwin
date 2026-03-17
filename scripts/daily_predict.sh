#!/bin/bash
# BharatTwin Daily Prediction Pipeline
# Run at 7:00 PM IST (after market close + bhavcopy available)
#
# Crontab entry:
#   30 13 * * 1-5 /Users/abhishekpatil/Developer/niftwin/bharattwin/scripts/daily_predict.sh >> /Users/abhishekpatil/Developer/niftwin/bharattwin/logs/daily.log 2>&1
#   (13:30 UTC = 19:00 IST)

set -e

export LLM_PROVIDER=sarvam
export SARVAM_API_KEY=sk_02dpyejl_HG1N0ShzyBb86WLV7Bu54nNo
export DATABASE_URL=postgresql://bharattwin:devpassword@localhost:5434/bharattwin

cd /Users/abhishekpatil/Developer/niftwin/bharattwin

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
