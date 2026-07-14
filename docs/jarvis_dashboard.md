# JARVIS — live HUD for the super-quant paper book

A self-contained, Jarvis-style dashboard over the BharatTwin strategy. Everything
it shows is REAL, pulled from the tested modules — no mock data.

## Run it
```bash
cd ~/Developer/bharattwin
DATABASE_URL=postgresql://localhost:5432/bharattwin \
DAWN_URL=postgresql://localhost:5432/dawn \
.venv/bin/python scripts/jarvis_server.py --port 7842 --refresh 120
# open http://localhost:7842
```
No web-framework dependency (Python stdlib `http.server`). A background thread
rebuilds the heavy snapshot every `--refresh` seconds; the browser polls
`/api/state` every 5s and re-renders. `Ctrl-C` to stop.

## What it shows
- **Strategy Vitals** — the validated numbers: Sharpe 1.19▸1.40 (overlay), maxDD
  -37.6%▸-19.4%, Deflated Sharpe 0.82, CPCV 100% OOS-positive, PBO 29%, momentum
  IC t 4.04, permutation p<0.001, est. alpha 4–6%.
- **Trend Regime (Tier-1.2)** — the live risk-on/off orb + target exposure, with
  the equal-weight index vs its 200d MA. Green ring = invested, red = de-risked.
- **Paper Book mark-to-market** — per-name and portfolio return, book id/notional.
- **Quality / Forensic (Tier-2)** — how many of the live picks carry accounting
  red flags, and which.
- **Holdings** — all names sorted by return, flagged names marked, quality bar.

## Data sources (all live)
- Regime, quality, freshness, signal ← one `orchestrator.run_monthly(dry_run=True,
  with_quality=True)` (nothing is written).
- Book ← latest `paper_portfolio` row.
- Marks ← Dawn latest close by default. For a TRUE intraday mark, drop live LTPs
  into `logs/live_ltp.json` as `{"SYMBOL": price, ...}` (e.g. fetched via the Kite
  MCP in an interactive session); the HUD picks them up automatically and flips the
  "MARKS" badge to LIVE. Dawn closes are EOD and often stale intraday, so without
  that file the P&L reflects the last official close, labelled honestly.

## Notes
- Read-only, PAPER ONLY — the server never places or modifies orders.
- Binds to 127.0.0.1 (localhost) only.
- Also launchable via the Browser pane: `preview_start {name: "jarvis"}`
  (see `.claude/launch.json`).
