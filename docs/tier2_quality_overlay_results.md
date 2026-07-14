# TIER 2 — Deep-Fundamental Quality / Forensic Overlay (LIVE)

> Built 2026-07-14. `src/quality.py` + `--quality` in the Phase A orchestrator.
> This is a **live current-snapshot overlay, not a backtested factor.**

## Why live-only (the data reality)
Dawn's deep fields (revenue, pat, operating_profit, cash_from_ops, total_assets,
total_debt, net_worth) are populated for essentially **one annual snapshot** —
period_end 2026-03-31, ~1,083 symbols. There is no quarterly history (older rows
carry only eps/bvps/shares). So a deep-fundamental factor cannot be *backtested*
here; it can only *annotate the current picks*. A properly backtested quality/
forensic sleeve still needs paid history (PROJECT_STATUS §7 Tier 3: Prowess/
Capitaline). This overlay is the free, do-it-now slice of that ambition.

## What it computes (per name, from the snapshot)
- **Quality score** = cross-sectional z-mean of ROA, operating-profitability
  (op_profit/assets), pre-tax ROIC (op_profit/(debt+equity)), −accruals
  ((pat−cfo)/assets, Sloan), and cash conversion (CFO/PAT). Financials are scored
  on ROA only (their CFO/accruals/leverage are not comparable).
- **Forensic red flags:**
  - `PAT > revenue` — earnings driven by an exceptional/one-off item, not operations
  - `negative net worth` — distressed balance sheet
  - `negative operating cash flow` — earnings not backed by cash *(non-financials)*
  - `weak cash conversion (CFO/PAT < 0.5)` *(non-financials)*
  - `high leverage (D/E > 3)` *(non-financials)*
- **Sector guard:** banks/NBFCs/insurers ("Financial Services", via
  `security_sector.nse_industry`) suppress the cash-flow/leverage flags, whose
  accounting meaning doesn't transfer. Sector-agnostic flags (PAT>rev, negative
  net worth) still fire.

## Result on the live 25-name book (as_of 2026-07-03)
**9 of 25 flagged.** The overlay does real work — it catches momentum names whose
run is accounting-driven rather than operational:

| Pick | q-rank | Flag(s) — why momentum here is suspect |
|---|---|---|
| RAYMOND | 21% | PAT > revenue + CFO/PAT=0.01 → the run is a demerger/one-off, not earnings |
| IDEA | 11% | negative net worth → distressed; the big PAT is a one-off, not operations |
| DIACABS | 12% | negative net worth + negative CFO |
| GMRAIRPORT | 39% | negative net worth |
| V2RETAIL, FCL | 14/29% | negative operating cash flow |
| ASHOKA, CUPID | 34/88% | weak cash conversion |
| TATAINVEST | 21% | PAT > revenue (holding co — investment income; sector-nuanced) |

Cleanest momentum names (high quality, no flags): WEBELSOLAR (97%), CHENNPETRO,
BPCL, SANDUMA, AIIL, HINDPETRO.

## How it's wired
`scripts/orchestrate_monthly.py --quality` adds a "3b. QUALITY/FORENSIC SCREEN"
section to the monthly run, sorted worst→best, so a human sees which holdings are
quality-suspect before placing tickets. **Advisory only** — it does not auto-drop
names (with one snapshot and no backtest, hard exclusion isn't justified). The
natural next step, once history exists, is to turn the cleanest flags into a hard
gate and to backtest a quality tilt.

## Honest limits
- One snapshot → point-in-time only, no trend (can't see *deteriorating* quality).
- No tax rate → ROIC is pre-tax; no working-capital detail → accruals are coarse.
- Full Piotroski/Beneish need prior-year values we don't have.
- Financial-sector scoring is ROA-only — thin, but honest.
- This corroborates the guide's "quality is the strongest untapped edge" while
  confirming the wall is **data history**, not method (PROJECT_STATUS §6).
