#!/usr/bin/env python3
"""
Refresh the Dawn FUNDAMENTALS panel from NSE Integrated Filing — the piece that keeps
the VALUE leg (E/P, P/B) current as companies report new quarters (e.g. Q1 FY27).

The price refresh (refresh_dawn_prices.py) only updates prices; this is its sibling for
fundamentals. It reuses the Dawn SOURCE repo's fetcher (~/Developer/Weekly, calc.fetchers)
which handles the NSE integrated-filing API (cookie warm-up), the iXBRL download, XBRL
parsing, and the upsert into security_fundamentals — the same code that built the panel.

Point-in-time is preserved downstream: compute_portfolio only uses a fundamental once
`period_end + REPORT_LAG_DAYS (90) <= rebalance date`, so a Q1 FY27 filing (period_end
2026-06-30) becomes usable ~2026-09-28 — no look-ahead. This job just gets the data IN.

It fetches for the liquid universe (₹2000cr+) and skips symbols whose stored filing is
still fresh (skip_fresh_days), so mid-quarter it's cheap and during earnings season it
picks up new filings as they land.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/refresh_dawn_fundamentals.py
  ... --skip-fresh-days 90     # re-check symbols whose latest filing is older than this
  ... --limit 20               # cap universe (testing)
  ... --dry-run                # list the universe + counts, fetch nothing
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

DAWN_URL = os.environ.get("DAWN_URL", os.environ.get("DATABASE_URL", "postgresql://localhost:5432/dawn"))
WEEKLY = os.environ.get("WEEKLY_REPO", str(Path.home() / "Developer" / "Weekly"))
MIN_MCAP_CR = 2000.0


def liquid_universe(limit: int | None) -> list[str]:
    """Symbols that traded recently and clear the ₹2000cr liquidity floor — the names
    the value leg actually ranks. mcap = latest close x shares_outstanding."""
    eng = create_engine(DAWN_URL)
    try:
        with eng.connect() as c:
            rows = c.execute(text("""
                WITH last_px AS (
                  SELECT DISTINCT ON (symbol) symbol, close
                  FROM stock_prices_daily
                  WHERE date::date >= (SELECT max(date::date) FROM stock_prices_daily) - INTERVAL '15 days'
                    AND close > 0 AND symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'
                  ORDER BY symbol, date::date DESC
                ),
                shr AS (
                  SELECT DISTINCT ON (symbol) symbol, shares_outstanding
                  FROM security_fundamentals WHERE shares_outstanding > 0
                  ORDER BY symbol, period_end DESC
                )
                SELECT p.symbol
                FROM last_px p JOIN shr s USING (symbol)
                WHERE p.close * s.shares_outstanding / 1e7 >= :floor
                ORDER BY p.close * s.shares_outstanding DESC
            """), {"floor": MIN_MCAP_CR}).fetchall()
    finally:
        eng.dispose()
    syms = [r[0] for r in rows]
    return syms[:limit] if limit else syms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-fresh-days", type=int, default=90)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not Path(WEEKLY, "calc", "fetchers.py").exists():
        sys.exit(f"Dawn source repo not found at {WEEKLY} (set WEEKLY_REPO). "
                 "The fundamentals fetcher lives there.")
    sys.path.insert(0, WEEKLY)
    os.environ["DATABASE_URL"] = DAWN_URL          # Weekly's store reads DATABASE_URL

    syms = liquid_universe(args.limit)
    print(f"liquid universe (₹{MIN_MCAP_CR:.0f}cr+): {len(syms)} symbols | skip_fresh_days="
          f"{args.skip_fresh_days}{'  [DRY RUN]' if args.dry_run else ''}")
    # current fundamentals recency
    eng = create_engine(DAWN_URL)
    with eng.connect() as c:
        mx = c.execute(text("SELECT max(period_end) FROM security_fundamentals WHERE ttm_eps IS NOT NULL")).scalar()
    eng.dispose()
    print(f"latest fundamental period_end in panel: {mx}")
    if args.dry_run:
        print("sample:", syms[:10]); return

    from calc import store, fetchers
    con = store.connect()
    print(f"fetching new integrated filings (this hits the NSE API per symbol — slow) ...")
    res = fetchers.fetch_fundamentals_bulk(con, syms, skip_fresh_days=args.skip_fresh_days)
    if hasattr(con, "commit"):
        con.commit()

    # report — fetch_fundamentals_bulk returns (fetched, skipped, failed)
    fetched = skipped = failed = 0
    if isinstance(res, (list, tuple)) and len(res) == 3:
        fetched, skipped, failed = (len(x) for x in res)
    elif isinstance(res, dict):
        fetched, skipped, failed = (len(res.get(k, [])) for k in ("fetched", "skipped", "failed"))
    eng = create_engine(DAWN_URL)
    with eng.connect() as c:
        mx2 = c.execute(text("SELECT max(period_end) FROM security_fundamentals WHERE ttm_eps IS NOT NULL")).scalar()
        q1 = c.execute(text("SELECT count(*) FROM security_fundamentals WHERE period_end='2026-06-30' AND ttm_eps IS NOT NULL")).scalar()
    eng.dispose()
    print(f"done — fetched {fetched}, skipped(fresh) {skipped}, failed {failed}. "
          f"latest period_end now: {mx2} | Q1-FY27 (2026-06-30) rows with EPS: {q1}")


if __name__ == "__main__":
    main()
