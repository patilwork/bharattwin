#!/usr/bin/env python3
"""
Refresh the Dawn daily price panel from NSE bhavcopy — the piece that lets the
SIGNAL advance past a stale snapshot.

Source: NSE UDiFF daily bhavcopy (one zip per trading day, ALL equities) —
  https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_<YYYYMMDD>_F_0000.csv.zip
This is the same NSE-native source Dawn's own fetcher uses (calc/prices.py), so
the refreshed rows are consistent with history. One HTTP download per day beats
thousands of per-symbol API calls.

It appends every missing trading day between the panel's current max date and the
target date (default: today), upserting raw closes into stock_prices_daily. Missing
files (weekends/holidays) 404 and are skipped. Idempotent (ON CONFLICT DO UPDATE).

CAVEAT: closes are raw/unadjusted (matching the existing panel — the backtest
adjusts via corporate_actions at read time). Splits/bonuses with an ex-date INSIDE
the refresh window are not auto-adjusted here; rare over a short window, but re-run
a corporate-actions refresh if a held name had one.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/refresh_dawn_prices.py
  ... --to 2026-07-14        # target end date (default: today)
  ... --dry-run              # fetch + parse + report, no DB writes
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta

from sqlalchemy import create_engine, text

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")
ARCHIVE = ("https://nsearchives.nseindia.com/content/cm/"
           "BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
SERIES_KEEP = {"EQ", "BE"}


def _pick(cols: dict, *needles):
    for lc, orig in cols.items():
        if all(n in lc for n in needles):
            return orig
    return None


def parse_bhavcopy_csv(csv_text: io.TextIOBase, fallback_date: str) -> list[dict]:
    """Parse a bhavcopy CSV (legacy or UDiFF) into rows [{isin,symbol,close,date}],
    keeping only EQ/BE series with a valid ISIN, symbol and positive close. Pure —
    no network. Column names are matched by alias so both formats work."""
    reader = csv.DictReader(csv_text)
    cols = {c.lower().strip(): c for c in (reader.fieldnames or [])}
    sym_c = _pick(cols, "tckrsymb") or _pick(cols, "symbol")
    close_c = _pick(cols, "clspric") or _pick(cols, "close")
    isin_c = _pick(cols, "isin")
    series_c = _pick(cols, "sctysrs") or _pick(cols, "series")
    trad_c = _pick(cols, "traddt")
    rows = []
    for r in reader:
        series = (r.get(series_c) or "").strip().upper() if series_c else "EQ"
        if series not in SERIES_KEEP:
            continue
        isin = (r.get(isin_c) or "").strip() if isin_c else ""
        sym = (r.get(sym_c) or "").strip().upper()
        try:
            close = float(str(r.get(close_c)).replace(",", ""))
        except (TypeError, ValueError):
            continue
        if not (isin and sym) or close <= 0:
            continue
        td = (r.get(trad_c) or "").strip()[:10] if trad_c else fallback_date
        rows.append({"isin": isin, "symbol": sym, "close": close, "date": td or fallback_date})
    return rows


def fetch_bhavcopy(d: date) -> list[dict] | None:
    """Download+parse one day's bhavcopy. Returns rows or None if the day has no
    file (weekend/holiday)."""
    url = ARCHIVE.format(ymd=d.strftime("%Y%m%d"))
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            blob = resp.read()
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return None
        raise
    zf = zipfile.ZipFile(io.BytesIO(blob))
    name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
    if not name:
        return None
    return parse_bhavcopy_csv(io.TextIOWrapper(zf.open(name), encoding="utf-8-sig"), d.isoformat())


def upsert(eng, rows: list[dict]) -> int:
    if not rows:
        return 0
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO stock_prices_daily (isin, date, close, symbol)
            VALUES (:isin, :date, :close, :symbol)
            ON CONFLICT (isin, date) DO UPDATE SET close = EXCLUDED.close, symbol = EXCLUDED.symbol
        """), rows)
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--to", default=None, help="target end date YYYY-MM-DD (default: today)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    eng = create_engine(DAWN_URL)
    with eng.connect() as conn:
        cur_max = conn.execute(text("SELECT max(date) FROM stock_prices_daily")).scalar()
    start = datetime.strptime(str(cur_max)[:10], "%Y-%m-%d").date() + timedelta(days=1)
    end = datetime.strptime(args.to, "%Y-%m-%d").date() if args.to else date.today()

    print(f"Dawn panel currently ends {cur_max}. Refreshing {start} → {end}"
          f"{'  [DRY RUN]' if args.dry_run else ''} ...")
    if start > end:
        print("already current — nothing to do."); return

    total, days = 0, 0
    d = start
    while d <= end:
        if d.weekday() >= 5:                       # skip Sat/Sun without a network call
            d += timedelta(days=1); continue
        rows = fetch_bhavcopy(d)
        if rows is None:
            print(f"  {d}  — no file (holiday?), skipped")
        else:
            n = len(rows) if args.dry_run else upsert(eng, rows)
            total += n; days += 1
            print(f"  {d}  {n:>5} rows{' (dry)' if args.dry_run else ' upserted'}")
        d += timedelta(days=1)

    eng.dispose()
    with create_engine(DAWN_URL).connect() as conn:
        new_max = conn.execute(text("SELECT max(date) FROM stock_prices_daily")).scalar()
    print(f"done — {days} trading day(s), {total} rows. Panel now ends {new_max}"
          f"{' (unchanged, dry run)' if args.dry_run else ''}.")


if __name__ == "__main__":
    main()
