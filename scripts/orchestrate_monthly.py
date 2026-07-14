#!/usr/bin/env python3
"""
Phase A monthly orchestrator CLI — paper only, generates order tickets a human
places. Wraps src.orchestrator.run_monthly and prints a readable run report.

Usage:
  DATABASE_URL=... DAWN_URL=... python3 scripts/orchestrate_monthly.py
  ... --dry-run                     # do everything except DB writes
  ... --as-of 2026-08-03            # form as of a specific date
  ... --notional 100000
  ... --live-prices /tmp/ltp.json   # {symbol: ltp} entry prices from Kite MCP
  ... --exit-prices /tmp/exit.json  # {symbol: ltp} to score the prior book
  ... --benchmark 1.2               # benchmark return pct for the scored book
  ... --json                        # emit the raw report as JSON
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import orchestrator


def _load(p):
    return json.loads(Path(p).read_text()) if p else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--notional", type=float, default=100_000.0)
    ap.add_argument("--live-prices", default=None, help="JSON {symbol: ltp} entry prices")
    ap.add_argument("--exit-prices", default=None, help="JSON {symbol: ltp} to score prior book")
    ap.add_argument("--benchmark", type=float, default=None, help="benchmark return pct")
    ap.add_argument("--dry-run", action="store_true", help="no DB writes")
    ap.add_argument("--quality", action="store_true",
                    help="add Tier-2 deep-fundamental quality/forensic screen on the picks")
    ap.add_argument("--all", action="store_true",
                    help="run the full pre-registered book set (Core + Trend + Quality)")
    ap.add_argument("--json", action="store_true", help="print raw JSON report")
    args = ap.parse_args()

    if args.all:
        combined = orchestrator.run_all_variants(
            as_of=args.as_of, notional=args.notional, dry_run=args.dry_run, with_quality=True)
        if args.json:
            print(json.dumps(combined, indent=2, default=str)); return
        r = combined["regime"]
        state = "RISK-ON" if r["risk_on"] else "RISK-OFF → CASH"
        print("=" * 72)
        print(f"PHASE A · ALL BOOKS{'  [DRY RUN]' if combined['dry_run'] else ''}  —  {combined['run_ts'][:19]}Z")
        print(f"regime [{r['ma_days']}d]: index {r['index_level']} vs MA {r['index_ma']} -> {state}")
        print("=" * 72)
        for rep in combined["variants"]:
            v = rep["variant"]; sig = rep["signal"]; rec = rep["recorded"]
            g = sig.get("quality_gate", {})
            rid = rec.get("portfolio_id") or f"exists id={rec.get('existing') or rec.get('existing_blocked')}"
            gate = f" | gate dropped {g['n_dropped']}" if g.get("applied") else ""
            print(f"  {v['label']:<14}{'★' if v['primary'] else ' '} exposure {sig['target_exposure']:.0%} "
                  f"holdings {sig['n_holdings']}{gate} -> {rid}")
        print("PAPER ONLY. Core is the pre-registered primary; Trend/Quality are attribution satellites.")
        return

    rep = orchestrator.run_monthly(
        as_of=args.as_of, notional=args.notional,
        live_prices=_load(args.live_prices), exit_prices=_load(args.exit_prices),
        benchmark_pct=args.benchmark, dry_run=args.dry_run, with_quality=args.quality,
    )

    if args.json:
        print(json.dumps(rep, indent=2, default=str))
        return

    f = rep["data_freshness"]
    sig = rep["signal"]
    reg = sig["regime"]
    orders = rep["order_tickets"]
    print("=" * 72)
    print(f"PHASE A MONTHLY ORCHESTRATOR{'  [DRY RUN]' if rep['dry_run'] else ''}  —  {rep['run_ts'][:19]}Z")
    print(f"model_version {rep['model_version']}")
    print("=" * 72)
    print(f"1. DATA: Dawn latest {f['latest_price_date']} ({f['rows']:,} rows), "
          f"stale {f['stale_days']}d -> {'STALE ⚠' if f['is_stale'] else 'ok'}")

    sp = rep["scored_prior"]
    if sp is None:
        print("2. SCORE: no prior book to score")
    elif "skipped" in sp:
        print(f"2. SCORE: prior book id={sp['portfolio_id']} skipped ({sp['skipped']})")
    elif sp.get("dry_run"):
        print(f"2. SCORE: prior book id={sp['portfolio_id']} would be scored (dry run)")
    elif sp is not None and sp.get("net_pct") is not None:
        print(f"2. SCORE: prior book id={sp['portfolio_id']} net {sp['net_pct']:+.2f}% "
              f"(gross {sp['gross_pct']:+.2f}%, excess "
              f"{sp['excess_pct'] if sp['excess_pct'] is not None else 'n/a'}, n={sp['n_scored']})")
    else:
        print(f"2. SCORE: {sp}")

    state = "RISK-ON (fully invested)" if reg["risk_on"] else "RISK-OFF → DE-RISK TO CASH"
    print(f"3. SIGNAL: as_of {sig['as_of']}  universe {sig['universe_size']}  holdings {sig['n_holdings']}")
    print(f"   trend overlay [{reg['ma_days']}d]: index {reg['index_level']} vs MA {reg['index_ma']} "
          f"-> {state} (exposure {sig['target_exposure']:.0%})")

    print(f"4. ORDER TICKETS: {orders['n_buy']} BUY, {orders['n_sell']} SELL, {orders['n_hold']} HOLD/REBAL "
          f"| cash ₹{orders['cash_value']:,.0f} ({orders['cash_weight']:.0%})")
    for t in orders["tickets"]:
        px = f"₹{t['ref_price']:.1f}" if t["ref_price"] else "n/a"
        val = f"₹{t['target_value']:,.0f}" if t["action"] != "SELL" else "—"
        print(f"   {t['action']:<9} {t['symbol']:<13} w {t['prev_weight']:.3f}->{t['target_weight']:.3f} "
              f"@ {px:>10}  target {val}")

    if "quality" in rep:
        q = rep["quality"]
        print(f"3b. QUALITY/FORENSIC SCREEN (Tier-2, advisory): {q['n_covered']}/{q['n_covered']+q['n_missing']} "
              f"with deep data, {q['n_flagged']} flagged")
        for sym, v in sorted(q["by_symbol"].items(), key=lambda kv: (kv[1].get("quality_score") is None,
                                                                     kv[1].get("quality_score") or 0)):
            if v.get("no_data"):
                print(f"   {sym:<13} (no deep data)")
                continue
            qs = f"{v['quality_score']:+.2f}" if v["quality_score"] is not None else " n/a"
            fin = "FIN " if v["financial"] else "    "
            flags = ("  ⚠ " + "; ".join(v["red_flags"])) if v["red_flags"] else ""
            print(f"   {sym:<13} {fin}q={qs} rank={v.get('quality_rank_pct')}{flags}")

    rec = rep["recorded"]
    if rec.get("dry_run"):
        print(f"5. RECORD: (dry run) — existing book for as_of: id={rec['existing']}")
    elif rec.get("portfolio_id"):
        print(f"5. RECORD: paper_portfolio id={rec['portfolio_id']}")
    else:
        print(f"5. RECORD: NOT recorded — book id={rec.get('existing_blocked')} already exists "
              f"(re-run with the runner's --force to override)")
    print("=" * 72)
    print("PAPER ONLY. Tickets are for the user to place manually; the agent places no live orders.")


if __name__ == "__main__":
    main()
