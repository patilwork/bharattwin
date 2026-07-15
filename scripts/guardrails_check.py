#!/usr/bin/env python3
"""
Phase B guardrails audit — check every live paper book (and a freshly-proposed one)
against the pre-registered risk limits, and manage the kill-switch. PAPER-SAFE:
reports only; blocks recording; never touches a broker.

Usage:
  DATABASE_URL=... DAWN_URL=... python3 scripts/guardrails_check.py          # audit live books
  ... --reset-killswitch     # clear a tripped latch (human action)
  ... --json                 # raw JSON
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sqlalchemy import create_engine, text

from src import guardrails, orchestrator, papertrack


def live_books() -> list[dict]:
    eng = create_engine(papertrack.BHARAT_URL)
    try:
        with eng.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, strategy, as_of_date, universe_size, holdings, params "
                "FROM paper_portfolio ORDER BY id")).fetchall()
    finally:
        eng.dispose()
    out = []
    for r in rows:
        holds = r[4] if isinstance(r[4], list) else json.loads(r[4])
        params = r[5] if isinstance(r[5], (dict, type(None))) else json.loads(r[5])
        # reconstruct the minimal pf shape evaluate() needs
        gross = sum(float(h.get("weight", 0) or 0) for h in holds)
        out.append({"id": r[0], "strategy": r[1], "as_of": str(r[2]),
                    "universe_size": r[3], "holdings": holds,
                    "target_exposure": gross, "params": params or {}})
    return out


def mtm_ret(book: dict) -> float | None:
    """Current loss/gain vs entry using latest Dawn close (for the drawdown check)."""
    syms = [h["symbol"] for h in book["holdings"]]
    if not syms:
        return None
    eng = create_engine(orchestrator.DAWN_URL)
    try:
        with eng.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ON (symbol) symbol, close FROM stock_prices_daily "
                "WHERE symbol = ANY(:s) AND close>0 ORDER BY symbol, date DESC"),
                {"s": syms}).fetchall()
    finally:
        eng.dispose()
    close = {r[0]: float(r[1]) for r in rows}
    num = den = 0.0
    for h in book["holdings"]:
        ep, mk, w = h.get("entry_price"), close.get(h["symbol"]), float(h.get("weight", 0) or 0)
        if ep and mk and w:
            num += w * (mk - ep) / ep * 100; den += w
    return num / den if den else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset-killswitch", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.reset_killswitch:
        print("kill-switch:", guardrails.reset_killswitch()); return

    fresh = orchestrator.data_freshness()
    sec = guardrails.load_sector_map()
    ks = guardrails.read_killswitch()

    results = []
    for b in live_books():
        ret = mtm_ret(b)
        r = guardrails.evaluate(b, fresh, sec, book_ret_pct=ret)
        results.append({"id": b["id"], "strategy": b["strategy"], "ret_pct": ret, **r})

    if args.json:
        print(json.dumps({"freshness": fresh, "killswitch": ks, "books": results}, default=str, indent=2)); return

    print("=" * 78)
    print(f"PHASE B GUARDRAILS AUDIT  —  data {fresh['latest_price_date']} "
          f"({fresh['stale_days']}d stale)  |  kill-switch: "
          f"{'TRIPPED — ' + ks.get('reason','') if ks.get('tripped') else 'clear'}")
    print("=" * 78)
    for r in results:
        line = f"  book #{r['id']:<2} {r['strategy']:<30} {r['status']:<5} MTM {r['ret_pct']:+.2f}%" \
               if r["ret_pct"] is not None else f"  book #{r['id']:<2} {r['strategy']:<30} {r['status']:<5}"
        print(line)
        for c in r["blocking"] + r["warnings"]:
            print(f"       {c['severity']:<5} {c['code']:<20} {c['msg']}")
    print("-" * 78)
    anyblock = any(not r["ok_to_record"] for r in results)
    print("VERDICT:", "one or more books would be BLOCKED from (re)recording" if anyblock
          else "all live books within risk limits")
    print("Limits (pre-registered):", ", ".join(f"{k}={v}" for k, v in guardrails.LIMITS.items()))


if __name__ == "__main__":
    main()
