#!/usr/bin/env python3
"""
Monthly paper-track rebalance runner.

  compute the momentum+value composite portfolio (liquid universe, top-25) and
  record it to paper_portfolio. Entry prices default to the latest Dawn close;
  pass --live-prices <json {symbol: ltp}> to use live Kite LTPs instead
  (the agent/caller fetches those via the Kite MCP).

  To score a prior portfolio: --score <portfolio_id> --exit-prices <json>
  [--exit-date YYYY-MM-DD] [--benchmark <nifty_return_pct>]

Usage:
  DATABASE_URL=... DAWN_URL=... python3 scripts/paper_rebalance.py
  ... --live-prices /tmp/ltp.json
  ... --score 1 --exit-prices /tmp/exit.json --exit-date 2026-08-01 --benchmark 1.2
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import papertrack


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=None, help="portfolio formation date (default: latest Dawn date)")
    ap.add_argument("--notional", type=float, default=100_000.0)
    ap.add_argument("--live-prices", default=None, help="JSON file {symbol: ltp} for entry prices")
    ap.add_argument("--score", type=int, default=None, help="score this portfolio_id instead of forming one")
    ap.add_argument("--exit-prices", default=None, help="JSON file {symbol: ltp} of exit prices")
    ap.add_argument("--exit-date", default=None)
    ap.add_argument("--benchmark", type=float, default=None, help="benchmark return pct over the period")
    ap.add_argument("--force", action="store_true",
                    help="record even if a book for this as_of_date already exists (default: refuse)")
    args = ap.parse_args()

    if args.score:
        exit_prices = json.loads(Path(args.exit_prices).read_text())
        res = papertrack.score_portfolio(args.score, exit_prices, args.exit_date, args.benchmark)
        print(json.dumps(res, indent=2))
        return

    live = json.loads(Path(args.live_prices).read_text()) if args.live_prices else None
    pf = papertrack.compute_portfolio(args.as_of)
    existing = papertrack.existing_portfolio_for(pf["as_of"], pf["strategy"])
    pid = papertrack.record_portfolio(pf, notional=args.notional, live_prices=live, force=args.force)
    reg = pf["regime"]
    state = "RISK-ON (fully invested)" if reg["risk_on"] else "RISK-OFF → DE-RISK TO CASH"
    if pid:
        tag = f"paper_portfolio id={pid}"
    else:
        tag = f"NOT recorded — book id={existing} already exists for {pf['as_of']} (use --force to override)"
    print(f"as_of={pf['as_of']} universe={pf['universe_size']} holdings={pf['n_holdings']} -> {tag}")
    print(f"trend overlay [{reg['ma_days']}d MA]: index {reg['index_level']} vs MA {reg['index_ma']} "
          f"-> {state}  (target_exposure={pf['target_exposure']:.0%})")
    if not reg["risk_on"]:
        print("  ⚠ book de-risked: names below are the intended holdings when the trend turns"
              " back up; hold cash until then.")
    for h in pf["holdings"]:
        print(f"  {h['symbol']:<13} z={h['composite_z']:>6.2f} entry=₹{h['entry_price']:>8.1f}"
              f"  w={h['weight']:.3f}")


if __name__ == "__main__":
    main()
