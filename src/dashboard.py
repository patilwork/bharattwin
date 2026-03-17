"""
BharatTwin CLI Dashboard — market state trends and replay scoreboard.

Usage:
    python -m src.dashboard                # market overview
    python -m src.dashboard --replays      # replay scoreboard
    python -m src.dashboard --full         # both
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def market_overview() -> None:
    """Print market state trend for recent dates."""
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT session_id, nifty50_close, banknifty_close, india_vix, factor_map
            FROM market_state
            WHERE universe_id = 'nifty50'
              AND nifty50_close IS NOT NULL
            ORDER BY session_id DESC
            LIMIT 15
        """)).mappings().fetchall()
    engine.dispose()

    if not rows:
        print("No market data available.")
        return

    print()
    print("=" * 90)
    print("MARKET STATE OVERVIEW — Recent Trading Days")
    print("=" * 90)
    print(f"{'Date':<14} {'Nifty':>10} {'BankNifty':>12} {'VIX':>8} {'Mom1d':>8} {'Mom5d':>8} {'Mom20d':>8} {'Vol20d':>8} {'Breadth':>8}")
    print("-" * 90)

    for row in reversed(list(rows)):
        sid = row["session_id"]
        d = sid.replace("CM_", "")
        n = f"{float(row['nifty50_close']):,.2f}" if row["nifty50_close"] else "N/A"
        b = f"{float(row['banknifty_close']):,.0f}" if row["banknifty_close"] else "N/A"
        v = f"{float(row['india_vix']):.2f}" if row["india_vix"] else "N/A"

        fm = row["factor_map"]
        if fm:
            fm = fm if isinstance(fm, dict) else json.loads(fm)
            m1 = f"{fm.get('momentum_1d', 0):+.2f}%" if fm.get("momentum_1d") is not None else "N/A"
            m5 = f"{fm.get('momentum_5d', 0):+.2f}%" if fm.get("momentum_5d") is not None else "N/A"
            m20 = f"{fm.get('momentum_20d', 0):+.2f}%" if fm.get("momentum_20d") is not None else "N/A"
            v20 = f"{fm.get('volatility_20d', 0):.1f}" if fm.get("volatility_20d") is not None else "N/A"
            adv = fm.get("breadth_adv", 0)
            dec = fm.get("breadth_dec", 0)
            breadth = f"{adv}/{dec}"
        else:
            m1 = m5 = m20 = v20 = breadth = "N/A"

        print(f"{d:<14} {n:>10} {b:>12} {v:>8} {m1:>8} {m5:>8} {m20:>8} {v20:>8} {breadth:>8}")

    # Summary stats
    latest = rows[0]
    oldest = rows[-1]
    if latest["nifty50_close"] and oldest["nifty50_close"]:
        n_latest = float(latest["nifty50_close"])
        n_oldest = float(oldest["nifty50_close"])
        chg_pct = (n_latest - n_oldest) / n_oldest * 100
        print("-" * 90)
        print(f"Period change: {n_oldest:,.2f} → {n_latest:,.2f} ({chg_pct:+.2f}%)")

    print("=" * 90)


def replay_scoreboard() -> None:
    """Print replay case scoreboard."""
    print()
    print("=" * 80)
    print("REPLAY SCOREBOARD")
    print("=" * 80)
    print(f"{'Case':<35} {'Actual':>8} {'Predicted':>10} {'Error':>8} {'Dir':>5} {'Agents':>8}")
    print("-" * 80)

    # Import and run replays (suppress agent print output)
    cases = []

    def _run_silent(fn):
        """Run a function with stdout suppressed."""
        import io, contextlib
        with contextlib.redirect_stdout(io.StringIO()):
            return fn()

    # Case 1: RBI Hike May 2022
    try:
        from src.replay.run_incontext_007 import run_incontext_replay
        from src.replay.cases.rbi_hike_may2022 import ACTUAL_NIFTY_RETURN_PCT as rbi_actual
        c = _run_silent(run_incontext_replay)
        dir_ok = "YES" if c.consensus_direction.value == "SELL" else "NO"
        error = abs(c.avg_return_pct - rbi_actual)
        cases.append(("RBI Hike May 2022", rbi_actual, c.avg_return_pct, error, dir_ok, f"{c.bear_count}B/{c.bull_count}L/{c.neutral_count}H"))
    except Exception as e:
        cases.append(("RBI Hike May 2022", None, None, None, "ERR", str(e)[:20]))

    # Case 2: Election June 2024
    try:
        from src.replay.run_election_010 import run_election_replay
        from src.replay.cases.election_june2024 import ACTUAL_NIFTY_RETURN_PCT as elec_actual
        c2 = _run_silent(run_election_replay)
        dir_ok2 = "YES" if c2.consensus_direction.value == "SELL" else "NO"
        error2 = abs(c2.avg_return_pct - elec_actual)
        cases.append(("Election June 2024", elec_actual, c2.avg_return_pct, error2, dir_ok2, f"{c2.bear_count}B/{c2.bull_count}L/{c2.neutral_count}H"))
    except Exception as e:
        cases.append(("Election June 2024", None, None, None, "ERR", str(e)[:20]))

    # Case 3: Live sim Mar 16→17
    try:
        from src.replay.run_live_008 import run_live_simulation, ACTUAL_NIFTY_RETURN_PCT as live_actual
        c3 = _run_silent(run_live_simulation)
        if c3.consensus_direction.value == "HOLD":
            dir_ok3 = "~" if abs(live_actual) < 1.5 else "NO"
        elif c3.consensus_direction.value == "BUY":
            dir_ok3 = "YES" if live_actual > 0 else "NO"
        else:
            dir_ok3 = "YES" if live_actual < 0 else "NO"
        error3 = abs(c3.avg_return_pct - live_actual)
        cases.append(("Live Mar16→17 (no event)", live_actual, c3.avg_return_pct, error3, dir_ok3, f"{c3.bear_count}B/{c3.bull_count}L/{c3.neutral_count}H"))
    except Exception as e:
        cases.append(("Live Mar16→17 (no event)", None, None, None, "ERR", str(e)[:20]))

    for name, actual, predicted, error, dir_ok, agents in cases:
        if actual is not None:
            print(f"{name:<35} {actual:>+7.2f}% {predicted:>+9.2f}% {error:>7.2f}pp {dir_ok:>5} {agents:>8}")
        else:
            print(f"{name:<35} {'N/A':>8} {'N/A':>10} {'N/A':>8} {dir_ok:>5} {agents:>8}")

    # Summary
    valid = [c for c in cases if c[3] is not None]
    if valid:
        avg_error = sum(c[3] for c in valid) / len(valid)
        dir_correct = sum(1 for c in valid if c[4] in ("YES", "~"))
        print("-" * 80)
        print(f"Average error: {avg_error:.2f}pp | Direction correct: {dir_correct}/{len(valid)}")

    print("=" * 80)


def flow_summary() -> None:
    """Print FII/DII flow summary."""
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT date, participant, net_crore
            FROM flow_store
            ORDER BY date, participant
        """)).fetchall()
    engine.dispose()

    if rows:
        print()
        print("=" * 50)
        print("FII/DII FLOWS")
        print("=" * 50)
        for r in rows:
            print(f"  {r[0]} | {r[1]:3s} | net={r[2]:>+12,.2f} cr")
        print("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BharatTwin Dashboard")
    parser.add_argument("--replays", action="store_true", help="Show replay scoreboard")
    parser.add_argument("--full", action="store_true", help="Show everything")
    args = parser.parse_args()

    if args.replays or args.full:
        replay_scoreboard()

    if args.full:
        market_overview()
        flow_summary()
    elif not args.replays:
        market_overview()
