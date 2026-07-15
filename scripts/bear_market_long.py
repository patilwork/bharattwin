#!/usr/bin/env python3
"""
Long-history bear test — how would the momentum book + trend overlay behave through the
PROLONGED grinding bears our India data (2016+) lacks: the 2000-2002 dot-com crash and
the 2008 GFC?

India price history is local-only from 2016, so this uses US S&P 500 via yfinance back
to 1999. US large-cap momentum has ~0 cross-sectional ALPHA (proven earlier) — that is
NOT the point here. The point is DRAWDOWN / bear BEHAVIOUR: does the momentum book crash,
does the 200d-MA trend overlay protect in a slow grind, and does it whipsaw?

CORE = momentum top-quintile fully invested; +OVERLAY = de-risk to cash below 200d MA;
BENCH = equal-weight universe. Survivorship caveat: today's S&P 500 constituents only,
so the real bears were WORSE than shown (dead names absent).

Usage:
  python3 scripts/bear_market_long.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bear_market_analysis import series, dd, cum

CACHE = Path(__file__).resolve().parent.parent / "data" / "us" / "us_close_2000.pkl"


def main():
    px = pd.read_pickle(CACHE).dropna(axis=1, how="all")
    core, overlay, bench = series(px, start="2001-01-31")

    print("\n" + "=" * 76)
    print(f"LONG-HISTORY BEAR TEST — US S&P 500, {core.index[0].date()}→{core.index[-1].date()}")
    print("(US momentum alpha ~0 — this is a DRAWDOWN/bear-behaviour test, not an alpha test)")
    print("=" * 76)
    print(f"{'':<28}{'CORE':>12}{'+OVERLAY':>12}{'BENCHMARK':>12}")
    print(f"{'full max drawdown':<28}{dd(core):>11.1f}%{dd(overlay):>11.1f}%{dd(bench):>11.1f}%")
    print(f"{'worst single month':<28}{core.min()*100:>11.1f}%{overlay.min()*100:>11.1f}%{bench.min()*100:>11.1f}%")

    print("\nPROLONGED BEARS (cumulative return through the window):")
    episodes = {
        "2000-02 dot-com (Sep00-Sep02)": ("2000-09-01", "2002-10-31"),
        "2008 GFC (Nov07-Mar09)":        ("2007-11-01", "2009-03-31"),
        "2008 — the crash (Sep-Nov08)":  ("2008-09-01", "2008-11-30"),
        "2020 COVID (Feb-Apr20)":        ("2020-02-01", "2020-04-30"),
        "2022 bear (Jan-Oct22)":         ("2022-01-01", "2022-10-31"),
    }
    print(f"{'bear':<32}{'CORE':>9}{'+OVERLAY':>11}{'BENCH':>9}")
    for name, (a, b) in episodes.items():
        sc, so, sb = (s[(s.index >= a) & (s.index <= b)] for s in (core, overlay, bench))
        if len(sb):
            print(f"{name:<32}{cum(sc):>8.1f}%{cum(so):>10.1f}%{cum(sb):>8.1f}%")

    # the 2009 momentum crash (Mar-May 2009 violent loser rally)
    mc = {"2009 momentum crash (Mar-May09)": ("2009-03-01", "2009-05-31")}
    print("\nMOMENTUM'S OWN NEMESIS (the recovery rip, where past-losers scream up):")
    for name, (a, b) in mc.items():
        sc, so, sb = (s[(s.index >= a) & (s.index <= b)] for s in (core, overlay, bench))
        print(f"{name:<32}{cum(sc):>8.1f}%{cum(so):>10.1f}%{cum(sb):>8.1f}%")

    print("\nWORST 8 MARKET MONTHS (1999-2026) — overlay protection:")
    print(f"{'month':<10}{'BENCH':>9}{'CORE':>9}{'+OVERLAY':>11}")
    for t in bench.nsmallest(8).index:
        print(f"{t.strftime('%Y-%m'):<10}{bench[t]*100:>8.1f}%{core[t]*100:>8.1f}%{overlay[t]*100:>10.1f}%")

    ann = lambda s: ((1 + s).prod() ** (12/len(s)) - 1) * 100
    shp = lambda s: s.mean()/s.std()*np.sqrt(12)
    print("\nFULL 2001-2026:")
    for lab, s in [("CORE", core), ("+OVERLAY", overlay), ("BENCH", bench)]:
        print(f"  {lab:<10} ann {ann(s):+.1f}%  Sharpe {shp(s):.2f}  maxDD {dd(s):.1f}%")
    print("-" * 76)
    print("READ: this is the prolonged-bear evidence India's 2016+ data can't give. Watch")
    print("(1) does CORE survive 2000-02 and 2008; (2) does the overlay PROTECT in the slow")
    print("grind or WHIPSAW; (3) the 2009 momentum crash. Survivorship makes the real bears")
    print("WORSE than shown. US momentum has ~0 alpha, so ignore the return level — read the")
    print("DRAWDOWNS and the overlay gap. The behaviour transfers even if the alpha doesn't.")


if __name__ == "__main__":
    main()
