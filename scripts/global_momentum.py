#!/usr/bin/env python3
"""
Global momentum efficiency-gradient — does the cross-sectional momentum edge exist
across the world's major exchanges, or only where the market is inefficient?

Same engine as the India/US tests (12-1, monthly, top-quintile long + long-short,
rank-IC). Universe per exchange = curated liquid mega/large-caps (the real major-index
constituents; cross-checked against the Morningstar screener). Prices from yfinance
(Morningstar has no stock price time-series). Momentum only (no global fundamentals panel).

CAVEATS (same as the US test — both flatter the developed markets):
  - Survivorship: current constituents only (dropouts/delistings absent). India's own
    engine was survivorship-CONTROLLED, so its IC_t ~4.0 is the honest anchor; the
    yfinance markets here are survivorship-FLATTERED and still mostly show ~0.
  - Large-cap only: the most efficient slice. India's edge was strongest in SMALL caps.

Usage:
  python3 scripts/global_momentum.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import us_replication
us_replication.MIN_NAMES = 20          # thinner curated universes than the 503-name US test
from us_replication import backtest, stat

CACHE = Path(__file__).resolve().parent.parent / "data" / "global"

# curated liquid large-caps per exchange (yfinance tickers). ~30 each is enough for a
# directional cross-sectional IC over ~120 monthly rebalances.
UNIV = {
 "XNAS (Nasdaq)": "AAPL MSFT NVDA GOOGL AMZN META TSLA AVGO PEP COST ADBE NFLX AMD CSCO TMUS INTC QCOM INTU AMAT TXN ISRG BKNG HON VRTX ADI REGN MU LRCX PANW KLAC SNPS CDNS MRVL ORLY CRWD FTNT ADP MELI ABNB".split(),
 "London (LSE)": [s+".L" for s in "AZN SHEL HSBA ULVR BP RIO GSK DGE BATS GLEN LSEG REL NG BARC VOD LLOY PRU AAL TSCO NWG CPG RKT IMB BA AV STAN SSE LGEN ABF WPP".split()],
 "Germany (XETRA)": [s+".DE" for s in "SAP SIE ALV DTE AIR MBG BMW VOW3 BAS BAYN DB1 MUV2 DHL ADS RWE IFX MRK HEN3 DBK VNA EOAN BEI CON HEI SHL ZAL FRE MTX RHM P911".split()],
 "Euronext (Paris)": [s+".PA" for s in "MC OR RMS TTE SAN SU AI EL CS BNP DG SGO KER BN CAP VIE ACA ENGI ORA ML LR PUB RI GLE DSY HO SW".split()],
 "Swiss (SIX)": [s+".SW" for s in "NESN ROG NOVN UBSG ZURN ABBN LONN SIKA GIVN SREN CFR ALC PGHN SCMN GEBN SLHN HOLN BAER SOON LOGN".split()],
 "ASX (Australia)": [s+".AX" for s in "BHP CBA CSL NAB WBC ANZ WES MQG FMG WDS TLS GMG RIO TCL WOW ALL STO QBE COL REA WTC XRO RMD FPH ORG S32 SUN MPL JHX COH".split()],
 "Tokyo (JPX)": [s+".T" for s in "7203 6758 6861 8035 6098 9984 9433 9432 4063 6367 8306 8058 8001 8031 4519 6902 7974 4568 6954 4661 6981 6501 7267 6301 6503 4901 8316 8411 9020 4502".split()],
 "HongKong (HKEX)": [s.zfill(4)+".HK" for s in "700 939 1299 941 3690 5 1810 9988 388 16 1398 2318 883 857 2628 11 2 1109 688 27 1113 1 3 175 9618 9999 2020 1928 288 2382".split()],
 "Korea (KRX)": [s+".KS" for s in "005930 000660 373220 207940 005380 005490 035420 051910 006400 000270 105560 055550 035720 012330 068270 028260 066570 003670 015760 034730 017670 018260 032830 009150 011200 086790 010130 316140 024110 030200".split()],
 "Taiwan (TWSE)": [s+".TW" for s in "2330 2317 2454 2308 2382 2412 2881 2882 3711 2303 2891 2886 1301 3008 2884 2357 2892 2880 5880 2885 1216 2002 2207 2379 3045 4938 2395 2409 6505 2887".split()],
 "Shanghai (SSE)": [s+".SS" for s in "600519 601398 600036 601857 600900 601288 600028 601988 601628 601318 600030 600887 601888 600276 601088 603288 600585 601668 601166 600690 601328 600309 600048 601601 600089 601995 603259 600438 601899 603501".split()],
 "Shenzhen (SZSE)": [s+".SZ" for s in "000001 000002 000333 000651 000858 002415 300750 002594 000725 002714 300059 000568 002304 300760 002230 000063 002475 300124 000538 002027 300015 002352 000100 300014 002460 000776 002241 300142 000625 002493".split()],
 "Brazil (B3)": [s+".SA" for s in "PETR4 VALE3 ITUB4 BBDC4 B3SA3 ABEV3 WEGE3 BBAS3 ITSA4 SUZB3 RENT3 RADL3 PRIO3 EQTL3 JBSS3 GGBR4 BPAC11 ELET3 VBBR3 RAIL3 LREN3 ENEV3 CSAN3 HAPV3 CMIG4 UGPA3 SBSP3 TOTS3 EMBR3 CPLE6".split()],
}


def fetch(mkt: str, syms: list) -> pd.DataFrame:
    import yfinance as yf
    CACHE.mkdir(parents=True, exist_ok=True)
    fp = CACHE / (mkt.split()[0].lower() + ".pkl")
    if fp.exists():
        return pd.read_pickle(fp)
    df = yf.download(syms, start="2015-01-01", end="2026-07-14", interval="1d",
                     auto_adjust=True, progress=False, threads=True)["Close"]
    df.to_pickle(fp)
    return df


def main():
    print("Global momentum gradient (12-1, monthly, top-quintile) — yfinance prices\n")
    rows = []
    for mkt, syms in UNIV.items():
        try:
            close = fetch(mkt, syms)
            close = close.dropna(axis=1, how="all")          # drop tickers that never downloaded
            cov = close.notna().mean().mean() * 100
            ic, ls, lo, bench = backtest(close, "2016-01-31")
            ic_m, ic_t = stat(ic)
            _, ls_s = stat(ls, ann=True)
            exc = pd.Series(lo.values - bench.values, index=lo.index)
            ex_a, ex_s = stat(exc, ann=True)
            rows.append((mkt, close.shape[1], cov, ic_m, ic_t, ls_s, ex_a, ex_s, len(ic)))
        except Exception as e:
            print(f"  {mkt}: FAILED ({e})")

    rows.append(("India (Dawn, survivorship-controlled)", 900, 100.0, 0.0486, 4.0, 0.60, None, None, 113))
    rows.sort(key=lambda r: -(r[4] if r[4] == r[4] else -9))

    print("=" * 90)
    print("EFFICIENCY GRADIENT — cross-sectional momentum IC_t by market (2016-2026)")
    print("=" * 90)
    print(f"{'exchange':<38}{'names':>6}{'cov%':>6}{'meanIC':>9}{'IC_t':>7}{'LS Shrp':>8}{'excess%':>9}")
    print("-" * 90)
    for mkt, n, cov, ic_m, ic_t, ls_s, ex_a, ex_s, nm in rows:
        ex = f"{ex_a:+.1f}" if ex_a is not None else "  --"
        star = "  <= inefficient" if ic_t >= 2 else ("  ~efficient" if ic_t < 1 else "")
        print(f"{mkt:<38}{n:>6}{cov:>6.0f}{ic_m:>9.4f}{ic_t:>7.2f}{ls_s:>8.2f}{ex:>9}{star}")
    print("-" * 90)
    print("READ: IC_t>2 = a real cross-sectional momentum edge; IC_t~0 = arbitraged away.")
    print("India (survivorship-CONTROLLED) is the honest anchor. The yfinance markets are")
    print("survivorship-FLATTERED + large-cap-only and STILL mostly ~0 — the edge concentrates")
    print("in less-efficient markets (esp. India small caps), not developed large caps.")


if __name__ == "__main__":
    main()
