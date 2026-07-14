"""
India transaction-cost engine.

Every honest backtest / benchmark / sizing decision must clear real costs.
This computes the full round-trip cost stack for Indian equities & derivatives:
STT/CTT, exchange transaction charges, SEBI turnover fee, stamp duty, brokerage,
GST, and an assumed slippage. Output includes the breakeven move a trade must
make just to cover costs.

IMPORTANT — rates change and vary by broker. The defaults below reflect the
post-Oct-2024 SEBI schedule (STT hikes on F&O) and Zerodha-style brokerage, but
they are configurable and MUST be re-verified against the current SEBI / NSE /
broker schedules before being trusted for live sizing. Marked (VERIFY) where
most volatile.

Segments:
    equity_delivery   — CNC cash equity (buy to hold)
    equity_intraday   — MIS cash equity
    index_futures     — Nifty/BankNifty futures
    index_options     — Nifty/BankNifty options (charged on premium)
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ── Rate schedule (fractions of turnover unless noted). VERIFY periodically. ──
# Sources to check: SEBI circulars, NSE "transaction charges", broker cost pages.


@dataclass(frozen=True)
class SegmentRates:
    # STT/CTT — Securities/Commodities Transaction Tax
    stt_buy: float
    stt_sell: float
    # Exchange transaction charge (NSE)
    exch_txn: float
    # SEBI turnover fee (0.0001% = 1e-6)
    sebi: float = 1e-6
    # Stamp duty (buy side only)
    stamp_buy: float = 0.0
    # Brokerage: percent of turnover, capped at a flat per-order amount
    brokerage_pct: float = 0.0
    brokerage_cap: float = 20.0  # ₹ per order (Zerodha-style)
    # GST applies to (brokerage + exch_txn + sebi)
    gst: float = 0.18
    # Is the charge base the premium (options) rather than notional?
    charge_on_premium: bool = False


# Post-Oct-2024 schedule. (VERIFY) marks the most change-prone rates.
RATES: dict[str, SegmentRates] = {
    "equity_delivery": SegmentRates(
        stt_buy=0.001, stt_sell=0.001,        # 0.1% both sides (VERIFY)
        exch_txn=2.97e-6,                      # ~0.00297% NSE (VERIFY)
        stamp_buy=0.00015,                     # 0.015% buy
        brokerage_pct=0.0, brokerage_cap=0.0,  # delivery often free
    ),
    "equity_intraday": SegmentRates(
        stt_buy=0.0, stt_sell=0.00025,         # 0.025% sell only
        exch_txn=2.97e-6,
        stamp_buy=0.00003,                     # 0.003% buy
        brokerage_pct=0.0003, brokerage_cap=20.0,
    ),
    "index_futures": SegmentRates(
        stt_buy=0.0, stt_sell=0.0002,          # 0.02% sell (hiked Oct-2024) (VERIFY)
        exch_txn=1.73e-5,                      # ~0.00173% NSE futures (VERIFY)
        stamp_buy=0.00002,                     # 0.002% buy
        brokerage_pct=0.0003, brokerage_cap=20.0,
    ),
    "index_options": SegmentRates(
        stt_buy=0.0, stt_sell=0.001,           # 0.1% on sell premium (hiked Oct-2024) (VERIFY)
        exch_txn=3.503e-4,                     # ~0.03503% NSE options premium (VERIFY)
        stamp_buy=0.00003,                     # 0.003% premium buy
        brokerage_pct=0.0, brokerage_cap=20.0,
        charge_on_premium=True,
    ),
}


@dataclass
class CostResult:
    segment: str
    notional: float                 # per-side turnover the charges apply to
    stt: float
    exchange: float
    sebi: float
    stamp: float
    brokerage: float
    gst: float
    slippage: float
    total: float                    # ₹ round trip
    total_bps: float                # round-trip cost in basis points of notional
    breakeven_move_pct: float       # move needed just to cover costs (= total_bps/100)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def round_trip_cost(
    notional: float,
    segment: str = "index_futures",
    slippage_bps: float = 1.5,
    n_orders: int = 2,
) -> CostResult:
    """Full round-trip (buy + sell) cost for a position of the given per-side
    turnover `notional` (₹). For options, pass the premium turnover as notional
    and it is treated as the charge base.

    slippage_bps: assumed one-way slippage in bps; applied on both legs.
    n_orders: order count for flat brokerage cap (default 2 = one in, one out).
    """
    if segment not in RATES:
        raise ValueError(f"unknown segment {segment!r}; choose from {list(RATES)}")
    r = RATES[segment]

    stt = notional * r.stt_buy + notional * r.stt_sell
    exchange = notional * r.exch_txn * 2
    sebi = notional * r.sebi * 2
    stamp = notional * r.stamp_buy  # buy side only

    # Brokerage: min(pct*turnover, cap) per order, summed over legs.
    def _brok() -> float:
        if r.brokerage_pct <= 0 and r.brokerage_cap <= 0:
            return 0.0
        per_leg_pct = notional * r.brokerage_pct
        per_leg = min(per_leg_pct, r.brokerage_cap) if r.brokerage_cap > 0 else per_leg_pct
        return per_leg * n_orders

    brokerage = _brok()
    gst = (brokerage + exchange + sebi) * r.gst
    slippage = notional * (slippage_bps / 1e4) * 2

    total = stt + exchange + sebi + stamp + brokerage + gst + slippage
    total_bps = (total / notional) * 1e4 if notional else 0.0

    return CostResult(
        segment=segment, notional=round(notional, 2),
        stt=round(stt, 2), exchange=round(exchange, 2), sebi=round(sebi, 2),
        stamp=round(stamp, 2), brokerage=round(brokerage, 2), gst=round(gst, 2),
        slippage=round(slippage, 2), total=round(total, 2),
        total_bps=round(total_bps, 3), breakeven_move_pct=round(total_bps / 100, 4),
    )


def breakeven_move_pct(segment: str = "index_futures", notional: float = 1_000_000, slippage_bps: float = 1.5) -> float:
    """The % move a position must make just to break even after round-trip cost."""
    return round_trip_cost(notional, segment, slippage_bps).breakeven_move_pct
