"""
Phase B — risk guardrails + kill-switch. The layer that must PASS before a book is
trusted (paper) and, later, before any real order (Phase C). PAPER-SAFE: it only
blocks/flags; it never places or cancels anything.

Three families of check, each with a severity:
  BLOCK — do not record/act on this book (stale data, thin universe, over-concentration,
          leverage, sub-floor names)
  WARN  — record but flag (soft concentration, individual borderline names)
  HALT  — a live book breached its drawdown limit -> trip the kill-switch

The kill-switch is a persistent latch (logs/killswitch.json). Once tripped it BLOCKS
all new recording until a human resets it. Limits are pre-registered here, not tuned
per book. (Upgrade path for Phase C: persist risk events to an append-only DB table.)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")
_KILL_PATH = Path(__file__).resolve().parent.parent / "logs" / "killswitch.json"

# Pre-registered risk limits (the guardrails). Do not tune per book.
LIMITS = {
    "max_name_weight": 0.08,     # no single position > 8% of the invested book
    "min_holdings": 15,          # diversification floor
    "max_sector_weight": 0.45,   # no one nse_industry > 45% of the book (WARN)
    "min_price": 10.0,           # penny-stock floor
    "min_mcap_cr": 2000.0,       # liquidity floor
    "max_gross": 1.0,            # gross exposure cap = NO leverage
    "max_stale_days": 5,         # data-freshness gate
    "min_universe": 200,         # universe-coverage gate
    "drawdown_halt": -0.25,      # live book loss vs entry that trips the kill-switch
}

BLOCK, WARN, HALT, OK = "BLOCK", "WARN", "HALT", "OK"


def _v(severity, code, msg, **extra):
    return {"severity": severity, "code": code, "msg": msg, **extra}


def load_sector_map() -> dict:
    """nse_symbol -> nse_industry (for sector-concentration checks)."""
    eng = create_engine(DAWN_URL)
    try:
        with eng.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ON (nse_symbol) nse_symbol, nse_industry "
                "FROM security_sector WHERE nse_symbol IS NOT NULL "
                "ORDER BY nse_symbol, nse_industry NULLS LAST")).fetchall()
    finally:
        eng.dispose()
    return {r[0]: (r[1] or "Unknown") for r in rows}


def check_data_quality(freshness: dict, universe_size: int, limits=LIMITS) -> list:
    out = []
    sd = freshness.get("stale_days")
    if sd is not None and sd > limits["max_stale_days"]:
        out.append(_v(BLOCK, "stale_data",
                      f"price panel {sd}d stale (> {limits['max_stale_days']}d)", stale_days=sd))
    if universe_size is not None and universe_size < limits["min_universe"]:
        out.append(_v(BLOCK, "thin_universe",
                      f"universe {universe_size} < {limits['min_universe']}", universe=universe_size))
    return out


def check_composition(holdings: list, target_exposure: float, sector_map: dict,
                      limits=LIMITS) -> list:
    """Position/sector/gross/floor checks on a proposed book. `holdings` carry
    weight, entry_price, mktcap_cr."""
    out = []
    invested = [h for h in holdings if float(h.get("weight", 0) or 0) > 0]
    n = len(invested)
    if n < limits["min_holdings"]:
        out.append(_v(BLOCK, "too_few_names", f"{n} holdings < {limits['min_holdings']} floor", n=n))

    gross = sum(float(h.get("weight", 0) or 0) for h in invested)
    if gross > limits["max_gross"] + 1e-6:
        out.append(_v(BLOCK, "leverage", f"gross exposure {gross:.2f} > {limits['max_gross']:.2f}", gross=gross))

    for h in invested:
        w = float(h.get("weight", 0) or 0)
        if w > limits["max_name_weight"] + 1e-6:
            out.append(_v(BLOCK, "position_size",
                          f"{h['symbol']} weight {w:.1%} > {limits['max_name_weight']:.0%}", symbol=h["symbol"]))
        px = h.get("entry_price")
        if px is not None and px < limits["min_price"]:
            out.append(_v(WARN, "price_floor", f"{h['symbol']} ₹{px} < ₹{limits['min_price']:.0f}", symbol=h["symbol"]))
        mc = h.get("mktcap_cr")
        if mc is not None and mc < limits["min_mcap_cr"]:
            out.append(_v(WARN, "mcap_floor", f"{h['symbol']} ₹{mc:.0f}cr < ₹{limits['min_mcap_cr']:.0f}cr", symbol=h["symbol"]))

    # sector concentration (weight within the invested book)
    if gross > 0:
        sec_w: dict = {}
        for h in invested:
            s = sector_map.get(h["symbol"], "Unknown")
            sec_w[s] = sec_w.get(s, 0.0) + float(h.get("weight", 0) or 0) / gross
        top_s, top_w = max(sec_w.items(), key=lambda kv: kv[1])
        if top_w > limits["max_sector_weight"]:
            out.append(_v(WARN, "sector_concentration",
                          f"{top_s} is {top_w:.0%} of book (> {limits['max_sector_weight']:.0%})",
                          sector=top_s, weight=round(top_w, 3)))
    return out


def check_drawdown(book_ret_pct: float | None, limits=LIMITS) -> list:
    """Trip on a live book's loss vs entry. (MVP: current MTM vs entry; a daily
    equity series would give true peak-to-trough — a Phase-C refinement.)"""
    if book_ret_pct is None:
        return []
    if book_ret_pct / 100.0 <= limits["drawdown_halt"]:
        return [_v(HALT, "drawdown_halt",
                   f"book down {book_ret_pct:.1f}% (<= {limits['drawdown_halt']*100:.0f}% halt)",
                   ret_pct=book_ret_pct)]
    return []


# ---- kill-switch latch -------------------------------------------------------
def read_killswitch() -> dict:
    if _KILL_PATH.exists():
        try:
            return json.loads(_KILL_PATH.read_text())
        except Exception:
            pass
    return {"tripped": False}


def trip_killswitch(reason: str, ts: str) -> dict:
    _KILL_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {"tripped": True, "reason": reason, "tripped_at": ts}
    _KILL_PATH.write_text(json.dumps(state, indent=2))
    return state


def reset_killswitch() -> dict:
    state = {"tripped": False, "reset_at": datetime.now(timezone.utc).isoformat()}
    _KILL_PATH.parent.mkdir(parents=True, exist_ok=True)
    _KILL_PATH.write_text(json.dumps(state, indent=2))
    return state


def evaluate(pf: dict, freshness: dict, sector_map: dict,
             book_ret_pct: float | None = None, now_ts: str | None = None,
             limits=LIMITS) -> dict:
    """Aggregate all checks for one proposed/live book into a verdict."""
    checks = []
    checks += check_data_quality(freshness, pf.get("universe_size"), limits)
    checks += check_composition(pf.get("holdings", []), pf.get("target_exposure", 1.0), sector_map, limits)
    dd = check_drawdown(book_ret_pct, limits)
    checks += dd

    kill = read_killswitch()
    if kill.get("tripped"):
        checks.append(_v(BLOCK, "killswitch", f"kill-switch tripped: {kill.get('reason','')}"))

    blocking = [c for c in checks if c["severity"] in (BLOCK, HALT)]
    warns = [c for c in checks if c["severity"] == WARN]
    status = HALT if any(c["severity"] == HALT for c in checks) else (BLOCK if blocking else (WARN if warns else OK))
    return {
        "status": status,
        "ok_to_record": len(blocking) == 0,
        "blocking": blocking,
        "warnings": warns,
        "checks": checks,
        "killswitch": kill,
        "should_trip": bool(dd),   # caller decides whether to latch the kill-switch
    }
