"""
Phase A — monthly paper-track orchestrator.

Ties the pieces into one repeatable, scheduler-friendly run. PAPER ONLY: it
generates order *tickets* for a human to place; it never touches a broker. The
sequence, matching the roadmap (PROJECT_STATUS.md §7 AUTOMATION Phase A):

  1. data freshness  — how stale is the Dawn price panel? (gate, not a fetch;
     the real fetcher lives in ~/Developer/Weekly and needs Kite Connect to
     automate — wired as a hook/TODO, not silently skipped)
  2. score last book — score the most recent unscored paper_portfolio whose
     holding month has elapsed, against current prices (append-only pnl)
  3. signal          — form the momentum+value composite WITH the Tier-1.2 trend
     overlay (de-risk to cash when the index is below its 200d MA)
  4. order tickets   — diff the new target book against what's currently held:
     SELL exits, BUY entries, HOLD/REBALANCE continuers, CASH on risk-off
  5. record          — append the new book to paper_portfolio (dup-guarded)
  6. report          — one structured dict (also printed) capturing the whole run

Every side-effecting step is idempotent; a dry run does everything except the
record/score writes.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

from sqlalchemy import create_engine, text

from src import papertrack

BHARAT_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/bharattwin")
DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")

STALE_AFTER_DAYS = 5        # price panel older than this at run time is flagged
MIN_HOLD_DAYS = 25          # a book is scoreable once ~a month has elapsed


def data_freshness(as_of: date | None = None) -> dict:
    """Latest date present in the Dawn price panel and how stale it is vs `as_of`
    (default: today). A gate for the run, not a fetch."""
    ref = as_of or datetime.now(timezone.utc).date()
    eng = create_engine(DAWN_URL)
    try:
        with eng.connect() as conn:
            latest = conn.execute(text("SELECT max(date) FROM stock_prices_daily")).scalar()
            nrows = conn.execute(text("SELECT count(*) FROM stock_prices_daily")).scalar()
    finally:
        eng.dispose()
    if latest is None:
        latest_d = None
    elif isinstance(latest, datetime):
        latest_d = latest.date()
    elif isinstance(latest, date):
        latest_d = latest
    else:
        latest_d = date.fromisoformat(str(latest)[:10])
    stale_days = (ref - latest_d).days if latest_d else None
    return {
        "latest_price_date": latest_d.isoformat() if latest_d else None,
        "rows": int(nrows or 0),
        "stale_days": stale_days,
        "is_stale": (stale_days is not None and stale_days > STALE_AFTER_DAYS),
        "ref_date": ref.isoformat(),
    }


def latest_unscored_portfolio(before_as_of: str | None = None) -> dict | None:
    """The most recent paper_portfolio that has no pnl row yet (and, if
    `before_as_of` is given, was formed strictly before it). Returns id/as_of/
    holdings or None."""
    eng = create_engine(BHARAT_URL)
    try:
        with eng.connect() as conn:
            q = """
                SELECT p.id, p.as_of_date, p.holdings, p.strategy
                FROM paper_portfolio p
                LEFT JOIN paper_portfolio_pnl n ON n.portfolio_id = p.id
                WHERE n.id IS NULL {before}
                ORDER BY p.as_of_date DESC, p.id DESC
                LIMIT 1
            """.format(before="AND p.as_of_date < :b" if before_as_of else "")
            row = conn.execute(text(q), {"b": before_as_of} if before_as_of else {}).fetchone()
    finally:
        eng.dispose()
    if not row:
        return None
    import json
    holdings = row[2] if isinstance(row[2], list) else json.loads(row[2])
    return {"id": row[0], "as_of": row[1].isoformat(), "holdings": holdings, "strategy": row[3]}


def build_order_tickets(prev_holdings: list[dict], new_holdings: list[dict],
                        notional: float, target_exposure: float) -> dict:
    """Diff the currently-held book against the new target book into human-placeable
    tickets. Pure function (no I/O). Weights in new_holdings are already scaled by
    the trend overlay, so a risk-off book (all weights 0) yields SELL-everything +
    a full CASH line."""
    prev_w = {h["symbol"]: float(h.get("weight", 0.0) or 0.0) for h in prev_holdings}
    prev_entry = {h["symbol"]: h.get("entry_price") for h in prev_holdings}
    new_w = {h["symbol"]: float(h.get("weight", 0.0) or 0.0) for h in new_holdings}
    new_entry = {h["symbol"]: h.get("entry_price") for h in new_holdings}

    tickets = []
    for sym in sorted(set(prev_w) | set(new_w)):
        pw, nw = prev_w.get(sym, 0.0), new_w.get(sym, 0.0)
        if nw > 0 and pw == 0:
            action = "BUY"
        elif nw == 0 and pw > 0:
            action = "SELL"
        elif nw > 0 and pw > 0:
            action = "HOLD" if abs(nw - pw) < 1e-9 else "REBALANCE"
        else:
            continue
        tickets.append({
            "symbol": sym,
            "action": action,
            "prev_weight": round(pw, 6),
            "target_weight": round(nw, 6),
            "target_value": round(nw * notional, 2),
            "ref_price": new_entry.get(sym) if nw > 0 else prev_entry.get(sym),
        })

    invested = sum(t["target_value"] for t in tickets if t["action"] != "SELL")
    cash_value = round(notional - invested, 2)
    n_buy = sum(t["action"] == "BUY" for t in tickets)
    n_sell = sum(t["action"] == "SELL" for t in tickets)
    n_hold = sum(t["action"] in ("HOLD", "REBALANCE") for t in tickets)
    return {
        "tickets": tickets,
        "cash_value": cash_value,
        "cash_weight": round(1.0 - target_exposure, 6),
        "n_buy": n_buy, "n_sell": n_sell, "n_hold": n_hold,
        "turnover_names": n_buy + n_sell,
    }


def run_monthly(as_of: str | None = None, notional: float = 100_000.0,
                live_prices: dict | None = None, exit_prices: dict | None = None,
                benchmark_pct: float | None = None, dry_run: bool = False,
                with_quality: bool = False) -> dict:
    """Execute the full monthly cycle and return a structured report. PAPER ONLY.
    with_quality adds the Tier-2 deep-fundamental quality/forensic annotation on
    the picks (advisory — surfaces accounting red flags, does not auto-drop)."""
    report: dict = {"run_ts": datetime.now(timezone.utc).isoformat(), "dry_run": dry_run,
                    "model_version": papertrack.ledger.git_commit()}

    # 1. data freshness gate
    fresh = data_freshness()
    report["data_freshness"] = fresh

    # 3. signal (compute first so we know the new as_of for scoring boundary)
    pf = papertrack.compute_portfolio(as_of)
    report["signal"] = {"as_of": pf["as_of"], "universe_size": pf["universe_size"],
                        "n_holdings": pf["n_holdings"], "regime": pf["regime"],
                        "target_exposure": pf["target_exposure"]}

    # Tier-2 quality/forensic overlay (advisory annotation on the picks)
    if with_quality:
        from src import quality
        report["quality"] = quality.annotate([h["symbol"] for h in pf["holdings"]], pf["as_of"])

    # 2. score the last elapsed, unscored book (against exit_prices or Dawn close)
    prev = latest_unscored_portfolio(before_as_of=pf["as_of"])
    score = None
    if prev:
        gap = (date.fromisoformat(pf["as_of"]) - date.fromisoformat(prev["as_of"])).days
        if gap >= MIN_HOLD_DAYS:
            xp = exit_prices or _dawn_close_prices([h["symbol"] for h in prev["holdings"]], pf["as_of"])
            if not dry_run:
                score = papertrack.score_portfolio(prev["id"], xp, pf["as_of"], benchmark_pct)
            else:
                score = {"portfolio_id": prev["id"], "exit_date": pf["as_of"], "dry_run": True}
        else:
            score = {"portfolio_id": prev["id"], "skipped": f"only {gap}d held (<{MIN_HOLD_DAYS})"}
    report["scored_prior"] = score

    # 4. order tickets vs currently-held book (the latest recorded book). Diffing
    # against a same-as_of book naturally yields HOLDs — nothing to trade.
    held = latest_unscored_portfolio() or {"holdings": []}
    orders = build_order_tickets(held["holdings"], pf["holdings"], notional, pf["target_exposure"])
    report["held_book"] = {"id": held.get("id"), "as_of": held.get("as_of")}
    report["order_tickets"] = orders

    # 5. record (dup-guarded)
    existing = papertrack.existing_portfolio_for(pf["as_of"], pf["strategy"])
    if dry_run:
        report["recorded"] = {"portfolio_id": None, "dry_run": True, "existing": existing}
    else:
        pid = papertrack.record_portfolio(pf, notional=notional, live_prices=live_prices)
        report["recorded"] = {"portfolio_id": pid, "existing_blocked": existing if pid is None else None}

    return report


def _dawn_close_prices(symbols: list[str], as_of: str) -> dict:
    """Latest Dawn close at//before `as_of` for the given symbols (exit-price fallback)."""
    if not symbols:
        return {}
    eng = create_engine(DAWN_URL)
    try:
        with eng.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol) symbol, close
                FROM stock_prices_daily
                WHERE symbol = ANY(:syms) AND date <= :d AND close > 0
                ORDER BY symbol, date DESC
            """), {"syms": list(symbols), "d": as_of}).fetchall()
    finally:
        eng.dispose()
    return {r[0]: float(r[1]) for r in rows}
