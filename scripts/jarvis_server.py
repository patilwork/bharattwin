#!/usr/bin/env python3
"""
JARVIS — live HUD for the BharatTwin super-quant paper book.

A tiny stdlib http.server (no web-framework dependency) that serves a self-
contained Jarvis-style dashboard and a /api/state JSON feed. State is REAL: it
reuses the tested modules — the Tier-1.2 trend regime, the Tier-2 quality/forensic
screen, Dawn data-freshness, and the live paper_portfolio — via one orchestrator
dry-run (nothing is written). A background thread refreshes the heavy snapshot on
an interval; the browser polls the cached state.

Live marks: Dawn closes are end-of-day (often stale intraday), so for a truly live
mark-to-market drop current LTPs into logs/live_ltp.json ({ "SYMBOL": price, ... },
e.g. fetched via the Kite MCP in an interactive session). If absent, the book shows
entry prices and an "awaiting live marks" state — honest, not faked.

Usage:
  DATABASE_URL=... DAWN_URL=... python3 scripts/jarvis_server.py [--port 7842] [--refresh 90]
  then open http://localhost:7842
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sqlalchemy import create_engine, text

from src import orchestrator, papertrack

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
HTML_PATH = HERE / "jarvis_dashboard.html"
LIVE_LTP_PATH = REPO / "logs" / "live_ltp.json"
BHARAT_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/bharattwin")
DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")

# Validated strategy vitals (from the backtest/robustness docs) — the HUD's "specs".
VITALS = {
    "sharpe_base": 1.19, "sharpe_overlay": 1.40,
    "maxdd_base": -37.6, "maxdd_overlay": -19.4,
    "deflated_sharpe": 0.82, "cpcv_oos_positive_pct": 100, "pbo_pct": 29,
    "alpha_est_lo": 4.0, "alpha_est_hi": 6.0,
    "permutation_p": "<0.001", "ic_t_momentum": 4.04,
}

STATE: dict = {"status": "booting", "snapshot": None, "built_ts": None, "error": None}
_LOCK = threading.Lock()


def _latest_closes(symbols: list[str]) -> dict:
    if not symbols:
        return {}
    eng = create_engine(DAWN_URL)
    try:
        with eng.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol) symbol, close, date
                FROM stock_prices_daily
                WHERE symbol = ANY(:s) AND close > 0
                ORDER BY symbol, date DESC
            """), {"s": list(symbols)}).fetchall()
    finally:
        eng.dispose()
    return {r[0]: {"close": float(r[1]), "date": str(r[2])} for r in rows}


def _held_book() -> dict | None:
    eng = create_engine(BHARAT_URL)
    try:
        with eng.connect() as conn:
            row = conn.execute(text("""
                SELECT id, as_of_date, notional, holdings FROM paper_portfolio
                ORDER BY as_of_date DESC, id DESC LIMIT 1
            """)).fetchone()
    finally:
        eng.dispose()
    if not row:
        return None
    holdings = row[3] if isinstance(row[3], list) else json.loads(row[3])
    return {"id": row[0], "as_of": str(row[1]), "notional": float(row[2] or 0), "holdings": holdings}


def _mark_to_market(book: dict, quality: dict | None) -> dict:
    """Per-name and portfolio return, entry -> live LTP (if provided) or latest close."""
    live = {}
    src = "entry"
    if LIVE_LTP_PATH.exists():
        try:
            live = {k.upper(): float(v) for k, v in json.loads(LIVE_LTP_PATH.read_text()).items() if v}
            src = "live_ltp"
        except Exception:
            live = {}
    syms = [h["symbol"] for h in book["holdings"]]
    closes = _latest_closes(syms)
    q_by = (quality or {}).get("by_symbol", {})

    names, rets = [], []
    for h in book["holdings"]:
        s = h["symbol"]
        entry = h.get("entry_price")
        mark = live.get(s) or (closes.get(s, {}).get("close"))
        ret = ((mark - entry) / entry * 100) if (entry and mark) else None
        if ret is not None:
            rets.append(ret * float(h.get("weight", 0) or (1.0 / len(book["holdings"]))))
        qf = q_by.get(s, {})
        names.append({
            "symbol": s, "entry": entry, "mark": mark, "ret_pct": ret,
            "weight": h.get("weight"), "mark_date": closes.get(s, {}).get("date"),
            "q_score": qf.get("quality_score"), "flags": qf.get("red_flags", []),
        })
    port_ret = sum(rets) if rets else None
    names_sorted = sorted(names, key=lambda n: (n["ret_pct"] is None, -(n["ret_pct"] or 0)))
    return {"mark_source": src, "port_ret_pct": port_ret, "n_marked": len(rets),
            "names": names_sorted}


def build_snapshot() -> dict:
    """Assemble the full live state from the tested modules (orchestrator dry-run)."""
    rep = orchestrator.run_monthly(dry_run=True, with_quality=True)
    book = _held_book()
    mtm = _mark_to_market(book, rep.get("quality")) if book else None
    return {
        "server_ts": datetime.now(timezone.utc).isoformat(),
        "model_version": rep.get("model_version"),
        "data_freshness": rep.get("data_freshness"),
        "regime": rep["signal"]["regime"],
        "signal": {k: rep["signal"][k] for k in ("as_of", "universe_size", "n_holdings", "target_exposure")},
        "quality_summary": {"n_flagged": rep.get("quality", {}).get("n_flagged"),
                            "n_covered": rep.get("quality", {}).get("n_covered"),
                            "flagged": rep.get("quality", {}).get("flagged", [])},
        "book": {"id": book["id"], "as_of": book["as_of"], "notional": book["notional"]} if book else None,
        "mtm": mtm,
        "vitals": VITALS,
    }


def _refresh_loop(interval: int):
    while True:
        try:
            snap = build_snapshot()
            with _LOCK:
                STATE.update(status="online", snapshot=snap,
                             built_ts=time.time(), error=None)
        except Exception as e:  # keep serving last good state
            with _LOCK:
                STATE.update(error=f"{type(e).__name__}: {e}")
                if STATE["snapshot"] is None:
                    STATE["status"] = "error"
        time.sleep(interval)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def _send(self, code, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/state"):
            with _LOCK:
                payload = {"status": STATE["status"], "error": STATE["error"],
                           "age_sec": round(time.time() - STATE["built_ts"], 1) if STATE["built_ts"] else None,
                           **(STATE["snapshot"] or {})}
            self._send(200, json.dumps(payload, default=str).encode(), "application/json")
        elif self.path in ("/", "/index.html"):
            try:
                self._send(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(500, b"dashboard html missing", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=7842)
    ap.add_argument("--refresh", type=int, default=90, help="heavy-snapshot refresh seconds")
    args = ap.parse_args()

    # warm the first snapshot synchronously so the first page load has data
    try:
        STATE.update(status="online", snapshot=build_snapshot(), built_ts=time.time())
    except Exception as e:
        STATE.update(status="error", error=f"{type(e).__name__}: {e}")
        print(f"[jarvis] initial snapshot failed: {e}", file=sys.stderr)

    threading.Thread(target=_refresh_loop, args=(args.refresh,), daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"[jarvis] online at http://localhost:{args.port}  (refresh {args.refresh}s)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[jarvis] shutting down")


if __name__ == "__main__":
    main()
