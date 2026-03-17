"""
BharatTwin public API — serves predictions, scoreboard, and agent data.

Lightweight FastAPI server for the public dashboard and social sharing.

Usage:
    uvicorn src.api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import io
import contextlib
import json
import os
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

app = FastAPI(
    title="BharatTwin API",
    description="8 AI agents predict the Indian stock market. Public scoreboard.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


@app.get("/")
def root():
    return {
        "name": "BharatTwin",
        "tagline": "8 AI agents predict the Indian stock market",
        "version": "0.1.0",
        "endpoints": ["/market", "/scoreboard", "/agents", "/health"],
    }


@app.get("/market")
def market_overview():
    """Latest market state with index prices and factors."""
    engine = _get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT session_id, nifty50_close, banknifty_close, india_vix, factor_map
            FROM market_state
            WHERE universe_id = 'nifty50' AND nifty50_close IS NOT NULL
            ORDER BY session_id DESC LIMIT 10
        """)).mappings().fetchall()
    engine.dispose()

    data = []
    for row in rows:
        fm = row["factor_map"]
        if fm and isinstance(fm, str):
            fm = json.loads(fm)
        elif fm is None:
            fm = {}

        data.append({
            "date": row["session_id"].replace("CM_", ""),
            "nifty": float(row["nifty50_close"]) if row["nifty50_close"] else None,
            "banknifty": float(row["banknifty_close"]) if row["banknifty_close"] else None,
            "vix": float(row["india_vix"]) if row["india_vix"] else None,
            "momentum_1d": fm.get("momentum_1d"),
            "momentum_20d": fm.get("momentum_20d"),
            "volatility_20d": fm.get("volatility_20d"),
            "breadth_adv": fm.get("breadth_adv"),
            "breadth_dec": fm.get("breadth_dec"),
        })

    return {"market": list(reversed(data))}


@app.get("/scoreboard")
def scoreboard():
    """Replay scoreboard — all cases with predictions vs actuals."""
    cases = []

    replay_defs = [
        {
            "name": "RBI Surprise Hike",
            "date": "2022-05-04",
            "event_type": "monetary_policy",
            "run_module": "src.replay.run_incontext_007",
            "run_fn": "run_incontext_replay",
            "actual_module": "src.replay.cases.rbi_hike_may2022",
        },
        {
            "name": "Election Results (Crash)",
            "date": "2024-06-04",
            "event_type": "political",
            "run_module": "src.replay.run_election_010",
            "run_fn": "run_election_replay",
            "actual_module": "src.replay.cases.election_june2024",
        },
        {
            "name": "Exit Poll Euphoria (Rally)",
            "date": "2024-06-03",
            "event_type": "political",
            "run_module": "src.replay.run_exitpoll_011",
            "run_fn": "run_exitpoll_replay",
            "actual_module": "src.replay.cases.exit_poll_june2024",
        },
        {
            "name": "No-Event Day",
            "date": "2026-03-17",
            "event_type": "none",
            "run_module": "src.replay.run_live_008",
            "run_fn": "run_live_simulation",
            "actual_module": "src.replay.run_live_008",
        },
    ]

    import importlib
    for rd in replay_defs:
        try:
            rm = importlib.import_module(rd["run_module"])
            cm = importlib.import_module(rd["actual_module"])
            fn = getattr(rm, rd["run_fn"])
            actual = getattr(cm, "ACTUAL_NIFTY_RETURN_PCT")

            with contextlib.redirect_stdout(io.StringIO()):
                consensus = fn()

            agents = []
            for dec in consensus.decisions:
                agents.append({
                    "id": dec.agent_id,
                    "role": dec.agent_role,
                    "direction": dec.direction.value,
                    "conviction": dec.conviction,
                    "base_pct": dec.nifty_return.base_pct,
                    "error_pp": round(abs(dec.nifty_return.base_pct - actual), 2),
                })

            error = abs(consensus.avg_return_pct - actual)
            dir_pred = consensus.consensus_direction.value
            dir_actual = "BUY" if actual > 0.25 else ("SELL" if actual < -0.25 else "HOLD")

            cases.append({
                "name": rd["name"],
                "date": rd["date"],
                "event_type": rd["event_type"],
                "actual_pct": actual,
                "predicted_pct": consensus.avg_return_pct,
                "error_pp": round(error, 2),
                "direction_predicted": dir_pred,
                "direction_actual": dir_actual,
                "direction_correct": dir_pred == dir_actual or (dir_pred == "HOLD" and abs(actual) < 1.0),
                "range_low": consensus.return_range.low_pct,
                "range_high": consensus.return_range.high_pct,
                "in_range": consensus.return_range.low_pct <= actual <= consensus.return_range.high_pct,
                "bull_count": consensus.bull_count,
                "bear_count": consensus.bear_count,
                "neutral_count": consensus.neutral_count,
                "agents": agents,
            })
        except Exception as e:
            cases.append({"name": rd["name"], "date": rd["date"], "error": str(e)})

    valid = [c for c in cases if "actual_pct" in c]
    avg_error = sum(c["error_pp"] for c in valid) / len(valid) if valid else 0
    dir_correct = sum(1 for c in valid if c.get("direction_correct")) if valid else 0

    return {
        "scoreboard": cases,
        "summary": {
            "total_cases": len(valid),
            "avg_error_pp": round(avg_error, 2),
            "direction_correct": dir_correct,
            "direction_total": len(valid),
            "in_range": sum(1 for c in valid if c.get("in_range")),
        },
    }


@app.get("/agents")
def agent_profiles():
    """Agent personas with calibration data."""
    from src.agents.personas import ALL_PERSONAS

    # Run calibration silently
    from src.calibration import calibrate
    with contextlib.redirect_stdout(io.StringIO()):
        scores = calibrate()

    agents = []
    for p in ALL_PERSONAS:
        s = scores.get(p.agent_id)
        agents.append({
            "id": p.agent_id,
            "role": p.role,
            "description": p.description,
            "focus_areas": p.focus_areas,
            "biases": p.biases,
            "risk_tolerance": p.risk_tolerance,
            "time_horizon": p.time_horizon,
            "calibration": {
                "avg_error_pp": round(s.total_error / s.cases, 2) if s and s.cases else None,
                "direction_accuracy_pct": round(s.direction_correct / s.cases * 100) if s and s.cases else None,
                "cases": s.cases if s else 0,
            } if s else None,
        })

    return {"agents": agents}


@app.get("/health")
def health():
    """System health check."""
    from src.health import run_health_check
    results = run_health_check()
    checks = {name: {"ok": ok, "message": msg} for name, (ok, msg) in results.items()}
    all_ok = all(v["ok"] for v in checks.values())
    return {"status": "healthy" if all_ok else "degraded", "checks": checks}
