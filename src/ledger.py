"""
Forecast ledger — the append-only spine of falsifiable prediction tracking.

Two immutable records per forecast:
  1. forecast_ledger — the prediction as-made: a hash of the exact input snapshot
     (and the snapshot itself), the model version, the per-agent outputs, the
     consensus, and a data-quality grade. Written at prediction time. Never updated.
  2. forecast_score — the realised outcome, written later against the ledger row.
     Kept separate so the prediction is never mutated or retroactively rebuilt.

Idempotency (not mutation): re-recording the same (run_date, model_version,
input_snapshot_hash) is a no-op; a changed model or changed inputs appends a
new row, preserving full history.

Usage:
    from src import ledger
    lid = ledger.record_forecast(d, state, consensus_result, provider, model)
    ledger.record_score(d, t1_close, t_close, outcome_session)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from datetime import date
from functools import lru_cache
from typing import Any, Optional

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Keys we expect in a healthy market_state snapshot; missing ones lower the grade.
_QUALITY_KEYS = [
    "nifty_close",
    "banknifty_close",
    "vix",
    "usdinr",
    "breadth_adv",
    "futures_oi_nifty",
    "factor_map",
]


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


@lru_cache(maxsize=1)
def git_commit() -> str:
    """Short git commit of the working tree — the model/version anchor."""
    try:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=here, stderr=subprocess.DEVNULL
        )
        return out.decode().strip() or "unknown"
    except Exception:
        return "unknown"


def _canonical(obj: Any) -> str:
    """Deterministic JSON for hashing (sorted keys, str-coerced)."""
    return json.dumps(obj, sort_keys=True, default=str, separators=(",", ":"))


def snapshot_hash(state: dict) -> str:
    """Stable sha256 of the input market-state snapshot the model actually saw."""
    return hashlib.sha256(_canonical(state).encode()).hexdigest()


def grade_data_quality(state: dict) -> dict:
    """Grade the input snapshot: which expected fields are present/usable."""
    missing = [k for k in _QUALITY_KEYS if state.get(k) in (None, "", [], {})]
    n = len(_QUALITY_KEYS)
    present = n - len(missing)
    score = round(present / n, 3) if n else 0.0
    grade = "A" if score >= 0.85 else ("B" if score >= 0.6 else "C")
    return {"grade": grade, "score": score, "present": present, "total": n, "missing": missing}


def _agent_outputs(result) -> list[dict]:
    out = []
    for dec in getattr(result, "decisions", []) or []:
        out.append(
            {
                "agent_id": dec.agent_id,
                "agent_role": dec.agent_role,
                "direction": dec.direction.value,
                "confidence_pct": dec.confidence_pct,
                "conviction": dec.conviction,
                "base_pct": dec.nifty_return.base_pct,
            }
        )
    return out


def record_forecast(
    run_date: date,
    state: dict,
    result,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> Optional[int]:
    """Append an immutable forecast record. Returns the ledger id, or None if it
    already existed (idempotent no-op). `result` is a ConsensusResult."""
    agents = _agent_outputs(result)
    avg_conf = round(sum(a["confidence_pct"] for a in agents) / len(agents), 2) if agents else None

    consensus_payload = result.model_dump(mode="json") if hasattr(result, "model_dump") else None

    row = {
        "run_date": run_date,
        "horizon": "next_session",
        "input_snapshot_hash": snapshot_hash(state),
        "model_version": git_commit(),
        "provider": provider,
        "model": model,
        "consensus_direction": result.consensus_direction.value,
        "consensus_return_pct": result.avg_return_pct,
        "range_low_pct": result.return_range.low_pct,
        "range_base_pct": result.return_range.base_pct,
        "range_high_pct": result.return_range.high_pct,
        "avg_confidence_pct": avg_conf,
        "bull_count": result.bull_count,
        "bear_count": result.bear_count,
        "neutral_count": result.neutral_count,
        "agent_outputs": json.dumps(agents),
        "consensus": json.dumps(consensus_payload),
        "input_snapshot": json.dumps(state, default=str),
        "data_quality": json.dumps(grade_data_quality(state)),
    }

    sql = text(
        """
        INSERT INTO forecast_ledger
            (run_date, horizon, input_snapshot_hash, model_version, provider, model,
             consensus_direction, consensus_return_pct, range_low_pct, range_base_pct,
             range_high_pct, avg_confidence_pct, bull_count, bear_count, neutral_count,
             agent_outputs, consensus, input_snapshot, data_quality)
        VALUES
            (:run_date, :horizon, :input_snapshot_hash, :model_version, :provider, :model,
             :consensus_direction, :consensus_return_pct, :range_low_pct, :range_base_pct,
             :range_high_pct, :avg_confidence_pct, :bull_count, :bear_count, :neutral_count,
             CAST(:agent_outputs AS jsonb), CAST(:consensus AS jsonb),
             CAST(:input_snapshot AS jsonb), CAST(:data_quality AS jsonb))
        ON CONFLICT ON CONSTRAINT uq_forecast_ledger_rmi DO NOTHING
        RETURNING id
        """
    )
    engine = _get_engine()
    try:
        with engine.begin() as conn:
            res = conn.execute(sql, row).fetchone()
    finally:
        engine.dispose()

    if res is None:
        logger.info("forecast_ledger: %s already recorded (idempotent no-op)", run_date)
        return None
    logger.info("forecast_ledger: recorded id=%s for %s (%s)", res[0], run_date, row["model_version"])
    return res[0]


def record_score(
    run_date: date,
    t1_close: float,
    t_close: float,
    outcome_session: Optional[str] = None,
) -> int:
    """Append immutable outcome scores for every unscored ledger row on run_date.
    Returns the number of score rows written. Uses FLAT_BAND_PCT from scoring for
    a single consistent direction rule."""
    from src.scoring import FLAT_BAND_PCT

    actual_pct = round((t_close - t1_close) / t1_close * 100, 4) if t1_close else 0.0
    actual_dir = "BUY" if actual_pct > FLAT_BAND_PCT else ("SELL" if actual_pct < -FLAT_BAND_PCT else "HOLD")

    engine = _get_engine()
    written = 0
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT l.id, l.consensus_direction, l.consensus_return_pct
                    FROM forecast_ledger l
                    LEFT JOIN forecast_score s ON s.ledger_id = l.id
                    WHERE l.run_date = :d AND s.id IS NULL
                    """
                ),
                {"d": run_date},
            ).fetchall()

            for lid, pred_dir, pred_pct in rows:
                error_pp = round(abs((pred_pct or 0.0) - actual_pct), 4)
                conn.execute(
                    text(
                        """
                        INSERT INTO forecast_score
                            (ledger_id, outcome_session, t1_close, t_close,
                             actual_return_pct, actual_direction, direction_correct, error_pp)
                        VALUES
                            (:ledger_id, :outcome_session, :t1_close, :t_close,
                             :actual_return_pct, :actual_direction, :direction_correct, :error_pp)
                        ON CONFLICT ON CONSTRAINT uq_forecast_score_ledger DO NOTHING
                        """
                    ),
                    {
                        "ledger_id": lid,
                        "outcome_session": outcome_session,
                        "t1_close": t1_close,
                        "t_close": t_close,
                        "actual_return_pct": actual_pct,
                        "actual_direction": actual_dir,
                        "direction_correct": (pred_dir == actual_dir),
                        "error_pp": error_pp,
                    },
                )
                written += 1
    finally:
        engine.dispose()

    logger.info("forecast_score: wrote %d score(s) for %s (actual %.2f%% → %s)", written, run_date, actual_pct, actual_dir)
    return written


def get_unscored() -> list[dict]:
    """Ledger rows that have no score yet."""
    engine = _get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT l.id, l.run_date, l.consensus_direction, l.model_version
                    FROM forecast_ledger l
                    LEFT JOIN forecast_score s ON s.ledger_id = l.id
                    WHERE s.id IS NULL
                    ORDER BY l.run_date
                    """
                )
            ).mappings().fetchall()
        return [dict(r) for r in rows]
    finally:
        engine.dispose()
