"""
Health check — verify all BharatTwin components are operational.

Usage:
    python -m src.health
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, timedelta

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def check_database() -> tuple[bool, str]:
    """Check database connectivity and table existence."""
    try:
        engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
        with engine.connect() as conn:
            tables = conn.execute(text(
                "SELECT tablename FROM pg_tables WHERE schemaname='public'"
            )).fetchall()
            table_names = [t[0] for t in tables]

            required = ["market_state", "bhavcopy_raw", "calendar_store",
                        "flow_store", "event_store", "agent_decisions"]
            missing = [t for t in required if t not in table_names]

            if missing:
                return False, f"Missing tables: {missing}"

            # Count rows
            counts = {}
            for t in required:
                count = conn.execute(text(f"SELECT count(*) FROM {t}")).scalar()
                counts[t] = count

        engine.dispose()
        return True, f"OK — {counts}"
    except Exception as e:
        return False, f"FAIL — {e}"


def check_market_state() -> tuple[bool, str]:
    """Check market_state has recent data with index prices."""
    try:
        engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT session_id, nifty50_close, india_vix, factor_map
                FROM market_state
                WHERE universe_id = 'nifty50' AND nifty50_close IS NOT NULL
                ORDER BY session_id DESC LIMIT 1
            """)).first()
        engine.dispose()

        if not row:
            return False, "No market_state rows with index prices"

        has_factors = row[3] is not None
        return True, f"Latest: {row[0]} Nifty={row[1]} VIX={row[2]} factors={'YES' if has_factors else 'NO'}"
    except Exception as e:
        return False, f"FAIL — {e}"


def check_bhavcopy() -> tuple[bool, str]:
    """Check bhavcopy coverage."""
    try:
        engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT MAX(data_date), COUNT(DISTINCT data_date)
                FROM bhavcopy_raw
            """)).first()
        engine.dispose()

        if not row or not row[0]:
            return False, "No bhavcopy data"

        return True, f"Latest: {row[0]} | {row[1]} dates total"
    except Exception as e:
        return False, f"FAIL — {e}"


def check_api_key() -> tuple[bool, str]:
    """Check if any LLM API key is set (Sarvam, Anthropic, or OpenAI-compatible)."""
    from src.agents.llm_providers import available_providers, get_provider
    available = available_providers()
    if available:
        current = get_provider()
        return True, f"Provider: {current} | Available: {', '.join(available)}"
    return False, "NO LLM KEY SET — agents will run in prompt mode only. Set SARVAM_API_KEY or ANTHROPIC_API_KEY."


def check_agents() -> tuple[bool, str]:
    """Verify agent framework loads correctly."""
    try:
        from src.agents.personas import ALL_PERSONAS, PERSONA_BY_ID
        from src.agents.base import BaseAgent
        from src.agents.schemas import AgentDecision, ConsensusResult

        count = len(ALL_PERSONAS)
        ids = list(PERSONA_BY_ID.keys())
        return True, f"{count} agents loaded: {', '.join(ids)}"
    except Exception as e:
        return False, f"FAIL — {e}"


def check_tests() -> tuple[bool, str]:
    """Run tests and check if they pass."""
    import subprocess
    try:
        result = subprocess.run(
            ["python3", "-m", "pytest", "tests/", "-q", "--tb=no"],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        last_line = result.stdout.strip().split("\n")[-1] if result.stdout else ""
        passed = result.returncode == 0
        return passed, last_line
    except Exception as e:
        return False, f"FAIL — {e}"


def run_health_check() -> dict[str, tuple[bool, str]]:
    """Run all health checks and return results."""
    checks = {
        "Database": check_database,
        "Market State": check_market_state,
        "Bhavcopy": check_bhavcopy,
        "API Key": check_api_key,
        "Agents": check_agents,
        "Tests": check_tests,
    }

    results = {}
    for name, fn in checks.items():
        ok, msg = fn()
        results[name] = (ok, msg)

    return results


def check_readiness() -> tuple[str, list[str]]:
    """
    Determine system readiness level.

    Returns:
        (level, reasons) where level is one of:
        - "production-ok": live daily predictions can run and be scored
        - "research-ok": backtesting and replay work, but no live tracking
        - "setup-incomplete": missing critical components
    """
    issues = []

    # Check for live prediction capability
    from src.agents.llm_providers import has_api_key
    if not has_api_key():
        issues.append("No LLM API key — agents can only run in prompt mode")

    # Check for stored predictions
    try:
        engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
        with engine.connect() as conn:
            decisions = conn.execute(text("SELECT count(*) FROM agent_decisions")).scalar()
            latest_ms = conn.execute(text(
                "SELECT MAX(session_id) FROM market_state WHERE factor_map IS NOT NULL"
            )).scalar()
            bhavcopy_dates = conn.execute(text(
                "SELECT count(DISTINCT data_date) FROM bhavcopy_raw"
            )).scalar()
            events = conn.execute(text("SELECT count(*) FROM event_store")).scalar()
        engine.dispose()
    except Exception:
        return "setup-incomplete", ["Cannot connect to database"]

    if bhavcopy_dates < 100:
        issues.append(f"Only {bhavcopy_dates} bhavcopy dates — need 100+ for research")
    if events < 5:
        issues.append(f"Only {events} events in event_store — need 5+ for replay validation")
    if decisions == 0:
        issues.append("No stored agent_decisions — no live predictions have been scored")
    if not latest_ms:
        issues.append("No market_state rows with factor_map — factors not computed")

    if "No LLM API key" in str(issues) or bhavcopy_dates < 100:
        return "setup-incomplete", issues
    elif decisions == 0:
        return "research-ok", issues
    else:
        return "production-ok", issues


def print_health() -> None:
    """Print health check report."""
    results = run_health_check()

    print()
    print("=" * 70)
    print("BHARATTWIN HEALTH CHECK")
    print("=" * 70)

    all_ok = True
    for name, (ok, msg) in results.items():
        status = "PASS" if ok else "FAIL"
        icon = "+" if ok else "X"
        if not ok:
            all_ok = False
        print(f"  [{icon}] {name:<15} {status:<6} {msg}")

    # Readiness assessment
    level, reasons = check_readiness()
    level_icon = {"production-ok": "+", "research-ok": "~", "setup-incomplete": "X"}
    print(f"\n  [{level_icon.get(level, '?')}] Readiness       {level.upper()}")
    for r in reasons:
        print(f"      — {r}")

    print("=" * 70)
    overall = "ALL CHECKS PASSED" if all_ok and level == "production-ok" else (
        "RESEARCH READY" if level == "research-ok" else "SOME CHECKS FAILED"
    )
    print(f"  Overall: {overall}")
    print("=" * 70)


if __name__ == "__main__":
    print_health()
