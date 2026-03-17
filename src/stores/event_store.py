"""
Event store — insert, query, and manage market events.

Events are the primary exogenous input to the agent framework. They represent
policy decisions, earnings surprises, geopolitical developments, or any catalyst
that agents should react to.

Source tiers:
  1 = Official (RBI announcement, NSE circular, government gazette)
  2 = Verified news (Reuters, Bloomberg, ET Now)
  3 = Social / unverified (Twitter, WhatsApp forwards)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, text

from src.utils.time_utils import IST

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def insert(
    event_ts: datetime,
    event_type: str,
    headline: str,
    raw_text: str = "",
    source_tier: int = 2,
    source_ref: str = "",
    extracted_entities: dict | None = None,
    factor_tags: list[str] | None = None,
    expected_channel: dict | None = None,
    confidence: float = 1.0,
) -> str:
    """
    Insert an event into event_store.

    Args:
        event_ts: When the event occurred (IST).
        event_type: Category — monetary_policy, earnings, geopolitical, regulatory, etc.
        headline: One-line summary.
        raw_text: Full event text/details.
        source_tier: 1=official, 2=verified news, 3=social.
        source_ref: URL or reference ID.
        extracted_entities: Structured data extracted from the event.
        factor_tags: Which factors this event affects (e.g. ["rate_sensitive", "fx"]).
        expected_channel: How this event transmits to markets.
        confidence: Confidence in the event data (0-1).

    Returns:
        The event_id (UUID string).
    """
    event_id = str(uuid.uuid4())
    engine = _get_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO event_store (
                event_id, event_ts_ist, event_type, source_tier, source_ref,
                headline, raw_text, extracted_entities, factor_tags,
                expected_channel, confidence, replay_available_from_ts
            ) VALUES (
                :eid, :ts, :etype, :tier, :sref,
                :headline, :raw_text,
                CAST(:entities AS jsonb), CAST(:tags AS jsonb),
                CAST(:channel AS jsonb), :conf, :replay_ts
            )
        """), {
            "eid": event_id,
            "ts": event_ts,
            "etype": event_type,
            "tier": source_tier,
            "sref": source_ref,
            "headline": headline,
            "raw_text": raw_text,
            "entities": json.dumps(extracted_entities or {}),
            "tags": json.dumps(factor_tags or []),
            "channel": json.dumps(expected_channel or {}),
            "conf": confidence,
            "replay_ts": event_ts,  # Available for replay from event time
        })

    engine.dispose()
    logger.info("event_store: inserted %s — %s", event_id[:8], headline[:60])
    return event_id


def get_by_date(d: date) -> list[dict]:
    """
    Get all events for a given date (by event_ts_ist date part).

    Returns list of event dicts, ordered by event_ts.
    """
    engine = _get_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT event_id, event_ts_ist, event_type, source_tier, headline,
                   raw_text, extracted_entities, factor_tags, expected_channel, confidence
            FROM event_store
            WHERE DATE(event_ts_ist) = :d
            ORDER BY event_ts_ist
        """), {"d": d}).mappings().fetchall()

    engine.dispose()
    return [dict(r) for r in rows]


def get_by_id(event_id: str) -> dict | None:
    """Fetch a single event by ID."""
    engine = _get_engine()

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT * FROM event_store WHERE event_id = :eid
        """), {"eid": event_id}).mappings().first()

    engine.dispose()
    return dict(row) if row else None


def to_agent_event(event: dict) -> dict:
    """
    Convert an event_store row to the agent framework event format.

    The agent framework expects:
        {headline, event_type, source_tier, raw_text, extracted_entities}
    """
    entities = event.get("extracted_entities", {})
    if isinstance(entities, str):
        entities = json.loads(entities)

    return {
        "headline": event["headline"],
        "event_type": event["event_type"],
        "source_tier": event.get("source_tier", 2),
        "raw_text": event.get("raw_text", ""),
        "extracted_entities": entities,
    }


def list_events(
    event_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    List events with optional filters.
    """
    engine = _get_engine()
    conditions = []
    params: dict[str, Any] = {"limit": limit}

    if event_type:
        conditions.append("event_type = :etype")
        params["etype"] = event_type
    if start_date:
        conditions.append("DATE(event_ts_ist) >= :start")
        params["start"] = start_date
    if end_date:
        conditions.append("DATE(event_ts_ist) <= :end")
        params["end"] = end_date

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT event_id, event_ts_ist, event_type, source_tier, headline, confidence
            FROM event_store
            {where}
            ORDER BY event_ts_ist DESC
            LIMIT :limit
        """), params).mappings().fetchall()

    engine.dispose()
    return [dict(r) for r in rows]
