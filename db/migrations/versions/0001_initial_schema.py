"""initial schema — all 5 core tables

Revision ID: 0001
Revises:
Create Date: 2026-03-17
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── market_state ─────────────────────────────────────────────────────────
    # Primary canonical table. One row per (universe, trading session).
    # JSONB for rich context; typed columns for fast range queries.
    op.create_table(
        "market_state",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asof_ts_ist", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.Text, nullable=False),          # e.g. "CM_2022-05-02"
        sa.Column("universe_id", sa.Text, nullable=False),         # "nifty50", "fo_index"
        # Core typed fields (fast queries)
        sa.Column("nifty50_close", sa.Numeric(12, 2)),
        sa.Column("banknifty_close", sa.Numeric(12, 2)),
        sa.Column("india_vix", sa.Numeric(8, 4)),
        sa.Column("repo_rate_pct", sa.Numeric(6, 4)),
        sa.Column("usdinr_ref", sa.Numeric(10, 4)),
        sa.Column("crude_indian_basket_usd", sa.Numeric(10, 4)),
        # Rich JSONB context
        sa.Column("returns_map", JSONB),
        sa.Column("deriv_map", JSONB),
        sa.Column("flow_map", JSONB),
        sa.Column("macro_map", JSONB),
        sa.Column("regime_state", JSONB),
        sa.Column("data_quality", JSONB),
        sa.Column("source_audit", JSONB),
        # Replay invariant — no field has source_ts > this value
        sa.Column("replay_cutoff_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_market_state_universe_ts", "market_state",
                    ["universe_id", "asof_ts_ist"])
    op.create_index("ix_market_state_replay_cutoff", "market_state",
                    ["replay_cutoff_ts"])
    op.create_unique_constraint("uq_market_state_universe_session", "market_state",
                                ["universe_id", "session_id"])

    # ── event_store ───────────────────────────────────────────────────────────
    # Append-only immutable event log. Never update rows.
    op.create_table(
        "event_store",
        sa.Column("event_id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("event_ts_ist", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.Text, nullable=False),
        # policy | macro_data | corporate | geopolitical | microstructure | surveillance
        sa.Column("source_tier", sa.SmallInteger, nullable=False),  # 1, 2, or 3
        sa.Column("source_ref", sa.Text),                           # URL or doc ID
        sa.Column("headline", sa.Text),
        sa.Column("raw_text", sa.Text),
        sa.Column("extracted_entities", JSONB),
        sa.Column("factor_tags", JSONB),                            # which buckets affected
        sa.Column("expected_channel", JSONB),                       # transmission channels
        sa.Column("confidence", sa.Numeric(5, 4)),                  # 0.0–1.0
        sa.Column("replay_available_from_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_event_store_ts", "event_store", ["event_ts_ist"])
    op.create_index("ix_event_store_type", "event_store", ["event_type"])
    op.create_index("ix_event_store_replay_ts", "event_store", ["replay_available_from_ts"])

    # ── calendar_store ────────────────────────────────────────────────────────
    # One row per calendar date. Lookup table for trading calendar logic.
    op.create_table(
        "calendar_store",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("is_trading_day", sa.Boolean, nullable=False),
        sa.Column("holiday_name", sa.Text),                         # null if trading day
        sa.Column("exchange", sa.Text, server_default="NSE"),
        # Expiry flags
        sa.Column("is_nifty_monthly_expiry", sa.Boolean, server_default="false"),
        sa.Column("is_banknifty_monthly_expiry", sa.Boolean, server_default="false"),
        sa.Column("is_finnifty_monthly_expiry", sa.Boolean, server_default="false"),
        sa.Column("is_nifty_weekly_expiry", sa.Boolean, server_default="false"),
        sa.Column("is_banknifty_weekly_expiry", sa.Boolean, server_default="false"),
        # Circuit breaker reference levels (from prior close)
        sa.Column("circuit_l1_pct", sa.Numeric(5, 2), server_default="10.0"),
        sa.Column("circuit_l2_pct", sa.Numeric(5, 2), server_default="15.0"),
        sa.Column("circuit_l3_pct", sa.Numeric(5, 2), server_default="20.0"),
        sa.Column("nifty_prior_close", sa.Numeric(12, 2)),
        sa.Column("settlement_type", sa.Text),                      # "T+1" or special
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_calendar_trading", "calendar_store", ["is_trading_day"])

    # ── flow_store ────────────────────────────────────────────────────────────
    # Daily FII/DII flows. Rows are provisional on insert, updated when confirmed.
    op.create_table(
        "flow_store",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("participant", sa.Text, nullable=False),          # "FII" | "DII" | "MF"
        sa.Column("segment", sa.Text, nullable=False),              # "cash" | "derivatives"
        sa.Column("buy_crore", sa.Numeric(14, 2)),
        sa.Column("sell_crore", sa.Numeric(14, 2)),
        sa.Column("net_crore", sa.Numeric(14, 2)),
        sa.Column("is_provisional", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("confirmed_at", sa.DateTime(timezone=True)),      # null until confirmed
        sa.Column("source_ref", sa.Text),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_flow_store_date_participant", "flow_store",
                    ["date", "participant", "segment"])
    op.create_unique_constraint("uq_flow_date_participant_segment", "flow_store",
                                ["date", "participant", "segment"])

    # ── anomaly_store ─────────────────────────────────────────────────────────
    # Microstructure anomaly flags. Append-only; never delete.
    op.create_table(
        "anomaly_store",
        sa.Column("anomaly_id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("detected_ts_ist", sa.DateTime(timezone=True), nullable=False),
        sa.Column("anomaly_type", sa.Text, nullable=False),
        # volume_spike | oi_spike | circular_oi | price_dislocation | ban_breach |
        # operator_signal | fii_block | unusual_pcr
        sa.Column("severity", sa.SmallInteger),                     # 1 (low) – 5 (critical)
        sa.Column("symbol", sa.Text),                               # null = index-level
        sa.Column("exchange_segment", sa.Text),                     # "NSE_CM" | "NSE_FO"
        sa.Column("signal_data", JSONB),                            # raw signal details
        sa.Column("context_snapshot", JSONB),                       # market state at time
        sa.Column("tier3_flag", sa.Boolean, server_default="false"),# Tier 3 source involved
        sa.Column("sebi_disclaimer", sa.Text),                      # mandatory disclaimer
        sa.Column("resolved", sa.Boolean, server_default="false"),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_anomaly_detected_ts", "anomaly_store", ["detected_ts_ist"])
    op.create_index("ix_anomaly_type_severity", "anomaly_store",
                    ["anomaly_type", "severity"])
    op.create_index("ix_anomaly_symbol", "anomaly_store", ["symbol"])

    # Enable pgcrypto for gen_random_uuid()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")


def downgrade() -> None:
    op.drop_table("anomaly_store")
    op.drop_table("flow_store")
    op.drop_table("calendar_store")
    op.drop_table("event_store")
    op.drop_table("market_state")
