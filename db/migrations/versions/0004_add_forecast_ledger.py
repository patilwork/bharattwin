"""Add append-only forecast ledger + scores.

The spine of falsifiable prediction tracking: an immutable record of every
forecast (input snapshot + its hash, model version, agent outputs, consensus,
data-quality grade) and a SEPARATE immutable record of the realised outcome.
Predictions are never overwritten; scoring appends to a different table.

Revision ID: 0004
Revises: 0003
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "forecast_ledger",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        # run_date = the market-state date the forecast is based on (matches agent_decisions.run_date)
        sa.Column("run_date", sa.Date, nullable=False),
        sa.Column("prediction_ts", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("horizon", sa.Text, nullable=False, server_default="next_session"),
        # reproducibility anchors
        sa.Column("input_snapshot_hash", sa.Text, nullable=False),
        sa.Column("model_version", sa.Text, nullable=False),   # git commit short hash
        sa.Column("provider", sa.Text, nullable=True),
        sa.Column("model", sa.Text, nullable=True),
        # consensus summary (denormalised for fast queries)
        sa.Column("consensus_direction", sa.Text, nullable=True),
        sa.Column("consensus_return_pct", sa.Float, nullable=True),
        sa.Column("range_low_pct", sa.Float, nullable=True),
        sa.Column("range_base_pct", sa.Float, nullable=True),
        sa.Column("range_high_pct", sa.Float, nullable=True),
        sa.Column("avg_confidence_pct", sa.Float, nullable=True),
        sa.Column("bull_count", sa.Integer, nullable=True),
        sa.Column("bear_count", sa.Integer, nullable=True),
        sa.Column("neutral_count", sa.Integer, nullable=True),
        # full immutable payloads
        sa.Column("agent_outputs", JSONB, nullable=True),
        sa.Column("consensus", JSONB, nullable=True),
        sa.Column("input_snapshot", JSONB, nullable=True),   # the PIT state actually seen
        sa.Column("data_quality", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        # Idempotency: same inputs + same model = same row; changed inputs/model appends a new one.
        sa.UniqueConstraint("run_date", "model_version", "input_snapshot_hash", name="uq_forecast_ledger_rmi"),
    )
    op.create_index("ix_forecast_ledger_run_date", "forecast_ledger", ["run_date"])

    op.create_table(
        "forecast_score",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("ledger_id", sa.BigInteger, sa.ForeignKey("forecast_ledger.id", ondelete="CASCADE"), nullable=False),
        sa.Column("scored_ts", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("outcome_session", sa.Text, nullable=True),
        sa.Column("t1_close", sa.Float, nullable=True),
        sa.Column("t_close", sa.Float, nullable=True),
        sa.Column("actual_return_pct", sa.Float, nullable=True),
        sa.Column("actual_direction", sa.Text, nullable=True),
        sa.Column("direction_correct", sa.Boolean, nullable=True),
        sa.Column("error_pp", sa.Float, nullable=True),
        sa.Column("brier", sa.Float, nullable=True),   # calibration, filled later
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        # One immutable score per ledger row — never rescore/overwrite.
        sa.UniqueConstraint("ledger_id", name="uq_forecast_score_ledger"),
    )
    op.create_index("ix_forecast_score_ledger_id", "forecast_score", ["ledger_id"])


def downgrade() -> None:
    op.drop_index("ix_forecast_score_ledger_id")
    op.drop_table("forecast_score")
    op.drop_index("ix_forecast_ledger_run_date")
    op.drop_table("forecast_ledger")
