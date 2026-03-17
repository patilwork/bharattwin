"""Add agent_decisions table for storing LLM agent predictions.

Revision ID: 0003
Revises: 0002
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_decisions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_date", sa.Date, nullable=False),
        sa.Column("run_ts_ist", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("agent_id", sa.Text, nullable=False),
        sa.Column("agent_role", sa.Text, nullable=False),
        sa.Column("session_id", sa.Text, nullable=False),
        sa.Column("decision", JSONB, nullable=False),
        sa.Column("consensus", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("run_date", "agent_id", "session_id", name="uq_agent_decision_run"),
    )
    op.create_index("ix_agent_decisions_run_date", "agent_decisions", ["run_date"])
    op.create_index("ix_agent_decisions_agent_id", "agent_decisions", ["agent_id"])


def downgrade() -> None:
    op.drop_index("ix_agent_decisions_agent_id")
    op.drop_index("ix_agent_decisions_run_date")
    op.drop_table("agent_decisions")
