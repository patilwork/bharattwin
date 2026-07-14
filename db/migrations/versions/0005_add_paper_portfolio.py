"""Add append-only paper-portfolio tables for the live out-of-sample track.

The cross-sectional momentum+value composite is a *portfolio*, not a single
direction call, so it gets its own append-only tables (parallel to the
forecast ledger): the portfolio as-formed, and its realised P&L when scored.

Revision ID: 0005
Revises: 0004
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_portfolio",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("created_ts", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("strategy", sa.Text, nullable=False),
        sa.Column("model_version", sa.Text, nullable=False),
        sa.Column("universe_size", sa.Integer, nullable=True),
        sa.Column("n_holdings", sa.Integer, nullable=True),
        sa.Column("notional", sa.Float, nullable=True),
        sa.Column("holdings", JSONB, nullable=False),   # [{symbol, weight, entry_price, composite_z, mktcap_cr}]
        sa.Column("params", JSONB, nullable=True),
        sa.UniqueConstraint("as_of_date", "strategy", "model_version", name="uq_paper_portfolio_asm"),
    )
    op.create_index("ix_paper_portfolio_as_of", "paper_portfolio", ["as_of_date"])

    op.create_table(
        "paper_portfolio_pnl",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("portfolio_id", sa.BigInteger, sa.ForeignKey("paper_portfolio.id", ondelete="CASCADE"), nullable=False),
        sa.Column("scored_ts", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("exit_date", sa.Date, nullable=False),
        sa.Column("gross_return_pct", sa.Float, nullable=True),
        sa.Column("net_return_pct", sa.Float, nullable=True),
        sa.Column("benchmark_return_pct", sa.Float, nullable=True),
        sa.Column("excess_return_pct", sa.Float, nullable=True),
        sa.Column("holdings_pnl", JSONB, nullable=True),
        sa.UniqueConstraint("portfolio_id", "exit_date", name="uq_paper_pnl_portfolio_exit"),
    )
    op.create_index("ix_paper_pnl_portfolio", "paper_portfolio_pnl", ["portfolio_id"])


def downgrade() -> None:
    op.drop_index("ix_paper_pnl_portfolio")
    op.drop_table("paper_portfolio_pnl")
    op.drop_index("ix_paper_portfolio_as_of")
    op.drop_table("paper_portfolio")
