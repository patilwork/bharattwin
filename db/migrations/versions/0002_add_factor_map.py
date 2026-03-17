"""Add factor_map JSONB column to market_state.

Revision ID: 0002
Revises: 0001
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("market_state", sa.Column("factor_map", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("market_state", "factor_map")
