"""add_ai_signal_market_regime

Revision ID: 20260224_0007
Revises: 20260224_0006
Create Date: 2026-02-25 00:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260224_0007"
down_revision = "20260224_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("market_regime", sa.String(length=32), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.drop_column("market_regime")

