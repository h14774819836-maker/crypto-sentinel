"""add_ai_signal_model_requested

Revision ID: 20260223_0004
Revises: 9852edfc3689
Create Date: 2026-02-23 18:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260223_0004"
down_revision = "9852edfc3689"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("model_requested", sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.drop_column("model_requested")
