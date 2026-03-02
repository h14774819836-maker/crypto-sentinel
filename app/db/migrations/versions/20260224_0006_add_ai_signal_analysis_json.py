"""add_ai_signal_analysis_json

Revision ID: 20260224_0006
Revises: 20260224_0005
Create Date: 2026-02-24 23:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260224_0006"
down_revision = "20260224_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("analysis_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.drop_column("analysis_json")
