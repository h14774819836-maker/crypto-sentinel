"""add_ai_analysis_failures

Revision ID: 20260301_0008
Revises: 20260224_0007
Create Date: 2026-03-01 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260301_0008"
down_revision = "20260224_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ai_analysis_failures",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("phase", sa.String(length=32), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("model_requested", sa.String(length=128), nullable=True),
        sa.Column("model_actual", sa.String(length=128), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("raw_response_excerpt", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("ai_analysis_failures", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_ai_analysis_failures_created_at"), ["created_at"], unique=False)
        batch_op.create_index(batch_op.f("ix_ai_analysis_failures_phase"), ["phase"], unique=False)
        batch_op.create_index(batch_op.f("ix_ai_analysis_failures_symbol"), ["symbol"], unique=False)
        batch_op.create_index(batch_op.f("ix_ai_analysis_failures_task"), ["task"], unique=False)
        batch_op.create_index(batch_op.f("ix_ai_analysis_failures_ts"), ["ts"], unique=False)
        batch_op.create_index(
            "ix_ai_analysis_failures_task_symbol",
            ["task", "symbol"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("ai_analysis_failures", schema=None) as batch_op:
        batch_op.drop_index("ix_ai_analysis_failures_task_symbol")
        batch_op.drop_index(batch_op.f("ix_ai_analysis_failures_ts"))
        batch_op.drop_index(batch_op.f("ix_ai_analysis_failures_task"))
        batch_op.drop_index(batch_op.f("ix_ai_analysis_failures_symbol"))
        batch_op.drop_index(batch_op.f("ix_ai_analysis_failures_phase"))
        batch_op.drop_index(batch_op.f("ix_ai_analysis_failures_created_at"))

    op.drop_table("ai_analysis_failures")
