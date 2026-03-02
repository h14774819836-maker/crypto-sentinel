"""add_anomaly_state

Revision ID: 20260224_0005
Revises: 2abc13bf2891
Create Date: 2026-02-24 14:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260224_0005"
down_revision = "2abc13bf2891"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "anomaly_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("state_key", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("event_family", sa.String(length=32), nullable=False),
        sa.Column("direction", sa.String(length=10), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("consecutive_hits", sa.Integer(), nullable=False),
        sa.Column("last_score", sa.Float(), nullable=True),
        sa.Column("last_regime", sa.String(length=32), nullable=True),
        sa.Column("last_metric_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_alert_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_enter_alert_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_escalate_alert_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_escalate_bucket", sa.String(length=32), nullable=True),
        sa.Column("last_alert_kind", sa.String(length=16), nullable=True),
        sa.Column("active_cycle_started_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("anomaly_state", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_anomaly_state_state_key"), ["state_key"], unique=True)
        batch_op.create_index(batch_op.f("ix_anomaly_state_symbol"), ["symbol"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("anomaly_state", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_anomaly_state_symbol"))
        batch_op.drop_index(batch_op.f("ix_anomaly_state_state_key"))
    op.drop_table("anomaly_state")

