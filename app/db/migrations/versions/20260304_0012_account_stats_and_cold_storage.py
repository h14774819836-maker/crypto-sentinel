"""add_account_stats_and_cold_storage

Revision ID: 20260304_0012
Revises: 20260304_0011
Create Date: 2026-03-04 15:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260304_0012"
down_revision = "20260304_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("futures_account_snapshots", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(batch_op.f("ix_futures_account_snapshots_last_seen_at"), ["last_seen_at"], unique=False)

    with op.batch_alter_table("margin_account_snapshots", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(batch_op.f("ix_margin_account_snapshots_last_seen_at"), ["last_seen_at"], unique=False)

    op.create_table(
        "account_stats_daily",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("day_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity_open", sa.Float(), nullable=True),
        sa.Column("equity_high", sa.Float(), nullable=True),
        sa.Column("equity_low", sa.Float(), nullable=True),
        sa.Column("equity_close", sa.Float(), nullable=True),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_snapshot_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("day_utc", name="uq_account_stats_daily_day_utc"),
    )
    with op.batch_alter_table("account_stats_daily", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_account_stats_daily_day_utc"), ["day_utc"], unique=False)

    op.create_table(
        "account_snapshot_raw",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("snapshot_type", sa.String(length=16), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_gzip", sa.LargeBinary(), nullable=False),
        sa.Column("payload_size", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("snapshot_type", "ts", name="uq_account_snapshot_raw_type_ts"),
    )
    with op.batch_alter_table("account_snapshot_raw", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_account_snapshot_raw_snapshot_type"), ["snapshot_type"], unique=False)
        batch_op.create_index(batch_op.f("ix_account_snapshot_raw_ts"), ["ts"], unique=False)
        batch_op.create_index("ix_account_snapshot_raw_ts_type", ["ts", "snapshot_type"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("account_snapshot_raw", schema=None) as batch_op:
        batch_op.drop_index("ix_account_snapshot_raw_ts_type")
        batch_op.drop_index(batch_op.f("ix_account_snapshot_raw_ts"))
        batch_op.drop_index(batch_op.f("ix_account_snapshot_raw_snapshot_type"))
    op.drop_table("account_snapshot_raw")

    with op.batch_alter_table("account_stats_daily", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_account_stats_daily_day_utc"))
    op.drop_table("account_stats_daily")

    with op.batch_alter_table("margin_account_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_margin_account_snapshots_last_seen_at"))
        batch_op.drop_column("last_seen_at")

    with op.batch_alter_table("futures_account_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_futures_account_snapshots_last_seen_at"))
        batch_op.drop_column("last_seen_at")
