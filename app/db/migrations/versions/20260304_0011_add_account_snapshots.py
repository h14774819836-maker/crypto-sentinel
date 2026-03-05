"""add_account_snapshots

Revision ID: 20260304_0011
Revises: 20260303_0010
Create Date: 2026-03-04 09:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260304_0011"
down_revision = "20260303_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "futures_account_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("account_json", sa.JSON(), nullable=True),
        sa.Column("balance_json", sa.JSON(), nullable=True),
        sa.Column("positions_json", sa.JSON(), nullable=True),
        sa.Column("total_margin_balance", sa.Float(), nullable=True),
        sa.Column("available_balance", sa.Float(), nullable=True),
        sa.Column("total_maint_margin", sa.Float(), nullable=True),
        sa.Column("btc_position_amt", sa.Float(), nullable=True),
        sa.Column("btc_mark_price", sa.Float(), nullable=True),
        sa.Column("btc_liquidation_price", sa.Float(), nullable=True),
        sa.Column("btc_unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts", name="uq_futures_account_snapshots_ts"),
    )
    with op.batch_alter_table("futures_account_snapshots", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_futures_account_snapshots_ts"), ["ts"], unique=False)

    op.create_table(
        "margin_account_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("account_json", sa.JSON(), nullable=True),
        sa.Column("trade_coeff_json", sa.JSON(), nullable=True),
        sa.Column("margin_level", sa.Float(), nullable=True),
        sa.Column("total_asset_of_btc", sa.Float(), nullable=True),
        sa.Column("total_liability_of_btc", sa.Float(), nullable=True),
        sa.Column("normal_bar", sa.Float(), nullable=True),
        sa.Column("margin_call_bar", sa.Float(), nullable=True),
        sa.Column("force_liquidation_bar", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts", name="uq_margin_account_snapshots_ts"),
    )
    with op.batch_alter_table("margin_account_snapshots", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_margin_account_snapshots_ts"), ["ts"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("margin_account_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_margin_account_snapshots_ts"))
    op.drop_table("margin_account_snapshots")

    with op.batch_alter_table("futures_account_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_futures_account_snapshots_ts"))
    op.drop_table("futures_account_snapshots")
