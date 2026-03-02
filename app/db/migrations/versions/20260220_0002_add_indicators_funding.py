"""add_new_indicators_and_funding_table

Revision ID: 20260220_0002
Revises: 20260220_0001
Create Date: 2026-02-20 21:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260220_0002"
down_revision = "20260220_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- MarketMetric: add 4 indicator columns (batch mode for SQLite) ---
    with op.batch_alter_table("market_metrics") as batch_op:
        batch_op.add_column(sa.Column("obv", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("stoch_rsi_k", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("stoch_rsi_d", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("ema_ribbon_trend", sa.String(length=10), nullable=True))

    # --- New table: funding_snapshots ---
    op.create_table(
        "funding_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("mark_price", sa.Float()),
        sa.Column("index_price", sa.Float()),
        sa.Column("last_funding_rate", sa.Float()),
        sa.Column("next_funding_time", sa.DateTime(timezone=True)),
        sa.Column("interest_rate", sa.Float()),
        sa.Column("open_interest", sa.Float()),
        sa.Column("open_interest_value", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("symbol", "ts", name="uq_funding_snapshots_symbol_ts"),
    )
    op.create_index("ix_funding_snapshots_symbol", "funding_snapshots", ["symbol"])
    op.create_index("ix_funding_snapshots_ts", "funding_snapshots", ["ts"])

    # --- AiSignal table (was created by create_all but not in init migration) ---
    # Ensure ai_signals exists (idempotent via IF NOT EXISTS in create_table)
    try:
        op.create_table(
            "ai_signals",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("symbol", sa.String(length=20), nullable=False),
            sa.Column("timeframe", sa.String(length=10), nullable=False),
            sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
            sa.Column("direction", sa.String(length=10), nullable=False),
            sa.Column("entry_price", sa.Float()),
            sa.Column("take_profit", sa.Float()),
            sa.Column("stop_loss", sa.Float()),
            sa.Column("confidence", sa.Integer(), nullable=False),
            sa.Column("reasoning", sa.Text(), nullable=False),
            sa.Column("model_name", sa.String(length=64), nullable=False),
            sa.Column("prompt_tokens", sa.Integer()),
            sa.Column("completion_tokens", sa.Integer()),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_ai_signals_symbol", "ai_signals", ["symbol"])
        op.create_index("ix_ai_signals_ts", "ai_signals", ["ts"])
    except Exception:
        pass  # table already exists from create_all fallback


def downgrade() -> None:
    op.drop_index("ix_funding_snapshots_ts", table_name="funding_snapshots")
    op.drop_index("ix_funding_snapshots_symbol", table_name="funding_snapshots")
    op.drop_table("funding_snapshots")

    with op.batch_alter_table("market_metrics") as batch_op:
        batch_op.drop_column("ema_ribbon_trend")
        batch_op.drop_column("stoch_rsi_d")
        batch_op.drop_column("stoch_rsi_k")
        batch_op.drop_column("obv")
