"""init

Revision ID: 20260220_0001
Revises:
Create Date: 2026-02-20 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260220_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ohlcv",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("symbol", "timeframe", "ts", name="uq_ohlcv_symbol_timeframe_ts"),
    )
    op.create_index("ix_ohlcv_symbol", "ohlcv", ["symbol"])
    op.create_index("ix_ohlcv_timeframe", "ohlcv", ["timeframe"])
    op.create_index("ix_ohlcv_ts", "ohlcv", ["ts"])

    op.create_table(
        "market_metrics",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("ret_1m", sa.Float()),
        sa.Column("ret_3m", sa.Float()),
        sa.Column("ret_5m", sa.Float()),
        sa.Column("ret_10m", sa.Float()),
        sa.Column("rolling_vol_20", sa.Float()),
        sa.Column("atr_14", sa.Float()),
        sa.Column("bb_zscore", sa.Float()),
        sa.Column("bb_bandwidth", sa.Float()),
        sa.Column("rsi_14", sa.Float()),
        sa.Column("macd_hist", sa.Float()),
        sa.Column("volume_zscore", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("symbol", "timeframe", "ts", name="uq_market_metrics_symbol_timeframe_ts"),
    )
    op.create_index("ix_market_metrics_symbol", "market_metrics", ["symbol"])
    op.create_index("ix_market_metrics_timeframe", "market_metrics", ["timeframe"])
    op.create_index("ix_market_metrics_ts", "market_metrics", ["ts"])

    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_uid", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("alert_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.String(length=32), nullable=False),
        sa.Column("regime", sa.String(length=32)),
        sa.Column("metrics_json", sa.JSON()),
        sa.Column("sent_to_telegram", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("event_uid", name="uq_alert_events_event_uid"),
    )
    op.create_index("ix_alert_events_event_uid", "alert_events", ["event_uid"], unique=True)
    op.create_index("ix_alert_events_symbol", "alert_events", ["symbol"])
    op.create_index("ix_alert_events_alert_type", "alert_events", ["alert_type"])
    op.create_index("ix_alert_events_ts", "alert_events", ["ts"])

    op.create_table(
        "worker_status",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("worker_id", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("worker_id", name="uq_worker_status_worker_id"),
    )
    op.create_index("ix_worker_status_worker_id", "worker_status", ["worker_id"], unique=True)

    op.create_table(
        "model_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("timeframe", sa.String(length=10), nullable=False),
        sa.Column("model_name", sa.String(length=64), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("model_versions")
    op.drop_index("ix_worker_status_worker_id", table_name="worker_status")
    op.drop_table("worker_status")
    op.drop_index("ix_alert_events_ts", table_name="alert_events")
    op.drop_index("ix_alert_events_alert_type", table_name="alert_events")
    op.drop_index("ix_alert_events_symbol", table_name="alert_events")
    op.drop_index("ix_alert_events_event_uid", table_name="alert_events")
    op.drop_table("alert_events")
    op.drop_index("ix_market_metrics_ts", table_name="market_metrics")
    op.drop_index("ix_market_metrics_timeframe", table_name="market_metrics")
    op.drop_index("ix_market_metrics_symbol", table_name="market_metrics")
    op.drop_table("market_metrics")
    op.drop_index("ix_ohlcv_ts", table_name="ohlcv")
    op.drop_index("ix_ohlcv_timeframe", table_name="ohlcv")
    op.drop_index("ix_ohlcv_symbol", table_name="ohlcv")
    op.drop_table("ohlcv")
