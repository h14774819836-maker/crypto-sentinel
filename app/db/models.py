from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.session import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Ohlcv(Base):
    __tablename__ = "ohlcv"
    __table_args__ = (UniqueConstraint("symbol", "timeframe", "ts", name="uq_ohlcv_symbol_timeframe_ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), index=True, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="binance")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class MarketMetric(Base):
    __tablename__ = "market_metrics"
    __table_args__ = (UniqueConstraint("symbol", "timeframe", "ts", name="uq_market_metrics_symbol_timeframe_ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), index=True, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    ret_1m: Mapped[float | None] = mapped_column(Float)
    ret_3m: Mapped[float | None] = mapped_column(Float)
    ret_5m: Mapped[float | None] = mapped_column(Float)
    ret_10m: Mapped[float | None] = mapped_column(Float)
    rolling_vol_20: Mapped[float | None] = mapped_column(Float)
    atr_14: Mapped[float | None] = mapped_column(Float)
    bb_zscore: Mapped[float | None] = mapped_column(Float)
    bb_bandwidth: Mapped[float | None] = mapped_column(Float)
    rsi_14: Mapped[float | None] = mapped_column(Float)
    macd_hist: Mapped[float | None] = mapped_column(Float)
    volume_zscore: Mapped[float | None] = mapped_column(Float)
    obv: Mapped[float | None] = mapped_column(Float)
    stoch_rsi_k: Mapped[float | None] = mapped_column(Float)
    stoch_rsi_d: Mapped[float | None] = mapped_column(Float)
    ema_ribbon_trend: Mapped[str | None] = mapped_column(String(10))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class AlertEvent(Base):
    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_uid: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False, default="1m")
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    alert_type: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    rule_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v0")
    regime: Mapped[str | None] = mapped_column(String(32))
    metrics_json: Mapped[dict | None] = mapped_column(JSON)
    sent_to_telegram: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class AnomalyState(Base):
    __tablename__ = "anomaly_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    state_key: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False, default="1m")
    event_family: Mapped[str] = mapped_column(String(32), nullable=False, default="momentum")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, default="NONE")
    active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    consecutive_hits: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_score: Mapped[float | None] = mapped_column(Float)
    last_regime: Mapped[str | None] = mapped_column(String(32))
    last_metric_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_alert_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_enter_alert_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_escalate_alert_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_escalate_bucket: Mapped[str | None] = mapped_column(String(32))
    last_alert_kind: Mapped[str | None] = mapped_column(String(16))
    active_cycle_started_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class WorkerStatus(Base):
    __tablename__ = "worker_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class ModelVersion(Base):
    __tablename__ = "model_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False)
    model_name: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class AiSignal(Base):
    __tablename__ = "ai_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False, default="1m")
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    entry_price: Mapped[float | None] = mapped_column(Float)
    take_profit: Mapped[float | None] = mapped_column(Float)
    stop_loss: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False, default="")
    market_regime: Mapped[str | None] = mapped_column(String(32))
    analysis_json: Mapped[dict | None] = mapped_column(JSON)
    manifest_id: Mapped[str | None] = mapped_column(String(128), index=True)
    blob_ref: Mapped[str | None] = mapped_column(Text)
    blob_sha256: Mapped[str | None] = mapped_column(String(64), index=True)
    blob_size_bytes: Mapped[int | None] = mapped_column(Integer)
    model_requested: Mapped[str | None] = mapped_column(String(64))
    model_name: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    sent_to_telegram: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)



class StrategyDecision(Base):
    __tablename__ = "strategy_decisions"
    __table_args__ = (
        UniqueConstraint("analysis_id", name="uq_strategy_decisions_analysis_id"),
        Index("ix_strategy_decisions_symbol_decision_ts", "symbol", "decision_ts"),
        Index("ix_strategy_decisions_manifest_id_decision_ts", "manifest_id", "decision_ts"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="binance")
    market_type: Mapped[str] = mapped_column(String(16), index=True, nullable=False, default="futures")
    base_timeframe: Mapped[str] = mapped_column(String(10), nullable=False, default="1h")
    decision_ts: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    manifest_id: Mapped[str | None] = mapped_column(String(128), index=True)
    analysis_id: Mapped[int | None] = mapped_column(ForeignKey("ai_signals.id"), index=True)
    account_equity: Mapped[float | None] = mapped_column(Float)
    capital_alloc: Mapped[float | None] = mapped_column(Float)
    leverage: Mapped[float | None] = mapped_column(Float)
    margin_mode: Mapped[str | None] = mapped_column(String(16))
    position_side: Mapped[str] = mapped_column(String(10), nullable=False, default="HOLD")
    qty: Mapped[float | None] = mapped_column(Float)
    notional: Mapped[float | None] = mapped_column(Float)
    entry_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="MARKET")
    entry_price: Mapped[float | None] = mapped_column(Float)
    take_profit: Mapped[float | None] = mapped_column(Float)
    stop_loss: Mapped[float | None] = mapped_column(Float)
    expiration_ts: Mapped[int | None] = mapped_column(BigInteger)
    max_hold_bars: Mapped[int | None] = mapped_column(Integer)
    fee_bps_assumption: Mapped[float | None] = mapped_column(Float)
    slippage_bps_assumption: Mapped[float | None] = mapped_column(Float)
    liq_price_est: Mapped[float | None] = mapped_column(Float)
    risk_notes: Mapped[str | None] = mapped_column(Text)
    regime_calc_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="online")
    confidence: Mapped[float | None] = mapped_column(Float)
    reason_brief: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class DecisionExecution(Base):
    __tablename__ = "decision_executions"
    __table_args__ = (UniqueConstraint("decision_id", name="uq_decision_executions_decision_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[int] = mapped_column(ForeignKey("strategy_decisions.id"), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    filled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    filled_ts: Mapped[int | None] = mapped_column(BigInteger)
    filled_price: Mapped[float | None] = mapped_column(Float)
    filled_qty: Mapped[float | None] = mapped_column(Float)
    avg_fill_price: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="SIMULATED")
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class DecisionEval(Base):
    __tablename__ = "decision_evals"
    __table_args__ = (UniqueConstraint("decision_id", name="uq_decision_evals_decision_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[int] = mapped_column(ForeignKey("strategy_decisions.id"), index=True, nullable=False)
    eval_replay_tf: Mapped[str] = mapped_column(String(10), nullable=False, default="1m")
    intrabar_flag: Mapped[str] = mapped_column(String(16), nullable=False, default="NONE")
    tp_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sl_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    first_hit_ts: Mapped[int | None] = mapped_column(BigInteger)
    exit_ts: Mapped[int | None] = mapped_column(BigInteger)
    exit_price: Mapped[float | None] = mapped_column(Float)
    outcome_raw: Mapped[str] = mapped_column(String(20), nullable=False, default="OPEN")
    r_multiple_raw: Mapped[float | None] = mapped_column(Float)
    mfe: Mapped[float | None] = mapped_column(Float)
    mae: Mapped[float | None] = mapped_column(Float)
    bars_to_outcome: Mapped[int | None] = mapped_column(Integer)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class StrategyScore(Base):
    __tablename__ = "strategy_scores"
    __table_args__ = (
        UniqueConstraint(
            "manifest_id",
            "window_start_ts",
            "window_end_ts",
            "split_type",
            "scoring_mode",
            name="uq_strategy_scores_window_mode",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    manifest_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    window_start_ts: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    window_end_ts: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    split_type: Mapped[str] = mapped_column(String(20), nullable=False)
    scoring_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="OK")
    n_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_resolved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_ambiguous: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_timeout: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    win_rate: Mapped[float | None] = mapped_column(Float)
    avg_r: Mapped[float | None] = mapped_column(Float)
    win_rate_ci_low: Mapped[float | None] = mapped_column(Float)
    win_rate_ci_high: Mapped[float | None] = mapped_column(Float)
    avg_r_ci_low: Mapped[float | None] = mapped_column(Float)
    avg_r_ci_high: Mapped[float | None] = mapped_column(Float)
    timeout_rate: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class StrategyFeatureStat(Base):
    __tablename__ = "strategy_feature_stats"
    __table_args__ = (
        UniqueConstraint(
            "manifest_id",
            "window_start_ts",
            "window_end_ts",
            "split_type",
            "regime_id",
            "scoring_mode",
            "feature_key",
            "bucket_key",
            name="uq_strategy_feature_stats_bucket",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    manifest_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    window_start_ts: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    window_end_ts: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    split_type: Mapped[str] = mapped_column(String(20), nullable=False)
    regime_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    scoring_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    feature_key: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    bucket_key: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="OK")
    n: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    win_rate: Mapped[float | None] = mapped_column(Float)
    avg_r: Mapped[float | None] = mapped_column(Float)
    ci_low: Mapped[float | None] = mapped_column(Float)
    ci_high: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class FundingSnapshot(Base):
    __tablename__ = "funding_snapshots"
    __table_args__ = (UniqueConstraint("symbol", "ts", name="uq_funding_snapshots_symbol_ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    mark_price: Mapped[float | None] = mapped_column(Float)
    index_price: Mapped[float | None] = mapped_column(Float)
    last_funding_rate: Mapped[float | None] = mapped_column(Float)
    next_funding_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    interest_rate: Mapped[float | None] = mapped_column(Float)
    open_interest: Mapped[float | None] = mapped_column(Float)
    open_interest_value: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class FuturesAccountSnapshot(Base):
    __tablename__ = "futures_account_snapshots"
    __table_args__ = (UniqueConstraint("ts", name="uq_futures_account_snapshots_ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    account_json: Mapped[dict | None] = mapped_column(JSON)
    balance_json: Mapped[list | None] = mapped_column(JSON)
    positions_json: Mapped[list | None] = mapped_column(JSON)
    total_margin_balance: Mapped[float | None] = mapped_column(Float)
    available_balance: Mapped[float | None] = mapped_column(Float)
    total_maint_margin: Mapped[float | None] = mapped_column(Float)
    btc_position_amt: Mapped[float | None] = mapped_column(Float)
    btc_mark_price: Mapped[float | None] = mapped_column(Float)
    btc_liquidation_price: Mapped[float | None] = mapped_column(Float)
    btc_unrealized_pnl: Mapped[float | None] = mapped_column(Float)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class MarginAccountSnapshot(Base):
    __tablename__ = "margin_account_snapshots"
    __table_args__ = (UniqueConstraint("ts", name="uq_margin_account_snapshots_ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    account_json: Mapped[dict | None] = mapped_column(JSON)
    trade_coeff_json: Mapped[dict | None] = mapped_column(JSON)
    margin_level: Mapped[float | None] = mapped_column(Float)
    total_asset_of_btc: Mapped[float | None] = mapped_column(Float)
    total_liability_of_btc: Mapped[float | None] = mapped_column(Float)
    normal_bar: Mapped[float | None] = mapped_column(Float)
    margin_call_bar: Mapped[float | None] = mapped_column(Float)
    force_liquidation_bar: Mapped[float | None] = mapped_column(Float)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class AccountStatsDaily(Base):
    __tablename__ = "account_stats_daily"
    __table_args__ = (UniqueConstraint("day_utc", name="uq_account_stats_daily_day_utc"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    day_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    equity_open: Mapped[float | None] = mapped_column(Float)
    equity_high: Mapped[float | None] = mapped_column(Float)
    equity_low: Mapped[float | None] = mapped_column(Float)
    equity_close: Mapped[float | None] = mapped_column(Float)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_snapshot_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class AccountSnapshotRaw(Base):
    __tablename__ = "account_snapshot_raw"
    __table_args__ = (
        UniqueConstraint("snapshot_type", "ts", name="uq_account_snapshot_raw_type_ts"),
        Index("ix_account_snapshot_raw_ts_type", "ts", "snapshot_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_type: Mapped[str] = mapped_column(String(16), index=True, nullable=False)  # futures|margin
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    payload_gzip: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


# ======== Intel / News ========


class NewsItem(Base):
    __tablename__ = "news_items"
    __table_args__ = (
        UniqueConstraint("url_hash", name="uq_news_items_url_hash"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    category: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="intel")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    title_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    url_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    raw_text: Mapped[str | None] = mapped_column(Text)
    region: Mapped[str | None] = mapped_column(String(32), index=True)
    topics_json: Mapped[list | None] = mapped_column(JSON)
    alert_keyword: Mapped[str | None] = mapped_column(String(64), index=True)
    severity: Mapped[int] = mapped_column(Integer, index=True, nullable=False, default=0)
    entities_json: Mapped[list | None] = mapped_column(JSON)
    metadata_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class IntelDigestCache(Base):
    __tablename__ = "intel_digest_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False, default="GLOBAL")
    lookback_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=24)
    digest_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True, nullable=False)


# ======== YouTube MVP ========


class YoutubeChannel(Base):
    __tablename__ = "youtube_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    channel_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    channel_title: Mapped[str | None] = mapped_column(String(256))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class YoutubeVideo(Base):
    __tablename__ = "youtube_videos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    video_id: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)
    channel_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    channel_title: Mapped[str | None] = mapped_column(String(256))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    transcript_text: Mapped[str | None] = mapped_column(Text)
    transcript_lang: Mapped[str | None] = mapped_column(String(20))
    needs_asr: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # ASR tracking
    asr_backend: Mapped[str | None] = mapped_column(String(64))
    asr_model: Mapped[str | None] = mapped_column(String(64))
    asr_processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    # AI analysis runtime tracking (cross-process progress visibility)
    analysis_runtime_status: Mapped[str | None] = mapped_column(String(32))
    analysis_stage: Mapped[str | None] = mapped_column(String(32))
    analysis_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    analysis_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    analysis_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    analysis_retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    analysis_next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    analysis_last_error_type: Mapped[str | None] = mapped_column(String(64))
    analysis_last_error_code: Mapped[str | None] = mapped_column(String(64))
    analysis_last_error_message: Mapped[str | None] = mapped_column(Text)
    # Explicit FSM (queryable status)
    status: Mapped[str] = mapped_column(String(32), default="pending_subtitle", nullable=False, index=True)
    status_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    asr_queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class YoutubeInsight(Base):
    __tablename__ = "youtube_insights"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    video_id: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False, default="BTCUSDT")
    analyst_view_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class YoutubeConsensus(Base):
    __tablename__ = "youtube_consensus"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    lookback_hours: Mapped[int] = mapped_column(Integer, nullable=False)
    consensus_json: Mapped[dict | None] = mapped_column(JSON)
    source_video_ids: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


# ======== LLM Debug Tracking ========

class LlmCall(Base):
    """Tracks metadata about LLM API requests for debugging/dashboard."""
    __tablename__ = "llm_calls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task: Mapped[str] = mapped_column(String(32), index=True, nullable=False)  # e.g., "market", "youtube", "selfcheck"
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(64), nullable=False)
    
    # "ok", "error", "timeout", "429"
    status: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    
    error_summary: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True, nullable=False)


class AiAnalysisFailure(Base):
    __tablename__ = "ai_analysis_failures"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False, default="1m")
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    phase: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    model_requested: Mapped[str | None] = mapped_column(String(128))
    model_actual: Mapped[str | None] = mapped_column(String(128))
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_summary: Mapped[str | None] = mapped_column(Text)
    raw_response_excerpt: Mapped[str | None] = mapped_column(Text)
    details_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True, nullable=False)


# ======== Telegram Agent Auditing & Memory ========

class TelegramSession(Base):
    __tablename__ = "telegram_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True, nullable=False)
    preferred_model_override: Mapped[str | None] = mapped_column(String(64))
    preferred_style: Mapped[str] = mapped_column(String(64), nullable=False, default="professional")
    risk_tolerance: Mapped[str] = mapped_column(String(64), nullable=False, default="moderate")
    summary_context: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class TelegramMessageLog(Base):
    __tablename__ = "telegram_message_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)  # 'user', 'assistant', 'system'
    content: Mapped[str] = mapped_column(Text, nullable=False)
    model_used: Mapped[str | None] = mapped_column(String(64))
    tool_calls_json: Mapped[dict | None] = mapped_column(JSON)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True, nullable=False)
