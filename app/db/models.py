from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
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
    model_requested: Mapped[str | None] = mapped_column(String(64))
    model_name: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    sent_to_telegram: Mapped[bool] = mapped_column(default=False, nullable=False)
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
