from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Select, and_, case, delete, desc, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.db.models import (
    AiSignal, AlertEvent, AnomalyState, FundingSnapshot, MarketMetric, Ohlcv, WorkerStatus,
    YoutubeChannel, YoutubeConsensus, YoutubeInsight, YoutubeVideo, LlmCall,
)

YOUTUBE_ANALYSIS_RUNTIME_STALE_SECONDS = 20 * 60
_UNSET = object()


def _dialect(session: Session) -> str:
    bind = session.get_bind()
    return bind.dialect.name if bind else ""


def _upsert_stmt(
    session: Session,
    model: Any,
    values: dict[str, Any],
    conflict_cols: list[str],
    update_cols: list[str] | None = None,
    do_nothing: bool = False,
):
    dialect = _dialect(session)
    if dialect == "postgresql":
        stmt = pg_insert(model).values(**values)
    elif dialect == "sqlite":
        stmt = sqlite_insert(model).values(**values)
    else:
        raise ValueError(f"Unsupported dialect for upsert: {dialect}")

    if do_nothing:
        return stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    update_cols = update_cols or []
    set_values = {col: values[col] for col in update_cols if col in values}
    return stmt.on_conflict_do_update(index_elements=conflict_cols, set_=set_values)


def upsert_ohlcv(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload, "updated_at": datetime.now(timezone.utc)}
    stmt = _upsert_stmt(
        session,
        Ohlcv,
        values,
        conflict_cols=["symbol", "timeframe", "ts"],
        update_cols=["open", "high", "low", "close", "volume", "source", "updated_at"],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def upsert_market_metric(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload, "updated_at": datetime.now(timezone.utc)}
    stmt = _upsert_stmt(
        session,
        MarketMetric,
        values,
        conflict_cols=["symbol", "timeframe", "ts"],
        update_cols=[
            "close",
            "ret_1m",
            "ret_3m",
            "ret_5m",
            "ret_10m",
            "rolling_vol_20",
            "atr_14",
            "bb_zscore",
            "bb_bandwidth",
            "rsi_14",
            "macd_hist",
            "volume_zscore",
            "obv",
            "stoch_rsi_k",
            "stoch_rsi_d",
            "ema_ribbon_trend",
            "updated_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def upsert_alert_event(session: Session, payload: dict[str, Any], commit: bool = True) -> bool:
    values = {**payload, "updated_at": datetime.now(timezone.utc)}
    stmt = _upsert_stmt(
        session,
        AlertEvent,
        values,
        conflict_cols=["event_uid"],
        do_nothing=True,
    )
    result = session.execute(stmt)
    if commit:
        session.commit()
    return bool(result.rowcount)


def mark_alert_sent(session: Session, event_uid: str) -> None:
    event = session.scalar(select(AlertEvent).where(AlertEvent.event_uid == event_uid))
    if not event:
        return
    event.sent_to_telegram = True
    event.updated_at = datetime.now(timezone.utc)
    session.commit()


def get_anomaly_state(session: Session, state_key: str) -> AnomalyState | None:
    return session.scalar(
        select(AnomalyState).where(AnomalyState.state_key == state_key).limit(1)
    )


def upsert_anomaly_state(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload, "updated_at": datetime.now(timezone.utc)}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        AnomalyState,
        values,
        conflict_cols=["state_key"],
        update_cols=[
            "symbol",
            "timeframe",
            "event_family",
            "direction",
            "active",
            "consecutive_hits",
            "last_score",
            "last_regime",
            "last_metric_ts",
            "last_alert_ts",
            "last_enter_alert_ts",
            "last_escalate_alert_ts",
            "last_escalate_bucket",
            "last_alert_kind",
            "active_cycle_started_ts",
            "updated_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def count_sent_alerts_today(
    session: Session,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
) -> int:
    return int(
        session.scalar(
            select(func.count(AlertEvent.id)).where(
                AlertEvent.symbol == symbol,
                AlertEvent.sent_to_telegram == True,  # noqa: E712
                AlertEvent.created_at >= start_utc,
                AlertEvent.created_at < end_utc,
            )
        )
        or 0
    )


def upsert_worker_status(
    session: Session,
    worker_id: str,
    started_at: datetime,
    last_seen: datetime,
    version: str,
    commit: bool = True,
) -> None:
    values = {
        "worker_id": worker_id,
        "started_at": started_at,
        "last_seen": last_seen,
        "version": version,
        "updated_at": datetime.now(timezone.utc),
    }
    stmt = _upsert_stmt(
        session,
        WorkerStatus,
        values,
        conflict_cols=["worker_id"],
        update_cols=["last_seen", "version", "updated_at"],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def get_worker_last_seen(session: Session, worker_id: str | None = None) -> datetime | None:
    query: Select[Any] = select(WorkerStatus)
    if worker_id:
        query = query.where(WorkerStatus.worker_id == worker_id)
    else:
        query = query.order_by(WorkerStatus.last_seen.desc())
    row = session.scalar(query)
    return row.last_seen if row else None


def get_latest_ohlcv_ts(session: Session, symbol: str, timeframe: str) -> datetime | None:
    return session.scalar(
        select(func.max(Ohlcv.ts)).where(Ohlcv.symbol == symbol, Ohlcv.timeframe == timeframe)
    )


def list_ohlcv_range(
    session: Session,
    symbol: str,
    timeframe: str,
    start_ts: datetime,
    end_ts: datetime,
) -> list[Ohlcv]:
    return list(
        session.scalars(
            select(Ohlcv)
            .where(
                Ohlcv.symbol == symbol,
                Ohlcv.timeframe == timeframe,
                Ohlcv.ts >= start_ts,
                Ohlcv.ts < end_ts,
            )
            .order_by(Ohlcv.ts.asc())
        )
    )


def list_recent_ohlcv(session: Session, symbol: str, timeframe: str, limit: int) -> list[Ohlcv]:
    rows = list(
        session.scalars(
            select(Ohlcv)
            .where(Ohlcv.symbol == symbol, Ohlcv.timeframe == timeframe)
            .order_by(Ohlcv.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


def list_recent_ohlcv_upto_ts(
    session: Session,
    symbol: str,
    timeframe: str,
    upto_ts: datetime,
    limit: int,
) -> list[Ohlcv]:
    rows = list(
        session.scalars(
            select(Ohlcv)
            .where(
                Ohlcv.symbol == symbol,
                Ohlcv.timeframe == timeframe,
                Ohlcv.ts <= upto_ts,
            )
            .order_by(Ohlcv.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


def list_ohlcv_after_ts(
    session: Session,
    symbol: str,
    timeframe: str,
    after_ts: datetime,
    limit: int,
) -> list[Ohlcv]:
    return list(
        session.scalars(
            select(Ohlcv)
            .where(
                Ohlcv.symbol == symbol,
                Ohlcv.timeframe == timeframe,
                Ohlcv.ts > after_ts,
            )
            .order_by(Ohlcv.ts.asc())
            .limit(limit)
        )
    )


def get_recent_vol_values(session: Session, symbol: str, timeframe: str, limit: int) -> list[float]:
    rows = list(
        session.scalars(
            select(MarketMetric.rolling_vol_20)
            .where(
                MarketMetric.symbol == symbol,
                MarketMetric.timeframe == timeframe,
                MarketMetric.rolling_vol_20.is_not(None),
            )
            .order_by(MarketMetric.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return [float(v) for v in rows if v is not None]


def recent_alert_exists(
    session: Session,
    symbol: str,
    alert_type: str,
    cooldown_seconds: int,
) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=cooldown_seconds)
    recent = session.scalar(
        select(AlertEvent.id)
        .where(
            AlertEvent.symbol == symbol,
            AlertEvent.alert_type == alert_type,
            AlertEvent.created_at >= cutoff,
        )
        .order_by(AlertEvent.created_at.desc())
        .limit(1)
    )
    return recent is not None


def recent_ai_signal_exists(
    session: Session,
    symbol: str,
    direction: str,
    entry_bucket: float | None,
    timeframe: str,
    cooldown_seconds: int,
) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=cooldown_seconds)
    stmt = select(AiSignal.id).where(
        AiSignal.symbol == symbol,
        AiSignal.direction == direction,
        AiSignal.timeframe == timeframe,
        AiSignal.sent_to_telegram == True,
        AiSignal.created_at >= cutoff,
    )
    if entry_bucket is not None:
        stmt = stmt.where(AiSignal.entry_price == entry_bucket)
        
    recent = session.scalar(stmt.order_by(AiSignal.created_at.desc()).limit(1))
    return recent is not None


def list_recent_sent_ai_signals(
    session: Session,
    *,
    symbol: str,
    timeframe: str,
    since_ts: datetime,
    limit: int = 200,
) -> list[AiSignal]:
    query = (
        select(AiSignal)
        .where(
            AiSignal.symbol == symbol,
            AiSignal.timeframe == timeframe,
            AiSignal.sent_to_telegram == True,  # noqa: E712
            AiSignal.created_at >= since_ts,
        )
        .order_by(AiSignal.created_at.desc())
        .limit(limit)
    )
    return list(session.scalars(query))


def list_alerts(session: Session, limit: int = 100, symbol: str | None = None, alert_type: str | None = None) -> list[AlertEvent]:
    query = select(AlertEvent)
    if symbol:
        query = query.where(AlertEvent.symbol == symbol.upper())
    if alert_type:
        query = query.where(AlertEvent.alert_type == alert_type)
    return list(session.scalars(query.order_by(AlertEvent.created_at.desc()).limit(limit)))


def get_latest_market_metrics(session: Session, symbols: list[str], timeframe: str = "1m") -> list[MarketMetric]:
    results: list[MarketMetric] = []
    for symbol in symbols:
        row = session.scalar(
            select(MarketMetric)
            .where(MarketMetric.symbol == symbol, MarketMetric.timeframe == timeframe)
            .order_by(MarketMetric.ts.desc())
            .limit(1)
        )
        if row:
            results.append(row)
    return results


def get_latest_market_metric(session: Session, symbol: str, timeframe: str = "1m") -> MarketMetric | None:
    return session.scalar(
        select(MarketMetric)
        .where(MarketMetric.symbol == symbol, MarketMetric.timeframe == timeframe)
        .order_by(MarketMetric.ts.desc())
        .limit(1)
    )


def get_latest_market_metric_ts(session: Session, symbol: str, timeframe: str = "1m") -> datetime | None:
    return session.scalar(
        select(MarketMetric.ts)
        .where(MarketMetric.symbol == symbol, MarketMetric.timeframe == timeframe)
        .order_by(MarketMetric.ts.desc())
        .limit(1)
    )


def get_latest_ohlcv(session: Session, symbol: str, timeframe: str = "1m") -> Ohlcv | None:
    return session.scalar(
        select(Ohlcv)
        .where(Ohlcv.symbol == symbol, Ohlcv.timeframe == timeframe)
        .order_by(Ohlcv.ts.desc())
        .limit(1)
    )


def insert_ai_signal(session: Session, payload: dict[str, Any], commit: bool = True) -> AiSignal:
    # Ignore extra keys so callers can evolve payloads without breaking persistence.
    allowed_keys = set(AiSignal.__table__.columns.keys())
    filtered_payload = {k: v for k, v in payload.items() if k in allowed_keys}
    if filtered_payload.get("ts") is None:
        filtered_payload["ts"] = datetime.now(timezone.utc)
    filtered_payload.setdefault("timeframe", "1m")
    signal = AiSignal(**filtered_payload)
    session.add(signal)
    if commit:
        session.commit()
    return signal


def list_ai_signals(
    session: Session, limit: int = 50, symbol: str | None = None
) -> list[AiSignal]:
    query = select(AiSignal)
    if symbol:
        query = query.where(AiSignal.symbol == symbol.upper())
    return list(session.scalars(query.order_by(AiSignal.created_at.desc()).limit(limit)))


def get_latest_ai_signals(session: Session, symbols: list[str]) -> list[AiSignal]:
    results: list[AiSignal] = []
    for symbol in symbols:
        row = session.scalar(
            select(AiSignal)
            .where(AiSignal.symbol == symbol)
            .order_by(AiSignal.created_at.desc())
            .limit(1)
        )
        if row:
            results.append(row)
    return results


# --------------- Funding Snapshots ---------------

def upsert_funding_snapshot(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    stmt = _upsert_stmt(
        session,
        FundingSnapshot,
        values,
        conflict_cols=["symbol", "ts"],
        update_cols=[
            "mark_price", "index_price", "last_funding_rate",
            "next_funding_time", "interest_rate",
            "open_interest", "open_interest_value",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def get_latest_funding_snapshots(session: Session, symbols: list[str]) -> list[FundingSnapshot]:
    results: list[FundingSnapshot] = []
    for symbol in symbols:
        row = session.scalar(
            select(FundingSnapshot)
            .where(FundingSnapshot.symbol == symbol)
            .order_by(FundingSnapshot.ts.desc())
            .limit(1)
        )
        if row:
            results.append(row)
    return results


def get_recent_funding_snapshots_for_symbol(
    session: Session,
    symbol: str,
    limit: int = 48,
) -> list[FundingSnapshot]:
    rows = list(
        session.scalars(
            select(FundingSnapshot)
            .where(FundingSnapshot.symbol == symbol)
            .order_by(FundingSnapshot.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


# --------------- Multi-TF helpers ---------------

def get_recent_market_metrics(
    session: Session, symbol: str, timeframe: str, limit: int = 50
) -> list[MarketMetric]:
    """Return the most recent N metric rows for AI historical context."""
    rows = list(
        session.scalars(
            select(MarketMetric)
            .where(MarketMetric.symbol == symbol, MarketMetric.timeframe == timeframe)
            .order_by(MarketMetric.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


# --------------- YouTube MVP ---------------


def add_youtube_channel(session: Session, channel_id: str, channel_url: str = "", channel_title: str | None = None, commit: bool = True) -> bool:
    """Add a channel. Returns True if inserted, False if already exists."""
    values = {
        "channel_id": channel_id,
        "channel_url": channel_url,
        "channel_title": channel_title,
        "enabled": True,
        "created_at": datetime.now(timezone.utc),
    }
    stmt = _upsert_stmt(session, YoutubeChannel, values, conflict_cols=["channel_id"], do_nothing=True)
    result = session.execute(stmt)
    if commit:
        session.commit()
    return bool(result.rowcount)


def remove_youtube_channel(session: Session, channel_id: str, commit: bool = True) -> bool:
    row = session.scalar(select(YoutubeChannel).where(YoutubeChannel.channel_id == channel_id))
    if not row:
        return False
    session.delete(row)
    if commit:
        session.commit()
    return True


def list_youtube_channels(session: Session, enabled_only: bool = True) -> list[YoutubeChannel]:
    query = select(YoutubeChannel)
    if enabled_only:
        query = query.where(YoutubeChannel.enabled == True)  # noqa: E712
    return list(session.scalars(query.order_by(YoutubeChannel.created_at.asc())))


def upsert_youtube_video(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        YoutubeVideo,
        values,
        conflict_cols=["video_id"],
        update_cols=["channel_title", "title"],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def _merge_youtube_queue_with_rescue(
    session: Session,
    base_query,
    *,
    limit: int,
    rescue_oldest: int = 1,
) -> list[YoutubeVideo]:
    limit = max(1, int(limit))
    rescue_oldest = max(0, int(rescue_oldest))
    if limit < 3 or rescue_oldest <= 0:
        return list(session.scalars(base_query.order_by(YoutubeVideo.published_at.desc()).limit(limit)))

    newest_count = max(1, limit - rescue_oldest)
    newest = list(session.scalars(base_query.order_by(YoutubeVideo.published_at.desc()).limit(newest_count)))
    oldest = list(session.scalars(base_query.order_by(YoutubeVideo.published_at.asc()).limit(rescue_oldest)))

    seen: set[str] = set()
    merged: list[YoutubeVideo] = []
    for row in newest + oldest:
        if row.video_id in seen:
            continue
        seen.add(row.video_id)
        merged.append(row)
        if len(merged) >= limit:
            break
    return merged


def list_unprocessed_youtube_videos(
    session: Session,
    limit: int = 10,
    rescue_oldest: int = 1,
) -> list[YoutubeVideo]:
    base_query = select(YoutubeVideo).where(YoutubeVideo.processed_at.is_(None))
    return _merge_youtube_queue_with_rescue(session, base_query, limit=limit, rescue_oldest=rescue_oldest)


def list_videos_needing_analysis(
    session: Session,
    limit: int = 10,
    rescue_oldest: int = 1,
) -> list[YoutubeVideo]:
    """Videos with transcript but no insight yet."""
    subq = select(YoutubeInsight.video_id).where(YoutubeInsight.video_id == YoutubeVideo.video_id).exists()
    now_utc = datetime.now(timezone.utc)
    stale_cutoff = datetime.now(timezone.utc) - timedelta(seconds=YOUTUBE_ANALYSIS_RUNTIME_STALE_SECONDS)
    retry_due = and_(
        YoutubeVideo.analysis_stage == "retry_wait",
        YoutubeVideo.analysis_next_retry_at.is_not(None),
        YoutubeVideo.analysis_next_retry_at <= now_utc,
    )
    candidate_condition = or_(
        ~subq,
        and_(subq, retry_due),
    )
    runtime_available_for_requeue = or_(
        YoutubeVideo.analysis_runtime_status.is_(None),
        YoutubeVideo.analysis_runtime_status.notin_(("queued", "running")),
        and_(
            YoutubeVideo.analysis_runtime_status.in_(("queued", "running")),
            YoutubeVideo.analysis_updated_at.is_not(None),
            YoutubeVideo.analysis_updated_at < stale_cutoff,
        ),
        retry_due,
    )
    base_query = (
        select(YoutubeVideo)
        .where(
            YoutubeVideo.transcript_text.is_not(None),
            candidate_condition,
            runtime_available_for_requeue,
        )
    )
    return _merge_youtube_queue_with_rescue(session, base_query, limit=limit, rescue_oldest=rescue_oldest)


def list_videos_needing_asr(
    session: Session,
    limit: int = 3,
    include_failed: bool = False,
    rescue_oldest: int = 1,
) -> list[YoutubeVideo]:
    """Videos marked needs_asr=true that still have no transcript."""
    base_query = select(YoutubeVideo).where(
        YoutubeVideo.needs_asr == True,  # noqa: E712
        YoutubeVideo.transcript_text.is_(None),
    )
    if not include_failed:
        base_query = base_query.where(
            ~(
                YoutubeVideo.asr_processed_at.is_not(None)
                & YoutubeVideo.last_error.is_not(None)
            )
        )
    return _merge_youtube_queue_with_rescue(session, base_query, limit=limit, rescue_oldest=rescue_oldest)


def update_youtube_video_transcript(
    session: Session,
    video_id: str,
    transcript_text: str | None,
    transcript_lang: str | None,
    needs_asr: bool,
    processed_at: datetime | None = None,
    commit: bool = True,
) -> None:
    row = session.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        return
    row.transcript_text = transcript_text
    row.transcript_lang = transcript_lang
    row.needs_asr = needs_asr
    row.processed_at = processed_at or datetime.now(timezone.utc)
    if commit:
        session.commit()


def update_youtube_video_asr_result(
    session: Session,
    video_id: str,
    transcript_text: str | None,
    transcript_lang: str | None,
    asr_backend: str,
    asr_model: str,
    last_error: str | None = None,
    commit: bool = True,
) -> None:
    row = session.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        return
    row.asr_backend = asr_backend
    row.asr_model = asr_model
    row.asr_processed_at = datetime.now(timezone.utc)
    row.last_error = last_error
    if transcript_text:
        row.transcript_text = transcript_text
        row.transcript_lang = transcript_lang
        row.needs_asr = False
    if commit:
        session.commit()


def update_youtube_video_analysis_runtime(
    session: Session,
    video_id: str,
    *,
    status: str | None | object = _UNSET,
    stage: str | None | object = _UNSET,
    started_at: datetime | None | object = _UNSET,
    updated_at: datetime | None | object = _UNSET,
    finished_at: datetime | None | object = _UNSET,
    retry_count: int | object = _UNSET,
    next_retry_at: datetime | None | object = _UNSET,
    last_error_type: str | None | object = _UNSET,
    last_error_code: str | None | object = _UNSET,
    last_error_message: str | None | object = _UNSET,
    reset: bool = False,
    commit: bool = True,
) -> None:
    row = session.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        return

    if reset:
        row.analysis_runtime_status = None
        row.analysis_stage = None
        row.analysis_started_at = None
        row.analysis_updated_at = None
        row.analysis_finished_at = None
        row.analysis_retry_count = 0
        row.analysis_next_retry_at = None
        row.analysis_last_error_type = None
        row.analysis_last_error_code = None
        row.analysis_last_error_message = None

    now = datetime.now(timezone.utc)
    if status is not _UNSET:
        row.analysis_runtime_status = status  # type: ignore[assignment]
    if stage is not _UNSET:
        row.analysis_stage = stage  # type: ignore[assignment]
    if started_at is not _UNSET:
        row.analysis_started_at = started_at  # type: ignore[assignment]
    if updated_at is _UNSET:
        row.analysis_updated_at = now
    else:
        row.analysis_updated_at = updated_at  # type: ignore[assignment]
    if finished_at is not _UNSET:
        row.analysis_finished_at = finished_at  # type: ignore[assignment]
    if retry_count is not _UNSET:
        row.analysis_retry_count = max(0, int(retry_count))  # type: ignore[assignment]
    if next_retry_at is not _UNSET:
        row.analysis_next_retry_at = next_retry_at  # type: ignore[assignment]
    if last_error_type is not _UNSET:
        row.analysis_last_error_type = last_error_type  # type: ignore[assignment]
    if last_error_code is not _UNSET:
        row.analysis_last_error_code = last_error_code  # type: ignore[assignment]
    if last_error_message is not _UNSET:
        row.analysis_last_error_message = last_error_message  # type: ignore[assignment]

    if commit:
        session.commit()


def update_youtube_video_analysis_retry_state(
    session: Session,
    video_id: str,
    *,
    retry_count: int | object = _UNSET,
    next_retry_at: datetime | None | object = _UNSET,
    last_error_type: str | None | object = _UNSET,
    last_error_code: str | None | object = _UNSET,
    last_error_message: str | None | object = _UNSET,
    commit: bool = True,
) -> None:
    update_youtube_video_analysis_runtime(
        session,
        video_id,
        retry_count=retry_count,
        next_retry_at=next_retry_at,
        last_error_type=last_error_type,
        last_error_code=last_error_code,
        last_error_message=last_error_message,
        commit=commit,
    )


def bulk_mark_youtube_videos_analysis_queued(
    session: Session,
    video_ids: list[str],
    now: datetime | None = None,
    reset_retry: bool = False,
    commit: bool = True,
) -> None:
    ids = [vid for vid in video_ids if vid]
    if not ids:
        return
    ts = now or datetime.now(timezone.utc)
    values: dict[str, Any] = {
        "analysis_runtime_status": "queued",
        "analysis_stage": "queued",
        "analysis_started_at": ts,
        "analysis_updated_at": ts,
        "analysis_finished_at": None,
    }
    if reset_retry:
        values.update({
            "analysis_retry_count": 0,
            "analysis_next_retry_at": None,
            "analysis_last_error_type": None,
            "analysis_last_error_code": None,
            "analysis_last_error_message": None,
        })
    session.execute(
        update(YoutubeVideo)
        .where(YoutubeVideo.video_id.in_(ids))
        .values(**values)
    )
    if commit:
        session.commit()


def save_youtube_insight(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        YoutubeInsight,
        values,
        conflict_cols=["video_id"],
        update_cols=["analyst_view_json"],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def delete_youtube_insight_by_video_id(session: Session, video_id: str, commit: bool = True) -> bool:
    row = session.scalar(select(YoutubeInsight).where(YoutubeInsight.video_id == video_id))
    if not row:
        return False
    session.delete(row)
    if commit:
        session.commit()
    return True


def get_recent_youtube_insights(
    session: Session, lookback_hours: int = 48, symbol: str = "BTCUSDT"
) -> list[YoutubeInsight]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    return list(
        session.scalars(
            select(YoutubeInsight)
            .where(YoutubeInsight.symbol == symbol, YoutubeInsight.created_at >= cutoff)
            .order_by(YoutubeInsight.created_at.desc())
        )
    )


def save_youtube_consensus(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    consensus = YoutubeConsensus(**values)
    session.add(consensus)
    if commit:
        session.commit()


def get_latest_youtube_consensus(session: Session, symbol: str = "BTCUSDT") -> YoutubeConsensus | None:
    return session.scalars(
        select(YoutubeConsensus)
        .where(YoutubeConsensus.symbol == symbol)
        .order_by(YoutubeConsensus.created_at.desc())
        .limit(1)
    ).first()


# ======== LLM Debug Tracking ========

def insert_llm_call(db: Session, data: dict, commit: bool = True) -> int:
    from app.db.models import LlmCall
    allowed_keys = set(LlmCall.__table__.columns.keys())
    filtered = {k: v for k, v in (data or {}).items() if k in allowed_keys}
    row = LlmCall(**filtered)
    db.add(row)
    if commit:
        db.commit()
        db.refresh(row)
    return row.id

def get_llm_calls(db: Session, limit: int = 50, task: str | None = None) -> list[Any]:
    from app.db.models import LlmCall
    stmt = select(LlmCall).order_by(LlmCall.created_at.desc()).limit(limit)
    if task:
        stmt = stmt.where(LlmCall.task == task)
    return list(db.scalars(stmt).all())


def insert_ai_analysis_failure(db: Session, payload: dict[str, Any], commit: bool = True) -> int:
    from app.db.models import AiAnalysisFailure

    allowed_keys = set(AiAnalysisFailure.__table__.columns.keys())
    filtered_payload = {k: v for k, v in (payload or {}).items() if k in allowed_keys}
    if filtered_payload.get("ts") is None:
        filtered_payload["ts"] = datetime.now(timezone.utc)
    filtered_payload.setdefault("timeframe", "1m")
    row = AiAnalysisFailure(**filtered_payload)
    db.add(row)
    if commit:
        db.commit()
        db.refresh(row)
    return row.id


def list_ai_analysis_failures(
    db: Session,
    *,
    limit: int = 50,
    task: str | None = "market",
    symbol: str | None = None,
) -> list[Any]:
    from app.db.models import AiAnalysisFailure

    stmt = select(AiAnalysisFailure).order_by(AiAnalysisFailure.created_at.desc()).limit(limit)
    if task:
        stmt = stmt.where(AiAnalysisFailure.task == task)
    if symbol:
        stmt = stmt.where(AiAnalysisFailure.symbol == symbol.upper())
    return list(db.scalars(stmt).all())

def get_llm_stats_1h(db: Session) -> dict:
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import func
    from app.db.models import LlmCall
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    
    # Simple aggregation per task
    results = db.execute(
        select(
            LlmCall.task,
            func.count(LlmCall.id).label("total"),
            func.sum(case((LlmCall.status == "ok", 1), else_=0)).label("ok"),
            func.sum(case((LlmCall.status == "429", 1), else_=0)).label("rate_limited"),
            func.sum(case((LlmCall.status == "error", 1), else_=0)).label("error"),
            func.avg(LlmCall.duration_ms).label("avg_duration")
        )
        .where(LlmCall.created_at >= cutoff)
        .group_by(LlmCall.task)
    ).all()
    
    stats = {}
    for r in results:
        stats[r.task] = {
            "total": r.total,
            "ok": r.ok or 0,
            "rate_limited": r.rate_limited or 0,
            "error": r.error or 0,
            "avg_duration": int(r.avg_duration) if r.avg_duration else 0
        }
    return stats


def list_youtube_videos(session: Session, limit: int = 20) -> list[YoutubeVideo]:
    return list(
        session.scalars(
            select(YoutubeVideo)
            .order_by(YoutubeVideo.published_at.desc())
            .limit(limit)
        )
    )

# ======== Telegram Memory ========

def get_or_create_telegram_session(session: Session, chat_id: int) -> tuple[Any, bool]:
    from app.db.models import TelegramSession
    row = session.scalar(select(TelegramSession).where(TelegramSession.chat_id == chat_id))
    if row:
        return row, False
    row = TelegramSession(chat_id=chat_id)
    session.add(row)
    session.commit()
    session.refresh(row)
    return row, True

def update_telegram_session(session: Session, chat_id: int, updates: dict[str, Any]) -> None:
    from app.db.models import TelegramSession
    row = session.scalar(select(TelegramSession).where(TelegramSession.chat_id == chat_id))
    if not row:
        return
    for k, v in updates.items():
        setattr(row, k, v)
    session.commit()

def insert_telegram_message_log(session: Session, payload: dict[str, Any]) -> None:
    from app.db.models import TelegramMessageLog
    row = TelegramMessageLog(**payload)
    session.add(row)
    session.commit()

def get_recent_telegram_messages(session: Session, chat_id: int, limit: int = 10) -> list[Any]:
    from app.db.models import TelegramMessageLog
    rows = list(
        session.scalars(
            select(TelegramMessageLog)
            .where(TelegramMessageLog.chat_id == chat_id)
            .order_by(TelegramMessageLog.created_at.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows
