from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Select, and_, case, delete, desc, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.youtube.status import (
    ASR_FAILED,
    ANALYZING,
    COMPLETED,
    FAILED,
    PENDING_ANALYSIS,
    PENDING_SUBTITLE,
    QUEUED_ASR,
)
from app.db.models import (
    AccountSnapshotRaw,
    AccountStatsDaily,
    AiSignal,
    AlertEvent,
    AnomalyState,
    DecisionEval,
    DecisionExecution,
    FuturesAccountSnapshot,
    FundingSnapshot,
    IntelDigestCache,
    LlmCall,
    MarketMetric,
    NewsItem,
    Ohlcv,
    MarginAccountSnapshot,
    StrategyDecision,
    StrategyFeatureStat,
    StrategyScore,
    WorkerStatus,
    YoutubeChannel,
    YoutubeConsensus,
    YoutubeInsight,
    YoutubeVideo,
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


def get_alert_event_by_uid(session: Session, event_uid: str) -> AlertEvent | None:
    return session.scalar(select(AlertEvent).where(AlertEvent.event_uid == event_uid).limit(1))


def update_alert_event_delivery(
    session: Session,
    *,
    event_uid: str,
    updates: dict[str, Any],
    commit: bool = True,
) -> bool:
    event = get_alert_event_by_uid(session, event_uid)
    if event is None:
        return False
    metrics = dict(event.metrics_json or {})
    delivery = dict(metrics.get("delivery") or {})
    delivery.update({k: v for k, v in (updates or {}).items() if v is not None})
    metrics["delivery"] = delivery
    event.metrics_json = metrics
    event.updated_at = datetime.now(timezone.utc)
    if commit:
        session.commit()
    return True


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
    return _to_utc_or_none(row.last_seen) if row else None


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
    session.flush()
    write_decision_from_ai_signal(session, signal, commit=False)
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


# --------------- Strategy Decisions ---------------

def _to_epoch_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _to_float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_trade_plan(analysis_json: Any) -> dict[str, Any]:
    if not isinstance(analysis_json, dict):
        return {}
    trade_plan = analysis_json.get("trade_plan")
    if isinstance(trade_plan, dict):
        return trade_plan
    signal = analysis_json.get("signal")
    if isinstance(signal, dict):
        return {
            "entry_mode": "market",
            "entry_price": signal.get("entry_price"),
            "take_profit": signal.get("take_profit"),
            "stop_loss": signal.get("stop_loss"),
            "capital_alloc_usdt": None,
            "leverage": None,
            "margin_mode": None,
            "expiration_ts_utc": None,
            "max_hold_bars": None,
            "liq_price_est": None,
            "fees_bps_assumption": None,
            "slippage_bps_assumption": None,
        }
    return {}


def write_decision_from_ai_signal(session: Session, signal: AiSignal, commit: bool = True) -> StrategyDecision | None:
    if signal is None or signal.id is None:
        return None

    analysis_json = getattr(signal, "analysis_json", None)
    if not isinstance(analysis_json, dict):
        analysis_json = {}
    trade_plan = _extract_trade_plan(analysis_json)
    side = str(getattr(signal, "direction", "HOLD") or "HOLD").upper()
    if side not in {"LONG", "SHORT", "HOLD"}:
        side = "HOLD"

    decision_ts = _to_epoch_seconds(getattr(signal, "ts", None) or getattr(signal, "created_at", None))
    if decision_ts is None:
        decision_ts = int(datetime.now(timezone.utc).timestamp())

    expiration_ts = _to_int_or_none(trade_plan.get("expiration_ts_utc"))
    max_hold_bars = _to_int_or_none(trade_plan.get("max_hold_bars"))
    if expiration_ts is None and (max_hold_bars is None or max_hold_bars <= 0):
        # Make timeout semantics explicit even for legacy outputs.
        max_hold_bars = 60

    entry_mode_raw = str(trade_plan.get("entry_mode") or "market").upper()
    entry_mode = entry_mode_raw if entry_mode_raw in {"MARKET", "LIMIT"} else "MARKET"
    margin_mode_raw = str(trade_plan.get("margin_mode") or "").upper()
    margin_mode = margin_mode_raw if margin_mode_raw in {"ISOLATED", "CROSS"} else None

    account_snapshot = trade_plan.get("account_snapshot")
    if not isinstance(account_snapshot, dict):
        account_snapshot = {}

    values = {
        "symbol": str(getattr(signal, "symbol", "") or "").upper(),
        "exchange": "binance",
        "market_type": str(trade_plan.get("market_type") or "futures").lower(),
        "base_timeframe": str(getattr(signal, "timeframe", "1m") or "1m"),
        "decision_ts": decision_ts,
        "manifest_id": getattr(signal, "manifest_id", None) or "legacy_unknown",
        "analysis_id": signal.id,
        "account_equity": _to_float_or_none(account_snapshot.get("equity")),
        "capital_alloc": _to_float_or_none(trade_plan.get("capital_alloc_usdt")),
        "leverage": _to_float_or_none(trade_plan.get("leverage")),
        "margin_mode": margin_mode,
        "position_side": side,
        "qty": _to_float_or_none(trade_plan.get("qty")),
        "notional": _to_float_or_none(trade_plan.get("notional")),
        "entry_mode": entry_mode,
        "entry_price": _to_float_or_none(trade_plan.get("entry_price")) if trade_plan else _to_float_or_none(getattr(signal, "entry_price", None)),
        "take_profit": _to_float_or_none(trade_plan.get("take_profit")) if trade_plan else _to_float_or_none(getattr(signal, "take_profit", None)),
        "stop_loss": _to_float_or_none(trade_plan.get("stop_loss")) if trade_plan else _to_float_or_none(getattr(signal, "stop_loss", None)),
        "expiration_ts": expiration_ts,
        "max_hold_bars": max_hold_bars,
        "fee_bps_assumption": _to_float_or_none(trade_plan.get("fees_bps_assumption")),
        "slippage_bps_assumption": _to_float_or_none(trade_plan.get("slippage_bps_assumption")),
        "liq_price_est": _to_float_or_none(trade_plan.get("liq_price_est")),
        "risk_notes": str(trade_plan.get("risk_notes") or "")[:500] or None,
        "regime_calc_mode": str(((analysis_json.get("meta") or {}).get("regime_calc_mode")) or "online").lower(),
        "confidence": (
            max(0.0, min(1.0, float(getattr(signal, "confidence", 0) or 0) / 100.0))
            if getattr(signal, "confidence", None) is not None
            else None
        ),
        "reason_brief": (str(getattr(signal, "reasoning", "") or "").strip()[:280] or None),
        "updated_at": datetime.now(timezone.utc),
    }
    if values["market_type"] != "futures":
        values["market_type"] = "futures"
    if values["regime_calc_mode"] not in {"online", "offline"}:
        values["regime_calc_mode"] = "online"

    existing = session.scalar(select(StrategyDecision).where(StrategyDecision.analysis_id == signal.id).limit(1))
    if existing:
        for k, v in values.items():
            setattr(existing, k, v)
        row = existing
    else:
        values["created_at"] = datetime.now(timezone.utc)
        row = StrategyDecision(**values)
        session.add(row)
    if commit:
        session.commit()
    return row


def _serialize_decision_row(decision: StrategyDecision, execution: DecisionExecution | None, eval_row: DecisionEval | None) -> dict[str, Any]:
    return {
        "id": decision.id,
        "symbol": decision.symbol,
        "exchange": decision.exchange,
        "market_type": decision.market_type,
        "base_timeframe": decision.base_timeframe,
        "decision_ts": decision.decision_ts,
        "manifest_id": decision.manifest_id,
        "analysis_id": decision.analysis_id,
        "position_side": decision.position_side,
        "entry_mode": decision.entry_mode,
        "entry_price": decision.entry_price,
        "take_profit": decision.take_profit,
        "stop_loss": decision.stop_loss,
        "expiration_ts": decision.expiration_ts,
        "max_hold_bars": decision.max_hold_bars,
        "leverage": decision.leverage,
        "margin_mode": decision.margin_mode,
        "capital_alloc": decision.capital_alloc,
        "fee_bps_assumption": decision.fee_bps_assumption,
        "slippage_bps_assumption": decision.slippage_bps_assumption,
        "liq_price_est": decision.liq_price_est,
        "regime_calc_mode": decision.regime_calc_mode,
        "confidence": decision.confidence,
        "reason_brief": decision.reason_brief,
        "execution": {
            "status": execution.status,
            "filled": execution.filled,
            "filled_ts": execution.filled_ts,
            "filled_price": execution.filled_price,
            "filled_qty": execution.filled_qty,
            "avg_fill_price": execution.avg_fill_price,
            "source": execution.source,
            "notes": execution.notes,
        }
        if execution
        else None,
        "eval": {
            "eval_replay_tf": eval_row.eval_replay_tf,
            "intrabar_flag": eval_row.intrabar_flag,
            "tp_hit": eval_row.tp_hit,
            "sl_hit": eval_row.sl_hit,
            "first_hit_ts": eval_row.first_hit_ts,
            "exit_ts": eval_row.exit_ts,
            "exit_price": eval_row.exit_price,
            "outcome_raw": eval_row.outcome_raw,
            "r_multiple_raw": eval_row.r_multiple_raw,
            "mfe": eval_row.mfe,
            "mae": eval_row.mae,
            "bars_to_outcome": eval_row.bars_to_outcome,
        }
        if eval_row
        else None,
    }


def list_strategy_decisions_raw(
    session: Session,
    *,
    symbol: str,
    from_ts: int,
    to_ts: int,
    manifest_id: str | None = None,
    side: str | None = None,
    outcome: str | None = None,
    regime: str | None = None,
    cursor: int | None = None,
    limit: int = 200,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    query = (
        select(StrategyDecision, DecisionExecution, DecisionEval)
        .outerjoin(DecisionExecution, DecisionExecution.decision_id == StrategyDecision.id)
        .outerjoin(DecisionEval, DecisionEval.decision_id == StrategyDecision.id)
        .where(
            StrategyDecision.symbol == symbol.upper(),
            StrategyDecision.decision_ts >= int(from_ts),
            StrategyDecision.decision_ts <= int(to_ts),
        )
    )
    if manifest_id:
        query = query.where(StrategyDecision.manifest_id == manifest_id)
    if side:
        query = query.where(StrategyDecision.position_side == side.upper())
    if regime:
        query = query.outerjoin(AiSignal, AiSignal.id == StrategyDecision.analysis_id).where(
            func.upper(func.coalesce(AiSignal.market_regime, "")) == regime.upper()
        )
    if outcome:
        outcome_u = outcome.upper()
        if outcome_u == "NO_FILL":
            query = query.where(
                or_(
                    DecisionExecution.status == "NO_FILL",
                    DecisionEval.outcome_raw == "NO_FILL",
                )
            )
        else:
            query = query.where(DecisionEval.outcome_raw == outcome_u)
    if cursor:
        query = query.where(StrategyDecision.id < int(cursor))
    query = query.order_by(StrategyDecision.decision_ts.desc(), StrategyDecision.id.desc()).limit(max(1, int(limit)) + 1)

    rows = list(session.execute(query).all())
    has_more = len(rows) > limit
    rows = rows[: max(1, int(limit))]
    next_cursor = rows[-1][0].id if (has_more and rows) else None
    items = [_serialize_decision_row(decision, execution, eval_row) for decision, execution, eval_row in rows]
    return items, has_more, next_cursor


def list_strategy_decisions_densified(
    session: Session,
    *,
    symbol: str,
    from_ts: int,
    to_ts: int,
    manifest_id: str | None = None,
    side: str | None = None,
    outcome: str | None = None,
    regime: str | None = None,
    bucket_seconds: int = 900,
) -> list[dict[str, Any]]:
    query = (
        select(StrategyDecision.decision_ts, DecisionExecution.status, DecisionEval.outcome_raw)
        .outerjoin(DecisionExecution, DecisionExecution.decision_id == StrategyDecision.id)
        .outerjoin(DecisionEval, DecisionEval.decision_id == StrategyDecision.id)
        .where(
            StrategyDecision.symbol == symbol.upper(),
            StrategyDecision.decision_ts >= int(from_ts),
            StrategyDecision.decision_ts <= int(to_ts),
        )
    )
    if manifest_id:
        query = query.where(StrategyDecision.manifest_id == manifest_id)
    if side:
        query = query.where(StrategyDecision.position_side == side.upper())
    if regime:
        query = query.outerjoin(AiSignal, AiSignal.id == StrategyDecision.analysis_id).where(
            func.upper(func.coalesce(AiSignal.market_regime, "")) == regime.upper()
        )
    if outcome:
        query = query.where(DecisionEval.outcome_raw == outcome.upper())
    rows = list(session.execute(query.order_by(StrategyDecision.decision_ts.asc())).all())

    bucket = max(60, int(bucket_seconds))
    buckets: dict[int, dict[str, Any]] = {}
    for decision_ts, exec_status, outcome_raw in rows:
        ts = int(decision_ts)
        bucket_ts = (ts // bucket) * bucket
        item = buckets.setdefault(
            bucket_ts,
            {
                "bucket_ts": bucket_ts,
                "count": 0,
                "tp": 0,
                "sl": 0,
                "ambiguous": 0,
                "timeout": 0,
                "no_fill": 0,
                "open": 0,
            },
        )
        item["count"] += 1
        if exec_status == "NO_FILL":
            item["no_fill"] += 1
            continue
        out = str(outcome_raw or "OPEN").upper()
        if out == "TP":
            item["tp"] += 1
        elif out == "SL":
            item["sl"] += 1
        elif out == "AMBIGUOUS":
            item["ambiguous"] += 1
        elif out == "TIMEOUT":
            item["timeout"] += 1
        elif out == "NO_FILL":
            item["no_fill"] += 1
        else:
            item["open"] += 1
    return [buckets[k] for k in sorted(buckets.keys())]


def get_strategy_decision_detail(session: Session, decision_id: int) -> dict[str, Any] | None:
    row = session.execute(
        select(StrategyDecision, DecisionExecution, DecisionEval)
        .outerjoin(DecisionExecution, DecisionExecution.decision_id == StrategyDecision.id)
        .outerjoin(DecisionEval, DecisionEval.decision_id == StrategyDecision.id)
        .where(StrategyDecision.id == int(decision_id))
        .limit(1)
    ).first()
    if not row:
        return None
    decision, execution, eval_row = row
    return _serialize_decision_row(decision, execution, eval_row)


def list_strategy_scores(
    session: Session,
    *,
    manifest_id: str | None = None,
    split_type: str | None = None,
    scoring_mode: str | None = None,
    limit: int = 200,
) -> list[StrategyScore]:
    query = select(StrategyScore)
    if manifest_id:
        query = query.where(StrategyScore.manifest_id == manifest_id)
    if split_type:
        query = query.where(StrategyScore.split_type == split_type.upper())
    if scoring_mode:
        query = query.where(StrategyScore.scoring_mode == scoring_mode.upper())
    query = query.order_by(StrategyScore.window_end_ts.desc(), StrategyScore.id.desc()).limit(max(1, int(limit)))
    return list(session.scalars(query))


def list_strategy_feature_stats(
    session: Session,
    *,
    manifest_id: str | None = None,
    split_type: str | None = None,
    scoring_mode: str | None = None,
    regime_id: str | None = None,
    status: str | None = None,
    limit: int = 500,
) -> list[StrategyFeatureStat]:
    query = select(StrategyFeatureStat)
    if manifest_id:
        query = query.where(StrategyFeatureStat.manifest_id == manifest_id)
    if split_type:
        query = query.where(StrategyFeatureStat.split_type == split_type.upper())
    if scoring_mode:
        query = query.where(StrategyFeatureStat.scoring_mode == scoring_mode.upper())
    if regime_id:
        query = query.where(StrategyFeatureStat.regime_id == regime_id)
    if status:
        query = query.where(StrategyFeatureStat.status == status.upper())
    query = query.order_by(StrategyFeatureStat.n.desc(), StrategyFeatureStat.id.desc()).limit(max(1, int(limit)))
    return list(session.scalars(query))


def list_manifest_ids_for_strategy(session: Session, *, limit: int = 1000) -> list[str]:
    rows = session.execute(
        select(StrategyDecision.manifest_id)
        .where(StrategyDecision.manifest_id.is_not(None))
        .group_by(StrategyDecision.manifest_id)
        .order_by(func.max(StrategyDecision.decision_ts).desc())
        .limit(max(1, int(limit)))
    ).all()
    return [str(r[0]) for r in rows if r and r[0]]


def list_strategy_decisions_for_eval(
    session: Session,
    *,
    limit: int = 200,
) -> list[tuple[StrategyDecision, DecisionExecution | None, DecisionEval | None]]:
    expiry_expr = func.coalesce(
        StrategyDecision.expiration_ts,
        StrategyDecision.decision_ts + (func.coalesce(StrategyDecision.max_hold_bars, 60) * 60),
    )
    rows = session.execute(
        select(StrategyDecision, DecisionExecution, DecisionEval)
        .outerjoin(DecisionExecution, DecisionExecution.decision_id == StrategyDecision.id)
        .outerjoin(DecisionEval, DecisionEval.decision_id == StrategyDecision.id)
        .where(
            StrategyDecision.market_type == "futures",
            or_(
                DecisionEval.id.is_(None),
                DecisionEval.outcome_raw == "OPEN",
            ),
        )
        .order_by(expiry_expr.asc(), StrategyDecision.decision_ts.asc(), StrategyDecision.id.asc())
        .limit(max(1, int(limit)))
    ).all()
    return [(d, e, v) for d, e, v in rows]


def upsert_decision_execution(
    session: Session,
    *,
    decision_id: int,
    payload: dict[str, Any],
    commit: bool = True,
) -> None:
    values = {**payload, "decision_id": int(decision_id), "updated_at": datetime.now(timezone.utc)}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        DecisionExecution,
        values,
        conflict_cols=["decision_id"],
        update_cols=[
            "status",
            "filled",
            "filled_ts",
            "filled_price",
            "filled_qty",
            "avg_fill_price",
            "source",
            "notes",
            "updated_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def upsert_decision_eval(
    session: Session,
    *,
    decision_id: int,
    payload: dict[str, Any],
    commit: bool = True,
) -> None:
    values = {**payload, "decision_id": int(decision_id), "updated_at": datetime.now(timezone.utc)}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    if "evaluated_at" not in values:
        values["evaluated_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        DecisionEval,
        values,
        conflict_cols=["decision_id"],
        update_cols=[
            "eval_replay_tf",
            "intrabar_flag",
            "tp_hit",
            "sl_hit",
            "first_hit_ts",
            "exit_ts",
            "exit_price",
            "outcome_raw",
            "r_multiple_raw",
            "mfe",
            "mae",
            "bars_to_outcome",
            "evaluated_at",
            "updated_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def list_strategy_rows_for_window(
    session: Session,
    *,
    manifest_id: str,
    window_start_ts: int,
    window_end_ts: int,
) -> list[tuple[StrategyDecision, DecisionExecution | None, DecisionEval | None, AiSignal | None]]:
    rows = session.execute(
        select(StrategyDecision, DecisionExecution, DecisionEval, AiSignal)
        .outerjoin(DecisionExecution, DecisionExecution.decision_id == StrategyDecision.id)
        .outerjoin(DecisionEval, DecisionEval.decision_id == StrategyDecision.id)
        .outerjoin(AiSignal, AiSignal.id == StrategyDecision.analysis_id)
        .where(
            StrategyDecision.manifest_id == manifest_id,
            StrategyDecision.decision_ts >= int(window_start_ts),
            StrategyDecision.decision_ts <= int(window_end_ts),
        )
        .order_by(StrategyDecision.decision_ts.asc(), StrategyDecision.id.asc())
    ).all()
    return [(d, e, v, s) for d, e, v, s in rows]


def upsert_strategy_score(
    session: Session,
    payload: dict[str, Any],
    *,
    commit: bool = True,
) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        StrategyScore,
        values,
        conflict_cols=["manifest_id", "window_start_ts", "window_end_ts", "split_type", "scoring_mode"],
        update_cols=[
            "status",
            "n_trades",
            "n_resolved",
            "n_ambiguous",
            "n_timeout",
            "win_rate",
            "avg_r",
            "win_rate_ci_low",
            "win_rate_ci_high",
            "avg_r_ci_low",
            "avg_r_ci_high",
            "timeout_rate",
            "created_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def upsert_strategy_feature_stat(
    session: Session,
    payload: dict[str, Any],
    *,
    commit: bool = True,
) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        StrategyFeatureStat,
        values,
        conflict_cols=[
            "manifest_id",
            "window_start_ts",
            "window_end_ts",
            "split_type",
            "regime_id",
            "scoring_mode",
            "feature_key",
            "bucket_key",
        ],
        update_cols=[
            "status",
            "n",
            "win_rate",
            "avg_r",
            "ci_low",
            "ci_high",
            "created_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


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


def upsert_futures_account_snapshot(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    stmt = _upsert_stmt(
        session,
        FuturesAccountSnapshot,
        values,
        conflict_cols=["ts"],
        update_cols=[
            "account_json",
            "balance_json",
            "positions_json",
            "total_margin_balance",
            "available_balance",
            "total_maint_margin",
            "btc_position_amt",
            "btc_mark_price",
            "btc_liquidation_price",
            "btc_unrealized_pnl",
            "last_seen_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def get_latest_futures_account_snapshot(session: Session) -> FuturesAccountSnapshot | None:
    return session.scalar(
        select(FuturesAccountSnapshot)
        .order_by(FuturesAccountSnapshot.ts.desc())
        .limit(1)
    )


def get_recent_futures_account_snapshots(session: Session, limit: int = 96) -> list[FuturesAccountSnapshot]:
    rows = list(
        session.scalars(
            select(FuturesAccountSnapshot)
            .order_by(FuturesAccountSnapshot.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


def upsert_margin_account_snapshot(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    stmt = _upsert_stmt(
        session,
        MarginAccountSnapshot,
        values,
        conflict_cols=["ts"],
        update_cols=[
            "account_json",
            "trade_coeff_json",
            "margin_level",
            "total_asset_of_btc",
            "total_liability_of_btc",
            "normal_bar",
            "margin_call_bar",
            "force_liquidation_bar",
            "last_seen_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def get_latest_margin_account_snapshot(session: Session) -> MarginAccountSnapshot | None:
    return session.scalar(
        select(MarginAccountSnapshot)
        .order_by(MarginAccountSnapshot.ts.desc())
        .limit(1)
    )


def get_recent_margin_account_snapshots(session: Session, limit: int = 96) -> list[MarginAccountSnapshot]:
    rows = list(
        session.scalars(
            select(MarginAccountSnapshot)
            .order_by(MarginAccountSnapshot.ts.desc())
            .limit(limit)
        )
    )
    rows.reverse()
    return rows


def touch_futures_account_snapshot_last_seen(
    session: Session,
    *,
    snapshot_id: int,
    seen_at: datetime,
    commit: bool = True,
) -> None:
    row = session.scalar(
        select(FuturesAccountSnapshot)
        .where(FuturesAccountSnapshot.id == int(snapshot_id))
        .limit(1)
    )
    if row is None:
        return
    row.last_seen_at = seen_at
    if commit:
        session.commit()


def touch_margin_account_snapshot_last_seen(
    session: Session,
    *,
    snapshot_id: int,
    seen_at: datetime,
    commit: bool = True,
) -> None:
    row = session.scalar(
        select(MarginAccountSnapshot)
        .where(MarginAccountSnapshot.id == int(snapshot_id))
        .limit(1)
    )
    if row is None:
        return
    row.last_seen_at = seen_at
    if commit:
        session.commit()


def upsert_account_snapshot_raw(
    session: Session,
    *,
    snapshot_type: str,
    ts: datetime,
    payload: dict[str, Any] | list[Any] | None,
    commit: bool = True,
) -> None:
    raw = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    values = {
        "snapshot_type": str(snapshot_type or "").strip().lower(),
        "ts": ts,
        "payload_gzip": gzip.compress(raw, compresslevel=6),
        "payload_size": len(raw),
    }
    stmt = _upsert_stmt(
        session,
        AccountSnapshotRaw,
        values,
        conflict_cols=["snapshot_type", "ts"],
        update_cols=["payload_gzip", "payload_size"],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def purge_old_account_snapshot_raw(session: Session, *, cutoff: datetime, commit: bool = True) -> int:
    stmt = delete(AccountSnapshotRaw).where(AccountSnapshotRaw.ts < cutoff)
    result = session.execute(stmt)
    if commit:
        session.commit()
    return int(result.rowcount or 0)


def _floor_day_utc(ts: datetime) -> datetime:
    ts_utc = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    ts_utc = ts_utc.astimezone(timezone.utc)
    return ts_utc.replace(hour=0, minute=0, second=0, microsecond=0)


def _to_utc_or_none(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def upsert_account_stats_daily(
    session: Session,
    *,
    sample_ts: datetime,
    equity_value: float | None,
    commit: bool = True,
) -> None:
    if equity_value is None:
        return
    sample_ts_utc = _to_utc_or_none(sample_ts) or sample_ts
    day_utc = _floor_day_utc(sample_ts_utc)
    now = datetime.now(timezone.utc)
    row = session.scalar(
        select(AccountStatsDaily)
        .where(AccountStatsDaily.day_utc == day_utc)
        .limit(1)
    )
    if row is None:
        row = AccountStatsDaily(
            day_utc=day_utc,
            equity_open=equity_value,
            equity_high=equity_value,
            equity_low=equity_value,
            equity_close=equity_value,
            sample_count=1,
            last_snapshot_ts=sample_ts_utc,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
    else:
        if row.equity_open is None:
            row.equity_open = equity_value
        if row.equity_high is None or equity_value > row.equity_high:
            row.equity_high = equity_value
        if row.equity_low is None or equity_value < row.equity_low:
            row.equity_low = equity_value
        row.equity_close = equity_value
        row.sample_count = int(row.sample_count or 0) + 1
        row_last_ts = _to_utc_or_none(getattr(row, "last_snapshot_ts", None))
        # Defensive fallback: malformed historical rows should not crash scheduler.
        if row_last_ts is None:
            row.last_snapshot_ts = sample_ts_utc
        elif sample_ts_utc >= row_last_ts:
            row.last_snapshot_ts = sample_ts_utc
        row.updated_at = now
    if commit:
        session.commit()


def list_account_stats_daily(
    session: Session,
    *,
    start_day: datetime | None = None,
    end_day: datetime | None = None,
    limit: int = 365,
) -> list[AccountStatsDaily]:
    stmt = select(AccountStatsDaily)
    if start_day is not None:
        stmt = stmt.where(AccountStatsDaily.day_utc >= _floor_day_utc(start_day))
    if end_day is not None:
        stmt = stmt.where(AccountStatsDaily.day_utc <= _floor_day_utc(end_day))
    stmt = stmt.order_by(AccountStatsDaily.day_utc.desc()).limit(max(1, int(limit)))
    rows = list(session.scalars(stmt))
    rows.reverse()
    return rows


def get_latest_futures_account_snapshot_in_range(
    session: Session,
    *,
    start_ts: datetime,
    end_ts: datetime,
) -> FuturesAccountSnapshot | None:
    return session.scalar(
        select(FuturesAccountSnapshot)
        .where(
            FuturesAccountSnapshot.ts >= start_ts,
            FuturesAccountSnapshot.ts < end_ts,
        )
        .order_by(FuturesAccountSnapshot.ts.desc())
        .limit(1)
    )


def purge_old_futures_account_snapshots(session: Session, *, cutoff: datetime, commit: bool = True) -> int:
    stmt = delete(FuturesAccountSnapshot).where(FuturesAccountSnapshot.ts < cutoff)
    result = session.execute(stmt)
    if commit:
        session.commit()
    return int(result.rowcount or 0)


def purge_old_margin_account_snapshots(session: Session, *, cutoff: datetime, commit: bool = True) -> int:
    stmt = delete(MarginAccountSnapshot).where(MarginAccountSnapshot.ts < cutoff)
    result = session.execute(stmt)
    if commit:
        session.commit()
    return int(result.rowcount or 0)


# --------------- Intel / News ---------------


def upsert_news_item(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload, "updated_at": datetime.now(timezone.utc)}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    stmt = _upsert_stmt(
        session,
        NewsItem,
        values,
        conflict_cols=["url_hash"],
        update_cols=[
            "ts_utc",
            "source",
            "category",
            "title",
            "title_hash",
            "url",
            "summary",
            "raw_text",
            "region",
            "topics_json",
            "alert_keyword",
            "severity",
            "entities_json",
            "metadata_json",
            "updated_at",
        ],
    )
    session.execute(stmt)
    if commit:
        session.commit()


def list_news_items(
    session: Session,
    *,
    last_hours: int = 24,
    category: str | None = None,
    severity_min: int | None = None,
    limit: int = 200,
) -> list[NewsItem]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(last_hours)))
    stmt = select(NewsItem).where(NewsItem.ts_utc >= cutoff)
    if category:
        stmt = stmt.where(NewsItem.category == category)
    if severity_min is not None:
        stmt = stmt.where(NewsItem.severity >= int(severity_min))
    stmt = stmt.order_by(NewsItem.ts_utc.desc()).limit(max(1, int(limit)))
    return list(session.scalars(stmt))


def save_intel_digest(session: Session, payload: dict[str, Any], commit: bool = True) -> None:
    values = {**payload}
    if "created_at" not in values:
        values["created_at"] = datetime.now(timezone.utc)
    row = IntelDigestCache(**values)
    session.add(row)
    if commit:
        session.commit()


def get_latest_intel_digest(
    session: Session,
    *,
    symbol: str = "GLOBAL",
    lookback_hours: int | None = None,
) -> IntelDigestCache | None:
    stmt = select(IntelDigestCache).where(IntelDigestCache.symbol == symbol)
    if lookback_hours is not None:
        stmt = stmt.where(IntelDigestCache.lookback_hours == int(lookback_hours))
    stmt = stmt.order_by(IntelDigestCache.created_at.desc()).limit(1)
    return session.scalars(stmt).first()


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
    now = datetime.now(timezone.utc)
    if "created_at" not in values:
        values["created_at"] = now
    if "status" not in values:
        values["status"] = PENDING_SUBTITLE
    if "status_updated_at" not in values:
        values["status_updated_at"] = now
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


def list_videos_stuck_in_asr(
    session: Session,
    stuck_hours: float = 2.0,
    limit: int = 50,
) -> list[YoutubeVideo]:
    """Videos stuck in QUEUED_ASR longer than stuck_hours (for dead-task alerting)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=stuck_hours)
    return list(
        session.scalars(
            select(YoutubeVideo)
            .where(
                YoutubeVideo.status == QUEUED_ASR,
                YoutubeVideo.status_updated_at.is_not(None),
                YoutubeVideo.status_updated_at < cutoff,
            )
            .order_by(YoutubeVideo.status_updated_at.asc())
            .limit(limit)
        )
    )


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
    now = datetime.now(timezone.utc)
    row.transcript_text = transcript_text
    row.transcript_lang = transcript_lang
    row.needs_asr = needs_asr
    row.processed_at = processed_at or now
    if transcript_text:
        row.status = PENDING_ANALYSIS
        row.status_updated_at = now
        row.asr_queued_at = None
    elif needs_asr:
        row.status = QUEUED_ASR
        row.status_updated_at = now
        row.asr_queued_at = row.asr_queued_at or now
    else:
        row.status = PENDING_SUBTITLE
        row.status_updated_at = now
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
    now = datetime.now(timezone.utc)
    row.asr_backend = asr_backend
    row.asr_model = asr_model
    row.asr_processed_at = now
    row.last_error = last_error
    if transcript_text:
        row.transcript_text = transcript_text
        row.transcript_lang = transcript_lang
        row.needs_asr = False
        row.status = PENDING_ANALYSIS
        row.status_updated_at = now
        row.asr_queued_at = None
    else:
        row.status = ASR_FAILED
        row.status_updated_at = now
    if commit:
        session.commit()


def update_youtube_video_status(
    session: Session,
    video_id: str,
    status: str,
    *,
    commit: bool = True,
) -> None:
    now = datetime.now(timezone.utc)
    session.execute(
        update(YoutubeVideo)
        .where(YoutubeVideo.video_id == video_id)
        .values(status=status, status_updated_at=now)
    )
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

    now = datetime.now(timezone.utc)
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
        row.status = PENDING_ANALYSIS
        row.status_updated_at = now

    if status is not _UNSET:
        row.analysis_runtime_status = status  # type: ignore[assignment]
        rt = str(status or "").lower()
        if rt in ("queued", "running"):
            row.status = ANALYZING
            row.status_updated_at = now
        elif rt in ("failed_paused", "failed"):
            row.status = FAILED
            row.status_updated_at = now
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
        if finished_at and str(status or "").lower() == "done":
            row.status = COMPLETED
            row.status_updated_at = now
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
        "status": ANALYZING,
        "status_updated_at": ts,
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
