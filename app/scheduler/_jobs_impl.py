from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import OrderedDict
from typing import Any

from app.alerts.dedup import should_emit, should_emit_ai_signal
from app.alerts.message_builder import (
    TelegramMessage,
    build_ai_signal_message,
    build_alert_ref,
    build_anomaly_message,
    build_flash_alert,
)
from app.db.repository import (
    count_sent_alerts_today,
    get_anomaly_state,
    get_latest_futures_account_snapshot,
    get_latest_funding_snapshots,
    get_latest_intel_digest,
    get_latest_margin_account_snapshot,
    get_latest_market_metric,
    get_latest_market_metrics,
    get_latest_ohlcv_ts,
    get_recent_market_metrics,
    get_recent_vol_values,
    insert_ai_signal,
    list_recent_sent_ai_signals,
    list_alerts,
    list_account_stats_daily,
    list_recent_ohlcv,
    mark_alert_sent,
    purge_old_account_snapshot_raw,
    purge_old_futures_account_snapshots,
    purge_old_margin_account_snapshots,
    touch_futures_account_snapshot_last_seen,
    touch_margin_account_snapshot_last_seen,
    upsert_account_snapshot_raw,
    upsert_account_stats_daily,
    upsert_anomaly_state,
    upsert_alert_event,
    upsert_futures_account_snapshot,
    upsert_funding_snapshot,
    upsert_margin_account_snapshot,
    upsert_ohlcv,
    update_alert_event_delivery,
)
from app.features.aggregator import aggregate_nm_from_1m, floor_utc_10m, rebuild_10m_range, rebuild_nm_range
from app.features.feature_pipeline import compute_and_store_latest_metric
from app.logging import logger
from app.providers.binance_provider import BinanceProvider
from app.providers.exchange_base import Candle
from app.scheduler.jobs_feature import run_feature_job
from app.scheduler.runtime import WorkerRuntime
from app.services.metric_utils import metric_to_dict
from app.signals.anomaly import (
    build_anomaly_state_key,
    build_event_uid,
    compute_mtf_confirmation,
    evaluate_anomalies,
    pick_adaptive_cooldown_seconds,
    score_anomaly_snapshot,
    score_to_severity_label_zh,
)
from app.storage.blobs import save_blob_with_meta
from app.strategy.manifest import build_manifest_id, build_manifest_payload
from app.utils.time import ensure_utc, utc_day_bounds, utc_now


_YT_ANALYZE_INFLIGHT: dict[str, datetime] = {}
_YT_ASR_INFLIGHT: dict[str, datetime] = {}
_YT_ANALYZE_INFLIGHT_TTL_SECONDS = 20 * 60
_YT_ASR_INFLIGHT_TTL_SECONDS = 120 * 60
_YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT = 420
_YT_ANALYSIS_STALL_WAITING_SECONDS_MIN = 420
_YT_RUNTIME_RECONCILE_DONE = False
_YT_AUTH_RECOVER_LAST_SIGNATURE: str | None = None
_ACCOUNT_LAST_RETENTION_CLEANUP_TS: datetime | None = None


@dataclass(slots=True)
class YoutubeAnalyzeVideoSnapshot:
    video_id: str
    title: str
    channel_id: str
    channel_title: str | None
    published_at: datetime | None
    transcript_text: str | None
    analysis_retry_count: int = 0


def _cleanup_inflight_expired(registry: dict[str, datetime], now_ts: datetime, ttl_seconds: int) -> None:
    expired = [k for k, ts in registry.items() if (now_ts - ensure_utc(ts)).total_seconds() >= ttl_seconds]
    for key in expired:
        registry.pop(key, None)


def _acquire_inflight(registry: dict[str, datetime], key: str, now_ts: datetime, ttl_seconds: int) -> bool:
    _cleanup_inflight_expired(registry, now_ts, ttl_seconds)
    if key in registry:
        return False
    registry[key] = now_ts
    return True


def _release_inflight(registry: dict[str, datetime], key: str) -> None:
    registry.pop(key, None)


def _safe_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    dt_utc = ensure_utc(dt).astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _bjt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    bjt = ensure_utc(dt).astimezone(timezone(timedelta(hours=8)))
    return bjt.isoformat()


def _extract_feature_keys_from_analysis(analysis_json: Any) -> list[str]:
    if not isinstance(analysis_json, dict):
        return []
    keys: set[str] = set()
    evidence = analysis_json.get("evidence")
    if isinstance(evidence, list):
        for item in evidence:
            if not isinstance(item, dict):
                continue
            metrics = item.get("metrics")
            if isinstance(metrics, dict):
                for mk in metrics.keys():
                    if mk:
                        keys.add(str(mk))
    return sorted(keys)


def _build_signal_manifest_id(runtime: WorkerRuntime, *, timeframe: str, analysis_json: Any) -> str:
    settings = runtime.settings
    market_cfg = settings.resolve_llm_config("market")
    payload = build_manifest_payload(
        prompt_template_hash="market_prompt_v2",
        schema_version="v2",
        model_provider=str(getattr(market_cfg, "provider", "") or "unknown"),
        model_name=str(getattr(market_cfg, "model", "") or "unknown"),
        temperature=float(getattr(market_cfg, "market_temperature", 0.1) or 0.1),
        top_p=None,
        max_tokens=4096,
        reasoning_effort=getattr(market_cfg, "reasoning_effort", None),
        feature_keys=_extract_feature_keys_from_analysis(analysis_json),
        timeframes=["1m"] + list(settings.multi_tf_interval_list or []),
        decision_rules_version="v1",
        facts_builder_version=str(getattr(settings, "strategy_facts_builder_version", "v1") or "v1"),
        data_pipeline_version=str(getattr(settings, "strategy_data_pipeline_version", "v1") or "v1"),
        exchange="binance",
        market_type="futures",
        regime_calc_mode=str(getattr(settings, "strategy_regime_calc_mode", "online") or "online"),
        eval_replay_tf=str(getattr(settings, "strategy_eval_replay_tf", "1m") or "1m"),
        ambiguous_policy="BOTH_HIT_AS_AMBIGUOUS",
        git_commit=os.getenv("GIT_COMMIT", "unknown"),
    )
    payload["base_timeframe"] = timeframe
    return build_manifest_id(payload)


def _safe_blob_meta(kind: str, payload: Any) -> tuple[str | None, str | None, int | None]:
    try:
        return save_blob_with_meta(kind, payload)
    except Exception as exc:
        logger.warning("save_blob_with_meta failed kind=%s err=%s", kind, exc)
        return None, None, None


def _epoch_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_effectively_equal(a: float | None, b: float | None, tolerance: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= max(0.0, float(tolerance))


def _futures_snapshot_changed(latest_row: Any | None, payload: dict[str, Any], tolerance: float) -> bool:
    if latest_row is None:
        return True
    keys = (
        "total_margin_balance",
        "available_balance",
        "total_maint_margin",
        "btc_position_amt",
        "btc_mark_price",
        "btc_liquidation_price",
        "btc_unrealized_pnl",
    )
    for key in keys:
        current = _to_float(payload.get(key))
        previous = _to_float(getattr(latest_row, key, None))
        if not _is_effectively_equal(current, previous, tolerance):
            return True
    return False


def _margin_snapshot_changed(latest_row: Any | None, payload: dict[str, Any], tolerance: float) -> bool:
    if latest_row is None:
        return True
    keys = (
        "margin_level",
        "total_asset_of_btc",
        "total_liability_of_btc",
        "normal_bar",
        "margin_call_bar",
        "force_liquidation_bar",
    )
    for key in keys:
        current = _to_float(payload.get(key))
        previous = _to_float(getattr(latest_row, key, None))
        if not _is_effectively_equal(current, previous, tolerance):
            return True
    return False


def _find_asset_balance(rows: list[dict[str, Any]], asset: str) -> dict[str, Any] | None:
    target = asset.upper()
    for row in rows:
        if str(row.get("asset") or "").upper() == target:
            return row
    return None


def _find_symbol_position(rows: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    target = symbol.upper()
    for row in rows:
        if str(row.get("symbol") or "").upper() == target:
            return row
    return None


def _build_account_event_uid(kind: str, ts: datetime, symbol: str, reason: str) -> str:
    raw = f"{kind}|{symbol.upper()}|{int(ts.timestamp())}|{reason}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:48]


def _liquidation_distance_pct(mark_price: float, liq_price: float, position_amt: float) -> float | None:
    if mark_price <= 0 or liq_price <= 0 or position_amt == 0:
        return None
    if position_amt > 0:
        distance = (mark_price - liq_price) / mark_price
    else:
        distance = (liq_price - mark_price) / mark_price
    if distance < 0:
        return None
    return distance * 100.0


def _calc_dynamic_liq_threshold_pct(
    *,
    mark_price: float,
    atr_14: float | None,
    atr_multiplier: float,
    static_floor_pct: float,
) -> tuple[float, float | None]:
    if mark_price <= 0:
        return max(0.0, static_floor_pct), None
    dynamic_pct = None
    if isinstance(atr_14, (int, float)) and atr_14 > 0:
        dynamic_pct = float(atr_multiplier) * (float(atr_14) / float(mark_price)) * 100.0
    threshold_pct = max(float(static_floor_pct), dynamic_pct if dynamic_pct is not None else float(static_floor_pct))
    return threshold_pct, dynamic_pct


def _build_account_snapshot_context(
    *,
    watch_symbol: str,
    futures_payload: dict[str, Any] | None,
    margin_payload: dict[str, Any] | None,
    min_balance_threshold: float,
) -> dict[str, Any]:
    futures_payload = futures_payload or {}
    margin_payload = margin_payload or {}
    available_balance = _to_float(futures_payload.get("available_balance"))
    mark_price = _to_float(futures_payload.get("btc_mark_price"))
    liq_price = _to_float(futures_payload.get("btc_liquidation_price"))
    position_amt = _to_float(futures_payload.get("btc_position_amt"))
    liq_distance_pct = None
    if mark_price and liq_price and position_amt:
        liq_distance_pct = _liquidation_distance_pct(mark_price, liq_price, position_amt)
    margin_level = _to_float(margin_payload.get("margin_level"))
    margin_call_bar = _to_float(margin_payload.get("margin_call_bar"))
    return {
        "watch_symbol": str(watch_symbol or "").upper(),
        "as_of_utc": (futures_payload.get("ts") or margin_payload.get("ts")),
        "futures": {
            "total_margin_balance": _to_float(futures_payload.get("total_margin_balance")),
            "available_balance": available_balance,
            "total_maint_margin": _to_float(futures_payload.get("total_maint_margin")),
            "position_amt": position_amt,
            "mark_price": mark_price,
            "liquidation_price": liq_price,
            "unrealized_pnl": _to_float(futures_payload.get("btc_unrealized_pnl")),
            "liq_distance_pct": liq_distance_pct,
        },
        "margin": {
            "margin_level": margin_level,
            "margin_call_bar": margin_call_bar,
            "force_liquidation_bar": _to_float(margin_payload.get("force_liquidation_bar")),
            "total_liability_of_btc": _to_float(margin_payload.get("total_liability_of_btc")),
        },
        "risk_flags": {
            "available_balance_low": (available_balance is not None and available_balance < float(min_balance_threshold)),
            "margin_near_call": (
                margin_level is not None
                and margin_call_bar is not None
                and margin_level <= margin_call_bar
            ),
        },
    }


def _should_skip_margin_level_alert(margin_payload: dict[str, Any]) -> bool:
    liability = _to_float(margin_payload.get("total_liability_of_btc"))
    if liability is None:
        return False
    return liability <= 1e-12


def _maybe_cleanup_account_snapshots(runtime: WorkerRuntime, now: datetime) -> int:
    global _ACCOUNT_LAST_RETENTION_CLEANUP_TS
    if _ACCOUNT_LAST_RETENTION_CLEANUP_TS is not None:
        elapsed = (ensure_utc(now) - ensure_utc(_ACCOUNT_LAST_RETENTION_CLEANUP_TS)).total_seconds()
        if elapsed < 3600:
            return 0
    retention_days = max(1, int(runtime.settings.account_snapshot_retention_days))
    cutoff = now - timedelta(days=retention_days)
    raw_retention_days = max(1, int(runtime.settings.account_snapshot_raw_retention_days or retention_days))
    raw_cutoff = now - timedelta(days=raw_retention_days)
    with runtime.session_factory() as session:
        deleted_fut = purge_old_futures_account_snapshots(session, cutoff=cutoff, commit=False)
        deleted_mar = purge_old_margin_account_snapshots(session, cutoff=cutoff, commit=False)
        deleted_raw = purge_old_account_snapshot_raw(session, cutoff=raw_cutoff, commit=False)
        session.commit()
    _ACCOUNT_LAST_RETENTION_CLEANUP_TS = now
    return deleted_fut + deleted_mar + deleted_raw


async def _send_account_risk_alert(
    runtime: WorkerRuntime,
    *,
    symbol: str,
    alert_type: str,
    reason: str,
    severity: str,
    metrics: dict[str, Any],
    event_ts: datetime,
) -> bool:
    with runtime.session_factory() as session:
        if not should_emit(session, symbol=symbol, alert_type=alert_type, cooldown_seconds=runtime.settings.alert_cooldown_seconds):
            return False
        event_uid = _build_account_event_uid(alert_type, event_ts, symbol, reason)
        payload = {
            "event_uid": event_uid,
            "symbol": symbol.upper(),
            "timeframe": "account",
            "ts": event_ts,
            "alert_type": alert_type,
            "severity": severity,
            "reason": reason,
            "rule_version": "account_v1",
            "metrics_json": metrics,
        }
        inserted = upsert_alert_event(session, payload)
        if not inserted:
            return False
        msg = TelegramMessage(
            text=(
                f"[Risk] {alert_type}\n"
                f"symbol={symbol.upper()} severity={severity}\n"
                f"{reason}\n"
                f"metrics={json.dumps(metrics, ensure_ascii=False)}"
            ),
        )
        sent = await runtime.telegram.send_message(msg)
        if sent:
            mark_alert_sent(session, event_uid)
        return sent


async def check_risk_and_alert(
    runtime: WorkerRuntime,
    *,
    ts: datetime,
    symbol: str,
    futures_payload: dict[str, Any],
    margin_payload: dict[str, Any],
) -> int:
    alerts_sent = 0

    available_balance = _to_float(futures_payload.get("available_balance"))
    min_balance = float(runtime.settings.account_alert_min_available_balance)
    if available_balance is not None and available_balance < min_balance:
        sent = await _send_account_risk_alert(
            runtime,
            symbol=symbol,
            alert_type="ACCOUNT_AVAILABLE_BALANCE_LOW",
            reason=f"availableBalance={available_balance:.4f} below threshold={min_balance:.4f}",
            severity="WARNING",
            metrics={"available_balance": available_balance, "threshold": min_balance},
            event_ts=ts,
        )
        alerts_sent += 1 if sent else 0

    mark_price = _to_float(futures_payload.get("btc_mark_price"))
    liq_price = _to_float(futures_payload.get("btc_liquidation_price"))
    position_amt = _to_float(futures_payload.get("btc_position_amt"))
    if (
        mark_price is not None
        and liq_price is not None
        and mark_price > 0
        and liq_price > 0
        and position_amt is not None
        and abs(position_amt) > 0
    ):
        atr_14 = None
        with runtime.session_factory() as session:
            metric_1h = get_latest_market_metric(session, symbol=symbol, timeframe="1h")
            if metric_1h is not None:
                atr_14 = _to_float(getattr(metric_1h, "atr_14", None))
        static_floor_pct = float(
            getattr(runtime.settings, "account_alert_liq_static_floor_pct", runtime.settings.account_alert_liq_distance_pct)
        )
        atr_multiplier = float(getattr(runtime.settings, "account_alert_liq_atr_multiplier", 1.5))
        liq_threshold, dynamic_threshold = _calc_dynamic_liq_threshold_pct(
            mark_price=mark_price,
            atr_14=atr_14,
            atr_multiplier=atr_multiplier,
            static_floor_pct=static_floor_pct,
        )
        distance_pct = _liquidation_distance_pct(mark_price, liq_price, position_amt)
        if distance_pct is not None and distance_pct <= liq_threshold:
            threshold_mode = "atr_dynamic" if dynamic_threshold is not None else "static_floor"
            sent = await _send_account_risk_alert(
                runtime,
                symbol=symbol,
                alert_type="ACCOUNT_LIQUIDATION_RISK",
                reason=(
                    f"markPrice={mark_price:.4f} liquidationPrice={liq_price:.4f} "
                    f"distance={distance_pct:.3f}% <= threshold={liq_threshold:.3f}% ({threshold_mode})"
                ),
                severity="CRITICAL",
                metrics={
                    "position_amt": position_amt,
                    "mark_price": mark_price,
                    "liquidation_price": liq_price,
                    "side": "LONG" if position_amt > 0 else "SHORT",
                    "distance_pct": distance_pct,
                    "threshold_pct": liq_threshold,
                    "threshold_mode": threshold_mode,
                    "atr_14": atr_14,
                    "atr_multiplier": atr_multiplier,
                    "dynamic_threshold_pct": dynamic_threshold,
                    "static_floor_pct": static_floor_pct,
                },
                event_ts=ts,
            )
            alerts_sent += 1 if sent else 0

    margin_level = _to_float(margin_payload.get("margin_level"))
    margin_call_bar = _to_float(margin_payload.get("margin_call_bar"))
    margin_buffer = float(runtime.settings.account_alert_margin_level_buffer)
    if (
        not _should_skip_margin_level_alert(margin_payload)
        and margin_level is not None
        and margin_call_bar is not None
        and margin_level <= (margin_call_bar + margin_buffer)
    ):
        sent = await _send_account_risk_alert(
            runtime,
            symbol=symbol,
            alert_type="MARGIN_LEVEL_NEAR_CALL",
            reason=(
                f"marginLevel={margin_level:.4f} near marginCallBar={margin_call_bar:.4f} "
                f"(buffer={margin_buffer:.4f})"
            ),
            severity="WARNING",
            metrics={
                "margin_level": margin_level,
                "margin_call_bar": margin_call_bar,
                "buffer": margin_buffer,
            },
            event_ts=ts,
        )
        alerts_sent += 1 if sent else 0

    return alerts_sent


async def _collect_and_store_account_snapshots(runtime: WorkerRuntime) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    watch_symbol = (runtime.settings.account_watch_symbol or "BTCUSDT").upper()
    http_client = runtime.http_client

    futures_account = await runtime.provider.get_futures_account(client=http_client)
    futures_balance = await runtime.provider.get_futures_balance(client=http_client)
    futures_positions = await runtime.provider.get_futures_positions(client=http_client)
    margin_account = await runtime.provider.get_margin_account(client=http_client)
    margin_trade_coeff = await runtime.provider.get_margin_trade_coeff(client=http_client)
    store_raw_in_main = bool(runtime.settings.account_snapshot_main_store_raw)

    usdt_balance_row = _find_asset_balance(futures_balance, "USDT") or {}
    watched_position = _find_symbol_position(futures_positions, watch_symbol) or {}
    futures_payload = {
        "ts": now,
        "account_json": futures_account if store_raw_in_main else None,
        "balance_json": futures_balance if store_raw_in_main else None,
        "positions_json": futures_positions if store_raw_in_main else None,
        "total_margin_balance": _to_float(futures_account.get("totalMarginBalance")),
        "available_balance": _to_float(futures_account.get("availableBalance"))
        or _to_float(usdt_balance_row.get("availableBalance")),
        "total_maint_margin": _to_float(futures_account.get("totalMaintMargin")),
        "btc_position_amt": _to_float(watched_position.get("positionAmt")),
        "btc_mark_price": _to_float(watched_position.get("markPrice")),
        "btc_liquidation_price": _to_float(watched_position.get("liquidationPrice")),
        "btc_unrealized_pnl": _to_float(watched_position.get("unRealizedProfit")),
        "last_seen_at": now,
    }
    margin_payload = {
        "ts": now,
        "account_json": margin_account if store_raw_in_main else None,
        "trade_coeff_json": margin_trade_coeff if store_raw_in_main else None,
        "margin_level": _to_float(margin_account.get("marginLevel")),
        "total_asset_of_btc": _to_float(margin_account.get("totalAssetOfBtc")),
        "total_liability_of_btc": _to_float(margin_account.get("totalLiabilityOfBtc")),
        "normal_bar": _to_float(margin_trade_coeff.get("normalBar")),
        "margin_call_bar": _to_float(margin_trade_coeff.get("marginCallBar")),
        "force_liquidation_bar": _to_float(margin_trade_coeff.get("forceLiquidationBar")),
        "last_seen_at": now,
    }

    rows_written = 0
    raw_rows_written = 0
    with runtime.session_factory() as session:
        latest_futures = get_latest_futures_account_snapshot(session)
        latest_margin = get_latest_margin_account_snapshot(session)
        tolerance = max(0.0, float(runtime.settings.account_snapshot_change_tolerance or 0.0))
        futures_changed = _futures_snapshot_changed(latest_futures, futures_payload, tolerance)
        margin_changed = _margin_snapshot_changed(latest_margin, margin_payload, tolerance)

        if futures_changed:
            upsert_futures_account_snapshot(session, futures_payload, commit=False)
            rows_written += 1
        elif latest_futures is not None:
            touch_futures_account_snapshot_last_seen(
                session,
                snapshot_id=int(latest_futures.id),
                seen_at=now,
                commit=False,
            )

        if margin_changed:
            upsert_margin_account_snapshot(session, margin_payload, commit=False)
            rows_written += 1
        elif latest_margin is not None:
            touch_margin_account_snapshot_last_seen(
                session,
                snapshot_id=int(latest_margin.id),
                seen_at=now,
                commit=False,
            )

        if runtime.settings.account_snapshot_raw_enabled:
            if futures_changed:
                upsert_account_snapshot_raw(
                    session,
                    snapshot_type="futures",
                    ts=now,
                    payload={
                        "account": futures_account,
                        "balance": futures_balance,
                        "positions": futures_positions,
                    },
                    commit=False,
                )
                raw_rows_written += 1
            if margin_changed:
                upsert_account_snapshot_raw(
                    session,
                    snapshot_type="margin",
                    ts=now,
                    payload={
                        "account": margin_account,
                        "trade_coeff": margin_trade_coeff,
                    },
                    commit=False,
                )
                raw_rows_written += 1

        if runtime.settings.account_daily_stats_enabled and futures_changed:
            upsert_account_stats_daily(
                session,
                sample_ts=now,
                equity_value=_to_float(futures_payload.get("total_margin_balance")),
                commit=False,
            )

        session.commit()

    return {
        "ts": now,
        "watch_symbol": watch_symbol,
        "futures_payload": futures_payload,
        "margin_payload": margin_payload,
        "rows_written": rows_written,
        "raw_rows_written": raw_rows_written,
    }


async def account_monitor_job(runtime: WorkerRuntime) -> dict[str, Any]:
    if not runtime.settings.account_monitor_enabled:
        return {"rows_read": 0, "rows_written": 0}
    if runtime.account_monitor_failed:
        return {"rows_read": 0, "rows_written": 0}
    if not runtime.settings.binance_api_key or not runtime.settings.binance_api_secret:
        logger.warning("account_monitor_job skipped: BINANCE_API_KEY/SECRET not configured")
        runtime.account_monitor_failed = True
        return {"rows_read": 0, "rows_written": 0}

    try:
        snap = await _collect_and_store_account_snapshots(runtime)
        ts = snap["ts"]
        symbol = str(snap["watch_symbol"])
        futures_payload = snap["futures_payload"]
        margin_payload = snap["margin_payload"]
        alerts_sent = await check_risk_and_alert(
            runtime,
            ts=ts,
            symbol=symbol,
            futures_payload=futures_payload,
            margin_payload=margin_payload,
        )

        deleted_rows = _maybe_cleanup_account_snapshots(runtime, ts)

        return {
            "rows_read": 5,
            "rows_written": int(snap.get("rows_written") or 0),
            "raw_rows_written": int(snap.get("raw_rows_written") or 0),
            "backlog": max(0, deleted_rows),
            "alerts_sent": alerts_sent,
        }
    except Exception as exc:
        logger.warning("account_monitor_job failed, disabling further retries: %s", exc)
        runtime.account_monitor_failed = True
        return {"rows_read": 0, "rows_written": 0}


async def account_user_stream_job(runtime: WorkerRuntime) -> None:
    if not runtime.settings.account_user_stream_enabled:
        return
    if runtime.account_user_stream_failed:
        return
    if not runtime.settings.binance_api_key or not runtime.settings.binance_api_secret:
        logger.warning("account_user_stream_job skipped: BINANCE_API_KEY/SECRET not configured")
        runtime.account_user_stream_failed = True
        return

    keepalive_seconds = max(1800, int(runtime.settings.account_user_stream_keepalive_seconds or 3000))
    while True:
        # Build a fresh REST baseline first, then apply WS-driven refreshes.
        try:
            snap = await _collect_and_store_account_snapshots(runtime)
            if runtime.settings.account_monitor_enabled:
                await check_risk_and_alert(
                    runtime,
                    ts=snap["ts"],
                    symbol=str(snap["watch_symbol"]),
                    futures_payload=snap["futures_payload"],
                    margin_payload=snap["margin_payload"],
                )
        except Exception as exc:
            logger.warning("account baseline snapshot failed before WS connect: %s", exc)
            runtime.account_user_stream_failed = True
            return
        try:
            listen_key = await runtime.provider.create_futures_listen_key(client=runtime.http_client)
        except Exception as exc:
            logger.warning("create futures listenKey failed: %s", exc)
            runtime.account_user_stream_failed = True
            return
        stop_event = asyncio.Event()

        async def keepalive_loop() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(keepalive_seconds)
                try:
                    await runtime.provider.keepalive_futures_listen_key(listen_key, client=runtime.http_client)
                except Exception as exc:
                    logger.warning("account user stream keepalive failed: %s", exc)
                    runtime.account_user_stream_failed = True
                    stop_event.set()
                    return

        keepalive_task = asyncio.create_task(keepalive_loop(), name="futures_listenkey_keepalive")

        async def on_event(payload: dict[str, Any]) -> None:
            if runtime.account_user_stream_failed:
                return
            event_type = str(payload.get("e") or "")
            if event_type == "ACCOUNT_UPDATE":
                try:
                    snap = await _collect_and_store_account_snapshots(runtime)
                    if runtime.settings.account_monitor_enabled:
                        await check_risk_and_alert(
                            runtime,
                            ts=snap["ts"],
                            symbol=str(snap["watch_symbol"]),
                            futures_payload=snap["futures_payload"],
                            margin_payload=snap["margin_payload"],
                        )
                except Exception as exc:
                    logger.warning("account snapshot refresh on ACCOUNT_UPDATE failed: %s", exc)
                    runtime.account_user_stream_failed = True

        try:
            await runtime.provider.consume_futures_user_stream(listen_key, on_event)
        except asyncio.CancelledError:
            stop_event.set()
            keepalive_task.cancel()
            await asyncio.gather(keepalive_task, return_exceptions=True)
            raise
        except Exception as exc:
            logger.warning("account user stream disconnected: %s", exc)
            runtime.account_user_stream_failed = True
        finally:
            stop_event.set()
            keepalive_task.cancel()
            await asyncio.gather(keepalive_task, return_exceptions=True)
            return


async def account_daily_stats_rollup_job(runtime: WorkerRuntime) -> dict[str, Any]:
    if not runtime.settings.account_daily_stats_enabled:
        return {"rows_read": 0, "rows_written": 0}
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    with runtime.session_factory() as session:
        existing = list_account_stats_daily(
            session,
            start_day=day_start,
            end_day=day_start,
            limit=1,
        )
        if existing:
            return {"rows_read": 1, "rows_written": 0}
        latest = get_latest_futures_account_snapshot(session)
        if latest is None:
            return {"rows_read": 0, "rows_written": 0}
        equity = _to_float(getattr(latest, "total_margin_balance", None))
        upsert_account_stats_daily(
            session,
            sample_ts=now,
            equity_value=equity,
            commit=False,
        )
        session.commit()
        return {"rows_read": 1, "rows_written": 1}


def _youtube_stall_thresholds(settings: Any) -> tuple[int, int]:
    running = max(
        int(getattr(settings, "youtube_analysis_stall_running_seconds", _YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT) or _YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT),
        60,
    )
    waiting = max(
        int(getattr(settings, "youtube_analysis_stall_waiting_seconds_effective", _YT_ANALYSIS_STALL_WAITING_SECONDS_MIN) or _YT_ANALYSIS_STALL_WAITING_SECONDS_MIN),
        _YT_ANALYSIS_STALL_WAITING_SECONDS_MIN,
    )
    return running, waiting


def _youtube_is_auth_error(error_type: str | None, error_code: str | None, message: str | None = None) -> bool:
    t = str(error_type or "").strip().lower()
    c = str(error_code or "").strip().lower()
    m = str(message or "").strip().lower()
    if t in {"auth", "provider_auth"}:
        return True
    if c in {"401", "auth", "authentication_error", "invalid_api_key", "invalid_request_error"}:
        return True
    return ("api key" in m and "invalid" in m) or ("authentication fails" in m)


def _youtube_classify_error(
    *,
    error_type: str | None,
    error_code: str | None,
    message: str | None,
) -> tuple[str, bool, str]:
    t = str(error_type or "").strip().lower()
    c = str(error_code or "").strip().lower()
    m = str(message or "").strip().lower()
    if _youtube_is_auth_error(t, c, m):
        return "provider_auth", False, "update_api_key"
    if t in {"rate_limit"} or c in {"429", "rate_limit"}:
        return "provider_rate_limit", True, "wait_auto_retry"
    if t in {"timeout"} or c in {"timeout"}:
        return "provider_timeout", True, "wait_auto_retry"
    if t in {"bad_request"} or c in {"bad_request"}:
        return "provider_bad_request", False, "manual_retry"
    if t in {"schema_error", "scoring_error", "parse_error", "no_json"} or c in {"schema", "scoring", "parse", "no_json"}:
        retryable = t in {"parse_error", "no_json"} or c in {"parse", "no_json"}
        return "schema", retryable, "manual_retry"
    if t in {"asr"} or c in {"asr"}:
        return "asr", False, "manual_retry"
    return "runtime", False, "check_worker_online"


def _youtube_runtime_reconcile_once(runtime: WorkerRuntime) -> dict[str, int]:
    global _YT_RUNTIME_RECONCILE_DONE
    if _YT_RUNTIME_RECONCILE_DONE:
        return {"queued_reset": 0, "running_blocked": 0, "error_backfilled": 0}
    _YT_RUNTIME_RECONCILE_DONE = True

    from sqlalchemy import or_, select

    from app.db.models import YoutubeInsight, YoutubeVideo
    from app.db.repository import update_youtube_video_analysis_runtime

    settings = runtime.settings
    now_utc = datetime.now(timezone.utc)
    running_threshold, waiting_threshold = _youtube_stall_thresholds(settings)
    stats = {"queued_reset": 0, "running_blocked": 0, "error_backfilled": 0}

    with runtime.session_factory() as session:
        rows = list(
            session.scalars(
                select(YoutubeVideo).where(
                    or_(
                        YoutubeVideo.analysis_runtime_status.in_(("queued", "running", "failed_paused")),
                        YoutubeVideo.analysis_runtime_status.is_not(None),
                    )
                )
            )
        )

        for row in rows:
            status = str(getattr(row, "analysis_runtime_status", "") or "").lower()
            stage = str(getattr(row, "analysis_stage", "") or "").lower()
            updated_at = getattr(row, "analysis_updated_at", None)
            updated_utc = ensure_utc(updated_at) if updated_at else None
            if updated_utc is None:
                continue
            age_seconds = (now_utc - updated_utc).total_seconds()
            if status == "queued" and stage != "retry_wait" and age_seconds >= waiting_threshold:
                # Reset stale queued rows to be schedulable again.
                update_youtube_video_analysis_runtime(session, row.video_id, reset=True, commit=False)
                stats["queued_reset"] += 1
                continue
            if status == "running" and age_seconds >= running_threshold:
                update_youtube_video_analysis_runtime(
                    session,
                    row.video_id,
                    status="failed_paused",
                    stage="failed",
                    finished_at=now_utc,
                    next_retry_at=None,
                    last_error_type="runtime",
                    last_error_code="stalled_timeout",
                    last_error_message=f"Analysis runtime exceeded stall threshold ({running_threshold}s).",
                    commit=False,
                )
                stats["running_blocked"] += 1

        missing_error_rows = [
            row
            for row in rows
            if str(getattr(row, "analysis_runtime_status", "") or "").lower() == "failed_paused"
            and not (
                getattr(row, "analysis_last_error_type", None)
                and getattr(row, "analysis_last_error_code", None)
                and getattr(row, "analysis_last_error_message", None)
            )
        ]
        for row in missing_error_rows:
            insight = session.scalar(
                select(YoutubeInsight)
                .where(YoutubeInsight.video_id == row.video_id)
                .order_by(YoutubeInsight.created_at.desc())
            )
            insight_json = insight.analyst_view_json if insight and isinstance(insight.analyst_view_json, dict) else None
            prov = (insight_json or {}).get("provenance") if isinstance(insight_json, dict) else None
            err = prov.get("analysis_error") if isinstance(prov, dict) else None
            if not isinstance(err, dict):
                continue
            err_type = str(err.get("type") or "").strip() or None
            err_code = str(err.get("status_code") or "").strip() or None
            err_msg = str(err.get("message") or "").strip()[:500] or None
            if not (err_type or err_code or err_msg):
                continue
            update_youtube_video_analysis_runtime(
                session,
                row.video_id,
                last_error_type=err_type or getattr(row, "analysis_last_error_type", None),
                last_error_code=err_code or getattr(row, "analysis_last_error_code", None),
                last_error_message=err_msg or getattr(row, "analysis_last_error_message", None),
                commit=False,
            )
            stats["error_backfilled"] += 1

        if any(stats.values()):
            session.commit()

    return stats


def _youtube_llm_signature(settings: Any) -> str:
    cfg = settings.resolve_llm_config("youtube")
    provider = str(getattr(cfg, "provider", "") or "").strip().lower()
    model = str(getattr(cfg, "model", "") or "").strip()
    base_url = str(getattr(cfg, "base_url", "") or "").strip()
    key_present = "1" if bool(getattr(cfg, "api_key", "")) else "0"
    return f"{provider}|{model}|{base_url}|{key_present}"


def _youtube_auto_recover_auth_failed(runtime: WorkerRuntime) -> int:
    global _YT_AUTH_RECOVER_LAST_SIGNATURE

    settings = runtime.settings
    current_signature = _youtube_llm_signature(settings)
    previous_signature = _YT_AUTH_RECOVER_LAST_SIGNATURE
    _YT_AUTH_RECOVER_LAST_SIGNATURE = current_signature

    if previous_signature is None:
        return 0
    if previous_signature == current_signature:
        return 0
    if not bool(getattr(settings, "youtube_auth_auto_recover_enabled", True)):
        return 0

    from sqlalchemy import select

    from app.db.models import YoutubeInsight, YoutubeVideo
    from app.db.repository import update_youtube_video_analysis_runtime

    batch = max(1, int(getattr(settings, "youtube_auth_auto_recover_batch", 20) or 20))
    max_attempts = max(1, int(getattr(settings, "youtube_auth_auto_recover_max_attempts", 2) or 2))
    now_utc = datetime.now(timezone.utc)

    with runtime.session_factory() as session:
        rows = list(
            session.scalars(
                select(YoutubeVideo)
                .where(
                    YoutubeVideo.analysis_runtime_status == "failed_paused",
                    YoutubeVideo.transcript_text.is_not(None),
                )
                .order_by(YoutubeVideo.analysis_updated_at.asc())
            )
        )

        recovered = 0
        for row in rows:
            retry_count = int(getattr(row, "analysis_retry_count", 0) or 0)
            if retry_count >= max_attempts:
                continue

            is_auth_failed = _youtube_is_auth_error(
                getattr(row, "analysis_last_error_type", None),
                getattr(row, "analysis_last_error_code", None),
                getattr(row, "analysis_last_error_message", None),
            )
            if not is_auth_failed:
                insight = session.scalar(
                    select(YoutubeInsight)
                    .where(YoutubeInsight.video_id == row.video_id)
                    .order_by(YoutubeInsight.created_at.desc())
                )
                insight_json = insight.analyst_view_json if insight and isinstance(insight.analyst_view_json, dict) else None
                prov = (insight_json or {}).get("provenance") if isinstance(insight_json, dict) else None
                err = prov.get("analysis_error") if isinstance(prov, dict) else None
                if isinstance(err, dict):
                    is_auth_failed = _youtube_is_auth_error(
                        str(err.get("type") or None),
                        str(err.get("status_code") or None),
                        str(err.get("message") or None),
                    )
            if not is_auth_failed:
                continue

            update_youtube_video_analysis_runtime(
                session,
                row.video_id,
                status="queued",
                stage="queued",
                started_at=now_utc,
                updated_at=now_utc,
                finished_at=None,
                retry_count=retry_count + 1,
                next_retry_at=None,
                last_error_type=None,
                last_error_code=None,
                last_error_message=None,
                commit=False,
            )
            recovered += 1
            if recovered >= batch:
                break

        if recovered > 0:
            session.commit()
        return recovered


def _build_failed_youtube_insight_placeholder(
    *,
    video,
    provider_name: str,
    model: str,
    error_type: str,
    status_code: str,
    message: str | None,
    request_id: str | None = None,
    status: str = "failed_paused",
    retry_policy: str = "manual_only",
) -> dict[str, Any]:
    return {
        "vta_version": "1.0",
        "meta": {
            "video_id": video.video_id,
            "publish_time_bjt": _bjt_iso(getattr(video, "published_at", None)),
        },
        "provenance": {
            "status": status,
            "retry_policy": retry_policy,
            "scores": None,
            "schema_errors": [],
            "analysis_error": {
                "type": error_type,
                "status_code": status_code,
                "message": (message or "")[:500],
            },
            "request_meta": {
                "provider": provider_name,
                "model": model,
                "request_id": request_id,
                "created_at_utc": _safe_iso(datetime.now(timezone.utc)),
            },
        },
    }


def _consensus_reject_reason(vta: Any) -> str:
    if not isinstance(vta, dict):
        return "not_dict"
    if not vta.get("vta_version"):
        return "no_vta_version"
    prov = vta.get("provenance") or {}
    if (prov.get("status") or "").lower() in {"failed_paused", "processing"}:
        return "failed_or_processing"
    if prov.get("analysis_error"):
        return "analysis_error"
    market_view = vta.get("market_view")
    if not isinstance(market_view, dict):
        return "no_market_view"
    if market_view.get("bias_1_7d") is None and market_view.get("bias_1_4w") is None:
        return "no_bias"
    if not isinstance(vta.get("levels"), dict):
        return "no_levels"
    scores = prov.get("scores")
    if not isinstance(scores, dict):
        return "no_scores"
    if scores.get("pq") is None:
        return "no_pq"
    return ""


def _is_valid_vta_for_consensus(vta: Any) -> bool:
    return _consensus_reject_reason(vta) == ""


def _youtube_queue_stats_snapshot(session, settings: Any | None = None) -> dict[str, int]:
    from sqlalchemy import select
    from app.db.models import YoutubeInsight, YoutubeVideo

    videos = list(
        session.execute(
            select(
                YoutubeVideo.video_id,
                YoutubeVideo.transcript_text,
                YoutubeVideo.needs_asr,
                YoutubeVideo.processed_at,
                YoutubeVideo.asr_processed_at,
                YoutubeVideo.last_error,
                YoutubeVideo.analysis_runtime_status,
                YoutubeVideo.analysis_stage,
                YoutubeVideo.analysis_updated_at,
                YoutubeInsight.analyst_view_json,
            ).outerjoin(YoutubeInsight, YoutubeInsight.video_id == YoutubeVideo.video_id)
        ).all()
    )

    stats = {
        "pending_subtitle": 0,
        "queued_asr": 0,
        "asr_failed_paused": 0,
        "pending_analysis": 0,
        "analysis_queued": 0,
        "analysis_running": 0,
        "analysis_retry_wait": 0,
        "analysis_stalled": 0,
        "analysis_failed_paused": 0,
        "analysis_done": 0,
    }
    now_ts = datetime.now(timezone.utc)
    if settings is None:
        running_stall_threshold = _YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT
        waiting_stall_threshold = _YT_ANALYSIS_STALL_WAITING_SECONDS_MIN
    else:
        running_stall_threshold, waiting_stall_threshold = _youtube_stall_thresholds(settings)
    for row in videos:
        transcript_text = row.transcript_text
        needs_asr = bool(row.needs_asr)
        if transcript_text:
            transcript_status = "transcribed"
        elif needs_asr:
            transcript_status = "asr_failed_paused" if (row.asr_processed_at is not None and row.last_error) else "queued_asr"
        else:
            transcript_status = "pending_subtitle"
        if transcript_status in stats:
            stats[transcript_status] += 1

        insight = row.analyst_view_json if isinstance(row.analyst_view_json, dict) else None
        if not transcript_text:
            continue
        runtime_status = str(getattr(row, "analysis_runtime_status", "") or "").lower()
        runtime_stage = str(getattr(row, "analysis_stage", "") or "").lower()
        runtime_active = runtime_status in {"queued", "running"}
        updated_at = getattr(row, "analysis_updated_at", None)
        updated_at = ensure_utc(updated_at) if updated_at is not None else None
        stalled = False
        if runtime_active and updated_at is not None and runtime_stage != "retry_wait":
            age_seconds = (now_ts - updated_at).total_seconds()
            threshold = running_stall_threshold if runtime_status == "running" else waiting_stall_threshold
            stalled = age_seconds >= threshold
        if insight is None:
            if runtime_status == "queued":
                stats["analysis_queued"] += 1
                if runtime_stage == "retry_wait":
                    stats["analysis_retry_wait"] += 1
            elif runtime_status == "running":
                stats["analysis_running"] += 1
            else:
                stats["pending_analysis"] += 1
            if stalled:
                stats["analysis_stalled"] += 1
            continue
        if _is_valid_vta_for_consensus(insight):
            stats["analysis_done"] += 1
        else:
            prov = (insight.get("provenance") or {}) if isinstance(insight, dict) else {}
            prov_status = (prov.get("status") or "").lower()
            if runtime_status == "queued":
                stats["analysis_queued"] += 1
                if runtime_stage == "retry_wait":
                    stats["analysis_retry_wait"] += 1
                if stalled:
                    stats["analysis_stalled"] += 1
                continue
            if runtime_status == "running" or prov_status == "processing":
                stats["analysis_running"] += 1
                if stalled:
                    stats["analysis_stalled"] += 1
                continue
            if (
                prov_status == "failed_paused"
                or prov.get("analysis_error")
                or prov.get("schema_errors")
                or prov.get("scores") is None
            ):
                stats["analysis_failed_paused"] += 1
            else:
                stats["pending_analysis"] += 1
    return stats


def _candle_to_payload(candle: Candle) -> dict:
    return {
        "symbol": candle.symbol,
        "timeframe": candle.timeframe,
        "ts": candle.ts,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "source": candle.source,
    }





def _compute_tick_return(
    points: list[tuple[datetime, float]],
    *,
    now: datetime,
    lookback_seconds: int,
) -> tuple[float, float] | None:
    if len(points) < 2:
        return None
    lookback_seconds = max(1, int(lookback_seconds))
    target = ensure_utc(now) - timedelta(seconds=lookback_seconds)
    base_price: float | None = None
    for ts, px in points:
        if ensure_utc(ts) <= target:
            base_price = float(px)
        else:
            break
    if base_price is None:
        base_price = float(points[0][1])
    if base_price <= 0:
        return None
    current_price = float(points[-1][1])
    return (current_price / base_price) - 1.0, base_price


async def _maybe_emit_tick_flash(runtime: WorkerRuntime, symbol: str, price: float, tick_ts: datetime) -> None:
    settings = runtime.settings
    if not getattr(settings, "anomaly_tick_flash_enabled", True):
        return
    if not settings.anomaly_flash_enabled:
        return
    now = ensure_utc(tick_ts)
    window = max(5, int(getattr(settings, "anomaly_tick_flash_lookback_seconds", 15) or 15))
    cooldown = max(10, int(getattr(settings, "anomaly_tick_flash_cooldown_seconds", 90) or 90))
    threshold = abs(float(getattr(settings, "anomaly_tick_flash_ret_threshold", 0.0018) or 0.0018))
    points = runtime.tick_price_windows.setdefault(symbol, [])
    points.append((now, float(price)))
    cutoff = now - timedelta(seconds=max(window * 3, 60))
    runtime.tick_price_windows[symbol] = [(ts, px) for ts, px in points if ensure_utc(ts) >= cutoff]
    points = runtime.tick_price_windows[symbol]
    ret_info = _compute_tick_return(points, now=now, lookback_seconds=window)
    if ret_info is None:
        return
    ret, base_price = ret_info
    if abs(ret) < threshold:
        return
    direction = "UP" if ret >= 0 else "DOWN"
    cooldown_key = f"{symbol}:{direction}"
    last_sent = runtime.tick_flash_last_sent.get(cooldown_key)
    if last_sent and (now - ensure_utc(last_sent)).total_seconds() < cooldown:
        return
    runtime.tick_flash_last_sent[cooldown_key] = now
    arrow = "上冲" if direction == "UP" else "下挫"
    pct = f"{ret * 100:+.2f}%"
    tick_event_uid = f"tick:{symbol}:{int(now.timestamp())}:{direction.lower()}"
    msg = TelegramMessage(
        text=(
            f"⚡️ <b>Tick快讯 #{symbol}</b>\n"
            f"{window}s 快速{arrow} | 幅度 {pct}\n"
            f"现价: {price:.4f} (基准: {base_price:.4f})\n\n"
            "⏳ 正在等待 1m 特征与AI诊断补充..."
        ),
        kind="anomaly_flash_tick",
        source_id=tick_event_uid,
    )
    send_res = await runtime.telegram.send_message_with_result(msg)
    if send_res.ok:
        logger.info(
            "[tick_flash] sent symbol=%s direction=%s ret=%s lookback=%ss",
            symbol,
            direction,
            pct,
            window,
        )
        if runtime.settings.anomaly_ai_diagnostic_enabled and send_res.message_id is not None:
            from app.ai.anomaly_analyst import enqueue_async_anomaly_diagnostic
            tick_payload = {
                "event_uid": tick_event_uid,
                "symbol": symbol,
                "timeframe": "tick",
                "ts": now,
                "alert_type": f"TICK_FLASH_{direction}",
                "severity": "INFO",
                "reason": f"{window}s 价格快速{arrow}，幅度 {pct}。",
                "metrics_json": {
                    "score": None,
                    "direction": direction,
                    "regime": "UNKNOWN",
                    "confirm": {"status": "tick_prealert"},
                    "thresholds": {
                        "tick_ret_threshold": threshold,
                        "tick_lookback_seconds": window,
                    },
                    "observations": {
                        "tick_ret": ret,
                        "base_price": base_price,
                        "latest_price": price,
                    },
                    "delivery": {
                        "source": "tick_prealert",
                    },
                },
            }
            task = asyncio.create_task(
                enqueue_async_anomaly_diagnostic(
                    runtime,
                    alert_payload=tick_payload,
                    event_uid=tick_event_uid,
                    reply_to_message_id=int(send_res.message_id),
                    alert_ref=f"TICK-{symbol}-{direction}",
                ),
                name=f"tick_diag_enqueue_{symbol}_{direction}",
            )
            def _tick_diag_done(t: asyncio.Task, _symbol: str = symbol) -> None:
                try:
                    t.result()
                except Exception as exc:
                    logger.warning("Tick diagnostic enqueue failed symbol=%s err=%s", _symbol, exc)
            task.add_done_callback(_tick_diag_done)


async def process_closed_candle(runtime: WorkerRuntime, candle: Candle) -> None:
    with runtime.session_factory() as session:
        upsert_ohlcv(session, _candle_to_payload(candle), commit=False)
        bucket = floor_utc_10m(candle.ts)
        aggregate_nm_from_1m(session, candle.symbol, bucket, 10, commit=False)
        # Also aggregate 5m and 15m from incoming 1m candles
        for n in (5, 15):
            aligned = candle.ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
            bucket_nm = aligned.replace(minute=(aligned.minute // n) * n)
            aggregate_nm_from_1m(session, candle.symbol, bucket_nm, n, commit=False)
        session.commit()


async def ws_consumer_job(runtime: WorkerRuntime) -> None:
    async def on_candle(candle: Candle) -> None:
        await process_closed_candle(runtime, candle)

    async def on_price(symbol: str, price: float, ts: datetime) -> None:
        runtime.latest_prices[symbol] = price
        await _maybe_emit_tick_flash(runtime, symbol, price, ts)

    await runtime.provider.consume_kline_stream(
        runtime.settings.watchlist_symbols,
        on_candle=on_candle,
        on_price=on_price if (runtime.settings.enable_miniticker or runtime.settings.anomaly_tick_flash_enabled) else None,
    )


async def startup_backfill_job(runtime: WorkerRuntime) -> None:
    days = runtime.settings.backfill_days_default
    for symbol in runtime.settings.watchlist_symbols:
        logger.info("Startup backfill: %s (%d days)", symbol, days)
        if runtime.sem_binance is not None:
            async with runtime.sem_binance:
                candles = await runtime.provider.backfill_recent_days(symbol, days=days)
        else:
            candles = await runtime.provider.backfill_recent_days(symbol, days=days)
        if not candles:
            continue

        with runtime.session_factory() as session:
            for candle in candles:
                upsert_ohlcv(session, _candle_to_payload(candle), commit=False)
            session.commit()
            rebuilt_10m = rebuild_10m_range(session, symbol, candles[0].ts, candles[-1].ts)
            # Also rebuild 5m and 15m
            rebuilt_5m = rebuild_nm_range(session, symbol, candles[0].ts, candles[-1].ts, 5)
            rebuilt_15m = rebuild_nm_range(session, symbol, candles[0].ts, candles[-1].ts, 15)
            logger.info(
                "Backfill done for %s: 1m=%d, 5m=%d, 10m=%d, 15m=%d",
                symbol, len(candles), rebuilt_5m, rebuilt_10m, rebuilt_15m,
            )


async def gap_fill_job(runtime: WorkerRuntime) -> None:
    for symbol in runtime.settings.watchlist_symbols:
        with runtime.session_factory() as session:
            last_ts = get_latest_ohlcv_ts(session, symbol, timeframe="1m")

        if runtime.sem_binance is not None:
            async with runtime.sem_binance:
                missing = await runtime.provider.fill_missing_since(symbol, last_ts)
        else:
            missing = await runtime.provider.fill_missing_since(symbol, last_ts)
        if not missing:
            continue

        with runtime.session_factory() as session:
            for candle in missing:
                upsert_ohlcv(session, _candle_to_payload(candle), commit=False)
            session.commit()
            rebuilt = rebuild_10m_range(session, symbol, missing[0].ts, missing[-1].ts)
            rebuild_nm_range(session, symbol, missing[0].ts, missing[-1].ts, 5)
            rebuild_nm_range(session, symbol, missing[0].ts, missing[-1].ts, 15)
            logger.info("Gap filled %s: inserted=%d rebuilt_10m=%d", symbol, len(missing), rebuilt)


async def feature_job(runtime: WorkerRuntime) -> None:
    return await run_feature_job(runtime)


def _bump_consecutive(active: bool, current_hits: int, above_enter: bool) -> int:
    if above_enter:
        return current_hits + 1
    if active:
        return current_hits
    return 0


def _alert_type_for_score_event(direction: str, event_kind: str) -> str:
    base = f"MOMENTUM_ANOMALY_{direction}"
    if event_kind == "ESCALATE":
        return f"{base}_ESCALATE"
    return base


def _regime_label_zh(regime: str | None) -> str:
    mapping = {
        "TRENDING": "趋势",
        "RANGING": "震荡",
        "VOLATILE": "高波动",
        "NEUTRAL": "中性",
    }
    return mapping.get((regime or "").upper(), regime or "未知")


def _confirm_status_zh(status: str | None) -> str:
    mapping = {
        "confirmed_5m": "5m已确认",
        "confirmed_15m": "15m已确认",
        "pending_mtf": "5m/15m待确认",
        "insufficient_data": "多周期数据不足",
        "not_required": "无需多周期确认",
    }
    return mapping.get(status or "", status or "未知")


def _direction_action_title_zh(direction: str) -> str:
    return "快速上冲" if direction == "UP" else "快速下挫"


def _score_level_zh(score: int) -> str:
    return score_to_severity_label_zh(score)


def _load_or_create_anomaly_state(session, *, state_key: str, symbol: str, timeframe: str, event_family: str, direction: str):
    state = get_anomaly_state(session, state_key)
    if state is not None:
        return state
    upsert_anomaly_state(
        session,
        {
            "state_key": state_key,
            "symbol": symbol,
            "timeframe": timeframe,
            "event_family": event_family,
            "direction": direction,
            "active": False,
            "consecutive_hits": 0,
        },
        commit=False,
    )
    session.flush()
    return get_anomaly_state(session, state_key)


def _cooldown_ready(last_alert_ts: datetime | None, cooldown_seconds: int, now_utc: datetime) -> bool:
    if last_alert_ts is None:
        return True
    return ensure_utc(last_alert_ts) <= (now_utc - timedelta(seconds=int(cooldown_seconds)))


def _current_5m_bucket(dt: datetime) -> str:
    dt_utc = ensure_utc(dt)
    minute = (dt_utc.minute // 5) * 5
    return dt_utc.replace(minute=minute, second=0, microsecond=0).isoformat()


def _build_score_alert_payload(
    *,
    snapshot,
    event_kind: str,
    confirm_result,
    cooldown_seconds: int,
    budget_used: int,
    budget_limit: int,
    suppressed_reason: str | None,
) -> dict[str, Any]:
    score = int(snapshot.score)
    direction = snapshot.direction
    event_title = _direction_action_title_zh(direction)
    confirm_text = _confirm_status_zh(confirm_result.status)
    reason = f"1分钟{event_title}，异常强度 Score {score}/100（{_score_level_zh(score)}），{confirm_text}。"
    price_ret = snapshot.observations.get("ret_1m")
    thr = snapshot.thresholds.get("price_threshold_ret")
    multiple = None
    if isinstance(price_ret, (int, float)) and isinstance(thr, (int, float)) and abs(thr) > 1e-12:
        multiple = abs(float(price_ret)) / abs(float(thr))

    alert_type = _alert_type_for_score_event(direction, event_kind)
    payload = {
        "symbol": snapshot.symbol,
        "timeframe": snapshot.timeframe,
        "ts": snapshot.ts,
        "alert_type": alert_type,
        "severity": "CRITICAL" if score >= 90 else "WARNING",
        "reason": reason,
        "rule_version": snapshot.rule_version,
        "regime": snapshot.regime,
        "metrics_json": {
            "score": score,
            "score_breakdown": snapshot.score_breakdown,
            "score_breakdown_meta": snapshot.score_breakdown_meta,
            "direction": direction,
            "regime": snapshot.regime,
            "confirm": {
                "status": confirm_result.status,
                "one_m": True,
                "five_m": confirm_result.five_m_confirmed,
                "fifteen_m": confirm_result.fifteen_m_confirmed,
                **(confirm_result.detail or {}),
            },
            "thresholds": snapshot.thresholds,
            "observations": {
                **snapshot.observations,
                "threshold_multiple": multiple,
            },
            "delivery": {
                "cooldown_seconds_applied": cooldown_seconds,
                "budget_used_today": budget_used,
                "budget_limit_today": budget_limit,
                "suppressed_reason": suppressed_reason,
            },
            "debug": {
                "event_family": snapshot.event_family,
                "event_kind": event_kind,
                "rule_version": snapshot.rule_version,
            },
        },
    }
    return payload


def _filter_v2_enabled() -> bool:
    return os.getenv("FILTER_V2", "").strip() == "1"


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num != num:  # NaN
        return None
    return num


def _clamp_int(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def _adaptive_enter_persist_bars(snapshot, settings: Settings) -> int:
    base = int(getattr(settings, "anomaly_persist_enter_bars", 2) or 2)
    obs = snapshot.observations if isinstance(getattr(snapshot, "observations", None), dict) else {}
    regime = str(getattr(snapshot, "regime", "") or "").upper()
    vol_pct = _to_float_or_none(obs.get("vol_percentile"))
    bb_bw_pct = _to_float_or_none(obs.get("bb_bandwidth_percentile_1m"))

    bars = base
    if regime in {"VOLATILE", "TRENDING"}:
        bars -= 1
    elif regime in {"RANGING", "NEUTRAL"}:
        bars += 0

    if (vol_pct is not None and vol_pct <= 0.20) or (bb_bw_pct is not None and bb_bw_pct <= 0.20):
        bars += 1
    if vol_pct is not None and vol_pct >= 0.97:
        bars -= 1
    return _clamp_int(bars, 1, 3)


def _escalate_min_interval_seconds(score: int, extreme_threshold: int) -> int:
    if score >= 98:
        return 60
    if score >= 96:
        return 90
    if score >= 94:
        return 120
    if score >= int(extreme_threshold):
        return 180
    return 300


def _anomaly_extreme_bypass_data_ok(snapshot, recent_1m_candles: list[Any], now: datetime) -> tuple[bool, dict[str, Any]]:
    age_sec: int | None = None
    snap_ts = getattr(snapshot, "ts", None)
    if isinstance(snap_ts, datetime):
        age_sec = int(max(0, (ensure_utc(now) - ensure_utc(snap_ts)).total_seconds()))

    window = list(recent_1m_candles[-10:]) if recent_1m_candles else []
    ts_list: list[datetime] = []
    for row in window:
        row_ts = getattr(row, "ts", None)
        if isinstance(row_ts, datetime):
            ts_list.append(ensure_utc(row_ts))

    max_gap_sec: int | None = None
    if len(ts_list) >= 2:
        gaps = [
            int(max(0, (ts_list[i] - ts_list[i - 1]).total_seconds()))
            for i in range(1, len(ts_list))
        ]
        max_gap_sec = max(gaps) if gaps else 0

    sample_size = len(ts_list)
    data_ok = bool(
        age_sec is not None
        and age_sec <= 150
        and sample_size >= 8
        and (max_gap_sec is None or max_gap_sec <= 150)
    )
    return data_ok, {
        "age_sec": age_sec,
        "max_gap_sec": max_gap_sec,
        "sample_size": sample_size,
    }


def _adaptive_exit_threshold(snapshot, enter_threshold: int, settings: Settings) -> int:
    base_delta = int(getattr(settings, "anomaly_hysteresis_exit_delta", 12) or 12)
    obs = snapshot.observations if isinstance(getattr(snapshot, "observations", None), dict) else {}
    vol_pct = _to_float_or_none(obs.get("vol_percentile"))
    regime = str(getattr(snapshot, "regime", "") or "").upper()

    factor = 1.0
    if regime == "TRENDING":
        factor = 1.20
    elif regime == "VOLATILE":
        factor = 1.35
    elif regime == "RANGING":
        factor = 0.85
    delta = int(round(base_delta * factor))
    if vol_pct is not None and vol_pct >= 0.95:
        delta += 2
    delta = _clamp_int(delta, 8, 35)
    return max(0, int(enter_threshold) - delta)


@dataclass(slots=True)
class AIGateDecision:
    action: str  # send | downgrade | skip
    reason_category: str | None = None
    debug: dict[str, Any] = field(default_factory=dict)
    resolved_entry: float | None = None
    resolved_take_profit: float | None = None
    resolved_stop_loss: float | None = None
    notes: list[str] = field(default_factory=list)


def _normalize_ai_market_regime(sig: Any) -> str:
    raw = ""
    market_regime = getattr(sig, "market_regime", None)
    if market_regime:
        raw = str(market_regime)
    elif isinstance(getattr(sig, "analysis_json", None), dict):
        raw = str((sig.analysis_json or {}).get("market_regime") or "")
    norm = raw.upper()
    if "VOLAT" in norm:
        return "VOLATILE"
    if "RANG" in norm:
        return "RANGING"
    if "TREND" in norm:
        return "TRENDING"
    if norm:
        return norm
    return "UNKNOWN"


def _pick_ai_gate_atr_ref(symbol_data: dict[str, Any]) -> tuple[float | None, str | None]:
    for tf in ("5m", "1m", "15m"):
        latest = ((symbol_data.get(tf) or {}).get("latest") or {})
        atr_val = _to_float_or_none(latest.get("atr_14"))
        if atr_val is not None and atr_val > 0:
            return atr_val, tf
    return None, None


def _extract_numeric_candidates(value: Any) -> list[float]:
    out: list[float] = []
    if value is None:
        return out
    if isinstance(value, (int, float)):
        v = _to_float_or_none(value)
        return [v] if v is not None else []
    if isinstance(value, dict):
        for key in ("price", "value", "level", "target"):
            v = _to_float_or_none(value.get(key))
            if v is not None:
                out.append(v)
        return out
    if isinstance(value, (list, tuple)):
        for item in value:
            out.extend(_extract_numeric_candidates(item))
        return out
    return out


def _extract_range_bounds(value: Any) -> tuple[float | None, float | None]:
    nums = sorted(_extract_numeric_candidates(value))
    if not nums:
        return None, None
    return nums[0], nums[-1]


def _resolve_signal_prices_for_filter(sig: Any) -> dict[str, Any]:
    direction = str(getattr(sig, "direction", "") or "").upper()
    analysis = getattr(sig, "analysis_json", None)
    analysis = analysis if isinstance(analysis, dict) else {}
    signal_obj = analysis.get("signal") if isinstance(analysis.get("signal"), dict) else {}
    levels_obj = analysis.get("levels") if isinstance(analysis.get("levels"), dict) else {}

    entry = _to_float_or_none(getattr(sig, "entry_price", None))
    tp = _to_float_or_none(getattr(sig, "take_profit", None))
    sl = _to_float_or_none(getattr(sig, "stop_loss", None))
    used_range = False

    if direction in {"LONG", "SHORT"}:
        if entry is None:
            entry_range = signal_obj.get("entry_range")
            if entry_range is None:
                entry_range = signal_obj.get("entry_zone")
            if entry_range is not None:
                lo, hi = _extract_range_bounds(entry_range)
                if lo is not None and hi is not None:
                    used_range = True
                    entry = hi if direction == "LONG" else lo
            if entry is None:
                entry = _to_float_or_none(signal_obj.get("entry"))

        if tp is None:
            tp = _to_float_or_none(signal_obj.get("take_profit"))
        if tp is None:
            tp = _to_float_or_none(signal_obj.get("tp"))
        if tp is None:
            tp_levels = signal_obj.get("take_profit_levels")
            if tp_levels is None:
                tp_levels = signal_obj.get("tp_levels")
            candidates = _extract_numeric_candidates(tp_levels)
            if candidates and entry is not None:
                if direction == "LONG":
                    favorable = sorted([x for x in candidates if x > entry])
                    tp = favorable[0] if favorable else None
                else:
                    favorable = sorted([x for x in candidates if x < entry], reverse=True)
                    tp = favorable[0] if favorable else None
            elif candidates:
                tp = candidates[0]

        if sl is None:
            sl = _to_float_or_none(signal_obj.get("stop_loss"))
        if sl is None:
            sl = _to_float_or_none(signal_obj.get("sl"))
        if sl is None:
            sl_range = signal_obj.get("stop_loss_range")
            if sl_range is None:
                sl_range = signal_obj.get("sl_range")
            lo, hi = _extract_range_bounds(sl_range)
            if lo is not None and hi is not None:
                sl = hi if direction == "LONG" else lo

    has_supports = bool(_extract_numeric_candidates(levels_obj.get("supports")))
    has_resistances = bool(_extract_numeric_candidates(levels_obj.get("resistances")))
    supports_res_only = (
        direction in {"LONG", "SHORT"}
        and (entry is None or tp is None or sl is None)
        and (has_supports or has_resistances)
    )

    return {
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "used_range": used_range,
        "supports_res_only": supports_res_only,
    }


def _classify_ai_data_freshness(symbol_data: dict[str, Any], now: datetime) -> dict[str, Any]:
    def _age_sec(tf: str) -> int | None:
        latest = ((symbol_data.get(tf) or {}).get("latest") or {})
        ts = latest.get("ts")
        if not isinstance(ts, datetime):
            return None
        return int(max(0, (ensure_utc(now) - ensure_utc(ts)).total_seconds()))

    age_1m = _age_sec("1m")
    age_5m = _age_sec("5m")
    age_15m = _age_sec("15m")
    ages = {"1m": age_1m, "5m": age_5m, "15m": age_15m}

    if age_1m is not None and age_1m <= 240:
        return {"action": "ok", "reason_category": None, "ages": ages}

    higher_tf_fresh = (age_5m is not None and age_5m <= 900) or (age_15m is not None and age_15m <= 1800)
    if age_1m is not None and 240 < age_1m <= 480 and higher_tf_fresh:
        return {"action": "downgrade", "reason_category": "stale_soft", "ages": ages}

    return {"action": "skip", "reason_category": "stale_hard", "ages": ages}


def _ai_signal_reason_append(reasoning: str | None, reason_category: str | None) -> str:
    text = (reasoning or "").strip()
    notes = {
        "edge_confidence": "置信度处于边缘区间，已降级为观察提醒（不提供具体点位）。",
        "sl_too_close": "止损距离过近，已降级为观察提醒（不提供具体点位）。",
        "rr_low": "风险收益比不足，已降级为观察提醒（不提供具体点位）。",
        "stale_soft": "数据可能延迟，已降级为观察提醒（不提供具体点位）。",
        "missing_prices": "价格点位信息不足，已降级为观察提醒（不提供具体点位）。",
    }
    note = notes.get(reason_category or "", "已降级为观察提醒（不提供具体点位）。")
    if note in text:
        return text
    return f"{text}\n[观察提醒] {note}" if text else f"[观察提醒] {note}"


def _upsert_ai_analysis_delivery_meta(
    analysis_json: dict[str, Any] | None,
    *,
    action: str,
    reason_category: str | None,
    debug: dict[str, Any],
    dedup_bucket_id: int | None = None,
    dedup_bucket_size: float | None = None,
) -> dict[str, Any]:
    data = analysis_json if isinstance(analysis_json, dict) else {}
    delivery = data.get("delivery")
    if not isinstance(delivery, dict):
        delivery = {}
        data["delivery"] = delivery
    delivery["gate_version"] = "v2"
    delivery["gate_decision"] = action
    delivery["reason_category"] = reason_category
    if dedup_bucket_id is not None:
        delivery["dedup_bucket_id"] = int(dedup_bucket_id)
    if dedup_bucket_size is not None:
        delivery["dedup_bucket_size"] = float(dedup_bucket_size)
    if debug:
        delivery["gate_debug"] = debug
    return data


def _dedup_ai_signal_v2(
    session,
    *,
    sig: Any,
    timeframe: str,
    now: datetime,
    cooldown_seconds: int,
    entry_price: float | None,
    atr_ref: float | None,
    is_observation: bool,
    reason_category: str | None,
) -> tuple[bool, dict[str, Any]]:
    since_ts = ensure_utc(now) - timedelta(seconds=int(cooldown_seconds) + 300)
    recent_rows = list_recent_sent_ai_signals(
        session,
        symbol=str(getattr(sig, "symbol", "") or ""),
        timeframe=timeframe,
        since_ts=since_ts,
        limit=200,
    )
    cooldown_cutoff = ensure_utc(now) - timedelta(seconds=int(cooldown_seconds))

    if is_observation:
        for row in recent_rows:
            if (row.direction or "").upper() != (getattr(sig, "direction", "") or "").upper():
                continue
            if ensure_utc(row.created_at) < cooldown_cutoff:
                continue
            if any(getattr(row, field) is not None for field in ("entry_price", "take_profit", "stop_loss")):
                continue
            row_analysis = row.analysis_json if isinstance(row.analysis_json, dict) else {}
            row_delivery = row_analysis.get("delivery") if isinstance(row_analysis.get("delivery"), dict) else {}
            if (row_delivery.get("reason_category") or None) == (reason_category or None):
                return True, {"mode": "observation", "recent_id": row.id}
        return False, {"mode": "observation"}

    if entry_price is None:
        return False, {"mode": "executable", "bucket_id": None, "bucket_size": None}

    bucket_size = max(float(entry_price) * 0.0010, 0.2 * float(atr_ref)) if atr_ref and atr_ref > 0 else float(entry_price) * 0.0010
    if bucket_size <= 0:
        bucket_size = 1e-9
    bucket_id = round(float(entry_price) / bucket_size)

    for row in recent_rows:
        if (row.direction or "").upper() != (getattr(sig, "direction", "") or "").upper():
            continue
        if ensure_utc(row.created_at) < cooldown_cutoff:
            continue
        if row.entry_price is None:
            continue
        prev_bucket = round(float(row.entry_price) / bucket_size)
        if prev_bucket == bucket_id:
            return True, {
                "mode": "price_bucket",
                "bucket_id": bucket_id,
                "bucket_size": bucket_size,
                "recent_id": row.id,
            }

    return False, {"mode": "price_bucket", "bucket_id": bucket_id, "bucket_size": bucket_size}


def _evaluate_ai_signal_gate_v2(
    *,
    sig: Any,
    symbol_data: dict[str, Any],
    now: datetime,
    threshold: int,
) -> AIGateDecision:
    decision = AIGateDecision(action="send")

    def _set_skip(reason: str, **debug: Any) -> AIGateDecision:
        decision.action = "skip"
        decision.reason_category = reason
        decision.debug.update(debug)
        return decision

    def _set_downgrade(reason: str, note: str | None = None, **debug: Any) -> None:
        if decision.action != "skip":
            decision.action = "downgrade"
        priority = {
            "edge_confidence": 1,
            "stale_soft": 2,
            "sl_too_close": 3,
            "rr_low": 4,
            "missing_prices": 5,
        }
        current_pri = priority.get(decision.reason_category or "", 0)
        new_pri = priority.get(reason, 0)
        if decision.reason_category is None or new_pri >= current_pri:
            decision.reason_category = reason
        if reason not in decision.notes:
            decision.notes.append(reason)
        if note:
            decision.debug.setdefault("notes_text", [])
            decision.debug["notes_text"].append(note)
        decision.debug.update(debug)

    direction = str(getattr(sig, "direction", "") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        return _set_skip("non_tradeable_direction", direction=direction)

    confidence = int(getattr(sig, "confidence", 0) or 0)
    if confidence < int(threshold):
        return _set_skip("confidence_low", confidence=confidence, threshold=int(threshold))
    if confidence < int(threshold) + 5:
        _set_downgrade("edge_confidence", confidence=confidence, threshold=int(threshold))

    freshness = _classify_ai_data_freshness(symbol_data, now)
    decision.debug["freshness"] = freshness
    if freshness["action"] == "skip":
        return _set_skip(freshness["reason_category"], freshness=freshness)
    if freshness["action"] == "downgrade":
        _set_downgrade("stale_soft", freshness=freshness)

    resolved = _resolve_signal_prices_for_filter(sig)
    entry = resolved["entry"]
    tp = resolved["tp"]
    sl = resolved["sl"]
    decision.resolved_entry = entry
    decision.resolved_take_profit = tp
    decision.resolved_stop_loss = sl
    decision.debug["price_resolve"] = {
        "used_range": bool(resolved.get("used_range")),
        "supports_res_only": bool(resolved.get("supports_res_only")),
    }

    if any(v is None for v in (entry, tp, sl)):
        if resolved.get("supports_res_only"):
            _set_downgrade("missing_prices", price_resolve=decision.debug["price_resolve"])
            return decision
        return _set_skip("missing_prices", price_resolve=decision.debug["price_resolve"])

    if direction == "LONG" and not (sl < entry < tp):
        return _set_skip("direction_inconsistent", entry=entry, sl=sl, tp=tp)
    if direction == "SHORT" and not (sl > entry > tp):
        return _set_skip("direction_inconsistent", entry=entry, sl=sl, tp=tp)

    atr_ref, atr_tf = _pick_ai_gate_atr_ref(symbol_data)
    decision.debug["atr_ref"] = {"value": atr_ref, "tf": atr_tf}
    min_sl_abs = max(entry * 0.0010, 0.8 * atr_ref) if atr_ref and atr_ref > 0 else (entry * 0.0020)
    sl_dist = abs(entry - sl)
    decision.debug["sl_distance"] = {"value": sl_dist, "min_required": min_sl_abs}
    if sl_dist < min_sl_abs:
        _set_downgrade("sl_too_close", sl_distance=sl_dist, min_sl_abs=min_sl_abs)

    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = (reward / risk) if risk > 0 else None
    regime = _normalize_ai_market_regime(sig)
    rr_threshold_map = {"TRENDING": 1.4, "RANGING": 1.15, "VOLATILE": 1.6}
    rr_threshold = rr_threshold_map.get(regime, 1.3)
    decision.debug["rr"] = {"value": rr, "threshold": rr_threshold, "regime": regime}
    if rr is None:
        return _set_skip("missing_prices", rr=None, regime=regime)
    if rr < rr_threshold:
        _set_downgrade("rr_low", rr=rr, rr_threshold=rr_threshold, regime=regime)

    return decision


async def _anomaly_job_legacy(runtime: WorkerRuntime) -> None:
    for symbol in runtime.settings.watchlist_symbols:
        with runtime.session_factory() as session:
            metric = get_latest_market_metric(session, symbol=symbol, timeframe="1m")
            if metric is None:
                continue
            candles = list_recent_ohlcv(session, symbol=symbol, timeframe="1m", limit=max(120, runtime.settings.breakout_lookback + 10))
            vol_history = get_recent_vol_values(session, symbol=symbol, timeframe="1m", limit=runtime.settings.vol_p75_min_candles)
            candidates = evaluate_anomalies(
                symbol=symbol,
                metric=metric,
                recent_1m_candles=candles,
                vol_history=vol_history,
                settings=runtime.settings,
            )

            for candidate in candidates:
                if not should_emit(
                    session,
                    symbol=candidate.symbol,
                    alert_type=candidate.alert_type,
                    cooldown_seconds=runtime.settings.alert_cooldown_seconds,
                ):
                    continue

                event_uid = build_event_uid(
                    symbol=candidate.symbol,
                    alert_type=candidate.alert_type,
                    timeframe=candidate.timeframe,
                    ts=candidate.ts,
                    rule_version=candidate.rule_version,
                )
                payload = {
                    "event_uid": event_uid,
                    "symbol": candidate.symbol,
                    "timeframe": candidate.timeframe,
                    "ts": candidate.ts,
                    "alert_type": candidate.alert_type,
                    "severity": candidate.severity,
                    "reason": candidate.reason,
                    "rule_version": candidate.rule_version,
                    "metrics_json": candidate.metrics,
                }

                inserted = upsert_alert_event(session, payload)
                if not inserted:
                    continue

                msg = build_anomaly_message(payload)
                sent = await runtime.telegram.send_message(msg)
                if sent:
                    mark_alert_sent(session, event_uid)


async def anomaly_job(runtime: WorkerRuntime) -> None:
    if runtime.settings.anomaly_engine_mode_normalized == "legacy_rules":
        await _anomaly_job_legacy(runtime)
        return
    use_filter_v2 = _filter_v2_enabled()

    for symbol in runtime.settings.watchlist_symbols:
        with runtime.session_factory() as session:
            metric = get_latest_market_metric(session, symbol=symbol, timeframe="1m")
            if metric is None:
                continue

            candles = list_recent_ohlcv(
                session,
                symbol=symbol,
                timeframe="1m",
                limit=max(160, runtime.settings.breakout_lookback + 20),
            )
            vol_history = get_recent_vol_values(
                session,
                symbol=symbol,
                timeframe="1m",
                limit=runtime.settings.vol_p75_min_candles,
            )
            recent_1m_metrics = get_recent_market_metrics(session, symbol=symbol, timeframe="1m", limit=200)
            mtf_5m = get_recent_market_metrics(session, symbol=symbol, timeframe="5m", limit=2)
            mtf_15m = get_recent_market_metrics(session, symbol=symbol, timeframe="15m", limit=2)

            snapshot = score_anomaly_snapshot(
                symbol=symbol,
                metric=metric,
                recent_1m_candles=candles,
                vol_history=vol_history,
                settings=runtime.settings,
                recent_1m_metrics=recent_1m_metrics,
            )
            if snapshot is None:
                continue

            confirm_result = compute_mtf_confirmation(
                direction=snapshot.direction,
                rows_5m=mtf_5m,
                rows_15m=mtf_15m,
                settings=runtime.settings,
            )
            five_detail = (confirm_result.detail or {}).get("five_m") or {}
            if snapshot.direction in {"UP", "DOWN"} and bool(five_detail.get("data_ok")) and bool(five_detail.get("close_ok")) and bool(five_detail.get("trend_ok")):
                original_structure = float(snapshot.score_breakdown.get("structure", 0.0))
                boosted_structure = min(20.0, round(original_structure + 4.0, 2))  # trend_align_5m = 0.2 * 20
                snapshot.score_breakdown["structure"] = boosted_structure
                snapshot.score = int(round(min(100.0, sum(float(v) for v in snapshot.score_breakdown.values()))))
                meta = snapshot.score_breakdown_meta or {}
                enabled = list(meta.get("components_enabled") or [])
                missing = list(meta.get("components_missing") or [])
                if "structure.trend_align_5m" not in enabled:
                    enabled.append("structure.trend_align_5m")
                missing = [x for x in missing if x != "structure.trend_align_5m"]
                meta["components_enabled"] = enabled
                meta["components_missing"] = missing
                meta["phase"] = "phase3_mtf_confirm_with_trend_align_5m"
                snapshot.score_breakdown_meta = meta
            now = utc_now()
            day_start, day_end = utc_day_bounds(now)
            enter_threshold = int(snapshot.thresholds.get("score_enter") or runtime.settings.anomaly_score_threshold_ranging)
            if use_filter_v2:
                exit_threshold = _adaptive_exit_threshold(snapshot, enter_threshold, runtime.settings)
            else:
                exit_threshold = int(
                    snapshot.thresholds.get("score_exit")
                    or max(0, enter_threshold - runtime.settings.anomaly_hysteresis_exit_delta)
                )
            adaptive_persist_bars = (
                _adaptive_enter_persist_bars(snapshot, runtime.settings)
                if use_filter_v2
                else int(runtime.settings.anomaly_persist_enter_bars)
            )
            extreme_quality_ok = False
            extreme_quality_detail: dict[str, Any] | None = None
            if use_filter_v2:
                extreme_quality_ok, extreme_quality_detail = _anomaly_extreme_bypass_data_ok(snapshot, candles, now)

            for direction in ("UP", "DOWN"):
                state_key = build_anomaly_state_key(symbol, "1m", snapshot.event_family, direction)
                state = _load_or_create_anomaly_state(
                    session,
                    state_key=state_key,
                    symbol=symbol,
                    timeframe="1m",
                    event_family=snapshot.event_family,
                    direction=direction,
                )
                if state is None:
                    logger.warning("[告警门控] 无法初始化状态 symbol=%s state_key=%s", symbol, state_key)
                    continue

                current_score = snapshot.score if snapshot.direction == direction else 0
                is_current_direction = snapshot.direction == direction and snapshot.alert_type is not None
                state.direction = direction
                state.last_metric_ts = snapshot.ts
                state.last_regime = snapshot.regime
                state.last_score = float(current_score)

                above_enter = is_current_direction and current_score >= enter_threshold
                state.consecutive_hits = _bump_consecutive(bool(state.active), int(state.consecutive_hits or 0), above_enter)

                if state.active and current_score <= exit_threshold:
                    logger.info(
                        "[告警门控] 退出活跃态 symbol=%s dir=%s score=%s exit=%s",
                        symbol, direction, current_score, exit_threshold,
                    )
                    state.active = False
                    state.consecutive_hits = 0
                    state.last_escalate_bucket = None
                    state.last_alert_kind = "EXIT_MARK"

                if not is_current_direction:
                    session.commit()
                    continue

                confirm_status = confirm_result.status
                mtf_ok = confirm_status in {"confirmed_5m", "confirmed_15m", "not_required"}
                extreme_unconfirmed = current_score >= int(runtime.settings.anomaly_score_threshold_unconfirmed_extreme)
                if use_filter_v2:
                    confirm_gate_ok = mtf_ok or (extreme_unconfirmed and extreme_quality_ok)
                else:
                    confirm_gate_ok = mtf_ok or extreme_unconfirmed
                if runtime.settings.anomaly_flash_enabled and not runtime.settings.anomaly_flash_require_mtf_confirm:
                    event_confirm_gate_ok = True
                else:
                    event_confirm_gate_ok = confirm_gate_ok

                event_kind: str | None = None
                if (not state.active) and state.consecutive_hits >= adaptive_persist_bars and current_score >= enter_threshold and event_confirm_gate_ok:
                    event_kind = "ENTER"
                elif (
                    state.active
                    and current_score >= int(runtime.settings.anomaly_score_threshold_unconfirmed_extreme)
                    and (use_filter_v2 or not state.last_escalate_bucket)
                    and event_confirm_gate_ok
                ):
                    event_kind = "ESCALATE"

                if event_kind is None:
                    logger.debug(
                        "[告警评分] symbol=%s score=%s regime=%s thr=%s confirm=%s hits=%s active=%s",
                        symbol, current_score, snapshot.regime, enter_threshold, confirm_status, state.consecutive_hits, state.active,
                    )
                    session.commit()
                    continue

                if use_filter_v2 and event_kind == "ESCALATE":
                    last_escalate_ts = ensure_utc(state.last_escalate_alert_ts) if state.last_escalate_alert_ts else None
                    min_interval_sec = _escalate_min_interval_seconds(
                        current_score,
                        int(runtime.settings.anomaly_score_threshold_unconfirmed_extreme),
                    )
                    if last_escalate_ts and (ensure_utc(now) - last_escalate_ts).total_seconds() < min_interval_sec:
                        logger.info(
                            "[anomaly_gate_v2] throttle escalate symbol=%s dir=%s score=%s interval=%ss",
                            symbol, direction, current_score, min_interval_sec,
                        )
                        session.commit()
                        continue

                cooldown_seconds = pick_adaptive_cooldown_seconds(current_score, runtime.settings)
                if not _cooldown_ready(state.last_alert_ts, cooldown_seconds, now):
                    logger.info(
                        "[告警门控] 冷却中 symbol=%s dir=%s kind=%s score=%s cooldown=%ss",
                        symbol, direction, event_kind, current_score, cooldown_seconds,
                    )
                    session.commit()
                    continue

                budget_limit = int(runtime.settings.anomaly_budget_per_symbol_per_day)
                budget_used = count_sent_alerts_today(session, symbol=symbol, start_utc=day_start, end_utc=day_end)
                suppressed_reason = None
                should_send = True
                if budget_limit > 0 and budget_used >= budget_limit and (runtime.settings.anomaly_budget_excess_action or "").strip().lower() == "store_only":
                    should_send = False
                    suppressed_reason = "budget_exceeded"
                    logger.info(
                        "[告警预算] 超预算仅落库 symbol=%s used=%s limit=%s score=%s",
                        symbol, budget_used, budget_limit, current_score,
                    )

                payload = _build_score_alert_payload(
                    snapshot=snapshot,
                    event_kind=event_kind,
                    confirm_result=confirm_result,
                    cooldown_seconds=cooldown_seconds,
                    budget_used=budget_used,
                    budget_limit=budget_limit,
                    suppressed_reason=suppressed_reason,
                )
                if use_filter_v2:
                    confirm_obj = payload.setdefault("metrics_json", {}).setdefault("confirm", {})
                    if isinstance(confirm_obj, dict):
                        confirm_obj["extreme_unconfirmed"] = bool(extreme_unconfirmed)
                        confirm_obj["extreme_bypass_data_ok"] = bool(extreme_quality_ok)
                        confirm_obj["extreme_bypass_data_quality"] = extreme_quality_detail or {}
                    debug_obj = payload.setdefault("metrics_json", {}).setdefault("debug", {})
                    if isinstance(debug_obj, dict):
                        debug_obj["filter_v2"] = True
                        debug_obj["adaptive_enter_persist_bars"] = int(adaptive_persist_bars)
                        debug_obj["exit_threshold_applied"] = int(exit_threshold)
                payload["event_uid"] = build_event_uid(
                    symbol=snapshot.symbol,
                    alert_type=payload["alert_type"],
                    timeframe=snapshot.timeframe,
                    ts=snapshot.ts,
                    rule_version=snapshot.rule_version,
                )

                inserted = upsert_alert_event(session, payload)
                if not inserted:
                    logger.debug("[告警门控] event_uid 已存在，跳过 symbol=%s uid=%s", symbol, payload["event_uid"][:8])
                    session.commit()
                    continue

                state.last_alert_ts = now
                state.last_alert_kind = event_kind
                state.active = True
                if event_kind == "ENTER":
                    state.active_cycle_started_ts = snapshot.ts
                    state.last_enter_alert_ts = now
                elif event_kind == "ESCALATE":
                    state.last_escalate_alert_ts = now
                    state.last_escalate_bucket = _current_5m_bucket(snapshot.ts)
                session.commit()

                if should_send:
                    if runtime.settings.anomaly_flash_enabled:
                        latest_price = runtime.latest_prices.get(symbol)
                        alert_ref = (
                            build_alert_ref(symbol, payload.get("event_uid"), payload.get("ts"))
                            if runtime.settings.anomaly_alert_ref_enabled
                            else str(payload.get("event_uid", ""))[:8]
                        )
                        msg = build_flash_alert(
                            payload,
                            latest_price=latest_price,
                            alert_ref=alert_ref,
                        )
                        sent_result = await runtime.telegram.send_message_with_result(msg)
                        if sent_result.ok:
                            mark_alert_sent(session, payload["event_uid"])
                            update_alert_event_delivery(
                                session,
                                event_uid=str(payload["event_uid"]),
                                updates={
                                    "alert_ref": alert_ref,
                                    "flash_message_id": sent_result.message_id,
                                    "flash_sent_at": utc_now().isoformat(),
                                    "flash_kind": str(event_kind),
                                },
                            )
                            logger.info(
                                "[告警推送] Flash已发送 symbol=%s kind=%s score=%s regime=%s confirm=%s",
                                symbol, event_kind, current_score, snapshot.regime, confirm_status,
                            )
                            if runtime.settings.anomaly_ai_diagnostic_enabled and sent_result.message_id is not None:
                                from app.ai.anomaly_analyst import enqueue_async_anomaly_diagnostic

                                task = asyncio.create_task(
                                    enqueue_async_anomaly_diagnostic(
                                        runtime,
                                        alert_payload=payload,
                                        event_uid=str(payload.get("event_uid") or ""),
                                        reply_to_message_id=int(sent_result.message_id),
                                        alert_ref=alert_ref,
                                    ),
                                    name=f"anomaly_ai_diag_{symbol}_{str(payload.get('event_uid', ''))[:8]}",
                                )

                                def _diag_done(t: asyncio.Task, _symbol: str = symbol) -> None:
                                    try:
                                        t.result()
                                    except Exception as exc:
                                        logger.warning("Anomaly AI diagnostic task failed symbol=%s err=%s", _symbol, exc)

                                task.add_done_callback(_diag_done)
                        else:
                            logger.warning(
                                "[告警推送] Flash发送失败 symbol=%s kind=%s score=%s",
                                symbol, event_kind, current_score,
                            )
                    else:
                        msg = build_anomaly_message(payload)
                        sent = await runtime.telegram.send_message(msg)
                        if sent:
                            mark_alert_sent(session, payload["event_uid"])
                            logger.info(
                                "[告警推送] 已发送 symbol=%s kind=%s score=%s regime=%s confirm=%s",
                                symbol, event_kind, current_score, snapshot.regime, confirm_status,
                            )
                        else:
                            logger.warning(
                                "[告警推送] 发送失败 symbol=%s kind=%s score=%s",
                                symbol, event_kind, current_score,
                            )
                else:
                    logger.info(
                        "[告警推送] 已抑制（仅落库） symbol=%s kind=%s score=%s reason=%s",
                        symbol, event_kind, current_score, suppressed_reason,
                    )


# ======== NEW: Multi-Timeframe Sync Job ========

async def multi_tf_sync_job(runtime: WorkerRuntime) -> None:
    """Sync multi-timeframe K-lines and compute indicators.

    - 5m/15m: aggregated from local 1m data (no extra API calls)
    - 1h/4h: incremental REST fetch from Binance
    """
    settings = runtime.settings
    rest_intervals = {"1h", "4h"}  # These need REST fetch

    for symbol in settings.watchlist_symbols:
        # --- REST fetch for 1h/4h ---
        for interval in settings.multi_tf_interval_list:
            if interval in rest_intervals:
                with runtime.session_factory() as session:
                    last_ts = get_latest_ohlcv_ts(session, symbol, timeframe=interval)

                interval_ms = BinanceProvider._interval_ms(interval)
                interval_td = timedelta(milliseconds=interval_ms)

                if last_ts is None:
                    # Initial backfill: fetch last N days
                    start_ts = datetime.now(timezone.utc) - timedelta(days=settings.multi_tf_backfill_days)
                    end_ts = datetime.now(timezone.utc) - interval_td
                else:
                    if last_ts.tzinfo is None:
                        last_ts = last_ts.replace(tzinfo=timezone.utc)
                    start_ts = last_ts + interval_td
                    end_ts = datetime.now(timezone.utc) - interval_td

                if start_ts >= end_ts:
                    continue

                try:
                    candles = await runtime.provider.fetch_klines(symbol, interval, start_ts, end_ts)
                except Exception as exc:
                    logger.warning("REST fetch %s %s failed: %r", symbol, interval, exc)
                    continue

                if candles:
                    with runtime.session_factory() as session:
                        for candle in candles:
                            upsert_ohlcv(session, _candle_to_payload(candle), commit=False)
                        session.commit()
                    logger.info("Multi-TF sync %s %s: fetched %d candles", symbol, interval, len(candles))

        # --- Compute indicators for all timeframes ---
        with runtime.session_factory() as session:
            for tf in settings.multi_tf_interval_list:
                compute_and_store_latest_metric(session, symbol=symbol, timeframe=tf)


# ======== NEW: Funding Rate Job ========

async def funding_rate_job(runtime: WorkerRuntime) -> None:
    """Fetch premium index + open interest from Binance Futures and store as snapshots."""
    now = datetime.now(timezone.utc)

    for symbol in runtime.settings.watchlist_symbols:
        premium = await runtime.provider.fetch_premium_index(symbol)
        oi = await runtime.provider.fetch_open_interest(symbol)

        if premium is None:
            continue

        payload: dict[str, Any] = {
            "symbol": symbol,
            "ts": now,
            "mark_price": premium.mark_price,
            "index_price": premium.index_price,
            "last_funding_rate": premium.last_funding_rate,
            "next_funding_time": premium.next_funding_time,
            "interest_rate": premium.interest_rate,
            "open_interest": oi,
            "open_interest_value": (oi * premium.mark_price) if oi and premium.mark_price else None,
        }

        with runtime.session_factory() as session:
            upsert_funding_snapshot(session, payload)

        logger.debug(
            "Funding snapshot %s: rate=%s, OI=%s",
            symbol,
            premium.last_funding_rate,
            oi,
        )


# ======== UPDATED: AI Analysis Job (multi-TF + funding) ========


def _metric_to_dict(m) -> dict[str, Any]:
    """Compatibility wrapper around app.services.metric_utils.metric_to_dict."""
    return metric_to_dict(m)


def _apply_ai_signal_downgrade(sig: Any, decision: AIGateDecision) -> None:
    reasoning = getattr(sig, "reasoning", "") or ""
    for reason in decision.notes or ([decision.reason_category] if decision.reason_category else []):
        reasoning = _ai_signal_reason_append(reasoning, reason)
    sig.reasoning = reasoning
    sig.entry_price = None
    sig.take_profit = None
    sig.stop_loss = None


async def _process_ai_signals_delivery_v2(
    runtime: WorkerRuntime,
    session,
    *,
    signals: list[Any],
    multi_tf_snapshots: dict[str, dict[str, Any]],
    now: datetime,
    threshold: int,
) -> None:
    settings = runtime.settings
    timeframe = "1m"

    for sig in signals:
        direction = str(getattr(sig, "direction", "") or "").upper()
        symbol_data = multi_tf_snapshots.get(getattr(sig, "symbol", None), {})
        if direction in {"LONG", "SHORT"}:
            decision = _evaluate_ai_signal_gate_v2(
                sig=sig,
                symbol_data=symbol_data,
                now=now,
                threshold=threshold,
            )
        else:
            decision = AIGateDecision(action="skip", reason_category="hold")

        atr_ref, _ = _pick_ai_gate_atr_ref(symbol_data)

        if decision.action in {"send", "downgrade"} and direction in {"LONG", "SHORT"}:
            dedup_hit, dedup_debug = _dedup_ai_signal_v2(
                session,
                sig=sig,
                timeframe=timeframe,
                now=now,
                cooldown_seconds=settings.alert_cooldown_seconds,
                entry_price=None if decision.action == "downgrade" else decision.resolved_entry,
                atr_ref=atr_ref,
                is_observation=(decision.action == "downgrade"),
                reason_category=decision.reason_category,
            )
            decision.debug["dedup"] = dedup_debug
            if dedup_hit:
                decision.action = "skip"
                decision.reason_category = "dedup"

        if decision.action == "send":
            # Use resolved prices when best-effort parsing extracted better values from analysis_json.
            if decision.resolved_entry is not None:
                sig.entry_price = decision.resolved_entry
            if decision.resolved_take_profit is not None:
                sig.take_profit = decision.resolved_take_profit
            if decision.resolved_stop_loss is not None:
                sig.stop_loss = decision.resolved_stop_loss
        elif decision.action == "downgrade":
            _apply_ai_signal_downgrade(sig, decision)

        analysis_json = getattr(sig, "analysis_json", None)
        sig.analysis_json = _upsert_ai_analysis_delivery_meta(
            analysis_json if isinstance(analysis_json, dict) else {},
            action=decision.action,
            reason_category=decision.reason_category,
            debug=decision.debug,
            dedup_bucket_id=(
                (decision.debug.get("dedup") or {}).get("bucket_id")
                if isinstance(decision.debug.get("dedup"), dict)
                else None
            ),
            dedup_bucket_size=(
                (decision.debug.get("dedup") or {}).get("bucket_size")
                if isinstance(decision.debug.get("dedup"), dict)
                else None
            ),
        )

        sent = False
        if decision.action in {"send", "downgrade"} and direction in {"LONG", "SHORT"}:
            msg = build_ai_signal_message(sig)
            sent = await runtime.telegram.send_message(msg)
            logger.info(
                "[ai_gate_v2] %s %s %s conf=%s reason=%s",
                decision.action,
                getattr(sig, "symbol", "?"),
                direction,
                getattr(sig, "confidence", None),
                decision.reason_category,
            )
        elif decision.action == "skip" and direction in {"LONG", "SHORT"}:
            logger.info(
                "[ai_gate_v2] skip %s %s conf=%s reason=%s",
                getattr(sig, "symbol", "?"),
                direction,
                getattr(sig, "confidence", None),
                decision.reason_category,
            )

        analysis_json = getattr(sig, "analysis_json", None)
        manifest_id = _build_signal_manifest_id(runtime, timeframe=timeframe, analysis_json=analysis_json)
        blob_ref, blob_sha256, blob_size_bytes = _safe_blob_meta("ai_signal_analysis", analysis_json or {})
        decision_ts = get_latest_ohlcv_ts(session, symbol=sig.symbol, timeframe=timeframe) or now
        payload = {
            "symbol": sig.symbol,
            "timeframe": timeframe,
            "ts": decision_ts,
            "direction": sig.direction,
            "entry_price": getattr(sig, "entry_price", None),
            "take_profit": getattr(sig, "take_profit", None),
            "stop_loss": getattr(sig, "stop_loss", None),
            "confidence": sig.confidence,
            "reasoning": getattr(sig, "reasoning", ""),
            "market_regime": getattr(sig, "market_regime", None),
            "analysis_json": analysis_json,
            "manifest_id": manifest_id,
            "blob_ref": blob_ref,
            "blob_sha256": blob_sha256,
            "blob_size_bytes": blob_size_bytes,
            "model_requested": getattr(sig, "model_requested", None),
            "model_name": getattr(sig, "model_name", ""),
            "prompt_tokens": getattr(sig, "prompt_tokens", None),
            "completion_tokens": getattr(sig, "completion_tokens", None),
            "sent_to_telegram": sent,
        }
        insert_ai_signal(session, payload, commit=False)


async def ai_analysis_job(runtime: WorkerRuntime) -> None:
    if runtime.market_analyst is None:
        return

    settings = runtime.settings

    # Build multi-timeframe data structure: {symbol: {tf: {latest: {...}, history: [...]}}}
    multi_tf_snapshots: dict[str, dict[str, Any]] = {}

    with runtime.session_factory() as session:
        for symbol in settings.watchlist_symbols:
            tf_data: dict[str, Any] = {}

            # Collect data for all timeframes: 1m + multi-tf intervals
            all_tfs = ["1m"] + settings.multi_tf_interval_list
            for tf in all_tfs:
                latest_metric = get_latest_market_metric(session, symbol=symbol, timeframe=tf)
                if latest_metric is None:
                    continue

                # Use real OHLCV history directly (avoid fake high/low approximations).
                recent_candles = list_recent_ohlcv(
                    session, symbol=symbol, timeframe=tf,
                    limit=settings.ai_history_candles,
                )
                candle_history = [
                    {"close": c.close, "high": c.high, "low": c.low, "open": c.open}
                    for c in recent_candles
                ]

                tf_data[tf] = {
                    "latest": _metric_to_dict(latest_metric),
                    "history": candle_history,
                }

            if tf_data:
                multi_tf_snapshots[symbol] = tf_data

        # Collect funding data
        funding_rows = get_latest_funding_snapshots(session, symbols=settings.watchlist_symbols)
        funding_data = [
            {
                "symbol": f.symbol,
                "ts": f.ts,
                "mark_price": f.mark_price,
                "index_price": f.index_price,
                "last_funding_rate": f.last_funding_rate,
                "open_interest": f.open_interest,
                "open_interest_value": f.open_interest_value,
            }
            for f in funding_rows
        ]

        # Collect recent alerts
        recent_alerts_rows = list_alerts(session, limit=10)
        recent_alerts = [
            {
                "symbol": a.symbol,
                "alert_type": a.alert_type,
                "severity": a.severity,
                "reason": a.reason,
                "ts": a.ts.isoformat() if a.ts else "",
            }
            for a in recent_alerts_rows
        ]

    if not multi_tf_snapshots:
        logger.info("AI analysis skipped: no market data available yet")
        return

    # Call LLM per symbol (new MarketAnalyst signature)
    from app.ai.analysis_flow import (
        build_scanner_hold_signal,
        prepare_context_and_snapshots,
        scanner_gate_passes,
    )
    from app.ai.market_context_builder import build_market_analysis_context
    from app.ai.analyst import attach_context_digest_to_analysis_json

    two_stage_enabled = bool(getattr(settings, "ai_two_stage_enabled", True))
    scan_threshold = int(getattr(settings, "ai_scan_confidence_threshold", 60) or 60)

    signals = []
    funding_by_symbol = {fd.get("symbol"): fd for fd in funding_data}
    alerts_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for a in recent_alerts:
        alerts_by_symbol.setdefault(a.get("symbol"), []).append(a)
    intel_digest_payload: dict[str, Any] | None = None
    account_context_payload: dict[str, Any] | None = None
    with runtime.session_factory() as session:
        intel_digest_row = get_latest_intel_digest(
            session,
            symbol="GLOBAL",
            lookback_hours=settings.intel_digest_lookback_hours,
        )
        if intel_digest_row is not None and isinstance(intel_digest_row.digest_json, dict):
            intel_digest_payload = intel_digest_row.digest_json
        futures_row = get_latest_futures_account_snapshot(session)
        margin_row = get_latest_margin_account_snapshot(session)
        futures_ctx_payload = {}
        margin_ctx_payload = {}
        if futures_row is not None:
            futures_ctx_payload = {
                "ts": futures_row.ts,
                "total_margin_balance": futures_row.total_margin_balance,
                "available_balance": futures_row.available_balance,
                "total_maint_margin": futures_row.total_maint_margin,
                "btc_position_amt": futures_row.btc_position_amt,
                "btc_mark_price": futures_row.btc_mark_price,
                "btc_liquidation_price": futures_row.btc_liquidation_price,
                "btc_unrealized_pnl": futures_row.btc_unrealized_pnl,
            }
        if margin_row is not None:
            margin_ctx_payload = {
                "ts": margin_row.ts,
                "margin_level": margin_row.margin_level,
                "margin_call_bar": margin_row.margin_call_bar,
                "force_liquidation_bar": margin_row.force_liquidation_bar,
                "total_liability_of_btc": margin_row.total_liability_of_btc,
            }
        if futures_ctx_payload or margin_ctx_payload:
            account_context_payload = _build_account_snapshot_context(
                watch_symbol=settings.account_watch_symbol,
                futures_payload=futures_ctx_payload,
                margin_payload=margin_ctx_payload,
                min_balance_threshold=float(settings.account_alert_min_available_balance),
            )

    for symbol, symbol_snapshots in multi_tf_snapshots.items():
        try:
            context = build_market_analysis_context(
                symbol=symbol,
                snapshots=symbol_snapshots,
                recent_alerts=alerts_by_symbol.get(symbol, []),
                funding_current=funding_by_symbol.get(symbol),
                funding_history=None,
                youtube_consensus=None,
                youtube_insights=None,
                intel_digest=intel_digest_payload,
                account_snapshot=account_context_payload,
                expected_timeframes=["4h", "1h", "15m", "5m", "1m"],
            )
            gate_ok, skip_reason = scanner_gate_passes(
                context, two_stage_enabled=two_stage_enabled, scan_threshold=scan_threshold
            )
            if not gate_ok and skip_reason:
                logger.info("[AI] Scanner gate rejected %s: %s", symbol, skip_reason)
                hold_sig = build_scanner_hold_signal(symbol, context, skip_reason)
                signals.append(hold_sig)
                continue

            context, symbol_snapshots = prepare_context_and_snapshots(
                context,
                symbol_snapshots,
                symbol,
                min_context_on_poor_data=bool(getattr(settings, "ai_min_context_on_poor_data", True)),
                min_context_on_non_tradeable=bool(getattr(settings, "ai_min_context_on_non_tradeable", True)),
            )

            if runtime.sem_llm is not None:
                async with runtime.sem_llm:
                    sigs, llm_meta = await runtime.market_analyst.analyze(
                        symbol, symbol_snapshots, context=context
                    )
            else:
                sigs, llm_meta = await runtime.market_analyst.analyze(symbol, symbol_snapshots, context=context)
            for sig in sigs:
                sig.analysis_json = attach_context_digest_to_analysis_json(sig.analysis_json, context)
            signals.extend(sigs)
            if llm_meta:
                llm_metas.append(llm_meta)
        except Exception as exc:
            logger.error("AI analysis failed for %s in worker job: %s", symbol, exc)
    logger.info("AI analysis returned %d signals", len(signals))

    with runtime.session_factory() as db:
        if llm_metas:
            from app.db.repository import insert_ai_analysis_failure, insert_llm_call
            for meta in llm_metas:
                try:
                    failure_events = meta.get("failure_events") if isinstance(meta, dict) else None
                    insert_llm_call(db, meta, commit=False)
                    if isinstance(failure_events, list):
                        for failure in failure_events:
                            if not isinstance(failure, dict):
                                continue
                            insert_ai_analysis_failure(db, failure, commit=False)
                except Exception as e:
                    logger.error("Failed to insert LLM call tracking: %s", e)
            db.commit()
    threshold = settings.ai_signal_confidence_threshold
    now = datetime.now(timezone.utc)
    use_filter_v2 = _filter_v2_enabled()
    
    with runtime.session_factory() as session:
        if use_filter_v2:
            await _process_ai_signals_delivery_v2(
                runtime,
                session,
                signals=signals,
                multi_tf_snapshots=multi_tf_snapshots,
                now=now,
                threshold=threshold,
            )
            session.commit()
            return

        for sig in signals:
            skip = False
            downgrade = False
            
            if sig.direction != "HOLD":
                if sig.confidence < threshold:
                    skip = True
                    logger.info("[telegram] skip signal: low confidence %d < %d for %s", sig.confidence, threshold, sig.symbol)
                elif sig.confidence < threshold + 5:
                    downgrade = True
                    logger.info("[telegram] downgrade signal: edge confidence %d for %s", sig.confidence, sig.symbol)
                
                entry = getattr(sig, 'entry_price', None)
                sl = getattr(sig, 'stop_loss', None)
                tp = getattr(sig, 'take_profit', None)
                
                # Check Direction Consistency
                if not skip and entry and sl and tp:
                    if sig.direction == "LONG" and not (sl < entry < tp):
                        skip = True
                        logger.info("[telegram] skip signal: LONG direction inconsistent (sl=%s, entry=%s, tp=%s) for %s", sl, entry, tp, sig.symbol)
                    elif sig.direction == "SHORT" and not (sl > entry > tp):
                        skip = True
                        logger.info("[telegram] skip signal: SHORT direction inconsistent (sl=%s, entry=%s, tp=%s) for %s", sl, entry, tp, sig.symbol)

                # Check Min distance (0.3%)
                if not skip and entry and sl:
                    dist = abs(entry - sl) / entry
                    if dist < 0.003:
                        skip = True
                        logger.info("[telegram] skip signal: stop too close (%.2f%% < 0.3%%) for %s", dist * 100, sig.symbol)

                # Check RR (Min 1.3)
                if not skip and entry and sl and tp:
                    risk = abs(entry - sl)
                    reward = abs(tp - entry)
                    if risk > 0 and (reward / risk) < 1.3:
                        skip = True
                        logger.info("[telegram] skip signal: RR too low (%.2f < 1.3) for %s", reward / risk, sig.symbol)

                # Check stale data
                if not skip:
                    symbol_data = multi_tf_snapshots.get(sig.symbol, {})
                    latest_1m_ts = symbol_data.get("1m", {}).get("latest", {}).get("ts")
                    if latest_1m_ts:
                        if latest_1m_ts.tzinfo is None:
                            latest_1m_ts = latest_1m_ts.replace(tzinfo=timezone.utc)
                        if (now - latest_1m_ts).total_seconds() > 180:
                            skip = True
                            logger.info("[telegram] skip signal: data too stale (%s) for %s", latest_1m_ts, sig.symbol)
                        
                # Dedup
                if not skip:
                    if not should_emit_ai_signal(
                        session, 
                        symbol=sig.symbol, 
                        direction=sig.direction, 
                        entry_price=entry, 
                        timeframe="1m", 
                        cooldown_seconds=settings.alert_cooldown_seconds
                    ):
                        skip = True
                        logger.info("[telegram] skip signal: dedup triggered for %s %s", sig.symbol, sig.direction)

            sent = False
            if not skip and sig.direction != "HOLD":
                if downgrade:
                    sig.reasoning += "\n [警告] 置信度处于边缘区间，降级为观察提醒，不提供具体点位。"
                    sig.entry_price = None
                    sig.take_profit = None
                    sig.stop_loss = None
                
                msg = build_ai_signal_message(sig)
                sent = await runtime.telegram.send_message(msg)

            analysis_json = getattr(sig, "analysis_json", None)
            manifest_id = _build_signal_manifest_id(runtime, timeframe="1m", analysis_json=analysis_json)
            blob_ref, blob_sha256, blob_size_bytes = _safe_blob_meta("ai_signal_analysis", analysis_json or {})
            decision_ts = get_latest_ohlcv_ts(session, symbol=sig.symbol, timeframe="1m") or now
            payload = {
                "symbol": sig.symbol,
                "timeframe": "1m",
                "ts": decision_ts,
                "direction": sig.direction,
                "entry_price": getattr(sig, 'entry_price', None),
                "take_profit": getattr(sig, 'take_profit', None),
                "stop_loss": getattr(sig, 'stop_loss', None),
                "confidence": sig.confidence,
                "reasoning": getattr(sig, 'reasoning', ''),
                "market_regime": getattr(sig, 'market_regime', None),
                "analysis_json": analysis_json,
                "manifest_id": manifest_id,
                "blob_ref": blob_ref,
                "blob_sha256": blob_sha256,
                "blob_size_bytes": blob_size_bytes,
                "model_requested": getattr(sig, 'model_requested', None),
                "model_name": getattr(sig, 'model_name', ''),
                "prompt_tokens": getattr(sig, 'prompt_tokens', None),
                "completion_tokens": getattr(sig, 'completion_tokens', None),
                "sent_to_telegram": sent,
            }
            insert_ai_signal(session, payload, commit=False)
        session.commit()


# ======== YouTube MVP Jobs ========


async def youtube_sync_job(runtime: WorkerRuntime) -> None:
    """Discover new videos via RSS and fetch transcripts (subtitles only)."""
    from app.db.repository import (
        list_unprocessed_youtube_videos,
        list_youtube_channels,
        update_youtube_video_transcript,
        upsert_youtube_video,
    )
    from app.providers.youtube_provider import fetch_channel_feed, fetch_transcript

    settings = runtime.settings
    if not settings.youtube_enabled:
        return

    # Get channels from DB
    with runtime.session_factory() as session:
        db_channels = list_youtube_channels(session)
        channel_ids = [ch.channel_id for ch in db_channels]

    # Also merge channels from .env config (seed)
    for cid in settings.youtube_channel_id_list:
        if cid and cid not in channel_ids:
            channel_ids.append(cid)

    if not channel_ids:
        logger.debug("YouTube sync: no channels configured")
        return

    # 1) Fetch RSS feeds and upsert videos
    total_new = 0
    for channel_id in channel_ids:
        try:
            entries = await fetch_channel_feed(channel_id, max_entries=settings.youtube_subtitle_fetch_max_per_run_effective)
        except Exception as exc:
            logger.warning("YouTube RSS fetch failed for %s: %s", channel_id, exc)
            continue

        with runtime.session_factory() as session:
            for entry in entries:
                upsert_youtube_video(session, {
                    "video_id": entry.video_id,
                    "channel_id": entry.channel_id,
                    "channel_title": entry.channel_title,
                    "title": entry.title,
                    "published_at": entry.published_at,
                    "url": entry.url,
                }, commit=False)
            session.commit()
        total_new += len(entries)

    logger.info("YouTube sync: fetched %d entries from %d channels", total_new, len(channel_ids))

    # 2) Fetch transcripts for unprocessed videos
    with runtime.session_factory() as session:
        unprocessed = list_unprocessed_youtube_videos(
            session,
            limit=settings.youtube_subtitle_fetch_max_per_run_effective,
            rescue_oldest=1,
        )
        stats = _youtube_queue_stats_snapshot(session, settings=settings)
    logger.info("[YT队列][sync] %s", stats)

    for video in unprocessed:
        try:
            result = fetch_transcript(video.video_id, settings.youtube_lang_list)
        except Exception as exc:
            logger.warning("Transcript fetch error for %s: %s", video.video_id, exc)
            result = None

        with runtime.session_factory() as session:
            if result:
                text, lang = result
                update_youtube_video_transcript(
                    session, video.video_id,
                    transcript_text=text, transcript_lang=lang,
                    needs_asr=False,
                )
                logger.info("Got transcript for %s (%s, %d chars)", video.video_id, lang, len(text))
            else:
                update_youtube_video_transcript(
                    session, video.video_id,
                    transcript_text=None, transcript_lang=None,
                    needs_asr=True,
                )
                logger.info("No transcript for %s, marked needs_asr=true", video.video_id)


async def youtube_asr_backfill_job(runtime: WorkerRuntime) -> None:
    """Transcribe videos that need ASR using local faster-whisper."""
    from app.db.repository import list_videos_needing_asr, update_youtube_video_asr_result
    from app.providers.youtube_provider import download_audio, transcribe_local

    settings = runtime.settings
    if not settings.youtube_enabled or not settings.asr_enabled:
        return

    with runtime.session_factory() as session:
        videos = list_videos_needing_asr(
            session,
            limit=settings.asr_max_videos_per_run,
            include_failed=False,
            rescue_oldest=1,
        )
        stats = _youtube_queue_stats_snapshot(session, settings=settings)
    logger.info("[YT队列][asr] %s", stats)

    if not videos:
        return

    logger.info("YouTube ASR backfill: %d videos to transcribe", len(videos))

    for video in videos:
        now_ts = datetime.now(timezone.utc)
        if not _acquire_inflight(_YT_ASR_INFLIGHT, video.video_id, now_ts, _YT_ASR_INFLIGHT_TTL_SECONDS):
            logger.info("YouTube ASR skip inflight duplicate: %s", video.video_id)
            continue
        try:
            last_error = None
            transcript_text = None
            transcript_lang = None
            # Step 1: Download audio
            audio_path = download_audio(
                video.video_id,
                cache_dir=settings.asr_audio_cache_dir,
                cookies_from_browser=settings.youtube_cookies_from_browser,
                cookies_file=settings.youtube_cookies_file,
            )
            if not audio_path:
                last_error = "Audio download failed"
                logger.warning("ASR: download failed for %s", video.video_id)
            else:
                # Step 2: Transcribe locally
                result = await asyncio.to_thread(
                    transcribe_local,
                    audio_path,
                    model_name=settings.asr_model,
                    device=settings.asr_device,
                    compute_type=settings.asr_compute_type,
                    vad_filter=settings.asr_vad_filter,
                )
                if result:
                    transcript_text, transcript_lang = result
                    logger.info(
                        "ASR success for %s: lang=%s, chars=%d",
                        video.video_id, transcript_lang, len(transcript_text),
                    )
                else:
                    last_error = "Transcription returned empty"

                # Step 3: Clean up audio if configured
                if not settings.asr_keep_audio and audio_path:
                    try:
                        os.remove(audio_path)
                    except OSError:
                        pass
        except Exception as exc:
            last_error = str(exc)[:500]
            logger.error("ASR failed for %s: %s", video.video_id, exc)
        finally:
            try:
                with runtime.session_factory() as session:
                    update_youtube_video_asr_result(
                        session,
                        video_id=video.video_id,
                        transcript_text=transcript_text,
                        transcript_lang=transcript_lang,
                        asr_backend=settings.asr_backend,
                        asr_model=settings.asr_model,
                        last_error=last_error,
                    )
            finally:
                _release_inflight(_YT_ASR_INFLIGHT, video.video_id)


async def youtube_analyze_job(runtime: WorkerRuntime) -> None:
    """Analyze videos with AI and generate consensus."""
    import json as _json

    from app.ai.youtube_prompts import (
        YOUTUBE_CONSENSUS_SYSTEM_PROMPT,
        YOUTUBE_VIDEO_SYSTEM_PROMPT,
        build_consensus_prompt,
        build_video_analysis_prompt,
    )
    from app.db.repository import (
        _UNSET as _YT_RUNTIME_UNSET,
        bulk_mark_youtube_videos_analysis_queued,
        get_recent_youtube_insights,
        list_videos_needing_analysis,
        save_youtube_consensus,
        save_youtube_insight,
        update_youtube_video_analysis_runtime,
    )

    settings = runtime.settings
    if not settings.youtube_enabled:
        return

    provider = runtime.youtube_llm_provider
    if provider is None:
        logger.debug("YouTube analyze: AI disabled, skipping")
        return

    youtube_config = settings.resolve_llm_config("youtube")
    youtube_provider_name = getattr(youtube_config, "provider", None) or getattr(getattr(provider, "config", None), "provider", None) or type(provider).__name__
    youtube_model_name = getattr(youtube_config, "model", None) or getattr(getattr(provider, "config", None), "model", None) or ""
    profile_resolver = getattr(settings, "resolve_llm_profile_name", None)
    youtube_profile_name = profile_resolver("youtube") if callable(profile_resolver) else "youtube"
    video_use_reasoning = youtube_config.use_reasoning.lower() == "true" or (
        youtube_config.use_reasoning.lower() == "auto" and provider.capabilities.supports_reasoning
    )
    reconcile_stats = _youtube_runtime_reconcile_once(runtime)
    if any(reconcile_stats.values()):
        logger.info(
            "YouTube runtime reconcile done: queued_reset=%d running_blocked=%d error_backfilled=%d",
            reconcile_stats["queued_reset"],
            reconcile_stats["running_blocked"],
            reconcile_stats["error_backfilled"],
        )
    recovered_auth = _youtube_auto_recover_auth_failed(runtime)
    if recovered_auth > 0:
        logger.info(
            "YouTube auth auto recover requeued=%d profile=%s provider=%s model=%s",
            recovered_auth,
            youtube_profile_name,
            youtube_provider_name,
            youtube_model_name,
        )

    # 1) Analyze individual videos (parallel inside a single scheduler job)
    with runtime.session_factory() as session:
        video_rows = list_videos_needing_analysis(
            session,
            limit=settings.youtube_analyze_max_per_run_effective,
            rescue_oldest=1,
        )
        stats = _youtube_queue_stats_snapshot(session, settings=settings)
    logger.info("[YT_QUEUE][analyze] %s", stats)

    queued_candidates = [
        YoutubeAnalyzeVideoSnapshot(
            video_id=row.video_id,
            title=row.title,
            channel_id=row.channel_id,
            channel_title=row.channel_title,
            published_at=row.published_at,
            transcript_text=row.transcript_text,
            analysis_retry_count=int(getattr(row, "analysis_retry_count", 0) or 0),
        )
        for row in video_rows
    ]
    if queued_candidates:
        logger.info("YouTube analyze: %d videos fetched from queue", len(queued_candidates))

    accepted_videos: list[YoutubeAnalyzeVideoSnapshot] = []
    for video in queued_candidates:
        now_ts = datetime.now(timezone.utc)
        if not _acquire_inflight(_YT_ANALYZE_INFLIGHT, video.video_id, now_ts, _YT_ANALYZE_INFLIGHT_TTL_SECONDS):
            logger.info("YouTube analyze skip inflight duplicate: %s", video.video_id)
            continue
        accepted_videos.append(video)

    def _mark_runtime(
        video_id: str,
        *,
        status: str | None | object = _YT_RUNTIME_UNSET,
        stage: str | None | object = _YT_RUNTIME_UNSET,
        started_at: datetime | None | object = _YT_RUNTIME_UNSET,
        updated_at: datetime | None | object = _YT_RUNTIME_UNSET,
        finished_at: datetime | None | object = _YT_RUNTIME_UNSET,
        retry_count: int | object = _YT_RUNTIME_UNSET,
        next_retry_at: datetime | None | object = _YT_RUNTIME_UNSET,
        last_error_type: str | None | object = _YT_RUNTIME_UNSET,
        last_error_code: str | None | object = _YT_RUNTIME_UNSET,
        last_error_message: str | None | object = _YT_RUNTIME_UNSET,
        reset: bool = False,
    ) -> None:
        try:
            with runtime.session_factory() as db:
                update_youtube_video_analysis_runtime(
                    db,
                    video_id,
                    status=status,
                    stage=stage,
                    started_at=started_at,
                    updated_at=updated_at,
                    finished_at=finished_at,
                    retry_count=retry_count,
                    next_retry_at=next_retry_at,
                    last_error_type=last_error_type,
                    last_error_code=last_error_code,
                    last_error_message=last_error_message,
                    reset=reset,
                )
        except Exception as exc:
            logger.warning("YouTube analysis runtime state update failed for %s: %s", video_id, exc)

    if accepted_videos:
        batch_now = datetime.now(timezone.utc)
        try:
            with runtime.session_factory() as session:
                bulk_mark_youtube_videos_analysis_queued(
                    session,
                    [v.video_id for v in accepted_videos],
                    now=batch_now,
                )
        except Exception:
            for video in accepted_videos:
                _release_inflight(_YT_ANALYZE_INFLIGHT, video.video_id)
            raise

        parallelism = max(1, int(getattr(youtube_config, "max_concurrency", 1) or 1))
        logger.info("YouTube analyze accepted=%d llm_parallelism=%d", len(accepted_videos), parallelism)
        max_auto_retries = max(0, int(getattr(settings, "youtube_analyze_max_auto_retries", 2) or 0))
        retry_base_seconds = max(1, int(getattr(settings, "youtube_analyze_retry_base_seconds", 60) or 60))
        retry_max_seconds = max(retry_base_seconds, int(getattr(settings, "youtube_analyze_retry_max_seconds", 900) or 900))

        async def _process_video(video: YoutubeAnalyzeVideoSnapshot) -> None:
            retry_count = int(video.analysis_retry_count or 0)

            def _retry_delay_seconds(next_retry_count: int) -> int:
                return min(retry_max_seconds, retry_base_seconds * (2 ** max(0, next_retry_count - 1)))

            def _record_failure_and_transition(
                *,
                error_type: str,
                status_code: str,
                message: str | None,
                failed_vta: dict[str, Any],
                retryable: bool,
            ) -> None:
                nonlocal retry_count
                category, classified_retryable, suggested_action = _youtube_classify_error(
                    error_type=error_type,
                    error_code=status_code,
                    message=message,
                )
                retryable = bool(retryable or classified_retryable)
                _mark_runtime(video.video_id, status="running", stage="save_insight")
                with runtime.session_factory() as session:
                    save_youtube_insight(session, {
                        "video_id": video.video_id,
                        "symbol": settings.youtube_target_symbol,
                        "analyst_view_json": failed_vta,
                    })
                now_utc = datetime.now(timezone.utc)
                msg = (message or "")[:500]
                if retryable and retry_count < max_auto_retries:
                    retry_count += 1
                    delay = _retry_delay_seconds(retry_count)
                    next_retry_at = now_utc + timedelta(seconds=delay)
                    _mark_runtime(
                        video.video_id,
                        status="queued",
                        stage="retry_wait",
                        updated_at=now_utc,
                        finished_at=None,
                        retry_count=retry_count,
                        next_retry_at=next_retry_at,
                        last_error_type=error_type,
                        last_error_code=status_code,
                        last_error_message=msg,
                    )
                    logger.warning(
                        "YouTube analyze transient failure video_id=%s queue_state=waiting reason=retry_wait "
                        "error.category=%s error.code=%s retryable=%s next_retry_at=%s "
                        "effective_llm.profile=%s effective_llm.provider=%s effective_llm.model=%s "
                        "suggested_action=%s err=%s retry=%d/%d next_retry_in=%ss",
                        video.video_id,
                        category,
                        status_code,
                        retryable,
                        _safe_iso(next_retry_at),
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        suggested_action,
                        msg[:200],
                        retry_count,
                        max_auto_retries,
                        delay,
                    )
                    return
                _mark_runtime(
                    video.video_id,
                    status="failed_paused",
                    stage="failed",
                    finished_at=now_utc,
                    next_retry_at=None,
                    last_error_type=error_type,
                    last_error_code=status_code,
                    last_error_message=msg,
                )
                logger.warning(
                    "YouTube AI blocked video_id=%s queue_state=blocked reason=failed_paused "
                    "error.category=%s error.code=%s retryable=%s next_retry_at=%s "
                    "effective_llm.profile=%s effective_llm.provider=%s effective_llm.model=%s "
                    "suggested_action=%s retries=%d/%d err=%s",
                    video.video_id,
                    category,
                    status_code,
                    retryable,
                    None,
                    youtube_profile_name,
                    youtube_provider_name,
                    youtube_model_name,
                    suggested_action,
                    retry_count,
                    max_auto_retries,
                    msg[:200],
                )

            try:
                prompt = build_video_analysis_prompt(
                    transcript=video.transcript_text or "",
                    title=video.title,
                    channel_title=video.channel_title or video.channel_id,
                    published_at=video.published_at.isoformat() if video.published_at else "",
                    symbol=settings.youtube_target_symbol,
                )

                import time

                start_time = time.perf_counter()
                status_code = "ok"
                error_msg = None
                response: dict[str, Any] = {}

                video_messages = [
                    {"role": "system", "content": YOUTUBE_VIDEO_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ]

                try:
                    from app.ai.provider import LLMAuthError, LLMBadRequestError, LLMRateLimitError, LLMTimeoutError

                    _mark_runtime(video.video_id, status="running", stage="llm_request")
                    response = await provider.generate_response(
                        messages=video_messages,
                        max_tokens=8192,
                        temperature=0.3,
                        response_format={"type": "json_object"},
                        use_reasoning=video_use_reasoning,
                    )
                except LLMRateLimitError as exc:
                    status_code = "429"
                    error_msg = str(exc)
                    logger.error(
                        "YouTube LLM error video_id=%s category=provider_rate_limit code=%s profile=%s provider=%s model=%s err=%s",
                        video.video_id,
                        status_code,
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        error_msg[:200],
                    )
                except LLMTimeoutError as exc:
                    status_code = "timeout"
                    error_msg = str(exc)
                    logger.error(
                        "YouTube LLM error video_id=%s category=provider_timeout code=%s profile=%s provider=%s model=%s err=%s",
                        video.video_id,
                        status_code,
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        error_msg[:200],
                    )
                except LLMAuthError as exc:
                    status_code = "auth"
                    error_msg = str(exc)
                    logger.error(
                        "YouTube LLM error video_id=%s category=provider_auth code=%s profile=%s provider=%s model=%s err=%s",
                        video.video_id,
                        status_code,
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        error_msg[:200],
                    )
                except LLMBadRequestError as exc:
                    status_code = "bad_request"
                    error_msg = str(exc)
                    logger.error(
                        "YouTube LLM error video_id=%s category=provider_bad_request code=%s profile=%s provider=%s model=%s err=%s",
                        video.video_id,
                        status_code,
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        error_msg[:200],
                    )
                except Exception as exc:
                    status_code = "error"
                    error_msg = str(exc)
                    logger.error(
                        "YouTube LLM error video_id=%s category=runtime code=%s profile=%s provider=%s model=%s err=%s",
                        video.video_id,
                        status_code,
                        youtube_profile_name,
                        youtube_provider_name,
                        youtube_model_name,
                        error_msg[:200],
                    )

                duration_ms = int((time.perf_counter() - start_time) * 1000)
                try:
                    with runtime.session_factory() as db:
                        from app.db.repository import insert_llm_call

                        insert_llm_call(db, {
                            "task": "youtube",
                            "provider_name": type(provider).__name__,
                            "model": youtube_model_name,
                            "status": status_code,
                            "duration_ms": duration_ms,
                            "prompt_tokens": response.get("prompt_tokens"),
                            "completion_tokens": response.get("completion_tokens"),
                            "error_summary": error_msg,
                        })
                except Exception as e:
                    logger.error("Failed to insert YouTube LLM call tracking: %s", e)

                if status_code != "ok" or not response:
                    error_type = "provider_error"
                    if status_code == "429":
                        error_type = "rate_limit"
                    elif status_code == "timeout":
                        error_type = "timeout"
                    elif status_code == "auth":
                        error_type = "auth"
                    elif status_code == "bad_request":
                        error_type = "bad_request"
                    elif status_code in {"error"}:
                        error_type = "transport"
                    retryable = error_type in {"rate_limit", "timeout", "transport"}
                    failed_vta = _build_failed_youtube_insight_placeholder(
                        video=video,
                        provider_name=provider.config.provider if hasattr(provider, "config") else type(provider).__name__,
                        model=youtube_model_name,
                        error_type=error_type,
                        status_code=status_code,
                        message=error_msg,
                        request_id=None,
                        retry_policy="auto_until_limit" if retryable else "manual_only",
                    )
                    _record_failure_and_transition(
                        error_type=error_type,
                        status_code=status_code,
                        message=error_msg,
                        failed_vta=failed_vta,
                        retryable=retryable,
                    )
                    return

                from app.ai.analyst import _extract_json
                from app.ai.vta_scorer import compute_scores, normalize_vta, validate_vta

                content = response.get("content", "")
                _mark_runtime(video.video_id, status="running", stage="parse_json")
                json_str = _extract_json(content)
                final_vta: dict[str, Any] = {}
                final_error: tuple[str, str, str | None, bool] | None = None

                if json_str:
                    try:
                        raw_data = _json.loads(json_str)
                        vta = normalize_vta(raw_data)
                        is_valid, errors = validate_vta(vta)
                        if not is_valid:
                            vta["provenance"]["schema_errors"] = errors
                            vta["provenance"]["scores"] = None
                            vta["provenance"]["status"] = "failed_paused"
                            vta["provenance"]["retry_policy"] = "manual_only"
                            vta["provenance"]["analysis_error"] = {
                                "type": "schema_error",
                                "status_code": "schema",
                                "message": "; ".join(errors)[:500],
                            }
                            final_error = ("schema_error", "schema", "; ".join(errors), False)
                            logger.warning("VTA schema validation failed for %s: %s", video.video_id, errors)
                        else:
                            try:
                                scores = compute_scores(vta)
                                vta["provenance"]["scores"] = scores
                                logger.info("VTA scored for %s: VSI=%s", video.video_id, scores.get("vsi"))
                            except Exception as e:
                                logger.error("VTA compute_scores error for %s: %s", video.video_id, e)
                                vta["provenance"]["scores"] = None
                                vta["provenance"]["schema_errors"].append(f"Scoring error: {e}")
                                vta["provenance"]["status"] = "failed_paused"
                                vta["provenance"]["retry_policy"] = "manual_only"
                                vta["provenance"]["analysis_error"] = {
                                    "type": "scoring_error",
                                    "status_code": "scoring",
                                    "message": str(e)[:500],
                                }
                                final_error = ("scoring_error", "scoring", str(e), False)
                        final_vta = vta
                    except Exception as e:
                        logger.error("VTA processing error for %s: %s", video.video_id, e)
                        final_vta = _build_failed_youtube_insight_placeholder(
                            video=video,
                            provider_name=provider.config.provider if hasattr(provider, "config") else type(provider).__name__,
                            model=youtube_model_name,
                            error_type="parse_error",
                            status_code="parse",
                            message=f"Parse/Processing error: {e}",
                            request_id=None,
                            retry_policy="auto_until_limit",
                        )
                        final_vta["raw_text_snippet"] = content[:500]
                        final_error = ("parse_error", "parse", f"Parse/Processing error: {e}", True)
                else:
                    logger.warning("Could not extract JSON from AI response for video %s", video.video_id)
                    final_vta = _build_failed_youtube_insight_placeholder(
                        video=video,
                        provider_name=provider.config.provider if hasattr(provider, "config") else type(provider).__name__,
                        model=youtube_model_name,
                        error_type="no_json",
                        status_code="no_json",
                        message="No JSON found in response",
                        request_id=None,
                        retry_policy="auto_until_limit",
                    )
                    final_vta["raw_text_snippet"] = content[:500]
                    final_error = ("no_json", "no_json", "No JSON found in response", True)

                if final_error is not None:
                    error_type, error_code, err_msg, retryable = final_error
                    _record_failure_and_transition(
                        error_type=error_type,
                        status_code=error_code,
                        message=err_msg,
                        failed_vta=final_vta,
                        retryable=retryable,
                    )
                    return

                _mark_runtime(video.video_id, status="running", stage="save_insight")
                with runtime.session_factory() as session:
                    save_youtube_insight(session, {
                        "video_id": video.video_id,
                        "symbol": settings.youtube_target_symbol,
                        "analyst_view_json": final_vta,
                    })

                final_ok = _is_valid_vta_for_consensus(final_vta)
                now_utc = datetime.now(timezone.utc)
                if final_ok:
                    _mark_runtime(
                        video.video_id,
                        status="done",
                        stage="done",
                        finished_at=now_utc,
                        retry_count=0,
                        next_retry_at=None,
                        last_error_type=None,
                        last_error_code=None,
                        last_error_message=None,
                    )
                else:
                    _mark_runtime(
                        video.video_id,
                        status="failed_paused",
                        stage="failed",
                        finished_at=now_utc,
                    )
            except Exception as exc:
                logger.error(
                    "YouTube analyze failed video_id=%s queue_state=blocked category=runtime code=worker_exception "
                    "effective_llm.profile=%s effective_llm.provider=%s effective_llm.model=%s err=%s",
                    video.video_id,
                    youtube_profile_name,
                    youtube_provider_name,
                    youtube_model_name,
                    str(exc)[:200],
                )
                failed_vta = _build_failed_youtube_insight_placeholder(
                    video=video,
                    provider_name=provider.config.provider if hasattr(provider, "config") else type(provider).__name__,
                    model=youtube_model_name,
                    error_type="transport",
                    status_code="worker_exception",
                    message=str(exc),
                    request_id=None,
                    retry_policy="auto_until_limit",
                )
                _record_failure_and_transition(
                    error_type="transport",
                    status_code="worker_exception",
                    message=str(exc),
                    failed_vta=failed_vta,
                    retryable=True,
                )
            finally:
                _release_inflight(_YT_ANALYZE_INFLIGHT, video.video_id)

        async with asyncio.TaskGroup() as tg:
            for video in accepted_videos:
                tg.create_task(_process_video(video), name=f"yt_analyze_{video.video_id}")

    # 2) Generate consensus
    with runtime.session_factory() as session:
        recent_insights = get_recent_youtube_insights(
            session,
            lookback_hours=settings.youtube_consensus_lookback_hours,
            symbol=settings.youtube_target_symbol,
        )

    if not recent_insights:
        logger.debug("YouTube consensus: no recent insights, skipping")
        return

    insight_views = []
    source_video_ids = []
    reject_stats = {
        "failed_paused": 0,
        "no_scores": 0,
        "invalid_schema": 0,
    }
    
    from math import exp
    from dateutil.parser import parse
    
    for ins in recent_insights:
        if ins.analyst_view_json:
            vta = ins.analyst_view_json
            reject_reason = _consensus_reject_reason(vta)
            if reject_reason:
                if reject_reason in {"failed_or_processing", "analysis_error"}:
                    reject_stats["failed_paused"] += 1
                elif reject_reason in {"no_scores", "no_pq"}:
                    reject_stats["no_scores"] += 1
                else:
                    reject_stats["invalid_schema"] += 1
                continue
            meta = vta.get('meta', {}) or {}
            pub_time_str = (
                meta.get('publish_time_bjt')
                or meta.get('publish_time_cst')
                or meta.get('publish_time_utc')
            )
            
            hours_since = 0
            if pub_time_str:
                try:
                    pub_ts = parse(pub_time_str)
                    if not pub_ts.tzinfo:
                        field_name = "publish_time_bjt" if meta.get("publish_time_bjt") else (
                            "publish_time_cst" if meta.get("publish_time_cst") else "publish_time_utc"
                        )
                        if field_name in ("publish_time_bjt", "publish_time_cst"):
                            pub_ts = pub_ts.replace(tzinfo=timezone(timedelta(hours=8)))
                        else:
                            pub_ts = pub_ts.replace(tzinfo=timezone.utc)
                    hours_since = (datetime.now(timezone.utc) - pub_ts).total_seconds() / 3600.0
                except:
                    pass
            if hours_since <= 0:
                # fallback to record creation time
                if ins.created_at:
                    if not ins.created_at.tzinfo:
                        ins.created_at = ins.created_at.replace(tzinfo=timezone.utc)
                    hours_since = (datetime.now(timezone.utc) - ins.created_at).total_seconds() / 3600.0
            
            hours_since = max(0.0, hours_since)
            alpha = 0.02
            recency_weight = exp(-alpha * hours_since)
            
            scores = vta.get('provenance', {}).get('scores')
            if scores:
                pq = scores.get('pq', 0)
                conv = vta.get('market_view', {}).get('conviction', 'MEDIUM')
                conv_map = {"VERY_HIGH": 1.0, "HIGH": 0.75, "MEDIUM": 0.5, "LOW": 0.25}
                conv_mult = conv_map.get(conv, 0.5)
                
                w = (pq / 100.0) * conv_mult * recency_weight
                vta['computed_weight'] = round(w, 4)
                vta['computed_recency_hours'] = round(hours_since, 1)
            
            insight_views.append(vta)
            source_video_ids.append(ins.video_id)

    if not insight_views:
        logger.info(
            "YouTube consensus skipped: recent=%d valid=0 rejected_failed_paused=%d rejected_no_scores=%d rejected_invalid_schema=%d",
            len(recent_insights),
            reject_stats["failed_paused"],
            reject_stats["no_scores"],
            reject_stats["invalid_schema"],
        )
        return

    logger.info(
        "YouTube consensus inputs: recent=%d valid=%d rejected_failed_paused=%d rejected_no_scores=%d rejected_invalid_schema=%d",
        len(recent_insights),
        len(insight_views),
        reject_stats["failed_paused"],
        reject_stats["no_scores"],
        reject_stats["invalid_schema"],
    )

    try:
        consensus_prompt = build_consensus_prompt(
            insights=insight_views,
            symbol=settings.youtube_target_symbol,
            lookback_hours=settings.youtube_consensus_lookback_hours,
        )

        consensus_messages = [
            {"role": "system", "content": YOUTUBE_CONSENSUS_SYSTEM_PROMPT},
            {"role": "user", "content": consensus_prompt},
        ]
        youtube_config = settings.resolve_llm_config("youtube")
        use_reasoning = youtube_config.use_reasoning.lower() == "true" or (
            youtube_config.use_reasoning.lower() == "auto" and provider.capabilities.supports_reasoning
        )
        provider_resp = await provider.generate_response(
            messages=consensus_messages,
            max_tokens=8192,
            temperature=0.3,
            response_format={"type": "json_object"},
            use_reasoning=use_reasoning,
        )

        from app.ai.analyst import _extract_json
        content = provider_resp.get("content", "")
        json_str = _extract_json(content)
        if json_str:
            consensus_data = _json.loads(json_str)
            with runtime.session_factory() as session:
                save_youtube_consensus(session, {
                    "symbol": settings.youtube_target_symbol,
                    "lookback_hours": settings.youtube_consensus_lookback_hours,
                    "consensus_json": consensus_data,
                    "source_video_ids": source_video_ids,
                })
            logger.info(
                "YouTube consensus generated: bias=%s, confidence=%s, sources=%d",
                consensus_data.get("consensus_bias"),
                consensus_data.get("confidence"),
                len(source_video_ids),
            )
        else:
            logger.warning("Could not extract JSON from consensus AI response")

    except Exception as exc:
        logger.error("YouTube consensus generation failed: %s", exc)
