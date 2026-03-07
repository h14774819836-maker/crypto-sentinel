"""AI analysis API routes: ai-analyze, ai-analyze/stream."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, is_dataclass, replace as dc_replace
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.ai.market_context_builder import build_market_analysis_context, sanitize_account_snapshot_for_ai
from app.config import LLMConfig, get_settings
from app.db.repository import (
    get_latest_futures_account_snapshot,
    get_latest_funding_snapshots,
    get_latest_intel_digest,
    get_latest_margin_account_snapshot,
    get_latest_market_metric,
    get_latest_ohlcv,
    get_recent_funding_snapshots_for_symbol,
    get_recent_youtube_insights,
    get_latest_youtube_consensus,
    insert_ai_signal,
    list_alerts,
    list_recent_ohlcv,
)
from app.db.session import SessionLocal, get_db
from app.logging import logger
from app.web.auth import require_admin
from app.web.shared import settings
from app.web.utils import _json_datetime, _liquidation_distance_pct, _to_float

# 与 run_ai.py 一致：大模型如 Kimi K2.5 / 豆包 首 token 较慢
_ANALYZE_TIMEOUT_SECONDS = 420.0
_AI_STREAM_ERROR_TTL_SECONDS = 900.0
_AI_STREAM_ERROR_LOCK = Lock()
_AI_STREAM_ERRORS: dict[tuple[str, str], dict[str, Any]] = {}


@dataclass(slots=True)
class MarketAIRequestOptions:
    requested_model: str | None
    effective_model: str
    llm_config: LLMConfig


def _json_dumps_safe(payload: Any) -> str:
    encoded = jsonable_encoder(
        payload,
        custom_encoder={
            datetime: lambda value: _json_datetime(value).isoformat() if _json_datetime(value) is not None else None,
        },
    )
    return json.dumps(encoded, ensure_ascii=False)


def _normalize_ai_stream_error_key_part(value: str | None, *, uppercase: bool = False) -> str:
    text = str(value or "").strip()
    if uppercase:
        text = text.upper()
    return text or "*"


def _prune_ai_stream_errors(now_ts: float | None = None) -> None:
    cutoff = float(now_ts if now_ts is not None else time.time()) - _AI_STREAM_ERROR_TTL_SECONDS
    expired = [
        key for key, payload in _AI_STREAM_ERRORS.items()
        if float(payload.get("recorded_at_epoch") or 0.0) < cutoff
    ]
    for key in expired:
        _AI_STREAM_ERRORS.pop(key, None)


def _record_ai_stream_error(
    *,
    model: str | None,
    symbol: str | None,
    error: str,
    phase: str,
) -> dict[str, Any]:
    payload = {
        "model": _normalize_ai_stream_error_key_part(model),
        "symbol": _normalize_ai_stream_error_key_part(symbol, uppercase=True),
        "error": str(error or "AI stream failed").strip()[:800] or "AI stream failed",
        "phase": str(phase or "stream").strip()[:80] or "stream",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "recorded_at_epoch": round(time.time(), 3),
    }
    key = (payload["model"], payload["symbol"])
    with _AI_STREAM_ERROR_LOCK:
        _prune_ai_stream_errors()
        _AI_STREAM_ERRORS[key] = payload
    return payload


def _clear_ai_stream_error(*, model: str | None, symbol: str | None) -> None:
    key = (
        _normalize_ai_stream_error_key_part(model),
        _normalize_ai_stream_error_key_part(symbol, uppercase=True),
    )
    with _AI_STREAM_ERROR_LOCK:
        _prune_ai_stream_errors()
        _AI_STREAM_ERRORS.pop(key, None)


def _get_ai_stream_error(*, model: str | None, symbol: str | None) -> dict[str, Any] | None:
    norm_model = _normalize_ai_stream_error_key_part(model)
    norm_symbol = _normalize_ai_stream_error_key_part(symbol, uppercase=True)
    keys = [
        (norm_model, norm_symbol),
        (norm_model, "*"),
        ("*", norm_symbol),
        ("*", "*"),
    ]
    with _AI_STREAM_ERROR_LOCK:
        _prune_ai_stream_errors()
        for key in keys:
            payload = _AI_STREAM_ERRORS.get(key)
            if payload is not None:
                return dict(payload)
    return None


def _clone_llm_config_with_model(config: LLMConfig, model: str) -> LLMConfig:
    """Clone config with request-scoped model override without mutating global config."""
    if getattr(config, "model", None) == model:
        return config
    if is_dataclass(config):
        return dc_replace(config, model=model)
    if hasattr(config, "model_copy"):
        return config.model_copy(update={"model": model})
    if hasattr(config, "copy"):
        try:
            return config.copy(update={"model": model})
        except TypeError:
            cloned = config.copy()
            setattr(cloned, "model", model)
            return cloned
    if hasattr(config, "__dict__"):
        data = dict(config.__dict__)
        data["model"] = model
        return type(config)(**data)
    raise TypeError(f"Unsupported LLM config type for model override: {type(config)!r}")


def _resolve_market_ai_request_options(model: str | None) -> MarketAIRequestOptions:
    from app.config import (
        _guess_provider_for_model,
        ARK_BASE_URL_DEFAULT,
        DEEPSEEK_BASE_URL_DEFAULT,
        NVIDIA_NIM_BASE_URL_DEFAULT,
        OPENROUTER_BASE_URL_DEFAULT,
    )

    requested_model = model.strip() if isinstance(model, str) else None
    if requested_model == "":
        requested_model = None

    if requested_model and requested_model not in settings.allowed_llm_models:
        allowed = ", ".join(sorted(settings.allowed_llm_models))
        raise HTTPException(status_code=400, detail=f"Unsupported model: {requested_model}. Allowed models: {allowed}")

    base_config = settings.resolve_llm_config("market")
    effective_model = requested_model or base_config.model

    target_provider = _guess_provider_for_model(effective_model)
    current_provider = (base_config.provider or "").strip().lower()

    if requested_model and target_provider != current_provider:
        provider_base_urls = {
            "deepseek": DEEPSEEK_BASE_URL_DEFAULT,
            "openrouter": OPENROUTER_BASE_URL_DEFAULT,
            "ark": ARK_BASE_URL_DEFAULT,
            "nvidia_nim": NVIDIA_NIM_BASE_URL_DEFAULT,
        }
        provider_api_keys = {
            "deepseek": settings.deepseek_api_key,
            "openrouter": settings.openrouter_api_key,
            "ark": settings.ark_api_key,
            "nvidia_nim": settings.nvidia_nim_api_key,
            "openai": settings.openai_api_key,
        }
        effective_config = LLMConfig(
            enabled=base_config.enabled,
            provider=target_provider,
            api_key=provider_api_keys.get(target_provider) or base_config.api_key or "",
            base_url=provider_base_urls.get(target_provider, base_config.base_url),
            model=effective_model,
            use_reasoning=base_config.use_reasoning,
            max_concurrency=base_config.max_concurrency,
            max_retries=base_config.max_retries,
            http_referer=base_config.http_referer,
            x_title=base_config.x_title,
            market_temperature=base_config.market_temperature,
        )
    else:
        effective_config = _clone_llm_config_with_model(base_config, effective_model)

    return MarketAIRequestOptions(
        requested_model=requested_model,
        effective_model=effective_model,
        llm_config=effective_config,
    )


def _build_recent_alerts_by_symbol(db: Session, limit: int = 50) -> dict[str, list[dict[str, Any]]]:
    recent_alerts_rows = list_alerts(db, limit=limit)
    out: dict[str, list[dict[str, Any]]] = {}
    for a in recent_alerts_rows:
        out.setdefault(a.symbol, [])
        if len(out[a.symbol]) >= 20:
            continue
        out[a.symbol].append(
            {
                "symbol": a.symbol,
                "alert_type": a.alert_type,
                "severity": a.severity,
                "reason": a.reason,
                "ts": a.ts,
            }
        )
    return out


def _build_funding_current_by_symbol(db: Session) -> dict[str, dict[str, Any]]:
    funding_rows = get_latest_funding_snapshots(db, symbols=settings.watchlist_symbols)
    return {
        f.symbol: {
            "symbol": f.symbol,
            "ts": f.ts,
            "mark_price": f.mark_price,
            "index_price": f.index_price,
            "last_funding_rate": f.last_funding_rate,
            "open_interest": f.open_interest,
            "open_interest_value": f.open_interest_value,
        }
        for f in funding_rows
    }


def _build_account_snapshot_context_for_ai(db: Session) -> dict[str, Any] | None:
    futures_row = get_latest_futures_account_snapshot(db)
    margin_row = get_latest_margin_account_snapshot(db)
    if futures_row is None and margin_row is None:
        return None
    futures_payload = {
        "total_margin_balance": _to_float(getattr(futures_row, "total_margin_balance", None)),
        "available_balance": _to_float(getattr(futures_row, "available_balance", None)),
        "total_maint_margin": _to_float(getattr(futures_row, "total_maint_margin", None)),
        "position_amt": _to_float(getattr(futures_row, "btc_position_amt", None)),
        "mark_price": _to_float(getattr(futures_row, "btc_mark_price", None)),
        "liquidation_price": _to_float(getattr(futures_row, "btc_liquidation_price", None)),
        "unrealized_pnl": _to_float(getattr(futures_row, "btc_unrealized_pnl", None)),
    }
    if (
        futures_payload["mark_price"] is not None
        and futures_payload["liquidation_price"] is not None
        and futures_payload["position_amt"] is not None
    ):
        futures_payload["liq_distance_pct"] = _liquidation_distance_pct(
            mark_price=float(futures_payload["mark_price"]),
            liq_price=float(futures_payload["liquidation_price"]),
            position_amt=float(futures_payload["position_amt"]),
        )
    margin_payload = {
        "margin_level": _to_float(getattr(margin_row, "margin_level", None)),
        "margin_call_bar": _to_float(getattr(margin_row, "margin_call_bar", None)),
        "force_liquidation_bar": _to_float(getattr(margin_row, "force_liquidation_bar", None)),
        "total_liability_of_btc": _to_float(getattr(margin_row, "total_liability_of_btc", None)),
    }
    min_balance = float(settings.account_alert_min_available_balance)
    available = futures_payload.get("available_balance")
    margin_level = margin_payload.get("margin_level")
    margin_call = margin_payload.get("margin_call_bar")
    as_of = None
    if futures_row is not None and futures_row.ts is not None:
        as_of = _json_datetime(futures_row.ts)
    if as_of is None and margin_row is not None and margin_row.ts is not None:
        as_of = _json_datetime(margin_row.ts)
    snapshot = {
        "watch_symbol": settings.account_watch_symbol.upper(),
        "as_of_utc": as_of,
        "futures": futures_payload,
        "margin": margin_payload,
        "risk_flags": {
            "available_balance_low": available is not None and available < min_balance,
            "margin_near_call": (
                margin_level is not None
                and margin_call is not None
                and margin_level <= margin_call
            ),
        },
    }
    return sanitize_account_snapshot_for_ai(
        snapshot,
        liq_distance_risk_threshold_pct=float(settings.account_alert_liq_distance_pct),
    )


def _build_market_ai_symbol_snapshots(db: Session, symbol: str) -> dict[str, Any]:
    tf_data: dict[str, Any] = {}
    all_tfs = ["1m"] + settings.multi_tf_interval_list
    for tf in all_tfs:
        latest = get_latest_market_metric(db, symbol=symbol, timeframe=tf)
        latest_candle = get_latest_ohlcv(db, symbol=symbol, timeframe=tf)
        if latest is None and latest_candle is None:
            continue

        recent_candles = list_recent_ohlcv(db, symbol=symbol, timeframe=tf, limit=settings.ai_history_candles)
        latest_ts = getattr(latest, "ts", None)
        latest_close = getattr(latest, "close", None)
        if latest_candle is not None and (
            latest_ts is None or (latest_candle.ts is not None and latest_candle.ts >= latest_ts)
        ):
            latest_ts = latest_candle.ts
            latest_close = latest_candle.close
        tf_data[tf] = {
            "latest": {
                "ts": latest_ts,
                "metric_ts": getattr(latest, "ts", None),
                "candle_ts": getattr(latest_candle, "ts", None),
                "close": latest_close,
                "ret_1m": getattr(latest, "ret_1m", None),
                "ret_10m": getattr(latest, "ret_10m", None),
                "rolling_vol_20": getattr(latest, "rolling_vol_20", None),
                "volume_zscore": getattr(latest, "volume_zscore", None),
                "rsi_14": getattr(latest, "rsi_14", None),
                "stoch_rsi_k": getattr(latest, "stoch_rsi_k", None),
                "stoch_rsi_d": getattr(latest, "stoch_rsi_d", None),
                "macd_hist": getattr(latest, "macd_hist", None),
                "bb_zscore": getattr(latest, "bb_zscore", None),
                "bb_bandwidth": getattr(latest, "bb_bandwidth", None),
                "atr_14": getattr(latest, "atr_14", None),
                "obv": getattr(latest, "obv", None),
                "ema_ribbon_trend": getattr(latest, "ema_ribbon_trend", None),
            },
            "history": [
                {"ts": c.ts, "close": c.close, "high": c.high, "low": c.low, "open": c.open}
                for c in recent_candles
            ],
        }
    return tf_data


async def _run_feature_refresh_with_catchup(
    *,
    runtime: Any,
    steps: list[dict[str, Any]],
    feature_job_fn: Any,
    max_runs: int = 8,
) -> dict[str, Any] | None:
    latest_result: dict[str, Any] | None = None
    for run_idx in range(max(1, int(max_runs or 1))):
        step_start = time.perf_counter()
        step_result = await feature_job_fn(runtime)
        latest_result = step_result if isinstance(step_result, dict) else None
        step_name = "feature" if run_idx == 0 else f"feature_catchup_{run_idx + 1}"
        step_payload: dict[str, Any] = {
            "step": step_name,
            "duration_ms": int((time.perf_counter() - step_start) * 1000),
        }
        if isinstance(step_result, dict):
            step_payload["result"] = step_result
        steps.append(step_payload)

        backlog = int((latest_result or {}).get("backlog") or 0)
        if backlog <= 0:
            break

    return latest_result


def _build_market_ai_symbol_inputs(
    db: Session,
    symbol: str,
    *,
    recent_alerts_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
    funding_current_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    snapshots = _build_market_ai_symbol_snapshots(db, symbol)
    if not snapshots:
        return {}, {}

    recent_alerts_by_symbol = recent_alerts_by_symbol or _build_recent_alerts_by_symbol(db, limit=60)
    funding_current_by_symbol = funding_current_by_symbol or _build_funding_current_by_symbol(db)

    funding_current = funding_current_by_symbol.get(symbol)
    funding_history = get_recent_funding_snapshots_for_symbol(db, symbol=symbol, limit=72)
    intel_digest_row = get_latest_intel_digest(
        db,
        symbol="GLOBAL",
        lookback_hours=settings.intel_digest_lookback_hours,
    )
    intel_digest_payload = intel_digest_row.digest_json if intel_digest_row and isinstance(intel_digest_row.digest_json, dict) else None

    youtube_consensus = None
    youtube_insights = []
    if settings.youtube_enabled and symbol == settings.youtube_target_symbol:
        youtube_consensus = get_latest_youtube_consensus(db, symbol=symbol)
        if youtube_consensus is not None:
            try:
                youtube_insights = get_recent_youtube_insights(
                    db,
                    lookback_hours=settings.youtube_consensus_lookback_hours,
                    symbol=symbol,
                )[:8]
            except Exception as exc:
                logger.warning("[AI分析] 拉取 YouTube insights 失败（symbol=%s）：%s", symbol, exc)

    account_snapshot = _build_account_snapshot_context_for_ai(db)
    context = build_market_analysis_context(
        symbol=symbol,
        snapshots=snapshots,
        recent_alerts=recent_alerts_by_symbol.get(symbol, []),
        funding_current=funding_current,
        funding_history=funding_history,
        youtube_consensus=youtube_consensus,
        youtube_insights=youtube_insights,
        intel_digest=intel_digest_payload,
        account_snapshot=account_snapshot,
        expected_timeframes=["4h", "1h", "15m", "5m", "1m"],
    )
    return snapshots, context


def _is_db_locked_error(exc: BaseException) -> bool:
    """Check if exception is SQLite 'database is locked' or similar."""
    msg = str(exc).lower()
    return "database is locked" in msg or "database_locked" in msg or "sqlite_busy" in msg


def _assess_market_data_freshness(db: Session, stale_seconds: int) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - max(1, int(stale_seconds))
    stale_symbols: list[str] = []
    missing_symbols: list[str] = []
    latest_ts: datetime | None = None

    for symbol in settings.watchlist_symbols:
        latest_ohlcv = get_latest_ohlcv(db, symbol=symbol, timeframe="1m")
        latest_metric = get_latest_market_metric(db, symbol=symbol, timeframe="1m")

        ts_candidates = [
            ts
            for ts in (
                getattr(latest_ohlcv, "ts", None),
                getattr(latest_metric, "ts", None),
            )
            if ts is not None
        ]
        if not ts_candidates:
            missing_symbols.append(symbol)
            continue

        symbol_latest = max(ts_candidates)
        if latest_ts is None or symbol_latest > latest_ts:
            latest_ts = symbol_latest
        if symbol_latest.timestamp() < cutoff:
            stale_symbols.append(symbol)

    return {
        "fresh": not missing_symbols and not stale_symbols,
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "latest_data_ts": latest_ts,
        "stale_seconds": int(stale_seconds),
    }


def _build_manual_preflight_lock_key() -> str:
    symbols_key = ",".join(sorted(settings.watchlist_symbols))
    return f"ai:manual-preflight:{symbols_key}"


async def _try_acquire_manual_preflight_lock(
    redis_url: str,
    *,
    lock_key: str,
    lock_seconds: int,
) -> tuple[bool, str | None, str | None]:
    from redis.asyncio import Redis
    from uuid import uuid4

    token = uuid4().hex
    client = Redis.from_url(redis_url)
    try:
        acquired = await client.set(lock_key, token, nx=True, ex=max(1, int(lock_seconds)))
        return bool(acquired), (token if acquired else None), None
    except Exception as exc:
        return False, None, str(exc)
    finally:
        await client.aclose()


async def _release_manual_preflight_lock(redis_url: str, *, lock_key: str, token: str | None) -> None:
    if not token:
        return
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        current = await client.get(lock_key)
        if isinstance(current, bytes):
            current = current.decode("utf-8")
        if current == token:
            await client.delete(lock_key)
    except Exception:
        return
    finally:
        await client.aclose()


def _refresh_report_to_http_exception(refresh_report: dict[str, Any]) -> HTTPException | None:
    code = str(refresh_report.get("code") or "").strip().lower()
    if code in {"core_data_not_ready", "preflight_redis_unavailable"}:
        return HTTPException(
            status_code=503,
            detail={
                "code": code,
                "message": refresh_report.get("error") or code,
                "refresh": refresh_report,
            },
        )
    return None


async def _run_refresh_steps(*, include_backfill: bool, mode: str) -> dict[str, Any]:
    from app.alerts.telegram import TelegramClient
    from app.providers.binance_provider import BinanceProvider
    from app.scheduler.jobs import (
        feature_job,
        funding_rate_job,
        gap_fill_job,
        multi_tf_sync_job,
        startup_backfill_job,
    )
    from app.scheduler.runtime import WorkerRuntime

    steps: list[dict[str, Any]] = []
    total_start = time.perf_counter()
    runtime = WorkerRuntime(
        settings=settings,
        session_factory=SessionLocal,
        provider=BinanceProvider(settings),
        telegram=TelegramClient(settings),
        started_at=datetime.now(timezone.utc),
        version=settings.app_version,
        sem_binance=asyncio.Semaphore(4),
    )

    max_retries = 3
    retry_delay = 2.0

    for attempt in range(max_retries):
        try:
            need_backfill = False
            if include_backfill:
                with SessionLocal() as check_db:
                    for symbol in settings.watchlist_symbols:
                        if get_latest_ohlcv(check_db, symbol=symbol, timeframe="1m") is None:
                            need_backfill = True
                            break

            if include_backfill and need_backfill:
                step_start = time.perf_counter()
                await startup_backfill_job(runtime)
                steps.append(
                    {
                        "step": "startup_backfill",
                        "duration_ms": int((time.perf_counter() - step_start) * 1000),
                    }
                )

            for step_name, step_coro in (
                ("gap_fill", gap_fill_job),
                ("multi_tf_sync", multi_tf_sync_job),
                ("funding_rate", funding_rate_job),
            ):
                step_start = time.perf_counter()
                step_result = await step_coro(runtime)
                step_payload: dict[str, Any] = {
                    "step": step_name,
                    "duration_ms": int((time.perf_counter() - step_start) * 1000),
                }
                if isinstance(step_result, dict):
                    step_payload["result"] = step_result
                steps.append(step_payload)

            await _run_feature_refresh_with_catchup(
                runtime=runtime,
                steps=steps,
                feature_job_fn=feature_job,
            )

            total_duration_ms = int((time.perf_counter() - total_start) * 1000)
            logger.info("[AI_ANALYZE] pre-refresh completed in %d ms mode=%s steps=%s", total_duration_ms, mode, steps)
            return {
                "ok": True,
                "duration_ms": total_duration_ms,
                "steps": steps,
                "preflight": "executed",
                "mode": mode,
            }
        except Exception as exc:
            total_duration_ms = int((time.perf_counter() - total_start) * 1000)
            if _is_db_locked_error(exc) and attempt < max_retries - 1:
                logger.warning(
                    "[AI_ANALYZE] pre-refresh db locked (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    max_retries,
                    retry_delay,
                    exc,
                )
                await asyncio.sleep(retry_delay)
                steps = []
                continue
            logger.exception("[AI_ANALYZE] pre-refresh failed after %d ms mode=%s: %s", total_duration_ms, mode, exc)
            return {
                "ok": False,
                "duration_ms": total_duration_ms,
                "steps": steps,
                "error": str(exc),
                "code": "pre_refresh_failed",
                "mode": mode,
            }


async def _refresh_market_data_before_ai_analysis() -> dict[str, Any]:
    """Run one on-demand ingest round before manual AI analysis."""
    mode = str(getattr(settings, "ai_manual_preflight_mode", "legacy") or "legacy").strip().lower()
    if mode != "stale_guarded":
        return await _run_refresh_steps(include_backfill=True, mode="legacy")

    stale_seconds = max(30, int(getattr(settings, "ai_manual_preflight_stale_seconds", 300) or 300))
    lock_seconds = max(10, int(getattr(settings, "ai_manual_preflight_lock_seconds", 120) or 120))

    with SessionLocal() as check_db:
        freshness = _assess_market_data_freshness(check_db, stale_seconds=stale_seconds)
    if freshness.get("fresh"):
        return {
            "ok": True,
            "preflight": "skipped_fresh",
            "mode": "stale_guarded",
            **freshness,
        }

    lock_key = _build_manual_preflight_lock_key()
    lock_acquired, lock_token, lock_error = await _try_acquire_manual_preflight_lock(
        settings.redis_url,
        lock_key=lock_key,
        lock_seconds=lock_seconds,
    )
    if lock_error:
        return {
            "ok": False,
            "code": "preflight_redis_unavailable",
            "error": f"Redis lock unavailable: {lock_error}",
            "mode": "stale_guarded",
            "preflight": "failed_redis",
        }
    if not lock_acquired:
        return {
            "ok": True,
            "preflight": "skipped_lock_held",
            "mode": "stale_guarded",
            **freshness,
        }

    try:
        ready_window = max(60, min(stale_seconds, 300))
        with SessionLocal() as ready_db:
            readiness = _assess_market_data_freshness(ready_db, stale_seconds=ready_window)
        if readiness.get("missing_symbols"):
            return {
                "ok": False,
                "code": "core_data_not_ready",
                "error": "core_data_not_ready",
                "mode": "stale_guarded",
                "preflight": "core_data_not_ready",
                "missing_symbols": readiness.get("missing_symbols"),
                "stale_symbols": readiness.get("stale_symbols"),
                "latest_data_ts": readiness.get("latest_data_ts"),
            }

        refresh_result = await _run_refresh_steps(include_backfill=False, mode="stale_guarded")
        refresh_result.setdefault("freshness_before", freshness)
        return refresh_result
    finally:
        await _release_manual_preflight_lock(settings.redis_url, lock_key=lock_key, token=lock_token)


router = APIRouter()


@router.post("/api/ai-analyze")
async def ai_analyze_now(
    model: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    dry_run: bool = Query(default=False),
    db: Session = Depends(get_db),
    _admin: str = Depends(require_admin),
):
    """Trigger an on-demand AI analysis and return the results."""
    request_opts = _resolve_market_ai_request_options(model)
    _clear_ai_stream_error(model=request_opts.effective_model, symbol=symbol)
    if dry_run:
        return {
            "ok": True,
            "model_requested": request_opts.requested_model,
            "model_effective": request_opts.effective_model,
        }
    market_config = request_opts.llm_config
    if not market_config.enabled or not market_config.api_key:
        return {"ok": False, "error": "市场分析 LLM 未启用或未配置 API Key（请在 LLM 调试页检查）"}

    refresh_report = await _refresh_market_data_before_ai_analysis()
    if not refresh_report.get("ok"):
        http_exc = _refresh_report_to_http_exception(refresh_report)
        if http_exc is not None:
            raise http_exc
        return {
            "ok": False,
            "error": f"Pre-analysis data refresh failed: {refresh_report.get('error') or 'unknown error'}",
            "refresh": refresh_report,
        }

    from app.ai.openai_provider import OpenAICompatibleProvider
    from app.ai.analyst import MarketAnalyst, attach_context_digest_to_analysis_json

    provider = OpenAICompatibleProvider(market_config)
    analyst = MarketAnalyst(settings, provider, market_config)

    recent_alerts_by_symbol = _build_recent_alerts_by_symbol(db, limit=60)
    funding_current_by_symbol = _build_funding_current_by_symbol(db)

    target_symbols = [symbol] if symbol and symbol.upper() in [s.upper() for s in settings.watchlist_symbols] else settings.watchlist_symbols
    _resolved_target_symbol = symbol.upper() if symbol else None
    if _resolved_target_symbol and _resolved_target_symbol not in target_symbols:
        target_symbols = [_resolved_target_symbol]

    symbol_inputs: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
        sym: _build_market_ai_symbol_inputs(
            db,
            sym,
            recent_alerts_by_symbol=recent_alerts_by_symbol,
            funding_current_by_symbol=funding_current_by_symbol,
        )
        for sym in target_symbols
    }
    available_symbols = [s for s, (snaps, _ctx) in symbol_inputs.items() if snaps]
    if not available_symbols:
        _record_ai_stream_error(
            model=request_opts.effective_model,
            symbol=symbol,
            error="暂无市场数据",
            phase="data",
        )
        return {"ok": False, "error": "暂无市场数据，请等待数据采集"}

    async def analyze_symbol(sym: str):
        symbol_snapshots, symbol_context = symbol_inputs.get(sym, ({}, {}))
        if not symbol_snapshots:
            return []
        from app.ai.analysis_flow import (
            build_scanner_hold_signal,
            prepare_context_and_snapshots,
            scanner_gate_passes,
        )

        two_stage = bool(getattr(settings, "ai_two_stage_enabled", True))
        scan_thresh = int(getattr(settings, "ai_scan_confidence_threshold", 60) or 60)
        gate_ok, skip_reason = scanner_gate_passes(
            symbol_context, two_stage_enabled=two_stage, scan_threshold=scan_thresh
        )
        if not gate_ok and skip_reason:
            return [build_scanner_hold_signal(sym, symbol_context, skip_reason)]

        symbol_context, symbol_snapshots = prepare_context_and_snapshots(
            symbol_context,
            symbol_snapshots,
            sym,
            min_context_on_poor_data=bool(getattr(settings, "ai_min_context_on_poor_data", True)),
            min_context_on_non_tradeable=bool(getattr(settings, "ai_min_context_on_non_tradeable", True)),
        )
        timeout_s = float(getattr(settings, "market_ai_stream_symbol_timeout_seconds", _ANALYZE_TIMEOUT_SECONDS) or _ANALYZE_TIMEOUT_SECONDS)
        model_l = (market_config.model or "").lower()
        if "kimi" in model_l or "k2.5" in model_l or "k2-" in model_l:
            timeout_s = max(timeout_s, 420.0)
        try:
            signals, _metadata = await asyncio.wait_for(
                analyst.analyze(sym, symbol_snapshots, context=symbol_context),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail=f"AI analysis timeout after {int(timeout_s)}s for symbol={sym}",
            )
        return signals

    tasks = [analyze_symbol(sym) for sym in available_symbols]
    results = await asyncio.gather(*tasks)

    signals = []
    for res in results:
        signals.extend(res)

    now = datetime.now(timezone.utc)
    items = []
    for sig in signals:
        symbol_context = symbol_inputs.get(sig.symbol, ({}, {}))[1]
        analysis_json_for_storage = attach_context_digest_to_analysis_json(sig.analysis_json, symbol_context)
        payload = {
            "symbol": sig.symbol,
            "timeframe": "1m",
            "ts": now,
            "direction": sig.direction,
            "entry_price": sig.entry_price,
            "take_profit": sig.take_profit,
            "stop_loss": sig.stop_loss,
            "confidence": sig.confidence,
            "reasoning": sig.reasoning,
            "analysis_json": analysis_json_for_storage,
            "model_requested": sig.model_requested or request_opts.effective_model,
            "model_name": sig.model_name,
            "prompt_tokens": sig.prompt_tokens,
            "completion_tokens": sig.completion_tokens,
        }
        insert_ai_signal(db, payload, commit=False)
        items.append({
            "symbol": sig.symbol,
            "direction": sig.direction,
            "entry_price": sig.entry_price,
            "take_profit": sig.take_profit,
            "stop_loss": sig.stop_loss,
            "confidence": sig.confidence,
            "reasoning": sig.reasoning,
            "analysis_json": analysis_json_for_storage,
            "model_requested": sig.model_requested or request_opts.effective_model,
            "model_name": sig.model_name,
        })
    db.commit()

    return {"ok": True, "count": len(items), "items": items, "refresh": refresh_report}


@router.get("/api/ai-analyze/stream")
async def ai_analyze_stream(
    model: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    _admin: str = Depends(require_admin),
):
    """Streaming endpoint for AI analysis using Server-Sent Events (SSE)."""
    try:
        return await _ai_analyze_stream_impl(model, symbol)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else _json_dumps_safe(exc.detail)
        _record_ai_stream_error(model=model, symbol=symbol, error=detail, phase="http")
        raise
    except Exception as e:
        logger.exception("ai_analyze_stream failed: %s", e)
        _record_ai_stream_error(model=model, symbol=symbol, error=str(e), phase="http")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/ai-analyze/last-error")
def ai_analyze_last_error(
    model: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    _admin: str = Depends(require_admin),
):
    item = _get_ai_stream_error(model=model, symbol=symbol)
    return {
        "ok": True,
        "error": item.get("error") if item else None,
        "item": item,
    }


async def _ai_analyze_stream_impl(
    model: str | None,
    symbol: str | None,
):
    """Inner implementation of ai_analyze_stream."""
    request_opts = _resolve_market_ai_request_options(model)
    _clear_ai_stream_error(model=request_opts.effective_model, symbol=symbol)
    market_config = request_opts.llm_config
    if not market_config.enabled or not market_config.api_key:
        _record_ai_stream_error(
            model=request_opts.effective_model,
            symbol=symbol,
            error="市场分析 LLM 未启用或未配置 API Key",
            phase="config",
        )
        async def err_gen():
            yield 'data: {"type": "error", "error": "市场分析 LLM 未启用或未配置 API Key"}\n\n'
        return StreamingResponse(err_gen(), media_type="text/event-stream")

    refresh_report = await _refresh_market_data_before_ai_analysis()
    if not refresh_report.get("ok"):
        http_exc = _refresh_report_to_http_exception(refresh_report)
        if http_exc is not None:
            detail = http_exc.detail if isinstance(http_exc.detail, str) else _json_dumps_safe(http_exc.detail)
            _record_ai_stream_error(
                model=request_opts.effective_model,
                symbol=symbol,
                error=detail,
                phase="refresh",
            )
            raise http_exc
        refresh_error_text = f"Pre-analysis data refresh failed: {refresh_report.get('error') or 'unknown error'}"
        _record_ai_stream_error(
            model=request_opts.effective_model,
            symbol=symbol,
            error=refresh_error_text,
            phase="refresh",
        )
        refresh_error = _json_dumps_safe(
            {
                "type": "error",
                "error": refresh_error_text,
                "code": refresh_report.get("code"),
                "refresh": refresh_report,
            }
        )

        async def err_gen():
            yield f"data: {refresh_error}\n\n"

        return StreamingResponse(err_gen(), media_type="text/event-stream")

    from app.ai.openai_provider import OpenAICompatibleProvider
    from app.ai.analyst import MarketAnalyst, attach_context_digest_to_analysis_json
    from app.ai.thinking_summarizer import ThinkingSummarizer
    from app.ai.prompts import THINKING_SUMMARY_PROMPT
    from app.ai.thinking_summary_utils import (
        extract_content_text,
        infer_stage_summary,
        refine_summary,
    )

    provider = OpenAICompatibleProvider(market_config)
    analyst = MarketAnalyst(settings, provider, market_config)
    thinking_summary_enabled = bool(getattr(settings, "ai_thinking_summary_enabled", True))
    fast_provider = None
    if thinking_summary_enabled:
        try:
            fast_config = settings.resolve_llm_config(
                str(getattr(settings, "ai_thinking_summary_profile", "thinking_summary"))
            )
            if fast_config.enabled and fast_config.api_key:
                fast_provider = OpenAICompatibleProvider(fast_config)
            else:
                logger.info(
                    "Thinking summary disabled: profile=%s enabled=%s has_api_key=%s",
                    getattr(settings, "ai_thinking_summary_profile", "thinking_summary"),
                    fast_config.enabled if fast_config else None,
                    bool(fast_config.api_key) if fast_config else False,
                )
        except Exception as e:
            logger.warning("Thinking summary provider not available: %s", e)
            thinking_summary_enabled = False

    if thinking_summary_enabled and fast_provider is not None:
        logger.info(
            "Thinking summary enabled: profile=%s, model=%s",
            getattr(settings, "ai_thinking_summary_profile", "thinking_summary"),
            getattr(fast_provider, "model", "?"),
        )

    with SessionLocal() as preload_db:
        recent_alerts_by_symbol = _build_recent_alerts_by_symbol(preload_db, limit=60)
        funding_current_by_symbol = _build_funding_current_by_symbol(preload_db)

        target_symbols = [symbol] if symbol and symbol.upper() in [s.upper() for s in settings.watchlist_symbols] else settings.watchlist_symbols
        _resolved_target_symbol = symbol.upper() if symbol else None
        if _resolved_target_symbol and _resolved_target_symbol not in target_symbols:
            target_symbols = [_resolved_target_symbol]

        symbol_inputs: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
            sym: _build_market_ai_symbol_inputs(
                preload_db,
                sym,
                recent_alerts_by_symbol=recent_alerts_by_symbol,
                funding_current_by_symbol=funding_current_by_symbol,
            )
            for sym in target_symbols
        }
    available_symbols = [s for s, (snaps, _ctx) in symbol_inputs.items() if snaps]
    if not available_symbols:
        _record_ai_stream_error(
            model=request_opts.effective_model,
            symbol=symbol,
            error="暂无市场数据",
            phase="data",
        )
        async def err_gen():
            yield 'data: {"type": "error", "error": "暂无市场数据"}\n\n'
        return StreamingResponse(err_gen(), media_type="text/event-stream")

    queue: asyncio.Queue = asyncio.Queue()
    cancel_event = asyncio.Event()

    async def _summarize_and_push(
        symbol: str,
        summarizer: ThinkingSummarizer,
        buffer_content: str,
        q: asyncio.Queue,
        fast_prov: OpenAICompatibleProvider,
        cancel: asyncio.Event,
        summaries_pushed_ref: list,
    ) -> None:
        if cancel.is_set():
            return
        try:
            is_first = summaries_pushed_ref[0] == 0
            max_chars = 50 if is_first else 300
            buf = buffer_content[-max_chars:] if len(buffer_content) > max_chars else buffer_content
            if len(buf.strip()) < 15:
                return
            content = THINKING_SUMMARY_PROMPT.format(buffer_content=buf)
            resp = await fast_prov.generate_response(
                messages=[{"role": "user", "content": content}],
                max_tokens=128,
                temperature=0.3,
                use_reasoning=False,
            )
            summary = extract_content_text(resp)
            if not summary:
                summary = infer_stage_summary(buffer_content)
            else:
                summary = refine_summary(summary, buffer_content) or summary
            if not summary:
                buf = (buffer_content or "").strip()
                for sep in ("。", "？", "！"):
                    parts = buf.split(sep)
                    if len(parts) >= 2:
                        last_sent = parts[-2].strip()
                        if len(last_sent) >= 10:
                            summary = (last_sent[:47] + "…") if len(last_sent) > 50 else last_sent
                            break
                if not summary:
                    summary = (buf[-60:].strip()[:47] + "…") if len(buf) > 50 else (buf[:47] or "思考中")
                if not summary or len(summary) < 8:
                    return
            if len(summary) < 8:
                return
            if summarizer.is_duplicate(summary):
                return
            summarizer.set_last_summary(summary)
            if cancel.is_set():
                return
            summaries_pushed_ref[0] += 1
            await q.put({"type": "thinking_state", "symbol": symbol, "content": summary})
            logger.info("Thinking summary pushed: symbol=%s content=%r", symbol, summary[:50] if summary else "")
        except Exception as e:
            logger.warning("Thinking summary failed (symbol=%s): %s", symbol, e)

    async def analyze_symbol_stream(sym: str):
        symbol_snapshots, symbol_context = symbol_inputs.get(sym, ({}, {}))
        if not symbol_snapshots:
            return []
        from app.ai.analysis_flow import (
            build_scanner_hold_signal,
            prepare_context_and_snapshots,
            scanner_gate_passes,
        )

        two_stage = bool(getattr(settings, "ai_two_stage_enabled", True))
        scan_thresh = int(getattr(settings, "ai_scan_confidence_threshold", 60) or 60)
        gate_ok, skip_reason = scanner_gate_passes(
            symbol_context, two_stage_enabled=two_stage, scan_threshold=scan_thresh
        )
        if not gate_ok and skip_reason:
            return [build_scanner_hold_signal(sym, symbol_context, skip_reason)]

        symbol_context, symbol_snapshots = prepare_context_and_snapshots(
            symbol_context,
            symbol_snapshots,
            sym,
            min_context_on_poor_data=bool(getattr(settings, "ai_min_context_on_poor_data", True)),
            min_context_on_non_tradeable=bool(getattr(settings, "ai_min_context_on_non_tradeable", True)),
        )

        await queue.put({"type": "data_sent", "symbol": sym, "message": "市场数据和提示词已发送，等待 AI 响应..."})
        use_typed = thinking_summary_enabled and fast_provider is not None
        summarizer = ThinkingSummarizer(
            min_chars=int(getattr(settings, "ai_thinking_summary_min_chars", 100) or 100),
            min_chars_first=int(getattr(settings, "ai_thinking_summary_min_chars_first", 30) or 30),
            min_interval_sec=float(getattr(settings, "ai_thinking_summary_interval_sec", 6.0) or 6.0),
        ) if use_typed else None
        max_streaming_summaries = int(getattr(settings, "ai_thinking_summary_max_streaming", 15) or 15)
        _summaries_pushed = [0]
        _reasoning_chunk_count = [0]
        _summary_task: list[asyncio.Task | None] = [None]

        async def cb_typed(chunk_type: str, text: str):
            if cancel_event.is_set():
                return
            if chunk_type == "reasoning" and summarizer is not None and fast_provider is not None:
                if _summaries_pushed[0] >= max_streaming_summaries:
                    return
                _reasoning_chunk_count[0] += 1
                triggered = summarizer.add_reasoning(text)
                if triggered:
                    if _reasoning_chunk_count[0] == 1:
                        await queue.put({"type": "thinking_state", "symbol": sym, "content": "思考中..."})
                    buf = summarizer.get_buffer_for_summary()
                    running = _summary_task[0]
                    if buf and (running is None or running.done()):
                        _summary_task[0] = asyncio.create_task(
                            _summarize_and_push(sym, summarizer, buf, queue, fast_provider, cancel_event, _summaries_pushed)
                        )
                return
            if chunk_type == "content":
                await queue.put({"type": "chunk", "symbol": sym, "text": text})

        async def cb_legacy(text: str):
            if cancel_event.is_set():
                return
            await queue.put({"type": "chunk", "symbol": sym, "text": text})

        cb = cb_typed if use_typed else cb_legacy

        timeout_s = float(getattr(settings, "market_ai_stream_symbol_timeout_seconds", _ANALYZE_TIMEOUT_SECONDS) or _ANALYZE_TIMEOUT_SECONDS)
        model_l = (market_config.model or "").lower()
        if "kimi" in model_l or "k2.5" in model_l or "k2-" in model_l:
            timeout_s = max(timeout_s, 420.0)
        try:
            signals, _metadata = await asyncio.wait_for(
                analyst.analyze(
                    sym,
                    symbol_snapshots,
                    context=symbol_context,
                    stream_callback=cb,
                    stream_callback_typed=use_typed,
                ),
                timeout=timeout_s,
            )
            if _summary_task[0] and not _summary_task[0].done():
                await _summary_task[0]
        except asyncio.TimeoutError as exc:
            raise RuntimeError(f"AI analysis timeout after {int(timeout_s)}s for symbol={sym}") from exc
        return signals

    async def worker():
        try:
            for sym in available_symbols:
                if cancel_event.is_set():
                    raise asyncio.CancelledError()
                signals = await analyze_symbol_stream(sym)
                if cancel_event.is_set():
                    raise asyncio.CancelledError()

                with SessionLocal() as write_db:
                    now = datetime.now(timezone.utc)
                    try:
                        for sig in signals:
                            symbol_context = symbol_inputs.get(sig.symbol, ({}, {}))[1]
                            analysis_json_for_storage = attach_context_digest_to_analysis_json(sig.analysis_json, symbol_context)
                            payload = {
                                "symbol": sig.symbol,
                                "timeframe": "1m",
                                "ts": now,
                                "direction": sig.direction,
                                "entry_price": sig.entry_price,
                                "take_profit": sig.take_profit,
                                "stop_loss": sig.stop_loss,
                                "confidence": sig.confidence,
                                "reasoning": sig.reasoning,
                                "analysis_json": analysis_json_for_storage,
                                "model_requested": sig.model_requested or request_opts.effective_model,
                                "model_name": sig.model_name,
                                "prompt_tokens": sig.prompt_tokens,
                                "completion_tokens": sig.completion_tokens,
                            }
                            insert_ai_signal(write_db, payload, commit=False)
                        if cancel_event.is_set():
                            write_db.rollback()
                            raise asyncio.CancelledError()
                        write_db.commit()
                    except asyncio.CancelledError:
                        write_db.rollback()
                        raise
                    except Exception:
                        write_db.rollback()
                        raise

                if cancel_event.is_set():
                    raise asyncio.CancelledError()

                await queue.put({
                    "type": "symbol_done",
                    "symbol": sym,
                    "signals": [
                        {
                            "symbol": sig.symbol,
                            "direction": sig.direction,
                            "entry_price": sig.entry_price,
                            "take_profit": sig.take_profit,
                            "stop_loss": sig.stop_loss,
                            "confidence": sig.confidence,
                            "reasoning": sig.reasoning,
                            "analysis_json": attach_context_digest_to_analysis_json(
                                sig.analysis_json,
                                symbol_inputs.get(sig.symbol, ({}, {}))[1],
                            ),
                        }
                        for sig in signals
                    ]
                })

            _clear_ai_stream_error(model=request_opts.effective_model, symbol=symbol)
            await queue.put({"type": "done", "count": len(available_symbols), "refresh": refresh_report})
        except asyncio.CancelledError:
            logger.info("ai_analyze_stream worker cancelled")
        except Exception as e:
            logger.exception("ai_analyze_stream worker error: %s", e)
            error_text = f"Analysis Error: {str(e)}"
            _record_ai_stream_error(
                model=request_opts.effective_model,
                symbol=symbol,
                error=error_text,
                phase="worker",
            )
            await queue.put({"type": "error", "error": error_text})

    worker_task = asyncio.create_task(worker())

    async def event_generator():
        try:
            # Emit an initial SSE comment immediately so clients can enter OPEN
            # state even when upstream LLM takes long before first chunk.
            yield ": connected\n\n"
            last_heartbeat = time.monotonic()
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    now_mono = time.monotonic()
                    if now_mono - last_heartbeat >= 2.0:
                        yield ": heartbeat\n\n"
                        last_heartbeat = now_mono
                    continue

                yield f"data: {_json_dumps_safe(msg)}\n\n"
                if msg["type"] in ("done", "error"):
                    break
        finally:
            cancel_event.set()
            if not worker_task.done():
                worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )
