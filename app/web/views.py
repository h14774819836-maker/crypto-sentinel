from __future__ import annotations

import asyncio
import sqlite3
import time
import json
import httpx
from dataclasses import dataclass, is_dataclass, replace as dc_replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app.config import LLMConfig, get_settings
from app.ai.market_context_builder import build_market_analysis_context
from app.ai.llm_runtime_reload import (
    apply_llm_config_in_api_process,
    read_llm_reload_ack,
    read_llm_reload_acks_redis,
    read_llm_reload_signal,
    refresh_llm_env_vars_from_dotenv,
    write_llm_reload_signal,
)
from app.db.models import YoutubeVideo
from app.db.repository import (
    get_strategy_decision_detail,
    list_account_stats_daily,
    get_latest_ai_signals,
    get_latest_futures_account_snapshot,
    get_latest_intel_digest,
    get_latest_margin_account_snapshot,
    get_latest_market_metrics,
    get_latest_ohlcv,
    get_recent_funding_snapshots_for_symbol,
    get_recent_youtube_insights,
    get_latest_youtube_consensus,
    get_worker_last_seen,
    insert_ai_signal,
    list_ohlcv_range,
    list_ai_signals,
    list_ai_analysis_failures,
    list_alerts,
    list_strategy_decisions_densified,
    list_strategy_decisions_raw,
    list_strategy_feature_stats,
    list_strategy_scores,
    list_recent_ohlcv,
    list_news_items,
    list_youtube_channels,
    list_youtube_videos,
)
from app.db.session import SessionLocal, get_db
from app.logging import logger
from app.ops.job_metrics import read_job_metrics_from_file
from app.services.health_probe import quick_db_health_and_worker
from app.web.auth import require_admin

router = APIRouter()
settings = get_settings()

# ---------- ASR concurrency limiter ----------
_asr_semaphore: asyncio.Semaphore | None = None


def _get_asr_semaphore() -> asyncio.Semaphore:
    global _asr_semaphore
    if _asr_semaphore is None:
        _asr_semaphore = asyncio.Semaphore(settings.asr_max_concurrent)
    return _asr_semaphore


TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

def format_bj_time(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if not dt:
        return "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    bj_dt = dt.astimezone(timezone(timedelta(hours=8)))
    return bj_dt.strftime(fmt)

templates.env.filters["bj_time"] = format_bj_time
YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT = 420
YT_ANALYSIS_STALL_WAITING_SECONDS_MIN = 420


def _epoch_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _datetime_from_epoch(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _json_datetime(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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

YOUTUBE_QUEUE_REASON_LABELS: dict[str, str] = {
    "done": "完成",
    "queued_wait_slot": "等待队列位",
    "pending_analysis": "等待分析",
    "retry_wait": "等待自动重试",
    "running_active": "分析中",
    "pending_subtitle": "等待字幕",
    "queued_asr": "ASR排队中",
    "auth_failed": "认证失败",
    "provider_rate_limit": "供应商限流",
    "provider_timeout": "供应商超时",
    "provider_bad_request": "供应商请求错误",
    "schema_failed": "结构校验失败",
    "asr_failed": "ASR失败",
    "worker_offline": "工作进程离线",
    "stalled_timeout": "超时卡住",
    "failed_paused": "失败并已暂停",
    "blocked_unknown": "已阻止",
}

YOUTUBE_ERROR_ACTIONS: dict[str, str] = {
    "provider_auth": "update_api_key",
    "provider_rate_limit": "wait_auto_retry",
    "provider_timeout": "wait_auto_retry",
    "provider_bad_request": "manual_retry",
    "schema": "manual_retry",
    "runtime": "check_worker_online",
    "asr": "manual_retry",
    "config": "update_api_key",
}


def _yt_is_valid_vta_for_consensus(vta: Any) -> bool:
    if not isinstance(vta, dict):
        return False
    if not vta.get("vta_version"):
        return False
    prov = vta.get("provenance") or {}
    if (prov.get("status") or "").lower() in {"failed_paused", "processing"}:
        return False
    if prov.get("analysis_error"):
        return False
    mv = vta.get("market_view")
    if not isinstance(mv, dict) or (mv.get("bias_1_7d") is None and mv.get("bias_1_4w") is None):
        return False
    if not isinstance(vta.get("levels"), dict):
        return False
    scores = prov.get("scores")
    if not isinstance(scores, dict) or scores.get("pq") is None:
        return False
    return True


def _yt_analysis_error_summary(vta: Any) -> str | None:
    if not isinstance(vta, dict):
        return None
    prov = vta.get("provenance") or {}
    err = prov.get("analysis_error") or {}
    if not isinstance(err, dict):
        return None
    status_code = err.get("status_code")
    msg = (err.get("message") or "").strip()
    if status_code and msg:
        return f"{status_code}: {msg[:120]}"
    if msg:
        return msg[:120]
    return status_code


def _to_utc_or_none(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _youtube_stall_thresholds() -> tuple[int, int]:
    running_threshold = max(
        int(getattr(settings, "youtube_analysis_stall_running_seconds", YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT) or YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT),
        60,
    )
    waiting_threshold = max(
        int(getattr(settings, "youtube_analysis_stall_waiting_seconds_effective", YT_ANALYSIS_STALL_WAITING_SECONDS_MIN) or YT_ANALYSIS_STALL_WAITING_SECONDS_MIN),
        YT_ANALYSIS_STALL_WAITING_SECONDS_MIN,
    )
    return running_threshold, waiting_threshold


def _youtube_error_detail(
    *,
    error_type: str | None,
    error_code: str | None,
    error_message: str | None,
) -> dict[str, Any] | None:
    type_norm = str(error_type or "").strip().lower()
    code_norm = str(error_code or "").strip().lower()
    message = (error_message or "").strip()
    if not (type_norm or code_norm or message):
        return None

    category = "runtime"
    retryable = False
    if type_norm in {"auth", "provider_auth"} or code_norm in {"auth", "401", "authentication_error", "invalid_api_key"}:
        category = "provider_auth"
        retryable = False
    elif type_norm in {"rate_limit"} or code_norm in {"429", "rate_limit"}:
        category = "provider_rate_limit"
        retryable = True
    elif type_norm in {"timeout"} or code_norm in {"timeout"}:
        category = "provider_timeout"
        retryable = True
    elif type_norm in {"bad_request"} or code_norm in {"bad_request"}:
        category = "provider_bad_request"
        retryable = False
    elif type_norm in {"schema_error", "scoring_error", "parse_error", "no_json"} or code_norm in {"schema", "scoring", "parse", "no_json"}:
        category = "schema"
        retryable = type_norm in {"parse_error", "no_json"} or code_norm in {"parse", "no_json"}
    elif type_norm in {"asr"} or code_norm in {"asr"}:
        category = "asr"
        retryable = False
    elif type_norm in {"config"} or code_norm in {"config"}:
        category = "config"
        retryable = False

    return {
        "category": category,
        "code": code_norm or type_norm or "unknown",
        "message": message[:500] if message else "",
        "retryable": retryable,
        "suggested_action": YOUTUBE_ERROR_ACTIONS.get(category, "manual_retry"),
    }


def _youtube_queue_reason_label(reason_code: str) -> str:
    return YOUTUBE_QUEUE_REASON_LABELS.get(reason_code, reason_code.replace("_", " "))


def _derive_youtube_queue_state(
    *,
    transcript_status: str,
    analysis_status: str,
    analysis_stage: str | None,
    analysis_stalled: bool,
    analysis_stalled_reason: str | None,
    worker_online: bool | None,
    error_detail: dict[str, Any] | None,
) -> tuple[str, str]:
    stage_norm = str(analysis_stage or "").strip().lower()

    if analysis_status == "done":
        return "done", "done"

    if transcript_status == "asr_failed_paused":
        return "blocked", "asr_failed"
    if transcript_status in {"pending_subtitle", "queued_asr"}:
        return "waiting", transcript_status

    if worker_online is False and analysis_status in {"pending", "queued", "running"}:
        return "blocked", "worker_offline"

    if analysis_stalled:
        return "blocked", "worker_offline" if analysis_stalled_reason == "worker_offline" else "stalled_timeout"

    if analysis_status == "failed_paused":
        category = str((error_detail or {}).get("category") or "").strip().lower()
        if category == "provider_auth":
            return "blocked", "auth_failed"
        if category == "provider_rate_limit":
            return "blocked", "provider_rate_limit"
        if category == "provider_timeout":
            return "blocked", "provider_timeout"
        if category == "provider_bad_request":
            return "blocked", "provider_bad_request"
        if category == "schema":
            return "blocked", "schema_failed"
        if category == "asr":
            return "blocked", "asr_failed"
        return "blocked", "failed_paused"

    if analysis_status == "running":
        return "running", "running_active"

    if analysis_status == "queued":
        if stage_norm == "retry_wait":
            return "waiting", "retry_wait"
        return "waiting", "queued_wait_slot"

    return "waiting", "pending_analysis"


def _derive_youtube_statuses(
    video: Any,
    insight_json: Any,
    *,
    now_utc: datetime | None = None,
    worker_online: bool | None = None,
) -> dict[str, Any]:
    transcript_text = getattr(video, "transcript_text", None)
    analysis_runtime_status = str(getattr(video, "analysis_runtime_status", "") or "").lower()
    analysis_stage = str(getattr(video, "analysis_stage", "") or "").lower() or None
    analysis_started_at = _to_utc_or_none(getattr(video, "analysis_started_at", None))
    analysis_updated_at = _to_utc_or_none(getattr(video, "analysis_updated_at", None))
    analysis_finished_at = _to_utc_or_none(getattr(video, "analysis_finished_at", None))
    analysis_retry_count = int(getattr(video, "analysis_retry_count", 0) or 0)
    analysis_next_retry_at = _to_utc_or_none(getattr(video, "analysis_next_retry_at", None))
    analysis_last_error_type = getattr(video, "analysis_last_error_type", None)
    analysis_last_error_code = getattr(video, "analysis_last_error_code", None)
    analysis_last_error_message = getattr(video, "analysis_last_error_message", None)
    now_ts = now_utc or datetime.now(timezone.utc)
    running_stall_threshold, waiting_stall_threshold = _youtube_stall_thresholds()

    # Use explicit status first (FSM)
    status = str(getattr(video, "status", "") or "").lower()
    if status == "pending_subtitle":
        transcript_status = "pending_subtitle"
        analysis_status = "pending"
    elif status == "queued_asr":
        transcript_status = "queued_asr"
        analysis_status = "pending"
    elif status == "asr_failed":
        transcript_status = "asr_failed_paused"
        analysis_status = "pending"
    elif status == "pending_analysis":
        transcript_status = "transcribed"
        analysis_status = "pending"
    elif status == "analyzing":
        transcript_status = "transcribed"
        analysis_status = analysis_runtime_status if analysis_runtime_status in {"queued", "running"} else "queued"
    elif status == "completed":
        transcript_status = "transcribed"
        analysis_status = "done"
    elif status == "failed":
        transcript_status = "transcribed"
        analysis_status = "failed_paused"
    else:
        # Fallback for legacy rows without status
        needs_asr = bool(getattr(video, "needs_asr", False))
        asr_processed_at = getattr(video, "asr_processed_at", None)
        last_error = getattr(video, "last_error", None)
        if transcript_text:
            transcript_status = "transcribed"
        elif needs_asr:
            transcript_status = "asr_failed_paused" if (asr_processed_at and last_error) else "queued_asr"
        else:
            transcript_status = "pending_subtitle"
        insight_available = isinstance(insight_json, dict)
        insight_valid = _yt_is_valid_vta_for_consensus(insight_json) if insight_available else False
        provenance = (insight_json.get("provenance") or {}) if insight_available else {}
        prov_status = (provenance.get("status") or "").lower()
        if insight_valid:
            analysis_status = "done"
        elif transcript_text and analysis_runtime_status in {"queued", "running"}:
            analysis_status = analysis_runtime_status
        elif transcript_text and analysis_runtime_status in {"failed_paused", "failed"}:
            analysis_status = "failed_paused"
        elif prov_status == "processing":
            analysis_status = "running"
        elif insight_available and (
            prov_status == "failed_paused"
            or provenance.get("analysis_error")
            or provenance.get("schema_errors")
            or provenance.get("scores") is None
        ):
            analysis_status = "failed_paused"
        else:
            analysis_status = "pending"

    insight_available = isinstance(insight_json, dict)
    insight_valid = _yt_is_valid_vta_for_consensus(insight_json) if insight_available else False

    elapsed_end = analysis_finished_at or now_ts
    analysis_elapsed_seconds = None
    if analysis_started_at is not None:
        analysis_elapsed_seconds = max(0, int((elapsed_end - analysis_started_at).total_seconds()))

    analysis_retry_eta_seconds = None
    if analysis_next_retry_at is not None:
        analysis_retry_eta_seconds = max(0, int((analysis_next_retry_at - now_ts).total_seconds()))
    analysis_retry_scheduled = (
        analysis_stage == "retry_wait"
        and analysis_status in {"queued", "running"}
        and analysis_retry_eta_seconds is not None
        and analysis_retry_eta_seconds > 0
    )

    analysis_stalled = False
    analysis_stalled_reason = None
    if analysis_status in {"queued", "running"} and analysis_stage != "retry_wait":
        ref_dt = analysis_updated_at or analysis_started_at
        if worker_online is False:
            analysis_stalled = True
            analysis_stalled_reason = "worker_offline"
        elif ref_dt is not None:
            age_seconds = (now_ts - ref_dt).total_seconds()
            threshold = running_stall_threshold if analysis_status == "running" else waiting_stall_threshold
            if age_seconds >= threshold:
                analysis_stalled = True
                analysis_stalled_reason = "stage_timeout"

    runtime_error_summary = None
    if analysis_last_error_code and analysis_last_error_message:
        runtime_error_summary = f"{analysis_last_error_code}: {analysis_last_error_message[:120]}"
    elif analysis_last_error_message:
        runtime_error_summary = str(analysis_last_error_message)[:120]
    elif analysis_last_error_code:
        runtime_error_summary = str(analysis_last_error_code)
    analysis_error_summary = _yt_analysis_error_summary(insight_json) or runtime_error_summary

    insight_error = None
    if isinstance(insight_json, dict):
        prov = insight_json.get("provenance") or {}
        err = prov.get("analysis_error") if isinstance(prov, dict) else None
        if isinstance(err, dict):
            insight_error = {
                "type": err.get("type"),
                "status_code": err.get("status_code"),
                "message": err.get("message"),
            }
    error_detail = _youtube_error_detail(
        error_type=(insight_error or {}).get("type") or analysis_last_error_type,
        error_code=(insight_error or {}).get("status_code") or analysis_last_error_code,
        error_message=(insight_error or {}).get("message") or analysis_last_error_message or analysis_error_summary,
    )

    queue_state, queue_reason_code = _derive_youtube_queue_state(
        transcript_status=transcript_status,
        analysis_status=analysis_status,
        analysis_stage=analysis_stage,
        analysis_stalled=analysis_stalled,
        analysis_stalled_reason=analysis_stalled_reason,
        worker_online=worker_online,
        error_detail=error_detail,
    )
    queue_reason_label = _youtube_queue_reason_label(queue_reason_code)

    status_notes = None
    if queue_state == "blocked" and queue_reason_code == "worker_offline":
        status_notes = "工作进程离线；队列已阻塞"
    elif queue_state == "blocked" and queue_reason_code == "stalled_timeout":
        status_notes = "处理超时卡住；需要手动重试"
    elif queue_state == "blocked" and queue_reason_code == "auth_failed":
        status_notes = "API Key 无效；请更新凭据，系统将自动重新排队"
    elif queue_state == "blocked" and analysis_status == "failed_paused":
        status_notes = "AI 分析失败；需要手动重试"
    elif analysis_status == "running":
        status_notes = f"正在分析 - {analysis_stage}" if analysis_stage else "正在分析"
    elif analysis_status == "queued":
        if analysis_stage == "retry_wait" and analysis_retry_eta_seconds is not None:
            status_notes = f"{analysis_retry_eta_seconds}秒后自动重试"
        else:
            status_notes = "排队等待 AI 分析"
    elif transcript_status == "asr_failed_paused":
        status_notes = "ASR 失败；需要手动重试"
    elif transcript_status == "queued_asr":
        status_notes = "ASR 排队中"
    elif transcript_status == "pending_subtitle":
        status_notes = "正在等待字幕"
    elif transcript_status == "transcribed" and analysis_status == "pending":
        status_notes = "文稿已就绪，等待 AI 分析"

    return {
        "status": getattr(video, "status", None),
        "status_updated_at": _to_utc_or_none(getattr(video, "status_updated_at", None)),
        "transcript_status": transcript_status,
        "analysis_status": analysis_status,
        "analysis_stage": analysis_stage,
        "analysis_started_at": analysis_started_at,
        "analysis_updated_at": analysis_updated_at,
        "analysis_finished_at": analysis_finished_at,
        "analysis_elapsed_seconds": analysis_elapsed_seconds,
        "analysis_retry_count": analysis_retry_count,
        "analysis_next_retry_at": analysis_next_retry_at,
        "analysis_retry_eta_seconds": analysis_retry_eta_seconds,
        "analysis_retry_scheduled": analysis_retry_scheduled,
        "analysis_last_error_type": analysis_last_error_type,
        "analysis_last_error_code": analysis_last_error_code,
        "analysis_last_error_message": analysis_last_error_message,
        "analysis_stalled": analysis_stalled,
        "analysis_stalled_reason": analysis_stalled_reason,
        "analysis_error_summary": analysis_error_summary,
        "error_detail": error_detail,
        "queue_state": queue_state,
        "queue_reason_code": queue_reason_code,
        "queue_reason_label": queue_reason_label,
        "insight_available": insight_available,
        "insight_valid": insight_valid,
        "status_notes": status_notes,
    }


def _youtube_queue_summary_from_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "waiting": 0,
        "running": 0,
        "blocked": 0,
        "done": 0,
        "total": 0,
        "blocked_breakdown": {},
    }
    blocked_breakdown: dict[str, int] = {}
    for item in items:
        state = str(item.get("queue_state") or "").strip().lower()
        if state not in {"waiting", "running", "blocked", "done"}:
            state = "waiting"
        summary[state] += 1
        summary["total"] += 1
        if state == "blocked":
            reason = str(item.get("queue_reason_code") or "blocked_unknown")
            blocked_breakdown[reason] = blocked_breakdown.get(reason, 0) + 1
    summary["blocked_breakdown"] = dict(sorted(blocked_breakdown.items(), key=lambda kv: kv[1], reverse=True))
    return summary


def _youtube_worker_status_snapshot(db: Session, now_utc: datetime | None = None) -> dict[str, Any]:
    now_ts = now_utc or datetime.now(timezone.utc)
    worker_id = getattr(settings, "worker_id", None)
    last_seen = get_worker_last_seen(db, worker_id=worker_id)
    last_seen_utc = _to_utc_or_none(last_seen)
    offline_threshold = max(int(getattr(settings, "worker_heartbeat_seconds", 15) or 15) * 2, 30)
    stale_seconds = None
    is_online = False
    if last_seen_utc is not None:
        stale_seconds = max(0, int((now_ts - last_seen_utc).total_seconds()))
        is_online = stale_seconds <= offline_threshold
    return {
        "worker_id": worker_id,
        "last_seen": last_seen_utc,
        "is_online": is_online,
        "stale_seconds": stale_seconds,
        "offline_threshold_seconds": offline_threshold,
    }


def _parse_utc_datetime(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return _to_utc_or_none(raw)
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _to_utc_or_none(dt)


def _youtube_scheduler_snapshot(now_utc: datetime | None = None) -> dict[str, Any]:
    now_ts = now_utc or datetime.now(timezone.utc)
    poll_seconds = max(1, int(getattr(settings, "youtube_analyze_poll_seconds_effective", 180) or 180))
    metrics_file = str(getattr(settings, "ops_job_metrics_file", "") or "").strip()
    last_run_at: datetime | None = None
    last_status: str | None = None

    if metrics_file:
        metrics = read_job_metrics_from_file(metrics_file, limit=400)
        for row in reversed(metrics):
            if str(row.get("job_name") or "") != "youtube_analyze_job":
                continue
            last_run_at = _parse_utc_datetime(row.get("ts_utc"))
            last_status = str(row.get("status") or "").strip() or None
            break

    next_run_eta_seconds: int | None = None
    if last_run_at is not None:
        next_run_eta_seconds = max(0, int((last_run_at + timedelta(seconds=poll_seconds) - now_ts).total_seconds()))

    return {
        "poll_seconds": poll_seconds,
        "last_run_at": last_run_at,
        "last_status": last_status,
        "next_run_eta_seconds": next_run_eta_seconds,
    }


def _youtube_effective_llm_snapshot() -> dict[str, Any]:
    cfg = settings.resolve_llm_config("youtube")
    return {
        "profile": settings.resolve_llm_profile_name("youtube"),
        "provider": cfg.provider,
        "model": cfg.model,
        "base_url": cfg.base_url,
        "api_key_present": bool(cfg.api_key),
    }


@router.get("/", response_class=HTMLResponse)
def overview_page(request: Request, db: Session = Depends(get_db)):
    snapshots = _build_market_snapshots(db)
    latest_alerts = list_alerts(db, limit=20)
    ai_signals = get_latest_ai_signals(db, symbols=settings.watchlist_symbols)

    # YouTube consensus
    yt_consensus = None
    yt_videos = []
    if settings.youtube_enabled:
        consensus_row = get_latest_youtube_consensus(db, symbol=settings.youtube_target_symbol)
        if consensus_row:
            yt_consensus = {
                "data": consensus_row.consensus_json,
                "created_at": consensus_row.created_at,
            }
        yt_videos = list_youtube_videos(db, limit=10)

    intel_digest = None
    if settings.intel_enabled:
        intel_digest_row = get_latest_intel_digest(
            db,
            symbol="GLOBAL",
            lookback_hours=settings.intel_digest_lookback_hours,
        )
        if intel_digest_row is not None:
            intel_digest = {
                "data": intel_digest_row.digest_json or {},
                "created_at": intel_digest_row.created_at,
            }

    return templates.TemplateResponse(
        "overview.html",
        {
            "request": request,
            "snapshots": snapshots,
            "alerts": latest_alerts,
            "ai_signals": ai_signals,
            "yt_consensus": yt_consensus,
            "yt_videos": yt_videos,
            "youtube_enabled": settings.youtube_enabled,
            "intel_enabled": settings.intel_enabled,
            "intel_digest": intel_digest,
            "model_registry": settings.llm_model_registry,
            "model_catalog": settings.llm_model_catalog,
            "model_tiers": settings.llm_model_tiers,
            "default_model": settings.resolve_llm_config("market").model,
            "generated_at": datetime.now(timezone.utc),
            "binance_ws_url": settings.binance_ws_url,
        },
    )


@router.get("/intel", response_class=HTMLResponse)
def intel_page(request: Request, db: Session = Depends(get_db)):
    digest_row = get_latest_intel_digest(
        db,
        symbol="GLOBAL",
        lookback_hours=settings.intel_digest_lookback_hours,
    )
    news_rows = list_news_items(
        db,
        last_hours=settings.intel_digest_lookback_hours,
        limit=min(500, max(50, settings.intel_max_items_per_run * 3)),
    )
    return templates.TemplateResponse(
        "intel.html",
        {
            "request": request,
            "digest": (digest_row.digest_json or {}) if digest_row else {},
            "digest_created_at": (digest_row.created_at if digest_row else None),
            "items": news_rows,
            "generated_at": datetime.now(timezone.utc),
            "intel_enabled": settings.intel_enabled,
        },
    )


@router.get("/alerts", response_class=HTMLResponse)
def alerts_page(request: Request, db: Session = Depends(get_db)):
    alerts = list_alerts(db, limit=200)
    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "alerts": alerts,
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/strategy", response_class=HTMLResponse)
def strategy_page(request: Request):
    symbols = settings.watchlist_symbols or ["BTCUSDT"]
    return templates.TemplateResponse(
        "strategy.html",
        {
            "request": request,
            "symbols": symbols,
            "default_symbol": symbols[0],
            "base_timeframes": ["1m", "5m", "15m", "1h", "4h"],
            "scoring_modes": ["STRICT", "REALISTIC", "OPTIMISTIC"],
            "generated_at": datetime.now(timezone.utc),
            "binance_ws_url": settings.binance_ws_url,
        },
    )


@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request, db: Session = Depends(get_db)):
    futures_row = get_latest_futures_account_snapshot(db)
    margin_row = get_latest_margin_account_snapshot(db)
    return templates.TemplateResponse(
        "account.html",
        {
            "request": request,
            "futures_row": futures_row,
            "margin_row": margin_row,
            "watch_symbol": settings.account_watch_symbol.upper(),
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/api/market")
def market_api(db: Session = Depends(get_db)):
    return {"items": _build_market_snapshots(db)}


@router.get("/api/ohlcv")
def ohlcv_api(
    symbol: str = Query(..., min_length=3, max_length=20),
    timeframe: str = Query(default="1m"),
    from_ts: int | None = Query(default=None, alias="from"),
    to_ts: int | None = Query(default=None, alias="to"),
    limit: int = Query(default=5000, ge=100, le=9000),
    db: Session = Depends(get_db),
):
    symbol_u = symbol.upper()
    if from_ts is not None and to_ts is not None and from_ts <= to_ts:
        start_dt = _datetime_from_epoch(from_ts)
        end_dt = _datetime_from_epoch(to_ts)
        if start_dt is None or end_dt is None:
            raise HTTPException(status_code=400, detail="invalid time range")
        rows = list_ohlcv_range(db, symbol=symbol_u, timeframe=timeframe, start_ts=start_dt, end_ts=end_dt)
    else:
        rows = list_recent_ohlcv(db, symbol=symbol_u, timeframe=timeframe, limit=limit)
    if len(rows) > limit:
        rows = rows[-limit:]
    return {
        "items": [
            {
                "ts": _epoch_seconds(row.ts),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
            }
            for row in rows
        ]
    }


@router.get("/api/account/futures")
def futures_account_api(
    include_raw: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    row = get_latest_futures_account_snapshot(db)
    if row is None:
        return {"item": None}
    item = {
        "ts": _json_datetime(row.ts),
        "created_at": _json_datetime(row.created_at),
        "total_margin_balance": row.total_margin_balance,
        "available_balance": row.available_balance,
        "total_maint_margin": row.total_maint_margin,
        "btc_position_amt": row.btc_position_amt,
        "btc_mark_price": row.btc_mark_price,
        "btc_liquidation_price": row.btc_liquidation_price,
        "btc_unrealized_pnl": row.btc_unrealized_pnl,
    }
    if include_raw:
        item["account"] = row.account_json or {}
        item["balance"] = row.balance_json or []
        item["positions"] = row.positions_json or []
    return {"item": item}


@router.get("/api/account/margin")
def margin_account_api(
    include_raw: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    row = get_latest_margin_account_snapshot(db)
    if row is None:
        return {"item": None}
    item = {
        "ts": _json_datetime(row.ts),
        "created_at": _json_datetime(row.created_at),
        "margin_level": row.margin_level,
        "total_asset_of_btc": row.total_asset_of_btc,
        "total_liability_of_btc": row.total_liability_of_btc,
        "normal_bar": row.normal_bar,
        "margin_call_bar": row.margin_call_bar,
        "force_liquidation_bar": row.force_liquidation_bar,
    }
    if include_raw:
        item["account"] = row.account_json or {}
        item["trade_coeff"] = row.trade_coeff_json or {}
    return {"item": item}


@router.get("/api/account/equity-curve")
def account_equity_curve_api(
    days: int = Query(default=90, ge=7, le=3650),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    start_day = now - timedelta(days=max(1, int(days)) - 1)
    rows = list_account_stats_daily(
        db,
        start_day=start_day,
        end_day=now,
        limit=max(30, int(days) + 10),
    )
    items = []
    for row in rows:
        ts = _epoch_seconds(row.day_utc)
        if ts is None:
            continue
        items.append(
            {
                "ts": ts,
                "open": row.equity_open,
                "high": row.equity_high,
                "low": row.equity_low,
                "close": row.equity_close,
                "sample_count": int(row.sample_count or 0),
            }
        )
    return {"items": items}


@router.get("/api/strategy/decisions")
def strategy_decisions_api(
    symbol: str = Query(..., min_length=3, max_length=20),
    from_ts: int = Query(..., alias="from"),
    to_ts: int = Query(..., alias="to"),
    manifest_id: str | None = Query(default=None),
    side: str | None = Query(default=None),
    outcome: str | None = Query(default=None),
    regime: str | None = Query(default=None),
    mode: str = Query(default="raw"),
    cursor: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    bucket_seconds: int = Query(default=900, ge=60, le=86400),
    db: Session = Depends(get_db),
):
    symbol_u = symbol.upper()
    mode_norm = mode.lower()
    if mode_norm not in {"raw", "densified"}:
        raise HTTPException(status_code=400, detail="mode must be raw or densified")
    if from_ts > to_ts:
        raise HTTPException(status_code=400, detail="from must be <= to")

    if mode_norm == "densified":
        items = list_strategy_decisions_densified(
            db,
            symbol=symbol_u,
            from_ts=from_ts,
            to_ts=to_ts,
            manifest_id=manifest_id,
            side=side,
            outcome=outcome,
            regime=regime,
            bucket_seconds=bucket_seconds,
        )
        return {
            "mode": "densified",
            "items": items,
            "has_more": False,
            "next_cursor": None,
        }

    items, has_more, next_cursor = list_strategy_decisions_raw(
        db,
        symbol=symbol_u,
        from_ts=from_ts,
        to_ts=to_ts,
        manifest_id=manifest_id,
        side=side,
        outcome=outcome,
        regime=regime,
        cursor=cursor,
        limit=limit,
    )
    return {
        "mode": "raw",
        "items": items,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


@router.get("/api/strategy/decisions/{decision_id}")
def strategy_decision_detail_api(decision_id: int, db: Session = Depends(get_db)):
    item = get_strategy_decision_detail(db, decision_id=decision_id)
    if item is None:
        raise HTTPException(status_code=404, detail="decision not found")
    return {"item": item}


@router.get("/api/strategy/scores")
def strategy_scores_api(
    manifest_id: str | None = Query(default=None),
    split_type: str | None = Query(default=None),
    scoring_mode: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    rows = list_strategy_scores(
        db,
        manifest_id=manifest_id,
        split_type=split_type,
        scoring_mode=scoring_mode,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "manifest_id": row.manifest_id,
                "window_start_ts": row.window_start_ts,
                "window_end_ts": row.window_end_ts,
                "split_type": row.split_type,
                "scoring_mode": row.scoring_mode,
                "status": row.status,
                "n_trades": row.n_trades,
                "n_resolved": row.n_resolved,
                "n_ambiguous": row.n_ambiguous,
                "n_timeout": row.n_timeout,
                "win_rate": row.win_rate,
                "avg_r": row.avg_r,
                "win_rate_ci_low": row.win_rate_ci_low,
                "win_rate_ci_high": row.win_rate_ci_high,
                "avg_r_ci_low": row.avg_r_ci_low,
                "avg_r_ci_high": row.avg_r_ci_high,
                "timeout_rate": row.timeout_rate,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }


@router.get("/api/strategy/feature-stats")
def strategy_feature_stats_api(
    manifest_id: str | None = Query(default=None),
    split_type: str | None = Query(default=None),
    scoring_mode: str | None = Query(default=None),
    regime_id: str | None = Query(default=None),
    status: str | None = Query(default="OK"),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    rows = list_strategy_feature_stats(
        db,
        manifest_id=manifest_id,
        split_type=split_type,
        scoring_mode=scoring_mode,
        regime_id=regime_id,
        status=status,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "manifest_id": row.manifest_id,
                "window_start_ts": row.window_start_ts,
                "window_end_ts": row.window_end_ts,
                "split_type": row.split_type,
                "regime_id": row.regime_id,
                "scoring_mode": row.scoring_mode,
                "feature_key": row.feature_key,
                "bucket_key": row.bucket_key,
                "status": row.status,
                "n": row.n,
                "win_rate": row.win_rate,
                "avg_r": row.avg_r,
                "ci_low": row.ci_low,
                "ci_high": row.ci_high,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }


@router.get("/api/intel/news")
def intel_news_api(
    last_hours: int = Query(default=24, ge=1, le=168),
    category: str | None = Query(default=None),
    severity_min: int | None = Query(default=None, ge=0, le=100),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    rows = list_news_items(
        db,
        last_hours=last_hours,
        category=category,
        severity_min=severity_min,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "ts_utc": row.ts_utc,
                "source": row.source,
                "category": row.category,
                "title": row.title,
                "url": row.url,
                "summary": row.summary,
                "region": row.region,
                "topics": row.topics_json or [],
                "alert_keyword": row.alert_keyword,
                "severity": row.severity,
                "entities": row.entities_json or [],
            }
            for row in rows
        ]
    }


@router.get("/api/intel/digest")
def intel_digest_api(db: Session = Depends(get_db)):
    row = get_latest_intel_digest(
        db,
        symbol="GLOBAL",
        lookback_hours=settings.intel_digest_lookback_hours,
    )
    return {
        "item": {
            "symbol": row.symbol,
            "lookback_hours": row.lookback_hours,
            "digest": row.digest_json or {},
            "created_at": row.created_at,
        }
        if row
        else None
    }


@router.post("/api/translate")
async def translate_api(payload: dict = Body(default_factory=dict)):
    """Translate text to Simplified Chinese. Uses LibreTranslate (free) with MyMemory fallback."""
    text = (payload.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Missing or empty 'text' field")
    if len(text) > 5000:
        raise HTTPException(status_code=400, detail="Text too long (max 5000 chars)")

    # Try LibreTranslate first
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                "https://libretranslate.com/translate",
                json={"q": text, "source": "auto", "target": "zh", "format": "text"},
                headers={"Content-Type": "application/json"},
            )
            if r.status_code == 200:
                data = r.json()
                translated = (data.get("translatedText") or "").strip()
                if translated:
                    return {"translated": translated, "source": "libretranslate"}
    except Exception:
        pass

    # Fallback: MyMemory (for short text)
    if len(text) <= 500:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    "https://api.mymemory.translated.net/get",
                    params={"q": text, "langpair": "en|zh"},
                )
                if r.status_code == 200:
                    data = r.json()
                    translated = (data.get("responseData", {}).get("translatedText") or "").strip()
                    if translated and translated != text:
                        return {"translated": translated, "source": "mymemory"}
        except Exception:
            pass

    raise HTTPException(status_code=503, detail="Translation service unavailable")


@router.get("/api/alerts")
def alerts_api(
    limit: int = Query(default=100, ge=1, le=500),
    symbol: str | None = Query(default=None),
    alert_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    rows = list_alerts(db, limit=limit, symbol=symbol, alert_type=alert_type)
    return {
        "items": [
            {
                "event_uid": row.event_uid,
                "symbol": row.symbol,
                "timeframe": row.timeframe,
                "ts": row.ts,
                "alert_type": row.alert_type,
                "severity": row.severity,
                "reason": row.reason,
                "rule_version": row.rule_version,
                "metrics": row.metrics_json,
                "sent_to_telegram": row.sent_to_telegram,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }


@router.get("/api/health")
def health_api():
    start = time.perf_counter()
    db_ok, worker_last_seen = _quick_db_health_and_worker(settings.database_url, settings.worker_id)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    recent_jobs = read_job_metrics_from_file(settings.ops_job_metrics_file, limit=20)
    last_job = recent_jobs[-1] if recent_jobs else None
    return {
        "api_ok": True,
        "db_ok": db_ok,
        "worker_last_seen": worker_last_seen,
        "server_time": datetime.now(timezone.utc),
        "db_probe_ms": elapsed_ms,
        "ops": {
            "job_metrics_count": len(recent_jobs),
            "last_job": last_job,
        },
    }


@router.get("/api/models")
def models_api():
    return {
        "items": settings.llm_model_catalog,
        "default_model": settings.resolve_llm_config("market").model,
    }


@router.get("/api/ai-signals")
def ai_signals_api(
    limit: int = Query(default=50, ge=1, le=200),
    symbol: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    rows = list_ai_signals(db, limit=limit, symbol=symbol)

    def _analysis_debug_summary(analysis_json: Any) -> dict[str, Any] | None:
        if not isinstance(analysis_json, dict):
            return None
        validation = analysis_json.get("validation") if isinstance(analysis_json.get("validation"), dict) else {}
        risk = analysis_json.get("risk") if isinstance(analysis_json.get("risk"), dict) else {}
        yt_reflection = (
            analysis_json.get("youtube_reflection")
            if isinstance(analysis_json.get("youtube_reflection"), dict)
            else {}
        )
        context_digest = (
            analysis_json.get("context_digest")
            if isinstance(analysis_json.get("context_digest"), dict)
            else {}
        )
        data_quality = (
            context_digest.get("data_quality")
            if isinstance(context_digest.get("data_quality"), dict)
            else None
        )
        warnings = validation.get("warnings") if isinstance(validation.get("warnings"), list) else []
        return {
            "has_details": True,
            "validation_status": validation.get("status"),
            "validation_warnings": warnings,
            "warning_count": len(warnings),
            "downgrade_reason": validation.get("downgrade_reason"),
            "rr": validation.get("rr", risk.get("rr")),
            "sl_atr_multiple": validation.get("sl_atr_multiple", risk.get("sl_atr_multiple")),
            "youtube_reflection_status": yt_reflection.get("status"),
            "data_quality": data_quality,
            "context_budget": context_digest.get("input_budget_meta") if context_digest else None,
            "tradeable_gate": context_digest.get("tradeable_gate") if context_digest else None,
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        analysis_json = getattr(row, "analysis_json", None)
        analysis_summary = _analysis_debug_summary(analysis_json)
        items.append(
            {
                "symbol": row.symbol,
                "direction": row.direction,
                "entry_price": row.entry_price,
                "take_profit": row.take_profit,
                "stop_loss": row.stop_loss,
                "confidence": row.confidence,
                "reasoning": row.reasoning,
                "analysis_json": analysis_json,
                "analysis_summary": analysis_summary,
                "validation_warnings": (analysis_summary or {}).get("validation_warnings"),
                "model_requested": getattr(row, "model_requested", None),
                "model_name": row.model_name,
                "created_at": row.created_at,
            }
        )
    return {"items": items}


# ======== LLM Debug Layer ========

def _mask_api_key(key: str | None) -> str:
    if not key:
        return ""
    if len(key) < 8:
        return "***"
    return f"{key[:4]}****{key[-4:]}"

def _llm_default_stats() -> dict[str, int]:
    return {"total": 0, "ok": 0, "rate_limited": 0, "error": 0, "avg_duration": 0}


def _provider_labels() -> dict[str, str]:
    return {
        "deepseek": "DeepSeek（官方）",
        "openrouter": "OpenRouter",
        "openai_compatible": "OpenAI 兼容",
        "ark": "Ark（火山引擎）",
        "nvidia_nim": "NVIDIA NIM",
    }


def _field_hints() -> dict[str, str]:
    return {
        "use_reasoning": "Reasoning mode: auto=true/false depending on model capability.",
        "reasoning_effort": "Reasoning effort (low, medium, high) for reasoning models (e.g. Doubao/DeepSeek).",
        "base_url_override": "Leave empty to use provider default base URL.",
        "api_key_override": "Leave empty to use provider default API key.",
        "routing": "Task routing decides which profile each task uses.",
        "openrouter_headers": "Optional OpenRouter HTTP-Referer / X-Title headers.",
        "nvidia_nim_model": "NVIDIA NIM for VLMs default model: nvidia_nim/qwen3.5-397b-a17b (mapped upstream to qwen/qwen3.5-397b-a17b).",
    }


def _masked_profile_dict(profile) -> dict:
    data = profile.model_dump(exclude_none=True)
    if "api_key_override" in data:
        data["api_key_override"] = _mask_api_key(data.get("api_key_override"))
    return data


def _build_task_editor_payload(s, task: str) -> dict:
    task_key = s.normalize_llm_task(task)
    routed_profile = s.resolve_llm_profile_name(task_key)
    profiles = s.llm_profiles
    profile = profiles.get(routed_profile) or profiles.get(task_key) or profiles.get("general")
    resolved = s.resolve_llm_config(task_key)

    editable = {}
    if profile is not None:
        editable = {
            "task": task_key,
            "profile_name": routed_profile,
            "enabled": profile.enabled,
            "provider": profile.provider,
            "api_key": "" if profile.api_key_override is None else _mask_api_key(profile.api_key_override),
            "base_url": profile.base_url_override,
            "model": profile.model,
            "use_reasoning": profile.use_reasoning,
            "reasoning_effort": profile.reasoning_effort or "",
            "max_concurrency": profile.max_concurrency,
            "max_retries": profile.max_retries,
            "http_referer": profile.http_referer or "",
            "x_title": profile.x_title or "",
        }

    return {
        "task": task_key,
        "routed_profile": routed_profile,
        "resolved": {
            "enabled": resolved.enabled,
            "provider": resolved.provider,
            "model": resolved.model,
            "base_url": resolved.base_url,
            "use_reasoning": resolved.use_reasoning,
            "reasoning_effort": resolved.reasoning_effort or "",
            "max_concurrency": resolved.max_concurrency,
            "max_retries": resolved.max_retries,
            "http_referer": resolved.http_referer,
            "x_title": resolved.x_title,
            "api_key_present": bool(resolved.api_key),
        },
        "editable_profile": editable,
    }


def _pick_models_by_keywords(registry: list[dict[str, str]], keywords: list[str], limit: int = 6) -> list[dict[str, str]]:
    picks: list[dict[str, str]] = []
    seen: set[str] = set()
    keys = [k.lower() for k in keywords]
    for item in registry:
        model_id = str(item.get("id", ""))
        label = str(item.get("label", ""))
        text = f"{model_id} {label}".lower()
        if any(k in text for k in keys):
            if model_id in seen:
                continue
            picks.append({"id": model_id, "label": label})
            seen.add(model_id)
            if len(picks) >= limit:
                break
    return picks


def _build_llm_model_presets(s) -> dict:
    catalog = s.llm_model_catalog

    def _flat(provider: str | None = None, tier: str | None = None, limit: int = 8) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for item in catalog:
            item_provider = str(item.get("provider") or "")
            item_tier = str(item.get("tier") or "")
            if provider and item_provider != provider:
                continue
            if tier and item_tier != tier:
                continue
            out.append({"id": str(item.get("id") or ""), "label": str(item.get("label") or item.get("id") or "")})
            if len(out) >= limit:
                break
        return [x for x in out if x["id"]]

    openrouter_advanced = _flat(provider="openrouter", tier="premium", limit=8) or _flat(provider="openrouter", limit=8)
    openrouter_balanced = _flat(provider="openrouter", tier="balanced", limit=8) or _flat(provider="openrouter", limit=8)
    deepseek_recommended = _flat(provider="deepseek", limit=8)
    ark_recommended = _flat(provider="ark", limit=8)
    nvidia_nim_recommended = _flat(provider="nvidia_nim", limit=8)

    premium_model = openrouter_advanced[0]["id"] if openrouter_advanced else "google/gemini-3.1-pro-preview"
    balanced_model = openrouter_balanced[0]["id"] if openrouter_balanced else "deepseek/deepseek-r1"
    ark_model = ark_recommended[0]["id"] if ark_recommended else "doubao-seed-2-0-pro-260215"
    nvidia_nim_model = (
        nvidia_nim_recommended[0]["id"] if nvidia_nim_recommended else "nvidia_nim/qwen3.5-397b-a17b"
    )

    return {
        "deepseek_recommended": deepseek_recommended,
        "ark_recommended": ark_recommended,
        "nvidia_nim_recommended": nvidia_nim_recommended,
        "openrouter_recommended": {
            "advanced": openrouter_advanced,
            "balanced": openrouter_balanced,
        },
        "tier_templates": {
            "cheap_deepseek": {
                "id": "cheap_deepseek",
                "label": "便宜 DeepSeek",
                "description": "适合低成本、快速的通用任务",
                "config": {"provider": "deepseek", "model": "deepseek-chat", "use_reasoning": "false", "base_url": "", "enabled": True},
            },
            "balanced_openrouter": {
                "id": "balanced_openrouter",
                "label": "均衡 OpenRouter",
                "description": "适合大多数通用任务，兼顾成本与效果",
                "config": {"provider": "openrouter", "model": balanced_model, "use_reasoning": "auto", "base_url": "", "enabled": True},
            },
            "premium_openrouter": {
                "id": "premium_openrouter",
                "label": "高级 OpenRouter",
                "description": "适合对效果要求高，对成本不敏感的任务",
                "config": {"provider": "openrouter", "model": premium_model, "use_reasoning": "true", "base_url": "", "enabled": True},
            },
            "cheap_ark": {
                "id": "cheap_ark",
                "label": "便宜 Ark",
                "description": "适合低成本、快速的通用任务",
                "config": {"provider": "ark", "model": ark_model, "use_reasoning": "false", "base_url": "", "enabled": True},
            },
            "balanced_ark": {
                "id": "balanced_ark",
                "label": "均衡 Ark",
                "description": "适合大多数通用任务，兼顾成本与效果",
                "config": {"provider": "ark", "model": ark_model, "use_reasoning": "auto", "base_url": "", "enabled": True},
            },
            "premium_ark": {
                "id": "premium_ark",
                "label": "高级 Ark",
                "description": "适合对效果要求高，对成本不敏感的任务",
                "config": {"provider": "ark", "model": ark_model, "use_reasoning": "true", "base_url": "", "enabled": True},
            },
            "premium_nvidia_nim": {
                "id": "premium_nvidia_nim",
                "label": "高级 NVIDIA NIM",
                "description": "适合视觉理解和复杂多模态任务",
                "config": {
                    "provider": "nvidia_nim",
                    "model": nvidia_nim_model,
                    "use_reasoning": "auto",
                    "base_url": "",
                    "enabled": True,
                },
            },
        },
    }


def _collect_profile_auto_heal_warnings(raw_profiles_json: str) -> list[str]:
    warnings: list[str] = []
    try:
        data = json.loads(raw_profiles_json or "{}")
    except json.JSONDecodeError:
        return warnings
    if not isinstance(data, dict):
        return warnings

    for profile_name, profile in data.items():
        if not isinstance(profile_name, str) or not isinstance(profile, dict):
            continue
        missing: list[str] = []
        if not str(profile.get("provider") or "").strip():
            missing.append("provider")
        if not str(profile.get("model") or "").strip():
            missing.append("model")
        if missing:
            warnings.append(f"profile {profile_name} missing {', '.join(missing)}; auto-healed with defaults.")
    return warnings


def _coerce_profile_for_save(s, profile_name: str, profile_data: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    out = dict(profile_data or {})
    current_profiles = s.llm_profiles
    fallback_profile = current_profiles.get(profile_name) or current_profiles.get("general")

    provider = str(out.get("provider") or "").strip().lower()
    if not provider:
        provider = str(getattr(fallback_profile, "provider", "deepseek") or "deepseek")
        out["provider"] = provider
        warnings.append(f"profile {profile_name} missing provider; auto-filled as {provider}.")

    model = str(out.get("model") or "").strip()
    if not model:
        model = s.default_model_for_provider(provider)
        out["model"] = model
        warnings.append(f"profile {profile_name} missing model; auto-filled as {model}.")

    return out, warnings


@router.get("/api/llm/config")
def llm_get_config(_admin: str = Depends(require_admin)):
    """Retrieve current LLM Config mapping (masking API keys) with UI metadata."""
    refresh_llm_env_vars_from_dotenv()
    get_settings.cache_clear()
    s = get_settings()
    profiles = s.llm_profiles
    tasks = ["telegram_chat", "market", "youtube", "selfcheck"]
    tasks_payload = {task: _build_task_editor_payload(s, task) for task in tasks}
    profiles_payload = {name: _masked_profile_dict(profile) for name, profile in profiles.items()}
    provider_labels = _provider_labels()
    field_hints = _field_hints()
    config_warnings = _collect_profile_auto_heal_warnings(s.llm_profiles_json)

    legacy_default = _build_task_editor_payload(s, "general")["editable_profile"]
    legacy_market = tasks_payload["market"]["editable_profile"]
    legacy_youtube = tasks_payload["youtube"]["editable_profile"]
    legacy_telegram = tasks_payload["telegram_chat"]["editable_profile"]

    return {
        "ok": True,
        "needs_restart": False,
        "hot_reload_capability": {
            "api": True,
            "worker": True,
            "mode": "redis" if getattr(s, "llm_hot_reload_use_redis", True) else "signal+heartbeat",
        },
        "common_keys": {
            "deepseek": _mask_api_key(s.deepseek_api_key),
            "openrouter": _mask_api_key(s.openrouter_api_key),
            "openai": _mask_api_key(s.openai_api_key),
            "ark": _mask_api_key(s.ark_api_key),
            "nvidia_nim": _mask_api_key(s.nvidia_nim_api_key),
        },
        "task_routing": s.llm_task_routing,
        "profiles": profiles_payload,
        "tasks": tasks_payload,
        "model_presets": _build_llm_model_presets(s),
        "model_catalog": s.llm_model_catalog,
        "model_tiers": s.llm_model_tiers,
        "provider_labels": provider_labels,
        "field_hints": field_hints,
        "warnings": config_warnings,
        "ui_meta": {
            "task_labels": {
                "telegram_chat": "聊天 (Telegram)",
                "market": "市场分析",
                "youtube": "YouTube",
                "selfcheck": "自检",
                "general": "通用默认",
            },
            "task_order": tasks,
            "provider_labels": provider_labels,
            "supported_tasks_for_selfcheck": ["market", "youtube", "telegram_chat", "selfcheck", "general"],
        },
        # 兼容旧前端字段
        "default": legacy_default,
        "telegram_chat": legacy_telegram,
        "market": legacy_market,
        "youtube": legacy_youtube,
    }

@router.post("/api/llm/config")
async def llm_update_config(req: Request, _admin: str = Depends(require_admin)):
    """Update `.env` locally with overrides from the UI and apply hot reload by default."""
    data = await req.json()
    task = str(data.get("task") or "").strip()
    apply_now_raw = data.get("apply_now", True)
    apply_now = str(apply_now_raw).strip().lower() not in {"0", "false", "no"} if isinstance(apply_now_raw, str) else bool(apply_now_raw)
    if task not in ("default", "telegram_chat", "market", "youtube", "selfcheck", "common_keys", "routing"):
        return {
            "ok": False,
            "error": "不支持的任务类型",
            "applied": {
                "api_reloaded": False,
                "worker_signal_sent": False,
                "worker_expected_apply_within_seconds": None,
                "signal_revision": None,
            },
        }

    try:
        from app.cli import ENV_PATH
        import dotenv

        refresh_llm_env_vars_from_dotenv(str(ENV_PATH))
        get_settings.cache_clear()
        current_settings = get_settings()
        applied = {
            "api_reloaded": False,
            "worker_signal_sent": False,
            "worker_expected_apply_within_seconds": current_settings.worker_heartbeat_seconds,
            "signal_revision": None,
        }
        warnings = _collect_profile_auto_heal_warnings(current_settings.llm_profiles_json)

        if task == "common_keys":
            keys_data = data.get("keys", {})
            key_alias_map = {
                "deepseek": "DEEPSEEK_API_KEY",
                "openrouter": "OPENROUTER_API_KEY",
                "openai": "OPENAI_API_KEY",
                "openai_compatible": "OPENAI_API_KEY",
                "ark": "ARK_API_KEY",
                "nvidia_nim": "NVIDIA_NIM_API_KEY",
            }
            for provider, key in keys_data.items():
                if isinstance(key, str) and "****" not in key and key.strip() != "":
                    env_key_name = key_alias_map.get(provider, f"{provider.upper()}_API_KEY")
                    dotenv.set_key(str(ENV_PATH), env_key_name, key.strip())
            message = "Provider default API keys saved"
            if apply_now:
                refreshed = apply_llm_config_in_api_process()
                applied["api_reloaded"] = True
                applied["worker_expected_apply_within_seconds"] = refreshed.worker_heartbeat_seconds
                applied["signal_revision"] = write_llm_reload_signal(
                    refreshed.llm_hot_reload_signal_file,
                    source="llm_ui",
                    reason="common_keys_updated",
                )
                applied["worker_signal_sent"] = True
                message = (
                    "Provider default API keys saved and hot-reloaded in API; "
                    f"worker should apply within {refreshed.worker_heartbeat_seconds}s."
                )
            return {"ok": True, "message": message, "applied": applied, "warnings": list(dict.fromkeys(warnings))}

        if task == "routing":
            routing_data = data.get("routing", {})
            if not isinstance(routing_data, dict):
                return {"ok": False, "error": "Invalid routing payload", "applied": applied}
            sanitized: dict[str, str] = {}
            for k, v in routing_data.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    continue
                key = k.strip().lower()
                val = v.strip()
                if key and val:
                    sanitized[key] = val
            dotenv.set_key(str(ENV_PATH), "LLM_TASK_ROUTING_JSON", json.dumps(sanitized, ensure_ascii=False))
            message = "Task routing saved"
            if apply_now:
                refreshed = apply_llm_config_in_api_process()
                applied["api_reloaded"] = True
                applied["worker_expected_apply_within_seconds"] = refreshed.worker_heartbeat_seconds
                applied["signal_revision"] = write_llm_reload_signal(
                    refreshed.llm_hot_reload_signal_file,
                    source="llm_ui",
                    reason="task_routing_updated",
                )
                applied["worker_signal_sent"] = True
                message = (
                    "Task routing saved and hot-reloaded in API; "
                    f"worker should apply within {refreshed.worker_heartbeat_seconds}s."
                )
            return {"ok": True, "message": message, "applied": applied, "warnings": list(dict.fromkeys(warnings))}

        task_key = "general" if task == "default" else task
        task_key = current_settings.normalize_llm_task(task_key)
        profile_key = current_settings.resolve_llm_profile_name(task_key)

        current_profiles: dict[str, dict[str, Any]] = {}
        for k, v in current_settings.llm_profiles.items():
            current_profiles[k] = v.model_dump(exclude_none=True)

        if profile_key not in current_profiles:
            current_profiles[profile_key] = current_profiles.get("general", {}).copy()

        for key, value in data.get("config", {}).items():
            if isinstance(value, str) and "****" in value:
                continue
            if key == "api_key":
                key = "api_key_override"
            elif key == "base_url":
                key = "base_url_override"

            if value is None or (isinstance(value, str) and value.strip() == ""):
                if key in {"provider", "model"}:
                    continue
                current_profiles[profile_key].pop(key, None)
            else:
                current_profiles[profile_key][key] = value

        for p_name, p_data in list(current_profiles.items()):
            fixed, auto_warnings = _coerce_profile_for_save(current_settings, p_name, p_data)
            current_profiles[p_name] = fixed
            warnings.extend(auto_warnings)

        fixed_target, target_warnings = _coerce_profile_for_save(current_settings, profile_key, current_profiles.get(profile_key, {}))
        current_profiles[profile_key] = fixed_target
        warnings.extend(target_warnings)

        dotenv.set_key(str(ENV_PATH), "LLM_PROFILES_JSON", json.dumps(current_profiles, ensure_ascii=False))
        message = f"Config saved (task={task_key}, profile={profile_key})"
        if apply_now:
            refreshed = apply_llm_config_in_api_process()
            applied["api_reloaded"] = True
            applied["worker_expected_apply_within_seconds"] = refreshed.worker_heartbeat_seconds
            applied["signal_revision"] = write_llm_reload_signal(
                refreshed.llm_hot_reload_signal_file,
                source="llm_ui",
                reason=f"profile_updated:{profile_key}",
            )
            applied["worker_signal_sent"] = True
            message = (
                "Config saved and hot-reloaded in API; "
                f"worker should apply within {refreshed.worker_heartbeat_seconds}s."
            )

        return {"ok": True, "message": message, "applied": applied, "warnings": list(dict.fromkeys(warnings))}

    except Exception as e:
        logger.error(f"Failed to save LLM config via UI: {e}")
        return {
            "ok": False,
            "error": str(e),
            "applied": {
                "api_reloaded": False,
                "worker_signal_sent": False,
                "worker_expected_apply_within_seconds": None,
                "signal_revision": None,
            },
        }


@router.get("/llm", response_class=HTMLResponse)
def llm_debug_page(request: Request):
    return templates.TemplateResponse(
        "llm_debug.html",
        {
            "request": request,
            "youtube_enabled": settings.youtube_enabled,
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/api/llm/status")
def llm_status_api(db: Session = Depends(get_db)):
    from app.db.repository import get_llm_stats_1h
    refresh_llm_env_vars_from_dotenv()
    get_settings.cache_clear()
    s = get_settings()

    telegram_chat_config = s.resolve_llm_config("telegram_chat")
    telegram_chat_profile = s.resolve_llm_profile_name("telegram_chat")
    market_config = s.resolve_llm_config("market")
    market_profile = s.resolve_llm_profile_name("market")
    youtube_config = s.resolve_llm_config("youtube")
    youtube_profile = s.resolve_llm_profile_name("youtube")
    selfcheck_config = s.resolve_llm_config("selfcheck")
    selfcheck_profile = s.resolve_llm_profile_name("selfcheck")

    stats = get_llm_stats_1h(db)
    hot_reload_data: dict[str, Any] = {}
    if getattr(s, "llm_hot_reload_use_redis", True):
        worker_acks = read_llm_reload_acks_redis(s.redis_url)
        hot_reload_data["workers"] = worker_acks
        if len(worker_acks) == 1:
            single = next(iter(worker_acks.values()))
            hot_reload_data["worker_ack_revision"] = single.get("revision")
            hot_reload_data["worker_ack_applied_at"] = single.get("applied_at")
            hot_reload_data["worker_ack_status"] = single.get("status")
            hot_reload_data["worker_ack_details"] = single.get("details")
        else:
            hot_reload_data["worker_ack_revision"] = None
            hot_reload_data["worker_ack_applied_at"] = None
            hot_reload_data["worker_ack_status"] = None
            hot_reload_data["worker_ack_details"] = None
        hot_reload_data["signal_revision"] = None
        hot_reload_data["signal_requested_at"] = None
    else:
        signal_state = read_llm_reload_signal(s.llm_hot_reload_signal_file)
        ack_state = read_llm_reload_ack(s.llm_hot_reload_ack_file)
        hot_reload_data = {
            "signal_revision": (signal_state or {}).get("revision"),
            "signal_requested_at": (signal_state or {}).get("requested_at"),
            "worker_ack_revision": (ack_state or {}).get("revision"),
            "worker_ack_applied_at": (ack_state or {}).get("applied_at"),
            "worker_ack_status": (ack_state or {}).get("status"),
            "worker_ack_details": (ack_state or {}).get("details"),
        }

    return {
        "ok": True,
        "task_routing": s.llm_task_routing,
        "hot_reload": hot_reload_data,
        "telegram_chat": {
            "profile": telegram_chat_profile,
            "enabled": telegram_chat_config.enabled,
            "provider": telegram_chat_config.provider,
            "model": telegram_chat_config.model,
            "base_url": telegram_chat_config.base_url,
            "use_reasoning": telegram_chat_config.use_reasoning,
            "reasoning_effort": telegram_chat_config.reasoning_effort,
            "max_concurrency": telegram_chat_config.max_concurrency,
            "config": {  
                "enabled": telegram_chat_config.enabled,
                "provider": telegram_chat_config.provider,
                "model": telegram_chat_config.model,
                "base_url": telegram_chat_config.base_url,
                "use_reasoning": telegram_chat_config.use_reasoning,
                "reasoning_effort": telegram_chat_config.reasoning_effort,
                "max_concurrency": telegram_chat_config.max_concurrency,
            },
            "stats_1h": stats.get("telegram_chat", _llm_default_stats()),
        },
        "market": {
            "profile": market_profile,
            "enabled": market_config.enabled,
            "provider": market_config.provider,
            "model": market_config.model,
            "base_url": market_config.base_url,
            "use_reasoning": market_config.use_reasoning,
            "reasoning_effort": market_config.reasoning_effort,
            "max_concurrency": market_config.max_concurrency,
            "stats_1h": stats.get("market", _llm_default_stats()),
        },
        "youtube": {
            "profile": youtube_profile,
            "enabled": youtube_config.enabled,
            "provider": youtube_config.provider,
            "model": youtube_config.model,
            "base_url": youtube_config.base_url,
            "use_reasoning": youtube_config.use_reasoning,
            "reasoning_effort": youtube_config.reasoning_effort,
            "max_concurrency": youtube_config.max_concurrency,
            "stats_1h": stats.get("youtube", _llm_default_stats()),
        },
        "selfcheck": {
            "profile": selfcheck_profile,
            "enabled": selfcheck_config.enabled,
            "provider": selfcheck_config.provider,
            "model": selfcheck_config.model,
            "base_url": selfcheck_config.base_url,
            "use_reasoning": selfcheck_config.use_reasoning,
            "reasoning_effort": selfcheck_config.reasoning_effort,
            "max_concurrency": selfcheck_config.max_concurrency,
            "stats_1h": stats.get("selfcheck", _llm_default_stats()),
        },
    }


@router.get("/api/llm/calls")
def llm_calls_api(
    task: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    from app.db.repository import get_llm_calls
    rows = get_llm_calls(db, limit=limit, task=task)
    return {
        "ok": True,
        "items": [
            {
                "id": r.id,
                "task": r.task,
                "provider_name": r.provider_name,
                "model": r.model,
                "status": r.status,
                "duration_ms": r.duration_ms,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "error_summary": r.error_summary,
                "created_at": r.created_at
            }
            for r in rows
        ]
    }


@router.get("/api/llm/failures")
def llm_failures_api(
    task: str = Query(default="market"),
    symbol: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    rows = list_ai_analysis_failures(db, limit=limit, task=task, symbol=symbol)
    return {
        "ok": True,
        "items": [
            {
                "id": r.id,
                "task": r.task,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "ts": r.ts,
                "attempt": r.attempt,
                "phase": r.phase,
                "provider_name": r.provider_name,
                "model_requested": r.model_requested,
                "model_actual": r.model_actual,
                "error_code": r.error_code,
                "error_summary": r.error_summary,
                "raw_response_excerpt": r.raw_response_excerpt,
                "details_json": r.details_json,
                "created_at": r.created_at,
            }
            for r in rows
        ],
    }


@router.post("/api/llm/selfcheck")
async def llm_selfcheck_api(body: dict, db: Session = Depends(get_db), _admin: str = Depends(require_admin)):
    task = body.get("task", "market")
    if task not in ["market", "youtube", "telegram_chat", "general", "selfcheck"]:
        return {"ok": False, "error": "Invalid task, must be market / youtube / telegram_chat / general / selfcheck"}

    s = settings
    config = s.resolve_llm_config(task)
    if not config.enabled:
        return {"ok": False, "error": f"{task} LLM is disabled in config"}

    from app.ai.openai_provider import OpenAICompatibleProvider
    from app.ai.provider import LLMRateLimitError, LLMTimeoutError
    
    provider = OpenAICompatibleProvider(config)
    
    messages = [{"role": "user", "content": "You are a health check bot. Please respond with exactly the word 'OK' and nothing else."}]
    
    import time
    start = time.perf_counter()
    status_code = "ok"
    err_msg = None
    response_text = ""
    resp = {}
    
    try:
        resp = await provider.generate_response(
            messages=messages,
            max_tokens=10,
            temperature=0.0,
            use_reasoning=False
        )
        response_text = resp.get("content", "").strip()
    except LLMRateLimitError as e:
        status_code = "429"
        err_msg = str(e)
    except LLMTimeoutError as e:
        status_code = "timeout"
        err_msg = str(e)
    except Exception as e:
        status_code = "error"
        err_msg = str(e)
        
    duration_ms = int((time.perf_counter() - start) * 1000)
    
    # Optionally log the selfcheck locally as a call
    from app.db.repository import insert_llm_call
    insert_llm_call(db, {
        "task": "selfcheck",
        "provider_name": type(provider).__name__,
        "model": config.model,
        "status": status_code,
        "duration_ms": duration_ms,
        "prompt_tokens": resp.get("prompt_tokens") if status_code == "ok" else None,
        "completion_tokens": resp.get("completion_tokens") if status_code == "ok" else None,
        "error_summary": err_msg,
    })
    
    return {
        "ok": status_code == "ok",
        "status": status_code,
        "duration_ms": duration_ms,
        "response": response_text,
        "error": err_msg
    }


def _build_market_snapshots(db: Session) -> list[dict]:
    metric_rows = get_latest_market_metrics(db, symbols=settings.watchlist_symbols, timeframe="1m")
    metrics_by_symbol = {row.symbol: row for row in metric_rows}

    snapshots: list[dict] = []
    for symbol in settings.watchlist_symbols:
        metric = metrics_by_symbol.get(symbol)
        if metric is not None:
            snapshots.append(
                {
                    "symbol": symbol,
                    "price": metric.close,
                    "ret_1m": metric.ret_1m,
                    "ret_10m": metric.ret_10m,
                    "rolling_vol_20": metric.rolling_vol_20,
                    "volume_zscore": metric.volume_zscore,
                    "updated_at": metric.ts,
                }
            )
            continue

        latest_candle = get_latest_ohlcv(db, symbol=symbol, timeframe="1m")
        if latest_candle is None:
            snapshots.append(
                {
                    "symbol": symbol,
                    "price": None,
                    "ret_1m": None,
                    "ret_10m": None,
                    "rolling_vol_20": None,
                    "volume_zscore": None,
                    "updated_at": None,
                }
            )
            continue

        snapshots.append(
            {
                "symbol": symbol,
                "price": latest_candle.close,
                "ret_1m": None,
                "ret_10m": None,
                "rolling_vol_20": None,
                "volume_zscore": None,
                "updated_at": latest_candle.ts,
            }
        )

    return snapshots


def _quick_db_health_and_worker(database_url: str, worker_id: str) -> tuple[bool, datetime | None]:
    return quick_db_health_and_worker(database_url, worker_id)


def _quick_generic_health_and_worker(worker_id: str) -> tuple[bool, datetime | None]:
    db_ok = False
    worker_last_seen = None
    with SessionLocal() as db:
        try:
            db.execute(text("SELECT 1"))
            db_ok = True
        except Exception:
            db_ok = False

        if db_ok:
            try:
                worker_last_seen = get_worker_last_seen(db, worker_id=worker_id)
            except Exception:
                worker_last_seen = None
    return db_ok, worker_last_seen


def _is_sqlite_url(database_url: str) -> bool:
    try:
        parsed = make_url(database_url)
        return parsed.get_backend_name() == "sqlite"
    except Exception:
        return database_url.startswith("sqlite")


def _resolve_sqlite_path(database_url: str) -> str:
    parsed = make_url(database_url)
    database = parsed.database or ""
    if database == ":memory:":
        return database
    if database.startswith("./"):
        database = database[2:]
    return str((Path.cwd() / database).resolve()) if database and not Path(database).is_absolute() else database


def _quick_sqlite_health_and_worker(database_url: str, worker_id: str) -> tuple[bool, datetime | None]:
    db_path = _resolve_sqlite_path(database_url)
    if not db_path or db_path == ":memory:":
        return False, None

    db_ok = False
    worker_last_seen = None
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=0.1)
        conn.execute("PRAGMA busy_timeout = 100")
        conn.execute("SELECT 1").fetchone()
        db_ok = True
        try:
            row = conn.execute(
                "SELECT last_seen FROM worker_status WHERE worker_id = ? ORDER BY last_seen DESC LIMIT 1",
                (worker_id,),
            ).fetchone()
            if row and row[0]:
                worker_last_seen = _parse_datetime(row[0])
        except sqlite3.OperationalError:
            worker_last_seen = None
    except sqlite3.OperationalError:
        db_ok = False
    except Exception:
        db_ok = False
    finally:
        if conn is not None:
            conn.close()
    return db_ok, worker_last_seen


def _parse_datetime(raw_value: str | datetime) -> datetime | None:
    if isinstance(raw_value, datetime):
        return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

