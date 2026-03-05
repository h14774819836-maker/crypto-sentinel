"""YouTube-specific helpers for status derivation and queue summary."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.db.repository import get_worker_last_seen
from app.ops.job_metrics import read_job_metrics_from_file
from app.web.shared import settings
from app.web.utils import _to_utc_or_none

YT_ANALYSIS_STALL_RUNNING_SECONDS_DEFAULT = 420
YT_ANALYSIS_STALL_WAITING_SECONDS_MIN = 420

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
    from app.web.utils import _parse_utc_datetime

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


def _youtube_scheduler_snapshot(now_utc: datetime | None = None) -> dict[str, Any]:
    from app.web.utils import _parse_utc_datetime

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
