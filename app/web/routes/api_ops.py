from __future__ import annotations

import json
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.config import get_settings
from app.runtime_control import (
    is_docker_compose_runtime,
    read_runtime_state,
    read_runtime_stop_request,
    request_runtime_stop,
)
from app.web.auth import require_admin

router = APIRouter()


def _docker_runtime_payload() -> dict[str, Any]:
    return {
        "mode": "docker_compose",
        "host": os.environ.get("APP_RUNTIME_HOST") or "127.0.0.1",
        "port": int(os.environ.get("APP_RUNTIME_PORT") or "8000"),
    }


def _schedule_current_process_shutdown(delay_seconds: float) -> None:
    def _worker() -> None:
        time.sleep(max(0.0, float(delay_seconds)))
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            os._exit(0)

    threading.Thread(target=_worker, name="runtime_shutdown", daemon=True).start()


def _read_metrics_file(path_str: str, *, limit: int) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    if not path_str:
        return [], ["metrics path is empty"]

    path = Path(path_str).expanduser().resolve()
    if not path.exists():
        return [], [f"metrics file not found: {path}"]

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], [f"metrics file read/parse failed: {path} error={exc}"]

    if not isinstance(raw, list):
        return [], [f"metrics file payload is not a list: {path}"]

    items = [item for item in raw if isinstance(item, dict)]
    if limit > 0:
        items = items[-limit:]
    return items, warnings


@router.get("/api/ops/jobs")
def ops_jobs_api(
    limit: int = Query(default=50, ge=1, le=500),
    worker: str = Query(default="core"),
):
    worker_norm = (worker or "core").strip().lower()
    if worker_norm not in {"core", "ai", "all"}:
        raise HTTPException(status_code=400, detail="worker must be one of: core, ai, all")

    settings = get_settings()
    warnings: list[str] = []

    metrics_map = dict(settings.ops_job_metrics_files_map)
    if settings.ops_job_metrics_files_json and not metrics_map:
        warnings.append("OPS_JOB_METRICS_FILES_JSON is invalid; fallback to OPS_JOB_METRICS_FILE")

    if not metrics_map and settings.ops_job_metrics_file:
        metrics_map = {"core": settings.ops_job_metrics_file}

    if worker_norm == "all":
        merged: list[dict[str, Any]] = []
        for wk, path in sorted(metrics_map.items()):
            items, file_warnings = _read_metrics_file(path, limit=limit)
            warnings.extend([f"{wk}: {msg}" for msg in file_warnings])
            merged.extend([{**item, "worker": wk} for item in items])
        if limit > 0:
            merged = merged[-limit:]
        return {
            "ok": True,
            "worker": "all",
            "count": len(merged),
            "items": merged,
            "warnings": warnings,
        }

    selected_path = metrics_map.get(worker_norm)
    if not selected_path and worker_norm == "core":
        selected_path = settings.ops_job_metrics_file

    if not selected_path:
        warnings.append(f"metrics path not configured for worker={worker_norm}")
        return {
            "ok": True,
            "worker": worker_norm,
            "count": 0,
            "items": [],
            "warnings": warnings,
        }

    items, file_warnings = _read_metrics_file(selected_path, limit=limit)
    warnings.extend(file_warnings)
    return {
        "ok": True,
        "worker": worker_norm,
        "count": len(items),
        "items": items,
        "warnings": warnings,
    }


@router.get("/api/ops/runtime")
def ops_runtime_api(_admin: str = Depends(require_admin)):
    docker_mode = is_docker_compose_runtime()
    runtime = None if docker_mode else read_runtime_state()
    stop_request = read_runtime_stop_request()
    if docker_mode:
        runtime = _docker_runtime_payload()
    return {
        "ok": True,
        "runtime": runtime,
        "stop_request": stop_request,
        "registered": runtime is not None,
    }


@router.post("/api/ops/runtime/shutdown")
def ops_runtime_shutdown_api(
    body: dict[str, Any] | None = Body(default=None),
    _admin: str = Depends(require_admin),
):
    docker_mode = is_docker_compose_runtime()
    runtime = None if docker_mode else read_runtime_state()
    if runtime is None and not docker_mode:
        raise HTTPException(status_code=409, detail="No supervised local runtime is registered")

    payload = body or {}
    raw_delay = payload.get("delay_seconds", 1.0)
    try:
        delay_seconds = max(0.0, min(10.0, float(raw_delay)))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="delay_seconds must be a number") from None

    reason = str(payload.get("reason") or "ui_shutdown").strip()[:200] or "ui_shutdown"
    stop_request = request_runtime_stop(
        reason=reason,
        requested_by="api",
        delay_seconds=delay_seconds,
    )
    runtime_payload = runtime
    if docker_mode:
        runtime_payload = _docker_runtime_payload()
        _schedule_current_process_shutdown(delay_seconds + 0.5)
    return {
        "ok": True,
        "message": "Shutdown requested",
        "runtime": {
            "mode": runtime_payload.get("mode"),
            "host": runtime_payload.get("host"),
            "port": runtime_payload.get("port"),
        },
        "stop_request": stop_request,
    }
