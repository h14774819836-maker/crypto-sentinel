from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.config import get_settings

router = APIRouter()


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
