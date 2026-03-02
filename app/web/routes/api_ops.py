from __future__ import annotations

from fastapi import APIRouter, Query

from app.config import get_settings
from app.ops.job_metrics import read_job_metrics_from_file

router = APIRouter()


@router.get("/api/ops/jobs")
def ops_jobs_api(limit: int = Query(default=50, ge=1, le=500)):
    settings = get_settings()
    items = read_job_metrics_from_file(settings.ops_job_metrics_file, limit=limit)
    return {
        "ok": True,
        "count": len(items),
        "items": items,
    }

