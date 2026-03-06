"""Core scheduler jobs: supervised wrapper and heartbeat."""
from __future__ import annotations

import asyncio
import time
from typing import Any

from app.db.repository import upsert_worker_status
from app.logging import logger
from app.ops.job_metrics import append_job_metric
from app.scheduler.runtime import WorkerRuntime
from app.worker.llm_hot_reload import maybe_reload_llm_runtime_from_signal
from app.worker.runtime_guard import touch_worker_identity_lease


async def supervised_job(job_name: str, coro_func, runtime: WorkerRuntime) -> None:
    started = time.perf_counter()
    status = "ok"
    error_type: str | None = None
    metric_payload: dict[str, Any] = {}
    try:
        result = await coro_func(runtime)
        if isinstance(result, dict):
            metric_payload = result
    except asyncio.CancelledError:
        status = "cancelled"
        error_type = "CancelledError"
        raise
    except Exception as exc:
        status = "error"
        error_type = type(exc).__name__
        logger.exception("Job %s failed: %s", job_name, exc)
    finally:
        duration_ms = int((time.perf_counter() - started) * 1000)
        append_job_metric(
            runtime,
            {
                "job_name": job_name,
                "status": status,
                "duration_ms": duration_ms,
                "rows_written": metric_payload.get("rows_written"),
                "rows_read": metric_payload.get("rows_read"),
                "backlog": metric_payload.get("backlog"),
                "error_type": error_type,
            },
        )


async def heartbeat_job(runtime: WorkerRuntime) -> None:
    if not getattr(runtime.settings, "llm_hot_reload_use_redis", True):
        await maybe_reload_llm_runtime_from_signal(runtime)
    try:
        await touch_worker_identity_lease(runtime.settings)
    except Exception as exc:
        logger.warning("Worker identity lease refresh failed: %s", exc)
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    with runtime.session_factory() as session:
        upsert_worker_status(
            session,
            worker_id=runtime.settings.worker_id,
            started_at=runtime.started_at,
            last_seen=now,
            version=runtime.version,
        )
