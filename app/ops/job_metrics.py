from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path_str: str, payload: Any) -> None:
    path = Path(path_str).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def append_job_metric(runtime: Any, metric: dict[str, Any]) -> None:
    metric = {
        "ts_utc": _utc_now_iso(),
        **metric,
    }
    runtime.job_metrics.append(metric)
    max_items = int(getattr(runtime.settings, "ops_job_metrics_window", 200) or 200)
    if len(runtime.job_metrics) > max_items:
        runtime.job_metrics[:] = runtime.job_metrics[-max_items:]

    metrics_file = getattr(runtime.settings, "ops_job_metrics_file", "")
    if metrics_file:
        _atomic_write_json(metrics_file, runtime.job_metrics)


def read_job_metrics_from_file(path_str: str, *, limit: int = 100) -> list[dict[str, Any]]:
    path = Path(path_str).expanduser().resolve()
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    items: list[dict[str, Any]] = [item for item in raw if isinstance(item, dict)]
    if limit > 0:
        return items[-limit:]
    return items

