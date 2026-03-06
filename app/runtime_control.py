from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
RUNTIME_STATE_PATH = DATA_DIR / "runtime_state.json"
RUNTIME_STOP_SIGNAL_PATH = DATA_DIR / "runtime_stop_signal.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def read_runtime_state() -> dict[str, Any] | None:
    return _read_json(RUNTIME_STATE_PATH)


def write_runtime_state(payload: dict[str, Any]) -> None:
    _atomic_write_json(RUNTIME_STATE_PATH, payload)


def read_runtime_stop_request() -> dict[str, Any] | None:
    return _read_json(RUNTIME_STOP_SIGNAL_PATH)


def clear_runtime_stop_request() -> None:
    try:
        RUNTIME_STOP_SIGNAL_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def clear_runtime_state() -> None:
    try:
        RUNTIME_STATE_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    clear_runtime_stop_request()


def request_runtime_stop(
    *,
    reason: str,
    requested_by: str = "cli",
    delay_seconds: float = 0.0,
) -> dict[str, Any]:
    delay = max(0.0, float(delay_seconds or 0.0))
    payload = {
        "requested_at": _utc_now_iso(),
        "requested_by": (requested_by or "cli").strip() or "cli",
        "reason": (reason or "manual_stop").strip()[:200] or "manual_stop",
        "delay_seconds": delay,
        "effective_after_epoch": round(time.time() + delay, 3),
    }
    _atomic_write_json(RUNTIME_STOP_SIGNAL_PATH, payload)
    return payload


def should_honor_runtime_stop_request(payload: dict[str, Any] | None, *, now_ts: float | None = None) -> bool:
    if not isinstance(payload, dict):
        return False
    effective_after = payload.get("effective_after_epoch")
    try:
        threshold = float(effective_after)
    except (TypeError, ValueError):
        threshold = 0.0
    return (now_ts if now_ts is not None else time.time()) >= threshold


def extract_runtime_pids(payload: dict[str, Any] | None) -> list[int]:
    if not isinstance(payload, dict):
        return []

    pids: list[int] = []
    supervisor_pid = payload.get("supervisor_pid")
    if isinstance(supervisor_pid, int) and supervisor_pid > 0:
        pids.append(supervisor_pid)

    children = payload.get("children")
    if isinstance(children, dict):
        for item in children.values():
            if not isinstance(item, dict):
                continue
            child_pid = item.get("pid")
            if isinstance(child_pid, int) and child_pid > 0 and child_pid not in pids:
                pids.append(child_pid)
    return pids
