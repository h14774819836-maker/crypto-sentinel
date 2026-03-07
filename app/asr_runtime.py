from __future__ import annotations

import importlib.util
import shutil
from typing import Any

from app.config import Settings
from app.logging import logger
from app.runtime_control import runtime_mode_from_env

_SUPPORTED_BACKENDS = {"local_faster_whisper"}
_SUPPORTED_COMPUTE_TYPES = {"default", "auto", "float16", "float32", "int8", "int8_float16", "int8_float32"}


def _cuda_device_count() -> int | None:
    try:
        import ctranslate2  # type: ignore
    except Exception:
        return None

    probe = getattr(ctranslate2, "get_cuda_device_count", None)
    if not callable(probe):
        return None
    try:
        return int(probe())
    except Exception:
        return None


def inspect_asr_runtime(settings: Settings) -> dict[str, Any]:
    backend = str(settings.asr_backend or "").strip().lower() or "local_faster_whisper"
    device = str(settings.asr_device or "").strip().lower() or "cpu"
    compute_type = str(settings.asr_compute_type or "").strip().lower() or "default"
    runtime_mode = runtime_mode_from_env() or "local_process"
    ffmpeg_available = shutil.which("ffmpeg") is not None
    faster_whisper_available = importlib.util.find_spec("faster_whisper") is not None
    issues: list[str] = []

    cuda_device_count: int | None = None
    if settings.asr_enabled and device == "cuda":
        cuda_device_count = _cuda_device_count()

    if not settings.asr_enabled:
        issues.append("ASR disabled by config")
        status = "disabled"
    else:
        status = "ready"
        if backend not in _SUPPORTED_BACKENDS:
            issues.append(f"unsupported backend={backend}")
        if not faster_whisper_available:
            issues.append("faster-whisper package missing")
        if not ffmpeg_available:
            issues.append("ffmpeg binary missing")
        if device == "cuda" and (cuda_device_count is None or cuda_device_count < 1):
            issues.append("cuda requested but no CUDA device detected")
        elif device not in {"cpu", "cuda", "auto"}:
            issues.append(f"unrecognized device={device}")
        if compute_type not in _SUPPORTED_COMPUTE_TYPES:
            issues.append(f"unrecognized compute_type={compute_type}")
        if issues:
            status = "degraded"

    summary = {
        "ready": "ASR runtime ready",
        "degraded": "ASR runtime degraded",
        "disabled": "ASR runtime disabled",
    }[status]

    return {
        "status": status,
        "enabled": bool(settings.asr_enabled),
        "backend": backend,
        "model": settings.asr_model,
        "device": device,
        "compute_type": compute_type,
        "runtime_mode": runtime_mode,
        "ffmpeg_available": ffmpeg_available,
        "faster_whisper_available": faster_whisper_available,
        "cuda_device_count": cuda_device_count,
        "issues": issues,
        "summary": summary,
    }


def log_asr_runtime_status(settings: Settings, *, component: str) -> dict[str, Any]:
    status = inspect_asr_runtime(settings)
    log_message = (
        "ASR self-check %s component=%s runtime_mode=%s backend=%s model=%s "
        "device=%s compute_type=%s ffmpeg=%s faster_whisper=%s cuda_devices=%s issues=%s"
    )
    issues_text = "; ".join(status["issues"]) if status["issues"] else "-"
    log_args = (
        status["status"],
        component,
        status["runtime_mode"],
        status["backend"],
        status["model"],
        status["device"],
        status["compute_type"],
        status["ffmpeg_available"],
        status["faster_whisper_available"],
        status["cuda_device_count"],
        issues_text,
    )
    if status["status"] == "degraded":
        logger.warning(log_message, *log_args)
    else:
        logger.info(log_message, *log_args)
    return status
