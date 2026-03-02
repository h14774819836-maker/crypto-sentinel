from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import dotenv_values

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"
_LLM_ENV_PREFIXES = ("LLM_",)
_LLM_ENV_KEYS = {
    "DEEPSEEK_API_KEY",
    "OPENROUTER_API_KEY",
    "OPENAI_API_KEY",
    "ARK_API_KEY",
    "TELEGRAM_ALERT_TEMPLATE_STYLE",
    "TELEGRAM_ALERT_INCLUDE_DEBUG",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_parent_dir(path_str: str) -> None:
    parent = Path(path_str).expanduser().resolve().parent
    parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path_str: str, payload: dict[str, Any]) -> None:
    _ensure_parent_dir(path_str)
    path = Path(path_str).expanduser().resolve()
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _read_json_file(path_str: str) -> dict[str, Any] | None:
    path = Path(path_str).expanduser().resolve()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        logger.warning("[LLM热更新] JSON 解析失败 path=%s error=%s", path, exc)
        return None
    except OSError as exc:
        logger.warning("[LLM热更新] 读取文件失败 path=%s error=%s", path, exc)
        return None
    return data if isinstance(data, dict) else None


def refresh_llm_env_vars_from_dotenv(env_path: str | None = None) -> None:
    """Refresh only LLM-related env vars from .env into current process env.

    Pydantic Settings reads process env before .env. UI writes .env at runtime,
    so we need to sync changed LLM keys into os.environ before cache_clear().
    """
    path = str(Path(env_path).expanduser().resolve()) if env_path else str(ENV_PATH)
    try:
        values = dotenv_values(path)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("[LLM热更新] 刷新进程环境变量失败 path=%s error=%s", path, exc)
        return

    for key, value in values.items():
        if value is None:
            continue
        if key in _LLM_ENV_KEYS or any(key.startswith(prefix) for prefix in _LLM_ENV_PREFIXES):
            os.environ[key] = str(value)


def apply_llm_config_in_api_process() -> Settings:
    """Reload cached settings and refresh module-level settings refs used by API routes."""
    refresh_llm_env_vars_from_dotenv()
    get_settings.cache_clear()
    refreshed_settings = get_settings()
    refreshed_modules: list[str] = []

    try:
        import app.web.views as web_views

        web_views.settings = refreshed_settings
        refreshed_modules.append("app.web.views")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("[LLM热更新][API] 刷新 app.web.views.settings 失败: %s", exc)

    try:
        import app.web.router as web_router

        web_router.settings = refreshed_settings
        refreshed_modules.append("app.web.router")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("[LLM热更新][API] 刷新 app.web.router.settings 失败: %s", exc)

    try:
        import app.web.api_telegram as api_telegram

        api_telegram.settings = refreshed_settings
        refreshed_modules.append("app.web.api_telegram")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("[LLM热更新][API] 刷新 app.web.api_telegram.settings 失败: %s", exc)

    logger.warning("[LLM热更新][API] 已刷新配置缓存 modules=%s", refreshed_modules)
    return refreshed_settings


def write_llm_reload_signal(signal_file: str, *, source: str = "llm_ui", reason: str = "config_saved") -> str:
    revision = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
    payload = {
        "revision": revision,
        "requested_at": _utc_now_iso(),
        "source": source,
        "reason": reason,
    }
    _atomic_write_json(signal_file, payload)
    logger.warning("[LLM热更新] 已写入 signal revision=%s path=%s", revision, signal_file)
    return revision


def read_llm_reload_signal(signal_file: str) -> dict[str, Any] | None:
    return _read_json_file(signal_file)


def write_llm_reload_ack(ack_file: str, revision: str, status: str, details: dict[str, Any] | None = None) -> None:
    payload = {
        "revision": revision,
        "applied_at": _utc_now_iso(),
        "status": status,
        "details": details or {},
    }
    _atomic_write_json(ack_file, payload)
    logger.warning("[LLM热更新] 已写入 ACK revision=%s status=%s path=%s", revision, status, ack_file)


def read_llm_reload_ack(ack_file: str) -> dict[str, Any] | None:
    return _read_json_file(ack_file)
