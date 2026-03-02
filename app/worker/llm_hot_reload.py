from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ai.llm_runtime_reload import read_llm_reload_signal, refresh_llm_env_vars_from_dotenv, write_llm_reload_ack
from app.config import get_settings
from app.logging import logger


def _summarize_llm_task(settings, task: str) -> dict[str, Any]:
    try:
        cfg = settings.resolve_llm_config(task)
        profile = settings.resolve_llm_profile_name(task)
        return {
            "task": task,
            "profile": profile,
            "enabled": bool(cfg.enabled),
            "provider": cfg.provider,
            "model": cfg.model,
            "base_url": cfg.base_url,
            "api_key_present": bool(cfg.api_key),
            "use_reasoning": cfg.use_reasoning,
        }
    except Exception as exc:
        return {"task": task, "error": str(exc)}


def _build_market_analyst_from_settings(settings):
    config = settings.resolve_llm_config("market")
    profile_name = settings.resolve_llm_profile_name("market")
    if not config.enabled or not config.api_key:
        logger.warning(
            "[LLM热更新][worker] market 未启用或缺少 API Key profile=%s enabled=%s api_key_present=%s",
            profile_name,
            config.enabled,
            bool(config.api_key),
        )
        return None

    from app.ai.analyst import MarketAnalyst
    from app.ai.openai_provider import OpenAICompatibleProvider

    provider = OpenAICompatibleProvider(config)
    analyst = MarketAnalyst(settings, provider, config)
    logger.warning(
        "[LLM热更新][worker] market 已重建 profile=%s provider=%s model=%s base_url=%s",
        profile_name,
        config.provider,
        config.model,
        config.base_url,
    )
    return analyst


def _build_youtube_provider_from_settings(settings):
    if not settings.youtube_enabled:
        logger.warning("[LLM热更新][worker] youtube_enabled=false，跳过 YouTube LLM 重建")
        return None

    config = settings.resolve_llm_config("youtube")
    profile_name = settings.resolve_llm_profile_name("youtube")
    if not config.enabled or not config.api_key:
        logger.warning(
            "[LLM热更新][worker] youtube LLM 未启用或缺少 API Key profile=%s enabled=%s api_key_present=%s",
            profile_name,
            config.enabled,
            bool(config.api_key),
        )
        return None

    from app.ai.openai_provider import OpenAICompatibleProvider

    provider = OpenAICompatibleProvider(config)
    logger.warning(
        "[LLM热更新][worker] youtube LLM 已重建 profile=%s provider=%s model=%s base_url=%s",
        profile_name,
        config.provider,
        config.model,
        config.base_url,
    )
    return provider


def apply_llm_config_to_worker_runtime(runtime) -> dict[str, Any]:
    """Reload cached Settings in worker process and rebuild long-lived LLM components."""
    refresh_llm_env_vars_from_dotenv()
    get_settings.cache_clear()
    new_settings = get_settings()

    runtime.settings = new_settings
    runtime.market_analyst = _build_market_analyst_from_settings(new_settings)
    runtime.youtube_llm_provider = _build_youtube_provider_from_settings(new_settings)

    details = {
        "applied_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "task_routing": dict(new_settings.llm_task_routing),
        "market": _summarize_llm_task(new_settings, "market"),
        "youtube": _summarize_llm_task(new_settings, "youtube"),
        "telegram_chat": _summarize_llm_task(new_settings, "telegram_chat"),
    }
    logger.warning(
        "[LLM热更新][worker] 已应用新配置 routing=%s market=%s youtube=%s",
        details["task_routing"],
        details["market"],
        details["youtube"],
    )
    return details


async def maybe_reload_llm_runtime_from_signal(runtime) -> None:
    runtime.llm_reload_last_check_ts = datetime.now(timezone.utc)
    settings = runtime.settings
    signal = read_llm_reload_signal(settings.llm_hot_reload_signal_file)
    if not signal:
        return

    revision = str(signal.get("revision") or "").strip()
    if not revision:
        return
    if revision == runtime.llm_reload_revision_applied:
        return

    logger.warning("[LLM热更新][worker] 检测到新 signal revision=%s，开始应用", revision)
    ack_file = settings.llm_hot_reload_ack_file
    try:
        details = apply_llm_config_to_worker_runtime(runtime)
    except Exception as exc:
        logger.exception("[LLM热更新][worker] 应用失败 revision=%s error=%s", revision, exc)
        write_llm_reload_ack(
            ack_file,
            revision=revision,
            status="error",
            details={"error": str(exc)},
        )
        return

    runtime.llm_reload_revision_applied = revision
    ack_file = runtime.settings.llm_hot_reload_ack_file
    write_llm_reload_ack(ack_file, revision=revision, status="ok", details=details)
