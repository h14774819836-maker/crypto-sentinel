from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from app.ai.llm_runtime_reload import (
    read_llm_reload_signal,
    refresh_llm_env_vars_from_dotenv,
    write_llm_reload_ack,
    write_llm_reload_ack_redis,
)
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
        "ai_analysis": {
            "two_stage_enabled": bool(getattr(new_settings, "ai_two_stage_enabled", True)),
            "scan_confidence_threshold": int(getattr(new_settings, "ai_scan_confidence_threshold", 60) or 60),
            "min_context_on_poor_data": bool(getattr(new_settings, "ai_min_context_on_poor_data", True)),
            "min_context_on_non_tradeable": bool(getattr(new_settings, "ai_min_context_on_non_tradeable", True)),
            "external_views_on_low_conf_only": bool(getattr(new_settings, "ai_external_views_on_low_conf_only", True)),
        },
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


async def _run_llm_reload_poll_loop(runtime, poll_seconds: float) -> None:
    while True:
        await maybe_reload_llm_runtime_from_signal(runtime)
        await asyncio.sleep(max(1.0, poll_seconds))


def _redis_available(redis_url: str) -> bool:
    """Quick check if Redis is reachable. Returns False on any connection error."""
    try:
        import redis
        r = redis.from_url(redis_url, socket_connect_timeout=2)
        r.ping()
        r.close()
        return True
    except Exception:
        return False


async def run_llm_reload_subscriber(runtime) -> None:
    """Subscribe to llm:reload Redis channel and apply config on message. Runs until cancelled."""
    settings = runtime.settings
    poll_seconds = float(getattr(settings, "llm_hot_reload_poll_seconds", 5) or 5)
    if not getattr(settings, "llm_hot_reload_use_redis", True):
        logger.info("[LLM热更新][worker] 使用文件轮询模式 (LLM_HOT_RELOAD_USE_REDIS=false)")
        await _run_llm_reload_poll_loop(runtime, poll_seconds)
        return
    if not _redis_available(settings.redis_url):
        logger.warning(
            "[LLM热更新][worker] Redis 不可用 (url=%s)，回退到文件轮询模式",
            settings.redis_url,
        )
        await _run_llm_reload_poll_loop(runtime, poll_seconds)
        return
    client = None
    pubsub = None
    try:
        from redis.asyncio import Redis

        client = Redis.from_url(settings.redis_url)
        pubsub = client.pubsub()
        await pubsub.subscribe("llm:reload")
        logger.warning("[LLM热更新][worker] 已订阅 Redis channel=llm:reload worker_id=%s", settings.worker_id)
        async for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue
            data = message.get("data")
            if not data:
                continue
            try:
                payload = json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)
            except (json.JSONDecodeError, TypeError):
                logger.warning("[LLM热更新][worker] 无效 message data")
                continue
            revision = str(payload.get("revision") or "").strip()
            if not revision or revision == runtime.llm_reload_revision_applied:
                continue
            logger.warning("[LLM热更新][worker] 收到 Redis signal revision=%s，开始应用", revision)
            try:
                details = apply_llm_config_to_worker_runtime(runtime)
                runtime.llm_reload_revision_applied = revision
                write_llm_reload_ack_redis(
                    settings.redis_url,
                    settings.worker_id,
                    revision=revision,
                    status="ok",
                    details=details,
                )
            except Exception as exc:
                logger.exception("[LLM热更新][worker] 应用失败 revision=%s error=%s", revision, exc)
                write_llm_reload_ack_redis(
                    settings.redis_url,
                    settings.worker_id,
                    revision=revision,
                    status="error",
                    details={"error": str(exc)},
                )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("[LLM热更新][worker] Redis 订阅异常 error=%s", exc)
        await _run_llm_reload_poll_loop(runtime, poll_seconds)
    finally:
        try:
            if pubsub is not None:
                await pubsub.unsubscribe("llm:reload")
                await pubsub.aclose()
            if client is not None:
                await client.aclose()
        except Exception:
            pass
