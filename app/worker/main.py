from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from app.alerts.telegram import TelegramClient
from app.alerts.telegram_dispatcher import process_telegram_update
from app.alerts.telegram_poller import TelegramPoller
from app.config import get_settings
from app.db.guards import ensure_db_backend_allowed
from app.db.session import SessionLocal
from app.logging import logger, setup_logging
from app.providers.binance_provider import BinanceProvider
from app.scheduler.jobs import account_user_stream_job, startup_backfill_job, supervised_job, ws_consumer_job
from app.scheduler.runtime import WorkerRuntime
from app.scheduler.scheduler import build_scheduler
from app.worker.llm_hot_reload import run_llm_reload_subscriber
from app.worker.runtime_guard import (
    ensure_split_worker_runtime_constraints,
    is_split_worker_role,
    release_worker_identity_lease,
    reserve_worker_identity_lease,
)


async def run_worker() -> None:
    setup_logging()

    settings = get_settings()
    ensure_db_backend_allowed(settings)
    worker_role = settings.worker_role_normalized
    is_core_worker = worker_role in {"core", "all"}
    is_ai_worker = worker_role in {"ai", "all"}
    await ensure_split_worker_runtime_constraints(settings)
    await reserve_worker_identity_lease(settings)

    try:
        market_analyst = None
        market_config = settings.resolve_llm_config("market")
        market_profile_name = settings.resolve_llm_profile_name("market")
        if is_ai_worker and market_config.enabled and market_config.api_key:
            from app.ai.analyst import MarketAnalyst
            from app.ai.openai_provider import OpenAICompatibleProvider

            market_provider = OpenAICompatibleProvider(market_config)
            market_analyst = MarketAnalyst(settings, market_provider, market_config)
            logger.info(
                "Market Analyst enabled (task=market, profile=%s, provider=%s, model=%s, base_url=%s)",
                market_profile_name,
                market_config.provider,
                market_config.model,
                market_config.base_url,
            )
        else:
            logger.info(
                "Market Analyst disabled (task=market, profile=%s, enabled=%s, api_key_present=%s)",
                market_profile_name,
                market_config.enabled,
                bool(market_config.api_key),
            )

        youtube_provider = None
        youtube_config = settings.resolve_llm_config("youtube") if (is_core_worker and settings.youtube_enabled) else None
        if is_core_worker and settings.youtube_enabled:
            youtube_profile_name = settings.resolve_llm_profile_name("youtube")
            model_lower = (youtube_config.model or "").lower()
            if "reasoner" in model_lower or model_lower.endswith("/deepseek-r1") or model_lower.endswith("deepseek-r1"):
                logger.warning(
                    "YouTube profile uses a reasoning model (%s). Structured JSON stability may be lower; "
                    "recommend deepseek-chat or another non-reasoner model for youtube task.",
                    youtube_config.model,
                )
            if youtube_config.enabled and youtube_config.api_key:
                from app.ai.openai_provider import OpenAICompatibleProvider

                youtube_provider = OpenAICompatibleProvider(youtube_config)
                logger.info(
                    "YouTube AI Summarization enabled (task=youtube, profile=%s, provider=%s, model=%s, base_url=%s)",
                    youtube_profile_name,
                    youtube_config.provider,
                    youtube_config.model,
                    youtube_config.base_url,
                )
            else:
                logger.info(
                    "YouTube AI Summarization disabled (task=youtube, profile=%s, enabled=%s, api_key_present=%s)",
                    youtube_profile_name,
                    youtube_config.enabled,
                    bool(youtube_config.api_key),
                )
        elif settings.youtube_enabled:
            logger.info("YouTube jobs disabled for worker_role=%s", worker_role)

        shared_http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
        runtime = WorkerRuntime(
            settings=settings,
            session_factory=SessionLocal,
            provider=BinanceProvider(settings),
            telegram=TelegramClient(settings),
            started_at=datetime.now(timezone.utc),
            version=settings.app_version,
            market_analyst=market_analyst,
            youtube_llm_provider=youtube_provider,
            http_client=shared_http_client,
            sem_llm=asyncio.Semaphore(max(1, int(getattr(market_config, "max_concurrency", 2) or 2))),
            sem_youtube=(
                asyncio.Semaphore(max(1, int(getattr(youtube_config, "max_concurrency", 2) or 2)))
                if youtube_config
                else asyncio.Semaphore(1)
            ),
            sem_binance=asyncio.Semaphore(4),
        )

        tg_poller: TelegramPoller | None = None
        tg_poller_task: asyncio.Task | None = None
        inbound_mode = settings.telegram_inbound_mode_normalized
        if is_core_worker and settings.telegram_enabled and settings.telegram_bot_token and inbound_mode == "polling":
            tg_poller = TelegramPoller(
                token=settings.telegram_bot_token,
                handle_update=process_telegram_update,
                timeout_seconds=settings.telegram_polling_timeout_seconds,
                interval_seconds=settings.telegram_polling_interval_seconds,
                state_file=settings.telegram_polling_state_file,
                auto_delete_webhook=settings.telegram_polling_auto_delete_webhook,
                drop_pending_updates=settings.telegram_polling_drop_pending_updates,
            )
            tg_poller_task = asyncio.create_task(tg_poller.run_forever(), name="telegram_poller")
            logger.warning(
                "[TG杞] 宸插湪 Worker 鍚姩 Telegram 杞浠诲姟 mode=%s timeout=%s interval=%s",
                inbound_mode,
                settings.telegram_polling_timeout_seconds,
                settings.telegram_polling_interval_seconds,
            )
        else:
            logger.warning(
                "[TG杞] 鏈惎鍔?Telegram 杞浠诲姟 enabled=%s token_present=%s mode=%s",
                settings.telegram_enabled,
                bool(settings.telegram_bot_token),
                inbound_mode,
            )

        if is_core_worker:
            try:
                await startup_backfill_job(runtime)
            except Exception as exc:
                logger.warning("Startup backfill failed for worker_role=%s: %s", worker_role, exc, exc_info=True)
        else:
            logger.info("Skip startup_backfill for worker_role=%s", worker_role)

        scheduler = build_scheduler(runtime)
        scheduler.start()

        ws_task: asyncio.Task | None = None
        if is_core_worker:
            ws_task = asyncio.create_task(supervised_job("ws_consumer_job", ws_consumer_job, runtime))
        llm_reload_task: asyncio.Task | None = None
        if getattr(settings, "llm_hot_reload_use_redis", True):
            llm_reload_task = asyncio.create_task(run_llm_reload_subscriber(runtime), name="llm_reload_subscriber")
        account_ws_task: asyncio.Task | None = None
        if is_core_worker and settings.account_user_stream_enabled:
            account_ws_task = asyncio.create_task(
                supervised_job("account_user_stream_job", account_user_stream_job, runtime),
                name="account_user_stream_job",
            )

        logger.info("Worker started role=%s worker_id=%s watchlist=%s", worker_role, settings.worker_id, settings.watchlist_symbols)
        try:
            while True:
                if llm_reload_task is not None and llm_reload_task.done():
                    task_exc = llm_reload_task.exception()
                    if task_exc is not None:
                        if is_split_worker_role(worker_role):
                            raise RuntimeError(f"LLM reload subscriber stopped unexpectedly: {task_exc}") from task_exc
                        logger.warning("LLM reload subscriber stopped unexpectedly in role=%s: %s", worker_role, task_exc)
                    llm_reload_task = None
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        finally:
            if tg_poller is not None:
                tg_poller.stop()
            if tg_poller_task is not None:
                tg_poller_task.cancel()
                await asyncio.gather(tg_poller_task, return_exceptions=True)
            if llm_reload_task is not None:
                llm_reload_task.cancel()
                await asyncio.gather(llm_reload_task, return_exceptions=True)
            if ws_task is not None:
                ws_task.cancel()
                await asyncio.gather(ws_task, return_exceptions=True)
            if account_ws_task is not None:
                account_ws_task.cancel()
                await asyncio.gather(account_ws_task, return_exceptions=True)
            scheduler.shutdown(wait=False)
            if runtime.http_client is not None:
                await runtime.http_client.aclose()
    finally:
        await release_worker_identity_lease(settings)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
