from __future__ import annotations

from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.logging import logger
from app.scheduler import jobs


def build_scheduler(runtime) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=runtime.settings.timezone)
    now = datetime.now(timezone.utc)
    worker_role = getattr(runtime.settings, "worker_role_normalized", "all")
    if worker_role not in {"all", "core", "ai"}:
        worker_role = "all"

    is_core_worker = worker_role in {"all", "core"}
    is_ai_worker = worker_role in {"all", "ai"}
    registered_job_ids: list[str] = []

    def _add_supervised_job(
        *,
        job_id: str,
        trigger: str,
        coro_func,
        max_instances: int,
        coalesce: bool,
        misfire_grace_time: int,
        next_run_time=None,
        **trigger_kwargs,
    ) -> None:
        scheduler.add_job(
            jobs.supervised_job,
            trigger,
            kwargs={"job_name": job_id, "coro_func": coro_func, "runtime": runtime},
            id=job_id,
            max_instances=max_instances,
            coalesce=coalesce,
            misfire_grace_time=misfire_grace_time,
            next_run_time=next_run_time,
            **trigger_kwargs,
        )
        registered_job_ids.append(job_id)

    _add_supervised_job(
        job_id="heartbeat_job",
        trigger="interval",
        seconds=runtime.settings.worker_heartbeat_seconds,
        coro_func=jobs.heartbeat_job,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=10,
        next_run_time=now,
    )

    if is_core_worker:
        _add_supervised_job(
            job_id="gap_fill_job",
            trigger="interval",
            seconds=runtime.settings.gap_fill_interval_seconds,
            coro_func=jobs.gap_fill_job,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
            next_run_time=now,
        )

        _add_supervised_job(
            job_id="feature_job",
            trigger="interval",
            seconds=runtime.settings.kline_sync_seconds,
            coro_func=jobs.feature_job,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
            next_run_time=now,
        )

        _add_supervised_job(
            job_id="anomaly_job",
            trigger="interval",
            seconds=runtime.settings.poll_seconds,
            coro_func=jobs.anomaly_job,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=15,
            next_run_time=now,
        )

        _add_supervised_job(
            job_id="multi_tf_sync_job",
            trigger="interval",
            seconds=runtime.settings.multi_tf_sync_seconds,
            coro_func=jobs.multi_tf_sync_job,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            next_run_time=now,
        )

        _add_supervised_job(
            job_id="funding_rate_job",
            trigger="interval",
            seconds=runtime.settings.funding_rate_sync_seconds,
            coro_func=jobs.funding_rate_job,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            next_run_time=now,
        )

        if runtime.settings.account_monitor_enabled:
            account_monitor_seconds = runtime.settings.account_monitor_seconds
            if runtime.settings.account_user_stream_enabled:
                account_monitor_seconds = max(
                    int(runtime.settings.account_monitor_ws_fallback_seconds),
                    int(account_monitor_seconds),
                )

            _add_supervised_job(
                job_id="account_monitor_job",
                trigger="interval",
                seconds=account_monitor_seconds,
                coro_func=jobs.account_monitor_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
                next_run_time=now,
            )

            if runtime.settings.account_daily_stats_enabled:
                _add_supervised_job(
                    job_id="account_daily_stats_rollup_job",
                    trigger="cron",
                    coro_func=jobs.account_daily_stats_rollup_job,
                    hour=0,
                    minute=1,
                    timezone="UTC",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=3600,
                )

        if runtime.settings.intel_enabled:
            _add_supervised_job(
                job_id="intel_news_job",
                trigger="interval",
                seconds=runtime.settings.intel_poll_seconds,
                coro_func=jobs.intel_news_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now,
            )

            _add_supervised_job(
                job_id="intel_digest_job",
                trigger="interval",
                seconds=runtime.settings.intel_digest_poll_seconds,
                coro_func=jobs.intel_digest_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now + timedelta(seconds=30),
            )

        if runtime.settings.youtube_enabled:
            _add_supervised_job(
                job_id="youtube_sync_job",
                trigger="interval",
                seconds=runtime.settings.youtube_poll_seconds,
                coro_func=jobs.youtube_sync_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now,
            )

            _add_supervised_job(
                job_id="youtube_analyze_job",
                trigger="interval",
                seconds=runtime.settings.youtube_analyze_poll_seconds_effective,
                coro_func=jobs.youtube_analyze_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now + timedelta(seconds=60),
            )

            if runtime.settings.asr_enabled:
                _add_supervised_job(
                    job_id="youtube_asr_backfill_job",
                    trigger="interval",
                    seconds=runtime.settings.youtube_asr_backfill_seconds_effective,
                    coro_func=jobs.youtube_asr_backfill_job,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                    next_run_time=now + timedelta(seconds=90),
                )

    ai_gate_enabled = False
    market_profile_enabled = None
    market_analyst_ready = None
    if is_ai_worker:
        market_config = runtime.settings.resolve_llm_config("market")
        market_profile_enabled = bool(market_config.enabled)
        market_analyst_ready = getattr(runtime, "market_analyst", None) is not None
        ai_gate_enabled = bool(market_profile_enabled and market_analyst_ready)
        if ai_gate_enabled:
            _add_supervised_job(
                job_id="ai_analysis_job",
                trigger="interval",
                seconds=runtime.settings.ai_analysis_interval_seconds,
                coro_func=jobs.ai_analysis_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
            )

            _add_supervised_job(
                job_id="decision_eval_job",
                trigger="interval",
                seconds=runtime.settings.strategy_eval_job_seconds,
                coro_func=jobs.decision_eval_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
                next_run_time=now,
            )

            _add_supervised_job(
                job_id="strategy_scores_job",
                trigger="interval",
                seconds=runtime.settings.strategy_scores_job_seconds,
                coro_func=jobs.strategy_scores_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now,
            )

            _add_supervised_job(
                job_id="strategy_research_job",
                trigger="interval",
                seconds=runtime.settings.strategy_research_job_seconds,
                coro_func=jobs.strategy_research_job,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
                next_run_time=now,
            )

    runtime.registered_job_ids = list(registered_job_ids)
    logger.info(
        "Scheduler built role=%s jobs=%s core_enabled=%s ai_enabled=%s ai_gate=%s "
        "market_profile_enabled=%s market_analyst_ready=%s",
        worker_role,
        registered_job_ids,
        is_core_worker,
        is_ai_worker,
        ai_gate_enabled,
        market_profile_enabled,
        market_analyst_ready,
    )
    return scheduler
