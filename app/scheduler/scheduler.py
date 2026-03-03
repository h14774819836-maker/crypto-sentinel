from __future__ import annotations

from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.scheduler import jobs


def build_scheduler(runtime) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=runtime.settings.timezone)
    now = datetime.now(timezone.utc)

    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.worker_heartbeat_seconds,
        kwargs={"job_name": "heartbeat_job", "coro_func": jobs.heartbeat_job, "runtime": runtime},
        id="heartbeat_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=10,
        next_run_time=now,
    )

    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.gap_fill_interval_seconds,
        kwargs={"job_name": "gap_fill_job", "coro_func": jobs.gap_fill_job, "runtime": runtime},
        id="gap_fill_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
        next_run_time=now,
    )

    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.kline_sync_seconds,
        kwargs={"job_name": "feature_job", "coro_func": jobs.feature_job, "runtime": runtime},
        id="feature_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
        next_run_time=now,
    )

    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.poll_seconds,
        kwargs={"job_name": "anomaly_job", "coro_func": jobs.anomaly_job, "runtime": runtime},
        id="anomaly_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=15,
        next_run_time=now,
    )

    # --- Multi-timeframe sync (5m/15m aggregation + 1h/4h REST fetch + indicators) ---
    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.multi_tf_sync_seconds,
        kwargs={"job_name": "multi_tf_sync_job", "coro_func": jobs.multi_tf_sync_job, "runtime": runtime},
        id="multi_tf_sync_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
        next_run_time=now,
    )

    # --- Funding rate snapshot (premiumIndex + openInterest) ---
    scheduler.add_job(
        jobs.supervised_job,
        "interval",
        seconds=runtime.settings.funding_rate_sync_seconds,
        kwargs={"job_name": "funding_rate_job", "coro_func": jobs.funding_rate_job, "runtime": runtime},
        id="funding_rate_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
        next_run_time=now,
    )

    # --- Intel news ingest/digest ---
    if runtime.settings.intel_enabled:
        scheduler.add_job(
            jobs.supervised_job,
            "interval",
            seconds=runtime.settings.intel_poll_seconds,
            kwargs={"job_name": "intel_news_job", "coro_func": jobs.intel_news_job, "runtime": runtime},
            id="intel_news_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            next_run_time=now,
        )

        from datetime import timedelta as _td

        scheduler.add_job(
            jobs.supervised_job,
            "interval",
            seconds=runtime.settings.intel_digest_poll_seconds,
            kwargs={"job_name": "intel_digest_job", "coro_func": jobs.intel_digest_job, "runtime": runtime},
            id="intel_digest_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            next_run_time=now + _td(seconds=30),
        )

    # --- AI analysis (with multi-TF + funding data) ---
    market_config = runtime.settings.resolve_llm_config("market")
    if market_config.enabled and getattr(runtime, "market_analyst", None) is not None:
        scheduler.add_job(
            jobs.supervised_job,
            "interval",
            seconds=runtime.settings.ai_analysis_interval_seconds,
            kwargs={"job_name": "ai_analysis_job", "coro_func": jobs.ai_analysis_job, "runtime": runtime},
            id="ai_analysis_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )

    # --- YouTube MVP ---
    if runtime.settings.youtube_enabled:
        scheduler.add_job(
            jobs.supervised_job,
            "interval",
            seconds=runtime.settings.youtube_poll_seconds,
            kwargs={"job_name": "youtube_sync_job", "coro_func": jobs.youtube_sync_job, "runtime": runtime},
            id="youtube_sync_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            next_run_time=now,
        )

        # YouTube AI analysis (delay 60s after sync)
        from datetime import timedelta as _td
        scheduler.add_job(
            jobs.supervised_job,
            "interval",
            seconds=runtime.settings.youtube_analyze_poll_seconds_effective,
            kwargs={"job_name": "youtube_analyze_job", "coro_func": jobs.youtube_analyze_job, "runtime": runtime},
            id="youtube_analyze_job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            next_run_time=now + _td(seconds=60),
        )

        # ASR backfill (only when ASR is enabled)
        if runtime.settings.asr_enabled:
            scheduler.add_job(
                jobs.supervised_job,
                "interval",
                seconds=runtime.settings.youtube_asr_backfill_seconds_effective,
                kwargs={"job_name": "youtube_asr_backfill_job", "coro_func": jobs.youtube_asr_backfill_job, "runtime": runtime},
                id="youtube_asr_backfill_job",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                next_run_time=now + _td(seconds=90),
            )

    return scheduler
