from __future__ import annotations

from types import SimpleNamespace

from app.scheduler.scheduler import build_scheduler


class _Cfg(SimpleNamespace):
    pass


def _build_settings(role: str, *, market_enabled: bool = True) -> _Cfg:
    settings = _Cfg(
        timezone="UTC",
        worker_role_normalized=role,
        worker_heartbeat_seconds=15,
        gap_fill_interval_seconds=120,
        kline_sync_seconds=60,
        poll_seconds=10,
        multi_tf_sync_seconds=300,
        funding_rate_sync_seconds=300,
        account_monitor_enabled=False,
        account_user_stream_enabled=False,
        account_monitor_ws_fallback_seconds=60,
        account_monitor_seconds=15,
        account_daily_stats_enabled=False,
        intel_enabled=False,
        ai_analysis_interval_seconds=600,
        strategy_eval_job_seconds=120,
        strategy_scores_job_seconds=900,
        strategy_research_job_seconds=1800,
        youtube_enabled=False,
        youtube_poll_seconds=1800,
        youtube_analyze_poll_seconds_effective=180,
        asr_enabled=False,
        youtube_asr_backfill_seconds_effective=600,
    )
    settings.resolve_llm_config = lambda _task: SimpleNamespace(enabled=market_enabled)
    return settings


def _job_ids_for(role: str, *, market_enabled: bool = True, has_analyst: bool = True) -> set[str]:
    runtime = SimpleNamespace(
        settings=_build_settings(role, market_enabled=market_enabled),
        market_analyst=(object() if has_analyst else None),
        registered_job_ids=[],
    )
    scheduler = build_scheduler(runtime)
    assert set(runtime.registered_job_ids) == {job.id for job in scheduler.get_jobs()}
    return {job.id for job in scheduler.get_jobs()}


def test_scheduler_core_role_excludes_ai_jobs():
    ids = _job_ids_for("core", market_enabled=True, has_analyst=True)
    assert "heartbeat_job" in ids
    assert "gap_fill_job" in ids
    assert "feature_job" in ids
    assert "ai_analysis_job" not in ids
    assert "decision_eval_job" not in ids


def test_scheduler_ai_role_excludes_core_jobs():
    ids = _job_ids_for("ai", market_enabled=True, has_analyst=True)
    assert "heartbeat_job" in ids
    assert "ai_analysis_job" in ids
    assert "decision_eval_job" in ids
    assert "strategy_scores_job" in ids
    assert "gap_fill_job" not in ids
    assert "feature_job" not in ids


def test_scheduler_ai_role_keeps_market_gate_for_strategy_jobs():
    ids_disabled = _job_ids_for("ai", market_enabled=False, has_analyst=True)
    assert "ai_analysis_job" not in ids_disabled
    assert "decision_eval_job" not in ids_disabled
    assert "strategy_scores_job" not in ids_disabled

    ids_no_analyst = _job_ids_for("ai", market_enabled=True, has_analyst=False)
    assert "ai_analysis_job" not in ids_no_analyst
    assert "strategy_research_job" not in ids_no_analyst
