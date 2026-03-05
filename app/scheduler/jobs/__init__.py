"""Scheduler jobs package. Re-exports from _jobs_impl for backward compatibility.

Migration: Code will be gradually moved from _jobs_impl to submodules
(core, market, ai, youtube, intel, strategy, anomaly).
"""
from __future__ import annotations

from app.scheduler.jobs.core import heartbeat_job, supervised_job
from app.scheduler.jobs.intel import intel_digest_job, intel_news_job
from app.scheduler.jobs.strategy import decision_eval_job, strategy_research_job, strategy_scores_job
from app.scheduler._jobs_impl import (
    # Market / Ingest / Account
    ws_consumer_job,
    startup_backfill_job,
    gap_fill_job,
    feature_job,
    multi_tf_sync_job,
    funding_rate_job,
    account_monitor_job,
    account_user_stream_job,
    account_daily_stats_rollup_job,
    process_closed_candle,
    _candle_to_payload,
    _liquidation_distance_pct,
    _calc_dynamic_liq_threshold_pct,
    # AI
    ai_analysis_job,
    AIGateDecision,
    # YouTube
    youtube_sync_job,
    youtube_analyze_job,
    youtube_asr_backfill_job,
    YoutubeAnalyzeVideoSnapshot,
    _YT_ANALYZE_INFLIGHT,
    _YT_ASR_INFLIGHT,
    _YT_RUNTIME_RECONCILE_DONE,
    _YT_AUTH_RECOVER_LAST_SIGNATURE,
    _youtube_auto_recover_auth_failed,
    _youtube_runtime_reconcile_once,
    # Anomaly
    anomaly_job,
    _adaptive_enter_persist_bars,
    _adaptive_exit_threshold,
    _anomaly_extreme_bypass_data_ok,
    _classify_ai_data_freshness,
    _dedup_ai_signal_v2,
    _escalate_min_interval_seconds,
    _evaluate_ai_signal_gate_v2,
    _pick_ai_gate_atr_ref,
    _resolve_signal_prices_for_filter,
)
from app.scheduler.runtime import WorkerRuntime

__all__ = [
    "supervised_job",
    "heartbeat_job",
    "ws_consumer_job",
    "startup_backfill_job",
    "gap_fill_job",
    "feature_job",
    "multi_tf_sync_job",
    "funding_rate_job",
    "account_monitor_job",
    "account_user_stream_job",
    "account_daily_stats_rollup_job",
    "process_closed_candle",
    "_candle_to_payload",
    "_liquidation_distance_pct",
    "_calc_dynamic_liq_threshold_pct",
    "ai_analysis_job",
    "AIGateDecision",
    "youtube_sync_job",
    "youtube_analyze_job",
    "youtube_asr_backfill_job",
    "YoutubeAnalyzeVideoSnapshot",
    "_YT_ANALYZE_INFLIGHT",
    "_YT_ASR_INFLIGHT",
    "_youtube_auto_recover_auth_failed",
    "_youtube_runtime_reconcile_once",
    "intel_news_job",
    "intel_digest_job",
    "decision_eval_job",
    "strategy_scores_job",
    "strategy_research_job",
    "anomaly_job",
    "_adaptive_enter_persist_bars",
    "_adaptive_exit_threshold",
    "_anomaly_extreme_bypass_data_ok",
    "_classify_ai_data_freshness",
    "_dedup_ai_signal_v2",
    "_escalate_min_interval_seconds",
    "_evaluate_ai_signal_gate_v2",
    "_pick_ai_gate_atr_ref",
    "_resolve_signal_prices_for_filter",
    "WorkerRuntime",
]
