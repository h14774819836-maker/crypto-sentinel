from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.web.views import _derive_youtube_statuses, _youtube_queue_summary_from_items


def _video(**kwargs):
    base = dict(
        transcript_text=None,
        needs_asr=False,
        asr_processed_at=None,
        last_error=None,
        processed_at=None,
        analysis_runtime_status=None,
        analysis_stage=None,
        analysis_started_at=None,
        analysis_updated_at=None,
        analysis_finished_at=None,
        analysis_retry_count=0,
        analysis_next_retry_at=None,
        analysis_last_error_type=None,
        analysis_last_error_code=None,
        analysis_last_error_message=None,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_queue_state_is_mutually_exclusive():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    items = []

    done_video = _video(transcript_text="tx")
    done_insight = {
        "vta_version": "1.0",
        "market_view": {"bias_1_7d": "BULLISH"},
        "levels": {"supports": [], "resistances": []},
        "provenance": {"scores": {"pq": 88}},
    }
    items.append(_derive_youtube_statuses(done_video, done_insight, now_utc=now, worker_online=True))

    running_video = _video(
        transcript_text="tx",
        analysis_runtime_status="running",
        analysis_stage="llm_request",
        analysis_started_at=now - timedelta(seconds=30),
        analysis_updated_at=now - timedelta(seconds=5),
    )
    items.append(_derive_youtube_statuses(running_video, None, now_utc=now, worker_online=True))

    waiting_video = _video(
        transcript_text="tx",
        analysis_runtime_status="queued",
        analysis_stage="queued",
        analysis_started_at=now - timedelta(seconds=30),
        analysis_updated_at=now - timedelta(seconds=5),
    )
    items.append(_derive_youtube_statuses(waiting_video, None, now_utc=now, worker_online=True))

    blocked_video = _video(
        transcript_text="tx",
        analysis_runtime_status="failed_paused",
        analysis_stage="failed",
        analysis_last_error_type="auth",
        analysis_last_error_code="401",
        analysis_last_error_message="api key invalid",
    )
    items.append(_derive_youtube_statuses(blocked_video, None, now_utc=now, worker_online=True))

    allowed = {"waiting", "running", "blocked", "done"}
    for state in [it["queue_state"] for it in items]:
        assert state in allowed

    assert [it["queue_state"] for it in items] == ["done", "running", "waiting", "blocked"]


def test_queue_summary_three_state_counts():
    sample = [
        {"queue_state": "waiting"},
        {"queue_state": "waiting"},
        {"queue_state": "running"},
        {"queue_state": "blocked", "queue_reason_code": "auth_failed"},
        {"queue_state": "blocked", "queue_reason_code": "auth_failed"},
        {"queue_state": "blocked", "queue_reason_code": "worker_offline"},
        {"queue_state": "done"},
    ]
    summary = _youtube_queue_summary_from_items(sample)
    assert summary["waiting"] == 2
    assert summary["running"] == 1
    assert summary["blocked"] == 3
    assert summary["done"] == 1
    assert summary["total"] == 7
    assert summary["blocked_breakdown"]["auth_failed"] == 2
    assert summary["blocked_breakdown"]["worker_offline"] == 1
