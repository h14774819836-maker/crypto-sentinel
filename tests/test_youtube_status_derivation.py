from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import app.web.views as views
from app.web.views import _derive_youtube_statuses


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


def test_status_derivation_failed_paused_for_failed_placeholder():
    v = _video(transcript_text="hello", needs_asr=False)
    insight = {
        "vta_version": "1.0",
        "provenance": {
            "status": "failed_paused",
            "analysis_error": {"status_code": "400", "message": "Invalid max_tokens"},
            "scores": None,
        },
    }
    s = _derive_youtube_statuses(v, insight)
    assert s["transcript_status"] == "transcribed"
    assert s["analysis_status"] == "failed_paused"
    assert s["queue_state"] == "blocked"
    assert "Invalid max_tokens" in (s["analysis_error_summary"] or "")


def test_status_derivation_done_for_valid_vta():
    v = _video(transcript_text="hello", needs_asr=False)
    insight = {
        "vta_version": "1.0",
        "market_view": {"bias_1_7d": "BULLISH"},
        "levels": {"supports": [], "resistances": []},
        "provenance": {"scores": {"pq": 70}},
    }
    s = _derive_youtube_statuses(v, insight)
    assert s["analysis_status"] == "done"
    assert s["queue_state"] == "done"
    assert s["insight_valid"] is True


def test_status_derivation_queued_from_runtime_fields():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="queued",
        analysis_stage="queued",
        analysis_started_at=now - timedelta(seconds=12),
        analysis_updated_at=now - timedelta(seconds=5),
        analysis_finished_at=None,
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "queued"
    assert s["queue_state"] == "waiting"
    assert s["queue_reason_code"] == "queued_wait_slot"
    assert s["analysis_stage"] == "queued"
    assert s["analysis_elapsed_seconds"] == 12
    assert s["analysis_stalled"] is False


def test_status_derivation_running_from_processing_placeholder_not_failed():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    v = _video(transcript_text="hello", needs_asr=False)
    insight = {
        "vta_version": "1.0",
        "provenance": {
            "status": "processing",
            "scores": None,
        },
    }
    s = _derive_youtube_statuses(v, insight, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "running"
    assert s["queue_state"] == "running"
    assert s["analysis_stalled"] is False


def test_status_derivation_marks_stalled_for_timeout():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="running",
        analysis_stage="llm_request",
        analysis_started_at=now - timedelta(seconds=240),
        analysis_updated_at=now - timedelta(seconds=200),
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "running"
    assert s["analysis_stalled"] is False


def test_status_derivation_marks_stalled_when_worker_offline():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="running",
        analysis_stage="llm_request",
        analysis_started_at=now - timedelta(seconds=20),
        analysis_updated_at=now - timedelta(seconds=5),
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=False)
    assert s["analysis_status"] == "running"
    assert s["analysis_stalled"] is True
    assert s["analysis_stalled_reason"] == "worker_offline"
    assert s["queue_state"] == "blocked"
    assert s["queue_reason_code"] == "worker_offline"


def test_status_notes_are_clean_and_stable_for_common_states():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    cases = [
        (
            _video(
                transcript_text="hello",
                needs_asr=False,
                analysis_runtime_status="running",
                analysis_stage="llm_request",
                analysis_started_at=now - timedelta(seconds=15),
                analysis_updated_at=now - timedelta(seconds=3),
            ),
            None,
            "running",
            "Analyzing - llm_request",
        ),
        (
            _video(
                transcript_text="hello",
                needs_asr=False,
                analysis_runtime_status="queued",
                analysis_stage="queued",
                analysis_started_at=now - timedelta(seconds=15),
                analysis_updated_at=now - timedelta(seconds=3),
            ),
            None,
            "queued",
            "Queued for AI analysis",
        ),
        (
            _video(transcript_text="hello", needs_asr=False),
            {
                "vta_version": "1.0",
                "provenance": {
                    "status": "failed_paused",
                    "analysis_error": {"status_code": "400", "message": "Invalid max_tokens"},
                    "scores": None,
                },
            },
            "failed_paused",
            "AI analysis failed; manual retry required",
        ),
        (
            _video(transcript_text=None, needs_asr=True, asr_processed_at=now - timedelta(minutes=2), last_error="boom"),
            None,
            "pending",
            "ASR failed; manual retry required",
        ),
        (
            _video(transcript_text=None, needs_asr=True, asr_processed_at=None, last_error=None),
            None,
            "pending",
            "ASR queued",
        ),
        (
            _video(transcript_text=None, needs_asr=False),
            None,
            "pending",
            "Waiting subtitles",
        ),
        (
            _video(transcript_text="hello", needs_asr=False, analysis_runtime_status=None),
            None,
            "pending",
            "Transcript ready, pending AI",
        ),
    ]

    for video, insight, expected_status, expected_note in cases:
        s = _derive_youtube_statuses(video, insight, now_utc=now, worker_online=True)
        assert s["analysis_status"] == expected_status
        assert s["status_notes"] == expected_note
        assert "?" not in (s["status_notes"] or "")


def test_retry_wait_derivation_and_eta():
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="queued",
        analysis_stage="retry_wait",
        analysis_started_at=now - timedelta(seconds=120),
        analysis_updated_at=now - timedelta(seconds=5),
        analysis_next_retry_at=now + timedelta(seconds=55),
        analysis_retry_count=1,
        analysis_last_error_type="timeout",
        analysis_last_error_code="timeout",
        analysis_last_error_message="request timeout",
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "queued"
    assert s["queue_state"] == "waiting"
    assert s["queue_reason_code"] == "retry_wait"
    assert s["analysis_stage"] == "retry_wait"
    assert s["analysis_retry_scheduled"] is True
    assert s["analysis_retry_eta_seconds"] == 55
    assert s["analysis_stalled"] is False
    assert s["analysis_retry_count"] == 1
    assert s["analysis_error_summary"] == "timeout: request timeout"


def test_waiting_not_stalled_before_dynamic_threshold(monkeypatch):
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(views.settings, "youtube_analyze_poll_seconds", 180, raising=False)
    monkeypatch.setattr(views.settings, "youtube_poll_seconds", 180, raising=False)
    monkeypatch.setattr(views.settings, "youtube_analysis_stall_running_seconds", 420, raising=False)

    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="queued",
        analysis_stage="queued",
        analysis_started_at=now - timedelta(seconds=300),
        analysis_updated_at=now - timedelta(seconds=300),
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "queued"
    assert s["analysis_stalled"] is False
    assert s["queue_state"] == "waiting"


def test_running_stalled_after_running_threshold(monkeypatch):
    now = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(views.settings, "youtube_analysis_stall_running_seconds", 420, raising=False)

    v = _video(
        transcript_text="hello",
        needs_asr=False,
        analysis_runtime_status="running",
        analysis_stage="llm_request",
        analysis_started_at=now - timedelta(seconds=600),
        analysis_updated_at=now - timedelta(seconds=500),
    )
    s = _derive_youtube_statuses(v, None, now_utc=now, worker_online=True)
    assert s["analysis_status"] == "running"
    assert s["analysis_stalled"] is True
    assert s["queue_state"] == "blocked"
    assert s["queue_reason_code"] == "stalled_timeout"
