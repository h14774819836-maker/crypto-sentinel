"""Tests for YouTube MVP integration."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.db.models import YoutubeChannel, YoutubeConsensus, YoutubeInsight, YoutubeVideo
from app.db.repository import (
    add_youtube_channel,
    bulk_mark_youtube_videos_analysis_queued,
    get_latest_youtube_consensus,
    get_recent_youtube_insights,
    list_unprocessed_youtube_videos,
    list_videos_needing_analysis,
    list_videos_needing_asr,
    list_youtube_channels,
    list_youtube_videos,
    remove_youtube_channel,
    save_youtube_consensus,
    save_youtube_insight,
    update_youtube_video_analysis_runtime,
    update_youtube_video_asr_result,
    update_youtube_video_transcript,
    upsert_youtube_video,
)
from app.db.session import Base


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    with SessionLocal() as db:
        yield db


# ── Channel CRUD ────────────────────────────────────────────────────


def test_add_and_list_channels(session):
    assert add_youtube_channel(session, "UC_test1", channel_url="https://youtube.com/@test1")
    assert add_youtube_channel(session, "UC_test2", channel_url="https://youtube.com/@test2")

    channels = list_youtube_channels(session)
    assert len(channels) == 2
    assert channels[0].channel_id == "UC_test1"


def test_add_channel_idempotent(session):
    assert add_youtube_channel(session, "UC_dup", channel_url="url1")
    assert not add_youtube_channel(session, "UC_dup", channel_url="url2")  # duplicate

    channels = list_youtube_channels(session)
    assert len(channels) == 1


def test_remove_channel(session):
    add_youtube_channel(session, "UC_del")
    assert remove_youtube_channel(session, "UC_del")
    assert not remove_youtube_channel(session, "UC_del")  # already gone

    channels = list_youtube_channels(session)
    assert len(channels) == 0


# ── Video Upsert ────────────────────────────────────────────────────


def test_video_upsert_idempotent(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    payload = {
        "video_id": "abc123",
        "channel_id": "UC_test",
        "channel_title": "Test Channel",
        "title": "BTC Analysis",
        "published_at": ts,
        "url": "https://youtube.com/watch?v=abc123",
    }
    upsert_youtube_video(session, payload)
    upsert_youtube_video(session, {**payload, "title": "Updated Title"})

    count = session.scalar(select(func.count(YoutubeVideo.id)))
    assert count == 1

    row = session.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "abc123"))
    assert row is not None
    assert row.title == "Updated Title"


# ── Unprocessed / Needs ASR / Needs Analysis ────────────────────────


def test_list_unprocessed_videos(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    upsert_youtube_video(session, {
        "video_id": "v1", "channel_id": "UC1", "title": "V1",
        "published_at": ts, "url": "url1",
    })
    upsert_youtube_video(session, {
        "video_id": "v2", "channel_id": "UC1", "title": "V2",
        "published_at": ts, "url": "url2",
    })

    unprocessed = list_unprocessed_youtube_videos(session, limit=10)
    assert len(unprocessed) == 2

    # Mark one as processed
    update_youtube_video_transcript(session, "v1", "some text", "en", needs_asr=False)
    unprocessed = list_unprocessed_youtube_videos(session, limit=10)
    assert len(unprocessed) == 1
    assert unprocessed[0].video_id == "v2"


def test_list_videos_needing_asr(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    upsert_youtube_video(session, {
        "video_id": "v_asr", "channel_id": "UC1", "title": "ASR needed",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_asr", None, None, needs_asr=True)

    needing = list_videos_needing_asr(session, limit=10)
    assert len(needing) == 1
    assert needing[0].video_id == "v_asr"


def test_list_videos_needing_asr_excludes_failed_by_default(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    upsert_youtube_video(session, {
        "video_id": "v_asr_fail", "channel_id": "UC1", "title": "ASR fail",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_asr_fail", None, None, needs_asr=True)
    # simulate failed ASR attempt
    update_youtube_video_asr_result(
        session, "v_asr_fail",
        transcript_text=None,
        transcript_lang=None,
        asr_backend="local_faster_whisper",
        asr_model="small",
        last_error="boom",
    )
    assert list_videos_needing_asr(session, limit=10) == []
    needing = list_videos_needing_asr(session, limit=10, include_failed=True)
    assert len(needing) == 1
    assert needing[0].video_id == "v_asr_fail"


def test_asr_result_update(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    upsert_youtube_video(session, {
        "video_id": "v_asr2", "channel_id": "UC1", "title": "For ASR",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_asr2", None, None, needs_asr=True)

    # Simulate successful ASR
    update_youtube_video_asr_result(
        session, "v_asr2",
        transcript_text="transcribed text",
        transcript_lang="zh",
        asr_backend="local_faster_whisper",
        asr_model="small",
    )

    row = session.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_asr2"))
    assert row is not None
    assert row.transcript_text == "transcribed text"
    assert row.needs_asr is False
    assert row.asr_backend == "local_faster_whisper"


def test_list_videos_needing_analysis(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    # Video with transcript
    upsert_youtube_video(session, {
        "video_id": "v_analyze", "channel_id": "UC1", "title": "Analyze me",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_analyze", "some text", "en", needs_asr=False)

    needing = list_videos_needing_analysis(session, limit=10)
    assert len(needing) == 1

    # Save insight → should no longer need analysis
    save_youtube_insight(session, {
        "video_id": "v_analyze",
        "symbol": "BTCUSDT",
        "analyst_view_json": {"bias": "bullish"},
    })

    needing = list_videos_needing_analysis(session, limit=10)
    assert len(needing) == 0


def test_youtube_queue_latest_first_with_rescue_window(session):
    base = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(6):
        upsert_youtube_video(session, {
            "video_id": f"vq{i}",
            "channel_id": "UCQ",
            "title": f"Q{i}",
            "published_at": base + timedelta(minutes=i),
            "url": f"url{i}",
        })
        update_youtube_video_transcript(session, f"vq{i}", "tx", "en", needs_asr=False)

    # all have transcript and no insights -> analysis queue
    rows = list_videos_needing_analysis(session, limit=4, rescue_oldest=1)
    ids = [r.video_id for r in rows]
    # newest first part
    assert ids[0] == "vq5"
    assert "vq0" in ids  # rescue oldest


def test_list_videos_needing_analysis_excludes_fresh_running_runtime(session):
    ts = datetime.now(timezone.utc) - timedelta(minutes=1)
    upsert_youtube_video(session, {
        "video_id": "v_run", "channel_id": "UC1", "title": "Running",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_run", "some text", "en", needs_asr=False)
    bulk_mark_youtube_videos_analysis_queued(session, ["v_run"], now=datetime.now(timezone.utc) - timedelta(seconds=30))
    update_youtube_video_analysis_runtime(
        session,
        "v_run",
        status="running",
        stage="llm_request",
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )

    needing = list_videos_needing_analysis(session, limit=10)
    assert [r.video_id for r in needing] == []


def test_list_videos_needing_analysis_rescues_stale_running_runtime(session):
    ts = datetime.now(timezone.utc) - timedelta(minutes=40)
    upsert_youtube_video(session, {
        "video_id": "v_stale", "channel_id": "UC1", "title": "Stale",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_stale", "some text", "en", needs_asr=False)
    update_youtube_video_analysis_runtime(
        session,
        "v_stale",
        status="running",
        stage="llm_request",
        started_at=datetime.now(timezone.utc) - timedelta(minutes=30),
        updated_at=datetime.now(timezone.utc) - timedelta(minutes=25),
    )

    needing = list_videos_needing_analysis(session, limit=10)
    assert [r.video_id for r in needing] == ["v_stale"]


def test_list_videos_needing_analysis_retry_wait_due_is_rescued(session):
    ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    upsert_youtube_video(session, {
        "video_id": "v_retry_due", "channel_id": "UC1", "title": "Retry due",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_retry_due", "some text", "en", needs_asr=False)
    save_youtube_insight(session, {
        "video_id": "v_retry_due",
        "symbol": "BTCUSDT",
        "analyst_view_json": {
            "vta_version": "1.0",
            "provenance": {
                "status": "failed_paused",
                "analysis_error": {"status_code": "timeout", "message": "timeout"},
                "scores": None,
            },
        },
    })
    update_youtube_video_analysis_runtime(
        session,
        "v_retry_due",
        status="queued",
        stage="retry_wait",
        started_at=ts,
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        retry_count=1,
        next_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        last_error_type="timeout",
        last_error_code="timeout",
        last_error_message="timeout",
    )

    needing = list_videos_needing_analysis(session, limit=10)
    assert [r.video_id for r in needing] == ["v_retry_due"]


def test_list_videos_needing_analysis_retry_wait_not_due_not_rescued(session):
    ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    upsert_youtube_video(session, {
        "video_id": "v_retry_not_due", "channel_id": "UC1", "title": "Retry not due",
        "published_at": ts, "url": "url",
    })
    update_youtube_video_transcript(session, "v_retry_not_due", "some text", "en", needs_asr=False)
    save_youtube_insight(session, {
        "video_id": "v_retry_not_due",
        "symbol": "BTCUSDT",
        "analyst_view_json": {
            "vta_version": "1.0",
            "provenance": {
                "status": "failed_paused",
                "analysis_error": {"status_code": "timeout", "message": "timeout"},
                "scores": None,
            },
        },
    })
    update_youtube_video_analysis_runtime(
        session,
        "v_retry_not_due",
        status="queued",
        stage="retry_wait",
        started_at=ts,
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=5),
        retry_count=1,
        next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=300),
        last_error_type="timeout",
        last_error_code="timeout",
        last_error_message="timeout",
    )

    needing = list_videos_needing_analysis(session, limit=10)
    assert [r.video_id for r in needing] == []


# ── Insights & Consensus ────────────────────────────────────────────


def test_insight_upsert(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    upsert_youtube_video(session, {
        "video_id": "v_ins", "channel_id": "UC1", "title": "Insight test",
        "published_at": ts, "url": "url",
    })

    data = {"bias": "bearish", "confidence": 75}
    save_youtube_insight(session, {
        "video_id": "v_ins", "symbol": "BTCUSDT", "analyst_view_json": data,
    })
    # Upsert same video_id
    data2 = {"bias": "bullish", "confidence": 80}
    save_youtube_insight(session, {
        "video_id": "v_ins", "symbol": "BTCUSDT", "analyst_view_json": data2,
    })

    count = session.scalar(select(func.count(YoutubeInsight.id)))
    assert count == 1

    insights = get_recent_youtube_insights(session, lookback_hours=1)
    assert len(insights) == 1
    assert insights[0].analyst_view_json["bias"] == "bullish"


def test_consensus_save_and_get(session):
    save_youtube_consensus(session, {
        "symbol": "BTCUSDT",
        "lookback_hours": 48,
        "consensus_json": {"consensus_bias": "bullish", "confidence": 70},
        "source_video_ids": ["v1", "v2"],
    })

    latest = get_latest_youtube_consensus(session, "BTCUSDT")
    assert latest is not None
    assert latest.consensus_json["consensus_bias"] == "bullish"
    assert latest.source_video_ids == ["v1", "v2"]


def test_list_youtube_videos(session):
    ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(5):
        upsert_youtube_video(session, {
            "video_id": f"v_list_{i}", "channel_id": "UC1", "title": f"Video {i}",
            "published_at": ts + timedelta(hours=i), "url": f"url{i}",
        })

    videos = list_youtube_videos(session, limit=3)
    assert len(videos) == 3
    # Most recent first
    assert videos[0].video_id == "v_list_4"


# ── RSS Parsing ─────────────────────────────────────────────────────


def test_rss_parsing():
    """Test XML parsing of YouTube Atom feed."""
    import xml.etree.ElementTree as ET

    sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
          xmlns="http://www.w3.org/2005/Atom">
      <title>Test Channel</title>
      <entry>
        <yt:videoId>dQw4w9WgXcQ</yt:videoId>
        <title>Test Video</title>
        <published>2026-02-01T12:00:00+00:00</published>
      </entry>
    </feed>"""

    root = ET.fromstring(sample_xml)
    ns = "http://www.w3.org/2005/Atom"
    yt_ns = "http://www.youtube.com/xml/schemas/2015"

    entries = root.findall(f"{{{ns}}}entry")
    assert len(entries) == 1

    vid = entries[0].find(f"{{{yt_ns}}}videoId")
    assert vid is not None
    assert vid.text == "dQw4w9WgXcQ"

    title = entries[0].find(f"{{{ns}}}title")
    assert title is not None
    assert title.text == "Test Video"


# ── Config Defaults ─────────────────────────────────────────────────


def test_config_youtube_defaults():
    """Verify YouTube is disabled by default."""
    import os
    os.environ.pop("YOUTUBE_ENABLED", None)
    os.environ.pop("ASR_ENABLED", None)

    from app.config import Settings
    s = Settings(_env_file=None)
    assert s.youtube_enabled is False
    assert s.asr_enabled is False
    assert s.youtube_channel_id_list == []
    assert s.youtube_lang_list == ["zh-Hans", "zh-Hant", "en"]
