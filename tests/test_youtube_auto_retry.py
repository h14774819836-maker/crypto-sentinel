from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.ai.youtube_prompts as yt_prompts_module
import app.db.repository as repo_module
from app.ai.provider import LLMBadRequestError, LLMTimeoutError
from app.db.models import YoutubeInsight, YoutubeVideo
from app.db.repository import upsert_youtube_video, update_youtube_video_transcript
from app.db.session import Base
from app.scheduler.jobs import WorkerRuntime, _YT_ANALYZE_INFLIGHT, youtube_analyze_job


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _TimeoutProvider:
    def __init__(self):
        self.capabilities = SimpleNamespace(supports_reasoning=False)
        self.config = SimpleNamespace(provider="fake")

    async def generate_response(self, **_kwargs):
        raise LLMTimeoutError("request timeout")


class _BadRequestProvider:
    def __init__(self):
        self.capabilities = SimpleNamespace(supports_reasoning=False)
        self.config = SimpleNamespace(provider="fake")

    async def generate_response(self, **_kwargs):
        raise LLMBadRequestError("invalid schema request")


def _build_runtime(session_local, provider, base_ts):
    settings = SimpleNamespace(
        youtube_enabled=True,
        youtube_analyze_max_per_run_effective=1,
        youtube_consensus_lookback_hours=48,
        youtube_target_symbol="BTCUSDT",
        youtube_analyze_max_auto_retries=2,
        youtube_analyze_retry_base_seconds=60,
        youtube_analyze_retry_max_seconds=900,
        resolve_llm_config=lambda task: SimpleNamespace(
            model="fake-model",
            use_reasoning="false",
            max_concurrency=2,
        ),
    )
    return WorkerRuntime(
        settings=settings,  # type: ignore[arg-type]
        session_factory=session_local,
        provider=MagicMock(),
        telegram=MagicMock(),
        started_at=base_ts,
        version="test",
        youtube_llm_provider=provider,  # type: ignore[arg-type]
    )


def _prepare_single_video(session_local, video_id: str, base_ts: datetime):
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": video_id,
                "channel_id": "UC1",
                "title": "Retry candidate",
                "published_at": base_ts,
                "url": f"https://youtube.com/watch?v={video_id}",
            },
        )
        update_youtube_video_transcript(db, video_id, "transcript", "en", needs_asr=False)


@pytest.mark.anyio
async def test_transient_timeout_schedules_retry_wait(monkeypatch):
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    session_local = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    base_ts = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    _prepare_single_video(session_local, "v_timeout", base_ts)

    runtime = _build_runtime(session_local, _TimeoutProvider(), base_ts)
    monkeypatch.setattr(repo_module, "get_recent_youtube_insights", lambda *args, **kwargs: [])
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_VIDEO_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_CONSENSUS_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "build_video_analysis_prompt", lambda **kwargs: "prompt")
    monkeypatch.setattr(yt_prompts_module, "build_consensus_prompt", lambda **kwargs: "consensus")

    _YT_ANALYZE_INFLIGHT.clear()
    try:
        await youtube_analyze_job(runtime)
    finally:
        _YT_ANALYZE_INFLIGHT.clear()

    with session_local() as db:
        row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_timeout"))
        assert row is not None
        assert row.analysis_runtime_status == "queued"
        assert row.analysis_stage == "retry_wait"
        assert row.analysis_retry_count == 1
        assert row.analysis_next_retry_at is not None
        assert row.analysis_last_error_type == "timeout"
        assert row.analysis_last_error_code == "timeout"
        assert "timeout" in (row.analysis_last_error_message or "")

        insight = db.scalar(select(YoutubeInsight).where(YoutubeInsight.video_id == "v_timeout"))
        assert insight is not None
        assert isinstance(insight.analyst_view_json, dict)


@pytest.mark.anyio
async def test_bad_request_goes_failed_paused_without_auto_retry(monkeypatch):
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    session_local = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    base_ts = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    _prepare_single_video(session_local, "v_bad_request", base_ts)

    runtime = _build_runtime(session_local, _BadRequestProvider(), base_ts)
    monkeypatch.setattr(repo_module, "get_recent_youtube_insights", lambda *args, **kwargs: [])
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_VIDEO_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_CONSENSUS_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "build_video_analysis_prompt", lambda **kwargs: "prompt")
    monkeypatch.setattr(yt_prompts_module, "build_consensus_prompt", lambda **kwargs: "consensus")

    _YT_ANALYZE_INFLIGHT.clear()
    try:
        await youtube_analyze_job(runtime)
    finally:
        _YT_ANALYZE_INFLIGHT.clear()

    with session_local() as db:
        row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_bad_request"))
        assert row is not None
        assert row.analysis_runtime_status == "failed_paused"
        assert row.analysis_stage == "failed"
        assert row.analysis_next_retry_at is None
        assert row.analysis_last_error_type == "bad_request"
        assert row.analysis_last_error_code == "bad_request"
        assert "invalid schema request" in (row.analysis_last_error_message or "")
