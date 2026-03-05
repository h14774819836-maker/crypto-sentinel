from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.ai.analyst as analyst_module
import app.ai.vta_scorer as vta_scorer_module
import app.ai.youtube_prompts as yt_prompts_module
import app.db.repository as repo_module
from app.db.models import YoutubeInsight, YoutubeVideo
from app.db.session import Base
from app.db.repository import upsert_youtube_video, update_youtube_video_transcript
from app.scheduler.jobs import WorkerRuntime, youtube_analyze_job, _YT_ANALYZE_INFLIGHT


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeProvider:
    def __init__(self, sleep_seconds: float = 0.08, max_concurrency: int = 2):
        self.sleep_seconds = sleep_seconds
        self.capabilities = SimpleNamespace(supports_reasoning=False)
        self.config = SimpleNamespace(provider="fake")
        self.semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        self.inflight = 0
        self.max_inflight = 0
        self.calls = 0

    async def generate_response(self, **_kwargs):
        async with self.semaphore:
            self.calls += 1
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            try:
                await asyncio.sleep(self.sleep_seconds)
                return {
                    "content": "{}",
                    "prompt_tokens": 10,
                    "completion_tokens": 20,
                }
            finally:
                self.inflight -= 1


def _valid_vta():
    return {
        "vta_version": "1.0",
        "meta": {},
        "market_view": {
            "bias_1_7d": "BULLISH",
            "bias_1_4w": None,
            "conviction": "MEDIUM",
        },
        "levels": {"supports": [], "resistances": []},
        "provenance": {
            "status": "ok",
            "retry_policy": "auto",
            "schema_errors": [],
            "scores": None,
        },
    }


@pytest.mark.anyio
async def test_youtube_analyze_job_processes_videos_in_parallel(monkeypatch):
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    base_ts = datetime(2026, 2, 25, 10, 0, tzinfo=timezone.utc)
    with SessionLocal() as db:
        for i in range(3):
            upsert_youtube_video(db, {
                "video_id": f"v{i}",
                "channel_id": "UC1",
                "title": f"Video {i}",
                "published_at": base_ts + timedelta(minutes=i),
                "url": f"https://youtube.com/watch?v=v{i}",
            })
            update_youtube_video_transcript(db, f"v{i}", f"transcript {i}", "en", needs_asr=False)

    fake_provider = _FakeProvider(sleep_seconds=0.08, max_concurrency=2)
    settings = SimpleNamespace(
        youtube_enabled=True,
        youtube_analyze_max_per_run_effective=3,
        youtube_consensus_lookback_hours=48,
        youtube_target_symbol="BTCUSDT",
        resolve_llm_config=lambda task: SimpleNamespace(
            model="fake-model",
            use_reasoning="false",
            max_concurrency=2,
        ),
    )
    runtime = WorkerRuntime(
        settings=settings,  # type: ignore[arg-type]
        session_factory=SessionLocal,
        provider=MagicMock(),
        telegram=MagicMock(),
        started_at=base_ts,
        version="test",
        youtube_llm_provider=fake_provider,  # type: ignore[arg-type]
    )

    monkeypatch.setattr(repo_module, "get_recent_youtube_insights", lambda *args, **kwargs: [])
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_VIDEO_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "YOUTUBE_CONSENSUS_SYSTEM_PROMPT", "system")
    monkeypatch.setattr(yt_prompts_module, "build_video_analysis_prompt", lambda **kwargs: "prompt")
    monkeypatch.setattr(yt_prompts_module, "build_consensus_prompt", lambda **kwargs: "consensus")
    monkeypatch.setattr(analyst_module, "_extract_json", lambda content: content)
    monkeypatch.setattr(vta_scorer_module, "normalize_vta", lambda _raw: _valid_vta())
    monkeypatch.setattr(vta_scorer_module, "validate_vta", lambda _vta: (True, []))
    monkeypatch.setattr(vta_scorer_module, "compute_scores", lambda _vta: {"pq": 70, "vsi": 12, "dc": 8})

    _YT_ANALYZE_INFLIGHT.clear()
    try:
        started = time.perf_counter()
        await youtube_analyze_job(runtime)
        elapsed = time.perf_counter() - started
    finally:
        _YT_ANALYZE_INFLIGHT.clear()

    assert fake_provider.max_inflight == 2
    assert fake_provider.calls == 3
    assert elapsed < 0.7

    with SessionLocal() as db:
        insight_count = db.scalar(select(func.count(YoutubeInsight.id)))
        assert insight_count == 3
        videos = list(db.scalars(select(YoutubeVideo).order_by(YoutubeVideo.video_id.asc())))
        assert [v.analysis_runtime_status for v in videos] == ["done", "done", "done"]
        assert all(v.analysis_stage == "done" for v in videos)
        assert all(v.analysis_finished_at is not None for v in videos)
