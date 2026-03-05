from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import get_settings
from app.db.models import WorkerStatus, YoutubeVideo
from app.db.repository import (
    upsert_youtube_video,
    update_youtube_video_analysis_runtime,
    update_youtube_video_transcript,
)
from app.db.session import Base, get_db
from app.main import app
import app.web.views as views


def _setup_db():
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_youtube_videos_api_returns_queue_summary_and_worker_fields(monkeypatch):
    session_local = _setup_db()
    now = datetime.now(timezone.utc)
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v1",
                "channel_id": "UC1",
                "title": "Video 1",
                "published_at": now - timedelta(minutes=5),
                "url": "https://youtube.com/watch?v=v1",
            },
        )
        update_youtube_video_transcript(db, "v1", "hello world", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v1",
            status="running",
            stage="llm_request",
            started_at=now - timedelta(seconds=15),
            updated_at=now - timedelta(seconds=3),
        )
        db.add(
            WorkerStatus(
                worker_id="test-worker",
                started_at=now - timedelta(hours=1),
                last_seen=now - timedelta(seconds=2),
                version="test",
            )
        )
        db.commit()

    def _override_get_db():
        with session_local() as db:
            yield db

    app.dependency_overrides[get_db] = _override_get_db
    monkeypatch.setattr(views.settings, "worker_id", "test-worker", raising=False)
    monkeypatch.setattr(views.settings, "worker_heartbeat_seconds", 10, raising=False)

    try:
        with TestClient(app) as client:
            resp = client.get("/api/youtube/videos?limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert "server_time" in data
            assert data["worker_status"]["worker_id"] == "test-worker"
            assert data["worker_status"]["is_online"] is True
            assert data["queue_summary"]["running"] == 1
            assert data["queue_summary"]["total"] == 1
            assert "scheduler" in data["queue_summary"]
            assert set(data["effective_llm"].keys()) >= {"profile", "provider", "model", "base_url"}

            item = data["items"][0]
            assert item["video_id"] == "v1"
            assert item["analysis_status"] == "running"
            assert item["analysis_stage"] == "llm_request"
            assert item["queue_state"] == "running"
            assert item["analysis_elapsed_seconds"] >= 1
            assert item["analysis_stalled"] is False
            assert item["status_notes"] == "正在分析 - llm_request"
            assert item["analysis_retry_scheduled"] is False
            assert item["analysis_retry_eta_seconds"] is None
            assert "?" not in (item["status_notes"] or "")

            resp2 = client.get("/api/youtube/transcript/v1")
            assert resp2.status_code == 200
            data2 = resp2.json()
            assert data2["ok"] is True
            assert data2["analysis_status"] == "running"
            assert data2["queue_state"] == "running"
            assert data2["worker_status"]["is_online"] is True
            assert data2["status_notes"] == "正在分析 - llm_request"
            assert data2["analysis_retry_scheduled"] is False
            assert "effective_llm" in data2
            assert "?" not in (data2["status_notes"] or "")
    finally:
        app.dependency_overrides.clear()


def test_youtube_api_exposes_retry_wait_and_queue_summary(monkeypatch):
    session_local = _setup_db()
    now = datetime.now(timezone.utc)
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v_retry_wait",
                "channel_id": "UC1",
                "title": "Retry wait",
                "published_at": now - timedelta(minutes=30),
                "url": "https://youtube.com/watch?v=v_retry_wait",
            },
        )
        update_youtube_video_transcript(db, "v_retry_wait", "transcript", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_retry_wait",
            status="queued",
            stage="retry_wait",
            started_at=now - timedelta(minutes=1),
            updated_at=now - timedelta(seconds=5),
            retry_count=1,
            next_retry_at=now + timedelta(seconds=40),
            last_error_type="timeout",
            last_error_code="timeout",
            last_error_message="request timeout",
        )
        db.add(
            WorkerStatus(
                worker_id="test-worker",
                started_at=now - timedelta(hours=1),
                last_seen=now - timedelta(seconds=2),
                version="test",
            )
        )
        db.commit()

    def _override_get_db():
        with session_local() as db:
            yield db

    app.dependency_overrides[get_db] = _override_get_db
    monkeypatch.setattr(views.settings, "worker_id", "test-worker", raising=False)
    monkeypatch.setattr(views.settings, "worker_heartbeat_seconds", 10, raising=False)

    try:
        with TestClient(app) as client:
            resp = client.get("/api/youtube/videos?limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["queue_summary"]["waiting"] == 1
            item = data["items"][0]
            assert item["analysis_status"] == "queued"
            assert item["analysis_stage"] == "retry_wait"
            assert item["queue_state"] == "waiting"
            assert item["queue_reason_code"] == "retry_wait"
            assert item["analysis_retry_scheduled"] is True
            assert item["analysis_retry_count"] == 1
            assert isinstance(item["analysis_retry_eta_seconds"], int)
            assert item["analysis_retry_eta_seconds"] >= 0
            assert item["analysis_last_error_type"] == "timeout"
            assert item["analysis_last_error_code"] == "timeout"
            assert "request timeout" in (item["analysis_last_error_message"] or "")

            resp2 = client.get("/api/youtube/transcript/v_retry_wait")
            assert resp2.status_code == 200
            data2 = resp2.json()
            assert data2["analysis_stage"] == "retry_wait"
            assert data2["analysis_retry_scheduled"] is True
            assert data2["analysis_retry_count"] == 1
            assert data2["queue_state"] == "waiting"
    finally:
        app.dependency_overrides.clear()


def test_youtube_manual_retry_analyze_marks_runtime_queued(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token")
    get_settings.cache_clear()
    session_local = _setup_db()
    now = datetime.now(timezone.utc)
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v_retry",
                "channel_id": "UC1",
                "title": "Retry me",
                "published_at": now - timedelta(minutes=30),
                "url": "https://youtube.com/watch?v=v_retry",
            },
        )
        update_youtube_video_transcript(db, "v_retry", "transcript", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_retry",
            status="failed_paused",
            stage="failed",
            started_at=now - timedelta(minutes=5),
            updated_at=now - timedelta(minutes=4),
            finished_at=now - timedelta(minutes=4),
            retry_count=2,
            next_retry_at=now + timedelta(minutes=5),
            last_error_type="timeout",
            last_error_code="timeout",
            last_error_message="request timeout",
        )
        db.commit()

    def _override_get_db():
        with session_local() as db:
            yield db

    app.dependency_overrides[get_db] = _override_get_db
    monkeypatch.setattr(views.settings, "worker_id", "test-worker", raising=False)
    monkeypatch.setattr(views.settings, "worker_heartbeat_seconds", 10, raising=False)

    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/youtube/analyze/v_retry",
                json={"force": False},
                headers={"Authorization": "Bearer test-admin-token"},
            )
            assert resp.status_code == 200
            payload = resp.json()
            assert payload["ok"] is True
            assert payload["status"] == "queued"
            assert payload["analysis_status_after_enqueue"] == "queued"

        with session_local() as db:
            row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_retry"))
            assert row is not None
            assert row.analysis_runtime_status == "queued"
            assert row.analysis_stage == "queued"
            assert row.analysis_finished_at is None
            assert row.analysis_retry_count == 0
            assert row.analysis_next_retry_at is None
            assert row.analysis_last_error_type is None
    finally:
        app.dependency_overrides.clear()
