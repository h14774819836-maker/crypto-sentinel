from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.scheduler.jobs as jobs_module
from app.db.models import YoutubeInsight, YoutubeVideo
from app.db.repository import update_youtube_video_analysis_runtime, upsert_youtube_video, update_youtube_video_transcript
from app.db.session import Base
from app.scheduler.jobs import WorkerRuntime, _youtube_auto_recover_auth_failed, _youtube_runtime_reconcile_once


def _setup_db():
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _build_runtime(session_local, settings):
    return WorkerRuntime(
        settings=settings,  # type: ignore[arg-type]
        session_factory=session_local,
        provider=MagicMock(),
        telegram=MagicMock(),
        started_at=datetime.now(timezone.utc),
        version="test",
    )


def test_reconcile_heals_stale_queued_on_start(monkeypatch):
    session_local = _setup_db()
    now = datetime.now(timezone.utc)

    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v_stale_queued",
                "channel_id": "UC1",
                "title": "Stale queued",
                "published_at": now - timedelta(hours=3),
                "url": "https://youtube.com/watch?v=v_stale_queued",
            },
        )
        update_youtube_video_transcript(db, "v_stale_queued", "tx", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_stale_queued",
            status="queued",
            stage="queued",
            started_at=now - timedelta(hours=2),
            updated_at=now - timedelta(hours=2),
        )

        upsert_youtube_video(
            db,
            {
                "video_id": "v_missing_error",
                "channel_id": "UC1",
                "title": "Missing error",
                "published_at": now - timedelta(hours=1),
                "url": "https://youtube.com/watch?v=v_missing_error",
            },
        )
        update_youtube_video_transcript(db, "v_missing_error", "tx", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_missing_error",
            status="failed_paused",
            stage="failed",
            started_at=now - timedelta(minutes=20),
            updated_at=now - timedelta(minutes=18),
            finished_at=now - timedelta(minutes=18),
            last_error_type=None,
            last_error_code=None,
            last_error_message=None,
        )
        db.add(
            YoutubeInsight(
                video_id="v_missing_error",
                symbol="BTCUSDT",
                analyst_view_json={
                    "vta_version": "1.0",
                    "provenance": {
                        "status": "failed_paused",
                        "analysis_error": {
                            "type": "auth",
                            "status_code": "401",
                            "message": "api key invalid",
                        },
                    },
                },
            )
        )
        db.commit()

    settings = SimpleNamespace(
        youtube_analysis_stall_running_seconds=420,
        youtube_analysis_stall_waiting_seconds_effective=420,
    )
    runtime = _build_runtime(session_local, settings)

    monkeypatch.setattr(jobs_module, "_YT_RUNTIME_RECONCILE_DONE", False)
    stats = _youtube_runtime_reconcile_once(runtime)
    assert stats["queued_reset"] == 1
    assert stats["error_backfilled"] >= 1

    with session_local() as db:
        queued_row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_stale_queued"))
        assert queued_row is not None
        assert queued_row.analysis_runtime_status is None

        missing_row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_missing_error"))
        assert missing_row is not None
        assert missing_row.analysis_last_error_type == "auth"
        assert missing_row.analysis_last_error_code == "401"


def test_auth_failed_auto_recover_after_config_signature_change(monkeypatch):
    session_local = _setup_db()
    now = datetime.now(timezone.utc)
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v_auth",
                "channel_id": "UC1",
                "title": "Auth failed",
                "published_at": now - timedelta(hours=1),
                "url": "https://youtube.com/watch?v=v_auth",
            },
        )
        update_youtube_video_transcript(db, "v_auth", "tx", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_auth",
            status="failed_paused",
            stage="failed",
            started_at=now - timedelta(minutes=30),
            updated_at=now - timedelta(minutes=20),
            finished_at=now - timedelta(minutes=20),
            retry_count=0,
            last_error_type="auth",
            last_error_code="401",
            last_error_message="Authentication fails",
        )

    cfg_state = {
        "provider": "deepseek",
        "model": "deepseek-chat",
        "base_url": "https://api.deepseek.com",
        "api_key": "k1",
    }

    def _resolve_llm_config(_task):
        return SimpleNamespace(**cfg_state)

    settings = SimpleNamespace(
        youtube_auth_auto_recover_enabled=True,
        youtube_auth_auto_recover_batch=20,
        youtube_auth_auto_recover_max_attempts=2,
        resolve_llm_config=_resolve_llm_config,
    )
    runtime = _build_runtime(session_local, settings)

    monkeypatch.setattr(jobs_module, "_YT_AUTH_RECOVER_LAST_SIGNATURE", None)
    first = _youtube_auto_recover_auth_failed(runtime)
    assert first == 0

    cfg_state["model"] = "doubao-seed-2-0-pro-260215"
    cfg_state["provider"] = "ark"
    cfg_state["base_url"] = "https://ark.cn-beijing.volces.com/api/v3"

    recovered = _youtube_auto_recover_auth_failed(runtime)
    assert recovered == 1

    with session_local() as db:
        row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_auth"))
        assert row is not None
        assert row.analysis_runtime_status == "queued"
        assert row.analysis_stage == "queued"
        assert row.analysis_retry_count == 1


def test_auth_failed_not_auto_recover_without_config_change(monkeypatch):
    session_local = _setup_db()
    now = datetime.now(timezone.utc)
    with session_local() as db:
        upsert_youtube_video(
            db,
            {
                "video_id": "v_auth_static",
                "channel_id": "UC1",
                "title": "Auth failed static",
                "published_at": now - timedelta(hours=1),
                "url": "https://youtube.com/watch?v=v_auth_static",
            },
        )
        update_youtube_video_transcript(db, "v_auth_static", "tx", "en", needs_asr=False)
        update_youtube_video_analysis_runtime(
            db,
            "v_auth_static",
            status="failed_paused",
            stage="failed",
            retry_count=0,
            started_at=now - timedelta(minutes=20),
            updated_at=now - timedelta(minutes=10),
            finished_at=now - timedelta(minutes=10),
            last_error_type="auth",
            last_error_code="401",
            last_error_message="Authentication fails",
        )

    cfg_state = {
        "provider": "deepseek",
        "model": "deepseek-chat",
        "base_url": "https://api.deepseek.com",
        "api_key": "k1",
    }

    def _resolve_llm_config(_task):
        return SimpleNamespace(**cfg_state)

    settings = SimpleNamespace(
        youtube_auth_auto_recover_enabled=True,
        youtube_auth_auto_recover_batch=20,
        youtube_auth_auto_recover_max_attempts=2,
        resolve_llm_config=_resolve_llm_config,
    )
    runtime = _build_runtime(session_local, settings)

    monkeypatch.setattr(jobs_module, "_YT_AUTH_RECOVER_LAST_SIGNATURE", None)
    assert _youtube_auto_recover_auth_failed(runtime) == 0
    assert _youtube_auto_recover_auth_failed(runtime) == 0

    with session_local() as db:
        row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == "v_auth_static"))
        assert row is not None
        assert row.analysis_runtime_status == "failed_paused"
