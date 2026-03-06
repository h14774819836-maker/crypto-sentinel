"""YouTube API routes."""
from __future__ import annotations

import asyncio
import json as _json
import os as _os
import queue
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from app.db.models import YoutubeInsight, YoutubeVideo
from app.db.repository import get_latest_youtube_consensus, list_youtube_channels
from app.db.session import SessionLocal, get_db
from app.web.auth import require_admin
from app.web.shared import get_asr_semaphore, settings
from app.web.youtube_helpers import (
    _derive_youtube_statuses,
    _youtube_effective_llm_snapshot,
    _youtube_queue_summary_from_items,
    _youtube_scheduler_snapshot,
    _youtube_worker_status_snapshot,
)

router = APIRouter()


@router.post("/api/youtube/sync")
async def youtube_manual_sync(db: Session = Depends(get_db), _admin: str = Depends(require_admin)):
    """Manually sync: only fetch the LATEST 1 video per channel + try transcript."""
    from app.db.repository import update_youtube_video_transcript, upsert_youtube_video
    from app.providers.youtube_provider import fetch_channel_feed, fetch_transcript

    channels = list_youtube_channels(db, enabled_only=True)
    channel_ids = [ch.channel_id for ch in channels]
    for cid in settings.youtube_channel_id_list:
        if cid and cid not in channel_ids:
            channel_ids.append(cid)
    if not channel_ids:
        return {"ok": False, "error": "没有已关注的频道"}

    results = []
    for cid in channel_ids:
        try:
            entries = await fetch_channel_feed(cid, max_entries=1)
        except Exception as exc:
            results.append({"channel": cid, "error": str(exc)})
            continue
        if not entries:
            results.append({"channel": cid, "status": "无新视频"})
            continue

        entry = entries[0]
        existing_video = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == entry.video_id))
        has_transcript = existing_video is not None and existing_video.transcript_text is not None

        upsert_youtube_video(db, {
            "video_id": entry.video_id,
            "channel_id": entry.channel_id,
            "channel_title": entry.channel_title,
            "title": entry.title,
            "published_at": entry.published_at,
            "url": entry.url,
        })

        if has_transcript:
            results.append({
                "channel": entry.channel_title or cid,
                "title": entry.title[:50],
                "status": "ℹ️ 已有转录",
            })
            continue

        try:
            transcript_result = fetch_transcript(entry.video_id, settings.youtube_lang_list)
        except Exception:
            transcript_result = None

        if transcript_result:
            text, lang = transcript_result
            update_youtube_video_transcript(db, entry.video_id, text, lang, needs_asr=False)
            results.append({
                "channel": entry.channel_title or cid,
                "title": entry.title[:50],
                "status": f"✅ 已转录 ({lang}, {len(text)}字)",
            })
        else:
            update_youtube_video_transcript(db, entry.video_id, None, None, needs_asr=True)
            results.append({
                "channel": entry.channel_title or cid,
                "title": entry.title[:50],
                "status": "🔔 无字幕，需要 ASR 转录",
            })

    return {"ok": True, "results": results}


@router.get("/api/youtube/asr/model")
def asr_model_status(_admin: str = Depends(require_admin)):
    """Check if Whisper model is cached locally."""
    if not settings.asr_enabled:
        return {"ready": False, "reason": "ASR not enabled"}

    try:
        from huggingface_hub import try_to_load_from_cache
        repo_id = f"Systran/faster-whisper-{settings.asr_model}"
        result = try_to_load_from_cache(repo_id, "model.bin")
        cached = result is not None
    except Exception:
        from pathlib import Path
        cache_dir = Path.home() / ".cache" / "huggingface" / "hub"
        repo_dir = cache_dir / f"models--Systran--faster-whisper-{settings.asr_model}"
        cached = repo_dir.exists() and any(repo_dir.rglob("model.bin"))

    return {
        "ready": cached,
        "model": settings.asr_model,
        "device": settings.asr_device,
        "compute_type": settings.asr_compute_type,
        "langs": settings.youtube_lang_list,
    }


@router.post("/api/youtube/asr/model")
async def asr_model_download(_admin: str = Depends(require_admin)):
    """Pre-download Whisper model with SSE progress."""
    if not settings.asr_enabled:
        return {"ok": False, "error": "ASR not enabled"}

    sem = get_asr_semaphore()
    if sem.locked():
        raise HTTPException(status_code=429, detail="ASR concurrency limit reached, try again later")
    await sem.acquire()

    progress_q: queue.Queue = queue.Queue()

    def _worker():
        try:
            from pathlib import Path
            model_sizes = {"tiny": 75, "base": 145, "small": 480, "medium": 1500, "large": 3100, "large-v2": 3100, "large-v3": 3100}
            expected_mb = model_sizes.get(settings.asr_model, 500)
            cache_base = Path.home() / ".cache" / "huggingface" / "hub"
            repo_name = f"models--Systran--faster-whisper-{settings.asr_model}"
            repo_dir = cache_base / repo_name

            if repo_dir.exists():
                blobs = list(repo_dir.rglob("model.bin"))
                if blobs:
                    progress_q.put(("done", f"✅ 模型已存在 ({settings.asr_model})", 100))
                    return

            progress_q.put(("step", f"📦 开始下载 Whisper {settings.asr_model} 模型 (~{expected_mb}MB)...", 0))

            import concurrent.futures
            import time
            with concurrent.futures.ThreadPoolExecutor(1) as pool:
                from faster_whisper import WhisperModel
                future = pool.submit(
                    WhisperModel, settings.asr_model,
                    device=settings.asr_device,
                    compute_type=settings.asr_compute_type,
                )

                while not future.done():
                    time.sleep(1)
                    try:
                        if repo_dir.exists():
                            total_bytes = sum(f.stat().st_size for f in repo_dir.rglob("*") if f.is_file())
                            downloaded_mb = total_bytes / 1024 / 1024
                            pct = min(downloaded_mb / expected_mb * 100, 99) if expected_mb > 0 else 0
                            progress_q.put(("progress", f"Downloading model... {pct:5.1f}% ({downloaded_mb:.0f}/{expected_mb}MB)", pct))
                        else:
                            progress_q.put(("progress", "Connecting to HuggingFace...", 0))
                    except Exception:
                        pass

                future.result()

            progress_q.put(("done", f"Model download completed ({settings.asr_model}, {settings.asr_device})", 100))
        except Exception as exc:
            progress_q.put(("error", f"Model download failed: {exc}", 0))

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()

    async def _event_stream():
        try:
            while True:
                try:
                    event_type, msg, pct = progress_q.get(timeout=0.5)
                    data = _json.dumps({"type": event_type, "message": msg, "percent": pct}, ensure_ascii=False)
                    yield f"data: {data}\n\n"
                    if event_type in ("done", "error"):
                        break
                except queue.Empty:
                    if not thread.is_alive():
                        yield f"data: {_json.dumps({'type': 'error', 'message': 'Download worker exited unexpectedly', 'percent': 0})}\n\n"
                        break
                    yield ": heartbeat\n\n"
                await asyncio.sleep(0.1)
        finally:
            sem.release()

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/youtube/asr/{video_id}")
async def youtube_manual_asr(video_id: str, db: Session = Depends(get_db), _admin: str = Depends(require_admin)):
    """Manually trigger ASR transcription with real-time SSE progress."""
    from app.db.repository import update_youtube_video_asr_result
    from app.providers.youtube_provider import download_audio, transcribe_local

    if not settings.asr_enabled:
        return {"ok": False, "step": "check_config", "error": "ASR not enabled. Set ASR_ENABLED=true in .env"}

    sem = get_asr_semaphore()
    if sem.locked():
        raise HTTPException(status_code=429, detail="ASR concurrency limit reached, try again later")
    await sem.acquire()

    row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        sem.release()
        return {"ok": False, "step": "find_video", "error": "Video not found"}
    if row.transcript_text:
        sem.release()
        return {"ok": True, "step": "done", "message": "Transcript already exists"}

    progress_q: queue.Queue = queue.Queue()

    def _yt_dlp_hook(d: dict):
        status = d.get("status", "")
        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            speed = d.get("speed") or 0
            eta = d.get("eta") or 0
            pct = (downloaded / total * 100) if total > 0 else 0

            def _fmt_size(b):
                if b >= 1024 * 1024:
                    return f"{b / 1024 / 1024:.1f}MiB"
                if b >= 1024:
                    return f"{b / 1024:.0f}KiB"
                return f"{b}B"

            msg = f"⬇️ 下载中 {pct:5.1f}%  of {_fmt_size(total)}  at {_fmt_size(speed)}/s  ETA {eta:.0f}s"
            progress_q.put(("progress", msg, pct))
        elif status == "finished":
            progress_q.put(("progress", "⬇️ 下载完成，准备转录...", 100))

    def _worker():
        try:
            progress_q.put(("step", "⬇️ 开始下载音频...", 0))
            audio_path = download_audio(
                video_id,
                cache_dir=settings.asr_audio_cache_dir,
                progress_hook=_yt_dlp_hook,
                cookies_from_browser=settings.youtube_cookies_from_browser,
                cookies_file=settings.youtube_cookies_file,
            )
            if not audio_path:
                with SessionLocal() as thread_db:
                    update_youtube_video_asr_result(
                        thread_db, video_id, None, None,
                        asr_backend=settings.asr_backend, asr_model=settings.asr_model,
                        last_error="音频下载失败",
                    )
                progress_q.put(("error", "❌ 音频下载失败", 0))
                return

            progress_q.put(("step", "🎧 正在加载 Whisper 模型并转录（首次需下载模型 ~500MB）...", 0))
            result = transcribe_local(
                audio_path,
                model_name=settings.asr_model,
                device=settings.asr_device,
                compute_type=settings.asr_compute_type,
                vad_filter=settings.asr_vad_filter,
            )

            if not settings.asr_keep_audio:
                try:
                    _os.remove(audio_path)
                except OSError:
                    pass

            with SessionLocal() as thread_db:
                if result:
                    text, lang = result
                    update_youtube_video_asr_result(
                        thread_db, video_id, text, lang,
                        asr_backend=settings.asr_backend, asr_model=settings.asr_model,
                    )
                    progress_q.put(("done", f"Transcription completed: lang={lang}, chars={len(text)}", 100))
                else:
                    update_youtube_video_asr_result(
                        thread_db, video_id, None, None,
                        asr_backend=settings.asr_backend, asr_model=settings.asr_model,
                        last_error="转录结果为空",
                    )
                    progress_q.put(("error", "❌ 转录结果为空", 0))
        except Exception as exc:
            progress_q.put(("error", f"❌ 异常: {exc}", 0))

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()

    async def _event_stream():
        try:
            while True:
                try:
                    event_type, msg, pct = progress_q.get(timeout=0.3)
                    data = _json.dumps({"type": event_type, "message": msg, "percent": pct}, ensure_ascii=False)
                    yield f"data: {data}\n\n"
                    if event_type in ("done", "error"):
                        break
                except queue.Empty:
                    if not thread.is_alive():
                        yield f"data: {_json.dumps({'type': 'error', 'message': 'Background task exited unexpectedly', 'percent': 0})}\n\n"
                        break
                    yield ": heartbeat\n\n"
                await asyncio.sleep(0.1)
        finally:
            sem.release()

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/youtube/channels")
async def add_channel_api(
    request: Request,
    db: Session = Depends(get_db),
    _admin: str = Depends(require_admin),
):
    body = await request.json()
    url = body.get("url", "").strip()
    if not url:
        return {"ok": False, "error": "请提供频道 URL"}

    from app.providers.youtube_provider import resolve_channel_id
    channel_id = await resolve_channel_id(url)
    if not channel_id:
        return {"ok": False, "error": f"无法解析频道 ID，请确认 URL 格式正确: {url}"}

    from app.db.repository import add_youtube_channel
    inserted = add_youtube_channel(db, channel_id=channel_id, channel_url=url)
    return {"ok": True, "channel_id": channel_id, "inserted": inserted}


@router.delete("/api/youtube/channels/{channel_id}")
def delete_channel_api(channel_id: str, db: Session = Depends(get_db), _admin: str = Depends(require_admin)):
    from app.db.repository import remove_youtube_channel
    removed = remove_youtube_channel(db, channel_id)
    return {"ok": removed}


@router.get("/api/youtube/channels")
def list_channels_api(db: Session = Depends(get_db)):
    channels = list_youtube_channels(db, enabled_only=False)
    return {
        "items": [
            {
                "channel_id": ch.channel_id,
                "channel_url": ch.channel_url,
                "channel_title": ch.channel_title,
                "enabled": ch.enabled,
                "created_at": ch.created_at,
            }
            for ch in channels
        ]
    }


@router.get("/api/youtube/consensus")
def yt_consensus_api(
    symbol: str = Query(default="BTCUSDT"),
    db: Session = Depends(get_db),
):
    row = get_latest_youtube_consensus(db, symbol=symbol)
    if not row:
        return {"ok": False, "data": None}
    return {
        "ok": True,
        "data": row.consensus_json,
        "source_video_ids": row.source_video_ids,
        "created_at": row.created_at,
    }


@router.get("/api/youtube/videos")
def yt_videos_api(
    limit: int = Query(default=20, ge=1, le=100),
    transcript_status: str | None = Query(default=None),
    analysis_status: str | None = Query(default=None),
    only_failed: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    now_utc = datetime.now(timezone.utc)
    worker_status = _youtube_worker_status_snapshot(db, now_utc=now_utc)
    scheduler_status = _youtube_scheduler_snapshot(now_utc=now_utc)
    effective_llm = _youtube_effective_llm_snapshot()
    videos = list(db.scalars(select(YoutubeVideo).order_by(YoutubeVideo.published_at.desc())))
    video_ids = [v.video_id for v in videos]
    insights = []
    if video_ids:
        insights = db.scalars(
            select(YoutubeInsight)
            .where(YoutubeInsight.video_id.in_(video_ids))
            .order_by(YoutubeInsight.created_at.desc())
        ).all()

    insight_map: dict[str, Any] = {}
    for ins in insights:
        if ins.video_id not in insight_map:
            insight_map[ins.video_id] = ins.analyst_view_json

    all_items: list[dict[str, Any]] = []
    for v in videos:
        ins_json = insight_map.get(v.video_id)
        scores = ins_json.get("provenance", {}).get("scores") if isinstance(ins_json, dict) else None
        statuses = _derive_youtube_statuses(
            v,
            ins_json,
            now_utc=now_utc,
            worker_online=worker_status.get("is_online"),
        )

        all_items.append({
            "video_id": v.video_id,
            "channel_id": v.channel_id,
            "channel_title": v.channel_title,
            "title": v.title,
            "published_at": v.published_at,
            "url": v.url,
            "needs_asr": v.needs_asr,
            "has_transcript": v.transcript_text is not None,
            "transcript_lang": v.transcript_lang,
            "asr_backend": v.asr_backend,
            "last_error": v.last_error,
            "created_at": v.created_at,
            "scores": scores,
            **statuses,
        })

    queue_summary = _youtube_queue_summary_from_items(all_items)
    queue_summary["scheduler"] = scheduler_status

    filtered = all_items
    if transcript_status:
        filtered = [it for it in filtered if it.get("transcript_status") == transcript_status]
    if analysis_status:
        filtered = [it for it in filtered if it.get("analysis_status") == analysis_status]
    if only_failed:
        filtered = [
            it for it in filtered
            if it.get("analysis_status") == "failed_paused" or it.get("transcript_status") == "asr_failed_paused"
        ]
    items = filtered[:limit]

    return {
        "items": items,
        "queue_summary": queue_summary,
        "worker_status": worker_status,
        "effective_llm": effective_llm,
        "server_time": now_utc,
        "filters_applied": {
            "limit": limit,
            "transcript_status": transcript_status,
            "analysis_status": analysis_status,
            "only_failed": only_failed,
        },
    }


@router.get("/api/youtube/transcript/{video_id}")
def yt_transcript_api(video_id: str, db: Session = Depends(get_db)):
    """Return transcript text and AI insight for a single video."""
    row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        return {"ok": False, "error": "Video not found"}

    insight_row = db.scalar(
        select(YoutubeInsight)
        .where(YoutubeInsight.video_id == video_id)
        .order_by(YoutubeInsight.created_at.desc())
    )
    insight_json = insight_row.analyst_view_json if insight_row and insight_row.analyst_view_json else None
    now_utc = datetime.now(timezone.utc)
    worker_status = _youtube_worker_status_snapshot(db, now_utc=now_utc)
    scheduler_status = _youtube_scheduler_snapshot(now_utc=now_utc)
    statuses = _derive_youtube_statuses(
        row,
        insight_json,
        now_utc=now_utc,
        worker_online=worker_status.get("is_online"),
    )

    return {
        "ok": True,
        "video_id": row.video_id,
        "title": row.title,
        "channel_title": row.channel_title,
        "transcript_text": row.transcript_text,
        "transcript_lang": row.transcript_lang,
        "char_count": len(row.transcript_text) if row.transcript_text else 0,
        "asr_backend": row.asr_backend,
        "asr_model": row.asr_model,
        "insight": insight_json,
        "worker_status": worker_status,
        "scheduler": scheduler_status,
        "effective_llm": _youtube_effective_llm_snapshot(),
        **statuses,
    }


@router.post("/api/youtube/analyze/{video_id}")
async def youtube_manual_retry_analyze(
    video_id: str,
    request: Request,
    db: Session = Depends(get_db),
    _admin: str = Depends(require_admin),
):
    from app.db.repository import delete_youtube_insight_by_video_id, update_youtube_video_analysis_runtime

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}
    force = bool(body.get("force", False))

    row = db.scalar(select(YoutubeVideo).where(YoutubeVideo.video_id == video_id))
    if not row:
        return {"ok": False, "video_id": video_id, "status": "error", "reason": "Video not found", "queued_for_worker": False}
    if not row.transcript_text:
        return {
            "ok": True,
            "video_id": video_id,
            "status": "skipped",
            "reason": "无转录文本，无法进行AI分析",
            "queued_for_worker": False,
            "current_analysis_status": "pending",
        }

    insight_row = db.scalar(
        select(YoutubeInsight)
        .where(YoutubeInsight.video_id == video_id)
        .order_by(YoutubeInsight.created_at.desc())
    )
    insight_json = insight_row.analyst_view_json if insight_row else None
    current_status = _derive_youtube_statuses(row, insight_json).get("analysis_status")

    if current_status == "done" and not force:
        return {
            "ok": True,
            "video_id": video_id,
            "status": "skipped",
            "reason": "该视频已有有效AI分析结果",
            "queued_for_worker": False,
            "current_analysis_status": current_status,
        }

    deleted = False
    if insight_row is not None:
        deleted = delete_youtube_insight_by_video_id(db, video_id)
    now_utc = datetime.now(timezone.utc)
    update_youtube_video_analysis_runtime(
        db,
        video_id,
        status="queued",
        stage="queued",
        started_at=now_utc,
        updated_at=now_utc,
        finished_at=None,
        retry_count=0,
        next_retry_at=None,
        last_error_type=None,
        last_error_code=None,
        last_error_message=None,
    )

    return {
        "ok": True,
        "video_id": video_id,
        "status": "queued",
        "reason": "已重新排队，等待 Worker 自动分析",
        "queued_for_worker": True,
        "current_analysis_status": current_status,
        "analysis_status_after_enqueue": "queued",
        "deleted_previous_insight": deleted,
    }
