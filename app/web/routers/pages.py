"""HTML page routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import YoutubeInsight
from app.db.repository import (
    get_latest_ai_signals,
    get_latest_futures_account_snapshot,
    get_latest_intel_digest,
    get_latest_margin_account_snapshot,
    get_latest_youtube_consensus,
    list_alerts,
    list_news_items,
    list_youtube_channels,
    list_youtube_videos,
)
from app.db.session import get_db
from app.web.shared import build_market_snapshots, settings, templates
from app.web.utils import _to_utc_or_none
from app.web.youtube_helpers import (
    _derive_youtube_statuses,
    _youtube_effective_llm_snapshot,
    _youtube_queue_summary_from_items,
    _youtube_scheduler_snapshot,
    _youtube_worker_status_snapshot,
)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def overview_page(request: Request, db: Session = Depends(get_db)):
    snapshots = build_market_snapshots(db)
    latest_alerts = list_alerts(db, limit=20)
    ai_signals = get_latest_ai_signals(db, symbols=settings.watchlist_symbols)

    yt_consensus = None
    yt_videos = []
    if settings.youtube_enabled:
        consensus_row = get_latest_youtube_consensus(db, symbol=settings.youtube_target_symbol)
        if consensus_row:
            yt_consensus = {
                "data": consensus_row.consensus_json,
                "created_at": consensus_row.created_at,
            }
        yt_videos = list_youtube_videos(db, limit=10)

    intel_digest = None
    if settings.intel_enabled:
        intel_digest_row = get_latest_intel_digest(
            db,
            symbol="GLOBAL",
            lookback_hours=settings.intel_digest_lookback_hours,
        )
        if intel_digest_row is not None:
            intel_digest = {
                "data": intel_digest_row.digest_json or {},
                "created_at": intel_digest_row.created_at,
            }

    return templates.TemplateResponse(
        "overview.html",
        {
            "request": request,
            "snapshots": snapshots,
            "alerts": latest_alerts,
            "ai_signals": ai_signals,
            "yt_consensus": yt_consensus,
            "yt_videos": yt_videos,
            "youtube_enabled": settings.youtube_enabled,
            "intel_enabled": settings.intel_enabled,
            "intel_digest": intel_digest,
            "model_registry": settings.llm_model_registry,
            "model_catalog": settings.llm_model_catalog,
            "model_tiers": settings.llm_model_tiers,
            "default_model": settings.resolve_llm_config("market").model,
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/intel", response_class=HTMLResponse)
def intel_page(request: Request, db: Session = Depends(get_db)):
    digest_row = get_latest_intel_digest(
        db,
        symbol="GLOBAL",
        lookback_hours=settings.intel_digest_lookback_hours,
    )
    news_rows = list_news_items(
        db,
        last_hours=settings.intel_digest_lookback_hours,
        limit=min(500, max(50, settings.intel_max_items_per_run * 3)),
    )
    return templates.TemplateResponse(
        "intel.html",
        {
            "request": request,
            "digest": (digest_row.digest_json or {}) if digest_row else {},
            "digest_created_at": (digest_row.created_at if digest_row else None),
            "items": news_rows,
            "generated_at": datetime.now(timezone.utc),
            "intel_enabled": settings.intel_enabled,
        },
    )


@router.get("/alerts", response_class=HTMLResponse)
def alerts_page(request: Request, db: Session = Depends(get_db)):
    alerts = list_alerts(db, limit=200)
    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "alerts": alerts,
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/strategy", response_class=HTMLResponse)
def strategy_page(request: Request):
    symbols = settings.watchlist_symbols or ["BTCUSDT"]
    return templates.TemplateResponse(
        "strategy.html",
        {
            "request": request,
            "symbols": symbols,
            "default_symbol": symbols[0],
            "base_timeframes": ["1m", "5m", "15m", "1h", "4h"],
            "scoring_modes": ["STRICT", "REALISTIC", "OPTIMISTIC"],
            "generated_at": datetime.now(timezone.utc),
            "binance_ws_url": settings.binance_ws_url,
        },
    )


@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request, db: Session = Depends(get_db)):
    futures_row = get_latest_futures_account_snapshot(db)
    margin_row = get_latest_margin_account_snapshot(db)
    return templates.TemplateResponse(
        "account.html",
        {
            "request": request,
            "futures_row": futures_row,
            "margin_row": margin_row,
            "watch_symbol": settings.account_watch_symbol.upper(),
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/youtube", response_class=HTMLResponse)
def youtube_page(request: Request, db: Session = Depends(get_db)):
    channels = list_youtube_channels(db, enabled_only=False)
    videos = list_youtube_videos(db, limit=30)
    now_utc = datetime.now(timezone.utc)
    cutoff_24h = now_utc - timedelta(hours=24)

    transcribed_count = sum(1 for v in videos if v.transcript_text)
    pending_transcript_count = sum(1 for v in videos if not v.transcript_text)
    recent_video_count_24h = sum(
        1
        for v in videos
        if (pub := _to_utc_or_none(v.published_at)) is not None and pub >= cutoff_24h
    )
    youtube_stats: dict[str, Any] = {
        "channel_count": len(channels),
        "recent_video_count_24h": recent_video_count_24h,
        "transcribed_count": transcribed_count,
        "pending_transcript_count": pending_transcript_count,
    }

    video_ids = [v.video_id for v in videos]
    insights = []
    if video_ids:
        insights = db.scalars(
            select(YoutubeInsight)
            .where(YoutubeInsight.video_id.in_(video_ids))
            .order_by(YoutubeInsight.created_at.desc())
        ).all()

    scores_map: dict[str, Any] = {}
    insight_map: dict[str, dict[str, Any]] = {}
    video_status_map: dict[str, dict[str, Any]] = {}
    worker_status = _youtube_worker_status_snapshot(db, now_utc=now_utc)
    scheduler_status = _youtube_scheduler_snapshot(now_utc=now_utc)
    for ins in insights:
        if ins.video_id not in insight_map:
            vta = ins.analyst_view_json
            if vta and isinstance(vta, dict):
                insight_map[ins.video_id] = vta
                scores = vta.get("provenance", {}).get("scores")
                if scores:
                    scores_map[ins.video_id] = scores
    queue_items_for_stats: list[dict[str, Any]] = []
    for v in videos:
        statuses = _derive_youtube_statuses(
            v,
            insight_map.get(v.video_id),
            now_utc=now_utc,
            worker_online=worker_status.get("is_online"),
        )
        video_status_map[v.video_id] = statuses
        queue_items_for_stats.append({
            "has_transcript": v.transcript_text is not None,
            **statuses,
        })
    youtube_queue_summary_initial = _youtube_queue_summary_from_items(queue_items_for_stats)

    return templates.TemplateResponse(
        "youtube.html",
        {
            "request": request,
            "channels": channels,
            "videos": videos,
            "scores_map": scores_map,
            "video_status_map": video_status_map,
            "youtube_stats": youtube_stats,
            "youtube_queue_summary_initial": youtube_queue_summary_initial,
            "youtube_worker_status": worker_status,
            "youtube_scheduler_status": scheduler_status,
            "youtube_effective_llm": _youtube_effective_llm_snapshot(),
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/llm", response_class=HTMLResponse)
def llm_debug_page(request: Request):
    return templates.TemplateResponse(
        "llm_debug.html",
        {
            "request": request,
            "youtube_enabled": settings.youtube_enabled,
            "generated_at": datetime.now(timezone.utc),
        },
    )
