"""Market, strategy, account, intel, alerts API routes."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.db.repository import (
    get_strategy_decision_detail,
    list_account_stats_daily,
    get_latest_futures_account_snapshot,
    get_latest_intel_digest,
    get_latest_margin_account_snapshot,
    list_alerts,
    list_ai_signals,
    list_news_items,
    list_ohlcv_range,
    list_recent_ohlcv,
    list_strategy_decisions_densified,
    list_strategy_decisions_raw,
    list_strategy_feature_stats,
    list_strategy_scores,
)
from app.db.session import get_db
from app.ops.job_metrics import read_job_metrics_from_file
from app.web.shared import build_market_snapshots, quick_db_health_and_worker_helper, settings
from app.web.utils import _datetime_from_epoch, _epoch_seconds, _json_datetime

router = APIRouter()


@router.get("/api/market")
def market_api(db: Session = Depends(get_db)):
    return {"items": build_market_snapshots(db)}


@router.get("/api/ohlcv")
def ohlcv_api(
    symbol: str = Query(..., min_length=3, max_length=20),
    timeframe: str = Query(default="1m"),
    from_ts: int | None = Query(default=None, alias="from"),
    to_ts: int | None = Query(default=None, alias="to"),
    limit: int = Query(default=5000, ge=100, le=9000),
    db: Session = Depends(get_db),
):
    symbol_u = symbol.upper()
    if from_ts is not None and to_ts is not None and from_ts <= to_ts:
        start_dt = _datetime_from_epoch(from_ts)
        end_dt = _datetime_from_epoch(to_ts)
        if start_dt is None or end_dt is None:
            raise HTTPException(status_code=400, detail="invalid time range")
        rows = list_ohlcv_range(db, symbol=symbol_u, timeframe=timeframe, start_ts=start_dt, end_ts=end_dt)
    else:
        rows = list_recent_ohlcv(db, symbol=symbol_u, timeframe=timeframe, limit=limit)
    if len(rows) > limit:
        rows = rows[-limit:]
    return {
        "items": [
            {
                "ts": _epoch_seconds(row.ts),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
            }
            for row in rows
        ]
    }


@router.get("/api/account/futures")
def futures_account_api(
    include_raw: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    row = get_latest_futures_account_snapshot(db)
    if row is None:
        return {"item": None}
    item = {
        "ts": _json_datetime(row.ts),
        "created_at": _json_datetime(row.created_at),
        "total_margin_balance": row.total_margin_balance,
        "available_balance": row.available_balance,
        "total_maint_margin": row.total_maint_margin,
        "btc_position_amt": row.btc_position_amt,
        "btc_mark_price": row.btc_mark_price,
        "btc_liquidation_price": row.btc_liquidation_price,
        "btc_unrealized_pnl": row.btc_unrealized_pnl,
    }
    if include_raw:
        item["account"] = row.account_json or {}
        item["balance"] = row.balance_json or []
        item["positions"] = row.positions_json or []
    return {"item": item}


@router.get("/api/account/margin")
def margin_account_api(
    include_raw: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    row = get_latest_margin_account_snapshot(db)
    if row is None:
        return {"item": None}
    item = {
        "ts": _json_datetime(row.ts),
        "created_at": _json_datetime(row.created_at),
        "margin_level": row.margin_level,
        "total_asset_of_btc": row.total_asset_of_btc,
        "total_liability_of_btc": row.total_liability_of_btc,
        "normal_bar": row.normal_bar,
        "margin_call_bar": row.margin_call_bar,
        "force_liquidation_bar": row.force_liquidation_bar,
    }
    if include_raw:
        item["account"] = row.account_json or {}
        item["trade_coeff"] = row.trade_coeff_json or {}
    return {"item": item}


@router.get("/api/account/stream")
async def account_stream_api(
    request: Request,
    db: Session = Depends(get_db),
):
    """Streaming endpoint for Account updates using Server-Sent Events (SSE)."""
    async def event_generator():
        last_futures_ts = None
        last_margin_ts = None
        
        # Initial comment to keep connection alive
        yield ": connected\n\n"
        
        while True:
            if await request.is_disconnected():
                break

            changed = False
            try:
                # Query db latest within loop since this runs indefinitely
                futures_row = get_latest_futures_account_snapshot(db)
                margin_row = get_latest_margin_account_snapshot(db)

                current_futures_ts = futures_row.ts if futures_row else None
                current_margin_ts = margin_row.ts if margin_row else None

                if current_futures_ts != last_futures_ts or current_margin_ts != last_margin_ts:
                    changed = True
                    last_futures_ts = current_futures_ts
                    last_margin_ts = current_margin_ts

                if changed:
                    # Construct payload
                    payload = {"futures": None, "margin": None}
                    
                    if futures_row:
                        payload["futures"] = {
                            "ts": _json_datetime(futures_row.ts),
                            "total_margin_balance": futures_row.total_margin_balance,
                            "available_balance": futures_row.available_balance,
                            "total_maint_margin": futures_row.total_maint_margin,
                            "btc_position_amt": futures_row.btc_position_amt,
                            "btc_mark_price": futures_row.btc_mark_price,
                            "btc_liquidation_price": futures_row.btc_liquidation_price,
                            "btc_unrealized_pnl": futures_row.btc_unrealized_pnl,
                        }
                    
                    if margin_row:
                        payload["margin"] = {
                            "ts": _json_datetime(margin_row.ts),
                            "margin_level": margin_row.margin_level,
                            "margin_call_bar": margin_row.margin_call_bar,
                            "force_liquidation_bar": margin_row.force_liquidation_bar,
                        }
                    
                    # Yield SSE Data block
                    yield f"data: {json.dumps(payload)}\n\n"
            
            except Exception as e:
                # Need to yield heartbeat if exception just to keep alive
                pass

            # Polling delay
            # Keep it tight enough for real-time feel, SQLite is fast
            await asyncio.sleep(1.0)
            
            # Send periodic heartbeat regardless of changes to avoid proxy timeouts
            yield ": heartbeat\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        },
    )


@router.get("/api/account/equity-curve")
def account_equity_curve_api(
    days: int = Query(default=90, ge=7, le=3650),
    db: Session = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    start_day = now - timedelta(days=max(1, int(days)) - 1)
    rows = list_account_stats_daily(
        db,
        start_day=start_day,
        end_day=now,
        limit=max(30, int(days) + 10),
    )
    items = []
    for row in rows:
        ts = _epoch_seconds(row.day_utc)
        if ts is None:
            continue
        items.append(
            {
                "ts": ts,
                "open": row.equity_open,
                "high": row.equity_high,
                "low": row.equity_low,
                "close": row.equity_close,
                "sample_count": int(row.sample_count or 0),
            }
        )
    return {"items": items}


@router.get("/api/strategy/decisions")
def strategy_decisions_api(
    symbol: str = Query(..., min_length=3, max_length=20),
    from_ts: int = Query(..., alias="from"),
    to_ts: int = Query(..., alias="to"),
    manifest_id: str | None = Query(default=None),
    side: str | None = Query(default=None),
    outcome: str | None = Query(default=None),
    regime: str | None = Query(default=None),
    mode: str = Query(default="raw"),
    cursor: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    bucket_seconds: int = Query(default=900, ge=60, le=86400),
    db: Session = Depends(get_db),
):
    symbol_u = symbol.upper()
    mode_norm = mode.lower()
    if mode_norm not in {"raw", "densified"}:
        raise HTTPException(status_code=400, detail="mode must be raw or densified")
    if from_ts > to_ts:
        raise HTTPException(status_code=400, detail="from must be <= to")

    if mode_norm == "densified":
        items = list_strategy_decisions_densified(
            db,
            symbol=symbol_u,
            from_ts=from_ts,
            to_ts=to_ts,
            manifest_id=manifest_id,
            side=side,
            outcome=outcome,
            regime=regime,
            bucket_seconds=bucket_seconds,
        )
        return {
            "mode": "densified",
            "items": items,
            "has_more": False,
            "next_cursor": None,
        }

    items, has_more, next_cursor = list_strategy_decisions_raw(
        db,
        symbol=symbol_u,
        from_ts=from_ts,
        to_ts=to_ts,
        manifest_id=manifest_id,
        side=side,
        outcome=outcome,
        regime=regime,
        cursor=cursor,
        limit=limit,
    )
    return {
        "mode": "raw",
        "items": items,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


@router.get("/api/strategy/decisions/{decision_id}")
def strategy_decision_detail_api(decision_id: int, db: Session = Depends(get_db)):
    item = get_strategy_decision_detail(db, decision_id=decision_id)
    if item is None:
        raise HTTPException(status_code=404, detail="decision not found")
    return {"item": item}


@router.get("/api/strategy/scores")
def strategy_scores_api(
    manifest_id: str | None = Query(default=None),
    split_type: str | None = Query(default=None),
    scoring_mode: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    rows = list_strategy_scores(
        db,
        manifest_id=manifest_id,
        split_type=split_type,
        scoring_mode=scoring_mode,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "manifest_id": row.manifest_id,
                "window_start_ts": row.window_start_ts,
                "window_end_ts": row.window_end_ts,
                "split_type": row.split_type,
                "scoring_mode": row.scoring_mode,
                "status": row.status,
                "n_trades": row.n_trades,
                "n_resolved": row.n_resolved,
                "n_ambiguous": row.n_ambiguous,
                "n_timeout": row.n_timeout,
                "win_rate": row.win_rate,
                "avg_r": row.avg_r,
                "win_rate_ci_low": row.win_rate_ci_low,
                "win_rate_ci_high": row.win_rate_ci_high,
                "avg_r_ci_low": row.avg_r_ci_low,
                "avg_r_ci_high": row.avg_r_ci_high,
                "timeout_rate": row.timeout_rate,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }


@router.get("/api/strategy/feature-stats")
def strategy_feature_stats_api(
    manifest_id: str | None = Query(default=None),
    split_type: str | None = Query(default=None),
    scoring_mode: str | None = Query(default=None),
    regime_id: str | None = Query(default=None),
    status: str | None = Query(default="OK"),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    rows = list_strategy_feature_stats(
        db,
        manifest_id=manifest_id,
        split_type=split_type,
        scoring_mode=scoring_mode,
        regime_id=regime_id,
        status=status,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "manifest_id": row.manifest_id,
                "window_start_ts": row.window_start_ts,
                "window_end_ts": row.window_end_ts,
                "split_type": row.split_type,
                "regime_id": row.regime_id,
                "scoring_mode": row.scoring_mode,
                "feature_key": row.feature_key,
                "bucket_key": row.bucket_key,
                "status": row.status,
                "n": row.n,
                "win_rate": row.win_rate,
                "avg_r": row.avg_r,
                "ci_low": row.ci_low,
                "ci_high": row.ci_high,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    }


@router.get("/api/intel/news")
def intel_news_api(
    last_hours: int = Query(default=24, ge=1, le=168),
    category: str | None = Query(default=None),
    severity_min: int | None = Query(default=None, ge=0, le=100),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    rows = list_news_items(
        db,
        last_hours=last_hours,
        category=category,
        severity_min=severity_min,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": row.id,
                "ts_utc": row.ts_utc,
                "source": row.source,
                "category": row.category,
                "title": row.title,
                "url": row.url,
                "summary": row.summary,
                "region": row.region,
                "topics": row.topics_json or [],
                "alert_keyword": row.alert_keyword,
                "severity": row.severity,
                "entities": row.entities_json or [],
            }
            for row in rows
        ]
    }


@router.get("/api/intel/digest")
def intel_digest_api(db: Session = Depends(get_db)):
    row = get_latest_intel_digest(
        db,
        symbol="GLOBAL",
        lookback_hours=settings.intel_digest_lookback_hours,
    )
    return {
        "item": {
            "symbol": row.symbol,
            "lookback_hours": row.lookback_hours,
            "digest": row.digest_json or {},
            "created_at": row.created_at,
        }
        if row
        else None
    }


@router.post("/api/translate")
async def translate_api(payload: dict = Body(default_factory=dict)):
    """Translate text to Simplified Chinese."""
    text = (payload.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Missing or empty 'text' field")
    if len(text) > 5000:
        raise HTTPException(status_code=400, detail="Text too long (max 5000 chars)")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                "https://libretranslate.com/translate",
                json={"q": text, "source": "auto", "target": "zh", "format": "text"},
                headers={"Content-Type": "application/json"},
            )
            if r.status_code == 200:
                data = r.json()
                translated = (data.get("translatedText") or "").strip()
                if translated:
                    return {"translated": translated, "source": "libretranslate"}
    except Exception:
        pass

    if len(text) <= 500:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    "https://api.mymemory.translated.net/get",
                    params={"q": text, "langpair": "en|zh"},
                )
                if r.status_code == 200:
                    data = r.json()
                    translated = (data.get("responseData", {}).get("translatedText") or "").strip()
                    if translated and translated != text:
                        return {"translated": translated, "source": "mymemory"}
        except Exception:
            pass

    raise HTTPException(status_code=503, detail="Translation service unavailable")


@router.get("/api/alerts")
def alerts_api(
    limit: int = Query(default=100, ge=1, le=500),
    symbol: str | None = Query(default=None),
    alert_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    rows = list_alerts(db, limit=limit, symbol=symbol, alert_type=alert_type)
    return {
        "items": [
            {
                "event_uid": row.event_uid,
                "symbol": row.symbol,
                "timeframe": row.timeframe,
                "ts": row.ts,
                "alert_type": row.alert_type,
                "severity": row.severity,
                "reason": row.reason,
                "rule_version": row.rule_version,
                "metrics": row.metrics_json,
                "sent_to_telegram": row.sent_to_telegram,
            }
            for row in rows
        ]
    }


@router.get("/api/health")
def health_api():
    start = time.perf_counter()
    db_ok, worker_last_seen = quick_db_health_and_worker_helper(settings.database_url, settings.worker_id)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    recent_jobs = read_job_metrics_from_file(settings.ops_job_metrics_file, limit=20)
    last_job = recent_jobs[-1] if recent_jobs else None
    return {
        "api_ok": True,
        "db_ok": db_ok,
        "worker_last_seen": worker_last_seen,
        "server_time": datetime.now(timezone.utc),
        "db_probe_ms": elapsed_ms,
        "ops": {
            "job_metrics_count": len(recent_jobs),
            "last_job": last_job,
        },
    }


@router.get("/api/models")
def models_api():
    return {
        "items": settings.llm_model_catalog,
        "default_model": settings.resolve_llm_config("market").model,
    }


@router.get("/api/ai-signals")
def ai_signals_api(
    limit: int = Query(default=50, ge=1, le=200),
    symbol: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    rows = list_ai_signals(db, limit=limit, symbol=symbol)

    def _analysis_debug_summary(analysis_json: Any) -> dict[str, Any] | None:
        if not isinstance(analysis_json, dict):
            return None
        validation = analysis_json.get("validation") if isinstance(analysis_json.get("validation"), dict) else {}
        risk = analysis_json.get("risk") if isinstance(analysis_json.get("risk"), dict) else {}
        yt_reflection = (
            analysis_json.get("youtube_reflection")
            if isinstance(analysis_json.get("youtube_reflection"), dict)
            else {}
        )
        context_digest = (
            analysis_json.get("context_digest")
            if isinstance(analysis_json.get("context_digest"), dict)
            else {}
        )
        data_quality = (
            context_digest.get("data_quality")
            if isinstance(context_digest.get("data_quality"), dict)
            else None
        )
        warnings = validation.get("warnings") if isinstance(validation.get("warnings"), list) else []
        return {
            "has_details": True,
            "validation_status": validation.get("status"),
            "validation_warnings": warnings,
            "warning_count": len(warnings),
            "downgrade_reason": validation.get("downgrade_reason"),
            "rr": validation.get("rr", risk.get("rr")),
            "sl_atr_multiple": validation.get("sl_atr_multiple", risk.get("sl_atr_multiple")),
            "youtube_reflection_status": yt_reflection.get("status"),
            "data_quality": data_quality,
            "context_budget": context_digest.get("input_budget_meta") if context_digest else None,
            "tradeable_gate": context_digest.get("tradeable_gate") if context_digest else None,
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        analysis_json = getattr(row, "analysis_json", None)
        analysis_summary = _analysis_debug_summary(analysis_json)
        items.append(
            {
                "symbol": row.symbol,
                "direction": row.direction,
                "entry_price": row.entry_price,
                "take_profit": row.take_profit,
                "stop_loss": row.stop_loss,
                "confidence": row.confidence,
                "reasoning": row.reasoning,
                "analysis_json": analysis_json,
                "analysis_summary": analysis_summary,
                "validation_warnings": (analysis_summary or {}).get("validation_warnings"),
                "model_requested": getattr(row, "model_requested", None),
                "model_name": row.model_name,
                "created_at": row.created_at,
            }
        )
    return {"items": items}
