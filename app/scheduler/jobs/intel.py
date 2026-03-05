"""Intel jobs: news fetch and digest generation."""
from __future__ import annotations

from typing import Any

from app.logging import logger
from app.scheduler.runtime import WorkerRuntime


async def intel_news_job(runtime: WorkerRuntime) -> dict[str, Any]:
    settings = runtime.settings
    if not bool(getattr(settings, "intel_enabled", False)):
        return {"rows_read": 0, "rows_written": 0}

    from app.db.repository import upsert_news_item
    from app.news.service import IntelService

    own_client = None
    http_client = runtime.http_client
    if http_client is None:
        import httpx

        own_client = httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=10.0))
        http_client = own_client

    try:
        service = IntelService(http_client, max_items_per_feed=max(10, settings.intel_max_items_per_run // 6))
        items = await service.fetch_news_items(max_items_per_run=settings.intel_max_items_per_run)
        if not items:
            return {"rows_read": 0, "rows_written": 0}
        with runtime.session_factory() as session:
            for item in items:
                upsert_news_item(session, item, commit=False)
            session.commit()
        logger.info("[intel] fetched=%d", len(items))
        return {"rows_read": len(items), "rows_written": len(items)}
    finally:
        if own_client is not None:
            await own_client.aclose()


async def intel_digest_job(runtime: WorkerRuntime) -> dict[str, Any]:
    settings = runtime.settings
    if not bool(getattr(settings, "intel_enabled", False)):
        return {"rows_read": 0, "rows_written": 0}

    from app.db.repository import get_latest_intel_digest, list_news_items, save_intel_digest
    from app.news.service import IntelService

    own_client = None
    http_client = runtime.http_client
    if http_client is None:
        import httpx

        own_client = httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=10.0))
        http_client = own_client

    try:
        service = IntelService(http_client)
        with runtime.session_factory() as session:
            rows = list_news_items(
                session,
                last_hours=settings.intel_digest_lookback_hours,
                limit=max(50, settings.intel_max_items_per_run * 3),
            )
            row_payloads = [
                {
                    "ts_utc": r.ts_utc,
                    "source": r.source,
                    "category": r.category,
                    "title": r.title,
                    "summary": r.summary,
                    "raw_text": r.raw_text,
                    "region": r.region,
                    "topics_json": r.topics_json or [],
                    "alert_keyword": r.alert_keyword,
                    "severity": r.severity,
                    "entities_json": r.entities_json or [],
                }
                for r in rows
            ]
            digest = service.build_digest(row_payloads, lookback_hours=settings.intel_digest_lookback_hours)
            save_intel_digest(
                session,
                {
                    "symbol": "GLOBAL",
                    "lookback_hours": settings.intel_digest_lookback_hours,
                    "digest_json": digest,
                },
                commit=False,
            )
            session.commit()
            latest = get_latest_intel_digest(
                session,
                symbol="GLOBAL",
                lookback_hours=settings.intel_digest_lookback_hours,
            )
        logger.info(
            "[intel_digest] items=%d risk_temperature=%s ts=%s",
            len(rows),
            (digest or {}).get("risk_temperature"),
            getattr(latest, "created_at", None),
        )
        return {"rows_read": len(rows), "rows_written": 1}
    finally:
        if own_client is not None:
            await own_client.aclose()
