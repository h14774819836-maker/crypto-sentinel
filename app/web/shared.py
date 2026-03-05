"""Shared state for web routes: templates, settings, ASR semaphore, helpers."""
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.repository import get_latest_market_metrics, get_latest_ohlcv
from app.services.health_probe import quick_db_health_and_worker
from app.web.utils import format_bj_time

settings = get_settings()
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["bj_time"] = format_bj_time

_asr_semaphore: asyncio.Semaphore | None = None


def get_asr_semaphore() -> asyncio.Semaphore:
    global _asr_semaphore
    if _asr_semaphore is None:
        _asr_semaphore = asyncio.Semaphore(settings.asr_max_concurrent)
    return _asr_semaphore


def build_market_snapshots(db: Session) -> list[dict]:
    metric_rows = get_latest_market_metrics(db, symbols=settings.watchlist_symbols, timeframe="1m")
    metrics_by_symbol = {row.symbol: row for row in metric_rows}

    snapshots: list[dict] = []
    for symbol in settings.watchlist_symbols:
        metric = metrics_by_symbol.get(symbol)
        if metric is not None:
            snapshots.append(
                {
                    "symbol": symbol,
                    "price": metric.close,
                    "ret_1m": metric.ret_1m,
                    "ret_10m": metric.ret_10m,
                    "rolling_vol_20": metric.rolling_vol_20,
                    "volume_zscore": metric.volume_zscore,
                    "updated_at": metric.ts,
                }
            )
            continue

        latest_candle = get_latest_ohlcv(db, symbol=symbol, timeframe="1m")
        if latest_candle is None:
            snapshots.append(
                {
                    "symbol": symbol,
                    "price": None,
                    "ret_1m": None,
                    "ret_10m": None,
                    "rolling_vol_20": None,
                    "volume_zscore": None,
                    "updated_at": None,
                }
            )
            continue

        snapshots.append(
            {
                "symbol": symbol,
                "price": latest_candle.close,
                "ret_1m": None,
                "ret_10m": None,
                "rolling_vol_20": None,
                "volume_zscore": None,
                "updated_at": latest_candle.ts,
            }
        )

    return snapshots


def quick_db_health_and_worker_helper(database_url: str, worker_id: str) -> tuple[bool, datetime | None]:
    """Thin wrapper for health probe."""
    return quick_db_health_and_worker(database_url, worker_id)
