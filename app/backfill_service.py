from __future__ import annotations

from app.config import Settings
from app.db.repository import upsert_ohlcv
from app.db.session import SessionLocal
from app.features.aggregator import rebuild_10m_range
from app.logging import logger
from app.providers.binance_provider import BinanceProvider
from app.scheduler.jobs import _candle_to_payload


async def run_backfill(days: int, symbols: list[str], settings: Settings) -> None:
    provider = BinanceProvider(settings)

    for symbol in symbols:
        logger.info("Backfill symbol=%s days=%d", symbol, days)
        candles = await provider.backfill_recent_days(symbol, days)
        if not candles:
            logger.info("No candles for %s", symbol)
            continue

        with SessionLocal() as session:
            for candle in candles:
                upsert_ohlcv(session, _candle_to_payload(candle), commit=False)
            session.commit()
            rebuilt = rebuild_10m_range(session, symbol, candles[0].ts, candles[-1].ts)
        logger.info("Backfill complete symbol=%s 1m=%d rebuilt10m=%d", symbol, len(candles), rebuilt)
