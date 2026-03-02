from __future__ import annotations

from app.features.feature_pipeline import compute_and_store_latest_metric, compute_and_store_pending_metrics
from app.logging import logger


async def run_feature_job(runtime) -> dict[str, int]:
    timeframes = runtime.settings.feature_timeframe_list or ["1m"]
    total_processed = 0
    total_pending_after = 0
    total_runs = 0
    for timeframe in timeframes:
        for symbol in runtime.settings.watchlist_symbols:
            total_runs += 1
            with runtime.session_factory() as session:
                if runtime.settings.feature_incremental_enabled:
                    result = compute_and_store_pending_metrics(
                        session,
                        symbol=symbol,
                        timeframe=timeframe,
                        lookback_rows=runtime.settings.feature_lookback_rows,
                        max_pending_bars=runtime.settings.feature_max_pending_bars,
                        max_batches=runtime.settings.feature_max_batches_per_run,
                    )
                    total_processed += result.processed_rows
                    total_pending_after += result.pending_after_run
                    if result.processed_rows > 0 or result.pending_after_run > 0:
                        logger.info(
                            "feature_job increment symbol=%s tf=%s processed=%d pending_after_run=%d last_metric_ts=%s",
                            symbol,
                            timeframe,
                            result.processed_rows,
                            result.pending_after_run,
                            result.last_metric_ts,
                        )
                else:
                    compute_and_store_latest_metric(session, symbol=symbol, timeframe=timeframe)
    return {
        "rows_written": total_processed,
        "rows_read": total_runs,
        "backlog": total_pending_after,
    }

