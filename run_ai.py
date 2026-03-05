import asyncio
import argparse
from datetime import datetime, timezone

from app.config import get_settings
from app.db.session import SessionLocal
from app.web.routers.api_ai import (
    _build_recent_alerts_by_symbol,
    _build_funding_current_by_symbol,
    _build_market_ai_symbol_inputs,
)
from app.ai.openai_provider import OpenAICompatibleProvider
from app.ai.analyst import MarketAnalyst
from app.ai.analysis_flow import prepare_context_and_snapshots

async def main():
    parser = argparse.ArgumentParser(description="Standalone AI Analysis Test")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to analyze")
    parser.add_argument("--model", type=str, default=None, help="Override model name")
    args = parser.parse_args()

    settings = get_settings()
    symbol = args.symbol.upper()

    market_config = settings.resolve_llm_config("market")
    if args.model:
        market_config.model = args.model

    if not market_config.enabled or not market_config.api_key:
        print("Market AI is disabled or API key is missing.")
        return

    print(f"Initializing AI analysis for {symbol} using model {market_config.model}...")

    provider = OpenAICompatibleProvider(market_config)
    analyst = MarketAnalyst(settings, provider, market_config)

    print("Fetching contexts from DB...")
    with SessionLocal() as db:
        recent_alerts_by_symbol = _build_recent_alerts_by_symbol(db, limit=60)
        funding_current_by_symbol = _build_funding_current_by_symbol(db)
        snapshots, context = _build_market_ai_symbol_inputs(
            db,
            symbol,
            recent_alerts_by_symbol=recent_alerts_by_symbol,
            funding_current_by_symbol=funding_current_by_symbol,
        )

    if not snapshots:
        print(f"No market snapshots found for {symbol}. Run the worker first to collect data.")
        return

    print("Preparing context and snapshots...")
    context, snapshots = prepare_context_and_snapshots(
        context,
        snapshots,
        symbol,
        min_context_on_poor_data=bool(getattr(settings, "ai_min_context_on_poor_data", True)),
        min_context_on_non_tradeable=bool(getattr(settings, "ai_min_context_on_non_tradeable", True)),
    )

    print("Starting analysis...")
    async def stream_callback(txt: str):
        # Print tokens as they arrive
        print(txt, end="", flush=True)

    try:
        signals, metadata = await analyst.analyze(
            symbol, 
            snapshots, 
            context=context,
            stream_callback=stream_callback
        )
        print("\n\n========== Analysis Completed ==========")
        for sig in signals:
            print(f"Signal: {sig.direction} (Confidence: {sig.confidence})")
            print(f"Reasoning: {sig.reasoning}")
            if sig.validation_warnings:
                print(f"Warnings: {sig.validation_warnings}")
            print(f"Action Limits: Entry={sig.entry_price}, TP={sig.take_profit}, SL={sig.stop_loss}")
        print("Metadata:", metadata)
    except Exception as e:
        print(f"\nAnalysis failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
