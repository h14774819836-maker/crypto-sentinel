import json
from httpx import ReadError
from typing import Any
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.db.session import SessionLocal

from app.db.repository import (
    get_latest_market_metrics,
    list_ai_signals,
    list_alerts
)
from app.config import get_settings
from .base import ToolCategory, agent_tools

settings = get_settings()

@agent_tools.register(
    name="get_market_snapshot",
    description="Fetch the latest price, returns and volume metrics for a specific coin/symbol.",
    category=ToolCategory.READ_ONLY,
    schema={
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "The trading symbol, e.g., 'BTCUSDT' or 'ETHUSDT'"
            }
        },
        "required": ["symbol"]
    }
)
def get_market_snapshot(args: dict[str, Any]) -> str:
    symbol = str(args.get("symbol", "")).upper()
    if not symbol:
        return json.dumps({"error": "Symbol is required"})
        
    with SessionLocal() as db:
        metrics = get_latest_market_metrics(db, symbols=[symbol])
        if not metrics:
            return json.dumps({"error": f"No recent market snapshot found for {symbol}"})
            
        m = metrics[0]
        return json.dumps({
            "symbol": m.symbol,
            "ts": m.ts.isoformat() if m.ts else None,
            "price": m.close,
            "ret_1m": m.ret_1m,
            "ret_10m": m.ret_10m,
            "rolling_vol_20": m.rolling_vol_20,
            "volume_zscore": m.volume_zscore,
            "ema_ribbon_trend": m.ema_ribbon_trend
        })


@agent_tools.register(
    name="get_latest_signals",
    description="Retrieve the most recent AI trade signals (LONG/SHORT/HOLD) and their reasoning for a symbol.",
    category=ToolCategory.READ_ONLY,
    schema={
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "The trading symbol, e.g., 'BTCUSDT'"
            },
            "limit": {
                "type": "integer",
                "description": "Number of recent signals to return (default 3, max 10)",
                "default": 3
            }
        },
        "required": ["symbol"]
    }
)
def get_latest_signals(args: dict[str, Any]) -> str:
    symbol = str(args.get("symbol", "")).upper()
    if not symbol:
        return json.dumps({"error": "Symbol is required"})
        
    limit = min(int(args.get("limit", 3)), 10)
    
    with SessionLocal() as db:
        signals = list_ai_signals(db, limit=limit, symbol=symbol)
        if not signals:
            return json.dumps({"info": f"No AI signals found for {symbol}"})
            
        results = []
        for s in signals:
            results.append({
                "signal_id": s.id,
                "created_at": s.created_at.isoformat(),
                "direction": s.direction,
                "confidence": s.confidence,
                "entry_price": s.entry_price,
                "reasoning": s.reasoning,
                "model": s.model_name
            })
        return json.dumps({"signals": results})


@agent_tools.register(
    name="get_alert_history",
    description="Fetch recent technical/volatility alerts triggered by the system for a given symbol.",
    category=ToolCategory.READ_ONLY,
    schema={
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "The trading symbol, e.g., 'BTCUSDT'"
            },
            "limit": {
                "type": "integer",
                "description": "Max alerts to return (default 5)"
            }
        },
        "required": ["symbol"]
    }
)
def get_alert_history(args: dict[str, Any]) -> str:
    symbol = str(args.get("symbol", "")).upper()
    if not symbol:
        return json.dumps({"error": "Symbol is required"})
        
    limit = min(int(args.get("limit", 5)), 20)
    
    with SessionLocal() as db:
        alerts = list_alerts(db, limit=limit, symbol=symbol)
        if not alerts:
            return json.dumps({"info": f"No recent alerts for {symbol}"})
            
        results = []
        for a in alerts:
            results.append({
                "alert_id": a.event_uid,
                "created_at": a.created_at.isoformat(),
                "type": a.alert_type,
                "severity": a.severity,
                "reason": a.reason
            })
        return json.dumps({"alerts": results})
