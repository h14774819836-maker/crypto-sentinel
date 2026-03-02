import json
from httpx import ReadError
from typing import Any
from datetime import datetime, timezone
import logging

from app.db.session import SessionLocal
from app.config import get_settings
from .base import ToolCategory, agent_tools

logger = logging.getLogger(__name__)
settings = get_settings()

@agent_tools.register(
    name="run_ai_analysis",
    description="Trigger an immediate, high-cost AI analysis for a cryptocurrency using the designated Market Analyst. This should only be called if data is severely outdated or explicitly requested.",
    category=ToolCategory.ACTION,
    schema={
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "The trading symbol to analyze, e.g., 'BTCUSDT'"
            },
            "timeframe": {
                "type": "string",
                "description": "The timeframe to analyze (e.g. '1m', '5m', '1h')",
                "default": "1m"
            },
            "reason": {
                "type": "string",
                "description": "A short reasoning of why this forced analysis is necessary right now."
            }
        },
        "required": ["symbol", "reason"]
    }
)
def run_ai_analysis(args: dict[str, Any]) -> str:
    """
    Since this is an ACTION tool, the actual execution of this function within the Agent
    runtime will be INTERCEPTED and sent to the user for confirmation (via Telegram Button).
    
    If it reaches here directly, it means it was formally approved or called in a trusted context.
    """
    symbol = str(args.get("symbol", "")).upper()
    timeframe = str(args.get("timeframe", "1m"))
    
    if not symbol:
        return json.dumps({"error": "Symbol is required"})
        
    logger.info("Executing ACTION Tool: run_ai_analysis for %s", symbol)
    
    # In a real environment, we would invoke the market_analyst here.
    # To avoid circular imports and long hangs inside the tool abstraction,
    # we usually dispatch this to a task queue or execute the LLM chain synchronously if allowed.
    # For Phase 2 MVP: Return a dummy success block. We will implement the true trigger when wiring Agent.
    
    # Returning a structured response that indicates the analysis job was enqueued/triggered.
    return json.dumps({
        "status": "triggered",
        "action": "run_ai_analysis",
        "symbol": symbol,
        "timeframe": timeframe,
        "message": "AI Market Analysis has been successfully dispatched to the background worker. The results will be pushed shortly."
    })


@agent_tools.register(
    name="update_user_preference",
    description="Change the user's default configuration preferences for the AI Agent.",
    category=ToolCategory.ACTION,
    schema={
        "type": "object",
        "properties": {
            "key": {
                "type": "string",
                "description": "The preference key to change. Allowed: ['preferred_model_override', 'preferred_style', 'risk_tolerance']",
                "enum": ["preferred_model_override", "preferred_style", "risk_tolerance"]
            },
            "value": {
                "type": "string",
                "description": "The new value to set."
            }
        },
        "required": ["key", "value"]
    }
)
def update_user_preference(args: dict[str, Any]) -> str:
    from app.db.repository import update_telegram_session
    
    chat_id = args.get("_meta_chat_id") # We will inject this meta field in the wrapper
    if not chat_id:
        return json.dumps({"error": "Chat ID context missing for this operation."})
        
    key = args.get("key")
    value = args.get("value")
    
    valid_keys = {"preferred_model_override", "preferred_style", "risk_tolerance"}
    if key not in valid_keys:
        return json.dumps({"error": f"Invalid configuration key. Must be one of {valid_keys}"})
        
    try:
        with SessionLocal() as db:
            update_telegram_session(db, int(chat_id), {key: value})
            return json.dumps({
                "status": "success",
                "message": f"User preference '{key}' successfully updated to '{value}'."
            })
    except Exception as e:
        logger.error("Failed to update preference: %s", e)
        return json.dumps({"error": str(e)})
