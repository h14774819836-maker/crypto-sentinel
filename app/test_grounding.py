import json
import sys
import logging

from app.ai.analyst import MarketAnalyst
from app.config import AppState

logging.basicConfig(level=logging.INFO)

state = AppState.load_or_default()
analyst = MarketAnalyst(state.config)

# Dummy simple facts
snapshots = {
    "1h": {
        "latest": {"close": 60000, "rsi_14": 45.2, "ema_ribbon_trend": "DOWN"}
    }
}
context = {
    "brief": {"tradeable_gate": {"tradeable": True}}
}

# Simulate typical doubt-seed-2-0-pro output
response_content = """{
    "market_regime": "ranging",
    "signal": {
        "symbol": "BTCUSDT",
        "direction": "HOLD",
        "entry_price": null,
        "take_profit": null,
        "stop_loss": null,
        "confidence": 40,
        "reasoning": "Test reasoning"
    },
    "anchors": [
        {
            "path": "facts.1h.latest.close",
            "value": "60000"
        }
    ],
    "evidence": [
        {
            "timeframe": "1h",
            "point": "Price is 60k",
            "metrics": {"close": 60000, "rsi_14": 45.20}
        }
    ]
}"""

signals, failure = analyst._parse_response_strict(
    response_content,
    symbol="BTCUSDT",
    snapshots=snapshots,
    context=context
)

if failure:
    print("FAILURE DETAILS:")
    print(json.dumps(failure, indent=2, ensure_ascii=False))
else:
    print("PARSED SUCCESSFULLY")
