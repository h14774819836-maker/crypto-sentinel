from __future__ import annotations


def no_trade_decision(reason: str = "V2 model not enabled") -> dict[str, str]:
    return {"decision": "NO_TRADE", "reason": reason}
