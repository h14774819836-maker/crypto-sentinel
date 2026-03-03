from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import parse_number
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class CrossFieldConsistencyValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        signal = ctx.data.get("signal")
        if not isinstance(signal, dict):
            return findings

        direction = str(signal.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            return findings

        entry = parse_number(signal.get("entry_price"))
        take_profit = parse_number(signal.get("take_profit"))
        stop_loss = parse_number(signal.get("stop_loss"))
        if entry is None or take_profit is None or stop_loss is None:
            findings.append(
                Finding(
                    code="CROSS_FIELD_PRICE_MISSING",
                    severity=Severity.HARD,
                    message="signal 缺少 entry/tp/sl，无法通过跨字段一致性校验",
                )
            )
            return findings

        if direction == "LONG" and not (take_profit > entry > stop_loss):
            findings.append(
                Finding(
                    code="CROSS_FIELD_PRICE_ORDER",
                    severity=Severity.HARD,
                    message="LONG 方向价格关系不合法，应满足 tp > entry > sl",
                )
            )
        if direction == "SHORT" and not (take_profit < entry < stop_loss):
            findings.append(
                Finding(
                    code="CROSS_FIELD_PRICE_ORDER",
                    severity=Severity.HARD,
                    message="SHORT 方向价格关系不合法，应满足 tp < entry < sl",
                )
            )

        risk = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        rr = (reward / risk) if risk > 1e-12 else None
        if rr is None:
            findings.append(
                Finding(
                    code="CROSS_FIELD_RR_UNDEFINED",
                    severity=Severity.HARD,
                    message="风险距离接近 0，RR 无法计算",
                )
            )
        elif rr < 0.8:
            findings.append(
                Finding(
                    code="CROSS_FIELD_RR_LOW",
                    severity=Severity.HARD,
                    message=f"RR 过低（{rr:.3f}）",
                    expected=2.0,
                    observed=rr,
                )
            )
        elif rr < 1.2:
            findings.append(
                Finding(
                    code="CROSS_FIELD_RR_SUBOPTIMAL",
                    severity=Severity.WARN,
                    message=f"RR 偏低（{rr:.3f}）",
                    expected=2.0,
                    observed=rr,
                )
            )

        atr = _pick_reference_atr(ctx)
        if atr is not None and atr > 1e-12:
            sl_atr = risk / atr
            if sl_atr < 0.2 or sl_atr > 8.0:
                findings.append(
                    Finding(
                        code="CROSS_FIELD_SL_ATR_EXTREME",
                        severity=Severity.HARD,
                        message=f"SL 距离 ATR 异常（{sl_atr:.3f} ATR）",
                        expected="0.3~5.0",
                        observed=sl_atr,
                    )
                )
            elif sl_atr < 0.3 or sl_atr > 5.0:
                findings.append(
                    Finding(
                        code="CROSS_FIELD_SL_ATR_WARN",
                        severity=Severity.WARN,
                        message=f"SL 距离 ATR 偏离常见区间（{sl_atr:.3f} ATR）",
                        expected="0.3~5.0",
                        observed=sl_atr,
                    )
                )

        close = ctx.facts_index.reference_price
        if close is not None and close > 1e-12:
            deviation = abs(entry - close) / close
            if deviation > 0.08:
                findings.append(
                    Finding(
                        code="CROSS_FIELD_ENTRY_FAR_FROM_CLOSE",
                        severity=Severity.HARD,
                        message=f"entry 与当前 close 偏离过大（{deviation:.2%}）",
                        observed=deviation,
                    )
                )
            elif deviation > 0.03:
                findings.append(
                    Finding(
                        code="CROSS_FIELD_ENTRY_DEVIATION",
                        severity=Severity.WARN,
                        message=f"entry 与当前 close 偏离较大（{deviation:.2%}）",
                        observed=deviation,
                    )
                )

        return findings


def _pick_reference_atr(ctx: GroundingContext) -> float | None:
    for tf in ("1m", "5m", "15m", "1h", "4h"):
        atr = ctx.facts_index.atr_by_timeframe.get(tf)
        if isinstance(atr, (int, float)) and atr > 0:
            return float(atr)
    return None
