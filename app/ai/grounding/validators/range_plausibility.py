from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import metric_group, normalize_metric_key, parse_number
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class RangePlausibilityValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        evidence = ctx.data.get("evidence") or []
        if not isinstance(evidence, list):
            return findings
        for ev_idx, ev in enumerate(evidence):
            if not isinstance(ev, dict):
                continue
            timeframe = str(ev.get("timeframe") or "")
            metrics = ev.get("metrics")
            if not isinstance(metrics, dict):
                continue
            for raw_metric, raw_value in metrics.items():
                value = parse_number(raw_value)
                if value is None:
                    continue
                metric = normalize_metric_key(raw_metric)
                group = metric_group(metric)

                if metric in {"rsi_14", "stoch_rsi_k", "stoch_rsi_d"} and not (0.0 <= value <= 100.0):
                    findings.append(
                        Finding(
                            code="METRIC_OUT_OF_RANGE",
                            severity=Severity.HARD,
                            message=f"evidence[{ev_idx}].metrics.{raw_metric} 超出范围 [0,100]: {value:.6g}",
                            metric=metric,
                            timeframe=timeframe,
                            observed=value,
                        )
                    )
                    continue

                if group == "price" and value <= 0:
                    findings.append(
                        Finding(
                            code="PRICE_NON_POSITIVE",
                            severity=Severity.HARD,
                            message=f"evidence[{ev_idx}].metrics.{raw_metric} 价格必须 > 0: {value:.6g}",
                            metric=metric,
                            timeframe=timeframe,
                            observed=value,
                        )
                    )
                    continue

                if group == "volatility" and value < 0:
                    findings.append(
                        Finding(
                            code="VOLATILITY_NEGATIVE",
                            severity=Severity.HARD,
                            message=f"evidence[{ev_idx}].metrics.{raw_metric} 波动率指标不能为负: {value:.6g}",
                            metric=metric,
                            timeframe=timeframe,
                            observed=value,
                        )
                    )
                    continue

                if group == "zscore":
                    if abs(value) > 20:
                        findings.append(
                            Finding(
                                code="ZSCORE_IMPLAUSIBLE",
                                severity=Severity.HARD,
                                message=f"evidence[{ev_idx}].metrics.{raw_metric} zscore 过大: {value:.6g}",
                                metric=metric,
                                timeframe=timeframe,
                                observed=value,
                            )
                        )
                    elif abs(value) > 10:
                        findings.append(
                            Finding(
                                code="ZSCORE_HIGH",
                                severity=Severity.WARN,
                                message=f"evidence[{ev_idx}].metrics.{raw_metric} zscore 偏高: {value:.6g}",
                                metric=metric,
                                timeframe=timeframe,
                                observed=value,
                            )
                        )
                    continue

                if metric == "funding_rate":
                    if abs(value) > 0.05:
                        findings.append(
                            Finding(
                                code="FUNDING_RATE_IMPLAUSIBLE",
                                severity=Severity.HARD,
                                message=f"evidence[{ev_idx}].metrics.{raw_metric} funding_rate 绝对值过大: {value:.6g}",
                                metric=metric,
                                timeframe=timeframe,
                                observed=value,
                            )
                        )
                    elif abs(value) > 0.01:
                        findings.append(
                            Finding(
                                code="FUNDING_RATE_HIGH",
                                severity=Severity.WARN,
                                message=f"evidence[{ev_idx}].metrics.{raw_metric} funding_rate 偏高: {value:.6g}",
                                metric=metric,
                                timeframe=timeframe,
                                observed=value,
                            )
                        )
        return findings
