from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from app.ai.provider import LLMProvider, LLMRateLimitError, LLMTimeoutError
from app.ai.prompts import SYSTEM_PROMPT, build_analysis_prompt
from app.config import LLMConfig, Settings
from app.ai.grounding.engine import DEFAULT_GROUNDING_MODE, GroundingEngine, build_facts_index, finding_to_dict
from app.logging import logger, market_ai_logger


@dataclass(slots=True)
class AiTradeSignal:
    symbol: str
    direction: str  # LONG / SHORT / HOLD
    entry_price: float | None
    take_profit: float | None
    stop_loss: float | None
    confidence: int  # 0-100
    reasoning: str
    model_name: str
    model_requested: str | None = None
    market_regime: str = ""
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    analysis_json: dict[str, Any] | None = None
    validation_warnings: list[str] | None = None


class MarketAnalyst:
    """Calls generalized LLM APIs and parses trading signals."""

    def __init__(self, settings: Settings, provider: LLMProvider, config: LLMConfig):
        self.settings = settings
        self.provider = provider
        self.config = config
        self.model = config.model
        logger.info(
            "MarketAnalyst initialised (provider=%s, model=%s, provider_type=%s, reasoning=%s)",
            self.config.provider,
            self.model,
            type(self.provider).__name__,
            self.config.use_reasoning,
        )
        self._prompt_log_max_chars = max(500, int(getattr(settings, "market_ai_prompt_log_max_chars", 12000) or 12000))
        self._response_log_max_chars = max(500, int(getattr(settings, "market_ai_response_log_max_chars", 8000) or 8000))

    async def analyze(
        self,
        symbol: str,
        snapshots: dict[str, Any],
        *,
        context: dict[str, Any] | None = None,
        stream_callback: Callable[[str], Awaitable[None]] | None = None,
    ) -> tuple[list[AiTradeSignal], dict[str, Any] | None]:
        """Run a full analysis cycle for one symbol and return parsed trade signals + tracking metadata."""
        if not snapshots:
            logger.warning("AI analysis skipped: no market snapshots for %s", symbol)
            self._log_market_event("skip_no_snapshots", symbol=symbol)
            return [], None

        user_prompt = build_analysis_prompt(symbol=symbol, snapshots=snapshots, context=context)
        logger.info(
            "========== 发送给 AI 的 Prompt 开始 (%s / %s) ==========\n%s\n========== 发送给 AI 的 Prompt 结束 ==========",
            self.model,
            symbol,
            user_prompt,
        )
        self._log_market_event(
            "request_prepared",
            symbol=symbol,
            model=self.model,
            provider=self.config.provider,
            use_reasoning=self.config.use_reasoning,
            snapshot_timeframes=sorted(list((snapshots or {}).keys())),
            prompt_chars=len(user_prompt),
            prompt_excerpt=_clip_text(user_prompt, self._prompt_log_max_chars),
            system_prompt_excerpt=_clip_text(SYSTEM_PROMPT, min(3000, self._prompt_log_max_chars)),
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        use_reasoning = self.config.use_reasoning.lower() == "true" or (
            self.config.use_reasoning.lower() == "auto" and self.provider.capabilities.supports_reasoning
        )
        # Force low temperature (greedy sampling) for rigid JSON structure
        market_temperature = max(0.0, min(0.05, float(getattr(self.config, "market_temperature", 0.01))))

        start_time = time.perf_counter()
        status = "ok"
        error_summary: str | None = None
        prompt_tokens: int | None = None
        completion_tokens: int | None = None
        actual_model = self.model
        final_signals: list[AiTradeSignal] = []

        failure_events: list[dict[str, Any]] = []
        model_attempts: list[dict[str, Any]] = []

        retry_model = self._resolve_retry_model(self.model)
        attempts = [
            {"attempt": 1, "model": self.model, "use_reasoning": use_reasoning},
            {"attempt": 2, "model": retry_model, "use_reasoning": False},
        ]
        retryable_failure_phases = {"extract_json", "json_parse", "schema", "grounding"}

        for attempt_spec in attempts:
            attempt = int(attempt_spec["attempt"])
            req_model = str(attempt_spec["model"] or self.model)
            attempt_reasoning = bool(attempt_spec["use_reasoning"])
            self._log_market_event(
                "attempt_started",
                symbol=symbol,
                attempt=attempt,
                requested_model=req_model,
                reasoning=attempt_reasoning,
            )
            model_attempts.append(
                {
                    "attempt": attempt,
                    "model_requested": req_model,
                    "use_reasoning": attempt_reasoning,
                }
            )
            try:
                response = await self.provider.generate_response(
                    messages=messages,
                    max_tokens=4096,
                    temperature=market_temperature,
                    response_format={"type": "json_object"},
                    use_reasoning=attempt_reasoning,
                    stream_callback=stream_callback,
                    model_override=req_model,
                )
            except LLMRateLimitError as exc:
                status = "429"
                error_summary = str(exc)
                logger.error("LLM RateLimit error (attempt=%s, model=%s): %s", attempt, req_model, exc)
                self._log_market_event(
                    "upstream_error",
                    symbol=symbol,
                    attempt=attempt,
                    requested_model=req_model,
                    error_type="LLMRateLimitError",
                    error=str(exc),
                )
                failure_events.append(
                    self._build_failure_event(
                        symbol=symbol,
                        attempt=attempt,
                        phase="upstream",
                        error_code="429",
                        error_summary=str(exc),
                        model_requested=req_model,
                        model_actual=req_model,
                        raw_response_excerpt=None,
                        details={"exception": "LLMRateLimitError"},
                    )
                )
                break
            except LLMTimeoutError as exc:
                status = "timeout"
                error_summary = str(exc)
                logger.error("LLM Timeout error (attempt=%s, model=%s): %s", attempt, req_model, exc)
                self._log_market_event(
                    "upstream_error",
                    symbol=symbol,
                    attempt=attempt,
                    requested_model=req_model,
                    error_type="LLMTimeoutError",
                    error=str(exc),
                )
                failure_events.append(
                    self._build_failure_event(
                        symbol=symbol,
                        attempt=attempt,
                        phase="upstream",
                        error_code="timeout",
                        error_summary=str(exc),
                        model_requested=req_model,
                        model_actual=req_model,
                        raw_response_excerpt=None,
                        details={"exception": "LLMTimeoutError"},
                    )
                )
                break
            except Exception as exc:
                status = "error"
                error_summary = str(exc)
                logger.error("LLM API call failed (attempt=%s, model=%s): %s", attempt, req_model, exc)
                self._log_market_event(
                    "upstream_error",
                    symbol=symbol,
                    attempt=attempt,
                    requested_model=req_model,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                failure_events.append(
                    self._build_failure_event(
                        symbol=symbol,
                        attempt=attempt,
                        phase="upstream",
                        error_code="error",
                        error_summary=str(exc),
                        model_requested=req_model,
                        model_actual=req_model,
                        raw_response_excerpt=None,
                        details={"exception": type(exc).__name__},
                    )
                )
                break

            content = response.get("content", "")
            reasoning_content = response.get("reasoning_content", "")
            prompt_tokens = response.get("prompt_tokens")
            completion_tokens = response.get("completion_tokens")
            actual_model = response.get("model", req_model)

            if reasoning_content:
                logger.info("LLM reasoning (CoT): %s", reasoning_content[:800])

            logger.info(
                "LLM response received (symbol=%s, attempt=%s, model=%s, prompt_tokens=%s, completion_tokens=%s)",
                symbol,
                attempt,
                actual_model,
                prompt_tokens,
                completion_tokens,
            )
            logger.debug("LLM raw content: %s", str(content)[:800])
            self._log_market_event(
                "response_received",
                symbol=symbol,
                attempt=attempt,
                requested_model=req_model,
                actual_model=actual_model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                content_chars=len(str(content or "")),
                content_excerpt=_clip_text(str(content or ""), self._response_log_max_chars),
                reasoning_excerpt=_clip_text(str(reasoning_content or ""), min(3000, self._response_log_max_chars)),
            )

            parsed, failure = self._parse_response_strict(content, symbol=symbol, snapshots=snapshots, context=context)
            if parsed:
                final_signals = parsed
                self._log_market_event(
                    "attempt_succeeded",
                    symbol=symbol,
                    attempt=attempt,
                    actual_model=actual_model,
                    signal_count=len(parsed),
                )
                break

            if not failure:
                failure = {"phase": "schema", "errors": ["未知解析失败"], "raw_response_excerpt": str(content)[:800]}
            phase = str(failure.get("phase") or "schema")
            errors = list(failure.get("errors") or [])
            failure_events.append(
                self._build_failure_event(
                    symbol=symbol,
                    attempt=attempt,
                    phase=phase,
                    error_code=phase,
                    error_summary="; ".join(errors)[:400] if errors else phase,
                    model_requested=req_model,
                    model_actual=actual_model,
                    raw_response_excerpt=failure.get("raw_response_excerpt"),
                    details={"errors": errors},
                )
            )
            self._log_market_event(
                "attempt_failed",
                symbol=symbol,
                attempt=attempt,
                phase=phase,
                errors=errors[:8],
                raw_excerpt=_clip_text(str(failure.get("raw_response_excerpt") or ""), 2000),
            )

            if attempt == 1 and phase in retryable_failure_phases:
                logger.warning("Market analysis parse/validation failed on attempt#1, retrying once (symbol=%s, phase=%s)", symbol, phase)
                self._log_market_event(
                    "retry_scheduled",
                    symbol=symbol,
                    attempt=attempt,
                    phase=phase,
                    next_model=retry_model,
                )
                continue
            break

        if final_signals:
            for sig in final_signals:
                sig.model_requested = model_attempts[-1]["model_requested"] if model_attempts else self.model
                sig.model_name = actual_model
                sig.prompt_tokens = prompt_tokens
                sig.completion_tokens = completion_tokens
        else:
            status = "error"
            if not error_summary:
                if failure_events:
                    error_summary = str(failure_events[-1].get("error_summary") or "analysis_failed")
                else:
                    error_summary = "analysis_failed"
            final_signals = [
                self._build_failed_hold_signal(
                    symbol=symbol,
                    snapshots=snapshots,
                    context=context,
                    failure_events=failure_events,
                    model_attempts=model_attempts,
                )
            ]
            for sig in final_signals:
                sig.model_requested = model_attempts[-1]["model_requested"] if model_attempts else self.model
                sig.model_name = actual_model
                sig.prompt_tokens = prompt_tokens
                sig.completion_tokens = completion_tokens

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        self._log_market_event(
            "analysis_finished",
            symbol=symbol,
            status=status,
            duration_ms=duration_ms,
            attempts=len(model_attempts),
            failure_count=len(failure_events),
            final_model=actual_model,
            error_summary=error_summary,
        )
        return final_signals, {
            "task": "market",
            "provider_name": type(self.provider).__name__,
            "model": actual_model,
            "status": status,
            "duration_ms": duration_ms,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "error_summary": error_summary,
            "failure_events": failure_events,
        }

    @staticmethod
    def _is_reasoner_model(model: str) -> bool:
        model_l = (model or "").lower()
        return "reasoner" in model_l or "r1" in model_l

    def _resolve_retry_model(self, model: str) -> str:
        if not self._is_reasoner_model(model):
            return model
        provider = (self.config.provider or "").strip().lower()
        if provider == "deepseek":
            return "deepseek-chat"
        if provider == "openrouter":
            return "deepseek/deepseek-chat"
        return model

    def _build_failure_event(
        self,
        *,
        symbol: str,
        attempt: int,
        phase: str,
        error_code: str,
        error_summary: str,
        model_requested: str,
        model_actual: str,
        raw_response_excerpt: str | None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "task": "market",
            "symbol": symbol,
            "timeframe": "1m",
            "ts": datetime.now(timezone.utc),
            "attempt": int(attempt),
            "phase": phase,
            "provider_name": type(self.provider).__name__,
            "model_requested": model_requested,
            "model_actual": model_actual,
            "error_code": error_code,
            "error_summary": error_summary[:400] if isinstance(error_summary, str) else str(error_summary),
            "raw_response_excerpt": (raw_response_excerpt or "")[:1000] if raw_response_excerpt else None,
            "details_json": details or {},
        }

    def _parse_response_strict(
        self,
        content: str,
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> tuple[list[AiTradeSignal], dict[str, Any] | None]:
        json_str = _extract_json(content)
        if not json_str:
            return [], {
                "phase": "extract_json",
                "errors": ["Could not extract JSON from AI response"],
                "raw_response_excerpt": str(content)[:1000],
            }

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            return [], {
                "phase": "json_parse",
                "errors": [f"JSON parse error: {exc}"],
                "raw_response_excerpt": str(content)[:1000],
            }

        if not isinstance(data, dict):
            return [], {
                "phase": "json_parse",
                "errors": ["AI response JSON is not an object"],
                "raw_response_excerpt": str(content)[:1000],
            }

        schema_errors = self._validate_market_schema(data)
        if schema_errors:
            return [], {
                "phase": "schema",
                "errors": schema_errors,
                "raw_response_excerpt": str(content)[:1000],
            }

        grounding_errors = self._validate_grounding(
            data,
            symbol=symbol,
            snapshots=snapshots,
            context=context,
        )
        if grounding_errors:
            return [], {
                "phase": "grounding",
                "errors": grounding_errors,
                "raw_response_excerpt": str(content)[:1000],
            }

        return self._parse_response_data(data, symbol=symbol, snapshots=snapshots, context=context), None

    def _parse_response(
        self,
        content: str,
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> list[AiTradeSignal]:
        json_str = _extract_json(content)
        if not json_str:
            logger.warning("Could not extract JSON from AI response")
            return []

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            logger.warning("JSON parse error in AI response: %s", exc)
            return []

        if not isinstance(data, dict):
            logger.warning("AI response JSON is not an object")
            return []

        return self._parse_response_data(data, symbol=symbol, snapshots=snapshots, context=context)

    def _parse_response_data(
        self,
        data: dict[str, Any],
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> list[AiTradeSignal]:

        market_regime = str(data.get("market_regime", "") or "")
        item = data.get("signal")
        if not isinstance(item, dict):
            logger.warning("AI response missing 'signal' object")
            return []

        parsed_symbol = str(item.get("symbol", symbol)).upper() or symbol.upper()
        direction = str(item.get("direction", "HOLD")).upper()
        if direction not in ("LONG", "SHORT", "HOLD"):
            direction = "HOLD"

        confidence = item.get("confidence", 0)
        if not isinstance(confidence, (int, float)):
            confidence = 0
        confidence = max(0, min(100, int(confidence)))

        reasoning = str(item.get("reasoning", "") or "").strip()
        analysis_json = self._build_analysis_json(data, market_regime, symbol=parsed_symbol)
        if not reasoning:
            reasoning = self._derive_reasoning_summary(analysis_json)

        signal = AiTradeSignal(
            symbol=parsed_symbol,
            direction=direction,
            entry_price=_safe_float(item.get("entry_price")),
            take_profit=_safe_float(item.get("take_profit")),
            stop_loss=_safe_float(item.get("stop_loss")),
            confidence=confidence,
            reasoning=reasoning,
            model_requested=None,
            model_name="",
            market_regime=market_regime,
            analysis_json=analysis_json,
            validation_warnings=[],
        )
        signal, analysis_json = self.validate_and_sanitize(signal, analysis_json, snapshots, context)
        signal.analysis_json = analysis_json
        signal.reasoning = (signal.reasoning or "").strip() or self._derive_reasoning_summary(analysis_json)
        if len(signal.reasoning) > 320:
            signal.reasoning = signal.reasoning[:319] + "…"
        if isinstance(signal.analysis_json, dict):
            sig_obj = signal.analysis_json.setdefault("signal", {})
            if isinstance(sig_obj, dict):
                sig_obj.update(
                    {
                        "symbol": signal.symbol,
                        "direction": signal.direction,
                        "entry_price": signal.entry_price,
                        "take_profit": signal.take_profit,
                        "stop_loss": signal.stop_loss,
                        "confidence": signal.confidence,
                        "reasoning": signal.reasoning,
                    }
                )

        return [signal]

    def _build_failed_hold_signal(
        self,
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
        failure_events: list[dict[str, Any]],
        model_attempts: list[dict[str, Any]],
    ) -> AiTradeSignal:
        errors = [str(item.get("error_summary") or "") for item in failure_events if isinstance(item, dict)]
        phase = str(failure_events[-1].get("phase") or "exhausted") if failure_events else "exhausted"
        attempts = len(model_attempts)

        analysis_json: dict[str, Any] = {
            "market_regime": "uncertain",
            "signal": {
                "symbol": symbol.upper(),
                "direction": "HOLD",
                "entry_price": None,
                "take_profit": None,
                "stop_loss": None,
                "confidence": 35,
                "reasoning": "结构化解析/校验失败，已降级为HOLD。",
            },
            "evidence": [],
            "anchors": [],
            "levels": {"supports": [], "resistances": []},
            "risk": {"rr": None, "sl_atr_multiple": None, "invalidations": []},
            "scenarios": {"base": "", "bull": "", "bear": ""},
            "validation_notes": [],
            "youtube_reflection": {},
            "validation": {
                "status": "failed",
                "phase": phase,
                "attempts": attempts,
                "errors": [e for e in errors if e],
                "model_attempts": model_attempts,
            },
        }
        analysis_json = attach_context_digest_to_analysis_json(analysis_json, context) or analysis_json
        return AiTradeSignal(
            symbol=symbol.upper(),
            direction="HOLD",
            entry_price=None,
            take_profit=None,
            stop_loss=None,
            confidence=35,
            reasoning="结构化解析/校验失败，已降级为HOLD。",
            model_requested=None,
            model_name="",
            market_regime="uncertain",
            analysis_json=analysis_json,
            validation_warnings=["analysis_failed"],
        )

    def _validate_market_schema(self, data: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        signal = data.get("signal")
        if not isinstance(signal, dict):
            return ["缺少 signal 对象"]
        direction = str(signal.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT", "HOLD"}:
            errors.append("signal.direction 非法")
        confidence = signal.get("confidence")
        if not isinstance(confidence, (int, float)):
            errors.append("signal.confidence 必须为数值")
        evidence = data.get("evidence")
        if not isinstance(evidence, list) or len(evidence) < 2:
            errors.append("evidence 至少需要 2 条")
        else:
            for idx, ev in enumerate(evidence):
                if not isinstance(ev, dict):
                    errors.append(f"evidence[{idx}] 必须为对象")
                    continue
                if not isinstance(ev.get("timeframe"), str):
                    errors.append(f"evidence[{idx}].timeframe 缺失或类型错误")
                if not isinstance(ev.get("point"), str):
                    errors.append(f"evidence[{idx}].point 缺失或类型错误")
                if not isinstance(ev.get("metrics"), dict):
                    errors.append(f"evidence[{idx}].metrics 缺失或类型错误")

        anchors = data.get("anchors")
        if not isinstance(anchors, list) or len(anchors) < 2:
            errors.append("anchors 至少需要 2 条")
        else:
            for idx, anchor in enumerate(anchors):
                if not isinstance(anchor, dict):
                    errors.append(f"anchors[{idx}] 必须为对象")
                    continue
                if not isinstance(anchor.get("path"), str) or not str(anchor.get("path")).strip():
                    errors.append(f"anchors[{idx}].path 缺失或类型错误")
                if not isinstance(anchor.get("value"), str):
                    errors.append(f"anchors[{idx}].value 缺失或类型错误")

        if direction == "HOLD":
            for k in ("entry_price", "take_profit", "stop_loss"):
                if signal.get(k) is not None:
                    errors.append(f"direction=HOLD 时 {k} 必须为 null")
        return errors

    def _validate_grounding(
        self,
        data: dict[str, Any],
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> list[str]:
        facts = self._build_grounding_facts(symbol=symbol, snapshots=snapshots, context=context)
        facts_index = build_facts_index(facts)
        mode = str(getattr(self.settings, "grounding_mode", DEFAULT_GROUNDING_MODE) or DEFAULT_GROUNDING_MODE)
        severe_multiplier = float(getattr(self.settings, "grounding_severe_multiplier", 3.0) or 3.0)

        result = GroundingEngine().validate(
            data=data,
            facts=facts,
            facts_index=facts_index,
            mode=mode,
            severe_multiplier=severe_multiplier,
        )

        grounding_validation_payload = {
            "mode": mode,
            "score": round(result.score, 2),
            "score_breakdown": result.score_breakdown,
            "hard_error_count": len(result.hard_errors),
            "warning_count": len(result.warnings),
            "stats": result.stats,
            "hard_errors": [finding_to_dict(item) for item in result.hard_errors[:8]],
            "top_warnings": [item.message for item in result.warnings[:5]],
            "warnings": [finding_to_dict(item) for item in result.warnings[:12]],
        }
        retry_blocking_hard = [item for item in result.hard_errors if _is_retry_blocking_grounding_finding(item)]
        non_blocking_hard = [item for item in result.hard_errors if not _is_retry_blocking_grounding_finding(item)]
        grounding_validation_payload["retry_blocking_error_count"] = len(retry_blocking_hard)
        grounding_validation_payload["non_blocking_hard_error_count"] = len(non_blocking_hard)
        grounding_validation_payload["retry_blocking_codes"] = [item.code for item in retry_blocking_hard[:8]]
        grounding_validation_payload["non_blocking_hard_codes"] = [item.code for item in non_blocking_hard[:8]]
        validation = data.setdefault("validation", {})
        if isinstance(validation, dict):
            validation["grounding"] = grounding_validation_payload

        notes = data.get("validation_notes")
        if not isinstance(notes, list):
            notes = []
            data["validation_notes"] = notes
        existing_notes = {str(item) for item in notes if isinstance(item, str)}
        for warning in result.warnings[:5]:
            if warning.message not in existing_notes:
                notes.append(warning.message)
                existing_notes.add(warning.message)
        for hard in non_blocking_hard[:5]:
            note = f"[grounding_non_blocking] {hard.message}"
            if note not in existing_notes:
                notes.append(note)
                existing_notes.add(note)

        self._log_market_event(
            "grounding_evaluated",
            symbol=symbol,
            mode=mode,
            score=round(result.score, 2),
            hard_error_count=len(result.hard_errors),
            warning_count=len(result.warnings),
            hard_codes=[item.code for item in result.hard_errors[:8]],
            retry_blocking_codes=[item.code for item in retry_blocking_hard[:8]],
            non_blocking_hard_codes=[item.code for item in non_blocking_hard[:8]],
            warning_codes=[item.code for item in result.warnings[:12]],
            hard_messages=[item.message for item in result.hard_errors[:5]],
            top_warnings=[item.message for item in result.warnings[:5]],
            score_breakdown=result.score_breakdown,
        )

        return [item.message for item in retry_blocking_hard]

    def _log_market_event(self, event: str, **payload: Any) -> None:
        base = {
            "event": event,
            "task": "market_analysis",
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        base.update(payload)
        try:
            market_ai_logger.info("[AI_MARKET] %s", json.dumps(base, ensure_ascii=False, default=str))
        except Exception:
            market_ai_logger.info("[AI_MARKET] %s payload_unserializable=%s", event, str(payload)[:800])

    def _build_grounding_facts(
        self,
        *,
        symbol: str,
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        from app.ai.prompts import _sanitize_snapshots_for_prompt
        return {
            "facts": {
                "symbol": symbol,
                "multi_tf_snapshots": _sanitize_snapshots_for_prompt(snapshots),
                "brief": (context or {}).get("brief") if isinstance(context, dict) else {},
                "funding_deltas": (context or {}).get("funding_deltas") if isinstance(context, dict) else {},
                "alerts_digest": (context or {}).get("alerts_digest") if isinstance(context, dict) else {},
                # Keep this for anchor compatibility: model occasionally emits facts.youtube_radar.* anchors.
                "youtube_radar": (context or {}).get("youtube_radar") if isinstance(context, dict) else {},
                "intel_digest": (context or {}).get("intel_digest") if isinstance(context, dict) else {},
                "data_quality": (context or {}).get("data_quality") if isinstance(context, dict) else {},
                "input_budget_meta": (context or {}).get("input_budget_meta") if isinstance(context, dict) else {},
            }
        }

    def _build_analysis_json(self, data: dict[str, Any], market_regime: str, *, symbol: str) -> dict[str, Any]:
        payload = {
            "market_regime": market_regime,
            "signal": data.get("signal") if isinstance(data.get("signal"), dict) else {},
            "evidence": data.get("evidence") if isinstance(data.get("evidence"), list) else [],
            "anchors": data.get("anchors") if isinstance(data.get("anchors"), list) else [],
            "levels": data.get("levels") if isinstance(data.get("levels"), dict) else {"supports": [], "resistances": []},
            "risk": data.get("risk") if isinstance(data.get("risk"), dict) else {},
            "scenarios": data.get("scenarios") if isinstance(data.get("scenarios"), dict) else {},
            "validation_notes": data.get("validation_notes") if isinstance(data.get("validation_notes"), list) else [],
            "youtube_reflection": data.get("youtube_reflection") if isinstance(data.get("youtube_reflection"), dict) else {},
            "validation": data.get("validation") if isinstance(data.get("validation"), dict) else {},
        }
        # Preserve any extra keys for future evolution/debugging
        extra = {k: v for k, v in data.items() if k not in payload}
        if extra:
            payload["extra"] = extra
        if not payload["signal"]:
            payload["signal"] = {"symbol": symbol}
        payload.setdefault("validation", {})
        return payload

    def _derive_reasoning_summary(self, analysis_json: dict[str, Any]) -> str:
        signal = analysis_json.get("signal") or {}
        yt = analysis_json.get("youtube_reflection") or {}
        evidence = analysis_json.get("evidence") or []
        direction = str(signal.get("direction") or "HOLD").upper()
        regime = analysis_json.get("market_regime") or "unknown"
        pieces = [f"{direction}（{regime}）"]
        if evidence and isinstance(evidence, list):
            first = evidence[0]
            if isinstance(first, dict) and first.get("point"):
                pieces.append(str(first.get("point")))
        if isinstance(yt, dict) and yt.get("status"):
            pieces.append(f"YouTube观点：{yt.get('status')}")
        return "；".join(pieces)

    def validate_and_sanitize(
        self,
        signal: AiTradeSignal,
        analysis_json: dict[str, Any],
        snapshots: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> tuple[AiTradeSignal, dict[str, Any]]:
        warnings: list[str] = []
        auto_fixes: list[str] = []
        status = "ok"
        downgrade_reason = None

        direction = signal.direction
        entry = signal.entry_price
        tp = signal.take_profit
        sl = signal.stop_loss

        # Safe normalization for HOLD
        if direction == "HOLD":
            if any(v is not None for v in (entry, tp, sl)):
                auto_fixes.append("hold_clear_prices")
            signal.entry_price = None
            signal.take_profit = None
            signal.stop_loss = None
        else:
            # Missing price fields -> downgrade to HOLD, do not invent numbers
            if entry is None or tp is None or sl is None:
                status = "downgraded"
                downgrade_reason = "缺少关键价格字段（entry/tp/sl）"
                warnings.append("缺少关键价格字段，已降级为HOLD")
                self._downgrade_to_hold(signal, confidence_cap=45)
            else:
                # Safe swap only when clearly reversed tp/sl
                if direction == "LONG" and not (tp > entry > sl):
                    if (sl > entry > tp):
                        signal.take_profit, signal.stop_loss = signal.stop_loss, signal.take_profit
                        tp, sl = signal.take_profit, signal.stop_loss
                        auto_fixes.append("swap_tp_sl_for_long")
                elif direction == "SHORT" and not (tp < entry < sl):
                    if (sl < entry < tp):
                        signal.take_profit, signal.stop_loss = signal.stop_loss, signal.take_profit
                        tp, sl = signal.take_profit, signal.stop_loss
                        auto_fixes.append("swap_tp_sl_for_short")

                if signal.direction == "LONG" and not (signal.take_profit is not None and signal.entry_price is not None and signal.stop_loss is not None and signal.take_profit > signal.entry_price > signal.stop_loss):
                    status = "downgraded"
                    downgrade_reason = "LONG 价格关系不合法"
                    warnings.append("LONG 的 TP/Entry/SL 关系不合法，已降级为HOLD")
                    self._downgrade_to_hold(signal, confidence_cap=45)
                elif signal.direction == "SHORT" and not (signal.take_profit is not None and signal.entry_price is not None and signal.stop_loss is not None and signal.take_profit < signal.entry_price < signal.stop_loss):
                    status = "downgraded"
                    downgrade_reason = "SHORT 价格关系不合法"
                    warnings.append("SHORT 的 TP/Entry/SL 关系不合法，已降级为HOLD")
                    self._downgrade_to_hold(signal, confidence_cap=45)

        rr_value, sl_atr_multiple = self._compute_rr_and_sl_atr_multiple(signal, snapshots)
        risk = analysis_json.setdefault("risk", {}) if isinstance(analysis_json, dict) else {}
        if isinstance(risk, dict):
            if rr_value is not None:
                risk.setdefault("rr", round(rr_value, 3))
            if sl_atr_multiple is not None:
                risk.setdefault("sl_atr_multiple", round(sl_atr_multiple, 3))

        if signal.direction in ("LONG", "SHORT"):
            if rr_value is None:
                warnings.append("无法计算 RR")
            elif rr_value < 2.0:
                status = "downgraded"
                downgrade_reason = f"风险收益比不足（RR={rr_value:.2f} < 2.0）"
                warnings.append(f"风险收益比不足（RR={rr_value:.2f}），已降级为HOLD")
                self._downgrade_to_hold(signal, confidence_cap=45)
            if sl_atr_multiple is not None and (sl_atr_multiple < 0.3 or sl_atr_multiple > 5.0):
                status = "downgraded"
                downgrade_reason = f"止损距离 ATR 异常（{sl_atr_multiple:.2f} ATR）"
                warnings.append(f"止损距离 ATR 异常（{sl_atr_multiple:.2f} ATR），已降级为HOLD")
                self._downgrade_to_hold(signal, confidence_cap=45)

        data_quality = (context or {}).get("data_quality") if isinstance(context, dict) else None
        if isinstance(data_quality, dict) and str(data_quality.get("overall") or "").upper() == "POOR" and signal.direction in ("LONG", "SHORT"):
            status = "downgraded"
            downgrade_reason = "数据质量较差（POOR）"
            warnings.append("数据质量较差（POOR），已降级为HOLD")
            self._downgrade_to_hold(signal, confidence_cap=45)

        analysis_json.setdefault("validation", {})
        if isinstance(analysis_json.get("validation"), dict):
            analysis_json["validation"].update(
                {
                    "status": status,
                    "auto_fixes": auto_fixes,
                    "downgrade_reason": downgrade_reason,
                    "warnings": warnings,
                    "rr": round(rr_value, 3) if isinstance(rr_value, (int, float)) else None,
                    "sl_atr_multiple": round(sl_atr_multiple, 3) if isinstance(sl_atr_multiple, (int, float)) else None,
                }
            )

        signal.validation_warnings = warnings
        if warnings and signal.reasoning:
            # Keep homepage summary readable but indicate downgrade when needed
            if any("降级" in w for w in warnings) and "已降级" not in signal.reasoning:
                signal.reasoning = signal.reasoning + "；已降级为HOLD（校验未通过）"
        return signal, analysis_json

    def _compute_rr_and_sl_atr_multiple(self, signal: AiTradeSignal, snapshots: dict[str, Any]) -> tuple[float | None, float | None]:
        if signal.direction not in ("LONG", "SHORT"):
            return None, None
        if any(v is None for v in (signal.entry_price, signal.take_profit, signal.stop_loss)):
            return None, None
        entry = float(signal.entry_price)
        tp = float(signal.take_profit)
        sl = float(signal.stop_loss)
        risk_dist = abs(entry - sl)
        reward_dist = abs(tp - entry)
        rr = (reward_dist / risk_dist) if risk_dist > 0 else None

        atr = None
        for tf in ("1m", "5m", "15m"):
            latest = ((snapshots.get(tf) or {}).get("latest") or {})
            atr_val = _safe_float(latest.get("atr_14"))
            if atr_val and atr_val > 0:
                atr = atr_val
                break
        sl_atr = (risk_dist / atr) if (atr and atr > 0) else None
        return rr, sl_atr

    @staticmethod
    def _downgrade_to_hold(signal: AiTradeSignal, confidence_cap: int = 45) -> None:
        signal.direction = "HOLD"
        signal.entry_price = None
        signal.take_profit = None
        signal.stop_loss = None
        signal.confidence = min(signal.confidence, confidence_cap)



def _extract_json(text: str) -> str | None:
    """Try to extract a JSON object from text that may contain markdown fences."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = text.strip()
    if text.startswith("{"):
        obj = _extract_first_balanced_json_object(text, start_index=0)
        if obj:
            return obj
    match = re.search(r"```[a-zA-Z]*\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if match:
        fenced = match.group(1).strip()
        if fenced.startswith("{"):
            obj = _extract_first_balanced_json_object(fenced, start_index=0)
            if obj:
                return obj
        return fenced
    brace_start = text.find("{")
    if brace_start != -1:
        obj = _extract_first_balanced_json_object(text, start_index=brace_start)
        if obj:
            return obj
    return None



def _extract_message_text(message: Any) -> str:
    """Best-effort extraction of plain text from chat completion message objects."""
    if message is None:
        return ""

    content = getattr(message, "content", None)
    text = ""

    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                for key in ("text", "content"):
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        parts.append(value)
                        break
                continue
            value = getattr(item, "text", None)
            if not isinstance(value, str):
                value = getattr(item, "content", None)
            if isinstance(value, str) and value.strip():
                parts.append(value)
        text = "\n".join(parts)

    if isinstance(text, str) and text.strip():
        return text

    reasoning_content = getattr(message, "reasoning_content", None)
    return reasoning_content if isinstance(reasoning_content, str) else ""



def _extract_first_balanced_json_object(text: str, start_index: int = 0) -> str | None:
    depth = 0
    obj_start = -1
    in_string = False
    escape = False

    for i in range(max(0, start_index), len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == '{':
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == '}':
            if depth > 0:
                depth -= 1
                if depth == 0 and obj_start >= 0:
                    return text[obj_start : i + 1]
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _clip_text(text: str, max_chars: int) -> str:
    if not isinstance(text, str):
        text = str(text)
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    remain = len(text) - max_chars
    return text[:max_chars] + f"...(truncated {remain} chars)"


def _is_retry_blocking_grounding_finding(finding: Any) -> bool:
    code = str(getattr(finding, "code", "") or "")
    path = str(getattr(finding, "path", "") or "")
    message = str(getattr(finding, "message", "") or "")
    token = code.upper()

    # Known noisy case: external-view anchor path should not force full retry.
    combined = f"{path} {message}".lower()
    if token == "ANCHOR_PATH_MISSING" and "youtube_radar" in combined:
        return False

    retry_prefixes = {
        "ANCHOR_PATH_",
        "ANCHOR_VALUE_",
        "EVIDENCE_METRIC_OUT_OF_TOL",
        "METRIC_OUT_OF_RANGE",
        "PRICE_NON_POSITIVE",
        "VOLATILITY_NEGATIVE",
        "FUNDING_RATE_IMPLAUSIBLE",
        "ZSCORE_IMPLAUSIBLE",
    }
    for prefix in retry_prefixes:
        if token.startswith(prefix):
            return True
    return False


def attach_context_digest_to_analysis_json(
    analysis_json: dict[str, Any] | None,
    context: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(analysis_json, dict):
        return analysis_json
    if not isinstance(context, dict):
        return analysis_json

    cloned = dict(analysis_json)
    context_digest: dict[str, Any] = {}

    data_quality = context.get("data_quality")
    if isinstance(data_quality, dict):
        context_digest["data_quality"] = {
            "overall": data_quality.get("overall"),
            "missing_timeframes": list(data_quality.get("missing_timeframes") or []),
            "funding_stale": data_quality.get("funding_stale"),
            "youtube_stale": data_quality.get("youtube_stale"),
            "intel_stale": data_quality.get("intel_stale"),
            "alerts_burst": data_quality.get("alerts_burst"),
            "notes": list(data_quality.get("notes") or [])[:5],
        }

    input_budget_meta = context.get("input_budget_meta")
    if isinstance(input_budget_meta, dict):
        context_digest["input_budget_meta"] = {
            "youtube_radar_chars_before_clip": input_budget_meta.get("youtube_radar_chars_before_clip"),
            "youtube_radar_chars_after_clip": input_budget_meta.get("youtube_radar_chars_after_clip"),
            "intel_digest_chars_before_clip": input_budget_meta.get("intel_digest_chars_before_clip"),
            "intel_digest_chars_after_clip": input_budget_meta.get("intel_digest_chars_after_clip"),
            "alerts_digest_chars": input_budget_meta.get("alerts_digest_chars"),
            "clip_steps_applied": list(input_budget_meta.get("clip_steps_applied") or [])[:8],
        }

    tradeable_gate = (((context.get("brief") or {}) if isinstance(context.get("brief"), dict) else {}).get("tradeable_gate"))
    if isinstance(tradeable_gate, dict):
        context_digest["tradeable_gate"] = {
            "tradeable": tradeable_gate.get("tradeable"),
            "reasons": list(tradeable_gate.get("reasons") or [])[:5],
        }

    intel_digest = context.get("intel_digest")
    if isinstance(intel_digest, dict):
        context_digest["intel_digest"] = {
            "risk_temperature": intel_digest.get("risk_temperature"),
            "high_risk_count": intel_digest.get("high_risk_count"),
            "total_items": intel_digest.get("total_items"),
            "top_narratives": list(intel_digest.get("top_narratives") or [])[:3],
            "main_characters": list(intel_digest.get("main_characters") or [])[:3],
        }

    if context_digest:
        cloned["context_digest"] = context_digest
    return cloned
