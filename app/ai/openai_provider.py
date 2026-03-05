import asyncio
import json
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI
from openai._exceptions import APIConnectionError, AuthenticationError, BadRequestError, RateLimitError

from app.ai.provider import (
    LLMAuthError,
    LLMBadRequestError,
    LLMCapabilities,
    LLMError,
    LLMProvider,
    LLMRateLimitError,
    LLMTimeoutError,
)
from app.config import LLMConfig
from app.logging import logger


def _usage_dict(usage: Any) -> dict[str, Any]:
    if not usage:
        return {}
    prompt_tokens = getattr(usage, "prompt_tokens", None)
    completion_tokens = getattr(usage, "completion_tokens", None)
    if prompt_tokens is None:
        prompt_tokens = getattr(usage, "input_tokens", None)
    if completion_tokens is None:
        completion_tokens = getattr(usage, "output_tokens", None)
    out: dict[str, Any] = {}
    if prompt_tokens is not None:
        out["prompt_tokens"] = prompt_tokens
    if completion_tokens is not None:
        out["completion_tokens"] = completion_tokens
    total_tokens = getattr(usage, "total_tokens", None)
    if total_tokens is not None:
        out["total_tokens"] = total_tokens
    cost = getattr(usage, "cost", None)
    if cost is not None:
        out["cost"] = cost
    return out


def _s(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return str(v)


class OpenAICompatibleProvider(LLMProvider):
    def __init__(self, config: LLMConfig):
        self.config = config
        self.model = config.model
        headers: dict[str, str] = {}
        if config.http_referer:
            headers["HTTP-Referer"] = config.http_referer
        if config.x_title:
            headers["X-Title"] = config.x_title
        self.client = AsyncOpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
            default_headers=headers or None,
        )
        self.semaphore = asyncio.Semaphore(max(1, config.max_concurrency))
        self.max_retries = max(0, config.max_retries)

        logger.info(
            "OpenAICompatibleProvider initialized: provider=%s, base_url=%s, model=%s, concurrency=%d, retries=%d, has_referrer=%s, has_title=%s",
            self.config.provider,
            self.config.base_url,
            self.model,
            self.config.max_concurrency,
            self.config.max_retries,
            bool(config.http_referer),
            bool(config.x_title),
        )

    @property
    def capabilities(self) -> LLMCapabilities:
        is_reasoner = self._is_reasoner_model(self.model)
        return LLMCapabilities(
            supports_json=not is_reasoner,
            supports_tools=not is_reasoner,
            supports_reasoning=True,
            supports_stream=True,
        )

    def _uses_responses_api(self) -> bool:
        return (self.config.provider or "").strip().lower() == "ark"

    def _is_reasoner_model(self, model: str | None = None) -> bool:
        model_l = (model or self.model or "").lower()
        return "reasoner" in model_l or "r1" in model_l

    def _resolve_upstream_model(self, model: str | None) -> str:
        model_norm = (model or self.model or "").strip()
        provider = (self.config.provider or "").strip().lower()
        if provider != "nvidia_nim":
            return model_norm
        aliases = {
            "nvidia_nim/qwen3.5-397b-a17b": "qwen/qwen3.5-397b-a17b",
            "nvidia_nim/kimi-k2.5": "moonshotai/kimi-k2.5",
            "nvidia_nim/llama-3_1-nemotron-ultra-253b-v1": "nvidia/llama-3.1-nemotron-ultra-253b-v1",
            "nvidia_nim/deepseek-v3_2": "deepseek-ai/deepseek-v3_2",
            "nvidia_nim/nemotron-3-nano-30b-a3b": "nvidia/nemotron-3-nano-30b-a3b",
        }
        return aliases.get(model_norm.lower(), model_norm)

    def _normalize_max_tokens(self, requested: Any, use_reasoning: bool, model: str | None = None) -> int:
        try:
            value = int(requested)
        except Exception:
            value = 4096
        value = max(1, value)
        is_reasoner = self._is_reasoner_model(model)
        model_l = (model or self.model or "").lower()
        if is_reasoner and use_reasoning:
            value = max(value, 16384)
        if ("deepseek-chat" in model_l or model_l.endswith("/deepseek-chat")) and not is_reasoner:
            value = min(value, 8192)
        return value

    def _request_summary(
        self,
        *,
        model: str,
        messages: List[Dict[str, Any]],
        use_reasoning: bool,
        final_max_tokens: int,
        response_format: Optional[Dict[str, str]],
        tools: Optional[List[Dict[str, Any]]],
        stream_callback: Optional[callable],
    ) -> dict[str, Any]:
        prompt_chars = 0
        for msg in messages or []:
            c = (msg or {}).get("content")
            if isinstance(c, str):
                prompt_chars += len(c)
            elif isinstance(c, list):
                for item in c:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        prompt_chars += len(item.get("text"))
        return {
            "provider": self.config.provider,
            "model": model,
            "use_reasoning": bool(use_reasoning),
            "final_max_tokens": final_max_tokens,
            "response_format_type": (response_format or {}).get("type") if isinstance(response_format, dict) else None,
            "tools_count": len(tools or []),
            "stream": bool(stream_callback is not None),
            "messages_count": len(messages or []),
            "prompt_chars_estimate": prompt_chars,
            "ts_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    def _msg_content_parts(self, content: Any) -> list[dict[str, Any]]:
        if isinstance(content, str):
            return [{"type": "input_text", "text": content}]
        if not isinstance(content, list):
            return [{"type": "input_text", "text": _s(content)}]
        out: list[dict[str, Any]] = []
        for it in content:
            if isinstance(it, str):
                out.append({"type": "input_text", "text": it})
                continue
            if not isinstance(it, dict):
                t = _s(it)
                if t:
                    out.append({"type": "input_text", "text": t})
                continue
            t = _s(it.get("type")).lower()
            if t in {"input_text", "text", "output_text"} and isinstance(it.get("text"), str):
                out.append({"type": "input_text", "text": it.get("text")})
            elif t == "input_image":
                obj: dict[str, Any] = {"type": "input_image"}
                if it.get("image_url"):
                    obj["image_url"] = it.get("image_url")
                if it.get("file_id"):
                    obj["file_id"] = it.get("file_id")
                if it.get("detail"):
                    obj["detail"] = it.get("detail")
                if obj.get("image_url") or obj.get("file_id"):
                    out.append(obj)
        return out or [{"type": "input_text", "text": ""}]

    def _to_responses_input(self, messages: List[Dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for msg in messages or []:
            if not isinstance(msg, dict):
                continue
            role = _s(msg.get("role")).lower().strip()
            if role in {"system", "developer", "user", "assistant"}:
                out.append({"type": "message", "role": role, "content": self._msg_content_parts(msg.get("content"))})
                if role == "assistant" and isinstance(msg.get("tool_calls"), list):
                    for tc in msg.get("tool_calls") or []:
                        if not isinstance(tc, dict):
                            continue
                        fn = tc.get("function") or {}
                        if not isinstance(fn, dict):
                            continue
                        call_id = _s(tc.get("id")).strip()
                        name = _s(fn.get("name")).strip()
                        args = _s(fn.get("arguments") or "{}")
                        if call_id and name:
                            out.append({"type": "function_call", "call_id": call_id, "name": name, "arguments": args})
                continue
            if role == "tool":
                call_id = _s(msg.get("tool_call_id")).strip()
                if not call_id:
                    continue
                raw = msg.get("content")
                payload = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False, default=str)
                out.append({"type": "function_call_output", "call_id": call_id, "output": payload})
        return out

    def _to_responses_tools(self, tools: Optional[List[Dict[str, Any]]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for t in tools or []:
            if not isinstance(t, dict):
                continue
            fn = t.get("function") or {}
            if not isinstance(fn, dict):
                continue
            name = _s(fn.get("name")).strip()
            if not name:
                continue
            obj: dict[str, Any] = {
                "type": "function",
                "name": name,
                "parameters": fn.get("parameters") or {},
                "strict": bool(fn.get("strict", False)),
            }
            if isinstance(fn.get("description"), str) and fn.get("description").strip():
                obj["description"] = fn.get("description")
            out.append(obj)
        return out

    def _normalize_responses_result(self, response: Any, fallback_model: str, *, content: str | None = None, reasoning: str | None = None) -> dict[str, Any]:
        output_items = getattr(response, "output", None) or []
        text = content if content is not None else _s(getattr(response, "output_text", None))
        if not text:
            for item in output_items:
                if _s(getattr(item, "type", None)) != "message":
                    continue
                parts = getattr(item, "content", None) or []
                chunks = []
                for p in parts:
                    if _s(getattr(p, "type", None)) == "output_text" and isinstance(getattr(p, "text", None), str):
                        chunks.append(getattr(p, "text"))
                if chunks:
                    text = "".join(chunks)
                    break

        rtxt = reasoning if reasoning is not None else ""
        if reasoning is None:
            chunks = []
            for item in output_items:
                if _s(getattr(item, "type", None)) != "reasoning":
                    continue
                for part in (getattr(item, "summary", None) or []):
                    if isinstance(getattr(part, "text", None), str):
                        chunks.append(getattr(part, "text"))
                for part in (getattr(item, "content", None) or []):
                    if isinstance(getattr(part, "text", None), str):
                        chunks.append(getattr(part, "text"))
            rtxt = "".join(chunks)

        tool_calls: list[dict[str, Any]] = []
        for item in output_items:
            if _s(getattr(item, "type", None)) != "function_call":
                continue
            call_id = _s(getattr(item, "call_id", None)).strip() or _s(getattr(item, "id", None)).strip()
            name = _s(getattr(item, "name", None)).strip()
            args = _s(getattr(item, "arguments", None) or "{}")
            if call_id and name:
                tool_calls.append({"id": call_id, "type": "function", "function": {"name": name, "arguments": args}})

        usage = _usage_dict(getattr(response, "usage", None))
        return {
            "content": text,
            "reasoning_content": rtxt,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "cost": usage.get("cost"),
            "model": _s(getattr(response, "model", None)).strip() or fallback_model,
            "tool_calls": tool_calls,
        }

    async def _call_responses_api(
        self,
        *,
        messages: List[Dict[str, Any]],
        max_tokens_norm: int,
        temperature: float,
        response_format: Optional[Dict[str, str]],
        use_reasoning: bool,
        stream_callback: Optional[callable],
        tools: Optional[List[Dict[str, Any]]],
        tool_choice: Optional[str],
        effective_model: str,
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "model": effective_model,
            "input": self._to_responses_input(messages),
            "max_output_tokens": max_tokens_norm,
        }
        if not self._is_reasoner_model(effective_model):
            kwargs["temperature"] = temperature
        thinking_type = "enabled" if use_reasoning else "disabled"
        kwargs["thinking"] = {"type": thinking_type}
        effort = self.config.reasoning_effort or "medium"
        if use_reasoning or self.config.reasoning_effort:
            kwargs["reasoning"] = {"effort": effort}
        if isinstance(response_format, dict) and _s(response_format.get("type")).lower() == "json_object":
            kwargs["text"] = {"format": {"type": "json_object"}}

        rs_tools = self._to_responses_tools(tools)
        if rs_tools:
            kwargs["tools"] = rs_tools
            if tool_choice:
                kwargs["tool_choice"] = tool_choice

        if stream_callback is None:
            resp = await self.client.responses.create(**kwargs)
            return self._normalize_responses_result(resp, effective_model)

        kwargs["stream"] = True
        stream = await self.client.responses.create(**kwargs)
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        final_resp: Any = None

        async for event in stream:
            et = _s(getattr(event, "type", None))
            if et == "response.output_text.delta":
                delta = _s(getattr(event, "delta", None))
                if delta:
                    content_parts.append(delta)
                    await stream_callback(delta)
                continue
            if et == "response.reasoning_text.delta":
                delta = _s(getattr(event, "delta", None))
                if delta:
                    reasoning_parts.append(delta)
                    await stream_callback(delta)
                continue
            if et == "response.error":
                err = getattr(event, "error", None)
                raise LLMError(_s(getattr(err, "message", None) or err))
            if et == "response.completed":
                final_resp = getattr(event, "response", None)

        if final_resp is None:
            raise LLMError("responses stream completed without final response object")
        return self._normalize_responses_result(
            final_resp,
            effective_model,
            content="".join(content_parts),
            reasoning="".join(reasoning_parts),
        )

    async def _call_chat_completions(
        self,
        *,
        messages: List[Dict[str, Any]],
        max_tokens_norm: int,
        temperature: float,
        response_format: Optional[Dict[str, str]],
        use_reasoning: bool,
        stream_callback: Optional[callable],
        tools: Optional[List[Dict[str, Any]]],
        tool_choice: Optional[str],
        effective_model: str,
    ) -> Dict[str, Any]:
        api_kwargs: Dict[str, Any] = {
            "model": effective_model, 
            "messages": messages, 
            "max_tokens": max_tokens_norm,
        }

        if self.config.reasoning_effort:
            api_kwargs["reasoning_effort"] = self.config.reasoning_effort

        if use_reasoning or self._is_reasoner_model(effective_model):
            if self._is_reasoner_model(effective_model):
                api_kwargs["max_tokens"] = self._normalize_max_tokens(api_kwargs["max_tokens"], use_reasoning, model=effective_model)
        else:
            api_kwargs["temperature"] = temperature
            if response_format:
                api_kwargs["response_format"] = response_format
            if tools:
                api_kwargs["tools"] = tools
                if tool_choice:
                    api_kwargs["tool_choice"] = tool_choice

        if stream_callback is not None and self.capabilities.supports_stream:
            api_kwargs["stream"] = True
            api_kwargs["stream_options"] = {"include_usage": True}
            response = await self.client.chat.completions.create(**api_kwargs)
            final_content = ""
            final_reasoning = ""
            prompt_tokens = 0
            completion_tokens = 0
            total_tokens = 0
            cost = None
            actual_model = effective_model
            async for chunk in response:
                cm = getattr(chunk, "model", None)
                if cm:
                    actual_model = cm
                if not chunk.choices:
                    u = _usage_dict(getattr(chunk, "usage", None))
                    if u:
                        prompt_tokens = u.get("prompt_tokens", prompt_tokens)
                        completion_tokens = u.get("completion_tokens", completion_tokens)
                        total_tokens = u.get("total_tokens", total_tokens)
                        cost = u.get("cost", cost)
                    continue
                choice = chunk.choices[0]
                if getattr(choice, "error", None):
                    err = choice.error
                    raise LLMError(getattr(err, "message", str(err)))
                delta = choice.delta
                d_reasoning = getattr(delta, "reasoning_content", None) or ""
                d_content = getattr(delta, "content", None) or ""
                stream_text = d_reasoning if d_reasoning else d_content
                if stream_text:
                    await stream_callback(stream_text)
                final_reasoning += d_reasoning
                final_content += d_content
                u = _usage_dict(getattr(chunk, "usage", None))
                if u:
                    prompt_tokens = u.get("prompt_tokens", prompt_tokens)
                    completion_tokens = u.get("completion_tokens", completion_tokens)
                    total_tokens = u.get("total_tokens", total_tokens)
                    cost = u.get("cost", cost)
            return {
                "content": final_content,
                "reasoning_content": final_reasoning,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "cost": cost,
                "model": actual_model,
                "tool_calls": [],
            }

        response = await self.client.chat.completions.create(**api_kwargs)
        choice = response.choices[0] if response.choices else None
        if choice is None or choice.message is None:
            raise LLMError("API returned empty response choices")
        if getattr(choice, "error", None):
            err = choice.error
            raise LLMError(getattr(err, "message", str(err)))

        content = getattr(choice.message, "content", "") or ""
        reasoning_content = getattr(choice.message, "reasoning_content", "") or ""
        tool_calls = []
        raw_tool_calls = getattr(choice.message, "tool_calls", None)
        if raw_tool_calls:
            for tc in raw_tool_calls:
                tool_calls.append({"id": tc.id, "type": tc.type, "function": {"name": tc.function.name, "arguments": tc.function.arguments}})

        usage = _usage_dict(getattr(response, "usage", None))
        actual_model = getattr(response, "model", None) or self.model or effective_model
        return {
            "content": content,
            "reasoning_content": reasoning_content,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "cost": usage.get("cost"),
            "model": actual_model,
            "tool_calls": tool_calls,
        }

    async def generate_response(
        self,
        messages: List[Dict[str, Any]],
        max_tokens: int = 4096,
        temperature: float = 0.3,
        response_format: Optional[Dict[str, str]] = None,
        use_reasoning: bool = False,
        stream_callback: Optional[callable] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_model = (model_override or self.model or "").strip() or self.model
        upstream_model = self._resolve_upstream_model(effective_model)
        max_tokens_norm = self._normalize_max_tokens(max_tokens, use_reasoning, model=upstream_model)

        request_summary = self._request_summary(
            model=upstream_model,
            messages=messages,
            use_reasoning=use_reasoning,
            final_max_tokens=max_tokens_norm,
            response_format=response_format,
            tools=tools,
            stream_callback=stream_callback,
        )
        request_summary["configured_model"] = effective_model


        retries = 0
        base_delay = 1.0
        while True:
            try:
                async with self.semaphore:
                    if self._uses_responses_api():
                        return await self._call_responses_api(
                            messages=messages,
                            max_tokens_norm=max_tokens_norm,
                            temperature=temperature,
                            response_format=response_format,
                            use_reasoning=use_reasoning,
                            stream_callback=stream_callback,
                            tools=tools,
                            tool_choice=tool_choice,
                            effective_model=upstream_model,
                        )
                    return await self._call_chat_completions(
                        messages=messages,
                        max_tokens_norm=max_tokens_norm,
                        temperature=temperature,
                        response_format=response_format,
                        use_reasoning=use_reasoning,
                        stream_callback=stream_callback,
                        tools=tools,
                        tool_choice=tool_choice,
                        effective_model=upstream_model,
                    )
            except RateLimitError as e:
                logger.warning("LLM Rate Limit Error (429) for model %s: %s | req=%s", self.model, str(e), request_summary)
                if retries >= self.max_retries:
                    raise LLMRateLimitError(f"Rate limit exceeded after {retries} retries: {e}") from e
                delay = base_delay * (2 ** retries) + random.uniform(0.1, 1.0)
                logger.info("Retrying in %.2f seconds", delay)
                await asyncio.sleep(delay)
                retries += 1
            except APIConnectionError as e:
                logger.warning("LLM Timeout/Connection Error for model %s: %s | req=%s", self.model, str(e), request_summary)
                if retries >= self.max_retries:
                    raise LLMTimeoutError(f"Connection error after {retries} retries: {e}") from e
                delay = base_delay * (2 ** retries) + random.uniform(0.1, 1.0)
                logger.info("Retrying in %.2f seconds", delay)
                await asyncio.sleep(delay)
                retries += 1
            except AuthenticationError as e:
                logger.error("LLM Auth Error for model %s: %s | req=%s", self.model, str(e), request_summary)
                raise LLMAuthError(str(e)) from e
            except BadRequestError as e:
                logger.error("LLM Bad Request for model %s: %s | req=%s", self.model, str(e), request_summary)
                raise LLMBadRequestError(str(e)) from e
            except Exception as e:
                logger.error("Unexpected LLM Error for model %s: %s | req=%s", self.model, str(e), request_summary)
                raise LLMError(f"Unexpected error: {e}") from e
