from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.ai.openai_provider import OpenAICompatibleProvider
from app.config import LLMConfig


def _cfg() -> LLMConfig:
    return LLMConfig(
        enabled=True,
        provider="ark",
        api_key="ark_test_key",
        base_url="https://ark.cn-beijing.volces.com/api/v3",
        model="doubao-seed-2-0-pro-260215",
        use_reasoning="auto",
        max_concurrency=1,
        max_retries=0,
    )


def test_ark_non_stream_response_maps_usage_and_json_format():
    provider = OpenAICompatibleProvider(_cfg())
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        return SimpleNamespace(
            output_text='{"ok":true}',
            output=[],
            usage=SimpleNamespace(input_tokens=11, output_tokens=7, total_tokens=18),
            model="doubao-seed-2-0-pro-260215",
        )

    provider.client.responses.create = _fake_create

    async def _run():
        return await provider.generate_response(
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1024,
            temperature=0.2,
            response_format={"type": "json_object"},
            use_reasoning=False,
        )

    result = asyncio.run(_run())
    assert result["content"] == '{"ok":true}'
    assert result["prompt_tokens"] == 11
    assert result["completion_tokens"] == 7
    assert result["total_tokens"] == 18
    assert called.get("max_output_tokens") == 1024
    assert (called.get("thinking") or {}).get("type") == "disabled"
    assert (called.get("text") or {}).get("format", {}).get("type") == "json_object"
    assert isinstance(called.get("input"), list) and called["input"][0]["type"] == "message"


def test_ark_tool_messages_are_converted_to_function_call_and_output():
    provider = OpenAICompatibleProvider(_cfg())
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        return SimpleNamespace(
            output_text="final answer",
            output=[
                SimpleNamespace(
                    type="function_call",
                    call_id="call_123",
                    id="fc_1",
                    name="test_tool",
                    arguments='{"k":"v"}',
                )
            ],
            usage=SimpleNamespace(input_tokens=20, output_tokens=10, total_tokens=30),
            model="doubao-seed-2-0-pro-260215",
        )

    provider.client.responses.create = _fake_create

    messages = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"id": "call_123", "type": "function", "function": {"name": "test_tool", "arguments": '{"k":"v"}'}}
            ],
        },
        {"role": "tool", "tool_call_id": "call_123", "name": "test_tool", "content": '{"ok":1}'},
    ]

    async def _run():
        return await provider.generate_response(
            messages=messages,
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "test_tool",
                        "description": "demo",
                        "parameters": {"type": "object", "properties": {"k": {"type": "string"}}},
                    },
                }
            ],
            tool_choice="auto",
        )

    result = asyncio.run(_run())
    assert result["tool_calls"][0]["id"] == "call_123"
    assert result["tool_calls"][0]["function"]["name"] == "test_tool"
    input_items = called.get("input") or []
    assert any(item.get("type") == "function_call" for item in input_items)
    assert any(item.get("type") == "function_call_output" for item in input_items)
    assert any(item.get("type") == "function" for item in (called.get("tools") or []))


def test_ark_reasoning_true_enables_thinking():
    provider = OpenAICompatibleProvider(_cfg())
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        return SimpleNamespace(
            output_text="ok",
            output=[],
            usage=SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2),
            model="doubao-seed-2-0-pro-260215",
        )

    provider.client.responses.create = _fake_create

    async def _run():
        return await provider.generate_response(
            messages=[{"role": "user", "content": "ping"}],
            use_reasoning=True,
        )

    asyncio.run(_run())
    assert (called.get("thinking") or {}).get("type") == "enabled"
    assert (called.get("reasoning") or {}).get("effort") == "medium"


def test_ark_streaming_maps_text_reasoning_and_usage():
    provider = OpenAICompatibleProvider(_cfg())

    class _Stream:
        def __init__(self, events):
            self.events = events

        def __aiter__(self):
            self._iter = iter(self.events)
            return self

        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration:
                raise StopAsyncIteration

    async def _fake_create(**kwargs):
        assert kwargs.get("stream") is True
        final_resp = SimpleNamespace(
            output_text="",
            output=[],
            usage=SimpleNamespace(input_tokens=5, output_tokens=3, total_tokens=8),
            model="doubao-seed-2-0-pro-260215",
        )
        return _Stream(
            [
                SimpleNamespace(type="response.output_text.delta", delta="Hel"),
                SimpleNamespace(type="response.reasoning_text.delta", delta="R"),
                SimpleNamespace(type="response.output_text.delta", delta="lo"),
                SimpleNamespace(type="response.completed", response=final_resp),
            ]
        )

    provider.client.responses.create = _fake_create

    chunks: list[str] = []

    async def _cb(text: str):
        chunks.append(text)

    async def _run():
        return await provider.generate_response(
            messages=[{"role": "user", "content": "hello"}],
            stream_callback=_cb,
        )

    result = asyncio.run(_run())
    assert "".join(chunks) == "HelRlo"
    assert result["content"] == "Hello"
    assert result["reasoning_content"] == "R"
    assert result["prompt_tokens"] == 5
    assert result["completion_tokens"] == 3
