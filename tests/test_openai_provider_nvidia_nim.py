from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.ai.openai_provider import OpenAICompatibleProvider
from app.config import LLMConfig


def _cfg(model: str = "nvidia_nim/qwen3.5-397b-a17b") -> LLMConfig:
    return LLMConfig(
        enabled=True,
        provider="nvidia_nim",
        api_key="nim_test_key",
        base_url="https://integrate.api.nvidia.com/v1",
        model=model,
        use_reasoning="auto",
        max_concurrency=1,
        max_retries=0,
    )


def test_nvidia_nim_maps_alias_model_to_upstream_model():
    provider = OpenAICompatibleProvider(_cfg())
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        message = SimpleNamespace(content='{"ok":true}', reasoning_content="", tool_calls=None)
        choice = SimpleNamespace(message=message, error=None)
        return SimpleNamespace(
            choices=[choice],
            usage=SimpleNamespace(prompt_tokens=3, completion_tokens=2, total_tokens=5),
            model=kwargs.get("model"),
        )

    provider.client.chat.completions.create = _fake_create

    async def _run():
        return await provider.generate_response(
            messages=[{"role": "user", "content": "ping"}],
            response_format={"type": "json_object"},
        )

    result = asyncio.run(_run())
    assert called.get("model") == "qwen/qwen3.5-397b-a17b"
    assert result.get("model") == "qwen/qwen3.5-397b-a17b"


def test_nvidia_nim_accepts_direct_upstream_model_without_mapping():
    provider = OpenAICompatibleProvider(_cfg(model="qwen/qwen3.5-397b-a17b"))
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        message = SimpleNamespace(content='{"ok":true}', reasoning_content="", tool_calls=None)
        choice = SimpleNamespace(message=message, error=None)
        return SimpleNamespace(choices=[choice], usage=None, model=kwargs.get("model"))

    provider.client.chat.completions.create = _fake_create

    async def _run():
        await provider.generate_response(messages=[{"role": "user", "content": "ping"}])

    asyncio.run(_run())
    assert called.get("model") == "qwen/qwen3.5-397b-a17b"


def test_nvidia_nim_maps_kimi_alias_model_to_upstream_model():
    provider = OpenAICompatibleProvider(_cfg(model="nvidia_nim/kimi-k2.5"))
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        message = SimpleNamespace(content="ok", reasoning_content="", tool_calls=None)
        choice = SimpleNamespace(message=message, error=None)
        return SimpleNamespace(choices=[choice], usage=None, model=kwargs.get("model"))

    provider.client.chat.completions.create = _fake_create

    async def _run():
        await provider.generate_response(messages=[{"role": "user", "content": "ping"}])

    asyncio.run(_run())
    assert called.get("model") == "moonshotai/kimi-k2.5"


def test_nvidia_nim_kimi_with_reasoning_adds_chat_template_kwargs_and_temp():
    """Kimi K2.5 + use_reasoning 时需显式 chat_template_kwargs.thinking、temperature=1.0、top_p=0.95。"""
    provider = OpenAICompatibleProvider(_cfg(model="nvidia_nim/kimi-k2.5"))
    called: dict = {}

    async def _fake_create(**kwargs):
        called.update(kwargs)
        msg = SimpleNamespace(content="ok", reasoning_content="thought", tool_calls=None)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=msg, error=None)],
            usage=None,
            model=kwargs.get("model"),
        )

    provider.client.chat.completions.create = _fake_create

    async def _run():
        await provider.generate_response(
            messages=[{"role": "user", "content": "ping"}],
            use_reasoning=True,
        )

    asyncio.run(_run())
    extra = called.get("extra_body") or {}
    assert extra.get("chat_template_kwargs") == {"thinking": True}, "NVIDIA Kimi 需显式开启 thinking"
    assert called.get("temperature") == 1.0, "Kimi modelcard 要求 temperature=1.0"
    assert called.get("top_p") == 0.95, "Kimi modelcard 要求 top_p=0.95"
    assert called.get("max_tokens") >= 16384, "思考过程耗 token，需 16384+"


def test_nvidia_nim_maps_nemotron_ultra_deepseek_nano_to_upstream():
    """Verify Llama Nemotron Ultra, DeepSeek V3.2, Nemotron Nano alias mapping."""
    mappings = [
        ("nvidia_nim/llama-3_1-nemotron-ultra-253b-v1", "nvidia/llama-3.1-nemotron-ultra-253b-v1"),
        ("nvidia_nim/deepseek-v3_2", "deepseek-ai/deepseek-v3_2"),
        ("nvidia_nim/nemotron-3-nano-30b-a3b", "nvidia/nemotron-3-nano-30b-a3b"),
    ]
    for config_model, expected_upstream in mappings:
        provider = OpenAICompatibleProvider(_cfg(model=config_model))
        called: dict = {}

        async def _fake_create(**kwargs):
            called.update(kwargs)
            msg = SimpleNamespace(content="ok", reasoning_content="", tool_calls=None)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=msg, error=None)],
                usage=None,
                model=kwargs.get("model"),
            )

        provider.client.chat.completions.create = _fake_create
        asyncio.run(provider.generate_response(messages=[{"role": "user", "content": "ping"}]))
        assert called.get("model") == expected_upstream, f"config={config_model}"
