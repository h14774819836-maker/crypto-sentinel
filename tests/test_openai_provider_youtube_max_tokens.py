from __future__ import annotations

from app.ai.openai_provider import OpenAICompatibleProvider
from app.config import LLMConfig


def _cfg(model: str) -> LLMConfig:
    return LLMConfig(
        enabled=True,
        provider="deepseek",
        api_key="test",
        base_url="https://api.deepseek.com",
        model=model,
        use_reasoning="auto",
        max_concurrency=1,
        max_retries=0,
    )


def test_deepseek_chat_reasoning_does_not_force_16384():
    p = OpenAICompatibleProvider(_cfg("deepseek-chat"))
    assert p._normalize_max_tokens("8192", use_reasoning=True) <= 8192


def test_reasoner_model_can_expand_token_budget():
    p = OpenAICompatibleProvider(_cfg("deepseek-reasoner"))
    assert p._normalize_max_tokens(4096, use_reasoning=True) >= 16384

