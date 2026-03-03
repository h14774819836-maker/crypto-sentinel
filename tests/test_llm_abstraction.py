import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from openai._exceptions import RateLimitError
import httpx

from app.config import Settings
from app.ai.provider import LLMRateLimitError, LLMCapabilities
from app.ai.openai_provider import OpenAICompatibleProvider

@pytest.fixture
def mock_settings():
    import json
    profiles = {
        "general": {
            "enabled": True,
            "provider": "openai_compatible",
            "model": "default-model",
            "use_reasoning": "auto"
        },
        "market": {
            "enabled": True,
            "provider": "openai_compatible",
            "model": "market-model",
            "use_reasoning": "auto",
            "api_key_override": "market_key"
        },
        "youtube": {
            "enabled": True,
            "provider": "openai_compatible",
            "model": "default-model",
            "use_reasoning": "true"
        }
    }
    return Settings(
        _env_file=None,
        openai_api_key="default_key",
        llm_profiles_json=json.dumps(profiles)
    )


def test_config_parsing(mock_settings):
    """Test that LLM task configs cascade from default to task-specific."""
    market_config = mock_settings.resolve_llm_config("market")
    assert market_config.enabled is True
    assert market_config.api_key == "market_key"
    assert market_config.model == "market-model"
    assert market_config.use_reasoning == "auto"

    youtube_config = mock_settings.resolve_llm_config("youtube")
    assert youtube_config.enabled is True
    assert youtube_config.api_key == "default_key"  # Fallback to default
    assert youtube_config.model == "default-model"  # Fallback to default
    assert youtube_config.use_reasoning == "true"   # Overridden


def test_failure_fallback_rate_limit(mock_settings):
    """Test that OpenRouter/DeepSeek 429 RateLimit triggers retry with backoff."""
    youtube_config = mock_settings.resolve_llm_config("youtube")
    provider = OpenAICompatibleProvider(youtube_config)

    provider.client.chat.completions.create = AsyncMock()
    err = RateLimitError(
        "Rate limit exceeded",
        response=httpx.Response(429, request=httpx.Request("POST", "https://api.openai.com")),
        body=None
    )
    provider.client.chat.completions.create.side_effect = err

    async def _run():
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(LLMRateLimitError):
                await provider.generate_response(messages=[{"role": "user", "content": "Hello"}])
            assert mock_sleep.call_count == youtube_config.max_retries

    asyncio.run(_run())


def test_provider_routing_isolation(mock_settings):
    """Test that market jobs and youtube jobs hit different instantiated providers."""
    from app.scheduler.jobs import WorkerRuntime
    from app.ai.analyst import MarketAnalyst

    market_config = mock_settings.resolve_llm_config("market")
    youtube_config = mock_settings.resolve_llm_config("youtube")

    market_provider = OpenAICompatibleProvider(market_config)
    youtube_provider = OpenAICompatibleProvider(youtube_config)

    market_analyst = MarketAnalyst(mock_settings, market_provider, market_config)

    runtime = WorkerRuntime(
        settings=mock_settings,
        session_factory=MagicMock(),
        provider=MagicMock(),
        telegram=MagicMock(),
        started_at=MagicMock(),
        version="1.0",
        market_analyst=market_analyst,
        youtube_llm_provider=youtube_provider,
    )

    assert runtime.market_analyst.provider is not runtime.youtube_llm_provider
    assert runtime.market_analyst.model == "market-model"
    assert runtime.youtube_llm_provider.model == "default-model"


def test_task_routing_supports_telegram_chat_profile_mapping():
    import json

    profiles = {
        "general": {
            "enabled": True,
            "provider": "deepseek",
            "model": "deepseek-chat",
            "use_reasoning": "auto",
        },
        "cheap_chat": {
            "enabled": True,
            "provider": "deepseek",
            "model": "deepseek-chat",
            "use_reasoning": "false",
        },
        "market": {
            "enabled": True,
            "provider": "openrouter",
            "model": "deepseek/deepseek-r1",
            "use_reasoning": "auto",
        },
    }
    routing = {
        "telegram_chat": "cheap_chat",
        "selfcheck": "market",
    }
    settings = Settings(
        _env_file=None,
        deepseek_api_key="ds_key",
        openrouter_api_key="or_key",
        llm_profiles_json=json.dumps(profiles),
        llm_task_routing_json=json.dumps(routing),
    )

    telegram_config = settings.resolve_llm_config("telegram_chat")
    selfcheck_config = settings.resolve_llm_config("selfcheck")

    assert settings.resolve_llm_profile_name("telegram_chat") == "cheap_chat"
    assert telegram_config.provider == "deepseek"
    assert telegram_config.api_key == "ds_key"
    assert telegram_config.model == "deepseek-chat"
    assert selfcheck_config.provider == "openrouter"
    assert selfcheck_config.api_key == "or_key"


def test_provider_auto_base_url_and_keys_for_deepseek_and_openrouter():
    import json

    profiles = {
        "general": {
            "enabled": True,
            "provider": "deepseek",
            "model": "deepseek-chat",
            "use_reasoning": "auto",
        },
        "market": {
            "enabled": True,
            "provider": "openrouter",
            "model": "deepseek/deepseek-r1",
            "use_reasoning": "auto",
        },
    }

    settings = Settings(
        _env_file=None,
        deepseek_api_key="ds_key",
        openrouter_api_key="or_key",
        llm_profiles_json=json.dumps(profiles),
    )

    general_cfg = settings.resolve_llm_config("telegram_chat")  # default route -> general
    market_cfg = settings.resolve_llm_config("market")

    assert general_cfg.api_key == "ds_key"
    assert general_cfg.base_url == "https://api.deepseek.com"
    assert market_cfg.api_key == "or_key"
    assert market_cfg.base_url == "https://openrouter.ai/api/v1"


def test_provider_auto_base_url_and_key_for_ark():
    import json

    profiles = {
        "general": {
            "enabled": True,
            "provider": "ark",
            "model": "doubao-seed-2-0-pro-260215",
            "use_reasoning": "auto",
        },
    }

    settings = Settings(
        _env_file=None,
        ark_api_key="ark_key",
        llm_profiles_json=json.dumps(profiles),
    )

    cfg = settings.resolve_llm_config("telegram_chat")
    assert cfg.provider == "ark"
    assert cfg.api_key == "ark_key"
    assert cfg.base_url == "https://ark.cn-beijing.volces.com/api/v3"


def test_provider_auto_base_url_and_key_for_nvidia_nim():
    import json

    profiles = {
        "general": {
            "enabled": True,
            "provider": "nvidia_nim",
            "model": "nvidia_nim/qwen3.5-397b-a17b",
            "use_reasoning": "auto",
        },
    }

    settings = Settings(
        _env_file=None,
        nvidia_nim_api_key="nim_key",
        llm_profiles_json=json.dumps(profiles),
    )

    cfg = settings.resolve_llm_config("telegram_chat")
    assert cfg.provider == "nvidia_nim"
    assert cfg.api_key == "nim_key"
    assert cfg.base_url == "https://integrate.api.nvidia.com/v1"


def test_provider_nvidia_nim_key_with_quotes_is_sanitized():
    import json

    profiles = {
        "general": {
            "enabled": True,
            "provider": "nvidia_nim",
            "model": "nvidia_nim/qwen3.5-397b-a17b",
            "use_reasoning": "auto",
        },
    }

    settings = Settings(
        _env_file=None,
        nvidia_nim_api_key="'nim_key_quoted'",
        llm_profiles_json=json.dumps(profiles),
    )

    cfg = settings.resolve_llm_config("telegram_chat")
    assert cfg.api_key == "nim_key_quoted"


def test_model_registry_includes_doubao_2():
    settings = Settings(_env_file=None)
    items = settings.llm_model_registry
    hit = next((x for x in items if x.get("id") == "doubao-seed-2-0-pro-260215"), None)
    assert hit is not None
    assert hit.get("label") == "豆包2.0"


def test_provider_generate_response_respects_model_override(mock_settings):
    cfg = mock_settings.resolve_llm_config("market")
    cfg.model = "deepseek-reasoner"
    provider = OpenAICompatibleProvider(cfg)
    called_kwargs: dict = {}

    async def _fake_create(**kwargs):
        called_kwargs.update(kwargs)
        message = SimpleNamespace(content="{}", reasoning_content="", tool_calls=None)
        choice = SimpleNamespace(message=message, error=None)
        return SimpleNamespace(choices=[choice], usage=None, model=kwargs.get("model"))

    provider.client.chat.completions.create = _fake_create

    async def _run():
        await provider.generate_response(
            messages=[{"role": "user", "content": "ping"}],
            temperature=0.1,
            response_format={"type": "json_object"},
            use_reasoning=False,
            model_override="deepseek-chat",
        )

    asyncio.run(_run())
    assert called_kwargs.get("model") == "deepseek-chat"
    assert called_kwargs.get("temperature") == 0.1
    assert called_kwargs.get("response_format") == {"type": "json_object"}
