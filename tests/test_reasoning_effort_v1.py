import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.ai.openai_provider import OpenAICompatibleProvider
from app.config import LLMConfig

pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_reasoning_effort_propagation():
    # Mock LLMConfig with reasoning_effort
    config = LLMConfig(
        enabled=True,
        provider="ark",
        api_key="test_key",
        base_url="https://ark.cn-beijing.volces.com/api/v3",
        model="doubao-seed-2-0-pro-260215",
        use_reasoning="true",
        max_concurrency=1,
        max_retries=1,
        reasoning_effort="medium"
    )

    # Initialize provider
    provider = OpenAICompatibleProvider(config)

    # Mock the AsyncOpenAI client
    with patch("app.ai.openai_provider.AsyncOpenAI") as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        provider.client = mock_client
        mock_response = MagicMock()
        mock_response.output_text = "OK"
        mock_response.output = []
        mock_response.usage = MagicMock(input_tokens=10, output_tokens=10, total_tokens=20)
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        messages = [{"role": "user", "content": "hello"}]
        await provider.generate_response(messages)

        args, kwargs = mock_client.responses.create.call_args
        assert (kwargs.get("reasoning") or {}).get("effort") == "medium"
        assert kwargs["model"] == "doubao-seed-2-0-pro-260215"
