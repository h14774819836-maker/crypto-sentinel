import abc
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class LLMError(Exception):
    """Base exception for all LLM provider errors."""
    pass

class LLMTimeoutError(LLMError):
    """Raised when the LLM API times out."""
    pass

class LLMRateLimitError(LLMError):
    """Raised when the LLM API rate limit is exceeded (e.g., HTTP 429)."""
    pass

class LLMAuthError(LLMError):
    """Raised on authentication failure (e.g., HTTP 401/403)."""
    pass

class LLMBadRequestError(LLMError):
    """Raised on a bad request (e.g., HTTP 400)."""
    pass


@dataclass(frozen=True)
class LLMCapabilities:
    supports_json: bool
    supports_tools: bool
    supports_reasoning: bool
    supports_stream: bool


class LLMProvider(abc.ABC):
    """Abstract base class for LLM API integration."""

    @property
    @abc.abstractmethod
    def capabilities(self) -> LLMCapabilities:
        """Return the capabilities supported by this provider/model."""
        pass

    @abc.abstractmethod
    async def generate_response(
        self,
        messages: List[Dict[str, Any]],
        max_tokens: int = 4096,
        temperature: float = 0.3,
        response_format: Optional[Dict[str, str]] = None,
        use_reasoning: bool = False,
        stream_callback: Optional[callable] = None,
        stream_callback_typed: bool = False,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a request to the LLM and return the response.
        Returns a dictionary containing:
            - "content": str
            - "reasoning_content": str (if applicable and requested)
            - "prompt_tokens": int
            - "completion_tokens": int
            - "model": str (the actual model used)
            - "tool_calls": List[Dict] (if tools were called)

        stream_callback: When provided, receives streamed chunks. If stream_callback_typed
            is False (default), called as callback(text: str). If stream_callback_typed
            is True, called as callback(chunk_type: str, text: str) where chunk_type
            is "reasoning" or "content".
        """
        pass
