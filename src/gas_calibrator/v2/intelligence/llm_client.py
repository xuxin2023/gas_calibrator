from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LLMConfig:
    """LLM client configuration."""

    provider: str = "openai"
    model: str = "gpt-4o-mini"
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    max_tokens: int = 1200
    temperature: float = 0.2
    timeout_s: float = 30.0
    max_retries: int = 3
    fallback_to_mock: bool = True


class LLMClient(ABC):
    """Abstract LLM client."""

    def __init__(self, config: LLMConfig):
        self.config = config

    @abstractmethod
    def complete(self, prompt: str, **kwargs: Any) -> str:
        """Return a completion for a plain prompt."""

    @abstractmethod
    def chat(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        """Return a completion for a chat payload."""


class OpenAIClient(LLMClient):
    """OpenAI-backed LLM client."""

    def _create_client(self) -> Any:
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - depends on optional package
            raise RuntimeError("openai package is required to use OpenAIClient") from exc

        return OpenAI(
            api_key=self.config.api_key,
            base_url=self.config.base_url or None,
            timeout=float(self.config.timeout_s),
            max_retries=int(self.config.max_retries),
        )

    def _request_options(self, **kwargs: Any) -> Dict[str, Any]:
        return {
            "model": kwargs.get("model", self.config.model),
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
        }

    def complete(self, prompt: str, **kwargs: Any) -> str:
        client = self._create_client()
        options = self._request_options(**kwargs)
        response = client.chat.completions.create(
            model=options["model"],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=options["max_tokens"],
            temperature=options["temperature"],
        )
        return (response.choices[0].message.content or "").strip()

    def chat(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        client = self._create_client()
        options = self._request_options(**kwargs)
        response = client.chat.completions.create(
            model=options["model"],
            messages=messages,
            max_tokens=options["max_tokens"],
            temperature=options["temperature"],
        )
        return (response.choices[0].message.content or "").strip()


class MockLLMClient(LLMClient):
    """Deterministic mock client used by tests and offline runs."""

    def complete(self, prompt: str, **kwargs: Any) -> str:
        return f"[Mock Response] Received prompt: {prompt[:100]}..."

    def chat(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        return f"[Mock Response] Chat response ({len(messages)} messages)"


def is_mock_client(client: Optional[LLMClient]) -> bool:
    if client is None:
        return True
    return isinstance(client, MockLLMClient) or str(getattr(client.config, "provider", "")).strip().lower() == "mock"


def complete_with_fallback(
    client: Optional[LLMClient],
    prompt: str,
    fallback_text: str,
    **kwargs: Any,
) -> str:
    if is_mock_client(client):
        return fallback_text
    try:
        response = "" if client is None else str(client.complete(prompt, **kwargs) or "").strip()
    except Exception:
        return fallback_text
    return response or fallback_text


def chat_with_fallback(
    client: Optional[LLMClient],
    messages: List[Dict[str, str]],
    fallback_text: str,
    **kwargs: Any,
) -> str:
    if is_mock_client(client):
        return fallback_text
    try:
        response = "" if client is None else str(client.chat(messages, **kwargs) or "").strip()
    except Exception:
        return fallback_text
    return response or fallback_text


def create_llm_client(config: Optional[LLMConfig] = None) -> LLMClient:
    """Factory that keeps tests offline by default."""

    config = config or LLMConfig(provider="mock", model="mock")
    provider = str(config.provider or "mock").strip().lower()

    if provider == "mock":
        return MockLLMClient(config)

    if provider == "openai":
        if not str(config.api_key or "").strip():
            if config.fallback_to_mock:
                return MockLLMClient(LLMConfig(provider="mock", model="mock"))
            raise ValueError("OpenAI provider requires api_key")
        return OpenAIClient(config)

    if config.fallback_to_mock:
        return MockLLMClient(LLMConfig(provider="mock", model="mock"))
    raise ValueError(f"Unsupported LLM provider: {config.provider}")
