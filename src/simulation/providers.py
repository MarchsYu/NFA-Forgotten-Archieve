# -*- coding: utf-8 -*-
"""
Simulation LLM providers – concrete client implementations.

Environment variables
---------------------
SIM_LLM_PROVIDER        "echo" (default) | "openai_compatible"
SIM_LLM_API_KEY         Required for openai_compatible.
SIM_LLM_BASE_URL        OpenAI-compatible endpoint base URL.
                        Default: "https://api.openai.com/v1"
SIM_LLM_MODEL           Model name (e.g. "gpt-4o-mini").
                        Required for openai_compatible; no default.
SIM_LLM_TIMEOUT_SECONDS Request timeout in seconds. Default: 60.

Usage
-----
    # Auto-configure from environment:
    client = build_llm_client()

    # Explicit override (e.g. CLI --provider flag):
    client = build_llm_client(provider="openai_compatible")
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from src.simulation.llm_client import EchoLLMClient, LLMClient

# ---------------------------------------------------------------------------
# Provider name constants
# ---------------------------------------------------------------------------

PROVIDER_ECHO             = "echo"
PROVIDER_OPENAI_COMPAT    = "openai_compatible"
_KNOWN_PROVIDERS          = {PROVIDER_ECHO, PROVIDER_OPENAI_COMPAT}

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------

_ENV_PROVIDER   = "SIM_LLM_PROVIDER"
_ENV_API_KEY    = "SIM_LLM_API_KEY"
_ENV_BASE_URL   = "SIM_LLM_BASE_URL"
_ENV_MODEL      = "SIM_LLM_MODEL"
_ENV_TIMEOUT    = "SIM_LLM_TIMEOUT_SECONDS"

_DEFAULT_BASE_URL = "https://api.openai.com/v1"
_DEFAULT_TIMEOUT  = 60


# ---------------------------------------------------------------------------
# OpenAI-compatible client
# ---------------------------------------------------------------------------

class OpenAICompatibleLLMClient(LLMClient):
    """
    LLM client for any OpenAI-compatible Chat Completions endpoint.

    Compatible with OpenAI, OpenRouter, and similar gateways.
    Uses httpx for HTTP – no heavy SDK required.

    Args:
        api_key:  Bearer token.  Never logged or included in exceptions.
        base_url: Endpoint base URL (default: https://api.openai.com/v1).
        model:    Model name (e.g. "gpt-4o-mini").
        timeout:  Request timeout in seconds (default: 60).
    """

    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: int = _DEFAULT_TIMEOUT,
    ):
        if not api_key:
            raise ValueError(
                "OpenAICompatibleLLMClient requires a non-empty api_key. "
                f"Set the {_ENV_API_KEY} environment variable."
            )
        if not model:
            raise ValueError(
                "OpenAICompatibleLLMClient requires a non-empty model name. "
                f"Set the {_ENV_MODEL} environment variable."
            )
        self._api_key  = api_key
        self._model    = model
        self._base_url = base_url.rstrip("/")
        self._timeout  = timeout

    def generate(self, prompt_messages: List[Dict[str, str]]) -> str:
        """
        Call the Chat Completions endpoint and return the assistant reply.

        Raises:
            RuntimeError: on HTTP error, network failure, or unexpected response.
                          The API key is never included in the error message.
        """
        import httpx  # imported lazily so tests can mock at the call site

        url = f"{self._base_url}/chat/completions"
        payload = {
            "model": self._model,
            "messages": prompt_messages,
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = httpx.post(
                url,
                json=payload,
                headers=headers,
                timeout=self._timeout,
            )
        except httpx.TimeoutException:
            raise RuntimeError(
                f"LLM request timed out after {self._timeout}s "
                f"(model={self._model}, base_url={self._base_url})."
            )
        except httpx.RequestError as exc:
            raise RuntimeError(
                f"LLM request failed: {type(exc).__name__} "
                f"(model={self._model}, base_url={self._base_url})."
            ) from exc

        if response.status_code != 200:
            raise RuntimeError(
                f"LLM API returned HTTP {response.status_code} "
                f"(model={self._model}, base_url={self._base_url}). "
                f"Response body: {response.text[:200]}"
            )

        try:
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            # content may be None when finish_reason is "tool_calls" or similar.
            # Treat None as an empty string rather than returning a non-str value.
            if content is None:
                raise RuntimeError(
                    "LLM response contained content=null (finish_reason may be "
                    "'tool_calls' or 'stop' with no text). "
                    f"model={self._model}, base_url={self._base_url}."
                )
            return content
        except (KeyError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"Unexpected LLM response shape: {exc}. "
                f"Raw body (first 200 chars): {response.text[:200]}"
            ) from exc


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_llm_client(
    provider: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    timeout: Optional[int] = None,
) -> LLMClient:
    """
    Build the appropriate LLMClient from arguments + environment variables.

    Argument precedence: explicit kwarg > environment variable > default.

    Args:
        provider:  Override SIM_LLM_PROVIDER.
        api_key:   Override SIM_LLM_API_KEY.
        base_url:  Override SIM_LLM_BASE_URL.
        model:     Override SIM_LLM_MODEL.
        timeout:   Override SIM_LLM_TIMEOUT_SECONDS.

    Returns:
        A configured LLMClient instance.

    Raises:
        ValueError: unknown provider or missing required config.
    """
    resolved_provider = (provider or os.getenv(_ENV_PROVIDER, PROVIDER_ECHO)).strip().lower()

    if resolved_provider not in _KNOWN_PROVIDERS:
        raise ValueError(
            f"Unknown SIM_LLM_PROVIDER '{resolved_provider}'. "
            f"Valid values: {sorted(_KNOWN_PROVIDERS)}."
        )

    if resolved_provider == PROVIDER_ECHO:
        return EchoLLMClient()

    # --- openai_compatible ---
    resolved_api_key  = api_key  or os.getenv(_ENV_API_KEY,  "")
    resolved_base_url = base_url or os.getenv(_ENV_BASE_URL, _DEFAULT_BASE_URL)
    resolved_model    = model    or os.getenv(_ENV_MODEL,    "")

    _raw_timeout = os.getenv(_ENV_TIMEOUT, str(_DEFAULT_TIMEOUT))
    if timeout is not None:
        resolved_timeout = timeout
    else:
        try:
            resolved_timeout = int(_raw_timeout)
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid value for {_ENV_TIMEOUT}: {_raw_timeout!r}. "
                f"Must be a positive integer (seconds)."
            )

    if not resolved_api_key:
        raise ValueError(
            f"Provider '{PROVIDER_OPENAI_COMPAT}' requires an API key. "
            f"Set the {_ENV_API_KEY} environment variable or pass api_key=."
        )
    if not resolved_model:
        raise ValueError(
            f"Provider '{PROVIDER_OPENAI_COMPAT}' requires a model name. "
            f"Set the {_ENV_MODEL} environment variable or pass model=."
        )

    return OpenAICompatibleLLMClient(
        api_key=resolved_api_key,
        model=resolved_model,
        base_url=resolved_base_url,
        timeout=resolved_timeout,
    )
