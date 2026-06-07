"""
LLM client – abstract interface + EchoLLMClient (fake, no network).

LLMClient defines the contract that SimulationService depends on.
EchoLLMClient satisfies it without touching any external provider,
making the full service pipeline testable without credentials or network.

To swap in a real provider in a future sub-task, implement LLMClient
and pass the new instance to SimulationService.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List


class LLMClient(ABC):
    """Abstract interface for a text-generation client."""

    @abstractmethod
    def generate(self, prompt_messages: List[Dict[str, str]]) -> str:
        """
        Generate a response given a list of prompt messages.

        Args:
            prompt_messages: list of {"role": str, "content": str} dicts.

        Returns:
            Generated text as a plain string.
        """


class EchoLLMClient(LLMClient):
    """
    Fake LLM client for development and testing.

    Returns a deterministic placeholder response that confirms the pipeline
    ran end-to-end without calling any external service.  The response
    includes the system prompt length and the user message so tests can
    assert on meaningful content without fragile string matching.
    """

    def generate(self, prompt_messages: List[Dict[str, str]]) -> str:
        system_len = sum(
            len(m["content"]) for m in prompt_messages if m.get("role") == "system"
        )
        user_msg = next(
            (m["content"] for m in prompt_messages if m.get("role") == "user"),
            "(no user message)",
        )
        return (
            f"[Echo Simulation] "
            f"system_prompt_chars={system_len} | "
            f"user_message={user_msg[:80]!r}"
        )
