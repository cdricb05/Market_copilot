"""
alpha_agent/llm_providers — Stage 3 LLM provider abstraction.

Two supported providers, both bounded, secret-redacting and tool-forbidding:

* anthropic_http — the eventual unattended production provider (exact usage
  metadata, deterministic request/response contracts, cost accounting);
* claude_code   — bounded DEVELOPMENT-ONLY fallback via the already installed
  Claude Code CLI (one live validation cycle when no API credential exists).

pytest never reaches a real provider: both adapters take injectable
transports / CLI runners.
"""
from __future__ import annotations

from .anthropic_http import AnthropicHttpProvider
from .base import BaseLLMProvider, CIRCUIT_OPEN_ERROR
from .claude_code import ClaudeCodeProvider

PROVIDER_ANTHROPIC_HTTP = AnthropicHttpProvider.name
PROVIDER_CLAUDE_CODE = ClaudeCodeProvider.name

PROVIDER_CLASSES = {
    AnthropicHttpProvider.name: AnthropicHttpProvider,
    ClaudeCodeProvider.name: ClaudeCodeProvider,
}

__all__ = ["BaseLLMProvider", "AnthropicHttpProvider", "ClaudeCodeProvider",
           "PROVIDER_CLASSES", "PROVIDER_ANTHROPIC_HTTP",
           "PROVIDER_CLAUDE_CODE", "CIRCUIT_OPEN_ERROR"]
