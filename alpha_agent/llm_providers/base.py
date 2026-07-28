"""
alpha_agent/llm_providers/base.py — shared provider contract.

Every provider:
* is audit-able without a call (credential presence by env-var NAME only,
  model resolution, classification);
* completes a prompt with bounded timeout, bounded retries, an exponential
  backoff and a per-provider circuit breaker;
* redacts every known secret value from any error text it emits;
* rejects tool calls and (where structured output is required) non-JSON;
* returns a uniform result dict — usage metadata verbatim when the provider
  reports it, None when it does not (never fabricated).

Secrets: values are read from an injectable `env` mapping into memory only for
request construction; they are never logged, persisted or echoed. The
`secret_values` list drives redaction of anything a provider might print.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Mapping, Optional

from ..llm_contracts import response_hash

CIRCUIT_OPEN_ERROR = "CIRCUIT_OPEN"
CB_CLOSED = "CLOSED"
CB_OPEN = "OPEN"


def redact_secrets(text: Any, secret_values: list[str]) -> str:
    out = str(text if text is not None else "")
    for value in secret_values:
        if value:
            out = out.replace(value, "***")
    return out


class BaseLLMProvider:
    """Common retry / breaker / redaction scaffolding."""

    name = "base"
    classification_ready = "UNDEFINED"

    def __init__(self, cfg: dict, *, env: Optional[Mapping[str, str]] = None,
                 sleep_fn: Callable[[float], None] = time.sleep,
                 secret_values: Optional[list[str]] = None):
        self.cfg = cfg or {}
        self.env: Mapping[str, str] = env if env is not None else {}
        self.sleep_fn = sleep_fn
        self.secret_values = list(secret_values or [])
        self.timeout_seconds = float(self.cfg.get("timeout_seconds", 120))
        self.max_retries = int(self.cfg.get("max_retries", 2))
        self.backoff_seconds = float(self.cfg.get("backoff_seconds", 2.0))
        self.breaker_threshold = int(self.cfg.get("circuit_breaker_threshold", 3))
        self.consecutive_failures = 0
        self.breaker_state = CB_CLOSED
        self.retry_count = 0

    # ------------------------------------------------------------------ #
    def redact(self, text: Any) -> str:
        return redact_secrets(text, self.secret_values)

    def _register_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.breaker_threshold:
            self.breaker_state = CB_OPEN

    def _register_success(self) -> None:
        self.consecutive_failures = 0
        self.breaker_state = CB_CLOSED

    def _result(self, *, ok: bool, status: str, text: Optional[str] = None,
                usage: Optional[dict] = None, model: Optional[str] = None,
                request_id: Optional[str] = None, error: Optional[str] = None,
                retries: int = 0, usage_reliable: bool = False) -> dict:
        return {
            "ok": ok, "provider": self.name, "status": status,
            "model": model, "request_id": request_id,
            "response_text": text,
            "response_hash": response_hash(text) if text is not None else None,
            "usage": usage, "usage_reliable": usage_reliable,
            "error": self.redact(error) if error else None,
            "retries": retries,
            "circuit_breaker_state": self.breaker_state,
        }

    # ------------------------------------------------------------------ #
    def circuit_open(self) -> bool:
        return self.breaker_state == CB_OPEN

    def audit(self) -> dict:  # pragma: no cover - abstract
        raise NotImplementedError

    def complete(self, prompt_obj: dict, *, max_output_tokens: int,
                 output_schema: Optional[dict] = None) -> dict:
        """Guarded entry point: breaker check then provider-specific call.

        `output_schema` is an optional structured-output JSON Schema. Only the
        development claude_code provider uses it (via the CLI --json-schema
        flag); the production provider ignores it so its behavior is
        unchanged."""
        if self.circuit_open():
            return self._result(ok=False, status=CIRCUIT_OPEN_ERROR,
                                error="circuit breaker OPEN after %d consecutive "
                                      "failures" % self.consecutive_failures)
        return self._complete(prompt_obj, max_output_tokens=max_output_tokens,
                              output_schema=output_schema)

    def _complete(self, prompt_obj: dict, *, max_output_tokens: int,
                  output_schema: Optional[dict] = None
                  ) -> dict:  # pragma: no cover - abstract
        raise NotImplementedError


__all__ = ["BaseLLMProvider", "redact_secrets", "CIRCUIT_OPEN_ERROR",
           "CB_CLOSED", "CB_OPEN"]
