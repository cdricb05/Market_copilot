"""
alpha_agent/llm_providers/anthropic_http.py — production provider adapter.

Anthropic Messages API over the standard-library HTTP stack (no SDK install).

* Credential: env var named in cfg["allowed_env_vars"] (default
  ANTHROPIC_API_KEY). Presence is checked by NAME; the value is read into
  memory ONLY to build the request header and is never logged or persisted.
* Model: env ALPHA_AGENT_LLM_MODEL when present, else cfg["default_model"].
  Never silently invented; the exact identifier is recorded everywhere.
* No `tools` field is ever sent; any `tool_use` content block in a response is
  rejected. Non-JSON payloads are rejected by the director's strict parser.
* Bounded timeout + bounded retries (5xx/429/network only) + circuit breaker.
* Captures the provider request id and exact input/output/cache token usage
  verbatim when returned — usage is authoritative, never estimated here.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Optional

from ..llm_contracts import (PC_BLOCKED_CREDENTIAL, PC_BLOCKED_ERROR,
                             PC_BLOCKED_MODEL, PC_PRODUCTION_READY)
from .base import BaseLLMProvider

DEFAULT_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_ANTHROPIC_VERSION = "2023-06-01"
_RETRYABLE_STATUS = {429, 500, 502, 503, 504, 529}


def default_transport(request: dict, timeout: float) -> dict:
    """Real HTTPS transport (stdlib). Injectable so pytest never reaches it."""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(request["url"], method="POST",
                                 data=request["body"].encode("utf-8"),
                                 headers=request["headers"])
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return {"status": int(resp.status), "headers": dict(resp.headers),
                    "body": resp.read().decode("utf-8", "replace"), "error": None}
    except urllib.error.HTTPError as exc:
        return {"status": int(exc.code), "headers": dict(exc.headers or {}),
                "body": exc.read().decode("utf-8", "replace"), "error": None}
    except Exception as exc:  # noqa: BLE001 - network family varies
        return {"status": None, "headers": {}, "body": "",
                "error": "%s: %s" % (type(exc).__name__, exc)}


class AnthropicHttpProvider(BaseLLMProvider):
    name = "anthropic_http"
    classification_ready = PC_PRODUCTION_READY

    def __init__(self, cfg: dict, *,
                 transport: Optional[Callable[[dict, float], dict]] = None,
                 **kwargs: Any):
        super().__init__(cfg, **kwargs)
        self.transport = transport or default_transport

    # ------------------------------------------------------------------ #
    def _resolve_key(self) -> Optional[str]:
        for name in self.cfg.get("allowed_env_vars", ["ANTHROPIC_API_KEY"]):
            value = self.env.get(name)
            if value:
                return value
        return None

    def resolve_model(self) -> Optional[str]:
        env_name = self.cfg.get("model_env_var", "ALPHA_AGENT_LLM_MODEL")
        return self.env.get(env_name) or self.cfg.get("default_model") or None

    def audit(self) -> dict:
        key_present = self._resolve_key() is not None
        model = self.resolve_model()
        if not key_present:
            classification = PC_BLOCKED_CREDENTIAL
        elif not model:
            classification = PC_BLOCKED_MODEL
        else:
            classification = PC_PRODUCTION_READY
        return {"provider": self.name, "classification": classification,
                "credential_env_vars_checked":
                    list(self.cfg.get("allowed_env_vars", ["ANTHROPIC_API_KEY"])),
                "credential_present": key_present,
                "model": model,
                "model_source": ("env:" + self.cfg.get("model_env_var",
                                                       "ALPHA_AGENT_LLM_MODEL"))
                if self.env.get(self.cfg.get("model_env_var",
                                             "ALPHA_AGENT_LLM_MODEL"))
                else "config:default_model",
                "api_url": self.cfg.get("api_url", DEFAULT_API_URL),
                "usage_metadata": "exact (provider-reported)",
                "production_candidate": True}

    # ------------------------------------------------------------------ #
    def _complete(self, prompt_obj: dict, *, max_output_tokens: int,
                  output_schema: Optional[dict] = None) -> dict:
        # output_schema is intentionally ignored: the production Anthropic
        # request construction and limits must remain unchanged. Structured
        # output bounding is a development-only (claude_code) concern.
        key = self._resolve_key()
        if not key:
            return self._result(ok=False, status=PC_BLOCKED_CREDENTIAL,
                                error="no credential present in allowed env vars")
        model = self.resolve_model()
        if not model:
            return self._result(ok=False, status=PC_BLOCKED_MODEL,
                                error="no model configured (env or config)")
        if key not in self.secret_values:
            self.secret_values.append(key)
        body = json.dumps({
            "model": model,
            "max_tokens": int(max_output_tokens),
            "system": prompt_obj["system"],
            "messages": [{"role": "user", "content": prompt_obj["user"]}],
        }, sort_keys=True)
        request = {
            "url": self.cfg.get("api_url", DEFAULT_API_URL),
            "body": body,
            "headers": {"x-api-key": key,
                        "anthropic-version": self.cfg.get(
                            "anthropic_version", DEFAULT_ANTHROPIC_VERSION),
                        "content-type": "application/json"},
        }
        attempts = self.max_retries + 1
        retries = 0
        last_error = "no attempt made"
        for attempt in range(attempts):
            if attempt > 0:
                retries += 1
                self.retry_count += 1
                self.sleep_fn(self.backoff_seconds * (2 ** (attempt - 1)))
            res = self.transport(request, self.timeout_seconds)
            if res.get("error"):
                last_error = "transport error: %s" % res["error"]
                continue
            status = res.get("status")
            if status in _RETRYABLE_STATUS:
                last_error = "retryable HTTP %s" % status
                continue
            if status != 200:
                self._register_failure()
                return self._result(
                    ok=False, status=PC_BLOCKED_ERROR, retries=retries,
                    error="HTTP %s: %s" % (status, str(res.get("body"))[:300]))
            try:
                payload = json.loads(res["body"])
            except ValueError:
                self._register_failure()
                return self._result(ok=False, status=PC_BLOCKED_ERROR,
                                    retries=retries,
                                    error="non-JSON response body from API")
            blocks = payload.get("content") or []
            if any(isinstance(b, dict) and b.get("type") == "tool_use"
                   for b in blocks):
                self._register_failure()
                return self._result(ok=False, status="REJECTED_TOOL_USE",
                                    model=model, retries=retries,
                                    error="provider returned a tool_use block; "
                                          "tools are forbidden in Stage 3")
            text = "".join(b.get("text", "") for b in blocks
                           if isinstance(b, dict) and b.get("type") == "text")
            usage = payload.get("usage") if isinstance(payload.get("usage"),
                                                       dict) else None
            headers = {str(k).lower(): v for k, v in
                       (res.get("headers") or {}).items()}
            request_id = payload.get("id") or headers.get("request-id") \
                or headers.get("x-request-id")
            self._register_success()
            return self._result(ok=True, status="OK", text=text, usage=usage,
                                usage_reliable=usage is not None, model=model,
                                request_id=request_id, retries=retries)
        self._register_failure()
        return self._result(ok=False, status=PC_BLOCKED_ERROR, retries=retries,
                            error="exhausted retries: %s" % last_error)


__all__ = ["AnthropicHttpProvider", "default_transport",
           "DEFAULT_API_URL", "DEFAULT_ANTHROPIC_VERSION"]
