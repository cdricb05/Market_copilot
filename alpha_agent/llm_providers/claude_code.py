"""
alpha_agent/llm_providers/claude_code.py — DEVELOPMENT-ONLY fallback provider.

Invokes the already installed Claude Code CLI (`claude -p --output-format
json`) for ONE bounded live validation cycle when no API credential exists.

* FIXED argument vector — nothing from the prompt, a record, or any LLM output
  ever reaches argv. The prompt travels on stdin from the static prompt file
  content the director already persisted.
* Tool use and file mutation are forbidden three ways: the --disallowedTools
  deny-list, the prompt's hard no-tools rule, and structural rejection of any
  tool indicator in the reply.
* Only a strict JSON envelope is accepted: the CLI wraps the model reply as
  {"type":"result", "result": "<text>", ...}; the inner text must itself be
  the required JSON object (the director parses it strictly).
* Provider is recorded as CLAUDE_CODE_DEVELOPMENT_ONLY. Usage numbers are
  captured verbatim when the CLI reports them but the COST is always
  UNAVAILABLE (subscription-side accounting is not billing-grade) and this
  provider alone never yields full production readiness.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from typing import Any, Callable, Optional

from ..llm_contracts import (PC_BLOCKED_ERROR, PC_BLOCKED_UNAVAILABLE,
                             PC_DEVELOPMENT_READY)
from .base import BaseLLMProvider

CLAUDE_CODE_DEVELOPMENT_ONLY = "CLAUDE_CODE_DEVELOPMENT_ONLY"

# Fixed executable + fixed argument structure. Never derived from config
# free-text, request content, or any LLM output.
_FIXED_ARGS = ("-p", "--output-format", "json")
_DEFAULT_DISALLOWED_TOOLS = (
    "Bash", "Edit", "Write", "NotebookEdit", "WebFetch", "WebSearch", "Task",
    "Agent", "Glob", "Grep", "Read", "TodoWrite", "Skill", "Workflow",
    "SlashCommand", "KillShell", "BashOutput", "EnterPlanMode", "ExitPlanMode")
_VERSION_TIMEOUT_SECONDS = 30


def default_runner(argv: list[str], input_text: Optional[str],
                   timeout: float) -> dict:
    """Real subprocess runner (shell=False always). Injectable for pytest."""
    try:
        proc = subprocess.run(argv, input=input_text, capture_output=True,
                              text=True, timeout=timeout, shell=False,
                              encoding="utf-8", errors="replace")
        return {"returncode": proc.returncode, "stdout": proc.stdout or "",
                "stderr": proc.stderr or "", "error": None}
    except subprocess.TimeoutExpired:
        return {"returncode": None, "stdout": "", "stderr": "",
                "error": "TIMEOUT after %ss" % timeout}
    except (OSError, subprocess.SubprocessError) as exc:
        return {"returncode": None, "stdout": "", "stderr": "",
                "error": "%s" % type(exc).__name__}


class ClaudeCodeProvider(BaseLLMProvider):
    name = "claude_code"
    classification_ready = PC_DEVELOPMENT_READY

    def __init__(self, cfg: dict, *,
                 runner: Optional[Callable[[list[str], Optional[str], float], dict]] = None,
                 which_fn: Callable[[str], Optional[str]] = shutil.which,
                 **kwargs: Any):
        super().__init__(cfg, **kwargs)
        self.runner = runner or default_runner
        self.which_fn = which_fn
        self.executable = str(cfg.get("executable", "claude"))
        self.disallowed_tools = list(cfg.get("disallowed_tools",
                                             _DEFAULT_DISALLOWED_TOOLS))
        # Development proof profile — locally-confirmed CLI flags only. Every
        # value is non-secret config; none is derived from a record or LLM
        # output. Absent when the profile is not configured (flags omitted).
        dev = cfg.get("development_profile") or {}
        self.dev_enabled = bool(dev.get("enabled"))
        self.effort = str(dev.get("effort")) if dev.get("effort") else None
        self.model_override = str(dev.get("model")) if dev.get("model") \
            else None
        self.use_json_schema = bool(dev.get("use_json_schema"))

    # ------------------------------------------------------------------ #
    def _argv(self, output_schema: Optional[dict] = None) -> list[str]:
        argv = [self.executable, *_FIXED_ARGS,
                "--disallowedTools", " ".join(self.disallowed_tools)]
        # --model / --effort / --json-schema are all confirmed against the
        # locally installed CLI's --help before use; each is added only when
        # explicitly configured (and, for the schema, supplied per call).
        if self.model_override:
            argv += ["--model", self.model_override]
        if self.effort:
            argv += ["--effort", self.effort]
        if self.use_json_schema and output_schema is not None:
            argv += ["--json-schema", json.dumps(output_schema, sort_keys=True)]
        return argv

    def audit(self) -> dict:
        path = self.which_fn(self.executable)
        if not path:
            return {"provider": self.name,
                    "classification": PC_BLOCKED_UNAVAILABLE,
                    "available": False, "development_only": True,
                    "reason": "claude executable not found on PATH"}
        version = None
        res = self.runner([self.executable, "--version"], None,
                          _VERSION_TIMEOUT_SECONDS)
        if res.get("error") is None and res.get("returncode") == 0:
            version = (res.get("stdout") or "").strip()[:80]
        return {"provider": self.name,
                "classification": PC_DEVELOPMENT_READY,
                "recorded_as": CLAUDE_CODE_DEVELOPMENT_ONLY,
                "available": True, "development_only": True,
                "version": version,
                "disallowed_tools": self.disallowed_tools,
                "usage_metadata": "CLI-reported (not billing-grade); "
                                  "cost recorded as UNAVAILABLE",
                "production_candidate": False}

    # ------------------------------------------------------------------ #
    def _complete(self, prompt_obj: dict, *, max_output_tokens: int,
                  output_schema: Optional[dict] = None) -> dict:
        if not self.which_fn(self.executable):
            return self._result(ok=False, status=PC_BLOCKED_UNAVAILABLE,
                                error="claude executable not found on PATH")
        # The static prompt file content is what travels; the director already
        # persisted prompts/<hash>.json with these exact fields.
        stdin_text = ("%s\n\n%s\n\n[max_output_tokens=%d; return ONLY the "
                      "required JSON object]"
                      % (prompt_obj["system"], prompt_obj["user"],
                         int(max_output_tokens)))
        argv = self._argv(output_schema)
        json_schema_used = self.use_json_schema and output_schema is not None
        res = self.runner(argv, stdin_text, self.timeout_seconds)
        if res.get("error"):
            self._register_failure()
            return self._result(ok=False, status=PC_BLOCKED_ERROR,
                                error="CLI invocation failed: %s" % res["error"])
        if res.get("returncode") != 0:
            self._register_failure()
            return self._result(
                ok=False, status=PC_BLOCKED_ERROR,
                error="CLI exit %s: %s" % (res.get("returncode"),
                                           str(res.get("stderr"))[:300]))
        try:
            outer = json.loads(res.get("stdout") or "")
        except ValueError:
            self._register_failure()
            return self._result(ok=False, status="REJECTED_NON_JSON_ENVELOPE",
                                error="CLI stdout was not the JSON result "
                                      "envelope")
        if not isinstance(outer, dict) or not isinstance(outer.get("result"), str):
            self._register_failure()
            return self._result(ok=False, status="REJECTED_NON_JSON_ENVELOPE",
                                error="CLI envelope missing string 'result'")
        if outer.get("is_error"):
            self._register_failure()
            return self._result(ok=False, status=PC_BLOCKED_ERROR,
                                error="CLI reported is_error=true")
        text = outer["result"]
        raw_usage = outer.get("usage") if isinstance(outer.get("usage"), dict) \
            else None
        usage = None
        if raw_usage is not None:
            usage = {"input_tokens": int(raw_usage.get("input_tokens") or 0),
                     "output_tokens": int(raw_usage.get("output_tokens") or 0),
                     "cache_creation_input_tokens":
                         int(raw_usage.get("cache_creation_input_tokens") or 0),
                     "cache_read_input_tokens":
                         int(raw_usage.get("cache_read_input_tokens") or 0)}
        model_usage = outer.get("modelUsage")
        cli_models = sorted(model_usage.keys()) \
            if isinstance(model_usage, dict) and model_usage else []
        # When a model is explicitly pinned in non-secret config we record that
        # exact identifier (no silent model change); otherwise fall back to the
        # CLI's reported model. The CLI's own reported models are kept for audit.
        model = self.model_override or (cli_models[0] if cli_models else None)
        self._register_success()
        # usage_reliable stays False by contract: development-only accounting.
        result = self._result(ok=True, status="OK", text=text, usage=usage,
                              usage_reliable=False, model=model,
                              request_id=outer.get("session_id"))
        result["invocation"] = {
            "model_override": self.model_override,
            "effort": self.effort,
            "json_schema_used": json_schema_used,
            "cli_reported_models": cli_models,
            "disallowed_tools": self.disallowed_tools}
        return result


__all__ = ["ClaudeCodeProvider", "CLAUDE_CODE_DEVELOPMENT_ONLY",
           "default_runner"]
