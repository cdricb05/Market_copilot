"""
alpha_agent.runtime_contracts — Stage 4 persistent-runtime contracts.

Pure, side-effect-free definitions shared by the Stage 4 runtime, the report
renderer, the CLI and the tests: terminal tokens, email-delivery statuses, the
runtime state-database schema, deterministic identifiers, the friendly-report
cadence metadata, config loading + validation and the secret-scan guard.

Safety invariants enforced here (never relaxed downstream):
  * No credential value is ever read, stored or printed. Configs may name
    environment variables but must never contain a secret value; ``load_config``
    scans for and rejects any embedded secret.
  * Stage 4 is RESEARCH AUTOMATION ONLY. Nothing in this module can create an
    order, fill, signal, trade decision, model promotion or Paper Trader / DB
    mutation; there is no network, PostgreSQL, subprocess or prediction call.
  * All identifiers are deterministic functions of their inputs so completed
    cycles are idempotent and identical inputs reproduce identical run ids.
"""
from __future__ import annotations

import hashlib
import html
import json
import re
from pathlib import Path
from typing import Any, Iterable

RUNTIME_STAGE = "4"
RUNTIME_SCHEMA_VERSION = "1.0.0"
RUNTIME_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Terminal tokens (exactly one printed per CLI invocation).
# --------------------------------------------------------------------------- #
READY = "ALPHA_AGENT_STAGE4_READY"
DEGRADED = "ALPHA_AGENT_STAGE4_DEGRADED"
EMAIL_CREDENTIAL_REQUIRED = "ALPHA_AGENT_STAGE4_EMAIL_CREDENTIAL_REQUIRED"
NO_NEW_RESEARCH_INPUT = "NO_NEW_RESEARCH_INPUT"
VERIFIED = "ALPHA_AGENT_STAGE4_VERIFIED"
BLOCKED = "ALPHA_AGENT_STAGE4_BLOCKED"

TERMINAL_TOKENS = frozenset({
    READY, DEGRADED, EMAIL_CREDENTIAL_REQUIRED, NO_NEW_RESEARCH_INPUT,
    VERIFIED, BLOCKED})

# --------------------------------------------------------------------------- #
# Email-delivery statuses (returned by the deterministic sender contract).
# --------------------------------------------------------------------------- #
EMAIL_SENT = "EMAIL_SENT"
EMAIL_ALREADY_SENT = "EMAIL_ALREADY_SENT"
EMAIL_CREDENTIAL_REQUIRED_STATUS = "EMAIL_CREDENTIAL_REQUIRED"
EMAIL_RETRYABLE_FAILURE = "EMAIL_RETRYABLE_FAILURE"
EMAIL_PERMANENT_FAILURE = "EMAIL_PERMANENT_FAILURE"
EMAIL_SKIPPED = "EMAIL_SKIPPED"  # explicitly not requested this cycle

EMAIL_STATUSES = frozenset({
    EMAIL_SENT, EMAIL_ALREADY_SENT, EMAIL_CREDENTIAL_REQUIRED_STATUS,
    EMAIL_RETRYABLE_FAILURE, EMAIL_PERMANENT_FAILURE, EMAIL_SKIPPED})
EMAIL_SUCCESS_STATUSES = frozenset({EMAIL_SENT, EMAIL_ALREADY_SENT})

# --------------------------------------------------------------------------- #
# Runtime modes.
# --------------------------------------------------------------------------- #
MODE_COLLECT = "collect"
MODE_RESEARCH = "research"
MODE_REPORT_ONLY = "report-only"
MODE_WATCHDOG = "watchdog"
MODE_VERIFY = "verify"
MODES = (MODE_COLLECT, MODE_RESEARCH, MODE_REPORT_ONLY, MODE_WATCHDOG,
         MODE_VERIFY)

# Report cycle labels.
LABEL_MORNING = "morning"
LABEL_POST_CLOSE = "post_close"
LABEL_MANUAL = "manual"
REPORT_LABELS = (LABEL_MORNING, LABEL_POST_CLOSE, LABEL_MANUAL)

# LLM provider classification carried through into the report.
CLAUDE_CODE_DEVELOPMENT_ONLY = "CLAUDE_CODE_DEVELOPMENT_ONLY"
LLM_SKIPPED_PROVIDER_UNAVAILABLE = "LLM_SKIPPED_PROVIDER_UNAVAILABLE"
COST_UNAVAILABLE = "UNAVAILABLE"

# The exact four Windows Task Scheduler task names Stage 4 owns.
TASK_COLLECT = "AlphaAgent-Collect"
TASK_MORNING = "AlphaAgent-Morning-Report"
TASK_POST_CLOSE = "AlphaAgent-PostClose-Report"
TASK_WATCHDOG = "AlphaAgent-Watchdog"
ALPHA_AGENT_TASK_NAMES = (TASK_COLLECT, TASK_MORNING, TASK_POST_CLOSE,
                          TASK_WATCHDOG)

# Component identifiers persisted in component_runs.
COMPONENT_STAGE1 = "stage1_registry"
COMPONENT_STAGE2 = "stage2_ingestion"
COMPONENT_STAGE35 = "stage3_5_news_rss"
COMPONENT_STAGE3 = "stage3_research_director"
COMPONENT_PORTFOLIO = "portfolio_context"
COMPONENT_REPORT = "report_render"
COMPONENT_EMAIL = "email_delivery"

# --------------------------------------------------------------------------- #
# Runtime state-database schema (stdlib sqlite3; FKs on; WAL set at connect).
# Exactly the ten tables required by the Stage 4 contract.
# --------------------------------------------------------------------------- #
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runtime_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_runs (
    run_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    label TEXT,
    cycle_date TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal TEXT,
    run_dir TEXT,
    llm_invoked INTEGER NOT NULL DEFAULT 0,
    llm_calls INTEGER NOT NULL DEFAULT 0,
    tokens_in INTEGER NOT NULL DEFAULT 0,
    tokens_out INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_runtime_runs_cycle
    ON runtime_runs (cycle_id, status);
CREATE INDEX IF NOT EXISTS ix_runtime_runs_mode_date
    ON runtime_runs (mode, cycle_date);

CREATE TABLE IF NOT EXISTS component_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    component TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal TEXT,
    counts_json TEXT,
    stage_run_id TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    last_success_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runtime_runs (run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_component_runs_comp
    ON component_runs (component, status);

CREATE TABLE IF NOT EXISTS scheduler_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    cycle_id TEXT,
    mode TEXT NOT NULL,
    label TEXT,
    scheduled_for TEXT,
    status TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS report_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    label TEXT NOT NULL,
    cycle_date TEXT NOT NULL,
    html_path TEXT,
    text_path TEXT,
    manifest_path TEXT,
    html_sha256 TEXT,
    status TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runtime_runs (run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_report_runs_cycle
    ON report_runs (cycle_id);

CREATE TABLE IF NOT EXISTS email_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id TEXT NOT NULL,
    run_id TEXT,
    subject TEXT NOT NULL,
    recipient TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    outbox_path TEXT,
    sent_at TEXT,
    recorded_at TEXT NOT NULL
);
-- At most one *successful* delivery per report cycle (idempotent email).
CREATE UNIQUE INDEX IF NOT EXISTS ux_email_sent_once
    ON email_deliveries (cycle_id) WHERE status = 'EMAIL_SENT';

CREATE TABLE IF NOT EXISTS heartbeat_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    run_id TEXT,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS runtime_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    component TEXT,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_locks (
    name TEXT PRIMARY KEY,
    run_id TEXT,
    acquired_at TEXT NOT NULL,
    pid INTEGER,
    host TEXT,
    released_at TEXT,
    stale_cleared INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS missed_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL,
    cycle_date TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    recovered INTEGER NOT NULL DEFAULT 0,
    recovery_run_id TEXT
);

CREATE TABLE IF NOT EXISTS recovery_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    target TEXT,
    detail TEXT,
    run_id TEXT,
    recorded_at TEXT NOT NULL
);
"""

# --------------------------------------------------------------------------- #
# Secret-scan guard.
# --------------------------------------------------------------------------- #
# Config keys whose VALUE would itself be a raw secret (so may only hold an
# env-var NAME). Deliberately NOT "credential"/"credential_dir"/… — those name
# a DPAPI store location or policy, not a secret; the value-pattern scan below
# still catches any credential-shaped value under any key.
_SECRET_KEY_TOKENS = ("api_key", "apikey", "api_token", "password",
                      "app_password", "access_key", "secret_key",
                      "auth_token", "bearer")
# Recognisable credential value shapes that must never appear in config/logs.
_SECRET_VALUE_PATTERNS = (
    re.compile(r"sk-ant-[A-Za-z0-9\-_]{12,}"),      # Anthropic keys
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),            # AWS access key id
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}"),  # Slack tokens
)
# Legitimate env-var NAME references are allowed even under a "secret" key.
_ENVVAR_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,64}$")


def scan_for_secrets(obj: Any, *, path: str = "$") -> list[str]:
    """Return a list of human-readable problems if *obj* embeds a secret.

    Env-var NAMES (upper snake case) are permitted; concrete values are not.
    """
    problems: list[str] = []

    def _visit(node: Any, where: str) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                key_l = str(k).lower()
                child = "%s.%s" % (where, k)
                if any(tok in key_l for tok in _SECRET_KEY_TOKENS):
                    _check_secret_slot(k, v, child)
                _visit(v, child)
        elif isinstance(node, (list, tuple)):
            for i, v in enumerate(node):
                _visit(v, "%s[%d]" % (where, i))
        elif isinstance(node, str):
            for pat in _SECRET_VALUE_PATTERNS:
                if pat.search(node):
                    problems.append("%s: embedded credential-shaped value" %
                                    where)
                    break

    def _check_secret_slot(key: str, value: Any, where: str) -> None:
        # Keys whose NAME implies a secret must reference an env-var name only,
        # unless the key itself is clearly a *_env_var / *_env / allowed list.
        key_l = str(key).lower()
        name_only_key = (key_l.endswith("_env_var") or key_l.endswith("_env")
                         or key_l.endswith("_env_vars")
                         or "env_var" in key_l or key_l in
                         ("allowed_env_vars", "sensitive_env_vars",
                          "redacted_query_params"))
        if isinstance(value, str) and value and not name_only_key:
            if not _ENVVAR_NAME.match(value):
                problems.append(
                    "%s: secret-named key must hold an env-var NAME only" %
                    where)

    _visit(obj, path)
    return problems


def redact(text: str) -> str:
    """Replace any credential-shaped substring with a fixed placeholder."""
    out = text
    for pat in _SECRET_VALUE_PATTERNS:
        out = pat.sub("REDACTED", out)
    return out


# --------------------------------------------------------------------------- #
# Deterministic identifiers.
# --------------------------------------------------------------------------- #
def canonical_json(obj: Any) -> str:
    """Stable JSON: sorted keys, compact separators, ascii-safe."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      default=str)


def _digest(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h


def report_cycle_id(label: str, cycle_date: str) -> str:
    """Idempotent per (label, local date): a report cycle runs once per day."""
    return "cyc_" + _digest("report", label, cycle_date)[:16]


def activity_cycle_id(mode: str, now_iso: str) -> str:
    """A non-report activity (collect/watchdog) cycle keyed by clock stamp.

    Deterministic for a fixed clock so tests reproduce identical ids; distinct
    per invocation in production because ``now_iso`` advances.
    """
    return "cyc_" + _digest(mode, now_iso)[:16]


def runtime_run_id(cycle_id: str, mode: str, fingerprint: str = "") -> str:
    return "stage4_" + _digest(cycle_id, mode, fingerprint)[:16]


# --------------------------------------------------------------------------- #
# Friendly-report subjects.
# --------------------------------------------------------------------------- #
def report_subject(label: str, cycle_date: str, *, degraded: bool = False,
                   test: bool = False) -> str:
    if test:
        return "TEST — Alpha Agent Automated Research Report"
    if degraded:
        return "Alpha Agent Research Report — ACTION REQUIRED — %s" % cycle_date
    if label == LABEL_MORNING:
        return "Alpha Agent Morning Research Report — %s" % cycle_date
    if label == LABEL_POST_CLOSE:
        return "Alpha Agent Post-Close Research Report — %s" % cycle_date
    return "Alpha Agent Research Report — %s" % cycle_date


def html_escape(value: Any) -> str:
    """HTML-escape any value (numbers/None included) for safe interpolation."""
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


# --------------------------------------------------------------------------- #
# Config loading + validation.
# --------------------------------------------------------------------------- #
_REQUIRED_CONFIG_KEYS = (
    "stage", "runtime_root", "recipient_email", "stage1_registry_root",
    "stage2_ingestion_root", "stage3_director_root", "stage3_5_news_rss_root",
    "operational_ledger_roots", "cadence", "provider_order",
    "allowed_task_names",
)


class ConfigError(ValueError):
    """Raised when the Stage 4 runtime config is missing or unsafe."""


def load_config(path: str | Path) -> dict:
    """Load, secret-scan and structurally validate the Stage 4 runtime config.

    Never returns a config containing a secret value; raises ``ConfigError`` on
    any embedded credential or missing required key.
    """
    p = Path(path)
    try:
        raw = p.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ConfigError("cannot read config %s: %s" % (p, exc)) from exc
    try:
        cfg = json.loads(raw)
    except ValueError as exc:
        raise ConfigError("config %s is not valid JSON: %s" % (p, exc)) from exc
    if not isinstance(cfg, dict):
        raise ConfigError("config root must be a JSON object")

    problems = scan_for_secrets(cfg)
    if problems:
        raise ConfigError("config contains a secret value: %s" %
                          "; ".join(problems))

    missing = [k for k in _REQUIRED_CONFIG_KEYS if k not in cfg]
    if missing:
        raise ConfigError("config missing required keys: %s" %
                          ", ".join(missing))
    if str(cfg.get("stage")) != RUNTIME_STAGE:
        raise ConfigError("config stage must be '%s'" % RUNTIME_STAGE)

    tasks = list(cfg.get("allowed_task_names") or [])
    if sorted(tasks) != sorted(ALPHA_AGENT_TASK_NAMES):
        raise ConfigError("allowed_task_names must be exactly %s" %
                          ", ".join(ALPHA_AGENT_TASK_NAMES))
    return cfg


def config_hash(cfg: dict) -> str:
    return _digest(canonical_json(cfg))[:16]


def cadence_value(cfg: dict, key: str, default: Any) -> Any:
    cad = cfg.get("cadence") or {}
    return cad.get(key, default)


def coerce_iterable(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def all_terminal_tokens() -> Iterable[str]:
    return sorted(TERMINAL_TOKENS)
