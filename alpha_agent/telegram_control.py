"""
alpha_agent.telegram_control — Stage 8 secure Telegram control plane.

A Telegram Bot API long-polling control interface for the Alpha Agent, built on
the same safety spine as the rest of Stage 8: it can ONLY answer read-only
evidence questions or enqueue a BOUNDED durable research job. It can never run a
shell command, execute Python, run SQL, delete a file, create an order / fill /
trade decision / signal, promote a model, or mutate holdings or cash. Those
capabilities do not exist anywhere on the router's code path.

Security controls (WS12/WS13):
  * The bot token is stored ONLY as a Windows DPAPI blob outside the repo; the
    PowerShell wrapper decrypts it in memory and passes it to this process over
    redirected STDIN only. It never appears in source, .env, a CLI argument, an
    environment variable, a log, test output or PROJECT_STATE.md.
  * Exactly one allowed numeric user id AND one allowed private chat id are
    accepted; every other user/chat is denied and audited.
  * Durable update offset + per-update deduplication make polling idempotent
    across restarts (a duplicate Telegram update never creates duplicate work).
  * Outbound replies are plain text, chunked to Telegram's size limit, and
    redaction-scrubbed so a secret can never be echoed.

No network happens in unit tests: the HTTP client and the clock are injected.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

STAGE = "8"
SCHEMA_VERSION = "1.0.0"

_API_BASE = "https://api.telegram.org"
_MAX_MESSAGE = 4000  # Telegram hard limit is 4096; leave headroom.
_LONG_POLL_TIMEOUT = 25

# Diagnostic classifications (read-only getMe probe).
TELEGRAM_AUTH_OK = "TELEGRAM_AUTH_OK"
TELEGRAM_TOKEN_MISSING = "TELEGRAM_TOKEN_MISSING"
TELEGRAM_AUTH_REJECTED = "TELEGRAM_AUTH_REJECTED"
TELEGRAM_UNREACHABLE = "TELEGRAM_UNREACHABLE"

# Intent kinds — the ONLY two side-effect classes plus help.
KIND_READ_ONLY = "READ_ONLY_QUERY"
KIND_RESEARCH_JOB = "RESEARCH_JOB"
KIND_HELP = "HELP"

# --------------------------------------------------------------------------- #
# The ONE command registry (Stage 8.1A). Both the /commands menu and the router
# derive from this, so a command is advertised IFF it has a real handler. Each
# entry is ``(command_display, one_line_help)``; ``<arg>`` in the display marks a
# parameterised command. Grouped for the operator menu.
# --------------------------------------------------------------------------- #
COMMAND_MENU = (
    ("GENERAL", (
        ("/status", "agent + research-queue + sources summary"),
        ("/health", "queue depth + watchdog health"),
        ("/commands", "this command menu"),
        ("/help", "alias of /commands"),
    )),
    ("PORTFOLIO", (
        ("/today", "today's paper-book performance (alias /pnl)"),
        ("/pnl", "paper-book P&L vs SPY"),
        ("/performance", "full paper-book performance"),
        ("/nav", "current NAV + market date"),
        ("/book", "active paper book summary"),
        ("/positions", "current positions + weights"),
        ("/attribution", "today's P&L by holding"),
    )),
    ("RESEARCH", (
        ("/queue", "research-queue state + next jobs"),
        ("/jobs", "recent research jobs + ids"),
        ("/job <id>", "one job's detail"),
        ("/experiments", "experiments + evidence decisions"),
        ("/candidates", "alpha candidate dispositions"),
        ("/sources", "source entitlement classifications"),
        ("/coverage", "data coverage + gaps"),
        ("/blocked", "blocked jobs and why"),
        ("/report", "latest research activity"),
        ("/data", "data-acquisition job status"),
    )),
    ("TOURNAMENT", (
        ("/tournament", "alpha tournament status + lifecycle counts"),
        ("/leaderboard", "top ranked candidates + decomposed scores"),
        ("/candidate <id>", "one candidate's full evidence card"),
        ("/why <id>", "plain-English retained, rejected or blocked reason"),
        ("/compare <id1> <id2>", "side-by-side candidate comparison"),
        ("/shadowbooks", "active research shadow books"),
        ("/shadowbook <id>", "one shadow book's forward evidence"),
        ("/families", "candidate + variant counts by factor family"),
    )),
    ("CAMPAIGNS", (
        ("/campaigns", "acquisition campaign progress + backlog"),
        ("/campaign <id>", "one campaign: cursor, batches, caps, stop reason"),
        ("/dataholds", "candidates on DATA_HOLD + the blocking evidence"),
        ("/datahold <id>", "one DATA_HOLD: coverage vs requirement + path"),
    )),
    ("ACTION", (
        ("/run <request>", "queue a BOUNDED read-only research request"),
    )),
)

# Command -> read-only evidence provider key (resolved by the injected
# ``providers`` map). ``/run`` is a research job; ``/help`` and ``/commands``
# render the menu; ``/job`` is read-only but takes an argument.
_COMMAND_PROVIDER = {
    "/status": "status", "/health": "health",
    "/today": "performance", "/pnl": "performance", "/performance": "performance",
    "/nav": "nav", "/book": "book", "/positions": "positions",
    "/attribution": "attribution",
    "/queue": "queue", "/jobs": "jobs", "/job": "job",
    "/experiments": "experiments", "/candidates": "candidates",
    "/sources": "sources", "/coverage": "coverage", "/blocked": "blocked",
    "/report": "report", "/data": "data",
    # Stage 9 tournament (all read-only).
    "/tournament": "tournament", "/leaderboard": "leaderboard",
    "/candidate": "candidate", "/why": "why", "/compare": "compare",
    "/shadowbooks": "shadowbooks", "/shadowbook": "shadowbook",
    "/families": "families",
    # Stage 9.3 controlled campaigns + data-hold readiness (all read-only).
    "/campaigns": "campaigns", "/campaign": "campaign",
    "/dataholds": "dataholds", "/datahold": "datahold",
}

# Commands whose handler takes the free-text remainder as an argument.
_ARG_COMMANDS = ("/job", "/candidate", "/why", "/compare", "/shadowbook",
                 "/campaign", "/datahold")

# The full set of implemented, advertised commands (single source of truth for
# the "/commands lists only implemented commands" contract).
COMMANDS = tuple(entry[0].split()[0] for _grp, cmds in COMMAND_MENU
                 for entry in cmds)

# Deterministic natural-language intent routing (keyword -> command). Checked in
# order; the FIRST research verb wins so "run/test/compare an experiment" always
# routes to a bounded research job rather than a read-only query.
_NL_RESEARCH_VERBS = ("test ", "run ", "compare ", "backtest ", "evaluate ")

# Injection / command-shaped substrings that must never be classified as a
# command or research request (routed to HELP instead). Defense-in-depth only.
_DANGEROUS_TOKENS = (
    ";", "&&", "||", "|", "`", "$(", "drop table", "delete from", "truncate",
    "insert into", "update ", "rm -rf", "os.system", "subprocess", "exec(",
    "eval(", "import os", "__import__", "sudo ", "powershell", "cmd.exe",
    "curl ", "wget ", "\n")
_NL_READ_ROUTES = (
    # Attribution FIRST — "what helped/hurt the portfolio today" mentions
    # "portfolio", so it must win over the /book route further down.
    (("helped", "hurt", "contribut", "detract", "what moved", "what drove",
      "biggest winner", "biggest loser", "attribution"), "/attribution"),
    # Performance/today — "how did the portfolio do today" also says
    # "portfolio"; route it to P&L before the /book route.
    (("how did", "do today", "how are we doing", "how's the book",
      "hows the book", "performance", "pnl", "p/l", "p&l", "how did we do",
      "made money", "lost money", "portfolio do"), "/performance"),
    (("nav", "net asset value", "book value"), "/nav"),
    (("position", "what do we hold", "what do we own",
      "show my holdings list"), "/positions"),
    (("tournament", "leaderboard", "standings"), "/leaderboard"),
    (("shadow book", "shadowbook", "shadow-book"), "/shadowbooks"),
    (("factor family", "factor families", "by family", "families"), "/families"),
    (("best alpha", "alpha candidate", "candidate", "best signal",
      "best factor", "most promising"), "/candidates"),
    (("experiment", "ran today", "what experiments"), "/experiments"),
    (("research agent", "working on", "agent working", "what's the agent",
      "whats the agent", "queue", "work list", "what's queued",
      "whats queued"), "/queue"),
    (("blocked", "why was", "rejected", "stuck"), "/blocked"),
    # "what data sources are available?" is a SOURCE-REGISTRY question — it must
    # win over the coverage-gap route, so /sources is checked first and matches
    # "source(s)" / "data available" directly.
    (("source", "which eodhd", "endpoints available", "norgate history",
      "data available", "available data", "what data have we",
      "what data do we", "what data are"), "/sources"),
    (("coverage", "not acquired", "missing data", "coverage gap"), "/coverage"),
    (("book", "holdings", "portfolio"), "/book"),
    (("report", "latest report", "send me"), "/report"),
    (("status", "what's up", "whats up", "summary"), "/status"),
    (("health", "system", "is everything ok"), "/health"),
)

# Tokens that, if present in ANY reply, indicate an accidental secret leak. The
# redaction guard scrubs known secret VALUES supplied by the caller; this set is
# a belt-and-braces check for credential-shaped strings.
_SECRET_SHAPES = (
    re.compile(r"\bsk-ant-[A-Za-z0-9\-_]{8,}"),
    re.compile(r"\b\d{6,}:[A-Za-z0-9_\-]{30,}\b"),  # telegram bot-token shape
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# Telegram user/chat ids routinely exceed the signed 32-bit range (e.g.
# 8284912423) and can be negative for groups/channels. They are therefore stored
# and compared everywhere as NORMALIZED DECIMAL STRINGS to avoid Int32 overflow,
# cross-language numeric coercion, JavaScript 2**53 precision loss and SQLite
# type inconsistency. Never cast a Telegram id to a fixed-width integer.
_TELEGRAM_ID_RE = re.compile(r"^-?\d+$")


def normalize_telegram_id(value: Any) -> Optional[str]:
    """Return the canonical decimal-string form of a Telegram user/chat id, or
    ``None`` if ``value`` is not a valid decimal identifier.

    Accepts an int (as delivered by the Telegram API) or a string (as stored in
    the non-secret allowlist). Trims whitespace, tolerates an optional leading
    ``+`` and, for chat ids generally, a leading ``-``. Canonicalizes via Python
    ``int`` (arbitrary precision — no overflow) so leading zeros and ``-0`` are
    normalized. Letters, decimals, blanks and empty input return ``None``."""
    if value is None:
        return None
    s = str(value).strip()
    if s.startswith("+"):
        s = s[1:]
    if not _TELEGRAM_ID_RE.match(s):
        return None
    return str(int(s))


def _as_id_list(value: Any) -> list:
    """Coerce an allowlist field to a list of raw ids. Accepts a JSON array, or
    a lone scalar (str/int). The scalar case guards against PowerShell
    ``ConvertTo-Json`` unwrapping a single-element array to a bare value: a
    string id is treated as ONE id, never iterated character-by-character
    (which would otherwise silently allow single-digit user ids)."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


# --------------------------------------------------------------------------- #
# Config (non-secret).
# --------------------------------------------------------------------------- #
class TelegramConfig:
    """Non-secret Telegram control config. The token is NEVER stored here — only
    the DPAPI blob location and the numeric allowlists."""

    def __init__(self, raw: dict):
        tg = (raw or {}).get("telegram") or raw or {}
        self.enabled = bool(tg.get("enabled", False))
        self.credential_dir = tg.get("credential_dir")
        self.bot_token_credential_file = tg.get(
            "bot_token_credential_file", "telegram_bot_token.dpapi")
        self.allowed_ids_file = tg.get("allowed_ids_file",
                                       "telegram_allowed_ids.json")
        # Numeric allowlists: inline in the config (tests) OR loaded from a
        # non-secret external file under the credential dir (production), so the
        # allowed ids never have to live in the repository config.
        users = _as_id_list(tg.get("allowed_user_ids"))
        chats = _as_id_list(tg.get("allowed_chat_ids"))
        if (not users or not chats) and self.credential_dir:
            ext = Path(self.credential_dir) / self.allowed_ids_file
            if ext.is_file():
                try:
                    d = json.loads(ext.read_text(encoding="utf-8-sig"))
                    users = users or _as_id_list(d.get("allowed_user_ids"))
                    chats = chats or _as_id_list(d.get("allowed_chat_ids"))
                except Exception:  # noqa: BLE001 - a bad allowlist denies all
                    pass
        # Store normalized decimal STRINGS (never fixed-width ints): Telegram
        # ids can exceed 32-bit range and JSON round-trips lose precision as
        # numbers. Invalid entries are dropped (a bad allowlist denies all).
        self.allowed_user_ids = [n for n in (normalize_telegram_id(x)
                                             for x in users) if n is not None]
        self.allowed_chat_ids = [n for n in (normalize_telegram_id(x)
                                             for x in chats) if n is not None]
        self.api_base = tg.get("api_base", _API_BASE)
        self.poll_timeout_seconds = int(tg.get("poll_timeout_seconds",
                                               _LONG_POLL_TIMEOUT))
        self.state_db = tg.get("state_db")

    def as_safe_dict(self) -> dict:
        return {"enabled": self.enabled,
                "allowed_user_ids": self.allowed_user_ids,
                "allowed_chat_ids": self.allowed_chat_ids,
                "api_base": self.api_base,
                "poll_timeout_seconds": self.poll_timeout_seconds,
                "credential_configured": bool(self.credential_dir)}


# --------------------------------------------------------------------------- #
# Authorization (WS12).
# --------------------------------------------------------------------------- #
def extract_message(update: dict) -> Optional[dict]:
    """Return the message body of an update (message or edited_message)."""
    return update.get("message") or update.get("edited_message")


def authorize(update: dict, cfg: TelegramConfig) -> "tuple[bool, str]":
    """Only the exact allowed user id in the exact allowed private chat id is
    accepted. Everything else is denied with a specific reason."""
    msg = extract_message(update)
    if not msg:
        return False, "no message body"
    frm = (msg.get("from") or {})
    chat = (msg.get("chat") or {})
    # Telegram delivers ids as integers; normalize both sides to decimal strings
    # so comparison never depends on int-vs-str type and never overflows.
    uid = normalize_telegram_id(frm.get("id"))
    cid = normalize_telegram_id(chat.get("id"))
    if cfg.allowed_user_ids and uid not in cfg.allowed_user_ids:
        return False, "user %s not allowed" % uid
    if cfg.allowed_chat_ids and cid not in cfg.allowed_chat_ids:
        return False, "chat %s not allowed" % cid
    if not cfg.allowed_user_ids or not cfg.allowed_chat_ids:
        return False, "no allowlist configured"
    if chat.get("type") not in (None, "private"):
        return False, "only a private chat is allowed"
    return True, "ok"


# --------------------------------------------------------------------------- #
# Durable offset + dedupe store.
# --------------------------------------------------------------------------- #
_STORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS telegram_meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS telegram_seen (
    update_id INTEGER PRIMARY KEY, processed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS telegram_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL, update_id INTEGER, user_id TEXT,
    chat_id TEXT, kind TEXT, command TEXT, allowed INTEGER, detail TEXT);
CREATE TABLE IF NOT EXISTS telegram_delivered (
    job_id TEXT PRIMARY KEY, delivered_at TEXT NOT NULL, chat_id TEXT);
"""


class TelegramStore:
    """Durable Telegram offset + update de-duplication + audit log (stdlib
    sqlite3; survives restart). Never stores a secret."""

    def __init__(self, db_path: str | Path,
                 clock: Optional[Callable[[], str]] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        conn = self._c()
        try:
            conn.executescript(_STORE_SCHEMA)
        finally:
            conn.close()

    def _c(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def get_offset(self) -> int:
        conn = self._c()
        try:
            row = conn.execute(
                "SELECT value FROM telegram_meta WHERE key='offset'").fetchone()
            return int(row["value"]) if row else 0
        finally:
            conn.close()

    def set_offset(self, offset: int) -> None:
        conn = self._c()
        try:
            conn.execute(
                "INSERT INTO telegram_meta(key,value) VALUES('offset',?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(int(offset)),))
        finally:
            conn.close()

    def seen(self, update_id: int) -> bool:
        conn = self._c()
        try:
            return conn.execute("SELECT 1 FROM telegram_seen WHERE update_id=?",
                                (int(update_id),)).fetchone() is not None
        finally:
            conn.close()

    def mark_seen(self, update_id: int) -> bool:
        """Record an update id. Returns False if it was already recorded
        (duplicate), True if newly recorded — the idempotency gate."""
        conn = self._c()
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO telegram_seen(update_id,processed_at)"
                " VALUES(?,?)", (int(update_id), self._clock()))
            return cur.rowcount > 0
        finally:
            conn.close()

    def audit(self, *, update_id, user_id, chat_id, kind, command, allowed,
              detail="") -> None:
        conn = self._c()
        try:
            conn.execute(
                "INSERT INTO telegram_audit(recorded_at,update_id,user_id,"
                "chat_id,kind,command,allowed,detail) VALUES(?,?,?,?,?,?,?,?)",
                (self._clock(),
                 None if update_id is None else int(update_id),
                 normalize_telegram_id(user_id), normalize_telegram_id(chat_id),
                 kind, command, 1 if allowed else 0, str(detail)[:500]))
        finally:
            conn.close()

    def last_request(self) -> Optional[dict]:
        conn = self._c()
        try:
            row = conn.execute(
                "SELECT recorded_at,user_id,command,kind,allowed FROM"
                " telegram_audit ORDER BY id DESC LIMIT 1").fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def is_delivered(self, job_id: str) -> bool:
        conn = self._c()
        try:
            return conn.execute(
                "SELECT 1 FROM telegram_delivered WHERE job_id=?",
                (str(job_id),)).fetchone() is not None
        finally:
            conn.close()

    def mark_delivered(self, job_id: str, chat_id) -> bool:
        """Record that a research RESULT was delivered back to Telegram for this
        job. Returns False if it was already recorded (so a result is sent to the
        chat AT MOST ONCE, even across restarts/re-polls) — the delivery
        idempotency gate mirroring ``mark_seen`` for inbound updates."""
        conn = self._c()
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO telegram_delivered(job_id,delivered_at,"
                "chat_id) VALUES(?,?,?)",
                (str(job_id), self._clock(), normalize_telegram_id(chat_id)))
            return cur.rowcount > 0
        finally:
            conn.close()


# --------------------------------------------------------------------------- #
# Output hygiene.
# --------------------------------------------------------------------------- #
def redact(text: str, secrets: Optional["list[str]"] = None) -> str:
    """Scrub known secret values and any credential-shaped substring from an
    outbound reply. A reply is NEVER allowed to carry a token/key/password."""
    out = text or ""
    for s in (secrets or []):
        if s:
            out = out.replace(s, "***")
    for pat in _SECRET_SHAPES:
        out = pat.sub("***", out)
    return out


def chunk_message(text: str, limit: int = _MAX_MESSAGE) -> "list[str]":
    if not text:
        return [""]
    lines = text.split("\n")
    chunks, cur = [], ""
    for line in lines:
        if len(cur) + len(line) + 1 > limit:
            if cur:
                chunks.append(cur)
            # A single over-long line is hard-split.
            while len(line) > limit:
                chunks.append(line[:limit])
                line = line[limit:]
            cur = line
        else:
            cur = line if not cur else cur + "\n" + line
    if cur:
        chunks.append(cur)
    return chunks or [""]


# --------------------------------------------------------------------------- #
# Intent routing (deterministic).
# --------------------------------------------------------------------------- #
def _intent(command, kind, *, research_text=None, arg=None, help_reason=None):
    return {"command": command, "kind": kind, "research_text": research_text,
            "arg": arg, "help_reason": help_reason}


def resolve_intent(text: str) -> dict:
    """Deterministically map a message to an intent. Returns
    ``{command, kind, research_text, arg, help_reason}``. Unknown text routes to
    HELP (never to an arbitrary action)."""
    raw = (text or "").strip()
    low = raw.lower()
    if not raw:
        return _intent("/help", KIND_HELP, help_reason="menu")

    # Defense-in-depth: injection/command-shaped input is routed to HELP and is
    # never interpreted. (The router already has NO code path to a shell, SQL,
    # file or trade action; this simply avoids even classifying such text.)
    if any(tok in low for tok in _DANGEROUS_TOKENS):
        return _intent("/help", KIND_HELP, help_reason="rejected")

    # Explicit slash command.
    if raw.startswith("/"):
        cmd = raw.split()[0].split("@")[0].lower()
        arg = raw[len(cmd):].strip()
        if cmd in ("/help", "/commands"):
            return _intent(cmd, KIND_HELP, help_reason="menu")
        if cmd == "/run":
            if not arg:
                return _intent("/run", KIND_HELP, help_reason="run_usage")
            return _intent("/run", KIND_RESEARCH_JOB, research_text=arg)
        if cmd in _ARG_COMMANDS:
            # Read-only; the free-text remainder is the argument (may be empty —
            # the provider then returns a usage hint, never an error).
            return _intent(cmd, KIND_READ_ONLY, arg=(arg or None))
        if cmd in _COMMAND_PROVIDER:
            return _intent(cmd, KIND_READ_ONLY)
        return _intent("/help", KIND_HELP, help_reason="unknown_command")

    # Natural language: a research verb + an experiment-ish object => job.
    if any(v in (" " + low) for v in _NL_RESEARCH_VERBS) and any(
            k in low for k in ("experiment", "momentum", "value", "quality",
                               "earnings", "reversal", "residual", "factor",
                               "surprise", "insider", "volatility", "accrual",
                               "champion", "signal", "combination")):
        return _intent("/run", KIND_RESEARCH_JOB, research_text=raw)

    # Natural language read-only routes.
    for keys, cmd in _NL_READ_ROUTES:
        if any(k in low for k in keys):
            return _intent(cmd, KIND_READ_ONLY)

    # Genuinely unrecognised free text: NOT the generic "unavailable" message —
    # an honest "conversational LLM mode is not enabled; use /commands".
    return _intent("/help", KIND_HELP, research_text=raw, help_reason="unknown_nl")


def _menu_text() -> str:
    """The /commands + /help menu, derived from the ONE command registry so it
    can only ever advertise commands that have a real handler."""
    lines = ["Alpha Agent — Paper Trader command center",
             "Read-only data + bounded research requests only. No trading, no "
             "orders, no automation."]
    for group, cmds in COMMAND_MENU:
        lines.append("")
        lines.append(group)
        for disp, hlp in cmds:
            lines.append("  %-16s %s" % (disp, hlp))
    lines.append("")
    lines.append("Plain English works too, e.g. 'what is my pnl?', 'show my "
                 "positions', 'what helped the portfolio today?', 'what is the "
                 "research agent working on?'.")
    return "\n".join(lines)


# Built once from the registry; both /commands and /help render it.
HELP_TEXT = _menu_text()

# Genuinely unrecognised free text (no LLM is wired yet) — actionable, specific.
UNKNOWN_NL_TEXT = (
    "I couldn't map that to a command. Conversational (LLM) mode is not enabled "
    "on this control plane yet — I only answer the fixed read-only commands and "
    "bounded /run research requests.\n"
    "Send /commands to see everything I can do.")

# /run with no request text.
RUN_USAGE_TEXT = (
    "Usage: /run <bounded research request>. Example: /run test residual "
    "momentum excluding financials. The request runs as a bounded, read-only "
    "experiment on the owned survivorship-free price panel; nothing is traded.")


# --------------------------------------------------------------------------- #
# Router.
# --------------------------------------------------------------------------- #
class ControlRouter:
    """Turns an authorized message into a reply. The ONLY effects available are:
    (a) call a read-only evidence provider, or (b) enqueue a bounded research
    job. There is no code path to shell/python/SQL/file/trade/promotion."""

    def __init__(self, *, providers: dict, queue=None,
                 secrets: Optional["list[str]"] = None):
        # providers: {key -> callable() -> str}, all READ-ONLY.
        self.providers = providers or {}
        self.queue = queue
        self.secrets = secrets or []

    def handle(self, update: dict, *, store: Optional[TelegramStore] = None
               ) -> "list[str]":
        msg = extract_message(update) or {}
        text = msg.get("text") or ""
        intent = resolve_intent(text)
        cmd, kind = intent["command"], intent["kind"]
        if kind == KIND_HELP:
            reply = self._help_reply(intent.get("help_reason"))
        elif kind == KIND_READ_ONLY:
            reply = self._read_only(cmd, intent.get("arg"))
        elif kind == KIND_RESEARCH_JOB:
            reply = self._enqueue_research(intent["research_text"], update)
        else:
            reply = HELP_TEXT
        reply = redact(reply, self.secrets)
        return chunk_message(reply)

    @staticmethod
    def _help_reply(reason: Optional[str]) -> str:
        if reason == "unknown_nl":
            return UNKNOWN_NL_TEXT
        if reason == "run_usage":
            return RUN_USAGE_TEXT
        return HELP_TEXT

    def _read_only(self, command: str, arg: Optional[str] = None) -> str:
        key = _COMMAND_PROVIDER.get(command)
        provider = self.providers.get(key)
        if provider is None:
            # Not the vague "unavailable" line: this command simply is not wired
            # to a provider in this build — point the operator at the real menu.
            return ("That command is not available in this build. Send "
                    "/commands to see the supported read-only commands.")
        try:
            out = provider(arg) if command in _ARG_COMMANDS else provider()
        except Exception as exc:  # noqa: BLE001 - a provider error is not fatal
            return "Could not read %s (%s)." % (key, type(exc).__name__)
        return out if isinstance(out, str) else json.dumps(out, default=str)

    def _enqueue_research(self, research_text: str, update: dict) -> str:
        """Enqueue a BOUNDED durable research job. Idempotent on the update id +
        request text so a duplicate Telegram delivery never double-queues."""
        if self.queue is None:
            return ("Received your research request but the queue is "
                    "unavailable right now.")
        from . import autonomous_research as ar
        uid = update.get("update_id")
        msg = extract_message(update) or {}
        cid = normalize_telegram_id((msg.get("chat") or {}).get("id"))
        # chat_id is carried so the bounded result can be delivered back to the
        # SAME private chat once the experiment completes; it never widens the
        # allowlist (delivery re-checks the chat against the allowlist).
        payload = {"request": research_text, "source": "telegram",
                   "update_id": uid, "chat_id": cid}
        dedupe = ar.make_dedupe_key(ar.CAT_TELEGRAM_REQUEST,
                                    "telegram.request", payload)
        job_id = self.queue.enqueue(
            ar.CAT_TELEGRAM_REQUEST, lane="telegram.request", payload=payload,
            priority=4, dedupe_key=dedupe, origin="telegram")
        return ("Queued your research request (job %s). It will run as a "
                "bounded, read-only experiment; nothing is traded." % job_id)


# --------------------------------------------------------------------------- #
# Poll loop (one iteration) — network client injected.
# --------------------------------------------------------------------------- #
def poll_once(*, client, token: str, cfg: TelegramConfig, store: TelegramStore,
              router: ControlRouter) -> dict:
    """Fetch one batch of updates, process authorized+new ones, advance the
    durable offset. ``client`` is an object exposing ``get_updates(token,
    offset, timeout)`` and ``send_message(token, chat_id, text)``; both are
    injected so tests never touch the network. Returns a summary."""
    offset = store.get_offset()
    resp = client.get_updates(token=token, offset=offset,
                              timeout=cfg.poll_timeout_seconds)
    updates = (resp or {}).get("result") or []
    summary = {"fetched": len(updates), "processed": 0, "denied": 0,
               "duplicates": 0, "replies": 0, "max_update_id": offset - 1,
               "offset": offset, "api_ok": (resp or {}).get("ok"),
               "api_error": (resp or {}).get("description")}
    for update in updates:
        uid = update.get("update_id")
        if uid is None:
            continue
        summary["max_update_id"] = max(summary["max_update_id"], int(uid))
        if store.seen(uid):
            summary["duplicates"] += 1
            continue
        msg = extract_message(update) or {}
        frm = (msg.get("from") or {})
        chat = (msg.get("chat") or {})
        allowed, reason = authorize(update, cfg)
        if not allowed:
            store.mark_seen(uid)
            store.audit(update_id=uid, user_id=frm.get("id"),
                        chat_id=chat.get("id"), kind="DENIED", command=None,
                        allowed=False, detail=reason)
            summary["denied"] += 1
            continue
        intent = resolve_intent(msg.get("text") or "")
        replies = router.handle(update, store=store)
        # Only mark seen AFTER a successful handle so a crash mid-handle retries.
        store.mark_seen(uid)
        store.audit(update_id=uid, user_id=frm.get("id"),
                    chat_id=chat.get("id"), kind=intent["kind"],
                    command=intent["command"], allowed=True)
        for chunk in replies:
            try:
                client.send_message(token=token, chat_id=chat.get("id"),
                                    text=chunk)
                summary["replies"] += 1
            except Exception:  # noqa: BLE001 - a send failure is not fatal
                pass
        summary["processed"] += 1
    if updates:
        store.set_offset(summary["max_update_id"] + 1)
    return summary


def format_research_result(job) -> str:
    """Turn a terminal TELEGRAM_REQUEST job into a plain-English result for the
    chat. Pure formatting of the already-computed research result — it runs no
    experiment and reads no secret."""
    state = getattr(job, "state", None)
    res = getattr(job, "result", None) or {}
    payload = getattr(job, "payload", None) or {}
    req = payload.get("request") or "your research request"
    if state == "COMPLETED":
        feat = res.get("mapped_feature") or "the requested factor"
        rows = res.get("results") or []
        head = ("Result for \"%s\" (job %s):\nRan a bounded, read-only "
                "experiment on the owned survivorship-free price panel"
                % (req, getattr(job, "job_id", "?")))
        panel = res.get("panel_symbols")
        if panel:
            head += " (%d symbols)" % panel
        head += ".\nFactor: %s." % feat
        lines = [head]
        for r in rows[:3]:
            lines.append(
                "- %s: rank-IC t=%s, decile-spread t=%s, net ann.=%s, "
                "beats-null=%s -> %s" % (
                    r.get("label") or r.get("feature"),
                    _fmt_num(r.get("rank_ic_t")), _fmt_num(r.get("spread_t")),
                    _fmt_num(r.get("net_annualized_return")),
                    r.get("beats_null_control"), r.get("decision")))
        note = res.get("sector_exclusion_note")
        if note:
            lines.append("Note: " + note)
        lines.append("This is research evidence only; nothing was traded.")
        return "\n".join(lines)
    # Non-completed terminal states (blocked/rejected) — honest reason.
    reason = res.get("reason") or getattr(job, "blocked_reason", None) or \
        "insufficient data / bounded gate not met"
    return ("Your research request \"%s\" (job %s) did not produce a promotable "
            "result: %s. Nothing was traded." % (
                req, getattr(job, "job_id", "?"), reason))


def _fmt_num(v) -> str:
    try:
        return "%.3f" % float(v)
    except (TypeError, ValueError):
        return str(v)


def deliver_results(*, client, token: str, queue, store: TelegramStore,
                    allowed_chat_ids: Optional["list"] = None,
                    secrets: Optional["list[str]"] = None,
                    limit: int = 50) -> dict:
    """Deliver terminal TELEGRAM_REQUEST results back to the originating private
    chat, EXACTLY ONCE (durable ``telegram_delivered`` gate). Safety: a result is
    only ever sent to a chat that is BOTH recorded on the job AND still on the
    allowlist — delivery can never broaden who receives messages. Read-only w.r.t.
    the research queue (it only reads job state/result); it never mutates a job,
    acquires data or runs an experiment."""
    from . import autonomous_research as ar
    summary = {"delivered": 0, "already": 0, "skipped_not_terminal": 0,
               "skipped_no_chat": 0, "skipped_not_allowed": 0, "send_errors": 0}
    allow = set(allowed_chat_ids or [])
    jobs = queue.list_jobs(category=ar.CAT_TELEGRAM_REQUEST, limit=limit)
    for job in jobs:
        if job.state not in ar.TERMINAL_STATES:
            summary["skipped_not_terminal"] += 1
            continue
        if store.is_delivered(job.job_id):
            summary["already"] += 1
            continue
        cid = normalize_telegram_id((job.payload or {}).get("chat_id"))
        if not cid:
            summary["skipped_no_chat"] += 1
            continue
        if allow and cid not in allow:
            # Never deliver to a chat that is not on the allowlist.
            summary["skipped_not_allowed"] += 1
            continue
        text = redact(format_research_result(job), secrets or [token])
        # Reserve the delivery slot BEFORE sending so a crash mid-send cannot
        # double-deliver; a send failure is surfaced but not retried in-loop.
        if not store.mark_delivered(job.job_id, cid):
            summary["already"] += 1
            continue
        ok = True
        for chunk in chunk_message(text):
            try:
                client.send_message(token=token, chat_id=cid, text=chunk)
            except Exception:  # noqa: BLE001 - a send failure is not fatal
                ok = False
        if ok:
            summary["delivered"] += 1
        else:
            summary["send_errors"] += 1
    return summary


def diagnose(*, client, token: Optional[str]) -> str:
    """Read-only ``getMe`` probe. Never sends a message. Returns a
    classification token."""
    if not token:
        return TELEGRAM_TOKEN_MISSING
    try:
        resp = client.get_me(token=token)
    except Exception:  # noqa: BLE001
        return TELEGRAM_UNREACHABLE
    if not isinstance(resp, dict):
        return TELEGRAM_UNREACHABLE
    if resp.get("ok") and (resp.get("result") or {}).get("id"):
        return TELEGRAM_AUTH_OK
    return TELEGRAM_AUTH_REJECTED


# --------------------------------------------------------------------------- #
# Real HTTP client (stdlib urllib). Injected in production; faked in tests.
# --------------------------------------------------------------------------- #
class UrllibTelegramClient:
    """Minimal Telegram Bot API client over stdlib urllib. The token is used
    only in the request URL path to api.telegram.org and never logged."""

    def __init__(self, api_base: str = _API_BASE, timeout: float = 40.0):
        self.api_base = api_base
        self.timeout = timeout

    def _call(self, token: str, method: str, params: dict) -> dict:
        import urllib.error
        import urllib.parse
        import urllib.request
        url = "%s/bot%s/%s" % (self.api_base, token, method)
        data = urllib.parse.urlencode(
            {k: v for k, v in params.items() if v is not None}).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                return json.loads(e.read().decode("utf-8"))
            except Exception:  # noqa: BLE001
                return {"ok": False, "error_code": e.code}

    def get_me(self, *, token: str) -> dict:
        return self._call(token, "getMe", {})

    def get_updates(self, *, token: str, offset: int, timeout: int) -> dict:
        return self._call(token, "getUpdates",
                          {"offset": offset, "timeout": timeout})

    def send_message(self, *, token: str, chat_id, text: str) -> dict:
        return self._call(token, "sendMessage",
                          {"chat_id": chat_id, "text": text,
                           "disable_web_page_preview": "true"})


# --------------------------------------------------------------------------- #
# Production wiring for read-only providers (best-effort, all read-only).
# --------------------------------------------------------------------------- #
def build_default_providers(*, stage8_config: Optional[dict] = None,
                            queue=None) -> dict:
    """Wire the read-only evidence providers to the live research state. Every
    provider is READ-ONLY (it only READS the durable queue + the last persisted
    source-registry snapshot — it never acquires data, runs an experiment or
    mutates anything). Failures degrade to a friendly string."""
    cfg = stage8_config or {}

    def _load_snapshot() -> dict:
        path = ((cfg.get("sources") or {}).get("registry_snapshot_path")
                if isinstance(cfg.get("sources"), dict) else None)
        if path and Path(path).is_file():
            try:
                return json.loads(Path(path).read_text(encoding="utf-8-sig"))
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def _cat_states(category: str) -> dict:
        if queue is None:
            return {}
        out = {}
        for j in queue.list_jobs(category=category, limit=500):
            out[j.state] = out.get(j.state, 0) + 1
        return out

    def _recent_jobs(limit: int = 12) -> list:
        """Most-recent jobs (list_jobs orders by priority/created; re-sort by the
        settled/created timestamp so the newest work shows first)."""
        if queue is None:
            return []
        rows = queue.list_jobs(limit=200)
        rows.sort(key=lambda j: (j.updated_at or j.created_at or ""),
                  reverse=True)
        return rows[:limit]

    def _queue_summary() -> str:
        if queue is None:
            return "Queue unavailable."
        c = queue.counts_by_state()
        lines = ["Research queue: %d queued, %d running, %d retryable, %d "
                 "blocked, %d completed, %d rejected, %d failed." % (
                     c.get("QUEUED", 0), c.get("RUNNING", 0),
                     c.get("RETRYABLE", 0), c.get("BLOCKED_SPECIFIC", 0),
                     c.get("COMPLETED", 0), c.get("REJECTED", 0),
                     c.get("FAILED_PERMANENT", 0))]
        nxt = queue.list_jobs(state="QUEUED", limit=5)
        if nxt:
            lines.append("Next queued:")
            lines.extend("  - %s (%s)" % (j.lane, j.category) for j in nxt)
        run = queue.list_jobs(state="RUNNING", limit=5)
        if run:
            lines.append("Running:")
            lines.extend("  - %s (%s)" % (j.lane, j.category) for j in run)
        return "\n".join(lines)

    def _blocked() -> str:
        if queue is None:
            return "Queue unavailable."
        rows = queue.blocked_jobs(limit=10)
        if not rows:
            return "No blocked jobs."
        return "Blocked jobs:\n" + "\n".join(
            "- %s [%s]: %s" % (j.lane, j.category, j.blocked_reason or "")
            for j in rows)

    def _sources() -> str:
        snap = _load_snapshot()
        tally = snap.get("classification_tally") or {}
        if not tally:
            return "Source registry not probed yet."
        srcs = snap.get("sources") or []
        now = [s.get("information_family") or s.get("source_id")
               for s in srcs if s.get("classification") == "ACCESSIBLE_NOW"]
        head = ", ".join(str(x) for x in now[:8]) if now else "none"
        return ("Sources: %d accessible now, %d need (free) repair, %d "
                "prospective-only, %d paid-not-owned, %d invalid-credential.\n"
                "Accessible now includes: %s%s" % (
                    tally.get("ACCESSIBLE_NOW", 0),
                    tally.get("ACCESSIBLE_AFTER_REPAIR", 0),
                    tally.get("PROSPECTIVE_ONLY", 0),
                    tally.get("PAID_NOT_OWNED", 0),
                    tally.get("INVALID_CREDENTIAL", 0),
                    head, " ..." if len(now) > 8 else ""))

    def _campaign_coverage() -> Optional[str]:
        """Production-universe campaign coverage (target/completed/remaining/
        repair) when the durable campaign store has been seeded. Read-only;
        returns None when no campaign store exists yet."""
        prod = cfg.get("production") if isinstance(cfg.get("production"), dict) \
            else {}
        campdb = prod.get("campaign_db")
        if not campdb or not Path(campdb).is_file():
            return None
        try:
            from . import acquisition_campaign as acq
            store = acq.CampaignStore(campdb)
            parts = []
            for c in store.list_campaigns():
                cid = c.get("campaign_id") if isinstance(c, dict) else c
                cov = store.coverage(cid)
                parts.append(
                    "  %s: %s/%s done, %s remaining, %s repair" % (
                        cid, cov.get("completed_symbol_count"),
                        cov.get("full_universe_target_count"),
                        cov.get("remaining_symbol_count"),
                        cov.get("repair_backlog_count")))
            return ("Production-universe campaigns:\n" + "\n".join(parts)
                    ) if parts else None
        except Exception:  # noqa: BLE001
            return None

    def _coverage() -> str:
        lines = []
        camp = _campaign_coverage()
        if camp:
            lines.append(camp)
        snap = _load_snapshot()
        tally = snap.get("classification_tally") or {}
        if tally:
            lines.append(
                "Coverage gaps: %d free sources awaiting a collector-lane "
                "repair; %d prospective fields with no owned/free history."
                % (tally.get("ACCESSIBLE_AFTER_REPAIR", 0),
                   tally.get("PROSPECTIVE_ONLY", 0)))
        elif not camp:
            return ("Coverage not computed yet. Expected source: the Stage 8 "
                    "source-registry snapshot (run a source probe). No coverage "
                    "figures are available until then.")
        lines.append(
            "PIT limitation: a prospective field has no backfill before its "
            "first valid snapshot; every gap carries an exact source + reason "
            "in the registry snapshot.")
        return "\n".join(lines)

    def _data() -> str:
        st = _cat_states("DATA_ACQUISITION")
        if not st:
            return "No data-acquisition jobs recorded yet."
        return "Data acquisition jobs: " + ", ".join(
            "%d %s" % (v, k.lower()) for k, v in sorted(st.items()))

    def _experiments() -> str:
        st = _cat_states("EXPERIMENT")
        tg = _cat_states("TELEGRAM_REQUEST")
        if not st and not tg and queue is None:
            return "No experiments recorded yet."
        parts = []
        if st:
            parts.append("Experiments: " + ", ".join(
                "%d %s" % (v, k.lower()) for k, v in sorted(st.items())))
        # Recent completed experiments with their real evidence metrics.
        if queue is not None:
            recent = [j for j in queue.list_jobs(category="EXPERIMENT", limit=60)
                      if j.state == "COMPLETED" and (j.result or {}).get("results")]
            recent.sort(key=lambda j: (j.finished_at or j.updated_at or ""),
                        reverse=True)
            shown, seen = [], set()
            for j in recent:
                for r in (j.result.get("results") or [])[:1]:
                    label = r.get("label") or r.get("feature")
                    if label in seen:
                        continue
                    seen.add(label)
                    shown.append(
                        "  %s: rank-IC t=%s, spread t=%s -> %s" % (
                            label, _fmt_num(r.get("rank_ic_t")),
                            _fmt_num(r.get("spread_t")), r.get("decision")))
                if len(shown) >= 5:
                    break
            if shown:
                parts.append("Recent evidence:")
                parts.extend(shown)
        if tg:
            parts.append("Your requested experiments: " + ", ".join(
                "%d %s" % (v, k.lower()) for k, v in sorted(tg.items())))
        return "\n".join(parts) if parts else "No experiments recorded yet."

    def _jobs() -> str:
        if queue is None:
            return "Queue unavailable."
        rows = _recent_jobs(12)
        if not rows:
            return "No research jobs recorded yet."
        lines = ["Recent research jobs (newest first):"]
        lines.extend("- %s [%s] %s (%s)" % (j.job_id, j.state, j.lane,
                                            j.category) for j in rows)
        lines.append("Use /job <id> for full detail.")
        return "\n".join(lines)

    def _job(arg: Optional[str] = None) -> str:
        if queue is None:
            return "Queue unavailable."
        jid = (arg or "").strip()
        if not jid:
            return ("Usage: /job <job_id>. Send /jobs to list recent job ids.")
        j = queue.get(jid)
        if j is None:
            return ("No job found with id '%s'. Send /jobs to list recent job "
                    "ids." % jid)
        lines = [
            "Job %s" % j.job_id,
            "Category: %s" % j.category,
            "Lane: %s" % j.lane,
            "State: %s" % j.state,
            "Origin: %s" % j.origin,
            "Attempts: %d/%d" % (j.attempts, j.max_attempts),
        ]
        if j.started_at or j.finished_at:
            lines.append("Last execution: started %s / finished %s" % (
                j.started_at or "n/a", j.finished_at or "n/a"))
        if j.blocked_reason:
            lines.append("Blocker: %s" % j.blocked_reason)
        res = j.result or {}
        if res:
            rrows = res.get("results") or []
            if rrows:
                top = rrows[0]
                lines.append("Result: %s -> %s (rank-IC t=%s)" % (
                    top.get("label") or top.get("feature"),
                    top.get("decision"), _fmt_num(top.get("rank_ic_t"))))
            disp = res.get("disposition") or res.get("decision") \
                or res.get("real_work") or res.get("note")
            if disp:
                lines.append("Disposition: %s" % disp)
            # Stage 9.2 tournament weakest-gate follow-up detail (read-only).
            if res.get("real_work") == "tournament_weakest_gate_validation":
                if res.get("candidate_id"):
                    lines.append("Candidate: %s (%s)" % (
                        res.get("candidate_id"),
                        res.get("candidate_lifecycle_state") or "DATA_HOLD"))
                if res.get("data_dependency"):
                    lines.append("Data dependency: %s | coverage sufficient: %s"
                                 % (res.get("data_dependency"),
                                    "yes" if res.get("sufficient") else "no"))
                cov = res.get("coverage") or {}
                shown = [(k, cov[k]) for k in (
                    "distinct_issuers", "insider_transaction_records",
                    "sec_8k_item202_events", "distinct_vintage_dates_on_disk",
                    "distinct_symbols_pit_classified") if k in cov]
                if shown:
                    lines.append("Coverage: " + ", ".join(
                        "%s=%s" % (k, v) for k, v in shown))
                if res.get("next_action"):
                    lines.append("Next action: %s" % res.get("next_action"))
                lines.append("No model is promoted to live trading (research "
                             "evidence only).")
            for k in ("run_dir", "run_id", "artifact_path", "output_path",
                      "evidence_path", "raw_path", "normalized_path"):
                if res.get(k):
                    lines.append("Artifact: %s" % res.get(k))
                    break
        return "\n".join(lines)

    def _candidates() -> str:
        if queue is None:
            return "Queue unavailable."
        keep, rejected, hold = [], set(), 0
        rej_count = 0
        for cat in ("EXPERIMENT", "ROBUSTNESS_TEST", "SIGNAL_COMBINATION"):
            for j in queue.list_jobs(category=cat, limit=200):
                res = j.result or {}
                hold += int(res.get("data_holds") or 0)
                for r in (res.get("results") or []):
                    dec = str(r.get("decision") or "")
                    label = r.get("label") or r.get("feature")
                    if "KEEP" in dec:
                        keep.append(label)
                    elif "REJECT" in dec:
                        rejected.add(label)
                        rej_count += 1
                    elif "HOLD" in dec or "DATA" in dec:
                        hold += 1
        keep_u = sorted(set(k for k in keep if k))
        if not keep_u and not rejected and not hold:
            return ("No evaluated alpha candidates yet. Completed experiments "
                    "surface KEEP_FOR_RESEARCH / REJECT / DATA_HOLD dispositions "
                    "here. Nothing is ever promoted to live trading.")
        lines = ["Alpha candidate dispositions (research evidence only):"]
        lines.append("KEEP_FOR_RESEARCH: %s" % (", ".join(keep_u) if keep_u
                                                else "none"))
        lines.append("REJECTED: %d evaluation(s)%s" % (
            rej_count, (" — e.g. " + ", ".join(sorted(rejected)[:5]))
            if rejected else ""))
        lines.append("DATA_HOLD: %d" % hold)
        lines.append("The paper research champion remains composite_sn. No "
                     "model is promoted to live trading — research only.")
        return "\n".join(lines)

    def _report() -> str:
        if queue is None:
            return ("Research activity unavailable — the durable research queue "
                    "is not reachable. Expected source: the Stage 8 autonomy "
                    "queue database.")
        c = queue.counts_by_state()
        return "\n".join([
            "RESEARCH ACTIVITY (read-only summary)",
            "Last autonomous progress: %s" % (queue.last_progress_at() or "n/a"),
            "Completed jobs: %d | queued: %d | blocked: %d" % (
                c.get("COMPLETED", 0), c.get("QUEUED", 0),
                c.get("BLOCKED_SPECIFIC", 0)),
            "The formal executive report is delivered by the Alpha Agent email "
            "cadence (Stage 7.1); this is the live research-activity summary.",
        ])

    def _health() -> str:
        if queue is None:
            return "Queue unavailable."
        depth = queue.depth()
        stale = len(queue.stale_running())
        return ("Health: queue depth %d (never-idle=%s), %d stale-running, last "
                "progress %s." % (depth, "yes" if depth >= 1 else "no", stale,
                                  queue.last_progress_at() or "n/a"))

    def _status() -> str:
        return "\n".join([_queue_summary(), _sources(), _health()])

    def _open_campaign_store():
        prod = cfg.get("production") if isinstance(cfg.get("production"), dict) \
            else {}
        campdb = prod.get("campaign_db")
        if not campdb or not Path(campdb).is_file():
            return None
        from . import acquisition_campaign as acq
        return acq.CampaignStore(campdb)

    def _sec_cont_caps():
        cd = (((cfg.get("autonomy") or {}).get("collect_drain") or {})
              .get("sec_continuation") or {})
        return (int(cd.get("daily_completed_batch_cap", 6) or 6),
                int(cd.get("max_consecutive_no_progress_batches", 2) or 2),
                bool(cd.get("enabled")))

    def _campaigns() -> str:
        store = _open_campaign_store()
        if store is None:
            return ("No acquisition campaigns yet. Expected source: the Stage 8 "
                    "campaign store (seeded by the first production batch).")
        lines = ["ACQUISITION CAMPAIGNS (read-only)"]
        for c in store.list_campaigns():
            cid = c.get("campaign_id") if isinstance(c, dict) else c
            cov = store.coverage(cid)
            nb = store.batch_count(cid)
            last = store.last_batch(cid) or {}
            lines.append(
                "%s: %s/%s done, %s remaining, %s repair | %d batches, last "
                "progress=%s%s" % (
                    cid, cov.get("completed_symbol_count"),
                    cov.get("full_universe_target_count"),
                    cov.get("remaining_symbol_count"),
                    cov.get("repair_backlog_count"), nb,
                    ("yes" if last.get("progress") else "no") if last else "n/a",
                    (" stop=%s" % last.get("stop_reason"))
                    if last.get("stop_reason") else ""))
        return "\n".join(lines)

    def _campaign(arg: str) -> str:
        arg = (arg or "").strip()
        if not arg:
            return "Usage: /campaign <campaign_id>  (see /campaigns for ids)"
        store = _open_campaign_store()
        if store is None:
            return "No campaign store available yet."
        cov = store.coverage(arg)
        if not cov.get("exists"):
            return "Unknown campaign %r. See /campaigns for valid ids." % arg
        from datetime import datetime as _d, timezone as _tz
        today = _d.now(_tz.utc).date().isoformat()
        cap, noprog_max, enabled = _sec_cont_caps()
        done_today = store.batches_on_date(arg, today)
        noprog = store.consecutive_no_progress(arg)
        last = store.last_batch(arg) or {}
        return "\n".join([
            "CAMPAIGN %s (read-only)" % arg,
            "State: %s | cursor %s/%s, remaining %s, repair %s, permanent %s" % (
                "COMPLETE" if cov.get("is_complete") else "IN_PROGRESS",
                cov.get("completed_symbol_count"),
                cov.get("full_universe_target_count"),
                cov.get("remaining_symbol_count"),
                cov.get("repair_backlog_count"),
                cov.get("permanent_failed_count")),
            "Batches: %d total, %d today (cap %d), consecutive no-progress %d "
            "(max %d)" % (store.batch_count(arg), done_today, cap, noprog,
                          noprog_max),
            "Last batch: progress=%s, records+%s, backlog=%s, stop=%s" % (
                "yes" if last.get("progress") else "no",
                last.get("records_added"), last.get("remaining_backlog"),
                last.get("stop_reason") or "none"),
            "Controlled continuation: %s (origin campaign-continuation admitted "
            "only for lane acq.sec_form4_8k + DATA_ACQUISITION)."
            % ("ENABLED" if enabled else "PAUSED"),
        ])

    providers = {"queue": _queue_summary, "blocked": _blocked,
                 "status": _status, "sources": _sources, "coverage": _coverage,
                 "data": _data, "experiments": _experiments, "health": _health,
                 "jobs": _jobs, "job": _job, "candidates": _candidates,
                 "report": _report, "campaigns": _campaigns,
                 "campaign": _campaign}
    return providers


# --------------------------------------------------------------------------- #
# Operational (Paper Trader) read-only providers — the ONE canonical source.
#
# Every provider READS the canonical operational-book aggregation
# (api.operational_book.load_operational_book) and the canonical forward
# attribution (api.forward_evidence.build_daily_attribution) — the same
# functions that power the operational UI. Nothing here writes, creates an
# order/fill/signal/decision, promotes a model or mutates holdings/cash. The
# loaders are INJECTABLE so unit tests never touch a DB or a desk ledger.
# --------------------------------------------------------------------------- #
def _fmt_money(x) -> str:
    v = _num_or_none(x)
    return "n/a" if v is None else "$%s" % format(v, ",.2f")


def _fmt_signed_money(x) -> str:
    v = _num_or_none(x)
    if v is None:
        return "n/a"
    return ("+$%s" % format(v, ",.2f")) if v >= 0 else ("-$%s" % format(abs(v), ",.2f"))


def _fmt_pct(x, digits: int = 2) -> str:
    """Format a PERCENT-unit value (1.22 -> '+1.22%')."""
    v = _num_or_none(x)
    return "n/a" if v is None else ("%+.*f%%" % (digits, v))


def _fmt_pp(x, digits: int = 2) -> str:
    """Format a percentage-point value ( -0.46 -> '-0.46 pp')."""
    v = _num_or_none(x)
    return "n/a" if v is None else ("%+.*f pp" % (digits, v))


def _fmt_weight(x, digits: int = 1) -> str:
    """Format a FRACTION weight (0.0466 -> '4.7%')."""
    v = _num_or_none(x)
    return "n/a" if v is None else ("%.*f%%" % (digits, v * 100.0))


def _num_or_none(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _fmt_score(x):
    v = _num_or_none(x)
    return "%.3f" % v if v is not None else "n/a"


def _fmt2(x):
    v = _num_or_none(x)
    return "%.2f" % v if v is not None else "n/a"


def _fmt3(x):
    v = _num_or_none(x)
    return "%.3f" % v if v is not None else "n/a"


def _op_unavailable(label: str, expected: str, last_date, reason: str) -> str:
    """The actionable missing-data diagnostic (never the vague generic line)."""
    return ("%s DATA UNAVAILABLE\nExpected source: %s\nLast successful date: "
            "%s\nReason: %s" % (label, expected, last_date or "unknown", reason))


def _default_ops_loader() -> dict:
    from paper_trader.api.operational_book import load_operational_book
    return load_operational_book()


def _default_attribution_loader() -> dict:
    from paper_trader.api.forward_evidence import build_daily_attribution
    return build_daily_attribution()


_OPS_SOURCE = "api.operational_book.load_operational_book"
_ATTR_SOURCE = "api.forward_evidence.build_daily_attribution"


def build_operational_providers(*, ops_loader: Optional[Callable] = None,
                                attribution_loader: Optional[Callable] = None
                                ) -> dict:
    """Read-only Paper Trader providers: performance / nav / book / positions /
    attribution, all sourced from the canonical operational-book + forward
    attribution functions. Degrade to an ACTIONABLE diagnostic, never a fabricated
    value and never the vague 'unavailable' line."""

    def _load(fn) -> "tuple[dict, Optional[str]]":
        try:
            r = fn()
            return (r if isinstance(r, dict) else {}, None)
        except Exception as exc:  # noqa: BLE001
            return ({}, "%s: %s" % (type(exc).__name__, str(exc)[:140]))

    def _ops():
        return _load(ops_loader or _default_ops_loader)

    def _attr():
        return _load(attribution_loader or _default_attribution_loader)

    def _performance() -> str:
        book, err = _ops()
        o = book.get("operational_book") or {}
        if err or not o.get("initialized"):
            return _op_unavailable(
                "PERFORMANCE", _OPS_SOURCE, o.get("nav_as_of_date"),
                err or "Alpha Paper Book #1 is not initialized yet.")
        attr, aerr = _attr()
        p = (attr.get("portfolio") or {}) if attr.get("available") else {}
        lines = [
            "PAPER BOOK PERFORMANCE",
            "Book: %s" % (o.get("book_label") or OPERATIONAL_BOOK_FALLBACK),
            "Strategy: %s" % (o.get("strategy_name") or "n/a"),
            "Market date: %s" % (attr.get("market_date")
                                 or o.get("nav_as_of_date")
                                 or o.get("desk_mark_date") or "unknown"),
            "NAV: %s" % _fmt_money(o.get("nav")),
        ]
        if attr.get("available"):
            lines += [
                "Today: %s (%s)" % (_fmt_signed_money(p.get("daily_pnl")),
                                    _fmt_pct(p.get("daily_return_pct"))),
                "Since baseline: %s (%s)" % (
                    _fmt_signed_money(p.get("cumulative_pnl")),
                    _fmt_pct(p.get("cumulative_return_pct"))),
                "SPY today: %s" % _fmt_pct(p.get("spy_daily_return_pct")),
                "SPY since baseline: %s" % _fmt_pct(
                    p.get("spy_cumulative_return_pct")),
                "Daily excess: %s" % _fmt_pp(p.get("daily_excess_return_pct")),
                "Cumulative excess: %s" % _fmt_pp(
                    p.get("cumulative_excess_return_pct")),
                "Drawdown: %s" % _fmt_pct(p.get("drawdown_pct")),
            ]
        else:
            lines.append(
                "Daily P&L: unavailable — %s" % (
                    aerr or attr.get("reason")
                    or "a prior completed operational mark is required."))
        return "\n".join(lines)

    def _nav() -> str:
        book, err = _ops()
        o = book.get("operational_book") or {}
        if err or not o.get("initialized"):
            return _op_unavailable(
                "NAV", _OPS_SOURCE, o.get("nav_as_of_date"),
                err or "Alpha Paper Book #1 is not initialized yet.")
        attr, _aerr = _attr()
        p = (attr.get("portfolio") or {}) if attr.get("available") else {}
        lines = [
            "%s — NAV" % (o.get("book_label") or OPERATIONAL_BOOK_FALLBACK),
            "Market date: %s" % (o.get("nav_as_of_date")
                                 or o.get("desk_mark_date") or "unknown"),
            "NAV: %s" % _fmt_money(o.get("nav")),
        ]
        if attr.get("available"):
            lines.append("Today: %s (%s)" % (
                _fmt_signed_money(p.get("daily_pnl")),
                _fmt_pct(p.get("daily_return_pct"))))
        return "\n".join(lines)

    def _book() -> str:
        book, err = _ops()
        o = book.get("operational_book") or {}
        if err or not o.get("initialized"):
            return _op_unavailable(
                "BOOK", _OPS_SOURCE, o.get("desk_mark_date"),
                err or "Alpha Paper Book #1 is not initialized yet.")
        ps = o.get("portfolio_summary") or {}
        hd = o.get("holdings_detail") or []
        lines = [
            o.get("book_label") or OPERATIONAL_BOOK_FALLBACK,
            "Strategy: %s" % (o.get("strategy_name") or "n/a"),
            "Target: %s" % (o.get("target_name") or "n/a"),
            "Holdings: %d (target %s)" % (o.get("holdings_count") or 0,
                                          o.get("target_count") or "n/a"),
            "Invested: %s | Cash: %s" % (_fmt_weight(ps.get("invested_weight")),
                                         _fmt_weight(ps.get("cash_weight"))),
            "Valuation date: %s" % (o.get("desk_mark_date") or "unknown"),
            "NAV: %s" % _fmt_money(o.get("nav")),
        ]
        top = [r for r in hd if r.get("market_value") is not None][:8]
        if top:
            lines.append("Top positions:")
            for i, r in enumerate(top, 1):
                lines.append("  %2d. %-5s %s  %s" % (
                    i, r.get("ticker"), _fmt_money(r.get("market_value")),
                    _fmt_weight(r.get("current_weight"))))
        return "\n".join(lines)

    def _positions() -> str:
        book, err = _ops()
        o = book.get("operational_book") or {}
        if err or not o.get("initialized"):
            return _op_unavailable(
                "POSITIONS", _OPS_SOURCE, o.get("desk_mark_date"),
                err or "Alpha Paper Book #1 is not initialized yet.")
        hd = o.get("holdings_detail") or []
        if not hd:
            return _op_unavailable(
                "POSITIONS", _OPS_SOURCE + ".holdings_detail",
                o.get("desk_mark_date"),
                "No holdings resolved from the desk-ledger replay.")
        lines = ["POSITIONS — %s (%d) as of %s" % (
            o.get("book_label") or OPERATIONAL_BOOK_FALLBACK, len(hd),
            o.get("desk_mark_date") or "unknown")]
        for r in hd:
            lines.append("%-5s %s @ %s  MV %s  %s  P/L %s" % (
                r.get("ticker"), r.get("quantity"),
                _fmt_money(r.get("latest_price")),
                _fmt_money(r.get("market_value")),
                _fmt_weight(r.get("current_weight")),
                _fmt_signed_money(r.get("unrealized_pnl"))))
        return "\n".join(lines)

    def _attribution() -> str:
        attr, aerr = _attr()
        if not attr.get("available"):
            return _op_unavailable(
                "ATTRIBUTION", _ATTR_SOURCE, attr.get("market_date"),
                aerr or attr.get("reason")
                or "a prior completed operational mark is required.")
        p = attr.get("portfolio") or {}
        recon = attr.get("reconciliation") or {}
        lines = [
            "DAILY ATTRIBUTION — %s (prior %s)" % (
                attr.get("market_date"), attr.get("prior_market_date")),
            "Total daily P&L: %s (%s)" % (
                _fmt_signed_money(p.get("daily_pnl")),
                _fmt_pct(p.get("daily_return_pct"))),
        ]
        win = attr.get("winners") or []
        los = attr.get("losers") or []
        if win:
            lines.append("Top contributors:")
            lines.extend("  %-5s %s (%s)" % (
                w.get("ticker"), _fmt_signed_money(w.get("pnl_contribution")),
                _fmt_pct(w.get("daily_return_pct"))) for w in win)
        if los:
            lines.append("Top detractors:")
            lines.extend("  %-5s %s (%s)" % (
                l.get("ticker"), _fmt_signed_money(l.get("pnl_contribution")),
                _fmt_pct(l.get("daily_return_pct"))) for l in los)
        lines.append("Reconciliation: contributions %s vs NAV move %s "
                     "(residual %s) %s" % (
                         _fmt_signed_money(recon.get("position_contribution_sum")),
                         _fmt_signed_money(recon.get("market_movement")),
                         _fmt_signed_money(recon.get("residual")),
                         "OK" if recon.get("reconciles") else "CHECK"))
        return "\n".join(lines)

    return {"performance": _performance, "nav": _nav, "book": _book,
            "positions": _positions, "attribution": _attribution}


# The operator-facing book name used when the payload could not be loaded.
OPERATIONAL_BOOK_FALLBACK = "Alpha Paper Book #1"


def _resolve_stage9_config_path(stage8_config) -> Optional[str]:
    """Repo-relative/absolute Stage 9 config path from the Stage 8 config's
    ``tournament.config`` pointer, or None."""
    if not isinstance(stage8_config, dict):
        return None
    t = stage8_config.get("tournament") \
        if isinstance(stage8_config.get("tournament"), dict) else {}
    p = t.get("config")
    if not p:
        return None
    pp = Path(p)
    if not pp.is_absolute():
        pp = Path(__file__).resolve().parents[1] / p
    return str(pp) if pp.exists() else None


def _default_tournament_payload(config_path):
    from paper_trader.alpha_agent import tournament as _tt
    return _tt.load_tournament(config_path=config_path)


def _default_candidate_payload(config_path, cid):
    from paper_trader.alpha_agent import tournament as _tt
    return _tt.load_candidate(config_path=config_path, candidate_id=cid)


def build_tournament_providers(*, config_path: Optional[str] = None,
                               tournament_loader: Optional[Callable] = None,
                               candidate_loader: Optional[Callable] = None
                               ) -> dict:
    """Read-only Stage 9 tournament providers: /tournament /leaderboard
    /candidate /why /compare /shadowbooks /shadowbook /families. Every provider
    reads the persistent candidate registry through the read-only tournament
    payload functions; none can mutate a candidate, an experiment, a shadow book
    or any trading state. An unknown id returns a precise diagnostic."""

    def _payload() -> dict:
        try:
            fn = tournament_loader or (
                lambda: _default_tournament_payload(config_path))
            r = fn()
            return r if isinstance(r, dict) else {"status": "UNAVAILABLE"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "UNAVAILABLE", "reason": type(exc).__name__}

    def _cand(cid):
        try:
            fn = candidate_loader or (
                lambda c: _default_candidate_payload(config_path, c))
            return fn(cid)
        except Exception:  # noqa: BLE001
            return None

    def _unavail(label, p):
        return ("%s UNAVAILABLE\nReason: %s\nThe candidate registry is created on "
                "the first production tournament cycle." % (
                    label, p.get("reason") or "not initialized yet"))

    def _tournament():
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("TOURNAMENT", p)
        cs = p.get("counts_by_state") or {}
        lines = ["ALPHA TOURNAMENT - READ-ONLY RESEARCH",
                 "No automatic promotion; not approved for live trading.",
                 "Candidates: %d ranked / %d total"
                 % (p.get("ranked", 0), sum(cs.values())),
                 "By state: " + ", ".join("%s %d" % (k, v)
                                          for k, v in cs.items() if v)]
        lines.append("Shadow books active: %d" % len(p.get("shadow_books") or []))
        ch = p.get("recent_changes") or []
        if ch:
            lines.append("Latest change: %s" % ch[0].get("kind"))
        blk = p.get("blockers") or []
        if blk:
            lines.append("Blockers: %s" % ", ".join(blk[:4]))
        return "\n".join(lines)

    def _leaderboard():
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("LEADERBOARD", p)
        rows = [r for r in (p.get("leaderboard") or [])
                if r.get("overall_rank")][:10]
        if not rows:
            return ("LEADERBOARD\nNo ranked candidates yet (all data-held or "
                    "untested).")
        lines = ["ALPHA TOURNAMENT LEADERBOARD (top %d)" % len(rows)]
        for r in rows:
            lines.append("#%s %-24s %-16s score %s | %s [+%s / -%s]" % (
                r.get("overall_rank"), (r.get("name") or "")[:24],
                r.get("lifecycle_state"), _fmt_score(r.get("combined_score")),
                r.get("family"), r.get("strongest_evidence"),
                r.get("largest_weakness")))
        return "\n".join(lines)

    def _candidate(arg=None):
        if not arg:
            return "Usage: /candidate <id or name>"
        d = _cand(arg.strip())
        if not d or not d.get("candidate"):
            return ("No candidate matches '%s'. Try /leaderboard for ids."
                    % arg.strip())
        c = d["candidate"]
        ev = d.get("evaluation") or {}
        m = ev.get("metrics") or {}
        lines = ["CANDIDATE - %s" % c.get("name"),
                 "id: %s" % c.get("candidate_id"),
                 "family: %s | state: %s" % (c.get("family"),
                                             c.get("lifecycle_state")),
                 "universe: %s" % c.get("universe"),
                 "PIT status: %s" % c.get("pit_status"),
                 "data dependencies: %s"
                 % ", ".join(c.get("data_dependencies") or []),
                 "combined score: %s" % _fmt_score(c.get("combined_score")),
                 "blocker / next required: %s"
                 % (c.get("blocker") or "run historical evaluation")]
        if m:
            lines.append(
                "evidence: rank-IC %s (t %s), spread t %s, net@25bps %s, "
                "maxDD%% %s" % (
                    _fmt3(m.get("rank_ic")), _fmt2(m.get("rank_ic_t")),
                    _fmt2(m.get("spread_t")), _fmt3(m.get("net25_spread")),
                    _fmt2(m.get("max_drawdown_pct"))))
            db = m.get("data_boundaries") or {}
            lines.append(
                "boundaries: periods %s, universe %s, PIT-valid %s, "
                "survivorship-safe %s" % (
                    db.get("scored_periods"), db.get("universe"),
                    db.get("point_in_time_valid"), db.get("survivorship_safe")))
        return "\n".join(lines)

    def _why(arg=None):
        if not arg:
            return "Usage: /why <id or name>"
        d = _cand(arg.strip())
        if not d or not d.get("candidate"):
            return "No candidate matches '%s'." % arg.strip()
        c = d["candidate"]
        gate = (d.get("evaluation") or {}).get("gate") or {}
        state = c.get("lifecycle_state")
        why = {
            "DATA_HOLD": "Held: the owned data cannot evaluate this factor "
            "honestly yet (%s). The data-acquisition lane keeps working on it."
            % (c.get("blocker") or "insufficient data"),
            "REJECTED": "Rejected on COMPLETE evidence: %s. A weak factor is "
            "never kept merely because one metric looked good." % (
                ", ".join(gate.get("failed_gates") or []) or c.get("blocker")),
            "KEEP_FOR_RESEARCH": "Retained: it passed every historical gate on "
            "complete evidence. Next it needs a shadow book with true-forward "
            "observations before any manual review.",
            "SHADOW_BOOK_ACTIVE": "Retained with an active research shadow book; "
            "accruing true-forward observations toward the manual-review "
            "threshold. Never auto-promoted.",
            "READY_FOR_MANUAL_REVIEW": "Awaiting HUMAN manual review. Even here "
            "it never changes the operating model automatically.",
            "PROPOSED": "Proposed; awaiting its first historical evaluation.",
            "TESTING": "Under historical evaluation this cycle.",
            "ARCHIVED": "Archived.",
        }.get(state, "State: %s" % state)
        return "WHY - %s (%s)\n%s" % (c.get("name"), state, why)

    def _compare(arg=None):
        parts = (arg or "").split()
        if len(parts) < 2:
            return "Usage: /compare <id1> <id2>"
        a = _cand(parts[0])
        b = _cand(parts[1])
        if not a or not a.get("candidate") or not b or not b.get("candidate"):
            return "Both ids must match a candidate. Try /leaderboard."
        ca, cb = a["candidate"], b["candidate"]
        ma = (a.get("evaluation") or {}).get("metrics") or {}
        mb = (b.get("evaluation") or {}).get("metrics") or {}

        def _row(lbl, va, vb):
            return "  %-14s %-15s %-15s" % (lbl, va, vb)

        lines = ["COMPARE",
                 _row("", (ca.get("name") or "")[:15], (cb.get("name") or "")[:15]),
                 _row("state", ca.get("lifecycle_state"), cb.get("lifecycle_state")),
                 _row("combined", _fmt_score(ca.get("combined_score")),
                      _fmt_score(cb.get("combined_score"))),
                 _row("rank-IC t", _fmt2(ma.get("rank_ic_t")),
                      _fmt2(mb.get("rank_ic_t"))),
                 _row("net@25bps", _fmt3(ma.get("net25_spread")),
                      _fmt3(mb.get("net25_spread"))),
                 _row("turnover", _fmt2(ma.get("turnover_per_rebalance")),
                      _fmt2(mb.get("turnover_per_rebalance"))),
                 _row("maxDD%", _fmt2(ma.get("max_drawdown_pct")),
                      _fmt2(mb.get("max_drawdown_pct"))),
                 _row("corr vs champ", _fmt2(ma.get("corr_vs_champion")),
                      _fmt2(mb.get("corr_vs_champion")))]
        sa = _num_or_none(ca.get("combined_score"))
        sb = _num_or_none(cb.get("combined_score"))
        if sa is not None or sb is not None:
            lead = ca if (sa or -1) >= (sb or -1) else cb
            lines.append("Higher combined score: %s" % lead.get("name"))
        return "\n".join(lines)

    def _shadowbooks():
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("SHADOW BOOKS", p)
        sb = p.get("shadow_books") or []
        if not sb:
            return ("SHADOW BOOKS\nNone active. A shadow book opens for the "
                    "strongest retained candidate.")
        lines = ["RESEARCH SHADOW BOOKS - NOT THE OPERATING PORTFOLIO"]
        for s in sb:
            lines.append("  %s  candidate %s  since %s  [%s]" % (
                s.get("shadow_book_id"), s.get("candidate_id"),
                s.get("inception_date"), s.get("status")))
        return "\n".join(lines)

    def _shadowbook(arg=None):
        if not arg:
            return "Usage: /shadowbook <shadow_book_id or candidate id>"
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("SHADOW BOOK", p)
        key = arg.strip()
        for s in (p.get("shadow_books") or []):
            if key in (s.get("shadow_book_id") or "") \
                    or key in (s.get("candidate_id") or ""):
                return ("RESEARCH SHADOW BOOK - NOT THE OPERATING PORTFOLIO\n"
                        "id: %s\ncandidate: %s\ninception: %s\nstatus: %s\n"
                        "Forward history is never backfilled before inception."
                        % (s.get("shadow_book_id"), s.get("candidate_id"),
                           s.get("inception_date"), s.get("status")))
        return "No shadow book matches '%s'." % key

    def _families():
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("FAMILIES", p)
        cf = p.get("counts_by_family") or {}
        vf = p.get("variants_by_family") or {}
        if not cf:
            return "FAMILIES\nNo candidates seeded yet."
        lines = ["CANDIDATE FAMILIES"]
        for fam in sorted(cf):
            lines.append("  %-18s %d candidates | %d generated variants" % (
                fam, cf.get(fam, 0), vf.get(fam, 0)))
        return "\n".join(lines)

    # data_dependency -> (responsible campaign, estimated path to evaluation)
    _DEP_PATH = {
        "sec_form4": ("sec_form4_8k", "grow owned SEC Form 4 issuer coverage "
                      "via the controlled sec_form4_8k continuation"),
        "sec_8k": ("sec_form4_8k", "grow owned SEC 8-K Item 2.02 coverage via "
                   "the controlled sec_form4_8k continuation"),
        "sec_form4_8k": ("sec_form4_8k", "controlled SEC Form 4 / 8-K "
                         "continuation"),
        "eodhd_analyst_vintages": (None, "calendar-time accrual of the daily "
                                   "EODHD analyst vintage (hard PIT floor; no "
                                   "backfill)"),
        "point_in_time_gics": (None, "grow the leakage-safe SEC ASSIGNED-SIC "
                               "PIT sector series"),
        "point_in_time_fundamentals": (None, "build the owned SEC companyfacts "
                                       "PIT-fundamentals slice (free per-CIK; no "
                                       "provider purchase)"),
    }

    def _dataholds():
        p = _payload()
        if p.get("status") != "OK":
            return _unavail("DATA HOLDS", p)
        rows = [r for r in (p.get("leaderboard") or [])
                if r.get("lifecycle_state") == "DATA_HOLD"]
        if not rows:
            return "DATA HOLDS\nNo candidates are on DATA_HOLD."
        lines = ["DATA_HOLD CANDIDATES (read-only) - %d" % len(rows),
                 "Each stays DATA_HOLD until real owned coverage meets the "
                 "configured requirement; nothing is forced."]
        for r in rows:
            lines.append("  %-26s %-16s %s" % (
                (r.get("name") or "")[:26], r.get("family"),
                r.get("blocker") or r.get("next_required_evidence") or ""))
        lines.append("Use /datahold <id> for coverage vs requirement + path.")
        return "\n".join(lines)

    def _datahold(arg=None):
        if not arg:
            return "Usage: /datahold <id or name>  (see /dataholds)"
        d = _cand(arg.strip())
        if not d or not d.get("candidate"):
            return "No candidate matches '%s'." % arg.strip()
        c = d["candidate"]
        if c.get("lifecycle_state") != "DATA_HOLD":
            return "%s is %s, not DATA_HOLD." % (c.get("name"),
                                                 c.get("lifecycle_state"))
        deps = c.get("data_dependencies") or []
        lines = ["DATA_HOLD - %s" % c.get("name"),
                 "family: %s | pit: %s" % (c.get("family"), c.get("pit_status")),
                 "missing evidence (blocker): %s" % (c.get("blocker") or "n/a"),
                 "data dependencies: %s" % ", ".join(deps)]
        for dep in deps:
            camp, path = _DEP_PATH.get(dep, (None, "acquire owned coverage"))
            lines.append("  - %s -> campaign: %s; path: %s" % (
                dep, camp or "calendar-time / build", path))
        cov = d.get("data_coverage") if isinstance(d.get("data_coverage"),
                                                    dict) else {}
        if cov.get("next_action"):
            lines.append("next action: %s" % cov.get("next_action"))
        return "\n".join(lines)

    return {"tournament": _tournament, "leaderboard": _leaderboard,
            "candidate": _candidate, "why": _why, "compare": _compare,
            "shadowbooks": _shadowbooks, "shadowbook": _shadowbook,
            "families": _families, "dataholds": _dataholds,
            "datahold": _datahold}


def build_command_center_providers(*, stage8_config: Optional[dict] = None,
                                   queue=None,
                                   ops_loader: Optional[Callable] = None,
                                   attribution_loader: Optional[Callable] = None
                                   ) -> dict:
    """The full read-only command center: the Stage 8 research providers PLUS the
    canonical Paper Trader operational providers PLUS the Stage 9 tournament
    providers. This is what the live poll loop wires so /pnl, /book, /positions,
    /attribution and /tournament all return real data."""
    providers = build_default_providers(stage8_config=stage8_config, queue=queue)
    providers.update(build_operational_providers(
        ops_loader=ops_loader, attribution_loader=attribution_loader))
    providers.update(build_tournament_providers(
        config_path=_resolve_stage9_config_path(stage8_config)))
    return providers


# --------------------------------------------------------------------------- #
# CLI entrypoints (invoked by the .ps1 wrappers; token arrives on STDIN only).
# --------------------------------------------------------------------------- #
def _read_token_from_stdin() -> str:
    line = sys.stdin.readline()
    # Windows stdin pipes can prepend a UTF-8 BOM that str.strip() misses.
    return line.replace("\ufeff", "").strip()


def _load_config(path: Optional[str]) -> dict:
    if not path:
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Alpha Agent Stage 8 Telegram control plane.")
    parser.add_argument("--mode",
                        choices=("diagnose", "poll", "route-test", "deliver"),
                        default="diagnose")
    parser.add_argument("--config", default=None)
    parser.add_argument("--max-cycles", type=int, default=0,
                        help="0 = run forever (poll mode)")
    parser.add_argument("--message", default=None,
                        help="route-test only: message text to classify")
    args = parser.parse_args(argv)

    raw = _load_config(args.config)
    cfg = TelegramConfig(raw)

    if args.mode == "route-test":
        intent = resolve_intent(args.message or "")
        sys.stdout.write(json.dumps(intent) + "\n")
        return 0

    token = _read_token_from_stdin()
    client = UrllibTelegramClient(api_base=cfg.api_base)

    if args.mode == "diagnose":
        classification = diagnose(client=client, token=token)
        meta = {}
        try:
            me = client.get_me(token=token)
            r = (me or {}).get("result") or {}
            # The bot's username/id are PUBLIC (non-secret); surfacing them lets
            # the user confirm they are messaging the right bot.
            meta = {"bot_username": r.get("username"),
                    "bot_id": normalize_telegram_id(r.get("id"))}
        except Exception:  # noqa: BLE001
            pass
        sys.stdout.write(json.dumps(
            {"classification": classification, **meta}) + "\n")
        token = None
        return 0 if classification == TELEGRAM_AUTH_OK else 1

    # The Telegram control plane and the autonomy cycle MUST share ONE queue;
    # honor the canonical ``autonomy.queue_db`` (falling back to the derived
    # path only when it is absent, e.g. in tests) exactly as the runtime does.
    from . import autonomous_research as ar
    _auto = raw.get("autonomy") if isinstance(raw.get("autonomy"), dict) else {}
    state_db = cfg.state_db or str(
        Path(raw.get("stage8_root", ".")) / "stage8" / "telegram_state.sqlite")
    queue_db = _auto.get("queue_db") or str(
        Path(raw.get("stage8_root", ".")) / "stage8" / "autonomy.sqlite")

    if args.mode == "deliver":
        # One-shot delivery of terminal research results back to the private
        # chat (used by the acceptance run and safe to call any time). Requires
        # the token (it sends messages) but acquires nothing and mutates no job.
        if not token:
            sys.stdout.write(json.dumps(
                {"classification": TELEGRAM_TOKEN_MISSING}) + "\n")
            return 1
        store = TelegramStore(state_db)
        queue = ar.ResearchQueue(queue_db)
        summary = deliver_results(
            client=client, token=token, queue=queue, store=store,
            allowed_chat_ids=cfg.allowed_chat_ids, secrets=[token])
        sys.stdout.write(json.dumps({"mode": "deliver", **summary}) + "\n")
        token = None
        return 0

    # poll mode
    if not token:
        sys.stdout.write(json.dumps(
            {"classification": TELEGRAM_TOKEN_MISSING}) + "\n")
        return 1
    store = TelegramStore(state_db)
    queue = ar.ResearchQueue(queue_db)
    # The full command center: Stage 8 research providers + the canonical Paper
    # Trader operational providers (performance / book / positions / attribution).
    providers = build_command_center_providers(stage8_config=raw, queue=queue)
    router = ControlRouter(providers=providers, queue=queue, secrets=[token])
    cycles = 0
    import time
    while True:
        try:
            summary = poll_once(client=client, token=token, cfg=cfg,
                                store=store, router=router)
            # After answering inbound messages, deliver any research RESULT that
            # has since completed back to its originating private chat (exactly
            # once). This is what closes the loop: a request enqueued on one poll
            # is answered with its real result on a later poll once the autonomy
            # cycle has run the bounded experiment.
            deliver = deliver_results(
                client=client, token=token, queue=queue, store=store,
                allowed_chat_ids=cfg.allowed_chat_ids, secrets=[token])
            summary["delivered_results"] = deliver
            # Per-cycle summary (counts only; carries no id/text/secret) so the
            # runner and audit trail show exactly what was processed.
            sys.stdout.write(json.dumps({"cycle": cycles + 1, **summary}) + "\n")
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - polling must never crash the task
            time.sleep(5)
        cycles += 1
        if args.max_cycles and cycles >= args.max_cycles:
            break
        time.sleep(1)
    token = None
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
