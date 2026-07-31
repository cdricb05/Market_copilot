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

# Supported explicit commands (WS12).
COMMANDS = ("/help", "/status", "/data", "/coverage", "/queue", "/experiments",
            "/blocked", "/book", "/performance", "/report", "/sources",
            "/health", "/run")

# Intent kinds — the ONLY two side-effect classes plus help.
KIND_READ_ONLY = "READ_ONLY_QUERY"
KIND_RESEARCH_JOB = "RESEARCH_JOB"
KIND_HELP = "HELP"

# Command -> read-only evidence provider key (resolved by the injected
# ``providers`` map). ``/run`` is handled separately as a research job.
_COMMAND_PROVIDER = {
    "/status": "status", "/data": "data", "/coverage": "coverage",
    "/queue": "queue", "/experiments": "experiments", "/blocked": "blocked",
    "/book": "book", "/performance": "performance", "/report": "report",
    "/sources": "sources", "/health": "health",
}

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
    (("experiment", "ran today", "what experiments"), "/experiments"),
    (("queue", "work list", "what's queued", "whats queued"), "/queue"),
    (("blocked", "why was", "rejected"), "/blocked"),
    # "what data sources are available?" is a SOURCE-REGISTRY question — it must
    # win over the coverage-gap route, so /sources is checked first and matches
    # "source(s)" / "data available" directly.
    (("source", "which eodhd", "endpoints available", "norgate history",
      "data available", "available data", "what data have we",
      "what data do we", "what data are"), "/sources"),
    (("coverage", "not acquired", "missing data", "coverage gap"), "/coverage"),
    (("book", "holdings", "portfolio"), "/book"),
    (("performance", "pnl", "p/l", "how are we doing"), "/performance"),
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
def resolve_intent(text: str) -> dict:
    """Deterministically map a message to an intent. Returns
    ``{command, kind, research_text}``. Unknown text routes to HELP (never to an
    arbitrary action)."""
    raw = (text or "").strip()
    low = raw.lower()
    if not raw:
        return {"command": "/help", "kind": KIND_HELP, "research_text": None}

    # Defense-in-depth: injection/command-shaped input is routed to HELP and is
    # never interpreted. (The router already has NO code path to a shell, SQL,
    # file or trade action; this simply avoids even classifying such text.)
    if any(tok in low for tok in _DANGEROUS_TOKENS):
        return {"command": "/help", "kind": KIND_HELP, "research_text": None}

    # Explicit slash command.
    if raw.startswith("/"):
        cmd = raw.split()[0].split("@")[0].lower()
        if cmd == "/run":
            arg = raw[len(cmd):].strip()
            if not arg:
                return {"command": "/run", "kind": KIND_HELP,
                        "research_text": None}
            return {"command": "/run", "kind": KIND_RESEARCH_JOB,
                    "research_text": arg}
        if cmd in _COMMAND_PROVIDER:
            return {"command": cmd, "kind": KIND_READ_ONLY,
                    "research_text": None}
        return {"command": "/help", "kind": KIND_HELP, "research_text": None}

    # Natural language: a research verb + an experiment-ish object => job.
    if any(v in (" " + low) for v in _NL_RESEARCH_VERBS) and any(
            k in low for k in ("experiment", "momentum", "value", "quality",
                               "earnings", "reversal", "residual", "factor",
                               "surprise", "insider", "volatility", "accrual",
                               "champion", "signal", "combination")):
        return {"command": "/run", "kind": KIND_RESEARCH_JOB,
                "research_text": raw}

    # Natural language read-only routes.
    for keys, cmd in _NL_READ_ROUTES:
        if any(k in low for k in keys):
            return {"command": cmd, "kind": KIND_READ_ONLY,
                    "research_text": None}

    return {"command": "/help", "kind": KIND_HELP, "research_text": raw}


HELP_TEXT = (
    "Alpha Agent control (read-only + research requests only; no trading).\n"
    "Commands:\n"
    "/status - agent + schedule + queue summary\n"
    "/data - data acquisition status\n"
    "/coverage - data coverage + gaps\n"
    "/sources - source entitlement classifications\n"
    "/queue - current research queue\n"
    "/experiments - experiments run + outcomes\n"
    "/blocked - blocked jobs and why\n"
    "/book - active paper book (read-only)\n"
    "/performance - paper P/L (read-only)\n"
    "/report - latest executive report status\n"
    "/health - system + watchdog health\n"
    "/run <request> - queue a BOUNDED research request\n"
    "You can also ask in plain English, e.g. 'what experiments ran today?', "
    "'which EODHD endpoints are available?', 'test residual momentum excluding "
    "financials', 'show the current research queue'.")


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
            reply = HELP_TEXT
        elif kind == KIND_READ_ONLY:
            reply = self._read_only(cmd)
        elif kind == KIND_RESEARCH_JOB:
            reply = self._enqueue_research(intent["research_text"], update)
        else:
            reply = HELP_TEXT
        reply = redact(reply, self.secrets)
        return chunk_message(reply)

    def _read_only(self, command: str) -> str:
        key = _COMMAND_PROVIDER.get(command)
        provider = self.providers.get(key)
        if provider is None:
            return "That information is not available right now."
        try:
            out = provider()
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

    def _queue_summary() -> str:
        if queue is None:
            return "Queue unavailable."
        c = queue.counts_by_state()
        return ("Research queue: %d queued, %d running, %d retryable, %d "
                "blocked, %d completed, %d rejected, %d failed." % (
                    c.get("QUEUED", 0), c.get("RUNNING", 0),
                    c.get("RETRYABLE", 0), c.get("BLOCKED_SPECIFIC", 0),
                    c.get("COMPLETED", 0), c.get("REJECTED", 0),
                    c.get("FAILED_PERMANENT", 0)))

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

    def _coverage() -> str:
        snap = _load_snapshot()
        tally = snap.get("classification_tally") or {}
        if not tally:
            return "Coverage not computed yet."
        return ("Coverage gaps: %d free sources awaiting a collector-lane "
                "repair; %d prospective fields with no owned/free history (must "
                "be collected forward from a PIT floor). Every gap has an exact "
                "source + reason in the registry snapshot." % (
                    tally.get("ACCESSIBLE_AFTER_REPAIR", 0),
                    tally.get("PROSPECTIVE_ONLY", 0)))

    def _data() -> str:
        st = _cat_states("DATA_ACQUISITION")
        if not st:
            return "No data-acquisition jobs recorded yet."
        return "Data acquisition jobs: " + ", ".join(
            "%d %s" % (v, k.lower()) for k, v in sorted(st.items()))

    def _experiments() -> str:
        st = _cat_states("EXPERIMENT")
        tg = _cat_states("TELEGRAM_REQUEST")
        if not st and not tg:
            return "No experiments recorded yet."
        parts = []
        if st:
            parts.append("Experiments: " + ", ".join(
                "%d %s" % (v, k.lower()) for k, v in sorted(st.items())))
        if tg:
            parts.append("Your requested experiments: " + ", ".join(
                "%d %s" % (v, k.lower()) for k, v in sorted(tg.items())))
        return "\n".join(parts)

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

    providers = {"queue": _queue_summary, "blocked": _blocked,
                 "status": _status, "sources": _sources, "coverage": _coverage,
                 "data": _data, "experiments": _experiments, "health": _health}
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
    providers = build_default_providers(stage8_config=raw, queue=queue)
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
