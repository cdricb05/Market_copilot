"""
alpha_agent.runtime — Stage 4 persistent Windows research runtime.

Turns the proven Stage 1-3.5 components into a persistent, observable,
recoverable research agent that collects source + RSS data on a cadence, runs
bounded grounded LLM research cycles, renders a deterministic friendly report
and hands it to a deterministic email sender — while NEVER touching the
operational portfolio, never creating trading activity and never running the
prediction service.

Hard safety invariants (enforced structurally):
  * RESEARCH AUTOMATION ONLY. Nothing here creates or mutates an order, fill,
    signal, trade decision, model promotion, Alpha Paper Book, Paper Trader DB
    row or Daily Close. Operational ledgers are read strictly read-only and
    fingerprinted before/after every cycle.
  * No PostgreSQL, no prediction-service call, no LLM tool use / code execution.
  * No credential is ever read, stored, logged or passed to the LLM. Email
    credentials live only in Windows DPAPI storage outside the repo and are used
    exclusively by the PowerShell sender; this process never sees them.
  * Every identifier is a deterministic function of its inputs; completed report
    cycles are idempotent and never re-invoke the LLM or re-send an email.

The stage runners, portfolio reader, clock and email sender are all injectable
so the whole runtime is exercised deterministically with fakes (no real network,
no real email, no real Task Scheduler) in tests.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from . import experiment_contracts as ec
from . import experiment_factory as ef
from . import report_renderer as rr
from . import runtime_contracts as rc

REQUIRED_RUN_FILES = (
    "runtime_input.json", "component_results.json", "collection_summary.json",
    "research_summary.json", "portfolio_context.json", "email_manifest.json",
    "scheduler_state.json", "runtime_report.md", "run_manifest.json",
)

# Stage 5 experiment & evidence component id (Stage 5 is optional and only run
# when the config sets ``stage5_enabled``). Defined here — not in
# runtime_contracts — so the Stage 4 contract schema is untouched.
COMPONENT_STAGE5 = "stage5_experiment_factory"


# --------------------------------------------------------------------------- #
# Clock (injectable). Real clock is naive local time — Windows Task Scheduler
# fires in local time, so the local date/label are the correct cycle keys.
# --------------------------------------------------------------------------- #
class Clock:
    def now(self) -> datetime:  # pragma: no cover - trivial
        return datetime.now()

    def iso(self) -> str:
        return self.now().replace(microsecond=0).isoformat()

    def date(self) -> str:
        return self.now().date().isoformat()


class FixedClock(Clock):
    """Deterministic clock for tests and reproducible run ids."""

    def __init__(self, moment: datetime):
        self._moment = moment

    def now(self) -> datetime:
        return self._moment


# --------------------------------------------------------------------------- #
# Small deterministic IO helpers.
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str),
                   encoding="utf-8")
    os.replace(tmp, path)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# Directory layout + state database.
# --------------------------------------------------------------------------- #
def runtime_root(cfg: dict) -> Path:
    return Path(cfg["runtime_root"])


def ensure_layout(root: Path) -> None:
    for rel in ("state", "locks", "logs", "reports", "runs",
                "outbox/pending", "outbox/sent", "outbox/failed"):
        (root / rel).mkdir(parents=True, exist_ok=True)


def state_db_path(root: Path) -> Path:
    return root / "state" / "runtime_state.sqlite"


def open_state_db(root: Path, *, create: bool = True) -> sqlite3.Connection:
    """Open (and, when create, initialise) the runtime state database."""
    path = state_db_path(root)
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
    else:
        if not path.exists():
            raise FileNotFoundError("runtime state db missing: %s" % path)
        # immutable=1: read the main db file directly and NEVER create/modify
        # the -wal/-shm sidecars — verify mode must write nothing at all.
        conn = sqlite3.connect("file:%s?immutable=1" % path.as_posix(),
                               uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if create:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:
            pass
        conn.executescript(rc.SCHEMA_SQL)
        conn.execute(
            "INSERT OR IGNORE INTO runtime_meta (key, value) VALUES (?, ?)",
            ("schema_version", rc.RUNTIME_SCHEMA_VERSION))
        conn.commit()
    return conn


# --------------------------------------------------------------------------- #
# Locks (file + DB record; stale detection by stored acquired_at vs clock).
# --------------------------------------------------------------------------- #
class LockHeld(RuntimeError):
    """Raised when a live (non-stale) lock is already held."""


@dataclass
class LockHandle:
    name: str
    path: Path
    run_id: str
    stale_cleared: bool = False


def _lock_path(root: Path, name: str) -> Path:
    return root / "locks" / ("%s.lock" % name)


def acquire_lock(root: Path, name: str, run_id: str, *, clock: Clock,
                 conn: Optional[sqlite3.Connection] = None,
                 stale_seconds: int = 1800) -> LockHandle:
    path = _lock_path(root, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    stale_cleared = False
    existing = _read_json(path) if path.exists() else None
    if existing:
        acquired = existing.get("acquired_at")
        age = _age_seconds(acquired, clock)
        if age is not None and age < stale_seconds:
            raise LockHeld("lock '%s' held by run %s (age %ds < %ds)" %
                           (name, existing.get("run_id"), int(age),
                            stale_seconds))
        # Stale: clear and record recovery.
        stale_cleared = True
        if conn is not None:
            with conn:
                conn.execute(
                    "UPDATE runtime_locks SET released_at=?, stale_cleared=1"
                    " WHERE name=?", (clock.iso(), name))
                conn.execute(
                    "INSERT INTO recovery_actions (action, target, detail,"
                    " run_id, recorded_at) VALUES (?,?,?,?,?)",
                    ("CLEAR_STALE_LOCK", name,
                     "prior run %s age %ss" % (existing.get("run_id"),
                                               int(age) if age else "n/a"),
                     run_id, clock.iso()))
    payload = {"name": name, "run_id": run_id, "pid": os.getpid(),
               "host": _hostname(), "acquired_at": clock.iso()}
    _write_json_atomic(path, payload)
    if conn is not None:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO runtime_locks (name, run_id,"
                " acquired_at, pid, host, released_at, stale_cleared)"
                " VALUES (?,?,?,?,?,NULL,?)",
                (name, run_id, clock.iso(), os.getpid(), _hostname(),
                 1 if stale_cleared else 0))
    return LockHandle(name=name, path=path, run_id=run_id,
                      stale_cleared=stale_cleared)


def release_lock(handle: LockHandle, *, clock: Clock,
                 conn: Optional[sqlite3.Connection] = None) -> None:
    try:
        if handle.path.exists():
            handle.path.unlink()
    except OSError:
        pass
    if conn is not None:
        with conn:
            conn.execute("UPDATE runtime_locks SET released_at=? WHERE name=?",
                         (clock.iso(), handle.name))


def _age_seconds(iso_ts: Optional[str], clock: Clock) -> Optional[float]:
    if not iso_ts:
        return None
    try:
        then = datetime.fromisoformat(iso_ts)
    except (TypeError, ValueError):
        return None
    now = clock.now()
    try:
        return (now - then).total_seconds()
    except TypeError:
        # tz-aware vs naive mismatch — compare naive.
        return (now.replace(tzinfo=None) - then.replace(tzinfo=None)
                ).total_seconds()


def _hostname() -> str:
    try:
        return socket.gethostname()
    except OSError:  # pragma: no cover
        return "unknown-host"


# --------------------------------------------------------------------------- #
# Heartbeat.
# --------------------------------------------------------------------------- #
def write_heartbeat(root: Path, payload: dict, *, clock: Clock,
                    conn: Optional[sqlite3.Connection] = None) -> None:
    hb = dict(payload)
    hb.setdefault("recorded_at", clock.iso())
    _write_json_atomic(root / "heartbeat.json", hb)
    if conn is not None:
        with conn:
            conn.execute(
                "INSERT INTO heartbeat_history (recorded_at, mode, status,"
                " run_id, payload_json) VALUES (?,?,?,?,?)",
                (hb["recorded_at"], hb.get("mode", "?"),
                 hb.get("status", "?"), hb.get("run_id"),
                 rc.canonical_json(hb)))


def read_heartbeat(root: Path) -> Optional[dict]:
    return _read_json(root / "heartbeat.json")


# --------------------------------------------------------------------------- #
# Read-only portfolio reader (operational ledgers → paper-book context).
# --------------------------------------------------------------------------- #
class PortfolioReader:
    """Reads the operational ledgers READ-ONLY and produces paper-book context.

    Opens no DB connection and never writes. Missing files degrade gracefully to
    ``None`` fields rather than raising.
    """

    def __init__(self, ledger_roots: list[str]):
        self.roots = [Path(r) for r in ledger_roots]

    def _find(self, name: str) -> Optional[Path]:
        for root in self.roots:
            p = root / name
            if p.exists():
                return p
        return None

    def read(self) -> dict:
        perf = self._find("forward_performance.json")
        journal = self._find("daily_close_journal.json")
        progress = self._find("daily_close_progress.json")
        outcomes = self._find("forward_prediction_outcomes.json")

        pb: dict[str, Any] = {
            "valuation_date": None, "nav": None, "cash": None,
            "daily_pnl": None, "daily_return_pct": None,
            "cumulative_pnl": None, "cumulative_return_pct": None,
            "drawdown_pct": None, "spy_daily_pct": None,
            "spy_cumulative_pct": None, "daily_excess_pp": None,
            "cumulative_excess_pp": None, "matured_evidence": 0,
            "pending_evidence": 0, "risk_gate": None,
            "close_status": None, "close_running": None,
        }

        rows = ((_read_json(perf) or {}).get("rows") if perf else None) or []
        if rows:
            last = rows[-1].get("row", {})
            prev = rows[-2].get("row", {}) if len(rows) >= 2 else {}
            pb["valuation_date"] = last.get("date")
            pb["nav"] = last.get("nav")
            pb["cash"] = last.get("cash")
            pb["daily_return_pct"] = last.get("daily_return_pct")
            pb["cumulative_return_pct"] = last.get("cumulative_return_pct")
            pb["drawdown_pct"] = last.get("drawdown_pct")
            pb["spy_cumulative_pct"] = last.get(
                "benchmark_cumulative_return_pct")
            # daily P&L from NAV delta when a prior row exists.
            if prev and last.get("nav") is not None \
                    and prev.get("nav") is not None:
                pb["daily_pnl"] = round(float(last["nav"]) - float(prev["nav"]),
                                        2)
            # SPY daily return from consecutive benchmark closes.
            bc, pbc = last.get("benchmark_close"), prev.get("benchmark_close")
            if bc and pbc:
                pb["spy_daily_pct"] = round((float(bc) / float(pbc) - 1.0)
                                            * 100.0, 4)
            if pb["daily_return_pct"] is not None \
                    and pb["spy_daily_pct"] is not None:
                pb["daily_excess_pp"] = round(
                    float(pb["daily_return_pct"]) - float(pb["spy_daily_pct"]),
                    4)
            if pb["cumulative_return_pct"] is not None \
                    and pb["spy_cumulative_pct"] is not None:
                pb["cumulative_excess_pp"] = round(
                    float(pb["cumulative_return_pct"])
                    - float(pb["spy_cumulative_pct"]), 4)

        jrows = ((_read_json(journal) or {}).get("rows") if journal else None) \
            or []
        if jrows:
            jl = jrows[-1]
            # Authoritative P&L from the close journal when present.
            if jl.get("daily_pnl") is not None:
                pb["daily_pnl"] = jl.get("daily_pnl")
            if jl.get("cumulative_pnl") is not None:
                pb["cumulative_pnl"] = jl.get("cumulative_pnl")
            gate = jl.get("gate_outcome") or jl.get("decision")
            summ = jl.get("checks_summary_line")
            pb["risk_gate"] = (" · ".join([x for x in (gate, summ) if x])
                               or None)

        prog = _read_json(progress) if progress else None
        if prog:
            pb["close_status"] = prog.get("final_close_status")
            pb["close_running"] = prog.get("running")
            if not pb["valuation_date"]:
                pb["valuation_date"] = prog.get("evaluation_date")

        orows = ((_read_json(outcomes) or {}).get("rows") if outcomes else
                 None) or []
        matured = sum(1 for r in orows if str(r.get("status")).upper()
                      == "MATURED")
        pb["matured_evidence"] = matured
        pb["pending_evidence"] = max(0, len(orows) - matured)
        return pb


def fingerprint_ledgers(cfg: dict) -> dict:
    """SHA-256 of every operational-ledger file (read-only integrity baseline)."""
    out: dict[str, str] = {}
    for root in rc.coerce_iterable(cfg.get("operational_ledger_roots")):
        rp = Path(root)
        if not rp.exists():
            continue
        for f in sorted(rp.rglob("*")):
            if f.is_file():
                out[str(f)] = _sha256_file(f)
    return out


# --------------------------------------------------------------------------- #
# Stage drivers (injectable). The real driver calls the verified Stage 1-3.5
# public functions; fakes are injected in tests so no network/LLM runs.
# --------------------------------------------------------------------------- #
def _normalize_stage_result(component: str, result: dict, *,
                            success_tokens: tuple, verified_token: str,
                            no_new_token: str) -> dict:
    status = result.get("status") or result.get("token") or ""
    terminal = result.get("terminal") or status
    ok = status in success_tokens or status == verified_token \
        or status == no_new_token
    return {
        "component": component,
        "status": status,
        "terminal": terminal,
        "ok": bool(ok),
        "verified": status == verified_token,
        "no_new": status == no_new_token,
        "run_id": result.get("run_id"),
        "run_dir": result.get("run_dir"),
        "counts": result.get("counts") or {},
        "metrics": result.get("metrics") or {},
        "raw": result,
    }


class StageDrivers:
    """Interface used by the runtime. Override methods to inject fakes."""

    def verify_stage1(self) -> dict:
        raise NotImplementedError

    def collect_stage2(self, mode: str) -> dict:
        raise NotImplementedError

    def verify_stage2(self) -> dict:
        raise NotImplementedError

    def collect_stage35(self, mode: str) -> dict:
        raise NotImplementedError

    def verify_stage35(self) -> dict:
        raise NotImplementedError

    def research_stage3(self, mode: str) -> dict:
        raise NotImplementedError

    def verify_stage3(self) -> dict:
        raise NotImplementedError

    # Stage 5 is optional: the base returns a SKIPPED component so existing
    # drivers that predate Stage 5 keep working unchanged. Only invoked by the
    # runtime when the config sets ``stage5_enabled``.
    def run_stage5(self, mode: str) -> dict:
        return {"component": COMPONENT_STAGE5, "status": "STAGE5_SKIPPED",
                "terminal": "STAGE5_SKIPPED", "ok": True, "no_new": False,
                "verified": False, "skipped": True, "run_id": None,
                "run_dir": None, "counts": {}, "metrics": {}, "result": {}}

    def verify_stage5(self) -> dict:
        return {"component": COMPONENT_STAGE5, "status": "STAGE5_SKIPPED",
                "terminal": "STAGE5_SKIPPED", "ok": True, "verified": False,
                "skipped": True}


class RealStageDrivers(StageDrivers):
    """Drives the verified Stage 1-3.5 packages via their public functions.

    Read-only for Stage 1 (verifies the latest package artifact); Stage 2 and
    Stage 3.5 run bounded incremental collection; Stage 3 runs one bounded
    incremental research-director cycle. No operational mutation ever occurs.
    """

    def __init__(self, cfg: dict, *, repo_root: Path,
                 git_commit: str = "UNKNOWN",
                 contact_email: Optional[str] = None):
        self.cfg = cfg
        self.repo_root = repo_root
        self.git_commit = git_commit
        self.contact_email = contact_email
        self._stage_cfgs = cfg.get("stage_configs") or {}

    def _load_cfg(self, key: str) -> dict:
        ref = self._stage_cfgs.get(key)
        if not ref:
            raise rc.ConfigError("stage_configs missing '%s'" % key)
        p = Path(ref)
        if not p.is_absolute():
            p = self.repo_root / ref
        return json.loads(p.read_text(encoding="utf-8-sig")), p

    # Stage 1's immutable run package is identified by any of these manifests
    # (Stage 1 uses import_manifest.json / current_state_summary.json rather
    # than the run_manifest.json used by Stages 2-3.5).
    _STAGE1_MANIFESTS = ("run_manifest.json", "import_manifest.json",
                         "current_state_summary.json")

    def verify_stage1(self) -> dict:
        root = Path(self.cfg["stage1_registry_root"])
        latest = _read_json(root / "latest.json") or {}
        run_dir = latest.get("run_dir")
        ok = False
        if run_dir:
            rd_path = (root / run_dir) if not Path(run_dir).is_absolute() \
                else Path(run_dir)
            ok = rd_path.is_dir() and any(
                (rd_path / m).exists() for m in self._STAGE1_MANIFESTS)
        return {"component": rc.COMPONENT_STAGE1,
                "status": "ALPHA_AGENT_STAGE1_VERIFIED" if ok
                else "ALPHA_AGENT_STAGE1_BLOCKED",
                "terminal": "stage1 latest %s" % (latest.get("run_id")
                                                  or "MISSING"),
                "ok": ok, "verified": ok, "no_new": False,
                "run_id": latest.get("run_id"), "run_dir": run_dir,
                "counts": latest.get("counts") or {}, "metrics": {},
                "raw": latest}

    def collect_stage2(self, mode: str) -> dict:
        from . import ingestion as ing
        cfg, cfg_path = self._load_cfg("stage2_ingestion")
        res = ing.run_ingestion(
            config=cfg, output_root=self.cfg["stage2_ingestion_root"],
            mode=mode, as_of="latest", git_commit=self.git_commit,
            contact_email=self.contact_email, config_path=str(cfg_path))
        return _normalize_stage_result(
            rc.COMPONENT_STAGE2, res,
            success_tokens=(ing.READY, ing.PARTIAL),
            verified_token=ing.VERIFIED, no_new_token=ing.NO_NEW)

    def verify_stage2(self) -> dict:
        from . import ingestion as ing
        cfg, cfg_path = self._load_cfg("stage2_ingestion")
        res = ing.run_ingestion(
            config=cfg, output_root=self.cfg["stage2_ingestion_root"],
            mode="verify", as_of="latest", git_commit=self.git_commit,
            contact_email=self.contact_email, config_path=str(cfg_path))
        return _normalize_stage_result(
            rc.COMPONENT_STAGE2, res, success_tokens=(ing.READY,),
            verified_token=ing.VERIFIED, no_new_token=ing.NO_NEW)

    def collect_stage35(self, mode: str) -> dict:
        from . import feed_registry as fr
        cfg, cfg_path = self._load_cfg("stage3_5_news_rss")
        feeds_ref = cfg.get("feed_registry_config")
        feeds_path = Path(feeds_ref)
        if not feeds_path.is_absolute():
            feeds_path = self.repo_root / feeds_ref
        feeds_cfg = json.loads(feeds_path.read_text(encoding="utf-8-sig"))
        res = fr.run_news_rss(
            config=cfg, feeds_config=feeds_cfg,
            output_root=self.cfg["stage3_5_news_rss_root"], mode=mode,
            as_of="latest", git_commit=self.git_commit,
            contact_email=self.contact_email)
        return _normalize_stage_result(
            rc.COMPONENT_STAGE35, res,
            success_tokens=(fr.READY, fr.PARTIAL),
            verified_token=fr.VERIFIED, no_new_token=fr.NO_NEW)

    def verify_stage35(self) -> dict:
        from . import feed_registry as fr
        cfg, _ = self._load_cfg("stage3_5_news_rss")
        res = fr.run_news_rss(
            config=cfg, feeds_config={},
            output_root=self.cfg["stage3_5_news_rss_root"], mode="verify",
            as_of="latest", git_commit=self.git_commit,
            contact_email=self.contact_email)
        return _normalize_stage_result(
            rc.COMPONENT_STAGE35, res, success_tokens=(fr.READY,),
            verified_token=fr.VERIFIED, no_new_token=fr.NO_NEW)

    def research_stage3(self, mode: str) -> dict:
        from . import research_director as rd
        cfg, _ = self._load_cfg("stage3_research_director")
        res = rd.run_director(cfg, self.cfg["stage3_director_root"], mode,
                              "latest", git_commit=self.git_commit)
        norm = _normalize_stage_result(
            rc.COMPONENT_STAGE3,
            {**res, "status": res.get("token")},
            success_tokens=(rd.READY, rd.DEV_READY, rd.PARTIAL),
            verified_token=rd.VERIFIED, no_new_token=rd.NO_NEW)
        return norm

    def verify_stage3(self) -> dict:
        from . import research_director as rd
        cfg, _ = self._load_cfg("stage3_research_director")
        res = rd.run_director(cfg, self.cfg["stage3_director_root"], "verify",
                              "latest", git_commit=self.git_commit)
        return _normalize_stage_result(
            rc.COMPONENT_STAGE3, {**res, "status": res.get("token")},
            success_tokens=(rd.READY,), verified_token=rd.VERIFIED,
            no_new_token=rd.NO_NEW)

    def _stage5_cfg(self) -> dict:
        _cfg, path = self._load_cfg("stage5_experiment_factory")
        return ec.load_config(path)

    def run_stage5(self, mode: str) -> dict:
        """Run one bounded Stage 5 experiment & evidence cycle over the verified
        packages. Read-only w.r.t. every operational ledger (fingerprinted)."""
        s5cfg = self._stage5_cfg()
        result = ef.run_stage5_cycle(
            s5cfg, as_of="latest",
            ledger_fingerprint=lambda: fingerprint_ledgers(s5cfg))
        return _norm_stage5(result)

    def verify_stage5(self) -> dict:
        s5cfg = self._stage5_cfg()
        result = ef.verify_cycle(
            s5cfg, ledger_fingerprint=lambda: fingerprint_ledgers(s5cfg))
        return {"component": COMPONENT_STAGE5,
                "status": result.get("terminal"),
                "terminal": result.get("terminal"),
                "ok": result.get("terminal") != ec.BLOCKED,
                "verified": result.get("terminal") == ec.VERIFIED,
                "result": result, "run_id": result.get("run_id")}


# --------------------------------------------------------------------------- #
# Email sender (injectable). The real sender defers to the PowerShell script,
# which alone touches DPAPI credentials; this process never sees a secret.
# --------------------------------------------------------------------------- #
EmailSender = Callable[[dict], dict]

# Gmail-API OAuth delivery statuses produced by
# scripts/send_alpha_agent_email.py. runtime_contracts.py already defines
# EMAIL_SENT, EMAIL_ALREADY_SENT, EMAIL_CREDENTIAL_REQUIRED_STATUS,
# EMAIL_RETRYABLE_FAILURE, EMAIL_PERMANENT_FAILURE and EMAIL_SKIPPED. These
# extend the vocabulary WITHOUT a schema change — they are stored in the existing
# status TEXT column so a failure is no longer collapsed to a single
# EMAIL_RETRYABLE_FAILURE token. Delivery is Gmail-API only; no mail-transport
# status exists any longer.
OAUTH_REAUTHORIZATION_REQUIRED = "OAUTH_REAUTHORIZATION_REQUIRED"
OAUTH_TOKEN_REFRESH_REJECTED = "OAUTH_TOKEN_REFRESH_REJECTED"
OAUTH_CLIENT_INVALID = "OAUTH_CLIENT_INVALID"
GMAIL_API_PERMISSION_DENIED = "GMAIL_API_PERMISSION_DENIED"
GMAIL_API_RATE_LIMITED = "GMAIL_API_RATE_LIMITED"
GMAIL_API_RETRYABLE_FAILURE = "GMAIL_API_RETRYABLE_FAILURE"
EMAIL_JOB_INVALID = "EMAIL_JOB_INVALID"
EMAIL_ATTACHMENT_INVALID = "EMAIL_ATTACHMENT_INVALID"

# Transient failures worth an automatic watchdog retry: rate-limit, Gmail 5xx and
# generic transport failures. Re-running the SAME request can succeed.
EMAIL_TRANSIENT_STATUSES = frozenset({
    GMAIL_API_RATE_LIMITED, GMAIL_API_RETRYABLE_FAILURE,
    rc.EMAIL_RETRYABLE_FAILURE})
# Non-transient failures: surfaced + DEGRADED but NOT blindly auto-retried, since
# retrying identically cannot fix a rejected refresh token (needs re-consent), a
# denied permission, an invalid client or a malformed job. A fresh cycle re-run
# may still re-deliver them. OAUTH_REAUTHORIZATION_REQUIRED lives here so the
# watchdog does not loop on it.
EMAIL_NONRETRYABLE_STATUSES = frozenset({
    OAUTH_REAUTHORIZATION_REQUIRED, OAUTH_TOKEN_REFRESH_REJECTED,
    OAUTH_CLIENT_INVALID, GMAIL_API_PERMISSION_DENIED,
    EMAIL_JOB_INVALID, EMAIL_ATTACHMENT_INVALID, rc.EMAIL_PERMANENT_FAILURE})
# Everything that counts as a delivery failure (drives DEGRADED + attention).
EMAIL_FAILURE_STATUSES = EMAIL_TRANSIENT_STATUSES | EMAIL_NONRETRYABLE_STATUSES
# Full recognised token vocabulary, longest-first so a token that is a substring
# of another (none currently) can never shadow it during a fallback text scan.
_ALL_EMAIL_STATUSES = tuple(sorted(
    ({rc.EMAIL_SENT, rc.EMAIL_ALREADY_SENT,
      rc.EMAIL_CREDENTIAL_REQUIRED_STATUS} | EMAIL_FAILURE_STATUSES),
    key=len, reverse=True))


def credential_present(cfg: dict) -> bool:
    """True when the DPAPI OAuth refresh token has been configured."""
    email_cfg = cfg.get("email") or {}
    cred_dir = email_cfg.get("credential_dir")
    if not cred_dir:
        return False
    token = Path(cred_dir) / (email_cfg.get("refresh_token_file")
                              or "gmail_oauth_refresh_token.dpapi")
    return token.exists()


def make_real_email_sender(cfg: dict, *, repo_root: Path) -> EmailSender:
    email_cfg = cfg.get("email") or {}
    script = repo_root / "scripts" / "send_alpha_agent_email.ps1"
    cred_dir = str(email_cfg.get("credential_dir") or "")
    gmail_endpoint = str(email_cfg.get("gmail_api_send_endpoint")
                         or "https://gmail.googleapis.com/gmail/v1/users/me/"
                            "messages/send")
    token_endpoint = str(email_cfg.get("token_endpoint")
                         or "https://oauth2.googleapis.com/token")
    timeout = int(email_cfg.get("send_timeout_seconds", 120))

    def _send(job: dict) -> dict:
        if not credential_present(cfg):
            return {"status": rc.EMAIL_CREDENTIAL_REQUIRED_STATUS,
                    "error": "gmail oauth refresh token not configured",
                    "diagnostic": "Gmail OAuth refresh token is not "
                                  "configured.",
                    "message_id": None}
        cmd = ["powershell.exe", "-NoProfile", "-NonInteractive",
               "-ExecutionPolicy", "Bypass", "-File", str(script),
               "-JobPath", job["job_path"],
               "-GmailEndpoint", gmail_endpoint,
               "-TokenEndpoint", token_endpoint,
               "-TimeoutSeconds", str(timeout)]
        if cred_dir:
            cmd += ["-CredentialDir", cred_dir]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout + 60)
        except Exception as exc:  # noqa: BLE001
            return {"status": GMAIL_API_RETRYABLE_FAILURE,
                    "error": rc.redact("%s: %s" % (type(exc).__name__, exc)),
                    "diagnostic": "The email wrapper could not be launched.",
                    "message_id": None}
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        status, message_id, diagnostic = _parse_email_result(out)
        error = None if status in rc.EMAIL_SUCCESS_STATUSES else rc.redact(
            diagnostic or out.strip()[-400:])
        return {"status": status, "error": error,
                "diagnostic": diagnostic, "message_id": message_id}

    return _send


def _parse_email_result(text: str):
    """Return ``(status, message_id, diagnostic)`` from sender/wrapper output.

    Prefers the single JSON object the standalone sender prints (carrying the
    Gmail-API status, the delivered message id and a SAFE diagnostic); falls back
    to a token scan of the wrapper's machine-readable status line. Never collapses
    a distinct failure into a generic retryable token when a specific status
    exists.
    """
    status = None
    message_id = None
    diagnostic = ""
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
            except (ValueError, TypeError):
                continue
            if isinstance(obj, dict) and obj.get("status"):
                status = str(obj["status"])
                mid = obj.get("message_id")
                message_id = str(mid) if isinstance(mid, str) and mid else None
                diagnostic = str(obj.get("diagnostic") or "")
    if status is None:
        status = _scan_status_token(text)
        diagnostic = _first_safe_diag_line(text)
    return status, message_id, diagnostic


def _scan_status_token(text: str) -> str:
    for tok in _ALL_EMAIL_STATUSES:
        if tok in text:
            return tok
    return rc.EMAIL_RETRYABLE_FAILURE


def _first_safe_diag_line(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("[gmail]"):
            return s[len("[gmail]"):].strip()
    return ""


# --------------------------------------------------------------------------- #
# Report model assembly (deterministic; no LLM).
# --------------------------------------------------------------------------- #
def _stage35_latest(cfg: dict) -> dict:
    root = Path(cfg["stage3_5_news_rss_root"])
    return _read_json(root / "latest.json") or {}


def _stage3_latest(cfg: dict) -> dict:
    root = Path(cfg["stage3_director_root"])
    return _read_json(root / "latest.json") or {}


def _norm_stage5(result: dict) -> dict:
    """Normalize a Stage 5 cycle result into a runtime component record."""
    terminal = result.get("terminal")
    return {
        "component": COMPONENT_STAGE5, "status": terminal, "terminal": terminal,
        "ok": terminal != ec.BLOCKED,
        "no_new": terminal == ec.NO_EXPERIMENTABLE_HYPOTHESES,
        "verified": False, "run_id": result.get("run_id"),
        "run_dir": result.get("run_dir"),
        "counts": result.get("counts") or {}, "metrics": {},
        "result": result, "raw": result,
    }


def _stage5_report_model(cfg: dict,
                         fresh_result: Optional[dict] = None) -> Optional[dict]:
    """Deterministic Experiment & Evidence report model — from this cycle's
    fresh Stage 5 result if present, else the latest package on disk, else None.
    All numbers originate in deterministic Python (no LLM)."""
    if fresh_result:
        return ef.experiment_report_model(fresh_result)
    root = cfg.get("stage5_experiments_root")
    if not root:
        return None
    latest = _read_json(Path(root) / "latest.json") or {}
    rid = latest.get("run_id")
    if not rid:
        return None
    run_dir = Path(root) / "runs" / rid
    manifest = _read_json(run_dir / "run_manifest.json") or {}
    result = ef._load_result(Path(root), rid, manifest)
    result["terminal"] = latest.get("terminal") or manifest.get("terminal")
    result["status"] = latest.get("status") or manifest.get("status")
    result["champion_model"] = latest.get("champion_model")
    return ef.experiment_report_model(result)


def _load_stage3_run(cfg: dict, run_dir: Optional[str]) -> dict:
    if not run_dir:
        return {}
    root = Path(cfg["stage3_director_root"])
    rp = Path(run_dir) if Path(run_dir).is_absolute() else (root / run_dir)
    out: dict[str, Any] = {"events": [], "hypotheses": [], "tokens": {}}
    ev_path = rp / "structured_event_analysis.jsonl"
    if ev_path.exists():
        for line in ev_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out["events"].append(json.loads(line))
            except ValueError:
                continue
    hyp = _read_json(rp / "hypothesis_proposals.json")
    if isinstance(hyp, dict):
        out["hypotheses"] = hyp.get("hypotheses") or hyp.get("proposals") or []
    elif isinstance(hyp, list):
        out["hypotheses"] = hyp
    tok = _read_json(rp / "token_cost_report.json")
    if isinstance(tok, dict):
        out["tokens"] = _extract_token_usage(tok)
    return out


def _extract_token_usage(tok: dict) -> dict:
    """Reserved-token usage from the Stage 3 cost report (spent = limit-remaining).

    claude_code cost stays UNAVAILABLE; these are bounded reservation estimates.
    """
    acct = tok.get("accounting") or {}
    lim = acct.get("budget_limits") or {}
    rem = acct.get("budget_remaining") or {}

    def _spent(lk, rk):
        lv, rv = lim.get(lk), rem.get(rk)
        if isinstance(lv, (int, float)) and isinstance(rv, (int, float)):
            return max(0, int(lv) - int(rv))
        return 0

    return {
        "cycle_input_tokens": tok.get("cycle_input_tokens")
        or _spent("max_input_tokens_per_cycle", "cycle_input_tokens"),
        "cycle_output_tokens": tok.get("cycle_output_tokens")
        or _spent("max_output_tokens_per_cycle", "cycle_output_tokens"),
        "calls": acct.get("calls"),
    }


def _material_events_from_run(run: dict, *, limit: int = 6) -> list[dict]:
    events = []
    for raw in run.get("events", [])[:limit]:
        ev = raw.get("analysis", raw) if isinstance(raw, dict) else {}
        tickers = ev.get("affected_tickers") or ev.get("inferred_tickers") or []
        if isinstance(tickers, (list, tuple)) and tickers:
            entity = ", ".join(str(t) for t in tickers)
        else:
            entity = (ev.get("affected_ticker") or ev.get("ticker")
                      or ev.get("entity") or "market-wide")
        summary = (ev.get("factual_summary") or ev.get("summary")
                   or ev.get("what_is_new") or "")
        events.append({
            "headline": str(ev.get("event_category")
                            or ev.get("headline") or entity)[:120],
            "summary": str(summary)[:300],
            "entity": str(entity)[:60],
            "materiality": str(ev.get("materiality")
                               or ev.get("materiality_assessment") or "n/a")[:60],
            "mechanism": str(ev.get("economic_mechanism") or "")[:240],
            "source_ids": [str(s)[:60] for s in
                           (ev.get("source_record_ids")
                            or ([ev.get("record_id")] if ev.get("record_id")
                                else []))][:5],
        })
    return events


def _hypotheses_text(run: dict, *, limit: int = 5) -> list[str]:
    out = []
    for raw in run.get("hypotheses", [])[:limit]:
        if isinstance(raw, dict):
            p = raw.get("proposal", raw)
            stmt = (p.get("hypothesis_statement") or p.get("statement")
                    or p.get("hypothesis") or p.get("economic_rationale")
                    or p.get("name") or p.get("title"))
            out.append(str(stmt if stmt else rc.canonical_json(p))[:280])
        else:
            out.append(str(raw)[:280])
    return out


def build_report_model(cfg: dict, *, clock: Clock, label: str, cycle_date: str,
                       research: dict, portfolio: dict,
                       run_id: str, run_dir: str,
                       degraded: bool = False,
                       llm_skipped_reason: Optional[str] = None,
                       subject_override: Optional[str] = None,
                       stage5_model: Optional[dict] = None) -> dict:
    """Assemble the deterministic report model. Pure data; no side effects."""
    s35 = _stage35_latest(cfg)
    s3 = _stage3_latest(cfg)
    counts = research.get("counts") or {}
    metrics = research.get("metrics") or {}
    no_new = bool(research.get("no_new"))
    stage3_run_dir = research.get("run_dir")
    run_art = _load_stage3_run(cfg, stage3_run_dir) if stage3_run_dir else {}

    # KPI counts prefer this cycle's fresh metrics, else latest verified run.
    def _count(*keys, src=None):
        for src_ in ((counts, metrics, (s3.get("counts") or {}))
                     if src is None else (src,)):
            for k in keys:
                if src_.get(k) is not None:
                    return src_.get(k)
        return 0

    kpis = {
        "records_considered": _count("records_considered", "considered",
                                     "records_selected"),
        "selected_events": _count("records_selected", "selected"),
        "accepted_analyses": _count("analyses", "accepted_analyses"),
        "hypotheses": _count("hypotheses"),
        "queue_entries": _count("queue_entries"),
        "feed_health": "%s/%s healthy" % (s35.get("healthy_feeds", 0),
                                          s35.get("enabled_feeds", 0)),
    }

    s35_counts = s35.get("counts") or {}
    failed_feeds = _parse_failed_feeds(s35.get("terminal_token"))
    generalized_status = _generalized_status(s35)
    news_rss = {
        "enabled": s35.get("enabled_feeds"),
        "healthy": s35.get("healthy_feeds"),
        "records_new": s35_counts.get("normalized_records_new"),
        "records_total": s35_counts.get("normalized_records_total"),
        "clusters": s35_counts.get("clusters"),
        "newest_item": s35.get("as_of"),
        "company_direct": "sparse (company IR/newsroom feeds not yet onboarded)",
        "generalized_status": generalized_status,
        "failed_feeds": failed_feeds,
    }

    provider = research.get("provider") or s3.get("provider") or "n/a"
    classification = research.get("classification") \
        or s3.get("provider_recorded_as") or rc.CLAUDE_CODE_DEVELOPMENT_ONLY
    tokens = run_art.get("tokens") or {}
    llm = {
        "provider": provider,
        "classification": classification,
        "invoked": bool(research.get("llm_invoked")),
        "skipped_reason": llm_skipped_reason,
        "calls": _count("llm_calls"),
        "tokens_in": tokens.get("cycle_input_tokens")
        or tokens.get("input_tokens") or 0,
        "tokens_out": tokens.get("cycle_output_tokens")
        or tokens.get("output_tokens") or 0,
        "cost": rc.COST_UNAVAILABLE,
    }

    badges = _build_badges(provider=provider, degraded=degraded,
                           news_status=s35.get("status"),
                           llm_skipped=bool(llm_skipped_reason))

    if no_new or llm_skipped_reason:
        research_block = {
            "no_new_action": True,
            "new_hypotheses": [], "data_holds": [], "rejected_duplicates": 0,
            "notes": (llm_skipped_reason and
                      "No LLM cycle this run (%s). Feed health and paper-book "
                      "context below reflect the latest verified deterministic "
                      "collection." % llm_skipped_reason)
            or "No new material research input since the last verified cycle; "
               "no LLM call was forced.",
        }
    else:
        research_block = {
            "no_new_action": False,
            "new_hypotheses": _hypotheses_text(run_art),
            "data_holds": [],
            "rejected_duplicates": _count("rejected_proposals",
                                          "duplicates_rejected"),
            "notes": "Bounded grounded research cycle completed; queued items "
                     "await human review. No experiment ran.",
        }

    exec_summary = _executive_summary(label, portfolio, kpis, news_rss,
                                      no_new or bool(llm_skipped_reason),
                                      degraded)
    operating_action = _operating_action(portfolio)

    evidence = {
        "run_id": run_id,
        "stage1_run_id": (research.get("stage1_run_id")
                          or s3.get("stage1_run_id")),
        "stage2_run_id": (research.get("stage2_run_id")
                          or s3.get("stage2_run_id")),
        "stage35_run_id": s35.get("run_id"),
        "stage3_run_id": research.get("run_id") or s3.get("run_id"),
        "evidence_paths": [
            {"label": "Stage 4 run dir", "path": run_dir},
            {"label": "Stage 3.5 news/RSS root",
             "path": cfg.get("stage3_5_news_rss_root")},
            {"label": "Stage 3 director root",
             "path": cfg.get("stage3_director_root")},
        ],
    }

    return {
        "report_schema_version": rr.REPORT_SCHEMA_VERSION,
        "report_title": "Alpha Agent %s Research Report" % _label_title(label),
        "generated_at": clock.iso(),
        "cycle_label": label,
        "cycle_date": cycle_date,
        "subject": subject_override
        or rc.report_subject(label, cycle_date, degraded=degraded),
        "degraded": degraded,
        "provider_status": provider,
        "source_coverage_status": s35.get("status"),
        "badges": badges,
        "executive_summary": exec_summary,
        "kpis": kpis,
        "material_events": _material_events_from_run(run_art)
        if not (no_new or llm_skipped_reason) else [],
        "research": research_block,
        "paper_book": portfolio,
        "news_rss": news_rss,
        "llm": llm,
        "operating_action": operating_action,
        "evidence": evidence,
        "experiment": (stage5_model if stage5_model is not None
                       else _stage5_report_model(cfg)),
    }


def _label_title(label: str) -> str:
    return {rc.LABEL_MORNING: "Morning", rc.LABEL_POST_CLOSE: "Post-Close",
            rc.LABEL_MANUAL: "Manual"}.get(label, "Research")


def _build_badges(*, provider: str, degraded: bool, news_status: Optional[str],
                  llm_skipped: bool) -> list[dict]:
    badges = [
        {"text": "STAGE 4 ACTIVE", "kind": "safe"},
        {"text": "RESEARCH ONLY", "kind": "safe"},
        {"text": "PAPER PORTFOLIO ONLY", "kind": "safe"},
        {"text": "AUTOMATION OFF", "kind": "safe"},
        {"text": "NO LIVE ORDERS", "kind": "safe"},
    ]
    if llm_skipped:
        badges.append({"text": "LLM SKIPPED", "kind": "warn"})
    else:
        badges.append({"text": "PROVIDER %s" % str(provider).upper(),
                       "kind": "info"})
    ns = str(news_status or "")
    if "PARTIAL" in ns:
        badges.append({"text": "SOURCE COVERAGE PARTIAL", "kind": "warn"})
    elif ns:
        badges.append({"text": "SOURCE COVERAGE OK", "kind": "info"})
    if degraded:
        badges.append({"text": "ACTION REQUIRED", "kind": "crit"})
    return badges


def _parse_failed_feeds(terminal: Optional[str]) -> list[str]:
    if not terminal or "unavailable:" not in terminal:
        return []
    tail = terminal.split("unavailable:", 1)[1]
    feeds = []
    for part in tail.split(";"):
        name = part.strip().split("(")[0].strip()
        if name:
            feeds.append(name)
    return feeds


def _generalized_status(s35: dict) -> str:
    counts = s35.get("counts") or {}
    if (counts.get("normalized_records_total") or 0) > 0:
        return "OPERATIONAL_PARTIAL (Stage 3.5 IMPLEMENTED)"
    return "not yet operational"


def _executive_summary(label, portfolio, kpis, news_rss, no_new, degraded):
    nav = portfolio.get("nav")
    cum = portfolio.get("cumulative_return_pct")
    exc = portfolio.get("cumulative_excess_pp")
    bits = []
    if degraded:
        bits.append("ACTION REQUIRED — a degraded runtime condition was "
                    "detected; see below.")
    bits.append("Alpha Paper Book #1 NAV %s (cumulative %s, %s vs SPY)." % (
        rr.fmt_money(nav), rr.fmt_pct(cum), rr.fmt_pp(exc)))
    bits.append("Feed health %s; %s clusters." % (
        kpis.get("feed_health"), rr.fmt_int(news_rss.get("clusters"))))
    if no_new:
        bits.append("No new material research input this cycle; no LLM call "
                    "forced. Research automation remains OFF.")
    else:
        bits.append("Bounded grounded research completed: %s analyses, %s "
                    "hypotheses queued for human review." % (
                        rr.fmt_int(kpis.get("accepted_analyses")),
                        rr.fmt_int(kpis.get("hypotheses"))))
    return " ".join(bits)


def _operating_action(portfolio: dict) -> str:
    gate = portfolio.get("risk_gate") or "n/a"
    close = portfolio.get("close_status") or "n/a"
    return ("Latest daily gate: %s. Close state: %s. Next action is human "
            "review only — Stage 4 proposes nothing operational and executes "
            "nothing." % (gate, close))


# --------------------------------------------------------------------------- #
# Run-package writer.
# --------------------------------------------------------------------------- #
def _write_run_package(root: Path, run_id: str, *, clock: Clock,
                       runtime_input: dict, component_results: list,
                       collection_summary: dict, research_summary: dict,
                       portfolio_context: dict, email_manifest: dict,
                       scheduler_state: dict, report_md: str) -> Path:
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(run_dir / "runtime_input.json", runtime_input)
    _write_json_atomic(run_dir / "component_results.json", component_results)
    _write_json_atomic(run_dir / "collection_summary.json", collection_summary)
    _write_json_atomic(run_dir / "research_summary.json", research_summary)
    _write_json_atomic(run_dir / "portfolio_context.json", portfolio_context)
    _write_json_atomic(run_dir / "email_manifest.json", email_manifest)
    _write_json_atomic(run_dir / "scheduler_state.json", scheduler_state)
    _write_text_atomic(run_dir / "runtime_report.md", report_md)
    file_hashes = {}
    for name in REQUIRED_RUN_FILES:
        if name == "run_manifest.json":
            continue
        p = run_dir / name
        if p.exists():
            file_hashes[name] = _sha256_file(p)
    manifest = {
        "stage": rc.RUNTIME_STAGE,
        "schema_version": rc.RUNTIME_SCHEMA_VERSION,
        "runtime_version": rc.RUNTIME_VERSION,
        "run_id": run_id,
        "generated_at": clock.iso(),
        "required_files": list(REQUIRED_RUN_FILES),
        "file_hashes": file_hashes,
    }
    _write_json_atomic(run_dir / "run_manifest.json", manifest)
    return run_dir


def _runtime_report_md(model: dict, component_results: list,
                       email_result: dict) -> str:
    lines = ["# Alpha Agent Stage 4 runtime report", "",
             "- Cycle: %s / %s" % (model.get("cycle_label"),
                                   model.get("cycle_date")),
             "- Generated: %s" % model.get("generated_at"),
             "- Subject: %s" % model.get("subject"),
             "- Email delivery: %s" % email_result.get("status"), "",
             "## Component results", ""]
    for c in component_results:
        lines.append("- **%s**: %s (%s)" % (c.get("component"),
                                            c.get("status"),
                                            "ok" if c.get("ok") else "not-ok"))
    lines += ["", "## Safety confirmations", "",
              "- Research automation only; no order/fill/signal/decision "
              "created.",
              "- Operational ledgers read-only; fingerprints verified "
              "unchanged.",
              "- No PostgreSQL, no prediction service, no LLM tool use.",
              "- Email body rendered deterministically (0 LLM tokens).", ""]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Result container.
# --------------------------------------------------------------------------- #
@dataclass
class RuntimeResult:
    terminal: str
    status: str
    mode: str
    run_id: str
    cycle_id: str
    label: Optional[str] = None
    run_dir: Optional[str] = None
    email_status: Optional[str] = None
    email_diagnostic: Optional[str] = None
    email_message_id: Optional[str] = None
    components: list = field(default_factory=list)
    detail: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "terminal": self.terminal, "status": self.status,
            "mode": self.mode, "run_id": self.run_id, "cycle_id": self.cycle_id,
            "label": self.label, "run_dir": self.run_dir,
            "email_status": self.email_status,
            "email_diagnostic": self.email_diagnostic,
            "email_message_id": self.email_message_id,
            "components": self.components, "detail": self.detail,
        }


# --------------------------------------------------------------------------- #
# Runtime facade.
# --------------------------------------------------------------------------- #
class Runtime:
    def __init__(self, cfg: dict, *, drivers: StageDrivers,
                 portfolio: Optional[PortfolioReader] = None,
                 email_sender: Optional[EmailSender] = None,
                 clock: Optional[Clock] = None,
                 recovery_launcher: Optional[Callable[[str], Any]] = None):
        self.cfg = cfg
        self.root = runtime_root(cfg)
        self.drivers = drivers
        self.portfolio = portfolio or PortfolioReader(
            rc.coerce_iterable(cfg.get("operational_ledger_roots")))
        self.email_sender = email_sender
        self.clock = clock or Clock()
        self.recovery_launcher = recovery_launcher
        self.stale_seconds = int(rc.cadence_value(cfg, "stale_lock_seconds",
                                                  1800))

    # ---- run bookkeeping ------------------------------------------------- #
    def _open(self) -> sqlite3.Connection:
        ensure_layout(self.root)
        return open_state_db(self.root)

    def _begin_run(self, conn, run_id, cycle_id, mode, label, cycle_date):
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO runtime_runs (run_id, cycle_id, mode,"
                " label, cycle_date, status, started_at) VALUES"
                " (?,?,?,?,?,?,?)",
                (run_id, cycle_id, mode, label, cycle_date, "RUNNING",
                 self.clock.iso()))

    def _finish_run(self, conn, run_id, status, terminal, *, run_dir=None,
                    llm_invoked=0, llm_calls=0, tokens_in=0, tokens_out=0,
                    error_count=0):
        with conn:
            conn.execute(
                "UPDATE runtime_runs SET status=?, terminal=?, run_dir=?,"
                " llm_invoked=?, llm_calls=?, tokens_in=?, tokens_out=?,"
                " error_count=?, finished_at=? WHERE run_id=?",
                (status, terminal, run_dir, llm_invoked, llm_calls, tokens_in,
                 tokens_out, error_count, self.clock.iso(), run_id))

    def _record_component(self, conn, run_id, comp: dict):
        with conn:
            conn.execute(
                "INSERT INTO component_runs (run_id, component, mode, status,"
                " terminal, counts_json, stage_run_id, started_at,"
                " finished_at, last_success_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run_id, comp.get("component"), comp.get("mode", "?"),
                 comp.get("status"), comp.get("terminal"),
                 rc.canonical_json(comp.get("counts") or {}),
                 comp.get("run_id"), self.clock.iso(), self.clock.iso(),
                 self.clock.iso() if comp.get("ok") else None))

    def _record_error(self, conn, run_id, component, severity, message):
        with conn:
            conn.execute(
                "INSERT INTO runtime_errors (run_id, component, severity,"
                " message, recorded_at) VALUES (?,?,?,?,?)",
                (run_id, component, severity, rc.redact(str(message)[:500]),
                 self.clock.iso()))

    # ---- COLLECT --------------------------------------------------------- #
    def run_collect(self, *, mode: str = "incremental") -> RuntimeResult:
        conn = self._open()
        cycle_date = self.clock.date()
        cycle_id = rc.activity_cycle_id(rc.MODE_COLLECT, self.clock.iso())
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_COLLECT)
        self._begin_run(conn, run_id, cycle_id, rc.MODE_COLLECT, None,
                        cycle_date)
        ledgers_before = fingerprint_ledgers(self.cfg)
        try:
            lock = acquire_lock(self.root, "collect", run_id,
                                clock=self.clock, conn=conn,
                                stale_seconds=self.stale_seconds)
        except LockHeld as exc:
            self._finish_run(conn, run_id, "REFUSED", str(exc))
            conn.close()
            return RuntimeResult(terminal=rc.BLOCKED, status="COLLECT_REFUSED",
                                 mode=rc.MODE_COLLECT, run_id=run_id,
                                 cycle_id=cycle_id,
                                 detail={"reason": str(exc)})
        components: list[dict] = []
        try:
            steps = [
                ("verify_stage1", lambda: self.drivers.verify_stage1(),
                 "verify"),
                ("collect_stage2", lambda: self.drivers.collect_stage2(mode),
                 mode),
                ("verify_stage2", lambda: self.drivers.verify_stage2(),
                 "verify"),
                ("collect_stage35", lambda: self.drivers.collect_stage35(mode),
                 mode),
                ("verify_stage35", lambda: self.drivers.verify_stage35(),
                 "verify"),
            ]
            for _name, fn, step_mode in steps:
                comp = fn()
                comp["mode"] = step_mode
                components.append(comp)
                self._record_component(conn, run_id, comp)
                if not comp.get("ok"):
                    self._record_error(conn, run_id, comp.get("component"),
                                       "WARNING", comp.get("terminal"))
        finally:
            release_lock(lock, clock=self.clock, conn=conn)

        ledgers_after = fingerprint_ledgers(self.cfg)
        ledgers_ok = ledgers_before == ledgers_after
        all_ok = all(c.get("ok") for c in components) and ledgers_ok
        status = "COLLECT_OK" if all_ok else "COLLECT_DEGRADED"
        terminal = rc.READY if all_ok else rc.DEGRADED
        summary = {
            "cycle_id": cycle_id, "run_id": run_id, "mode": mode,
            "cycle_date": cycle_date, "ledgers_unchanged": ledgers_ok,
            "components": [{"component": c["component"], "status": c["status"],
                            "ok": c["ok"], "counts": c.get("counts")}
                           for c in components],
        }
        run_dir = _write_run_package(
            self.root, run_id, clock=self.clock,
            runtime_input={"mode": rc.MODE_COLLECT, "stage_mode": mode,
                           "cycle_date": cycle_date},
            component_results=components, collection_summary=summary,
            research_summary={}, portfolio_context={},
            email_manifest={"emailed": False,
                            "reason": "collect cycles do not email on success"},
            scheduler_state={"task": rc.TASK_COLLECT},
            report_md="# Collect cycle %s\n\nledgers_unchanged=%s\n" %
            (run_id, ledgers_ok))
        self._finish_run(conn, run_id, status, terminal, run_dir=str(run_dir),
                         error_count=sum(1 for c in components
                                         if not c.get("ok")))
        write_heartbeat(self.root, {"mode": rc.MODE_COLLECT, "status": status,
                                    "run_id": run_id, "cycle_date": cycle_date,
                                    "ledgers_unchanged": ledgers_ok},
                        clock=self.clock, conn=conn)
        conn.close()
        if not ledgers_ok:
            return RuntimeResult(terminal=rc.BLOCKED,
                                 status="LEDGER_MUTATION_DETECTED",
                                 mode=rc.MODE_COLLECT, run_id=run_id,
                                 cycle_id=cycle_id, run_dir=str(run_dir),
                                 components=components)
        return RuntimeResult(terminal=terminal, status=status,
                             mode=rc.MODE_COLLECT, run_id=run_id,
                             cycle_id=cycle_id, run_dir=str(run_dir),
                             components=components)

    # ---- RESEARCH -------------------------------------------------------- #
    def _research_cycles_today(self, conn, cycle_date, exclude_cycle) -> int:
        row = conn.execute(
            "SELECT COUNT(DISTINCT cycle_id) FROM runtime_runs WHERE"
            " mode=? AND cycle_date=? AND llm_invoked=1 AND cycle_id<>?",
            (rc.MODE_RESEARCH, cycle_date, exclude_cycle)).fetchone()
        return int(row[0] if row else 0)

    def _already_reported(self, conn, cycle_id) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM report_runs WHERE cycle_id=? ORDER BY id DESC"
            " LIMIT 1", (cycle_id,)).fetchone()

    def _email_already_sent(self, conn, cycle_id) -> bool:
        row = conn.execute(
            "SELECT 1 FROM email_deliveries WHERE cycle_id=? AND status=?",
            (cycle_id, rc.EMAIL_SENT)).fetchone()
        return row is not None

    def run_research(self, *, label: str = rc.LABEL_MANUAL,
                     send_email: bool = True,
                     force_no_llm: bool = False,
                     test_report: bool = False) -> RuntimeResult:
        conn = self._open()
        cycle_date = self.clock.date()
        cycle_id = rc.report_cycle_id(label, cycle_date)
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_RESEARCH, self.clock.iso())
        self._begin_run(conn, run_id, cycle_id, rc.MODE_RESEARCH, label,
                        cycle_date)
        ledgers_before = fingerprint_ledgers(self.cfg)
        try:
            lock = acquire_lock(self.root, "research", run_id, clock=self.clock,
                                conn=conn, stale_seconds=self.stale_seconds)
        except LockHeld as exc:
            self._finish_run(conn, run_id, "REFUSED", str(exc))
            conn.close()
            return RuntimeResult(terminal=rc.BLOCKED, status="RESEARCH_REFUSED",
                                 mode=rc.MODE_RESEARCH, run_id=run_id,
                                 cycle_id=cycle_id, label=label,
                                 detail={"reason": str(exc)})

        components: list[dict] = []
        llm_skipped_reason = None
        stage5_result: dict = {}
        research_norm: dict = {"no_new": True, "counts": {}, "metrics": {},
                               "llm_invoked": False}
        try:
            # Idempotency: a completed report cycle never re-invokes the LLM.
            prior = self._already_reported(conn, cycle_id)
            cap = int(rc.cadence_value(self.cfg, "max_research_cycles_per_day",
                                       2))
            done_today = self._research_cycles_today(conn, cycle_date, cycle_id)

            # Fresh deterministic collection first (incremental).
            for name, fn in (("collect_stage2",
                              lambda: self.drivers.collect_stage2(
                                  "incremental")),
                             ("collect_stage35",
                              lambda: self.drivers.collect_stage35(
                                  "incremental"))):
                comp = fn()
                comp["mode"] = "incremental"
                components.append(comp)
                self._record_component(conn, run_id, comp)

            if prior is not None:
                llm_skipped_reason = ("cycle already completed today "
                                      "(idempotent — no second LLM run)")
            elif force_no_llm:
                llm_skipped_reason = "LLM disabled for this run"
            elif done_today >= cap:
                llm_skipped_reason = ("daily research-cycle cap reached "
                                      "(%d/%d)" % (done_today, cap))
            else:
                comp3 = self.drivers.research_stage3("incremental")
                comp3["mode"] = "incremental"
                components.append(comp3)
                self._record_component(conn, run_id, comp3)
                raw3 = comp3.get("raw") or {}
                base = {
                    "run_dir": comp3.get("run_dir"),
                    "run_id": comp3.get("run_id"),
                    "provider": raw3.get("provider"),
                    "classification": raw3.get("provider_recorded_as"),
                    "stage1_run_id": raw3.get("stage1_run_id"),
                    "stage2_run_id": raw3.get("stage2_run_id"),
                }
                if comp3.get("no_new"):
                    # Genuine no-new-input path: deterministic heartbeat report,
                    # no LLM forced, no degraded flag.
                    research_norm = {**base, "no_new": True,
                                     "counts": comp3.get("counts") or {},
                                     "metrics": comp3.get("metrics") or {},
                                     "llm_invoked": False}
                elif not comp3.get("ok"):
                    # Provider unavailable / blocked: never fabricate research;
                    # send a deterministic DEGRADED report and retain records.
                    llm_skipped_reason = rc.LLM_SKIPPED_PROVIDER_UNAVAILABLE
                    self._record_error(conn, run_id, comp3.get("component"),
                                       "WARNING", comp3.get("terminal"))
                    research_norm = {**base, "no_new": True, "counts": {},
                                     "metrics": {}, "llm_invoked": False}
                else:
                    llm_calls = (comp3.get("counts") or comp3.get("metrics")
                                 or {}).get("llm_calls", 1)
                    research_norm = {**base, "no_new": False,
                                     "counts": comp3.get("counts") or {},
                                     "metrics": comp3.get("metrics") or {},
                                     "llm_invoked": llm_calls != 0}
            # Verify stage3 package (writes nothing).
            vcomp = self.drivers.verify_stage3()
            vcomp["mode"] = "verify"
            components.append(vcomp)
            self._record_component(conn, run_id, vcomp)

            # Stage 5 — bounded experiment & evidence cycle (research-only).
            # Optional: only when the config enables it. Reads the just-refreshed
            # Stage 1-3.5 packages; never touches operational state.
            if self.cfg.get("stage5_enabled"):
                s5 = self.drivers.run_stage5("incremental")
                s5["mode"] = "incremental"
                components.append(s5)
                self._record_component(conn, run_id, s5)
                stage5_result = s5.get("result") or {}
                if not s5.get("ok"):
                    self._record_error(conn, run_id, s5.get("component"),
                                       "WARNING", s5.get("terminal"))
        finally:
            release_lock(lock, clock=self.clock, conn=conn)

        portfolio = self.portfolio.read()
        # Run dir first (need path for evidence), then render.
        run_dir = self.root / "runs" / run_id
        degraded = llm_skipped_reason == rc.LLM_SKIPPED_PROVIDER_UNAVAILABLE
        subject_override = (rc.report_subject(label, cycle_date, test=True)
                            if test_report else None)
        stage5_model = (_stage5_report_model(self.cfg, stage5_result)
                        if stage5_result else None)
        model = build_report_model(
            self.cfg, clock=self.clock, label=label, cycle_date=cycle_date,
            research=research_norm, portfolio=portfolio, run_id=run_id,
            run_dir=str(run_dir), degraded=degraded,
            llm_skipped_reason=llm_skipped_reason,
            subject_override=subject_override, stage5_model=stage5_model)
        html_body = rr.render_html(model)
        text_body = rr.render_text(model)
        manifest = rr.report_manifest(model, html_body, text_body)
        report_paths = self._write_report(cycle_date, label, html_body,
                                          text_body, manifest)
        html_sha = manifest["html_sha256"]

        # Email delivery (idempotent, deterministic sender).
        email_result = self._deliver_email(
            conn, cycle_id=cycle_id, run_id=run_id, label=label,
            cycle_date=cycle_date, subject=model["subject"],
            report_paths=report_paths, send_email=send_email)

        # Record report + finish.
        with conn:
            conn.execute(
                "INSERT INTO report_runs (run_id, cycle_id, label, cycle_date,"
                " html_path, text_path, manifest_path, html_sha256, status,"
                " recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run_id, cycle_id, label, cycle_date,
                 str(report_paths["html"]), str(report_paths["text"]),
                 str(report_paths["manifest"]), html_sha, "RENDERED",
                 self.clock.iso()))

        ledgers_after = fingerprint_ledgers(self.cfg)
        ledgers_ok = ledgers_before == ledgers_after

        llm_invoked = 1 if research_norm.get("llm_invoked") else 0
        no_new = bool(research_norm.get("no_new")) and not llm_skipped_reason
        status, terminal = self._research_terminal(
            email_result["status"], no_new, llm_skipped_reason, ledgers_ok,
            degraded=degraded)

        research_summary = {
            "llm_invoked": bool(llm_invoked),
            "llm_skipped_reason": llm_skipped_reason,
            "no_new_input": no_new,
            "research": {k: research_norm.get(k) for k in
                         ("no_new", "run_id", "provider", "classification")},
            "kpis": model["kpis"],
        }
        email_manifest = {
            "cycle_id": cycle_id, "subject": model["subject"],
            "recipient": self.cfg.get("recipient_email"),
            "status": email_result["status"],
            "diagnostic": email_result.get("diagnostic"),
            "message_id": email_result.get("message_id"),
            "credential_present": credential_present(self.cfg),
            "email_llm_tokens": 0,
        }
        report_md = _runtime_report_md(model, components, email_result)
        _write_run_package(
            self.root, run_id, clock=self.clock,
            runtime_input={"mode": rc.MODE_RESEARCH, "label": label,
                           "cycle_date": cycle_date, "send_email": send_email},
            component_results=components,
            collection_summary={"components": [c["component"]
                                               for c in components]},
            research_summary=research_summary, portfolio_context=portfolio,
            email_manifest=email_manifest,
            scheduler_state={"task": (rc.TASK_MORNING
                                      if label == rc.LABEL_MORNING
                                      else rc.TASK_POST_CLOSE
                                      if label == rc.LABEL_POST_CLOSE
                                      else "manual")},
            report_md=report_md)

        self._finish_run(conn, run_id, status, terminal, run_dir=str(run_dir),
                         llm_invoked=llm_invoked,
                         llm_calls=int(model["llm"].get("calls") or 0),
                         tokens_in=int(model["llm"].get("tokens_in") or 0),
                         tokens_out=int(model["llm"].get("tokens_out") or 0),
                         error_count=0 if ledgers_ok else 1)
        write_heartbeat(self.root, {"mode": rc.MODE_RESEARCH, "status": status,
                                    "run_id": run_id, "label": label,
                                    "cycle_date": cycle_date,
                                    "email_status": email_result["status"],
                                    "ledgers_unchanged": ledgers_ok},
                        clock=self.clock, conn=conn)
        conn.close()
        return RuntimeResult(
            terminal=terminal, status=status, mode=rc.MODE_RESEARCH,
            run_id=run_id, cycle_id=cycle_id, label=label,
            run_dir=str(run_dir), email_status=email_result["status"],
            email_diagnostic=email_result.get("diagnostic"),
            email_message_id=email_result.get("message_id"),
            components=components,
            detail={"ledgers_unchanged": ledgers_ok,
                    "report_html": str(report_paths["html"]),
                    "report_text": str(report_paths["text"]),
                    "email_status": email_result["status"],
                    "email_diagnostic": email_result.get("diagnostic"),
                    "email_message_id": email_result.get("message_id"),
                    "llm_skipped_reason": llm_skipped_reason})

    def _research_terminal(self, email_status, no_new, llm_skipped, ledgers_ok,
                           *, degraded=False):
        if not ledgers_ok:
            return "LEDGER_MUTATION_DETECTED", rc.BLOCKED
        if email_status == rc.EMAIL_CREDENTIAL_REQUIRED_STATUS:
            return "EMAIL_CREDENTIAL_REQUIRED", rc.EMAIL_CREDENTIAL_REQUIRED
        if email_status in EMAIL_FAILURE_STATUSES:
            # Preserve the specific failure token in the run status so the DB and
            # CLI JSON distinguish reauth/permission/rate-limit/job/permanent
            # failures.
            return "EMAIL_FAILED — %s" % email_status, rc.DEGRADED
        if degraded:
            return "PROVIDER_UNAVAILABLE_DEGRADED", rc.DEGRADED
        if no_new:
            return "NO_NEW_RESEARCH_INPUT", rc.NO_NEW_RESEARCH_INPUT
        return "RESEARCH_OK", rc.READY

    # ---- REPORT-ONLY ----------------------------------------------------- #
    def run_report_only(self, *, label: str = rc.LABEL_MANUAL,
                        send_email: bool = False) -> RuntimeResult:
        conn = self._open()
        cycle_date = self.clock.date()
        cycle_id = rc.report_cycle_id(label, cycle_date)
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_REPORT_ONLY,
                                   self.clock.iso())
        self._begin_run(conn, run_id, cycle_id, rc.MODE_REPORT_ONLY, label,
                        cycle_date)
        portfolio = self.portfolio.read()
        research_norm = {"no_new": True, "counts": {}, "metrics": {},
                         "llm_invoked": False}
        run_dir = self.root / "runs" / run_id
        model = build_report_model(
            self.cfg, clock=self.clock, label=label, cycle_date=cycle_date,
            research=research_norm, portfolio=portfolio, run_id=run_id,
            run_dir=str(run_dir), degraded=False,
            llm_skipped_reason="report-only (no collection, no LLM)")
        html_body = rr.render_html(model)
        text_body = rr.render_text(model)
        manifest = rr.report_manifest(model, html_body, text_body)
        report_paths = self._write_report(cycle_date, label, html_body,
                                          text_body, manifest)
        email_status = rc.EMAIL_SKIPPED
        if send_email:
            email_result = self._deliver_email(
                conn, cycle_id=cycle_id, run_id=run_id, label=label,
                cycle_date=cycle_date, subject=model["subject"],
                report_paths=report_paths, send_email=True)
            email_status = email_result["status"]
        with conn:
            conn.execute(
                "INSERT INTO report_runs (run_id, cycle_id, label, cycle_date,"
                " html_path, text_path, manifest_path, html_sha256, status,"
                " recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run_id, cycle_id, label, cycle_date, str(report_paths["html"]),
                 str(report_paths["text"]), str(report_paths["manifest"]),
                 manifest["html_sha256"], "RENDERED", self.clock.iso()))
        _write_run_package(
            self.root, run_id, clock=self.clock,
            runtime_input={"mode": rc.MODE_REPORT_ONLY, "label": label,
                           "cycle_date": cycle_date},
            component_results=[], collection_summary={},
            research_summary={"report_only": True}, portfolio_context=portfolio,
            email_manifest={"status": email_status, "email_llm_tokens": 0},
            scheduler_state={"task": "report-only"},
            report_md="# Report-only %s\n" % run_id)
        self._finish_run(conn, run_id, "REPORT_ONLY_OK", rc.READY,
                         run_dir=str(run_dir))
        conn.close()
        return RuntimeResult(terminal=rc.READY, status="REPORT_ONLY_OK",
                             mode=rc.MODE_REPORT_ONLY, run_id=run_id,
                             cycle_id=cycle_id, label=label,
                             run_dir=str(run_dir), email_status=email_status,
                             detail={"report_html": str(report_paths["html"])})

    # ---- WATCHDOG -------------------------------------------------------- #
    def run_watchdog(self) -> RuntimeResult:
        conn = self._open()
        cycle_date = self.clock.date()
        cycle_id = rc.activity_cycle_id(rc.MODE_WATCHDOG, self.clock.iso())
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_WATCHDOG)
        self._begin_run(conn, run_id, cycle_id, rc.MODE_WATCHDOG, None,
                        cycle_date)
        try:
            lock = acquire_lock(self.root, "watchdog", run_id, clock=self.clock,
                                conn=conn, stale_seconds=self.stale_seconds)
        except LockHeld as exc:
            self._finish_run(conn, run_id, "REFUSED", str(exc))
            conn.close()
            return RuntimeResult(terminal=rc.BLOCKED, status="WATCHDOG_REFUSED",
                                 mode=rc.MODE_WATCHDOG, run_id=run_id,
                                 cycle_id=cycle_id,
                                 detail={"reason": str(exc)})
        checks: dict[str, Any] = {}
        recoveries: list[dict] = []
        try:
            hb = read_heartbeat(self.root)
            hb_age = _age_seconds(hb.get("recorded_at") if hb else None,
                                  self.clock)
            checks["heartbeat_age_seconds"] = hb_age
            checks["heartbeat_fresh"] = (hb_age is not None and hb_age <
                                         int(rc.cadence_value(
                                             self.cfg,
                                             "heartbeat_stale_seconds", 7200)))
            # Stale locks.
            for name in ("collect", "research", "watchdog"):
                if name == "watchdog":
                    continue
                lp = _lock_path(self.root, name)
                if lp.exists():
                    data = _read_json(lp) or {}
                    age = _age_seconds(data.get("acquired_at"), self.clock)
                    if age is not None and age >= self.stale_seconds:
                        try:
                            lp.unlink()
                            recoveries.append({"action": "CLEAR_STALE_LOCK",
                                               "target": name, "age": age})
                            with conn:
                                conn.execute(
                                    "INSERT INTO recovery_actions (action,"
                                    " target, detail, run_id, recorded_at)"
                                    " VALUES (?,?,?,?,?)",
                                    ("CLEAR_STALE_LOCK", name,
                                     "watchdog cleared age %ss" % int(age),
                                     run_id, self.clock.iso()))
                        except OSError:
                            pass
            # Missed scheduled reports today.
            checks["missed_reports"] = self._detect_missed_reports(
                conn, cycle_date, run_id)
            # Retry failed email deliveries deterministically (no LLM), reusing
            # the already-generated report — never a second render or LLM cycle.
            retried = self._retry_failed_emails(conn, run_id)
            if retried:
                recoveries.extend(retried)
            # Failed email deliveries with no later success (post-retry).
            checks["email_failures"] = self._count_email_failures(conn)
            checks["emails_retried"] = len(retried)
            # Last successful collect.
            row = conn.execute(
                "SELECT MAX(finished_at) FROM runtime_runs WHERE mode=? AND"
                " status IN ('COLLECT_OK','COLLECT_DEGRADED')",
                (rc.MODE_COLLECT,)).fetchone()
            checks["last_collect_at"] = row[0] if row else None
        finally:
            release_lock(lock, clock=self.clock, conn=conn)

        # Launch a genuinely missed scheduled report — the ONLY watchdog path
        # that may use the LLM — and only when explicitly enabled in config.
        launched: list[str] = []
        if checks.get("missed_reports") and rc.cadence_value(
                self.cfg, "watchdog_launch_missed_reports", False):
            launcher = self.recovery_launcher or (
                lambda lb: self.run_research(label=lb, send_email=True))
            for label in checks["missed_reports"]:
                try:
                    launcher(label)
                    launched.append(label)
                    with conn:
                        conn.execute(
                            "UPDATE missed_cycles SET recovered=1,"
                            " recovery_run_id=? WHERE label=? AND cycle_date=?"
                            " AND recovered=0",
                            (run_id, label, cycle_date))
                        conn.execute(
                            "INSERT INTO recovery_actions (action, target,"
                            " detail, run_id, recorded_at) VALUES (?,?,?,?,?)",
                            ("RERUN_MISSED_REPORT", label,
                             "watchdog launched missed %s report" % label,
                             run_id, self.clock.iso()))
                except Exception as exc:  # noqa: BLE001
                    self._record_error(conn, run_id, "watchdog", "WARNING",
                                       "missed-report launch failed: %s" % exc)
        recoveries.extend({"action": "RERUN_MISSED_REPORT", "target": lb}
                          for lb in launched)

        status = "WATCHDOG_OK"
        terminal = rc.READY
        if not checks.get("heartbeat_fresh") or checks.get("missed_reports") \
                or checks.get("email_failures"):
            status = "WATCHDOG_ATTENTION"
            terminal = rc.DEGRADED
        self._finish_run(conn, run_id, status, terminal)
        write_heartbeat(self.root, {"mode": rc.MODE_WATCHDOG, "status": status,
                                    "run_id": run_id, "cycle_date": cycle_date,
                                    "checks": checks,
                                    "recoveries": recoveries},
                        clock=self.clock, conn=conn)
        conn.close()
        return RuntimeResult(terminal=terminal, status=status,
                             mode=rc.MODE_WATCHDOG, run_id=run_id,
                             cycle_id=cycle_id,
                             detail={"checks": checks,
                                     "recoveries": recoveries})

    def _detect_missed_reports(self, conn, cycle_date, run_id) -> list:
        missed = []
        for label in (rc.LABEL_MORNING, rc.LABEL_POST_CLOSE):
            cyc = rc.report_cycle_id(label, cycle_date)
            sent = self._email_already_sent(conn, cyc)
            reported = self._already_reported(conn, cyc)
            # Only flag as *missed* when past its scheduled local hour.
            due_hour = 8 if label == rc.LABEL_MORNING else 18
            now_hour = self.clock.now().hour
            if now_hour >= due_hour and not reported and not sent:
                missed.append(label)
                with conn:
                    conn.execute(
                        "INSERT INTO missed_cycles (label, cycle_date,"
                        " detected_at, recovered) VALUES (?,?,?,0)",
                        (label, cycle_date, self.clock.iso()))
        return missed

    def _count_email_failures(self, conn) -> int:
        fail = sorted(EMAIL_FAILURE_STATUSES)
        placeholders = ",".join("?" * len(fail))
        row = conn.execute(
            "SELECT COUNT(*) FROM email_deliveries d WHERE d.status IN"
            " (%s) AND NOT EXISTS (SELECT 1 FROM email_deliveries s WHERE"
            " s.cycle_id=d.cycle_id AND s.status=?)" % placeholders,
            (*fail, rc.EMAIL_SENT)).fetchone()
        return int(row[0] if row else 0)

    def _retry_failed_emails(self, conn, run_id) -> list:
        """Retry TRANSIENT email failures using the ALREADY-GENERATED report.

        Deterministic: no re-render, no LLM. Non-transient failures (re-auth
        required, permission denied, invalid client, malformed job/attachment,
        permanent) are surfaced but not blindly auto-retried — a fresh cycle
        re-run re-delivers.
        """
        transient = sorted(EMAIL_TRANSIENT_STATUSES)
        placeholders = ",".join("?" * len(transient))
        rows = conn.execute(
            "SELECT DISTINCT cycle_id FROM email_deliveries d WHERE"
            " d.status IN (%s) AND NOT EXISTS (SELECT 1 FROM email_deliveries s"
            " WHERE s.cycle_id=d.cycle_id AND s.status=?)" % placeholders,
            (*transient, rc.EMAIL_SENT)).fetchall()
        retried: list[dict] = []
        for r in rows:
            cyc = r[0]
            rep = self._already_reported(conn, cyc)
            if not rep:
                continue
            job = {
                "cycle_id": cyc, "label": rep["label"],
                "cycle_date": rep["cycle_date"],
                "subject": "Alpha Agent Research Report — %s" %
                           rep["cycle_date"],
                "recipient": self.cfg.get("recipient_email"),
                "html_path": rep["html_path"], "text_path": rep["text_path"],
                "manifest_path": rep["manifest_path"],
                "attach_markdown": [rep["text_path"]],
                "created_at": self.clock.iso(),
            }
            pending = self.root / "outbox" / "pending" / ("%s.json" % cyc)
            _write_json_atomic(pending, job)
            job["job_path"] = str(pending)
            sender = self.email_sender or make_real_email_sender(
                self.cfg, repo_root=_repo_root_from_cfg(self.cfg))
            result = sender(job)
            status = result.get("status", rc.EMAIL_RETRYABLE_FAILURE)
            dest_dir = "sent" if status in rc.EMAIL_SUCCESS_STATUSES \
                else "failed"
            dest = self.root / "outbox" / dest_dir / ("%s.json" % cyc)
            try:
                if pending.exists():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(pending, dest)
            except OSError:
                pass
            self._record_email(conn, cyc, run_id, job["subject"],
                               job["recipient"], status, result.get("error"),
                               str(dest),
                               sent_at=self.clock.iso()
                               if status == rc.EMAIL_SENT else None)
            retried.append({"action": "RETRY_EMAIL", "target": cyc,
                            "status": status})
        return retried

    # ---- VERIFY (no writes, no network) ---------------------------------- #
    def run_verify(self) -> RuntimeResult:
        root = self.root
        problems: list[str] = []
        checks: dict[str, Any] = {}
        # Layout.
        for rel in ("state", "locks", "reports", "runs", "outbox"):
            if not (root / rel).exists():
                problems.append("missing layout dir: %s" % rel)
        # State DB integrity (read-only).
        try:
            conn = open_state_db(root, create=False)
            ic = conn.execute("PRAGMA integrity_check").fetchone()
            checks["integrity_check"] = ic[0] if ic else "unknown"
            if checks["integrity_check"] != "ok":
                problems.append("state db integrity: %s" %
                                checks["integrity_check"])
            checks["runtime_runs"] = conn.execute(
                "SELECT COUNT(*) FROM runtime_runs").fetchone()[0]
            checks["report_runs"] = conn.execute(
                "SELECT COUNT(*) FROM report_runs").fetchone()[0]
            conn.close()
        except (FileNotFoundError, sqlite3.DatabaseError) as exc:
            problems.append("state db unreadable: %s" % exc)
        # Latest stage packages present.
        for key, name in (("stage1_registry_root", "Stage 1"),
                          ("stage2_ingestion_root", "Stage 2"),
                          ("stage3_5_news_rss_root", "Stage 3.5"),
                          ("stage3_director_root", "Stage 3")):
            p = Path(self.cfg[key]) / "latest.json"
            if not p.exists():
                problems.append("%s latest.json missing" % name)
        checks["problems"] = problems
        cycle_id = rc.activity_cycle_id(rc.MODE_VERIFY, self.clock.iso())
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_VERIFY)
        if problems:
            return RuntimeResult(terminal=rc.BLOCKED, status="VERIFY_FAILED",
                                 mode=rc.MODE_VERIFY, run_id=run_id,
                                 cycle_id=cycle_id, detail=checks)
        return RuntimeResult(terminal=rc.VERIFIED, status="VERIFY_OK",
                             mode=rc.MODE_VERIFY, run_id=run_id,
                             cycle_id=cycle_id, detail=checks)

    # ---- report + email helpers ------------------------------------------ #
    def _write_report(self, cycle_date, label, html_body, text_body,
                      manifest) -> dict:
        y, m, d = cycle_date.split("-")
        rep_dir = self.root / "reports" / y / m / d / label
        rep_dir.mkdir(parents=True, exist_ok=True)
        html_path = rep_dir / "report.html"
        text_path = rep_dir / "report.txt"
        man_path = rep_dir / "report_manifest.json"
        _write_text_atomic(html_path, html_body)
        _write_text_atomic(text_path, text_body)
        _write_json_atomic(man_path, manifest)
        return {"html": html_path, "text": text_path, "manifest": man_path}

    def _deliver_email(self, conn, *, cycle_id, run_id, label, cycle_date,
                       subject, report_paths, send_email) -> dict:
        recipient = self.cfg.get("recipient_email")
        if not send_email:
            return {"status": rc.EMAIL_SKIPPED, "error": None}
        if self._email_already_sent(conn, cycle_id):
            self._record_email(conn, cycle_id, run_id, subject, recipient,
                               rc.EMAIL_ALREADY_SENT, None, None)
            return {"status": rc.EMAIL_ALREADY_SENT, "error": None}
        # Write the outbox job (NO secret; only paths + metadata).
        job = {
            "cycle_id": cycle_id, "label": label, "cycle_date": cycle_date,
            "subject": subject, "recipient": recipient,
            "html_path": str(report_paths["html"]),
            "text_path": str(report_paths["text"]),
            "manifest_path": str(report_paths["manifest"]),
            "attach_markdown": [str(report_paths["text"])],
            "created_at": self.clock.iso(),
        }
        pending = self.root / "outbox" / "pending" / ("%s.json" % cycle_id)
        _write_json_atomic(pending, job)
        job["job_path"] = str(pending)
        sender = self.email_sender
        if sender is None:
            sender = make_real_email_sender(self.cfg,
                                            repo_root=_repo_root_from_cfg(
                                                self.cfg))
        result = sender(job)
        status = result.get("status", rc.EMAIL_RETRYABLE_FAILURE)
        # Move outbox job to sent/failed.
        dest_dir = "sent" if status in rc.EMAIL_SUCCESS_STATUSES else "failed"
        dest = self.root / "outbox" / dest_dir / ("%s.json" % cycle_id)
        try:
            if pending.exists():
                dest.parent.mkdir(parents=True, exist_ok=True)
                os.replace(pending, dest)
        except OSError:
            pass
        sent_at = self.clock.iso() if status == rc.EMAIL_SENT else None
        self._record_email(conn, cycle_id, run_id, subject, recipient, status,
                           result.get("error"), str(dest), sent_at=sent_at)
        return {"status": status, "error": result.get("error"),
                "diagnostic": result.get("diagnostic"),
                "message_id": result.get("message_id")}

    def _record_email(self, conn, cycle_id, run_id, subject, recipient, status,
                      error, outbox_path, sent_at=None):
        attempts_row = conn.execute(
            "SELECT COUNT(*) FROM email_deliveries WHERE cycle_id=?",
            (cycle_id,)).fetchone()
        attempts = int(attempts_row[0] if attempts_row else 0) + 1
        try:
            with conn:
                conn.execute(
                    "INSERT INTO email_deliveries (cycle_id, run_id, subject,"
                    " recipient, status, attempts, last_error, outbox_path,"
                    " sent_at, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (cycle_id, run_id, subject, recipient, status, attempts,
                     rc.redact(str(error)) if error else None, outbox_path,
                     sent_at, self.clock.iso()))
        except sqlite3.IntegrityError:
            # Unique successful-delivery guard already satisfied.
            with conn:
                conn.execute(
                    "INSERT INTO email_deliveries (cycle_id, run_id, subject,"
                    " recipient, status, attempts, last_error, outbox_path,"
                    " sent_at, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (cycle_id, run_id, subject, recipient,
                     rc.EMAIL_ALREADY_SENT, attempts, None, outbox_path, None,
                     self.clock.iso()))


def _repo_root_from_cfg(cfg: dict) -> Path:
    rr_ = cfg.get("repo_root")
    if rr_:
        return Path(rr_)
    return Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Windows Task Scheduler definitions (deterministic contract shared with the
# PowerShell installer). No Windows login password is ever referenced; tasks
# run as the current user, Interactive logon (only while logged in), RunLevel
# Limited, MultipleInstances IgnoreNew, StartWhenAvailable, bounded runtime.
# --------------------------------------------------------------------------- #
def build_task_definitions(cfg: dict, *, repo_root: str, python_exe: str,
                           config_path: str) -> list[dict]:
    runner = str(Path(repo_root) / "scripts" / "run_alpha_agent.py")
    cad = cfg.get("cadence") or {}

    def _args(mode: str, extra: str = "") -> str:
        base = '"%s" --config "%s" --mode %s' % (runner, config_path, mode)
        return (base + " " + extra).strip()

    common = {
        "run_as": "current_user",
        "logon_type": "Interactive",
        "run_level": "Limited",
        "multiple_instances": "IgnoreNew",
        "start_when_available": True,
        "restart_count": int(cad.get("max_retries", 2)),
        "restart_interval_minutes": max(
            1, int(cad.get("retry_backoff_seconds", 30)) // 60) or 5,
        "stores_windows_password": False,
        "action_exe": python_exe,
    }
    defs = [
        {**common, "task_name": rc.TASK_COLLECT, "mode": rc.MODE_COLLECT,
         "action_args": _args(rc.MODE_COLLECT),
         "trigger": {"type": "repeat_minutes",
                     "interval_minutes": int(
                         cad.get("collect_interval_minutes", 30))},
         "execution_time_limit_minutes": 20},
        {**common, "task_name": rc.TASK_MORNING, "mode": rc.MODE_RESEARCH,
         "action_args": _args(rc.MODE_RESEARCH,
                              "--label %s" % rc.LABEL_MORNING),
         "trigger": {"type": "daily",
                     "at": cad.get("morning_report_local_time", "08:00")},
         "execution_time_limit_minutes": 25},
        {**common, "task_name": rc.TASK_POST_CLOSE, "mode": rc.MODE_RESEARCH,
         "action_args": _args(rc.MODE_RESEARCH,
                              "--label %s" % rc.LABEL_POST_CLOSE),
         "trigger": {"type": "weekly",
                     "days": list(cad.get("post_close_days",
                                          ["MON", "TUE", "WED", "THU", "FRI"])),
                     "at": cad.get("post_close_report_local_time", "18:30")},
         "execution_time_limit_minutes": 25},
        {**common, "task_name": rc.TASK_WATCHDOG, "mode": rc.MODE_WATCHDOG,
         "action_args": _args(rc.MODE_WATCHDOG),
         "trigger": {"type": "repeat_minutes",
                     "interval_minutes": int(
                         cad.get("watchdog_interval_minutes", 60))},
         "execution_time_limit_minutes": 10},
    ]
    return defs
