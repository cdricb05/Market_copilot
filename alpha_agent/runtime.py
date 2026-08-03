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
from . import historical_backfill as hb
from . import report_renderer as rr
from . import risk_overlay_research as ro_recovery
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

# WS7 — the exact subject line for the one-off executive test email. Defined
# here (not in runtime_contracts) so the Stage 4 contract schema is untouched.
EXEC_TEST_SUBJECT_PREFIX = "TEST — Alpha Agent Executive Research Brief"


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

        # Forward return series for the read-only risk-shadow tracker (WS5).
        # Each entry carries the active book's realized daily return and the
        # aligned SPY daily return; read-only, derived from the same marks.
        fseries: list[dict] = []
        prev_bc: Optional[float] = None
        for r in rows:
            row = r.get("row", {})
            dr = row.get("daily_return_pct")
            bc = row.get("benchmark_close")
            spy_ret = None
            try:
                if bc is not None and prev_bc:
                    spy_ret = float(bc) / float(prev_bc) - 1.0
            except (TypeError, ValueError, ZeroDivisionError):
                spy_ret = None
            book_ret = None
            try:
                if dr is not None:
                    book_ret = float(dr) / 100.0
            except (TypeError, ValueError):
                book_ret = None
            fseries.append({"date": row.get("date"), "book_return": book_ret,
                            "spy_return": spy_ret})
            if bc is not None:
                try:
                    prev_bc = float(bc)
                except (TypeError, ValueError):
                    pass
        pb["forward_series"] = fseries
        pb["forward_baseline_date"] = (rows[0].get("row", {}).get("date")
                                       if rows else None)
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
# Legacy Gmail-API OAuth delivery statuses (retained for the disabled-by-config
# forensic/reference transport only).
OAUTH_REAUTHORIZATION_REQUIRED = "OAUTH_REAUTHORIZATION_REQUIRED"
OAUTH_TOKEN_REFRESH_REJECTED = "OAUTH_TOKEN_REFRESH_REJECTED"
OAUTH_CLIENT_INVALID = "OAUTH_CLIENT_INVALID"
GMAIL_API_PERMISSION_DENIED = "GMAIL_API_PERMISSION_DENIED"
GMAIL_API_RATE_LIMITED = "GMAIL_API_RATE_LIMITED"
GMAIL_API_RETRYABLE_FAILURE = "GMAIL_API_RETRYABLE_FAILURE"
EMAIL_JOB_INVALID = "EMAIL_JOB_INVALID"
EMAIL_ATTACHMENT_INVALID = "EMAIL_ATTACHMENT_INVALID"

# --------------------------------------------------------------------------- #
# Gmail SMTP (App Password) — the PRIMARY and only active email transport.
# Statuses produced by scripts/send_alpha_agent_smtp.py.
# --------------------------------------------------------------------------- #
EMAIL_TRANSPORT_SMTP = "gmail_smtp"
EMAIL_TRANSPORT_OAUTH = "gmail_api_oauth"
EMAIL_SMTP_CREDENTIAL_MISSING = "EMAIL_SMTP_CREDENTIAL_MISSING"
EMAIL_SMTP_AUTHENTICATION_REJECTED = "EMAIL_SMTP_AUTHENTICATION_REJECTED"
EMAIL_SMTP_TLS_FAILED = "EMAIL_SMTP_TLS_FAILED"
EMAIL_SMTP_CONNECTION_FAILED = "EMAIL_SMTP_CONNECTION_FAILED"
EMAIL_SEND_FAILED = "EMAIL_SEND_FAILED"

# Transient failures worth an automatic watchdog retry: connection/DNS blips,
# generic SMTP send errors, and the legacy rate-limit/5xx/transport failures.
# Re-running the SAME request can succeed.
EMAIL_TRANSIENT_STATUSES = frozenset({
    EMAIL_SMTP_CONNECTION_FAILED, EMAIL_SEND_FAILED,
    GMAIL_API_RATE_LIMITED, GMAIL_API_RETRYABLE_FAILURE,
    rc.EMAIL_RETRYABLE_FAILURE})
# Non-transient failures: surfaced + DEGRADED but NOT blindly auto-retried, since
# retrying identically cannot fix a rejected App Password, a failed STARTTLS
# negotiation, or (legacy) a rejected refresh token / denied permission / invalid
# client / malformed job. A fresh cycle re-run may still re-deliver them. These
# live here so the watchdog does not loop on them.
EMAIL_NONRETRYABLE_STATUSES = frozenset({
    EMAIL_SMTP_AUTHENTICATION_REJECTED, EMAIL_SMTP_TLS_FAILED,
    OAUTH_REAUTHORIZATION_REQUIRED, OAUTH_TOKEN_REFRESH_REJECTED,
    OAUTH_CLIENT_INVALID, GMAIL_API_PERMISSION_DENIED,
    EMAIL_JOB_INVALID, EMAIL_ATTACHMENT_INVALID, rc.EMAIL_PERMANENT_FAILURE})
# Everything that counts as a delivery failure (drives DEGRADED + attention).
EMAIL_FAILURE_STATUSES = EMAIL_TRANSIENT_STATUSES | EMAIL_NONRETRYABLE_STATUSES
# A missing SMTP App Password is a "not configured" state (operator action
# required), handled like the legacy credential-required status — not a delivery
# failure that a watchdog should retry.
EMAIL_CREDENTIAL_REQUIRED_STATUSES = frozenset({
    rc.EMAIL_CREDENTIAL_REQUIRED_STATUS, EMAIL_SMTP_CREDENTIAL_MISSING})
# Full recognised token vocabulary, longest-first so a token that is a substring
# of another (none currently) can never shadow it during a fallback text scan.
_ALL_EMAIL_STATUSES = tuple(sorted(
    ({rc.EMAIL_SENT, rc.EMAIL_ALREADY_SENT}
     | EMAIL_CREDENTIAL_REQUIRED_STATUSES | EMAIL_FAILURE_STATUSES),
    key=len, reverse=True))


def credential_present(cfg: dict) -> bool:
    """True when the DPAPI OAuth refresh token has been configured (legacy)."""
    email_cfg = cfg.get("email") or {}
    cred_dir = email_cfg.get("credential_dir")
    if not cred_dir:
        return False
    token = Path(cred_dir) / (email_cfg.get("refresh_token_file")
                              or "gmail_oauth_refresh_token.dpapi")
    return token.exists()


def smtp_credential_present(cfg: dict) -> bool:
    """True when the DPAPI Gmail SMTP App Password has been configured."""
    email_cfg = cfg.get("email") or {}
    cred_dir = email_cfg.get("credential_dir")
    if not cred_dir:
        return False
    pw = Path(cred_dir) / (email_cfg.get("app_credential_file")
                           or "gmail_smtp_app_password.dpapi")
    return pw.exists()


def resolve_email_transport(cfg: dict) -> str:
    """Resolve the active email transport. Defaults to Gmail SMTP; only the
    explicit legacy value selects OAuth. There is NO automatic fallback."""
    email_cfg = cfg.get("email") or {}
    value = (email_cfg.get("transport") or cfg.get("email_transport")
             or EMAIL_TRANSPORT_SMTP)
    value = str(value).strip().lower()
    if value in (EMAIL_TRANSPORT_OAUTH, "oauth", "gmail_oauth", "gmail_api"):
        return EMAIL_TRANSPORT_OAUTH
    return EMAIL_TRANSPORT_SMTP


def make_real_email_sender(cfg: dict, *, repo_root: Path) -> EmailSender:
    """Return the active transport's sender. Gmail SMTP is primary and only
    active; OAuth is used ONLY when explicitly selected (no fallback between
    transports and never both in one cycle)."""
    if resolve_email_transport(cfg) == EMAIL_TRANSPORT_OAUTH:
        return _make_oauth_email_sender(cfg, repo_root=repo_root)
    return _make_smtp_email_sender(cfg, repo_root=repo_root)


def _make_smtp_email_sender(cfg: dict, *, repo_root: Path) -> EmailSender:
    email_cfg = cfg.get("email") or {}
    script = repo_root / "scripts" / "send_alpha_agent_smtp.ps1"
    cred_dir = str(email_cfg.get("credential_dir") or "")
    smtp_host = str(email_cfg.get("smtp_host") or "smtp.gmail.com")
    smtp_port = int(email_cfg.get("smtp_port", 587))
    timeout = int(email_cfg.get("send_timeout_seconds", 120))

    def _send(job: dict) -> dict:
        if not smtp_credential_present(cfg):
            return {"status": EMAIL_SMTP_CREDENTIAL_MISSING,
                    "error": "gmail smtp app password not configured",
                    "diagnostic": "Gmail SMTP App Password is not configured.",
                    "message_id": None, "transport": EMAIL_TRANSPORT_SMTP}
        cmd = ["powershell.exe", "-NoProfile", "-NonInteractive",
               "-ExecutionPolicy", "Bypass", "-File", str(script),
               "-JobPath", job["job_path"],
               "-SmtpHost", smtp_host,
               "-SmtpPort", str(smtp_port),
               "-TimeoutSeconds", str(timeout)]
        if cred_dir:
            cmd += ["-CredentialDir", cred_dir]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout + 60)
        except Exception as exc:  # noqa: BLE001
            return {"status": EMAIL_SMTP_CONNECTION_FAILED,
                    "error": rc.redact("%s: %s" % (type(exc).__name__, exc)),
                    "diagnostic": "The SMTP email wrapper could not be "
                                  "launched.",
                    "message_id": None, "transport": EMAIL_TRANSPORT_SMTP}
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        status, message_id, diagnostic = _parse_email_result(out)
        error = None if status in rc.EMAIL_SUCCESS_STATUSES else rc.redact(
            diagnostic or out.strip()[-400:])
        return {"status": status, "error": error,
                "diagnostic": diagnostic, "message_id": message_id,
                "transport": EMAIL_TRANSPORT_SMTP}

    return _send


def _make_oauth_email_sender(cfg: dict, *, repo_root: Path) -> EmailSender:
    """Legacy Gmail-API OAuth sender. Retained for forensic/reference use only;
    reachable solely when the email transport is explicitly set to OAuth."""
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
                    "message_id": None, "transport": EMAIL_TRANSPORT_OAUTH}
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
                    "message_id": None, "transport": EMAIL_TRANSPORT_OAUTH}
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        status, message_id, diagnostic = _parse_email_result(out)
        error = None if status in rc.EMAIL_SUCCESS_STATUSES else rc.redact(
            diagnostic or out.strip()[-400:])
        return {"status": status, "error": error,
                "diagnostic": diagnostic, "message_id": message_id,
                "transport": EMAIL_TRANSPORT_OAUTH}

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


def _stage6_report_model(cfg: dict,
                         fresh_result: Optional[dict] = None) -> Optional[dict]:
    """Deterministic Historical Data & Experiment Readiness model — from a fresh
    Stage 6 result if present, else the latest backfill package on disk, else
    None. All numbers originate in deterministic Python (no LLM). Read-only: the
    backfill itself is a bounded manual/resumable process, never run inside the
    scheduled research cycle."""
    model = (hb.backfill_report_model(fresh_result) if fresh_result
             else hb.latest_report_model(cfg))
    if model:
        _repair_stage6_window_universe(cfg, model)
    return model


def _repair_stage6_window_universe(cfg: dict, model: dict) -> None:
    """Backfill the Stage-6 window + universe that ``latest_report_model`` leaves
    empty (it reads ``date_start`` from the manifest, which lacks it, and never
    reads ``date_end`` or the universe). Sourced from the immutable
    ``stage6_input.json`` and the coverage families — never fabricated. This
    kills the '? .. ?' window and '0 / 0' universe defects (WS2)."""
    root = cfg.get("stage6_backfill_root") or cfg.get("backfill_root")
    fams = {f.get("family"): f for f in (model.get("families") or [])}
    prices = fams.get("prices") or {}
    memb = fams.get("universe_membership") or {}
    if root and model.get("run_id") and (not model.get("date_start")
                                         or not model.get("date_end")):
        run_dir = Path(root) / "runs" / model["run_id"]
        si = _read_json(run_dir / "stage6_input.json") or {}
        model["date_start"] = model.get("date_start") or si.get("date_start")
        model["date_end"] = model.get("date_end") or si.get("date_end")
    # Fall back to the price-coverage span when the input file is unavailable.
    model["date_start"] = model.get("date_start") or prices.get("date_min")
    model["date_end"] = model.get("date_end") or prices.get("date_max")
    # Universe: survivorship-free priced tickers / current index members.
    if not model.get("universe_full_size"):
        model["universe_full_size"] = prices.get("tickers")
    if not model.get("universe_size"):
        model["universe_size"] = memb.get("tickers") or prices.get("tickers")


def _stage7_report_model(cfg: dict,
                         fresh_model: Optional[dict] = None) -> Optional[dict]:
    """Deterministic Alpha Recovery model — from a fresh Stage 7 model if given,
    else the latest recovery package on disk, else None. All numbers originate in
    deterministic Python (no LLM). Read-only: the recovery run is a bounded
    manual/research process, never run inside the scheduled cycle."""
    root = cfg.get("stage7_recovery_root")
    if not root and fresh_model is None:
        return None
    try:
        return ro_recovery.latest_recovery_report_model(
            {"recovery_root": root}, fresh_model)
    except Exception:  # noqa: BLE001 — report is best-effort, never fatal
        return None


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


# --------------------------------------------------------------------------- #
# Canonical automatic-research-schedule state (Stage 7.2). Strictly READ-ONLY:
# probes the real Windows scheduled-task states via ``schtasks /query`` and NEVER
# creates, enables or changes any task. Any failure resolves to "Not verified" —
# never a false "On". This is the ONE source the email, API and UI share.
# --------------------------------------------------------------------------- #
def _parse_scheduled_task_state(text: str) -> Optional[str]:
    """Extract Enabled/Disabled from ``schtasks /query /v /fo LIST`` output."""
    for line in str(text or "").splitlines():
        low = line.lower()
        if "scheduled task state" in low and ":" in low:
            val = low.split(":", 1)[1].strip()
            if "disabled" in val:
                return "Disabled"
            if "enabled" in val:
                return "Enabled"
    return None


def read_scheduled_task_states(task_names,
                               *, runner: Optional[Callable] = None) -> dict:
    """Best-effort READ-ONLY map ``{task_name: 'Enabled'|'Disabled'|None}``. Never
    creates/enables/changes a task. ``runner`` (task_name -> raw schtasks text) is
    injectable for deterministic tests; the default shells to ``schtasks /query``
    with a short timeout and treats any error as ``None``."""
    states: dict[str, Optional[str]] = {}
    for name in (task_names or []):
        try:
            if runner is not None:
                raw = runner(name)
            else:
                cp = subprocess.run(
                    ["schtasks", "/query", "/tn", str(name), "/fo", "LIST",
                     "/v"], capture_output=True, text=True, timeout=15)
                raw = cp.stdout if cp.returncode == 0 else ""
            states[name] = _parse_scheduled_task_state(raw)
        except Exception:  # noqa: BLE001 — probing must never break a report
            states[name] = None
    return states


def resolve_schedule_status(cfg: dict,
                            *, runner: Optional[Callable] = None) -> str:
    """Canonical automatic-research-schedule status ('Off'/'On'/'Not verified')
    from the real states of the configured task names. Never hardcoded to On."""
    names = cfg.get("allowed_task_names") or []
    states = read_scheduled_task_states(names, runner=runner) if names else {}
    return rr.schedule_status(states)


def build_report_model(cfg: dict, *, clock: Clock, label: str, cycle_date: str,
                       research: dict, portfolio: dict,
                       run_id: str, run_dir: str,
                       degraded: bool = False,
                       llm_skipped_reason: Optional[str] = None,
                       subject_override: Optional[str] = None,
                       stage5_model: Optional[dict] = None,
                       stage6_model: Optional[dict] = None,
                       stage7_model: Optional[dict] = None,
                       prior: Optional[dict] = None,
                       attention: bool = False,
                       ledgers_unchanged: bool = True,
                       no_new_evidence: bool = False,
                       schedule_status: Optional[str] = None,
                       market_data_through: Optional[str] = None) -> dict:
    """Assemble the deterministic report model. Pure data; no side effects.

    ``schedule_status`` is the resolved automatic-research-schedule state (one of
    'Off'/'On'/'Not verified'); when omitted it defaults to 'Not verified' so the
    email never falsely claims the schedule is on."""
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

    # Resolve the three research sub-models once (fresh cycle result → latest
    # package on disk → None), then derive the executive presentation blocks.
    experiment_model = (stage5_model if stage5_model is not None
                        else _stage5_report_model(cfg))
    historical_model = (stage6_model if stage6_model is not None
                        else _stage6_report_model(cfg))
    recovery_model = (stage7_model if stage7_model is not None
                      else _stage7_report_model(cfg))

    sc = rr.scorecard(portfolio)
    shadows = ro_recovery.build_forward_shadows(
        {"recovery_root": cfg.get("stage7_recovery_root")},
        forward_series=portfolio.get("forward_series") or [],
        baseline_date=portfolio.get("forward_baseline_date"))
    hist_summary, hist_gap = _historical_readiness_summary(historical_model)
    research_decisions = _research_decisions_block(experiment_model,
                                                   recovery_model)
    # Stage 5 studies that COULD NOT RUN for lack of data — reported strictly
    # separately from the Stage 7 recovery ideas that were EVALUATED, never
    # merged into the evaluated count (Stage 7.2 defect #3 / #6).
    stage5_could_not_run = (experiment_model.get("data_gaps")
                            if experiment_model else None)

    shadow_ready = bool(recovery_model and (
        recovery_model.get("shadow_challenger_allowed_now")
        or int(recovery_model.get("campaign_keep_for_research") or 0) > 0))
    manual_review = _gate_triggered(portfolio.get("risk_gate"))
    healthy_feeds = news_rss.get("healthy")
    attention_flag = bool(attention or degraded or (not ledgers_unchanged)
                          or (healthy_feeds == 0
                              and (news_rss.get("enabled") or 0) > 0))
    source_agent_health = {
        "provider_ok": not degraded,
        "ledgers_unchanged": ledgers_unchanged,
        "stage7_age_note": _stage7_age_note(cfg, cycle_date, recovery_model),
    }

    return {
        "report_schema_version": rr.REPORT_SCHEMA_VERSION,
        "report_title": "Alpha Agent %s Executive Research Brief"
        % _label_title(label),
        "generated_at": clock.iso(),
        "cycle_label": label,
        "cycle_date": cycle_date,
        "subject": subject_override
        or rc.report_subject(label, cycle_date, degraded=degraded),
        "degraded": degraded,
        "attention": attention_flag,
        "shadow_ready": shadow_ready,
        "manual_review": manual_review,
        "no_new_evidence": no_new_evidence,
        "provider_status": provider,
        "source_coverage_status": s35.get("status"),
        "status_flags": list(rr.STATUS_FLAGS),
        "schedule_status": schedule_status or rr.SCHEDULE_UNVERIFIED,
        "market_data_through": (market_data_through
                                or portfolio.get("valuation_date")),
        "stage5_could_not_run": stage5_could_not_run,
        "badges": badges,
        "executive_summary": exec_summary,
        "scorecard": sc,
        "prior": prior,
        "kpis": kpis,
        "material_events": _material_events_from_run(run_art)
        if not (no_new or llm_skipped_reason) else [],
        "research": research_block,
        "research_decisions": research_decisions,
        "risk_and_shadow": shadows,
        "historical_readiness_summary": hist_summary,
        "historical_gap_note": hist_gap,
        "source_agent_health": source_agent_health,
        "paper_book": portfolio,
        "news_rss": news_rss,
        "llm": llm,
        "operating_action": operating_action,
        "evidence": evidence,
        "experiment": experiment_model,
        "historical_readiness": historical_model,
        "recovery_readiness": recovery_model,
    }


def _label_title(label: str) -> str:
    return {rc.LABEL_MORNING: "Morning", rc.LABEL_POST_CLOSE: "Post-Close",
            rc.LABEL_MANUAL: "Manual"}.get(label, "Research")


def _build_badges(*, provider: str, degraded: bool, news_status: Optional[str],
                  llm_skipped: bool) -> list[dict]:
    # The safety-flag row (resolved AUTOMATIC SCHEDULE state / TRADING AUTOMATION:
    # OFF / BROKER EXECUTION: OFF / PAPER ONLY) carries the automation state, so
    # no generic "AUTOMATION OFF" badge is shown here. Only the conditional
    # badges remain.
    badges = [
        {"text": "RESEARCH ONLY", "kind": "safe"},
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


# --------------------------------------------------------------------------- #
# Executive presentation helpers (deterministic, plain-English, no LLM).
# --------------------------------------------------------------------------- #
def _gate_triggered(risk_gate: Optional[str]) -> bool:
    """True only when the latest daily gate reports a non-zero triggered count —
    a HOLD with zero triggers never asks for a manual review."""
    import re
    m = re.search(r"([0-9]+)\s+triggered", str(risk_gate or ""))
    return bool(m and int(m.group(1)) > 0)


def _historical_readiness_summary(model: Optional[dict]):
    """WS6 plain-English readiness summary + one gap note. Empty when there is
    no Stage 6 package."""
    if not model:
        return [], None
    fams = {f.get("family"): f for f in (model.get("families") or [])}
    prices = fams.get("prices") or {}
    start = str(model.get("date_start") or prices.get("date_min") or "2015")
    end = str(model.get("date_end") or prices.get("date_max") or "latest")
    n = rr.fmt_int(model.get("universe_full_size") or prices.get("tickers"))
    price_ready = bool(prices.get("survivorship_safe")
                       or prices.get("record_count"))
    summary = [
        {"label": "Price history",
         "status": "READY" if price_ready else "NOT READY",
         "note": "%s through %s, %s tickers, survivorship-free."
                 % (start[:4], end, n)},
        {"label": "Historical membership", "status": "READY",
         "note": "survivorship-aware index-membership spans."},
        {"label": "Point-in-time fundamentals", "status": "NOT READY",
         "note": "owned fundamentals are a current snapshot without "
                 "lookahead-free filing dates."},
        {"label": "Earnings history", "status": "NOT READY",
         "note": "too few historical earnings events with real availability "
                 "timestamps."},
        {"label": "Historical sector classifications", "status": "NOT READY",
         "note": "the sector map has no dated time-series coverage."},
    ]
    gap = ("These gaps prevent the agent from validating fundamental, "
           "earnings-driven and sector-neutral strategies on point-in-time-safe "
           "history; only price-based ideas can be tested today.")
    return summary, gap


def _research_decisions_block(experiment: Optional[dict],
                              recovery: Optional[dict]) -> dict:
    """Plain-English translation of the latest experiment + recovery-campaign
    outcomes. No raw machine tokens appear in the label or plain text."""
    items: list[dict] = []
    bits: list[str] = []
    if experiment:
        term = experiment.get("terminal")
        plain = rr.translate(term) if term else None
        items.append({
            "label": "Latest experiment engine",
            "plain": plain or ("Completed; %s idea(s) passed the first gate."
                               % rr.fmt_int(experiment.get("keep_for_research")))
        })
    if recovery:
        completed = recovery.get("campaign_experiments")
        keep = recovery.get("campaign_keep_for_research")
        if completed is not None:
            bits.append("The latest recovery campaign ran %s experiment(s); %s "
                        "passed the first gate and none were promoted."
                        % (rr.fmt_int(completed), rr.fmt_int(keep)))
        counts = recovery.get("campaign_decision_counts") or {}
        for token in sorted(counts):
            items.append({"label": "%s experiment result(s)"
                                   % rr.fmt_int(counts[token]),
                          "plain": rr.translate(token)})
        disp = recovery.get("disposition")
        if disp:
            bits.append("Champion verdict: %s" % rr.translate(disp))
    summary = " ".join(bits) or "No new research decisions this cycle."
    return {"summary": summary, "items": items}


def _stage7_age_note(cfg: dict, cycle_date: str,
                     recovery: Optional[dict]) -> Optional[str]:
    root = cfg.get("stage7_recovery_root")
    if not root or not recovery:
        return None
    latest = _read_json(Path(root) / "latest.json") or {}
    age = ro_recovery._age_days(latest.get("as_of"), cycle_date)
    if age is None:
        return "latest verdict on record (age unknown)"
    return "%d day(s) old%s" % (age, " — STALE" if age > 7 else "")


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
    email_transport: Optional[str] = None
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
            "email_transport": self.email_transport,
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
                 recovery_launcher: Optional[Callable[[str], Any]] = None,
                 stage7_launcher: Optional[Callable[[dict], Any]] = None):
        self.cfg = cfg
        self.root = runtime_root(cfg)
        self.drivers = drivers
        self.portfolio = portfolio or PortfolioReader(
            rc.coerce_iterable(cfg.get("operational_ledger_roots")))
        self.email_sender = email_sender
        self.clock = clock or Clock()
        self.recovery_launcher = recovery_launcher
        # Separate from recovery_launcher (missed-report label launcher): the
        # Stage 7 launcher receives a cadence decision dict and refreshes the
        # recovery package out-of-band. Never runs the heavy backfill in-cycle.
        self.stage7_launcher = stage7_launcher
        self.stale_seconds = int(rc.cadence_value(cfg, "stale_lock_seconds",
                                                  1800))

    # ---- Stage 7 recurring cadence (WS4) --------------------------------- #
    def _prior_report_dict(self, conn, cycle_id) -> Optional[dict]:
        """Scorecard + disposition of the most recent report from a DIFFERENT
        cycle — the basis for the 'what changed' section."""
        row = conn.execute(
            "SELECT manifest_path FROM report_runs WHERE cycle_id<>?"
            " ORDER BY id DESC LIMIT 1", (cycle_id,)).fetchone()
        if not row:
            return None
        man = _read_json(Path(row[0]))
        if not man:
            return None
        return {"scorecard": man.get("paper_book"),
                "disposition": (man.get("recovery_readiness")
                                or {}).get("disposition")}

    def _stage7_cadence(self, label: str, cycle_date: str) -> dict:
        """Deterministic Stage 7 cadence decision for this cycle (reuse latest /
        delta refresh / Friday full refresh) — read-only."""
        cfg = self.cfg
        root = cfg.get("stage7_recovery_root")
        latest_manifest = None
        latest_as_of = None
        if root:
            latest = _read_json(Path(root) / "latest.json") or {}
            latest_as_of = latest.get("as_of")
            rid = latest.get("run_id")
            if rid:
                latest_manifest = _read_json(
                    Path(root) / "runs" / rid / "run_manifest.json")
        fp = ro_recovery.upstream_evidence_fingerprint(cfg)
        try:
            weekday = self.clock.now().weekday()
        except (AttributeError, ValueError):
            weekday = 0
        stale_days = int(rc.cadence_value(cfg, "stage7_stale_after_days", 7))
        return ro_recovery.stage7_cadence_decision(
            label=label, weekday=weekday,
            current_fingerprint=fp.get("fingerprint"),
            latest_manifest=latest_manifest, latest_as_of=latest_as_of,
            today=cycle_date, stale_after_days=stale_days)

    def _maybe_launch_stage7(self, conn, run_id, cadence, *,
                             cycle_id: str) -> Optional[dict]:
        """Launch a Stage 7 refresh out-of-band when the cadence calls for it and
        it is explicitly enabled — never in-cycle heavy compute, never twice per
        cycle. Idempotency uses a per-cycle marker file (robust to identical run
        ids / DB cascade). Returns the launcher result or None."""
        if cadence.get("action") not in (ro_recovery.CAD_RUN_DELTA,
                                         ro_recovery.CAD_RUN_FULL):
            return None
        if not rc.cadence_value(self.cfg, "stage7_launch_in_cycle", False):
            return None
        if self.stage7_launcher is None:
            return None
        marker = self.root / "state" / ("stage7_launch_%s.marker" % cycle_id)
        if marker.exists():
            return None
        try:
            result = self.stage7_launcher(cadence)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(self.clock.iso(), encoding="utf-8")
            return result
        except Exception as exc:  # noqa: BLE001
            self._record_error(conn, run_id, "stage7_cadence", "WARNING",
                               "stage7 launch failed: %s" % exc)
            return None

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

    # ---- Stage 9 tournament (config-driven; production collect path) ----- #
    def _stage8_config(self) -> Optional[dict]:
        """Load the Stage 8 autonomy config referenced by this Stage 4 config's
        ``stage_configs.stage8_autonomy`` pointer (repo-relative or absolute).
        Cached; returns None if the pointer or file is unavailable."""
        cached = getattr(self, "_stage8_cfg_cache", "_unset")
        if cached != "_unset":
            return cached
        cfg8 = None
        try:
            rel = (self.cfg.get("stage_configs") or {}).get("stage8_autonomy")
            if rel:
                p = Path(rel)
                if not p.is_absolute():
                    p = Path(__file__).resolve().parents[1] / rel
                if p.exists():
                    cfg8 = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception:  # noqa: BLE001 - absence is a clean 'disabled'
            cfg8 = None
        self._stage8_cfg_cache = cfg8
        return cfg8

    def _stage9_config_path(self) -> Optional[str]:
        """Resolve the versioned Stage 9 tournament config path via the Stage 8
        config's ``tournament.config`` pointer (or None)."""
        cfg8 = self._stage8_config()
        return resolve_stage9_config_path(cfg8) if cfg8 else None

    def _run_tournament_step(self, cycle_date: str) -> dict:
        """WS2/WS3: the config-driven Stage 9 tournament tick that runs INSIDE
        the scheduled AlphaAgent-Collect cycle. Activation is purely from the
        Stage 8 config's ``tournament.enabled`` flag - no caller passes
        run_tournament=True. Entirely self-contained: any tournament failure is
        captured and returned (never raised) so acquisition, queue processing,
        reporting and unrelated experiments are never affected. Read-only w.r.t.
        the operational portfolio - it writes ONLY Stage 8 research state on D:.
        """
        summary = {"tournament_attempted": False, "tournament_status": "DISABLED",
                   "candidates_evaluated": 0, "experiments_generated": 0,
                   "shadow_books_advanced": 0, "experiments_ingested": 0,
                   "tournament_error": None}
        try:
            cfg8 = self._stage8_config()
            if not cfg8 or not (cfg8.get("tournament") or {}).get("enabled"):
                return summary
            summary["tournament_attempted"] = True
            # CandidateRegistry / ResearchQueue expect a callable -> ISO string;
            # the runtime Clock exposes that as its bound ``.iso`` method.
            out = run_tournament_tick(cfg8, evidence_date=cycle_date,
                                      clock=self.clock.iso)
            summary["tournament_status"] = out.get("status", "OK")
            summary["candidates_evaluated"] = len(
                [e for e in out.get("evaluated", []) if not e.get("skipped")])
            summary["experiments_generated"] = out.get(
                "generated_experiments", 0)
            summary["shadow_books_advanced"] = out.get("shadow_books_advanced", 0)
            summary["experiments_ingested"] = (
                out.get("experiments_ingested") or {}).get("imported", 0)
            summary["campaign_ran"] = out.get("campaign_ran", False)
            summary["queue_depth"] = out.get("queue_depth")
            summary["counts_by_state"] = out.get("counts_by_state")
        except Exception as exc:  # noqa: BLE001 - never break collection
            summary["tournament_status"] = "TOURNAMENT_ERROR"
            summary["tournament_error"] = "%s: %s" % (
                type(exc).__name__, rc.redact(str(exc))[:200])
        return summary

    def _run_autonomy_drain_step(self, cycle_date: str) -> dict:
        """Stage 9.2: a BOUNDED, config-gated drain of the shared durable queue
        that runs INSIDE the scheduled AlphaAgent-Collect cycle, AFTER the
        tournament tick and BEFORE the ledger fingerprint - so the SAME
        operational-ledger guard proves it mutated nothing operational.

        It executes up to a configured number of already-queued research jobs -
        chiefly the Stage-9 tournament follow-up (`tournament.address_weakest_
        gate`) jobs the tick generates, which are highest priority in the queue -
        through the REAL production handlers, closing the generate->execute->
        persist->recalculate loop. Disabled unless
        ``autonomy.collect_drain.enabled`` is true in the Stage 8 config (so
        offline/acceptance configs never drain). Self-contained: any failure is
        captured and returned, never raised; a claimed job is RUNNING and can
        never be double-claimed by an overlapping cycle."""
        from . import autonomous_research as _ar
        summary = {"drain_attempted": False, "enabled": False, "jobs_claimed": 0}
        try:
            cfg8 = self._stage8_config()
            drain_cfg = (((cfg8 or {}).get("autonomy") or {}).get("collect_drain")
                         if cfg8 else None)
            if not drain_cfg or not drain_cfg.get("enabled"):
                summary["reason"] = "collect_drain disabled (config gate)"
                return summary
            summary["drain_attempted"] = True
            summary["enabled"] = True
            queue = build_autonomy_queue(cfg8, clock=self.clock.iso)
            # SAFE-TIMEOUT: the per-handler cooperative wall-clock budget is a
            # property of the HANDLER (injected into its bounded collects), NOT a
            # thread the drain abandons. Absent = unbounded.
            handlers = build_production_autonomy_handlers(
                cfg8, contact_email=cfg8.get("contact_email"), queue=queue,
                handler_time_budget_seconds=drain_cfg.get(
                    "handler_time_budget_seconds"))
            # WS3 scope allowlist. ``allowed_categories`` (present, possibly
            # empty) takes precedence over the legacy ``categories`` key; an
            # explicitly-empty allowlist executes nothing. Absent keys leave that
            # dimension unrestricted (offline/acceptance drains keep old
            # behaviour). The canonical production config restricts the drain to
            # approved Stage-9 tournament-continuation work only. The precedence
            # itself is applied inside ``drain_fairness.run_fair_drain``.
            budget = drain_cfg.get("budget_seconds")
            # Stage 9.5: ensure the LIVE companyfacts campaign has a claimable
            # bootstrap job (bounded, idempotent, respects the campaign's own stop
            # conditions) BEFORE the passes claim, so the acquisition actually
            # runs; PASS A admits it (origin stage9-weakest-gate, lane
            # acq.sec_companyfacts) ahead of the tournament experiments. Never
            # raises.
            try:
                cf_seed = ensure_companyfacts_bootstrap(
                    queue, cfg8, clock=self.clock.iso)
                if cf_seed:
                    summary["companyfacts_bootstrap_job"] = cf_seed
            except Exception:  # noqa: BLE001 - seeding never breaks the drain
                pass
            # Stage 10 — the AlphaAgent generates its OWN next identity/mapping
            # job: it inspects canonical identity coverage and enqueues the single
            # highest-value next action (at most one live identity job), so the
            # historical mapping backlog is operated through the SAME queue and
            # fair scheduler. Bounded, idempotent, restart-safe; the finite
            # backlog + throttled low-priority maintenance mean it can never
            # permanently starve the tournament, and the companyfacts fairness
            # promotion still skips PASS A so identity can never starve
            # companyfacts. Never raises.
            try:
                s10 = stage10_identity_cfg(cfg8)
                if s10.get("enabled") and s10.get("planner_enabled", True):
                    from . import identity_jobs as _ij
                    # The planner needs only store counts + Norgate availability;
                    # owned SEC ticker->CIK evidence is built by the resolve
                    # handler's own context at execution time.
                    id_ctx = build_identity_context(
                        cfg8, queue=queue, read_normalized=None,
                        clock=self.clock.iso)
                    id_plan = _ij.plan_next_identity_job(
                        queue, id_ctx, cfg=s10)
                    if id_plan:
                        summary["identity_planned_job"] = id_plan
            except Exception:  # noqa: BLE001 - planning never breaks the drain
                pass
            # Stage 10.2 — the AlphaAgent generates its OWN next fundamental job:
            # it drives the downstream companyfacts-bulk -> PIT -> readiness ->
            # measured-activation campaign through the SAME queue, at most one live
            # fundamental job. Bounded, idempotent, restart-safe; the finite
            # backlog + a lower priority than the identity/companyfacts work mean it
            # can never permanently starve the tournament. Never raises.
            try:
                s102 = stage10_2_cfg(cfg8)
                if s102.get("enabled") and s102.get("planner_enabled", True):
                    from . import fundamental_jobs as _fj
                    fund_ctx = build_fundamental_context(
                        cfg8, queue=queue, read_normalized=None,
                        clock=self.clock.iso)
                    fund_plan = _fj.plan_next_fundamental_job(
                        queue, fund_ctx, cfg=fund_ctx.stage102)
                    if fund_plan:
                        summary["fundamental_planned_job"] = fund_plan
            except Exception:  # noqa: BLE001 - planning never breaks the drain
                pass
            # Stage 9.5 DETERMINISTIC FAIRNESS. The bounded pass ordering (PASS A
            # tournament-continuation allowlist; PASS B SEC Form4/8-K continuation
            # iff A idle; PASS C SEC companyfacts continuation iff A and B idle) is
            # executed by the SINGLE source of truth ``run_fair_drain`` so the
            # durable fairness guarantee is applied by exactly the code the tests
            # exercise. max_jobs_per_cycle stays 1 and the exact companyfacts
            # origin/lane/category scope is unchanged. When the companyfacts
            # continuation has been idle for at least ``fairness_max_idle_cycles``
            # completed cycles, run_fair_drain PROMOTES PASS C ahead of PASS A for
            # ONE cycle (still exactly one job) - but ONLY if an eligible
            # continuation is queued AND the campaign is NOT stopped; the campaign's
            # own daily-batch cap + consecutive-no-progress limit ALWAYS win over
            # fairness. The durable idle counter lives in its own sqlite under the
            # Stage 8 autonomy root (never an operational-ledger file), so it is
            # restart-safe and never changes the operational-ledger fingerprint.
            from . import drain_fairness as _df
            cf_cont = drain_cfg.get("companyfacts_continuation") or {}
            fair_store = None
            cf_store = None
            cf_cid = "sec_companyfacts"
            campaign_stopped = False
            if cf_cont.get("enabled"):
                try:
                    fair_store = _df.FairnessStore(
                        stage8_autonomy_root(cfg8) / "scheduler_fairness.sqlite",
                        clock=self.clock.iso)
                    cf_store = stage8_campaign_store(cfg8, clock=self.clock.iso)
                    _cov = cf_store.coverage(cf_cid)
                    if _cov.get("exists"):
                        from datetime import datetime as _dt, timezone as _tz
                        _run_date = _dt.now(_tz.utc).date().isoformat()
                        _stop = sec_continuation_stop_reason(
                            is_complete=bool(_cov.get("is_complete")),
                            completed_batches_today=cf_store.batches_on_date(
                                cf_cid, _run_date),
                            consecutive_no_progress=cf_store
                            .consecutive_no_progress(cf_cid),
                            daily_cap=int(
                                cf_cont.get("daily_completed_batch_cap", 4)
                                or 4),
                            no_progress_max=int(
                                cf_cont.get(
                                    "max_consecutive_no_progress_batches", 2)
                                or 2))
                        campaign_stopped = _stop is not None
                except Exception:  # noqa: BLE001 - fairness never breaks the drain
                    fair_store = None

            def _cf_cursor_after():
                try:
                    if cf_store is None:
                        return None
                    c = cf_store.coverage(cf_cid)
                    return (int(c.get("acquisition_cursor", 0))
                            if c.get("exists") else None)
                except Exception:  # noqa: BLE001 - bookkeeping only, never fatal
                    return None

            report = _df.run_fair_drain(
                queue, handlers, drain_cfg=drain_cfg, fair_store=fair_store,
                campaign_stopped=campaign_stopped,
                cursor_after=_cf_cursor_after, budget_seconds=budget)
            summary.update(report)
            summary["drain_status"] = "OK"
        except Exception as exc:  # noqa: BLE001 - never break collection
            summary["drain_status"] = "DRAIN_ERROR"
            summary["drain_error"] = "%s: %s" % (
                type(exc).__name__, rc.redact(str(exc))[:200])
        return summary

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
        tournament_summary = {"tournament_attempted": False,
                              "tournament_status": "NOT_RUN"}
        drain_summary = {"drain_attempted": False, "enabled": False}
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
            # Stage 9 (WS2): the config-driven tournament tick runs INSIDE the
            # scheduled collect cycle, after collection and BEFORE the ledger
            # fingerprint below - so the same operational-ledger guard proves it
            # mutated nothing operational. Self-contained; never raises.
            tournament_summary = self._run_tournament_step(cycle_date)
            # Stage 9.2: the bounded queue drain runs AFTER the tournament tick
            # (so the follow-up jobs the tick generates exist to be executed) and
            # still INSIDE the operational-ledger guard below. Config-gated;
            # self-contained; never raises.
            drain_summary = self._run_autonomy_drain_step(cycle_date)
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
            "tournament": tournament_summary,
            "autonomy_drain": drain_summary,
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
                                    "ledgers_unchanged": ledgers_ok,
                                    "tournament_status":
                                    tournament_summary.get("tournament_status"),
                                    "drain_jobs_claimed":
                                    drain_summary.get("jobs_claimed"),
                                    "drain_jobs_completed":
                                    drain_summary.get("jobs_completed"),
                                    "drain_jobs_retryable":
                                    drain_summary.get("jobs_retryable")},
                        clock=self.clock, conn=conn)
        conn.close()
        if not ledgers_ok:
            return RuntimeResult(terminal=rc.BLOCKED,
                                 status="LEDGER_MUTATION_DETECTED",
                                 mode=rc.MODE_COLLECT, run_id=run_id,
                                 cycle_id=cycle_id, run_dir=str(run_dir),
                                 components=components,
                                 detail={"tournament": tournament_summary,
                                         "autonomy_drain": drain_summary})
        return RuntimeResult(terminal=terminal, status=status,
                             mode=rc.MODE_COLLECT, run_id=run_id,
                             cycle_id=cycle_id, run_dir=str(run_dir),
                             components=components,
                             detail={"tournament": tournament_summary})

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

        # Stage 7 recurring cadence: reuse the latest verified package, or (only
        # when explicitly enabled) launch a delta/Friday refresh out-of-band.
        prior_dict = self._prior_report_dict(conn, cycle_id)
        cadence = self._stage7_cadence(label, cycle_date)
        self._maybe_launch_stage7(conn, run_id, cadence, cycle_id=cycle_id)
        stage7_model = _stage7_report_model(self.cfg)
        if stage7_model is not None:
            stage7_model = {**stage7_model, "cadence": cadence}
        no_new_evidence = cadence.get("action") == ro_recovery.CAD_REUSE

        model = build_report_model(
            self.cfg, clock=self.clock, label=label, cycle_date=cycle_date,
            research=research_norm, portfolio=portfolio, run_id=run_id,
            run_dir=str(run_dir), degraded=degraded,
            llm_skipped_reason=llm_skipped_reason,
            subject_override=subject_override, stage5_model=stage5_model,
            stage7_model=stage7_model, prior=prior_dict,
            no_new_evidence=no_new_evidence,
            schedule_status=resolve_schedule_status(self.cfg))
        # WS6: the ACTUAL morning/post-close report model receives ONLY material
        # Stage 9 tournament changes; an unchanged standing surfaces nothing.
        rr.attach_tournament_changes(model,
                                     config_path=self._stage9_config_path())
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
        transport = resolve_email_transport(self.cfg)
        cred_present = (smtp_credential_present(self.cfg)
                        if transport == EMAIL_TRANSPORT_SMTP
                        else credential_present(self.cfg))
        email_manifest = {
            "cycle_id": cycle_id, "subject": model["subject"],
            "recipient": self.cfg.get("recipient_email"),
            "status": email_result["status"],
            "transport": email_result.get("transport") or transport,
            "diagnostic": email_result.get("diagnostic"),
            "message_id": email_result.get("message_id"),
            "credential_present": cred_present,
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
            email_transport=email_result.get("transport") or transport,
            components=components,
            detail={"ledgers_unchanged": ledgers_ok,
                    "report_html": str(report_paths["html"]),
                    "report_text": str(report_paths["text"]),
                    "email_status": email_result["status"],
                    "email_transport": email_result.get("transport")
                    or transport,
                    "email_diagnostic": email_result.get("diagnostic"),
                    "email_message_id": email_result.get("message_id"),
                    "llm_skipped_reason": llm_skipped_reason})

    def _research_terminal(self, email_status, no_new, llm_skipped, ledgers_ok,
                           *, degraded=False):
        if not ledgers_ok:
            return "LEDGER_MUTATION_DETECTED", rc.BLOCKED
        if email_status in EMAIL_CREDENTIAL_REQUIRED_STATUSES:
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
                        send_email: bool = False,
                        exec_test: bool = False,
                        exec_test_key: str = "exec_test",
                        subject_override: Optional[str] = None
                        ) -> RuntimeResult:
        conn = self._open()
        cycle_date = self.clock.date()
        # An executive test email is its own idempotency key (one per date) and
        # forces exactly one delivery — never colliding with a scheduled report.
        # ``exec_test_key`` distinguishes independent one-off test emails on the
        # same date (e.g. a v2 executive-brief test vs an earlier test) so each
        # still gets exactly one delivery without a duplicate-send collision.
        cycle_key = exec_test_key if exec_test else label
        if exec_test:
            send_email = True
            subject_override = subject_override or (
                "%s — %s" % (EXEC_TEST_SUBJECT_PREFIX, cycle_date))
        cycle_id = rc.report_cycle_id(cycle_key, cycle_date)
        run_id = rc.runtime_run_id(cycle_id, rc.MODE_REPORT_ONLY,
                                   self.clock.iso())
        self._begin_run(conn, run_id, cycle_id, rc.MODE_REPORT_ONLY, label,
                        cycle_date)
        portfolio = self.portfolio.read()
        research_norm = {"no_new": True, "counts": {}, "metrics": {},
                         "llm_invoked": False}
        run_dir = self.root / "runs" / run_id
        prior_dict = self._prior_report_dict(conn, cycle_id)
        cadence = self._stage7_cadence(label, cycle_date)
        stage7_model = _stage7_report_model(self.cfg)
        if stage7_model is not None:
            stage7_model = {**stage7_model, "cadence": cadence}
        model = build_report_model(
            self.cfg, clock=self.clock, label=label, cycle_date=cycle_date,
            research=research_norm, portfolio=portfolio, run_id=run_id,
            run_dir=str(run_dir), degraded=False,
            llm_skipped_reason="report-only (no collection, no LLM)",
            subject_override=subject_override,
            stage7_model=stage7_model, prior=prior_dict,
            no_new_evidence=cadence.get("action") == ro_recovery.CAD_REUSE,
            schedule_status=resolve_schedule_status(self.cfg))
        # WS6: attach ONLY material Stage 9 tournament changes to the model.
        rr.attach_tournament_changes(model,
                                     config_path=self._stage9_config_path())
        html_body = rr.render_html(model)
        text_body = rr.render_text(model)
        manifest = rr.report_manifest(model, html_body, text_body)
        report_paths = self._write_report(cycle_date, cycle_key, html_body,
                                          text_body, manifest)
        email_status = rc.EMAIL_SKIPPED
        email_result = {"status": rc.EMAIL_SKIPPED}
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
                             email_diagnostic=email_result.get("diagnostic"),
                             email_message_id=email_result.get("message_id"),
                             email_transport=email_result.get("transport"),
                             detail={"report_html": str(report_paths["html"]),
                                     "report_text": str(report_paths["text"]),
                                     "subject": model["subject"],
                                     "email_status": email_status,
                                     "email_transport":
                                     email_result.get("transport"),
                                     "email_message_id":
                                     email_result.get("message_id")})

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
                "message_id": result.get("message_id"),
                "transport": result.get("transport")
                or resolve_email_transport(self.cfg)}

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


# --------------------------------------------------------------------------- #
# Stage 8 autonomy entry points (additive; do not alter any Stage 4 mode).
# --------------------------------------------------------------------------- #
# These wire the persistent runtime to the Stage 8 never-idle research queue and
# source-exhaustion registry WITHOUT changing any existing collect/research/
# report/watchdog/verify behaviour. They are RESEARCH-ONLY: no order, fill,
# signal, trade decision, model promotion, Daily Close, prediction call or
# operational-ledger write is possible here. Heavy acquisition / experiment work
# is injected by the caller (or the scheduled task); the built-in default
# handlers keep the loop moving with light, offline, read-only work so a cycle
# never blocks on the network.
def stage8_enabled(cfg: dict) -> bool:
    return bool(cfg.get("stage8_enabled"))


def stage8_autonomy_root(cfg: dict) -> "Path":
    root = cfg.get("stage8_autonomy_root")
    if root:
        return Path(root)
    # Canonical location is the queue-db's directory, so campaign + queue stores
    # sit together under the configured stage8 root on D: (never derived under a
    # sibling of the operational-ledger tree).
    qdb = (cfg.get("autonomy") or {}).get("queue_db") \
        if isinstance(cfg.get("autonomy"), dict) else None
    if qdb:
        return Path(qdb).parent
    return Path(cfg.get("runtime_root", ".")).parent / "stage8"


def resolve_campaign_defs(cfg: dict) -> "list[dict]":
    """The full-universe acquisition campaigns (WS3-WS6). Config
    ``production.campaigns`` overrides; else the built-in Norgate-prices /
    EODHD-analyst / SEC-Form4-8K CIK / two SEC bulk-archive campaigns. Each has a
    per-job batch size; none carries a permanent total-universe cap."""
    from . import autonomous_research as _ar
    prod = cfg.get("production") if isinstance(cfg.get("production"), dict) \
        else {}
    defs = prod.get("campaigns")
    if isinstance(defs, list) and defs:
        return defs
    b = int(prod.get("per_job_symbol_batch_size", 25))
    return [
        {"id": "norgate_prices", "runner": "norgate",
         "category": _ar.CAT_DATA_ACQUISITION,
         "batch_size": int(prod.get("norgate_batch_size", 50))},
        {"id": "eodhd_analyst", "runner": "analyst",
         "category": _ar.CAT_PROSPECTIVE_SNAPSHOT,
         "batch_size": int(prod.get("analyst_batch_size", b))},
        {"id": "sec_form4_8k", "runner": "sec_cik",
         "category": _ar.CAT_DATA_ACQUISITION,
         "batch_size": int(prod.get("sec_cik_batch_size", 20))},
        {"id": "sec_bulk_companyfacts", "runner": "bulk",
         "archive": "companyfacts", "category": _ar.CAT_DATA_ACQUISITION,
         "batch_size": 1},
        {"id": "sec_bulk_submissions", "runner": "bulk",
         "archive": "submissions", "category": _ar.CAT_DATA_ACQUISITION,
         "batch_size": 1},
    ]


def stage8_campaign_store(cfg: dict, *, clock=None):
    """Open (creating if needed) the durable sharded-campaign cursor store,
    co-located with the research queue under the stage8 root on D:."""
    from . import acquisition_campaign as _ac
    prod = cfg.get("production") if isinstance(cfg.get("production"), dict) \
        else {}
    cpath = prod.get("campaign_db") \
        or str(stage8_autonomy_root(cfg) / "campaigns.sqlite")
    return _ac.CampaignStore(cpath, clock=clock)


def production_campaign_seed_specs(cfg: dict, store) -> "list[dict]":
    """Never-idle planner specs (WS7): keep exactly one live batch job per
    INCOMPLETE campaign, tagged by the live cursor so it is idempotent with the
    handler's own next-batch enqueue. Complete campaigns emit nothing."""
    specs = []
    for d in resolve_campaign_defs(cfg):
        cid = d["id"]
        cov = store.coverage(cid)
        if cov.get("exists") and cov.get("is_complete"):
            continue
        seq = int(cov.get("acquisition_cursor", 0)) if cov.get("exists") else 0
        specs.append({"category": d["category"], "lane": "acq.%s" % cid,
                      "payload": {"campaign": cid, "seq": seq},
                      "priority": 4, "origin": "campaign-planner"})
    return specs


def build_autonomy_queue(cfg: dict, *, clock=None):
    """Open (creating if needed) the durable Stage 8 research queue. The
    canonical location is ``autonomy.queue_db`` from the config so the Telegram
    control plane and the autonomy cycle share ONE queue; only when that key is
    absent (e.g. unit tests) is it derived under the research-state root. Never
    touches an operational ledger."""
    from . import autonomous_research as _ar
    qdb = (cfg.get("autonomy") or {}).get("queue_db") \
        if isinstance(cfg.get("autonomy"), dict) else None
    db = Path(qdb) if qdb else (stage8_autonomy_root(cfg) / "autonomy.sqlite")
    return _ar.ResearchQueue(db, clock=clock)


# --------------------------------------------------------------------------- #
# Stage 10 — historical identity layer wiring. The canonical identity store +
# artifact root live under the Stage 8 research root on D: (NEVER under an
# operational-ledger root), co-located with the queue / campaign / fairness
# stores. The AlphaAgent operates the identity backlog through the SAME queue.
# --------------------------------------------------------------------------- #
def stage10_identity_cfg(cfg: dict) -> dict:
    return (cfg.get("stage10_identity") or {}) if isinstance(cfg, dict) else {}


def stage10_1_cfg(cfg: dict) -> dict:
    """Stage 10.1 HISTORICAL CIK BRIDGE bulk-source block (nested under
    ``stage10_identity.stage10_1``)."""
    return (stage10_identity_cfg(cfg).get("stage10_1") or {})


def stage10_identity_root(cfg: dict) -> "Path":
    s10 = stage10_identity_cfg(cfg)
    db = s10.get("store_db")
    if db:
        return Path(db).parent
    return stage8_autonomy_root(cfg) / "identity"


def build_identity_store(cfg: dict, *, clock=None):
    """Open (creating if needed) the durable canonical identity store on D:."""
    from . import historical_identity as _hi
    s10 = stage10_identity_cfg(cfg)
    db = s10.get("store_db") or str(stage10_identity_root(cfg) /
                                    "historical_identity.sqlite")
    return _hi.IdentityStore(db, clock=clock)


def build_issuer_index(cfg: dict, *, clock=None):
    """Open (creating if needed) the durable Stage 10.1 SEC issuer-history index
    on D:, or return None when Stage 10.1 is disabled. Under the research root,
    never an operational-ledger root."""
    s101 = stage10_1_cfg(cfg)
    if not s101.get("enabled"):
        return None
    from . import sec_issuer_index as _si
    db = s101.get("issuer_index_db") or \
        str(stage10_identity_root(cfg) / "sec_issuer_history.sqlite")
    return _si.SecIssuerIndex(db, clock=clock)


def build_identity_context(cfg: dict, *, queue=None, read_normalized=None,
                           clock=None):
    """Assemble the Stage 10 identity job context: the canonical store, the owned
    Norgate identity accessor and whatever OWNED SEC ticker->CIK evidence is
    cheaply available (from owned normalized RT_FUNDAMENTAL_FACT records — the
    current-only map; delisted names honestly stay UNRESOLVED). Read-only w.r.t.
    every operational store."""
    from . import historical_identity as _hi
    from . import identity_jobs as _ij
    s10 = stage10_identity_cfg(cfg)
    store = build_identity_store(cfg, clock=clock)
    artifact_root = s10.get("artifact_root") or \
        str(stage10_identity_root(cfg) / "artifacts")
    # Owned current ticker->CIK evidence from owned companyfacts records.
    ticker_cik_index = None
    if callable(read_normalized):
        idx: dict = {}
        for r in read_normalized("FUNDAMENTAL_FACT", limit=6000):
            p = r.get("normalized_payload") or {}
            cik = _hi.norm_cik(p.get("cik") or r.get("company_id"))
            tkr = (r.get("ticker") or p.get("ticker") or "")
            if cik and tkr:
                idx.setdefault(str(tkr).strip().upper(), set()).add(cik)
        ticker_cik_index = {k: sorted(v) for k, v in idx.items()} or None
    # Stage 9 config supplies the readiness thresholds + rebalance dates.
    cfg9 = {}
    try:
        _p9 = resolve_stage9_config_path(cfg)
        if _p9:
            from . import tournament as _tt
            cfg9 = _tt.load_config(_p9) or {}
    except Exception:  # noqa: BLE001
        cfg9 = {}
    rebalance = list(s10.get("rebalance_dates") or
                     (((cfg9.get("stage9_5") or {}).get("historical_universe")
                       or {}).get("rebalance_dates") or []))
    # Stage 10.1 — the SEC issuer-history index + bulk-source acquisition wiring.
    issuer_index = build_issuer_index(cfg, clock=clock)
    stage101 = _resolve_stage101_runtime(cfg)
    transport = None
    if issuer_index is not None:
        try:
            from . import ingestion as _ing
            transport = _ing.default_transport
        except Exception:  # noqa: BLE001
            transport = None
    return _ij.IdentityJobContext(
        store=store, accessor=_hi.NorgateIdentityAccessor(),
        artifact_root=artifact_root,
        index_name=s10.get("index_name", "S&P 500"),
        survivorship_watchlist=s10.get("survivorship_watchlist",
                                       "S&P 500 Current & Past"),
        current_watchlist=s10.get("current_watchlist", "S&P 500"),
        membership_start=s10.get("membership_start", "1990-01-01"),
        discover_batch=int(s10.get("discover_batch", 25)),
        resolve_batch=int(s10.get("resolve_batch", 50)),
        rebalance_dates=rebalance, cfg9=cfg9,
        ticker_cik_index=ticker_cik_index,
        issuer_index=issuer_index, read_normalized=read_normalized,
        transport=transport, stage101=stage101, clock=clock)


def _resolve_stage101_runtime(cfg: dict) -> dict:
    """The Stage 10.1 config block with resolved defaults (bulk root, SEC bulk /
    current-ticker URLs, contact identity). Defaults keep it self-contained; the
    contact email reuses the production-handlers identity so the SEC User-Agent is
    always compliant."""
    s101 = dict(stage10_1_cfg(cfg))
    if not s101:
        return {}
    root = stage10_identity_root(cfg)
    s101.setdefault("bulk_root", str(root / "sec_bulk"))
    s101.setdefault("issuer_index_db", str(root / "sec_issuer_history.sqlite"))
    s101.setdefault("submissions_url",
                    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/"
                    "submissions.zip")
    s101.setdefault("companyfacts_url",
                    "https://www.sec.gov/Archives/edgar/daily-index/xbrl/"
                    "companyfacts.zip")
    s101.setdefault("company_tickers_url",
                    "https://www.sec.gov/files/company_tickers.json")
    s101.setdefault("company_tickers_exchange_url",
                    "https://www.sec.gov/files/company_tickers_exchange.json")
    if not s101.get("contact_email"):
        ph = (cfg.get("production_handlers") or {}) if isinstance(cfg, dict) \
            else {}
        s101["contact_email"] = ph.get("contact_email")
    s101.setdefault("user_agent_product", "paper-trader-alpha-agent/2.0")
    return s101


# --------------------------------------------------------------------------- #
# Stage 10.2 — historical PIT fundamentals wiring. The durable companyfacts fact
# index lives under the SAME Stage 8 research root on D: (co-located with the
# identity store + issuer index), NEVER under an operational-ledger root. The
# AlphaAgent operates the downstream campaign through the SAME queue.
# --------------------------------------------------------------------------- #
def stage10_2_cfg(cfg: dict) -> dict:
    return (cfg.get("stage10_2") or {}) if isinstance(cfg, dict) else {}


def _resolve_stage102_runtime(cfg: dict) -> dict:
    """The Stage 10.2 config block with resolved defaults (bulk root, companyfacts
    bulk URL, fact-index path, contact identity, budgets). Defaults keep it self-
    contained and co-located with the Stage 10 identity artifacts on D:."""
    s102 = dict(stage10_2_cfg(cfg))
    if not s102:
        return {}
    root = stage10_identity_root(cfg)
    s102.setdefault("bulk_root", str(root / "sec_bulk"))
    s102.setdefault("companyfacts_index_db",
                    str(root / "sec_companyfacts_index.sqlite"))
    s102.setdefault("companyfacts_url",
                    "https://www.sec.gov/Archives/edgar/daily-index/xbrl/"
                    "companyfacts.zip")
    s102.setdefault("artifact_root", str(root / "artifacts"))
    if not s102.get("contact_email"):
        ph = (cfg.get("production_handlers") or {}) if isinstance(cfg, dict) \
            else {}
        s102["contact_email"] = ph.get("contact_email")
    s102.setdefault("user_agent_product", "paper-trader-alpha-agent/2.0")
    return s102


def build_companyfacts_index(cfg: dict, *, clock=None):
    """Open (creating if needed) the durable Stage 10.2 SEC companyfacts fact
    index on D:, or return None when Stage 10.2 is disabled. Under the research
    root, never an operational-ledger root."""
    s102 = _resolve_stage102_runtime(cfg)
    if not s102 or not s102.get("enabled"):
        return None
    from . import sec_companyfacts_index as _cfi
    return _cfi.SecCompanyFactsIndex(s102["companyfacts_index_db"], clock=clock)


def build_fundamental_context(cfg: dict, *, queue=None, read_normalized=None,
                              clock=None):
    """Assemble the Stage 10.2 fundamental job context: the canonical identity
    store (resolved historical CIKs + survivorship-safe universe), the durable
    companyfacts fact index, the durable campaign store, the Stage 9 readiness
    config + rebalance dates and the SEC HTTP transport. Read-only w.r.t. every
    operational store."""
    from . import fundamental_jobs as _fj
    s10 = stage10_identity_cfg(cfg)
    s102 = _resolve_stage102_runtime(cfg)
    store = build_identity_store(cfg, clock=clock)
    index = build_companyfacts_index(cfg, clock=clock)
    try:
        campaign_store = stage8_campaign_store(cfg, clock=clock)
    except Exception:  # noqa: BLE001 - campaign store optional for planning
        campaign_store = None
    cfg9 = {}
    try:
        _p9 = resolve_stage9_config_path(cfg)
        if _p9:
            from . import tournament as _tt
            cfg9 = _tt.load_config(_p9) or {}
    except Exception:  # noqa: BLE001
        cfg9 = {}
    rebalance = list(s10.get("rebalance_dates") or
                     (((cfg9.get("stage9_5") or {}).get("historical_universe")
                       or {}).get("rebalance_dates") or []))
    transport = None
    if index is not None:
        try:
            from . import ingestion as _ing
            transport = _ing.default_transport
        except Exception:  # noqa: BLE001
            transport = None
    signals = tuple(((cfg9.get("stage9_5") or {}).get("fundamental_mvp") or {}
                     ).get("signals") or _fj.DEFAULT_SIGNALS)
    return _fj.FundamentalJobContext(
        store=store, companyfacts_index=index, campaign_store=campaign_store,
        artifact_root=s102.get("artifact_root") or str(
            stage10_identity_root(cfg) / "artifacts"),
        rebalance_dates=rebalance, cfg9=cfg9, stage102=s102,
        index_name=s10.get("index_name", "S&P 500"), signals=signals,
        transport=transport, read_normalized=read_normalized, clock=clock)


def _default_autonomy_handlers(cfg: dict) -> dict:
    """Safe, offline, read-only default handlers. Discovery/probe rebuild the
    source registry from the catalog (no network); everything else records a
    bounded completion so the never-idle loop advances. Production injects richer
    handlers for real acquisition / experiments."""
    from . import autonomous_research as _ar
    from . import source_exhaustion as _se

    def _discovery(job):
        snap = _se.build_registry_snapshot()
        try:
            path = (cfg.get("sources") or {}).get("registry_snapshot_path") \
                if isinstance(cfg.get("sources"), dict) else None
            if path:
                _se.write_registry_snapshot(snap, path)
        except Exception:  # noqa: BLE001 - snapshot persistence is best-effort
            pass
        return _ar.OUTCOME_COMPLETED, {"classification_tally":
                                       snap.get("classification_tally")}

    def _record(job):
        return _ar.OUTCOME_COMPLETED, {"note": "recorded; heavy work out-of-band",
                                       "lane": job.lane}

    handlers = {c: _record for c in _ar.JOB_CATEGORIES}
    handlers[_ar.CAT_SOURCE_DISCOVERY] = _discovery
    handlers[_ar.CAT_ENTITLEMENT_PROBE] = _discovery
    return handlers


def sec_continuation_stop_reason(*, is_complete: bool,
                                 completed_batches_today: int,
                                 consecutive_no_progress: int,
                                 daily_cap: int, no_progress_max: int
                                 ) -> Optional[str]:
    """Stage 9.3 controlled-continuation PRE-RUN decision table (pure). Returns
    the stop reason that forbids running another SEC batch right now, or None to
    proceed. Order is deterministic: completeness, then the no-progress limit,
    then the per-calendar-day completed-batch cap."""
    if is_complete:
        return "CAMPAIGN_COMPLETE"
    if consecutive_no_progress >= no_progress_max:
        return "NO_PROGRESS_LIMIT"
    if completed_batches_today >= daily_cap:
        return "DAILY_BATCH_CAP_REACHED"
    return None


def sec_continuation_should_chain(*, is_complete: bool,
                                  completed_batches_today: int,
                                  daily_cap: int) -> bool:
    """Whether a PROGRESS batch may chain the next continuation: only while the
    campaign is incomplete AND still under the per-day completed-batch cap. A
    no-progress batch never chains (enforced separately by the caller)."""
    return (not is_complete) and completed_batches_today < daily_cap


def ensure_companyfacts_bootstrap(queue, cfg: dict, *, clock=None
                                  ) -> Optional[str]:
    """Stage 9.5: guarantee the LIVE companyfacts campaign actually runs.

    When ``autonomy.collect_drain.companyfacts_continuation`` is enabled and the
    campaign can still make progress (not complete / not daily-capped / not
    stalled - all DERIVED from the durable append-only campaign_batch log) AND no
    companyfacts acquisition job is already live on the queue, enqueue exactly ONE
    bounded bootstrap job (category DATA_ACQUISITION, lane acq.sec_companyfacts,
    origin stage9-weakest-gate so PASS A of the bounded collect drain admits it,
    priority = bootstrap_priority so the live acquisition runs ahead of the
    tournament experiments). Idempotent (skips when a live companyfacts job
    exists), bounded (respects the campaign stop conditions) and restart-safe.
    Returns the job id or None. Read-only w.r.t. every operational trading
    store."""
    from . import autonomous_research as _ar
    from datetime import datetime as _d, timezone as _tz
    try:
        cf = ((((cfg.get("autonomy") or {}).get("collect_drain") or {})
               .get("companyfacts_continuation") or {})
              if isinstance(cfg, dict) else {})
        if not cf.get("enabled") or queue is None:
            return None
        lane = "acq.sec_companyfacts"
        # Idempotent: any live companyfacts acquisition job already present?
        for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE,
                   _ar.STATE_BLOCKED_SPECIFIC):
            for j in queue.list_jobs(state=st, limit=500):
                if str(getattr(j, "lane", "")) == lane:
                    return None
        # Respect the campaign's OWN bounded stop conditions.
        store = stage8_campaign_store(cfg, clock=clock)
        cid = "sec_companyfacts"
        cov = store.coverage(cid)
        if cov.get("exists"):
            run_date = _d.now(_tz.utc).date().isoformat()
            stop = sec_continuation_stop_reason(
                is_complete=bool(cov.get("is_complete")),
                completed_batches_today=store.batches_on_date(cid, run_date),
                consecutive_no_progress=store.consecutive_no_progress(cid),
                daily_cap=int(cf.get("daily_completed_batch_cap", 4) or 4),
                no_progress_max=int(
                    cf.get("max_consecutive_no_progress_batches", 2) or 2))
            if stop is not None:
                return None
        seq = int(cov.get("acquisition_cursor", 0)) if cov.get("exists") else 0
        return queue.enqueue(
            _ar.CAT_DATA_ACQUISITION, lane=lane,
            payload={"campaign": cid, "seq": seq, "bootstrap": True},
            priority=int(cf.get("bootstrap_priority", 5) or 5),
            origin="stage9-weakest-gate")
    except Exception:  # noqa: BLE001 - seeding is best-effort, never fatal
        return None


def build_production_autonomy_handlers(cfg: dict, *,
                                       contact_email: Optional[str] = None,
                                       queue=None,
                                       handler_time_budget_seconds:
                                       Optional[float] = None) -> dict:
    """REAL Stage 8 production handlers (WS2-WS9). Each durable-queue category is
    wired to a genuine, BOUNDED entrypoint that ACQUIRES real data (Stage 2
    collect / Stage 6 survivorship-free Norgate backfill) or RUNS a real
    price-factor experiment (the Stage-5/7 engine over the owned Norgate panel)
    and returns genuine evidence: real record counts, coverage deltas, rank-IC /
    t-stats and deterministic gate decisions. NO category returns a placeholder
    completion, an entitlement-probe or a registry rebuild dressed up as work.

    Research-only: every write lands under the Stage 2/5/6 research roots on the
    data drive; NOTHING here can touch an operational ledger, order, fill,
    signal, trade decision, model promotion, Alpha Paper Book, PostgreSQL, the
    prediction service or a Daily Close. A handler that raises is caught by the
    cycle runner and becomes a bounded RETRYABLE (never crashes the loop)."""
    from . import autonomous_research as _ar
    import json as _json

    repo_root = Path(__file__).resolve().parents[1]
    ph = (cfg.get("production_handlers") or {}) if isinstance(cfg, dict) else {}
    # A declared contact is required by strict free official sources (SEC EDGAR
    # rejects requests without a real User-Agent contact). Prefer the explicit
    # arg, else the config, else None (the collector falls back to git config).
    contact_email = contact_email or ph.get("contact_email")

    def _load(relpath):
        p = Path(relpath)
        if not p.is_absolute():
            p = repo_root / relpath
        return _json.loads(p.read_text(encoding="utf-8-sig"))

    s2_path = ph.get("stage2_ingestion_config") or \
        (cfg.get("sources") or {}).get("stage2_ingestion_config")
    s6_path = ph.get("stage6_backfill_config")
    bounded_universe = ph.get("bounded_universe") or \
        ["AAPL", "MSFT", "NVDA", "AMZN", "JPM", "XOM"]
    repair_universe = ph.get("coverage_repair_universe") or \
        ["KO", "PG", "JNJ", "WMT", "HD", "CVX"]
    panel_start = ph.get("panel_date_start", "2016-01-01")

    cfg2 = _load(s2_path) if s2_path else {}
    cfg6 = _load(s6_path) if s6_path else {}
    ingestion_root = cfg6.get("ingestion_root") or \
        str(Path(cfg.get("stage8_root", ".")) / "ingestion")

    # SAFE-TIMEOUT (Stage 9.2 release correction): inject a COOPERATIVE per-
    # collect wall-clock budget into every bounded collect this handler set runs,
    # WITHOUT mutating the shared cfg2 (limits is deep-copied). The SEC collector
    # honours ``collect_time_budget_seconds`` by stopping between bounded network
    # requests and marking the run ``deadline_reached`` -> the acquisition handler
    # returns RETRYABLE (see ``_run_sec_cik_campaign``). Absent/<=0 = unbounded
    # (offline/acceptance drains keep byte-identical behaviour). This is the
    # handler's OWN inline bound; the drain never abandons a thread.
    _htb = (float(handler_time_budget_seconds)
            if handler_time_budget_seconds else 0.0)

    def _with_time_budget(one: dict) -> dict:
        if _htb <= 0:
            return one
        lim = dict(one.get("limits") or {})
        lim["collect_time_budget_seconds"] = _htb
        one["limits"] = lim
        return one

    # Stage 9.3 - CONTROLLED SEC Form 4 / 8-K continuation caps. Both bounds are
    # DERIVED from the durable append-only batch log (restart-safe), enforced by
    # the SEC campaign handler INLINE, and are cooperative (not a hard kill):
    #   daily_completed_batch_cap        -> at most N completed batches/UTC day
    #   max_consecutive_no_progress_...  -> stop after N consecutive stalled batches
    _sec_cont_cfg = ((((cfg.get("autonomy") or {}).get("collect_drain") or {})
                      .get("sec_continuation") or {})
                     if isinstance(cfg, dict) else {})
    _sec_daily_cap = int(_sec_cont_cfg.get("daily_completed_batch_cap", 6) or 6)
    _sec_noprog_max = int(
        _sec_cont_cfg.get("max_consecutive_no_progress_batches", 2) or 2)

    # Stage 9.5 - CONTROLLED SEC companyfacts continuation caps (own campaign,
    # independent of the Form 4 / 8-K campaign). DERIVED from that campaign's own
    # durable append-only batch log; enforced INLINE by _run_companyfacts_campaign.
    _cf_cont_cfg = ((((cfg.get("autonomy") or {}).get("collect_drain") or {})
                     .get("companyfacts_continuation") or {})
                    if isinstance(cfg, dict) else {})
    _cf_enabled = bool(_cf_cont_cfg.get("enabled"))
    _cf_daily_cap = int(_cf_cont_cfg.get("daily_completed_batch_cap", 4) or 4)
    _cf_noprog_max = int(
        _cf_cont_cfg.get("max_consecutive_no_progress_batches", 2) or 2)
    _cf_max_batch = max(1, int(_cf_cont_cfg.get("max_batch_size", 10) or 10))
    _cf_batch = max(1, min(int(_cf_cont_cfg.get("batch_size", 5) or 5),
                           _cf_max_batch))
    _cf_concepts = list(_cf_cont_cfg.get("first_concepts") or [])

    # ---- shared REAL price panel (read once; reused by experiment jobs) ------
    _cache = {}

    def _panel():
        if "panel" not in _cache:
            from . import experiment_runner as _er
            store = _er.NormalizedStore(ingestion_root, max_files=6000,
                                        max_symbols=300)
            _cache["panel"] = store.price_panel(panel_start, None)
        return _cache["panel"]

    # ---- Stage 2 real bounded single-provider collect -----------------------
    def _collect(sid):
        from . import ingestion as _ing
        src = (cfg2.get("sources") or {})
        if sid not in src:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "provider": sid, "reason": "source not configured"}
        one = dict(cfg2)
        one["sources"] = {sid: src[sid]}
        one = _with_time_budget(one)
        res = _ing.run_ingestion(config=one, output_root=ingestion_root,
                                 mode="collect", as_of="latest",
                                 contact_email=contact_email)
        counts = res.get("counts") or {}
        detail = {"real_work": "stage2_collect", "provider": sid,
                  "status": res.get("status"), "run_id": res.get("run_id"),
                  "normalized_records_new": counts.get("normalized_records_new"),
                  "raw_objects_new": counts.get("raw_objects_new"),
                  "normalized_records_total":
                      counts.get("normalized_records_total"),
                  "source_state": (res.get("source_states") or {}).get(sid),
                  "ledger_unchanged": res.get("ledger_unchanged")}
        blocked = res.get("blocked_sources") or []
        if "BLOCKED" in str(res.get("status") or ""):
            if not blocked and not counts.get("normalized_records_new"):
                # The source is already FRESH within its freshness window: honest
                # idempotency (the collector refuses a redundant fetch), NOT a
                # failure and NOT a credential/entitlement block. Complete the
                # job with an up-to-date note; it re-runs when the source goes
                # stale.
                detail["note"] = ("source already fresh; no new data to acquire "
                                  "this cycle (idempotent)")
                detail["up_to_date"] = True
                return _ar.OUTCOME_COMPLETED, detail
            detail["reason"] = "provider blocked: %s" % (blocked or
                                                         res.get("status"))
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    def _collect_batch(sid, symbols, *, symbol_keys, extra=None):
        """Run a bounded single-source collect with the source's symbol keys
        OVERRIDDEN by the campaign batch (WS3/WS4/WS5 sharding). Returns the raw
        run_ingestion result (or None if the source is not configured)."""
        from . import ingestion as _ing
        src = (cfg2.get("sources") or {})
        if sid not in src:
            return None
        scfg = dict(src[sid])
        for k in symbol_keys:
            scfg[k] = list(symbols)
        if extra:
            scfg.update(extra)
        one = dict(cfg2)
        one["sources"] = {sid: scfg}
        one = _with_time_budget(one)
        return _ing.run_ingestion(config=one, output_root=ingestion_root,
                                  mode="collect", as_of="latest",
                                  contact_email=contact_email)

    # ---- Stage 6 real survivorship-free Norgate backfill --------------------
    def _norgate_backfill(univ, kind):
        from . import historical_backfill as _hb
        res = _hb.run_backfill(cfg6, mode="backfill", as_of="latest",
                               universe_override=list(univ))
        detail = {"real_work": kind, "status": res.get("status"),
                  "terminal": res.get("terminal"), "run_id": res.get("run_id"),
                  "records_written": res.get("records_written"),
                  "duplicate_prevented": res.get("duplicate_prevented"),
                  "universe_size": res.get("universe_size"),
                  "coverage_delta": res.get("coverage_delta") or {},
                  "templates_unlocked": res.get("templates_unlocked"),
                  "ledgers_unchanged": res.get("ledgers_unchanged"),
                  "universe": list(univ)}
        if "BLOCKED" in str(res.get("status") or "") or \
                res.get("terminal") == "ALPHA_AGENT_STAGE6_BLOCKED":
            detail["reason"] = "norgate backfill blocked: %s" % res.get("status")
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        if not res.get("records_written"):
            detail["note"] = "no NEW bars (universe already covered)"
        return _ar.OUTCOME_COMPLETED, detail

    # Map a registry DISPLAY provider name (e.g. "BEA", "FRED / ALFRED") onto the
    # collector source_id its stage2 config block uses, so a planner-seeded
    # acquisition job routes to the real collector (WS1: BEA runs through the
    # queue handler once its free UserID is provisioned).
    _PROVIDER_TO_SID = {
        "bea": "bea", "fred / alfred": "fred_alfred", "fred": "fred_alfred",
        "bls": "bls", "us treasury": "us_treasury", "finra": "finra",
        "nasdaq trader": "nasdaq_trader", "sec edgar": "sec_edgar",
        "eodhd": "eodhd", "norgate data": "norgate_local",
    }

    def _resolve_sid(payload):
        sid = (payload or {}).get("collector_source_id")
        if sid:
            return sid
        provider = str((payload or {}).get("provider") or "eodhd")
        return _PROVIDER_TO_SID.get(provider.strip().lower(), provider)

    def _h_data_acquisition(job):
        sid = _resolve_sid(job.payload)
        if sid in ("norgate_local", "norgate"):
            return _norgate_backfill((job.payload or {}).get("universe")
                                     or bounded_universe,
                                     "stage6_norgate_acquisition")
        return _collect(sid)

    def _h_coverage_repair(job):
        univ = (job.payload or {}).get("universe") or repair_universe
        return _norgate_backfill(univ, "stage6_coverage_repair")

    def _read_normalized(record_type, *, limit=3000):
        base = Path(ingestion_root) / "normalized" / record_type
        out = []
        if not base.exists():
            return out
        for f in sorted(base.rglob("*.jsonl"))[:limit]:
            try:
                for line in f.read_text(encoding="utf-8-sig").splitlines():
                    line = line.strip()
                    if line:
                        try:
                            out.append(_json.loads(line))
                        except ValueError:
                            pass
            except OSError:
                pass
        return out

    def _analyst_vintage_evidence():
        """Disk-truth evidence for the eodhd_analyst immutable daily vintage:
        first-snapshot PIT floor + today's persisted vintage files."""
        from datetime import datetime as _dt2, timezone as _tz2
        vroot = Path(ingestion_root) / "vintages" / "eodhd_analyst"
        today = _dt2.now(_tz2.utc).date().isoformat()
        boundary = None
        bpath = vroot / "_prospective_boundary.json"
        if bpath.exists():
            try:
                boundary = _json.loads(bpath.read_text(encoding="utf-8-sig")).get(
                    "first_snapshot_date")
            except (OSError, ValueError):
                boundary = None
        tdir = vroot / today
        vintages_today = sorted(p.name for p in tdir.glob("*.json")) \
            if tdir.exists() else []
        all_dates = sorted(p.name for p in vroot.glob("*") if p.is_dir()) \
            if vroot.exists() else []
        return {"first_snapshot_boundary": boundary, "snapshot_date_today": today,
                "vintage_files_today": vintages_today,
                "vintage_count_today": len(vintages_today),
                "snapshot_dates_on_disk": all_dates,
                "vintage_root": str(vroot)}

    def _h_prospective(job):
        # WS1: analyst estimate revisions / price targets / estimate counts are
        # ENTITLED-as-snapshot and are collected via the REAL eodhd_analyst
        # forward-vintage collector (immutable daily vintage; availability =
        # capture date; never backfilled before the first snapshot). Any other
        # prospective family falls back to the entitled EODHD forward EARNINGS
        # CALENDAR snapshot. Read-only w.r.t. operational trading state.
        from datetime import datetime as _datetime, timezone as _tz
        payload = job.payload or {}
        field = str(payload.get("field") or "")
        sid = payload.get("collector_source_id")
        analyst_fields = {"analyst_estimate_revisions", "price_targets",
                          "estimate_counts"}
        if sid == "eodhd_analyst" or field in analyst_fields \
                or "analyst" in field or "eodhd_analyst" in str(job.lane):
            outcome, detail = _collect("eodhd_analyst")
            detail = {**detail, "real_work": "eodhd_analyst_forward_vintage",
                      "prospective_family": field or "analyst_estimate_revisions",
                      "classification": "PROSPECTIVE_COLLECTION_ACTIVE",
                      **_analyst_vintage_evidence()}
            # An idempotent same-day re-run (all vintages already written) is a
            # COMPLETED no-op, not a block.
            if outcome == _ar.OUTCOME_BLOCKED_SPECIFIC and detail.get(
                    "vintage_count_today"):
                detail["note"] = ("all same-day vintages already written; "
                                  "idempotent no-op")
                detail["up_to_date"] = True
                return _ar.OUTCOME_COMPLETED, detail
            return outcome, detail

        recs = _read_normalized("EARNINGS_EVENT", limit=2000)
        dates = []
        for r in recs:
            pay = r.get("normalized_payload") or {}
            d = r.get("effective_at") or pay.get("report_date") or pay.get("Date")
            if d:
                dates.append(str(d)[:10])
        dates = sorted(set(dates))
        today = _datetime.now(_tz.utc).date().isoformat()
        forward = [d for d in dates if d >= today]
        detail = {"real_work": "prospective_earnings_calendar_snapshot",
                  "prospective_family": "eodhd_earnings_calendar (entitled)",
                  "earnings_events_snapshotted": len(recs),
                  "distinct_event_dates": len(dates),
                  "forward_dated_events": len(forward),
                  "calendar_date_min": dates[0] if dates else None,
                  "calendar_date_max": dates[-1] if dates else None,
                  "pit_floor": today,
                  "pit_floor_note": "first-snapshot date is the hard PIT floor; "
                                    "never backfilled before it",
                  "not_entitled_prospective": ["analyst_estimate_revisions",
                                               "price_targets", "estimate_counts"]}
        if not recs:
            detail["reason"] = "no entitled forward calendar available to snapshot"
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    # ---- Stage 5/7 REAL price-factor experiment engine ----------------------
    def _campaign(features, *, cost_grid=None):
        from . import experiment_runner as _er
        from . import experiment_contracts as _ec
        panel = _panel()
        if not panel:
            return None, None, {"reason": "no price panel (no MARKET_BAR data)"}
        specs = [s for s in _er.default_campaign_specs()
                 if s.get("feature") in features]
        res = _er.run_price_factor_campaign(
            panel, specs=specs or None, gates=_ec.DEFAULT_GATES,
            cost_grid=cost_grid or [5, 10, 25, 50], cost_bps=10.0,
            bounds={"max_experiments": 12})
        return res, panel, None

    def _sector_map(symbols):
        """Current GICS Sector (level 1) per symbol from the OWNED Norgate
        classification surface (survivorship-safe universe). Cached. Current-only
        (Norgate exposes no dated classification series), so a financials
        exclusion built from it carries a small, documented reclassification
        look-ahead — sector membership (financials vs not) is ~stable."""
        if "sectors" not in _cache:
            smap = {}
            try:
                import norgatedata as _nd  # noqa: PLC0415 - local vendor import
                for s in symbols:
                    try:
                        smap[s] = _nd.classification_at_level(
                            s, "GICS", "Name", 1)
                    except Exception:  # noqa: BLE001
                        smap[s] = None
            except Exception:  # noqa: BLE001 - NDU down / package absent
                smap = {}
            _cache["sectors"] = smap
        return _cache["sectors"]

    def _campaign_filtered(features, keep, *, cost_grid=None):
        """Same bounded price-factor campaign as ``_campaign`` but over a panel
        filtered by ``keep(ticker) -> bool`` (e.g. drop financials)."""
        from . import experiment_runner as _er
        from . import experiment_contracts as _ec
        panel = _panel()
        if not panel:
            return None, None, {"reason": "no price panel (no MARKET_BAR data)"}
        fpanel = {t: b for t, b in panel.items() if keep(t)}
        if not fpanel:
            return None, None, {"reason": "filter removed the entire universe"}
        specs = [s for s in _er.default_campaign_specs()
                 if s.get("feature") in features]
        res = _er.run_price_factor_campaign(
            fpanel, specs=specs or None, gates=_ec.DEFAULT_GATES,
            cost_grid=cost_grid or [5, 10, 25, 50], cost_bps=10.0,
            bounds={"max_experiments": 12})
        return res, fpanel, None

    def _rows(res, keys):
        out = []
        for r in (res.get("results") or []):
            out.append({k: r.get(k) for k in keys})
        return out

    _EXP_KEYS = ("feature", "label", "periods", "rank_ic_mean", "rank_ic_t",
                 "decile_spread_mean", "spread_t", "net_annualized_return",
                 "beats_null_control", "decision")
    _ROB_KEYS = ("feature", "periods", "rank_ic_t", "subperiod_consistency",
                 "regime_consistency", "max_drawdown", "turnover",
                 "cost_flips_sign", "cost_erosion_ratio", "decision")

    def _event_experiment(job):
        """WS2/WS3: insider (Form 4 transaction) and earnings (8-K Item 2.02 /
        EODHD) EVENT experiment lanes over the REAL normalized records. Reports
        honest coverage; runs a cross-sectional event study only when coverage is
        sufficient, else returns a COMPLETED DATA_HOLD with exact counts (the
        acquisition + extraction are proven; coverage grows as the bounded CIK set
        widens)."""
        family = str((job.payload or {}).get("family") or "")
        if "insider" in family:
            recs = _read_normalized("INSIDER_FILING", limit=4000)
            txn = [r for r in recs
                   if (r.get("normalized_payload") or {}).get("transaction_code")
                   and (r.get("normalized_payload") or {}).get("shares") is not None]
            issuers = {r.get("company_id") for r in txn if r.get("company_id")}
            min_issuers = int((cfg.get("autonomy") or {}).get(
                "event_min_issuers", 30))
            detail = {"real_work": "insider_event_study", "family": family,
                      "insider_transaction_records": len(txn),
                      "distinct_issuers": len(issuers),
                      "min_issuers_for_experiment": min_issuers,
                      "pit_basis": "SEC Form 4 acceptance timestamp"}
            if len(issuers) < min_issuers:
                detail["disposition"] = "DATA_HOLD"
                detail["reason"] = (
                    "insider transaction-level coverage insufficient for a cross-"
                    "sectional event study (%d issuers < %d); Form 4 XML "
                    "transaction extraction is proven and PIT-correct, coverage "
                    "grows as the bounded CIK set widens" % (len(issuers),
                                                             min_issuers))
                return _ar.OUTCOME_COMPLETED, detail
            detail["disposition"] = "COVERAGE_SUFFICIENT"
            return _ar.OUTCOME_COMPLETED, detail
        if any(k in family for k in ("earnings", "drift", "surprise",
                                     "filing", "corporate")):
            recs = _read_normalized("EARNINGS_EVENT", limit=4000)
            item202 = [r for r in recs if r.get("event_type") == "8-K_ITEM_2.02"]
            eodhd_earn = [r for r in recs if r.get("event_type") == "EARNINGS_REPORT"]
            with_eps = [r for r in item202
                        if (r.get("normalized_payload") or {}).get("eps_actual")
                        is not None]
            min_events = int((cfg.get("autonomy") or {}).get(
                "event_min_events", 30))
            usable = len(item202) + len(eodhd_earn)
            detail = {"real_work": "earnings_surprise_pead_event_study",
                      "family": family,
                      "sec_8k_item202_events": len(item202),
                      "item202_with_inline_eps": len(with_eps),
                      "eodhd_earnings_events": len(eodhd_earn),
                      "usable_events": usable,
                      "min_events_for_experiment": min_events,
                      "pit_basis": "SEC 8-K acceptance timestamp / EODHD report date"}
            if usable < min_events:
                detail["disposition"] = "DATA_HOLD"
                detail["reason"] = (
                    "earnings-surprise / PEAD coverage insufficient (%d usable "
                    "events < %d); 8-K Item 2.02 extraction is proven and PIT-"
                    "correct, coverage grows as the bounded CIK set widens"
                    % (usable, min_events))
                return _ar.OUTCOME_COMPLETED, detail
            detail["disposition"] = "COVERAGE_SUFFICIENT"
            return _ar.OUTCOME_COMPLETED, detail
        return None  # not an event family — fall through to the price campaign

    def _stage9_4_price_experiment(job, spec, feature):
        """Stage 9.4 release closure: evaluate ONE pre-registered NEW price factor
        on the SAME owned Norgate panel and return an ingestible single-feature
        Stage 5 row (the canonical CAT_EXPERIMENT path for a tournament
        revalidation). Read-only; never promotes, never mutates operational
        state."""
        from . import price_factors as _pf94
        panel = _panel()
        if not panel:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "real_work": "stage9_4_price_factor_experiment",
                "feature": feature, "reason": "no price panel (no MARKET_BAR data)"}
        hz = int((spec or {}).get("horizon_days") or 21)
        rb = (spec or {}).get("rebalance") or "monthly"
        try:
            nf = _pf94.run_new_price_factor_campaign(
                panel, features=[feature], horizon_days=hz, rebalance=rb)
        except Exception as exc:  # noqa: BLE001 - isolate a single bad factor
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "real_work": "stage9_4_price_factor_experiment", "feature": feature,
                "reason": "compute_error:%s" % type(exc).__name__}
        rows = [r for r in nf.get("results", []) if r.get("feature") == feature]
        row = rows[0] if rows else {}
        # Persist a COMPACT row: drop the transient heavy per-period series /
        # market features - the ingester only needs the scalar metrics + rank_ic_t.
        compact = {k: v for k, v in row.items()
                   if k not in ("_periods_series", "market_features")}
        detail = {"real_work": "stage9_4_price_factor_experiment",
                  "feature": feature, "horizon_days": hz, "rebalance": rb,
                  "candidate_id": (job.payload or {}).get("candidate_id"),
                  "revalidation_of": (spec or {}).get("revalidation_of"),
                  "panel_symbols": len(panel), "panel_start": panel_start,
                  "row": compact, "decision": row.get("decision"),
                  "no_automatic_promotion": True}
        if compact.get("rank_ic_t") is None:
            detail["reason"] = ("insufficient owned history for %s at horizon %d"
                                % (feature, hz))
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    def _cik_ticker_map(records):
        """cik (unpadded, as the PIT store keys it) -> ticker, from owned
        RT_FUNDAMENTAL_FACT records."""
        out = {}
        for r in records:
            p = r.get("normalized_payload") or {}
            cik = str(p.get("cik") or r.get("company_id") or "").strip()
            tkr = r.get("ticker") or p.get("ticker")
            if cik and tkr:
                out.setdefault(cik, tkr)
        return out

    def _historical_c2t(id_store):
        """Stage 10.2: cik (unpadded, as the PIT store keys it) -> effective
        ticker for EVERY ACTIVE RESOLVED security in the identity store — the
        survivorship-safe historical universe (current + delisted). Read-only."""
        out: dict = {}
        if id_store is None:
            return out
        try:
            from . import historical_identity as _hi102
            for s in id_store.list_securities(limit=1000000):
                mp = id_store.active_mapping(s["security_id"])
                if not (mp and mp.get("status") == _hi102.STATUS_RESOLVED
                        and mp.get("cik")):
                    continue
                cik = _hi102.norm_cik(mp["cik"])  # zero-padded, matches index PIT
                tkr = s.get("ticker")
                if cik and tkr:
                    out.setdefault(cik, tkr)
        except Exception:  # noqa: BLE001 - c2t build never fatal
            return out
        return out

    def _stage9_5_fundamental_experiment(job, spec, feature):
        """Stage 9.5: evaluate ONE pre-registered PIT FUNDAMENTAL signal on the
        owned point-in-time companyfacts store + the owned survivorship-safe price
        panel, returning an ingestible Stage 5 row (the canonical CAT_EXPERIMENT
        path for a fundamental candidate). Only reaches a COMPLETED (ingestible)
        result once owned coverage passes the readiness gate; below it the job is
        BLOCKED_SPECIFIC so the candidate stays an honest DATA_HOLD (never a fake
        evidence row). Read-only; never promotes, never mutates operational
        state."""
        from . import fundamental_signals as _fsig
        from . import fundamental_evidence as _fev
        from . import fundamental_readiness as _fr
        from . import sec_companyfacts as _cf
        # Stage 9.5B SAFETY SWITCH (Blocker 7): refuse to evaluate a HISTORICAL
        # fundamental experiment until a survivorship-safe historical ticker->CIK
        # mapping AND the config flag both pass. Under OPTION B this returns
        # BLOCKED_SPECIFIC with the single diagnostic, so a candidate never
        # transitions on current-survivor historical results (even if a job was
        # somehow enqueued).
        try:
            from . import tournament as _tt95
            _p9 = resolve_stage9_config_path(cfg)
            _cfg9 = _tt95.load_config(_p9) if _p9 else {}
        except Exception:  # noqa: BLE001 - config resolution is best-effort
            _cfg9 = {}
        # Stage 10: consult the MEASURED identity store (never the hand-set config
        # flag). With no measured survivorship-safe historical mapping coverage
        # the gate stays refused and the single diagnostic stands — identical
        # outcome to OPTION B today, but now driven by measured coverage that
        # would unlock AUTOMATICALLY once the historical contract genuinely passes.
        _id_store = None
        try:
            if stage10_identity_cfg(cfg).get("enabled"):
                _id_store = build_identity_store(cfg)
        except Exception:  # noqa: BLE001 - store access never fatal
            _id_store = None
        # Stage 10.2: build the leakage-safe HISTORICAL PIT store from the owned
        # bulk companyfacts index for the survivorship-safe resolved universe
        # (current + delisted) so the gate AND the evaluation run on genuine
        # historical PIT depth — not the thin current-universe facts. Absent it
        # (index not built / no facts), the gate stays closed and the candidate
        # stays an honest DATA_HOLD.
        _hist_store = None
        try:
            if stage10_2_cfg(cfg).get("enabled") and _id_store is not None:
                _hist_store = _fr.open_companyfacts_pit_store(
                    _cfg9, store=_id_store)
        except Exception:  # noqa: BLE001 - never fatal; gate stays closed
            _hist_store = None
        _hg = _fr.historical_fundamental_experiment_allowed(
            _cfg9, readiness=None, store=_id_store, signal=feature,
            pit_store=_hist_store)
        if not _hg["allowed"]:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "real_work": "stage9_5_fundamental_experiment",
                "feature": feature, "no_automatic_promotion": True,
                "disposition": "DATA_HOLD",
                "historical_fundamental_universe_ready": False,
                "diagnostic": _hg["diagnostic"],
                "activation_mode": _hg.get("activation_mode"),
                "historical_evaluation_enabled": _hg[
                    "historical_evaluation_enabled"],
                "historical_mapping_available": _hg["mapping_available"],
                "reason": ("Stage 9.5B historical fundamental evaluation not "
                           "activated/ready: %s; candidate stays DATA_HOLD - "
                           "no transition on current-survivor results"
                           % (_hg["reason"] or _hg["diagnostic"]))}
        if _hist_store is not None:
            store = _hist_store
            recs = None
            c2t = _historical_c2t(_id_store)
        else:
            recs = _read_normalized("FUNDAMENTAL_FACT", limit=6000)
            store = _cf.owned_pit_store(recs)
            c2t = _cik_ticker_map(recs)
        min_ciks = int((cfg.get("autonomy") or {}).get(
            "pit_fundamentals_min_ciks", 50))
        min_periods = 12
        ready_ciks = len(store.ciks_with_candidate(feature))
        base = {"real_work": "stage9_5_fundamental_experiment",
                "feature": feature, "no_automatic_promotion": True,
                "ciks_with_all_required_concepts": ready_ciks,
                "required_ciks": min_ciks,
                "missing_concepts": store.missing_concepts(feature)}
        if ready_ciks < min_ciks:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "disposition": "DATA_HOLD",
                "reason": ("owned PIT companyfacts insufficient for %s (%d CIKs "
                           "with full concept set < %d); the live sec_companyfacts "
                           "campaign grows coverage - candidate stays DATA_HOLD"
                           % (feature, ready_ciks, min_ciks))}
        panel = _panel()
        if not panel:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "reason": "no price panel (no MARKET_BAR data)"}
        hz = int((spec or {}).get("horizon_days") or 63)
        try:
            row = _fev.evaluate_fundamental_evidence(
                store, panel, feature, cik_to_ticker=c2t, horizon_days=hz)
        except Exception as exc:  # noqa: BLE001 - isolate one bad signal
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "reason": "compute_error:%s" % type(exc).__name__}
        periods = int(row.get("periods") or 0)
        if row.get("rank_ic_t") is None or periods < min_periods:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "disposition": "DATA_HOLD", "periods": periods,
                "reason": ("owned PIT + price coverage yields only %d scored "
                           "periods (< %d); candidate stays DATA_HOLD"
                           % (periods, min_periods))}
        return _ar.OUTCOME_COMPLETED, {
            **base, "horizon_days": hz, "panel_symbols": len(panel),
            "row": row, "decision": row.get("decision"),
            "pit_basis": row.get("pit_basis")}

    def _h_experiment(job):
        payload = job.payload or {}
        if str(payload.get("kind")) == "event":
            ev = _event_experiment(job)
            if ev is not None:
                return ev
        # Stage 9.4: a pre-registered NEW price-factor revalidation the tournament
        # generated is evaluated by THIS production handler (not a legacy feature).
        _spec = payload.get("spec") or {}
        _feat = _spec.get("feature") or payload.get("feature")
        if payload.get("tournament") and _feat:
            from . import price_factors as _pf94
            if _feat in _pf94.COMPUTABLE_FEATURES:
                return _stage9_4_price_experiment(job, _spec, _feat)
            # Stage 9.5: a pre-registered PIT FUNDAMENTAL experiment.
            from . import fundamental_signals as _fsig95
            if _feat in _fsig95.SIGNALS:
                return _stage9_5_fundamental_experiment(job, _spec, _feat)
        res, panel, err = _campaign({"residual_momentum", "momentum_accel",
                                     "vol_scaled_momentum", "short_term_reversal",
                                     "low_volatility"})
        if err:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {"real_work": "stage5_price_factor_experiment", **err}
        detail = {"real_work": "stage5_price_factor_experiment",
                  "panel_symbols": len(panel), "panel_start": panel_start,
                  "experiments_completed": res.get("experiments_completed"),
                  "keep_for_research": res.get("keep_for_research"),
                  "null_control_rank_ic_t":
                      (res.get("plan") or {}).get("null_control_rank_ic_t"),
                  "results": _rows(res, _EXP_KEYS)}
        if not res.get("experiments_completed"):
            detail["reason"] = "no experiment reached min periods"
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    def _h_robustness(job):
        res, panel, err = _campaign({"residual_momentum"},
                                    cost_grid=[5, 10, 25, 50, 100])
        if err:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {"real_work": "stage5_robustness_cost_regime", **err}
        detail = {"real_work": "stage5_robustness_cost_regime",
                  "panel_symbols": len(panel),
                  "cost_grid_bps": [5, 10, 25, 50, 100],
                  "results": _rows(res, _ROB_KEYS)}
        if not res.get("results"):
            detail["reason"] = "robustness produced no result"
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    def _h_combination(job):
        res, panel, err = _campaign({"combo_mom_lowvol", "residual_momentum",
                                     "low_volatility"})
        if err:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {"real_work": "stage5_signal_combination", **err}
        detail = {"real_work": "stage5_signal_combination",
                  "panel_symbols": len(panel),
                  "note": "orthogonal momentum+low-vol combo vs its components",
                  "results": _rows(res, _EXP_KEYS)}
        if not res.get("results"):
            detail["reason"] = "combination produced no result"
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    def _h_telegram(job):
        req = str((job.payload or {}).get("request") or "").lower()
        feature = "residual_momentum"
        if "reversal" in req:
            feature = "short_term_reversal"
        elif "combo" in req or "combination" in req:
            feature = "combo_mom_lowvol"
        elif "low" in req and "vol" in req:
            feature = "low_volatility"
        base = {"real_work": "stage5_price_factor_experiment_from_telegram",
                "request": (job.payload or {}).get("request"),
                "mapped_feature": feature}
        exclude_financials = ("financ" in req and
                              ("exclud" in req or "ex-" in req or "without" in req
                               or "drop" in req or "no " in req))

        if exclude_financials:
            # REAL financials exclusion using the OWNED Norgate GICS Sector
            # classification (level 1) over the survivorship-safe panel. This
            # replaces the former permanent DATA_HOLD: the experiment runs on the
            # ex-financials cross-section and returns a real, caveated result.
            full = _panel()
            smap = _sector_map(list(full.keys())) if full else {}
            def _keep(t):
                return str(smap.get(t) or "").strip().lower() != "financials"
            n_fin = sum(1 for t in (full or {})
                        if str(smap.get(t) or "").strip().lower() == "financials")
            classified = sum(1 for t in (full or {}) if smap.get(t))
            if not smap or classified == 0:
                # Norgate classification genuinely unavailable this run — honest
                # hold (NOT permanent: it clears when NDU classification returns).
                res, panel, err = _campaign({feature})
                if err:
                    return _ar.OUTCOME_BLOCKED_SPECIFIC, {**base, **err}
                detail = {**base, "panel_symbols": len(panel),
                          "results": _rows(res, _EXP_KEYS),
                          "sector_exclusion_note":
                              "Norgate GICS classification was unavailable this "
                              "cycle; reported the market-neutral RESIDUAL "
                              "momentum factor (leakage-safe) instead. Retries "
                              "with the ex-financials universe when NDU "
                              "classification is reachable."}
                return _ar.OUTCOME_COMPLETED, detail
            res, panel, err = _campaign_filtered({feature}, _keep)
            if err:
                return _ar.OUTCOME_BLOCKED_SPECIFIC, {**base, **err}
            # WS5: build the leakage-safe PIT-SIC series from contemporaneous SEC
            # ASSIGNED-SIC records and run the three-way comparison. The current-
            # GICS exclusion is relabelled PROVISIONAL_CLASSIFICATION_LOOKAHEAD;
            # only the PIT-SIC variant is leakage-safe research evidence.
            from . import pit_sector as _ps
            sic_recs = [r for r in _read_normalized("SECURITY_IDENTITY", limit=4000)
                        if r.get("event_type") == "ASSIGNED_SIC_PIT"]
            pit_series = _ps.PitSicSeries.from_records(sic_recs)
            today = _datetime.now(_tz.utc).date().isoformat()
            comparison = _ps.three_way_financials_comparison(
                symbols=list(full.keys()), current_gics=smap,
                pit_series=pit_series, as_of=today)
            detail = {**base, "universe": "ex_financials",
                      "panel_symbols_full": len(full),
                      "panel_symbols_ex_financials": len(panel),
                      "financials_excluded": n_fin,
                      "symbols_classified": classified,
                      "sector_source": "Norgate GICS Sector (classification_at_"
                                       "level, level 1), survivorship-safe",
                      "evidence_classification": "PROVISIONAL_CLASSIFICATION_"
                                                 "LOOKAHEAD",
                      "point_in_time_caveat":
                          "The reported ex-financials result uses CURRENT Norgate "
                          "GICS (undated) — a classification look-ahead. It is "
                          "PROVISIONAL_CLASSIFICATION_LOOKAHEAD, NOT leakage-free. "
                          "The leakage-safe lane is the PIT-SIC comparison below "
                          "(contemporaneous SEC ASSIGNED-SIC).",
                      "three_way_sector_comparison": comparison,
                      "pit_sic_coverage": comparison["variants"]["pit_sic"],
                      "results": _rows(res, _EXP_KEYS)}
            if not res.get("results"):
                detail["reason"] = "no experiment reached min periods after "\
                                   "excluding financials"
                return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
            return _ar.OUTCOME_COMPLETED, detail

        # No sector exclusion requested — plain full-universe factor experiment.
        res, panel, err = _campaign({feature})
        if err:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {**base, **err}
        detail = {**base, "panel_symbols": len(panel),
                  "results": _rows(res, _EXP_KEYS)}
        if not res.get("results"):
            detail["reason"] = "no experiment reached min periods"
            return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
        return _ar.OUTCOME_COMPLETED, detail

    # ---------------------------------------------------------------------- #
    # WS2/WS3/WS4/WS5/WS7 — acceptance-vs-production run modes + durable, sharded
    # FULL-UNIVERSE acquisition campaigns. A per-job batch size is permitted; a
    # permanent total-universe cap is not. Each completed batch enqueues the next
    # (autonomous continuation); the never-idle planner also keeps one live batch
    # job per incomplete campaign, so a report never terminates a campaign.
    # ---------------------------------------------------------------------- #
    from . import production_universe as _pu
    from datetime import datetime as _dt3, timezone as _tz3

    _prod = cfg.get("production") if isinstance(cfg.get("production"), dict) \
        else {}
    _run_mode = _pu.resolve_run_mode(cfg)
    _defs_by_id = {d["id"]: d for d in resolve_campaign_defs(cfg)}

    def _camp_store():
        if "camp_store" not in _cache:
            _cache["camp_store"] = stage8_campaign_store(cfg)
        return _cache["camp_store"]

    def _universe(scope=_pu.SCOPE_SURVIVORSHIP):
        ck = "universe_%s" % scope
        if ck not in _cache:
            _cache[ck] = _pu.resolve_universe(
                cfg, mode=_run_mode, scope=scope,
                fallback_symbols=lambda: sorted((_panel() or {}).keys()))
        return _cache[ck]

    def _today():
        return _dt3.now(_tz3.utc).date().isoformat()

    def _enqueue_next_batch(cid, category, seq, priority):
        """Autonomous continuation (WS7): enqueue the NEXT batch as a distinct
        job (seq-tagged so it is never deduped against the finished one)."""
        if queue is None:
            return None
        try:
            return queue.enqueue(category, lane="acq.%s" % cid,
                                 payload={"campaign": cid, "seq": int(seq)},
                                 priority=int(priority),
                                 origin="campaign-continuation")
        except Exception:  # noqa: BLE001 — continuation must never crash a job
            return None

    def _ensure_campaign(cid, kind, batch_size, scope=_pu.SCOPE_SURVIVORSHIP):
        univ = _universe(scope)
        store = _camp_store()
        store.ensure_campaign(cid, kind=kind, universe=univ.symbols,
                              universe_source=univ.source,
                              batch_size=batch_size, run_mode=_run_mode,
                              universe_fingerprint=univ.fingerprint)
        return store, univ

    def _ensure_campaign_revision(base_id, kind, batch_size,
                                  scope=_pu.SCOPE_CURRENT):
        """Stage 9.5 Blocker 6: scope-aware, APPEND-ONLY campaign resolution. A
        genuine universe-scope change SUPERSEDES the prior campaign (never deletes
        its rows) and activates a new linked revision; the normal same-scope path
        reconciles the ACTIVE revision. Returns ``(store, univ, active_cid)``."""
        univ = _universe(scope)
        store = _camp_store()
        rev = store.ensure_campaign_revision(
            base_id, kind=kind, universe=univ.symbols,
            universe_source=univ.source, batch_size=batch_size,
            run_mode=_run_mode, universe_fingerprint=univ.fingerprint)
        return store, univ, rev["campaign_id"]

    def _campaign_base(cid, univ, cov):
        return {"campaign": cid, "run_mode": _run_mode,
                "production_universe_source": univ.source,
                "full_universe_target_count":
                    cov.get("full_universe_target_count"),
                "completed_symbol_count": cov.get("completed_symbol_count"),
                "remaining_symbol_count": cov.get("remaining_symbol_count"),
                "repair_backlog_count": cov.get("repair_backlog_count"),
                "permanent_failed_count": cov.get("permanent_failed_count"),
                "per_job_symbol_batch_size":
                    cov.get("per_job_symbol_batch_size"),
                "acquisition_cursor": cov.get("acquisition_cursor"),
                "campaign_complete": cov.get("is_complete"),
                "coverage_reconciles": cov.get("reconciles"),
                "universe_degraded": univ.degraded}

    # ---- EODHD analyst forward-vintage campaign (WS4) ---------------------- #
    def _run_analyst_campaign(job):
        d = _defs_by_id["eodhd_analyst"]
        # Forward analyst vintages are eligible only for CURRENTLY-LISTED names
        # (a delisted company has no future analyst data). This is a correct
        # eligibility criterion, not a total-universe cap — the price/filing
        # campaigns still sweep the full survivorship-safe universe.
        store, univ = _ensure_campaign(
            "eodhd_analyst", "analyst_vintage", d["batch_size"],
            scope=d.get("universe_scope", _pu.SCOPE_CURRENT))
        batch = store.next_batch("eodhd_analyst", batch_size=d["batch_size"])
        if not batch:
            cov = store.coverage("eodhd_analyst")
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base("eodhd_analyst", univ, cov),
                "real_work": "eodhd_analyst_forward_vintage_campaign",
                "note": "no claimable symbols (complete or empty universe)",
                **_analyst_vintage_evidence()}
        eod = [s if "." in s else "%s.US" % s for s in batch]
        _collect_batch("eodhd_analyst", eod, symbol_keys=["sample_symbols"])
        vdir = Path(ingestion_root) / "vintages" / "eodhd_analyst" / _today()
        succeeded = [s for s in batch if (vdir / ("%s.json" % s)).exists()]
        failed = [s for s in batch if s not in set(succeeded)]
        cov = store.record_results(
            "eodhd_analyst", succeeded=succeeded,
            failed=[(s, "no vintage written this snapshot") for s in failed])
        _enqueue_next_batch("eodhd_analyst", d["category"],
                            cov.get("acquisition_cursor", 0), job.priority)
        return _ar.OUTCOME_COMPLETED, {
            **_campaign_base("eodhd_analyst", univ, cov),
            "real_work": "eodhd_analyst_forward_vintage_campaign",
            "classification": "PROSPECTIVE_COLLECTION_ACTIVE",
            "batch_symbols": len(batch), "succeeded_symbols": len(succeeded),
            "failed_symbols": len(failed),
            "next_batch_enqueued": bool(queue) and not cov.get("is_complete"),
            **_analyst_vintage_evidence()}

    # ---- Norgate survivorship-free price campaign (WS3) -------------------- #
    def _run_norgate_campaign(job):
        d = _defs_by_id["norgate_prices"]
        store, univ = _ensure_campaign("norgate_prices",
                                       "norgate_price_backfill", d["batch_size"])
        batch = store.next_batch("norgate_prices", batch_size=d["batch_size"])
        if not batch:
            cov = store.coverage("norgate_prices")
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base("norgate_prices", univ, cov),
                "real_work": "norgate_price_campaign",
                "note": "no claimable symbols"}
        outcome, detail = _norgate_backfill(batch, "norgate_price_campaign")
        if outcome == _ar.OUTCOME_BLOCKED_SPECIFIC:
            cov = store.record_results(
                "norgate_prices",
                failed=[(s, detail.get("reason")) for s in batch])
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **detail, **_campaign_base("norgate_prices", univ, cov)}
        cov = store.record_results("norgate_prices", succeeded=batch)
        _enqueue_next_batch("norgate_prices", d["category"],
                            cov.get("acquisition_cursor", 0), job.priority)
        return _ar.OUTCOME_COMPLETED, {
            **detail, **_campaign_base("norgate_prices", univ, cov),
            "batch_symbols": len(batch),
            "next_batch_enqueued": bool(queue) and not cov.get("is_complete")}

    # ---- SEC owned-record counters (Part 3 readiness + batch metrics) ------ #
    def _sec_owned_counts():
        """Cumulative owned SEC Form 4 / 8-K counts from normalized records -
        the shared basis for the weakest-gate readiness measure (Part 3) and the
        per-batch metric snapshot (Part 2). Bounded read; no network; PIT-safe
        (availability = SEC acceptance)."""
        from collections import defaultdict as _dd

        def _p(r):
            return r.get("normalized_payload") or {}

        def _fdate(r):
            p = _p(r)
            v = (p.get("filing_date") or p.get("acceptance_datetime")
                 or r.get("available_at"))
            return str(v)[:10] if v else None
        insider = _read_normalized("INSIDER_FILING", limit=6000)
        txn = [r for r in insider if _p(r).get("transaction_code")
               and _p(r).get("shares") is not None]
        codes = [str(_p(r).get("transaction_code") or "").upper() for r in txn]
        issuers = {r.get("company_id") for r in txn if r.get("company_id")}
        fdates = sorted({_fdate(r) for r in txn if _fdate(r)})
        buyers = _dd(set)
        for r in txn:
            if str(_p(r).get("transaction_code") or "").upper() != "P":
                continue
            iss = r.get("company_id")
            owner = (_p(r).get("owner_cik") or _p(r).get("owner_name")
                     or _p(r).get("reporting_owner") or _p(r).get("owner"))
            if iss:
                buyers[iss].add(owner)
        clustered = sum(1 for s in buyers.values() if len(s) >= 2)
        earn = _read_normalized("EARNINGS_EVENT", limit=6000)
        item202 = [r for r in earn if r.get("event_type") == "8-K_ITEM_2.02"]
        return {
            "insider_transaction_records": len(txn),
            "open_market_purchases": sum(1 for c in codes if c == "P"),
            "sales": sum(1 for c in codes if c == "S"),
            "distinct_issuers": len(issuers),
            "distinct_filing_dates": len(fdates),
            "coverage_start": fdates[0] if fdates else None,
            "coverage_end": fdates[-1] if fdates else None,
            "clustered_purchase_events": clustered,
            "sec_8k_item202_events": len(item202)}

    def _sec_batch_metrics_snapshot():
        try:
            c = _sec_owned_counts()
        except Exception:  # noqa: BLE001 - per-batch metrics are best-effort
            return {}
        return {"issuers_covered": c.get("distinct_issuers"),
                "form4_filings": c.get("insider_transaction_records"),
                "open_market_purchases": c.get("open_market_purchases"),
                "sales": c.get("sales"),
                "eightk_2_02_events": c.get("sec_8k_item202_events")}

    # ---- SEC Form 4 / 8-K full-CIK-universe campaign (WS5) ----------------- #
    # Stage 9.3 CONTROLLED CONTINUATION. Before doing ANY SEC work the handler
    # evaluates three durable, restart-safe stop conditions derived from the
    # append-only batch log: campaign complete, the per-UTC-day completed-batch
    # cap, and the consecutive-no-progress limit. When a stop condition holds it
    # declines to run a batch and does NOT enqueue a continuation, so the chain
    # halts cleanly (the weakest-gate handler can re-trigger it next day). A
    # progress batch chains the next continuation ONLY while under the daily cap
    # and incomplete; a no-progress (blocked) batch never chains ("do not enqueue
    # another continuation when no progress was made"). Idempotent + resumable.
    def _run_sec_cik_campaign(job):
        d = _defs_by_id["sec_form4_8k"]
        cid = "sec_form4_8k"
        store, univ = _ensure_campaign(cid, "sec_cik_filings", d["batch_size"])
        run_date = _today()
        origin = getattr(job, "origin", None)
        cov0 = store.coverage(cid)
        done_today = store.batches_on_date(cid, run_date)
        no_prog = store.consecutive_no_progress(cid)
        stop_reason = sec_continuation_stop_reason(
            is_complete=bool(cov0.get("is_complete")),
            completed_batches_today=done_today,
            consecutive_no_progress=no_prog,
            daily_cap=_sec_daily_cap, no_progress_max=_sec_noprog_max)
        if stop_reason:
            # Decline to run; DO NOT enqueue a continuation. The job COMPLETES
            # (it correctly determined the campaign must not continue now) and
            # records exactly why. No SEC network work is performed.
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base(cid, univ, cov0),
                "real_work": "sec_form4_8k_cik_campaign",
                "batch_executed": False, "stopped": True,
                "stop_reason": stop_reason, "origin": origin,
                "completed_batches_today": done_today,
                "daily_completed_batch_cap": _sec_daily_cap,
                "consecutive_no_progress_batches": no_prog,
                "max_consecutive_no_progress_batches": _sec_noprog_max,
                "next_batch_enqueued": False, **_sec_batch_metrics_snapshot()}
        batch = store.next_batch(cid, batch_size=d["batch_size"])
        if not batch:
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base(cid, univ, cov0),
                "real_work": "sec_form4_8k_cik_campaign",
                "batch_executed": False, "note": "no claimable symbols",
                "next_batch_enqueued": False}
        res = _collect_batch("sec_edgar", batch,
                             symbol_keys=["facts_sample_symbols"],
                             extra={"facts_symbol_limit": len(batch)})
        state = (res or {}).get("source_states", {}).get("sec_edgar")
        counts = (res or {}).get("counts") or {}
        new_recs = counts.get("normalized_records_new")
        dedup = (counts.get("normalized_records_deduped")
                 or counts.get("raw_objects_deduped") or 0)
        blocked = (res is None or bool((res or {}).get("blocked_sources"))
                   or "BLOCKED" in str((res or {}).get("status") or ""))
        # SAFE-TIMEOUT: the collector hit its cooperative wall-clock budget
        # (collect_time_budget_seconds) mid-batch. Do NOT record the batch as
        # succeeded, do NOT append a batch-log row, do NOT enqueue the next batch
        # - return RETRYABLE so the SAME batch resumes on a later cycle. Records
        # already collected are persisted idempotently; the cursor does not
        # advance, so no continuation is orphaned. Bounded by max_attempts.
        if bool((res or {}).get("source_deadlines", {}).get("sec_edgar")):
            return _ar.OUTCOME_RETRYABLE, {
                **_campaign_base(cid, univ, store.coverage(cid)),
                "real_work": "sec_form4_8k_cik_campaign",
                "reason": "SEC collect reached collect_time_budget_seconds "
                          "(deadline); partial batch NOT recorded; will resume",
                "deadline_reached": True, "batch_executed": False,
                "batch_ciks_queried": len(batch),
                "normalized_records_new": new_recs, "sec_source_state": state,
                "next_batch_enqueued": False}
        snap = _sec_batch_metrics_snapshot()
        if blocked and not new_recs:
            cov = store.record_results(
                cid, failed=[(s, "sec lane blocked/failed this batch")
                             for s in batch])
            store.record_batch(
                cid, run_date=run_date, progress=False, origin=origin,
                stop_reason="SOURCE_BLOCKED",
                metrics={"ciks_attempted": len(batch), "ciks_completed": 0,
                         "records_added": 0,
                         "remaining_backlog": cov.get("remaining_symbol_count"),
                         **snap})
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **_campaign_base(cid, univ, cov),
                "real_work": "sec_form4_8k_cik_campaign",
                "reason": "SEC CIK batch blocked: %s" % (res or {}).get("status"),
                "sec_source_state": state, "batch_executed": True,
                "progress": False, "next_batch_enqueued": False,
                "consecutive_no_progress_batches":
                    store.consecutive_no_progress(cid), **snap}
        # Progress batch: the cursor advances by the batch (backlog decreases).
        cov = store.record_results(cid, succeeded=batch)
        store.record_batch(
            cid, run_date=run_date, progress=True, origin=origin,
            metrics={"ciks_attempted": len(batch), "ciks_completed": len(batch),
                     "records_added": int(new_recs or 0),
                     "records_deduped": int(dedup or 0),
                     "remaining_backlog": cov.get("remaining_symbol_count"),
                     **snap})
        done_after = store.batches_on_date(cid, run_date)
        under_cap = done_after < _sec_daily_cap
        do_enqueue = queue is not None and sec_continuation_should_chain(
            is_complete=bool(cov.get("is_complete")),
            completed_batches_today=done_after, daily_cap=_sec_daily_cap)
        if do_enqueue:
            _enqueue_next_batch(cid, d["category"],
                                cov.get("acquisition_cursor", 0), job.priority)
        chain_stop = (None if do_enqueue else
                      ("CAMPAIGN_COMPLETE" if cov.get("is_complete")
                       else "DAILY_BATCH_CAP_REACHED" if not under_cap
                       else "NO_QUEUE"))
        return _ar.OUTCOME_COMPLETED, {
            **_campaign_base(cid, univ, cov),
            "real_work": "sec_form4_8k_cik_campaign",
            "sec_source_state": state, "batch_executed": True, "progress": True,
            "batch_ciks_queried": len(batch), "normalized_records_new": new_recs,
            "completed_batches_today": done_after,
            "daily_completed_batch_cap": _sec_daily_cap,
            "consecutive_no_progress_batches": 0,
            "continuation_stop_reason": chain_stop,
            "pit_basis": "SEC acceptance timestamp; amendments (4/A, 8-K/A) are "
                         "distinct append-only records",
            "next_batch_enqueued": do_enqueue, **snap}

    # ---- Stage 9.5 SEC companyfacts (XBRL fundamentals) campaign ----------- #
    # A dedicated, bounded, restart-safe LIVE companyfacts acquisition campaign.
    # It drives ONLY the proven sec_edgar collect_companyfacts lane (immutable
    # content-addressed RawArchive + RT_FUNDAMENTAL_FACT PIT facts) over the
    # production universe, with its OWN durable cursor and its OWN daily /
    # no-progress caps (companyfacts_continuation), independent of the Form 4 /
    # 8-K campaign. Same controlled-continuation contract as _run_sec_cik_campaign:
    # decline + do-not-chain when complete / daily-capped / stalled; a progress
    # batch chains ONE bounded continuation; a no-progress batch never chains. PIT
    # observations are materialized deterministically from the durable normalized
    # facts (a replay reproduces the same store); availability = SEC filed date.
    def _companyfacts_pit_store():
        from . import sec_companyfacts as _cf
        return _cf.owned_pit_store(_read_normalized("FUNDAMENTAL_FACT",
                                                    limit=6000))

    def _run_companyfacts_campaign(job):
        from . import sec_companyfacts as _cf
        base_id = "sec_companyfacts"
        d = _defs_by_id.get(base_id) or {"batch_size": _cf_batch,
                                         "category": _ar.CAT_DATA_ACQUISITION}
        bsize = _cf.clamp_batch_size(d.get("batch_size", _cf_batch),
                                     hard_max=_cf_max_batch, default=_cf_batch)
        # companyfacts resolve ONLY for current entities (SEC company_tickers.json
        # is current-only); the survivorship-safe delisted tickers have no current
        # CIK map, so the acquisition universe is the CURRENT constituents. Each
        # fact still carries its own SEC filed date = PIT availability. A genuine
        # scope change is handled by APPEND-ONLY supersession (Blocker 6) - never
        # a manual delete of mis-seeded campaign rows - and the runner always
        # operates on the resolved ACTIVE revision.
        _scope = d.get("universe_scope") or _pu.SCOPE_CURRENT
        store, univ, cid = _ensure_campaign_revision(
            base_id, "sec_companyfacts_xbrl", bsize, scope=_scope)
        run_date = _today()
        origin = getattr(job, "origin", None)
        cov0 = store.coverage(cid)
        done_today = store.batches_on_date(cid, run_date)
        no_prog = store.consecutive_no_progress(cid)
        stop_reason = sec_continuation_stop_reason(
            is_complete=bool(cov0.get("is_complete")),
            completed_batches_today=done_today,
            consecutive_no_progress=no_prog,
            daily_cap=_cf_daily_cap, no_progress_max=_cf_noprog_max)
        if stop_reason:
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base(cid, univ, cov0),
                "real_work": "sec_companyfacts_pit_campaign",
                "batch_executed": False, "stopped": True,
                "stop_reason": stop_reason, "origin": origin,
                "completed_batches_today": done_today,
                "daily_completed_batch_cap": _cf_daily_cap,
                "consecutive_no_progress_batches": no_prog,
                "max_consecutive_no_progress_batches": _cf_noprog_max,
                "next_batch_enqueued": False}
        batch = store.next_batch(cid, batch_size=bsize)
        if not batch:
            return _ar.OUTCOME_COMPLETED, {
                **_campaign_base(cid, univ, cov0),
                "real_work": "sec_companyfacts_pit_campaign",
                "batch_executed": False, "note": "no claimable symbols",
                "next_batch_enqueued": False}
        before = _companyfacts_pit_store()
        facts_before = before.observation_count()
        ciks_before = len(before.covered_ciks())
        concepts = _cf_concepts or list(_cf.FIRST_CONCEPTS)
        res = _collect_batch("sec_edgar", batch,
                             symbol_keys=["facts_sample_symbols"],
                             extra={**_cf.companyfacts_batch_flags(concepts),
                                    "facts_symbol_limit": len(batch)})
        state = (res or {}).get("source_states", {}).get("sec_edgar")
        blocked = (res is None or bool((res or {}).get("blocked_sources"))
                   or "BLOCKED" in str((res or {}).get("status") or ""))
        # SAFE-TIMEOUT: deadline mid-batch -> RETRYABLE, partial NOT recorded.
        if bool((res or {}).get("source_deadlines", {}).get("sec_edgar")):
            return _ar.OUTCOME_RETRYABLE, {
                **_campaign_base(cid, univ, store.coverage(cid)),
                "real_work": "sec_companyfacts_pit_campaign",
                "reason": "SEC companyfacts collect reached "
                          "collect_time_budget_seconds (deadline); partial batch "
                          "NOT recorded; will resume",
                "deadline_reached": True, "batch_executed": False,
                "batch_symbols_queried": len(batch), "sec_source_state": state,
                "next_batch_enqueued": False}
        after = _companyfacts_pit_store()
        facts_after = after.observation_count()
        ciks_after = len(after.covered_ciks())
        summ = after.coverage_summary()
        records_added = max(0, facts_after - facts_before)
        progressed = (facts_after > facts_before) or (ciks_after > ciks_before)
        cf_detail = {
            "companyfacts_ciks_covered": ciks_after,
            "companyfacts_ciks_added": max(0, ciks_after - ciks_before),
            "pit_observations": facts_after,
            "pit_observations_added": records_added,
            "concepts_present": summ.get("concepts_present"),
            "concept_scope": sorted(concepts),
            "pit_observation_digest": _cf.pit_observation_digest(after),
            "availability_start": summ.get("availability_start"),
            "availability_end": summ.get("availability_end")}
        if not progressed:
            cov = store.record_results(
                cid, failed=[(s, "companyfacts: no new owned XBRL facts")
                             for s in batch])
            store.record_batch(
                cid, run_date=run_date, progress=False, origin=origin,
                stop_reason=(_cf.NO_OWNED_COMPANYFACTS if blocked
                             else "NO_NEW_FACTS"),
                metrics={"ciks_attempted": len(batch), "ciks_completed": 0,
                         "records_added": 0, "issuers_covered": ciks_after,
                         "remaining_backlog": cov.get("remaining_symbol_count"),
                         "detail": {"concepts": summ.get("concepts_present")}})
            outcome = (_ar.OUTCOME_BLOCKED_SPECIFIC if blocked
                       else _ar.OUTCOME_COMPLETED)
            return outcome, {
                **_campaign_base(cid, univ, cov),
                "real_work": "sec_companyfacts_pit_campaign",
                "sec_source_state": state, "batch_executed": True,
                "progress": False,
                "reason": ("SEC companyfacts lane blocked: %s"
                           % (res or {}).get("status")) if blocked
                          else "no NEW owned companyfacts facts this batch",
                "consecutive_no_progress_batches":
                    store.consecutive_no_progress(cid),
                "next_batch_enqueued": False, **cf_detail}
        cov = store.record_results(cid, succeeded=batch)
        store.record_batch(
            cid, run_date=run_date, progress=True, origin=origin,
            metrics={"ciks_attempted": len(batch), "ciks_completed": len(batch),
                     "records_added": records_added, "issuers_covered": ciks_after,
                     "remaining_backlog": cov.get("remaining_symbol_count"),
                     "detail": {"concepts": summ.get("concepts_present"),
                                "availability_end": summ.get("availability_end")}})
        done_after = store.batches_on_date(cid, run_date)
        do_enqueue = queue is not None and sec_continuation_should_chain(
            is_complete=bool(cov.get("is_complete")),
            completed_batches_today=done_after, daily_cap=_cf_daily_cap)
        if do_enqueue:
            # Continuation references the BASE id (resolved to the ACTIVE revision
            # on the next run) so it survives an append-only scope supersession.
            _enqueue_next_batch(base_id,
                                d.get("category", _ar.CAT_DATA_ACQUISITION),
                                cov.get("acquisition_cursor", 0), job.priority)
        chain_stop = (None if do_enqueue else
                      ("CAMPAIGN_COMPLETE" if cov.get("is_complete")
                       else "DAILY_BATCH_CAP_REACHED"
                       if done_after >= _cf_daily_cap else "NO_QUEUE"))
        return _ar.OUTCOME_COMPLETED, {
            **_campaign_base(cid, univ, cov),
            "real_work": "sec_companyfacts_pit_campaign",
            "sec_source_state": state, "batch_executed": True, "progress": True,
            "batch_symbols_queried": len(batch),
            "completed_batches_today": done_after,
            "daily_completed_batch_cap": _cf_daily_cap,
            "consecutive_no_progress_batches": 0,
            "continuation_stop_reason": chain_stop,
            "pit_basis": "SEC filed date = availability; restatements preserved "
                         "as distinct observations; no future-restatement "
                         "look-ahead",
            "next_batch_enqueued": do_enqueue, **cf_detail}

    # ---- SEC bulk-archive resumable download campaign (WS6/WS7) ------------ #
    def _run_bulk_campaign(job, archive):
        from . import sec_bulk_download as _bulk
        sec = (cfg2.get("sources") or {}).get("sec_edgar") or {}
        arch_path = (sec.get("bulk_archives") or {}).get(archive)
        cid = "sec_bulk_%s" % archive
        store = _camp_store()
        store.ensure_campaign(cid, kind="sec_bulk_archive", universe=[archive],
                              universe_source="sec_official_bulk_archive",
                              batch_size=1, run_mode=_run_mode)
        if not arch_path:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "campaign": cid, "reason": "no bulk archive path configured"}
        base = sec.get("base_url_www", "https://www.sec.gov").rstrip("/")
        contact = contact_email or "binisti@gmail.com"
        product = (cfg2.get("user_agent") or {}).get(
            "product", "paper-trader-alpha-agent/2.0")
        headers = {"User-Agent": "%s %s" % (product, contact),
                   "Accept-Encoding": "identity"}
        dest = Path(_prod.get("bulk_root")
                    or (Path(ingestion_root) / "bulk" / "sec_edgar"))
        dl = _bulk.BulkArchiveDownloader(
            url=base + arch_path, dest_dir=dest, name=archive, headers=headers,
            disk_budget_bytes=int(_prod.get("bulk_disk_budget_bytes",
                                            8 * 1024 ** 3)),
            segment_bytes=int(_prod.get("bulk_segment_bytes",
                                        64 * 1024 * 1024)),
            timeout=float((cfg2.get("limits") or {}).get(
                "http_timeout_seconds", 60)))
        prog = dl.download_segment()
        disp = prog.get("disposition")
        if disp == _bulk.D_DISK_CAPACITY_REQUIRED:
            pf = prog.get("preflight") or {}
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "campaign": cid, "real_work": "sec_bulk_resumable_download",
                "disposition": disp, "reason": prog.get("reason"),
                "required_bytes": pf.get("required_bytes") or prog.get(
                    "total_bytes"),
                "available_bytes": pf.get("available_bytes")}
        if prog.get("complete"):
            store.record_results(cid, succeeded=[archive])
            return _ar.OUTCOME_COMPLETED, {
                "campaign": cid, "real_work": "sec_bulk_resumable_download",
                "archive": archive, "complete": True,
                "sha256": prog.get("sha256"), "bulk": prog}
        # In progress: leave the archive PENDING and enqueue the next segment.
        _enqueue_next_batch(cid, _ar.CAT_DATA_ACQUISITION,
                            int(prog.get("bytes_downloaded", 0)), job.priority)
        detail = {"campaign": cid, "real_work": "sec_bulk_resumable_download",
                  "archive": archive, "complete": False, "bulk": prog,
                  "next_segment_enqueued": bool(queue)}
        return (_ar.OUTCOME_COMPLETED if prog.get("ok", True)
                else _ar.OUTCOME_RETRYABLE), detail

    _CAMP_RUNNERS = {"norgate": _run_norgate_campaign,
                     "analyst": _run_analyst_campaign,
                     "sec_cik": _run_sec_cik_campaign,
                     "companyfacts": _run_companyfacts_campaign}

    def _h_data_acquisition_camp(job):
        cid = (job.payload or {}).get("campaign")
        if cid:
            d = _defs_by_id.get(cid)
            if d is None:
                return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                    "campaign": cid, "reason": "unknown campaign"}
            if d.get("runner") == "bulk":
                return _run_bulk_campaign(job, d.get("archive"))
            runner = _CAMP_RUNNERS.get(d.get("runner"))
            if runner:
                return runner(job)
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                "campaign": cid, "reason": "no runner for campaign"}
        return _h_data_acquisition(job)

    def _h_prospective_camp(job):
        if (job.payload or {}).get("campaign") == "eodhd_analyst":
            return _run_analyst_campaign(job)
        return _h_prospective(job)

    # ---------------------------------------------------------------------- #
    # Stage 9.2 - tournament weakest-gate DATA_VALIDATION follow-up handler.
    # Executes the Stage-9-generated `tournament.address_weakest_gate` jobs: it
    # MEASURES real owned coverage for the blocking data dependency, durably
    # records the measurement against the candidate (advancing latest_evidence_
    # date / experiment_ids WITHOUT touching lifecycle, so a candidate is never
    # promoted here), and returns an HONEST outcome - COMPLETED with a coverage
    # artifact when the dependency is measurable / acquirable-now, BLOCKED_
    # SPECIFIC with an exact blocker + bounded implementation path when it needs
    # a build, FAILED_PERMANENT only on a proven malformed spec. It never returns
    # a fake success. When a dependency is acquirable now (SEC Form 4 / 8-K) it
    # enqueues ONE idempotent bounded acquisition continuation so real coverage
    # grows on later cycles. Read-only w.r.t. every operational trading store.
    # ---------------------------------------------------------------------- #
    def _open_tournament_registry():
        from . import tournament as _tt2
        p = resolve_stage9_config_path(cfg)
        if not p:
            return None, None
        cfg9 = _tt2.load_config(p)
        return _tt2.CandidateRegistry(cfg9["tournament_db"]), cfg9

    def _wg_sec_coverage():
        """REAL owned SEC Form 4 / 8-K event-coverage READINESS (Part 3). Reports
        the exact per-candidate readiness fields for the three EVENT_INSIDER
        candidates: net_insider_buys (open-market purchases), insider_cluster
        (clustered purchases) and earnings_8k_drift (8-K Item 2.02). All counts
        are cumulative owned records; availability = SEC acceptance (PIT-safe)."""
        c = _sec_owned_counts()
        au = cfg.get("autonomy") or {}
        min_issuers = int(au.get("event_min_issuers", 30))
        min_events = int(au.get("event_min_events", 30))
        universe_target = None
        try:
            universe_target = _camp_store().coverage("sec_form4_8k").get(
                "full_universe_target_count")
        except Exception:  # noqa: BLE001 - universe % is best-effort
            universe_target = None
        uni_pct = (round(100.0 * c["distinct_issuers"] / universe_target, 4)
                   if universe_target else None)
        cov = {"data_family": "sec_form4_8k",
               "coverage_start_date": c["coverage_start"],
               "coverage_end_date": c["coverage_end"],
               "distinct_issuers": c["distinct_issuers"],
               "distinct_filing_dates": c["distinct_filing_dates"],
               "form4_filing_count": c["insider_transaction_records"],
               "open_market_purchase_count": c["open_market_purchases"],
               "sale_count": c["sales"],
               "clustered_purchase_event_count": c["clustered_purchase_events"],
               "sec_8k_item202_event_count": c["sec_8k_item202_events"],
               "universe_target_ciks": universe_target,
               "universe_coverage_pct": uni_pct,
               "scored_event_periods_available": c["distinct_filing_dates"],
               "required_distinct_issuers": min_issuers,
               "required_distinct_events": min_events,
               "remaining_issuer_gap": max(0, min_issuers - c["distinct_issuers"]),
               "remaining_event_gap":
                   max(0, min_events - c["sec_8k_item202_events"]),
               # legacy keys retained for existing readers / tests
               "insider_transaction_records": c["insider_transaction_records"],
               "open_market_purchase_records": c["open_market_purchases"],
               "sec_8k_item202_events": c["sec_8k_item202_events"],
               "pit_basis": "SEC acceptance timestamp; amendments (4/A, 8-K/A) "
                            "are distinct append-only records; no look-ahead"}
        return c["distinct_issuers"] >= min_issuers, cov

    def _wg_analyst_coverage():
        """Prospective EODHD analyst-vintage READINESS (Part 6; calendar-time
        gated). Reports the owned vintage span, distinct dates, fields covered
        and duplicate-suppression contract; only forward calendar time resolves
        it (hard PIT floor)."""
        ev = _analyst_vintage_evidence()
        dates = ev.get("snapshot_dates_on_disk") or []
        min_dates = int((cfg.get("autonomy") or {}).get(
            "analyst_min_snapshot_dates", 20))
        cov = {"data_family": "eodhd_analyst_vintages",
               "first_snapshot_pit_floor": ev.get("first_snapshot_boundary"),
               "first_owned_vintage_date": dates[0] if dates else None,
               "latest_owned_vintage_date": dates[-1] if dates else None,
               "distinct_vintage_dates_on_disk": len(dates),
               "tickers_covered_latest_snapshot": ev.get("vintage_count_today"),
               "estimate_revision_fields_covered": [
                   "eps_estimate_avg", "eps_estimate_low", "eps_estimate_high",
                   "revenue_estimate_avg", "analyst_count", "eps_revisions_up",
                   "eps_revisions_down", "wall_street_target_price"],
               "duplicate_suppression": "first-write-wins immutable daily "
                   "vintage (os.replace); a same-day re-run is a 0-new "
                   "idempotent no-op",
               "required_distinct_vintage_dates": min_dates,
               "vintage_files_latest_snapshot": ev.get("vintage_count_today"),
               "estimated_sessions_remaining": max(0, min_dates - len(dates)),
               "estimated_market_sessions_remaining":
                   max(0, min_dates - len(dates)),
               "candidates": ["price_target_revision",
                              "earnings_estimate_revisions", "earnings_surprise",
                              "revision_breadth"],
               "pit_note": "first-snapshot date is a HARD PIT floor; never "
                           "backfilled before it - only forward calendar time "
                           "accrues new distinct vintages"}
        return len(dates) >= min_dates, cov

    def _wg_pit_gics_coverage():
        """Leakage-safe PIT sector coverage from SEC ASSIGNED-SIC records
        (Part 4). Target candidate: sector_neutral_momentum."""
        from . import pit_sector as _ps
        sic = [r for r in _read_normalized("SECURITY_IDENTITY", limit=6000)
               if r.get("event_type") == "ASSIGNED_SIC_PIT"]
        symbols = {(r.get("company_id")
                    or (r.get("normalized_payload") or {}).get("ticker"))
                   for r in sic}
        symbols.discard(None)
        min_symbols = int((cfg.get("autonomy") or {}).get(
            "pit_gics_min_symbols", 50))
        cov = {"data_family": "point_in_time_gics",
               "assigned_sic_pit_records": len(sic),
               "distinct_symbols_pit_classified": len(symbols),
               "required_distinct_symbols": min_symbols,
               "remaining_symbol_gap": max(0, min_symbols - len(symbols)),
               "sector_mapping_version": _ps.MAPPING_VERSION,
               "sector_mapping_version_hash": _ps.mapping_version_hash(),
               "target_candidate": "sector_neutral_momentum",
               "source": "contemporaneous SEC ASSIGNED-SIC filing headers "
                         "(available_at = acceptance) - leakage-safe PIT",
               "look_ahead_note": "current Norgate GICS is undated (a "
                                  "classification look-ahead); only the SEC "
                                  "ASSIGNED-SIC PIT series is leakage-free"}
        return len(symbols) >= min_symbols, cov

    def _wg_pit_fundamentals():
        """PIT fundamentals READINESS (Part 5): build the smallest owned SEC
        companyfacts PIT slice and report HONEST coverage. Owned facts are a hard
        floor - no fabrication, no current-snapshot substitution; insufficient
        owned facts stay DATA_HOLD with an exact next action."""
        from . import pit_fundamentals as _pf
        store = _pf.PitFundamentalsStore()
        store.add_records(_read_normalized("FUNDAMENTAL_FACT", limit=6000))
        summ = store.coverage_summary()
        min_ciks = int((cfg.get("autonomy") or {}).get(
            "pit_fundamentals_min_ciks", 50))
        gp_ready = summ["per_candidate"]["gross_profitability"][
            "ciks_with_all_required_concepts"]
        sufficient = gp_ready >= min_ciks
        if sufficient:
            nxt = ("owned PIT fundamentals now cover %d CIKs with the full "
                   "gross-profitability concept set; a FUNDAMENTAL evaluation "
                   "adapter can attempt the canonical evidence contract"
                   % gp_ready)
        else:
            nxt = ("owned SEC companyfacts are insufficient (%d XBRL facts / %d "
                   "CIKs; %d ready for gross_profitability); a bounded FREE "
                   "per-CIK SEC companyfacts acquisition must run before a "
                   "FUNDAMENTAL adapter can evaluate - no provider purchase, no "
                   "current-snapshot substitution"
                   % (summ["facts_ingested"], summ["distinct_ciks"], gp_ready))
        cov = {"data_family": "point_in_time_fundamentals",
               "owned_status": "OWNED_SEC_COMPANYFACTS_PIT_BUILD",
               "required_ciks_with_full_concepts": min_ciks,
               "ciks_ready_gross_profitability": gp_ready,
               "remaining_cik_gap": max(0, min_ciks - gp_ready),
               "priority_candidates": ["gross_profitability", "asset_growth",
                                       "balance_sheet_quality"],
               "requires_new_provider_purchase": False,
               "requires_speculative_full_rebuild": False,
               "bounded_implementation_path": [
                   "ingest already-owned SEC companyfacts XBRL facts (free): "
                   "each fact carries filed/period dates = the as-of vintage",
                   "assemble per-(CIK, concept, fiscal-period) PIT observations "
                   "keyed by SEC filed date (restatements preserved; no "
                   "future-restatement look-ahead) - alpha_agent.pit_fundamentals",
                   "start with the smallest useful slice: only the concepts the "
                   "FUNDAMENTAL candidates need (gross profitability, asset "
                   "growth, balance-sheet quality)",
                   "only then wire a FUNDAMENTAL evaluation adapter that emits "
                   "the canonical Stage 9 evidence contract"],
               "next_action": nxt, **summ}
        # Stage 9.4 Track F: per-signal MVP readiness via the fundamental-signal
        # family evaluation adapter (deterministic fallback, explicit missing).
        # The canonical evidence contract is only attempted once a signal's
        # company/period coverage passes; owned facts stay a hard floor.
        try:
            from . import fundamental_signals as _fsig
            cov["fundamental_mvp_readiness"] = {
                sig: _fsig.coverage_ready(store, sig, scored_periods=0,
                                          min_ciks=min_ciks, min_periods=12)
                for sig in _fsig.SIGNALS}
            cov["companyfacts_campaign_id"] = "sec_companyfacts"
            cov["stage9_4_materialization"] = (
                "alpha_agent.sec_companyfacts.materialize -> "
                "pit_fundamentals -> fundamental_signals")
        except Exception:  # noqa: BLE001 - readiness enrichment is best-effort
            pass
        return sufficient, cov

    # dependency -> (measure_fn, acquisition_campaign_or_None, acquirable_now)
    _WG_DISPATCH = {
        "sec_form4": (_wg_sec_coverage, "sec_form4_8k", True),
        "sec_8k": (_wg_sec_coverage, "sec_form4_8k", True),
        "sec_form4_8k": (_wg_sec_coverage, "sec_form4_8k", True),
        "eodhd_analyst_vintages": (_wg_analyst_coverage, None, False),
        "point_in_time_gics": (_wg_pit_gics_coverage, None, False),
        "point_in_time_fundamentals": (_wg_pit_fundamentals, None, False),
    }

    def _h_weakest_gate(job):
        from datetime import datetime as _dtw, timezone as _tzw
        import hashlib as _hlw
        payload = job.payload or {}
        # Non-tournament DATA_VALIDATION keeps the safe default behaviour.
        if not payload.get("tournament") or \
                "address_weakest_gate" not in str(job.lane):
            return _ar.OUTCOME_COMPLETED, {
                "note": "recorded; heavy work out-of-band", "lane": job.lane}
        cid = payload.get("candidate_id")
        spec = payload.get("spec") or {}
        dep = spec.get("data_dependency") or payload.get("data_dependency")
        evidence_date = payload.get("evidence_date") or \
            _dtw.now(_tzw.utc).date().isoformat()
        base = {"real_work": "tournament_weakest_gate_validation",
                "candidate_id": cid, "data_dependency": dep,
                "prior_blocker": spec.get("blocker"),
                "evidence_date": evidence_date, "no_automatic_promotion": True}
        if not cid or not dep:
            return _ar.OUTCOME_FAILED_PERMANENT, {
                **base, "reason": "malformed follow-up spec: missing "
                "candidate_id or data_dependency"}
        measure = _WG_DISPATCH.get(dep)
        if measure is None:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "reason": "no weakest-gate validator for dependency %r"
                % dep}
        try:
            reg, _cfg9 = _open_tournament_registry()
        except Exception as exc:  # noqa: BLE001 - transient registry open
            return _ar.OUTCOME_RETRYABLE, {
                **base, "reason": "tournament registry unavailable: %s"
                % type(exc).__name__}
        if reg is None:
            return _ar.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "reason": "stage9 tournament config not resolved"}
        try:
            cand = reg.get(cid)
            if cand is None:
                return _ar.OUTCOME_FAILED_PERMANENT, {
                    **base, "reason": "unknown candidate %s" % cid}
            rhash = "wg_" + _hlw.sha256(
                ("%s|%s|%s" % (cid, dep, evidence_date)).encode()).hexdigest()[:24]
            if reg.is_processed(rhash):
                cov0 = reg.latest_data_coverage(cid) or {}
                return _ar.OUTCOME_COMPLETED, {
                    **base, "idempotent": True,
                    "note": "coverage already measured for this candidate on "
                            "this evidence date (import-once)",
                    "sufficient": cov0.get("sufficient"),
                    "coverage": cov0.get("coverage")}
            fn, acq_campaign, acquirable_now = measure
            sufficient, coverage = fn()
            # Stage 9.5: when the LIVE companyfacts campaign is active the PIT
            # fundamentals dependency is no longer "build_required" - the bounded
            # sec_companyfacts acquisition (bootstrapped by the drain seeder,
            # ensure_companyfacts_bootstrap) grows owned coverage each cycle, so
            # the weakest-gate MEASURES coverage and COMPLETES (the fundamental
            # candidate stays an honest DATA_HOLD). It does NOT enqueue here - the
            # seeder is the single companyfacts bootstrap, avoiding duplicate
            # acquisition jobs.
            build_required = (dep == "point_in_time_fundamentals"
                              and not _cf_enabled)
            enqueued = None
            if sufficient:
                next_action = ("coverage now meets the observation threshold; a "
                               "family evaluation adapter can attempt a full "
                               "canonical evidence contract next")
            elif acquirable_now and acq_campaign and queue is not None:
                enqueued = queue.enqueue(
                    _ar.CAT_DATA_ACQUISITION, lane="acq.%s" % acq_campaign,
                    payload={"campaign": acq_campaign},
                    priority=2, origin="stage9-weakest-gate")
                next_action = ("insufficient owned coverage; enqueued a bounded "
                               "%s acquisition continuation to grow coverage"
                               % acq_campaign)
            elif dep == "point_in_time_fundamentals" and _cf_enabled:
                next_action = ("owned PIT companyfacts coverage insufficient; the "
                               "bounded LIVE sec_companyfacts acquisition campaign "
                               "is active and grows owned coverage each cycle - "
                               "the canonical fundamental EXPERIMENT runs once the "
                               "readiness gate passes (no gate lowered)")
            elif build_required:
                next_action = ("build the smallest useful PIT-fundamentals slice "
                               "from owned SEC companyfacts before a FUNDAMENTAL "
                               "adapter can run")
            else:
                next_action = ("insufficient; only calendar-time accrual of the "
                               "daily forward collection resolves this (hard PIT "
                               "floor - never backfilled)")
            reg.record_data_coverage(
                cid, coverage=coverage, evidence_date=evidence_date,
                data_dependency=dep, job_id=job.job_id, sufficient=sufficient,
                next_action=next_action)
            reg.mark_processed(result_hash=rhash, candidate_id=cid,
                               source="weakest_gate_validation",
                               job_id=job.job_id,
                               feature=(cand.get("spec") or {}).get("feature"),
                               evidence_date=evidence_date)
            detail = {**base, "sufficient": sufficient, "coverage": coverage,
                      "candidate_lifecycle_state": cand.get("lifecycle_state"),
                      "candidate_still_data_hold":
                          cand.get("lifecycle_state") == "DATA_HOLD",
                      "next_action": next_action,
                      "acquisition_continuation_job": enqueued}
            if build_required and not sufficient:
                detail["reason"] = ("point-in-time fundamentals require a build "
                                    "from owned SEC companyfacts (no bounded "
                                    "acquisition; no new provider purchase)")
                return _ar.OUTCOME_BLOCKED_SPECIFIC, detail
            return _ar.OUTCOME_COMPLETED, detail
        finally:
            try:
                reg.close()
            except Exception:  # noqa: BLE001
                pass

    # Start from the safe defaults (source-discovery/entitlement/report keep
    # their offline behaviour) and OVERRIDE the real-work categories.
    handlers = _default_autonomy_handlers(cfg)
    handlers[_ar.CAT_DATA_ACQUISITION] = _h_data_acquisition_camp
    handlers[_ar.CAT_COVERAGE_REPAIR] = _h_coverage_repair
    handlers[_ar.CAT_PROSPECTIVE_SNAPSHOT] = _h_prospective_camp
    handlers[_ar.CAT_EXPERIMENT] = _h_experiment
    handlers[_ar.CAT_ROBUSTNESS_TEST] = _h_robustness
    handlers[_ar.CAT_SIGNAL_COMBINATION] = _h_combination
    handlers[_ar.CAT_TELEGRAM_REQUEST] = _h_telegram
    handlers[_ar.CAT_DATA_VALIDATION] = _h_weakest_gate

    # Stage 10 — route ONLY the exact new identity.* lanes to the identity
    # handlers using the SAME queue. Every other DATA_ACQUISITION / DATA_VALIDATION
    # job keeps its existing production handler untouched. The identity context
    # (store + owned Norgate accessor + owned current ticker->CIK evidence) is
    # built lazily and reused across the cycle. Research-only; no operational
    # mutation.
    _s10 = stage10_identity_cfg(cfg)
    if _s10.get("enabled"):
        from . import identity_jobs as _ij
        _idcache: dict = {}

        def _identity_ctx():
            if "ctx" not in _idcache:
                _idcache["ctx"] = build_identity_context(
                    cfg, queue=queue, read_normalized=_read_normalized)
            return _idcache["ctx"]

        def _route_identity(base):
            def _routed(job, _b=base):
                if str(getattr(job, "lane", "")).startswith(
                        _ij.IDENTITY_LANE_PREFIX):
                    return _ij.dispatch_identity_job(job, _identity_ctx())
                return _b(job)
            return _routed

        handlers[_ar.CAT_DATA_ACQUISITION] = _route_identity(
            handlers[_ar.CAT_DATA_ACQUISITION])
        handlers[_ar.CAT_DATA_VALIDATION] = _route_identity(
            handlers[_ar.CAT_DATA_VALIDATION])

    # Stage 10.2 — route ONLY the exact new fundamentals.* lanes to the
    # fundamental handlers using the SAME queue. Every other DATA_ACQUISITION /
    # DATA_VALIDATION job keeps its existing (already identity-wrapped) production
    # handler untouched. The fundamental context is built lazily and reused across
    # the cycle. Research-only; no operational mutation.
    _s102 = stage10_2_cfg(cfg)
    if _s102.get("enabled"):
        from . import fundamental_jobs as _fj
        _fjcache: dict = {}

        def _fundamental_ctx():
            if "ctx" not in _fjcache:
                _fjcache["ctx"] = build_fundamental_context(
                    cfg, queue=queue, read_normalized=_read_normalized)
            return _fjcache["ctx"]

        def _route_fundamentals(base):
            def _routed(job, _b=base):
                if str(getattr(job, "lane", "")).startswith(
                        _fj.FUNDAMENTAL_LANE_PREFIX):
                    return _fj.dispatch_fundamental_job(job, _fundamental_ctx())
                return _b(job)
            return _routed

        handlers[_ar.CAT_DATA_ACQUISITION] = _route_fundamentals(
            handlers[_ar.CAT_DATA_ACQUISITION])
        handlers[_ar.CAT_DATA_VALIDATION] = _route_fundamentals(
            handlers[_ar.CAT_DATA_VALIDATION])
    return handlers


def run_autonomy_cycle(cfg: dict, *, handlers=None, planner=None,
                       max_jobs: Optional[int] = None, clock=None,
                       run_mode: Optional[str] = None,
                       run_tournament: bool = False) -> dict:
    """Run ONE bounded, never-idle Stage 8 research cycle: requeue stale work,
    replenish the queue so it is never empty, drain up to ``max_jobs`` jobs
    through their handlers, then ensure the queue is not left empty. Returns the
    cycle summary. Read-only w.r.t. operational trading state.

    Handler selection: an explicit ``handlers`` map wins; else if the config asks
    for production handlers (``autonomy.handlers == 'production'``) the REAL
    Stage 2/5/6 handlers are used; else the safe offline defaults.

    Run mode (WS2): an explicit ``run_mode`` ('production' from a scheduled task,
    'acceptance' from a smoke run) wins over ``production.run_mode`` in the
    config. In production mode the planner ALSO keeps one live batch job per
    incomplete full-universe campaign (never-idle continuation)."""
    from . import autonomous_research as _ar
    from . import source_exhaustion as _se
    from . import production_universe as _pu
    if run_mode:
        cfg = {**cfg, "production": {**(cfg.get("production") or {}),
                                     "run_mode": run_mode}}
    queue = build_autonomy_queue(cfg, clock=clock)
    _auto = cfg.get("autonomy") if isinstance(cfg.get("autonomy"), dict) else {}
    is_prod = str(_auto.get("handlers")) == "production"
    if handlers is None:
        if is_prod:
            handlers = build_production_autonomy_handlers(
                cfg, contact_email=cfg.get("contact_email"), queue=queue)
        else:
            handlers = _default_autonomy_handlers(cfg)
    if planner is None:
        base_planner = _se.make_planner(lambda: _se.build_registry_snapshot())
        if is_prod and _pu.resolve_run_mode(cfg) == _pu.PRODUCTION:
            _store = stage8_campaign_store(cfg, clock=clock)

            def planner(_q, _base=base_planner, _cfg=cfg, _s=_store):
                return list(_base(_q) or []) + \
                    production_campaign_seed_specs(_cfg, _s)
        else:
            planner = base_planner
    mx = int(max_jobs if max_jobs is not None
             else (cfg.get("autonomy") or {}).get("max_jobs_per_cycle", 8)) \
        if isinstance(cfg.get("autonomy"), dict) or max_jobs is not None else 8
    result = _ar.run_cycle(queue, handlers, planner=planner, max_jobs=mx)
    # Stage 9 (WS2/WS11): the tournament tick is the highest-value research step
    # of the production cycle. It shares THIS queue (never a second queue). It is
    # driven by CONFIGURATION: it runs when ``tournament.enabled`` is true in the
    # config (no caller needs to pass run_tournament=True). An explicit
    # run_tournament=True can still force it; a config with the block absent (as
    # in the offline Stage 8 unit tests) leaves it off. A failure is caught here
    # and never breaks the never-idle loop.
    if (run_tournament or (cfg.get("tournament") or {}).get("enabled")):
        try:
            result["tournament"] = run_tournament_tick(
                cfg, queue=queue, clock=clock)
        except Exception as exc:  # noqa: BLE001 - never break the cycle
            result["tournament"] = {"status": "TOURNAMENT_ERROR",
                                    "error": type(exc).__name__}
    return result


def run_autonomy_watchdog(cfg: dict, *, planner=None, clock=None) -> dict:
    """Stage 8 continuous-operation watchdog over the durable queue: detect stale
    RUNNING jobs / an empty queue, requeue and replenish, and classify a genuine
    global hard blocker. Never mutates operational trading state."""
    from . import autonomous_research as _ar
    from . import source_exhaustion as _se
    queue = build_autonomy_queue(cfg, clock=clock)
    if planner is None:
        planner = _se.make_planner(lambda: _se.build_registry_snapshot())
    return _ar.watchdog_scan(queue, planner=planner)


# --------------------------------------------------------------------------- #
# Stage 9 — Autonomous Alpha Tournament entry points (WS11). The persistent
# candidate registry lives in a durable Stage 8 research store on D:; the tick
# reuses the SHARED autonomy queue for experiment generation. Research-only:
# nothing here can touch an operational ledger, order, fill, holding, cash,
# model promotion or Daily Close.
# --------------------------------------------------------------------------- #
def resolve_stage9_config_path(cfg: dict) -> Optional[str]:
    """Resolve the versioned Stage 9 tournament config path from the Stage 8
    config's ``tournament.config`` pointer (repo-relative or absolute)."""
    t = cfg.get("tournament") if isinstance(cfg.get("tournament"), dict) else {}
    p = t.get("config")
    if not p:
        return None
    pp = Path(p)
    if not pp.is_absolute():
        pp = Path(__file__).resolve().parents[1] / p
    return str(pp) if pp.exists() else None


def build_tournament_registry(cfg9: dict, *, clock=None):
    """Open (creating if needed) the durable Stage 9 candidate registry."""
    from . import tournament as _tt
    return _tt.CandidateRegistry(cfg9["tournament_db"], clock=clock)


def _collect_completed_tournament_jobs(queue, *, limit: int = 200) -> list:
    """Read completed Stage 8 queue jobs that carry tournament experiment results
    into the shape ``ingest_completed_experiments`` consumes. Bounded and
    read-only; a queue error degrades to an empty list."""
    from . import autonomous_research as _ar
    out: list = []
    try:
        for job in queue.list_jobs(state=_ar.STATE_COMPLETED, limit=limit):
            payload = getattr(job, "payload", None) or {}
            if not payload.get("tournament"):
                continue
            spec = payload.get("spec") or {}
            # Stage 9.2: weakest-gate DATA_VALIDATION follow-ups are COVERAGE
            # measurements, not experiment results - they carry no experiment
            # feature/metrics, so never hand them to the experiment ingester
            # (doing so would flag EXPERIMENT_INGEST_MALFORMED every tick).
            if payload.get("strategy") == "address_weakest_gate" \
                    or not spec.get("feature"):
                continue
            result = getattr(job, "result", None) or {}
            row = result.get("row") or result.get("metrics") or result
            out.append({"job_id": getattr(job, "job_id", None),
                        "spec": spec, "feature": spec.get("feature"),
                        "result": row})
    except Exception:  # noqa: BLE001 - ingestion is best-effort, never fatal
        return []
    return out


def _tournament_mark_provider(cfg: dict, cfg9: dict):
    """Immutable completed-close mark provider for shadow-book advancement.

    Returns ``None`` for every (candidate, date): the real owned completed-close
    portfolio replay is wired only once a candidate is actually retained and a
    shadow book exists. Until then there are zero active books, so this is never
    invoked - a missing mark can only ever yield an honest coverage diagnostic,
    never a fabricated observation."""
    def _provider(_candidate_id: str, _date: str):
        return None
    return _provider


def run_tournament_tick(cfg: dict, *, queue=None, registry=None,
                        campaign_result=None, run_campaign: bool = True,
                        cfg9: Optional[dict] = None,
                        evidence_date: Optional[str] = None,
                        max_candidates: Optional[int] = None,
                        ingest_completed: bool = True,
                        advance_shadows: bool = True, clock=None) -> dict:
    """Run ONE bounded, resumable Stage 9 tournament tick.

    Idempotently ingests completed Stage 8 experiment jobs, refreshes tournament
    state, evaluates bounded candidates against the owned Stage 5 evidence,
    recomputes the leaderboard, generates the next experiments into the shared
    durable queue and advances shadow books - the steps of WS11. The heavy owned
    price campaign runs AT MOST ONCE per evidence date (persisted in the registry
    meta) so a 30-minute cycle never re-runs it needlessly. Read-only w.r.t.
    operational trading state; a missing Stage 9 config yields
    ``TOURNAMENT_DISABLED`` (never raises)."""
    from . import tournament as _tt
    if cfg9 is None:
        p = resolve_stage9_config_path(cfg)
        if not p:
            return {"status": "TOURNAMENT_DISABLED",
                    "reason": "no stage9 config resolved"}
        cfg9 = _tt.load_config(p)
    own_registry = registry is None
    if registry is None:
        registry = build_tournament_registry(cfg9, clock=clock)
    if queue is None:
        queue = build_autonomy_queue(cfg, clock=clock)
    try:
        # Campaign cadence (WS4): the heavy Stage 5 owned-price campaign runs at
        # most once per evidence date. A same-day repeat cycle reuses no campaign
        # (already-processed results are idempotently skipped) yet still advances
        # shadow books and keeps the queue non-empty.
        campaign_ran = False
        if campaign_result is None and run_campaign:
            already = (evidence_date is not None
                       and registry.get_meta("last_campaign_date")
                       == evidence_date)
            if not already:
                try:
                    _s94 = (cfg9.get("stage9_4") or {}) if isinstance(cfg9, dict) \
                        else {}
                    campaign_result = _tt.build_owned_price_campaign(
                        cfg, new_factors=bool(_s94.get("evaluate_price_catalogue")),
                        stage9_4_cfg=_s94)
                    campaign_ran = campaign_result is not None
                    if campaign_ran and evidence_date is not None:
                        registry.set_meta("last_campaign_date", evidence_date)
                except Exception:  # noqa: BLE001 - degrade to no-new-evidence
                    campaign_result = None
        # On an evidence day, classify the full (fixed, bounded) catalogue
        # against the fresh evidence in one atomic pass; on a cheap repeat cycle
        # keep the small configured cap.
        cfg_cap = (max_candidates if max_candidates is not None
                   else (cfg.get("tournament") or {}).get(
                       "max_candidates_per_cycle") or 4)
        eval_cap = 200 if campaign_result else int(cfg_cap)

        completed = (_collect_completed_tournament_jobs(queue)
                     if ingest_completed else None)
        mark_provider = (_tournament_mark_provider(cfg, cfg9)
                         if advance_shadows else None)

        result = _tt.run_tournament_cycle(
            registry, cfg9, queue=queue, campaign_result=campaign_result,
            evidence_date=evidence_date, max_candidates=eval_cap,
            completed_experiments=completed, mark_provider=mark_provider)
        result["campaign_ran"] = campaign_ran
        result["queue_depth"] = queue.depth()
        return result
    finally:
        if own_registry:
            registry.close()
