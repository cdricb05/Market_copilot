"""api/monthly_momentum_emitter.py — Phase 29D.2 production monthly-momentum emitter bridge.

The ONE production PRODUCER wired behind the canonical monthly-input adapter's
injectable emitter seam (``api.monthly_momentum_input.emit_if_due`` →
``emitter_fn(month=..., eligible=..., inputs_dir=...)``). Its single job is to run
the AUTHORITATIVE, owned survivorship-free monthly momentum calculation for the
eligible session and return a validated artifact for the adapter to promote — so a
due month is resolved by ONE ``RUN DAILY RESEARCH CYCLE`` action with NO hidden
prerequisite command, button, restart, or manual file operation.

Ownership boundaries (unchanged; this module owns NONE of the mathematics):

  * ``research.phase24_daily_panel``          — owned survivorship-free daily SOURCE PANEL
    (Norgate Russell-1000 Current & Past total-return NPZ). Read-only here.
  * ``research.phase25_multi_horizon_inputs`` — the mathematical OWNER of the frozen
    ``mom_6_1`` monthly momentum (``close[m-1]/close[m-7]-1`` on month-end closes).
  * ``api.monthly_momentum_input``            — the operational adapter + validation +
    idempotent atomic promotion of the canonical ``current_momentum_scores.csv``.
  * ``api.daily_research_cycle``              — the combined orchestrator.
  * ``api.monthly_momentum_emitter`` (THIS)   — a pure-stdlib SUBPROCESS BRIDGE that
    inspects the owned panel's coverage, runs the Phase-25 mathematics in an ISOLATED
    temporary output directory through an EXPLICIT subprocess argument ARRAY (never a
    shell-command string and never a shell invocation), validates the produced artifacts
    and hands them back. It computes NO ``mom_6_1`` and imports NEITHER numpy NOR pandas —
    the heavy numeric work happens only in the external subprocess, so the Paper Trader
    process stays deliberately pure-stdlib.

Source-panel policy (Workstream C, revised by Release 54.2.3): when the owned panel
already COVERS the eligible session (its last trading date equals the eligible date) the
bridge runs Phase 25 with no provider call. When the panel is BEHIND the eligible session
it now performs ONE CONTROLLED, POINT-IN-TIME-BOUNDED refresh through the panel owner's
own ``refresh_daily_panel_as_of(as_of=<eligible session>)`` — never an unbounded rebuild
"to latest". The cutoff is supplied INTERNALLY from the eligible research session; no
caller, route or operator control may choose it. A FUTURE-dated or UNVERIFIABLE panel is
still an explicit DATA_HOLD blocker and is NEVER "fixed" by rebuilding backwards, because
that would discard observations a later session legitimately holds. No future-dated rows;
no current-constituent substitution into historical dates; point-in-time provenance
preserved.

WHY THE REFRESH LIVES BEHIND THIS BRIDGE. Before Release 54.2.3 nothing in the running
system was responsible for advancing the panel: the acquisition is a one-time research
build (a NO-OP once the NPZ exists) and this bridge refused to trigger it, so the panel
stopped on the day it was first built and every later month became an unresolvable
blocker. The panel owner still owns the pull, the assembly, the survivorship treatment
and the quality contract (there is no second panel writer); this bridge owns only the
POLICY — when a refresh is due, which cutoff binds it, and what a failure means.

Safety: no order / signal / decision / fill; no operational-ledger or database write;
no prediction service; no canonical-input write (the adapter owns promotion). Only
research-side reads (the owned panel) and bounded research-side writes (an isolated
temp output dir + a compact emission / diagnostic manifest under the emitter work
root) ever occur, and only when a due month is actually produced.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional

PHASE = "29D.2"
CANONICAL_EMITTER_OWNER = "api.monthly_momentum_emitter"
EMITTER_SOURCE = "research.phase25_multi_horizon_inputs"
PANEL_SOURCE = "research.phase24_daily_panel"

# --------------------------------------------------------------------------- #
# Explicit, overridable configuration (Workstream A). Env vars win over the safe
# defaults; a test overrides every path/timeout through ``resolve_config(overrides)``.
# --------------------------------------------------------------------------- #
REPO_ENV = "PAPER_TRADER_MONTHLY_EMITTER_REPO"
PYTHON_ENV = "PAPER_TRADER_MONTHLY_EMITTER_PYTHON"
PANEL_NPZ_ENV = "PAPER_TRADER_MONTHLY_EMITTER_PANEL_NPZ"
PANEL_MANIFEST_ENV = "PAPER_TRADER_MONTHLY_EMITTER_PANEL_MANIFEST"
WORK_ENV = "PAPER_TRADER_MONTHLY_EMITTER_WORK_DIR"
TIMEOUT_ENV = "PAPER_TRADER_MONTHLY_EMITTER_TIMEOUT_SECONDS"
DISABLE_ENV = "PAPER_TRADER_MONTHLY_EMITTER_DISABLED"
# Release 54.2.3 — the CONTROLLED bounded source-panel refresh.
PANEL_REFRESH_DISABLE_ENV = "PAPER_TRADER_SOURCE_PANEL_REFRESH_DISABLED"
PANEL_REFRESH_TIMEOUT_ENV = "PAPER_TRADER_SOURCE_PANEL_REFRESH_TIMEOUT_SECONDS"

DEFAULT_REPO = r"C:\Users\binis\Stock_Prediction_app_push"
DEFAULT_PYTHON = r"C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe"
DEFAULT_PANEL_NPZ = (r"D:\Stock_Prediction_app_data\phase24_cache"
                     r"\daily_panel\russell1000_cp_daily.npz")
DEFAULT_PANEL_MANIFEST = (r"D:\Stock_Prediction_app_data\phase24_cache"
                          r"\daily_panel\manifest.json")
DEFAULT_WORK_DIR = r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_emitter"
DEFAULT_TIMEOUT = 900  # 15 minutes; the Phase-25 build reads a ~74MB NPZ + resamples.
# The bounded panel refresh re-pulls ~3.6k symbols from the LOCAL owned Norgate database
# (measured ~64 symbols/second) and re-assembles a ~74MB NPZ. 30 minutes is generous.
DEFAULT_PANEL_REFRESH_TIMEOUT = 1800

# Module locations inside the external research repo (used for availability only).
PHASE24_REL = "research/phase24_daily_panel.py"
PHASE25_REL = "research/phase25_multi_horizon_inputs.py"

# Produced artifact filenames (the momentum CSV is the one the adapter promotes).
OUTPUT_MOM_FILE = "current_momentum_scores.csv"
OUTPUT_MANIFEST_FILE = "inputs_manifest.json"
# The frozen monthly schema every produced row must carry (mirrors the adapter's
# REQUIRED_COLUMNS so the two validation layers agree).
REQUIRED_OUTPUT_COLUMNS = ("ticker", "mom_6_1", "is_member", "sector",
                           "market_as_of_date", "month_label")
# The identity fields that define the artifact content (market_as_of_date is the
# intramonth-advancing provenance and is excluded from the identity hash).
_IDENTITY_COLUMNS = ("ticker", "mom_6_1", "is_member", "sector", "month_label")

# Source-panel policy decisions.
_PANEL_USE_EXISTING = "USE_EXISTING"
_PANEL_BLOCK = "BLOCK"
#: Release 54.2.3 — the panel is behind the eligible session and ONE controlled refresh
#: bounded to that session is available. The cycle performs it; it is not an operator step.
_PANEL_REFRESH_BOUNDED = "REFRESH_BOUNDED"

# --------------------------------------------------------------------------- #
# Release 54.2.3 — canonical SOURCE-PANEL data-quality vocabulary. One spelling,
# read by the Daily Research Cycle and rendered by the operator surfaces; no
# consumer re-derives a panel state from dates of its own.
# --------------------------------------------------------------------------- #
# The ``status`` field keeps its ESTABLISHED spellings (existing consumers and their
# regressions are not churned for vocabulary); ``panel_state`` carries the canonical
# data-quality name beside it, and the two are set together in one place.
PANEL_CURRENT = "SOURCE_PANEL_CURRENT"                # covers the eligible session
PANEL_STALE = "SOURCE_PANEL_STALE"                    # behind it; a bounded refresh is due
PANEL_REFRESHING = "SOURCE_PANEL_REFRESHING"          # a bounded refresh is in flight
PANEL_INCOMPLETE = "SOURCE_PANEL_INCOMPLETE"          # refreshed but short of the cutoff
PANEL_FUTURE_DATED = "MONTHLY_PANEL_FUTURE_DATED"     # ahead of the session (never rebuilt back)
PANEL_UNVERIFIABLE = "MONTHLY_PANEL_COVERAGE_UNVERIFIABLE"
PANEL_UNIVERSE_FAILED = "HISTORICAL_UNIVERSE_COVERAGE_FAILED"  # historical names would be lost
SOURCE_PANEL_STATES = (PANEL_CURRENT, PANEL_STALE, PANEL_REFRESHING, PANEL_INCOMPLETE,
                       PANEL_FUTURE_DATED, PANEL_UNVERIFIABLE, PANEL_UNIVERSE_FAILED)
#: The established ``status`` spelling for a panel behind the eligible session.
STATUS_PANEL_BEHIND = "MONTHLY_PANEL_BEHIND_ELIGIBLE"
STATUS_PANEL_CURRENT = "MONTHLY_PANEL_CURRENT"
#: Refresh failures that are PERMANENT for this session (a retry cannot change them)
#: rather than transient. Both still fail closed; only the operator wording differs.
_PERMANENT_REFRESH_CODES = frozenset({PANEL_UNIVERSE_FAILED, PANEL_FUTURE_DATED})

# Stdout marker the driver prints so the bridge can parse the Phase-25 manifest.
_RESULT_MARKER = "EMITTER_RESULT_JSON:"
# Stdout marker the bounded-refresh driver prints so the bridge can parse its manifest.
_REFRESH_MARKER = "PANEL_REFRESH_RESULT_JSON:"

# Bounded retention of research-side diagnostics / temp roots (never unbounded).
_MAX_DIAGNOSTICS = 20
_MAX_RETAINED_TMP = 5


# --------------------------------------------------------------------------- #
# The subprocess DRIVER. A version-controlled constant executed as an explicit
# argv element (``[python, "-c", DRIVER, repo, npz, out_dir]``) — NOT a shell
# string. It inserts the external repo on sys.path, runs the OWNED Phase-25
# mathematics into the ISOLATED out_dir (never the canonical inputs dir) and
# prints a compact JSON manifest. It imports numpy/pandas ONLY inside the
# subprocess (transitively via the research modules), never in this process.
# --------------------------------------------------------------------------- #
DRIVER_SRC = (
    "import sys, json\n"
    "repo, npz, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]\n"
    "if repo:\n"
    "    sys.path.insert(0, repo)\n"
    "from research.phase25_multi_horizon_inputs import build_inputs\n"
    "m = build_inputs(npz_path=(npz or None), out_dir=out_dir, log=lambda *a, **k: None)\n"
    "summary = {\n"
    "    'market_as_of_date': m.get('market_as_of_date'),\n"
    "    'current_month_label': m.get('current_month_label'),\n"
    "    'source_npz_fingerprint': m.get('source_npz_fingerprint'),\n"
    "    'counts': m.get('counts'),\n"
    "    'outputs': m.get('outputs'),\n"
    "    'out_dir': m.get('out_dir') or out_dir,\n"
    "}\n"
    "sys.stdout.write('" + _RESULT_MARKER + "' + json.dumps(summary) + '\\n')\n"
)

# --------------------------------------------------------------------------- #
# The BOUNDED PANEL REFRESH driver (Release 54.2.3). Same discipline as above: a
# version-controlled constant executed as an explicit argv element, never a shell
# string. It calls the panel OWNER's own bounded entry point with the internally
# supplied as-of cutoff — this process computes no panel, imports no numpy and
# chooses no date. A refusal by the owner's quality contract is reported as a
# structured code, not a stack trace.
# --------------------------------------------------------------------------- #
REFRESH_DRIVER_SRC = (
    "import sys, json\n"
    "repo, as_of = sys.argv[1], sys.argv[2]\n"
    "if repo:\n"
    "    sys.path.insert(0, repo)\n"
    "from research.phase24_daily_panel import refresh_daily_panel_as_of, PanelRefreshError\n"
    "try:\n"
    "    m = refresh_daily_panel_as_of(as_of=as_of, log=lambda *a, **k: None)\n"
    "    out = {'ok': True, 'as_of': as_of, 'last_date': str(m.get('last_date'))[:10],\n"
    "           'first_date': str(m.get('first_date'))[:10],\n"
    "           'n_trading_days': m.get('n_trading_days'),\n"
    "           'securities_pulled': m.get('securities_pulled'),\n"
    "           'delisted_or_removed': m.get('delisted_or_removed'),\n"
    "           'symbols_missing': m.get('symbols_missing'),\n"
    "           'price_coverage_fraction': m.get('price_coverage_fraction'),\n"
    "           'as_of_cutoff': m.get('as_of_cutoff'),\n"
    "           'previous_last_date': m.get('previous_last_date'),\n"
    "           'build_seconds': m.get('build_seconds')}\n"
    "except PanelRefreshError as exc:\n"
    "    out = {'ok': False, 'as_of': as_of, 'code': getattr(exc, 'code', None),\n"
    "           'error': str(exc)[:400], 'detail': getattr(exc, 'detail', {})}\n"
    "sys.stdout.write('" + _REFRESH_MARKER + "' + json.dumps(out, default=str) + '\\n')\n"
)


# --------------------------------------------------------------------------- #
# Errors. A DATA_HOLD (recoverable) is mapped by the adapter to an honest BLOCKED
# (S_UNAVAILABLE); a hard error maps to S_INVALID (the produced artifact is wrong).
# --------------------------------------------------------------------------- #
class MonthlyEmitterError(RuntimeError):
    """A hard failure producing the monthly momentum input (adapter -> S_INVALID)."""

    monthly_data_hold = False

    def __init__(self, message: str, *, emitter_status: Optional[str] = None,
                 retry: str = "PERMANENT", detail: Optional[dict] = None):
        super().__init__(message)
        self.emitter_status = emitter_status
        self.retry_classification = retry
        self.detail = detail or {}


class MonthlyEmitterHold(MonthlyEmitterError):
    """A recoverable DATA_HOLD (adapter -> S_UNAVAILABLE / BLOCKED). Marker attr set."""

    monthly_data_hold = True

    def __init__(self, message: str, *, emitter_status: Optional[str] = None,
                 retry: str = "TRANSIENT", detail: Optional[dict] = None):
        super().__init__(message, emitter_status=emitter_status, retry=retry,
                         detail=detail)


# --------------------------------------------------------------------------- #
# Small pure helpers.
# --------------------------------------------------------------------------- #
def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _month_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 7 and s[4] == "-" and s[:4].isdigit() and s[5:7].isdigit():
        return s[:7]
    return None


def _eligible_month(value: Any) -> Optional[str]:
    d = _coerce_date(value)
    return d.strftime("%Y-%m") if d else None


def _hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _content_hash(rows: list[dict]) -> str:
    ident = sorted(
        tuple(str(r.get(c) if r.get(c) is not None else "") for c in _IDENTITY_COLUMNS)
        for r in rows)
    return _hash(ident)


def _fingerprint_file(path: Any) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
    except OSError:
        return None


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _read_csv_rows(path: Path) -> tuple[Optional[list[dict]], Optional[list[str]]]:
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            return rows, list(reader.fieldnames or [])
    except OSError:
        return None, None


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _tail(text: Any, n: int = 4000) -> str:
    s = "" if text is None else str(text)
    return s[-n:]


def _compact_argv(argv: list) -> list:
    """A log-safe argv: the inline driver source is replaced by a short marker so the
    manifest/diagnostic stays compact (there are no secrets in the argv — paths only)."""
    out = []
    skip_next = False
    for i, a in enumerate(argv or []):
        if skip_next:
            out.append("<DRIVER_SRC>")
            skip_next = False
            continue
        out.append(a)
        if a == "-c":
            skip_next = True
    return out


def _parse_marker(stdout: str, marker: str) -> dict:
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(marker):
            try:
                return json.loads(line[len(marker):])
            except ValueError:
                return {}
    return {}


def _parse_result_marker(stdout: str) -> dict:
    return _parse_marker(stdout, _RESULT_MARKER)


def _parse_refresh_marker(stdout: str) -> dict:
    return _parse_marker(stdout, _REFRESH_MARKER)


def _rmtree(path: Path) -> None:
    try:
        import shutil
        shutil.rmtree(str(path), ignore_errors=True)
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------- #
# Configuration.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EmitterConfig:
    repo: str
    python: str
    panel_npz: str
    panel_manifest: str
    work_dir: str
    timeout_seconds: int
    phase24_module: str
    phase25_module: str
    #: Release 54.2.3 — the CONTROLLED bounded source-panel refresh.
    panel_refresh_enabled: bool = True
    panel_refresh_timeout_seconds: int = DEFAULT_PANEL_REFRESH_TIMEOUT


def _int_env(name: str, default: int) -> int:
    try:
        v = int(str(os.environ.get(name, "")).strip())
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def resolve_config(overrides: Optional[dict] = None) -> EmitterConfig:
    o = dict(overrides or {})

    def pick(key: str, env: str, default: str) -> str:
        if o.get(key) is not None:
            return str(o[key])
        v = os.environ.get(env)
        return str(v) if v else default

    repo = pick("repo", REPO_ENV, DEFAULT_REPO)
    python = pick("python", PYTHON_ENV, DEFAULT_PYTHON)
    panel_npz = pick("panel_npz", PANEL_NPZ_ENV, DEFAULT_PANEL_NPZ)
    panel_manifest = pick("panel_manifest", PANEL_MANIFEST_ENV, DEFAULT_PANEL_MANIFEST)
    work_dir = pick("work_dir", WORK_ENV, DEFAULT_WORK_DIR)
    timeout = int(o["timeout_seconds"]) if o.get("timeout_seconds") else \
        _int_env(TIMEOUT_ENV, DEFAULT_TIMEOUT)
    phase24 = str(Path(repo) / PHASE24_REL)
    phase25 = str(Path(repo) / PHASE25_REL)
    if o.get("panel_refresh_enabled") is not None:
        refresh_enabled = bool(o["panel_refresh_enabled"])
    else:
        refresh_enabled = str(os.environ.get(PANEL_REFRESH_DISABLE_ENV, "")).strip().lower() \
            not in ("1", "true", "yes")
    refresh_timeout = int(o["panel_refresh_timeout_seconds"]) \
        if o.get("panel_refresh_timeout_seconds") \
        else _int_env(PANEL_REFRESH_TIMEOUT_ENV, DEFAULT_PANEL_REFRESH_TIMEOUT)
    return EmitterConfig(repo=repo, python=python, panel_npz=panel_npz,
                         panel_manifest=panel_manifest, work_dir=work_dir,
                         timeout_seconds=int(timeout), phase24_module=phase24,
                         phase25_module=phase25,
                         panel_refresh_enabled=refresh_enabled,
                         panel_refresh_timeout_seconds=int(refresh_timeout))


def _config_summary(cfg: EmitterConfig) -> dict:
    """Paths + timeout only (there are no secrets in the emitter configuration)."""
    return {
        "repo": cfg.repo, "python": cfg.python, "panel_npz": cfg.panel_npz,
        "panel_manifest": cfg.panel_manifest, "work_dir": cfg.work_dir,
        "timeout_seconds": cfg.timeout_seconds,
        "panel_refresh_enabled": cfg.panel_refresh_enabled,
        "panel_refresh_timeout_seconds": cfg.panel_refresh_timeout_seconds,
    }


def check_availability(cfg: EmitterConfig) -> dict:
    disabled = str(os.environ.get(DISABLE_ENV, "")).strip().lower() in ("1", "true", "yes")
    repo_ok = Path(cfg.repo).is_dir()
    python_ok = Path(cfg.python).is_file()
    phase24_ok = Path(cfg.phase24_module).is_file()
    phase25_ok = Path(cfg.phase25_module).is_file()
    panel_ok = Path(cfg.panel_npz).is_file()
    manifest_ok = Path(cfg.panel_manifest).is_file()
    reasons: list[str] = []
    if disabled:
        reasons.append("DISABLED_BY_ENV")
    if not repo_ok:
        reasons.append("REPO_MISSING")
    if not python_ok:
        reasons.append("PYTHON_MISSING")
    if not phase24_ok:
        reasons.append("PHASE24_MODULE_MISSING")
    if not phase25_ok:
        reasons.append("PHASE25_MODULE_MISSING")
    if not panel_ok:
        reasons.append("SOURCE_PANEL_MISSING")
    if not manifest_ok:
        reasons.append("PANEL_MANIFEST_MISSING")
    available = (not disabled and repo_ok and python_ok and phase24_ok
                 and phase25_ok and panel_ok and manifest_ok)
    return {"available": bool(available), "disabled": bool(disabled),
            "reasons": reasons, "repo_ok": repo_ok, "python_ok": python_ok,
            "phase24_ok": phase24_ok, "phase25_ok": phase25_ok, "panel_ok": panel_ok,
            "panel_manifest_ok": manifest_ok}


# --------------------------------------------------------------------------- #
# Source-panel policy (Workstream C). Reads the owned Phase-24 panel MANIFEST
# (pure-stdlib JSON; no numpy) and decides whether the panel already covers the
# eligible session. Phase 24 supports NO safe incremental extension, so a behind /
# future / unverifiable panel is an explicit blocker (never a silent rebuild).
# --------------------------------------------------------------------------- #
def inspect_source_panel(cfg: EmitterConfig, *, eligible: Any) -> dict:
    elig_d = _coerce_date(eligible)
    manifest = _read_json(Path(cfg.panel_manifest))
    last = _coerce_date((manifest or {}).get("last_date"))
    first = _coerce_date((manifest or {}).get("first_date"))
    refresh_supported = bool(cfg.panel_refresh_enabled)
    out: dict[str, Any] = {
        "panel_manifest": cfg.panel_manifest, "panel_npz": cfg.panel_npz,
        "panel_last_date": last.isoformat() if last else None,
        "panel_first_date": first.isoformat() if first else None,
        "panel_as_of_cutoff": (manifest or {}).get("as_of_cutoff"),
        "eligible": elig_d.isoformat() if elig_d else None,
        # A bounded as-of rebuild is supported (Release 54.2.3); an INCREMENTAL
        # extension of the existing NPZ still is not, and never needed to be.
        "incremental_supported": False, "refresh_supported": refresh_supported,
        "refresh_bounded_to_session": refresh_supported,
    }
    if elig_d is None:
        out.update(action=_PANEL_BLOCK, status="MONTHLY_PANEL_ELIGIBLE_UNKNOWN",
                   panel_state=PANEL_UNVERIFIABLE,
                   covered=False, refresh_required=False,
                   reason="No eligible session date; cannot evaluate source-panel coverage.")
        return out
    if last is None:
        out.update(action=_PANEL_BLOCK, status=PANEL_UNVERIFIABLE,
                   panel_state=PANEL_UNVERIFIABLE,
                   covered=False, refresh_required=True,
                   reason=("Owned Phase-24 panel manifest is missing/unreadable at %s; "
                           "coverage cannot be verified — refusing to emit blindly."
                           % cfg.panel_manifest))
        return out
    if last > elig_d:
        # NEVER "repaired" by a bounded rebuild: rebuilding backwards would discard
        # observations a later session legitimately holds. Blocking is the safe answer.
        out.update(action=_PANEL_BLOCK, status=PANEL_FUTURE_DATED,
                   panel_state=PANEL_FUTURE_DATED,
                   covered=False, refresh_required=False,
                   reason=("Owned panel last trading date %s is AHEAD of the eligible "
                           "session %s (future data)." % (last.isoformat(),
                                                          elig_d.isoformat())))
        return out
    if last < elig_d:
        if refresh_supported:
            out.update(action=_PANEL_REFRESH_BOUNDED, status=STATUS_PANEL_BEHIND,
                       panel_state=PANEL_STALE,
                       covered=False, refresh_required=True,
                       refresh_as_of=elig_d.isoformat(),
                       reason=("Owned survivorship-free panel last date %s is BEHIND the "
                               "eligible session %s. ONE controlled refresh bounded to %s "
                               "will advance it; no observation later than that session is "
                               "read." % (last.isoformat(), elig_d.isoformat(),
                                          elig_d.isoformat())))
            return out
        out.update(action=_PANEL_BLOCK, status=STATUS_PANEL_BEHIND, panel_state=PANEL_STALE,
                   covered=False, refresh_required=True,
                   reason=("Owned survivorship-free panel last date %s is BEHIND the "
                           "eligible session %s and the controlled bounded refresh is "
                           "disabled, so the new month's frozen input cannot be produced."
                           % (last.isoformat(), elig_d.isoformat())))
        return out
    out.update(action=_PANEL_USE_EXISTING, status=STATUS_PANEL_CURRENT,
               panel_state=PANEL_CURRENT,
               covered=True, refresh_required=False,
               reason=("Owned panel covers the eligible session %s; no provider call."
                       % elig_d.isoformat()))
    return out


# --------------------------------------------------------------------------- #
# Explicit subprocess argument array (Workstream A). Never a shell string.
# --------------------------------------------------------------------------- #
def build_run_command(cfg: EmitterConfig, *, out_dir: str) -> list[str]:
    return [cfg.python, "-c", DRIVER_SRC, cfg.repo, cfg.panel_npz, str(out_dir)]


def build_refresh_command(cfg: EmitterConfig, *, as_of: Any) -> list[str]:
    """The bounded panel-refresh argv. ``as_of`` is the eligible research session and is
    the ONLY date the driver ever sees — there is no "latest" mode and no operator input."""
    d = _coerce_date(as_of)
    if d is None:
        raise MonthlyEmitterError(
            "A bounded source-panel refresh requires an as-of session.",
            emitter_status=PANEL_UNVERIFIABLE)
    return [cfg.python, "-c", REFRESH_DRIVER_SRC, cfg.repo, d.isoformat()]


def _default_runner(cmd: list[str], *, timeout: int, cwd: Optional[str] = None) -> dict:
    t0 = time.monotonic()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                              cwd=cwd)  # noqa: S603 (explicit argv, no shell)
        return {"argv": list(cmd), "returncode": proc.returncode,
                "stdout": proc.stdout or "", "stderr": proc.stderr or "",
                "duration_seconds": round(time.monotonic() - t0, 3), "timed_out": False}
    except subprocess.TimeoutExpired as exc:
        return {"argv": list(cmd), "returncode": None,
                "stdout": exc.stdout if isinstance(exc.stdout, str) else "",
                "stderr": exc.stderr if isinstance(exc.stderr, str) else "",
                "duration_seconds": round(time.monotonic() - t0, 3), "timed_out": True}


# --------------------------------------------------------------------------- #
# Bounded research-side diagnostics / emission manifests (never the operational
# ledger; never the canonical inputs dir).
# --------------------------------------------------------------------------- #
def _prune(dir_path: Path, keep: int) -> None:
    try:
        entries = sorted(dir_path.iterdir(), key=lambda p: p.name)
    except OSError:
        return
    for old in entries[:-keep] if keep > 0 else entries:
        if old.is_dir():
            _rmtree(old)
        else:
            try:
                old.unlink()
            except OSError:
                pass


def _write_diagnostic(cfg: EmitterConfig, record: dict, *, reason: str) -> str:
    diag_dir = Path(cfg.work_dir) / "_diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)
    name = "%s_%s_%s.json" % (str(record.get("eligible") or "unknown"),
                              reason, os.getpid())
    path = diag_dir / name
    _atomic_write_json(path, {"phase": PHASE, "reason": reason,
                              "owner": CANONICAL_EMITTER_OWNER, **record})
    _prune(diag_dir, _MAX_DIAGNOSTICS)
    return str(path)


def _write_emission_manifest(cfg: EmitterConfig, record: dict) -> str:
    em_dir = Path(cfg.work_dir) / "_emissions"
    em_dir.mkdir(parents=True, exist_ok=True)
    path = em_dir / ("%s.json" % str(record.get("eligible") or "unknown"))
    _atomic_write_json(path, {"phase": PHASE, "owner": CANONICAL_EMITTER_OWNER,
                              "math_owner": EMITTER_SOURCE, "panel_owner": PANEL_SOURCE,
                              **record})
    _prune(em_dir, _MAX_DIAGNOSTICS)
    return str(path)


# --------------------------------------------------------------------------- #
# Output validation (Workstream D). Validates the Phase-25 outputs BEFORE the
# adapter promotes anything: files exist, non-empty, expected schema, unique
# tickers, produced month == eligible month, produced market date == eligible
# date, no future observation, no null required scores, provenance + source-panel
# fingerprint, strategy identity, deterministic content hash, no intramonth
# approximation. A wrong month / wrong date fails here (before promotion).
# --------------------------------------------------------------------------- #
def _load_and_validate_outputs(cfg: EmitterConfig, out_dir: Path, *, month: Any,
                               eligible: Any, panel: dict, run_record: dict,
                               driver_summary: dict) -> dict:
    out_dir = Path(out_dir)
    mom_path = out_dir / OUTPUT_MOM_FILE
    manifest_path = out_dir / OUTPUT_MANIFEST_FILE
    elig_d = _coerce_date(eligible)
    elig_month = _eligible_month(eligible)
    due_month = _month_str(month) or elig_month

    if not mom_path.exists():
        raise MonthlyEmitterError(
            "Emitter produced no %s in the isolated output dir." % OUTPUT_MOM_FILE,
            emitter_status="MONTHLY_OUTPUT_MISSING", detail=run_record)

    rows, fieldnames = _read_csv_rows(mom_path)
    if not rows:
        raise MonthlyEmitterError(
            "Produced %s is empty." % OUTPUT_MOM_FILE,
            emitter_status="MONTHLY_OUTPUT_EMPTY", detail=run_record)

    manifest = _read_json(manifest_path) or {}
    errors: list[str] = []

    # Schema.
    missing_cols = [c for c in REQUIRED_OUTPUT_COLUMNS if c not in (fieldnames or [])]
    if missing_cols:
        errors.append("SCHEMA_MISSING_COLUMNS: %s" % ", ".join(missing_cols))

    # Produced month / market date, and per-row uniqueness + non-null scores.
    seen: set[str] = set()
    dup = 0
    null_score = 0
    prod_month = _month_str((rows[0] or {}).get("month_label"))
    prod_asof = _coerce_date((rows[0] or {}).get("market_as_of_date"))
    month_mismatch_rows = 0
    for r in rows:
        tk = (r.get("ticker") or "").strip().upper()
        if not tk:
            errors.append("EMPTY_TICKER encountered.")
            break
        if tk in seen:
            dup += 1
        seen.add(tk)
        mv = r.get("mom_6_1")
        try:
            if mv is None or str(mv).strip() == "" or \
                    (mv != mv):  # NaN string never equals itself after float()
                null_score += 1
            else:
                float(mv)
        except (TypeError, ValueError):
            null_score += 1
        if _month_str(r.get("month_label")) != prod_month:
            month_mismatch_rows += 1

    if dup:
        errors.append("DUPLICATE_TICKERS: %d duplicate ticker rows." % dup)
    if null_score:
        errors.append("NULL_REQUIRED_SCORE: %d rows with a missing/invalid mom_6_1." % null_score)
    if month_mismatch_rows:
        errors.append("INCONSISTENT_MONTH_ROWS: %d rows disagree on month_label." % month_mismatch_rows)

    # Period: produced month must equal the eligible/due month.
    if prod_month is None:
        errors.append("MISSING_PERIOD: produced rows carry no month_label.")
    elif elig_month is not None and prod_month != elig_month:
        errors.append("PERIOD_MISMATCH: produced month %s != eligible month %s."
                      % (prod_month, elig_month))
    elif due_month is not None and prod_month != due_month:
        errors.append("PERIOD_MISMATCH: produced month %s != due month %s."
                      % (prod_month, due_month))

    # Provenance / date: produced market date must equal the eligible session and
    # never be future-dated.
    if prod_asof is None:
        errors.append("MISSING_PROVENANCE: produced rows carry no market_as_of_date.")
    else:
        if elig_d is not None and prod_asof > elig_d:
            errors.append("FUTURE_PROVENANCE: produced market date %s is later than the "
                          "eligible session %s." % (prod_asof.isoformat(), elig_d.isoformat()))
        elif elig_d is not None and prod_asof != elig_d:
            errors.append("PROVENANCE_MISMATCH: produced market date %s != eligible "
                          "session %s." % (prod_asof.isoformat(), elig_d.isoformat()))

    # Manifest cross-check (provenance).
    man_asof = _coerce_date(manifest.get("market_as_of_date"))
    man_month = _month_str(manifest.get("current_month_label"))
    if manifest:
        if man_asof is not None and elig_d is not None and man_asof != elig_d:
            errors.append("MANIFEST_DATE_MISMATCH: manifest market_as_of_date %s != "
                          "eligible %s." % (man_asof.isoformat(), elig_d.isoformat()))
        if man_month is not None and elig_month is not None and man_month != elig_month:
            errors.append("MANIFEST_MONTH_MISMATCH: manifest month %s != eligible month %s."
                          % (man_month, elig_month))
    else:
        errors.append("MISSING_MANIFEST: no %s produced (provenance unverifiable)."
                      % OUTPUT_MANIFEST_FILE)

    # No intramonth approximation marker may appear in the produced manifest.
    if manifest.get("approximated") or manifest.get("intramonth") \
            or manifest.get("intramonth_approximation"):
        errors.append("INTRAMONTH_APPROXIMATION_FORBIDDEN: the frozen mom_6_1 monthly "
                      "input is never approximated intramonth.")

    if errors:
        raise MonthlyEmitterError(
            "Produced monthly momentum artifact failed validation: %s" % "; ".join(errors),
            emitter_status="MONTHLY_OUTPUT_VALIDATION_FAILED",
            detail={**run_record, "errors": errors})

    # Source-panel fingerprint (identity of the owned panel that produced this).
    panel_fp = _fingerprint_file(cfg.panel_npz)
    manifest_fp = manifest.get("source_npz_fingerprint")

    artifact = {
        "month_label": prod_month,
        "market_as_of_date": prod_asof.isoformat() if prod_asof else None,
        "rows": rows,
        "source": EMITTER_SOURCE,
        "approximated": False,
        "content_hash": _content_hash(rows),
        "panel_fingerprint": panel_fp,
        "source_panel": {
            "last_date": panel.get("panel_last_date"),
            "npz": cfg.panel_npz, "npz_fingerprint": panel_fp,
            "manifest_fingerprint": manifest_fp,
            "fingerprint_matches_manifest": (manifest_fp is not None
                                             and panel_fp is not None
                                             and manifest_fp == panel_fp),
        },
        "provenance": {
            "emitter_owner": CANONICAL_EMITTER_OWNER,
            "math_owner": EMITTER_SOURCE,
            "panel_owner": PANEL_SOURCE,
            "market_as_of_date": prod_asof.isoformat() if prod_asof else None,
            "month_label": prod_month,
            "counts": manifest.get("counts") or driver_summary.get("counts"),
        },
        "run": {
            "argv": run_record.get("argv"), "returncode": run_record.get("returncode"),
            "duration_seconds": run_record.get("duration_seconds"),
            "stdout_tail": run_record.get("stdout_tail"),
            "stderr_tail": run_record.get("stderr_tail"),
        },
        "row_count": len(rows),
        "emitter_status": "MONTHLY_EMITTER_PRODUCED",
        "validated": True,
    }
    return artifact


# --------------------------------------------------------------------------- #
# Isolated production emission (Workstream B). Unique temp dir under the emitter
# work root; run Phase 25 into it; capture args/stdout/stderr/exit/duration;
# validate; write an emission manifest and delete the temp on success; retain a
# compact diagnostic on failure (never touching the live canonical inputs).
# --------------------------------------------------------------------------- #
def _emit_in_isolation(cfg: EmitterConfig, *, month: Any, eligible: Any, panel: dict,
                       runner: Callable) -> dict:
    tmp_root = Path(cfg.work_dir) / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    _prune(tmp_root, _MAX_RETAINED_TMP)  # bound retained failed temp roots
    tmp = Path(tempfile.mkdtemp(prefix="mme_%s_" % (_coerce_date(eligible) or "run"),
                                dir=str(tmp_root)))
    cmd = build_run_command(cfg, out_dir=str(tmp))
    result = runner(cmd, timeout=cfg.timeout_seconds, cwd=cfg.repo)
    record = {
        "argv": _compact_argv(result.get("argv") or cmd),
        "returncode": result.get("returncode"),
        "duration_seconds": result.get("duration_seconds"),
        "timed_out": bool(result.get("timed_out")),
        "stdout_tail": _tail(result.get("stdout")),
        "stderr_tail": _tail(result.get("stderr")),
        "month": _month_str(month), "eligible": str(_coerce_date(eligible) or ""),
        "out_dir": str(tmp), "panel": panel,
    }
    try:
        if result.get("timed_out"):
            _write_diagnostic(cfg, record, reason="TIMEOUT")
            raise MonthlyEmitterHold(
                "Monthly emitter subprocess timed out after %ss." % cfg.timeout_seconds,
                emitter_status="MONTHLY_EMITTER_TIMEOUT", retry="TRANSIENT",
                detail=record)
        if result.get("returncode") != 0:
            _write_diagnostic(cfg, record, reason="NONZERO_EXIT")
            raise MonthlyEmitterError(
                "Monthly emitter subprocess exited with code %s." % result.get("returncode"),
                emitter_status="MONTHLY_EMITTER_SUBPROCESS_FAILED", detail=record)
        summary = _parse_result_marker(result.get("stdout") or "")
        artifact = _load_and_validate_outputs(
            cfg, tmp, month=month, eligible=eligible, panel=panel,
            run_record=record, driver_summary=summary)
    except MonthlyEmitterError:
        # Retain the diagnostic evidence; never mutate the live canonical inputs.
        raise
    except Exception as exc:  # noqa: BLE001
        _write_diagnostic(cfg, {**record, "unexpected_error": str(exc)[:300]},
                          reason="UNEXPECTED")
        raise MonthlyEmitterError(
            "Monthly emitter post-processing failed: %s" % str(exc)[:200],
            emitter_status="MONTHLY_EMITTER_OUTPUT_ERROR", detail=record) from exc

    # Success: record a compact emission manifest and delete the temp working dir.
    _write_emission_manifest(cfg, {
        **record, "content_hash": artifact["content_hash"],
        "market_as_of_date": artifact["market_as_of_date"],
        "month_label": artifact["month_label"], "row_count": artifact["row_count"],
        "panel_fingerprint": artifact.get("panel_fingerprint"),
        "source_panel": artifact.get("source_panel"),
    })
    _rmtree(tmp)
    return artifact


# --------------------------------------------------------------------------- #
# Release 54.2.3 — the CONTROLLED bounded source-panel refresh.
#
# Called ONLY from ``production_emitter``, and only when ``inspect_source_panel``
# decided REFRESH_BOUNDED for this eligible session. It performs exactly ONE
# refresh per emission attempt, drives the panel OWNER's own bounded entry point,
# and re-inspects the panel afterwards so the decision to emit rests on the
# manifest the refresh actually produced — never on the fact that a refresh ran.
#
# Every failure path fails CLOSED (the previous panel is left intact by the owner's
# atomic promotion) and is reported with the canonical data-quality code, so the
# operator is told which condition stopped it rather than "the refresh failed".
# --------------------------------------------------------------------------- #
def refresh_source_panel(cfg: EmitterConfig, *, as_of: Any,
                         runner: Optional[Callable] = None) -> dict:
    """Run ONE bounded as-of refresh of the owned source panel. Returns the refresh
    record; raises ``MonthlyEmitterHold`` when the panel could not be advanced safely."""
    run = runner if runner is not None else _default_runner
    cmd = build_refresh_command(cfg, as_of=as_of)
    d = _coerce_date(as_of)
    result = run(cmd, timeout=cfg.panel_refresh_timeout_seconds, cwd=cfg.repo)
    parsed = _parse_refresh_marker(result.get("stdout") or "")
    record = {
        "as_of": d.isoformat() if d else None,
        # ``eligible`` names the diagnostic file (the refresh is always bound to the
        # eligible session, so the two are the same date by construction).
        "eligible": d.isoformat() if d else None,
        "argv": _compact_argv(result.get("argv") or cmd),
        "returncode": result.get("returncode"),
        "duration_seconds": result.get("duration_seconds"),
        "timed_out": bool(result.get("timed_out")),
        "stdout_tail": _tail(result.get("stdout")),
        "stderr_tail": _tail(result.get("stderr")),
        "refresh": parsed,
        "bounded_to_session": True,
        "operator_supplied_date": False,
        "owner": PANEL_SOURCE,
        "policy_owner": CANONICAL_EMITTER_OWNER,
    }

    def _hold(code: str, message: str) -> None:
        _write_diagnostic(cfg, {**record, "panel_state": code}, reason="PANEL_REFRESH")
        raise MonthlyEmitterHold(
            message, emitter_status=code,
            retry=("PERMANENT" if code in _PERMANENT_REFRESH_CODES else "TRANSIENT"),
            detail=record)

    if result.get("timed_out"):
        _hold(PANEL_REFRESHING,
              "The bounded source-panel refresh for %s timed out after %ss; the previous "
              "panel is unchanged." % (record["as_of"], cfg.panel_refresh_timeout_seconds))
    if result.get("returncode") != 0:
        _hold(PANEL_INCOMPLETE,
              "The bounded source-panel refresh for %s exited with code %s; nothing was "
              "promoted." % (record["as_of"], result.get("returncode")))
    if not parsed:
        _hold(PANEL_UNVERIFIABLE,
              "The bounded source-panel refresh for %s produced no result manifest, so its "
              "outcome cannot be verified." % record["as_of"])
    if not parsed.get("ok"):
        code = str(parsed.get("code") or PANEL_INCOMPLETE)
        if code not in SOURCE_PANEL_STATES:
            code = PANEL_INCOMPLETE
        _hold(code, "The owned source panel could not be advanced to %s: %s"
                    % (record["as_of"], parsed.get("error") or code))

    # The refresh CLAIMS success; the manifest on disk decides. A claimed cutoff that
    # does not match the panel actually persisted is never taken on trust.
    after = inspect_source_panel(cfg, eligible=as_of)
    record["panel_after"] = after
    record["panel_state"] = after.get("panel_state")
    if not after.get("covered"):
        _hold(str(after.get("panel_state") or PANEL_INCOMPLETE),
              "The source panel still does not cover %s after a bounded refresh (%s)."
              % (record["as_of"], after.get("reason") or "no reason reported"))
    record["panel_state"] = PANEL_CURRENT
    _write_diagnostic(cfg, record, reason="PANEL_REFRESH_OK")
    return record


# --------------------------------------------------------------------------- #
# The production emitter callable (matches the adapter seam). Never writes the
# canonical CSV — the adapter owns promotion.
# --------------------------------------------------------------------------- #
def production_emitter(*, month: Any, eligible: Any, inputs_dir: Any = None,
                       config: Optional[EmitterConfig] = None,
                       runner: Optional[Callable] = None,
                       panel_refresh_runner: Optional[Callable] = None) -> dict:
    cfg = config if config is not None else resolve_config()
    avail = check_availability(cfg)
    if not avail["available"]:
        raise MonthlyEmitterHold(
            "Monthly emitter environment unavailable: %s" % ", ".join(avail["reasons"]),
            emitter_status="MONTHLY_EMITTER_ENVIRONMENT_UNAVAILABLE", detail=avail)
    panel = inspect_source_panel(cfg, eligible=eligible)
    panel_refresh = None
    if panel["action"] == _PANEL_REFRESH_BOUNDED:
        # ONE controlled, session-bounded refresh — the prerequisite maintenance the
        # governed cycle performs for itself, so the operator runs no separate step.
        panel_refresh = refresh_source_panel(cfg, as_of=eligible,
                                             runner=panel_refresh_runner)
        panel = panel_refresh.get("panel_after") or inspect_source_panel(
            cfg, eligible=eligible)
        panel = {**panel, "refreshed": True,
                 "refreshed_as_of": panel_refresh.get("as_of")}
    if panel["action"] != _PANEL_USE_EXISTING:
        raise MonthlyEmitterHold(panel["reason"], emitter_status=panel["status"],
                                 detail=panel)
    artifact = _emit_in_isolation(
        cfg, month=month, eligible=eligible, panel=panel,
        runner=runner if runner is not None else _default_runner)
    if panel_refresh is not None:
        artifact["source_panel_refresh"] = {
            k: panel_refresh.get(k) for k in
            ("as_of", "duration_seconds", "panel_state", "bounded_to_session",
             "operator_supplied_date", "refresh")}
    return artifact


# --------------------------------------------------------------------------- #
# Resolver used by the adapter's production wiring. Returns a seam-shaped callable
# ONLY when the environment is available (else None → the adapter blocks honestly
# with an S_UNAVAILABLE / DATA_HOLD, never a stack trace).
# --------------------------------------------------------------------------- #
def resolve_production_emitter(config: Optional[EmitterConfig] = None) -> Optional[Callable]:
    cfg = config if config is not None else resolve_config()
    if not check_availability(cfg)["available"]:
        return None

    def _emit(*, month, eligible, inputs_dir):
        return production_emitter(month=month, eligible=eligible,
                                  inputs_dir=inputs_dir, config=cfg)

    return _emit


# --------------------------------------------------------------------------- #
# Read-only status for the Daily Research Cycle status contract (no subprocess).
# --------------------------------------------------------------------------- #
def status(config: Optional[EmitterConfig] = None, *, eligible: Any = None) -> dict:
    cfg = config if config is not None else resolve_config()
    avail = check_availability(cfg)
    out = {
        "emitter_owner": CANONICAL_EMITTER_OWNER,
        "math_owner": EMITTER_SOURCE,
        "panel_owner": PANEL_SOURCE,
        "available": bool(avail["available"]),
        "availability": avail,
        "config": _config_summary(cfg),
        "incremental_supported": False,
        # Release 54.2.3 — a bounded as-of rebuild IS supported; it is what makes a
        # behind-panel a recoverable input rather than a true blocker.
        "panel_refresh_supported": bool(cfg.panel_refresh_enabled),
        "panel_refresh_bounded_to_session": bool(cfg.panel_refresh_enabled),
        "source_panel_state_vocabulary": list(SOURCE_PANEL_STATES),
    }
    if eligible is not None:
        panel = inspect_source_panel(cfg, eligible=eligible)
        out["source_panel"] = {
            "panel_last_date": panel.get("panel_last_date"),
            "panel_as_of_cutoff": panel.get("panel_as_of_cutoff"),
            "eligible": panel.get("eligible"),
            "covered": panel.get("covered"),
            "refresh_required": panel.get("refresh_required"),
            "refresh_supported": panel.get("refresh_supported"),
            "refresh_bounded_to_session": panel.get("refresh_bounded_to_session"),
            "refresh_as_of": panel.get("refresh_as_of"),
            "action": panel.get("action"),
            "panel_state": panel.get("panel_state"),
            "status": panel.get("status"),
            "reason": panel.get("reason"),
            # THE decision, made by this owner and read (never re-derived) by the Daily
            # Research Cycle: can the panel cover the eligible session right now —
            # either because it already does, or because ONE bounded refresh will
            # advance it? A future-dated or unverifiable panel is False: neither is
            # ever repaired by rebuilding.
            "can_cover_eligible_session": panel.get("action") in (
                _PANEL_USE_EXISTING, _PANEL_REFRESH_BOUNDED),
        }
    return out


__all__ = [
    "PHASE", "CANONICAL_EMITTER_OWNER", "EMITTER_SOURCE", "PANEL_SOURCE",
    "REPO_ENV", "PYTHON_ENV", "PANEL_NPZ_ENV", "PANEL_MANIFEST_ENV", "WORK_ENV",
    "TIMEOUT_ENV", "DISABLE_ENV", "OUTPUT_MOM_FILE", "OUTPUT_MANIFEST_FILE",
    "REQUIRED_OUTPUT_COLUMNS", "EmitterConfig", "MonthlyEmitterError",
    "MonthlyEmitterHold", "resolve_config", "check_availability",
    "inspect_source_panel", "build_run_command", "production_emitter",
    "resolve_production_emitter", "status",
    # Release 54.2.3 — the controlled bounded source-panel refresh.
    "PANEL_REFRESH_DISABLE_ENV", "PANEL_REFRESH_TIMEOUT_ENV",
    "DEFAULT_PANEL_REFRESH_TIMEOUT", "REFRESH_DRIVER_SRC",
    "PANEL_CURRENT", "PANEL_STALE", "PANEL_REFRESHING", "PANEL_INCOMPLETE",
    "PANEL_FUTURE_DATED", "PANEL_UNVERIFIABLE", "PANEL_UNIVERSE_FAILED",
    "SOURCE_PANEL_STATES", "STATUS_PANEL_BEHIND", "STATUS_PANEL_CURRENT",
    "build_refresh_command", "refresh_source_panel",
]
