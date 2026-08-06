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

Source-panel policy (Workstream C): Phase 24 supports NO safe incremental extension
(``build_daily_panel_from_norgate`` is a NO-OP when the NPZ exists and a decades-long
FULL rebuild with ``force=True``). So this bridge NEVER triggers a panel refresh: when
the owned panel already COVERS the eligible session (its last trading date equals the
eligible date) it runs Phase 25 with no provider call; when the panel is BEHIND, FUTURE
or UNVERIFIABLE it returns an explicit DATA_HOLD blocker rather than silently doing an
uncontrolled full rebuild. No future-dated rows; no current-constituent substitution
into historical dates; point-in-time provenance preserved.

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

DEFAULT_REPO = r"C:\Users\binis\Stock_Prediction_app_push"
DEFAULT_PYTHON = r"C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe"
DEFAULT_PANEL_NPZ = (r"D:\Stock_Prediction_app_data\phase24_cache"
                     r"\daily_panel\russell1000_cp_daily.npz")
DEFAULT_PANEL_MANIFEST = (r"D:\Stock_Prediction_app_data\phase24_cache"
                          r"\daily_panel\manifest.json")
DEFAULT_WORK_DIR = r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_emitter"
DEFAULT_TIMEOUT = 900  # 15 minutes; the Phase-25 build reads a ~74MB NPZ + resamples.

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

# Stdout marker the driver prints so the bridge can parse the Phase-25 manifest.
_RESULT_MARKER = "EMITTER_RESULT_JSON:"

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


def _parse_result_marker(stdout: str) -> dict:
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(_RESULT_MARKER):
            try:
                return json.loads(line[len(_RESULT_MARKER):])
            except ValueError:
                return {}
    return {}


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
    return EmitterConfig(repo=repo, python=python, panel_npz=panel_npz,
                         panel_manifest=panel_manifest, work_dir=work_dir,
                         timeout_seconds=int(timeout), phase24_module=phase24,
                         phase25_module=phase25)


def _config_summary(cfg: EmitterConfig) -> dict:
    """Paths + timeout only (there are no secrets in the emitter configuration)."""
    return {
        "repo": cfg.repo, "python": cfg.python, "panel_npz": cfg.panel_npz,
        "panel_manifest": cfg.panel_manifest, "work_dir": cfg.work_dir,
        "timeout_seconds": cfg.timeout_seconds,
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
    out: dict[str, Any] = {
        "panel_manifest": cfg.panel_manifest, "panel_npz": cfg.panel_npz,
        "panel_last_date": last.isoformat() if last else None,
        "panel_first_date": first.isoformat() if first else None,
        "eligible": elig_d.isoformat() if elig_d else None,
        "incremental_supported": False, "refresh_supported": False,
    }
    if elig_d is None:
        out.update(action=_PANEL_BLOCK, status="MONTHLY_PANEL_ELIGIBLE_UNKNOWN",
                   covered=False, refresh_required=False,
                   reason="No eligible session date; cannot evaluate source-panel coverage.")
        return out
    if last is None:
        out.update(action=_PANEL_BLOCK, status="MONTHLY_PANEL_COVERAGE_UNVERIFIABLE",
                   covered=False, refresh_required=True,
                   reason=("Owned Phase-24 panel manifest is missing/unreadable at %s; "
                           "coverage cannot be verified — refusing to emit blindly."
                           % cfg.panel_manifest))
        return out
    if last > elig_d:
        out.update(action=_PANEL_BLOCK, status="MONTHLY_PANEL_FUTURE_DATED",
                   covered=False, refresh_required=False,
                   reason=("Owned panel last trading date %s is AHEAD of the eligible "
                           "session %s (future data)." % (last.isoformat(),
                                                          elig_d.isoformat())))
        return out
    if last < elig_d:
        out.update(action=_PANEL_BLOCK, status="MONTHLY_PANEL_BEHIND_ELIGIBLE",
                   covered=False, refresh_required=True,
                   reason=("Owned survivorship-free panel last date %s is BEHIND the "
                           "eligible session %s. Phase 24 supports no safe incremental "
                           "extension; a controlled owned-panel refresh is required "
                           "(never an uncontrolled full rebuild on the daily cycle)."
                           % (last.isoformat(), elig_d.isoformat())))
        return out
    out.update(action=_PANEL_USE_EXISTING, status="MONTHLY_PANEL_CURRENT",
               covered=True, refresh_required=False,
               reason=("Owned panel covers the eligible session %s; no provider call."
                       % elig_d.isoformat()))
    return out


# --------------------------------------------------------------------------- #
# Explicit subprocess argument array (Workstream A). Never a shell string.
# --------------------------------------------------------------------------- #
def build_run_command(cfg: EmitterConfig, *, out_dir: str) -> list[str]:
    return [cfg.python, "-c", DRIVER_SRC, cfg.repo, cfg.panel_npz, str(out_dir)]


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
# The production emitter callable (matches the adapter seam). Never writes the
# canonical CSV — the adapter owns promotion.
# --------------------------------------------------------------------------- #
def production_emitter(*, month: Any, eligible: Any, inputs_dir: Any = None,
                       config: Optional[EmitterConfig] = None,
                       runner: Optional[Callable] = None) -> dict:
    cfg = config if config is not None else resolve_config()
    avail = check_availability(cfg)
    if not avail["available"]:
        raise MonthlyEmitterHold(
            "Monthly emitter environment unavailable: %s" % ", ".join(avail["reasons"]),
            emitter_status="MONTHLY_EMITTER_ENVIRONMENT_UNAVAILABLE", detail=avail)
    panel = inspect_source_panel(cfg, eligible=eligible)
    if panel["action"] != _PANEL_USE_EXISTING:
        raise MonthlyEmitterHold(panel["reason"], emitter_status=panel["status"],
                                 detail=panel)
    return _emit_in_isolation(cfg, month=month, eligible=eligible, panel=panel,
                              runner=runner if runner is not None else _default_runner)


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
    }
    if eligible is not None:
        panel = inspect_source_panel(cfg, eligible=eligible)
        out["source_panel"] = {
            "panel_last_date": panel.get("panel_last_date"),
            "eligible": panel.get("eligible"),
            "covered": panel.get("covered"),
            "refresh_required": panel.get("refresh_required"),
            "refresh_supported": panel.get("refresh_supported"),
            "status": panel.get("status"),
            "reason": panel.get("reason"),
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
]
