r"""alpha_agent.alpha_recovery.futures_trend_runtime - the TRUE_FORWARD prospective
decision owner for ``ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1`` (MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1).

THE SAME SHAPE AS THE FX CARRY CADENCE PRODUCER, FOR THE SAME REASONS
    ``api.canonical_forward_accrual`` resolves an ALPHA_RECOVERY_OFFENSIVE
    challenger's book only from decisions frozen per session by
    ``alpha_agent.alpha_recovery.prospective_decision`` under a declared policy,
    and values it only on the marks the policy's execution contract declares. So
    this owner, like ``fx_carry_cadence_runtime``:

      * keeps its own forward copy of the R41 curve store under the campaign
        research root, refreshed from owned Norgate settlements by the canonical
        builder (``alpha_agent.r41.curve_state``) in a CHILD process, never in
        the long-lived worker and never into the frozen store;
      * decides on the newest published session whose open interest and volume
        are FINAL (the session before the newest published one), enters at the
        settlement of the next eligible exchange session, and freezes exactly ONE
        decision between 00:00 and 09:30 ET on that entry session;
      * rebalances when the realised sessions in (registration, newest published]
        number a multiple of 21, holds 21 sessions, and never backfills a boundary
        whose window shut without a decision.

WHAT IS DIFFERENT
    The score is arithmetic over each market's own settlements (no ridge, no
    training block), so scoring runs in-process on the stdlib and numpy; only the
    vendor refresh needs a child. The construction is the ONE R64 risk-controlled
    book, reached through ``futures_trend_challenger.unlevered_target``; the
    volatility-target leverage uses ONLY past unlevered book returns, rebuilt
    from the store at decision time from data at or before the information
    session (never the entry session).

RESEARCH ONLY. No order, no fill, no promotion, no capital, no registration, no
purchase, no operational-store write, no backfill.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from alpha_agent import r63 as _r63

from . import REPO_ROOT, RESEARCH_ROOT_ENV, now_iso, research_root, stable_hash
from . import fx_carry_cadence_runtime as FXR
from . import futures_trend_challenger as FTC
from . import next_open_challenger as NOC
from . import prospective_decision as PD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.futures_trend_runtime"
CHALLENGER_ID = FTC.CHALLENGER_ID

TRADE_EVERY = int(FTC.FROZEN_TRADE_EVERY)
EVALUATION_HORIZON_SESSIONS = int(FTC.FROZEN_HORIZON)
WINDOW_OPENS_ET = FXR.WINDOW_OPENS_ET
WINDOW_CLOSES_ET = FXR.WINDOW_CLOSES_ET
INFORMATION_ASOF_ET = FXR.INFORMATION_ASOF_ET
VENDOR_PUBLICATION_ET = FXR.VENDOR_PUBLICATION_ET
EXECUTION_BOUNDARY = FXR.EXECUTION_BOUNDARY
INFORMATION_SESSION_RULE = ("the newest published futures session whose open interest and "
                            "volume are final: the published session before the newest")
FORWARD_SUBDIR = FXR.FORWARD_SUBDIR
MARKS_RETURN_FIELD = FXR.MARKS_RETURN_FIELD
MARKS_OWNER = FTC.MARK_OWNER
MARKS_REFRESH_RETRY_SECONDS = FXR.MARKS_REFRESH_RETRY_SECONDS
LIVE_INSTANT_TOLERANCE_SECONDS = FXR.LIVE_INSTANT_TOLERANCE_SECONDS
TIMEOUT_REFRESH = 3600
ENTRYPOINT = "scripts/run_futures_trend_forward_cycle.py"
R41_ROOT_ENV = FXR.R41_ROOT_ENV
CURVES_DIR_ENV = FXR.CURVES_DIR_ENV
FROZEN_CURVES_DIR = FXR.FROZEN_CURVES_DIR
HISTORY_PERIODS = 63          # unlevered periods the volatility target looks back over

ST_AWAITING_POLICY = "FUTURES_TREND_AWAITING_POLICY_DECLARATION"
ST_OUTSIDE_WINDOW = "FUTURES_TREND_OUTSIDE_A_DECISION_WINDOW"
ST_ALREADY_FROZEN = "FUTURES_TREND_ALREADY_FROZEN"
ST_AWAITING_PUBLICATION = "FUTURES_TREND_AWAITING_VENDOR_PUBLICATION"
ST_AWAITING_FINAL = "FUTURES_TREND_AWAITING_FINAL_INFORMATION"
ST_NOT_A_REBALANCE = "FUTURES_TREND_HOLDING_NOT_A_REBALANCE_SESSION"
ST_BEFORE_FIRST_LEGAL = "FUTURES_TREND_BEFORE_FIRST_LEGAL_DECISION"
ST_DUE_DRY_RUN = "FUTURES_TREND_DUE_DRY_RUN"
ST_FROZEN = "FUTURES_TREND_FROZEN"
ST_MISSED = "FUTURES_TREND_MISSED"
ST_BLOCKED = "FUTURES_TREND_BLOCKED"
STATES = (ST_AWAITING_POLICY, ST_OUTSIDE_WINDOW, ST_ALREADY_FROZEN, ST_AWAITING_PUBLICATION,
          ST_AWAITING_FINAL, ST_NOT_A_REBALANCE, ST_BEFORE_FIRST_LEGAL, ST_DUE_DRY_RUN,
          ST_FROZEN, ST_MISSED, ST_BLOCKED)
PROGRESS_STATES = (ST_FROZEN,)
DATA_WAIT_STATES = (ST_AWAITING_PUBLICATION, ST_AWAITING_FINAL)
MISSED_STATES = (ST_MISSED,)
FAILURE_STATES = (ST_BLOCKED,)

SAFETY = dict(FTC.SAFETY)


# --------------------------------------------------------------------------- #
# Paths - everything this owner writes lives under ONE directory
# --------------------------------------------------------------------------- #
def forward_dir() -> Path:
    return research_root() / FORWARD_SUBDIR / CHALLENGER_ID


def r41_root() -> Path:
    return forward_dir() / "r41"


def curves_dir() -> Path:
    return r41_root() / "_data_curves"


def staging_root() -> Path:
    return forward_dir() / "r41_staging"


def runs_dir() -> Path:
    return forward_dir() / "runs"


def marks_store_relative() -> str:
    return "%s/%s/r41/_data_curves" % (FORWARD_SUBDIR, CHALLENGER_ID)


def _write_json(path: Path, body: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(path)
    return path


def _read_json(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Marks and calendar - the FX owner's pure readers, on THIS owner's store
# --------------------------------------------------------------------------- #
def daily_rows(market: str, store: Optional[Path] = None) -> dict:
    return FXR.daily_rows(market, Path(store or curves_dir()))


def published_sessions(scope: list, store: Optional[Path] = None,
                       rows_by_market: Optional[dict] = None) -> list:
    rows = rows_by_market if rows_by_market is not None else {m: daily_rows(m, store) for m in scope}
    return FXR.published_sessions(scope, rows_by_market=rows)


def series_from_rows(rows_by_market: dict, through: Optional[str] = None) -> dict:
    """``market -> (dates, ret1)`` of finite settlement returns at or before ``through``."""
    out = {}
    for m, rows in (rows_by_market or {}).items():
        dates, rets = [], []
        for d in sorted(rows):
            if through is not None and d > through:
                continue
            v = FXR._fnum((rows[d] or {}).get(MARKS_RETURN_FIELD))
            if v is not None:
                dates.append(d)
                rets.append(v)
        out[m] = (dates, rets)
    return out


def plan_entry(*, entry_session: str, registration_session: str, scope: list,
               published: list, rows_by_market: dict) -> dict:
    """Is a decision DUE for ``entry_session``, and on which sessions? Pure."""
    s = str(entry_session)[:10]
    t_required = NOC.previous_eligible_session(s)
    out = {"entry_session": s, "newest_session_required": t_required,
           "registration_session": registration_session,
           "newest_published_session": published[-1] if published else None}
    if not published or published[-1] < t_required:
        return {**out, "state": ST_AWAITING_PUBLICATION,
                "detail": "the vendor has not published %s, the session before the entry "
                          "session" % t_required}
    if published[-1] > t_required:
        return {**out, "state": ST_BLOCKED,
                "detail": "the marks hold %s, which is not before the entry session %s"
                          % (published[-1], s)}
    t = t_required
    prior = [d for d in published if d < t]
    u = prior[-1] if prior else None
    idx = FXR.rebalance_index(published, registration_session, t)
    boundaries_after = [d for d in published if d > str(registration_session)]
    prev_boundary = (boundaries_after[idx - TRADE_EVERY]
                     if idx >= TRADE_EVERY and len(boundaries_after) >= idx else None)
    out.update({"information_session": u, "rebalance_index": idx,
                "previous_boundary_session": prev_boundary,
                "sessions_to_next_boundary": (TRADE_EVERY - idx % TRADE_EVERY) % TRADE_EVERY})
    if idx % TRADE_EVERY:
        return {**out, "state": ST_NOT_A_REBALANCE,
                "detail": "position %d on the grid; the frozen book holds between rebalances" % idx}
    if u is None or u < str(registration_session):
        return {**out, "state": ST_BEFORE_FIRST_LEGAL,
                "detail": "the information session %s precedes the registration session %s"
                          % (u, registration_session)}
    fin = FXR.information_is_final(scope, u, rows_by_market)
    out["information_final"] = fin
    if not fin["final"]:
        return {**out, "state": ST_AWAITING_FINAL,
                "detail": "the information session's open interest or volume is not final"}
    return {**out, "state": "DUE",
            "maturity_session_expected": NOC.next_eligible_session(s, EVALUATION_HORIZON_SESSIONS),
            "information_asof_at": NOC._et_instant(u, INFORMATION_ASOF_ET)}


def previous_book(prev_boundary_session: Optional[str]) -> dict:
    if not prev_boundary_session:
        return {"weights": {}, "session": None, "flat_because": "FIRST_FORWARD_BOOK"}
    rec = PD.load_decision(CHALLENGER_ID, prev_boundary_session)
    if not rec:
        return {"weights": {}, "session": prev_boundary_session,
                "flat_because": "NO_DECISION_WAS_FROZEN_AT_THE_PRECEDING_BOUNDARY"}
    return {"weights": dict(rec.get("weights") or {}), "session": prev_boundary_session,
            "flat_because": None}


# --------------------------------------------------------------------------- #
# The score - in-process, from data at or before the information session
# --------------------------------------------------------------------------- #
def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with Path(p).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def class_of_scope(scope: list) -> dict:
    from alpha_agent.r63 import panels as P
    out = {}
    for m in scope:
        meta = P.futures_market_meta(m) or {}
        out[m] = meta.get("asset_class") or "UNCLASSIFIED"
    return out


def unlevered_history(scope: list, series: dict, class_of: dict, information_session: str,
                      published: list, *, periods: int = HISTORY_PERIODS) -> dict:
    """Past UNLEVERED book returns on the same 21-session grid, strictly before ``u``.

    Each past slot is scored from data at or before that slot and realised over
    the next 21 own settlements of every held market - the same arithmetic the
    validation used, replayed on this store, so the leverage estimate is a pure
    function of information that existed before the decision.
    """
    upto = [d for d in published if d <= information_session]
    if not upto or upto[-1] != information_session:
        return {"history": [], "slots": []}
    idx = len(upto) - 1
    slots = []
    k = 1
    while idx - k * TRADE_EVERY >= 0 and len(slots) < periods:
        slots.append(upto[idx - k * TRADE_EVERY])
        k += 1
    slots = sorted(slots)
    history, used = [], []
    for s in slots:
        sc = FTC.score_markets(scope, series, s)
        w_u = FTC.unlevered_target(sc, class_of)
        if not w_u:
            continue
        r = 0.0
        covered = 0.0
        for m, w in w_u.items():
            rr = FTC.realised_return(series[m], sc[m]["own_index"], EVALUATION_HORIZON_SESSIONS)
            if rr is None:
                continue
            r += w * rr
            covered += abs(w)
        if covered <= 0:
            continue
        history.append(r)
        used.append(s)
    return {"history": history, "slots": used}


def score_decision(*, entry_session: str, information_session: str, newest_session: str,
                   prev_weights: dict, scope: list, rows_by_market: dict,
                   store: Optional[Path] = None) -> dict:
    """Score the frozen rule as of the information session and build the book."""
    from alpha_agent.r64 import construction as B
    series = series_from_rows(rows_by_market, through=information_session)
    published = published_sessions(scope, rows_by_market=rows_by_market)
    class_of = class_of_scope(scope)
    scores = FTC.score_markets(scope, series, information_session)
    w_u = FTC.unlevered_target(scores, class_of)
    if not w_u:
        return {"ok": False, "reason": "NO_TARGET_AT_THE_INFORMATION_SESSION",
                "markets_scored": len(scores), "min_markets": FTC.MIN_MARKETS_PER_DECISION}
    pol = dict(B.default_policy())
    hist = unlevered_history(scope, series, class_of, information_session, published)
    gross_u = sum(abs(x) for x in w_u.values())
    lev, state = B.leverage_from_history(hist["history"], gross_u, EVALUATION_HORIZON_SESSIONS, pol)
    w_t = {m: lev * v for m, v in w_u.items()}
    w = B.apply_no_trade_band(w_t, dict(prev_weights or {}), float(pol["no_trade_band"]))
    weights = {m: round(float(v), 10) for m, v in sorted(w.items()) if abs(float(v)) > 1e-12}
    held = sorted(m for m in w if m in (prev_weights or {}) and w[m] == (prev_weights or {}).get(m))
    st = Path(store or curves_dir())
    scope_sha = {}
    for m in scope:
        p = st / ("%s_daily.csv" % m)
        if p.exists():
            scope_sha[m] = _sha256_file(p)
    source_data_hash = stable_hash({"store": marks_store_relative(), "scope_daily_sha256": scope_sha,
                                    "newest_session": newest_session})
    feature_state_hash = stable_hash({
        "information_session": information_session,
        "scores": {m: round(v["score"], 12) for m, v in sorted(scores.items())},
        "vol_annualised": {m: round(v["vol_annualised"], 12) for m, v in sorted(scores.items())},
        "leverage": round(float(lev), 12), "leverage_state": state,
        "history_periods": len(hist["history"]),
        "history_last_slot": hist["slots"][-1] if hist["slots"] else None})
    return {
        "ok": True, "reason": None, "calculation_owner": CALCULATION_OWNER,
        "entry_session": entry_session, "information_session": information_session,
        "newest_published_session": newest_session,
        "weights": weights,
        "predictions": {m: round(v["score"], 8) for m, v in sorted(scores.items())},
        "vol_annualised": {m: round(v["vol_annualised"], 8) for m, v in sorted(scores.items())},
        "unlevered_target": {m: round(v, 10) for m, v in sorted(w_u.items())},
        "leverage": float(lev), "leverage_state": state, "gross_unlevered": float(gross_u),
        "gross_levered": float(sum(abs(v) for v in weights.values())),
        "names_held_by_the_band": held,
        "previous_weights": dict(prev_weights or {}),
        "checks": {"markets_scored": len(scores), "markets_in_scope": len(scope),
                   "history_periods": len(hist["history"]),
                   "history_last_slot": hist["slots"][-1] if hist["slots"] else None,
                   "class_counts": {c: sum(1 for m in w_u if class_of.get(m) == c)
                                    for c in sorted(set(class_of.values()))},
                   "frozen_cell_id": FTC.FROZEN_CELL_ID},
        "source_data_hash": source_data_hash, "feature_state_hash": feature_state_hash,
    }


# --------------------------------------------------------------------------- #
# Child-process work (store refresh). Never run in a long-lived process.
# --------------------------------------------------------------------------- #
def refresh_store_in_child(kind: str, scope: list) -> dict:
    """Rebuild the scope from owned Norgate settlements into THIS owner's forward
    store, through a staging root and one market at a time - the FX producer's
    protocol, on this challenger's directories."""
    staging = Path(os.environ.get(R41_ROOT_ENV) or "")
    if staging.resolve() != staging_root().resolve():
        return {"ok": False, "reason": "CHILD_ENVIRONMENT_NOT_THE_STAGING_ROOT",
                "detail": "%s=%s" % (R41_ROOT_ENV, staging)}
    from alpha_agent.r41 import curve_state as CS
    markets = list(scope)
    if not markets:
        return {"ok": False, "reason": "NO_MARKETS_TO_REFRESH"}
    registry = CS.load_registry()
    dest = curves_dir()
    dest.mkdir(parents=True, exist_ok=True)
    results, failed = {}, []
    for m in markets:
        try:
            r = CS.build_market_store(m, registry=registry, force=True)
            if r.get("state") != "OK":
                failed.append(m)
                results[m] = {"state": r.get("state")}
                continue
            dp = CS.daily_path(m)
            if dp.exists():
                dp.unlink()
            CS.build_daily_series([m])
            for src in (CS.bars_path(m), CS.meta_path(m), CS.daily_path(m)):
                os.replace(src, dest / src.name)
            results[m] = {"state": "OK", "last": r.get("last")}
        except Exception as exc:                          # noqa: BLE001
            failed.append(m)
            results[m] = {"state": "ERROR", "error": "%s: %s" % (type(exc).__name__, str(exc)[:160])}
    body = {"calculation_owner": CALCULATION_OWNER, "kind": kind, "built_at": now_iso(),
            "builder": "alpha_agent.r41.curve_state", "markets": results,
            "n_markets": len(markets), "n_failed": len(failed), "failed": failed}
    _write_json(forward_dir() / ("store_refresh_%s.json" % kind), body)
    return {"ok": not failed, **body}


def _run_child(args: list, *, env_extra: dict, timeout: int) -> dict:
    script = REPO_ROOT / ENTRYPOINT
    env = dict(os.environ)
    env.update({k: str(v) for k, v in env_extra.items()})
    env.setdefault("PYTHONUTF8", "1")
    try:
        proc = subprocess.run([sys.executable, str(script)] + list(args), env=env,
                              capture_output=True, text=True, timeout=int(timeout),
                              cwd=str(REPO_ROOT))
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "CHILD_TIMEOUT", "timeout_seconds": timeout}
    return {"ok": proc.returncode == 0, "returncode": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-1500:], "stderr_tail": (proc.stderr or "")[-1500:]}


def run_refresh(kind: str = "scope") -> dict:
    staging_root().mkdir(parents=True, exist_ok=True)
    res = _run_child(["--child-refresh", kind], env_extra={R41_ROOT_ENV: staging_root()},
                     timeout=TIMEOUT_REFRESH)
    body = _read_json(forward_dir() / ("store_refresh_%s.json" % kind)) or {}
    return {**res, "refresh": {k: body.get(k) for k in ("built_at", "n_markets", "n_failed", "failed")}}


def seed_store_from_frozen(scope: list) -> dict:
    """Copy the frozen store's daily series for the scope into this owner's forward
    store when the forward store is empty. The frozen store is READ, never written;
    the copy is then EXTENDED by the child refresh. Idempotent."""
    import shutil
    dest = curves_dir()
    dest.mkdir(parents=True, exist_ok=True)
    copied, present, missing = [], [], []
    for m in scope:
        dst = dest / ("%s_daily.csv" % m)
        if dst.exists():
            present.append(m)
            continue
        src = Path(FROZEN_CURVES_DIR) / ("%s_daily.csv" % m)
        if not src.exists():
            missing.append(m)
            continue
        shutil.copyfile(src, dst)
        copied.append(m)
    return {"copied": copied, "already_present": present, "missing_in_frozen_store": missing,
            "frozen_store_written": False}


# --------------------------------------------------------------------------- #
# The policy - declared ONCE, before any decision exists
# --------------------------------------------------------------------------- #
def cost_policy(scope: list) -> dict:
    """The kernel takes ONE rate: the strictest R38 per-market cost in the scope."""
    from alpha_agent.r63 import panels as P
    per = {}
    for m in scope:
        meta = P.futures_market_meta(m) or {}
        per[m] = float(meta.get("cost_bps_per_side", _r63.FUT_DEFAULT_COST_BPS))
    return {"bps_per_side": max(per.values()) if per else float(_r63.FUT_DEFAULT_COST_BPS),
            "per_market_bps_per_side": per,
            "rule": "the maximum R38 per-market cost per side across the scope, charged by the "
                    "canonical kernel on GROSS exposure at entry and exit of every held book",
            "record_rule": "R38 per-market cost per side on one-way turnover",
            "conservative_relative_to_the_record": True}


def execution_contract() -> dict:
    return {
        "decision_session_is": "the ENTRY session",
        "execution_boundary": EXECUTION_BOUNDARY,
        "information_session_is": INFORMATION_SESSION_RULE,
        "information_asof_et_on_the_information_session": list(INFORMATION_ASOF_ET),
        "decision_window_opens_et_on_the_entry_session": list(WINDOW_OPENS_ET),
        "decision_window_closes_et_on_the_entry_session": list(WINDOW_CLOSES_ET),
        "entry_mark": "the entry session's daily settlement, valued by the declared marks",
        "horizon_counts_from": "ENTRY_SESSION",
        "holding_sessions": EVALUATION_HORIZON_SESSIONS,
        "rebalance_rule": ("a decision is due when the realised futures sessions in (registration "
                           "session, newest published session] number a multiple of %d" % TRADE_EVERY),
        "previous_book_rule": ("the decision frozen at the preceding boundary; flat when that "
                               "boundary has no decision; the first forward book starts flat"),
        "volatility_history_rule": ("unlevered target-book returns on the same 21-session grid, "
                                    "realised by the information session"),
        "pit_delay_versus_the_historical_record": (
            "one session: the record decides at the newest published session; prospectively the "
            "newest session's open interest and volume are provisional until one vendor update "
            "later, so the decision is formed on the session before the newest published one"),
        "calendar_owner": "the scope's realised settlement calendar (marks) + engine.exchange_calendar "
                          "for the not-yet-realised entry session",
        "accrual_arms_the_owner_frozen_entry_session": True,
        "valuation_marks": {
            "owner": MARKS_OWNER,
            "store_relative_to_research_root": marks_store_relative(),
            "file_pattern": "<market>_daily.csv",
            "return_field": MARKS_RETURN_FIELD,
            "calendar": "sessions with a finite %s in any scope market" % MARKS_RETURN_FIELD,
        },
        "horizon_contract": dict(FTC.HORIZON_CONTRACT),
        "requires_live_market_data": False,
    }


def policy_declaration(identity: dict, *, scope: list) -> dict:
    return {
        "challenger_id": CHALLENGER_ID,
        "information_cutoff_et": WINDOW_OPENS_ET,
        "entry_mark_et": WINDOW_CLOSES_ET,
        "rebalance_cadence_sessions": TRADE_EVERY,
        "evaluation_horizon_sessions": EVALUATION_HORIZON_SESSIONS,
        "cost_policy": cost_policy(scope),
        "instrument_scope": list(scope),
        "identity": dict(identity or {}),
        "emission_rule": (
            "every 21st realised futures session after registration: the frozen 12-1 time-series "
            "trend rule is scored on the newest session whose open interest and volume are final, "
            "the R64 risk-controlled book is rebuilt with the 25%% no-trade band, and ONE decision "
            "is frozen between 00:00 and 09:30 ET on the entry session, entered at that session's "
            "settlement and held %d sessions; a boundary whose window closes without a decision is "
            "MISSED for ever and is never backfilled" % EVALUATION_HORIZON_SESSIONS),
        "execution_contract": execution_contract(),
    }


# --------------------------------------------------------------------------- #
# The advance - one idempotent call the canonical runtime makes
# --------------------------------------------------------------------------- #
def _live_instant(now) -> bool:
    ts = NOC._as_utc(now)
    return ts is not None and abs((datetime.now(timezone.utc) - ts).total_seconds()) \
        <= LIVE_INSTANT_TOLERANCE_SECONDS


def _side_effects_allowed(now, injected: bool) -> dict:
    if injected:
        return {"allowed": True, "why": "INJECTED_WORKERS"}
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return {"allowed": False, "why": "UNDER_PYTEST_WITHOUT_INJECTED_WORKERS"}
    if not _live_instant(now):
        return {"allowed": False, "why": "THE_INSTANT_IS_NOT_THE_MACHINE_CLOCK"}
    return {"allowed": True, "why": "LIVE_CLOCK"}


def _writes_allowed() -> bool:
    return not os.environ.get("PYTEST_CURRENT_TEST") or bool(os.environ.get(RESEARCH_ROOT_ENV))


def marks_refresh_due(now, scope: list, published: list) -> dict:
    ts = NOC._as_utc(now)
    expected = None
    if ts is not None:
        d = ts.date()
        for _ in range(10):
            ds = d.isoformat()
            pub = NOC._as_utc(NOC._et_instant(ds, VENDOR_PUBLICATION_ET))
            if NOC.is_eligible_session(ds) and pub is not None and pub <= ts:
                expected = ds
                break
            d = d - timedelta(days=1)
    newest = published[-1] if published else None
    due = expected is not None and (newest is None or newest < expected)
    marker = _read_json(forward_dir() / "marks_refresh_attempt.json") or {}
    if due and marker.get("newest_after") == newest and marker.get("attempted_at"):
        last = NOC._as_utc(marker["attempted_at"])
        if last is not None and ts is not None and (ts - last).total_seconds() < MARKS_REFRESH_RETRY_SECONDS:
            return {"due": False, "why": "RETRY_BACKOFF", "expected": expected, "newest": newest}
    return {"due": due, "expected": expected, "newest": newest}


def advance(*, now=None, execute: bool = True,
            scorer: Optional[Callable] = None,
            refresher: Optional[Callable] = None,
            store: Optional[Path] = None) -> dict:
    """ONE call; exactly one ``state`` comes back. Safe at any hour, safe repeated."""
    ts = now or now_iso()
    out = {"calculation_owner": CALCULATION_OWNER, "challenger_id": CHALLENGER_ID, "now": ts,
           "paid_dollars": 0.0, "backfill_allowed": False, **{k: v for k, v in SAFETY.items()}}
    pol = PD.load_policy(CHALLENGER_ID)
    if not pol:
        return {**out, "state": ST_AWAITING_POLICY,
                "detail": "no emission policy is declared; run %s --declare-policy" % ENTRYPOINT}
    ident = pol.get("identity") or {}
    registration_session = str(ident.get("registration_session") or "")
    scope = list(pol.get("instrument_scope") or [])
    if not registration_session or not scope:
        return {**out, "state": ST_BLOCKED, "detail": "the declared policy carries no registration "
                                                       "session or scope"}
    hold = ((pol.get("execution_contract") or {}).get("holding_sessions"))
    if hold is not None and int(hold) != int(pol.get("evaluation_horizon_sessions") or 0):
        return {**out, "state": ST_BLOCKED,
                "detail": "the declared holding period (%s) disagrees with the evaluation horizon "
                          "(%s); no decision is frozen" % (hold, pol.get("evaluation_horizon_sessions"))}
    store = Path(store) if store else curves_dir()
    injected = scorer is not None or refresher is not None
    fx = _side_effects_allowed(ts, injected)
    out["side_effects"] = fx

    rows = {m: daily_rows(m, store) for m in scope}
    published = published_sessions(scope, rows_by_market=rows)
    due = marks_refresh_due(ts, scope, published)
    out["marks_refresh"] = due
    if execute and due.get("due") and fx["allowed"]:
        r = (refresher or run_refresh)("scope")
        rows = {m: daily_rows(m, store) for m in scope}
        published = published_sessions(scope, rows_by_market=rows)
        _write_json(forward_dir() / "marks_refresh_attempt.json",
                    {"attempted_at": now_iso() if not injected else ts,
                     "newest_after": published[-1] if published else None,
                     "ok": bool(r.get("ok"))})
        out["marks_refresh"] = {**due, "ran": True, "ok": bool(r.get("ok")),
                                "newest_after": published[-1] if published else None}
    out["newest_published_session"] = published[-1] if published else None

    win = FXR.locate_window(ts)
    out["window"] = win
    if not win["in_window"]:
        last = win.get("last_session")
        if last and not PD.load_decision(CHALLENGER_ID, last):
            upto = [d for d in published if d <= NOC.previous_eligible_session(last)]
            p_last = plan_entry(entry_session=last, registration_session=registration_session,
                                scope=scope, published=upto, rows_by_market=rows)
            if p_last["state"] in ("DUE", ST_AWAITING_FINAL) and \
                    (NOC._as_utc(ts) - NOC._as_utc(FXR.window_for(last)["closes_at"])) < timedelta(hours=18) \
                    and (p_last.get("information_session") or "") >= registration_session:
                return {**out, "state": ST_MISSED, "plan": p_last, "backfill_refused": True,
                        "detail": "the %s window closed at %s with no decision; this boundary is "
                                  "missed permanently" % (last, FXR.window_for(last)["closes_at"])}
        return {**out, "state": ST_OUTSIDE_WINDOW, "next_window": win.get("next_window")}

    s = win["session"]
    held = PD.load_decision(CHALLENGER_ID, s)
    if held:
        return {**out, "state": ST_ALREADY_FROZEN, "entry_session": s,
                "decision_record_hash": held.get("record_hash")}
    plan = plan_entry(entry_session=s, registration_session=registration_session, scope=scope,
                      published=published, rows_by_market=rows)
    out["plan"] = plan
    if plan["state"] != "DUE":
        return {**out, "state": plan["state"], "entry_session": s, "detail": plan.get("detail")}
    if not execute or not fx["allowed"]:
        return {**out, "state": ST_DUE_DRY_RUN, "entry_session": s,
                "detail": "a decision is due; nothing was scored or written (%s)"
                          % ("dry run" if not execute else fx["why"])}
    if not _writes_allowed():
        return {**out, "state": ST_BLOCKED, "detail": "refused: pytest without a redirected research root"}

    prev = previous_book(plan.get("previous_boundary_session"))
    if scorer is not None:
        scored = scorer(plan, prev["weights"])
    else:
        scored = score_decision(entry_session=s, information_session=plan["information_session"],
                                newest_session=plan["newest_session_required"],
                                prev_weights=prev["weights"], scope=scope, rows_by_market=rows,
                                store=store)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        _write_json(runs_dir() / ("%s_score_%s.json" % (stamp, s)), scored)
    out["score"] = {k: scored.get(k) for k in ("ok", "reason", "leverage", "leverage_state",
                                                 "gross_unlevered", "gross_levered", "checks")}
    if not scored.get("ok"):
        return {**out, "state": ST_BLOCKED, "entry_session": s,
                "detail": "scoring refused: %s" % scored.get("reason")}
    if scored.get("information_session") != plan["information_session"] or \
            scored.get("entry_session") != s:
        return {**out, "state": ST_BLOCKED, "entry_session": s,
                "detail": "the scored sessions do not match the plan"}
    ctx = {
        "information_session": plan["information_session"],
        "information_asof_at": plan["information_asof_at"],
        "newest_published_session": plan["newest_session_required"],
        "entry_session": s,
        "entry_boundary": EXECUTION_BOUNDARY,
        "window_opens_at": FXR.window_for(s)["opens_at"],
        "window_closes_at": FXR.window_for(s)["closes_at"],
        "holding_sessions": EVALUATION_HORIZON_SESSIONS,
        "maturity_session_expected": plan.get("maturity_session_expected"),
        "rebalance_index": plan["rebalance_index"],
        "previous_book": {"session": prev["session"], "flat_because": prev["flat_because"],
                          "weights": prev["weights"]},
        "leverage": scored.get("leverage"), "leverage_state": scored.get("leverage_state"),
        "gross_unlevered": scored.get("gross_unlevered"),
        "names_held_by_the_band": scored.get("names_held_by_the_band"),
        "scoring_checks": scored.get("checks"),
        "frozen_cell_id": FTC.FROZEN_CELL_ID,
        "calendar_owner": "realised settlement marks + engine.exchange_calendar",
    }
    freeze_now = ts if injected else now_iso()
    res = PD.freeze_decision(challenger_id=CHALLENGER_ID, eligible_session=s,
                             weights=scored["weights"],
                             source_data_hash=scored["source_data_hash"],
                             feature_state_hash=scored["feature_state_hash"],
                             feature_observed_at=plan["information_asof_at"],
                             now=freeze_now, context=ctx)
    out["freeze"] = {k: res.get(k) for k in ("outcome", "frozen", "detail", "path")}
    if res.get("outcome") == PD.FROZEN:
        return {**out, "state": ST_FROZEN, "entry_session": s,
                "weights": (res.get("decision") or {}).get("weights"),
                "maturity_session_expected": plan.get("maturity_session_expected")}
    if res.get("outcome") == PD.ALREADY_FROZEN:
        return {**out, "state": ST_ALREADY_FROZEN, "entry_session": s}
    return {**out, "state": ST_BLOCKED, "entry_session": s,
            "detail": "the decision owner refused: %s" % res.get("outcome")}


def schedule_preview(*, registration_session: str, published: list, horizon_days: int = 120) -> list:
    """The next rebalance entry sessions on the exchange calendar (a projection)."""
    last = published[-1] if published else registration_session
    idx = FXR.rebalance_index(published, registration_session, last)
    out, d, pos = [], last, idx
    for _ in range(horizon_days):
        nxt = NOC.next_eligible_session(d)
        if not nxt:
            break
        if pos % TRADE_EVERY == 0 and d >= registration_session:
            info = NOC.previous_eligible_session(d)
            out.append({"entry_session": nxt, "newest_published_session": d,
                        "information_session_expected": info, "rebalance_index": pos,
                        "legal": bool(info and info >= registration_session)})
        if nxt > registration_session:
            pos += 1
        d = nxt
    return out


__all__ = [
    "CALCULATION_OWNER", "CHALLENGER_ID", "TRADE_EVERY", "EVALUATION_HORIZON_SESSIONS",
    "WINDOW_OPENS_ET", "WINDOW_CLOSES_ET", "STATES", "PROGRESS_STATES", "DATA_WAIT_STATES",
    "MISSED_STATES", "FAILURE_STATES", "SAFETY", "ENTRYPOINT", "R41_ROOT_ENV", "CURVES_DIR_ENV",
    "forward_dir", "curves_dir", "staging_root", "runs_dir", "marks_store_relative",
    "daily_rows", "published_sessions", "series_from_rows", "plan_entry", "previous_book",
    "class_of_scope", "unlevered_history", "score_decision", "refresh_store_in_child",
    "run_refresh", "seed_store_from_frozen", "cost_policy", "execution_contract",
    "policy_declaration", "marks_refresh_due", "advance", "schedule_preview",
]
