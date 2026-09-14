r"""alpha_agent.alpha_recovery.fx_carry_cadence_runtime - THE per-session
prospective decision owner for ``ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7``.

WHY THIS EXISTS
    The challenger was registered for TRUE_FORWARD evidence on 2026-09-14 and
    could accrue nothing: ``api.canonical_forward_accrual`` reads an Alpha
    Recovery book ONLY from decisions frozen per session by
    :mod:`prospective_decision` under a declared policy, and no producer froze
    any. This module is that producer and nothing else. It computes no new
    signal: the scores come from the ONE scorer
    (``alpha_agent.r63.sensitivity.run_cell``) on the ONE dataset owner
    (``alpha_agent.r64.experiments.assemble``), and the book from the ONE
    construction's primitives (``alpha_agent.r64.construction``) in exactly the
    order ``alpha_agent.alpha_recovery.cadence.build_book_cadence`` applies them.

THE FOUR SESSIONS, NEVER COLLAPSED
    newest published  t   the newest FX session the vendor has published
    information       u   the published session before t: the newest session
                          whose open interest and volume are FINAL
    entry             s   the eligible session after t; its emission window is
                          00:00-09:30 ET on s and the position is entered at
                          s's settlement
    maturity              five realised FX sessions after s (the frozen
                          trade-every-5 holding period)

WHY THE INFORMATION SESSION IS NOT t (MEASURED 2026-09-14, NOT ASSUMED)
    Norgate publishes a futures session's open interest one update LATE and its
    volume provisionally: on 2026-09-14 every one of the nine scope markets
    carried Open Interest 0.0 on its newest row (2026-09-11), DX carried volume
    0, and a rebuild of the R41 store showed that its ONLY revised row was its
    last. The frozen baseline blocks VOLUME_PARTICIPATION and LIQUIDITY read
    open interest and volume AT the decision session, so a decision formed on t
    would read incomplete inputs (the scorer drops every such row). The frozen
    record's rule "decision at close t from data <= t, effective close t+1" was
    therefore measured on REVISED data; it is not reproducible prospectively.
    The nearest PIT-legal execution of the SAME construction forms the decision
    on u, the newest session whose inputs are final, and enters at the
    settlement of the session after t. That is one session later than the
    historical rule - a handicap, disclosed, never a look-ahead - and it is the
    only change: universe, scorer, folds, construction, cadence, band and costs
    are the frozen ones.

THE CADENCE
    The canonical accrual grid is every fifth realised FX session strictly after
    the registration session. A decision is due on entry session s exactly when
    the number of realised FX sessions in (registration session, t] is a
    multiple of 5, so both owners name the same boundaries from the same marks.
    Between boundaries the frozen book HOLDS, so nothing is frozen.

THE BOOK STATE (three PIT consequences, each declared)
    1. The previous book is the decision frozen at the immediately preceding
       boundary; a boundary with no frozen decision left the book flat.
    2. The volatility target reads the unlevered target-book returns realised
       by the information session (labels maturing after u are not visible).
    3. The first forward book starts flat, so its first rebalance trades the
       whole target.

WHAT IT MAY NOT DO
    No order, no fill, no proposal, no promotion, no capital, no registration,
    no purchase, no operational-store write, and no backfill: a boundary whose
    window shut without a decision is MISSED permanently and nothing is written
    for it. The frozen R41 store is READ, never written: the forward store lives
    under this challenger's own research directory and is rebuilt from owned
    Norgate settlements by the canonical builder (``alpha_agent.r41.curve_state``).
"""
from __future__ import annotations

import csv
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
from . import fx_carry_cadence_challenger as FXC
from . import next_open_challenger as NOC
from . import prospective_decision as PD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.fx_carry_cadence_runtime"
CHALLENGER_ID = FXC.CHALLENGER_ID

# --------------------------------------------------------------------------- #
# 1. THE FROZEN IDENTITY THIS OWNER EXECUTES (read from the record owner)
# --------------------------------------------------------------------------- #
SCOPE = "FX_FUTURES"
MODE = "XS"
DIMENSION = "CARRY"
LABEL_HORIZON = int(FXC.FROZEN_HORIZON)
TRADE_EVERY = int(FXC.FROZEN_TRADE_EVERY)
NO_TRADE_BAND = float(FXC.FROZEN_NO_TRADE_BAND)
#: The weights frozen at a rebalance are held unchanged for exactly the frozen
#: trade-every sessions; the scorer's 1-session label is the INFORMATION
#: horizon, not the holding period.
EVALUATION_HORIZON_SESSIONS = TRADE_EVERY

# --------------------------------------------------------------------------- #
# 2. THE PROSPECTIVE CONTRACT
# --------------------------------------------------------------------------- #
WINDOW_OPENS_ET = (0, 0)
#: Shuts before the earliest settlement any scope market could print on the
#: entry session - the same instant the estate's other per-session boundary uses.
WINDOW_CLOSES_ET = (9, 30)
#: The as-of instant of the information set: the information session is complete.
INFORMATION_ASOF_ET = (17, 0)
#: The vendor may carry a session's settlement no earlier than this, so a store
#: that has not reached the newest session settled before it is refreshed.
VENDOR_PUBLICATION_ET = (20, 0)
EXECUTION_BOUNDARY = "SETTLEMENT_OF_THE_ELIGIBLE_SESSION_AFTER_THE_NEWEST_PUBLISHED_SESSION"
INFORMATION_SESSION_RULE = ("the newest published FX session whose open interest and "
                            "volume are final: the published session before the newest")

FORWARD_SUBDIR = "forward_evidence"
MARKS_RETURN_FIELD = "ret1"
MARKS_OWNER = ("alpha_agent.r41.curve_state (owned Norgate dated-contract settlements, "
               "R38 observable roll rule; the registration's price_mark_owner layer)")
MARKS_REFRESH_RETRY_SECONDS = 1800
LIVE_INSTANT_TOLERANCE_SECONDS = 900
TIMEOUT_REFRESH_SCOPE = 900
TIMEOUT_REFRESH_ALL = 3600
TIMEOUT_SCORE = 3600

ENTRYPOINT = "scripts/run_fx_carry_cadence_forward_cycle.py"
#: The two environment variables a CHILD process receives. Must equal the
#: owners' own names (asserted by a test): ``alpha_agent.r41.RESEARCH_ROOT_ENV``
#: and ``alpha_agent.r63.panels.CURVES_DIR_ENV``.
R41_ROOT_ENV = "PAPER_TRADER_R41_RESEARCH_ROOT"
CURVES_DIR_ENV = "PAPER_TRADER_R63_FUTURES_CURVES_DIR"
FROZEN_CURVES_DIR = _r63.R41_ROOT / "_data_curves"

# --------------------------------------------------------------------------- #
# 3. STATES - exactly one is returned per advance
# --------------------------------------------------------------------------- #
ST_AWAITING_POLICY = "FX_CADENCE_AWAITING_POLICY_DECLARATION"
ST_OUTSIDE_WINDOW = "FX_CADENCE_OUTSIDE_A_DECISION_WINDOW"
ST_ALREADY_FROZEN = "FX_CADENCE_ALREADY_FROZEN"
ST_AWAITING_PUBLICATION = "FX_CADENCE_AWAITING_VENDOR_PUBLICATION"
ST_AWAITING_FINAL = "FX_CADENCE_AWAITING_FINAL_INFORMATION"
ST_NOT_A_REBALANCE = "FX_CADENCE_HOLDING_NOT_A_REBALANCE_SESSION"
ST_BEFORE_FIRST_LEGAL = "FX_CADENCE_BEFORE_FIRST_LEGAL_DECISION"
ST_DUE_DRY_RUN = "FX_CADENCE_DUE_DRY_RUN"
ST_FROZEN = "FX_CADENCE_FROZEN"
ST_MISSED = "FX_CADENCE_MISSED"
ST_BLOCKED = "FX_CADENCE_BLOCKED"
STATES = (ST_AWAITING_POLICY, ST_OUTSIDE_WINDOW, ST_ALREADY_FROZEN, ST_AWAITING_PUBLICATION,
          ST_AWAITING_FINAL, ST_NOT_A_REBALANCE, ST_BEFORE_FIRST_LEGAL, ST_DUE_DRY_RUN,
          ST_FROZEN, ST_MISSED, ST_BLOCKED)
PROGRESS_STATES = (ST_FROZEN,)
DATA_WAIT_STATES = (ST_AWAITING_PUBLICATION, ST_AWAITING_FINAL)
MISSED_STATES = (ST_MISSED,)
FAILURE_STATES = (ST_BLOCKED,)

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "promotes_model": False,
    "capital_eligible": False,
    "allocates_capital": False,
    "mutates_portfolio": False,
    "creates_orders": False,
    "creates_fills": False,
    "registers_forward_challenger": False,
    "writes_the_frozen_r41_store": False,
    "backfill_allowed": False,
    "automatic_promotion": False,
    "manual_review_required": True,
}


# --------------------------------------------------------------------------- #
# 4. PATHS - everything this owner writes lives under ONE directory
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
    """The marks store, relative to the campaign research root (declared in the policy)."""
    return "%s/%s/r41/_data_curves" % (FORWARD_SUBDIR, CHALLENGER_ID)


def frozen_store_markets(frozen_dir: Optional[Path] = None) -> list:
    """The store composition the frozen record was scored on: every daily series."""
    d = Path(frozen_dir or FROZEN_CURVES_DIR)
    return sorted(p.name[:-len("_daily.csv")] for p in d.glob("*_daily.csv")) if d.exists() else []


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
# 5. THE MARKS AND THE REALISED FX CALENDAR (the SAME rule the accrual reads)
# --------------------------------------------------------------------------- #
def _fnum(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def daily_rows(market: str, store: Optional[Path] = None) -> dict:
    """``{session: row}`` for one market's daily series, or ``{}``."""
    p = Path(store or curves_dir()) / ("%s_daily.csv" % market)
    if not p.exists():
        return {}
    out = {}
    with p.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            d = str(r.get("date") or "")[:10]
            if d:
                out[d] = r
    return out


def published_sessions(scope: list, store: Optional[Path] = None,
                       rows_by_market: Optional[dict] = None) -> list:
    """Every session on which ANY scope market printed a finite settlement return."""
    rows_by_market = rows_by_market if rows_by_market is not None else {
        m: daily_rows(m, store) for m in scope}
    out = set()
    for m in scope:
        for d, r in (rows_by_market.get(m) or {}).items():
            if _fnum(r.get(MARKS_RETURN_FIELD)) is not None:
                out.add(d)
    return sorted(out)


def information_is_final(scope: list, session: str, rows_by_market: dict) -> dict:
    """Are the information session's inputs complete for EVERY scope market?"""
    incomplete = []
    for m in scope:
        r = (rows_by_market.get(m) or {}).get(session)
        if r is None:
            incomplete.append({"market": m, "reason": "NO_ROW"})
            continue
        missing = [k for k in ("c1", MARKS_RETURN_FIELD) if _fnum(r.get(k)) is None]
        for k in ("oi1", "v1"):
            v = _fnum(r.get(k))
            if v is None or v <= 0:
                missing.append(k)
        if missing:
            incomplete.append({"market": m, "reason": "NOT_FINAL", "fields": missing})
    return {"final": not incomplete, "incomplete": incomplete, "session": session}


# --------------------------------------------------------------------------- #
# 6. THE WINDOW AND THE BOUNDARY - pure functions of the clock and the marks
# --------------------------------------------------------------------------- #
def window_for(session: str) -> dict:
    return {"session": session,
            "opens_at": NOC._et_instant(session, WINDOW_OPENS_ET),
            "closes_at": NOC._et_instant(session, WINDOW_CLOSES_ET)}


def locate_window(now) -> dict:
    """The entry session whose window contains ``now``, else the last and next ones."""
    ts = NOC._as_utc(now)
    if ts is None:
        return {"in_window": False, "session": None, "last_session": None,
                "next_session": None}
    base = ts.date()
    candidates = [(base + timedelta(days=k)).isoformat() for k in range(-6, 8)]
    eligible = [d for d in candidates if NOC.is_eligible_session(d)]
    last = nxt = None
    for d in eligible:
        w = window_for(d)
        opens, closes = NOC._as_utc(w["opens_at"]), NOC._as_utc(w["closes_at"])
        if opens <= ts < closes:
            return {"in_window": True, "session": d, **w}
        if closes <= ts:
            last = d
        elif opens > ts and nxt is None:
            nxt = d
    return {"in_window": False, "session": None, "last_session": last, "next_session": nxt,
            "next_window": window_for(nxt) if nxt else None}


def rebalance_index(published: list, registration_session: str, newest_session: str) -> int:
    """Realised FX sessions in (registration, newest]: the entry's position on the grid."""
    return len([d for d in published if str(registration_session) < d <= str(newest_session)])


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
    idx = rebalance_index(published, registration_session, t)
    boundaries_after = [d for d in published if d > str(registration_session)]
    prev_boundary = (boundaries_after[idx - TRADE_EVERY]
                     if idx >= TRADE_EVERY and len(boundaries_after) >= idx else None)
    out.update({"information_session": u, "rebalance_index": idx,
                "previous_boundary_session": prev_boundary,
                "sessions_to_next_boundary": (TRADE_EVERY - idx % TRADE_EVERY) % TRADE_EVERY})
    if idx % TRADE_EVERY:
        return {**out, "state": ST_NOT_A_REBALANCE,
                "detail": "position %d on the grid; the frozen book holds between "
                          "rebalances" % idx}
    if u is None or u < str(registration_session):
        return {**out, "state": ST_BEFORE_FIRST_LEGAL,
                "detail": "the information session %s precedes the registration session "
                          "%s" % (u, registration_session)}
    fin = information_is_final(scope, u, rows_by_market)
    out["information_final"] = fin
    if not fin["final"]:
        return {**out, "state": ST_AWAITING_FINAL,
                "detail": "the information session's open interest or volume is not final"}
    return {**out, "state": "DUE",
            "maturity_session_expected": NOC.next_eligible_session(s, EVALUATION_HORIZON_SESSIONS),
            "information_asof_at": NOC._et_instant(u, INFORMATION_ASOF_ET)}


def previous_book(prev_boundary_session: Optional[str]) -> dict:
    """The weights held into this boundary: the preceding boundary's decision, or flat."""
    if not prev_boundary_session:
        return {"weights": {}, "session": None, "flat_because": "FIRST_FORWARD_BOOK"}
    rec = PD.load_decision(CHALLENGER_ID, prev_boundary_session)
    if not rec:
        return {"weights": {}, "session": prev_boundary_session,
                "flat_because": "NO_DECISION_WAS_FROZEN_AT_THE_PRECEDING_BOUNDARY"}
    return {"weights": dict(rec.get("weights") or {}), "session": prev_boundary_session,
            "flat_because": None}


# --------------------------------------------------------------------------- #
# 7. THE BOOK - the ONE construction's primitives in build_book_cadence's order
# --------------------------------------------------------------------------- #
def unlevered_target(pp, vv, inst, *, policy: dict) -> Optional[dict]:
    """The frozen per-period unlevered target (FX scope carries no class budget)."""
    from alpha_agent.r64 import construction as B
    w_u = B.xs_unlevered(pp, vv, inst, max_name_weight=policy["max_name_weight"])
    if not w_u:
        return None
    vol_by = {int(j): float(x) for j, x in zip(inst, vv)}
    return B.apply_instrument_cap(w_u, vol_by, float(policy["max_instrument_risk_share"]))


def construction_policy() -> dict:
    from alpha_agent.r64 import construction as B
    pol = dict(B.default_policy())
    pol["no_trade_band"] = NO_TRADE_BAND
    return pol


def forward_weights(*, w_u: dict, history: list, prev: dict, policy: dict) -> dict:
    """A REBALANCE of the frozen cadence book: lever the target, then band it."""
    from alpha_agent.r64 import construction as B
    gross_u = sum(abs(x) for x in w_u.values())
    lev, state = B.leverage_from_history(history, gross_u, LABEL_HORIZON, policy)
    w_t = {x: lev * val for x, val in w_u.items()}
    w = B.apply_no_trade_band(w_t, prev, float(policy["no_trade_band"]))
    held = sorted(k for k in w if k in prev and w[k] == prev[k])
    return {"weights": w, "leverage": float(lev), "leverage_state": state,
            "gross_unlevered": float(gross_u), "names_held_by_the_band": held}


# --------------------------------------------------------------------------- #
# 8. CHILD-PROCESS WORK (store refresh, scoring). Never run in a long-lived one.
# --------------------------------------------------------------------------- #
def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with Path(p).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def refresh_store_in_child(kind: str, scope: list) -> dict:
    """Rebuild markets from owned Norgate settlements into the forward store.

    Built into a STAGING root and moved into place one market at a time, only
    when the canonical builder reports it OK - so a vendor hiccup can never
    replace a good market with an empty one.
    """
    staging = Path(os.environ.get(R41_ROOT_ENV) or "")
    if staging.resolve() != staging_root().resolve():
        return {"ok": False, "reason": "CHILD_ENVIRONMENT_NOT_THE_STAGING_ROOT",
                "detail": "%s=%s" % (R41_ROOT_ENV, staging)}
    from alpha_agent.r41 import curve_state as CS
    markets = list(scope) if kind == "scope" else frozen_store_markets()
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


def _store_identity(scope: list, frozen_last: Optional[str]) -> dict:
    """The forward store must BE the frozen store, extended - checked, not assumed."""
    import pandas as pd
    fwd, frz = curves_dir(), FROZEN_CURVES_DIR
    fwd_m, frz_m = frozen_store_markets(fwd), frozen_store_markets(frz)
    out = {"markets_forward": len(fwd_m), "markets_frozen": len(frz_m),
           "markets_identical": fwd_m == frz_m}
    if not out["markets_identical"]:
        out["missing"] = sorted(set(frz_m) - set(fwd_m))
        out["extra"] = sorted(set(fwd_m) - set(frz_m))
        return {**out, "ok": False}
    union_f, union_z = set(), set()
    for m in frz_m:
        union_f |= set(pd.read_csv(fwd / ("%s_daily.csv" % m), usecols=["date"])["date"].astype(str))
        union_z |= set(pd.read_csv(frz / ("%s_daily.csv" % m), usecols=["date"])["date"].astype(str))
    last = frozen_last or max(union_z)
    out["frozen_union_last"] = max(union_z)
    out["union_dates_identical_through_frozen_end"] = (
        sorted(d for d in union_f if d <= last) == sorted(d for d in union_z if d <= last))
    diffs = {}
    cols = ["date", "ret1", "c1", "c2", "c3", "slope_ann", "slope23_ann"]
    for m in scope:
        a = pd.read_csv(frz / ("%s_daily.csv" % m), usecols=cols)
        b = pd.read_csv(fwd / ("%s_daily.csv" % m), usecols=cols)
        j = a.merge(b, on="date", suffixes=("_z", "_f"))
        j = j[j["date"] < last]
        worst = 0.0
        for c in cols[1:]:
            za, fb = j[c + "_z"].to_numpy(float), j[c + "_f"].to_numpy(float)
            if ((za != za) != (fb != fb)).any():
                worst = float("inf")
                break
            ok = za == za
            if ok.any():
                worst = max(worst, float(abs(za[ok] - fb[ok]).max()))
        diffs[m] = worst
    out["scope_price_history_max_abs_diff_before_frozen_end"] = diffs
    out["scope_price_history_identical"] = all(v == 0.0 for v in diffs.values())
    out["ok"] = bool(out["union_dates_identical_through_frozen_end"]
                     and out["scope_price_history_identical"])
    return out


def score_in_child(*, entry_session: str, information_session: str,
                   newest_session: str, prev_weights: dict, scope: list) -> dict:
    """Score the frozen cell through the information session and build the book.

    The latest decision slot carries no realised label, so the scorer is run
    TWICE: once on the frozen decision slots, once with the unlabelled slots up
    to the information session appended under a placeholder label. The
    placeholder cannot move a fitted parameter (those slots are test rows of the
    LOCKBOX fold, whose training block ends before 2023), and that is PROVED on
    every run by requiring every labelled prediction to be identical.
    """
    import numpy as np
    from alpha_agent.r63 import ontology as ONT
    from alpha_agent.r63 import panels as P
    from alpha_agent.r63 import sensitivity as S
    from alpha_agent.r64 import experiments as R64X

    if Path(P.R41_CURVES).resolve() != curves_dir().resolve():
        return {"ok": False, "reason": "CHILD_ENVIRONMENT_NOT_THE_FORWARD_STORE",
                "detail": "r63.panels reads %s" % P.R41_CURVES}
    frozen_cell = _read_json(research_root() / "cells" / FXC.CADENCE_CELL_FILE) or {}
    ident = _store_identity(scope, None)
    if not ident.get("ok"):
        return {"ok": False, "reason": "FORWARD_STORE_IS_NOT_THE_FROZEN_STORE_EXTENDED",
                "store_identity": ident}

    ds = R64X.assemble(SCOPE, MODE, LABEL_HORIZON)
    dates = [str(d) for d in ds["dates"]]
    if list(ds["inst"]) != list(scope):
        return {"ok": False, "reason": "SCOPE_DIFFERS_FROM_THE_REGISTERED_UNIVERSE",
                "dataset_instruments": list(ds["inst"]), "registered": list(scope)}
    if information_session not in dates:
        return {"ok": False, "reason": "INFORMATION_SESSION_NOT_ON_THE_GRID"}
    ix_u = dates.index(information_session)
    base = tuple(d for d in ONT.baseline_for(SCOPE) if d in ds["blocks"])

    plain = S.run_cell(ds, base, DIMENSION, keep_predictions=True)
    p1 = plain.get("_predictions")
    if p1 is None:
        return {"ok": False, "reason": "FROZEN_SCORER_PRODUCED_NO_PREDICTIONS",
                "verdict": plain.get("verdict"), "why": plain.get("why")}
    dec = np.asarray(ds["dec"], dtype=int)
    add = np.arange(int(dec[-1]) + 1, ix_u + 1, dtype=int) if ix_u > int(dec[-1]) else \
        np.array([], dtype=int)
    ext = dict(ds)
    ext["dec"] = np.concatenate([dec, add]) if len(add) else dec
    y = np.array(ds["y"], dtype=float, copy=True)
    ys = np.array(ds["y_scaled"], dtype=float, copy=True)
    for j in add:
        y[:, j] = np.where(np.isfinite(y[:, j]), y[:, j], 0.0)
        ys[:, j] = np.where(np.isfinite(ys[:, j]), ys[:, j], 0.0)
    ext["y"], ext["y_scaled"] = y, ys
    extended = S.run_cell(ext, base, DIMENSION, keep_predictions=True)
    p2 = extended.get("_predictions")
    if p2 is None:
        return {"ok": False, "reason": "EXTENDED_SCORER_PRODUCED_NO_PREDICTIONS"}

    k1 = {(int(g), int(i)): v for g, i, v in zip(p1["gid"], p1["iid"], p1["pred_BD"])}
    k2 = {(int(g), int(i)): v for g, i, v in zip(p2["gid"], p2["iid"], p2["pred_BD"])}
    worst = 0.0
    for key, v in k1.items():
        w = k2.get(key)
        if w is None or (np.isfinite(v) != np.isfinite(w)):
            worst = float("inf")
            break
        if np.isfinite(v):
            worst = max(worst, abs(float(v) - float(w)))
    identical = worst <= 1e-12

    ext_dec = [int(x) for x in ext["dec"]]
    if ix_u not in ext_dec:
        return {"ok": False, "reason": "INFORMATION_SESSION_IS_NOT_A_DECISION_SLOT"}
    g_u = ext_dec.index(ix_u)
    okp2 = np.isfinite(p2["pred_B"]) & np.isfinite(p2["pred_BD"])
    at_u = (p2["gid"] == g_u) & okp2
    kinds = sorted(set(str(k) for k in np.asarray(p2["fold_kind"])[at_u]))
    pol = construction_policy()
    pp, vv, inst = p2["pred_BD"][at_u], p2["vol"][at_u], p2["iid"][at_u]
    fin = np.isfinite(pp)
    w_u = unlevered_target(pp[fin], vv[fin], inst[fin], policy=pol)
    if not w_u:
        return {"ok": False, "reason": "NO_TARGET_AT_THE_INFORMATION_SESSION",
                "rows_at_information_session": int(at_u.sum())}

    # The unlevered target-book history, exactly as build_book_cadence appends it,
    # restricted to periods whose labels are realised by the information session.
    okp1 = np.isfinite(p1["pred_B"]) & np.isfinite(p1["pred_BD"])
    g_all, i_all = p1["gid"][okp1], p1["iid"][okp1]
    pr, yr, vl = p1["pred_BD"][okp1], p1["y_raw"][okp1], p1["vol"][okp1]
    order = np.lexsort((i_all, g_all))
    g_all, i_all, pr, yr, vl = g_all[order], i_all[order], pr[order], yr[order], vl[order]
    history, last_hist = [], None
    for g in np.unique(g_all):
        if int(dec[int(g)]) > ix_u - (LABEL_HORIZON + 1):
            continue
        m = g_all == g
        ok = np.isfinite(pr[m]) & np.isfinite(yr[m])
        if not ok.any():
            continue
        wg = unlevered_target(pr[m][ok], vl[m][ok], i_all[m][ok], policy=pol)
        if not wg:
            continue
        ret_by = {int(j): float(x) for j, x in zip(i_all[m][ok], yr[m][ok])}
        history.append(sum(wg[x] * ret_by.get(x, 0.0) for x in wg))
        last_hist = dates[int(dec[int(g)])]

    inst_names = list(ds["inst"])
    prev_by_iid = {inst_names.index(k): float(v) for k, v in (prev_weights or {}).items()
                   if k in inst_names}
    book = forward_weights(w_u=w_u, history=history, prev=prev_by_iid, policy=pol)
    weights = {inst_names[int(k)]: round(float(v), 10) for k, v in book["weights"].items()
               if abs(float(v)) > 1e-12}
    preds = {inst_names[int(i)]: float(p) for i, p in zip(inst[fin], pp[fin])}
    vols = {inst_names[int(i)]: float(v) for i, v in zip(inst[fin], vv[fin])}
    scope_sha = {m: _sha256_file(curves_dir() / ("%s_daily.csv" % m)) for m in scope}
    checks = {
        "store_identity": ident,
        "labelled_predictions_identical": bool(identical),
        "labelled_prediction_max_abs_diff": worst,
        "information_session_fold_kinds": kinds,
        "information_session_scored_by_the_lockbox_model": kinds == ["LOCKBOX"],
        "n_instruments_scored": len(preds),
        "appended_unlabelled_slots": [dates[int(j)] for j in add],
        "frozen_cell_conditional_t": (frozen_cell.get("conditional") or {}).get("t"),
        "history_periods": len(history),
        "history_last_label_session": last_hist,
    }
    ok = bool(identical and kinds == ["LOCKBOX"] and len(preds) == len(scope))
    source_data_hash = stable_hash({"store": marks_store_relative(), "markets": ident["markets_forward"],
                                    "scope_daily_sha256": scope_sha, "newest_session": newest_session,
                                    "frozen_store_identical": True})
    feature_state_hash = stable_hash({"information_session": information_session,
                                      "pred_BD": {k: round(v, 12) for k, v in preds.items()},
                                      "vol_63": {k: round(v, 12) for k, v in vols.items()},
                                      "leverage": round(book["leverage"], 12),
                                      "leverage_state": book["leverage_state"],
                                      "history_periods": len(history),
                                      "history_last_label_session": last_hist})
    return {"ok": ok, "reason": None if ok else "SCORING_CHECK_FAILED",
            "calculation_owner": CALCULATION_OWNER, "entry_session": entry_session,
            "information_session": information_session, "newest_published_session": newest_session,
            "weights": weights, "predictions": preds, "vol_63": vols,
            "unlevered_target": {inst_names[int(k)]: float(v) for k, v in w_u.items()},
            "leverage": book["leverage"], "leverage_state": book["leverage_state"],
            "gross_unlevered": book["gross_unlevered"],
            "names_held_by_the_band": [inst_names[int(k)] for k in book["names_held_by_the_band"]],
            "previous_weights": dict(prev_weights or {}), "checks": checks,
            "source_data_hash": source_data_hash, "feature_state_hash": feature_state_hash}


def reproduce_frozen_cell_in_child() -> dict:
    """Re-measure the frozen cadence cell on the FROZEN store through this code path."""
    from alpha_agent.r63 import panels as P

    from . import cadence as CAD
    if Path(P.R41_CURVES).resolve() != Path(FROZEN_CURVES_DIR).resolve():
        return {"ok": False, "reason": "CHILD_ENVIRONMENT_NOT_THE_FROZEN_STORE"}
    sp = next(s for s in CAD.default_grid() if s["cell_id"] == FXC.FROZEN_CELL_ID)
    cell = CAD.measure_cell(sp, verbose=False)
    frozen = _read_json(research_root() / "cells" / FXC.CADENCE_CELL_FILE) or {}

    def pick(c):
        e = c.get("cadence_economics") or {}
        return {"conditional_t": (c.get("conditional") or {}).get("t"),
                "conditional_increment": (c.get("conditional") or {}).get("increment"),
                "ann_net_increment": e.get("ann_net_increment"),
                "t_increment": e.get("t_increment"),
                "augmented_sharpe": (e.get("augmented") or {}).get("sharpe"),
                "effective_periods": c.get("effective_periods"),
                "n_instruments": c.get("n_instruments")}
    a, b = pick(cell), pick(frozen)
    diffs = {k: (None if a[k] is None or b[k] is None else abs(float(a[k]) - float(b[k])))
             for k in a}
    ok = all(v is not None and v <= 1e-9 for v in diffs.values())
    return {"ok": ok, "reproduced": a, "frozen": b, "abs_diff": diffs,
            "cell_id": FXC.FROZEN_CELL_ID}


# --------------------------------------------------------------------------- #
# 9. RUNNING A CHILD
# --------------------------------------------------------------------------- #
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


def run_refresh(kind: str) -> dict:
    staging_root().mkdir(parents=True, exist_ok=True)
    res = _run_child(["--child-refresh", kind], env_extra={R41_ROOT_ENV: staging_root()},
                     timeout=TIMEOUT_REFRESH_SCOPE if kind == "scope" else TIMEOUT_REFRESH_ALL)
    body = _read_json(forward_dir() / ("store_refresh_%s.json" % kind)) or {}
    return {**res, "refresh": {k: body.get(k) for k in ("built_at", "n_markets", "n_failed", "failed")}}


def run_score(plan: dict, prev_weights: dict) -> dict:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = runs_dir() / ("%s_score_%s.json" % (stamp, plan["entry_session"]))
    prev_path = runs_dir() / ("%s_prev_%s.json" % (stamp, plan["entry_session"]))
    _write_json(prev_path, {"weights": dict(prev_weights or {})})
    refresh = run_refresh("all")
    if not refresh.get("ok"):
        return {"ok": False, "reason": "FULL_STORE_REFRESH_FAILED", "refresh": refresh}
    res = _run_child(["--child-score", "--entry", plan["entry_session"],
                      "--info", plan["information_session"],
                      "--newest", plan["newest_session_required"],
                      "--prev-json", str(prev_path), "--out", str(out_path)],
                     env_extra={CURVES_DIR_ENV: curves_dir()}, timeout=TIMEOUT_SCORE)
    body = _read_json(out_path)
    if not body:
        return {"ok": False, "reason": "CHILD_SCORE_PRODUCED_NO_RESULT", "child": res}
    return {**body, "child_returncode": res.get("returncode"), "score_artifact": str(out_path)}


# --------------------------------------------------------------------------- #
# 10. THE POLICY - declared ONCE, before any decision exists
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
            "record_rule": FXC.cost_model({"forward_specification": {"costs": "R38 per-market cost "
                                                                     "per side on one-way turnover"}})["rule"],
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
        "rebalance_rule": ("a decision is due when the realised FX sessions in (registration "
                           "session, newest published session] number a multiple of %d"
                           % TRADE_EVERY),
        "previous_book_rule": ("the decision frozen at the preceding boundary; flat when that "
                               "boundary has no decision; the first forward book starts flat"),
        "volatility_history_rule": ("unlevered target-book returns whose labels are realised by "
                                    "the information session"),
        "pit_delay_versus_the_historical_record": (
            "one session: the record decided at close t and entered at close t+1 on revised open "
            "interest and volume; prospectively those inputs are final one vendor update later, so "
            "the decision is formed on u and entered at the settlement of u+2"),
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
        "requires_live_market_data": False,
    }


def policy_declaration(identity: dict, *, scope: Optional[list] = None) -> dict:
    """Keyword arguments for :func:`prospective_decision.declare_policy`."""
    if scope is None:
        uni = FXC.resolve_universe()
        if not uni.get("ok"):
            raise ValueError("the registered universe cannot be resolved: %s" % uni.get("reason"))
        scope = list(uni["instruments"])
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
            "every fifth realised FX session after registration: the frozen cell %s is scored on the "
            "newest session whose open interest and volume are final, the cadence book is rebalanced "
            "with the frozen 0.25 no-trade band, and ONE decision is frozen between 00:00 and 09:30 ET "
            "on the entry session, entered at that session's settlement and held %d sessions; a "
            "boundary whose window closes without a decision is MISSED for ever and is never "
            "backfilled" % (FXC.FROZEN_CELL_ID, EVALUATION_HORIZON_SESSIONS)),
        "execution_contract": execution_contract(),
    }


# --------------------------------------------------------------------------- #
# 11. THE ADVANCE - one idempotent call the canonical runtime makes
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
    """Under pytest a decision may only be written into a redirected research root."""
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

    win = locate_window(ts)
    out["window"] = win
    if not win["in_window"]:
        last = win.get("last_session")
        if last and not PD.load_decision(CHALLENGER_ID, last):
            upto = [d for d in published if d <= NOC.previous_eligible_session(last)]
            p_last = plan_entry(entry_session=last, registration_session=registration_session,
                                scope=scope, published=upto, rows_by_market=rows)
            # Only a plan that KNOWS its grid position can be a missed boundary:
            # while publication was pending the position was never established.
            if p_last["state"] in ("DUE", ST_AWAITING_FINAL) and \
                    (NOC._as_utc(ts) - NOC._as_utc(window_for(last)["closes_at"])) < timedelta(hours=18) \
                    and (p_last.get("information_session") or "") >= registration_session:
                return {**out, "state": ST_MISSED, "plan": p_last, "backfill_refused": True,
                        "detail": "the %s window closed at %s with no decision; this boundary is "
                                  "missed permanently" % (last, window_for(last)["closes_at"])}
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
    scored = (scorer or run_score)(plan, prev["weights"])
    out["score"] = {k: scored.get(k) for k in ("ok", "reason", "leverage", "leverage_state",
                                                 "gross_unlevered", "score_artifact", "checks")}
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
        "window_opens_at": window_for(s)["opens_at"],
        "window_closes_at": window_for(s)["closes_at"],
        "holding_sessions": EVALUATION_HORIZON_SESSIONS,
        "maturity_session_expected": plan.get("maturity_session_expected"),
        "rebalance_index": plan["rebalance_index"],
        "previous_book": {"session": prev["session"], "flat_because": prev["flat_because"],
                          "weights": prev["weights"]},
        "leverage": scored.get("leverage"), "leverage_state": scored.get("leverage_state"),
        "gross_unlevered": scored.get("gross_unlevered"),
        "names_held_by_the_band": scored.get("names_held_by_the_band"),
        "scoring_checks": scored.get("checks"),
        "score_artifact": scored.get("score_artifact"),
        "frozen_cell_id": FXC.FROZEN_CELL_ID,
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


def schedule_preview(*, registration_session: str, published: list, horizon_days: int = 60) -> list:
    """The next rebalance entry sessions, on the exchange calendar (a projection, not a promise)."""
    last = published[-1] if published else registration_session
    idx = rebalance_index(published, registration_session, last)
    out, d, pos = [], last, idx
    for _ in range(horizon_days):
        nxt = NOC.next_eligible_session(d)
        if not nxt:
            break
        if pos % TRADE_EVERY == 0 and d >= registration_session:
            out.append({"entry_session": nxt, "newest_published_session": d,
                        "rebalance_index": pos})
        pos += 1
        d = nxt
    return out


__all__ = [
    "CALCULATION_OWNER", "CHALLENGER_ID", "TRADE_EVERY", "NO_TRADE_BAND",
    "EVALUATION_HORIZON_SESSIONS", "WINDOW_OPENS_ET", "WINDOW_CLOSES_ET", "STATES",
    "PROGRESS_STATES", "DATA_WAIT_STATES", "MISSED_STATES", "FAILURE_STATES", "SAFETY",
    "forward_dir", "curves_dir", "marks_store_relative", "daily_rows", "published_sessions",
    "information_is_final", "locate_window", "rebalance_index", "plan_entry", "previous_book",
    "unlevered_target", "forward_weights", "construction_policy", "policy_declaration",
    "execution_contract", "cost_policy", "advance", "schedule_preview",
    "refresh_store_in_child", "score_in_child", "reproduce_frozen_cell_in_child",
]
