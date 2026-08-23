"""alpha_agent.r40.research_cycle - the ONE prospective research cycle
(Track A): FORWARD_RESEARCH_CYCLE_STATE + FORWARD_CAPTURE_LEDGER_STATUS.

Release 39 created a manual capture command. This module is the canonical
callable the Persistent Daily Research Cycle can later be attached to - it
is NOT a scheduler and activates nothing. One call:

1. determines the eligible research dates for every registered shadow
   (strictly after that shadow's immutable freeze, present in the CURRENT
   panel, not yet captured, never in the future);
2. reads the required research inputs and measures their freshness
   (latest market session, macro overlay, COT, VX, NY Fed), flagging
   stale or missing sources per snapshot rather than silently imputing;
3. scores every registered shadow with its FROZEN specification (the R39
   members through the R39 capture owner, the R40 members through the
   registry-v2 scorer; nothing is ever refitted);
4. captures every newly eligible decision - CONTIGUOUSLY when catching up
   (no date may be skipped), recording the capture lateness in sessions;
5. matures outcomes whose horizon has passed and captures the permitted
   supporting marks (realised per-market forwards, sign accuracy, rank IC);
6. updates the always-valid sequential evidence for every shadow;
7. reports blocked candidates, and does NOTHING twice when rerun.

Ledgers are the canonical chain-hashed desk ledgers (append-only,
rewrite-detectable). No TRUE_FORWARD row can be dated at or before a
candidate's freeze; no historical row can enter; no row is backdated.

If no eligible date exists yet the cycle reports
``FORWARD_CAPTURE_STATE = READY_WAITING_FOR_ELIGIBLE_DATE`` and returns.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import research_shadow as RS
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C
from . import sequential as SQ
from . import shadow_registry as SR

CALCULATION_OWNER = "alpha_agent.r40.research_cycle"
STATE_NAME = "forward_research_cycle_state.json"
LEDGER_STATUS_NAME = "forward_capture_ledger_status.json"

#: Freshness tolerances (calendar days behind the latest market session)
#: declared before any cycle runs.
FRESHNESS_TOLERANCE_DAYS = {"macro_overlay": 10, "cot": 14, "vx": 7,
                            "nyfed": 21}
ON_TIME_MAX_SESSIONS = 1


def _desk():
    return RS._desk()


def _now() -> pd.Timestamp:
    return pd.Timestamp(time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))


# --------------------------------------------------------------------------- #
# Inputs and freshness
# --------------------------------------------------------------------------- #
def input_freshness(state: dict, now: pd.Timestamp) -> dict:
    fut = state.get("fut")
    latest = pd.to_datetime(fut["decision_date"]).max() if fut is not None \
        and not fut.empty else None
    out = {"latest_market_session": str(latest.date()) if latest is not None
           else None, "as_of": str(now)}
    macro = state.get("macro")
    if macro is not None and not macro.empty:
        last = {c: macro[c].dropna().index.max() for c in macro.columns}
        stale = {c: str(d.date()) for c, d in last.items()
                 if latest is not None and d is not None
                 and (latest - d).days > FRESHNESS_TOLERANCE_DAYS[
                     "macro_overlay"]}
        out["macro_overlay"] = {"last_by_series": {c: str(d.date())
                                                   for c, d in last.items()
                                                   if d is not None},
                                "stale_series": stale,
                                "state": "STALE" if stale else "FRESH"}
    if fut is not None and "has_cot" in fut.columns:
        cot_last = pd.to_datetime(fut.loc[fut["has_cot"] == 1,
                                          "decision_date"]).max() \
            if (fut["has_cot"] == 1).any() else None
        out["cot"] = {"last": str(cot_last.date()) if cot_last is not None
                      else None,
                      "state": "MISSING" if cot_last is None else
                      "STALE" if (latest - cot_last).days >
                      FRESHNESS_TOLERANCE_DAYS["cot"] else "FRESH"}
    vx = state.get("vx")
    if vx is not None and not vx.empty:
        vlast = pd.to_datetime(vx["decision_date"]).max()
        out["vx"] = {"last": str(vlast.date()),
                     "state": "STALE" if latest is not None and
                     (latest - vlast).days > FRESHNESS_TOLERANCE_DAYS["vx"]
                     else "FRESH"}
    intl = state.get("fut_intl_rates")
    if intl is not None and not intl.empty:
        ilast = pd.to_datetime(intl["decision_date"]).max()
        out["intl_rates"] = {"last": str(ilast.date())}
    out["stale_sources"] = sorted(k for k, v in out.items()
                                  if isinstance(v, dict)
                                  and v.get("state") in ("STALE", "MISSING"))
    return out


# --------------------------------------------------------------------------- #
# Eligibility
# --------------------------------------------------------------------------- #
def _panel_for(sh: dict, state: dict):
    lane = sh.get("lane")
    if lane == "VX":
        return state.get("vx")
    if lane == "FUT_INTL_RATES":
        return state.get("fut_intl_rates")
    return state.get("fut")


def eligible_dates(sh: dict, panel: pd.DataFrame, captured: set,
                   now: pd.Timestamp) -> list:
    if panel is None or panel.empty:
        return []
    frozen_at = pd.Timestamp(str(sh["frozen_at"]).replace("Z", ""))
    dates = sorted(pd.to_datetime(panel["decision_date"]).unique())
    return [d for d in dates
            if d > frozen_at and d <= now and str(d.date()) not in captured]


def next_expected_eligible(sh: dict, now: pd.Timestamp) -> str:
    frozen_at = pd.Timestamp(str(sh["frozen_at"]).replace("Z", ""))
    start = max(frozen_at, now)
    if sh.get("lane") == "VX":
        return "first 5th-session VX decision after %s" % start.date()
    me = (start + pd.offsets.MonthEnd(0))
    if me <= start:
        me = start + pd.offsets.MonthEnd(1)
    return "each market's last session of %s" % me.strftime("%Y-%m")


def _lateness_sessions(decision_date: pd.Timestamp, now: pd.Timestamp) -> int:
    return int(np.busday_count(decision_date.date(), now.date()))


# --------------------------------------------------------------------------- #
# R40 member capture / maturation (R39 members go through the R39 owner)
# --------------------------------------------------------------------------- #
def _capture_r40(registry: dict, state: dict, now: pd.Timestamp,
                 freshness: dict, campaign_id: str) -> dict:
    desk = _desk()
    sdir = SR.shadow_dir(campaign_id)
    rows = desk._read_ledger(sdir, SR.SNAPSHOT_LEDGER)
    appended, blocked, per_shadow = [], [], {}
    for sh in registry["shadows"]:
        if sh.get("origin_release") != "release40":
            continue
        captured = {r["decision_date"] for r in rows
                    if r.get("shadow_id") == sh["shadow_id"]}
        panel = _panel_for(sh, state)
        if panel is None or panel.empty:
            blocked.append({"shadow_id": sh["shadow_id"],
                            "reason": "PANEL_UNAVAILABLE"})
            continue
        elig = eligible_dates(sh, panel, captured, now)
        per_shadow[sh["shadow_id"]] = {"eligible": len(elig),
                                       "already_captured": len(captured)}
        dd = pd.to_datetime(panel["decision_date"])
        n_ok = 0
        for d in elig:                       # contiguous: every date, in order
            rows_d = panel[dd == d]
            w = SR.score_at(sh, rows_d)
            if w is None:
                blocked.append({"shadow_id": sh["shadow_id"],
                                "decision_date": str(d.date()),
                                "reason": "FEATURES_UNAVAILABLE_OR_TOO_FEW_"
                                          "MARKETS"})
                continue
            late = _lateness_sessions(pd.Timestamp(d), now)
            appended.append({
                "kind": "SHADOW_TARGET_SNAPSHOT",
                "shadow_id": sh["shadow_id"],
                "candidate_id": sh["candidate_id"],
                "spec_hash": sh.get("spec_hash"),
                "coefficient_hash": sh.get("coefficient_hash"),
                "decision_date": str(pd.Timestamp(d).date()),
                "captured_at": str(now) + "Z",
                "forward_evidence_type": "TRUE_FORWARD",
                "capture_lateness_sessions": late,
                "evidence_grade": "ON_TIME" if late <= ON_TIME_MAX_SESSIONS
                else "LATE_CAPTURE_CONTIGUOUS",
                "input_freshness_stale_sources": freshness.get(
                    "stale_sources", []),
                "horizon_sessions": sh["horizon_sessions"],
                "weights": {k: round(float(v), 8) for k, v in w.items()},
                "n_predictions": int(len(w)),
                "promotion_allowed": False})
            n_ok += 1
        per_shadow[sh["shadow_id"]]["captured_now"] = n_ok
    if appended:
        desk._append_ledger(sdir, SR.SNAPSHOT_LEDGER, appended)
    return {"appended": len(appended), "blocked": blocked,
            "per_shadow": per_shadow,
            "verify": desk.verify_ledger(sdir, SR.SNAPSHOT_LEDGER)}


def _mature_r40(registry: dict, state: dict, now: pd.Timestamp,
                campaign_id: str) -> dict:
    desk = _desk()
    sdir = SR.shadow_dir(campaign_id)
    snaps = desk._read_ledger(sdir, SR.SNAPSHOT_LEDGER)
    outs = desk._read_ledger(sdir, SR.OUTCOME_LEDGER)
    done = {(r["shadow_id"], r["decision_date"]) for r in outs}
    by_shadow = {s["shadow_id"]: s for s in registry["shadows"]}
    appended = []
    prev_w: dict = {}
    for snap in snaps:
        key = (snap["shadow_id"], snap["decision_date"])
        sh = by_shadow.get(snap["shadow_id"])
        if sh is None:
            continue
        panel = _panel_for(sh, state)
        if panel is None or panel.empty:
            continue
        fwd_col = "fwd_%d" % sh["horizon_sessions"]
        rows_d = panel[pd.to_datetime(panel["decision_date"])
                       == pd.Timestamp(snap["decision_date"])]
        if key in done or rows_d.empty or fwd_col not in rows_d.columns:
            prev_w[snap["shadow_id"]] = snap["weights"]
            continue
        fwd = rows_d.set_index("market_id")[fwd_col]
        w = pd.Series(snap["weights"], dtype=float)
        r = fwd.reindex(w.index)
        if r.isna().all():
            continue                                   # not matured yet
        gross = float((w * r.fillna(0.0)).sum())
        pw = pd.Series(prev_w.get(snap["shadow_id"], {}), dtype=float)
        union = w.index.union(pw.index)
        dw = (w.reindex(union, fill_value=0.0)
              - pw.reindex(union, fill_value=0.0)).abs()
        bps = (sh.get("cost_model") or {}).get("bps_per_side") or {}
        rate = pd.Series({m: float(bps.get(m, 10.0)) / 1e4 for m in dw.index})
        cost = float((dw * rate).sum())
        m = r.notna()
        sign_acc = float((np.sign(w[m]) == np.sign(r[m])).mean()) \
            if m.sum() else None
        ic = float(w[m].rank().corr(r[m].rank())) if m.sum() >= 5 else None
        appended.append({
            "kind": "SHADOW_OUTCOME", "shadow_id": snap["shadow_id"],
            "decision_date": snap["decision_date"],
            "matured_at": str(now) + "Z",
            "gross_return": round(gross, 8), "cost": round(cost, 8),
            "net_return": round(gross - cost, 8),
            "n_matured_markets": int(m.sum()),
            "supporting": {"sign_accuracy": sign_acc, "rank_ic": ic,
                           "traded_notional": float(dw.sum())}})
        prev_w[snap["shadow_id"]] = snap["weights"]
    if appended:
        desk._append_ledger(sdir, SR.OUTCOME_LEDGER, appended)
    return {"appended": len(appended),
            "verify": desk.verify_ledger(sdir, SR.OUTCOME_LEDGER)}


# --------------------------------------------------------------------------- #
# Sequential evidence
# --------------------------------------------------------------------------- #
def sequential_evidence(registry: dict, campaign_id: str) -> dict:
    desk = _desk()
    designs = (_r39.read_json(campaign_dir(campaign_id) / SQ.DESIGNS_NAME)
               or {}).get("designs") or {}
    r39_outs = desk._read_ledger(RS.shadow_dir(C.R39_CONTINUATION_CAMPAIGN_ID),
                                 RS.OUTCOME_LEDGER)
    r40_outs = desk._read_ledger(SR.shadow_dir(campaign_id), SR.OUTCOME_LEDGER)
    out, e_values = {}, []
    for sh in registry["shadows"]:
        src = r39_outs if sh.get("origin_release") == "release39" else r40_outs
        rets = [float(r["net_return"]) for r in src
                if r.get("shadow_id") == sh["shadow_id"]]
        des = designs.get(sh["shadow_id"]) or {}
        sigma0 = des.get("sigma0_per_period")
        if sigma0 is None:
            out[sh["shadow_id"]] = {"state": "NO_DESIGN", "n": len(rets)}
            continue
        ev = SQ.evaluate_shadow(rets, sigma0=float(sigma0),
                         shadow_id=sh["shadow_id"],
                         max_horizon=int(des.get("max_horizon_observations")
                                         or 60))
        e_values.append(ev["e_value"])
        out[sh["shadow_id"]] = {
            "n_true_forward_outcomes": ev["n_observations"],
            "e_value": ev["e_value"],
            "decision_state": ev["decision_state"],
            "confidence_sequence": ev["confidence_sequence"],
            "interest": ("INSUFFICIENT_FORWARD_EVIDENCE"
                         if ev["n_observations"] < 6 else
                         "FORWARD_FUTILITY" if ev["decision_state"]
                         == "FAILURE_BOUNDARY_CROSSED" else
                         "FORWARD_INTEREST_STRENGTHENED"
                         if ev["e_value"] > 1.0 else
                         "FORWARD_INTEREST_WEAKENED"),
            "promotion_allowed": False}
    return {"per_shadow": out,
            "family_e_value": SQ.family_e_value(e_values),
            "family_success_e": SQ.FAMILY_E_SUCCESS}


# --------------------------------------------------------------------------- #
# The ONE callable
# --------------------------------------------------------------------------- #
def run_cycle(*, mode: str = "capture", fresh_state: dict = None,
              now: pd.Timestamp = None, campaign_id: str = CAMPAIGN_ID,
              build_state: bool = True) -> dict:
    """mode: 'status' (no inputs read), 'capture' (capture + mature +
    evidence), 'mature' (mature + evidence only)."""
    now = now or _now()
    registry = SR.load(campaign_id)
    r39_registry = RS.load_registry(C.R39_CONTINUATION_CAMPAIGN_ID) or {}
    if not registry:
        # pre-freeze: the R39 family is the whole family
        registry = {"shadows": [dict(s, origin_release="release39")
                                for s in r39_registry.get("shadows", [])],
                    "n_shadows": r39_registry.get("n_shadows", 0),
                    "frozen_at": r39_registry.get("frozen_at")}
    desk = _desk()
    r39_dir = RS.shadow_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
    r40_dir = SR.shadow_dir(campaign_id)
    result = {"mode": mode, "as_of": str(now), "n_shadows":
              registry.get("n_shadows")}
    if mode != "status":
        if fresh_state is None and build_state:
            fresh_state = build_fresh_state(registry)
        if fresh_state is None:
            result["state"] = "NO_FRESH_STATE"
        else:
            fresh = input_freshness(fresh_state, now)
            result["input_freshness"] = fresh
            cap39 = {"appended": 0, "state": "NO_R39_REGISTRY"}
            mat39 = {"appended": 0, "state": "NO_R39_REGISTRY"}
            cap40 = {"appended": 0, "blocked": [], "per_shadow": {}}
            if mode == "capture":
                if r39_registry:
                    # R39 members: the R39 owner (refuses d <= its freeze)
                    cap39 = RS.capture(None, campaign_id=
                                       C.R39_CONTINUATION_CAMPAIGN_ID,
                                       fresh_state=fresh_state)
                cap40 = _capture_r40(registry, fresh_state, now, fresh,
                                     campaign_id)
            if r39_registry:
                mat39 = RS.mature(campaign_id=C.R39_CONTINUATION_CAMPAIGN_ID,
                                  fresh_state=fresh_state)
            mat40 = _mature_r40(registry, fresh_state, now, campaign_id)
            result.update({"capture_r39": cap39, "capture_r40": cap40,
                           "mature_r39": mat39, "mature_r40": mat40})
            # eligibility report for every member (what WAS eligible)
            elig = {}
            for sh in registry["shadows"]:
                panel = _panel_for(sh, fresh_state)
                src_dir = r39_dir if sh.get("origin_release") == "release39" \
                    else r40_dir
                snaps = desk._read_ledger(src_dir, RS.SNAPSHOT_LEDGER)
                captured = {r["decision_date"] for r in snaps
                            if r.get("shadow_id") == sh["shadow_id"]}
                elig[sh["shadow_id"]] = {
                    "captured_total": len(captured),
                    "remaining_eligible": len(eligible_dates(
                        sh, panel, captured, now)) if panel is not None
                    else None,
                    "next_expected": next_expected_eligible(sh, now),
                    "frozen_at": sh["frozen_at"]}
            result["eligibility"] = elig
            total_new = cap39.get("appended", 0) + cap40.get("appended", 0)
            any_rows = any(v["captured_total"] > 0 for v in elig.values())
            result["FORWARD_CAPTURE_STATE"] = (
                "CAPTURED_NEW_DECISIONS" if total_new > 0 else
                "NOTHING_NEW_IDEMPOTENT" if any_rows else
                "READY_WAITING_FOR_ELIGIBLE_DATE")
            result["blocked_candidates"] = cap40.get("blocked", [])
    result["sequential_evidence"] = sequential_evidence(registry, campaign_id)
    status = {
        "r39_snapshot_ledger": desk.verify_ledger(r39_dir, RS.SNAPSHOT_LEDGER),
        "r39_outcome_ledger": desk.verify_ledger(r39_dir, RS.OUTCOME_LEDGER),
        "r40_snapshot_ledger": desk.verify_ledger(r40_dir, SR.SNAPSHOT_LEDGER),
        "r40_outcome_ledger": desk.verify_ledger(r40_dir, SR.OUTCOME_LEDGER),
    }
    status["true_forward_snapshots"] = status["r39_snapshot_ledger"]["n_rows"] \
        + status["r40_snapshot_ledger"]["n_rows"]
    status["true_forward_outcomes"] = status["r39_outcome_ledger"]["n_rows"] \
        + status["r40_outcome_ledger"]["n_rows"]
    status["all_chains_intact"] = all(v["intact"] for k, v in status.items()
                                      if isinstance(v, dict))
    result["ledger_status"] = status
    result.setdefault("FORWARD_CAPTURE_STATE",
                      "STATUS_ONLY" if mode == "status" else "NO_FRESH_STATE")
    body = artifact_body("r40_forward_research_cycle_state/1", {
        "calculation_owner": CALCULATION_OWNER, **result,
        "idempotent": True, "catch_up_contiguous":
            C.CATCH_UP_MUST_BE_CONTIGUOUS,
        "no_backdating": True, "automation": "OFF - callable only",
        "scheduler_changed": False, "orders_created": 0,
        "promotions": 0})
    body["cycle_state_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / STATE_NAME, body,
                    immutable=False)
    lb = artifact_body("r40_forward_capture_ledger_status/1", {
        "calculation_owner": CALCULATION_OWNER, "as_of": str(now), **status,
        "primitives": "api.paper_trading_desk chain-hash ledgers"})
    lb["ledger_status_hash"] = _r39.sha(lb)
    _r39.write_json(campaign_dir(campaign_id) / LEDGER_STATUS_NAME, lb,
                    immutable=False)
    return body


# --------------------------------------------------------------------------- #
# Fresh state (CURRENT data through the canonical builders)
# --------------------------------------------------------------------------- #
def build_fresh_state(registry: dict) -> dict:
    """The R39 owner's current universal state, plus whatever the R40
    members need: the international-rates panel (canonical R38 layer
    builder), sequence lags, graph aggregates, admissible bundles."""
    state = RS.build_fresh_state()
    needs = {s.get("lane") for s in registry.get("shadows", [])}
    bundles = {s.get("bundle") for s in registry.get("shadows", [])}
    if "FUT_INTL_RATES" in needs:
        state["fut_intl_rates"] = build_fresh_intl_rates(state)
    fut = state["fut"]
    if "SEQ_CLS" in bundles:
        from ..r39 import models_ext as MX
        from ..r39.continuation_director import SEQ_BASE_FEATURES
        fut, _cols = MX.add_sequence_lags(
            fut, [c for c in SEQ_BASE_FEATURES if c in fut.columns])
    if "GRAPH_AGG" in bundles:
        from .model_challenge import add_graph_aggregates
        fut, _n, _i = add_graph_aggregates(fut)
    state["fut"] = fut
    return state


def build_fresh_intl_rates(state: dict) -> pd.DataFrame:
    from ..r38 import contract as C38
    from ..r38 import enumeration as EN38
    from ..r38 import research_layer as RL38
    from ..r39.continuation_director import (INTL_RATES_COST_BPS,
                                             INTL_RATES_MARKETS)
    from ..r39.target_factory import materialise
    from ..r39.universal_state import build_futures_panel
    registry = EN38.load_contract_registry(C38.CAMPAIGN_ID)
    layer = {}
    for mkt in INTL_RATES_MARKETS:
        lists = registry["contract_symbols"].get(mkt, {})
        primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
        symbols = lists.get(primary, []) if primary else []
        series = RL38.build_market_series(mkt, symbols)
        if series is None or not len(series):
            continue
        df = series.reset_index()
        df = df.rename(columns={df.columns[0]: "Date"})
        df["Date"] = pd.to_datetime(df["Date"]).astype("datetime64[ns]")
        layer[mkt] = df
    meta = pd.DataFrame([{"market_id": m, "asset_class": "RATES",
                          "economic_group": "INTL_RATES_FUTURES",
                          "currency": "LOCAL",
                          "cost_bps_per_side": INTL_RATES_COST_BPS}
                         for m in layer])
    cot = pd.DataFrame(columns=["market_id", "decision_date",
                                "cot_commercial_z"])
    cot["decision_date"] = pd.to_datetime(cot["decision_date"])
    panel = build_futures_panel(layer, meta, cot, state["macro"])
    return materialise(panel, scope_col="asset_class")
