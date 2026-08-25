"""alpha_agent.r43.frontier - Tracks P, R and S, plus the ZONE_C lockbox.

Four jobs, in this order:

1. **ZONE_C access.** The lockbox opens for a lineage only if that lineage
   cleared the frozen pre-gate (``ZONE_C_PREGATE_T = 2.5``) on ZONE_B, and it
   opens ONCE. Every access is recorded with the reason it was granted, so
   the count of accesses is auditable rather than asserted.
2. **Search adjustment.** Deflated Sharpe at the CUMULATIVE FAMILY
   denominator and, reported beside it, at the global one - the family
   answers "how hard did we search THIS question", the global answers "how
   hard has this estate searched anything". Benjamini-Hochberg runs within
   the family. Both owners are imported, never re-implemented.
3. **The cross-family frontier.** Every ZONE_B candidate from every lane, on
   one ranked table with the contract's required fields, ranked by
   evidence-weighted implementable economic value - never by Sharpe alone.
4. **The prospective freeze.** Candidates good enough to be worth future
   evidence are frozen BEFORE their future exists, as
   RESEARCH_SHADOW_ONLY with PROMOTION_ALLOWED = False.

The freeze rule this release adds, and the reason it exists: a candidate is
frozen TOGETHER WITH ITS VOLATILITY-MATCHED PASSIVE CONTROL, so what the
future tests is the INCREMENT the signal claims to add - not the exposure
anyone could have had for free. R42 learned that a premium can be real and a
timing rule worthless at the same time; freezing only the timing rule would
lose exactly that distinction.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, sha
from . import burden as B
from . import contract as C
from . import judge as J
from ..r31 import multiple_testing as MT
from ..r39 import burden as R39B
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.frontier"
ZONE_C_LEDGER = "r43_zone_c_access_ledger.json"
SHADOW_REGISTRY = "r43_shadow_registry.json"
FRONTIER_ARTIFACT = "R43_CANDIDATE_FRONTIER.json"


# --------------------------------------------------------------------------- #
# ZONE_C lockbox
# --------------------------------------------------------------------------- #
def _ledger_path():
    return campaign_dir(CAMPAIGN_ID) / ZONE_C_LEDGER


def zone_c_ledger() -> dict:
    body = read_json(_ledger_path())
    return body or {"schema": "r43_zone_c_access/1", "accesses": {}}


def may_open_zone_c(candidate_id: str, zone_b_t: float) -> dict:
    led = zone_c_ledger()
    already = candidate_id in (led.get("accesses") or {})
    eligible = zone_b_t is not None and zone_b_t >= C.ZONE_C_PREGATE_T
    return {"candidate_id": candidate_id, "zone_b_t": zone_b_t,
            "pregate_t": C.ZONE_C_PREGATE_T, "eligible": bool(eligible),
            "already_accessed": bool(already),
            "may_open": bool(eligible and not already)}


def record_zone_c(candidate_id: str, *, zone_b_t: float, reason: str,
                  result: dict) -> dict:
    import json
    led = zone_c_ledger()
    led.setdefault("accesses", {})[candidate_id] = {
        "at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "zone_b_t": zone_b_t, "pregate_t": C.ZONE_C_PREGATE_T,
        "reason": reason, "result": result,
        "one_access_per_lineage": True,
    }
    p = _ledger_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(led, indent=1, sort_keys=True, default=str),
                 encoding="utf-8")
    return led["accesses"][candidate_id]


def open_zone_c_for_rv(candidate_id: str, kind: str, signal: str,
                       expression: str, zones: dict, zone_b_t: float) -> dict:
    """The single controlled read of the lockbox for an RV candidate."""
    from . import rv as RV
    from . import killer as K
    gate = may_open_zone_c(candidate_id, zone_b_t)
    if not gate["may_open"]:
        return {"state": "NOT_OPENED", "gate": gate}
    structures = RV.build_structures(kind)
    bk = RV.book_streams(structures, signal, expression)
    card = RV.score_book(bk, zones["C"])
    passive = K._passive_control(structures, zones["C"], bk)
    result = {
        "zone_c": {k: v for k, v in card.items() if k != "diff_stream"},
        "passive_increment": passive,
        "zone_c_range": zones.get("c_range"),
    }
    record_zone_c(candidate_id, zone_b_t=zone_b_t,
                  reason="ZONE_B t %.3f >= frozen pre-gate %.2f"
                         % (zone_b_t, C.ZONE_C_PREGATE_T),
                  result=result)
    return {"state": "OPENED", "gate": gate, **result}


# --------------------------------------------------------------------------- #
# Search adjustment
# --------------------------------------------------------------------------- #
def search_adjustment(candidates: list) -> dict:
    """Deflated Sharpe at the family and global denominators, plus BH.

    ``candidates`` carries, per row: candidate_id, family, zone_b card and
    the per-period excess stream used to compute it.
    """
    if not candidates:
        return {"state": "NO_CANDIDATES"}
    summ = B.summary()
    sharpes = []
    for c in candidates:
        s = c.get("zone_b", {}).get("sharpe")
        if s is not None:
            sharpes.append(float(s) / np.sqrt(J.TRADING_DAYS))
    tsv = float(np.var(sharpes, ddof=1)) if len(sharpes) > 1 else 1e-6

    rows, pvals = [], []
    for c in candidates:
        stream = c.get("excess_stream")
        fam = c.get("family")
        fam_n = summ["cumulative_family_counts"].get(fam, 1)
        glob_n = summ["global_cumulative"]
        t = c.get("zone_b", {}).get("excess_t_hac")
        p = MT.two_sided_p(t) if t is not None else None
        pvals.append(p if p is not None else 1.0)
        row = {"candidate_id": c["candidate_id"], "family": fam,
               "zone_b_t": t, "p_two_sided": p,
               "family_denominator": fam_n, "global_denominator": glob_n}
        if stream is not None and len(np.asarray(stream)) >= 24:
            arr = np.asarray(stream, dtype=float)
            row["dsr_at_family_burden"] = R39B.deflated_sharpe(
                arr, n_trials=fam_n, trial_sharpe_variance=tsv).get("dsr")
            row["dsr_at_global_burden"] = R39B.deflated_sharpe(
                arr, n_trials=glob_n, trial_sharpe_variance=tsv).get("dsr")
        rows.append(row)
    bh = MT.benjamini_hochberg(pvals, q=C.RESEARCH_CANDIDATE_GATE[
        "family_bh_q"])
    for row, keep in zip(rows, (bh.get("reject") or [False] * len(rows))):
        row["bh_survivor"] = bool(keep)
    return {
        "calculation_owner": CALCULATION_OWNER,
        "trial_sharpe_variance": tsv,
        "global_denominator": summ["global_cumulative"],
        "family_denominators": summ["cumulative_family_counts"],
        "bh_q": C.RESEARCH_CANDIDATE_GATE["family_bh_q"],
        "bh": {k: v for k, v in bh.items() if k != "p_values"},
        "rows": rows,
        "dsr_owner": "alpha_agent.r39.burden.deflated_sharpe",
        "bh_owner": "alpha_agent.r31.multiple_testing.benjamini_hochberg",
    }


# --------------------------------------------------------------------------- #
# The cross-family frontier
# --------------------------------------------------------------------------- #
#: Ranking tiers. A candidate's QUALIFICATION_STATE dominates its return:
#: a rejected or refuted book must never outrank a surviving one because it
#: happened to post a larger number.
_QUALIFICATION_WEIGHT = {
    "QUALIFIED_ALPHA_CANDIDATE": 1.00,
    "RESEARCH_CANDIDATE_ZONE_C_NOT_OPENED": 0.60,
    "RESEARCH_CANDIDATE_STRUCTURAL_PREMIUM_NOT_TIMING_ALPHA": 0.40,
    "RESEARCH_CANDIDATE_ZONE_C_DID_NOT_CONFIRM": 0.25,
    "REFUTED_ON_ZONE_C_SIGN_FLIP": 0.15,
    "NOT_FULLY_ATTACKED": 0.20,
    "KILLED_BY_ALTERNATIVE_CONTROL": 0.10,
    "KILLED_BY_ALPHA_KILLER": 0.10,
    "REJECTED_AT_RESEARCH_CANDIDATE_GATE": 0.05,
}


def _economic_value(row: dict) -> float:
    """Evidence-weighted implementable economic value.

    NOT Sharpe. A candidate scores on (a) whether its net residual on
    committed capital is positive, (b) how much evidence supports it,
    (c) whether it survives its own control, and (d) what its qualification
    state actually is.

    Two corrections worth naming, because the first draft got both wrong and
    the ranking it produced was visibly absurd - a book rejected at its own
    gate stood above the only survivor:

    * an UNMEASURED control is not a passing control. Scoring "no increment
      measured" as 1.0 rewarded candidates precisely for not having been
      attacked. It now scores 0.5, below a measured-and-strong control and
      above a measured-and-weak one.
    * the qualification state now multiplies the score, so a refuted or
      rejected candidate cannot outrank a surviving one on size alone.
    """
    net = row.get("NET_RESIDUAL_ALPHA")
    t = row.get("T_STAT")
    if net is None or t is None or net <= 0:
        return -abs(float(net or 0.0))
    evidence = min(max((float(t) - 1.0) / 2.0, 0.0), 2.0)
    control = row.get("_increment_t")
    control_w = 0.5 if control is None else (
        1.0 if control >= 2.0 else 0.25 if control >= 0.0 else 0.05)
    robust = {"SURVIVES_FULL_BATTERY": 1.0,
              "PARTIAL_BATTERY_ONLY": 0.5,
              "NOT_ATTACKED": 0.5}.get(row.get("ROBUSTNESS"), 0.25)
    qual = _QUALIFICATION_WEIGHT.get(row.get("QUALIFICATION_STATE"), 0.05)
    return float(net) * evidence * control_w * robust * qual


def frontier_rows(lane_results: dict, adjust: dict) -> list:
    """Every ZONE_B candidate in the release, on one table."""
    by_id = {r["candidate_id"]: r for r in (adjust.get("rows") or [])}
    rows = []
    for lane, res in lane_results.items():
        if not isinstance(res, dict):
            continue
        for a in (res.get("advanced") or []):
            spec = a.get("spec") or {}
            zb = a.get("zone_b") or {}
            adj = by_id.get(a["candidate_id"], {})
            kill = a.get("kill") or {}
            # The carry lane attaches its own passive increment; the RV lane's
            # lives inside the kill battery's ALTERNATIVE_ECONOMIC_CONTROL.
            # Reading only the first would silently report every RV
            # candidate's increment as absent - which is the one number that
            # decides whether a candidate is alpha or an exposure.
            inc = (a.get("passive_increment")
                   or ((kill.get("tests") or {})
                       .get("ALTERNATIVE_ECONOMIC_CONTROL") or {}))
            row = {
                "CANDIDATE_ID": a["candidate_id"],
                "LANE": lane,
                "ASSET": spec.get("asset_family"),
                "ASSET_CLASS": _asset_class(spec),
                "HORIZON": spec.get("horizon"),
                "INFORMATION_FAMILY": spec.get("information_family"),
                "ECONOMIC_EXPRESSION": spec.get("economic_expression"),
                "MODEL": spec.get("model"),
                "GROSS_RETURN": zb.get("gross_ann_on_notional"),
                "FULL_COST": zb.get("cost_ann_on_notional"),
                "COMMITTED_CAPITAL": a.get("committed_capital"),
                "CASH_HURDLE": zb.get("cash_hurdle_ann"),
                "NET_RESIDUAL_ALPHA": zb.get("excess_ann"),
                "VOLATILITY": zb.get("vol_ann"),
                "SHARPE": zb.get("sharpe"),
                "T_STAT": zb.get("excess_t_hac"),
                "SEARCH_ADJUSTMENT": {
                    "family_denominator": adj.get("family_denominator"),
                    "global_denominator": adj.get("global_denominator"),
                    "dsr_at_family_burden": adj.get("dsr_at_family_burden"),
                    "dsr_at_global_burden": adj.get("dsr_at_global_burden"),
                    "bh_survivor": adj.get("bh_survivor")},
                "ROBUSTNESS": _robustness(kill),
                "CAPACITY": a.get("capacity") or "NOT_BINDING_AT_RESEARCH_"
                                                 "SCALE",
                "PIT_STATUS": "PIT_TRUE",
                "FORWARD_READY": bool((a.get("gate") or {}).get("passes")),
                "QUALIFICATION_STATE": _qualification(a, adj, inc),
                "_increment_ann": inc.get("increment_ann"),
                "_increment_t": inc.get("increment_t_hac"),
                "_gate_checks": (a.get("gate") or {}).get("checks"),
                "_zone_c": a.get("zone_c"),
                "ZONE_C": ({"excess_ann": (a.get("zone_c") or {})
                            .get("excess_ann"),
                            "t": (a.get("zone_c") or {}).get("excess_t_hac"),
                            "opened": bool(a.get("zone_c"))}
                           if a.get("zone_c") is not None
                           else {"opened": False,
                                 "gate": a.get("zone_c_gate")}),
            }
            row["ECONOMIC_VALUE_SCORE"] = _economic_value(row)
            rows.append(row)
    rows.sort(key=lambda r: -(r["ECONOMIC_VALUE_SCORE"] or -9e9))
    for i, r in enumerate(rows, 1):
        r["RANK"] = i
    return rows


def _robustness(kill: dict) -> str:
    """A battery that was not fully run is NOT a kill, and saying so would be
    a lie in the flattering direction for the tests that did not run and the
    damning direction for the candidate. Report exactly what happened."""
    if not kill:
        return "NOT_ATTACKED"
    if kill.get("state") == "PARTIAL":
        return "PARTIAL_BATTERY_ONLY"
    if kill.get("survives"):
        return "SURVIVES_FULL_BATTERY"
    return "KILLED_BY_%s" % ",".join(kill.get("killed_by") or ["UNKNOWN"])


def _asset_class(spec: dict) -> str:
    fam = (spec.get("asset_family") or "").upper()
    for key, val in (("RATES", "RATES"), ("FX", "FX"),
                     ("COMMODITY", "COMMODITY"), ("EQUITY", "EQUITY"),
                     ("MULTI_ASSET", "MULTI_ASSET"),
                     ("CRYPTO", "CRYPTO"), ("LIQUID_FUTURES",
                                            "MULTI_ASSET")):
        if key in fam:
            return val
    return "MULTI_ASSET"


def _qualification(a: dict, adj: dict, inc: dict) -> str:
    """The lockbox is decisive, and it is checked FIRST.

    Earlier drafts of this function reported the control verdict before the
    ZONE_C verdict, which meant a candidate refuted by the lockbox could
    still be labelled as merely "a structural premium". Refutation outranks
    interpretation.
    """
    gate = (a.get("gate") or {}).get("passes")
    if not gate:
        return "REJECTED_AT_RESEARCH_CANDIDATE_GATE"
    kill = a.get("kill") or {}
    t_inc0 = inc.get("increment_t_hac")
    if kill and not kill.get("survives"):
        # Distinguish "the control killed it" from "we could not run the
        # whole battery on this book shape". Only one of those is a verdict.
        if t_inc0 is not None and t_inc0 < 0:
            return "KILLED_BY_ALTERNATIVE_CONTROL"
        if kill.get("state") == "PARTIAL":
            return "NOT_FULLY_ATTACKED"
        return "KILLED_BY_ALPHA_KILLER"
    zc = a.get("zone_c")
    zb = (a.get("zone_b") or {}).get("excess_ann")
    t_inc = inc.get("increment_t_hac")
    if zc is not None:
        zca = zc.get("excess_ann")
        if zca is not None and zb is not None and zca * zb <= 0:
            return "REFUTED_ON_ZONE_C_SIGN_FLIP"
        if (zc.get("excess_t_hac") or 0) < C.QUALIFIED_ALPHA_GATE[
                "zone_c_after_cost_excess_t_min"]:
            return "RESEARCH_CANDIDATE_ZONE_C_DID_NOT_CONFIRM"
        if t_inc is not None and t_inc < 2.0:
            return "RESEARCH_CANDIDATE_STRUCTURAL_PREMIUM_NOT_TIMING_ALPHA"
        return "QUALIFIED_ALPHA_CANDIDATE"
    if t_inc is not None and t_inc < 2.0:
        return "RESEARCH_CANDIDATE_STRUCTURAL_PREMIUM_NOT_TIMING_ALPHA"
    return "RESEARCH_CANDIDATE_ZONE_C_NOT_OPENED"


# --------------------------------------------------------------------------- #
# Track R - the prospective freeze
# --------------------------------------------------------------------------- #
def _zone_c_refuted(row: dict) -> bool:
    zc = row.get("_zone_c") or {}
    zb, zca = row.get("NET_RESIDUAL_ALPHA"), zc.get("excess_ann")
    if zca is None or zb is None:
        return False
    return bool(zca * zb <= 0)


def freeze_shadows(rows: list, *, lane_results: dict) -> dict:
    """Freeze the credible survivors BEFORE their future exists."""
    import json
    eligible, declined = [], []
    for r in rows:
        req = C.FREEZE_REQUIRES
        checks = {
            "zone_b_t": bool((r.get("T_STAT") or 0)
                             >= req["zone_b_excess_t_hac_min"]),
            "positive_on_committed_capital": bool(
                (r.get("NET_RESIDUAL_ALPHA") or 0) > 0),
            "positive_at_2x_cost": bool(
                (r.get("_gate_checks") or {}).get("positive_at_2x_cost")),
            "survives_full_kill_battery":
                r.get("ROBUSTNESS") == "SURVIVES_FULL_BATTERY",
            "pit_true": r.get("PIT_STATUS") == "PIT_TRUE",
            # POST-FREEZE TIGHTENING, DISCLOSED. FREEZE_REQUIRES did not
            # name the lockbox, because the lockbox is opened AFTER the
            # freeze rule was written. Freezing a candidate the lockbox has
            # already refuted would contradict the contract's own standard
            # for a shadow - "sufficient historical credibility to justify
            # future evidence collection" - so refutation is enforced here.
            # This can only DECLINE a freeze, never grant one.
            "zone_c_not_refuted": not _zone_c_refuted(r),
            "increment_over_passive_not_zero": bool(
                r.get("_increment_t") is None or r["_increment_t"] >= 2.0),
        }
        if all(checks.values()):
            eligible.append((r, checks))
        else:
            declined.append({"candidate_id": r["CANDIDATE_ID"],
                             "checks": checks,
                             "reason": "; ".join(k for k, v in checks.items()
                                                 if not v)})
    eligible = eligible[:C.MAX_NEW_SHADOWS]
    frozen_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
    shadows = {}
    for r, checks in eligible:
        sid = "R43_%s" % r["CANDIDATE_ID"].upper()
        shadows[sid] = {
            "shadow_id": sid,
            "candidate_id": r["CANDIDATE_ID"],
            "state": C.SHADOW_STATE,
            "promotion_allowed": C.PROMOTION_ALLOWED,
            "frozen_at": frozen_at,
            "first_eligible_decision_date": str(
                (_dt.date.today() + _dt.timedelta(days=1)).isoformat()),
            "rule": {
                "lane": r["LANE"],
                "information_family": r["INFORMATION_FAMILY"],
                "economic_expression": r["ECONOMIC_EXPRESSION"],
                "horizon": r["HORIZON"], "model": r["MODEL"],
                "asset": r["ASSET"],
            },
            "historical_qualification": r["QUALIFICATION_STATE"],
            "historical_zone_b_excess_ann": r["NET_RESIDUAL_ALPHA"],
            "historical_zone_b_t": r["T_STAT"],
            "committed_capital": r["COMMITTED_CAPITAL"],
            "collateral_class": "REMUNERATED_MARGIN",
            # The distinction R42 taught the estate.
            "paired_control": {
                "control": "VOLATILITY_MATCHED_PASSIVE_LONG_SAME_STRUCTURES",
                "historical_increment_ann": r.get("_increment_ann"),
                "historical_increment_t": r.get("_increment_t"),
                "what_the_future_tests": "whether the SIGNAL's increment "
                                         "over the passive exposure is "
                                         "positive - freezing the timing "
                                         "rule alone would lose exactly the "
                                         "distinction that killed R42",
            },
            "freeze_checks": checks,
            "never_backfilled": C.NEVER_BACKFILL_PROSPECTIVE_ROWS,
            "parameters_immutable": C.NEVER_CHANGE_FROZEN_CANDIDATE_PARAMETERS,
            "rows_captured": 0,
        }
    body = {"schema": "r43_shadow_registry/1", "campaign_id": CAMPAIGN_ID,
            "frozen_at": frozen_at, "max_new_shadows": C.MAX_NEW_SHADOWS,
            "n_frozen": len(shadows), "shadows": shadows,
            "declined": declined,
            "prior_release_shadows_mutated": False,
            "promotion_allowed": False}
    body["registry_hash"] = sha(body)
    p = campaign_dir(CAMPAIGN_ID) / SHADOW_REGISTRY
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(body, indent=1, sort_keys=True, default=str),
                 encoding="utf-8")
    return body


# --------------------------------------------------------------------------- #
# Track S - near-real-time readiness
# --------------------------------------------------------------------------- #
def readiness(rows: list) -> dict:
    """For every surviving candidate: what it reads, how often, how stale it
    may be, and what triggers a reassessment."""
    out = []
    for r in rows:
        if not r.get("FORWARD_READY"):
            continue
        fam = r["INFORMATION_FAMILY"]
        src = {
            "RATES_RV": ("Norgate dated futures settlements",
                         "daily, same evening (NDU hourly sync)"),
            "COMMODITY_CURVE": ("Norgate dated futures settlements",
                                "daily, same evening"),
            "FX": ("Norgate dated FX futures settlements", "daily"),
            "CROSS_ASSET": ("Norgate futures + FRED daily panel",
                            "daily; FRED next-day"),
            "EVENT_DRIVEN": ("FRED release calendar + futures settlements",
                             "per scheduled release"),
            "EQUITY_RESIDUAL": ("Norgate US equities (TOTALRETURN)", "daily"),
            "TECHNICAL_STRUCTURE": ("Norgate futures settlements", "daily"),
        }.get(fam, ("owned panel", "daily"))
        out.append({
            "candidate_id": r["CANDIDATE_ID"],
            "source": src[0], "source_refresh_cadence": src[1],
            "observable_timestamp": "the session's settlement",
            "max_safe_staleness": "one session",
            "feature_refresh_latency": "seconds (the panels are local)",
            "scoring_latency": "seconds",
            "material_update_trigger": "a new settlement for any market in "
                                       "the book, or a scheduled release "
                                       "for the event lane",
            "decision_cadence": r["HORIZON"],
            "portfolio_reassessment_trigger":
                "after the signal refresh, per the canonical sequence",
            "execution_automated": False,
        })
    return {"flow": ("MATERIAL INFORMATION UPDATE -> INCREMENTAL FEATURE "
                     "REFRESH -> SIGNAL REFRESH -> OPPORTUNITY FRONTIER "
                     "UPDATE -> FULL PORTFOLIO REASSESSMENT"),
            "execution_automated": False,
            "orders": 0,
            "candidates": out}
