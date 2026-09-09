"""alpha_agent.r63.challengers - research-only challenger candidates.

A cell that survives PIT, OOS, multiple-testing control, costs, robustness,
effective sample and orthogonality becomes an immutable RESEARCH ARTIFACT
capable of later entering the governed forward process through the existing
R61/R62 owners. R63 never calls those owners, never promotes, never adopts,
never registers, never changes a holding.

Classification (protocol section "challenger_rules"):

    READY_FOR_FORWARD_QUALIFICATION   INCREMENTAL_INFORMATION_CANDIDATE with
                                      a positive net increment at 2x cost,
                                      lockbox sign agreement and credible PIT
    MORE_RESEARCH_REQUIRED            conditional t >= 2 and DISTINCT but one
                                      of economics / stability / FDR / PIT
    REJECTED                          everything else (recorded, with why)
"""
from __future__ import annotations

import json

from . import (CH_MORE, CH_READY, CH_REJECTED, CONDITIONAL_T_FLOOR,
               PARTIAL_RESIDUAL_SHARE_MAX, research_root, stable_hash,
               write_artifact)
from . import ontology as ONT
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.challengers"
SCHEMA = "r63_challenger_candidates/1"
ARTIFACT_NAME = "r63_challenger_candidates.json"

PIT_BY_DIMENSION = {
    "INSIDER_BEHAVIOUR": "PIT_TRUE", "DISCLOSURE_INTENSITY_LANGUAGE": "PIT_TRUE",
    "EVENT_INFORMATION": "PIT_TRUE", "CORPORATE_DISCLOSURES": "PIT_TRUE",
    "FUNDAMENTAL_CHANGE": "PIT_TRUE", "FUNDAMENTAL_LEVELS": "PIT_TRUE", "FREE_CASH_FLOW": "PIT_TRUE",
    "POSITIONING_COMMITMENTS": "PIT_BY_DECLARED_LAG", "INVENTORY": "PIT_BY_DECLARED_LAG",
    "MACRO_LEVELS": "PIT_TRUE", "MACRO_CHANGE": "PIT_TRUE", "MACRO_SURPRISES": "PIT_TRUE",
    "FUNDING_LIQUIDITY_CONDITIONS": "PIT_TRUE",
}


def _pit(dim: str) -> str:
    return PIT_BY_DIMENSION.get(dim, "PIT_MARKET_OBSERVABLE")


def classify(cell: dict) -> tuple:
    v = cell.get("verdict")
    c, e, r = cell.get("conditional") or {}, cell.get("economics") or {}, cell.get("redundancy") or {}
    if v == S.V_CANDIDATE:
        two_x = e.get("ann_net_increment_at_2x_cost")
        if (two_x is not None and two_x > 0) and c.get("lockbox_sign_agrees") is not False:
            return CH_READY, "every gate passed; net increment positive at 2x cost; lockbox sign agrees"
        return CH_MORE, "candidate on every gate but the 2x-cost or lockbox-sign check"
    t = c.get("t")
    rs = r.get("residual_share")
    if t is not None and t >= CONDITIONAL_T_FLOOR and rs is not None and rs >= PARTIAL_RESIDUAL_SHARE_MAX \
            and v in (S.V_NOT_ECON, S.V_UNSTABLE, S.V_NOT_FDR):
        return CH_MORE, "conditional t=%.2f, DISTINCT (residual share %.2f), failed %s" % (t, rs, v)
    return CH_REJECTED, "verdict %s" % v


def strongest_failure_mode(cell: dict) -> str:
    c, e, s, r = (cell.get("conditional") or {}), (cell.get("economics") or {}), \
        (cell.get("stability") or {}), (cell.get("redundancy") or {})
    aug = e.get("augmented") or {}
    if (aug.get("max_dd") is not None and aug["max_dd"] < S.DEGENERATE_DD) or \
            (e.get("book") == "XS_LONG_SHORT" and aug.get("max_weight") is not None
             and aug["max_weight"] > 0.5 + 1e-9):
        return "BOOK_DEGENERATE (max weight %.2f, max drawdown %.2f)" % (
            aug.get("max_weight") or 0.0, aug.get("max_dd") or 0.0)
    if c.get("lockbox_sign_agrees") is False:
        return "LOCKBOX_SIGN_FLIP"
    if (s.get("share_blocks_positive") or 0) < 0.6:
        return "REGIME_INSTABILITY (share of positive 3-year blocks %.2f)" % (s.get("share_blocks_positive") or 0)
    if (e.get("ann_net_increment") or 0) < 0:
        return "COSTS_DESTROY_INCREMENT"
    if r.get("redundancy") == "PARTIALLY_REDUNDANT":
        return "PARTIAL_REDUNDANCY_WITH_BASELINE"
    if not cell.get("fdr_pass_conditional"):
        return "MULTIPLE_TESTING"
    return "EFFECTIVE_SAMPLE"


def record(cell: dict, state: str, why: str) -> dict:
    c, e, s, r, sec = (cell.get("conditional") or {}), (cell.get("economics") or {}), \
        (cell.get("stability") or {}), (cell.get("redundancy") or {}), (cell.get("secondary") or {})
    dim = ONT.dimension(cell["dimension"])
    body = {
        "schema": "r63_challenger_candidate/1",
        "challenger_id": "R63_%s_%s_H%d_%s" % (cell["scope"], cell["dimension"], int(cell["horizon"]),
                                              stable_hash(cell["cell_id"])[:8].upper()),
        "cell_id": cell["cell_id"], "classification": state, "why": why,
        "hypothesis": "%s carries incremental %s information for %s at a %d-session horizon, "
                      "conditional on %s" % (cell["dimension"], cell.get("mode"), cell["scope"],
                                             int(cell["horizon"]), ", ".join(cell.get("baseline") or [])),
        "economic_mechanism": dim["latent_state"],
        "information_dimensions": [cell["dimension"]], "information_class": dim["information_class"],
        "asset_class": cell["scope"], "mode": cell.get("mode"),
        "instruments": cell.get("n_instruments"), "horizon_sessions": int(cell["horizon"]),
        "cadence_sessions": cell.get("cadence"),
        "baseline": cell.get("baseline"), "added_information": cell["dimension"], "kind": cell.get("kind"),
        "oos_improvement": {"increment": c.get("increment"), "t": c.get("t"), "p_one_sided": c.get("p_one_sided"),
                            "selection": c.get("increment_selection"), "lockbox": c.get("increment_lockbox"),
                            "partial_rank_ic_t": sec.get("partial_t"), "permutation_drop": sec.get("permutation_drop"),
                            "oos_r2_increment": sec.get("oos_r2_increment_mean")},
        "net_improvement": {"ann_net_increment": e.get("ann_net_increment"),
                            "ann_net_increment_at_2x_cost": e.get("ann_net_increment_at_2x_cost"),
                            "sharpe_increment": e.get("sharpe_increment"), "t": e.get("t_increment"),
                            "book": e.get("book")},
        "turnover": (e.get("augmented") or {}).get("mean_oneway_turnover"),
        "risk": {"max_dd_augmented": (e.get("augmented") or {}).get("max_dd"),
                 "max_dd_baseline": (e.get("baseline") or {}).get("max_dd"),
                 "max_weight": (e.get("augmented") or {}).get("max_weight")},
        "sample": {"periods": cell.get("n_periods"), "effective_periods": cell.get("effective_periods"),
                   "coverage": cell.get("coverage"), "selection_periods": cell.get("n_selection_periods"),
                   "lockbox_periods": cell.get("n_lockbox_periods")},
        "regimes": s.get("regime"), "blocks": s.get("blocks"),
        "sensitivity": {"share_blocks_positive": s.get("share_blocks_positive"),
                        "share_instruments_positive": s.get("share_instruments_positive"),
                        "lockbox_sign_agrees": c.get("lockbox_sign_agrees")},
        "fdr": {"conditional_pass": cell.get("fdr_pass_conditional"),
                "economic_pass": cell.get("fdr_pass_economic")},
        "orthogonality": {"residual_share": r.get("residual_share"), "label": r.get("redundancy"),
                          "max_abs_rank_corr_with_baseline": r.get("max_abs_rank_corr")},
        "pit_quality": _pit(cell["dimension"]),
        "strongest_failure_mode": strongest_failure_mode(cell),
        "forward_qualification_recommendation": (
            "freeze through the governed R61/R62 owners after human review" if state == CH_READY else
            "do not freeze; re-test only with the named failure addressed" if state == CH_MORE else
            "do not freeze"),
        "promotion_allowed": False, "live_registration_performed": False,
        "holdings_changed": False, "records_are_immutable": True,
    }
    body["record_hash"] = stable_hash(body)
    return body


def build(cells: list) -> dict:
    scored = [c for c in cells if c.get("conditional")]
    ready, more, rejected = [], [], []
    for c in scored:
        state, why = classify(c)
        if state == CH_READY:
            ready.append(record(c, state, why))
        elif state == CH_MORE:
            more.append(record(c, state, why))
        elif (c["conditional"].get("t") or -99) >= 1.5:
            rejected.append({"cell_id": c["cell_id"], "why": why,
                             "t": c["conditional"].get("t"),
                             "strongest_failure_mode": strongest_failure_mode(c)})
    d = research_root() / "challengers"
    d.mkdir(parents=True, exist_ok=True)
    for rec in ready + more:
        p = d / ("%s.json" % rec["challenger_id"])
        if not p.exists():
            p.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str), encoding="utf-8")
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "n_scored_cells": len(scored),
            "counts": {CH_READY: len(ready), CH_MORE: len(more), CH_REJECTED: len(scored) - len(ready) - len(more)},
            "ready_for_forward_qualification": ready,
            "more_research_required": sorted(more, key=lambda r: -(r["oos_improvement"].get("t") or -99))[:40],
            "rejected_of_note": sorted(rejected, key=lambda r: -(r["t"] or -99))[:60],
            "promotion_performed": False, "live_registration_performed": False,
            "artifact_dir": str(d)}
    write_artifact(ARTIFACT_NAME, body)
    return body
