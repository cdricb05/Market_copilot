"""alpha_agent.r64.challenger - immutable governed research candidates.

A cell that survives the R64 ladder becomes an immutable RESEARCH ARTIFACT
carrying every field the R64 result standard demands and a
``freeze_record_hash`` over its complete FORWARD SPECIFICATION (signal,
universe, horizon, cadence, construction, costs, emission rule), so a later
human-gated registration through the existing canonical owners
(``api.forward_challenger_registry`` via the R61/R62 adoption path) can bind to
exactly this specification. R64 never calls those owners, never promotes,
never adopts, never registers, never changes a holding.

Classification (protocol section "challenger_rules"):

    READY_FOR_FORWARD_QUALIFICATION   ECONOMIC_UNDER_CONTROLS, positive net
                                      increment at 2x cost, lockbox sign
                                      agreement, market-observable information
                                      and (multi-horizon family) family Holm
                                      p below alpha
    MORE_RESEARCH_REQUIRED            conditional t >= 2 and DISTINCT but one
                                      of economics-under-controls / stability
                                      / FDR fails
    REJECTED                          everything else, with the binding failure
"""
from __future__ import annotations

import json

from alpha_agent.r63 import ontology as ONT

from . import (CARRY_FAMILY, CARRY_VARIANTS, CH_MORE, CH_READY, CH_REJECTED,
               CONDITIONAL_T_FLOOR, DEGENERATE_DD, HOLM_ALPHA, LEVERAGE_AT_CAP_SHARE_MAX,
               LOCKBOX_START, PARTIAL_RESIDUAL_SHARE_MAX, V_ECON, V_NOT_ECON, V_NOT_FDR,
               V_UNSTABLE, protocol_hash, research_root, stable_hash, write_artifact)
from . import construction as B
from . import family as FAM

CALCULATION_OWNER = "alpha_agent.r64.challenger"
SCHEMA = "r64_challenger_candidates/1"
RECORD_SCHEMA = "r64_challenger_candidate/1"
ARTIFACT_NAME = "r64_challenger_candidates.json"
PIT_QUALITY = "PIT_MARKET_OBSERVABLE"   # every R64 cell reads exchange settlements

FEATURE_TEXT = {
    "CARRY": "slope_ann (annualised log ratio of the second to the front dated settlement) "
             "and its 21-session change",
    "CARRY_TO_RISK": "slope_ann divided by the trailing 63-session realised volatility of "
                     "the front contract return, and its 21-session change",
    "CARRY_CLASS_NEUTRAL": "slope_ann standardised within its asset class on each session "
                           "(cross-sectional z-score within class), and its 21-session change",
}


def binding_failure(cell: dict) -> str:
    c, e, s, r = (cell.get("conditional") or {}), (cell.get("r64_economics") or {}), \
        (cell.get("stability") or {}), (cell.get("redundancy") or {})
    aug = e.get("augmented") or {}
    if aug.get("degenerate_under_controls"):
        return ("DEGENERATE_UNDER_CONTROLS (max drawdown %.2f against %.2f; leverage at the cap "
                "on %.0f%% of periods against %.0f%%)"
                % (aug.get("max_dd") or 0.0, DEGENERATE_DD,
                   100.0 * (aug.get("share_periods_leverage_at_cap") or 0.0),
                   100.0 * LEVERAGE_AT_CAP_SHARE_MAX))
    if c.get("lockbox_sign_agrees") is False:
        return "LOCKBOX_SIGN_FLIP"
    if (s.get("share_blocks_positive") or 0) < 0.6:
        return "REGIME_INSTABILITY (share of positive 3-year blocks %.2f)" % (
            s.get("share_blocks_positive") or 0)
    if (e.get("ann_net_increment") or 0) < 0:
        return "COSTS_DESTROY_INCREMENT"
    if cell.get("r64_verdict") == V_NOT_ECON:
        return "NOT_ECONOMIC_UNDER_CONTROLS (net increment %.4f/yr, Sharpe increment %.3f)" % (
            e.get("ann_net_increment") or 0.0, e.get("sharpe_increment") or 0.0)
    if r.get("redundancy") == "PARTIALLY_REDUNDANT":
        return "PARTIAL_REDUNDANCY_WITH_BASELINE"
    if not cell.get("fdr_pass_conditional"):
        return "MULTIPLE_TESTING (%s)" % (cell.get("fdr_family") or "no family")
    return "EFFECTIVE_SAMPLE"


def classify(cell: dict, family: dict | None = None) -> tuple:
    v = cell.get("r64_verdict")
    c, e, r = cell.get("conditional") or {}, cell.get("r64_economics") or {}, \
        cell.get("redundancy") or {}
    if v == V_ECON:
        two_x = e.get("ann_net_increment_at_2x_cost")
        fam_ok = family is None or family.get("verdict") == FAM.F_SURVIVES
        if (two_x is not None and two_x > 0) and c.get("lockbox_sign_agrees") is not False \
                and fam_ok:
            return CH_READY, ("every gate passed under the risk-controlled book; net increment "
                              "positive at 2x cost; lockbox sign agrees%s"
                              % ("; family Holm p %.4g below %.2f"
                                 % (family.get("family_p_conditional"), HOLM_ALPHA)
                                 if family else ""))
        return CH_MORE, ("economic under controls but the 2x-cost, lockbox-sign or family "
                         "Holm check failed")
    t, rs = c.get("t"), r.get("residual_share")
    if t is not None and t >= CONDITIONAL_T_FLOOR and rs is not None \
            and rs >= PARTIAL_RESIDUAL_SHARE_MAX and v in (V_NOT_ECON, V_UNSTABLE, V_NOT_FDR):
        return CH_MORE, "conditional t=%.2f, DISTINCT (residual share %.2f), failed %s" % (t, rs, v)
    return CH_REJECTED, "verdict %s" % v


def forward_specification(cell: dict) -> dict:
    """The complete, hashable specification a later governed forward
    registration would freeze. Nothing here is executed."""
    dim = cell.get("dimension")
    e = cell.get("r64_economics") or {}
    pol = ((e.get("augmented") or {}).get("policy")) or B.default_policy()
    return {
        "schema": "r64_forward_specification/1",
        "cell_id": cell.get("cell_id"), "scope": cell.get("scope"), "mode": cell.get("mode"),
        "horizon_sessions": int(cell.get("horizon") or 0), "cadence_sessions": cell.get("cadence"),
        "universe": {"instruments": cell.get("instruments"),
                     "rule": "R63 universe rule over the R41 dated-contract store (>= 2000 sessions, "
                             "a bar on/after 2026-08-01, declared exclusions only)"},
        "signal": {"baseline_dimensions": cell.get("baseline"), "added_dimension": dim,
                   "added_feature": FEATURE_TEXT.get(dim, dim), "kind": cell.get("kind"),
                   "model": "ridge on training-standardised features; penalty chosen by blocked "
                            "inner CV on the baseline arm and forced on the augmented arm; "
                            "refit on the R63 yearly expanding folds"},
        "construction": {k: pol.get(k) for k in sorted(pol)},
        "costs": {"per_side_bps_by_instrument": cell.get("cost_bps_per_side"),
                  "rule": "R38 measured cost per market; 15bp where unmeasured; charged on one-way "
                          "turnover"},
        "emission_rule": ("a decision is formed at close t from data dated <= t and is emitted "
                          "STRICTLY BEFORE session t+1; the position is effective at close t+1 "
                          "(NEXT_CLOSE); no backfill"),
        "lockbox_start": LOCKBOX_START,
        "pit_quality": PIT_QUALITY,
        "protocol_sha256": protocol_hash(),
    }


def record(cell: dict, state: str, why: str, family: dict | None = None) -> dict:
    c, e, s, r, sec = (cell.get("conditional") or {}), (cell.get("r64_economics") or {}), \
        (cell.get("stability") or {}), (cell.get("redundancy") or {}), (cell.get("secondary") or {})
    aug, basel = e.get("augmented") or {}, e.get("baseline") or {}
    dim = cell.get("dimension")
    onto = ONT.dimension(cell.get("base_dimension") or dim)
    spec = forward_specification(cell)
    fam_rows = (family or {}).get("rows") or []
    body = {
        "schema": RECORD_SCHEMA,
        "challenger_id": "R64_%s_%s_H%d_%s" % (cell["scope"], dim, int(cell["horizon"]),
                                              stable_hash(cell["cell_id"])[:8].upper()),
        "cell_id": cell["cell_id"], "classification": state, "why": why,
        "hypothesis": ("%s carries incremental %s information for %s at a %d-session horizon, "
                       "conditional on %s, and the increment survives a bounded risk-controlled "
                       "book" % (dim, cell.get("mode"), cell["scope"], int(cell["horizon"]),
                                 ", ".join(cell.get("baseline") or []))),
        "mechanism": onto["latent_state"],
        "information_class": onto["information_class"],
        "information_dimensions": [cell.get("base_dimension") or dim],
        "construction_variant": dim,
        "economic_family": CARRY_FAMILY if dim in CARRY_VARIANTS else dim,
        "asset_class": cell["scope"], "horizon_sessions": int(cell["horizon"]),
        "cadence_sessions": cell.get("cadence"), "mode": cell.get("mode"),
        "universe": {"instruments": cell.get("instruments"), "count": cell.get("n_instruments")},
        "pit_source": "R41 dated-contract curve store (Norgate dated futures), exchange settlements "
                      "on their own session",
        "pit_quality": PIT_QUALITY,
        "baseline": cell.get("baseline"),
        "incremental_oos_result": {"increment": c.get("increment"), "t": c.get("t"),
                                   "p_one_sided": c.get("p_one_sided"),
                                   "selection": c.get("increment_selection"),
                                   "lockbox": c.get("increment_lockbox"),
                                   "t_lockbox": c.get("t_lockbox"),
                                   "partial_rank_ic_t": sec.get("partial_t"),
                                   "permutation_drop": sec.get("permutation_drop"),
                                   "oos_r2_increment": sec.get("oos_r2_increment_mean")},
        "conditional_information_value": {"residual_share": r.get("residual_share"),
                                          "redundancy": r.get("redundancy"),
                                          "max_abs_rank_corr_with_baseline": r.get("max_abs_rank_corr")},
        "multiple_testing": {"fdr_pass_conditional": cell.get("fdr_pass_conditional"),
                             "fdr_family": cell.get("fdr_family"),
                             "fdr_pass_conditional_r64_campaign": cell.get("fdr_pass_conditional_r64_campaign"),
                             "fdr_pass_economic_r64_campaign": cell.get("fdr_pass_economic_r64_campaign"),
                             "family_holm_p_conditional": (family or {}).get("family_p_conditional"),
                             "family_holm_p_economic": (family or {}).get("family_p_economic"),
                             "family_verdict": (family or {}).get("verdict"),
                             "family_rows": [{k: row.get(k) for k in
                                              ("cell_id", "conditional_p_holm", "economic_p_holm",
                                               "r64_verdict")} for row in fam_rows]},
        "net_return": {"augmented_ann_net": aug.get("ann_net"), "baseline_ann_net": basel.get("ann_net"),
                       "ann_net_increment": e.get("ann_net_increment"),
                       "ann_net_increment_at_2x_cost": e.get("ann_net_increment_at_2x_cost"),
                       "t_increment": e.get("t_increment"),
                       "p_increment_one_sided": e.get("p_increment_one_sided"),
                       "sharpe_augmented": aug.get("sharpe"), "sharpe_baseline": basel.get("sharpe"),
                       "sharpe_increment": e.get("sharpe_increment")},
        "turnover": {"augmented_mean_oneway": aug.get("mean_oneway_turnover"),
                     "baseline_mean_oneway": basel.get("mean_oneway_turnover")},
        "costs": {"augmented_ann_cost_drag": aug.get("ann_cost_drag"),
                  "baseline_ann_cost_drag": basel.get("ann_cost_drag"),
                  "per_side_bps_by_instrument": cell.get("cost_bps_per_side")},
        "volatility": {"augmented_ann_vol": aug.get("ann_vol"), "baseline_ann_vol": basel.get("ann_vol"),
                       "target": (aug.get("policy") or {}).get("target_vol")},
        "drawdown": {"augmented_max_dd": aug.get("max_dd"), "baseline_max_dd": basel.get("max_dd")},
        "concentration": {"max_weight": aug.get("max_weight"),
                          "mean_gross_leverage": aug.get("mean_gross_leverage"),
                          "max_gross_leverage": aug.get("max_gross_leverage"),
                          "share_periods_leverage_at_cap": aug.get("share_periods_leverage_at_cap"),
                          "max_class_share": aug.get("max_class_share")},
        "robustness": {"share_blocks_positive": s.get("share_blocks_positive"),
                       "blocks": s.get("blocks"), "regimes": s.get("regime"),
                       "lockbox_sign_agrees": c.get("lockbox_sign_agrees"),
                       "share_instruments_positive": s.get("share_instruments_positive"),
                       "reproduction_matches_r63": (cell.get("reproduction") or {}).get("matches")},
        "sample": {"periods": cell.get("n_periods"), "effective_periods": cell.get("effective_periods"),
                   "selection_periods": cell.get("n_selection_periods"),
                   "lockbox_periods": cell.get("n_lockbox_periods"), "coverage": cell.get("coverage")},
        "r63_verdict": cell.get("verdict"), "r64_verdict": cell.get("r64_verdict"),
        "binding_failure": None if state == CH_READY else binding_failure(cell),
        "forward_specification": spec,
        "freeze_record_hash": stable_hash(spec),
        "forward_qualification_recommendation": (
            "eligible for a HUMAN-gated registration through api.forward_challenger_registry "
            "via the governed R61/R62 adoption path, binding freeze_record_hash" if state == CH_READY
            else "do not freeze; re-test only with the named binding failure addressed" if state == CH_MORE
            else "do not freeze"),
        "promotion_allowed": False, "live_registration_performed": False,
        "holdings_changed": False, "records_are_immutable": True,
    }
    body["record_hash"] = stable_hash(body)
    return body


def build(cells: list, family: dict | None = None) -> dict:
    from .experiments import TAG_REPRODUCTION
    scored = [c for c in cells if c.get("conditional")]
    ready, more, rejected = [], [], []
    for c in scored:
        fam = family if (c.get("scope") == "FX_FUTURES" and c.get("dimension") == "CARRY"
                         and c.get("r64_tag") == TAG_REPRODUCTION) else None
        state, why = classify(c, fam)
        if state == CH_READY:
            ready.append(record(c, state, why, fam))
        elif state == CH_MORE:
            more.append(record(c, state, why, fam))
        else:
            rejected.append({"cell_id": c["cell_id"], "why": why,
                             "t": (c.get("conditional") or {}).get("t"),
                             "r64_verdict": c.get("r64_verdict"),
                             "binding_failure": binding_failure(c)})
    d = research_root() / "challengers"
    d.mkdir(parents=True, exist_ok=True)
    for rec in ready + more:
        # Immutable AND never stale: the file name carries the record hash, so a
        # re-run whose economics changed writes a NEW file and the earlier record
        # survives untouched (the R63 write-once rule left a first-pass file
        # standing next to a regenerated candidates artifact; this cannot).
        p = d / ("%s_%s.json" % (rec["challenger_id"], rec["record_hash"][:12]))
        rec["record_file"] = p.name
        if not p.exists():
            p.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str), encoding="utf-8")
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "n_scored_cells": len(scored),
            "counts": {CH_READY: len(ready), CH_MORE: len(more), CH_REJECTED: len(rejected)},
            "ready_for_forward_qualification": ready,
            "more_research_required": sorted(more, key=lambda r: -(r["incremental_oos_result"].get("t") or -99)),
            "rejected": sorted(rejected, key=lambda r: -(r["t"] or -99)),
            "fx_carry_family": {k: (family or {}).get(k) for k in
                                ("family_id", "verdict", "family_p_conditional", "family_p_economic",
                                 "n_cells")},
            "promotion_performed": False, "live_registration_performed": False,
            "registration_owner_not_called": "api.forward_challenger_registry",
            "artifact_dir": str(d)}
    write_artifact(ARTIFACT_NAME, body)
    return body
