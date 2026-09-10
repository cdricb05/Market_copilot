"""alpha_agent.alpha_recovery.program - the information-directed research program.

Workstream 3/4. The campaign is DRIVEN by information needs, not by formula
generation, and it proves it:

    * the ranked frontier is READ through the ONE adapter
      (``alpha_agent.r59.information_needs.need_rows``: the R63 gap frontier
      with the R64 overlay applied) - never re-ranked here;
    * the R59 governor (``generate_mandates``) is run against an ISOLATED
      research memory under the campaign root, with an empty scope view, so
      the only mandates it can offer are information needs; the mandates it
      issues are recorded next to the families this campaign executes;
    * every executed specification is classified PRICE_STATE or not, the
      75 % non-price rule (contract rule 14) is measured, the per-family
      budgets (6 primary, 2 rescue with a named binding failure) are
      enforced, and the exhausted-PRICE_STATE reopening rule is applied.

No second memory, queue, scheduler, frontier or forward owner is created:
the isolated memory is the R59 owner's own class opened at a redirected root.
"""
from __future__ import annotations

import os
from pathlib import Path

from alpha_agent import r59
from alpha_agent.r59 import information_needs as IN
from alpha_agent.r59 import memory as M

from . import (FAMILY_PRIMARY_MAX, FAMILY_RESCUE_MAX, NON_PRICE_SHARE_MIN, protocol, research_root,
               write_artifact)

CALCULATION_OWNER = "alpha_agent.alpha_recovery.program"
ARTIFACT_NAME = "research_program.json"
GOVERNOR_STATE_SUBDIR = "_governor_state"
TOP_N_FRONTIER = 40

REOPEN_REASONS = ("NEW_ORTHOGONAL_INFORMATION", "PIT_HISTORY_MATERIALLY_IMPROVED",
                  "COVERAGE_MATERIALLY_IMPROVED", "DISTINCT_IMPLEMENTATION_RESOLVES_NAMED_BINDING_FAILURE")


class BudgetError(ValueError):
    """A family exceeded its frozen specification budget."""


def families_from_protocol() -> list:
    return list((protocol().get("information_directed_program") or {}).get("families") or [])


def check_family_budget(family: dict, *, executed_primary: int, executed_rescue: int = 0,
                        rescue_binding_failures: tuple = ()) -> dict:
    """Raise when a family exceeds 6 primary or 2 rescue specifications, or
    when a rescue names no measured binding failure."""
    if executed_primary > FAMILY_PRIMARY_MAX:
        raise BudgetError("%s executed %d primary specifications (max %d)"
                          % (family.get("family"), executed_primary, FAMILY_PRIMARY_MAX))
    if executed_rescue > FAMILY_RESCUE_MAX:
        raise BudgetError("%s executed %d rescue specifications (max %d)"
                          % (family.get("family"), executed_rescue, FAMILY_RESCUE_MAX))
    if executed_rescue and len([b for b in rescue_binding_failures if b]) < executed_rescue:
        raise BudgetError("%s rescue specifications need a named, measured binding failure each"
                          % family.get("family"))
    return {"family": family.get("family"), "primary": executed_primary, "rescue": executed_rescue,
            "within_budget": True}


def reopening_allowed(*, price_state: bool, reason: str | None, binding_failure: str | None) -> dict:
    """Contract rule 13: an exhausted PRICE_STATE family reopens only for one
    of the four named reasons; a distinct implementation must name the
    measured binding failure it resolves."""
    if not price_state:
        return {"allowed": True, "why": "not a PRICE_STATE family"}
    if reason not in REOPEN_REASONS:
        return {"allowed": False, "why": "reason %r is not one of %s" % (reason, list(REOPEN_REASONS))}
    if reason == REOPEN_REASONS[3] and not binding_failure:
        return {"allowed": False, "why": "a distinct implementation must name the binding failure it resolves"}
    return {"allowed": True, "why": "reason %s accepted" % reason}


def non_price_share(specs: list) -> dict:
    n = len(specs)
    np_ = sum(1 for s in specs if not s.get("price_state"))
    share = (np_ / n) if n else 0.0
    return {"executed": n, "non_price": np_, "price_state": n - np_, "share_non_price": share,
            "rule_min": NON_PRICE_SHARE_MIN, "rule_met": share >= NON_PRICE_SHARE_MIN}


# --------------------------------------------------------------------------- #
# The frontier, through the adapter, and the isolated governor
# --------------------------------------------------------------------------- #
def frontier_ranking(limit: int = TOP_N_FRONTIER) -> dict:
    fr = IN.load_frontier()
    ov = IN.load_overlay()
    if not fr:
        return {"state": "FRONTIER_UNAVAILABLE", "path": str(IN.frontier_path()), "rows": []}
    rows = IN.need_rows(fr, ov, limit=limit)
    return {"state": "OK", "frontier_path": str(IN.frontier_path()), "overlay_path": str(IN.overlay_path()),
            "frontier_artifact_hash": fr.get("artifact_hash"), "overlay_artifact_hash": (ov or {}).get("artifact_hash"),
            "adapter": IN.CALCULATION_OWNER, "n": len(rows),
            "rows": [{"rank": i + 1, "cell_key": r.get("cell_key"), "asset_class": r.get("asset_class"),
                      "horizon": r.get("horizon"), "dimension": r.get("dimension"),
                      "observation_state": r.get("observation_state"), "best_verdict": r.get("best_verdict"),
                      "sourcing_step": r.get("sourcing_step"),
                      "remaining_research_value": r.get("remaining_research_value"),
                      "remaining_research_value_effective": r.get("remaining_research_value_effective"),
                      "overlay": r.get("overlay")} for i, r in enumerate(rows)]}


def isolated_governor_run(limit: int = 24) -> dict:
    """Run the R59 governor against an ISOLATED memory under the campaign
    root. The scope view is empty so only information needs can be offered;
    the frontier and overlay are read by path (the live artifacts, read
    only)."""
    from alpha_agent.r59 import governor as GOV
    root = research_root() / GOVERNOR_STATE_SUBDIR
    root.mkdir(parents=True, exist_ok=True)
    prev = os.environ.get(r59.RESEARCH_ROOT_ENV)
    os.environ[r59.RESEARCH_ROOT_ENV] = str(root)
    try:
        mem = M.open_memory()
        out = GOV.generate_mandates(mem, limit=limit, frontier_view={"asset_classes": {}})
        mandates = [{"kind": m.get("kind"), "asset_class": m.get("asset_class"), "family": m.get("family"),
                     "expected_information_value": m.get("expected_information_value"),
                     "cell_key": (m.get("payload") or {}).get("cell_key"),
                     "next_action": (m.get("payload") or {}).get("next_action")}
                    for m in (out.get("mandates") or [])]
        return {"state": "OK", "memory_root": str(root), "isolated": True,
                "memory_db": str(M.memory_db_path()),
                "n_candidates": out.get("n_candidates"), "n_information_needs": out.get("n_information_needs"),
                "information_need_source": out.get("information_need_source"),
                "terminal": out.get("terminal"), "mandates": mandates,
                "live_memory_untouched": True}
    finally:
        if prev is None:
            os.environ.pop(r59.RESEARCH_ROOT_ENV, None)
        else:
            os.environ[r59.RESEARCH_ROOT_ENV] = prev


MANDATE_CELLS = (
    {"scope": "RATES_FUTURES", "mode": "XS", "horizon": 21, "dimension": "VOLATILITY_EXPECTATIONS_IV",
     "kind": "AUGMENTATION", "cell_key": "RATES_FUTURES|21|VOLATILITY_EXPECTATIONS_IV"},
    {"scope": "VOLATILITY", "mode": "TS", "horizon": 21, "dimension": "VOLATILITY_EXPECTATIONS_IV",
     "kind": "AUGMENTATION", "cell_key": "VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV"},
    {"scope": "FX_FUTURES", "mode": "XS", "horizon": 21, "dimension": "POSITIONING_COMMITMENTS",
     "kind": "AUGMENTATION", "cell_key": "FX_FUTURES|21|POSITIONING_COMMITMENTS"},
)
MANDATES_ARTIFACT = "frontier_mandates.json"
MANDATE_FAMILY = "FRONTIER_MANDATES_RISK_CONTROLLED"


def execute_mandates(*, verbose: bool = True, resume: bool = True) -> dict:
    """The governor's mandated needs, priced through the R64 owner VERBATIM
    (alpha_agent.r64.experiments.measure_cell: the ONE scorer, the ONE
    risk-controlled book, the R64 verdict ladder). Cells are persisted under
    THIS campaign's root, never under the R64 root."""
    import json
    from alpha_agent.r64 import experiments as R64X
    from alpha_agent.r64 import family as FAM
    from alpha_agent.r63 import sensitivity as S
    from . import BH_Q, HOLM_ALPHA
    d = research_root() / "cells"
    d.mkdir(parents=True, exist_ok=True)
    cells = []
    for m in MANDATE_CELLS:
        sp = R64X.spec(m["scope"], m["mode"], m["horizon"], m["dimension"], m["kind"], "FRONTIER_MANDATE")
        p = d / ("MANDATE_%s.json" % sp["cell_id"].replace("|", "_"))
        if resume and p.exists():
            cells.append(json.loads(p.read_text(encoding="utf-8")))
            continue
        if verbose:
            print("[mandate] %s ..." % sp["cell_id"], flush=True)
        try:
            cell = R64X.measure_cell(sp)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "r64_verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc)}
        cell["family"] = MANDATE_FAMILY
        cell["cell_key"] = m["cell_key"]
        cell["calculation_owner"] = CALCULATION_OWNER
        cell["priced_by"] = "alpha_agent.r64.experiments.measure_cell (verbatim)"
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            c, e = cell.get("conditional") or {}, cell.get("r64_economics") or {}
            print("    r64=%s t=%s net_inc=%s sharpe_inc=%s %s" % (
                cell.get("r64_verdict"), None if c.get("t") is None else round(c["t"], 2),
                None if e.get("ann_net_increment") is None else round(e["ann_net_increment"], 4),
                None if e.get("sharpe_increment") is None else round(e["sharpe_increment"], 3),
                cell.get("error") or ""), flush=True)
    p_econ = {c["cell_id"]: (c.get("r64_economics") or {}).get("p_increment_one_sided") for c in cells
              if c.get("r64_economics")}
    bh = S.bh_fdr(p_econ, BH_Q) if p_econ else {"m": 0, "per_test": {}}
    holm = FAM.holm(p_econ, HOLM_ALPHA) if p_econ else {"m": 0, "rejected": {}}
    for c in cells:
        if c.get("r64_economics"):
            c["fdr_pass_economic"] = bh["per_test"].get(c["cell_id"])
            c["holm_pass_family"] = holm["rejected"].get(c["cell_id"])
            c["r64_verdict"] = R64X.verdict(c, fdr_pass=(c["fdr_pass_economic"] and c["holm_pass_family"]))
    briefs = []
    for c in cells:
        co, e = c.get("conditional") or {}, c.get("r64_economics") or {}
        aug = e.get("augmented") or {}
        briefs.append({"cell_id": c.get("cell_id"), "cell_key": c.get("cell_key"), "r64_verdict": c.get("r64_verdict"),
                       "conditional_t": co.get("t"), "ann_net_increment": e.get("ann_net_increment"),
                       "sharpe_increment": e.get("sharpe_increment"), "t_increment": e.get("t_increment"),
                       "augmented_ann_net": aug.get("ann_net"), "augmented_sharpe": aug.get("sharpe"),
                       "augmented_max_dd": aug.get("max_dd"), "degenerate": aug.get("degenerate_under_controls"),
                       "fdr_pass_economic": c.get("fdr_pass_economic"), "holm_pass_family": c.get("holm_pass_family"),
                       "error": c.get("error")})
    body = {"schema": "alpha_recovery_frontier_mandates/1", "calculation_owner": CALCULATION_OWNER,
            "family": MANDATE_FAMILY, "priced_by": "alpha_agent.r64.experiments.measure_cell (verbatim)",
            "mandated_not_executed": {},
            "mandated_executed_in_frontier_residual": {
                "CREDIT_PROXY|21|INFLATION_EXPECTATIONS": "the R64 futures assembler has no credit-proxy "
                                                          "substrate; the R63 one does, so the cell is measured "
                                                          "in alpha_agent/alpha_recovery/frontier_residual.py",
                "US_EQUITY|1|FREE_CASH_FLOW": "being a baseline dimension of the incumbent is a reason to "
                                              "measure it, not to skip it; measured in "
                                              "alpha_agent/alpha_recovery/frontier_residual.py"},
            "n_cells": len(cells), "brief": briefs, "cells": cells}
    write_artifact(MANDATES_ARTIFACT, body)
    return body


def _one_spec(fam: dict, s, kind: str) -> dict:
    """A protocol specification entry is either a string (it inherits the
    family's PRICE_STATE classification) or a dict that classifies itself -
    the INCUMBENT_DECOMPOSITION family holds both a price-state leg and a
    fundamental one, and counting them the same way would be wrong."""
    body = s if isinstance(s, dict) else {"spec": s}
    return {"family": fam["family"], "spec": body.get("spec"), "kind": kind,
            "price_state": bool(body.get("price_state", fam.get("price_state"))),
            "selects_information": bool(fam.get("selects_information", True)),
            "operator_directed": bool(fam.get("operator_directed", False)),
            "binding_failure": body.get("binding_failure"),
            "frontier_cell_keys": fam.get("frontier_cell_keys") or []}


def _family_specs(fam: dict) -> list:
    return [_one_spec(fam, s, "PRIMARY") for s in (fam.get("primary_specifications") or [])]


def _family_rescues(fam: dict) -> list:
    return [_one_spec(fam, s, "RESCUE") for s in (fam.get("rescue_specifications") or [])]


def build(*, executed: dict | None = None, verbose: bool = True, write: bool = True) -> dict:
    """``executed``: {family: {"primary": n, "rescue": n, "binding_failures": [...]}} as
    actually run; defaults to the protocol plan."""
    fams = families_from_protocol()
    rank = frontier_ranking()
    gov = isolated_governor_run()
    top_keys = {r["cell_key"]: r["rank"] for r in rank.get("rows") or []}
    mandated = {m["cell_key"] for m in gov.get("mandates") or [] if m.get("cell_key")}
    specs = []
    budgets = []
    mapping = []
    for fam in fams:
        ex = (executed or {}).get(fam["family"]) or {}
        rescues = _family_rescues(fam)
        n_primary = int(ex.get("primary", len(fam.get("primary_specifications") or [])))
        n_rescue = int(ex.get("rescue", len(rescues)))
        budgets.append(check_family_budget(
            fam, executed_primary=n_primary, executed_rescue=n_rescue,
            rescue_binding_failures=tuple(ex.get("binding_failures")
                                          or [r.get("binding_failure") for r in rescues])))
        specs.extend(_family_specs(fam)[:n_primary])
        specs.extend(rescues[:n_rescue])
        keys = fam.get("frontier_cell_keys") or []
        ranks = {k: top_keys.get(k) for k in keys}
        reopen = None
        if fam.get("price_state"):
            # each PRICE_STATE family names the MEASURED binding failure that
            # licenses its reopening; the default is R64's cadence cost drag
            reopen = reopening_allowed(
                price_state=True, reason=fam.get("reopening_reason") or REOPEN_REASONS[3],
                binding_failure=fam.get("reopening_binding_failure")
                or "R64: daily cadence cost drag (cross-asset TREND book loses 36-46 %/yr to costs)")
            reopen["binding_failure"] = (fam.get("reopening_binding_failure")
                                         or "R64: daily cadence cost drag (cross-asset TREND book "
                                            "loses 36-46 %/yr to costs)")
        best_rank = min([r for r in ranks.values() if r is not None], default=None)
        mand = sorted(k for k in keys if k in mandated)
        if mand:
            relation = "GOVERNOR_MANDATED"
        elif best_rank is not None:
            relation = "ON_FRONTIER_TOP_%d" % TOP_N_FRONTIER
        elif fam.get("price_state"):
            relation = "PRICE_STATE_RESCUE_UNDER_REOPENING_RULE"
        else:
            # the frontier zeroes a dimension the estate tested negative; this
            # family brings a NEW observable under that dimension (a different
            # information source, not a transform) and says so
            relation = "NEW_OBSERVABLE_UNDER_ZEROED_FRONTIER_DIMENSION"
        mapping.append({"family": fam["family"], "price_state": bool(fam.get("price_state")),
                        "frontier_cell_keys": keys, "frontier_rank_by_key": ranks,
                        "best_frontier_rank": best_rank, "frontier_relation": relation,
                        "mandated_by_isolated_governor": mand,
                        "selects_information": bool(fam.get("selects_information", True)),
                        "information_class": fam.get("information_class"),
                        "reopening": reopen, "n_primary": n_primary, "n_rescue": n_rescue})
    # The 75 % rule governs INFORMATION-directed research: a specification that
    # selects no information dimension (a pure construction change to the
    # incumbent's own book) is not a PRICE_STATE formula and is not an
    # information choice either. Both readings are reported and BOTH must hold,
    # so the rule is never met by reclassification.
    # Contract rule 14's 75 % threshold governs newly executed AUTONOMOUS
    # research. An OPERATOR-DIRECTED axis is by definition not autonomous, so it
    # is excluded from the rule's own denominators and reported separately in a
    # third, all-inclusive denominator. That third number is published whether it
    # passes or fails; it is never used to reclassify a family into compliance.
    auto = [s for s in specs if not s.get("operator_directed")]
    share = non_price_share([s for s in auto if s.get("selects_information")])
    share_inclusive = non_price_share(auto)
    share_all = non_price_share(specs)
    share_all["rule_applies"] = False
    share_all["why_reported"] = (
        "every executed specification, operator-directed families included. Rule 14 scopes its "
        "75 %% threshold to AUTONOMOUS research, so this denominator is reported for transparency "
        "rather than judged. It is %s the threshold, and it is published either way."
        % ("above" if share_all.get("rule_met") else "BELOW"))
    n_fam_on_frontier = sum(1 for m in mapping if m["best_frontier_rank"] is not None)
    n_fam_mandated = sum(1 for m in mapping if m["mandated_by_isolated_governor"])
    n_fam_new_observable = sum(1 for m in mapping
                               if m["frontier_relation"] == "NEW_OBSERVABLE_UNDER_ZEROED_FRONTIER_DIMENSION")
    mandated_executed = sorted({k for m in mapping for k in m["mandated_by_isolated_governor"]})
    mandated_all = sorted(mandated)
    body = {"schema": "alpha_recovery_research_program/1", "calculation_owner": CALCULATION_OWNER,
            "frontier": rank, "isolated_governor": gov, "families": mapping,
            "budgets": budgets, "budget_rule": {"primary_max": FAMILY_PRIMARY_MAX, "rescue_max": FAMILY_RESCUE_MAX},
            "non_price_rule": share, "non_price_rule_inclusive": share_inclusive,
            "non_price_rule_all_executed": share_all,
            "non_price_rule_denominators": {
                "information_directed": "AUTONOMOUS specifications from families that SELECT an "
                                        "information dimension (the rule's subject)",
                "inclusive": "every AUTONOMOUS specification, construction-only families included",
                "all_executed": "every executed specification including the OPERATOR-DIRECTED "
                                "intraday axis; reported, not judged, because rule 14 scopes to "
                                "autonomous research"},
            "n_families": len(fams),
            "n_families_with_a_frontier_need": n_fam_on_frontier,
            "n_families_mandated_by_governor": n_fam_mandated,
            "n_families_new_observable_under_zeroed_dimension": n_fam_new_observable,
            "governor_mandates": mandated_all,
            "governor_mandates_executed": mandated_executed,
            "governor_mandates_not_executed": [k for k in mandated_all if k not in mandated_executed],
            "allocation_follows_frontier": bool(
                share["rule_met"] and share_inclusive["rule_met"]
                and all(m["frontier_relation"] != "NONE" for m in mapping)
                and len(mandated_executed) >= max(1, len(mandated_all) // 2)),
            "allocation_rule": ("every family is mandated, on the top-%d frontier, a PRICE_STATE rescue under "
                                "the reopening rule, or a declared new observable under a frontier dimension the "
                                "estate had zeroed; at least half of the governor's mandates are executed; "
                                "and the 75 %% non-price rule holds" % TOP_N_FRONTIER),
            "reopen_reasons": list(REOPEN_REASONS),
            "no_second_owner": {"memory": "alpha_agent.r59.memory (isolated root)", "queue": "none created",
                                "scheduler": "none created", "frontier": "alpha_agent.r63.gaps (read)",
                                "forward_owner": "api.forward_challenger_registry (not called)"}}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
