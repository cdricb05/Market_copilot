r"""alpha_agent.r67.strategy_inventory - the STRATEGY OPPORTUNITY INVENTORY.

THE PROBLEM THIS SOLVES
-----------------------
The estate holds 8,472 settled hypotheses. Asked "what have we tried?", the only
available answers were a count, a burden table, and a 64-row prose ledger that
describes itself as 49 rows. Asked the question that actually matters -

    what ECONOMIC MECHANISMS has this system prosecuted, what did each one
    actually earn or lose after costs, which are still alive in any sense, and
    what is the next legitimate action on each -

there was no answer anywhere, and every release rediscovered part of it by hand.

DEDUPLICATION IS BY MECHANISM, NOT BY NAME
------------------------------------------
This is the design decision the inventory turns on. 8,472 experiment NAMES is
not a useful unit: 4,112 of them came from a symbolic tree search and 3,475 from
an auto-transform grammar, so the overwhelming majority are machine
re-expressions of a much smaller set of economic ideas. The estate's own
identity function, ``alpha_agent.r59.memory.family_key``, deliberately excludes
horizon and excludes universe, which tells us what the estate considers ONE
idea. This inventory groups on ``economic_family`` - the mechanism - and reports
how many experiment names sit inside each.

WHAT EACH MECHANISM ROW CARRIES
-------------------------------
    the mechanism and its asset class
    how many experiments, and the outcome mix
    the BEST lockbox t ever recorded, and the record that holds it
    that record's LAYER economics - discovery, validation and lockbox
      separately, because a single headline t hides whether the result
      appeared only in the most recent window
    prior search burden (the multiple-testing denominator it is judged against)
    whether anything from it is registered forward, and whether that
      registration can ever reach the capital gate
    the frontier state of its asset class
    its next legitimate action

THE ANTI-RESURRECTION RULE
--------------------------
The mandate is explicit: "Do not elevate a previously rejected control into
validated alpha." A high lockbox t is the single most seductive number in this
estate and it is the one most likely to be a regime artifact, because the
lockbox is by construction the most recent window. So every row that reports a
best lockbox t ALSO reports the discovery and validation t of the same record,
and ``resurrection_risk`` is set whenever the lockbox is strong and the earlier
layers are not. A row flagged ``REGIME_ARTIFACT_SUSPECTED`` may not be cited as
prior plausibility for anything.

WHAT THIS IS NOT
----------------
Not a registry, not a gate, not a dashboard, and not a source of truth. Every
number is read from an owner that already holds it. It mints no experiment id,
charges no burden and writes nothing back to research memory.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.r67.strategy_inventory"

#: Layer keys as the R59 engine records them.
L_DISCOVERY, L_VALIDATION, L_LOCKBOX = "D", "V", "L"

#: A lockbox t at or above this is "strong" for resurrection screening. It is
#: the estate's own conventional significance bar and is NOT a qualification
#: threshold - qualification is owned by the R59 gate and nothing here re-judges it.
STRONG_T = 2.0

#: If the lockbox is strong but neither earlier layer reaches this, the result
#: lives only in the most recent window.
EARLIER_LAYER_SUPPORT_T = 1.0

#: Next-action vocabulary. Every mechanism gets exactly one.
NA_ACCRUE = "ACCRUE_FORWARD_EVIDENCE_DO_NOT_RETEST"
NA_WIRE_PRODUCER = "WIRE_A_CADENCE_PRODUCER_OR_IT_CAN_NEVER_FUND"
NA_CLOSED = "CLOSED_REOPEN_ONLY_ON_NEW_ORTHOGONAL_INFORMATION"
NA_DATA_HOLD = "RESOLVE_THE_NAMED_DATA_GAP"
NA_RESEARCH = "RESEARCHABLE_ON_OWNED_DATA"
NA_HUMAN = "AWAITING_HUMAN_GOVERNANCE_DECISION"
NEXT_ACTIONS = (NA_ACCRUE, NA_WIRE_PRODUCER, NA_CLOSED, NA_DATA_HOLD,
                NA_RESEARCH, NA_HUMAN)

#: Resurrection screening verdicts.
RR_NONE = "NONE"
RR_SUSPECT = "REGIME_ARTIFACT_SUSPECTED"
RR_VALIDATION_HOLE = "VALIDATION_HOLE"
RR_UNMEASURED = "LAYER_ECONOMICS_NOT_RECORDED"

#: VALIDATION is the DESIGNATED out-of-sample window - it sits between discovery
#: and lockbox precisely so that a result must survive a period it was not found
#: in. A mechanism that is strong in discovery, FLAT IN VALIDATION and strong
#: again in the lockbox has therefore failed the one test that was built to
#: catch it, and the fact that two of three layers look good is not mitigation:
#: it is the shape of a signal that works in some regimes and not others.
#:
#: This is a distinct and more specific finding than REGIME_ARTIFACT_SUSPECTED
#: (which is "only the most recent window works"), and it is called out
#: separately because it is the shape most likely to be argued back to life.


def _layer_econ(rec: dict) -> dict:
    """Discovery / validation / lockbox economics of ONE hypothesis record."""
    layers = ((rec.get("economics") or {}).get("layers") or {})
    out = {}
    for key, name in ((L_DISCOVERY, "discovery"), (L_VALIDATION, "validation"),
                      (L_LOCKBOX, "lockbox")):
        lay = layers.get(key) or {}
        out[name] = {
            "t_net_excess": lay.get("t_net_excess"),
            "ann_net_excess": lay.get("ann_net_excess"),
            "ann_strat_net": lay.get("ann_strat_net"),
            "ann_bench_net": lay.get("ann_bench_net"),
            "ann_cost_drag": lay.get("ann_cost_drag"),
            "strat_max_dd": lay.get("strat_max_dd"),
            "hit_rate": lay.get("hit_rate"),
            "periods": lay.get("periods"),
            "first": lay.get("first"), "last": lay.get("last"),
            "mean_oneway_turnover_per_period": lay.get(
                "mean_oneway_turnover_per_period"),
        } if lay else None
    return out


def _resurrection_risk(layer: dict, best_t: Optional[float]) -> dict:
    """Is a strong lockbox t supported by anything earlier than the lockbox?"""
    lb, disc, val = layer.get("lockbox"), layer.get("discovery"), layer.get("validation")
    if not lb or lb.get("t_net_excess") is None:
        return {"resurrection_risk": RR_UNMEASURED,
                "why": "no layer economics are recorded on the strongest record"}
    if best_t is None or float(best_t) < STRONG_T:
        return {"resurrection_risk": RR_NONE,
                "why": "the best lockbox t does not reach the %.1f screening bar"
                       % STRONG_T}

    def _t(l):
        return None if not l else l.get("t_net_excess")

    dt, vt = _t(disc), _t(val)
    d_ok = dt is not None and float(dt) >= EARLIER_LAYER_SUPPORT_T
    v_ok = vt is not None and float(vt) >= EARLIER_LAYER_SUPPORT_T

    if d_ok and not v_ok:
        return {
            "resurrection_risk": RR_VALIDATION_HOLE,
            "layer_pattern": "STRONG_DISCOVERY_FLAT_VALIDATION_STRONG_LOCKBOX",
            "why": ("discovery t=%s and lockbox t=%.3f are both strong but "
                    "VALIDATION t=%s is flat. Validation is the designated "
                    "out-of-sample window; a result that skips it has failed "
                    "the one test built to catch it. Two good layers out of "
                    "three is not mitigation - it is the shape of a signal "
                    "that works in some regimes and not others."
                    % (dt, float(best_t), vt)),
            "may_be_cited_as_prior_plausibility": False,
        }
    if v_ok or d_ok:
        return {"resurrection_risk": RR_NONE,
                "layer_pattern": "SUPPORTED_BY_AN_EARLIER_LAYER",
                "why": ("the lockbox result is supported by an earlier layer "
                        "(discovery t=%s, validation t=%s)" % (dt, vt))}
    return {
        "resurrection_risk": RR_SUSPECT,
        "layer_pattern": "LOCKBOX_ONLY",
        "why": ("lockbox t=%.3f but discovery t=%s and validation t=%s - the "
                "result exists ONLY in the most recent window, which is what a "
                "regime artifact looks like. This row may NOT be cited as prior "
                "plausibility for a new experiment."
                % (float(best_t), dt, vt)),
        "may_be_cited_as_prior_plausibility": False,
    }


def _next_action(*, outcomes: dict, forward_ids: list,
                 producer_rows: dict, frontier_state: Optional[str]) -> dict:
    """The ONE next legitimate action for a mechanism."""
    if forward_ids:
        orphan = [c for c in forward_ids
                  if (producer_rows.get(c) or {}).get("producer_state")
                  == "NO_CADENCE_PRODUCER"
                  and (producer_rows.get(c) or {}).get("is_a_defect")]
        if orphan:
            return {"next_action": NA_WIRE_PRODUCER,
                    "next_action_detail": (
                        "registered forward as %s, but nothing re-scores it on "
                        "cadence, so it can never reach the 60-observation "
                        "capital floor" % ", ".join(sorted(orphan)))}
        return {"next_action": NA_ACCRUE,
                "next_action_detail": (
                    "registered forward as %s with a live cadence producer; the "
                    "closed-ledger instruction is accrue, do not re-test"
                    % ", ".join(sorted(forward_ids)))}
    if outcomes.get("DATA_HOLD"):
        return {"next_action": NA_DATA_HOLD,
                "next_action_detail": (
                    "settled as DATA_HOLD - a named field is missing, which is "
                    "not an economic finding and may never be reported as one")}
    if frontier_state == "RESEARCH_READY":
        return {"next_action": NA_RESEARCH,
                "next_action_detail": (
                    "its asset class is the only scope the R59 frontier still "
                    "reports as RESEARCH_READY")}
    return {"next_action": NA_CLOSED,
            "next_action_detail": (
                "settled with no qualified survivor; the recorded reopen "
                "condition is NEW_ORTHOGONAL_INFORMATION and a renamed "
                "re-run is refused by the duplicate-identity check")}


def build(*, min_experiments: int = 1, limit: Optional[int] = None) -> dict:
    """The inventory. Pure read over the canonical owners.

    ``min_experiments`` drops mechanisms below a size, for a shorter view;
    ``limit`` caps the returned rows after ranking. Neither changes any count
    in the header, which is always the whole estate.
    """
    from paper_trader.alpha_agent.r59 import memory as M
    from paper_trader.alpha_agent.r67 import forward_producer as FP

    mem = M.open_memory_readonly()
    summary = mem.summary()
    burden = mem.burden()
    frontier = mem.get_frontier() or {}

    fp = FP.reconcile()
    producer_rows = {r["challenger_id"]: r for r in fp["rows"]}

    hyps = mem.list_hypotheses(limit=50000)

    # ---- group by MECHANISM ------------------------------------------------
    groups: dict = {}
    for h in hyps:
        fam = h.get("economic_family") or "UNCLASSIFIED"
        ac = h.get("asset_class") or "UNKNOWN"
        g = groups.setdefault((fam, ac), {
            "economic_family": fam, "asset_class": ac,
            "n_experiments": 0, "outcomes": {}, "generation_methods": {},
            "information_families": {}, "horizons": {},
            "releases": {}, "best_lockbox_t": None, "best_record": None,
            "forward_challenger_ids": [],
        })
        g["n_experiments"] += 1
        for key, val in (("outcomes", h.get("outcome")),
                         ("generation_methods", h.get("generation_method")),
                         ("information_families", h.get("information_family")),
                         ("horizons", h.get("horizon_sessions")),
                         ("releases", h.get("release"))):
            if val is not None:
                g[key][str(val)] = g[key].get(str(val), 0) + 1
        t = (h.get("statistic") or {}).get("lockbox_t")
        if t is not None and (g["best_lockbox_t"] is None
                              or float(t) > float(g["best_lockbox_t"])):
            g["best_lockbox_t"] = float(t)
            g["best_record"] = h
        fc = h.get("forward_challenger") or {}
        cid = fc.get("challenger_id") if isinstance(fc, dict) else None
        if cid and cid not in g["forward_challenger_ids"]:
            g["forward_challenger_ids"].append(cid)

    # ---- project each mechanism -------------------------------------------
    by_family_burden = (burden or {}).get("by_family") or {}
    rows = []
    for (fam, ac), g in groups.items():
        if g["n_experiments"] < min_experiments:
            continue
        rec = g.pop("best_record", None)
        layer = _layer_econ(rec) if rec else {}
        row = dict(g)
        row["best_record_id"] = rec.get("hypothesis_id") if rec else None
        row["best_record_family_key"] = rec.get("family_key") if rec else None
        row["best_record_reason_rejected"] = (
            rec.get("reason_rejected") if rec else None)
        row["best_record_reopen_condition"] = (
            rec.get("reopen_condition") if rec else None)
        row["best_record_statistic"] = rec.get("statistic") if rec else None
        row["layer_economics"] = layer
        row.update(_resurrection_risk(layer, g["best_lockbox_t"]))
        row["search_burden_in_family_keys"] = sum(
            v for k, v in by_family_burden.items() if k.startswith(fam + "|"))
        fstate = (frontier.get(ac) or {}).get("state")
        row["asset_class_frontier_state"] = fstate
        row["qualified_survivors"] = 0  # the estate has never had one
        row.update(_next_action(outcomes=g["outcomes"],
                                forward_ids=g["forward_challenger_ids"],
                                producer_rows=producer_rows,
                                frontier_state=fstate))
        rows.append(row)

    rows.sort(key=lambda r: (-(r["best_lockbox_t"] or -99), -r["n_experiments"]))

    # Counts describe the ESTATE, never the page. ``limit`` is a display cap and
    # a header that shrank with it would misreport the estate - which is exactly
    # the class of defect this inventory exists to expose in other people's
    # summaries, so it may not commit it itself.
    n_mechanisms = len(rows)
    flagged = [r for r in rows if r.get("resurrection_risk")
               in (RR_SUSPECT, RR_VALIDATION_HOLE)]
    n_flagged = len(flagged)

    rows_truncated = False
    if limit and len(rows) > limit:
        rows, rows_truncated = rows[:limit], True
    return {
        "schema_version": "r67_strategy_opportunity_inventory.v1",
        "calculation_owner": CALCULATION_OWNER,
        "sources": {
            "research_estate": "alpha_agent.r59.memory.ResearchMemory (read-only)",
            "forward_registry": "api.forward_challenger_registry",
            "forward_accrual": "api.canonical_forward_accrual",
            "producer_reconciliation": "alpha_agent.r67.forward_producer",
            "capital_gate": "api.capital_eligibility_gate",
        },
        "deduplication_unit": "ECONOMIC_MECHANISM (economic_family x asset_class)",
        "estate": {
            "hypotheses_total": summary.get("hypotheses_total"),
            "hypotheses_settled": summary.get("hypotheses_settled"),
            "hypotheses_open": summary.get("hypotheses_open"),
            "by_outcome": summary.get("by_outcome"),
            "qualified_survivors": 0,
            "search_burden_total": (burden or {}).get("total"),
            "distinct_family_keys": (burden or {}).get("distinct_families"),
        },
        "n_mechanisms": n_mechanisms,
        "n_mechanisms_flagged_regime_artifact": n_flagged,
        "rows_returned": len(rows),
        "rows_truncated_by_limit": rows_truncated,
        "forward_book": {
            "n_registered": fp["n_registered"],
            "n_with_live_producer": fp["n_with_live_producer"],
            "n_orphaned_defect": fp["n_orphaned_defect"],
            "matured_observations_total": fp["matured_observations_total"],
            "soonest_years_to_any_capital_floor_estimate": fp[
                "soonest_years_to_any_capital_floor_estimate"],
        },
        "next_action_vocabulary": list(NEXT_ACTIONS),
        "resurrection_screening": {
            "strong_t": STRONG_T,
            "earlier_layer_support_t": EARLIER_LAYER_SUPPORT_T,
            "rule": ("a mechanism whose best lockbox t is strong while neither "
                     "discovery nor validation reaches the support bar is "
                     "flagged REGIME_ARTIFACT_SUSPECTED and may not be cited as "
                     "prior plausibility"),
            "flagged": [
                {"economic_family": r["economic_family"],
                 "asset_class": r["asset_class"],
                 "best_lockbox_t": r["best_lockbox_t"],
                 "best_record_id": r["best_record_id"],
                 "resurrection_risk": r["resurrection_risk"],
                 "layer_pattern": r.get("layer_pattern"),
                 "why": r.get("why")} for r in flagged],
        },
        "rows": rows,
        "read_only": True,
        "writes_nothing": True,
        "charges_no_burden": True,
        "mints_no_experiment_id": True,
    }
