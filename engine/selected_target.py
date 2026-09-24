r"""R69.2 - THE SELECTED TARGET: one immutable, implementable representation.

R63 let an operator SELECT one of the three reviewed targets. R69.1 proved that an
approved MINIMUM_REPAIR selection produced the FULL TARGET's order plan - 20 names
and 35% turnover in place of the 14 names and 21% the operator chose - and failed
that path closed, because the repaired book's weights existed only inside the review
PROJECTION, were recomputed on every read, and had no owner anywhere.

This kernel is that owner. It answers exactly one question:

    "For the target the operator selected, what is the complete, implementable
     book - every weight, every allocation row and every economic - as the review
     that was actually on screen measured it?"

IT IS NOT AN OPTIMISER, and it is not a second review.
  * Every weight is READ VERBATIM from ``review.states[target].weights``, which
    ``engine.proposal_decision_review`` derived using only canonical primitives.
    Not one weight is computed, adjusted, redistributed, capped or rounded here.
  * Every economic (turnover, cost, score, volatility on both bases, concentration,
    cash) is READ VERBATIM from the same state. Nothing is re-priced.
  * Every allocation row's METADATA (sector, asset class, currency, multiplier,
    instrument type, execution convention, initial margin, cost bps) travels from
    the immutable proposal artifact's own row for that instrument, so a future or
    an FX leg is described and charged exactly as the proposal describes it.
  * Every row's ACTION label is re-derived by the reallocation kernel's own public
    ``reoptimised_action`` - the same rule that owner applies to its own repaired
    rows - so a label can never disagree with the weight beside it.

FULL_TARGET KEEPS THE ARTIFACT'S ALLOCATION LIST VERBATIM. The projection is proved
equivalent to it by test rather than replacing it, so this release cannot move one
historical economic. ``projected_full_target_allocations`` exists for that proof and
for nothing else.

CURRENT is represented but NOT implementable: it is the book that already exists, so
there is no target to implement and no order to place. It says so by name rather than
producing an empty plan that would read like success.

DETERMINISTIC AND PURE. No I/O, no clock, no network, no LLM. Called twice with the
same inputs it returns the same payload byte for byte, which is what lets a selection
bind its identity. It writes nothing, approves nothing, creates no order plan, creates
no order and moves no capital.
"""
from __future__ import annotations

from typing import Any, Optional

from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import proposal_decision_review as _pdr
from paper_trader.engine import reallocation_proposal as _rp

PHASE = "R69.2"
CALCULATION_OWNER = "engine.selected_target"
SCHEMA_VERSION = "selected_target.v1"

#: The three targets, re-exported from the review kernel that defines them. Never
#: re-spelled: one vocabulary, one owner.
TARGET_CURRENT = _pdr.STATE_CURRENT
TARGET_MINIMUM_REPAIR = _pdr.STATE_MINIMUM_REPAIR
TARGET_FULL_TARGET = _pdr.STATE_FULL_TARGET
TARGET_ORDER = tuple(_pdr.STATE_ORDER)
TARGET_LABELS = dict(_pdr.STATE_LABELS)

#: Where a target's allocation rows come from. Published on every block so a reader
#: never has to infer whether it is looking at the artifact or at a projection.
SOURCE_ARTIFACT_VERBATIM = "PROPOSAL_ARTIFACT_ALLOCATIONS_VERBATIM"
SOURCE_REVIEW_PROJECTION = "REVIEW_STATE_WEIGHTS_PROJECTED_ONTO_ARTIFACT_METADATA"
SOURCE_VOCAB = (SOURCE_ARTIFACT_VERBATIM, SOURCE_REVIEW_PROJECTION)

# --------------------------------------------------------------------------- #
# Why a target cannot be implemented. Structured, never free text.
# --------------------------------------------------------------------------- #
#: The selected target IS the current book. Nothing is bought, nothing is sold and
#: no order exists to place. The governed approval gate already refuses it
#: (``PDS_SELECTED_TARGET_IS_NO_CHANGE``); this states the same fact one layer down.
NOT_IMPLEMENTABLE_NO_CHANGE = "TARGET_IS_NO_CHANGE"
#: The review state carries no weight vector at all, so there is no book to build.
#: Never guessed and never defaulted to another target.
NOT_IMPLEMENTABLE_NO_WEIGHTS = "TARGET_WEIGHTS_UNAVAILABLE"
#: The review could not measure the repaired book's risk, so the repair is not
#: proven valid. The review already refuses to make it selectable; if a stale
#: envelope ever presented it anyway, it still cannot be implemented.
NOT_IMPLEMENTABLE_UNVERIFIED = "TARGET_NOT_VERIFIED_BY_REVIEW"
#: The proposal artifact carries no allocation row and no NAV, so no row can be
#: described or sized.
NOT_IMPLEMENTABLE_NO_ARTIFACT = "PROPOSAL_ALLOCATIONS_UNAVAILABLE"
NOT_IMPLEMENTABLE_VOCAB = (NOT_IMPLEMENTABLE_NO_CHANGE, NOT_IMPLEMENTABLE_NO_WEIGHTS,
                           NOT_IMPLEMENTABLE_UNVERIFIED, NOT_IMPLEMENTABLE_NO_ARTIFACT)

#: The economics frozen with a selection. Every key is read from the review state
#: the operator was shown; none is computed here.
ECONOMIC_KEYS = (
    "positions", "changes", "one_way_turnover", "two_way_turnover",
    "estimated_cost", "cost_basis", "traded_notional",
    "score", "score_improvement", "score_cost_hurdle",
    "score_improvement_net_of_cost", "switching_hurdle",
    "clears_switching_hurdle", "margin_above_hurdle",
    "portfolio_volatility", "volatility_state", "volatility_basis",
    "portfolio_volatility_capital_basis", "volatility_capital_basis",
    "volatility_capital_basis_assumption", "invested_weight",
    "score_basis", "score_excludes_uninvested_capital",
    "concentration", "largest_position", "sector_concentration", "cash_weight",
    "allocation_by_asset_class", "allocation_by_sleeve",
    "expected_return", "expected_return_state",
    "risk_measurement_state", "released_to",
    "turnover_budget", "turnover_budget_exceeded",
)

#: The instrument / provenance fields copied VERBATIM from the artifact's own row.
#: Every one of them belongs to `api.reallocation_proposal`; none is invented here.
_CARRIED_ROW_FIELDS = (
    "sector", "asset_class", "currency", "multiplier", "instrument_type",
    "execution_convention", "initial_margin_per_unit", "unit_notional_usd",
    "cost_bps_per_side", "sleeve_id", "score", "combined_score", "score_basis",
    "rank", "capital_usage_ratio", "source_hoc_recommendation",
)

_TOL = 1.0e-12


def _f(x: Any) -> Optional[float]:
    """Same numeric conventions as the review kernel this projects: a bool is not
    a number, and an unparseable value is absent rather than zero."""
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    v = _f(x)
    return None if v is None else round(v, nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if _f(x) is None else round(float(x), 2)


# --------------------------------------------------------------------------- #
# Allocation rows for a target, from weights this kernel did not compute
# --------------------------------------------------------------------------- #
def project_allocations(*, proposal: dict, weights: dict, policy: dict) -> list:
    """Allocation-shaped rows describing ``weights``, in the artifact's own shape.

    The weight of every row is the one it was GIVEN. This function decides three
    things and nothing else: which rows exist, what each row's action label is, and
    what each row's money fields are at the proposal's own NAV.

    A row exists for every currently-held name and for every name the target holds.
    A name that is neither held nor targeted has no row - labelling a 0.0% -> 0.0%
    move would be describing a change nobody makes (R54.2.4, the same rule the
    proposal kernel applies).
    """
    current = _pdr.current_weights(proposal)
    nav = _f((proposal.get("portfolio") or {}).get("nav")) or 0.0
    band = float(policy.get("material_weight_delta", 1.0e-4) or 1.0e-4)
    by_ticker = {a.get("ticker"): a for a in (proposal.get("allocations") or [])
                 if a.get("ticker")}
    held = {tk for tk, w in current.items() if (_f(w) or 0.0) > band}

    rows: list[dict] = []
    for tk in sorted(set(current) | set(weights) | set(by_ticker)):
        src = by_ticker.get(tk) or {}
        cw = _f(current.get(tk)) or 0.0
        pw = _f(weights.get(tk)) or 0.0
        is_held = tk in held
        # THE canonical rule, from the owner that owns it: the label follows the
        # weights. None means there is no row at all.
        action, reason_codes = _rp.reoptimised_action(
            ticker=tk, action=src.get("action") or _rp.ACT_RETAIN,
            reason_codes=list(src.get("reason_codes") or []),
            delta=round(pw - cw, 8), proposed=pw, held=is_held, policy=policy,
            counterparty_proposed=_f(weights.get(
                ((src.get("replacement_relationship") or {}).get("counterparty")))))
        if action is None:
            continue
        row = {"ticker": tk, "action": action,
               "reason_codes": sorted(set(reason_codes or [])),
               "held": is_held,
               "current_weight": _r(cw, 8), "proposed_weight": _r(pw, 8),
               "delta_weight": _r(pw - cw, 8),
               "current_market_value": _money(src.get("current_market_value")
                                              if tk in by_ticker else cw * nav),
               "proposed_market_value": _money(pw * nav),
               "capital_change": _money((pw - cw) * nav),
               "replacement_relationship": None,
               "allocation_row_source": ("ARTIFACT_ROW" if tk in by_ticker
                                         else "HELD_NAME_WITHOUT_ARTIFACT_ROW")}
        for k in _CARRIED_ROW_FIELDS:
            row[k] = src.get(k)
        rows.append(row)
    return rows


def projected_full_target_allocations(*, proposal: dict, policy: dict) -> list:
    """The projection applied to the FULL TARGET, for equivalence proof ONLY.

    It is never published as the full target's implementation and never reaches an
    order plan: the artifact's own allocation list is used for that, verbatim. This
    exists so a test can prove the projection reproduces the artifact's own
    ``proposed_weight`` for every row - which is what makes it trustworthy for the
    minimum repair, where no artifact list exists to compare against.
    """
    return project_allocations(proposal=proposal,
                               weights=_pdr.target_weights(proposal), policy=policy)


# --------------------------------------------------------------------------- #
# The risk-contribution comparison (DISPLAY ONLY - no policy is changed)
# --------------------------------------------------------------------------- #
#: R69.1 raised the dynamic-denominator question for manual review and this release
#: answers NONE of it: ``risk_contribution_excess_multiple`` and the 3/N basis are
#: exactly as ``engine.holding_opportunity_cost`` declares them. What was missing was
#: not a rule but a SIGHT LINE: on 2026-09-22 the minimum repair discharged both
#: RISK_CONTRIBUTION_CAP obligations without touching either breaching name, because
#: eleven retention exits shrank the covariance universe from 25 names to 14 and
#: lifted the cap from 12.0% to 21.4%. AMD and DDOG kept their exact weights and their
#: share of portfolio risk ROSE about six points each - into compliance. Every number
#: needed to see that was already published on the two states and nothing put them
#: side by side.
RISK_POLICY_OWNER = "engine.holding_opportunity_cost"
RISK_POLICY_UNCHANGED_NOTE = (
    "The per-name risk-contribution cap is 3/N over the covariance universe of the "
    "book being judged, declared once by engine.holding_opportunity_cost. This "
    "release changes no threshold, no basis and no obligation: it only publishes the "
    "before and after limits together, on both the invested and the capital basis, so "
    "a cap that moved because the universe shrank is visible rather than implied. The "
    "policy question itself is raised for manual review in "
    "docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md."
)
#: A name whose obligation closed while its weight did not materially move.
DISCHARGE_WITHOUT_REDUCTION = "DISCHARGED_BY_UNIVERSE_CHANGE_NOT_BY_REDUCTION"


def risk_contribution_comparison(*, before_state: dict, after_state: dict,
                                 policy: dict) -> dict:
    """Before vs after, read from the two states. NOTHING is measured here.

    ``before_state`` is always CURRENT (the book as it stands); ``after_state`` is
    the selected target. Both already carry the canonical risk owner's own
    contributions, limit block and breach list.
    """
    band = float(policy.get("material_weight_delta", 1.0e-4) or 1.0e-4)

    def _side(st: dict) -> dict:
        limit_block = dict(st.get("risk_contribution_limit") or {})
        contributions = {k: _f(v) for k, v in
                         (st.get("risk_contributions") or {}).items()}
        breaches = list(st.get("risk_contribution_breaches") or [])
        return {
            "limit": _f(limit_block.get("limit")),
            "limit_basis": limit_block.get("basis"),
            "n_covariance_names": limit_block.get("n_covariance_names"),
            "excess_multiple": _f(limit_block.get("risk_contribution_excess_multiple")),
            "contributions": {k: _r(v, 6) for k, v in sorted(contributions.items())},
            "breaches": breaches,
            "breach_count": len(breaches),
            "breached_instruments": sorted({b.get("ticker") for b in breaches
                                            if b.get("ticker")}),
            "portfolio_volatility_invested_basis": _f(st.get("portfolio_volatility")),
            "portfolio_volatility_capital_basis": _f(
                st.get("portfolio_volatility_capital_basis")),
            "volatility_state": st.get("volatility_state"),
            "invested_weight": _f(st.get("invested_weight")),
            "cash_weight": _f(st.get("cash_weight")),
            "concentration": _f(st.get("concentration")),
            "largest_position": _f(st.get("largest_position")),
            "sector_concentration": _f(st.get("sector_concentration")),
            "measurement_state": st.get("risk_measurement_state"),
        }

    before, after = _side(before_state), _side(after_state)
    w_before = before_state.get("weights") or {}
    w_after = after_state.get("weights") or {}

    # Every name that was in breach BEFORE and is not in breach AFTER, whose weight
    # did not materially move. The obligation closed without the position changing.
    discharged = []
    after_breached = set(after["breached_instruments"])
    for tk in before["breached_instruments"]:
        wb, wa = _f(w_before.get(tk)) or 0.0, _f(w_after.get(tk)) or 0.0
        if tk in after_breached or abs(wa - wb) > band or wa <= _TOL:
            continue
        discharged.append({
            "ticker": tk, "code": DISCHARGE_WITHOUT_REDUCTION,
            "weight_before": _r(wb, 8), "weight_after": _r(wa, 8),
            "weight_change": _r(wa - wb, 8),
            "risk_contribution_before": before["contributions"].get(tk),
            "risk_contribution_after": after["contributions"].get(tk),
            "limit_before": _r(before["limit"], 8),
            "limit_after": _r(after["limit"], 8),
        })

    shares_after = [v for v in after["contributions"].values() if v is not None]
    top2 = sum(sorted(shares_after, reverse=True)[:2]) if shares_after else None
    return {
        "owner": CALCULATION_OWNER,
        "policy_owner": RISK_POLICY_OWNER,
        "policy_changed_by_this_release": False,
        "thresholds_changed_by_this_release": False,
        "measured_here": False,
        "read_from": "engine.proposal_decision_review state (both sides)",
        "note": RISK_POLICY_UNCHANGED_NOTE,
        "manual_review_reference": "docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md",
        "before": before,
        "after": after,
        "limit_changed": bool(before["limit"] is not None and after["limit"] is not None
                              and abs(after["limit"] - before["limit"]) > 1.0e-9),
        "limit_delta": _r(None if (before["limit"] is None or after["limit"] is None)
                          else after["limit"] - before["limit"], 8),
        "covariance_universe_changed": bool(
            before["n_covariance_names"] is not None
            and after["n_covariance_names"] is not None
            and before["n_covariance_names"] != after["n_covariance_names"]),
        "breaches_closed": sorted(set(before["breached_instruments"])
                                  - set(after["breached_instruments"])),
        "breaches_opened": sorted(set(after["breached_instruments"])
                                  - set(before["breached_instruments"])),
        "discharged_without_reduction": discharged,
        "discharged_without_reduction_count": len(discharged),
        "largest_risk_share_after": (_r(max(shares_after), 6) if shares_after else None),
        "two_largest_risk_shares_after": _r(top2, 6),
        "volatility_invested_basis_rose": bool(
            before["portfolio_volatility_invested_basis"] is not None
            and after["portfolio_volatility_invested_basis"] is not None
            and after["portfolio_volatility_invested_basis"]
            > before["portfolio_volatility_invested_basis"]),
        "volatility_capital_basis_fell": bool(
            before["portfolio_volatility_capital_basis"] is not None
            and after["portfolio_volatility_capital_basis"] is not None
            and after["portfolio_volatility_capital_basis"]
            < before["portfolio_volatility_capital_basis"]),
    }


# --------------------------------------------------------------------------- #
# THE selected-target representation
# --------------------------------------------------------------------------- #
def selected_target_hash(block: Optional[dict]) -> Optional[str]:
    """A stable identity over the WEIGHTS, the allocation rows and the economics.

    Distinct from R63's ``selected_target_hash`` over the option's headline
    economics, which is kept unchanged: that one proves the operator saw the same
    summary; this one proves they get the same BOOK.
    """
    if not block:
        return None
    try:
        return _cr.stable_hash({
            "schema_version": block.get("schema_version"),
            "target": block.get("target"),
            "weights": block.get("weights"),
            "allocations": block.get("allocations"),
            "economics": block.get("economics"),
            "implementable": block.get("implementable"),
        })
    except Exception:  # noqa: BLE001 - an identity must never break a pure read
        return None


def build_selected_target(*, proposal: dict, review: dict, target: str,
                          identity: Optional[dict] = None) -> dict:
    """ONE target's complete, immutable, implementable representation.

    ``review`` is the payload ``engine.proposal_decision_review.build_review``
    returned for ``proposal``. Both are read; neither is mutated.
    """
    if target not in TARGET_ORDER:
        raise ValueError("Unknown target %r; one of %s is required."
                         % (target, ", ".join(TARGET_ORDER)))
    policy = _pdr.review_policy(proposal)
    states = (review or {}).get("states") or {}
    state = dict(states.get(target) or {})
    current_state = dict(states.get(TARGET_CURRENT) or {})
    weights = {k: _r(_f(v), 8) for k, v in (state.get("weights") or {}).items()
               if (_f(v) or 0.0) > _TOL}
    nav = _f((proposal.get("portfolio") or {}).get("nav"))
    allocs = list(proposal.get("allocations") or [])

    # --- which allocation list implements this target -------------------------- #
    if target == TARGET_FULL_TARGET:
        # VERBATIM. The artifact is and stays the authority for its own target; the
        # projection is proved equivalent by test and never substituted for it.
        allocations, source = [dict(a) for a in allocs], SOURCE_ARTIFACT_VERBATIM
    else:
        allocations, source = (project_allocations(proposal=proposal, weights=weights,
                                                   policy=policy),
                               SOURCE_REVIEW_PROJECTION)

    # --- implementability: named, never inferred ------------------------------- #
    reason = None
    if target == TARGET_CURRENT:
        reason = NOT_IMPLEMENTABLE_NO_CHANGE
    elif not weights:
        reason = NOT_IMPLEMENTABLE_NO_WEIGHTS
    elif not allocs or nav is None:
        reason = NOT_IMPLEMENTABLE_NO_ARTIFACT
    elif (target == TARGET_MINIMUM_REPAIR
          and state.get("risk_measurement_state") == _pdr.RISK_NOT_MEASURED
          and (state.get("repair_adjustments") or [])):
        reason = NOT_IMPLEMENTABLE_UNVERIFIED

    economics = {k: state.get(k) for k in ECONOMIC_KEYS if k in state}
    constraint_status = dict(state.get("constraint_status") or {})
    block = {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "calculation_owner": CALCULATION_OWNER,
        "target": target,
        "label": TARGET_LABELS.get(target, target),
        "allocation_source": source,
        "allocation_source_vocabulary": list(SOURCE_VOCAB),
        "weights_source": "engine.proposal_decision_review states[%s].weights" % target,
        "weights_read_verbatim": True,
        "weights": weights,
        "position_count": len(weights),
        "allocations": allocations,
        "allocation_count": len(allocations),
        "trading_actions": sorted({a.get("action") for a in allocations
                                   if a.get("action") in _rp.ACTION_VOCAB
                                   and a.get("action") != _rp.ACT_RETAIN}),
        "nav_basis": _money(nav),
        "economics": economics,
        "economics_source": "engine.proposal_decision_review states[%s]" % target,
        "economics_read_verbatim": True,
        "constraint_status": constraint_status,
        "mandatory_obligations_remaining": len(
            constraint_status.get("obligations_remaining") or []),
        "repair_adjustments": list(state.get("repair_adjustments") or []),
        "risk_contribution": risk_contribution_comparison(
            before_state=current_state, after_state=state, policy=policy),
        "implementable": reason is None,
        "not_implementable_reason": reason,
        "not_implementable_vocabulary": list(NOT_IMPLEMENTABLE_VOCAB),
        "not_implementable_detail": _IMPLEMENTABILITY_DETAIL.get(reason),
        # The identities this representation belongs to. A consumer that cannot
        # match all of them is looking at another world's target.
        "bound_identity": {
            "proposal_id": (identity or {}).get("proposal_id"),
            "proposal_hash": (identity or {}).get("proposal_hash"),
            "review_hash": (identity or {}).get("review_hash"),
            "hoc_assessment_hash": (identity or {}).get("hoc_assessment_hash"),
            "portfolio_state_hash": (identity or {}).get("portfolio_state_hash"),
            "corporate_actions_hash": (identity or {}).get("corporate_actions_hash"),
            "universe_scoring_hash": (identity or {}).get("universe_scoring_hash"),
            "active_book_id": (identity or {}).get("active_book_id"),
            "eligible_market_date": (identity or {}).get("eligible_market_date"),
        },
        # What this kernel is NOT.
        "is_an_optimiser": False,
        "computes_weights": False,
        "recomputes_economics": False,
        "second_proposal_engine": False,
        "second_risk_engine": False,
        "is_an_approval": False,
        "creates_order_plan": False,
        "creates_orders": False,
        "deploys_capital": False,
        "decided_by_llm": False,
        "reuses_not_rebuilds": [
            "engine.proposal_decision_review (every weight and every economic)",
            "engine.reallocation_proposal.reoptimised_action (the action label)",
            "engine.holding_opportunity_cost (the risk-contribution contract, read)",
            "api.reallocation_proposal artifact allocations (instrument metadata)",
        ],
    }
    block["selected_target_implementation_hash"] = selected_target_hash(block)
    return block


_IMPLEMENTABILITY_DETAIL = {
    NOT_IMPLEMENTABLE_NO_CHANGE: (
        "This target IS the current book. There is nothing to buy, nothing to sell "
        "and no order plan to build; the decision is to keep the book unchanged."),
    NOT_IMPLEMENTABLE_NO_WEIGHTS: (
        "The review published no weight vector for this target, so the book it "
        "describes cannot be built. No target is substituted for it."),
    NOT_IMPLEMENTABLE_UNVERIFIED: (
        "The repaired book's risk was never measured, so the repair is not proven "
        "valid and may not be implemented."),
    NOT_IMPLEMENTABLE_NO_ARTIFACT: (
        "The proposal artifact carries no allocation rows or no NAV, so no "
        "instrument can be described or sized."),
}


def build_all_selected_targets(*, proposal: dict, review: dict,
                               identity: Optional[dict] = None) -> dict:
    """All three targets, keyed by name, in the review's own order."""
    return {t: build_selected_target(proposal=proposal, review=review, target=t,
                                     identity=identity)
            for t in TARGET_ORDER}


def summarise(block: Optional[dict]) -> dict:
    """The compact form a read model or a surface renders. Adds no fact."""
    b = block or {}
    econ = b.get("economics") or {}
    risk = b.get("risk_contribution") or {}
    return {
        "target": b.get("target"), "label": b.get("label"),
        "implementable": bool(b.get("implementable")),
        "not_implementable_reason": b.get("not_implementable_reason"),
        "not_implementable_detail": b.get("not_implementable_detail"),
        "position_count": b.get("position_count"),
        "allocation_count": b.get("allocation_count"),
        "changes": econ.get("changes"),
        "one_way_turnover": econ.get("one_way_turnover"),
        "estimated_cost": econ.get("estimated_cost"),
        "cash_weight": econ.get("cash_weight"),
        "concentration": econ.get("concentration"),
        "portfolio_volatility": econ.get("portfolio_volatility"),
        "portfolio_volatility_capital_basis":
            econ.get("portfolio_volatility_capital_basis"),
        "mandatory_obligations_remaining": b.get("mandatory_obligations_remaining"),
        "allocation_source": b.get("allocation_source"),
        "risk_contribution_limit_before": (risk.get("before") or {}).get("limit"),
        "risk_contribution_limit_after": (risk.get("after") or {}).get("limit"),
        "discharged_without_reduction_count":
            risk.get("discharged_without_reduction_count"),
        "selected_target_implementation_hash":
            b.get("selected_target_implementation_hash"),
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "SCHEMA_VERSION",
    "TARGET_CURRENT", "TARGET_MINIMUM_REPAIR", "TARGET_FULL_TARGET",
    "TARGET_ORDER", "TARGET_LABELS",
    "SOURCE_ARTIFACT_VERBATIM", "SOURCE_REVIEW_PROJECTION", "SOURCE_VOCAB",
    "NOT_IMPLEMENTABLE_NO_CHANGE", "NOT_IMPLEMENTABLE_NO_WEIGHTS",
    "NOT_IMPLEMENTABLE_UNVERIFIED", "NOT_IMPLEMENTABLE_NO_ARTIFACT",
    "NOT_IMPLEMENTABLE_VOCAB", "ECONOMIC_KEYS",
    "RISK_POLICY_OWNER", "RISK_POLICY_UNCHANGED_NOTE", "DISCHARGE_WITHOUT_REDUCTION",
    "project_allocations", "projected_full_target_allocations",
    "risk_contribution_comparison", "selected_target_hash",
    "build_selected_target", "build_all_selected_targets", "summarise",
]
