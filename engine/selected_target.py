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
from paper_trader.engine import holding_opportunity_cost as _hoc
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

# --------------------------------------------------------------------------- #
# R69.5 - the REFERENCE comparison, and the policy-review state it produces
# --------------------------------------------------------------------------- #
#: R69.2 made the moving cap visible. It did not make the moving cap ANSWERABLE:
#: ``discharged_without_reduction`` asks only "did this name's weight move?", so a
#: token trim removes a name from the list entirely. On 2026-09-23 the full target
#: trimmed AMD by 0.36 points of NAV - enough to drop it from that list, nowhere
#: near enough to bring it under the cap the current book was actually judged
#: against. And the list can say nothing at all about a name that was never in
#: breach before: the same target raised ALAB from 1.84% to 3.21% of NAV and added
#: SNDK at 2.88%, and both now carry more portfolio risk than the current book's own
#: limit allows - concentrations the relaxed cap tolerates and the original does not.
#:
#: So this release asks the question the operator actually has: HOLDING THE CAP
#: STILL, does the target comply? The reference is not a new threshold and not a
#: second policy. It is the limit ``engine.holding_opportunity_cost`` already
#: applied to the BEFORE book, reused verbatim, and every breach against it is
#: measured by that same owner's own ``risk_contribution_breaches``.
REFERENCE_LIMIT_BASIS = "BEFORE_BOOK_GOVERNED_LIMIT_HELD_CONSTANT"
#: The reference is not available when either side published no limit.
REFERENCE_UNAVAILABLE = "UNAVAILABLE_NO_LIMIT_ON_ONE_SIDE"
#: A name in breach BEFORE whose share is under the BEFORE limit after the change:
#: the position (or the rest of the book) genuinely carries less risk.
CLOSED_BY_EXPOSURE = "CLOSED_BY_EXPOSURE_REDUCTION_ALONE"
#: A name in breach BEFORE that is compliant only because the limit rose. Its share
#: is still above the limit the breach was raised against.
CLOSED_BY_LIMIT_RELAXATION = "CLOSED_ONLY_BECAUSE_THE_LIMIT_ROSE"
#: A name in breach BEFORE and still in breach AFTER, on the target's own limit.
STILL_IN_BREACH = "STILL_IN_BREACH_ON_THE_GOVERNED_LIMIT"
#: A name that was NOT in breach against the reference before and IS after. Never a
#: denominator effect: the target put more risk into that name than the current
#: book's own limit admits.
REFERENCE_BREACH_OPENED = "OPENED_AGAINST_THE_REFERENCE_LIMIT"

#: The manual-review states. A target that clears its own 3/N cap only because that
#: cap moved is NOT refused here and NOT approved here: it is held at a named state
#: that an operator must rule on. Nothing in this module changes a threshold.
POLICY_REVIEW_NOT_REQUIRED = "NOT_REQUIRED"
POLICY_REVIEW_REQUIRED = "RISK_POLICY_REVIEW_REQUIRED_DENOMINATOR_RELAXATION"
POLICY_REVIEW_VOCAB = (POLICY_REVIEW_NOT_REQUIRED, POLICY_REVIEW_REQUIRED)
#: The token an operator sends to record that they ruled on the state above. It is
#: deliberately NOT a button: a policy ruling that can be clicked by reflex is not a
#: ruling. The approval gate (`api.portfolio_decision`) binds it to the exact
#: instruments, the exact reference limit and the exact frozen book.
POLICY_REVIEW_ACK_TOKEN = "ACKNOWLEDGE_RISK_CONTRIBUTION_REFERENCE_BREACH"
#: The three courses R69.1 put to the operator, carried as DATA so the screen and
#: the refusal quote the same list. None of them is taken here.
POLICY_REVIEW_OPTIONS = (
    "ACCEPT_AS_IS - the cap is a relative-concentration rule, cash is a real asset "
    "choice, and both limits are published.",
    "JUDGE_AGAINST_THE_BEFORE_UNIVERSE - a risk-contribution breach must be repaired "
    "by reducing the breaching name, not by shrinking the universe around it.",
    "ADD_AN_ABSOLUTE_COMPANION_FLOOR - no name above X% of portfolio risk whatever N "
    "is.",
)
POLICY_REVIEW_REFERENCE_DOC = "docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md"


def reference_limit_compliance(*, before: dict, after: dict,
                               weights_before: dict, weights_after: dict,
                               band: float = 1.0e-4) -> dict:
    """Would the AFTER book comply if the per-name cap had NOT moved?

    The reference limit is ``before["limit"]`` - the number the canonical risk owner
    already applied to the current book - held constant and applied to the target's
    own contributions. No threshold is invented, nothing is re-measured, and the
    governed 3/N policy is untouched: this is a COMPARISON, published beside the
    governed verdict and never in place of it.

    ``indicative_weight_at_reference`` is a FIRST-ORDER figure (scale the weight by
    ``reference / share``) offered so the operator can see the order of magnitude of
    the change the reference would demand. It is not a solved target: reducing one
    name moves every other name's share, and solving that is the optimiser's job,
    not this kernel's.
    """
    ref = _f(before.get("limit"))
    governed = _f(after.get("limit"))
    contributions = {k: _f(v) for k, v in (after.get("contributions") or {}).items()}
    before_contrib = {k: _f(v) for k, v in (before.get("contributions") or {}).items()}
    if ref is None or governed is None:
        return {"state": REFERENCE_UNAVAILABLE, "reference_limit": ref,
                "governed_limit": governed, "basis": REFERENCE_LIMIT_BASIS,
                "limit_owner": RISK_POLICY_OWNER, "measured_by": RISK_POLICY_OWNER,
                "reference_is_a_new_threshold": False,
                "complies_with_reference": None, "breaches": [], "breach_count": 0,
                "breached_instruments": [], "opened_against_reference": []}

    # The ONE canonical breach function, on the reference limit. Not a second rule:
    # the same callable the governed verdict on both sides came from.
    breaches = _hoc.risk_contribution_breaches(contributions=contributions, limit=ref)
    before_rows = _hoc.risk_contribution_breaches(contributions=before_contrib,
                                                  limit=ref)
    before_breached = {b.get("ticker") for b in before_rows}
    # The same measure on the BEFORE book, so the two sides are comparable: how
    # much of the portfolio's risk, and how much of its NAV, sat above this limit
    # then and sits above it now.
    before_risk = sum(_f(b.get("risk_contribution_pct")) or 0.0 for b in before_rows)
    before_nav = sum(_f((weights_before or {}).get(b.get("ticker"))) or 0.0
                     for b in before_rows)
    rows, risk_share, nav_share = [], 0.0, 0.0
    for b in breaches:
        tk = b.get("ticker")
        share = _f(b.get("risk_contribution_pct"))
        wa = _f((weights_after or {}).get(tk)) or 0.0
        wb = _f((weights_before or {}).get(tk)) or 0.0
        scale = (ref / share) if (share and share > 0) else None
        rows.append({
            "ticker": tk,
            "code": (REFERENCE_BREACH_OPENED if tk not in before_breached
                     else "CARRIED_FROM_THE_BEFORE_BOOK"),
            "risk_contribution_before": _r(before_contrib.get(tk), 6),
            "risk_contribution": _r(share, 6),
            "reference_limit": _r(ref, 8),
            "governed_limit": _r(governed, 8),
            "excess_over_reference": _r(b.get("excess"), 8),
            "compliant_on_governed_limit": bool(share is not None and share <= governed),
            "weight_before": _r(wb, 8), "weight_after": _r(wa, 8),
            "weight_change": _r(wa - wb, 8),
            # Materiality is decided HERE, by the owner that holds the band, so a
            # surface can label the row without doing arithmetic of its own. The
            # architecture audit forbids weight arithmetic in the browser for
            # exactly this reason: a second rule in the one place no test reaches.
            "weight_moved": bool(abs(wa - wb) > band),
            "indicative_weight_at_reference": (None if scale is None
                                               else _r(wa * scale, 8)),
            "indicative_weight_change_required": (None if scale is None
                                                  else _r(wa * scale - wa, 8)),
            "indicative_basis": ("FIRST_ORDER_PROPORTIONAL_SCALING_OF_THIS_NAME_ONLY"
                                 "_NOT_A_REOPTIMISATION"),
            "solved": False,
        })
        risk_share += (share or 0.0)
        nav_share += wa
    return {
        "state": "AVAILABLE",
        "basis": REFERENCE_LIMIT_BASIS,
        "reference_limit": _r(ref, 8),
        "reference_limit_source": ("the limit engine.holding_opportunity_cost applied "
                                   "to the BEFORE book, held constant"),
        "reference_is_a_new_threshold": False,
        "governed_limit": _r(governed, 8),
        "limit_owner": RISK_POLICY_OWNER,
        "measured_by": RISK_POLICY_OWNER,
        "measured_here": False,
        "complies_with_reference": not breaches,
        "breaches": rows,
        "breach_count": len(rows),
        "breached_instruments": sorted({r["ticker"] for r in rows if r["ticker"]}),
        "opened_against_reference": sorted({r["ticker"] for r in rows
                                            if r["code"] == REFERENCE_BREACH_OPENED}),
        "risk_share_of_reference_breaches": _r(risk_share, 6) if rows else 0.0,
        "nav_share_of_reference_breaches": _r(nav_share, 8) if rows else 0.0,
        "breached_instruments_before": sorted(t for t in before_breached if t),
        "breach_count_before": len(before_rows),
        "risk_share_of_reference_breaches_before": _r(before_risk, 6),
        "nav_share_of_reference_breaches_before": _r(before_nav, 8),
    }


def discharge_attribution(*, before: dict, after: dict, weights_before: dict,
                          weights_after: dict) -> list:
    """For every name in breach BEFORE: what actually closed the breach.

    Two additive effects, both read and neither modelled:

    * ``exposure_effect``  = share_before - share_after  (the book carries less of
      this name's risk - whether because the position was cut or because the rest of
      the book changed around it);
    * ``limit_effect``     = limit_after  - limit_before (the bar moved).

    Their sum must cover the original excess for the breach to close, and which one
    did the work is the whole question R69.1 raised. A name that is under the BEFORE
    limit afterwards closed on exposure alone and needs no policy ruling.
    """
    lb, la = _f(before.get("limit")), _f(after.get("limit"))
    cb = {k: _f(v) for k, v in (before.get("contributions") or {}).items()}
    ca = {k: _f(v) for k, v in (after.get("contributions") or {}).items()}
    out = []
    for tk in sorted(before.get("breached_instruments") or []):
        sb, sa = cb.get(tk), ca.get(tk)
        if sb is None or lb is None:
            continue
        held_after = (_f((weights_after or {}).get(tk)) or 0.0) > _TOL
        exposure = None if sa is None else _r(sb - sa, 8)
        limit_eff = None if (lb is None or la is None) else _r(la - lb, 8)
        if not held_after:
            code = CLOSED_BY_EXPOSURE
        elif sa is None or la is None:
            code = STILL_IN_BREACH
        elif sa > la + _TOL:
            code = STILL_IN_BREACH
        elif sa <= lb + _TOL:
            code = CLOSED_BY_EXPOSURE
        else:
            code = CLOSED_BY_LIMIT_RELAXATION
        denom = (exposure or 0.0) + (limit_eff or 0.0)
        out.append({
            "ticker": tk, "code": code,
            "excess_over_before_limit": _r(sb - lb, 8),
            "risk_contribution_before": _r(sb, 6),
            "risk_contribution_after": _r(sa, 6),
            "limit_before": _r(lb, 8), "limit_after": _r(la, 8),
            "weight_before": _r(_f((weights_before or {}).get(tk)) or 0.0, 8),
            "weight_after": _r(_f((weights_after or {}).get(tk)) or 0.0, 8),
            "exposed_after": held_after,
            "exposure_effect": exposure,
            "limit_effect": limit_eff,
            "exposure_share_of_closure": (_r(exposure / denom, 6)
                                          if (exposure is not None and denom > _TOL
                                              and exposure >= 0.0) else None),
            "compliant_at_before_limit": (None if sa is None else bool(sa <= lb + _TOL)),
            "compliant_at_governed_limit": (None if (sa is None or la is None)
                                            else bool(sa <= la + _TOL)),
        })
    return out


def policy_review_state(*, reference: dict, attribution: list,
                        limit_changed: bool) -> dict:
    """Does this target need a manual RISK-POLICY ruling before it can be approved?

    It does when both are true: the per-name cap ROSE, and the target does not
    comply with the cap the current book was judged against. That is precisely
    "compliance depends on denominator-driven cap relaxation" - and it is the only
    condition under which this state is raised. When the limit did not move, the
    reference IS the governed limit and any breach is an ordinary mandatory repair
    that the existing gates already own.

    This grants nothing, changes nothing and refuses nothing. It NAMES a decision.
    """
    breached = list(reference.get("breached_instruments") or [])
    required = bool(limit_changed and breached
                    and reference.get("state") == "AVAILABLE")
    relaxation_only = sorted({a["ticker"] for a in (attribution or [])
                              if a.get("code") == CLOSED_BY_LIMIT_RELAXATION})
    opened = list(reference.get("opened_against_reference") or [])
    return {
        "owner": CALCULATION_OWNER,
        "policy_owner": RISK_POLICY_OWNER,
        "state": POLICY_REVIEW_REQUIRED if required else POLICY_REVIEW_NOT_REQUIRED,
        "vocabulary": list(POLICY_REVIEW_VOCAB),
        "required": required,
        "policy_changed_here": False,
        "exception_granted_here": False,
        "target_approved_here": False,
        "declared_policy": {
            "limit": "risk_contribution_excess_multiple / n_covariance_names (3/N)",
            "basis": _hoc.RISK_CONTRIBUTION_LIMIT_BASIS,
            "owner": RISK_POLICY_OWNER,
            "unchanged_by_this_release": True,
        },
        "reference_limit": reference.get("reference_limit"),
        "governed_limit": reference.get("governed_limit"),
        "instruments": breached,
        "instruments_breaching_only_because_the_limit_rose": relaxation_only,
        "instruments_opened_against_the_reference": opened,
        "acknowledgement_token": POLICY_REVIEW_ACK_TOKEN,
        "decision_required": (
            "Rule on whether a per-name risk-contribution cap with a MOVING "
            "denominator is the intended governance for this target. %d name(s) "
            "(%s) sit above the %s limit the current book was judged against and "
            "are compliant only because the cap rose to %s. Approval is withheld "
            "until that ruling is recorded; neither the cap nor the target has "
            "been changed."
            % (len(breached), ", ".join(breached) or "none",
               reference.get("reference_limit"), reference.get("governed_limit"))
            if required else
            "None. This target complies with the limit the current book was judged "
            "against, so no risk-policy ruling stands between it and the existing "
            "approval gates."),
        "options": list(POLICY_REVIEW_OPTIONS),
        "manual_review_reference": POLICY_REVIEW_REFERENCE_DOC,
    }


# --------------------------------------------------------------------------- #
# R82 - the governed RULING on the moving denominator
#
# R69.5 NAMED the decision and withheld approval until an operator recorded one.
# It could record exactly ONE outcome: a bound acknowledgement that, once
# accepted, let the approval through. That is ACCEPT_AS_IS under a neutral name,
# and it left the second course R69.1 put to the operator - judge the cap against
# the universe the breach was RAISED against - unrecordable, because the only door
# available unblocks the very approval that ruling refuses.
#
# This block is that vocabulary, and the derived state a recorded ruling produces
# over ONE frozen book. It invents NO threshold:
# JUDGE_AGAINST_THE_BEFORE_UNIVERSE binds a limit
# ``engine.holding_opportunity_cost`` already published for the before-book, and
# every breach under it was measured by that same owner's own
# ``risk_contribution_breaches`` inside ``reference_limit_compliance``. The
# declared 3/N policy is untouched; a ruling is scoped to the exact frozen
# proposal and selected target it names and never to the estate; and nothing here
# can make an approval MORE available than it was before the ruling.
# --------------------------------------------------------------------------- #
RULING_ACCEPT_AS_IS = "ACCEPT_AS_IS"
RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE = "JUDGE_AGAINST_THE_BEFORE_UNIVERSE"
#: Declared so it cannot be quietly coined elsewhere, and REFUSED for now: an
#: absolute companion floor needs a threshold X that no owner has set, and setting
#: one here would be inventing governance inside a ruling recorder. It stays a
#: separate policy decision.
RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR = "ADD_AN_ABSOLUTE_COMPANION_FLOOR"
RULING_VOCAB = (RULING_ACCEPT_AS_IS, RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE,
                RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR)
RULING_NOT_AVAILABLE_THIS_RELEASE = (RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR,)
RULING_AVAILABLE = tuple(r for r in RULING_VOCAB
                         if r not in RULING_NOT_AVAILABLE_THIS_RELEASE)
RULING_UNAVAILABLE_REASON = (
    "ADD_AN_ABSOLUTE_COMPANION_FLOOR requires an absolute per-name risk threshold "
    "that no owner declares. Recording it here would invent that threshold inside "
    "a ruling recorder, so it is refused and remains a separate policy decision.")

#: WHICH cap a ruling makes binding for the book it names. Both are limits the
#: canonical risk owner already published; neither is new.
BINDS_GOVERNED_LIMIT = "THE_TARGETS_OWN_3_OVER_N_LIMIT"
BINDS_REFERENCE_LIMIT = "THE_BEFORE_BOOK_LIMIT_HELD_CONSTANT"

#: How ONE name is treated once the ruling is on record.
TREATMENT_OBLIGATION_REOPENED = "OBLIGATION_REOPENED_AGAINST_THE_RULED_CAP"
TREATMENT_COMPLIANT_AT_RULED_CAP = "COMPLIANT_AT_THE_RULED_CAP"
TREATMENT_UNAFFECTED = "UNAFFECTED_BY_THE_RULING"

#: The ruled state a frozen book carries. ``UNRULED`` is the honest third value:
#: no ruling has been recorded, which is not the same as a ruling that permits.
RULED_UNRULED = "NO_RULING_ON_RECORD"
RULED_REFERENCE_BINDS = "RULED_REFERENCE_LIMIT_BINDS_OBLIGATIONS_REOPENED"
RULED_REFERENCE_SATISFIED = "RULED_REFERENCE_LIMIT_BINDS_AND_IS_SATISFIED"
RULED_GOVERNED_STANDS = "RULED_GOVERNED_LIMIT_STANDS"
#: R82.1 - a ruling artifact EXISTS for this book and carries no verified operator
#: provenance, so it is readable audit evidence and binds nothing. Distinct from
#: ``UNRULED`` because the operator must be told the record is there, and distinct
#: from every binding state because an unverified record may not govern anything.
RULED_UNVERIFIED = "RULING_ON_RECORD_BUT_PROVENANCE_UNVERIFIED"
RULED_STATE_VOCAB = (RULED_UNRULED, RULED_UNVERIFIED, RULED_REFERENCE_BINDS,
                     RULED_REFERENCE_SATISFIED, RULED_GOVERNED_STANDS)

# --------------------------------------------------------------------------- #
# R82.1 - the ruling vocabulary IN PLAIN ENGLISH, published as data.
#
# R69.5 shipped the three courses as prose in ``POLICY_REVIEW_OPTIONS`` - one
# English sentence each, with no machine-readable availability and no statement of
# what choosing one does NOT do. A screen cannot build an operator control from
# that, and the screen it did build had no control at all.
#
# This is the same three courses as structured data: what the ruling means, what it
# does to the book, and - said once per option, because it is the thing an operator
# most needs to know - that recording it does not approve anything.
# --------------------------------------------------------------------------- #
RULING_PLAIN_ENGLISH = {
    RULING_ACCEPT_AS_IS: {
        "title": "Accept as is - keep the target's own dynamic 3/N cap",
        "detail": [
            "The per-name risk-contribution cap stays the declared 3/N over the "
            "covariance universe of the book being judged, which is the policy "
            "exactly as engine.holding_opportunity_cost declares it.",
            "The cap is a relative-concentration rule, and a repair that exits "
            "names genuinely holds a smaller universe. No name is reduced and no "
            "successor target is built.",
            "Every name the selected target holds stays exactly as it was selected.",
        ],
        "does_not_approve": True,
        "builds_a_successor_target": False,
        "reopens_obligations": False,
    },
    RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE: {
        "title": ("Judge against the before universe - the cap the current book was "
                  "held to binds this decision"),
        "detail": [
            "A risk-contribution breach raised under the pre-repair covariance "
            "universe must be repaired by REDUCING the breaching name. Shrinking "
            "the universe around it does not discharge the obligation.",
            "Every name in breach of that held cap reopens as a mandatory "
            "RISK_CONTRIBUTION_CAP obligation with required action REDUCE.",
            "Paper Trader then re-solves the repair against the held cap, using the "
            "same repair owner and the same covariance owner, and publishes the "
            "policy-compliant successor target for review.",
        ],
        "does_not_approve": True,
        "builds_a_successor_target": True,
        "reopens_obligations": True,
    },
    RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR: {
        "title": "Add an absolute companion floor - no name above X% of portfolio risk",
        "detail": [
            "Shown for completeness because R69.1 put it to the operator. It is "
            "UNAVAILABLE in this release.",
            RULING_UNAVAILABLE_REASON,
            "It remains a separate policy decision and cannot be recorded here.",
        ],
        "does_not_approve": True,
        "builds_a_successor_target": False,
        "reopens_obligations": False,
    },
}


def ruling_options() -> list:
    """The three courses as ORDERED, machine-readable options for a write surface.

    The availability of each one is the vocabulary's own, so a screen can never
    offer a ruling the recorder would refuse, and never hide one the recorder would
    accept. Pure; no I/O.
    """
    out = []
    for r in RULING_VOCAB:
        text = RULING_PLAIN_ENGLISH.get(r) or {}
        effect = ruling_effect(r)
        out.append({
            "ruling": r,
            "title": text.get("title") or r.replace("_", " ").title(),
            "detail": list(text.get("detail") or []),
            "available": bool(effect["available"]),
            "unavailable_reason": (None if effect["available"] else effect["reason"]),
            "unavailable_detail": (None if effect["available"] else effect["detail"]),
            "binds": effect["binds"],
            "does_not_approve": True,
            "builds_a_successor_target": bool(text.get("builds_a_successor_target")),
            "reopens_obligations": bool(text.get("reopens_obligations")),
        })
    return out


#: The scope sentence, carried as DATA so the gate, the API and the screen quote
#: one sentence rather than three paraphrases.
RULING_SCOPE = (
    "This ruling binds the ONE frozen proposal and the ONE selected target it "
    "names. It changes no declared threshold, grants no standing exception, "
    "creates no absolute companion floor, approves nothing and can never make an "
    "approval more available than it was before the ruling was recorded.")


def ruling_effect(ruling: Optional[str]) -> dict:
    """What ONE ruling token does. Pure lookup; refuses an unknown or held token."""
    r = ruling if isinstance(ruling, str) else None
    if r not in RULING_VOCAB:
        return {"ruling": r, "known": False, "available": False,
                "vocabulary": list(RULING_VOCAB),
                "available_vocabulary": list(RULING_AVAILABLE),
                "binds": None, "reason": "RULING_NOT_IN_VOCABULARY",
                "detail": ("Unknown ruling %r. One of %s is required."
                           % (r, ", ".join(RULING_AVAILABLE)))}
    if r in RULING_NOT_AVAILABLE_THIS_RELEASE:
        return {"ruling": r, "known": True, "available": False,
                "vocabulary": list(RULING_VOCAB),
                "available_vocabulary": list(RULING_AVAILABLE),
                "binds": None, "reason": "RULING_NOT_AVAILABLE_IN_THIS_RELEASE",
                "detail": RULING_UNAVAILABLE_REASON}
    binds = (BINDS_REFERENCE_LIMIT
             if r == RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
             else BINDS_GOVERNED_LIMIT)
    return {"ruling": r, "known": True, "available": True,
            "vocabulary": list(RULING_VOCAB),
            "available_vocabulary": list(RULING_AVAILABLE),
            "binds": binds, "reason": None,
            # Stated once, here, so no caller has to infer it: a ruling is a
            # constraint on approval, never a licence for one. ACCEPT_AS_IS
            # leaves the R69.5 acknowledgement gate exactly as it stands.
            "unblocks_approval": False,
            "detail": ("The per-name risk-contribution cap binding this frozen "
                       "book is %s." % binds)}


def apply_policy_ruling(*, risk_contribution: Optional[dict],
                        ruling: Optional[str]) -> dict:
    """The derived state a recorded ruling produces over ONE frozen book.

    NOTHING is measured here and no threshold is invented. Both caps and every
    breach under either of them were produced by ``engine.holding_opportunity_cost``
    and are merely READ off the frozen ``risk_contribution`` block:

    * ``JUDGE_AGAINST_THE_BEFORE_UNIVERSE`` makes ``reference_compliance``'s own
      ``reference_limit`` the binding cap, so every row that block already lists as
      a breach becomes an OPEN mandatory repair obligation on this book. A breach
      that existed under the frozen pre-repair covariance universe therefore cannot
      be discharged by the universe shrinking around it.
    * ``ACCEPT_AS_IS`` leaves the target's own 3/N cap binding - the state the
      system was already in - and reopens nothing.

    ``max_valid_weight`` is the FIRST-ORDER indicative figure
    ``reference_limit_compliance`` already published, reused verbatim and labelled
    as such: solving a per-name reduction moves every other name's share, and that
    is the optimiser's job, not this kernel's.
    """
    rc = risk_contribution if isinstance(risk_contribution, dict) else {}
    ref = rc.get("reference_compliance") or {}
    after = rc.get("after") or {}
    governed = _f(after.get("limit"))
    effect = ruling_effect(ruling)
    base = {
        "owner": CALCULATION_OWNER,
        "policy_owner": RISK_POLICY_OWNER,
        "measured_here": False,
        "measured_by": RISK_POLICY_OWNER,
        "ruling": effect["ruling"],
        "ruling_known": effect["known"],
        "ruling_available": effect["available"],
        "ruling_vocabulary": list(RULING_VOCAB),
        "available_rulings": list(RULING_AVAILABLE),
        "binds": effect["binds"],
        "governed_limit": _r(governed, 8),
        "reference_limit": ref.get("reference_limit"),
        "declared_policy_changed": False,
        "standing_exception_granted": False,
        "absolute_companion_floor_created": False,
        "new_threshold_invented": False,
        "unblocks_approval": False,
        "target_approved_here": False,
        "creates_order_plan": False,
        "creates_orders": False,
        "scope": RULING_SCOPE,
        "state_vocabulary": list(RULED_STATE_VOCAB),
        "manual_review_reference": POLICY_REVIEW_REFERENCE_DOC,
    }
    if not effect["available"]:
        return {**base, "state": RULED_UNRULED, "applied": False,
                "binding_limit": _r(governed, 8),
                "binding_limit_source": BINDS_GOVERNED_LIMIT,
                "reopened_obligations": [], "reopened_obligation_count": 0,
                "obligations_open_after_ruling": None,
                "instruments_in_breach_of_the_binding_limit": [],
                "complies_with_the_binding_limit": None,
                "approval_blocked_by_this_ruling": False,
                "treatment": [],
                "reason": effect["reason"], "detail": effect["detail"]}

    judged = effect["ruling"] == RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
    ref_available = ref.get("state") == "AVAILABLE"
    binding = (_f(ref.get("reference_limit")) if (judged and ref_available)
               else governed)
    rows = list(ref.get("breaches") or []) if (judged and ref_available) else []

    # The obligation rows. Every field is read, and the tier, reason code,
    # constraint code and required action are the canonical owners' own tokens -
    # no private vocabulary is forked for a ruled obligation.
    obligations, treatment = [], []
    for b in rows:
        tk = b.get("ticker")
        obligations.append({
            "instrument_id": tk, "ticker": tk,
            "tier": _hoc.OBLIGATION_TIER_HARD,
            "obligation_type": _hoc.OBLIGATION_TIER_HARD,
            "reason_code": _hoc.OBLIGATION_REASON_MANDATORY,
            "constraint_code": _cr.C_RISK_CONTRIBUTION,
            "required_action": _hoc.REQUIRED_ACTION_REDUCE,
            "source_owner": RISK_POLICY_OWNER,
            "reopened_by_ruling": RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE,
            "risk_contribution": b.get("risk_contribution"),
            "binding_limit": _r(binding, 8),
            "excess_over_binding_limit": b.get("excess_over_reference"),
            "current_weight": b.get("weight_after"),
            "max_valid_weight": b.get("indicative_weight_at_reference"),
            "max_valid_weight_basis": b.get("indicative_basis"),
            "solved": False,
            "detail": (
                "%s carries %s of portfolio risk against the %s cap this ruling "
                "makes binding. The breach existed under the frozen pre-repair "
                "covariance universe and the ruling refuses to discharge it by "
                "shrinking that universe, so it must be repaired by reducing the "
                "name." % (tk, b.get("risk_contribution"),
                           ref.get("reference_limit"))),
        })
        treatment.append({
            "ticker": tk,
            "code": TREATMENT_OBLIGATION_REOPENED,
            "risk_contribution_before": b.get("risk_contribution_before"),
            "risk_contribution_after": b.get("risk_contribution"),
            "weight_before": b.get("weight_before"),
            "weight_after": b.get("weight_after"),
            "weight_moved": b.get("weight_moved"),
            "compliant_on_governed_limit": b.get("compliant_on_governed_limit"),
            "compliant_on_binding_limit": False,
            "binding_limit": _r(binding, 8),
            "governed_limit": _r(governed, 8),
            "excess_over_binding_limit": b.get("excess_over_reference"),
            "required_action": _hoc.REQUIRED_ACTION_REDUCE,
            "obligation_open": True,
            "opened_by_this_target": (
                b.get("code") == REFERENCE_BREACH_OPENED),
        })

    if judged and not ref_available:
        # The reference was never computable for this book, so the ruling has
        # nothing to bind. It is recorded and says so; it does not fall back to the
        # relaxed cap, which would read as permission.
        return {**base, "state": RULED_UNRULED, "applied": False,
                "binding_limit": None,
                "binding_limit_source": BINDS_REFERENCE_LIMIT,
                "reopened_obligations": [], "reopened_obligation_count": 0,
                "obligations_open_after_ruling": None,
                "instruments_in_breach_of_the_binding_limit": [],
                "complies_with_the_binding_limit": None,
                "approval_blocked_by_this_ruling": False, "treatment": [],
                "reason": REFERENCE_UNAVAILABLE,
                "detail": ("This book published no reference limit on one side, so "
                           "the ruled cap cannot be established. The ruling is "
                           "recorded and binds nothing; the target is neither "
                           "compliant nor in breach against it.")}

    if judged:
        state = RULED_REFERENCE_BINDS if obligations else RULED_REFERENCE_SATISFIED
    else:
        state = RULED_GOVERNED_STANDS
    instruments = sorted({o["ticker"] for o in obligations if o.get("ticker")})
    return {
        **base,
        "state": state, "applied": True,
        "binding_limit": _r(binding, 8),
        "binding_limit_source": effect["binds"],
        "binding_limit_is_a_new_threshold": False,
        "reopened_obligations": obligations,
        "reopened_obligation_count": len(obligations),
        # The frozen book's OWN count, read from the artifact, kept beside the
        # ruled count so the two are never confused for one another.
        "obligations_open_on_the_governed_limit": (after.get("breach_count")),
        "obligations_open_after_ruling": len(obligations),
        "instruments_in_breach_of_the_binding_limit": instruments,
        "complies_with_the_binding_limit": not obligations,
        "approval_blocked_by_this_ruling": bool(obligations),
        "treatment": treatment,
        "reason": None,
        "detail": (
            ("%d obligation(s) (%s) are OPEN against the %s cap this ruling makes "
             "binding for this frozen book. The target satisfies its own %s cap and "
             "that cap is not the one this book is judged against, so approval is "
             "not available. Reject, hold, or select a target that complies at %s."
             % (len(obligations), ", ".join(instruments), binding, governed,
                binding))
            if obligations else
            ("No name breaches the %s cap this ruling makes binding, so the ruling "
             "opens no obligation. It grants nothing: the existing approval gates "
             "are unchanged." % binding)),
    }


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
            # R69.5 - the canonical limit block spells this ``excess_multiple``. It
            # was read here under the policy KEY's name and so was always None: the
            # one number that says WHY the limit is what it is never reached a
            # screen. Both spellings are accepted so a limit block from any caller
            # still answers.
            "excess_multiple": _f(limit_block.get("excess_multiple")
                                  if limit_block.get("excess_multiple") is not None
                                  else limit_block.get(
                                      "risk_contribution_excess_multiple")),
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
    # R69.5 - the same two sides, judged a second way: against the limit the BEFORE
    # book was already held to. Published BESIDE the governed verdict, never in
    # place of it, and measured by the governed owner's own breach function.
    limit_changed = bool(before["limit"] is not None and after["limit"] is not None
                         and after["limit"] > before["limit"] + 1.0e-9)
    reference = reference_limit_compliance(
        before=before, after=after, weights_before=w_before, weights_after=w_after,
        band=band)
    attribution = discharge_attribution(
        before=before, after=after, weights_before=w_before, weights_after=w_after)
    return {
        "reference_compliance": reference,
        "discharge_attribution": attribution,
        "policy_review": policy_review_state(
            reference=reference, attribution=attribution,
            limit_changed=limit_changed),
        "limit_relaxed": limit_changed,
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
        # R69.5 - a surface that renders only this summary still learns whether the
        # target owes a risk-policy ruling, and on how many names.
        "complies_with_reference_limit":
            (risk.get("reference_compliance") or {}).get("complies_with_reference"),
        "reference_limit": (risk.get("reference_compliance") or {}).get("reference_limit"),
        "reference_breach_count":
            (risk.get("reference_compliance") or {}).get("breach_count"),
        "reference_breached_instruments":
            (risk.get("reference_compliance") or {}).get("breached_instruments"),
        "risk_policy_review_state": (risk.get("policy_review") or {}).get("state"),
        "risk_policy_review_required": (risk.get("policy_review") or {}).get("required"),
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
    "REFERENCE_LIMIT_BASIS", "REFERENCE_UNAVAILABLE", "CLOSED_BY_EXPOSURE",
    "CLOSED_BY_LIMIT_RELAXATION", "STILL_IN_BREACH", "REFERENCE_BREACH_OPENED",
    "POLICY_REVIEW_NOT_REQUIRED", "POLICY_REVIEW_REQUIRED", "POLICY_REVIEW_VOCAB",
    "POLICY_REVIEW_ACK_TOKEN", "POLICY_REVIEW_OPTIONS", "POLICY_REVIEW_REFERENCE_DOC",
    "RULING_ACCEPT_AS_IS", "RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE",
    "RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR", "RULING_VOCAB",
    "RULING_AVAILABLE", "RULING_NOT_AVAILABLE_THIS_RELEASE",
    "RULING_UNAVAILABLE_REASON", "RULING_SCOPE",
    "BINDS_GOVERNED_LIMIT", "BINDS_REFERENCE_LIMIT",
    "TREATMENT_OBLIGATION_REOPENED", "TREATMENT_COMPLIANT_AT_RULED_CAP",
    "TREATMENT_UNAFFECTED", "RULED_UNRULED", "RULED_REFERENCE_BINDS",
    "RULED_REFERENCE_SATISFIED", "RULED_GOVERNED_STANDS", "RULED_STATE_VOCAB",
    "RULED_UNVERIFIED", "RULING_PLAIN_ENGLISH", "ruling_options",
    "ruling_effect", "apply_policy_ruling",
    "reference_limit_compliance", "discharge_attribution", "policy_review_state",
    "project_allocations", "projected_full_target_allocations",
    "risk_contribution_comparison", "selected_target_hash",
    "build_selected_target", "build_all_selected_targets", "summarise",
]
