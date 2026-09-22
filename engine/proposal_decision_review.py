r"""R62 - PORTFOLIO PROPOSAL DECISION REVIEW: the pure adjudication kernel.

Paper Trader already produced every fact an operator needs to judge a reallocation
proposal and then left the operator to combine them by hand. This kernel is the
missing last mile: given the ONE authoritative proposal, the ONE opportunity-cost
assessment and the ONE outcome-evidence store, it answers

    "Given the current portfolio, the proposal, its costs, its constraints, the
     evidence accumulated from previous decisions and the available alternatives,
     what should the operator REVIEW?"

It is NOT an optimiser and NOT a second proposal engine. It performs no allocation
search, proposes no new holding, deploys no capital and invents no threshold. It
adjudicates three states and explains the difference between them:

    A. CURRENT          - the book as it stands, read VERBATIM from the proposal
                          artifact (do nothing).
    B. MINIMUM_REPAIR   - the smallest economically valid change set that repairs
                          the CURRENT book's actual hard / governance violations,
                          with every released dollar going to CASH. A repair
                          restores validity; the global allocator owns capital
                          deployment (Release-32 design rule), so a repair may
                          never quietly become an alpha optimisation.
    C. FULL_TARGET      - the existing canonical reallocation proposal, read
                          VERBATIM. Not one number is recomputed.

Every primitive is borrowed from the owner that already owns it, so the three
states are comparable by construction and this module can never disagree with the
backend:

  * ``engine.constrained_reallocation`` - the constraint inventory, ``name_caps``,
    ``candidate_meta``, ``verify_feasibility``, ``one_way_turnover``, ``herfindahl``
    and the weighted-score basis.
  * ``engine.holding_opportunity_cost`` - THE per-name risk-contribution contract
    (field, ``risk_contribution_limit`` 3/N, ``risk_contribution_breaches``) and the
    covariance kernel behind it, plus the per-holding retention verdict.
  * ``engine.reallocation_proposal`` - annualised portfolio volatility, the coverage
    reconciliation, concentration and the turnover / cost model, through that
    owner's own public aliases.
  * ``engine.reassessment_outcomes`` - the matured decision evidence. Read, never
    recomputed and never allowed to override the proposal.

DETERMINISTIC AND OFFLINE. No LLM, no API call, no prompt, no network: the operator
explanation is assembled from structured reason codes and authoritative backend
facts. The verdict is a RECOMMENDATION FOR MANUAL REVIEW. It approves nothing,
creates no order plan, confirms nothing, executes nothing, promotes nothing and
mutates no artifact.
"""
from __future__ import annotations

from typing import Any, Optional

from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import holding_opportunity_cost as _hoc
from paper_trader.engine import reallocation_proposal as _rp

CALCULATION_OWNER = "engine.proposal_decision_review"
SCHEMA_VERSION = "proposal_decision_review.v1"
PHASE = "R62"

#: The review policy. It introduces NO tuned threshold of its own: every bar it
#: applies is a bar an existing owner already published (see
#: :func:`incremental_hurdle_contract`). Versioned so a later change is visible.
REVIEW_POLICY_VERSION = "proposal_decision_review_policy.v1"
#: What counts as a repair obligation, and what a repair may do about it.
REPAIR_SCOPE_VERSION = "proposal_repair_scope.v1"

# --------------------------------------------------------------------------- #
# The three states
# --------------------------------------------------------------------------- #
STATE_CURRENT = "CURRENT"
STATE_MINIMUM_REPAIR = "MINIMUM_REPAIR"
STATE_FULL_TARGET = "FULL_TARGET"
STATE_ORDER = (STATE_CURRENT, STATE_MINIMUM_REPAIR, STATE_FULL_TARGET)
STATE_LABELS = {
    STATE_CURRENT: "Current portfolio (do nothing)",
    STATE_MINIMUM_REPAIR: "Minimum constraint repair",
    STATE_FULL_TARGET: "Full zero-base target (the proposal)",
}

# --------------------------------------------------------------------------- #
# Review verdicts. RECOMMENDATIONS FOR MANUAL REVIEW - never an approval.
# --------------------------------------------------------------------------- #
VERDICT_FULL_TARGET_REVIEWABLE = "FULL_TARGET_REVIEWABLE"
VERDICT_MINIMAL_REPAIR_PREFERRED = "MINIMAL_REPAIR_PREFERRED"
VERDICT_DEFER_WEAK_INCREMENTAL_EDGE = "DEFER_WEAK_INCREMENTAL_EDGE"
VERDICT_BLOCKED_CONSTRAINT_OR_DATA = "BLOCKED_CONSTRAINT_OR_DATA"
VERDICT_NO_CHANGE_REQUIRED = "NO_CHANGE_REQUIRED"
VERDICT_VOCAB = (VERDICT_FULL_TARGET_REVIEWABLE, VERDICT_MINIMAL_REPAIR_PREFERRED,
                 VERDICT_DEFER_WEAK_INCREMENTAL_EDGE,
                 VERDICT_BLOCKED_CONSTRAINT_OR_DATA, VERDICT_NO_CHANGE_REQUIRED)
VERDICT_LABELS = {
    VERDICT_FULL_TARGET_REVIEWABLE: "Review the full target",
    VERDICT_MINIMAL_REPAIR_PREFERRED: "Review the minimum repair first",
    VERDICT_DEFER_WEAK_INCREMENTAL_EDGE: "Defer - the edge is too weak for the churn",
    VERDICT_BLOCKED_CONSTRAINT_OR_DATA: "Blocked - constraint or data",
    VERDICT_NO_CHANGE_REQUIRED: "No change required",
}

# --------------------------------------------------------------------------- #
# Change classification. Exactly ONE primary reason per proposed change, and every
# reason is one an existing owner can actually support.
# --------------------------------------------------------------------------- #
# The five OBLIGATION reasons are re-exported from the canonical mandatory-repair
# contract (R63), never forked. The remaining four classify a CHANGE rather than
# an obligation, so they stay owned here.
REASON_MANDATORY = _hoc.OBLIGATION_REASON_MANDATORY
REASON_OPPORTUNITY = "OPPORTUNITY_IMPROVEMENT"
REASON_RISK = "RISK_REDUCTION"
REASON_CONCENTRATION = _hoc.OBLIGATION_REASON_CONCENTRATION
REASON_LIQUIDITY = _hoc.OBLIGATION_REASON_LIQUIDITY
REASON_UNIVERSE = _hoc.OBLIGATION_REASON_UNIVERSE
REASON_RETENTION = _hoc.OBLIGATION_REASON_RETENTION
REASON_REOPTIMIZATION = "PORTFOLIO_REOPTIMIZATION"
REASON_OTHER = "OTHER_EXISTING_CANONICAL_REASON"
REASON_VOCAB = (REASON_MANDATORY, REASON_OPPORTUNITY, REASON_RISK,
                REASON_CONCENTRATION, REASON_LIQUIDITY, REASON_UNIVERSE,
                REASON_RETENTION, REASON_REOPTIMIZATION, REASON_OTHER)
#: A change whose primary reason is one of these implements a constraint or a
#: governance rule: it is FORCED, and is never weighed against an economic hurdle.
FORCED_REASONS = (REASON_MANDATORY, REASON_CONCENTRATION, REASON_LIQUIDITY,
                  REASON_UNIVERSE, REASON_RETENTION)

#: Constraint code -> primary reason. Every key is a code the canonical constraint
#: inventory declares; nothing is invented here.
CONSTRAINT_REASON = {
    _cr.C_ELIGIBLE_UNIVERSE: REASON_UNIVERSE,
    _cr.C_LIQUIDITY_FLOOR: REASON_LIQUIDITY,
    _cr.C_LIQUIDITY_PARTICIPATION: REASON_LIQUIDITY,
    _cr.C_NAME_CAP: REASON_CONCENTRATION,
    _cr.C_SECTOR_CAP: REASON_CONCENTRATION,
    _cr.C_CONCENTRATION: REASON_CONCENTRATION,
    _cr.C_ASSET_CLASS_CAP: REASON_CONCENTRATION,
    _cr.C_SLEEVE_CAP: REASON_CONCENTRATION,
    _cr.C_CURRENCY_CAP: REASON_CONCENTRATION,
    _cr.C_COLLATERAL_CAP: REASON_CONCENTRATION,
    # The risk-contribution cap resolves by OBJECT, not by code alone: a name that
    # breaches the limit on the book being judged is a mandatory repair; a name a
    # constraint merely holds below the limit is a risk reduction.
    _cr.C_RISK_CONTRIBUTION: REASON_MANDATORY,
    _cr.C_LONG_ONLY: REASON_MANDATORY,
    _cr.C_GROSS_EXPOSURE: REASON_MANDATORY,
    _cr.C_CASH_BOUNDS: REASON_MANDATORY,
    _cr.C_MIN_POSITION: REASON_MANDATORY,
    _cr.C_MAX_POSITIONS: REASON_MANDATORY,
    _cr.C_UNIT_GRANULARITY: REASON_MANDATORY,
}

# --------------------------------------------------------------------------- #
# Repair scope (REPAIR_SCOPE_VERSION). Two tiers, both owned elsewhere.
# --------------------------------------------------------------------------- #
#: A mandatory portfolio limit the CURRENT book actually breaches. Evaluated by
#: ``engine.constrained_reallocation.verify_feasibility`` for everything that is
#: arithmetic over weights, and by the ``engine.holding_opportunity_cost``
#: risk-contribution contract for the one limit that verifier declares it cannot
#: check.
#: R63 - re-exported from the canonical mandatory-repair contract, never forked.
TIER_HARD = _hoc.OBLIGATION_TIER_HARD
#: A held name the canonical opportunity-cost decision policy classifies as BROKEN:
#: it is outside the book's own retention rules (it fell beyond the exit buffer) or
#: outside the eligible universe. HOC owns the verdict; this kernel only reads it.
TIER_GOVERNANCE = _hoc.OBLIGATION_TIER_GOVERNANCE
TIER_VOCAB = tuple(_hoc.OBLIGATION_TIER_VOCAB)

#: What a repair does with the capital it releases. Declared, not discovered.
RELEASED_CAPITAL_DESTINATION = "CASH"
REPAIR_DOC = (
    "The minimum repair answers ONE question: what is the smallest change that makes "
    "the CURRENT portfolio valid again? It exits names the governed retention / "
    "eligibility rules no longer admit, reduces names that breach a mandatory limit "
    "to the compliant level using the SAME first-order rule the reallocation kernel "
    "uses, and releases every freed dollar to CASH. It never adds a position, never "
    "increases one and never redistributes to the next-best opportunity - that would "
    "make it a second optimiser. Cash is a real asset choice; the global allocator "
    "owns capital deployment."
)

#: R63 - re-exported from the canonical mandatory-repair contract, never forked.
REPAIR_ACTION_EXIT = _hoc.REQUIRED_ACTION_EXIT
REPAIR_ACTION_REDUCE = _hoc.REQUIRED_ACTION_REDUCE
#: The obligation is real and the owner that raised it is named, but the COMPLIANT
#: weight cannot be derived from the persisted evidence this review is allowed to
#: read. Reducing "somewhat" would not be minimal and would not be a repair, so the
#: review says it cannot size it and fails closed instead of guessing.
REPAIR_ACTION_NOT_SIZEABLE = _hoc.REQUIRED_ACTION_NOT_SIZEABLE
REPAIR_ACTION_VOCAB = tuple(_hoc.REQUIRED_ACTION_VOCAB)

#: Risk measurement states for a state this kernel had to measure itself.
RISK_MEASURED = "MEASURED"
RISK_NOT_MEASURED = "NOT_MEASURED"

EXPECTED_RETURN_STATE_NOT_CALIBRATED = _rp.EXPECTED_RETURN_STATE_NOT_CALIBRATED
#: Evidence that has not reached the canonical sufficiency gate says so and stops.
EVIDENCE_INSUFFICIENT = "EVIDENCE_INSUFFICIENT"

#: Proposal read states in which there is nothing current to adjudicate.
NON_REVIEWABLE_READ_STATES = (
    "NOT_RUN", "UNAVAILABLE", "NO_ACTIVE_BOOK", "BLOCKED", "WITHHELD",
    "STALE_CORPORATE_ACTION_REVIEW_REQUIRED", "SUPERSEDED_BY_NEWER_DECISION",
)

_TOL = 1.0e-12


# --------------------------------------------------------------------------- #
# Small numeric helpers (same rounding conventions as the owners above)
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def _sub(a: Optional[float], b: Optional[float]) -> Optional[float]:
    return None if (a is None or b is None) else float(a) - float(b)


# --------------------------------------------------------------------------- #
# The declared hurdle contract
# --------------------------------------------------------------------------- #
def incremental_hurdle_contract(*, switching_hurdle: Optional[float],
                                hurdle_owner: str) -> dict:
    """How the FULL target's INCREMENT over the minimum repair is judged.

    This is the one place a review could have smuggled in a new number, so it is
    declared instead. The bar is the EXISTING switching hurdle - the same frozen
    net-improvement bar the proposal owner already applied to the whole change -
    applied to a different economic object: the step FROM the repaired book TO the
    full target. Nothing is fitted, and nothing is tuned on any particular
    proposal's answer.
    """
    return {
        "hurdle": _r(switching_hurdle, 6),
        "hurdle_owner": hurdle_owner,
        "hurdle_source": "EXISTING_SWITCHING_HURDLE",
        "basis": "EXISTING_SWITCHING_HURDLE_APPLIED_TO_THE_INCREMENT",
        "new_threshold_introduced": False,
        "tuned_on_this_proposal": False,
        "tuned_on_outcomes": False,
        "review_policy_version": REVIEW_POLICY_VERSION,
        "doc": ("The increment is the step from the MINIMUM REPAIR to the FULL "
                "TARGET: its score improvement, net of the transaction cost of the "
                "extra trading it requires, judged against the same hurdle the "
                "proposal owner applies to the whole switch. When no repair is "
                "required the repaired book IS the current book, so the increment "
                "and the whole switch are the same object and the two verdicts "
                "agree by construction."),
    }


# --------------------------------------------------------------------------- #
# Reading the proposal artifact (verbatim - nothing here recomputes a proposal)
# --------------------------------------------------------------------------- #
def current_weights(proposal: dict) -> dict:
    """The CURRENT book, from the proposal's own allocation rows."""
    return {a["ticker"]: (_f(a.get("current_weight")) or 0.0)
            for a in (proposal.get("allocations") or [])
            if a.get("ticker") and (_f(a.get("current_weight")) or 0.0) > _TOL}


def target_weights(proposal: dict) -> dict:
    """The FULL TARGET, from the proposal's own allocation rows."""
    return {a["ticker"]: (_f(a.get("proposed_weight")) or 0.0)
            for a in (proposal.get("allocations") or [])
            if a.get("ticker") and (_f(a.get("proposed_weight")) or 0.0) > _TOL}


def candidate_rows(proposal: dict) -> list:
    """The proposal's allocation rows re-presented as the CANDIDATE rows the
    constraint kernel's own helpers expect (``name_caps`` / ``candidate_meta`` /
    ``verify_feasibility``). Asset-agnostic: every instrument attribute the rows
    carry - asset class, sleeve, currency, instrument type, capital usage, unit
    notional, per-instrument cost - travels through untouched, so a book holding
    futures or FX is judged by the same code as an equity book."""
    rows = []
    for a in (proposal.get("allocations") or []):
        if not a.get("ticker"):
            continue
        rows.append({
            "ticker": a["ticker"],
            "sector": a.get("sector"),
            "score": a.get("score"),
            "rank": a.get("rank"),
            "asset_class": a.get("asset_class"),
            "sleeve_id": a.get("sleeve_id"),
            "currency": a.get("currency"),
            "instrument_type": a.get("instrument_type"),
            "capital_usage_ratio": a.get("capital_usage_ratio"),
            "multiplier": a.get("multiplier"),
            "unit_notional_usd": a.get("unit_notional_usd"),
            "cost_bps_per_side": a.get("cost_bps_per_side"),
            # adv_dollar is deliberately absent: the review reads persisted
            # evidence only, and the liquidity verdict for a HELD name is owned by
            # the opportunity-cost assessment's own liquidity_state.
        })
    return rows


def review_policy(proposal: dict) -> dict:
    """The policy this review applies: the constraint kernel's canonical defaults,
    OVERRIDDEN by the proposal's own persisted policy. Every threshold therefore
    belongs to the owner that declared it, at the value that proposal was built
    with - never at today's value and never at a value chosen here."""
    pol = dict(_cr.default_policy())
    pol.update({k: v for k, v in (proposal.get("policy") or {}).items()
                if v is not None})
    # The proposal owner spells its net-improvement bar `min_net_improvement`;
    # the constraint kernel spells the same bar `min_switching_net_improvement`.
    # One value, two spellings - bind them so neither can drift.
    mni = _f((proposal.get("policy") or {}).get("min_net_improvement"))
    if mni is not None:
        pol["min_switching_net_improvement"] = mni
    return pol


def switching_hurdle(proposal: dict, policy: dict) -> Optional[float]:
    """The hurdle, preferring the value the owner PUBLISHED on this very proposal."""
    published = _f((proposal.get("switching_economics") or {}).get("switching_hurdle"))
    if published is not None:
        return published
    return _f(policy.get("min_switching_net_improvement"))


def score_cost_hurdle(two_way_turnover: Optional[float], policy: dict) -> Optional[float]:
    """Transaction cost expressed in score points, by the proposal owner's own
    formula: two-way turnover x round-trip bps x score points per bp."""
    t = _f(two_way_turnover)
    if t is None:
        return None
    return t * float(policy["round_trip_cost_bps"]) * float(policy["score_points_per_cost_bp"])


# --------------------------------------------------------------------------- #
# REPAIR OBLIGATIONS - what is actually wrong with the CURRENT book
# --------------------------------------------------------------------------- #
def _hoc_reviews(hoc_assessment: Optional[dict]) -> dict:
    return {r["ticker"]: r for r in ((hoc_assessment or {}).get("holding_reviews") or [])
            if r.get("ticker")}


#: Where the constraint CODE comes from when the opportunity-cost owner leaves it
#: unset. The owner spells its own governance codes; a code that names a
#: CONSTRAINT belongs to the constraint kernel's inventory, so it is attached
#: here rather than duplicated there.
_CONSTRAINT_CODE_FOR_REASON = {
    _hoc.OBLIGATION_REASON_UNIVERSE: _cr.C_ELIGIBLE_UNIVERSE,
    _hoc.OBLIGATION_REASON_LIQUIDITY: _cr.C_LIQUIDITY_PARTICIPATION,
}


def _as_tuple(got: Optional[dict]) -> Optional[tuple]:
    """Adapt one canonical obligation into this kernel's published row shape."""
    if not got:
        return None
    code = got.get("constraint_code") or _CONSTRAINT_CODE_FOR_REASON.get(
        got.get("reason_code"))
    return (got["obligation_type"], got["reason_code"], code, got["detail"])


def _retention_obligation(review: dict) -> Optional[tuple]:
    """``(tier, reason, constraint_code, detail)`` when the opportunity-cost owner
    has classified this holding as outside the book's rules, else ``None``.

    R63: the classification is now DELEGATED to the canonical mandatory-repair
    contract on the opportunity-cost owner, so the reallocation kernel and this
    review read one interpretation instead of two that happened to agree. The
    verdict was always HOC's - ``deterioration_state == BROKEN`` - and this
    function is now only the adapter onto the row shape published here.
    """
    return _as_tuple(_hoc.retention_obligation(review))


def _liquidity_obligation(review: dict) -> Optional[tuple]:
    """An ILLIQUID holding, by the opportunity-cost owner's own liquidity vocabulary.

    The review reads persisted evidence only, so it never re-derives an ADV figure
    to price a participation cap; it reuses the liquidity STATE the assessment
    published for that holding. Delegated to the canonical contract (R63).
    """
    return _as_tuple(_hoc.liquidity_obligation(review))


def held_book_risk_state(proposal: dict) -> dict:
    """The CURRENT book's per-name risk shares and limit, VERBATIM from the
    proposal artifact (the ``engine.holding_opportunity_cost`` contract measured
    them; this kernel re-measures nothing for the current book)."""
    risk = proposal.get("risk") or {}
    rcp = proposal.get("risk_contribution_policy") or {}
    limit_block = (rcp.get("limit_held_book") or risk.get("risk_contribution_limit_before")
                   or {})
    return {
        "contributions": dict(risk.get("risk_contributions_before") or {}),
        "limit": _f(limit_block.get("limit")),
        "limit_block": dict(limit_block),
        "breaches": list(rcp.get("held_book_breaches")
                         or risk.get("risk_contribution_breaches_before") or []),
        "state": limit_block.get("state"),
        "field": _hoc.RISK_CONTRIBUTION_FIELD,
        "owner": _hoc.RISK_CONTRIBUTION_POLICY_OWNER,
    }


def repair_obligations(*, proposal: dict, hoc_assessment: Optional[dict],
                       policy: dict) -> list:
    """Everything actually wrong with the CURRENT book, one row per obligation.

    Two tiers, each read from the owner that decides it:

      * HARD_CONSTRAINT_VIOLATION - a mandatory limit the current weights breach.
        Weight arithmetic is checked by the constraint kernel's own independent
        verifier; the per-name risk-contribution limit is checked by the
        opportunity-cost contract, because that verifier explicitly declares it
        cannot check that one.
      * GOVERNANCE_RETENTION_FAILURE - a holding the opportunity-cost decision
        policy has already classified as outside the book's retention or
        eligibility rules.

    Nothing here is a judgement of this kernel's own.
    """
    rows = _hoc_reviews(hoc_assessment)
    cur = current_weights(proposal)
    cands = candidate_rows(proposal)
    caps, binding = _cr.name_caps(candidates=cands, nav=_f(
        (proposal.get("portfolio") or {}).get("nav")), policy=policy)
    meta = _cr.candidate_meta(cands)
    sector_of = {c["ticker"]: (c.get("sector") or _cr.UNCLASSIFIED_SECTOR) for c in cands}

    out: list[dict] = []

    # 1. Weight arithmetic, by the kernel's own independent verifier.
    ver = _cr.verify_feasibility(weights=cur, caps=caps, sector_of=sector_of,
                                 current=cur, policy=policy, meta=meta)
    for v in (ver.get("violations") or []):
        code = v.get("code")
        tk = v.get("ticker")
        reason = CONSTRAINT_REASON.get(code, REASON_OTHER)
        if code == _cr.C_NAME_CAP and binding.get(tk) in (
                _cr.C_LIQUIDITY_PARTICIPATION, _cr.C_LIQUIDITY_FLOOR):
            code, reason = binding[tk], REASON_LIQUIDITY
        out.append({
            "ticker": tk,
            "tier": TIER_HARD,
            "primary_reason": reason,
            "constraint_code": code,
            "owner": _cr.CALCULATION_OWNER,
            "current_weight": _r(cur.get(tk), 8) if tk else None,
            "value": v.get("value"),
            "limit": v.get("limit"),
            "sector": v.get("sector"),
            "asset_class": v.get("asset_class"),
            "sleeve_id": v.get("sleeve_id"),
            "repair_action": (REPAIR_ACTION_EXIT
                              if (tk and (_f(caps.get(tk)) or 0.0) <= 0.0)
                              else REPAIR_ACTION_REDUCE),
            "detail": "The current book breaches %s." % code,
        })

    # 2. The per-name risk-contribution limit, by ITS owner's contract.
    rstate = held_book_risk_state(proposal)
    for b in rstate["breaches"]:
        out.append({
            "ticker": b.get("ticker"),
            "tier": TIER_HARD,
            "primary_reason": REASON_MANDATORY,
            "constraint_code": _cr.C_RISK_CONTRIBUTION,
            "owner": _hoc.RISK_CONTRIBUTION_POLICY_OWNER,
            "current_weight": _r(cur.get(b.get("ticker")), 8),
            "value": b.get(_hoc.RISK_CONTRIBUTION_FIELD),
            "limit": b.get("limit"),
            "excess": b.get("excess"),
            "repair_action": REPAIR_ACTION_REDUCE,
            "detail": ("%s carries %s of portfolio risk against a limit of %s "
                       "(%s times the equal-weight share of the %s names in the "
                       "covariance universe)."
                       % (b.get("ticker"), b.get(_hoc.RISK_CONTRIBUTION_FIELD),
                          b.get("limit"),
                          (rstate["limit_block"] or {}).get("excess_multiple"),
                          (rstate["limit_block"] or {}).get("n_covariance_names"))),
        })

    # 3. Governance: retention / eligibility / liquidity, by the HOC owner.
    for tk in sorted(cur):
        review = rows.get(tk) or {}
        for probe in (_retention_obligation, _liquidity_obligation):
            got = probe(review)
            if not got:
                continue
            tier, reason, code, detail = got
            out.append({
                "ticker": tk,
                "tier": tier,
                "primary_reason": reason,
                "constraint_code": code,
                "owner": _hoc.CALCULATION_OWNER,
                "current_weight": _r(cur.get(tk), 8),
                "current_rank": review.get("current_rank"),
                "deterioration_state": review.get("deterioration_state"),
                "hoc_recommendation": review.get("recommendation"),
                "reason_codes": list(review.get("reason_codes") or []),
                "repair_action": (REPAIR_ACTION_EXIT if reason in (
                    REASON_UNIVERSE, REASON_RETENTION)
                    else REPAIR_ACTION_NOT_SIZEABLE),
                "detail": detail,
            })

    out.sort(key=lambda r: (TIER_VOCAB.index(r["tier"]) if r["tier"] in TIER_VOCAB else 9,
                            r.get("constraint_code") or "", r.get("ticker") or ""))
    return out


# --------------------------------------------------------------------------- #
# THE MINIMUM REPAIR - a deterministic reduction, never a search
# --------------------------------------------------------------------------- #
def solve_minimum_repair(*, proposal: dict, obligations: list, policy: dict,
                         aligned_returns: Optional[dict] = None) -> dict:
    """The smallest valid book reachable from the CURRENT one, and how it got there.

    There is no optimisation in here. Names an obligation says cannot be held are
    exited; names above a limit are reduced TO that limit - for the risk
    contribution using the identical first-order rule the reallocation kernel
    applies (``w * limit / share``, re-measured afterwards by the canonical risk
    owner, exactly as that kernel does). Everything released becomes cash.

    ``aligned_returns`` is the canonical owned return panel. With it the repaired
    book's risk is RE-MEASURED after each round, because exiting names changes both
    the shares and the limit (3/N over a smaller covariance universe). Without it
    the repaired book's risk is not measured and the repair is reported as
    unverified rather than guessed.
    """
    cur = current_weights(proposal)
    cands = candidate_rows(proposal)
    nav = _f((proposal.get("portfolio") or {}).get("nav"))
    caps, _binding = _cr.name_caps(candidates=cands, nav=nav, policy=policy)
    meta = _cr.candidate_meta(cands)
    sector_of = {c["ticker"]: (c.get("sector") or _cr.UNCLASSIFIED_SECTOR) for c in cands}

    w = dict(cur)
    adjustments: list[dict] = []
    ceilings: dict[str, float] = {}

    def _note(ticker, code, action, before, after, reason, **kw):
        row = {"ticker": ticker, "constraint": code, "action": action,
               "primary_reason": reason,
               "before": _r(before, 8), "after": _r(after, 8),
               "released_weight": _r((before or 0.0) - (after or 0.0), 8),
               "released_to": RELEASED_CAPITAL_DESTINATION}
        row.update({k: v for k, v in kw.items() if v is not None})
        adjustments.append(row)

    # --- 1. exits the obligations require -------------------------------------- #
    for ob in obligations:
        tk = ob.get("ticker")
        if not tk or ob.get("repair_action") != REPAIR_ACTION_EXIT:
            continue
        before = w.get(tk, 0.0)
        if before <= _TOL:
            continue
        _note(tk, ob.get("constraint_code"), REPAIR_ACTION_EXIT, before, 0.0,
              ob.get("primary_reason"), tier=ob.get("tier"), owner=ob.get("owner"))
        w[tk] = 0.0
        ceilings[tk] = 0.0

    # --- 2. arithmetic limits: reduce to the limit ----------------------------- #
    for ob in obligations:
        tk = ob.get("ticker")
        code = ob.get("constraint_code")
        if (not tk or ob.get("repair_action") != REPAIR_ACTION_REDUCE
                or code == _cr.C_RISK_CONTRIBUTION):
            continue
        limit = _f(ob.get("limit"))
        before = w.get(tk, 0.0)
        if limit is None or before <= limit + _TOL:
            continue
        _note(tk, code, REPAIR_ACTION_REDUCE, before, limit, ob.get("primary_reason"),
              limit=_r(limit, 8), tier=ob.get("tier"), owner=ob.get("owner"))
        w[tk] = limit
        ceilings[tk] = limit

    # --- 3. the risk-contribution cap, re-measured round by round -------------- #
    max_rounds = int(policy.get("max_risk_contribution_repair_rounds", 3) or 3)
    rounds: list[dict] = []
    risk_state = RISK_NOT_MEASURED
    measured: dict = {}
    if aligned_returns:
        for rnd in range(max_rounds + 1):
            live = {tk: v for tk, v in w.items() if v > _TOL}
            measured = _rp.portfolio_volatility(weights=live,
                                                aligned_returns=aligned_returns,
                                                policy=policy)
            # A panel was supplied; that is not the same as a measurement. The
            # covariance owner decides, and when it could not measure, this repair
            # is UNVERIFIED and says so.
            if measured.get("state") != _rp.VOL_STATE_AVAILABLE:
                risk_state = RISK_NOT_MEASURED
                rounds.append({"round": rnd, "limit": None, "breaches": None,
                               "covariance_names": 0, "largest_share": None,
                               "state": measured.get("state")})
                break
            risk_state = RISK_MEASURED
            contributions = dict(measured.get("contributions") or {})
            limit_block = _hoc.risk_contribution_limit(
                n_covariance_names=len(measured.get("included_tickers") or []),
                policy=policy)
            limit = _f(limit_block.get("limit"))
            breaches = _hoc.risk_contribution_breaches(contributions=contributions,
                                                       limit=limit)
            rounds.append({"round": rnd, "limit": limit_block,
                           "breaches": breaches,
                           "covariance_names": len(measured.get("included_tickers") or []),
                           "largest_share": (max(contributions.values())
                                             if contributions else None)})
            # Clean, or out of rounds. Either way the LAST round's breach list is
            # what gets published, so a repair that ran out of rounds with a breach
            # still open is reported as open rather than as finished.
            if not breaches or rnd >= max_rounds:
                break
            for b in breaches:
                tk = b.get("ticker")
                share = _f(b.get(_hoc.RISK_CONTRIBUTION_FIELD))
                before = w.get(tk, 0.0)
                if not tk or share is None or share <= 0 or before <= _TOL:
                    continue
                # The SAME deterministic first-order reduction the reallocation
                # kernel uses. The exact risk is re-measured by the canonical owner
                # on the next round; this is not a second risk model.
                after = before * (float(limit) / share)
                _note(tk, _cr.C_RISK_CONTRIBUTION, REPAIR_ACTION_REDUCE, before, after,
                      REASON_MANDATORY, limit=_r(limit, 8),
                      risk_contribution=_r(share, 6), repair_round=rnd,
                      owner=_hoc.RISK_CONTRIBUTION_POLICY_OWNER)
                w[tk] = after
                ceilings[tk] = after

    final = {tk: round(v, 10) for tk, v in w.items() if v > _TOL}

    # --- 4. verify the repaired book, independently of the repair -------------- #
    ver = _cr.verify_feasibility(weights=final, caps=caps, sector_of=sector_of,
                                 current=cur, policy=policy, meta=meta)
    remaining = list(ver.get("violations") or [])
    # Never claimed as clean when it was never measured.
    rc_remaining = (rounds[-1]["breaches"] if rounds else None)
    if risk_state != RISK_MEASURED:
        rc_remaining = None

    repaired_codes = sorted({a.get("constraint") for a in adjustments if a.get("constraint")})
    return {
        "weights": final,
        "adjustments": adjustments,
        "weight_ceilings": {k: _r(v, 8) for k, v in sorted(ceilings.items())},
        "risk_repair_rounds": rounds,
        "risk_measurement_state": risk_state,
        "measured": measured,
        "verification": ver,
        "constraints_repaired": repaired_codes,
        "constraints_remaining": remaining,
        "risk_contribution_breaches_remaining": rc_remaining,
        "released_to": RELEASED_CAPITAL_DESTINATION,
        "repair_scope_version": REPAIR_SCOPE_VERSION,
        "doc": REPAIR_DOC,
        "deploys_capital": False,
        "adds_positions": False,
        "increases_positions": False,
        "is_an_optimiser": False,
    }


# --------------------------------------------------------------------------- #
# THE THREE COMPARABLE STATES
# --------------------------------------------------------------------------- #
def _material(delta: Optional[float], policy: dict) -> bool:
    d = _f(delta)
    band = float(policy.get("material_weight_delta", 1.0e-4) or 1.0e-4)
    return d is not None and abs(d) > band


def _change_count(before: dict, after: dict, policy: dict) -> int:
    return sum(1 for tk in (set(before) | set(after))
               if _material((after.get(tk) or 0.0) - (before.get(tk) or 0.0), policy))


def _delta_rows(*, before: dict, after: dict, proposal: dict, nav: Optional[float],
                policy: dict) -> list:
    """Allocation-shaped rows for the step ``before -> after``, so the canonical
    turnover / cost model can price it. ``cost_bps_per_side`` travels from the
    proposal's own row for that instrument, so a future or an FX leg is charged at
    its declared rate exactly as the proposal charges it."""
    bps = {a["ticker"]: a.get("cost_bps_per_side")
           for a in (proposal.get("allocations") or []) if a.get("ticker")}
    navv = _f(nav) or 0.0
    rows = []
    for tk in sorted(set(before) | set(after)):
        d = (after.get(tk) or 0.0) - (before.get(tk) or 0.0)
        if not _material(d, policy):
            continue
        rows.append({"ticker": tk, "action": "DELTA", "delta_weight": d,
                     "capital_change": d * navv, "cost_bps_per_side": bps.get(tk)})
    return rows


def price_step(*, before: dict, after: dict, proposal: dict, policy: dict) -> dict:
    """Price the step from one portfolio state to another with the canonical
    turnover / cost model. Used for CURRENT -> MINIMUM_REPAIR and for the
    MINIMUM_REPAIR -> FULL_TARGET increment."""
    nav = _f((proposal.get("portfolio") or {}).get("nav"))
    rows = _delta_rows(before=before, after=after, proposal=proposal, nav=nav,
                       policy=policy)
    block = _rp.turnover_and_cost(allocations=rows, nav=nav or 0.0, policy=policy,
                                  proposed_zero={}, selected={})
    # The kernel's turnover is derived from the rows it was given; state it against
    # the canonical pairwise measure too, so the two can never silently diverge.
    block["one_way_turnover"] = _r(_cr.one_way_turnover(before, after), 6)
    block["two_way_turnover"] = _r(2.0 * (_f(block["one_way_turnover"]) or 0.0), 6)
    block["trade_count"] = len(rows)
    return block


def _score_of(proposal: dict) -> dict:
    return {a["ticker"]: _f(a.get("score"))
            for a in (proposal.get("allocations") or []) if a.get("ticker")}


def _sector_of(proposal: dict) -> dict:
    return {a["ticker"]: (a.get("sector") or _cr.UNCLASSIFIED_SECTOR)
            for a in (proposal.get("allocations") or []) if a.get("ticker")}


#: Cash carries no variance and no covariance, so a book's NAV-level volatility is
#: its invested sleeve's volatility scaled by the invested share. Stated, not assumed.
VOLATILITY_CAPITAL_BASIS_ASSUMPTION = "CASH_CARRIES_NO_VARIANCE_AND_NO_COVARIANCE"
VOLATILITY_BASIS_INVESTED = "INVESTED_SLEEVE (the canonical covariance basis)"
VOLATILITY_BASIS_CAPITAL = "CAPITAL (NAV), invested sleeve scaled by the invested share"
#: The score divides by invested weight, so it says nothing about uninvested capital.
SCORE_BASIS = "COMBINED_PERCENTILE_NORMALISED_OVER_INVESTED_WEIGHT"
SCORE_EXCLUDES_UNINVESTED = (
    "The portfolio score is normalised over INVESTED weight, so capital held in cash "
    "carries no score either way. Cash has no percentile in the eligible universe and "
    "this review will not invent one for it. Where two states hold materially "
    "different uninvested capital their scores are NOT directly comparable, and that "
    "is reported rather than resolved: no calibrated expected return exists for cash "
    "or for anything else."
)


def _capital_basis_volatility(vol: Optional[float],
                              invested_weight: Optional[float]) -> Optional[float]:
    v, iw = _f(vol), _f(invested_weight)
    return None if (v is None or iw is None) else v * iw


def _economics(*, score: Optional[float], score_before: Optional[float],
               two_way: Optional[float], hurdle: Optional[float],
               policy: dict) -> dict:
    improvement = _sub(score, score_before)
    cost_points = score_cost_hurdle(two_way, policy)
    net = _sub(improvement, cost_points)
    return {
        "score": _r(score, 6),
        "score_improvement": _r(improvement, 6),
        "score_cost_hurdle": _r(cost_points, 6),
        "score_improvement_net_of_cost": _r(net, 6),
        "switching_hurdle": _r(hurdle, 6),
        "clears_switching_hurdle": (None if (net is None or hurdle is None)
                                    else bool(net >= hurdle)),
        "margin_above_hurdle": _r(_sub(net, hurdle), 6),
        "improvement_basis": _cr.IMPROVEMENT_BASIS,
        "expected_return": None,
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
    }


def build_current_state(*, proposal: dict, obligations: list) -> dict:
    """State A. Every number is the proposal artifact's own, verbatim."""
    risk = proposal.get("risk") or {}
    pf = proposal.get("portfolio") or {}
    se = proposal.get("switching_economics") or {}
    rstate = held_book_risk_state(proposal)
    return {
        "state": STATE_CURRENT,
        "label": STATE_LABELS[STATE_CURRENT],
        "source": "PROPOSAL_ARTIFACT_VERBATIM",
        "weights": current_weights(proposal),
        "positions": pf.get("current_holding_count"),
        "changes": 0,
        "one_way_turnover": 0.0,
        "two_way_turnover": 0.0,
        "estimated_cost": 0.0,
        "score": _f(se.get("score_before")),
        "score_improvement": 0.0,
        "score_cost_hurdle": 0.0,
        "score_improvement_net_of_cost": 0.0,
        "switching_hurdle": _f(se.get("switching_hurdle")),
        "clears_switching_hurdle": None,
        "margin_above_hurdle": None,
        "portfolio_volatility": _f(risk.get("portfolio_volatility_before")),
        "volatility_state": risk.get("volatility_before_state"),
        "volatility_basis": VOLATILITY_BASIS_INVESTED,
        "portfolio_volatility_capital_basis": _r(_capital_basis_volatility(
            _f(risk.get("portfolio_volatility_before")),
            1.0 - (_f(pf.get("current_cash_weight")) or 0.0)), 6),
        "volatility_capital_basis": VOLATILITY_BASIS_CAPITAL,
        "volatility_capital_basis_assumption": VOLATILITY_CAPITAL_BASIS_ASSUMPTION,
        "invested_weight": _r(1.0 - (_f(pf.get("current_cash_weight")) or 0.0), 6),
        "score_basis": SCORE_BASIS,
        "score_excludes_uninvested_capital": True,
        "concentration": _f(risk.get("concentration_before")),
        "largest_position": _f(risk.get("largest_position_before")),
        "sector_concentration": _f(risk.get("sector_concentration_before")),
        "cash_weight": _f(pf.get("current_cash_weight")),
        "allocation_by_asset_class": dict(pf.get("current_allocation_by_asset_class") or {}),
        "allocation_by_sleeve": dict(pf.get("current_allocation_by_sleeve") or {}),
        "risk_contributions": dict(rstate["contributions"]),
        "risk_contribution_limit": rstate["limit_block"],
        "risk_contribution_breaches": list(rstate["breaches"]),
        "expected_return": None,
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
        "constraint_status": {
            "valid": not obligations,
            "violations": [{"code": o.get("constraint_code"), "ticker": o.get("ticker"),
                            "tier": o.get("tier"), "reason": o.get("primary_reason")}
                           for o in obligations],
            # Doing nothing closes nothing, so EVERY obligation is open here. Spelled
            # on the same key the other two states use, or a surface reading that key
            # would render the unrepaired book as the clean one.
            "obligations_satisfied": 0,
            "obligations_remaining": [
                {"ticker": o.get("ticker"), "constraint_code": o.get("constraint_code"),
                 "tier": o.get("tier"), "primary_reason": o.get("primary_reason"),
                 "remaining_weight": o.get("current_weight"),
                 "detail": o.get("detail")} for o in obligations],
            "open_obligations": len(obligations),
            "hard_constraint_violations": sum(1 for o in obligations
                                              if o.get("tier") == TIER_HARD),
            "governance_retention_failures": sum(1 for o in obligations
                                                 if o.get("tier") == TIER_GOVERNANCE),
            "owner": "engine.constrained_reallocation + engine.holding_opportunity_cost",
        },
        "consequences_of_deferring": (
            "Nothing is traded and nothing is paid. Every open obligation above "
            "stays open and the book keeps carrying it until a later session "
            "repairs it." if obligations else
            "Nothing is traded and nothing is paid. The current book breaches no "
            "mandatory limit and holds no name outside the retention rules."),
    }


def build_full_target_state(*, proposal: dict, obligations: list, policy: dict) -> dict:
    """State C. The existing canonical proposal, read verbatim - never recomputed."""
    risk = proposal.get("risk") or {}
    pf = proposal.get("portfolio") or {}
    se = proposal.get("switching_economics") or {}
    turn = proposal.get("turnover") or {}
    ctl = proposal.get("complete_target_limits") or {}
    rcp = proposal.get("risk_contribution_policy") or {}
    cons = proposal.get("constraints") or {}
    tgt = target_weights(proposal)
    remaining = _unsatisfied_obligations(obligations=obligations, weights=tgt,
                                         rc_breaches=list(rcp.get("after_target_breaches")
                                                          or []),
                                         violations=list(cons.get("violations") or [])
                                         + list(ctl.get("breaches") or []))
    return {
        "state": STATE_FULL_TARGET,
        "label": STATE_LABELS[STATE_FULL_TARGET],
        "source": "PROPOSAL_ARTIFACT_VERBATIM",
        "weights": tgt,
        "positions": pf.get("proposed_holding_count"),
        "changes": _change_count(current_weights(proposal), tgt, policy),
        "one_way_turnover": _f(turn.get("one_way_turnover")),
        "two_way_turnover": _f(turn.get("two_way_turnover")),
        "estimated_cost": _f(turn.get("estimated_transaction_cost")),
        "score": _f(se.get("score_after")),
        "score_improvement": _f(se.get("score_improvement")),
        "score_cost_hurdle": _f(se.get("score_cost_hurdle")),
        "score_improvement_net_of_cost": _f(se.get("score_improvement_net_of_cost")),
        "switching_hurdle": _f(se.get("switching_hurdle")),
        "clears_switching_hurdle": se.get("clears_switching_hurdle"),
        "margin_above_hurdle": _r(_sub(_f(se.get("score_improvement_net_of_cost")),
                                       _f(se.get("switching_hurdle"))), 6),
        "portfolio_volatility": _f(risk.get("portfolio_volatility_after")),
        "volatility_state": risk.get("volatility_after_state"),
        "volatility_basis": VOLATILITY_BASIS_INVESTED,
        "portfolio_volatility_capital_basis": _r(_capital_basis_volatility(
            _f(risk.get("portfolio_volatility_after")),
            1.0 - (_f(pf.get("proposed_cash_weight")) or 0.0)), 6),
        "volatility_capital_basis": VOLATILITY_BASIS_CAPITAL,
        "volatility_capital_basis_assumption": VOLATILITY_CAPITAL_BASIS_ASSUMPTION,
        "invested_weight": _r(1.0 - (_f(pf.get("proposed_cash_weight")) or 0.0), 6),
        "score_basis": SCORE_BASIS,
        "score_excludes_uninvested_capital": True,
        "concentration": _f(risk.get("concentration_after")),
        "largest_position": _f(risk.get("largest_position_after")),
        "sector_concentration": _f(risk.get("sector_concentration_after")),
        "cash_weight": _f(pf.get("proposed_cash_weight")),
        "allocation_by_asset_class": dict(pf.get("proposed_allocation_by_asset_class") or {}),
        "allocation_by_sleeve": dict(pf.get("proposed_allocation_by_sleeve") or {}),
        "risk_contributions": dict(risk.get("risk_contributions_after") or {}),
        "risk_contribution_limit": dict(rcp.get("limit_after_target")
                                        or risk.get("risk_contribution_limit_after") or {}),
        "risk_contribution_breaches": list(rcp.get("after_target_breaches") or []),
        "expected_return": None,
        "expected_return_state": se.get("expected_return_state")
                                 or EXPECTED_RETURN_STATE_NOT_CALIBRATED,
        "constraint_status": {
            "valid": bool(ctl.get("all_ok")) and bool(cons.get("all_ok")),
            "violations": list(cons.get("violations") or []) + list(ctl.get("breaches") or []),
            "binding_constraints": list(
                (proposal.get("constraint_reoptimization") or {}).get(
                    "constraints_that_reshaped") or []),
            "breached_limits_before_reoptimization": list(
                (proposal.get("constraint_reoptimization") or {}).get(
                    "breached_limits") or []),
            "obligations_satisfied": len(obligations) - len(remaining),
            "obligations_remaining": remaining,
            "owner": ctl.get("owner") or "engine.reallocation_proposal",
        },
    }


def _unsatisfied_obligations(*, obligations: list, weights: dict,
                             rc_breaches: Optional[list],
                             violations: list) -> list:
    """Which of the CURRENT book's obligations this state does NOT close.

    Satisfaction is judged on the RESULTING BOOK, never by matching trades: the
    full target closes the Sep-18-style risk-contribution breach through its own
    composition change without trading that name at all, and a trade-matching rule
    would have called that unrepaired.
    """
    breached_names = {b.get("ticker") for b in (rc_breaches or [])}
    violated = {(v.get("code"), v.get("ticker")) for v in (violations or [])}
    out = []
    for o in obligations:
        tk = o.get("ticker")
        code = o.get("constraint_code")
        if o.get("repair_action") == REPAIR_ACTION_EXIT:
            satisfied = (_f(weights.get(tk)) or 0.0) <= _TOL
        elif o.get("repair_action") == REPAIR_ACTION_NOT_SIZEABLE:
            # The obligation stands until an owner that CAN size it closes it.
            satisfied = False
        elif code == _cr.C_RISK_CONTRIBUTION:
            # Never claim satisfaction that was not measured.
            satisfied = (rc_breaches is not None) and (tk not in breached_names)
        else:
            satisfied = (code, tk) not in violated
        if not satisfied:
            out.append({"ticker": tk, "constraint_code": code, "tier": o.get("tier"),
                        "primary_reason": o.get("primary_reason"),
                        "remaining_weight": _r(weights.get(tk), 8),
                        "detail": o.get("detail")})
    return out


def build_repair_state(*, proposal: dict, repair: dict, obligations: list,
                       policy: dict, hurdle: Optional[float]) -> dict:
    """State B. Computed here - with canonical primitives only."""
    cur = current_weights(proposal)
    w = repair["weights"]
    se = proposal.get("switching_economics") or {}
    score_before = _f(se.get("score_before"))
    priced = price_step(before=cur, after=w, proposal=proposal, policy=policy)
    score = _cr.weighted_score(w, _score_of(proposal))
    econ = _economics(score=score, score_before=score_before,
                      two_way=priced.get("two_way_turnover"), hurdle=hurdle,
                      policy=policy)

    measured = repair.get("measured") or {}
    vol_state, vol = _rp.effective_volatility(
        measured, float(policy.get("min_volatility_coverage", 0.8) or 0.8)
    ) if repair.get("risk_measurement_state") == RISK_MEASURED else (
        RISK_NOT_MEASURED, None)
    rounds = repair.get("risk_repair_rounds") or []
    last = rounds[-1] if rounds else {}

    sector_w: dict[str, float] = {}
    sector_of = _sector_of(proposal)
    for tk, v in w.items():
        sector_w[sector_of.get(tk, _cr.UNCLASSIFIED_SECTOR)] = (
            sector_w.get(sector_of.get(tk, _cr.UNCLASSIFIED_SECTOR), 0.0) + v)
    known = [v for k, v in sector_w.items() if k != _cr.UNCLASSIFIED_SECTOR]

    meta = _cr.candidate_meta(candidate_rows(proposal))
    groups = _cr.group_weights({k: v for k, v in w.items() if v > 0}, meta)
    invested = sum(w.values())
    by_class = dict(groups["by_class"])
    by_class["CASH"] = max(0.0, 1.0 - invested)
    by_sleeve = dict(groups["by_sleeve"])
    by_sleeve["cash_usd"] = max(0.0, 1.0 - invested)

    remaining = _unsatisfied_obligations(
        obligations=obligations, weights=w,
        rc_breaches=repair.get("risk_contribution_breaches_remaining"),
        violations=list(repair.get("constraints_remaining") or []))
    return {
        "state": STATE_MINIMUM_REPAIR,
        "label": STATE_LABELS[STATE_MINIMUM_REPAIR],
        "source": "COMPUTED_BY_%s" % CALCULATION_OWNER,
        "weights": {k: _r(v, 8) for k, v in sorted(w.items())},
        "positions": len(w),
        "changes": priced.get("trade_count"),
        "one_way_turnover": _f(priced.get("one_way_turnover")),
        "two_way_turnover": _f(priced.get("two_way_turnover")),
        "estimated_cost": _f(priced.get("estimated_transaction_cost")),
        "cost_basis": priced.get("cost_basis"),
        "traded_notional": priced.get("traded_notional"),
        **econ,
        "portfolio_volatility": _r(vol, 6),
        "volatility_state": vol_state,
        "volatility_basis": VOLATILITY_BASIS_INVESTED,
        "portfolio_volatility_capital_basis": _r(
            _capital_basis_volatility(vol, invested), 6),
        "volatility_capital_basis": VOLATILITY_BASIS_CAPITAL,
        "volatility_capital_basis_assumption": VOLATILITY_CAPITAL_BASIS_ASSUMPTION,
        "invested_weight": _r(invested, 6),
        "score_basis": SCORE_BASIS,
        "score_excludes_uninvested_capital": True,
        "volatility_coverage": _r(_f(measured.get("covered_weight")), 6),
        "concentration": _r(_rp.herfindahl(w), 6),
        "largest_position": _r(_rp.largest_weight(w), 6),
        "sector_concentration": _r(max(known) if known else None, 6),
        "cash_weight": _r(max(0.0, 1.0 - invested), 6),
        "allocation_by_asset_class": {k: _r(v, 6) for k, v in sorted(by_class.items())},
        "allocation_by_sleeve": {k: _r(v, 6) for k, v in sorted(by_sleeve.items())},
        "risk_contributions": {k: _r(v, 6) for k, v in sorted(
            (measured.get("contributions") or {}).items())},
        "risk_contribution_limit": dict(last.get("limit") or {}),
        "risk_contribution_breaches": (
            repair.get("risk_contribution_breaches_remaining") or []),
        "risk_measurement_state": repair.get("risk_measurement_state"),
        "risk_repair_rounds": len([r for r in rounds if r.get("breaches")]),
        "expected_return": None,
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
        "repair_adjustments": repair.get("adjustments") or [],
        "released_to": repair.get("released_to"),
        "turnover_budget": _f(policy.get("max_one_way_turnover")),
        "turnover_budget_exceeded": bool(
            _f(priced.get("one_way_turnover")) is not None
            and _f(policy.get("max_one_way_turnover")) is not None
            and _f(priced.get("one_way_turnover")) >
            _f(policy.get("max_one_way_turnover")) + 1.0e-12),
        "constraint_status": {
            "valid": bool(repair.get("verification", {}).get("valid"))
                     and not remaining,
            "violations": list(repair.get("constraints_remaining") or []),
            "constraints_repaired": list(repair.get("constraints_repaired") or []),
            "obligations_satisfied": len(obligations) - len(remaining),
            "obligations_remaining": remaining,
            "verification": repair.get("verification"),
            "owner": _cr.CALCULATION_OWNER,
        },
    }


# --------------------------------------------------------------------------- #
# CHANGE CLASSIFICATION - one primary reason per proposed change
# --------------------------------------------------------------------------- #
#: The proposal's own per-row reason codes that mean "this is an opportunity".
_OPPORTUNITY_ROW_CODES = frozenset({
    "ELIGIBLE_TOP_CANDIDATE_NOT_HELD", "HOC_REPLACE", "REPLACEMENT_COUNTERPARTY_RETAINED",
    "QUALIFIED_REPLACEMENT_CLEARS_NET_THRESHOLD",
})


def _risk_constraint_reason(*, ticker: str, held_breaches: list) -> str:
    """RISK_CONTRIBUTION_CAP resolves by object: a name in breach on the book being
    judged is a mandatory repair; a name the cap merely holds below the limit is a
    risk reduction."""
    return (REASON_MANDATORY
            if ticker in {b.get("ticker") for b in (held_breaches or [])}
            else REASON_RISK)


def _kernel_adjustments(proposal: dict) -> dict:
    """``{ticker: [adjustment...]}`` from the reallocation kernel's own ledger."""
    out: dict[str, list] = {}
    for a in ((proposal.get("constraint_reoptimization") or {}).get(
            "constraint_adjustments") or []):
        tk = a.get("ticker")
        if tk:
            out.setdefault(tk, []).append(a)
    return out


def _accepted_legs(proposal: dict) -> dict:
    return {leg["ticker"]: leg for leg in
            ((proposal.get("constraint_reoptimization") or {}).get("turnover") or {}
             ).get("accepted_trades") or [] if leg.get("ticker")}


def classify_changes(*, proposal: dict, hoc_assessment: Optional[dict],
                     obligations: list, policy: dict) -> dict:
    """Classify every change the FULL TARGET proposes into exactly one primary
    reason, and split them into FORCED and DISCRETIONARY.

    Resolution order (declared, first match wins):

      1. the change discharges a repair obligation on that name  -> that obligation
      2. the reallocation kernel's own adjustment ledger attributes the REDUCTION
         to a constraint                                          -> that constraint
      3. the row or the opportunity-cost owner calls it an
         opportunity / replacement                                -> OPPORTUNITY_IMPROVEMENT
      4. the row is only a constraint re-optimisation of a held
         name the owner said to HOLD                              -> PORTFOLIO_REOPTIMIZATION
      5. otherwise                                                -> OTHER_EXISTING_CANONICAL_REASON

    A constraint that merely LIMITED THE SIZE of an increase is never promoted to
    the reason FOR the change: it is recorded as ``constraint_limited_size``.
    """
    rows = _hoc_reviews(hoc_assessment)
    adj = _kernel_adjustments(proposal)
    legs = _accepted_legs(proposal)
    held_breaches = held_book_risk_state(proposal)["breaches"]
    ob_by_ticker: dict[str, list] = {}
    for o in obligations:
        if o.get("ticker"):
            ob_by_ticker.setdefault(o["ticker"], []).append(o)
    nav = _f((proposal.get("portfolio") or {}).get("nav")) or 0.0
    hoc_policy = (hoc_assessment or {}).get("policy") or {}
    rc_bps = float(policy["round_trip_cost_bps"])
    pts_bp = float(policy["score_points_per_cost_bp"])
    reference = _f((((proposal.get("constraint_reoptimization") or {}).get("turnover")
                     or {}).get("ordering_reference_score")))

    changes: list[dict] = []
    for a in (proposal.get("allocations") or []):
        tk = a.get("ticker")
        d = _f(a.get("delta_weight"))
        if not tk or not _material(d, policy):
            continue
        cw = _f(a.get("current_weight")) or 0.0
        pw = _f(a.get("proposed_weight")) or 0.0
        row_codes = list(a.get("reason_codes") or [])
        review = rows.get(tk) or {}
        reductions = [x for x in adj.get(tk, [])
                      if (_f(x.get("after")) is not None and _f(x.get("before")) is not None
                          and _f(x.get("after")) < _f(x.get("before")) - _TOL)]
        limited = sorted({x.get("constraint") for x in reductions}) if d > 0 else []

        primary = None
        constraint_code = None
        owner = None
        partial = False
        # 1. does this change discharge - wholly or partly - an obligation on this
        #    name? A reduction of a holding the rules say to EXIT is still driven by
        #    that rule; the turnover budget simply did not fund the whole exit.
        for o in ob_by_ticker.get(tk, []):
            if d >= 0:
                continue
            exit_ob = o.get("repair_action") == REPAIR_ACTION_EXIT
            primary, constraint_code, owner = (o["primary_reason"],
                                               o.get("constraint_code"), o.get("owner"))
            partial = bool(exit_ob and pw > _TOL)
            break
        # 2. a reduction the kernel attributes to a constraint
        if primary is None and d < 0 and reductions:
            code = reductions[0].get("constraint")
            constraint_code, owner = code, _cr.CALCULATION_OWNER
            primary = (_risk_constraint_reason(ticker=tk, held_breaches=held_breaches)
                       if code == _cr.C_RISK_CONTRIBUTION
                       else CONSTRAINT_REASON.get(code, REASON_OTHER))
        # 3. an opportunity the owners already named
        if primary is None and (set(row_codes) & _OPPORTUNITY_ROW_CODES
                                or a.get("source_hoc_recommendation") in (
                                    _hoc.REC_ADD, _hoc.REC_REPLACE)):
            primary, owner = REASON_OPPORTUNITY, _hoc.CALCULATION_OWNER
        # 4. a pure constraint re-optimisation of a retained holding
        if primary is None and a.get("source_hoc_recommendation") == _hoc.REC_HOLD:
            primary, owner = REASON_REOPTIMIZATION, _cr.CALCULATION_OWNER
        if primary is None:
            primary, owner = REASON_OTHER, _cr.CALCULATION_OWNER

        forced = primary in FORCED_REASONS
        leg = legs.get(tk) or {}
        density = _f(leg.get("score_improvement_per_turnover_unit"))
        leg_turn = _f(leg.get("turnover_cost"))
        first_order = (density * 2.0 * leg_turn
                       if (density is not None and leg_turn is not None) else None)
        leg_cost_points = ((2.0 * leg_turn) * rc_bps * pts_bp
                           if leg_turn is not None else None)
        # A per-name hurdle exists only where an owner declared one: the
        # opportunity-cost owner publishes gross / net improvement and the
        # min_net_improvement bar for a HELD name. Nothing is invented for a
        # candidate that is not held.
        held = cw > _TOL
        per_name_hurdle = _f(hoc_policy.get("min_net_improvement")) if held else None
        gross = _f(review.get("gross_score_improvement")) if held else None
        net = _f(review.get("net_improvement")) if held else None

        changes.append({
            "ticker": tk,
            "action": a.get("action"),
            "asset_class": a.get("asset_class"),
            "sleeve_id": a.get("sleeve_id"),
            "sector": a.get("sector"),
            "current_weight": _r(cw, 6),
            "proposed_weight": _r(pw, 6),
            "delta_weight": _r(d, 6),
            "capital_change": _money(_f(a.get("capital_change"))),
            "primary_reason": primary,
            "forced_change": forced,
            "discretionary_change": not forced,
            "constraint_code": constraint_code,
            "reason_owner": owner,
            "partially_discharges_obligation": partial,
            "constraint_limited_size": limited,
            "proposal_reason_codes": row_codes,
            "source_hoc_recommendation": a.get("source_hoc_recommendation"),
            "rank": a.get("rank"),
            "rank_change": review.get("rank_change"),
            "previous_rank": review.get("previous_rank"),
            "score": _f(a.get("score")),
            "signal_state": review.get("deterioration_state"),
            "signal_reason_codes": list(review.get("reason_codes") or []),
            # Discretionary economics (nulls where no owner publishes a value).
            "gross_score_improvement": _r(gross, 6),
            "net_score_improvement": _r(net, 6),
            "per_name_hurdle": _r(per_name_hurdle, 6),
            "per_name_margin_above_hurdle": _r(_sub(net, per_name_hurdle), 6),
            "per_name_hurdle_state": (
                None if held else
                "NO_PER_NAME_HURDLE_DECLARED_FOR_A_NON_HELD_CANDIDATE"),
            "switching_cost_usd": _money(_f(review.get("switching_cost_usd"))) if held else
                                  _money(abs(_f(a.get("capital_change")) or 0.0)
                                         * float(policy["cost_rate_per_side"])),
            "switching_cost_bps": _f(review.get("switching_cost_bps")) if held else
                                  _f(a.get("cost_bps_per_side")),
            "first_order_score_contribution": _r(first_order, 6),
            "first_order_score_cost": _r(leg_cost_points, 6),
            "first_order_net_contribution": _r(_sub(first_order, leg_cost_points), 6),
            "first_order_basis": ("SCORE_IMPROVEMENT_PER_UNIT_OF_ONE_WAY_TURNOVER "
                                  "against the current book's weighted score %s"
                                  % (_r(reference, 6) if reference is not None else "n/a")),
            "first_order_only": True,
            "risk_contribution_before": _f(review.get(_hoc.RISK_CONTRIBUTION_FIELD)),
            "risk_contribution_after": _f(((proposal.get("risk") or {}).get(
                "risk_contributions_after") or {}).get(tk)),
        })

    changes.sort(key=lambda c: (not c["forced_change"],
                                REASON_VOCAB.index(c["primary_reason"]),
                                -(abs(c["delta_weight"] or 0.0)), c["ticker"]))
    by_reason: dict[str, int] = {}
    for c in changes:
        by_reason[c["primary_reason"]] = by_reason.get(c["primary_reason"], 0) + 1
    mandatory = [c for c in changes if c["forced_change"]]
    discretionary = [c for c in changes if not c["forced_change"]]
    return {
        "changes": changes,
        "mandatory_changes": mandatory,
        "discretionary_changes": discretionary,
        "mandatory_change_count": len(mandatory),
        "discretionary_change_count": len(discretionary),
        "by_primary_reason": dict(sorted(by_reason.items())),
        "reason_vocabulary": list(REASON_VOCAB),
        "forced_reasons": list(FORCED_REASONS),
        "forced_classification_note": (
            "The reallocation kernel treats a retention-rule exit as DISCRETIONARY "
            "when it orders trades against the turnover budget (its mandatory tier is "
            "reserved for names the eligible universe or a cap cannot hold at all). "
            "This review classifies it as governance-FORCED, because the "
            "opportunity-cost owner has already ruled the holding outside the book's "
            "retention rules. Both owners are named on every row so the two views "
            "can be told apart rather than blended."),
        "kernel_mandatory_turnover": (
            ((proposal.get("constraint_reoptimization") or {}).get("turnover") or {}
             ).get("mandatory_turnover")),
        "kernel_mandatory_tier_owner": _cr.CALCULATION_OWNER,
        "governance_tier_owner": _hoc.CALCULATION_OWNER,
        "resolution_order": [
            "REPAIR_OBLIGATION_ON_THIS_NAME",
            "KERNEL_CONSTRAINT_ADJUSTMENT_REDUCING_THIS_NAME",
            "OPPORTUNITY_OR_REPLACEMENT_NAMED_BY_AN_OWNER",
            "CONSTRAINT_REOPTIMIZATION_OF_A_RETAINED_HOLDING",
            "OTHER_EXISTING_CANONICAL_REASON",
        ],
        "owner": CALCULATION_OWNER,
    }


def withheld_changes(proposal: dict) -> dict:
    """What the FULL TARGET did NOT do, from the kernel's own deferral ledger."""
    turn = ((proposal.get("constraint_reoptimization") or {}).get("turnover") or {})
    deferred = list(turn.get("deferred_trades") or [])
    return {
        "deferred_trades": deferred,
        "deferred_trade_count": turn.get("deferred_trade_count") or len(deferred),
        "deferred_reasons": sorted({d.get("deferred_reason") for d in deferred
                                    if d.get("deferred_reason")}),
        "budget_binds": turn.get("budget_binds"),
        "budget": turn.get("budget"),
        "unbudgeted_one_way_turnover": turn.get("unbudgeted_one_way_turnover"),
        "mandatory_turnover": turn.get("mandatory_turnover"),
        "budget_subordinated_to_mandatory_constraints": turn.get(
            "budget_subordinated_to_mandatory_constraints"),
        "owner": _cr.CALCULATION_OWNER,
    }


# --------------------------------------------------------------------------- #
# MARGINAL ECONOMICS - does the FULL target earn its additional churn?
# --------------------------------------------------------------------------- #
def marginal_economics(*, proposal: dict, states: dict, policy: dict,
                       hurdle: Optional[float]) -> dict:
    """The three comparisons, plus the one that decides the verdict."""
    cur = states[STATE_CURRENT]
    rep = states[STATE_MINIMUM_REPAIR]
    full = states[STATE_FULL_TARGET]

    step = price_step(before=rep["weights"], after=full["weights"],
                      proposal=proposal, policy=policy)
    incr_improvement = _sub(full.get("score"), rep.get("score"))
    incr_cost_points = score_cost_hurdle(step.get("two_way_turnover"), policy)
    incr_net = _sub(incr_improvement, incr_cost_points)
    contract = incremental_hurdle_contract(
        switching_hurdle=hurdle,
        hurdle_owner=(proposal.get("switching_economics") or {}).get("owner")
        or _cr.CALCULATION_OWNER)

    def _vs_current(s: dict) -> dict:
        return {
            "score_improvement": _r(_sub(s.get("score"), cur.get("score")), 6),
            "score_cost_hurdle": _r(_f(s.get("score_cost_hurdle")), 6),
            "score_improvement_net_of_cost": _r(
                _f(s.get("score_improvement_net_of_cost")), 6),
            "clears_switching_hurdle": s.get("clears_switching_hurdle"),
            "one_way_turnover": _r(_f(s.get("one_way_turnover")), 6),
            "two_way_turnover": _r(_f(s.get("two_way_turnover")), 6),
            "estimated_cost": _money(_f(s.get("estimated_cost"))),
            "volatility_delta": _r(_sub(_f(s.get("portfolio_volatility")),
                                        _f(cur.get("portfolio_volatility"))), 6),
            "concentration_delta": _r(_sub(_f(s.get("concentration")),
                                           _f(cur.get("concentration"))), 6),
            "cash_delta": _r(_sub(_f(s.get("cash_weight")), _f(cur.get("cash_weight"))), 6),
            "positions_delta": (None if (s.get("positions") is None
                                         or cur.get("positions") is None)
                                else int(s["positions"]) - int(cur["positions"])),
            "volatility_delta_capital_basis": _r(_sub(
                _f(s.get("portfolio_volatility_capital_basis")),
                _f(cur.get("portfolio_volatility_capital_basis"))), 6),
            "invested_weight_delta": _r(_sub(_f(s.get("invested_weight")),
                                             _f(cur.get("invested_weight"))), 6),
            "score_comparability": _comparability(cur, s),
        }

    full_open = (full.get("constraint_status") or {}).get("obligations_remaining") or []
    rep_open = (rep.get("constraint_status") or {}).get("obligations_remaining") or []

    def _comparability(a: dict, b: dict) -> dict:
        """Is a score difference between these two states a like-for-like one?

        The band is the proposal owner's OWN materiality band - the same one it uses
        to decide whether a weight change counts at all. No new number is introduced
        to answer this question.
        """
        iw_a, iw_b = _f(a.get("invested_weight")), _f(b.get("invested_weight"))
        gap = None if (iw_a is None or iw_b is None) else abs(iw_a - iw_b)
        comparable = gap is not None and gap <= float(
            policy.get("material_weight_delta", 1.0e-4) or 1.0e-4)
        return {
            "comparable": bool(comparable),
            "invested_weight_gap": _r(gap, 6),
            "invested_weight": {a["state"]: _r(iw_a, 6), b["state"]: _r(iw_b, 6)},
            "band": _f(policy.get("material_weight_delta")),
            "band_owner": "engine.reallocation_proposal (material_weight_delta)",
            "code": (None if comparable else "SCORE_BASIS_EXCLUDES_UNINVESTED_CAPITAL"),
            "score_basis": SCORE_BASIS,
            "detail": (None if comparable else SCORE_EXCLUDES_UNINVESTED),
        }

    return {
        "minimum_repair_vs_current": _vs_current(rep),
        "full_target_vs_current": _vs_current(full),
        "full_target_vs_minimum_repair": {
            "additional_one_way_turnover": _r(_f(step.get("one_way_turnover")), 6),
            "additional_two_way_turnover": _r(_f(step.get("two_way_turnover")), 6),
            "additional_estimated_cost": _money(_f(step.get("estimated_transaction_cost"))),
            "additional_trade_count": step.get("trade_count"),
            "one_way_turnover_difference": _r(_sub(_f(full.get("one_way_turnover")),
                                                   _f(rep.get("one_way_turnover"))), 6),
            "estimated_cost_difference": _money(_sub(_f(full.get("estimated_cost")),
                                                     _f(rep.get("estimated_cost")))),
            "incremental_score_improvement": _r(incr_improvement, 6),
            "incremental_score_cost_hurdle": _r(incr_cost_points, 6),
            "incremental_score_improvement_net_of_cost": _r(incr_net, 6),
            "clears_incremental_hurdle": (None if (incr_net is None or hurdle is None)
                                          else bool(incr_net >= hurdle)),
            "margin_above_incremental_hurdle": _r(_sub(incr_net, hurdle), 6),
            "hurdle_contract": contract,
            "volatility_delta": _r(_sub(_f(full.get("portfolio_volatility")),
                                        _f(rep.get("portfolio_volatility"))), 6),
            "concentration_delta": _r(_sub(_f(full.get("concentration")),
                                           _f(rep.get("concentration"))), 6),
            "cash_delta": _r(_sub(_f(full.get("cash_weight")),
                                  _f(rep.get("cash_weight"))), 6),
            "positions_delta": (None if (full.get("positions") is None
                                         or rep.get("positions") is None)
                                else int(full["positions"]) - int(rep["positions"])),
            "volatility_delta_capital_basis": _r(_sub(
                _f(full.get("portfolio_volatility_capital_basis")),
                _f(rep.get("portfolio_volatility_capital_basis"))), 6),
            "score_comparability": _comparability(rep, full),
            "obligations_left_open_by_full_target": full_open,
            "obligations_left_open_by_minimum_repair": rep_open,
            "turnover_basis": ("ONE_WAY_TURNOVER_OF_THE_STEP_FROM_THE_REPAIRED_BOOK "
                               "TO THE FULL TARGET (the trading actually added), with "
                               "the plain difference of the two turnovers published "
                               "alongside it"),
            "cost_basis": step.get("cost_basis"),
        },
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
        "score_converted_to_dollars": False,
        "note": ("Score is a combined percentile, never a return. No dollar value is "
                 "attached to a score improvement here, and none is implied: expected "
                 "return stays NOT_CALIBRATED because no validated forecast exists."),
        "owner": CALCULATION_OWNER,
    }


# --------------------------------------------------------------------------- #
# HISTORICAL DECISION EVIDENCE - read, never recomputed, never an override
# --------------------------------------------------------------------------- #
def historical_evidence(*, outcome_evidence: Optional[dict],
                        classification: dict) -> dict:
    """The matured decision evidence Paper Trader already stores, mapped onto the
    kinds of change THIS proposal contains.

    It informs the review and never rewrites a historical decision, never changes
    the proposal and never changes the verdict: the canonical policy-intelligence
    owner itself publishes ``recommends_manual_review_only``.
    """
    base = {
        "available": False,
        "state": EVIDENCE_INSUFFICIENT,
        "evidence_sufficient": False,
        "buckets": [],
        "cautions": [],
        "informs_review": True,
        "overrides_proposal": False,
        "changes_verdict": False,
        "rewrites_history": False,
        "owner": "api.reassessment_outcomes / engine.reassessment_outcomes",
        "message": ("No matured decision-outcome evidence was available to this "
                    "review. EVIDENCE_INSUFFICIENT - no confidence is invented."),
    }
    ev = outcome_evidence or {}
    score = ev.get("scorecard") or {}
    intel = ev.get("policy_intelligence") or {}
    if ev.get("status") != "OK" or not score:
        return base

    counts = classification.get("by_primary_reason") or {}
    opportunity_changes = int(counts.get(REASON_OPPORTUNITY, 0)) + int(
        counts.get(REASON_REOPTIMIZATION, 0))
    exit_changes = sum(1 for c in (classification.get("changes") or [])
                       if (c.get("proposed_weight") or 0.0) <= _TOL)
    retained = sum(1 for c in (classification.get("changes") or [])
                   if (c.get("proposed_weight") or 0.0) > _TOL
                   and (c.get("current_weight") or 0.0) > _TOL)

    def _bucket(name, block, applies, note):
        b = dict(block or {})
        b.update({"evidence_class": name, "applies_to_changes": applies, "note": note})
        return b

    buckets = [
        _bucket("REPLACEMENT_AND_ADDITION", score.get("replacement_outcomes"),
                opportunity_changes,
                "Matured comparisons of a replacement against the incumbent it "
                "displaced. This proposal's discretionary changes are the same kind "
                "of decision."),
        _bucket("HOLD", score.get("hold_outcomes"), retained,
                "Matured comparisons of a HOLD against the best known alternative at "
                "the time - what doing nothing has historically cost or saved."),
        _bucket("EXIT", score.get("exit_outcomes"), exit_changes,
                "Matured forward returns of names that were exited. Whether the "
                "released capital did better elsewhere is a separate question and is "
                "not asserted."),
    ]
    controls = list(intel.get("controls") or [])
    cautions = []
    rep = score.get("replacement_outcomes") or {}
    if (rep.get("measured_spreads") or 0) > 0 and (rep.get("hit_rate") is not None
                                                   and rep["hit_rate"] < 0.5):
        cautions.append({
            "code": "REPLACEMENT_EVIDENCE_ADVERSE",
            "detail": ("Replacements have underperformed the incumbent they displaced "
                       "in %s of %s matured comparisons (mean spread %s)."
                       % (rep.get("losses"), rep.get("measured_spreads"),
                          rep.get("mean_spread"))),
        })
    for c in controls:
        if c.get("verdict") == "CONTROL_REGRET" and c.get("evidence_sufficient"):
            cautions.append({
                "code": "CONTROL_REGRET_REPORTED",
                "reason_code": c.get("reason_code"),
                "detail": ("%s withheld %s actions; %s of the %s measured would have "
                           "improved the book."
                           % (c.get("reason_code"), c.get("observations"),
                              c.get("control_cost_count"), c.get("measured_spreads"))),
            })
    if not ev.get("evidence_sufficient"):
        cautions.append({"code": EVIDENCE_INSUFFICIENT,
                         "detail": (score.get("evidence") or {}).get("interpretation")})
    return {
        **base,
        "available": True,
        "state": ev.get("evidence_state"),
        "evidence_sufficient": bool(ev.get("evidence_sufficient")),
        "matured_observations": (score.get("evidence") or {}).get("matured_observations"),
        "primary_horizon": score.get("primary_horizon"),
        "buckets": buckets,
        "withheld_action_controls": controls,
        "policy_state": intel.get("policy_state"),
        "policy_findings": list(intel.get("findings") or []),
        "cautions": cautions,
        "message": ev.get("message"),
    }


# --------------------------------------------------------------------------- #
# THE VERDICT - ONE deterministic recommendation for MANUAL REVIEW
# --------------------------------------------------------------------------- #
#: The ladder, declared in order. It was written before any proposal was scored
#: through it, it applies only bars an existing owner published, and it is
#: exhaustive: every input combination lands on exactly one rung.
VERDICT_LADDER = (
    ("1", VERDICT_BLOCKED_CONSTRAINT_OR_DATA,
     "The proposal is not reviewable as current, the kernel raised a true blocker, "
     "or the minimum repair could not be verified."),
    ("2", VERDICT_NO_CHANGE_REQUIRED,
     "Nothing obliges a change and the proposal moves nothing material."),
    ("3", VERDICT_FULL_TARGET_REVIEWABLE,
     "The full target resolves every mandatory repair obligation AND the step "
     "from the minimum repair to it clears the existing switching hurdle after "
     "the cost of the extra trading."),
    ("4", VERDICT_MINIMAL_REPAIR_PREFERRED,
     "A repair obligation is open and the full target's increment over the repair "
     "does not clear the hurdle."),
    ("5", VERDICT_DEFER_WEAK_INCREMENTAL_EDGE,
     "Nothing obliges a change and the proposed change does not clear the hurdle."),
)


def decide(*, read_state: Optional[str], proposal: dict, obligations: list,
           repair: dict, states: dict, margins: dict) -> dict:
    """Walk the ladder. Deterministic, total, and free of any threshold of its own."""
    codes: list[str] = []
    verdict = None

    outcome = proposal.get("outcome")
    blockers = list(proposal.get("blockers") or [])
    true_blockers = list((proposal.get("reallocation_outcome") or {}).get(
        "true_blockers") or [])
    repair_required = bool(obligations)
    repair_measured = (repair.get("risk_measurement_state") == RISK_MEASURED
                       or not (repair.get("adjustments") or []))
    repair_open = ((states[STATE_MINIMUM_REPAIR].get("constraint_status") or {}
                    ).get("obligations_remaining") or [])
    repair_verified = bool(repair_measured and not repair_open)

    # --- rung 1 ---------------------------------------------------------------- #
    if read_state in NON_REVIEWABLE_READ_STATES:
        codes.append("PROPOSAL_NOT_REVIEWABLE_%s" % read_state)
        verdict = VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    elif outcome == _cr.OUTCOME_TRUE_BLOCKER or blockers or true_blockers:
        codes.append("TRUE_BLOCKER_PRESENT")
        verdict = VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    elif not repair_measured:
        # A repair whose resulting risk was never measured is a repair nobody has
        # proved valid. It is refused rather than published as if it were clean.
        codes.append("MINIMUM_REPAIR_NOT_MEASURABLE")
        verdict = VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    elif repair_open:
        # Even the SMALLEST valid change set cannot close every obligation, so
        # there is no repair to recommend and no clean baseline to price the full
        # target against. Fail closed and name what is still open.
        codes.append("MINIMUM_REPAIR_INCOMPLETE")
        codes.extend(sorted({"UNREPAIRABLE_%s" % (o.get("constraint_code") or "OBLIGATION")
                             for o in repair_open}))
        verdict = VERDICT_BLOCKED_CONSTRAINT_OR_DATA

    incr = (margins or {}).get("full_target_vs_minimum_repair") or {}
    clears = incr.get("clears_incremental_hurdle")
    full_changes = int(states[STATE_FULL_TARGET].get("changes") or 0)
    # R63 - THE invariant: a full target may not be published as reviewable while
    # it knowingly leaves an obligation an owner has already ruled. Before this
    # release this was computed and reported as an observation that never changed
    # the verdict, which is precisely how the 2026-09-18 target could have been
    # recommended with LH and VLO still past the exit buffer. It now BINDS, and
    # the fall-through is the repair, never the unrepaired target.
    full_target_open = list(incr.get("obligations_left_open_by_full_target") or [])

    if verdict is None:
        codes.append("REPAIR_OBLIGATION_OPEN" if repair_required
                     else "NO_REPAIR_OBLIGATION")
        # --- rung 2 ------------------------------------------------------------ #
        if not repair_required and full_changes == 0:
            codes.append("NO_MATERIAL_CHANGE_PROPOSED")
            verdict = VERDICT_NO_CHANGE_REQUIRED
        # --- rung 3 ------------------------------------------------------------ #
        elif clears is True and not full_target_open:
            codes.append("INCREMENTAL_NET_IMPROVEMENT_CLEARS_HURDLE")
            verdict = VERDICT_FULL_TARGET_REVIEWABLE
        elif full_target_open:
            codes.append("FULL_TARGET_NOT_REVIEWABLE_OBLIGATIONS_OPEN")
            codes.extend(sorted({"UNRESOLVED_%s" % (o.get("constraint_code")
                                                    or o.get("primary_reason")
                                                    or "OBLIGATION")
                                 for o in full_target_open}))
            verdict = (VERDICT_MINIMAL_REPAIR_PREFERRED if repair_required
                       else VERDICT_DEFER_WEAK_INCREMENTAL_EDGE)
        else:
            codes.append("INCREMENTAL_NET_IMPROVEMENT_BELOW_HURDLE" if clears is False
                         else "INCREMENTAL_NET_IMPROVEMENT_NOT_MEASURABLE")
            # --- rung 4 / 5 ---------------------------------------------------- #
            verdict = (VERDICT_MINIMAL_REPAIR_PREFERRED if repair_required
                       else VERDICT_DEFER_WEAK_INCREMENTAL_EDGE)

    # Observations that travel WITH the verdict without ever changing it. (The
    # one exception is the reviewability invariant above, which BINDS by design.)
    full_open = full_target_open
    if full_open:
        codes.append("FULL_TARGET_LEAVES_OBLIGATIONS_OPEN")
    vd = _f(margins.get("full_target_vs_current", {}).get("volatility_delta"))
    if vd is not None and vd > 0:
        codes.append("FULL_TARGET_INCREASES_PORTFOLIO_RISK")
    elif vd is not None and vd < 0:
        codes.append("FULL_TARGET_REDUCES_PORTFOLIO_RISK")
    if states[STATE_MINIMUM_REPAIR].get("turnover_budget_exceeded"):
        codes.append("MINIMUM_REPAIR_EXCEEDS_TURNOVER_BUDGET")
    comp = (incr.get("score_comparability") or {})
    if comp.get("code"):
        codes.append(comp["code"])
    iw = _f(states[STATE_MINIMUM_REPAIR].get("cash_weight"))
    iw_cur = _f(states[STATE_CURRENT].get("cash_weight"))
    if iw is not None and iw_cur is not None and iw > iw_cur:
        codes.append("MINIMUM_REPAIR_HOLDS_UNINVESTED_CAPITAL")

    return {
        "verdict": verdict,
        "label": VERDICT_LABELS[verdict],
        "verdict_vocabulary": list(VERDICT_VOCAB),
        "reason_codes": codes,
        "ladder": [{"rung": r, "verdict": v, "condition": c} for r, v, c in VERDICT_LADDER],
        "inputs": {
            "read_state": read_state,
            "proposal_outcome": outcome,
            "repair_required": repair_required,
            "repair_obligation_count": len(obligations),
            "minimum_repair_verified": repair_verified,
            "minimum_repair_risk_measured": repair_measured,
            "minimum_repair_obligations_remaining": repair_open,
            "full_target_material_changes": full_changes,
            "full_target_clears_switching_hurdle":
                states[STATE_FULL_TARGET].get("clears_switching_hurdle"),
            "incremental_clears_hurdle": clears,
            "incremental_net_improvement":
                incr.get("incremental_score_improvement_net_of_cost"),
            "hurdle": (incr.get("hurdle_contract") or {}).get("hurdle"),
            "score_comparability": comp,
            "full_target_obligations_left_open": full_target_open,
            "full_target_reviewable": not full_target_open,
        },
        "review_policy_version": REVIEW_POLICY_VERSION,
        "repair_scope_version": REPAIR_SCOPE_VERSION,
        "decided_by": CALCULATION_OWNER,
        "decided_by_llm": False,
        "recommends_manual_review_only": True,
        "is_an_approval": False,
        "approves_proposal": False,
        "creates_order_plan": False,
        "confirms_order_plan": False,
        "executes": False,
        "creates_orders": False,
        "creates_fills": False,
        "promotes_model": False,
        "deploys_capital": False,
        "mutates_proposal": False,
        "manual_approval_still_required": True,
    }


# --------------------------------------------------------------------------- #
# TARGET SELECTABILITY (R63) - WHICH of the three states the operator may select
#
# The backend decides this and publishes the reason. A browser may render the
# answer but never derive it: a disabled control whose rule lives in JavaScript is
# a rule nobody can test and nobody can audit.
#
# Selecting is NOT approving. Nothing here approves a proposal, creates an order
# plan, creates an order or moves capital; it decides only which target the
# operator is ALLOWED to put in front of the existing Approve gate.
# --------------------------------------------------------------------------- #
SELECT_BLOCK_NOT_REVIEWABLE = "PROPOSAL_NOT_REVIEWABLE"
#: RE-EXPORTED from the contract owner, never re-spelled: the read seams refuse
#: an unrepaired target with this same code, so one refusal cannot be worded two
#: ways depending on which surface asked.
SELECT_BLOCK_OBLIGATIONS_OPEN = _hoc.OBLIGATIONS_UNRESOLVED
SELECT_BLOCK_REPAIR_UNVERIFIED = "MINIMUM_REPAIR_NOT_VERIFIED"
SELECT_BLOCK_REPAIR_UNMEASURED = "MINIMUM_REPAIR_RISK_NOT_MEASURED"
SELECT_BLOCK_NO_CHANGE = "TARGET_MOVES_NOTHING"
SELECT_BLOCKER_VOCAB = (SELECT_BLOCK_NOT_REVIEWABLE, SELECT_BLOCK_OBLIGATIONS_OPEN,
                        SELECT_BLOCK_REPAIR_UNVERIFIED, SELECT_BLOCK_REPAIR_UNMEASURED,
                        SELECT_BLOCK_NO_CHANGE)

#: Which state each verdict points at. A recommendation only - the operator
#: selects, and nothing here pre-selects anything.
VERDICT_RECOMMENDS = {
    VERDICT_FULL_TARGET_REVIEWABLE: STATE_FULL_TARGET,
    VERDICT_MINIMAL_REPAIR_PREFERRED: STATE_MINIMUM_REPAIR,
    VERDICT_DEFER_WEAK_INCREMENTAL_EDGE: STATE_CURRENT,
    VERDICT_NO_CHANGE_REQUIRED: STATE_CURRENT,
    VERDICT_BLOCKED_CONSTRAINT_OR_DATA: None,
}


def target_selection_options(*, states: dict, verdict: dict, margins: dict,
                             repair: dict, read_state: Optional[str]) -> dict:
    """The three targets with a SELECTABLE verdict and a named reason for each.

    Deterministic and total: every option lands on selectable or not, and a
    non-selectable option always carries at least one blocker the operator can
    read. Pure - this kernel holds no clock, so session freshness is applied by
    the read owner ON TOP of this and may only ever REMOVE selectability.
    """
    reviewable = read_state not in NON_REVIEWABLE_READ_STATES
    incr = (margins or {}).get("full_target_vs_minimum_repair") or {}
    full_open = list(incr.get("obligations_left_open_by_full_target") or [])
    repair_open = list((states[STATE_MINIMUM_REPAIR].get("constraint_status") or {}
                        ).get("obligations_remaining") or [])
    repair_measured = (repair.get("risk_measurement_state") == RISK_MEASURED
                       or not (repair.get("adjustments") or []))
    recommended = VERDICT_RECOMMENDS.get(verdict.get("verdict"))

    def _blockers(state: str) -> list:
        out = []
        if not reviewable:
            out.append({"code": SELECT_BLOCK_NOT_REVIEWABLE,
                        "detail": ("The standing proposal is not in a reviewable "
                                   "state (%s)." % read_state)})
        if state == STATE_MINIMUM_REPAIR:
            if not repair_measured:
                out.append({"code": SELECT_BLOCK_REPAIR_UNMEASURED,
                            "detail": ("The repaired book's risk was never "
                                       "measured, so the repair is not proven "
                                       "valid.")})
            if repair_open:
                out.append({"code": SELECT_BLOCK_REPAIR_UNVERIFIED,
                            "instruments": sorted({o.get("ticker") for o in repair_open
                                                   if o.get("ticker")}),
                            "detail": ("Even the smallest valid change set leaves "
                                       "%d obligation(s) open." % len(repair_open))})
        if state == STATE_FULL_TARGET and full_open:
            out.append({"code": SELECT_BLOCK_OBLIGATIONS_OPEN,
                        "instruments": sorted({o.get("ticker") for o in full_open
                                               if o.get("ticker")}),
                        "obligations": full_open,
                        "detail": ("%d mandatory repair obligation(s) remain "
                                   "unresolved: %s."
                                   % (len(full_open),
                                      ", ".join(sorted({str(o.get("ticker"))
                                                        for o in full_open}))))})
        return out

    options = []
    for state in STATE_ORDER:
        st = states[state] or {}
        blockers = _blockers(state)
        cs = st.get("constraint_status") or {}
        options.append({
            "target": state,
            "label": STATE_LABELS[state],
            "recommended": bool(recommended == state),
            "selectable": not blockers,
            "blockers": blockers,
            "blocker_codes": sorted({b["code"] for b in blockers}),
            # The facts the operator compares BEFORE selecting. Read from the
            # states this review already built; nothing is recomputed here.
            "positions": st.get("positions"),
            "changes": st.get("changes"),
            "one_way_turnover": st.get("one_way_turnover"),
            "estimated_cost": st.get("estimated_cost"),
            "score": st.get("score"),
            "score_improvement_net_of_cost": st.get("score_improvement_net_of_cost"),
            "portfolio_volatility": st.get("portfolio_volatility"),
            "portfolio_volatility_capital_basis":
                st.get("portfolio_volatility_capital_basis"),
            "concentration": st.get("concentration"),
            "largest_position": st.get("largest_position"),
            "cash_weight": st.get("cash_weight"),
            "mandatory_obligations_remaining": len(cs.get("obligations_remaining") or []),
            "obligations_remaining": list(cs.get("obligations_remaining") or []),
            "is_defer": state == STATE_CURRENT,
        })
    return {
        "owner": CALCULATION_OWNER,
        "decided_by_backend": True,
        "decided_by_llm": False,
        "options": options,
        "option_order": list(STATE_ORDER),
        "recommended_target": recommended,
        "auto_selected": False,
        "auto_selection_doc": ("The recommended target is identified, never "
                               "pre-selected. Selection is an explicit operator "
                               "act."),
        "blocker_vocabulary": list(SELECT_BLOCKER_VOCAB),
        "selectable_targets": sorted(o["target"] for o in options if o["selectable"]),
        "full_target_reviewable": not full_open,
        "reviewability_invariant":
            "FULL_TARGET_SELECTABLE => ALL_MANDATORY_REPAIR_OBLIGATIONS_RESOLVED",
        "selection_is_approval": False,
        "selection_creates_order_plan": False,
        "selection_creates_orders": False,
    }


# --------------------------------------------------------------------------- #
# THE EXPLANATION - assembled from reason codes and authoritative facts
# --------------------------------------------------------------------------- #
def _pct(x: Optional[float], nd: int = 1) -> str:
    return "n/a" if _f(x) is None else "%.*f%%" % (nd, float(x) * 100.0)


def _num(x: Optional[float], nd: int = 3) -> str:
    return "n/a" if _f(x) is None else "%.*f" % (nd, float(x))


def _usd(x: Optional[float]) -> str:
    return "n/a" if _f(x) is None else "$%s" % format(float(x), ",.2f")


def _plural(n: int, one: str, many: Optional[str] = None) -> str:
    return "%d %s" % (n, one if n == 1 else (many or one + "s"))


def explain(*, verdict: dict, states: dict, obligations: list, classification: dict,
            margins: dict, evidence: dict, withheld: dict) -> dict:
    """The operator's paragraph. Every sentence is generated from a structured fact
    or reason code above - there is no template of a conclusion anywhere, and no
    language model is consulted at runtime or at build time."""
    cur = states[STATE_CURRENT]
    rep = states[STATE_MINIMUM_REPAIR]
    full = states[STATE_FULL_TARGET]
    incr = margins.get("full_target_vs_minimum_repair") or {}
    codes = list(verdict.get("reason_codes") or [])
    paras: list[str] = []

    hard = [o for o in obligations if o.get("tier") == TIER_HARD]
    gov = [o for o in obligations if o.get("tier") == TIER_GOVERNANCE]

    # 1. what is wrong with doing nothing
    if not obligations:
        paras.append(
            "Doing nothing breaches no mandatory limit: the current portfolio holds "
            "%s and every one of them is inside the book's constraints and retention "
            "rules. Deferring costs nothing and leaves no obligation open."
            % _plural(int(cur.get("positions") or 0), "position"))
    else:
        parts = []
        if hard:
            parts.append("%s against a hard portfolio limit (%s)"
                         % (_plural(len(hard), "breach", "breaches"),
                            ", ".join(sorted({o.get("constraint_code") or "?"
                                              for o in hard}))))
        if gov:
            parts.append("%s outside the governed retention / eligibility rules (%s)"
                         % (_plural(len(gov), "holding"),
                            ", ".join(sorted({o.get("ticker") or "?" for o in gov}))))
        paras.append(
            "Doing nothing is not free. The current portfolio carries %s. "
            "No cash is spent, but every one of those obligations stays open until a "
            "later session repairs it." % " and ".join(parts))

    # 2. what the minimum repair costs
    if obligations:
        paras.append(
            "The minimum repair closes %s of them with %s: one-way turnover %s at an "
            "estimated %s, leaving %s and %s cash (from %s). Every dollar it frees "
            "goes to cash - a repair restores validity and never deploys capital."
            % ((rep.get("constraint_status") or {}).get("obligations_satisfied"),
               _plural(int(rep.get("changes") or 0), "change"),
               _pct(rep.get("one_way_turnover")), _usd(rep.get("estimated_cost")),
               _plural(int(rep.get("positions") or 0), "position"),
               _pct(rep.get("cash_weight")), _pct(cur.get("cash_weight"))))
        if "MINIMUM_REPAIR_HOLDS_UNINVESTED_CAPITAL" in codes:
            paras.append(
                "That cash balance is the CONSEQUENCE of repairing the book, not a "
                "recommendation to hold it: the repair is defined never to deploy "
                "capital, so deciding where the released %s should go is the "
                "allocator's question and the reason a full target exists at all."
                % _pct(_sub(_f(rep.get("cash_weight")), _f(cur.get("cash_weight")))))
        if rep.get("turnover_budget_exceeded"):
            paras.append(
                "That repair alone needs %s of one-way turnover against a %s budget. "
                "The budget governs discretionary trading; it may not trap the book in "
                "an open obligation, so the repair is shown at its true size."
                % (_pct(rep.get("one_way_turnover")), _pct(rep.get("turnover_budget"))))

    # 3. what the full target adds
    mand = classification.get("mandatory_change_count") or 0
    disc = classification.get("discretionary_change_count") or 0
    paras.append(
        "The full target proposes %s: %s forced by a constraint or a governance rule "
        "and %s discretionary. It moves %s of one-way turnover for an estimated %s, "
        "and improves the portfolio score by %s, or %s net of that cost, against a "
        "hurdle of %s."
        % (_plural(int(full.get("changes") or 0), "change"), mand, disc,
           _pct(full.get("one_way_turnover")), _usd(full.get("estimated_cost")),
           _num(full.get("score_improvement")),
           _num(full.get("score_improvement_net_of_cost")),
           _num(full.get("switching_hurdle"))))

    # 4. the increment that decides it
    paras.append(
        "Against the minimum repair rather than against doing nothing, the full "
        "target adds %s of one-way turnover and %s of cost for %s of extra score, "
        "or %s net. The existing switching hurdle of %s applied to that increment is "
        "%s by %s."
        % (_pct(incr.get("additional_one_way_turnover")),
           _usd(incr.get("additional_estimated_cost")),
           _num(incr.get("incremental_score_improvement")),
           _num(incr.get("incremental_score_improvement_net_of_cost")),
           _num((incr.get("hurdle_contract") or {}).get("hurdle")),
           ("cleared" if incr.get("clears_incremental_hurdle") is True
            else "not cleared" if incr.get("clears_incremental_hurdle") is False
            else "not measurable"),
           _num(incr.get("margin_above_incremental_hurdle"))))

    comp = incr.get("score_comparability") or {}
    if comp.get("code"):
        paras.append(
            "Read that comparison with its basis in mind. The portfolio score is "
            "normalised over INVESTED weight, and these two states do not hold the "
            "same amount of invested capital: %s against %s. The repair's score "
            "advantage is an average over the capital it still has deployed and "
            "prices none of the %s it leaves in cash. Cash has no percentile in the "
            "eligible universe, this review will not invent one for it, and expected "
            "return is %s - so the score cannot settle, on its own, whether "
            "redeploying that capital is worth the extra churn."
            % (_pct((comp.get("invested_weight") or {}).get(STATE_MINIMUM_REPAIR)),
               _pct((comp.get("invested_weight") or {}).get(STATE_FULL_TARGET)),
               _pct(rep.get("cash_weight")),
               EXPECTED_RETURN_STATE_NOT_CALIBRATED))

    # 5. risk, on the basis that survives a difference in invested capital
    paras.append(
        "On capital, portfolio volatility is %s today, %s under the repair and %s "
        "under the full target (cash carries no variance, so this is the invested "
        "sleeve scaled by what is actually invested; on the invested sleeve alone "
        "the three read %s, %s and %s). Concentration moves from %s to %s under the "
        "repair and to %s under the full target. Expected return is %s: no validated "
        "forecast exists, so no dollar value is attached to any of this."
        % (_pct(cur.get("portfolio_volatility_capital_basis")),
           _pct(rep.get("portfolio_volatility_capital_basis")),
           _pct(full.get("portfolio_volatility_capital_basis")),
           _pct(cur.get("portfolio_volatility")), _pct(rep.get("portfolio_volatility")),
           _pct(full.get("portfolio_volatility")),
           _num(cur.get("concentration"), 4), _num(rep.get("concentration"), 4),
           _num(full.get("concentration"), 4),
           EXPECTED_RETURN_STATE_NOT_CALIBRATED))

    # 6. what the full target leaves open
    open_full = incr.get("obligations_left_open_by_full_target") or []
    if open_full:
        paras.append(
            "The full target does NOT close every obligation: %s remain open (%s), "
            "because the turnover budget deferred %s."
            % (_plural(len(open_full), "obligation"),
               ", ".join(sorted({str(o.get("ticker")) for o in open_full})),
               _plural(int(withheld.get("deferred_trade_count") or 0), "trade")))

    # 7. evidence
    if evidence.get("available"):
        bits = []
        for b in (evidence.get("buckets") or []):
            if b.get("evidence_class") == "EXIT":
                if (b.get("observations") or 0) > 0:
                    bits.append("exits %s avoided a loss and %s missed upside"
                                % (b.get("avoided_loss_count"),
                                   b.get("missed_upside_count")))
                continue
            if (b.get("measured_spreads") or 0) > 0:
                bits.append("%s %s win / %s loss"
                            % (b["evidence_class"].replace("_", " ").lower(),
                               b.get("wins"), b.get("losses")))
        paras.append((
            "Historical evidence (%s, %s matured observations at the %s-session "
            "horizon): %s. %s"
            % (evidence.get("state"), evidence.get("matured_observations"),
               evidence.get("primary_horizon"),
               "; ".join(bits) or "nothing measurable yet",
               " ".join(c.get("detail") or "" for c in (evidence.get("cautions") or []))
               )).strip())
        paras.append(
            "That evidence informs this review and changes nothing: it does not "
            "rewrite a past decision and it does not override the proposal.")
    else:
        paras.append(
            "%s - no matured decision-outcome evidence was available to this review, "
            "so no confidence is claimed from it." % EVIDENCE_INSUFFICIENT)

    # 8. the recommendation itself
    paras.append(
        "Recommended review path: %s. This is a recommendation for MANUAL REVIEW "
        "only. It approves nothing, creates no order plan, confirms nothing and "
        "executes nothing; manual approval remains mandatory."
        % verdict.get("verdict"))

    return {
        "headline": "%s - %s" % (verdict.get("verdict"), verdict.get("label")),
        "paragraphs": paras,
        "text": "\n\n".join(paras),
        "reason_codes": codes,
        "generated_by": CALCULATION_OWNER,
        "generated_from": "STRUCTURED_REASON_CODES_AND_AUTHORITATIVE_BACKEND_FACTS",
        "llm_used": False,
        "prompt_executed": False,
        "network_call": False,
        "deterministic": True,
    }


def _safety() -> dict:
    return {
        "read_only": True,
        "preview_only": True,
        "review_only": True,
        "manual_review": True,
        "manual_approval_required": True,
        "paper_only": True,
        "automation_off": True,
        "approves_proposal": False,
        "rejects_proposal": False,
        "supersedes_proposal": False,
        "mutates_proposal": False,
        "regenerates_proposal": False,
        "created_order_plan": False,
        "confirmed_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "performed_broker_execution": False,
        "created_operational_target": False,
        "confirmed_alpha_target": False,
        "promoted_model": False,
        "recalibrated_model": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_prediction": False,
        "called_provider": False,
        "called_llm": False,
        "safety_badges": ["PREVIEW ONLY", "REVIEW ONLY", "NO ORDERS",
                          "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW"],
    }


# --------------------------------------------------------------------------- #
# THE ONE ENTRY POINT
# --------------------------------------------------------------------------- #
def build_review(*, proposal: dict, hoc_assessment: Optional[dict] = None,
                 outcome_evidence: Optional[dict] = None,
                 aligned_returns: Optional[dict] = None,
                 read_state: Optional[str] = None,
                 identity: Optional[dict] = None) -> dict:
    """Adjudicate ONE persisted proposal. Pure: no I/O, no clock, no network.

    Called twice with the same inputs it returns the same payload, byte for byte -
    the review is a projection of evidence, never an event.
    """
    policy = review_policy(proposal)
    hurdle = switching_hurdle(proposal, policy)
    obligations = repair_obligations(proposal=proposal, hoc_assessment=hoc_assessment,
                                     policy=policy)
    repair = solve_minimum_repair(proposal=proposal, obligations=obligations,
                                  policy=policy, aligned_returns=aligned_returns)
    states = {
        STATE_CURRENT: build_current_state(proposal=proposal, obligations=obligations),
        STATE_MINIMUM_REPAIR: build_repair_state(proposal=proposal, repair=repair,
                                                 obligations=obligations, policy=policy,
                                                 hurdle=hurdle),
        STATE_FULL_TARGET: build_full_target_state(proposal=proposal,
                                                   obligations=obligations,
                                                   policy=policy),
    }
    classification = classify_changes(proposal=proposal, hoc_assessment=hoc_assessment,
                                      obligations=obligations, policy=policy)
    withheld = withheld_changes(proposal)
    margins = marginal_economics(proposal=proposal, states=states, policy=policy,
                                 hurdle=hurdle)
    evidence = historical_evidence(outcome_evidence=outcome_evidence,
                                   classification=classification)
    verdict = decide(read_state=read_state, proposal=proposal,
                     obligations=obligations, repair=repair, states=states,
                     margins=margins)
    explanation = explain(verdict=verdict, states=states, obligations=obligations,
                          classification=classification, margins=margins,
                          evidence=evidence, withheld=withheld)
    selection = target_selection_options(states=states, verdict=verdict,
                                         margins=margins, repair=repair,
                                         read_state=read_state)
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "calculation_owner": CALCULATION_OWNER,
        "review_policy_version": REVIEW_POLICY_VERSION,
        "repair_scope_version": REPAIR_SCOPE_VERSION,
        "reviewed_proposal": dict(identity or {}),
        "proposal_read_state": read_state,
        "states": states,
        "state_order": list(STATE_ORDER),
        "state_labels": dict(STATE_LABELS),
        "repair_obligations": obligations,
        "repair": {k: v for k, v in repair.items()
                   if k not in ("measured", "weights")},
        "change_classification": classification,
        "withheld_changes": withheld,
        "marginal_economics": margins,
        "historical_evidence": evidence,
        "review_verdict": verdict,
        "explanation": explanation,
        "target_selection": selection,
        "tier_vocabulary": list(TIER_VOCAB),
        "repair_action_vocabulary": list(REPAIR_ACTION_VOCAB),
        "runtime_llm_dependency": "NONE",
        "proposal_owner": "api.reallocation_proposal",
        "proposal_calculation_owner": _rp.CALCULATION_OWNER,
        "constraint_owner": _cr.CALCULATION_OWNER,
        "opportunity_cost_owner": _hoc.CALCULATION_OWNER,
        "evidence_owner": "api.reassessment_outcomes",
        "reuses_not_rebuilds": [
            "engine.constrained_reallocation (constraint inventory, name caps, "
            "instrument metadata, feasibility verification, turnover, concentration, "
            "portfolio score)",
            "engine.holding_opportunity_cost (risk-contribution contract, covariance, "
            "retention and liquidity verdicts)",
            "engine.reallocation_proposal (portfolio volatility, coverage gate, "
            "concentration, turnover and transaction-cost model)",
            "engine.reassessment_outcomes (matured decision evidence)",
        ],
        "second_proposal_engine": False,
        "second_opportunity_cost_engine": False,
        "second_risk_engine": False,
        "second_evidence_store": False,
        "safety": _safety(),
    }
