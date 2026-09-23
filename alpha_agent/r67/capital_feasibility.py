r"""alpha_agent.r67.capital_feasibility - whose rule is stopping this trade?

R66 asked whether a ~$99k paper book can hold an institutional hedged structure
and answered it for eleven of them: 0 IMPLEMENTABLE, 1 SINGLE_POSITION_ONLY, 10
NOT_IMPLEMENTABLE_AT_THIS_NAV. Those verdicts are not disputed here and are not
recomputed.

What R66 did NOT do is say WHOSE RULE each verdict came from, and that omission
produced two errors that matter.

ERROR 1 - A PROVISIONAL CONVENTION WAS REPORTED AS A LAW OF NATURE
------------------------------------------------------------------
``MAX_GROSS_NOTIONAL_OVER_NAV = 1.0`` was declared inside R66's own feasibility
module, one screen above the code that applies it. No authorised portfolio
policy anywhere in ``api/`` contains it. R66 then described gross notional as
"a STRUCTURAL constraint set by exchange contract size against NAV, and no cash
policy changes it". The first half is true: contract size IS indivisible and
exchange-set. The second half smuggles in a threshold. That one contract is
large is a fact; that gross notional may not exceed 1.00x NAV is a CHOICE, and
it is a choice nobody with authority made. A futures book at 1.5x gross
notional is not physically impossible - it is outside a convention R66 invented.

The same applies to ``VARIATION_MARGIN_RESERVE_MULTIPLE = 1.0`` and
``HEDGE_ERROR_TOLERANCE = 0.10``. Both are defensible. Neither is authorised.

ERROR 2 - THE ONE CONSTRAINT THAT ACTUALLY BINDS WAS NEVER CHECKED
-------------------------------------------------------------------
Every one of R66's eleven structures has a SHORT leg. The operational book is
LONG ONLY, and this is not a convention - it is declared by the canonical owners
in two places:

    api.capital_pool          semantics.long_only = True
                              safety.short_exposure_supported = False
    api.capital_eligibility_gate
                              SHORT_LEG_RULE = "SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK"

and the gate already applies it to the two declared non-equity candidates. The
governance contract's retired-restriction R02 is explicit that long/short
RESEARCH is permitted but "a leg that cannot be expressed is reported, never
silently dropped".

So the binding constraint on every hedged structure R66 measured is one level
ABOVE notional and margin: the second leg cannot be held at ALL, at any size, at
any NAV, with any cash policy. R66 reported the blocker as "instrument
granularity, plus the absence of owned micro-contract data". That is the blocker
on the LONG leg's size. It is not the first blocker. Buying a smaller contract
does not make a short leg expressible in a long-only book.

WHAT THIS MODULE DOES
---------------------
It re-answers feasibility with every constraint attributed to one of four
owners, and it evaluates them in the order they actually bind:

    AUTHORISED_POLICY   a rule an owner in api/ declares. BINDS. Changing it is
                        a human governance decision.
    EXCHANGE            contract size, multiplier, indivisibility, margin as
                        published. BINDS and no policy can relieve it.
    RESEARCH_ASSUMPTION a modelling choice made to compute something. Does NOT
                        bind; it is reported so the reader can vary it.
    R66_CONVENTION      a threshold R66 declared for itself. Does NOT bind. It
                        is retained ONLY as a labelled sensitivity.

Verdicts under authorised constraints are the answer. Everything else is a
read-only sensitivity, and adopting any of it remains a human decision.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.r67.capital_feasibility"

# --------------------------------------------------------------------------- #
# Constraint provenance
# --------------------------------------------------------------------------- #
OWNER_POLICY = "AUTHORISED_POLICY"
OWNER_EXCHANGE = "EXCHANGE"
OWNER_RESEARCH = "RESEARCH_ASSUMPTION"
OWNER_R66 = "R66_CONVENTION"
CONSTRAINT_OWNERS = (OWNER_POLICY, OWNER_EXCHANGE, OWNER_RESEARCH, OWNER_R66)

#: Only these two OWNERS produce a binding verdict. The other two are reported.
BINDING_OWNERS = (OWNER_POLICY, OWNER_EXCHANGE)

#: Every constraint R66 applied, attributed. ``source`` is where the number
#: actually lives, so a reader can check the attribution rather than trust it.
CONSTRAINT_PROVENANCE = {
    "LONG_ONLY_BOOK": {
        "owner": OWNER_POLICY,
        "binds": True,
        "source": ("api.capital_pool semantics.long_only=True and "
                   "safety.short_exposure_supported=False; "
                   "api.capital_eligibility_gate.SHORT_LEG_RULE"),
        "statement": ("the operational book may hold no short position; a "
                      "structure with a short leg is not expressible at any "
                      "size, at any NAV, under any cash policy"),
        "r66_checked_it": False,
        "note": ("this is the constraint that actually binds every hedged "
                 "structure R66 measured, and R66 never evaluated it"),
    },
    "MAX_NAME_WEIGHT": {
        "owner": OWNER_POLICY, "binds": True,
        "source": "api.opportunity_frontier policy.max_name_weight",
        "statement": "no single name may exceed this fraction of NAV",
        "r66_checked_it": False,
    },
    "MIN_POSITION_WEIGHT": {
        "owner": OWNER_POLICY, "binds": True,
        "source": "api.opportunity_frontier policy.min_position_weight",
        "statement": "a position below this weight is not worth holding",
        "r66_checked_it": False,
    },
    "CONTRACT_INDIVISIBILITY": {
        "owner": OWNER_EXCHANGE, "binds": True,
        "source": "futures_market_registry.json point_value per market",
        "statement": ("a futures contract is an indivisible unit; a hedge "
                      "ratio must be expressed in integers"),
        "r66_checked_it": True,
        "note": "R66 got this one exactly right and it is reused unchanged",
    },
    "INITIAL_MARGIN": {
        "owner": OWNER_EXCHANGE, "binds": True,
        "source": "futures_market_registry.json margin per market",
        "statement": "cash must cover initial margin per contract",
        "r66_checked_it": True,
        "limitation": ("CURRENT margin applied to a CURRENT question, which is "
                       "legitimate; it is NOT point-in-time and may never be "
                       "used to margin a historical position"),
    },
    "MAX_GROSS_NOTIONAL_OVER_NAV": {
        "owner": OWNER_R66, "binds": False,
        "source": ("research/agents/campaign_r66_hedged_rv/feasibility.py "
                   "MAX_GROSS_NOTIONAL_OVER_NAV = 1.0"),
        "statement": "gross notional may not exceed 1.00x NAV",
        "r66_checked_it": True,
        "note": ("declared by R66 for itself and reported as structural. No "
                 "authorised policy in api/ contains it. Retained ONLY as a "
                 "labelled sensitivity; it may not decide a verdict"),
    },
    "VARIATION_MARGIN_RESERVE": {
        "owner": OWNER_R66, "binds": False,
        "source": ("research/agents/campaign_r66_hedged_rv/feasibility.py "
                   "VARIATION_MARGIN_RESERVE_MULTIPLE = 1.0"),
        "statement": "hold initial margin plus 1.0x of it against variation",
        "r66_checked_it": True,
        "note": ("prudent and unauthorised. A real reserve should be sized "
                 "from the structure's own drawdown, not from a multiple of "
                 "margin, which is an exchange number and not a risk measure"),
    },
    "HEDGE_ERROR_TOLERANCE": {
        "owner": OWNER_RESEARCH, "binds": False,
        "source": ("research/agents/campaign_r66_hedged_rv/feasibility.py "
                   "HEDGE_ERROR_TOLERANCE = 0.10"),
        "statement": "an integer hedge within 10% of the intended ratio is faithful",
        "r66_checked_it": True,
    },
    "VOL_MATCHED_HEDGE_RATIO_AS_DV01_PROXY": {
        "owner": OWNER_RESEARCH, "binds": False,
        "source": "research/agents/campaign_r66_hedged_rv/feasibility.py hedge_ratio()",
        "statement": ("the hedge ratio equalises DAILY DOLLAR VOLATILITY, and "
                      "R66 claimed it is close to the DV01 ratio for two points "
                      "on one issuer's curve"),
        "r66_checked_it": True,
        "note": ("defensible WITHIN one curve and false ACROSS instruments. "
                 "R66's own Gate A is the proof: ZN against SR3 gave a median "
                 "vol-matched beta of 15.4 that ADDED variance. A volatility "
                 "hedge is not a DV01 hedge and this module never calls it one"),
    },
    "SPAN_SPREAD_CREDIT": {
        "owner": OWNER_RESEARCH, "binds": False,
        "source": "R66 SPAN_CREDIT_STATE = UNKNOWN_NOT_OWNED_ASSUMPTION_ONLY",
        "statement": "no exchange spread-credit table is owned; default credit 0",
        "r66_checked_it": True,
        "note": ("a 0% credit is the conservative UPPER BOUND on committed "
                 "capital and R66 was right to default to it"),
    },
}

# --------------------------------------------------------------------------- #
# Verdicts
# --------------------------------------------------------------------------- #
V_NOT_EXPRESSIBLE = "NOT_EXPRESSIBLE_UNDER_AUTHORISED_POLICY"
V_NOT_AT_THIS_NAV = "NOT_IMPLEMENTABLE_AT_THIS_NAV"
V_MARGIN_SHORT = "MARGIN_EXCEEDS_FREE_CASH"
V_IMPLEMENTABLE = "IMPLEMENTABLE_UNDER_AUTHORISED_POLICY"
VERDICTS = (V_NOT_EXPRESSIBLE, V_NOT_AT_THIS_NAV, V_MARGIN_SHORT, V_IMPLEMENTABLE)


def authorised_policy() -> dict:
    """The live authorised policy, read from its owners. Nothing is declared here."""
    from paper_trader.api.capital_pool import load_capital_pool
    from paper_trader.api import capital_eligibility_gate as G

    pool = load_capital_pool()
    sem, safety = pool.get("semantics") or {}, pool.get("safety") or {}
    out = {
        "long_only": bool(sem.get("long_only")),
        "short_exposure_supported": bool(safety.get("short_exposure_supported")),
        "short_leg_rule": G.SHORT_LEG_RULE,
        "nav_usd": pool.get("nav"),
        "free_cash_usd": pool.get("cash"),
        "available_capital_usd": pool.get("available_capital"),
        "collateral_usd": pool.get("collateral"),
        "valuation_date": pool.get("valuation_date"),
        "collateral_semantics": sem.get("collateral"),
        "policy_owner": "api.capital_pool + api.capital_eligibility_gate",
    }
    try:
        from paper_trader.api.opportunity_frontier import load_opportunity_frontier
        pol = (load_opportunity_frontier() or {}).get("policy") or {}
        out.update({
            "max_name_weight": pol.get("max_name_weight"),
            "min_position_weight": pol.get("min_position_weight"),
            "min_adv_dollar": pol.get("min_adv_dollar"),
            "frontier_policy_owner": "api.opportunity_frontier",
        })
    except Exception:                                        # noqa: BLE001
        out["frontier_policy_owner"] = "UNAVAILABLE"
    return out


def assess_structure(*, legs: list, policy: Optional[dict] = None,
                     label: str = "", r66_record: Optional[dict] = None) -> dict:
    """Is this structure holdable, and WHOSE rule decides?

    ``legs`` is a list of ``{"symbol", "side", "n_contracts",
    "notional_usd", "initial_margin_usd"}``. Nothing is priced here - the
    caller supplies the exchange facts (R66's ``market_facts`` is the owner of
    those) so this module stays a pure ruling over provenance.

    The evaluation ORDER is the point. A structure that cannot be expressed is
    refused before anyone computes whether its notional would have fitted,
    because reporting a notional verdict for a position the book may not hold
    invites exactly the misreading R66 produced.
    """
    pol = policy or authorised_policy()
    nav = float(pol.get("nav_usd") or 0.0)
    cash = float(pol.get("free_cash_usd") or 0.0)

    shorts = [l for l in legs if str(l.get("side", "")).upper() == "SHORT"]
    gross = sum(abs(float(l.get("notional_usd") or 0.0)) for l in legs)
    net = sum((1.0 if str(l.get("side", "")).upper() == "LONG" else -1.0)
              * abs(float(l.get("notional_usd") or 0.0)) for l in legs)
    margin = sum(float(l.get("initial_margin_usd") or 0.0) for l in legs)

    out = {
        "label": label,
        "calculation_owner": CALCULATION_OWNER,
        "nav_usd": nav, "free_cash_usd": cash,
        "n_legs": len(legs), "n_short_legs": len(shorts),
        "gross_notional_usd": gross,
        "gross_notional_over_nav": (gross / nav) if nav else None,
        "net_notional_usd": net,
        "net_notional_over_nav": (net / nav) if nav else None,
        "initial_margin_usd": margin,
        "margin_over_free_cash": (margin / cash) if cash else None,
        "valuation_date": pol.get("valuation_date"),
        "verdict_vocabulary": list(VERDICTS),
        "binding_owners": list(BINDING_OWNERS),
    }
    if r66_record is not None:
        out["r66_verdict"] = r66_record.get("verdict")

    # ---- 1. AUTHORISED POLICY, evaluated FIRST because it binds hardest ----
    if shorts and not pol.get("short_exposure_supported", False):
        return {**out,
                "verdict": V_NOT_EXPRESSIBLE,
                "binding_constraint": "LONG_ONLY_BOOK",
                "binding_constraint_owner": OWNER_POLICY,
                "not_expressible_legs": [
                    {"symbol": l.get("symbol"), "side": "SHORT",
                     "reason": pol.get("short_leg_rule")} for l in shorts],
                "explanation": (
                    "%d of %d legs are SHORT and the operational book declares "
                    "short_exposure_supported=false. The structure is not "
                    "expressible at ANY size, at ANY NAV, under ANY cash "
                    "policy. Notional and margin are reported below for "
                    "completeness and decide nothing."
                    % (len(shorts), len(legs))),
                "what_would_change_it": (
                    "a human governance decision to support short exposure in "
                    "the position contract and the NAV replay - NOT a cash "
                    "policy change, NOT a smaller contract, and NOT more NAV"),
                "governance_contract_note": (
                    "retired restriction R02 permits long/short RESEARCH; it "
                    "requires that an inexpressible leg be REPORTED, which is "
                    "what this row is"),
                "r66_reported_blocker_was": (
                    "instrument granularity and the absence of owned "
                    "micro-contract data - which is the constraint on the LONG "
                    "leg's SIZE, one level below this one")}

    # ---- 2. EXCHANGE constraints, which no policy can relieve ----
    if cash and margin > cash:
        return {**out, "verdict": V_MARGIN_SHORT,
                "binding_constraint": "INITIAL_MARGIN",
                "binding_constraint_owner": OWNER_EXCHANGE,
                "explanation": (
                    "initial margin $%.2f exceeds free cash $%.2f. This is an "
                    "exchange requirement on a real position, not a convention."
                    % (margin, cash)),
                "what_would_change_it": (
                    "holding more cash, a smaller contract, or a genuine "
                    "exchange spread credit - the estate owns no SPAN table so "
                    "the credit is unknown and assumed zero")}

    return {**out, "verdict": V_IMPLEMENTABLE,
            "binding_constraint": None,
            "binding_constraint_owner": None,
            "explanation": (
                "every leg is expressible under authorised policy and margin "
                "fits free cash. NOTE: this says the book COULD hold it, never "
                "that it SHOULD - capital eligibility is a separate gate that "
                "requires forward evidence."),
            "eligibility_note": (
                "holdability is necessary and not sufficient. "
                "api.capital_eligibility_gate requires 60 matured forward "
                "observations before any sleeve may hold capital")}


def sensitivity(structure: dict) -> dict:
    """What the NON-binding constraints would have said. Read-only, adopts nothing.

    Reported separately and explicitly labelled so that no number here can be
    mistaken for a verdict. This is the mandate's "read-only sensitivity under
    transparent alternative policies, without adopting them automatically".
    """
    gon = structure.get("gross_notional_over_nav")
    margin = structure.get("initial_margin_usd") or 0.0
    cash = structure.get("free_cash_usd") or 0.0
    return {
        "is_a_verdict": False,
        "adopted": False,
        "requires_human_approval_to_adopt": True,
        "r66_gross_notional_convention": {
            "threshold_over_nav": 1.0,
            "owner": OWNER_R66,
            "would_refuse": bool(gon is not None and gon > 1.0),
            "measured_over_nav": gon,
            "note": ("R66 reported this as structural. It is a threshold R66 "
                     "declared for itself and no authorised policy contains it"),
        },
        "r66_variation_margin_reserve": {
            "multiple": 1.0,
            "owner": OWNER_R66,
            "cash_required_usd": margin * 2.0,
            "would_refuse": bool(cash and margin * 2.0 > cash),
            "note": ("sizing a reserve as a multiple of MARGIN uses an exchange "
                     "number as a risk measure; a reserve sized from the "
                     "structure's own drawdown would be the honest version"),
        },
        "span_credit_band": {
            "owner": OWNER_RESEARCH,
            "credits_considered": [0.0, 0.5, 0.75],
            "committed_capital_usd": {
                "credit_0_pct": margin,
                "credit_50_pct": margin * 0.5,
                "credit_75_pct": margin * 0.25,
            },
            "state": "UNKNOWN_NOT_OWNED_ASSUMPTION_ONLY",
            "note": "0% is the conservative upper bound and is what binds above",
        },
    }
