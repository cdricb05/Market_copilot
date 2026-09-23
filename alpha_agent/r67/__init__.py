r"""alpha_agent.r67 - the research-to-capital projection.

R67 owns NOTHING durable. Every module here is a READ-ONLY PROJECTION over
owners that already exist:

    research estate      alpha_agent.r59.memory.ResearchMemory (read-only handle)
    forward registry     api.forward_challenger_registry
    forward accrual      api.canonical_forward_accrual
    capital pool / NAV   api.capital_pool -> api.paper_trading_desk.book_nav
    capital eligibility  api.capital_eligibility_gate
    frontier             api.opportunity_frontier
    contract specs       the R38 native futures market registry

WHY A PROJECTION AND NOT A NEW OWNER
------------------------------------
The estate's standing rule is that a second registry, gate, queue or forward
clock is a defect, not a feature. Everything R67 needs already has exactly one
owner; what did NOT exist was a place where those owners are read TOGETHER and
asked one question:

    given everything the estate has settled, everything it has registered
    forward, and what the book can actually hold - what is the best eligible
    use of capital, and what is the next legitimate action on everything else?

So R67 composes. It never computes a NAV, never mints an experiment id, never
emits a prediction, never charges burden and never writes an operational store.

THE THREE PROJECTIONS
---------------------
``forward_producer``   Which registered challengers can EVER reach the capital
                       gate, and on what date. This is the release's sharpest
                       finding: a registration with no cadence producer accrues
                       one observation against a sixty-observation floor.

``strategy_inventory`` The STRATEGY OPPORTUNITY INVENTORY - every economic
                       mechanism the estate has touched, deduplicated by
                       MECHANISM rather than by experiment name, carrying its
                       evidence, its failure reason and its next legitimate
                       action.

``capital_feasibility`` Can this book hold a given strategy? R66 answered this
                       for eleven hedged structures and got the right verdicts
                       for an incomplete reason. R67 re-answers it with every
                       constraint attributed to its OWNER, because an
                       authorised policy, an exchange rule, a research
                       assumption and a provisional convention are four
                       different things and only the first two bind.
"""
from __future__ import annotations

RELEASE = "R67"
RUN_ID = "R67_MULTI_STRATEGY_ALPHA_AND_PORTFOLIO_CAPITAL_ENGINE"

#: Everything in this package is read-only by construction.
SAFETY = {
    "research_only": True,
    "paper_only": True,
    "read_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "mutates_holdings": False,
    "mutates_operational_store": False,
    "promotes_model": False,
    "activates_sleeve": False,
    "charges_search_burden": False,
    "emits_forward_predictions": False,
}

__all__ = ["RELEASE", "RUN_ID", "SAFETY"]
