"""alpha_agent.agents_v2 - PAPER_TRADER_ALPHA_AGENTS_V2.

The twelve quant research agents were born in a sibling repository under a
"Phase 8-A" contract: S&P 500, long-only, monthly, five price families, no
fundamentals, no regime work, "never touch Paper Trader", "no commit, no push",
Bash-oriented, and a signal-publishing agent that was preview-only forever.
That contract was right for the repository it governed and is WRONG here. It is
not edited and it is not silently overridden: this package is a NEW, versioned
identity with its own governance contract
(``research/agents/governance_contract.json``), which names every restriction
it retires, the authority that retired it, and every safety principle it keeps.

WHAT THIS PACKAGE IS
--------------------
The Claude agents ORCHESTRATE research. They own no durable state. Everything
that must survive a session is owned by the engine that already owns it:

    durable hypothesis identity, graveyard, burden   alpha_agent.r59.memory
    the ONE statistical qualification gate           alpha_agent.r59.engines.gate
    the search-burden denominator                    alpha_agent.r59.handlers
    the prospective freeze                           alpha_agent.r59.handlers
    forward adoption / registration / accrual        api.prospective_adoption ->
                                                     api.forward_challenger_registry ->
                                                     api.canonical_forward_accrual
    maturation                                       alpha_agent.r52.runtime
    capital eligibility                              api.capital_eligibility_gate

So this package adds NO second registry, NO second gate, NO second queue and NO
second forward clock. ``pipeline`` is a governed PROJECTION over the one
research memory: it enforces who may hand what to whom, and it journals every
handoff in the memory's own append-only ``events`` table.

RESEARCH ONLY. Nothing here can create an order or a fill, enable a broker,
promote a model, approve a portfolio proposal or make a sleeve capital
eligible. Like every R59 module it never imports the application layer; the
governed adoption owner is INJECTED by the entrypoint that composes them.
"""
from __future__ import annotations

from pathlib import Path

AGENT_SYSTEM_VERSION = "PAPER_TRADER_ALPHA_AGENTS_V2"
SUPERSEDES = "PHASE_8A_NORGATE_RESEARCH_ENGINE_V1"
SOURCE_REPOSITORY = r"C:\Users\binis\Stock_Prediction_app_push"
AUTHORISING_RUN_ID = "PORT_AND_ACTIVATE_PAPER_TRADER_ALPHA_AGENTS_R56_V1"

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "research" / "agents"
AGENT_DEFINITION_DIR = REPO_ROOT / ".claude" / "agents"

# --------------------------------------------------------------------------- #
# The roster, in canonical orchestration order.
# --------------------------------------------------------------------------- #
DIRECTOR = "quant-research-director"
DATA_FOUNDATION = "data-foundation-agent"
UNIVERSE = "universe-construction-agent"
FEATURES = "feature-library-agent"
MOMENTUM = "momentum-signal-agent"
REVERSAL = "reversal-signal-agent"
TREND_BREADTH = "trend-breadth-signal-agent"
VOL_LIQUIDITY = "volatility-liquidity-agent"
SKEPTIC = "validation-skeptic-agent"
RISK = "risk-portfolio-agent"
META = "meta-model-ensemble-agent"
PUBLISHING = "signal-publishing-agent"

SIGNAL_AGENTS = (MOMENTUM, REVERSAL, TREND_BREADTH, VOL_LIQUIDITY)
ROSTER = (DIRECTOR, DATA_FOUNDATION, UNIVERSE, FEATURES, *SIGNAL_AGENTS,
          SKEPTIC, RISK, META, PUBLISHING)

CONTRACT_FILES = (
    "governance_contract.json",
    "agent_manifest.json",
    "agent_contracts.json",
    "experiment_registry_schema.json",
    "handoff_contracts.json",
    "validation_gate_schema.json",
    "research_director_protocol.json",
)

# --------------------------------------------------------------------------- #
# Canonical owners. These are NAMES, never imports: a research module may not
# import the application layer, and naming an owner is how a contract points at
# an authority without acquiring it.
# --------------------------------------------------------------------------- #
RESEARCH_REGISTRY_OWNER = "alpha_agent.r59.memory.ResearchMemory"
QUALIFICATION_GATE_OWNER = "alpha_agent.r59.engines.gate"
SEARCH_BURDEN_OWNER = "alpha_agent.r59.handlers.search_denominator"
PROSPECTIVE_FREEZE_OWNER = "alpha_agent.r59.handlers.freeze_qualified"
FORWARD_ADOPTION_OWNER = "api.prospective_adoption"
#: The ONE operator door that starts a forward clock for a freeze that already
#: exists. The agents' entrypoint is deliberately NOT a second one: it freezes,
#: names this door and the challenger id, and stops.
OPERATOR_ADOPTION_ENTRYPOINT = "scripts/adopt_prospective_freeze.py"
FORWARD_REGISTRAR_OWNER = "api.forward_challenger_registry"
FORWARD_EVIDENCE_OWNER = "api.canonical_forward_accrual"
FORWARD_MATURATION_OWNER = "alpha_agent.r52.runtime"
CAPITAL_ELIGIBILITY_OWNER = "api.capital_eligibility_gate"
PORTFOLIO_PROPOSAL_OWNER = "api.reallocation_proposal"

#: How an agent-native experiment is labelled inside the ONE research memory.
RELEASE = "AGENTS_V2"
GENERATION_METHOD = "CLAUDE_AGENT_PREREGISTERED"
ORIGIN_PREFIX = "AGENTS_V2:"

#: The boundary the publishing agent stops at, stated once.
SIGNAL_PUBLISHING_BOUNDARY = (
    "RESEARCH_CANDIDATE_ARTIFACT + GOVERNED_PROSPECTIVE_REGISTRATION_REQUEST; "
    "never an order, a fill, a proposal approval, a champion promotion or a "
    "capital-eligibility decision")

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "broker_enabled": False,
    "promotes_model": False,
    "automatic_model_promotion_allowed": False,
    "approves_proposal": False,
    "makes_sleeve_capital_eligible": False,
    "mutates_operational_store": False,
    "automation_enabled": False,
    "backfills_forward_evidence": False,
    "manual_review_required": True,
    "shell": "WINDOWS_POWERSHELL_ONLY",
}
