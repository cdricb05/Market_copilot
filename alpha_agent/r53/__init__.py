r"""Release 53 - active risk, intraday alpha & cross-market capital offensive.

Three bottlenecks, one release, one canonical allocator:

**Track A - active risk appetite** (:mod:`alpha_agent.r53.risk_appetite`).
Are the production portfolio controls risk protection or alpha dilution?
Answered empirically: a binding-constraint census over the real governed
artifacts, plus a walk-forward policy-region study on the owned
survivorship-free panel that calls the CANONICAL constraint kernel
(``engine.constrained_reallocation``) with variant policies. Three named
SHADOW policies are frozen; the production policy is never touched.

**Track B - intraday alpha** (:mod:`alpha_agent.r53.intraday_factory`,
:mod:`alpha_agent.r53.runtime_status`). A prospective intraday challenger
framework: frozen specifications, an append-only chain-hashed prediction
ledger built on the SAME canonical desk primitives every ledger has used
since Phase 27, slot-based emission with first-emission-wins deduplication,
forfeiture as first-class state (mirroring R52), and a scoring contract that
never sums matured with mark-to-market (the R46.5 lesson). Emission is gated
on the canonical intraday-lane owner (:mod:`alpha_agent.r46.intraday`): while
that lane is DATA_BLOCKED the factory freezes specs and refuses to emit,
because a prediction stamped after its data is not prospective.

**Track C - cross-market capital** (:mod:`alpha_agent.r53.capital_competition`,
:mod:`alpha_agent.r53.multi_horizon_view`). The Alpha-to-capital bridge: for
every serious challenger sleeve, IF it were eligible today, how much capital
would the CURRENT canonical allocator give it - run hermetically through the
real owners (investability registry ``approvals=`` seam -> opportunity
frontier -> constrained-reallocation kernel), never through a second
allocator, and never writing an operational store.

**Latency** (:mod:`alpha_agent.r53.latency`). The event-to-decision latency
profile measured from the owned run manifests, with the dominant bottlenecks
named and a budget per operating mode.

RESEARCH ONLY. Paper only. No orders, no fills, no broker, no promotion, no
sleeve activation, no automation, no production-policy change, no operational
write. Portfolio mutation stays behind the existing manual governance gates.
"""
from __future__ import annotations

from pathlib import Path

RELEASE = "R53"
CAMPAIGN_ID = "r53_active_risk_intraday_alpha_v1"
RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\active_risk_intraday_alpha_r53")
EVIDENCE_ROOT = Path(r"D:\Temp\paper_trader_release53_active_risk_intraday_alpha")

# The canonical helpers are OWNED by the R46 package; re-exported here so every
# R53 module hashes and writes artifacts with the same conventions rather than
# growing a second implementation.
from ..r46 import artifact_body, read_json, sha, write_json  # noqa: E402,F401

__all__ = [
    "RELEASE", "CAMPAIGN_ID", "RESEARCH_ROOT", "EVIDENCE_ROOT",
    "artifact_body", "read_json", "sha", "write_json",
    "research_dir", "safety_block",
    "risk_appetite", "intraday_factory", "runtime_status",
    "capital_competition", "multi_horizon_view", "latency",
]


def research_dir() -> Path:
    d = RESEARCH_ROOT / CAMPAIGN_ID
    d.mkdir(parents=True, exist_ok=True)
    return d


def safety_block() -> dict:
    """The R53 safety declaration, embedded in every artifact this release
    writes. Every flag is load-bearing and asserted by the release tests."""
    return {
        "safety": [
            "RESEARCH ONLY", "PAPER ONLY", "PREVIEW ONLY", "SHADOW ONLY",
            "NO LIVE BROKER ORDERS", "NO ORDERS", "NO FILLS",
            "AUTOMATION OFF FOR THE PORTFOLIO", "MANUAL REVIEW",
            "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
            "NO SLEEVE ACTIVATION", "NO PRODUCTION POLICY CHANGE",
            "NO PORTFOLIO CYCLE CALL", "NO DAILY CLOSE",
            "NO BACKDATED FORWARD ROW", "NO BACKFILL",
            "NO PURCHASE", "NO SCHEDULER MUTATION",
        ],
        "mutates_production_policy": False,
        "mutates_holdings": False,
        "mutates_cash": False,
        "mutates_nav": False,
        "creates_order": False,
        "creates_fill": False,
        "creates_proposal": False,
        "approves_proposal": False,
        "promotes_model": False,
        "activates_sleeve": False,
        "calls_portfolio_cycle": False,
        "runs_daily_close": False,
        "backfills_predictions": False,
        "changes_scheduler": False,
        "may_spend_money": False,
        "writes_operational_store": False,
        "second_allocator_created": False,
        "second_forward_evidence_system_created": False,
    }
