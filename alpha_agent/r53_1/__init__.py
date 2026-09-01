r"""Release 53.1 - intraday activation & alpha-to-capital conversion.

Release 53 built the machinery; Release 53.1 turns discoveries into operating
capability, four workstreams, one canonical allocator:

**Track A - continuous collection durability**
(:mod:`alpha_agent.r53_1.collection_runtime`). The R29 collection worker is
canonical and healthy in code; what failed on 2026-08-28 was the OPERATING
envelope (Interactive principal, logon-only trigger, no periodic recovery).
The durable Scheduled Task definition is derived here and installed/validated
by ``scripts\install_information_collection_task.ps1`` /
``scripts\validate_information_collection_task.ps1`` - the R52 installer
pattern applied to the collector. Research code never mutates a scheduler.

**Track B - true intraday data** (:mod:`alpha_agent.r53_1.feed_capability`,
:mod:`alpha_agent.r53_1.intraday_feed`, :mod:`alpha_agent.r53_1.intraday_signals`).
Every ALREADY-OWNED source is probed for current-session capability and
classified honestly (REAL_TIME / NEAR_REAL_TIME / DELAYED_INTRADAY /
DAILY_ONLY / NOT_ENTITLED) by the ONE lane owner
(:mod:`alpha_agent.r46.intraday`). The canonical adapter normalizes
current-session bars from ``engine.market_data`` (the declared canonical
Yahoo owner) and feeds the FROZEN R53 factory specs
(:mod:`alpha_agent.r53.intraday_factory`) - no second evidence system, no
spec retune, TRUE_FORWARD emission only inside a legal slot.

**Track C - risk budget, not risk veto**
(:mod:`alpha_agent.r53_1.risk_budget`). A SHADOW extension of the canonical
allocator answering "how much expected return per unit of incremental
portfolio risk, and is the new risk use superior to the current risk use?" -
explicit budgets, diversification-aware, hermetic, never a second allocator.

**Track D - executable at ~$99k NAV**
(:mod:`alpha_agent.r53_1.executable_universe`,
:mod:`alpha_agent.r53_1.short_capability`). Micro/mini contract feasibility
from the OWNED Norgate databases, ETF proxy classification
(SAME_THESIS_SAME_MARKET / PROXY_WITH_BASIS_RISK / NOT_EQUIVALENT), futures
risk-model research, and the long/short architecture assessment. SHADOW
research only; production stays long-only and unchanged.

RESEARCH ONLY. Paper only. No orders, no fills, no broker, no promotion, no
sleeve activation, no automation, no production-policy change, no operational
write, no scheduler mutation from research code, no purchase.
"""
from __future__ import annotations

from pathlib import Path

RELEASE = "R53.1"
CAMPAIGN_ID = "r53_1_intraday_activation_v1"
RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\active_risk_intraday_alpha_r53")
EVIDENCE_ROOT = Path(r"D:\Temp\paper_trader_release53_1_intraday_activation")

# Canonical helpers stay OWNED by the R46 package - same convention as R53.
from ..r46 import artifact_body, read_json, sha, write_json  # noqa: E402,F401

__all__ = [
    "RELEASE", "CAMPAIGN_ID", "RESEARCH_ROOT", "EVIDENCE_ROOT",
    "artifact_body", "read_json", "sha", "write_json",
    "research_dir", "safety_block",
    "collection_runtime", "feed_capability", "intraday_feed",
    "intraday_signals", "risk_budget", "executable_universe",
    "short_capability",
]


def research_dir() -> Path:
    d = RESEARCH_ROOT / CAMPAIGN_ID
    d.mkdir(parents=True, exist_ok=True)
    return d


def safety_block() -> dict:
    """The R53.1 safety declaration, embedded in every artifact this release
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
            "NO PURCHASE", "NO SCHEDULER MUTATION FROM RESEARCH CODE",
            "NO SHORT ACTIVATION",
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
        "second_market_data_owner_created": False,
        "activates_short_exposure": False,
    }
