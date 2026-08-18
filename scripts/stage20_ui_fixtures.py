r"""scripts/stage20_ui_fixtures.py — Stage 20 / 20.1 hermetic UI-acceptance fixtures.

ONE SHARED SCENARIO CONTRACT (Stage 20.1, Workstream B)
=======================================================
Stage 20 seeded ONLY the portfolio-reassessment artifact. Every OTHER canonical surface
(operational book, rebalance lifecycle, holding opportunity cost, reallocation proposal,
daily close, workflow state) therefore read its OWN empty store in the acceptance root and
fell back to an unrelated default world. The rendered page could simultaneously claim
``PROPOSAL_READY`` with a visible REVIEW PORTFOLIO PROPOSAL button *and* ``Operational
Book: NOT INITIALIZED`` / ``0 pending orders`` / ``Run the Daily Close`` — two live mutation
CTAs describing two different worlds.

Stage 20.1 makes ONE ``World`` the sole source of truth for a scenario. Every canonical
panel payload is DERIVED from that single object by the REAL read owners:

    World  ->  seeded desk ledgers (book / marks / historical fills / lineage orders)
           ->  api.operational_book.load_operational_book      (lineage-aware counts)
           ->  api.rebalance_execution.load_rebalance_state    (Stage-19 lifecycle)
           ->  api.holding_opportunity_cost.load_holding_opportunity_cost
           ->  api.reallocation_proposal.load_reallocation_proposal
           ->  api.portfolio_reassessment.load_portfolio_reassessment
           ->  api.daily_action_gate.load_daily_action_gate
           ->  api.workflow_state.load_workflow_state          (ONE primary action)

No panel manufactures a state of its own, so a scenario cannot render two worlds at once.

Strictly offline and read-only with respect to LIVE state: it opens no live store, calls no
provider, contacts no prediction service, creates no live order, and writes only inside the
throwaway acceptance root it is given. It exists so the Playwright acceptance run never
touches live operational state — in particular the 29 SUBMITTED NEXT_CLOSE paper orders of
the current real rebalance.

Usage:
    python scripts/stage20_ui_fixtures.py --out <path.json>
    python scripts/stage20_ui_fixtures.py --seed-dir <root> --scenario scenario_5_execution_pending
    python scripts/stage20_ui_fixtures.py --check          # cross-panel consistency report
"""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paper_trader.api import alpha_book as ab
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import daily_close as dc
from paper_trader.api import daily_research_cycle as drcmod
from paper_trader.api import data_freshness as df
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import multi_horizon_ledger as mhz_ledger
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as prs
from paper_trader.api import reallocation_proposal as ralloc
from paper_trader.api import rebalance_execution as rbx
from paper_trader.api import workflow_state as wfs
from paper_trader.engine import portfolio_reassessment as kernel

#: The composition owner of every payload below. Stage 20.1 architecture guard: this module
#: is the ONE scenario owner for the hermetic acceptance environment.
SCENARIO_OWNER = "scripts/stage20_ui_fixtures.py"

BOOK = "alpha_paper_book_1"
BOOK_LABEL = "Alpha Paper Book #1"
DATE = "2026-08-12"
PREV = "2026-08-11"
NEXT = "2026-08-13"
NAV = 100000.0
INITIAL_CAPITAL = 100000.0
COST = desk.COST_RATE_PER_SIDE

# The live Stage-19 execution facts (SHAPE only — no live store is ever read).
LIVE_PLAN = "rbop_2026-08-12_alpha_paper_book_1_1a198f560cca"
LIVE_PLAN_HASH = "1a198f560cca5c7457e58f151b0e409b772b5ab85368a4f8bdf5eacc4d9315b9"
LIVE_PROPOSAL_HASH = "reap_2026_08_12_alpha_paper_book_1"
SUPERSEDED_PLAN = "rbop_2026-08-12_alpha_paper_book_1_5bf9c6c20f8a"
LIVE_SUBMITTED = 29
LIVE_BUYS = 15
LIVE_SELLS = 14
SUPERSEDED_CANCELLED = 22
HISTORICAL_FILLS = 25

#: The real August-12 book (25 held names) — the same universe Stage 19.2 reproduces.
HELD = ["ALAB", "AMD", "ANET", "APD", "CAH", "CAT", "DDOG", "DVA", "DVN", "EOG", "EXPD",
        "FANG", "FTNT", "GWW", "HST", "KEYS", "LYV", "MNST", "MO", "NDSN", "RCL", "TECH",
        "TT", "VLO", "XYZ"]
#: 15 BUY orders — 8 genuinely new names plus 7 increases in retained names.
BUY_ADDS = ["ABNB", "AIZ", "CVS", "DXCM", "EXPE", "ITW", "LH", "SPG"]
BUY_INCREASES = ["ALAB", "AMD", "ANET", "CAT", "DDOG", "DVA", "DVN"]
#: 14 SELL orders — 8 exits plus 6 reductions.
SELL_EXITS = ["APD", "CAH", "EXPD", "MO", "NDSN", "RCL", "TECH", "TT"]
SELL_REDUCES = ["EOG", "FANG", "FTNT", "GWW", "HST", "KEYS"]

_ALL_TICKERS = sorted(set(HELD) | set(BUY_ADDS) | {desk.BENCHMARK_TICKER})
#: Deterministic, varied prices so whole-share arithmetic is genuinely exercised.
PRICE = {tk: round(41.0 + 7.3 * i, 2) for i, tk in enumerate(_ALL_TICKERS)}

_SECTORS = ("Tech", "Fin", "Health", "Ind", "Energy")


# =========================================================================== #
# Synthetic Slice-6 assessment inputs (structurally faithful, deterministic).
# =========================================================================== #
def _review(ticker, **kw):
    base = {
        "ticker": ticker, "sector": "Tech", "current_quantity": 100,
        "current_weight": 0.04, "market_value": 0.04 * NAV,
        "current_rank": 10, "previous_rank": 10, "rank_change": 0,
        "current_score": 0.80, "score_components": {}, "signal_strength": 0.80,
        "deterioration_state": "STABLE", "deterioration_reason_codes": [],
        "return_5d": 0.01, "return_20d": 0.02, "return_60d": 0.05,
        "volatility_20d": 0.20, "volatility_60d": 0.22, "drawdown_60d": -0.05,
        "risk_contribution_pct": 0.04, "concentration_contribution": 0.04,
        "median_dollar_volume_20d": 5.0e7, "estimated_days_to_liquidate": 0.1,
        "liquidity_state": "LIQUID",
        "strongest_replacement_ticker": None, "replacement_rank": None,
        "replacement_score": None, "replacement_sector": None,
        "gross_score_improvement": None, "risk_adjusted_improvement": None,
        "switching_cost_bps": 25.0, "switching_cost_usd": 100.0,
        "net_improvement": None, "recommendation": "HOLD",
        "recommendation_confidence": "HIGH", "reason_codes": [], "explanation": "",
        "required_data_complete": True,
    }
    base.update(kw)
    return base


def _replace(ticker, *, gross, net, rank=62, prev=18, sector="Fin", weight=0.04):
    return _review(ticker, sector=sector, current_weight=weight,
                   market_value=weight * NAV, current_rank=rank, previous_rank=prev,
                   rank_change=prev - rank, recommendation="REPLACE",
                   deterioration_state="DETERIORATING",
                   deterioration_reason_codes=["RANK_WORSENED"],
                   drawdown_60d=-0.184, strongest_replacement_ticker="CVS",
                   replacement_rank=9, replacement_score=0.95,
                   replacement_sector="Health", gross_score_improvement=gross,
                   risk_adjusted_improvement=gross, net_improvement=net,
                   switching_cost_usd=weight * NAV * 0.0025)


def _filler(i):
    """One quiet, retained holding with a realistic sector."""
    return _review("H%02d" % i, sector=_SECTORS[i % len(_SECTORS)], current_rank=i + 1)


def _quiet_book():
    return [_filler(i) for i in range(25)]


#: Stage 22.1 — the EXACT live 2026-08-14 names whose owned trailing analytics were
#: absent, so the acceptance world is the real one rather than a plausible-looking one.
LIVE_INCOMPLETE_HOLDINGS = ("AIZ", "DVA", "DVN", "DXCM", "EXPE", "FANG", "HST", "LH",
                            "LYV", "XYZ")


def _incomplete(ticker, i):
    """A holding whose REQUIRED point-in-time analytics genuinely do not exist.

    This is the live gap shape verbatim: the name is ranked and scored (it IS in the
    owned universe), but it has no trailing price history in the panel, so return_20d /
    volatility_60d / median dollar volume are absent and ``required_data_complete`` is
    False. Nothing is zeroed and no current snapshot stands in for history.
    """
    return _review(ticker, sector=_SECTORS[i % len(_SECTORS)], current_rank=i + 1,
                   return_5d=None, return_20d=None, return_60d=None,
                   volatility_20d=None, volatility_60d=None, drawdown_60d=None,
                   median_dollar_volume_20d=None, estimated_days_to_liquidate=None,
                   liquidity_state="UNAVAILABLE", recommendation_confidence="LOW",
                   required_data_complete=False)


def _coverage_blocked_book():
    """25 holdings, 15 complete and 10 genuinely incomplete — the live 0.60 fraction,
    below the 0.80 ``min_holdings_data_complete_fraction`` floor."""
    incomplete = [_incomplete(tk, i) for i, tk in enumerate(LIVE_INCOMPLETE_HOLDINGS)]
    return incomplete + [_filler(i) for i in range(10, 25)]


#: The four strongly-deteriorating names shared by scenarios 3, 5, 5b and 6.
def _strong():
    return [_replace("MRNA", gross=1.20, net=1.15, rank=58, prev=14),
            _replace("PARA", gross=1.10, net=1.05, rank=71, prev=22),
            _replace("WBA", gross=1.05, net=1.00, rank=88, prev=30),
            _replace("ENPH", gross=1.00, net=0.95, rank=95, prev=33)]


def _hoc_assessment(reviews, *, state="READY", gaps=None, eligible=DATE):
    invested = sum(r["market_value"] for r in reviews)
    sw: dict[str, float] = {}
    for r in reviews:
        sw[r["sector"]] = sw.get(r["sector"], 0.0) + r["current_weight"]
    counts = {"HOLD": 0, "REDUCE": 0, "EXIT": 0, "REPLACE": 0, "ADD": 0}
    for r in reviews:
        counts[r["recommendation"]] = counts.get(r["recommendation"], 0) + 1
    return {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": eligible, "active_book_id": BOOK,
        "assessment_state": state, "assessment_hash": "hoc_fixture_%s" % state.lower(),
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": NAV, "cash": NAV - invested, "invested_value": invested,
            "holdings_count": len(reviews),
            "max_name_weight": max(r["current_weight"] for r in reviews),
            "max_name_ticker": reviews[0]["ticker"],
            "max_sector_weight": max(sw.values()), "max_sector": max(sw, key=sw.get),
            "sector_weights": sw,
            "herfindahl_index": sum(r["current_weight"] ** 2 for r in reviews),
            "portfolio_variance_daily": 0.0001,
            "risk_contribution_state": "AVAILABLE"},
        "recommendation_counts": counts,
        "holding_reviews": reviews,
        "addition_candidates": [
            {"ticker": "CVS", "rank": 9, "score": 0.95, "combined_score": 0.95,
             "sector": "Health", "recommendation": "ADD"},
            {"ticker": "ADBE", "rank": 12, "score": 0.93, "combined_score": 0.93,
             "sector": "Tech", "recommendation": "ADD"},
            {"ticker": "LIN", "rank": 17, "score": 0.91, "combined_score": 0.91,
             "sector": "Materials", "recommendation": "ADD"}],
        "diagnostics": {"eligible_universe_size": 503},
        # Stage 22.1 — the completeness counts are part of the assessment's own data
        # quality, and the Stage-20 reassessment gate reads them. Omitting them made a
        # data-incomplete world unrepresentable in the harness.
        "data_quality": {
            "data_gaps": list(gaps or []),
            "holdings_evaluated": len(reviews),
            "holdings_data_complete": sum(1 for r in reviews
                                          if r.get("required_data_complete")),
            "previous_ranking_state": "AVAILABLE",
            "risk_contribution_state": "AVAILABLE",
            "liquidity_names_unavailable": sum(
                1 for r in reviews if r.get("liquidity_state") == "UNAVAILABLE")},
        "provenance": {"portfolio_state_hash": "ps_fixture",
                       "corporate_actions_hash": None,
                       "universe_scoring_hash": "us_fixture"},
    }


def _scoring():
    return {"output_hash": "us_fixture", "input_contract_hash": "uic_fixture",
            "strategy_id": "fundamental_momentum_50_50_v1", "strategy_version": "v1",
            "primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn", "model_registry_version": "29",
            "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe"}


def _fresh_rows(eligible=DATE):
    return [
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": eligible,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book",
         "reason": "Current through the eligible session %s." % eligible,
         "expected_through_date": eligible},
        {"source_id": "price_score_refresh", "status": "FRESH", "as_of_date": eligible,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Current through the eligible session %s." % eligible,
         "expected_through_date": eligible},
        {"source_id": "fundamental_quarterly", "status": "NOT_DUE",
         "as_of_date": "2026-05-22", "cadence": "QUARTERLY",
         "required_for_portfolio_reassessment": False,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Current under its own quarterly cadence.",
         "expected_through_date": "2026-05-22"},
    ]


def _blocked_rows(eligible=DATE):
    return [
        {"source_id": "price_score_refresh", "status": "MISSING", "as_of_date": None,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Source date is absent.", "expected_through_date": eligible},
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": eligible,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": eligible}]


def _freshness(rows=None, eligible=DATE):
    return {"eligible_market_date": eligible, "source_freshness": rows or _fresh_rows(eligible)}


# =========================================================================== #
# THE SHARED SCENARIO CONTRACT (Workstream B)
#
# ONE declarative object per acceptance scenario. Everything the rendered page shows
# is derived from it — no endpoint may invent a state of its own.
# =========================================================================== #
def world(*, scenario_id, title, hoc_reviews, hoc_state="READY", freshness_rows=None,
          history=None, execution="NONE", close="PROCESSED",
          close_status="HOLD",
          session="CLOSED", research="CURRENT", hoc_artifact="PRESENT",
          drc=None, hoc_producer=None,
          reassessment_evidence=None, hoc_gaps=None,
          expect_state=None, expect_primary_action=None, expect_attention=0,
          expect_execution_precedence=False, expect_workflow_state=None,
          expect_cycle_stage=None, expect_evidence_class=None,
          expect_mutation_count=None) -> dict:
    """Declare ONE coherent synthetic world.

    ``execution`` — the Stage-19 lifecycle cohort present on the desk:
        ``"NONE"``      no rebalance orders at all;
        ``"PENDING"``   the live shape: 29 SUBMITTED (15 BUY / 14 SELL), 0 current-plan
                        fills, a superseded plan with 22 CANCELLED, 25 historical fills;
        ``"EXECUTED"``  the same plan fully settled (29 FILLED);
        ``"AWAITING_REVIEW"`` (Release 29.3) a produced, immutable proposal with NO
                        recorded decision and NO orders — the manual-review state.

    ``close`` — the daily-close journal state for the eligible session:
        ``"PROCESSED"`` the eligible session has already been closed (no new close is
                        due — the operator is passively monitoring the execution);
        ``"DUE"``       a newly eligible completed session exists and has NOT been closed.

    ``close_status`` (Release 29.4) — which COMPLETED close the journal recorded:
        ``"HOLD"``              DAILY_CLOSE_COMPLETE_HOLD;
        ``"MEMBERSHIP_DRIFT"``  DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT — the status the
                        REAL 2026-08-17 close recorded. Both are equally COMPLETE: a
                        portfolio finding never decides whether an operational close
                        happened. The workflow owner used to keep a private literal copy
                        of this vocabulary that still spelled the pre-29.3 token, and
                        that copy is what made the completed Aug-17 close read as
                        invalid and offered a second Daily Close for it on Aug-18.

    Stage 22 adds the knobs the canonical NORMAL CYCLE needs end to end:

    ``session``  — ``"CLOSED"`` (the offline injected-date rule: the eligible session is
                   complete) or ``"OPEN"`` (a live weekday clock BEFORE the 17:30 ET
                   cutoff, so today's session is still forming). Only ``"OPEN"`` can
                   produce the genuine pre-close WAITING_FOR_SESSION_CLOSE state.
    ``research`` — ``"CURRENT"`` or ``"STALE"`` (the owned model inputs lag the eligible
                   session, so the required signal refresh is genuinely due).
    ``hoc_artifact`` — ``"PRESENT"`` or ``"ABSENT"``. ABSENT is the POST-CLOSE state the
                   live system reaches every day: the close is complete for the session
                   and no opportunity-cost assessment exists for it yet.
    ``reassessment_evidence`` — ``None``, or ``"STALE_CORPORATE_ACTION"`` to reproduce
                   the real BLOCKED_EVIDENCE assessment the operator sees when a
                   corporate action was registered after the assessment was produced.

    Release 29.5 separates two things this harness used to conflate. It derived the Daily
    Research Cycle state from ``hoc_artifact`` — artifact present therefore cycle COMPLETE
    — which is exactly the inference that deadlocked the live system on 2026-08-18: since
    Releases 28/29 the event-driven refresh writes a real artifact through the same
    canonical owner whenever continuous collection finds material information.

    ``drc`` — the cycle owner's own verdict, independent of the artifact:
        ``None``            keep the legacy derivation (artifact present -> COMPLETE);
        ``"NOT_STARTED"``   no run manifest for this session. With an artifact PRESENT
                            this is the live Aug-18 world: current signal state, governed
                            cycle DUE;
        ``"COMPLETE"``      a governed run manifest exists;
        ``"INCONSISTENT"``  an artifact CLAIMS a run manifest that cannot be read — the
                            genuine corruption that must still suspend the cycle.
    ``hoc_producer`` — ``"api.event_signal_refresh"`` (Class 1) or
                   ``"api.daily_research_cycle"`` (Class 2). Defaults to the class implied
                   by ``drc``.
    """
    return {
        "scenario_id": scenario_id,
        "title": title,
        "owner": SCENARIO_OWNER,
        # --- identity + dates -------------------------------------------------- #
        "book_id": BOOK, "book_label": BOOK_LABEL, "book_initialized": True,
        "eligible_market_date": DATE, "prior_market_date": PREV,
        "nav": NAV, "initial_capital": INITIAL_CAPITAL,
        # --- Stage-19 execution lineage --------------------------------------- #
        "execution": execution,
        "current_plan": {
            "order_plan_id": LIVE_PLAN, "order_plan_hash": LIVE_PLAN_HASH,
            "proposal_hash": LIVE_PROPOSAL_HASH, "approval_date": DATE,
            "submitted": LIVE_SUBMITTED if execution == "PENDING" else 0,
            "filled": LIVE_SUBMITTED if execution == "EXECUTED" else 0,
            "buys": LIVE_BUYS, "sells": LIVE_SELLS},
        "superseded_plan": {"order_plan_id": SUPERSEDED_PLAN,
                            "cancelled": SUPERSEDED_CANCELLED if execution != "NONE" else 0},
        "historical_implementation_fills": HISTORICAL_FILLS,
        # --- daily close ------------------------------------------------------- #
        "close": close, "close_status": close_status,
        # --- Stage 22 normal-cycle position ------------------------------------ #
        "session": session, "research": research, "hoc_artifact": hoc_artifact,
        # Release 29.5 — the cycle verdict is DECLARED, never inferred from the artifact.
        "drc": drc, "hoc_producer": hoc_producer,
        "reassessment_evidence": reassessment_evidence,
        # --- research evidence -------------------------------------------------- #
        "hoc_reviews": hoc_reviews, "hoc_state": hoc_state, "hoc_gaps": hoc_gaps or [],
        "freshness_rows": freshness_rows, "recent_change_history": history or [],
        # --- acceptance expectations ------------------------------------------- #
        "expect": {
            "state": expect_state,
            "primary_action": expect_primary_action,
            "attention": expect_attention,
            "execution_precedence": expect_execution_precedence,
            "workflow_state": expect_workflow_state,
            "cycle_stage": expect_cycle_stage,
            "evidence_class": expect_evidence_class,
            "mutation_count": expect_mutation_count,
            "book_initialized": True,
            "current_submitted": LIVE_SUBMITTED if execution == "PENDING" else 0,
            "current_filled": LIVE_SUBMITTED if execution == "EXECUTED" else 0,
            "historical_fills": HISTORICAL_FILLS,
            # Release 29.3 — AWAITING_REVIEW carries NO order cohort at all: a produced
            # proposal is evidence, never an order plan, so there is nothing to supersede.
            "superseded_cancelled": (
                SUPERSEDED_CANCELLED
                if execution not in ("NONE", "AWAITING_REVIEW") else 0),
        },
    }


def scenarios() -> dict:
    """The complete acceptance scenario set. ONE world each."""
    quiet = _quiet_book()
    strong = _strong()
    two_soft = [_replace("MRNA", gross=0.30, net=0.28, rank=58, prev=14),
                _replace("PARA", gross=0.28, net=0.26, rank=71, prev=22)]
    churn_history = [{"eligible_market_date": PREV, "ticker": t, "direction": "IN"}
                     for t in ("MRNA", "PARA", "WBA", "ENPH")]
    return {
        "scenario_1_portfolio_current": world(
            scenario_id="scenario_1_portfolio_current",
            title="Portfolio current — no change is economically justified",
            hoc_reviews=quiet, execution="EXECUTED", close="PROCESSED",
            expect_state=kernel.STATE_NO_CHANGE, expect_primary_action=None,
            expect_attention=0),
        "scenario_2_deteriorating_no_action": world(
            scenario_id="scenario_2_deteriorating_no_action",
            title="Two holdings deteriorating but no economic action",
            hoc_reviews=two_soft + [_filler(i) for i in range(23)],
            execution="EXECUTED", close="PROCESSED",
            expect_state=kernel.STATE_CHANGE_CANDIDATE, expect_primary_action=None,
            expect_attention=2),
        "scenario_3_proposal_review": world(
            scenario_id="scenario_3_proposal_review",
            title="Strong replacement — one proposal review action",
            hoc_reviews=strong + [_filler(i) for i in range(21)],
            execution="NONE", close="PROCESSED",
            expect_state=kernel.STATE_PROPOSAL_READY,
            expect_primary_action="REVIEW_PORTFOLIO_PROPOSAL", expect_attention=4),
        # Release 29.3 — the PROPOSAL REVIEW REQUIRED world: the reassessment cleared the
        # portfolio gate, the canonical proposal owner produced ONE complete immutable
        # target, and no manual decision has been recorded yet. Nothing is approved and
        # no order exists; this is the only state in which an operator proposal action
        # may appear anywhere in the product.
        "scenario_11_proposal_review_required": world(
            scenario_id="scenario_11_proposal_review_required",
            title="Proposal produced — manual review required, no orders",
            hoc_reviews=strong + [_filler(i) for i in range(21)],
            execution="AWAITING_REVIEW", close="PROCESSED",
            expect_state=kernel.STATE_PROPOSAL_READY,
            expect_primary_action="REVIEW_PORTFOLIO_PROPOSAL", expect_attention=4),
        "scenario_4_data_blocked": world(
            scenario_id="scenario_4_data_blocked",
            title="Data-blocked reassessment — named blocker, no action",
            hoc_reviews=quiet, freshness_rows=_blocked_rows(),
            execution="EXECUTED", close="PROCESSED",
            expect_state=kernel.STATE_BLOCKED_DATA,
            # The required price-score refresh is genuinely absent, so the ONE honest
            # page-level action is to run the Daily Research Cycle. What must NOT appear
            # is a proposal-review CTA: the reassessment itself is data-blocked.
            expect_primary_action="RUN_DAILY_RESEARCH_CYCLE",
            expect_attention=0,
            # Stage 22.1 — this scenario's historical contract (a NAMED data blocker and
            # a path back to the Daily Research Cycle) is now pinned in Stage-22
            # normal-cycle vocabulary rather than left unchecked. It is the SAME family
            # as scenario 10 with an EARLIER-ranked cause: here a required research INPUT
            # is missing, so the cycle has not yet reached the reassessment and the DRC is
            # still runnable; in scenario 10 the cycle already ran and the reassessment
            # itself reached no verdict, so the cycle is suspended. One interpretation,
            # two positions in the same sequence.
            expect_workflow_state="RESEARCH_CYCLE_REQUIRED",
            expect_cycle_stage="DAILY_RESEARCH_CYCLE",
            expect_evidence_class="SYSTEM_BLOCKER",
            expect_mutation_count=1),
        "scenario_5_execution_pending": world(
            scenario_id="scenario_5_execution_pending",
            title="Stage-19 execution pending — it keeps operator precedence",
            hoc_reviews=strong + [_filler(i) for i in range(21)],
            execution="PENDING", close="PROCESSED",
            expect_state=kernel.STATE_PROPOSAL_READY, expect_primary_action=None,
            expect_attention=4, expect_execution_precedence=True),
        "scenario_5b_execution_pending_close_due": world(
            scenario_id="scenario_5b_execution_pending_close_due",
            title="Execution pending AND a newly eligible close — the close is the ONE action",
            hoc_reviews=strong + [_filler(i) for i in range(21)],
            execution="PENDING", close="DUE",
            expect_state=kernel.STATE_PROPOSAL_READY,
            expect_primary_action="RUN_DAILY_CLOSE",
            expect_attention=4, expect_execution_precedence=True),
        "scenario_6_reconciled_then_next": world(
            scenario_id="scenario_6_reconciled_then_next",
            title="Executed/reconciled, then the next reassessment (churn holds)",
            hoc_reviews=strong + [_filler(i) for i in range(21)],
            history=churn_history, execution="EXECUTED", close="PROCESSED",
            expect_state=kernel.STATE_NO_CHANGE, expect_primary_action=None,
            expect_attention=0),
        # =================================================================== #
        # STAGE 22 — the canonical NORMAL CYCLE, end to end. Scenarios 7 -> 8 -> 9
        # are the three transitions the live system makes every day, and 1 / 3 are
        # its two legitimate endings (monitor, or manual proposal review).
        # =================================================================== #
        "scenario_7_pre_close_expected_stale_evidence": world(
            scenario_id="scenario_7_pre_close_expected_stale_evidence",
            title=("Pre-close: nothing to do, and the superseded assessment is demoted "
                   "to evidence"),
            # The live 25-holding book with the eligible session already fully processed,
            # a still-open market session, and a prior assessment that is correctly
            # BLOCKED_EVIDENCE. Nothing is required of the operator, so that blocked card
            # must NOT compete with "no action required" or read as an incident.
            # DEGRADED with two documented gaps — the real shape of the live surface
            # that reported only "1 data gap(s) documented". One is portfolio-level
            # (no prior-session artifact) and one is per-holding (no owned volume), so
            # the machine-readable taxonomy is genuinely exercised end to end.
            hoc_reviews=([_review("H00", liquidity_state="UNAVAILABLE",
                                  median_dollar_volume_20d=None,
                                  estimated_days_to_liquidate=None)]
                         + [_filler(i) for i in range(1, 25)]),
            hoc_state="DEGRADED", hoc_gaps=["PRIOR_RANK_UNAVAILABLE"],
            execution="EXECUTED", close="PROCESSED",
            session="OPEN", reassessment_evidence="STALE_CORPORATE_ACTION",
            expect_state="BLOCKED_EVIDENCE", expect_primary_action=None,
            expect_attention=0,
            expect_workflow_state="WAITING_FOR_SESSION_CLOSE",
            expect_cycle_stage="WAIT_FOR_SESSION_CLOSE",
            expect_evidence_class="EXPECTED_STALE_EVIDENCE",
            expect_mutation_count=0),
        # =================================================================== #
        # RELEASE 29.4 — the live 2026-08-18 08:31 ET SESSION-AUTHORITY defect.
        #
        # The exact world the operator was looking at: the market session is still
        # open, the 2026-08-17 session was fully closed the previous evening with
        # DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT, forward evidence was captured, and
        # the Holding Opportunity-Cost assessment is current. Nothing is required.
        #
        # Before Release 29.4 this rendered as READY_FOR_DAILY_CLOSE with an
        # executable RUN DAILY CLOSE and "No completed operational close has been
        # recorded yet" — a second close offered for a session already processed.
        # =================================================================== #
        "scenario_12_pre_close_membership_drift": world(
            scenario_id="scenario_12_pre_close_membership_drift",
            title=("Pre-close: the session is still open and the completed close "
                   "stays completed"),
            # The portfolio lane is genuinely loud — two holdings deteriorating, the
            # same CHANGE_CANDIDATE the real Aug-17 reassessment reached — and the
            # operational lane still has nothing to do. That is the whole point: the
            # two lanes answer different questions.
            hoc_reviews=two_soft + [_filler(i) for i in range(23)],
            execution="NONE", close="PROCESSED", close_status="MEMBERSHIP_DRIFT",
            session="OPEN",
            expect_state=kernel.STATE_CHANGE_CANDIDATE, expect_primary_action=None,
            expect_attention=2,
            expect_workflow_state="WAITING_FOR_SESSION_CLOSE",
            expect_cycle_stage="WAIT_FOR_SESSION_CLOSE",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=0),
        # The mirror image: once a NEWER session is eligible, the membership-drift
        # token must not suppress its close. The repair is session authority, not a
        # blanket suppression of the Daily Close.
        "scenario_13_post_close_next_session_due": world(
            scenario_id="scenario_13_post_close_next_session_due",
            title=("Post-close: a newer eligible session makes the Daily Close the "
                   "ONE action again"),
            hoc_reviews=quiet, execution="NONE", close="DUE",
            close_status="MEMBERSHIP_DRIFT",
            expect_state=kernel.STATE_NO_CHANGE,
            expect_primary_action="RUN_DAILY_CLOSE", expect_attention=0,
            expect_workflow_state="READY_FOR_DAILY_CLOSE",
            expect_cycle_stage="DAILY_CLOSE",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=1),
        # =================================================================== #
        # RELEASE 29.5 — the post-close deadlock, and the corruption that must
        # still fail closed.
        #
        # On 2026-08-18 the Daily Close SUCCEEDED, continuous collection then ran
        # the event-driven refresh (evt_b91704271fb7a992, requested_by
        # PAPER_TRADER_INFORMATION_COLLECTION), and that refresh wrote a real
        # opportunity-cost artifact for the eligible session. No DRC manifest
        # existed, so the pair "artifact + no manifest" was read as corruption:
        # INCONSISTENT_STATE, cycle stage RECOVERY, zero executable stages — and
        # the only thing that could have written the manifest was the stage
        # RECOVERY had just disabled.
        # =================================================================== #
        "scenario_14_pre_drc_live_signal": world(
            scenario_id="scenario_14_pre_drc_live_signal",
            title=("Post-close: live signal state exists, and the governed research "
                   "cycle is the ONE action"),
            hoc_reviews=two_soft + [_filler(i) for i in range(23)],
            execution="NONE", close="PROCESSED", close_status="MEMBERSHIP_DRIFT",
            hoc_artifact="PRESENT", drc="NOT_STARTED",
            hoc_producer=hoc.PRODUCER_EVENT_SIGNAL_REFRESH,
            expect_state=kernel.STATE_CHANGE_CANDIDATE, expect_attention=2,
            expect_primary_action="RUN_DAILY_RESEARCH_CYCLE",
            expect_workflow_state="RESEARCH_CYCLE_REQUIRED",
            expect_cycle_stage="DAILY_RESEARCH_CYCLE",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=1),
        # The mirror image: an artifact that CLAIMS a run manifest nobody can read is
        # real corruption. The repair narrowed the trigger to the claim; it did not
        # remove the fail-closed contract.
        "scenario_15_falsely_terminal_artifact": world(
            scenario_id="scenario_15_falsely_terminal_artifact",
            title=("Corruption: an artifact claims a research-cycle manifest that "
                   "does not exist"),
            hoc_reviews=quiet, execution="NONE", close="PROCESSED",
            close_status="MEMBERSHIP_DRIFT",
            hoc_artifact="PRESENT", drc="INCONSISTENT",
            hoc_producer=hoc.PRODUCER_DAILY_RESEARCH_CYCLE,
            expect_state=kernel.STATE_NO_CHANGE, expect_attention=0,
            expect_primary_action=None,
            expect_workflow_state="INCONSISTENT_STATE",
            expect_cycle_stage="RECOVERY",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=0),
        "scenario_8_session_complete_close_due": world(
            scenario_id="scenario_8_session_complete_close_due",
            title="Session complete: the Daily Close is the ONE action",
            hoc_reviews=quiet, execution="NONE", close="DUE",
            expect_state=kernel.STATE_NO_CHANGE,
            expect_primary_action="RUN_DAILY_CLOSE", expect_attention=0,
            expect_workflow_state="READY_FOR_DAILY_CLOSE",
            expect_cycle_stage="DAILY_CLOSE",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=1),
        "scenario_9_post_close_research_due": world(
            scenario_id="scenario_9_post_close_research_due",
            title="Daily Close complete: the Daily Research Cycle is the ONE action",
            # The exact post-close handoff. The close is recorded for the eligible
            # session and every required signal input is CURRENT — yet no opportunity-cost
            # assessment exists for the session that was just closed, so the research
            # cycle is due. Before Stage 22 this state rendered as "monitor / no action
            # required" and the reassessment silently never happened.
            hoc_reviews=quiet, execution="EXECUTED", close="PROCESSED",
            hoc_artifact="ABSENT",
            expect_state=kernel.STATE_NOT_READY,
            expect_primary_action="RUN_DAILY_RESEARCH_CYCLE", expect_attention=0,
            expect_workflow_state="RESEARCH_CYCLE_REQUIRED",
            expect_cycle_stage="DAILY_RESEARCH_CYCLE",
            expect_evidence_class="CURRENT_EVIDENCE",
            expect_mutation_count=1),
        # =================================================================== #
        # STAGE 22.1 — the live 2026-08-14 BLOCKED PORTFOLIO DECISION.
        #
        # Everything upstream genuinely succeeded: the Daily Close is recorded for
        # the eligible session, every required signal input is CURRENT, the Daily
        # Research Cycle completed and produced a DEGRADED opportunity-cost
        # assessment over all 25 holdings. Only 15 of those holdings have complete
        # required analytics (0.60 < the 0.80 floor), so the canonical reassessment
        # correctly refuses with INSUFFICIENT_HOLDING_DATA_COMPLETENESS and reaches
        # NO portfolio-level verdict.
        #
        # Before Stage 22.1 this exact combination rendered as DAILY_CYCLE_COMPLETE
        # / MONITOR_PORTFOLIO / "No portfolio change requires review" — a blocked
        # session presented as a cleared economic gate. The expectations below are
        # what makes that unrepresentable.
        # =================================================================== #
        "scenario_10_reassessment_data_blocked": world(
            scenario_id="scenario_10_reassessment_data_blocked",
            title=("Post-cycle: the reassessment reached no verdict — a named data "
                   "blocker, never 'no change'"),
            hoc_reviews=_coverage_blocked_book(), hoc_state="DEGRADED",
            hoc_gaps=["HOLDING_ANALYTICS_UNAVAILABLE"],
            # The live shape: no rebalance orders are working and NO reallocation
            # proposal exists (proposal_hash null) — a blocked reassessment produces
            # none, and none may be manufactured from blocked evidence.
            execution="NONE", close="PROCESSED",
            expect_state=kernel.STATE_BLOCKED_DATA,
            # NO page-level mutation: the cycle is suspended until the operator
            # repairs the named cause, and the path back is the Daily Research
            # Cycle — never a proposal review built from blocked evidence.
            expect_primary_action=None, expect_attention=0,
            expect_workflow_state="RESEARCH_CYCLE_BLOCKED",
            expect_cycle_stage="RECOVERY",
            expect_evidence_class="SYSTEM_BLOCKER",
            expect_mutation_count=0),
    }


SCENARIO_KEYS = tuple(scenarios())


# =========================================================================== #
# Store seeding — the ONE world written through the REAL ledger primitives.
# =========================================================================== #
def _bars(tk: str) -> list[list]:
    px = PRICE[tk]
    return [[PREV, round(px * 0.995, 4)], [DATE, px]]


def _historical_fill(tk: str, qty: int, i: int) -> dict:
    gross = qty * PRICE[tk]
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": "f_%02d_%s" % (i, tk), "order_id": "ord_hist_%02d" % i,
        "book_id": BOOK, "ticker": tk, "side": desk.SIDE_BUY, "quantity": qty,
        "fill_date": "2026-07-01", "fill_price": PRICE[tk],
        "gross_value": round(gross, 2), "transaction_cost": round(cost, 4),
        "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": desk.EXECUTION_MODEL_DEFAULT, "immutable": True}}


def _lineage_order(oid, tk, side, plan_id, plan_hash, *, qty=10) -> dict:
    return {"order_id": oid, "book_id": BOOK, "ticker": tk, "side": side,
            "quantity": qty, "order_type": "MARKET_ON_CLOSE",
            "execution_model": desk.EXECUTION_MODEL_DEFAULT,
            "approval_date": DATE, "created_date": DATE,
            "rebalance_lineage": {"order_plan_id": plan_id,
                                  "order_plan_hash": plan_hash,
                                  "proposal_id": "reap_%s_%s" % (DATE, BOOK),
                                  "proposal_hash": LIVE_PROPOSAL_HASH}}


def _historical_order(tk: str, i: int) -> dict:
    """The originating order of one historical initial-implementation fill. It carries NO
    rebalance lineage, which is exactly what keeps it out of the current-rebalance cohort."""
    return {"order_id": "ord_hist_%02d" % i, "book_id": BOOK, "ticker": tk,
            "side": desk.SIDE_BUY, "quantity": int(3_800.0 / PRICE[tk]) or 1,
            "order_type": "MARKET_ON_CLOSE",
            "execution_model": desk.EXECUTION_MODEL_DEFAULT,
            "approval_date": "2026-07-01", "created_date": "2026-07-01",
            "status": desk.ST_FILLED}


def _plan_cohort(spec: dict) -> list[dict]:
    """The CURRENT plan cohort: 15 BUY + 14 SELL, in the declared terminal status, PLUS
    the 25 lineage-free historical implementation orders and the superseded cancelled
    plan. Three permanently separate cohorts — never folded into one another."""
    out = [_historical_order(tk, i) for i, tk in enumerate(HELD)]
    ex = spec["execution"]
    if ex in ("NONE", "AWAITING_REVIEW"):
        # Release 29.3 — a produced proposal awaiting manual review creates NO order.
        return out
    status = desk.ST_SUBMITTED if ex == "PENDING" else desk.ST_FILLED
    plan = spec["current_plan"]
    for i, tk in enumerate(BUY_ADDS + BUY_INCREASES):
        o = _lineage_order("ord_cur_b%02d" % i, tk, desk.SIDE_BUY,
                           plan["order_plan_id"], plan["order_plan_hash"])
        o["status"] = status
        out.append(o)
    for i, tk in enumerate(SELL_EXITS + SELL_REDUCES):
        o = _lineage_order("ord_cur_s%02d" % i, tk, desk.SIDE_SELL,
                           plan["order_plan_id"], plan["order_plan_hash"])
        o["status"] = status
        out.append(o)
    # The superseded DEFECTIVE plan — 22 CANCELLED, permanently separate.
    for i in range(spec["superseded_plan"]["cancelled"]):
        tk = (BUY_ADDS + SELL_EXITS + BUY_INCREASES)[i % 22]
        o = _lineage_order("ord_sup_%02d" % i, tk,
                           desk.SIDE_BUY if i % 2 == 0 else desk.SIDE_SELL,
                           spec["superseded_plan"]["order_plan_id"], "5bf9c6c20f8a")
        o["approval_date"] = PREV
        o["status"] = desk.ST_CANCELLED
        out.append(o)
    return out


def _seed_orders(sdir: Path, orders: list[dict]) -> None:
    """Seed the append-only order ledger through its REAL event vocabulary, so the
    lineage fold in api.operational_book is exercised exactly as in production."""
    if not orders:
        return
    desk._append_ledger(sdir, desk.ORDERS_FILE,       # noqa: SLF001 - canonical primitive
                        [{"event": "ORDER_CREATED",
                          "order": {k: v for k, v in o.items() if k != "status"}}
                         for o in orders])
    trans = []
    for o in orders:
        if o["status"] == desk.ST_PROPOSED:
            continue
        trans.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                      "from_status": desk.ST_PROPOSED, "to_status": desk.ST_SUBMITTED,
                      "approval_date": o.get("approval_date"),
                      "marks_latest_at_approval": DATE})
        if o["status"] != desk.ST_SUBMITTED:
            trans.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                          "from_status": desk.ST_SUBMITTED, "to_status": o["status"]})
    if trans:
        desk._append_ledger(sdir, desk.ORDERS_FILE, trans)   # noqa: SLF001


def seed_desk(spec: dict, root: Path) -> Path:
    """Write the world's desk ledgers into a THROWAWAY root. Never a live store.

    IDEMPOTENT BY CONSTRUCTION. The desk ledgers are APPEND-ONLY, so seeding twice into the
    same root would append a SECOND copy of the world: 50 historical fills instead of 25,
    doubled holdings and a nonsense negative cash balance. The acceptance harness composes
    a scenario once to verify it and again to serve it, so this is a routine path, not an
    edge case. The world is therefore rebuilt from scratch every time.
    """
    sdir = Path(root) / "desk"
    if sdir.exists():
        shutil.rmtree(sdir)
    sdir.mkdir(parents=True, exist_ok=True)

    book = {"book_id": BOOK, "book_number": 1, "display_name": BOOK_LABEL,
            "initial_capital": INITIAL_CAPITAL,
            "execution_model": desk.EXECUTION_MODEL_DEFAULT,
            "currency": desk.BOOK_CURRENCY, "benchmark": desk.BENCHMARK_TICKER,
            "status": "OPEN", "creation_date": "2026-07-01",
            "snapshot_market_date": "2026-07-01",
            "model_id": "fundamental_momentum_50_50_v1",
            "frozen_target_weights": {tk: 0.04 for tk in HELD}}
    desk._append_ledger(sdir, desk.BOOKS_FILE,       # noqa: SLF001
                        [{"event": "BOOK_CREATED", "book": book}])

    # Alpha-book policy + initialization linkage: what makes the book INITIALIZED.
    policy = dict(ab.DEFAULT_POLICY)
    desk._append_ledger(sdir, ab.POLICY_FILE,        # noqa: SLF001
                        [{"event": "POLICY_VERSION", "policy": policy,
                          "policy_version": 1}])
    desk._append_ledger(sdir, ab.RECORDS_FILE,       # noqa: SLF001
                        [{"event": "ALPHA_BOOK_INITIALIZED", "book_id": BOOK,
                          "initialization_date": "2026-07-01",
                          "snapshot_id": "snap_2026-07-01_%s" % BOOK,
                          "target_market_date": DATE,
                          "target_weight": 0.04,
                          "constituents": list(HELD)}])

    # The 25 historical initial-implementation fills (NO rebalance lineage — they are a
    # permanently separate cohort and are never folded into the current rebalance).
    fills = [_historical_fill(tk, int(3_800.0 / PRICE[tk]) or 1, i)
             for i, tk in enumerate(HELD)]
    desk._append_ledger(sdir, desk.FILLS_FILE, fills)   # noqa: SLF001

    desk._atomic_write_json(sdir / desk.MARKS_FILE, {   # noqa: SLF001
        "phase": "STAGE20_1_ACCEPTANCE", "kind": "provider_cache_not_a_ledger",
        "series": {tk: _bars(tk) for tk in _ALL_TICKERS},
        "latest_completed_date": DATE, "updated_at": "%sT22:00:00+00:00" % DATE})

    _seed_orders(sdir, _plan_cohort(spec))

    # The daily-close journal. PROCESSED => the eligible session is already closed, so no
    # new close is due and pending orders stay passively monitored. DUE => a newly
    # eligible completed session exists that has never been closed.
    rows = [{"event": dc.DAILY_CLOSE_EVENT, "book_id": BOOK, "market_date": PREV,
             "decision": dc.DECISION_HOLD, "close_status": dc.CLOSE_COMPLETE_HOLD,
             "is_baseline": False}]
    if spec["close"] == "PROCESSED":
        rows.append({"event": dc.DAILY_CLOSE_EVENT, "book_id": BOOK,
                     "market_date": spec["eligible_market_date"],
                     "decision": (dc.DECISION_ORDERS_PENDING
                                  if spec["execution"] == "PENDING" else dc.DECISION_HOLD),
                     "close_status": dc.CLOSE_COMPLETE_HOLD, "is_baseline": False})
    desk._append_ledger(sdir, dc.DAILY_CLOSE_JOURNAL_FILE, rows)   # noqa: SLF001
    return sdir


def seed_ledger(spec: dict, root: Path) -> Path:
    """The confirmed multi-horizon target snapshot the alpha book is tracking.

    Without it the book has NO confirmed target, ``load_desk_mark_readiness`` reports
    ``NO_CONFIRMED_TARGET`` and the Operational Book panel offers CONFIRM TARGET SNAPSHOT
    — a fourth unrelated world. It is part of the SAME scenario, so it is seeded here.
    """
    ldir = Path(root) / "mhz"
    ldir.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "snapshot_id": "snap_%s_%s" % (DATE, BOOK),
        "confirmation_status": mhz_ledger.STATUS_CONFIRMED,
        "market_as_of_date": spec["eligible_market_date"],
        "calculation_timestamp": "%sT21:00:00+00:00" % DATE,
        "sleeves": {mreg.SLEEVE_COMBINED: {
            "period": DATE[:7], "constituents_top25": list(HELD),
            "constituents_top50": list(HELD) + list(BUY_ADDS),
            "target_weights": {"top25": 0.04, "top50": 0.02}}},
    }
    (ldir / mhz_ledger.SNAPSHOTS_FILE).write_text(
        json.dumps({"snapshots": [snapshot]}, indent=2, sort_keys=True),
        encoding="utf-8")
    return ldir


#: The combined-sleeve model and its Top-25 book id, used to shape the frozen owned-model
#: payload below exactly as ``api.multi_horizon_engine`` shapes the real one.
_COMBINED_MODEL_ID = "fundamental_momentum_50_50_v1"
_COMBINED_BOOK_ID = "fundamental_momentum_50_50_top25"


def _engine_current(spec: dict) -> dict:
    """The frozen owned-model ``current`` payload the Daily Action Gate scores against.

    Stage 21 (Workstream 0F) — hermetic clock ownership. This was one of three live-world
    reads the Stage-20.1 harness still performed. ``daily_action_gate`` falls back to the
    REAL owned-model loader when ``current`` is not injected, so the gate resolved
    ``market_as_of_date`` from the live panel: 2026-08-13 today, 2026-08-14 tomorrow, while
    every seeded panel stayed frozen on the scenario's eligible session 2026-08-12. The
    workflow owner then correctly reported ASSESSMENT_AHEAD_OF_ELIGIBLE_SESSION and
    collapsed scenarios 4, 5 and 5b to INSPECT_STATE_INCONSISTENCY. The product was right;
    the harness was reading two different worlds.

    The payload is the scenario's own: the 25 HELD names rank 1..25 (so the recomputed
    Top-25 target IS the book and the gate's own diff invents no churn), the 8 BUY_ADDS
    rank 26..33, and every date is the scenario's.
    """
    eligible = spec["eligible_market_date"]
    ranked = list(HELD) + [t for t in BUY_ADDS if t not in HELD]
    combined = {}
    for i, tk in enumerate(ranked):
        pct = round(1.0 - (i / 100.0), 4)
        combined[tk] = {
            "combined_score": round(2.0 - (i / 100.0), 4),
            "sector": _SECTORS[i % len(_SECTORS)],
            "fund_percentile": pct, "mom_percentile": pct,
            "fund_rank": i + 1, "mom_rank": i + 1,
            "adv_dollar": 5.0e7,
        }
    constituents = [{"ticker": tk, "rank": i + 1, "weight": 0.04,
                     "score": combined[tk]["combined_score"],
                     "sector": combined[tk]["sector"], "adv_dollar": 5.0e7}
                    for i, tk in enumerate(HELD)]
    risk = {tk: {"realized_vol_63d": 0.20, "max_drawdown_252d": -0.12,
                 "adv_dollar_20d": 5.0e7, "sector": combined[tk]["sector"]}
            for tk in ranked}
    return {
        "status": dag.eng.STATUS_READY,
        "market_as_of_date": eligible,
        "momentum_month": DATE[:7],
        "fundamental_month": "2026-05",
        "fundamental_as_of_date": "2026-05-22",
        "scores": {},
        "combined": {"combined": combined},
        "books": {"primary_book_id": _COMBINED_BOOK_ID,
                  "books": {_COMBINED_BOOK_ID: {
                      "constituents": constituents, "equal_weight": 0.04,
                      "size_actual": len(constituents)}}},
        "inputs": {"available": True, "market_as_of_date": eligible,
                   "momentum_month": DATE[:7], "fundamental_month": "2026-05",
                   "fundamental_as_of_date": "2026-05-22",
                   "risk": risk, "validations": {"risk_available": True},
                   "warnings": []},
        "warnings": [],
    }


def _target_readiness(spec: dict) -> dict:
    """The frozen alpha-target readiness contract, in ``api.alpha_target`` shape.

    Second live-world read (Stage 21, Workstream 0F): ``operational_book`` resolved this
    from the owned model panel, so the Operational Book panel carried the REAL
    ``alpha_market_date`` while every seeded panel carried the scenario's — surfacing as
    TARGET_READINESS_MISMATCH the moment the two dates diverged.
    """
    eligible = spec["eligible_market_date"]
    return {"state": "TARGET_CONFIRMED",
            "dates": {"alpha_market_date": eligible,
                      "latest_completed_market_date": eligible},
            "alpha_market_aligned": True,
            "snapshot_confirmation_allowed": False,
            "confirmation_blockers": [], "required_next_action": None}


def _research_inputs(spec: dict) -> dict:
    """The owned model-input dates the freshness contract classifies. Scenario 4 declares
    a genuinely ABSENT price-score refresh rather than a hand-written 'MISSING' row.

    Stage 22: ``research="STALE"`` declares inputs that genuinely LAG the eligible
    session (present, just older) — a different, weaker condition than ABSENT, and the
    one the normal cycle meets when a signal refresh is due.
    """
    blocked = spec["freshness_rows"] is not None
    if blocked:
        as_of = None
    elif spec.get("research") == "STALE":
        as_of = spec["prior_market_date"]
    else:
        as_of = spec["eligible_market_date"]
    return {"available": not blocked,
            "market_as_of_date": as_of,
            "momentum_month": DATE[:7],
            "fundamental_as_of_date": "2026-05-22"}


#: Stage 22 — the LIVE clock a pre-close world needs. ``reference_today`` always resolves
#: an already-completed session, so BEFORE_SESSION_CLOSE is unreachable through it; a
#: weekday datetime before the 17:30 ET cutoff is what makes today's session "still
#: forming" through the REAL market-session owner rather than a hand-written status.
_PRE_CLOSE_NOW = datetime(int(NEXT[:4]), int(NEXT[5:7]), int(NEXT[8:10]), 10, 30,
                          tzinfo=timezone(timedelta(hours=-4)))


def _clock(spec: dict) -> dict:
    """The ONE clock every panel of a scenario resolves its session from."""
    if spec.get("session") == "OPEN":
        return {"now": _PRE_CLOSE_NOW}
    return {"reference_today": NEXT}


def _write_artifact_store(root: Path, sub: str, artifact_id: str, artifact: dict,
                          *, key_field: str, extra: dict) -> Path:
    """Write ONE immutable artifact + its index entry in the canonical store layout
    shared by the HOC / reallocation / reassessment / decision owners."""
    store = Path(root) / sub
    (store / "artifacts").mkdir(parents=True, exist_ok=True)
    path = store / "artifacts" / ("%s.json" % artifact_id)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True, default=str),
                    encoding="utf-8")
    ipath = store / "index.json"
    index = {}
    if ipath.exists():
        try:
            index = json.loads(ipath.read_text(encoding="utf-8"))
        except ValueError:
            index = {}
    entry = {key_field: artifact_id, "path": str(path),
             "eligible_market_date": DATE, "active_book_id": BOOK}
    entry.update(extra)
    index["%s|%s" % (BOOK, DATE)] = entry
    ipath.write_text(json.dumps(index, indent=2, sort_keys=True), encoding="utf-8")
    return path


# =========================================================================== #
# Composition — every canonical panel payload from the ONE world.
# =========================================================================== #
def _portfolio_state(spec: dict, opbook: dict) -> dict:
    """The canonical portfolio-state read, derived from the SAME operational book the
    Operational Book panel renders — so the two can never disagree."""
    from paper_trader.api import corporate_actions as ca
    return {
        "dates": {"eligible_market_date": spec["eligible_market_date"],
                  "valuation_date": opbook.get("nav_as_of_date"),
                  "desk_mark_date": opbook.get("desk_mark_date")},
        "active_book": {"book_id": spec["book_id"], "book_label": spec["book_label"],
                        "status": "ACTIVE", "initialized": bool(opbook.get("initialized",
                                                                           True)),
                        "holdings_count": opbook.get("holdings_count") or 0,
                        "pending_order_count": opbook.get("pending_order_count") or 0},
        "capital": {"nav": opbook.get("nav"), "cash": opbook.get("cash")},
        "state_hash": "ps_fixture",
        "corporate_actions": {"registry_fingerprint": ca.EMPTY_REGISTRY_FINGERPRINT,
                              "actions": []},
    }


#: Stage 22 — the corporate-action staleness block the reassessment kernel consumes to
#: reach BLOCKED_EVIDENCE. Shaped exactly like api.corporate_actions.staleness_vs_registry
#: so the fixture declares a real condition instead of forcing a state.
_CA_STALE = {
    "stale": True, "verifiable": True,
    "reason": "CORPORATE_ACTION_REGISTERED_AFTER_ASSESSMENT",
    "message": ("A corporate action was registered after this assessment was produced, "
                "so it evaluated holdings that no longer describe the portfolio."),
}


def _reassessment_artifact(spec: dict) -> dict:
    """Run the REAL Stage-20 owner over the world's evidence and package the artifact."""
    assessment = _hoc_assessment(spec["hoc_reviews"], state=spec["hoc_state"],
                                 gaps=spec.get("hoc_gaps"),
                                 eligible=spec["eligible_market_date"])
    if spec.get("hoc_artifact") == "ABSENT":
        # No opportunity-cost assessment exists for this session, so the reassessment
        # honestly reports NOT_READY (HOLDING_OPPORTUNITY_COST_NOT_RUN) — it is never
        # fabricated from an assessment that was not produced.
        assessment = dict(assessment, assessment_hash=None, assessment_state="NOT_RUN")
    ca_stale = (dict(_CA_STALE)
                if spec.get("reassessment_evidence") == "STALE_CORPORATE_ACTION"
                else None)
    run = prs.run_reassessment(input_contract=prs.build_input_contract(
        portfolio_state={"dates": {"eligible_market_date": spec["eligible_market_date"]},
                         "active_book": {"book_id": BOOK, "book_label": BOOK_LABEL,
                                         "status": "ACTIVE", "initialized": True,
                                         "holdings_count": len(spec["hoc_reviews"])},
                         "capital": {"nav": NAV, "cash": 0.0},
                         "state_hash": "ps_fixture"},
        scoring=_scoring(), hoc_assessment=assessment,
        freshness=_freshness(spec["freshness_rows"], spec["eligible_market_date"]),
        recent_change_history=spec["recent_change_history"],
        corporate_action_stale=ca_stale, policy=prs.resolve_policy()))
    return assessment, {
        "reassessment_id": "prs_%s_%s_%s" % (DATE, BOOK, spec["scenario_id"][:12]),
        "schema_version": prs.SCHEMA_VERSION,
        "composition_owner": prs.COMPOSITION_OWNER,
        "calculation_owner": prs.CALCULATION_OWNER,
        "generated_at": "%sT21:05:00Z" % DATE,
        "identity": prs.artifact_identity(input_contract=run["input_contract"],
                                          result=run["reassessment"]),
        "input_contract": {}, "reassessment": run["reassessment"]}


def _reallocation_artifact(spec: dict, nav: float) -> dict:
    """A TRUTHFUL reallocation artifact: it exists ONLY where the world genuinely
    carries a confirmed order plan derived from it. Never invented to tidy the UI."""
    if spec["execution"] == "NONE":
        return None
    allocations = []
    for tk in SELL_EXITS:
        allocations.append({"ticker": tk, "sector": "Ind", "action": "EXIT",
                            "current_weight": 0.04, "proposed_weight": 0.0, "held": True})
    for tk in SELL_REDUCES:
        allocations.append({"ticker": tk, "sector": "Energy", "action": "REDUCE",
                            "current_weight": 0.04, "proposed_weight": 0.03, "held": True})
    for tk in BUY_ADDS:
        allocations.append({"ticker": tk, "sector": "Health", "action": "ADD",
                            "current_weight": 0.0, "proposed_weight": 0.039, "held": False})
    for tk in BUY_INCREASES:
        allocations.append({"ticker": tk, "sector": "Tech", "action": "INCREASE",
                            "current_weight": 0.03, "proposed_weight": 0.039, "held": True})
    two_way = sum(abs(a["proposed_weight"] - a["current_weight"]) for a in allocations)
    return {"proposal_id": "reap_%s_%s" % (DATE, BOOK),
            "schema_version": "reallocation_proposal.v1",
            "identity": {"active_book_id": BOOK, "eligible_market_date": DATE,
                         "proposal_hash": LIVE_PROPOSAL_HASH,
                         "portfolio_state_hash": "ps_fixture",
                         "hoc_assessment_hash": "hoc_fixture_ready",
                         "universe_scoring_hash": "us_fixture",
                         "allocation_policy_version":
                             "reallocation_allocation_policy.v1"},
            "proposal": {"proposal_state": "READY", "portfolio": {"nav": nav},
                         "allocations": allocations,
                         # Release 29.3 — materiality is derived by api.portfolio_decision
                         # from the proposal's OWN action counts. They are published ONLY
                         # for the AWAITING_REVIEW cohort, whose whole purpose is to reach
                         # the manual-review state; every pre-existing scenario keeps the
                         # exact artifact it had before, byte for byte.
                         **({"action_counts": {
                             a: sum(1 for r in allocations if r["action"] == a)
                             for a in ("RETAIN", "INCREASE", "REDUCE", "EXIT", "ADD",
                                       "REPLACE_OUT", "REPLACE_IN")}}
                            if spec["execution"] == "AWAITING_REVIEW" else {}),
                         "proposal_hash": LIVE_PROPOSAL_HASH,
                         "turnover": {"one_way_turnover": round(two_way / 2.0, 6),
                                      "two_way_turnover": round(two_way, 6),
                                      "estimated_transaction_cost":
                                          round(two_way * nav * COST, 2)}}}


def _decision_record(spec: dict) -> dict:
    # Release 29.3 — "AWAITING_REVIEW": the proposal exists but NO manual decision has
    # been recorded, so the portfolio-decision owner must say PROPOSAL_REVIEW_REQUIRED.
    if spec["execution"] in ("NONE", "AWAITING_REVIEW"):
        return None
    return {"record_id": "pdec_%s_%s" % (DATE, BOOK), "decision": pdec.DECISION_APPROVE,
            "proposal_id": "reap_%s_%s" % (DATE, BOOK),
            "proposal_hash": LIVE_PROPOSAL_HASH,
            "binding": {"active_book_id": BOOK, "eligible_market_date": DATE,
                        "proposal_hash": LIVE_PROPOSAL_HASH}}


def _close_progress(spec: dict) -> dict:
    """The probe-free daily-close progress the workflow owner reads.

    Stage 21 (Workstream 0B): the payload carries the DURABLE RUN CONTRACT, so the
    hermetic world is faithful to what `api.daily_close.load_close_progress` actually
    returns. Without it the acceptance backend answered the progress route with a stub
    that had no run identity and no retry contract - the exact ambiguity Workstream 0B
    exists to remove, reproduced inside the harness meant to prove it was removed.
    """
    closed = (spec["prior_market_date"] if spec["close"] == "DUE"
              else spec["eligible_market_date"])
    # Release 29.4 — which COMPLETED status the journal recorded. Both are equally a
    # completed operational close; only api.daily_close decides that, and it decides it
    # from the close's own run record, never from a portfolio finding.
    status = (dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
              if spec.get("close_status") == "MEMBERSHIP_DRIFT"
              else dc.CLOSE_COMPLETE_HOLD)
    return {
        "status": status, "final_close_status": status,
        "running": False, "done": True, "market_date": closed,
        "schema_version": dc.CLOSE_RUN_SCHEMA_VERSION,
        "outcome": dc.RUN_COMPLETED,
        "run_state_vocabulary": list(dc.RUN_STATE_VOCAB),
        "run_id": "dcr_%s_%s_fixture" % (closed, BOOK),
        "idempotency_key": "%s|%s" % (BOOK, closed),
        "book_id": BOOK,
        "writes_occurred": True,
        "safe_retry_allowed": False,
        "retry_guidance": ("The close for %s is already recorded as COMPLETED. Re-running "
                           "it would be a no-op; nothing needs to be retried." % closed),
        "client_timeout_is_not_an_outcome": True,
    }


def _hoc_provenance(spec: dict) -> dict:
    """Release 29.5 — the ONE provenance declaration for this world's HOC artifact.

    Read by BOTH the persisted artifact (which the Daily Action Gate classifies) and
    ``_research_cycle_status`` (which the workflow owner reads as the cycle verdict), so
    the gate and the cycle owner can never disagree about what the artifact is.
    """
    legacy = "COMPLETE" if spec.get("hoc_artifact") != "ABSENT" else "NOT_STARTED"
    state = spec.get("drc") or legacy
    producer = spec.get("hoc_producer") or (
        hoc.PRODUCER_DAILY_RESEARCH_CYCLE if state == "COMPLETE"
        else hoc.PRODUCER_EVENT_SIGNAL_REFRESH)
    run_id = None
    if producer == hoc.PRODUCER_DAILY_RESEARCH_CYCLE:
        # A governed run stamps its id. When the cycle is INCONSISTENT that id is the
        # ORPHANED claim — a manifest the cycle owner cannot read — which is precisely
        # the corruption the fail-closed blocker still has to catch.
        run_id = ("drc_%s_fixture000" % spec["eligible_market_date"]
                  if state == "COMPLETE"
                  else "drc_%s_orphaned" % spec["eligible_market_date"])
    return hoc.build_provenance(producer_owner=producer, drc_run_id=run_id)


def _research_cycle_status(spec: dict) -> dict:
    """Release 29.5 — the DAILY RESEARCH CYCLE owner's verdict for this world.

    Before 29.5 this was derived as "an opportunity-cost artifact exists, therefore the
    cycle COMPLETED". That inference is the live defect in miniature: since Releases 28/29
    the event-driven refresh writes a real artifact through the same canonical owner, so
    artifact existence stopped being evidence that the governed cycle ran. Scenarios now
    DECLARE the cycle verdict, and the artifact's producer is declared with it.
    """
    legacy = "COMPLETE" if spec.get("hoc_artifact") != "ABSENT" else "NOT_STARTED"
    state = spec.get("drc") or legacy
    governed = (state == "COMPLETE")
    present = (spec.get("hoc_artifact") != "ABSENT")
    # The SAME declaration the persisted artifact carries, classified by the artifact
    # owner rather than re-derived here.
    prov = hoc.classify_artifact_provenance({hoc.PROVENANCE_KEY: _hoc_provenance(spec)})
    out = {
        "state": state,
        "run_id": prov["drc_run_id"] if governed else None,
        "blockers": [],
        "executable": state in ("NOT_STARTED", "INCONSISTENT"),
        "governed_research_evidence_current": governed,
        "governed_manifest_run_id": prov["drc_run_id"] if governed else None,
        "governed_evidence_owner": "api.daily_research_cycle",
        "opportunity_cost_selected": present,
        "opportunity_cost_artifact_class": prov["artifact_class"] if present else None,
        "opportunity_cost_producer_owner": prov["producer_owner"] if present else None,
        "opportunity_cost_claims_drc_terminal": bool(present
                                                     and prov["claims_drc_terminal"]),
        "opportunity_cost_proves_drc_complete": False,
    }
    if state == "INCONSISTENT":
        # An artifact claiming a run manifest that cannot be read: the genuine split
        # brain. The claimed run is NAMED so the operator knows which run failed.
        out["blockers"] = [{
            "code": drcmod.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST,
            "detail": ("An artifact claiming governed DRC terminal provenance has no "
                       "readable run manifest."),
            "claimed_drc_run_id": prov["drc_run_id"]}]
    return out


def _daily_close_payload(spec: dict, opbook: dict, nav: float) -> dict:
    """Release 29.3 — the composed DAILY CLOSE read payload.

    Before 29.3 the acceptance backend left ``GET /v1/operations/daily-close`` unbound,
    so it answered from the (empty) throwaway store and the Today money lane rendered
    as a hole: NAV, P&L, drawdown, cash and holdings were all absent from every
    acceptance screenshot, which is exactly the region the visual acceptance has to
    judge. The payload below mirrors the live production shape so the hermetic Today
    screen is faithful. It is synthetic and derived from the scenario spec only — no
    live store, no provider, no write.
    """
    closed = (spec["prior_market_date"] if spec["close"] == "DUE"
              else spec["eligible_market_date"])
    cash = float((opbook.get("book") or {}).get("cash") or 0.0)
    holdings = len((opbook.get("holdings") or []))
    # Release 29.4 — a PROCESSED close reports the status the journal actually recorded,
    # so the Today card is faithful to a MEMBERSHIP_DRIFT world rather than always
    # showing HOLD. A DUE close is still simply due.
    if spec["close"] == "DUE":
        status = dc.CLOSE_DUE
    elif spec.get("close_status") == "MEMBERSHIP_DRIFT":
        status = dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
    else:
        status = dc.CLOSE_COMPLETE_HOLD
    return {
        "status": "DAILY_CLOSE_OK",
        "close_status": status,
        "close_status_label": dc._PRESENTATION[status]["label"],  # noqa: SLF001
        "book_id": BOOK,
        "latest_eligible_market_date": spec["eligible_market_date"],
        "last_processed_date": closed,
        "current_valuation_date": closed,
        "holdings_count": holdings,
        "proposed_change_count": 0,
        "operational_close": True,
        "pnl": {
            "valuation_date": closed,
            "starting_capital": 100000.0,
            "nav": round(nav, 2),
            "cash": round(cash, 2),
            "invested_value": round(nav - cash, 2),
            "daily_pnl": -574.57,
            "daily_return_pct": -0.5696,
            "daily_pnl_available": True,
            "daily_pnl_display": None,
            "cumulative_pnl": round(nav - 100000.0, 2),
            "cumulative_return_pct": round((nav - 100000.0) / 1000.0, 4),
            "spy_cumulative_return_pct": 3.3797,
            "excess_return_pct": -3.0762,
            "drawdown_pct": -1.8665,
            "basis": "CURRENT_ECONOMIC_STATE",
        },
        # The operator presentation the card renders: headline, explanation and the
        # (non-executing) primary action. Same shape the live owner returns.
        "headline": dc._PRESENTATION[status]["headline"],   # noqa: SLF001
        "next_action": dc._PRESENTATION[status]["next_action"],   # noqa: SLF001
        "current_task": dc._PRESENTATION[status]["current_task"],   # noqa: SLF001
        "primary_action": {
            "label": dc._PRESENTATION[status]["primary_action_label"],   # noqa: SLF001
            "kind": dc._PRESENTATION[status]["primary_action_kind"],   # noqa: SLF001
            "enabled": bool(spec["close"] == "DUE"),
            "runs_daily_close": bool(spec["close"] == "DUE"),
        },
        "operational_close": {"complete": bool(spec["close"] != "DUE"),
                              "market_date": closed, "valid": True},
        "checks_summary": {"line": ""},
        "safety": {"paper_only": True, "manual_review": True, "automation_off": True,
                   "creates_orders": False},
    }


def compose(scenario_key: str, *, root=None) -> dict:
    """Derive EVERY canonical panel payload for ONE scenario from ONE world.

    This is the whole point of Stage 20.1: a single synthetic world in, a complete set of
    mutually consistent endpoint payloads out — each produced by its REAL canonical owner.
    """
    spec = scenarios()[scenario_key]
    tmp = Path(root) if root else Path(tempfile.mkdtemp(prefix="stage20_1_"))
    sdir = seed_desk(spec, tmp)
    ldir = seed_ledger(spec, tmp)
    # Stage 22 HARD ISOLATION. Every canonical artifact reader falls back to its
    # PRODUCTION root when no artifact is injected (``artifact=None`` means "not
    # supplied", not "none exists"). A scenario that legitimately has no opportunity-cost
    # or reallocation artifact therefore used to read the operator's REAL store — so the
    # composed "synthetic" world could silently contain live evidence, and an absent
    # artifact was unrepresentable. Every such reader is now pointed at this scenario's
    # OWN empty root, so "no artifact" is a fact of the world rather than an accident.
    art_root = tmp / "artifact_stores"
    hoc_store = art_root / "hoc"
    realloc_store = art_root / "realloc"
    # The scenario's OWN (empty) corporate-action registry. Without it the staleness
    # readers resolved the operator's real registry, so every synthetic assessment was
    # reported stale against a live corporate action that this world never declared.
    ca_store = art_root / "corporate_actions"
    for _p in (hoc_store, realloc_store, ca_store):
        _p.mkdir(parents=True, exist_ok=True)

    # 1. Operational book — the REAL lineage-aware fold over the seeded ledgers. The
    #    canonical operational facts live under the `operational_book` block; every other
    #    panel below is derived from THAT, so no two panels can describe different books.
    #    `ledger_dir` MUST be the seeded snapshot root: without it the book resolves its
    #    confirmed target from the real research ledger — a foreign world.
    readiness = _target_readiness(spec)
    opbook_payload = ob.load_operational_book(desk_dir=sdir, ledger_dir=ldir, today=NEXT,
                                              target_readiness=readiness)
    opbook = opbook_payload.get("operational_book") or {}
    pstate = _portfolio_state(spec, opbook)

    # 2. Research evidence artifacts (truthful provenance).
    assessment, prs_art = _reassessment_artifact(spec)
    realloc_art = _reallocation_artifact(spec, opbook.get("nav") or NAV)
    decision = _decision_record(spec)

    # 3. Stage-19 controlled-rebalance lifecycle — REAL owner over the same desk.
    rebalance = rbx.load_rebalance_state(
        desk_dir=sdir, plan_dir=tmp / "plans", actions_dir=tmp / "ca",
        active_book_id=BOOK, eligible_market_date=spec["eligible_market_date"],
        portfolio_state=pstate, artifact=realloc_art, decision_record=decision)

    # 4. The three research read contracts — REAL owners, artifacts injected.
    #    Stage 22: an ABSENT opportunity-cost artifact is the POST-CLOSE world, and it is
    #    modelled by genuinely withholding the artifact — never by hand-writing a state.
    hoc_artifact = ({"assessment": assessment,
                     "identity": {"active_book_id": BOOK,
                                  "eligible_market_date": spec["eligible_market_date"],
                                  "assessment_hash": assessment["assessment_hash"]},
                     "input_contract": {
                         "eligible_market_date": spec["eligible_market_date"],
                         "active_book_id": BOOK},
                     "assessment_id": "hoc_%s_%s" % (DATE, BOOK),
                     # Release 29.5 — the artifact STATES its producer, exactly as the
                     # real one does. Without it the gate would classify the fixture's
                     # artifact from an ABSENT provenance block while the cycle seam
                     # declared something else, and the two owners this release exists to
                     # reconcile would disagree inside the harness built to prove they
                     # agree. Both read `_hoc_provenance(spec)`, so there is exactly ONE
                     # declaration behind the gate's view and the cycle's view.
                     hoc.PROVENANCE_KEY: _hoc_provenance(spec)}
                    # READY and DEGRADED are BOTH persistable production states (the
                    # owner writes an artifact for either); only a blocked / absent
                    # assessment leaves no artifact behind.
                    if (spec["hoc_state"] in ("READY", "DEGRADED")
                        and spec.get("hoc_artifact") != "ABSENT") else None)
    hoc_payload = hoc.load_holding_opportunity_cost(
        portfolio_state=pstate, artifact=hoc_artifact, hoc_dir=hoc_store)
    realloc_payload = ralloc.load_reallocation_proposal(
        portfolio_state=pstate, artifact=realloc_art,
        reallocation_dir=realloc_store)
    reassessment = prs.load_portfolio_reassessment(
        portfolio_state=pstate, artifact=prs_art, rebalance_state=rebalance)

    # 5. The Daily Action Gate — the shared path through which the workflow owner reads
    #    the HOC and reallocation states. Fed the SAME artifacts, so HOC / reallocation /
    #    reassessment provenance can never disagree across panels.
    gate = dag.load_daily_action_gate(
        today=NEXT, operational=opbook_payload, current=_engine_current(spec),
        opportunity_cost=hoc.load_assessment_summary(
            active_book_id=BOOK, eligible_market_date=spec["eligible_market_date"],
            artifact=hoc_artifact, hoc_dir=hoc_store, actions_dir=ca_store),
        reallocation=ralloc.load_proposal_summary(
            active_book_id=BOOK, eligible_market_date=spec["eligible_market_date"],
            artifact=realloc_art, reallocation_dir=realloc_store,
            actions_dir=ca_store))

    # 6. The operator workflow state — ONE primary action, Stage-19 precedence applied.
    #    Every raw read model is injected from the SAME world, so the freshness contract
    #    and the market session it derives are the scenario's own — never the live one.
    # Stage 21 (Workstream 0D): the freshness contract must be the SCENARIO's, never the
    # live one. It was previously the only read model left uninjected, so the workflow
    # owner resolved the REAL market session while every other panel used the frozen
    # scenario date. The moment the wall clock advanced past the scenario's eligible
    # session that produced TARGET_READINESS_MISMATCH / ASSESSMENT_AHEAD_OF_ELIGIBLE_SESSION
    # and collapsed scenarios 4, 5 and 5b to INSPECT_STATE_INCONSISTENCY — a harness that
    # decays with the calendar rather than a real defect in the product.
    # Third live-world read (Stage 21, Workstream 0F): with `daily_close_status` omitted the
    # freshness owner loaded the REAL close progress, so `latest_daily_close` reported the
    # operator's actual last close and went FUTURE_DATED against the frozen session. The
    # scenario already owns its close journal — inject the same one it seeded.
    # Stage 22: ONE clock per scenario. A pre-close world needs a LIVE weekday datetime
    # before the 17:30 ET cutoff — the offline `reference_today` rule always resolves an
    # already-completed session, so BEFORE_SESSION_CLOSE is unreachable through it and a
    # "waiting for the close" scenario could only ever have been faked.
    clock = _clock(spec)
    scenario_freshness = df.load_data_freshness(
        operational=opbook_payload,
        inputs=_research_inputs(spec),
        daily_status={"latest_valid_mark_date": spec["eligible_market_date"]},
        desk_marks=desk.read_marks(sdir),
        daily_close_status=_close_progress(spec),
        forward_status={"latest_snapshot_date": spec["eligible_market_date"]},
        **clock)
    workflow = wfs.load_workflow_state(
        operational=opbook_payload, gate=gate,
        close_progress=_close_progress(spec),
        freshness=scenario_freshness,
        inputs=_research_inputs(spec),
        daily_status={"latest_valid_mark_date": spec["eligible_market_date"]},
        desk_marks=desk.read_marks(sdir),
        forward_status={"latest_snapshot_date": spec["eligible_market_date"]},
        target_readiness=readiness,
        research_cycle=_research_cycle_status(spec),
        # Stage 22: bind the LAST two seams too. Before this the workflow owner read the
        # reassessment summary and the recorded portfolio decision from the operator's
        # REAL artifact stores while every other panel used the scenario's world.
        reassessment_summary=prs.load_reassessment_summary(
            active_book_id=BOOK, eligible_market_date=spec["eligible_market_date"],
            artifact=prs_art),
        # Release 29.3 HERMETICITY: api.workflow_state treats decision_record=None as
        # "not injected" and falls back to loading the REAL portfolio-decision store.
        # A scenario with a proposal but deliberately NO recorded decision therefore
        # picked up the operator's live APPROVE record. An empty dict is an explicit
        # "no decision exists" and blocks the fallback.
        decision_record=(decision or {}),
        **clock)

    return {
        "scenario": scenario_key, "title": spec["title"], "owner": SCENARIO_OWNER,
        "world": {k: v for k, v in spec.items() if k != "hoc_reviews"},
        "expect": spec["expect"],
        "panels": {
            "portfolio_state": pstate,
            "operational_book": opbook_payload,
            "rebalance": rebalance,
            "holding_opportunity_cost": hoc_payload,
            "reallocation_proposal": realloc_payload,
            "portfolio_reassessment": reassessment,
            "daily_action_gate": gate,
            "workflow_state": workflow,
            "daily_close": _daily_close_payload(
                spec, opbook, opbook.get("nav") or NAV),
        },
        "artifacts": {"reassessment": prs_art, "reallocation": realloc_art,
                      "decision": decision, "hoc_assessment": assessment},
        "desk_dir": str(sdir),
        "consistency": cross_panel_consistency({
            "portfolio_state": pstate, "operational_book": opbook,
            "rebalance": rebalance, "holding_opportunity_cost": hoc_payload,
            "reallocation_proposal": realloc_payload,
            "portfolio_reassessment": reassessment, "daily_action_gate": gate,
            "workflow_state": workflow}, spec),
    }


# =========================================================================== #
# Cross-panel consistency verdict (Workstream G) — deterministic, no rendering.
# =========================================================================== #
#: Every action code that MUTATES normal workflow state. At most ONE of these may be
#: offered at a time, and none at all while a Stage-19 execution is pending with no
#: newly eligible close.
MUTATION_ACTION_CODES = (
    "RUN_DAILY_CLOSE", "WAIT_FOR_OR_REFRESH_OWNED_DATA", "RUN_DAILY_RESEARCH_CYCLE",
    "RUN_RESEARCH_CYCLE", "REVIEW_PORTFOLIO_PROPOSAL", "INITIALIZE_ALPHA_BOOK",
    "REFRESH_DESK", "CONFIRM_REBALANCE", "REVIEW_AND_CONFIRM",
)

#: Operational-book lifecycle actions that genuinely advance the book (and so compete for
#: the operator's single primary action). The passive monitoring states are NOT here.
_OPERATIONAL_MUTATION_CODES = (
    "INITIALIZE_ALPHA_BOOK", "CONFIRM_TARGET_SNAPSHOT", "GENERATE_ORDER_PLAN",
    "REVIEW_ORDER_PLAN", "CONFIRM_ORDER_PLAN", "CONFIRM_PAPER_ORDERS",
    "REVIEW_AND_CONFIRM_ORDER_PLAN",
)


def _op(panels: dict) -> dict:
    """The canonical operational-book block, however the payload is nested."""
    raw = panels.get("operational_book") or {}
    inner = raw.get("operational_book")
    return inner if isinstance(inner, dict) else raw


def _mutation_actions(panels: dict) -> list[dict]:
    """Every ENABLED normal-workflow mutation CTA the composed payloads offer."""
    out = []
    primary = (panels.get("workflow_state") or {}).get("primary_action") or {}
    if (primary.get("action_code") in MUTATION_ACTION_CODES
            and primary.get("execution_available")):
        out.append({"panel": "workflow_state", "action": primary.get("action_code")})
    pres = (panels.get("portfolio_reassessment") or {}).get("presentation") or {}
    if pres.get("primary_action") in MUTATION_ACTION_CODES:
        out.append({"panel": "portfolio_reassessment",
                    "action": pres.get("primary_action")})
    reb = panels.get("rebalance") or {}
    if reb.get("confirmation_available"):
        out.append({"panel": "rebalance", "action": "CONFIRM_REBALANCE"})
    # The Operational Book panel renders the CANONICAL presentation contract (Phase
    # 27B.2/27B.5), never the legacy flat `next_action_code`: in every post-submission
    # state that raw code is still the pre-Stage-19.3 `REFRESH_DESK`, while the canonical
    # contract already replaces it with the passive lifecycle task ("Monitor Pending Paper
    # Orders" / "Await Next Eligible Close") and `requires_separate_desk_refresh = False`.
    # Judging the raw code here would report a competing desk-refresh CTA the operator is
    # never actually offered. Only a genuinely enabled book-lifecycle mutation counts.
    cs = (_op(panels).get("canonical_state")
          or (panels.get("operational_book") or {}).get("canonical_state") or {})
    code = cs.get("next_action_code")
    offered = (code in _OPERATIONAL_MUTATION_CODES
               or (code == "REFRESH_DESK" and cs.get("requires_separate_desk_refresh")))
    if offered and cs.get("primary_action_enabled"):
        out.append({"panel": "operational_book", "action": code})
    return out


def cross_panel_consistency(panels: dict, spec: dict) -> dict:
    """Deterministic cross-panel verdict: do all panels describe the SAME world?"""
    violations: list[str] = []
    opb = _op(panels)
    pstate = panels.get("portfolio_state") or {}
    reb = panels.get("rebalance") or {}
    prsp = panels.get("portfolio_reassessment") or {}
    exp = spec["expect"]

    lineage = ((opb.get("pending_orders") or {}).get("current_rebalance")) or {}

    # 1. Book initialization must agree everywhere.
    init_states = {
        "operational_book": bool(opb.get("initialized", opb.get("holdings_count"))),
        "portfolio_state": bool((pstate.get("active_book") or {}).get("initialized")),
    }
    if len(set(init_states.values())) > 1:
        violations.append("BOOK_INITIALIZATION_DISAGREES: %s" % init_states)
    if exp["book_initialized"] and not all(init_states.values()):
        violations.append("BOOK_SHOULD_BE_INITIALIZED_BUT_A_PANEL_SAYS_NOT")

    # 2. Current-rebalance lineage counts must match the declared world exactly and stay
    #    separate from the historical and superseded cohorts.
    if int(lineage.get("submitted_count") or 0) != exp["current_submitted"]:
        violations.append("CURRENT_SUBMITTED_MISMATCH: %s != %s"
                          % (lineage.get("submitted_count"), exp["current_submitted"]))
    if int(lineage.get("filled_count") or 0) != exp["current_filled"]:
        violations.append("CURRENT_FILLED_MISMATCH: %s != %s"
                          % (lineage.get("filled_count"), exp["current_filled"]))
    if int(lineage.get("historical_implementation_fill_count") or 0) != exp["historical_fills"]:
        violations.append("HISTORICAL_FILL_COHORT_MISMATCH: %s != %s"
                          % (lineage.get("historical_implementation_fill_count"),
                             exp["historical_fills"]))
    if int(lineage.get("superseded_order_count") or 0) != exp["superseded_cancelled"]:
        violations.append("SUPERSEDED_COHORT_MISMATCH: %s != %s"
                          % (lineage.get("superseded_order_count"),
                             exp["superseded_cancelled"]))

    # 3. Stage-19 execution precedence: the rebalance lifecycle and the reassessment lane
    #    must agree about whether an execution is in flight.
    exec_active = bool((prsp.get("execution_precedence") or {}).get("execution_active"))
    if exec_active != bool(exp["execution_precedence"]):
        violations.append("EXECUTION_PRECEDENCE_MISMATCH: %s != %s"
                          % (exec_active, exp["execution_precedence"]))
    if exp["execution_precedence"] and reb.get("rebalance_state") != rbx.RB_PLAN_CONFIRMED:
        violations.append("REBALANCE_STATE_NOT_EXECUTION_PENDING: %s"
                          % reb.get("rebalance_state"))
    if exp["execution_precedence"] and int(lineage.get("submitted_count") or 0) == 0:
        violations.append("EXECUTION_PENDING_BUT_NO_SUBMITTED_ORDERS")

    # 4. The reassessment state must be the declared one.
    if exp["state"] and prsp.get("state") != exp["state"]:
        violations.append("REASSESSMENT_STATE_MISMATCH: %s != %s"
                          % (prsp.get("state"), exp["state"]))

    # 5. AT MOST ONE enabled normal mutation CTA across the ENTIRE page.
    mutations = _mutation_actions(panels)
    if len(mutations) > 1:
        violations.append("MULTIPLE_MUTATION_ACTIONS: %s" % mutations)
    expected_primary = exp["primary_action"]
    offered = [m["action"] for m in mutations]
    if expected_primary is None and offered:
        violations.append("EXPECTED_NO_PRIMARY_ACTION_BUT_OFFERED: %s" % offered)
    if expected_primary is not None and expected_primary not in offered:
        violations.append("EXPECTED_PRIMARY_ACTION_MISSING: %s (offered %s)"
                          % (expected_primary, offered))

    # 6. STAGE 22 — the canonical NORMAL CYCLE. The workflow state, the cycle stage, the
    #    per-stage gates and the stale-evidence hierarchy must all agree, and AT MOST ONE
    #    stage gate may be open. This is what makes "no alternate path" checkable rather
    #    than a claim in a comment.
    wf = panels.get("workflow_state") or {}
    cyc = wf.get("normal_cycle") or {}
    gates = cyc.get("stage_gates") or {}
    ec = wf.get("evidence_classification") or {}
    if exp.get("workflow_state") and wf.get("overall_state") != exp["workflow_state"]:
        violations.append("WORKFLOW_STATE_MISMATCH: %s != %s"
                          % (wf.get("overall_state"), exp["workflow_state"]))
    if exp.get("cycle_stage") and cyc.get("current_stage") != exp["cycle_stage"]:
        violations.append("CYCLE_STAGE_MISMATCH: %s != %s"
                          % (cyc.get("current_stage"), exp["cycle_stage"]))
    if exp.get("evidence_class") and ec.get("classification") != exp["evidence_class"]:
        violations.append("EVIDENCE_CLASSIFICATION_MISMATCH: %s != %s"
                          % (ec.get("classification"), exp["evidence_class"]))
    open_gates = sorted(k for k, v in gates.items() if v.get("execution_allowed"))
    if len(open_gates) > 1:
        violations.append("MULTIPLE_OPEN_STAGE_GATES: %s" % open_gates)
    if open_gates and open_gates[0] != cyc.get("current_stage"):
        violations.append("OPEN_GATE_IS_NOT_THE_CURRENT_STAGE: %s vs %s"
                          % (open_gates, cyc.get("current_stage")))
    if exp.get("mutation_count") is not None and len(mutations) != exp["mutation_count"]:
        violations.append("MUTATION_COUNT_MISMATCH: %d != %d"
                          % (len(mutations), exp["mutation_count"]))
    # A demoted (expected-stale) assessment must never compete with the primary action,
    # and it must still fail closed against a portfolio change.
    if ec.get("classification") == "EXPECTED_STALE_EVIDENCE":
        if ec.get("competes_with_primary_action"):
            violations.append("DEMOTED_EVIDENCE_STILL_COMPETES")
        if not ec.get("blocks_portfolio_action"):
            violations.append("DEMOTED_EVIDENCE_STOPPED_FAILING_CLOSED")
        if ec.get("is_operational_incident"):
            violations.append("EXPECTED_STALE_EVIDENCE_RENDERED_AS_INCIDENT")

    # 7. STAGE 22 — ONE CLOCK PER SCENARIO, structurally. The operational panel states the
    #    latest completed session twice: the alpha-target readiness contract states it, and
    #    the desk-mark readiness states it again as the session the marks are required for.
    #    In production both are the same clock call, so they cannot disagree. A hermetic
    #    world that froze only the first left the second reading the REAL calendar, and the
    #    whole panel quietly degraded to DESK_MARK_BEHIND / REFRESH_DESK_MARKS the day the
    #    wall clock passed the frozen session — a harness that decays with the calendar,
    #    which is precisely what Stage 21 Workstream 0F set out to make impossible. Checked
    #    here, in the scenario owner, so no future panel can reintroduce a second clock.
    eligible = spec["eligible_market_date"]
    required_dates = {
        "current_target.latest_completed_market_date":
            (opb.get("current_target") or {}).get("latest_completed_market_date"),
        "desk_mark_required_date": opb.get("desk_mark_required_date"),
    }
    for field, value in required_dates.items():
        if value is not None and value != eligible:
            violations.append("REQUIRED_SESSION_NOT_FROZEN: %s = %s != %s"
                              % (field, value, eligible))

    return {"consistent": not violations, "violations": violations,
            "workflow_state": wf.get("overall_state"),
            "cycle_stage": cyc.get("current_stage"),
            "cycle_next_stage": cyc.get("next_stage"),
            "open_stage_gates": open_gates,
            "evidence_classification": ec.get("classification"),
            "evidence_presentation_class": ec.get("presentation_class"),
            "assessment_binding_state": (wf.get("assessment_binding") or {}).get("state"),
            "mutation_actions": mutations,
            "mutation_action_count": len(mutations),
            "required_session_dates": required_dates,
            "desk_mark_status": opb.get("desk_mark_status"),
            "current_rebalance": {
                "order_plan_id": lineage.get("order_plan_id"),
                "submitted_count": lineage.get("submitted_count"),
                "filled_count": lineage.get("filled_count"),
                "buy_count": lineage.get("buy_count"),
                "sell_count": lineage.get("sell_count")},
            "historical_implementation_fill_count":
                lineage.get("historical_implementation_fill_count"),
            "superseded_order_count": lineage.get("superseded_order_count"),
            "counts_are_lineage_scoped": lineage.get("counts_are_lineage_scoped")}


# =========================================================================== #
# Fixture emission + hermetic seeding
# =========================================================================== #
def build() -> dict:
    """The complete fixture document: every scenario, every panel, one world each."""
    out = {}
    for key in SCENARIO_KEYS:
        composed = compose(key)
        out[key] = {
            "title": composed["title"],
            "expect_state": composed["expect"]["state"],
            "expect_primary_action": composed["expect"]["primary_action"],
            "expect_attention": composed["expect"]["attention"],
            "expect_execution_precedence": composed["expect"]["execution_precedence"],
            "panels": composed["panels"],
            "consistency": composed["consistency"],
        }
    return {
        "generated_by": SCENARIO_OWNER,
        "owner": prs.COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "hermetic": True, "live_store_read": False, "provider_called": False,
        "shared_scenario_contract": True,
        "scenarios": out,
    }


def seed(*, reassessment_dir, scenario: str, book_id: str = BOOK, eligible=DATE) -> dict:
    """Seed a HERMETIC acceptance root with the COMPLETE world for ONE scenario.

    Stage 20.1: this used to write only the reassessment artifact, which is exactly why
    every other panel rendered an unrelated default world. It now writes the whole world —
    desk ledgers (book / marks / historical fills / lineage orders / close journal) and
    every research artifact store — into the throwaway acceptance root.
    """
    spec = scenarios()[scenario]
    root = Path(reassessment_dir)
    # `reassessment_dir` is the reassessment store INSIDE the acceptance root.
    accept_root = root.parent
    sdir = seed_desk(spec, accept_root)
    ldir = seed_ledger(spec, accept_root)

    opbook_payload = ob.load_operational_book(desk_dir=sdir, ledger_dir=ldir, today=NEXT,
                                              target_readiness=_target_readiness(spec))
    opbook = opbook_payload.get("operational_book") or {}
    assessment, prs_art = _reassessment_artifact(spec)
    realloc_art = _reallocation_artifact(spec, opbook.get("nav") or NAV)
    decision = _decision_record(spec)

    art_id = "prs_%s_%s_%s" % (eligible or "nodate", book_id, scenario[:12])
    prs_art = dict(prs_art)
    prs_art["reassessment_id"] = art_id
    prs_art["identity"] = dict(prs_art["identity"], active_book_id=book_id,
                               eligible_market_date=eligible)
    prs_art["reassessment"] = dict(prs_art["reassessment"], active_book_id=book_id,
                                   eligible_market_date=eligible)
    _write_artifact_store(accept_root, root.name, art_id, prs_art,
                          key_field="artifact_id",
                          extra={"reassessment_hash":
                                 prs_art["identity"]["reassessment_hash"],
                                 "generated_at": prs_art["generated_at"]})

    if spec["hoc_state"] in ("READY", "DEGRADED"):
        hoc_art = {"assessment_id": "hoc_%s_%s" % (eligible, book_id),
                   "assessment": assessment,
                   "identity": {"active_book_id": book_id,
                                "eligible_market_date": eligible,
                                "assessment_hash": assessment["assessment_hash"]}}
        _write_artifact_store(accept_root, "hoc", hoc_art["assessment_id"], hoc_art,
                              key_field="artifact_id",
                              extra={"assessment_hash": assessment["assessment_hash"]})
    if realloc_art is not None:
        _write_artifact_store(accept_root, "realloc", realloc_art["proposal_id"],
                              realloc_art, key_field="proposal_id",
                              extra={"proposal_hash": LIVE_PROPOSAL_HASH})
    if decision is not None:
        _write_artifact_store(accept_root, "decisions", decision["record_id"], decision,
                              key_field="record_id",
                              extra={"proposal_hash": LIVE_PROPOSAL_HASH,
                                     "decision": decision["decision"]})

    composed = compose(scenario, root=accept_root / "_compose")
    return {"scenario": scenario, "title": spec["title"],
            "index_key": "%s|%s" % (book_id or "?", eligible or "?"),
            "desk_dir": str(sdir), "acceptance_root": str(accept_root),
            "expect_state": spec["expect"]["state"],
            "expect_primary_action": spec["expect"]["primary_action"],
            "expect_attention": spec["expect"]["attention"],
            "expect_execution_precedence": spec["expect"]["execution_precedence"],
            "consistency": composed["consistency"]}


def check() -> tuple[int, list[str]]:
    """Cross-panel consistency report for EVERY scenario. Returns (failures, lines)."""
    lines, failures = [], 0
    for key in SCENARIO_KEYS:
        c = compose(key)
        cons = c["consistency"]
        panels = c["panels"]
        ok = "OK  " if cons["consistent"] else "FAIL"
        lines.append(
            "%s %-42s state=%-20s actions=%d submitted=%s filled=%s hist=%s canc=%s"
            % (ok, key, (panels["portfolio_reassessment"] or {}).get("state"),
               cons["mutation_action_count"],
               cons["current_rebalance"]["submitted_count"],
               cons["current_rebalance"]["filled_count"],
               cons["historical_implementation_fill_count"],
               cons["superseded_order_count"]))
        for v in cons["violations"]:
            failures += 1
            lines.append("       violation: %s" % v)
    return failures, lines


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 20/20.1 hermetic UI fixtures.")
    ap.add_argument("--out", help="Fixture JSON output path.")
    ap.add_argument("--seed-dir", help="HERMETIC reassessment root to seed (acceptance).")
    ap.add_argument("--scenario", help="Scenario key to seed.")
    ap.add_argument("--book-id", default=BOOK)
    ap.add_argument("--eligible", default=DATE)
    ap.add_argument("--check", action="store_true",
                    help="Report cross-panel consistency for every scenario.")
    args = ap.parse_args(argv)

    if args.check:
        failures, lines = check()
        for ln in lines:
            print(ln)
        print("CROSS_PANEL_CONSISTENCY: %s (%d violation(s))"
              % ("OK" if not failures else "FAILED", failures))
        return 1 if failures else 0

    if args.seed_dir:
        if not args.scenario:
            ap.error("--scenario is required with --seed-dir")
        if args.scenario not in SCENARIO_KEYS:
            ap.error("unknown scenario '%s'; expected one of: %s"
                     % (args.scenario, ", ".join(SCENARIO_KEYS)))
        info = seed(reassessment_dir=args.seed_dir, scenario=args.scenario,
                    book_id=args.book_id, eligible=args.eligible)
        print(json.dumps(info, indent=2, sort_keys=True, default=str))
        return 0 if info["consistency"]["consistent"] else 1

    if not args.out:
        ap.error("--out, --seed-dir or --check is required")
    data = build()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n",
                   encoding="utf-8")
    for k, v in data["scenarios"].items():
        print("%-42s state=%-22s actions=%d consistent=%s"
              % (k, (v["panels"]["portfolio_reassessment"] or {}).get("state"),
                 v["consistency"]["mutation_action_count"],
                 v["consistency"]["consistent"]))
    print("fixtures written to: %s" % out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
