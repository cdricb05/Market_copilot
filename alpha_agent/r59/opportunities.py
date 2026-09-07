"""alpha_agent.r59.opportunities - the persistent DATA OPPORTUNITY FRONTIER.

Section K. Every time research concludes NEW_ORTHOGONAL_INFORMATION_REQUIRED,
the estate must be able to answer twelve questions without a human going back
through release documents. This module owns those answers as persistent state
rather than prose, and - critically - it re-measures them: an opportunity is a
live claim that can be resolved, not a note.

The most valuable state here is ALREADY_OWNED_UNUSED. Buying information the
estate already has and cannot read is the most expensive mistake available, so
that state is checked FIRST and, where an owned reader closes the gap, the
opportunity is resolved without any purchase.

This module purchases nothing, starts no trial, calls no paid endpoint and
creates no provider account. Purchase VERDICTS remain the property of the
existing gate (``api.data_expansion`` / ``engine.data_expansion_gate`` and the
Stage-13A analyst framework); this frontier records the state that feeds them.
"""
from __future__ import annotations

from typing import Optional

from .. import r59
from . import form4 as F4
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.opportunities"

# The twelve questions the frontier must be able to answer (section K), kept
# with the data so an artifact is self-describing.
FRONTIER_QUESTIONS = (
    "what information is missing",
    "which asset class needs it",
    "which unresolved hypothesis would it enable",
    "do we already own it but fail to parse or use it",
    "does a free proxy exist",
    "can the free proxy falsify the hypothesis first",
    "which provider offers it",
    "is historical PIT integrity available",
    "what effective sample does it unlock",
    "what is its expected information value",
    "what does it cost",
    "what is the purchase-gate verdict",
)


def seed(mem: Optional[M.ResearchMemory] = None) -> dict:
    """Declare the R59 opportunities the estate can act on.

    The owned-information families imported from R58's inventory are already in
    memory; this adds the named, cross-release opportunities that no single
    release owned.
    """
    mem = mem or M.open_memory()

    # 1. Insider direction - owned, complete, and unreadable by the pipeline.
    cov = F4.coverage_report()
    owned = cov["owned_r46_parse"]
    mem.set_opportunity(
        "INSIDER_TRANSACTION_DIRECTION",
        title="Form 4 acquired/disposed direction for the full daily universe",
        state=r59.DO_ALREADY_OWNED_UNUSED,
        asset_class=r59.AC_US_EQUITY,
        information_need="signed insider open-market activity per issuer",
        unlocks=["INSIDER_CLUSTER_BUYING", "INSIDER_NET_PURCHASE_RATIO"],
        owned_but_unused=True,
        free_proxy="SEC EDGAR Form 4 (already collected daily)",
        provider="SEC_EDGAR_FREE",
        pit_integrity="acceptance instant recorded to the second",
        effective_sample="%s business days owned" % owned["day_files"],
        expected_value="HIGH_BUT_PROSPECTIVE_ONLY",
        cost_usd_year=0.0,
        gate_verdict="NO_PURCHASE_REQUIRED",
        detail={"resolution": "alpha_agent.r59.form4 reads the owned R46 parse",
                "coverage": cov})

    # 2. Analyst revisions (Steele) - waiting on an external sample.
    mem.set_opportunity(
        "ANALYST_REVISIONS",
        title="Historical point-in-time analyst estimate revisions",
        state=r59.DO_WAITING_FOR_SAMPLE,
        asset_class=r59.AC_US_EQUITY,
        information_need="PIT consensus estimates with a revision history and "
                         "delisted coverage",
        unlocks=["EPS_REVISION_BREADTH", "REVISION_MAGNITUDE",
                 "REVISION_ACCELERATION", "ESTIMATE_DISPERSION_CHANGE"],
        free_proxy="SEC companyfacts fundamental CHANGE (owned; a weaker "
                   "substitute that measures realised results, not "
                   "expectations)",
        provider="STEELE_PENDING_SAMPLE",
        pit_integrity="UNKNOWN_UNTIL_SAMPLE_INSPECTED",
        effective_sample="UNKNOWN_UNTIL_SAMPLE_INSPECTED",
        expected_value="UNRESOLVED",
        gate_verdict="AWAITING_SAMPLE",
        detail={"prior_evidence": [
            "Stage 13B prospective revision ledger: PEAD sales 63d t=2.27",
            "Stage 13C out-of-sample replication FAILED (t -0.29)",
            "Intrinio live trial closed DO_NOT_BUY (survivorship-safe "
            "reconstruction failed)"],
            "evaluation_owner": "alpha_agent.r59.steele",
            "purchase_gate_owner": "api.data_expansion",
            "no_purchase_may_occur": True})

    # 3. Options / implied-volatility surface - investigation only.
    mem.set_opportunity(
        "OPTIONS_IMPLIED_VOLATILITY_SURFACE",
        title="Historical option implied volatility, skew and term structure",
        state=r59.DO_PURCHASE_CANDIDATE,
        asset_class=r59.AC_US_EQUITY,
        information_need="per-name implied volatility surface history with "
                         "point-in-time snapshots",
        unlocks=["IMPLIED_MINUS_REALISED_VOL", "SKEW_CHANGE",
                 "TERM_STRUCTURE_CHANGE", "EVENT_IMPLIED_VOL",
                 "VOLATILITY_RISK_PREMIUM", "OPTIONS_INFORMED_EQUITY_SELECTION"],
        free_proxy="owned &VX futures term structure (index-level only) and "
                   "owned realised-volatility state from the Norgate panel",
        provider="UNQUOTED",
        pit_integrity="REQUIRED: end-of-day snapshots as published, with no "
                      "restatement backfill",
        effective_sample="needs >=15 years and delisted-name coverage to clear "
                         "the same partition R57/R58 used",
        expected_value="GENUINELY_DISTINCT_INFORMATION",
        cost_usd_year=None,
        gate_verdict="INSUFFICIENT_EVIDENCE_TO_RECOMMEND_PURCHASE",
        detail={"why_distinct": "an implied surface is a forward-looking price "
                                "of risk; every information family the estate "
                                "owns is backward-looking realised state",
                "falsify_free_first": "the index-level volatility-risk-premium "
                                      "hypothesis can be tested on the owned "
                                      "&VX curve BEFORE any per-name purchase; "
                                      "a failure there removes the main reason "
                                      "to buy",
                "blocking_condition": "no cost is quoted and no sample has "
                                      "been inspected, so the ten-condition "
                                      "purchase gate cannot be evaluated",
                "purchase_gate_owner": "api.data_expansion"})

    # 3b. EODHD utilisation, MEASURED against the provider rather than inferred
    #     from the owned store. The probe answered three separate questions the
    #     estate had been conflating: entitlement, universe and history.
    from . import provider_probe as PP
    probe = r59.read_json(r59.research_root() / "data"
                          / "r59_eodhd_capability_probe.json")
    if probe:
        v = PP.verdict(probe)
        f = probe.get("findings") or {}
        mem.set_opportunity(
            "EODHD_NEWS_UNIVERSE",
            title="EODHD news for the full universe (collector samples 7 names)",
            state=v["state"],
            asset_class=r59.AC_US_EQUITY,
            information_need="cross-sectional news flow per issuer",
            unlocks=["NEWS_INTENSITY", "NEWS_SENTIMENT_CHANGE"],
            owned_but_unused=(v["state"] == r59.DO_ALREADY_OWNED_UNUSED),
            free_proxy="owned SEC filing events (universe-wide, free)",
            provider="EODHD",
            pit_integrity="publication timestamps present on the feed",
            effective_sample="prospective only - the endpoint served no 2016 "
                             "or 2020 history on probe",
            expected_value="PROSPECTIVE_INFORMATION_FAMILY",
            cost_usd_year=0.0,
            gate_verdict="NO_PURCHASE_REQUIRED - already entitled",
            detail={"root_cause": "sources.eodhd.sample_symbols in "
                                  "configs/alpha_agent/stage2_ingestion.json "
                                  "is a seven-name SMOKE list",
                    "entitlement_measured": f.get("news_off_sample_recent"),
                    "history_measured": f.get("news_history"),
                    "verdict": v})
        mem.set_opportunity(
            "EODHD_CORPORATE_ACTION_HISTORY",
            title="EODHD dividend/split history for the full universe",
            state=r59.DO_FREE_AVAILABLE,
            asset_class=r59.AC_US_EQUITY,
            information_need="universe-wide corporate-action history",
            unlocks=["CORPORATE_ACTION_CORRECTNESS",
                     "DIVIDEND_INITIATION_EVENTS", "SPLIT_EVENTS"],
            owned_but_unused=True,
            provider="EODHD",
            pit_integrity="dated actions; 16+ years served on probe",
            effective_sample="CAT.US returned 67 actions, 2010-01-15 to "
                             "2026-07-20",
            expected_value="REMOVES_AD_HOC_SPLIT_REPAIR_DEPENDENCE",
            cost_usd_year=0.0,
            gate_verdict="NO_PURCHASE_REQUIRED - already entitled",
            detail={"measured": f.get("dividends_off_sample_history"),
                    "why_it_matters": "the estate collects corporate actions "
                                      "for 5 tickers while 16 years of "
                                      "universe-wide history is entitled; the "
                                      "MNST split phantom was repaired by hand"})
        mem.set_opportunity(
            "EODHD_EARNINGS_SESSION_TIMING",
            title="Earnings before/after-market flag as a usable session bound",
            state=r59.DO_FREE_AVAILABLE,
            asset_class=r59.AC_US_EQUITY,
            information_need="whether a report landed before the open or after "
                             "the close",
            unlocks=["POST_EARNINGS_ANNOUNCEMENT_DRIFT",
                     "EARNINGS_REACTION_TIMING"],
            owned_but_unused=False,
            provider="EODHD",
            pit_integrity="an upper bound only; available_at stays null because "
                          "a date plus a coarse flag cannot prove an instant",
            effective_sample="6,126 rows over 6,076 symbols in a 14-day probe "
                             "window, all flagged BeforeMarket/AfterMarket",
            expected_value="ENABLES_A_PEAD_STUDY_THAT_WAS_PREVIOUSLY_BLOCKED",
            cost_usd_year=0.0,
            gate_verdict="NO_PURCHASE_REQUIRED - fixed in the canonical "
                         "collector",
            detail={"measured": f.get("earnings_calendar"),
                    "fix": "alpha_agent.collectors.eodhd._earnings_session_bound "
                           "puts a next-session public-by bound in the payload "
                           "as public_by_session_open and leaves available_at "
                           "null; an unflagged row gets no bound at all"})

    # 4. Point-in-time sector classification - a measured, blocking gap.
    mem.set_opportunity(
        "POINT_IN_TIME_SECTOR",
        title="Point-in-time GICS/sector classification history",
        state=r59.DO_BLOCKED,
        asset_class=r59.AC_US_EQUITY,
        information_need="sector membership as it was known on each decision "
                         "date",
        unlocks=["SECTOR_NEUTRAL_NORMALISATION",
                 "PIT_SECTOR_CONCENTRATION_GATE",
                 "EXACT_OPERATIONAL_LEG_REPLICATION"],
        free_proxy="current classification (used and disclosed by R58's "
                   "sector-exclusion robustness check)",
        provider="UNQUOTED",
        pit_integrity="the whole point of the dataset",
        expected_value="ENABLING_NOT_ALPHA_BEARING",
        gate_verdict="NOT_A_PURCHASE_CANDIDATE_ON_ALPHA_GROUNDS",
        detail={"note": "this buys correctness, not edge; it is what stops a "
                        "sector-neutral claim from being retrospective"})

    # 5. Intraday / execution-cost reality for the futures scopes.
    mem.set_opportunity(
        "FUTURES_EXECUTION_COST_REALITY",
        title="Measured futures bid/ask and slippage by market and hour",
        state=r59.DO_FREE_AVAILABLE,
        asset_class=r59.AC_CROSS_ASSET,
        information_need="whether the flat 2bp/side cost model flatters the "
                         "illiquid markets in the 103-market panel",
        unlocks=["COST_HONEST_FUTURES_VERDICTS"],
        free_proxy="owned Norgate volume and open interest",
        provider="NORGATE_OWNED",
        pit_integrity="daily bars only; no intraday microstructure owned",
        expected_value="PROTECTS_AGAINST_FALSE_POSITIVES",
        cost_usd_year=0.0,
        gate_verdict="NO_PURCHASE_REQUIRED",
        detail={"why_it_matters": "a uniform cost rate across 103 markets "
                                  "makes a thin market look as cheap as the "
                                  "ES contract; any future futures survivor "
                                  "must be re-tested under per-market costs "
                                  "before it can be believed"})
    return {"seeded": 5, "calculation_owner": CALCULATION_OWNER}


def reassess(mem: Optional[M.ResearchMemory] = None, *,
             opportunity_id: str) -> dict:
    """Re-measure ONE opportunity against what is on disk right now.

    Returns whether the blocker is still real. An opportunity whose blocker has
    been removed by an owned reader is RESOLVED here without any purchase, which
    is the whole reason ALREADY_OWNED_UNUSED is checked before anything else.
    """
    mem = mem or M.open_memory()
    rows = {o["opportunity_id"]: o for o in mem.opportunities()}
    row = rows.get(opportunity_id)
    if row is None:
        return {"state": "UNKNOWN_OPPORTUNITY", "resolved": False,
                "opportunity_id": opportunity_id,
                "note": "not present in the frontier"}

    # The insider family is the one R59 can actually resolve.
    if opportunity_id in ("INSIDER_TRANSACTION_DIRECTION",
                          "OWNED_INSIDER_FILING"):
        cov = F4.coverage_report()
        owned = cov["owned_r46_parse"]
        resolved = bool(owned["with_direction"]) and \
            owned["direction_populated_fraction"] == 1.0
        state = (r59.DO_FREE_AVAILABLE if resolved
                 else r59.DO_ALREADY_OWNED_UNUSED)
        note = ("owned R46 parse carries direction on %d of %d transactions "
                "(%s); readable through alpha_agent.r59.form4. Historical "
                "backtest remains impossible: %s"
                % (owned["with_direction"], owned["transactions"],
                   owned["direction_populated_fraction"],
                   cov["research_readiness"]["reason"]))
        mem.set_opportunity(
            opportunity_id, title=row["title"], state=state,
            asset_class=row.get("asset_class"),
            information_need=row.get("information_need") or "",
            unlocks=row.get("unlocks"),
            owned_but_unused=not resolved,
            free_proxy=row.get("free_proxy"), provider=row.get("provider"),
            pit_integrity=row.get("pit_integrity"),
            effective_sample="%s business days" % owned["day_files"],
            expected_value="HIGH_BUT_PROSPECTIVE_ONLY",
            cost_usd_year=0.0, gate_verdict="NO_PURCHASE_REQUIRED",
            detail={"resolved_by": "alpha_agent.r59.form4", "coverage": cov})
        mem.event("DATA_OPPORTUNITY_REASSESSED", subject=opportunity_id,
                  detail={"state": state, "resolved": resolved})
        return {"state": state, "resolved": resolved,
                "opportunity_id": opportunity_id, "note": note}

    # Owned families imported from R58's inventory: re-state the measured
    # blocker rather than inventing progress.
    detail = row.get("detail") or {}
    reason = (detail.get("r58_classification") or row.get("pit_integrity")
              or "no owned reader closes this gap")
    mem.event("DATA_OPPORTUNITY_REASSESSED", subject=opportunity_id,
              detail={"state": row["state"], "resolved": False})
    return {"state": row["state"], "resolved": False,
            "opportunity_id": opportunity_id,
            "note": "blocker unchanged (%s); no owned reader closes it and no "
                    "purchase is permitted" % reason}


def publish(mem: Optional[M.ResearchMemory] = None) -> dict:
    """Write the frontier as a machine-readable artifact."""
    mem = mem or M.open_memory()
    rows = mem.opportunities()
    by_state: dict = {}
    for o in rows:
        by_state.setdefault(o["state"], []).append(o["opportunity_id"])
    body = {
        "calculation_owner": CALCULATION_OWNER,
        "questions_answered": list(FRONTIER_QUESTIONS),
        "n_opportunities": len(rows),
        "by_state": by_state,
        "opportunities": rows,
        "purchase_authority": "NONE - this module cannot purchase, subscribe, "
                              "start a trial or call a paid endpoint",
    }
    p = r59.write_artifact("r59_data_opportunity_frontier.json", body)
    body["artifact_path"] = str(p)
    return body
