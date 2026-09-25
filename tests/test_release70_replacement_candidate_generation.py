"""Release 70 — alpha-to-portfolio conversion: candidate generation and outcome honesty.

WHAT R70 FIXED, and what these tests hold in place.

Before R70 the holding opportunity-cost kernel computed ONE replacement candidate for
the WHOLE portfolio: ``_strongest_replacement`` was called once, before the per-holding
loop, took no holding argument, and returned the single best-ranked non-held eligible
row. Every holding was compared against that same name. Measured on the production
store, ``strongest_replacement_ticker`` was a single ticker on 900 of 900 review rows
across 36 sessions. The estate read that as evidence about that ticker. It was evidence
about a call site.

Four consequences, each covered below:

  * a portfolio could not be compared against a diverse opportunity set at all;
  * the scan aborted on a rejection COUNT, so a book holding the top-ranked names could
    produce no candidate whatsoever;
  * outcome statistics bucketed on the action governance PERMITTED, so every
    churn-withheld REPLACE was filed as a HOLD and the replacement bucket could only
    ever contain sessions where no control bound; and
  * a bucket of many rows carrying ONE distinct candidate across overlapping sessions
    published a hit rate and an adverse verdict as though it held many independent
    trials.

Hermetic: pure kernels driven by explicit contracts. No DB, provider, prediction,
network, operational ledger or real cycle is touched.
"""
from __future__ import annotations

from paper_trader.engine import holding_opportunity_cost as k
from paper_trader.engine import reassessment_outcomes as ro


# --------------------------------------------------------------------------- #
# Deterministic builders (same shape as the Slice-6 suite).
# --------------------------------------------------------------------------- #
def _rets(n, seed=1):
    return [(((i * 7 + seed * 13) % 21) - 10) / 1000.0 for i in range(n)]


def _adj_from_rets(rets, start=100.0):
    adj = [start]
    for r in rets:
        adj.append(adj[-1] * (1.0 + r))
    return adj


def _trailing(seed=1, n=130):
    rets = _rets(n, seed)
    adj = _adj_from_rets(rets)
    return {"dates": ["d%03d" % i for i in range(len(adj))], "adj": adj,
            "ret": [None] + rets}


def _urow(ticker, rank, pct, sector="Tech", adv=2e7, eligible=True):
    return {"ticker": ticker, "rank": rank, "combined_score": pct, "percentile": pct,
            "fundamental_score": pct, "fundamental_percentile": pct,
            "momentum_score": pct, "momentum_percentile": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _pos(ticker, sector, weight, mv=4000.0, qty=100):
    return {"ticker": ticker, "sector": sector, "quantity": qty,
            "current_weight": weight, "market_value": mv}


def _ic(**over):
    """Two held names in DIFFERENT sectors, each with same-sector alternatives.

    This is the shape the pre-R70 kernel could not express: whatever the universe
    held, both holdings received the same single candidate.
    """
    ic = {
        "schema_version": k.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-05",
        "active_book_id": "alpha_paper_book_1",
        "active_book_label": "Alpha Paper Book #1",
        "valuation_date": "2026-08-05",
        "portfolio_state_hash": "PSHASH",
        "universe_scoring_hash": "USHASH",
        "universe_input_contract_hash": "USIN",
        "nav": 100000.0, "cash": 8000.0,
        "positions": [_pos("AAA", "Tech", 0.04), _pos("BBB", "Energy", 0.04)],
        # Four sectors x four non-held alternatives, interleaved by rank. The universe
        # must be wider than the shortlist or every holding trivially receives the
        # whole pool and the selection logic is never exercised.
        "universe_rows": [
            _urow("AAA", 20, 0.50, "Tech"),
            _urow("BBB", 21, 0.45, "Energy"),
            _urow("TEC1", 1, 0.99, "Tech"), _urow("ENE1", 2, 0.98, "Energy"),
            _urow("HLT1", 3, 0.97, "Health"), _urow("UTL1", 4, 0.96, "Utilities"),
            _urow("TEC2", 5, 0.95, "Tech"), _urow("ENE2", 6, 0.94, "Energy"),
            _urow("HLT2", 7, 0.93, "Health"), _urow("UTL2", 8, 0.92, "Utilities"),
            _urow("TEC3", 9, 0.91, "Tech"), _urow("ENE3", 10, 0.90, "Energy"),
            _urow("HLT3", 11, 0.89, "Health"), _urow("UTL3", 12, 0.88, "Utilities"),
            _urow("TEC4", 13, 0.87, "Tech"), _urow("ENE4", 14, 0.86, "Energy"),
            _urow("HLT4", 15, 0.85, "Health"), _urow("UTL4", 16, 0.84, "Utilities"),
        ],
        "previous_ranking": {"AAA": 19, "BBB": 20},
        "previous_ranking_state": "AVAILABLE",
        "trailing_prices": {"AAA": _trailing(1), "BBB": _trailing(2)},
        "median_dollar_volume": {"AAA": 5e7, "BBB": 5e7},
        "aligned_returns": {"dates": ["d%03d" % i for i in range(60)],
                            "series": {"AAA": _rets(60, 1), "BBB": _rets(60, 2)}},
    }
    ic.update(over)
    return ic


def _run(**over):
    return k.build_assessment(input_contract=_ic(**over))


def _review(res, ticker):
    return next(r for r in res["holding_reviews"] if r["ticker"] == ticker)


# =========================================================================== #
# 1–6. Candidate generation is PER HOLDING, diverse, and bounded.
# =========================================================================== #
def test_01_every_holding_gets_a_shortlist_not_a_single_shared_name():
    res = _run()
    for r in res["holding_reviews"]:
        assert r["replacement_shortlist"], "%s received no shortlist" % r["ticker"]
        assert r["replacement_shortlist_size"] >= 2


def test_02_different_holdings_receive_different_candidates():
    """THE regression for the R70 defect. Two holdings, two distinct candidate sets."""
    res = _run()
    aaa = {c["ticker"] for c in _review(res, "AAA")["replacement_shortlist"]}
    bbb = {c["ticker"] for c in _review(res, "BBB")["replacement_shortlist"]}
    assert aaa != bbb, "both holdings received an identical shortlist"
    # Each sees an alternative in its OWN sector — the risk-neutral comparison.
    assert aaa & {"TEC1", "TEC2"}, "Tech holding saw no Tech alternative"
    assert bbb & {"ENE1", "ENE2"}, "Energy holding saw no Energy alternative"


def test_03_distinct_candidate_count_is_published_and_exceeds_one():
    dg = _run()["diagnostics"]
    assert dg["distinct_shortlist_candidates"] > 1
    assert dg["eligible_candidate_pool_size"] == 16     # sixteen non-held eligible rows
    assert isinstance(dg["non_allocated_comparison_candidate"], list)


def test_04_shortlist_is_economically_diverse_not_one_sector():
    r = _review(_run(), "AAA")
    sectors = r["replacement_shortlist_sectors"]
    assert len(sectors) >= 2, "shortlist collapsed into a single sector: %s" % sectors
    per_sector: dict = {}
    for c in r["replacement_shortlist"]:
        per_sector[c["sector"]] = per_sector.get(c["sector"], 0) + 1
    cap = k.default_policy()["max_candidates_per_sector_in_shortlist"]
    reserved = k.default_policy()["same_sector_reserved_slots"]
    for sec, n in per_sector.items():
        assert n <= max(cap, reserved), "sector %s over-represented (%d)" % (sec, n)


def test_05_strongest_candidate_is_the_best_ranked_shortlist_entry():
    """Backwards compatibility: every downstream consumer still reads this field."""
    for tk in ("AAA", "BBB"):
        r = _review(_run(), tk)
        assert r["strongest_replacement_ticker"] == r["replacement_shortlist"][0]["ticker"]
        assert r["replacement_rank"] == r["replacement_shortlist"][0]["rank"]
        ranks = [c["rank"] for c in r["replacement_shortlist"]]
        assert ranks == sorted(ranks), "shortlist is not rank-ordered"


def test_06_shortlist_size_is_bounded_by_policy():
    size = k.default_policy()["replacement_shortlist_size"]
    for r in _run()["holding_reviews"]:
        assert len(r["replacement_shortlist"]) <= size


# =========================================================================== #
# 7–9. The scan no longer aborts on a rejection COUNT.
# =========================================================================== #
def test_07_a_book_holding_the_top_ranked_names_still_gets_candidates():
    """Pre-R70 the scan broke at 25 rejections, so a book holding the top of the
    universe could produce NO candidate at all. The rejection RECORD is bounded; the
    scan is not."""
    held = [_pos("H%02d" % i, "Tech", 0.01) for i in range(30)]
    rows = [_urow("H%02d" % i, i + 1, 0.9, "Tech") for i in range(30)]
    rows.append(_urow("LATE", 99, 0.95, "Health"))     # the only non-held name
    res = _run(positions=held, universe_rows=rows,
               previous_ranking={p["ticker"]: 5 for p in held},
               trailing_prices={p["ticker"]: _trailing(i) for i, p in enumerate(held)},
               median_dollar_volume={p["ticker"]: 5e7 for p in held},
               aligned_returns={"dates": ["d%03d" % i for i in range(60)],
                                "series": {p["ticker"]: _rets(60, i)
                                           for i, p in enumerate(held)}})
    assert res["diagnostics"]["eligible_candidate_pool_size"] == 1
    assert _review(res, "H00")["strongest_replacement_ticker"] == "LATE"


def test_08_rejection_record_stays_bounded():
    limit = k.default_policy()["rejection_record_limit"]
    held = [_pos("H%02d" % i, "Tech", 0.01) for i in range(30)]
    rows = [_urow("H%02d" % i, i + 1, 0.9, "Tech") for i in range(30)]
    rows.append(_urow("LATE", 99, 0.95, "Health"))
    res = _run(positions=held, universe_rows=rows,
               previous_ranking={p["ticker"]: 5 for p in held},
               trailing_prices={p["ticker"]: _trailing(i) for i, p in enumerate(held)},
               median_dollar_volume={p["ticker"]: 5e7 for p in held},
               aligned_returns={"dates": ["d%03d" % i for i in range(60)],
                                "series": {p["ticker"]: _rets(60, i)
                                           for i, p in enumerate(held)}})
    assert len(res["diagnostics"]["rejected_candidate_scan"]) <= limit


def test_09_sector_constraint_is_holding_relative_not_global():
    """A swap moves weight FROM the incumbent's sector TO the candidate's, so the cap
    test needs the incumbent. An already-breaching sector cannot be perpetuated by a
    weight-neutral same-sector swap either."""
    cap = k.default_policy()["sector_cap_fraction"]
    res = _run(positions=[_pos("AAA", "Health", cap * 0.6),
                          _pos("BBB", "Health", cap * 0.6)],
               universe_rows=[_urow("AAA", 10, 0.50, "Health"),
                              _urow("BBB", 12, 0.45, "Health"),
                              _urow("HLT9", 1, 0.99, "Health"),
                              _urow("UTL1", 3, 0.90, "Utilities")])
    # Health is over the cap; a Health candidate keeps it over the cap -> refused.
    r = _review(res, "AAA")
    assert "HLT9" not in {c["ticker"] for c in r["replacement_shortlist"]}
    assert r["strongest_replacement_ticker"] == "UTL1"
    reasons = {x["reason"] for x in res["diagnostics"]["rejected_candidate_scan"]}
    assert "SECTOR_CONCENTRATION_CONSTRAINED" in reasons


# =========================================================================== #
# 10–11. Determinism / no duplicated evidence across repeated runs.
# =========================================================================== #
def test_10_repeated_runs_are_byte_identical():
    a, b = _run(), _run()
    assert a["assessment_hash"] == b["assessment_hash"]
    for ra, rb in zip(a["holding_reviews"], b["holding_reviews"]):
        assert ra["replacement_shortlist"] == rb["replacement_shortlist"]


def test_11_repeated_runs_do_not_multiply_distinct_candidates():
    """Running twice must not make the evidence look twice as wide."""
    a, b = _run(), _run()
    assert (a["diagnostics"]["distinct_shortlist_candidates"]
            == b["diagnostics"]["distinct_shortlist_candidates"])


# =========================================================================== #
# 12–15. The ORIGINAL action survives governance blocking.
# =========================================================================== #
def _obs(**over):
    o = {"maturity": ro.MAT_MATURE, "horizon_eligible_closes": 20,
         "recommendation": ro.REC_HOLD, "source_recommendation": ro.REC_REPLACE,
         "action_withheld": True, "withheld_reason_codes": ["CHURN_COOLDOWN_ACTIVE"],
         "ticker": "AAA", "replacement_ticker": "TEC1",
         "eligible_market_date": "2026-08-05", "realized_spread": 0.02,
         "governance_state": ro.GOV_NO_CHANGE}
    o.update(over)
    return o


def test_12_a_blocked_replace_is_not_reported_as_a_hold():
    o = _obs()
    assert ro._proposed_action(o) == ro.REC_REPLACE
    assert o["recommendation"] == ro.REC_HOLD      # governance's answer, preserved
    card = ro.build_scorecard([o])
    assert card["by_proposed_action"][ro.REC_REPLACE] == 1
    assert card["by_recommendation"][ro.REC_HOLD] == 1
    assert card["withheld_by_proposed_action"][ro.REC_REPLACE] == 1


def test_13_withheld_replacements_reach_the_replacement_bucket():
    card = ro.build_scorecard([_obs()])
    assert card["replacement_outcomes"]["observations"] == 1
    assert card["hold_outcomes"]["observations"] == 0
    assert card["replacement_outcomes_scope"] == "PROPOSED_REPLACE_INCLUDING_WITHHELD"
    assert card["withheld_replacement_outcomes"]["observations"] == 1


def test_14_a_genuine_hold_stays_a_hold():
    o = _obs(recommendation=ro.REC_HOLD, source_recommendation=ro.REC_HOLD,
             action_withheld=False, withheld_reason_codes=[])
    card = ro.build_scorecard([o])
    assert card["hold_outcomes"]["observations"] == 1
    assert card["replacement_outcomes"]["observations"] == 0


def test_15_observation_carries_immutable_decision_evidence():
    """Candidate identity, score, entry mark, decision date, costs, intended position
    change and maturity horizon are all frozen onto the observation at the decision."""
    cal = ["2026-08-%02d" % (d + 1) for d in range(31)]
    rec = {
        "ticker": "AAA", "sector": "Tech", "recommendation": ro.REC_HOLD,
        "source_recommendation": ro.REC_REPLACE, "action_withheld": True,
        "withheld_reason_codes": ["CHURN_COOLDOWN_ACTIVE"],
        "current_weight": 0.08, "market_value": 8000.0,
        "strongest_replacement_ticker": "TEC1", "replacement_rank": 1,
        "replacement_score": 0.99, "replacement_sector": "Tech",
        "signal_score": 0.50, "current_rank": 20,
        "replacement_shortlist": [{"ticker": "TEC1", "rank": 1, "score": 0.99},
                                  {"ticker": "ENE1", "rank": 2, "score": 0.98}],
        "switching_cost_bps": 25.0, "switching_cost_usd": 20.0,
        "expected_net_improvement": 0.44,
        "proposed_exposure_reduction": 0.08,
        "proposed_exposure_reduction_basis": "FULL_POSITION",
    }
    row = {"reassessment_id": "R1", "eligible_market_date": "2026-08-05",
           "active_book_id": "alpha_paper_book_1", "decision": "PROPOSAL_REVIEW"}
    obs = ro.build_observation(row=row, rec=rec, horizon=20, calendar=cal,
                               series={}, proposal=None, lineage=None)

    assert obs["source_recommendation"] == ro.REC_REPLACE      # the ORIGINAL action
    assert obs["recommendation"] == ro.REC_HOLD                # what governance allowed
    assert obs["action_withheld"] is True
    assert obs["replacement_ticker"] == "TEC1"                 # candidate identity
    assert obs["replacement_score_at_decision"] == 0.99        # candidate score
    assert obs["incumbent_score_at_decision"] == 0.50
    assert obs["shortlist_size_at_decision"] == 2              # what was considered
    assert obs["replacement_shortlist_at_decision"][0]["ticker"] == "TEC1"
    assert obs["incumbent_mark_at_decision"] == 8000.0         # entry mark
    assert obs["decision_date"] == "2026-08-05"                # decision date
    assert obs["switching_cost_usd_at_decision"] == 20.0       # applicable costs
    assert obs["switching_cost_bps_at_decision"] == 25.0
    assert obs["proposed_exposure_reduction_at_decision"] == 0.08   # intended change
    assert obs["proposed_exposure_reduction_basis"] == "FULL_POSITION"
    assert obs["horizon_eligible_closes"] == 20                # maturity horizon
    assert obs["maturity_market_date"] is not None
    assert "Frozen at the decision" in obs["evidence_immutability"]


# =========================================================================== #
# 16–19. A tiny or correlated sample cannot display a confident verdict.
# =========================================================================== #
def test_16_single_candidate_bucket_is_refused_a_verdict():
    """The exact pre-R70 shape: many rows, ONE distinct candidate."""
    rows = [_obs(eligible_market_date="2026-08-%02d" % (d + 1), realized_spread=0.01)
            for d in range(12)]
    b = ro._bucket(rows, ro.default_policy())
    assert b["distinct_candidates"] == 1
    assert b["verdict_permitted"] is False
    assert "SINGLE_CANDIDATE_NOT_EVIDENCE" in b["verdict_blocked_reason_codes"]


def test_17_overlapping_sessions_are_clustered_not_counted_separately():
    """12 consecutive sessions measured at a 20-session horizon observe overlapping
    returns; they are one cluster, not twelve independent trials."""
    rows = [_obs(eligible_market_date="2026-08-%02d" % (d + 1)) for d in range(12)]
    b = ro._bucket(rows, ro.default_policy())
    assert b["distinct_sessions"] == 12
    assert b["session_clusters"] == 1
    assert b["effective_observations"] < b["measured_spreads"]


def test_18_a_diverse_bucket_is_permitted_a_verdict():
    rows = []
    for i in range(12):
        rows.append(_obs(ticker="H%02d" % i, replacement_ticker="C%02d" % i,
                         eligible_market_date="2026-%02d-01" % (i + 1),
                         realized_spread=0.01 * (1 if i % 2 else -1)))
    b = ro._bucket(rows, ro.default_policy())
    assert b["distinct_candidates"] == 12
    assert b["effective_observations"] >= 8
    assert b["verdict_permitted"] is True


def test_19_policy_intelligence_publishes_no_adverse_finding_on_one_candidate():
    rows = [_obs(eligible_market_date="2026-08-%02d" % (d + 1), realized_spread=-0.05,
                 action_withheld=False, recommendation=ro.REC_REPLACE,
                 source_recommendation=ro.REC_REPLACE, withheld_reason_codes=[])
            for d in range(25)]
    pi = ro.build_policy_intelligence(rows)
    assert pi["replacements_above_hurdle"]["verdict_permitted"] is False
    assert pi["policy_state"] != ro.POLICY_REVIEW_CANDIDATE
    assert any("distinct candidate" in f for f in pi["findings"])


# =========================================================================== #
# 20–21. A control is judged at the horizon it DECLARES.
# =========================================================================== #
def test_20_churn_cooldown_is_evaluated_at_its_declared_horizon():
    pol = ro.default_policy()
    assert pol["control_declared_horizon"]["CHURN_COOLDOWN_ACTIVE"] == 5
    rows = []
    for i in range(14):
        for h in (5, 20):
            rows.append(_obs(ticker="H%02d" % i, replacement_ticker="C%02d" % i,
                             eligible_market_date="2026-%02d-01" % ((i % 12) + 1),
                             horizon_eligible_closes=h,
                             realized_spread=(-0.02 if h == 5 else 0.05)))
    pi = ro.build_policy_intelligence(rows)
    ctl = next(c for c in pi["controls"] if c["reason_code"] == "CHURN_COOLDOWN_ACTIVE")
    assert ctl["declared_horizon_eligible_closes"] == 5
    assert ctl["horizon_source"] == "CONTROL_DECLARED"
    # Judged at H=5 the withheld replacements LOST, so the control helped.
    assert ctl["verdict"] == "CONTROL_BENEFIT"


def test_21_an_undeclared_control_falls_back_to_the_primary_horizon():
    rows = [_obs(ticker="H%02d" % i, replacement_ticker="C%02d" % i,
                 eligible_market_date="2026-%02d-01" % ((i % 12) + 1),
                 withheld_reason_codes=["TURNOVER_BUDGET_EXHAUSTED"])
            for i in range(14)]
    pi = ro.build_policy_intelligence(rows)
    ctl = next(c for c in pi["controls"]
               if c["reason_code"] == "TURNOVER_BUDGET_EXHAUSTED")
    assert ctl["horizon_source"] == "PRIMARY_HORIZON_FALLBACK"
    assert ctl["declared_horizon_eligible_closes"] == ro.default_policy()["primary_horizon"]


# =========================================================================== #
# 22–23. Actions are scored at the quantity they actually proposed.
# =========================================================================== #
def test_22_reduce_is_sized_by_its_proposed_quantity_not_the_whole_position():
    m = ro._metrics_for(action=ro.REC_REDUCE, inc_ret=-0.10, rep_ret=None,
                        weight=0.08, executed=True, policy=ro.default_policy(),
                        proposed_reduction=0.02)
    assert m["portfolio_impact_sizing_weight"] == 0.02
    assert m["portfolio_impact_sizing_basis"] == "PROPOSED_QUANTITY"


def test_23_missing_proposed_quantity_falls_back_and_says_so():
    m = ro._metrics_for(action=ro.REC_REPLACE, inc_ret=0.01, rep_ret=0.03,
                        weight=0.08, executed=True, policy=ro.default_policy())
    assert m["portfolio_impact_sizing_weight"] == 0.08
    assert m["portfolio_impact_sizing_basis"] == "FULL_POSITION_WEIGHT_FALLBACK"
    assert m["portfolio_impact"] == round(0.08 * 0.02, 6)


# =========================================================================== #
# 24. Safety: nothing here creates a target, an order or an approval.
# =========================================================================== #
def test_24_shortlist_entries_are_non_allocated_comparisons_only():
    for r in _run()["holding_reviews"]:
        for c in r["replacement_shortlist"]:
            assert c["label"] == k.NON_ALLOCATED_LABEL
            assert "allocation" not in c
            assert "quantity" not in c
            assert "order" not in c
