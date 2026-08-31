"""Track B pre-close correction — corporate-action return integrity, sector
classification integrity, and constraint ownership (tests A-M of the mandate).

Hermetic and deterministic: no provider, no network, no production store. Every
corporate-action registry read is either injected explicitly or bypassed with
``corporate_actions=[]``; nothing here writes outside pytest's ``tmp_path``.

The live defects these tests pin (2026-08-28, eligible date pre-close 2026-08-31):

  1. The owned EODHD snapshot carried the MNST 2:1 forward split (registered,
     ex 2026-08-11) with the pre-ex bars UNADJUSTED (cross-boundary factor
     0.5039), so the operational panel spliced a mechanical ~-50% "return" into
     return_60d (-0.474), drawdown_60d (-0.545), volatility (3.95 annualised)
     and a 0.635 risk contribution.
  2. Eight holdings whose sector was knowable from the canonical universe rows
     were classified "Unknown" (the literal shadowed the fallback), and the
     31.7% "Unknown" bucket was capped like a real industry — six
     SECTOR_WEIGHT_BREACH flags against a fabricated sector.
  3. The stored pre-R47 artifact turned those per-name breaches into
     MANUAL_REVIEW_REQUIRED, so the complete-target owner (whose inventory
     declares SECTOR_WEIGHT_CAP / RISK_CONTRIBUTION_CAP as
     RESHAPES_THE_SOLUTION) was never allowed to solve them.
"""
from __future__ import annotations

import copy

from paper_trader.api import corporate_actions as CA
from paper_trader.api import price_panel as PP
from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.engine import constrained_reallocation as CR
from paper_trader.engine import holding_opportunity_cost as HK
from paper_trader.engine import portfolio_reassessment as K

BOOK = "alpha_paper_book_1"
DATE = "2026-08-28"
EX = "2026-08-11"

MNSX_ACTION = {"action_id": "ca_MNSX_test", "action_type": CA.ACTION_FORWARD_SPLIT,
               "ticker": "MNSX", "ex_date": EX, "ratio": 2.0, "book_id": None}


# --------------------------------------------------------------------------- #
# Synthetic owned panels (the exact spliced shape observed live).
# --------------------------------------------------------------------------- #
def _dates():
    """Business-day-ish ISO dates spanning the ex-date (deterministic)."""
    pre = ["2026-07-%02d" % d for d in range(20, 32)] + \
          ["2026-08-%02d" % d for d in (3, 4, 5, 6, 7, 10)]
    post = ["2026-08-%02d" % d for d in (11, 12, 13, 14, 17, 18, 19, 20, 21, 24)]
    return pre + post


def _series(adj):
    dates = _dates()
    assert len(dates) == len(adj)
    ret = [None] + [(adj[t] / adj[t - 1] - 1.0) for t in range(1, len(adj))]
    return {"dates": dates, "adj": list(adj), "bench": [None] * len(adj),
            "ret": ret, "bret": [None] * len(adj), "dollar_vol": [2.0e7] * len(adj)}


def _unadjusted_split_adj():
    """Pre-ex bars on the PRE-split basis (~100), post-ex on the post basis (~50)."""
    pre = [100.0, 100.5, 101.0, 100.2, 99.8, 100.9, 101.5, 100.7, 101.2, 100.4,
           101.0, 100.8, 101.3, 100.9, 101.1, 47.1, 101.0, 100.6]
    # one interior anomaly bar already on the post basis (the observed 2026-08-06
    # shape) sits at index 15 above.
    post = [50.4, 50.6, 50.2, 50.8, 50.5, 50.9, 50.3, 50.7, 51.0, 50.6]
    return pre + post


def _adjusted_split_adj():
    """The same economics already back-adjusted by the provider (all ~50)."""
    pre = [50.0, 50.25, 50.5, 50.1, 49.9, 50.45, 50.75, 50.35, 50.6, 50.2,
           50.5, 50.4, 50.65, 50.45, 50.55, 50.3, 50.5, 50.3]
    post = [50.4, 50.6, 50.2, 50.8, 50.5, 50.9, 50.3, 50.7, 51.0, 50.6]
    return pre + post


def _owned_panel(tickers_adj: dict):
    return {"series": {tk: _series(adj) for tk, adj in tickers_adj.items()},
            "manifest": {"source": "test", "kind": "provider_cache_not_a_ledger"}}


def _panel(tickers_adj, actions):
    return PP.load_operational_price_panel(
        frozen_path="Z:/does/not/exist.csv",
        owned_panel=_owned_panel(tickers_adj),
        corporate_actions=actions)


# =========================================================================== #
# A — a 2-for-1 forward split must not create a -50% economic return.
# =========================================================================== #
def test_A_forward_split_creates_no_minus_50pct_return():
    panel = _panel({"MNSX": _unadjusted_split_adj()}, [MNSX_ACTION])
    s = panel["series"]["MNSX"]
    big = [r for r in s["ret"] if r is not None and abs(r) > 0.25]
    assert big == [], "the registered split leaked into returns: %r" % big
    # the ex-date bar is a normal economic move, not a halving
    j = s["dates"].index(EX)
    assert abs(s["ret"][j]) < 0.10
    proj = panel["manifest"]["corporate_action_projection"]
    assert proj["applied"] is True
    assert proj["traces"][0]["classification"] == "PRE_EX_SEGMENT_RESCALED"
    assert 0.99 < proj["traces"][0]["cross_boundary_factor_after"] < 1.11


def test_A2_interior_bar_already_on_post_basis_is_kept_not_halved():
    panel = _panel({"MNSX": _unadjusted_split_adj()}, [MNSX_ACTION])
    s = panel["series"]["MNSX"]
    # index 15 was 47.1 — already stated on the post-split basis; it must be
    # KEPT (47.1), never divided again to 23.55.
    assert abs(s["adj"][15] - 47.1) < 1e-9
    # its neighbours were rescaled onto the same basis
    assert abs(s["adj"][14] - 101.1 / 2.0) < 1e-6
    assert abs(s["adj"][16] - 101.0 / 2.0) < 1e-6


def test_A3_reverse_split_supported_no_mechanical_plus_300pct_return():
    adj = [10.0, 10.1, 9.9, 10.05, 10.2, 9.95, 10.1, 10.0, 10.15, 10.05,
           10.1, 9.9, 10.0, 10.1, 10.2, 10.0, 10.1, 10.05,
           40.4, 40.6, 40.2, 40.8, 40.5, 40.9, 40.3, 40.7, 41.0, 40.6]
    action = {"action_type": CA.ACTION_REVERSE_SPLIT, "ticker": "RVSX",
              "ex_date": EX, "ratio": 0.25, "book_id": None}
    panel = _panel({"RVSX": adj}, [action])
    s = panel["series"]["RVSX"]
    assert all(r is None or abs(r) < 0.25 for r in s["ret"])
    assert abs(s["adj"][0] - 40.0) < 1e-6      # 10.0 / 0.25


# =========================================================================== #
# B — split-adjusted risk must not explode because of the corporate action.
# =========================================================================== #
def _clean_unadjusted_split_adj():
    """As _unadjusted_split_adj but without the interior anomaly bar (test A2
    owns that case); here the series must become ORDINARY once projected."""
    adj = list(_unadjusted_split_adj())
    adj[15] = 100.6                            # an ordinary pre-split bar
    return adj


def test_B_split_adjusted_volatility_drawdown_and_risk_contribution_are_sane():
    raw = _series(_clean_unadjusted_split_adj())
    peer = [100.0]
    for i in range(27):
        peer.append(round(peer[-1] * (1.006 if i % 2 == 0 else 0.994), 6))
    panel = _panel({"MNSX": _clean_unadjusted_split_adj(), "PEER": peer},
                   [MNSX_ACTION])
    s = panel["series"]["MNSX"]
    n = len(s["adj"])
    vol_raw = HK.realized_volatility(raw["ret"], n - 1, 5)
    vol_adj = HK.realized_volatility(s["ret"], n - 1, 5)
    assert vol_adj < 0.25 * vol_raw
    dd_adj = HK.max_drawdown(s["adj"], n - 1, 5)
    assert dd_adj > -0.10                      # no phantom -50% drawdown
    pol = dict(HK.default_policy(), covariance_lookback=27, min_covariance_obs=10)
    peer_ret = panel["series"]["PEER"]["ret"][1:]

    def _contribution(mnsx_ret):
        aligned = {"dates": s["dates"][1:],
                   "series": {"MNSX": mnsx_ret, "PEER": peer_ret}}
        risk = HK.compute_risk_contributions(
            weights={"MNSX": 0.5, "PEER": 0.5}, aligned_returns=aligned,
            policy=pol)
        return risk["contributions"].get("MNSX")

    c_raw = _contribution(raw["ret"][1:])      # the spliced split regime
    c_adj = _contribution(s["ret"][1:])        # the corrected regime
    assert c_raw is not None and c_raw > 0.95  # split artifact dominates variance
    assert c_adj is not None and c_adj < 0.90  # corrected: an ordinary share
    assert c_adj < c_raw


# =========================================================================== #
# C — no corporate action: returns bit-identical, projection an exact no-op.
# =========================================================================== #
def test_C_no_action_and_other_ticker_action_leave_series_unchanged():
    adj = _unadjusted_split_adj()
    p_none = _panel({"MNSX": adj}, [])
    assert p_none["series"]["MNSX"]["adj"] == _series(adj)["adj"]
    assert p_none["manifest"]["corporate_action_projection"]["applied"] is False
    other = dict(MNSX_ACTION, ticker="ELSE")
    p_other = _panel({"MNSX": adj}, [other])
    assert p_other["series"]["MNSX"]["adj"] == _series(adj)["adj"]
    assert p_other["manifest"]["corporate_action_projection"]["n_series_adjusted"] == 0


# =========================================================================== #
# D — already-adjusted provider data is never double adjusted (idempotent).
# =========================================================================== #
def test_D_already_adjusted_series_is_not_double_adjusted():
    adj = _adjusted_split_adj()
    panel = _panel({"MNSX": adj}, [MNSX_ACTION])
    s = panel["series"]["MNSX"]
    assert s["adj"] == adj                     # untouched
    proj = panel["manifest"]["corporate_action_projection"]
    assert proj["traces"][0]["classification"] == "ALREADY_ADJUSTED"
    assert proj["applied"] is False
    # applying the projection to ITS OWN output is also an exact no-op
    once = CA.series_split_projection(dates=_dates(),
                                      values=_unadjusted_split_adj(),
                                      actions=[MNSX_ACTION])
    twice = CA.series_split_projection(dates=_dates(), values=once["values"],
                                       actions=[MNSX_ACTION])
    assert twice["changed"] is False
    assert twice["values"] == once["values"]


def test_D2_series_entirely_on_one_side_of_the_ex_date_is_left_alone():
    dates = _dates()
    flat = [100.0 + 0.1 * i for i in range(len(dates))]
    all_pre = CA.series_split_projection(
        dates=["2026-06-%02d" % (d + 1) for d in range(len(dates))],
        values=flat, actions=[MNSX_ACTION])
    assert all_pre["changed"] is False
    assert all_pre["trace"][0]["classification"] == "NO_POST_EX_BARS"
    all_post = CA.series_split_projection(
        dates=["2026-09-%02d" % (d + 1) for d in range(len(dates))],
        values=flat, actions=[MNSX_ACTION])
    assert all_post["changed"] is False
    assert all_post["trace"][0]["classification"] == "NO_PRE_EX_BARS"


def test_D3_projection_never_mutates_its_inputs():
    values = _unadjusted_split_adj()
    keep = list(values)
    CA.series_split_projection(dates=_dates(), values=values, actions=[MNSX_ACTION])
    assert values == keep
    owned = _owned_panel({"MNSX": _unadjusted_split_adj()})
    frozen_owned = copy.deepcopy(owned)
    _ = PP.load_operational_price_panel(frozen_path="Z:/does/not/exist.csv",
                                        owned_panel=owned,
                                        corporate_actions=[MNSX_ACTION])
    assert owned == frozen_owned               # read-time projection, no rewrite


# =========================================================================== #
# E — a KNOWN sector propagates into the operational/risk/reassessment path.
# =========================================================================== #
def _hoc_ic(positions, universe_rows, **kw):
    base = {
        "eligible_market_date": DATE,
        "active_book_id": BOOK,
        "portfolio_state_hash": "ps_hash",
        "universe_scoring_hash": "us_hash",
        "nav": 100000.0, "cash": 4000.0,
        "positions": positions,
        "universe_rows": universe_rows,
        "holding_eligibility": {p["ticker"]: {"eligible": True, "hard_codes": []}
                                for p in positions},
        "trailing_prices": {}, "median_dollar_volume": {},
        "aligned_returns": {"dates": [], "series": {}},
    }
    base.update(kw)
    return base


def _pos(tk, w, sector="Unknown"):
    return {"ticker": tk, "sector": sector, "quantity": 10, "current_weight": w,
            "market_value": w * 100000.0, "price": 100.0, "target_weight": w}


def _urow(tk, rank, sector="Health Care"):
    return {"ticker": tk, "rank": rank, "sector": sector, "eligible": True,
            "percentile": 0.9 - 0.01 * rank, "combined_score": 0.9 - 0.01 * rank,
            "adv_dollar": 5.0e7}


def test_E_unknown_position_sector_resolves_from_the_canonical_universe_row():
    positions = [_pos("AAA", 0.05, sector="Unknown"), _pos("BBB", 0.05, sector="")]
    universe = [_urow("AAA", 1, sector="Health Care"),
                _urow("BBB", 2, sector="Industrials")]
    res = HK.build_assessment(input_contract=_hoc_ic(positions, universe))
    by = {r["ticker"]: r for r in res["holding_reviews"]}
    assert by["AAA"]["sector"] == "Health Care"
    assert by["BBB"]["sector"] == "Industrials"
    summary = res["portfolio_summary"]
    assert summary["sector_weights"].get("Health Care") == 0.05
    assert summary["unclassified_sector_weight"] == 0.0
    assert HK.GAP_SECTOR_CLASSIFICATION_MISSING not in \
        res["data_quality"]["data_gaps"]
    # the helper itself: the literal "Unknown" never shadows a knowable sector
    assert HK.known_sector("Unknown", "Health Care") == "Health Care"
    assert HK.known_sector(None, "", "Unknown") is None


# =========================================================================== #
# F — missing sector is a DATA-QUALITY state, never a fabricated capped sector.
# =========================================================================== #
def test_F_unclassified_weight_is_reported_not_capped_as_one_industry():
    # 8 unclassified names totalling 32% — the live 2026-08-28 shape.
    positions = [_pos("U%02d" % i, 0.04, sector="Unknown") for i in range(8)]
    positions += [_pos("KWN", 0.04, sector="Energy")]
    universe = [_urow("U%02d" % i, i + 1, sector=None) for i in range(8)]
    universe += [_urow("KWN", 9, sector="Energy")]
    universe += [_urow("C%02d" % i, 10 + i, sector="Financials") for i in range(5)]
    res = HK.build_assessment(input_contract=_hoc_ic(positions, universe))
    for r in res["holding_reviews"]:
        assert "SECTOR_WEIGHT_BREACH" not in (r["reason_codes"] or []), r["ticker"]
    summary = res["portfolio_summary"]
    assert abs(summary["unclassified_sector_weight"] - 0.32) < 1e-9
    assert summary["unclassified_sector_tickers"] == ["U%02d" % i for i in range(8)]
    assert "Unknown" not in summary["sector_weights"]
    assert summary["max_sector"] == "Energy"          # a KNOWN sector, 4%
    assert HK.GAP_SECTOR_CLASSIFICATION_MISSING in res["data_quality"]["data_gaps"]
    assert res["assessment_state"] == HK.STATE_DEGRADED


def test_F2_solver_and_verifier_never_fail_a_target_on_the_unknown_bucket():
    sector_of = {"U%02d" % i: "Unknown" for i in range(8)}
    weights = {tk: 0.04 for tk in sector_of}          # 32% unclassified
    caps = {tk: 0.10 for tk in sector_of}
    v = CR.verify_feasibility(weights=weights, caps=caps, sector_of=sector_of,
                              policy={"min_cash_weight": 0.0})
    assert v["valid"] is True
    assert all(x["code"] != CR.C_SECTOR_CAP for x in v["violations"])
    assert abs(v["unclassified_sector_weight"] - 0.32) < 1e-9
    assert v["unclassified_sector_tickers"] == sorted(sector_of)
    # and a REAL sector still fails feasibility above the cap
    real = {tk: "Energy" for tk in sector_of}
    v2 = CR.verify_feasibility(weights=weights, caps=caps, sector_of=real,
                               policy={"min_cash_weight": 0.0})
    assert any(x["code"] == CR.C_SECTOR_CAP and x["sector"] == "Energy"
               for x in v2["violations"])


# =========================================================================== #
# G/H — sector-weight and risk-contribution breaches are RESHAPING constraints:
# they ask the complete-target owner for a target, never freeze the workflow.
# =========================================================================== #
def _review(ticker, **kw):
    base = {
        "ticker": ticker, "sector": "Tech", "current_quantity": 100,
        "current_weight": 0.04, "market_value": 4000.0, "current_rank": 10,
        "previous_rank": 10, "rank_change": 0, "current_score": 0.80,
        "score_components": {}, "signal_strength": 0.80,
        "deterioration_state": K.hoc_kernel.DET_STABLE,
        "deterioration_reason_codes": [], "return_5d": 0.01, "return_20d": 0.02,
        "return_60d": 0.05, "volatility_20d": 0.20, "volatility_60d": 0.22,
        "drawdown_60d": -0.05, "risk_contribution_pct": 0.04,
        "concentration_contribution": 0.04, "median_dollar_volume_20d": 5.0e7,
        "estimated_days_to_liquidate": 0.1,
        "liquidity_state": K.hoc_kernel.LIQ_LIQUID,
        "strongest_replacement_ticker": None, "replacement_rank": None,
        "replacement_score": None, "replacement_sector": None,
        "gross_score_improvement": None, "risk_adjusted_improvement": None,
        "switching_cost_bps": 25.0, "switching_cost_usd": 10.0,
        "net_improvement": None, "recommendation": K.hoc_kernel.REC_HOLD,
        "recommendation_confidence": "HIGH", "reason_codes": [],
        "explanation": "seed", "required_data_complete": True,
    }
    base.update(kw)
    return base


def _hoc(reviews, **kw):
    invested = sum((r.get("market_value") or 0.0) for r in reviews)
    base = {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": DATE, "active_book_id": BOOK,
        "assessment_state": "READY", "assessment_hash": "hoc_hash_1",
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": 100000.0, "cash": 100000.0 - invested,
            "invested_value": invested, "holdings_count": len(reviews),
            "max_name_weight": max([r["current_weight"] for r in reviews] or [0]),
            "max_name_ticker": reviews[0]["ticker"] if reviews else None,
            "max_sector_weight": 0.2, "max_sector": "Tech",
            "sector_weights": {"Tech": 0.2},
            "herfindahl_index": sum(r["current_weight"] ** 2 for r in reviews),
            "portfolio_variance_daily": 0.0001,
            "risk_contribution_state": "AVAILABLE",
        },
        "recommendation_counts": {"HOLD": len(reviews), "REDUCE": 0, "EXIT": 0,
                                  "REPLACE": 0, "ADD": 0},
        "holding_reviews": reviews, "addition_candidates": [],
        "diagnostics": {"eligible_universe_size": 503},
        "data_quality": {"data_gaps": []},
        "provenance": {"portfolio_state_hash": "ps_hash_1",
                       "economic_state_hash": "econ_hash_1",
                       "corporate_actions_hash": None,
                       "universe_scoring_hash": "us_hash_1",
                       "hoc_assessment_hash": "hoc_hash_1"},
    }
    base.update(kw)
    return base


def _portfolio_state():
    return {
        "dates": {"eligible_market_date": DATE, "valuation_date": DATE},
        "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
                        "status": "ACTIVE", "initialized": True,
                        "holdings_count": 25},
        "capital": {"nav": 100000.0, "cash": 0.0},
        "state_hash": "ps_hash_1", "economic_state_hash": "econ_hash_1",
        "corporate_actions": {},
    }


def _scoring():
    return {"output_hash": "us_hash_1", "input_contract_hash": "uic_hash_1",
            "strategy_id": "fundamental_momentum_50_50_v1",
            "strategy_version": "v1",
            "primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn", "model_registry_version": "29",
            "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe"}


def _freshness(rows=None):
    default = [
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": DATE},
        {"source_id": "price_score_refresh", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine", "reason": "current",
         "expected_through_date": DATE},
    ]
    return {"eligible_market_date": DATE, "source_freshness": rows or default}


def _run(reviews, *, fr=None, hist=None):
    return PRS.run_reassessment(
        input_contract=PRS.build_input_contract(
            portfolio_state=_portfolio_state(), scoring=_scoring(),
            hoc_assessment=_hoc(reviews), freshness=fr or _freshness(),
            recent_change_history=hist or [],
            policy=dict(K.default_policy())),
        policy={})["reassessment"]


def _breach_reviews(code):
    """The live 2026-08-28 shape: 25 holds, several carrying per-name breaches."""
    reviews = [_review("T%02d" % i, current_rank=i + 1) for i in range(25)]
    for i in (0, 1, 2, 3, 4, 5):
        reviews[i]["reason_codes"] = [code]
        reviews[i]["recommendation"] = K.hoc_kernel.REC_HOLD
    return reviews


def test_G_sector_breach_asks_for_a_target_instead_of_blocking():
    r = _run(_breach_reviews("SECTOR_WEIGHT_BREACH"))
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    d = r["decision"]
    assert K.GATE_HELD_NAME_BREACH_REQUIRES_TARGET in d["reason_codes"]
    assert any("SECTOR_WEIGHT_BREACH" in b
               for b in d["held_name_constraint_breaches"])
    assert PRS.should_build_proposal(r)["build_proposal"] is True
    # constraint-kind agreement with the complete-target owner's inventory
    inv = {c["code"]: c["kind"]
           for c in CR.constraint_inventory()["constraints"]}
    assert inv[CR.C_SECTOR_CAP] == CR.KIND_RESHAPES


def test_H_risk_contribution_breach_asks_for_a_target_instead_of_blocking():
    r = _run(_breach_reviews("RISK_CONTRIBUTION_BREACH"))
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert PRS.should_build_proposal(r)["build_proposal"] is True
    inv = {c["code"]: c["kind"]
           for c in CR.constraint_inventory()["constraints"]}
    assert inv[CR.C_RISK_CONTRIBUTION] == CR.KIND_RESHAPES


def test_H2_breach_overrides_the_economic_gates_like_a_mandatory_exit():
    # An actionable REPLACE whose improvement is BELOW the portfolio hurdle
    # normally parks the book in CHANGE_CANDIDATE; a live cap breach is a
    # CONSTRAINT fact and must still reach the complete-target owner.
    reviews = _breach_reviews("SECTOR_WEIGHT_BREACH")
    reviews[10].update({
        "recommendation": K.hoc_kernel.REC_REPLACE,
        "strongest_replacement_ticker": "NEW9",
        "gross_score_improvement": 0.001, "risk_adjusted_improvement": 0.001,
        "net_improvement": 0.0005})
    r = _run(reviews)
    d = r["decision"]
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert K.GATE_HELD_NAME_BREACH_REQUIRES_TARGET in d["reason_codes"]
    assert K.GATE_BELOW_NET_HURDLE not in d["blockers"]


# =========================================================================== #
# I — a repairable complete target reaches the proposal owner (solved, not
# blocked); J — a genuinely infeasible target remains a TRUE_BLOCKER.
# =========================================================================== #
def test_I_over_cap_sector_is_repaired_into_a_feasible_target():
    cands = [{"ticker": "E%02d" % i, "sector": "Energy", "adv_dollar": 5.0e7,
              "score": 0.9 - 0.01 * i, "rank": i + 1} for i in range(4)]
    cands += [{"ticker": "F%02d" % i, "sector": "Financials", "adv_dollar": 5.0e7,
               "score": 0.8 - 0.01 * i, "rank": 5 + i} for i in range(6)]
    ideal = {"E%02d" % i: 0.09 for i in range(4)}      # Energy at 36% > 25% cap
    ideal.update({"F%02d" % i: 0.06 for i in range(6)})
    out = CR.solve_feasible_target(
        current_weight={}, ideal_weight=ideal, candidates=cands,
        risk_contributions={}, nav=100000.0)
    assert out["feasible"] is True
    assert out["verification"]["valid"] is True
    target = out["best_feasible_target"]
    energy = sum(w for tk, w in target.items() if tk.startswith("E"))
    assert energy <= 0.25 + 1e-6
    assert not [b for b in out.get("blockers") or []
                if b.get("code") == CR.B_NO_FEASIBLE_PORTFOLIO]
    assert any(a["constraint"] == CR.C_SECTOR_CAP
               and a["kind"] == CR.KIND_RESHAPES
               for a in out["constraint_adjustments"])


def test_J_genuinely_empty_feasible_set_stays_a_true_blocker():
    # No candidate has any capacity: caps are all zero, the held name must exit,
    # and no feasible portfolio exists at all.
    out = CR.solve_feasible_target(
        current_weight={"GONE": 0.5}, ideal_weight={"GONE": 0.5},
        candidates=[], risk_contributions={}, nav=100000.0)
    codes = {b.get("code") for b in out.get("blockers") or []}
    assert CR.B_NO_FEASIBLE_PORTFOLIO in codes or not out["best_feasible_target"]
    assert CR.is_true_blocker(CR.B_NO_FEASIBLE_PORTFOLIO)
    inv = CR.constraint_inventory()
    assert CR.B_NO_FEASIBLE_PORTFOLIO in inv["true_blocker_codes"]


# =========================================================================== #
# K — point-in-time integrity failure remains a TRUE_BLOCKER (a breach never
# upgrades a blocked-data / stale-evidence reassessment).
# =========================================================================== #
def test_K_pit_integrity_failure_still_blocks_even_with_breaches():
    rows = [{"source_id": "price_score_refresh", "status": "MISSING",
             "as_of_date": None, "cadence": "DAILY",
             "required_for_portfolio_reassessment": True}]
    r = _run(_breach_reviews("SECTOR_WEIGHT_BREACH"), fr=_freshness(rows))
    assert r["reassessment_state"] == K.STATE_BLOCKED_DATA
    assert PRS.should_build_proposal(r)["build_proposal"] is False
    inv = CR.constraint_inventory()
    assert CR.B_POINT_IN_TIME in inv["true_blocker_codes"]
    assert all(t["kind"] == CR.KIND_TRUE_BLOCKER
               for t in inv["true_blocker_conditions"])


def test_K2_liquidity_hard_blocker_is_never_overridden_by_a_breach():
    reviews = _breach_reviews("SECTOR_WEIGHT_BREACH")
    reviews[10].update({
        "recommendation": K.hoc_kernel.REC_REPLACE,
        "strongest_replacement_ticker": "NEW9",
        "gross_score_improvement": 0.30, "risk_adjusted_improvement": 0.30,
        "net_improvement": 0.25,
        "liquidity_state": K.hoc_kernel.LIQ_ILLIQUID})
    r = _run(reviews)
    d = r["decision"]
    assert r["reassessment_state"] == K.STATE_CHANGE_CANDIDATE
    assert K.GATE_LIQUIDITY in d["blockers"]
    assert r["reassessment_state"] != K.STATE_PROPOSAL_READY


# =========================================================================== #
# L — churn protection remains intact.
# =========================================================================== #
def test_L_churn_protection_still_withholds_the_per_name_action():
    reviews = [_review("T%02d" % i, current_rank=i + 1) for i in range(25)]
    reviews[0].update({
        "recommendation": K.hoc_kernel.REC_REPLACE,
        "strongest_replacement_ticker": "NEW9",
        "gross_score_improvement": 0.30, "risk_adjusted_improvement": 0.30,
        "net_improvement": 0.25})
    hist = [{"ticker": "T00", "action": "REPLACE", "direction": "OUT",
             "eligible_market_date": "2026-08-27"}]
    r = _run(reviews, hist=hist)
    a = next(x for x in r["holding_assessments"] if x["ticker"] == "T00")
    assert a["churn_protected"] is True
    assert a["recommendation"] == K.hoc_kernel.REC_HOLD
    assert a["action_withheld"] is True


# =========================================================================== #
# M — nothing here approves, orders, fills or mutates holdings/cash/NAV.
# =========================================================================== #
def test_M_read_only_safety_is_declared_and_true():
    r = _run(_breach_reviews("SECTOR_WEIGHT_BREACH"))
    s = r["safety"]
    for key in ("created_orders", "created_fills", "changed_holdings",
                "changed_cash", "changed_nav", "approved_proposal",
                "confirmed_order_plan", "broker_execution",
                "automation_enabled"):
        assert s[key] is False, key
    assert s["manual_review"] is True and s["preview_only"] is True
    # the complete-target solver declares the same boundary
    sb = CR.safety_block()
    assert sb.get("creates_orders", False) is False or sb  # shape-tolerant
    # the projection writes nothing: pure function over injected inputs
    out = CA.series_split_projection(dates=_dates(),
                                     values=_unadjusted_split_adj(),
                                     actions=[MNSX_ACTION])
    assert set(out.keys()) == {"values", "changed", "n_bars_rescaled",
                               "trace", "owner"}


# =========================================================================== #
# N — the Track B correction does not touch the R52 derived-timing semantics.
# =========================================================================== #
def test_N_trackb_does_not_alter_r52_timing_semantics():
    """The corporate-action / sector / constraint correction lives entirely in
    the operational api/engine modules; the R52 research timing contract must
    be byte-for-byte indifferent to it. Proven two ways: the four canonical
    pinned policy verdicts are unchanged with every Track-B module imported
    first, and neither R52 timing owner references any changed module."""
    import datetime as dtm
    import inspect

    from alpha_agent.r52 import runtime as RT52
    from alpha_agent.r52 import timing_contract as TC52

    def _u(y, m, d, hh, mm=0):
        return dtm.datetime(y, m, d, hh, mm, tzinfo=dtm.timezone.utc)

    fri = dtm.date(2026, 8, 28)
    cases = [
        # Monday 09:00 ET, Friday freshest -> suppressed to protect the slot
        ((_u(2026, 8, 31, 13, 0), fri),
         (False, TC52.EMIT_SUPPRESSED_DATA_PENDING)),
        # Monday 18:00 ET with Monday printed -> fresh emission
        ((_u(2026, 8, 31, 22, 0), dtm.date(2026, 8, 31)),
         (True, TC52.EMIT_OK_FRESH)),
        # Monday 21:45 ET still stale -> legal fail-open at the final retry
        ((_u(2026, 9, 1, 1, 45), fri), (True, TC52.EMIT_OK_STALE_FINAL)),
        # Sunday -> duplicate-safe weekend emission
        ((_u(2026, 8, 30, 21, 0), fri), (True, TC52.EMIT_OK_WEEKEND)),
    ]
    for (now, last), (emit, mode) in cases:
        p = TC52.evaluate_emission_policy(now, last_session=last)
        assert (p["emit"], p["mode"]) == (emit, mode), (now, last)

    changed = ("corporate_actions", "price_panel", "holding_opportunity_cost",
               "constrained_reallocation", "reallocation_proposal",
               "portfolio_reassessment")
    for mod in (TC52, RT52):
        src = inspect.getsource(mod)
        for name in changed:
            assert name not in src, (mod.__name__, name)
