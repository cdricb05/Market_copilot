"""Slice 7 (Phase 29H) — Reallocation Proposal Engine tests (Milestone 3).

Deterministic and hermetic: the pure kernel is driven by explicit input-contract
dicts; the api composition/persistence/read owner is driven by injected
portfolio-state / scoring / HOC-assessment / price-panel fixtures and temporary
artifact roots; the endpoint is exercised through a monkeypatched loader; the DRC
integration reuses the Slice-3 harness with injected engine seams. No DB / provider /
prediction / operational ledger / real cycle is touched.

Covers the required matrix:
  A. pure engine (1-20)     B. api / persistence (21-40)
  C. DRC (41-48)            D. workflow / UI (49-64)
  E. architecture (65-71)   + bounded performance.
"""
from __future__ import annotations

import copy
import importlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from paper_trader.engine import reallocation_proposal as R
from paper_trader.api import reallocation_proposal as A
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import workflow_state as ws
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"
KERNEL_SRC = (ROOT / "engine" / "reallocation_proposal.py").read_text(encoding="utf-8")
OWNER_SRC = (ROOT / "api" / "reallocation_proposal.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic builders (no RNG)
# --------------------------------------------------------------------------- #
def _rets(n, seed=1):
    return [(((i * 7 + seed * 13) % 21) - 10) / 1000.0 for i in range(n)]


def _aligned(tickers, n=80):
    dates = ["d%03d" % i for i in range(n)]
    series = {tk: _rets(n, seed=i + 1) for i, tk in enumerate(tickers)}
    return {"dates": dates, "series": series}


def _pos(ticker, sector, w, mv):
    return {"ticker": ticker, "sector": sector, "current_weight": w, "market_value": mv,
            "quantity": 100, "price": mv / 100.0}


def _urow(ticker, rank, pct, sector="Tech", adv=5e7, eligible=True):
    return {"ticker": ticker, "rank": rank, "percentile": pct, "combined_score": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _review(ticker, rec, rank, pct, sector="Tech", repl=None, dd=-0.1, liq="LIQUID"):
    return {"ticker": ticker, "recommendation": rec, "current_rank": rank,
            "current_score": pct, "signal_strength": pct,
            "strongest_replacement_ticker": repl, "drawdown_60d": dd,
            "liquidity_state": liq, "switching_cost_usd": 10.0, "net_improvement": 0.5}


# Canonical 4-holding scenario: AAA HOLD, BBB REDUCE, CCC EXIT, DDD REPLACE→EEE.
def _kic(*, n_target=4, name_cap=0.5, sector_cap=1.0, with_returns=True, **over):
    holdings = ["AAA", "BBB", "CCC", "DDD"]
    cands = ["EEE", "FFF", "GGG"]
    ic = {
        "schema_version": R.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-06",
        "active_book_id": "alpha_paper_book_1",
        "active_book_label": "Alpha Paper Book #1",
        "valuation_date": "2026-08-06",
        "nav": 100000.0, "cash": 0.0,
        "portfolio_state_hash": "PSH", "universe_scoring_hash": "USH",
        "universe_input_contract_hash": "UIC",
        "hoc_assessment_hash": "HOC1", "hoc_assessment_state": "READY",
        "hoc_available": True, "hoc_data_gaps": [],
        "hoc_recommendation_counts": {"HOLD": 1, "REDUCE": 1, "EXIT": 1, "REPLACE": 1, "ADD": 0},
        "positions": [
            _pos("AAA", "Tech", 0.25, 25000.0), _pos("BBB", "Tech", 0.25, 25000.0),
            _pos("CCC", "Fin", 0.25, 25000.0), _pos("DDD", "Fin", 0.25, 25000.0)],
        "hoc_reviews": [
            _review("AAA", "HOLD", 5, 0.90), _review("BBB", "REDUCE", 12, 0.60),
            _review("CCC", "EXIT", 80, 0.10, sector="Fin"),
            _review("DDD", "REPLACE", 40, 0.30, sector="Fin", repl="EEE")],
        "universe_rows": [
            _urow("AAA", 5, 0.90), _urow("BBB", 12, 0.60), _urow("DDD", 40, 0.30, sector="Fin"),
            _urow("EEE", 2, 0.95, sector="Health"), _urow("FFF", 3, 0.93, sector="Energy"),
            _urow("GGG", 4, 0.92, sector="Energy")],
        "aligned_returns": _aligned(holdings + cands) if with_returns else {"dates": [], "series": {}},
    }
    ic.update(over)
    return ic


def _pol(**over):
    p = dict(R.default_policy())
    # 4-name toy book: every portfolio limit is scaled to it (see _api_pol). The
    # Release 29.3 complete-target turnover budget is exercised at its real boundary in
    # tests/test_release29_3_decision_integrity.py.
    p.update({"target_position_count": 4, "candidate_rank_max": 50, "max_name_weight": 0.5,
              "sector_cap_fraction": 1.0, "min_covariance_obs": 20, "covariance_lookback": 60,
              "max_one_way_turnover": 1.0,
              "min_volatility_coverage": 0.5})
    p.update(over)
    return p


def _build(**over):
    pol = over.pop("policy", None) or _pol()
    return R.build_proposal(input_contract=_kic(**over), policy=pol)


def _alloc(res, ticker):
    return next((a for a in res["allocations"] if a["ticker"] == ticker), None)


# =========================================================================== #
# A. PURE ENGINE (1-20)
# =========================================================================== #
def test_01_identical_inputs_deterministic():
    a = _build()
    b = _build()
    assert a["proposal_hash"] == b["proposal_hash"]
    assert a["allocations"] == b["allocations"]


def test_02_weights_reconcile():
    res = _build()
    total = sum(a["proposed_weight"] for a in res["allocations"])
    cash_w = res["portfolio"]["proposed_cash_weight"]
    assert abs(total + cash_w - 1.0) < 1e-6
    # no capital creation: invested + cash == NAV
    nav = res["portfolio"]["nav"]
    invested = res["portfolio"]["proposed_invested_value"]
    assert abs(invested + res["portfolio"]["proposed_cash"] - nav) < 0.05


def test_03_no_duplicate_ticker():
    res = _build()
    tks = [a["ticker"] for a in res["allocations"]]
    assert len(tks) == len(set(tks))
    assert res["constraints"]["no_duplicate_ok"] is True


def test_04_exit_yields_zero_weight():
    res = _build()
    ccc = _alloc(res, "CCC")
    assert ccc["action"] == "EXIT" and ccc["proposed_weight"] == 0.0


def test_05_reduce_lowers_weight():
    res = _build()
    bbb = _alloc(res, "BBB")
    assert bbb["action"] == "REDUCE"
    assert bbb["proposed_weight"] < bbb["current_weight"]


def test_06_hold_retained_when_feasible():
    res = _build()
    aaa = _alloc(res, "AAA")
    assert aaa["action"] in ("RETAIN", "INCREASE")
    assert aaa["proposed_weight"] > 0
    assert aaa["source_hoc_recommendation"] == "HOLD"


def test_07_replace_mapping_traceable():
    res = _build()
    ddd = _alloc(res, "DDD")
    eee = _alloc(res, "EEE")
    assert ddd["action"] == "REPLACE_OUT" and ddd["proposed_weight"] == 0.0
    assert ddd["replacement_relationship"]["counterparty"] == "EEE"
    assert eee["action"] == "REPLACE_IN"
    assert eee["replacement_relationship"]["counterparty"] == "DDD"


def test_08_add_competition_best_rank_wins():
    res = _build()
    # 4 slots: AAA(hold), BBB(reduce), EEE(replace_in) -> 1 ADD slot -> FFF(rank3) beats GGG(rank4).
    fff = _alloc(res, "FFF")
    ggg = _alloc(res, "GGG")
    assert fff is not None and fff["action"] == "ADD"
    assert ggg is None or ggg["proposed_weight"] == 0.0


def test_09_no_ineligible_candidate():
    ic = _kic()
    for r in ic["universe_rows"]:
        if r["ticker"] == "FFF":
            r["eligible"] = False
    res = R.build_proposal(input_contract=ic, policy=_pol())
    fff = _alloc(res, "FFF")
    assert fff is None or fff["proposed_weight"] == 0.0


def test_10_position_limit_respected():
    res = _build()
    cap = res["policy"]["max_name_weight"]
    assert all(a["proposed_weight"] <= cap + 1e-9 for a in res["allocations"])
    assert res["constraints"]["name_cap_ok"] is True
    proposed = sum(1 for a in res["allocations"] if a["proposed_weight"] > 0)
    assert proposed <= res["policy"]["target_position_count"]
    assert res["constraints"]["position_count_ok"] is True


def test_11_sector_limit_respected():
    # Many same-sector candidates; the count-cap bounds the sector weight.
    rows = [_urow("H%02d" % i, 100 + i, 0.5, sector="Held") for i in range(1)]
    urows = [_urow("K%02d" % i, i + 1, 0.9 - i * 0.001, sector="Energy") for i in range(20)]
    positions = [_pos("P0", "Held", 0.04, 4000.0)]
    ic = _kic(positions=positions,
              hoc_reviews=[_review("P0", "HOLD", 200, 0.2, sector="Held")],
              universe_rows=rows + urows,
              aligned_returns={"dates": [], "series": {}})
    pol = _pol(target_position_count=25, max_name_weight=0.10, sector_cap_fraction=0.25)
    res = R.build_proposal(input_contract=ic, policy=pol)
    assert res["risk"]["sector_concentration_after"] <= 0.25 + 1e-6
    assert res["constraints"]["sector_cap_ok"] is True


def test_12_liquidity_policy_excludes_illiquid():
    ic = _kic()
    for r in ic["universe_rows"]:
        if r["ticker"] == "FFF":
            r["adv_dollar"] = 1.0e5  # below the 1e7 floor
    pol = _pol(min_adv_dollar=1.0e7)
    res = R.build_proposal(input_contract=ic, policy=pol)
    fff = _alloc(res, "FFF")
    assert fff is None or fff["proposed_weight"] == 0.0


def test_13_switching_cost_hurdle_defers_replace():
    # DDD (0.30); ALL non-held candidates only marginally better (gross 0.01 < min_gross
    # 0.02) -> no candidate clears the net-of-cost hurdle -> DDD retained, not swapped.
    ic = _kic()
    for r in ic["universe_rows"]:
        if r["ticker"] in ("EEE", "FFF", "GGG"):
            r["percentile"] = 0.31
    res = R.build_proposal(input_contract=ic, policy=_pol())
    ddd = _alloc(res, "DDD")
    assert ddd["action"] in ("RETAIN", "INCREASE", "REDUCE")
    assert "REPLACE_DEFERRED_NO_FEASIBLE_CANDIDATE" in ddd["reason_codes"]


def test_14_turnover_calculation():
    res = _build()
    t = res["turnover"]
    # two-way = sum |delta|; one-way = half.
    two_way = sum(abs(a["delta_weight"]) for a in res["allocations"])
    assert abs(t["two_way_turnover"] - round(two_way, 6)) < 1e-6
    assert abs(t["one_way_turnover"] - round(two_way / 2.0, 6)) < 1e-6


def test_15_transaction_cost_calculation():
    res = _build()
    t = res["turnover"]
    traded = t["gross_buys"] + t["gross_sells"]
    assert abs(t["traded_notional"] - round(traded, 2)) < 0.02
    expected = traded * desk.COST_RATE_PER_SIDE
    assert abs(t["estimated_transaction_cost"] - round(expected, 2)) < 0.05
    assert t["cost_rate_per_side"] == desk.COST_RATE_PER_SIDE


def test_16_score_before_after():
    res = _build()
    s = res["signal"]
    assert s["score_before"] is not None and s["score_after"] is not None
    assert s["score_improvement"] == round(s["score_after"] - s["score_before"], 6)
    # exiting the two weakest names and adding strong ones must improve the score.
    assert s["score_after"] > s["score_before"]


def test_17_expected_return_null_when_unavailable():
    res = _build()
    s = res["signal"]
    assert s["expected_return_before"] is None
    assert s["expected_return_after"] is None
    assert s["expected_return_improvement"] is None
    assert s["expected_return_state"] == "NOT_CALIBRATED"


def test_18_expected_return_gap_is_by_design_not_degrading():
    # With full returns + READY HOC, the ONLY expected-return gap is by-design → READY.
    res = _build()
    er = next(g for g in res["data_gaps"] if g["code"] == "EXPECTED_RETURN_NOT_CALIBRATED")
    assert er["by_design"] is True and er["affects"] == "EXPECTED_RETURN_ANALYTICS"


def test_19_risk_data_gaps_explicit():
    # No aligned returns → volatility before/after UNAVAILABLE (explicit, never fabricated).
    res = _build(with_returns=False)
    assert res["risk"]["portfolio_volatility_before"] is None
    assert res["risk"]["portfolio_volatility_after"] is None
    gaps = {g["code"] for g in res["data_gaps"]}
    assert "PORTFOLIO_VOLATILITY_AFTER_UNAVAILABLE" in gaps
    assert res["proposal_state"] == "DEGRADED"


def test_20_no_mutation_of_input_objects():
    ic = _kic()
    snapshot = copy.deepcopy(ic)
    R.build_proposal(input_contract=ic, policy=_pol())
    assert ic == snapshot


def test_20b_volatility_authoritative_when_data_supports():
    res = _build()  # with returns + coverage
    assert res["risk"]["portfolio_volatility_before"] is not None
    assert res["risk"]["portfolio_volatility_after"] is not None
    assert res["risk"]["volatility_after_state"] == "AVAILABLE"
    assert res["proposal_state"] == "READY"


def test_20c_concentration_before_after():
    res = _build()
    assert res["risk"]["concentration_before"] is not None
    assert res["risk"]["concentration_after"] is not None
    assert res["risk"]["largest_position_before"] is not None
    assert res["risk"]["largest_position_after"] is not None


def test_20d_volatility_state_never_contradicts_value_below_coverage_floor():
    # Live 2026-08-07 regression: the covariance kernel computes over only ~59% of invested
    # weight while min_volatility_coverage = 0.80. The value must be WITHHELD and the state
    # must be the honest INSUFFICIENT_COVERAGE — never a false AVAILABLE (the exact contract
    # contradiction the live Slice-7 payload exposed).
    n = 80
    aligned = {"dates": ["d%03d" % i for i in range(n)],
               "series": {"AAA": _rets(n, 3), "BBB": _rets(n, 5)}}  # CCC deliberately uncovered
    # Covered names (AAA+BBB) sum to 0.59 of invested weight; CCC (0.41) has no return series.
    current = {"AAA": 0.30, "BBB": 0.29, "CCC": 0.41}
    pol = _pol(min_volatility_coverage=0.80, min_covariance_obs=20)
    risk, gaps = R._risk_block(
        current_weight=current, proposed_weight=current,
        sector_of={"AAA": "Tech", "BBB": "Tech", "CCC": "Fin"}, aligned_returns=aligned,
        hoc_reviews=[], urows={}, held_set={"AAA", "BBB", "CCC"}, policy=pol)
    # value withheld, coverage below floor, honest state, explicit gap — all consistent.
    assert risk["portfolio_volatility_before"] is None
    assert risk["volatility_before_state"] == "INSUFFICIENT_COVERAGE"
    assert risk["volatility_before_state"] != "AVAILABLE"
    assert risk["volatility_before_coverage"] < 0.80
    assert abs(risk["volatility_before_coverage"] - 0.59) < 0.01   # reproduces the live ~59%
    assert "PORTFOLIO_VOLATILITY_BEFORE_UNAVAILABLE" in risk["risk_data_gaps"]
    assert "PORTFOLIO_VOLATILITY_AFTER_UNAVAILABLE" in risk["risk_data_gaps"]
    # Structural invariant: a value is published IFF the state is AVAILABLE, both sides.
    for side in ("before", "after"):
        has_val = risk["portfolio_volatility_%s" % side] is not None
        is_avail = risk["volatility_%s_state" % side] == "AVAILABLE"
        assert has_val == is_avail
        assert risk["volatility_%s_state" % side] in R.VOLATILITY_STATE_VOCAB


def test_20e_volatility_state_unavailable_when_kernel_cannot_compute():
    # No aligned returns at all → the kernel cannot compute → honest UNAVAILABLE (distinct
    # from INSUFFICIENT_COVERAGE), value withheld, gap raised. State still never AVAILABLE.
    risk, gaps = R._risk_block(
        current_weight={"AAA": 0.5, "BBB": 0.5}, proposed_weight={"AAA": 0.5, "BBB": 0.5},
        sector_of={"AAA": "Tech", "BBB": "Fin"}, aligned_returns={"dates": [], "series": {}},
        hoc_reviews=[], urows={}, held_set={"AAA", "BBB"}, policy=_pol(min_volatility_coverage=0.80))
    assert risk["portfolio_volatility_before"] is None
    assert risk["volatility_before_state"] == "UNAVAILABLE"
    assert risk["volatility_after_state"] == "UNAVAILABLE"
    assert "PORTFOLIO_VOLATILITY_BEFORE_UNAVAILABLE" in risk["risk_data_gaps"]


# =========================================================================== #
# API fixtures (portfolio_state / scoring / hoc)
# =========================================================================== #
def _ps():
    return {
        "active_book": {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
                        "status": "ACTIVE", "holdings_count": 4, "initialized": True},
        "dates": {"eligible_market_date": "2026-08-06", "valuation_date": "2026-08-06"},
        "capital": {"nav": 100000.0, "cash": 0.0}, "state_hash": "PSH",
        "positions": [
            {"ticker": "AAA", "sector": "Tech", "portfolio_weight": 0.25, "market_value": 25000.0,
             "quantity": 100, "price": 250.0},
            {"ticker": "BBB", "sector": "Tech", "portfolio_weight": 0.25, "market_value": 25000.0,
             "quantity": 100, "price": 250.0},
            {"ticker": "CCC", "sector": "Fin", "portfolio_weight": 0.25, "market_value": 25000.0,
             "quantity": 100, "price": 250.0},
            {"ticker": "DDD", "sector": "Fin", "portfolio_weight": 0.25, "market_value": 25000.0,
             "quantity": 100, "price": 250.0}]}


def _scoring():
    return {"output_hash": "USH", "input_contract_hash": "UIC", "ranking_date": "2026-08-06",
            "rankings": _kic()["universe_rows"], "exclusions": {}}


def _hoc(state="READY", gaps=None):
    return {"assessment_hash": "HOC1", "assessment_state": state,
            "recommendation_counts": {"HOLD": 1, "REDUCE": 1, "EXIT": 1, "REPLACE": 1, "ADD": 0},
            "data_quality": {"data_gaps": gaps or []},
            "holding_reviews": _kic()["hoc_reviews"]}


def _api_pol():
    # This is a 4-name toy book, so every portfolio limit is scaled to it exactly as the
    # name cap and sector cap already were. Release 29.3 added the complete-target
    # turnover budget (0.35, calibrated for the real 25-name book); in a 4-name book a
    # single swap is 25% of the portfolio, so the production value would withhold every
    # construction fixture. The budget itself is exercised at its boundary on a
    # realistic book by tests/test_release29_3_decision_integrity.py.
    return A.resolve_policy({"target_position_count": 4, "candidate_rank_max": 50,
                             "max_name_weight": 0.5, "sector_cap_fraction": 1.0,
                             "max_one_way_turnover": 1.0,
                             "min_covariance_obs": 20, "min_volatility_coverage": 0.5})


def _rp(ps=None, scoring=None, hoc=None, price_panel=None, tmp=None, pol=None):
    return A.run_and_persist(portfolio_state=ps or _ps(), scoring=scoring or _scoring(),
                             hoc_assessment=hoc if hoc is not None else _hoc(),
                             price_panel=price_panel, policy=pol or _api_pol(),
                             reallocation_dir=str(tmp) if tmp else None)


# =========================================================================== #
# B. API / PERSISTENCE (21-40)
# =========================================================================== #
def test_21_not_run(tmp_path):
    r = A.load_reallocation_proposal(portfolio_state=_ps(), reallocation_dir=str(tmp_path))
    assert r["state"] == "NOT_RUN"
    assert r["artifact"] is None


def test_22_ready_proposal(tmp_path):
    # provide price panel so after-volatility computes → READY
    pp = {"series": {tk: {"dates": ["d%03d" % i for i in range(80)],
                          "adj": [100.0 + i for i in range(80)],
                          "ret": [None] + _rets(79, seed=j + 1)}
                     for j, tk in enumerate(["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"])}}
    out = _rp(price_panel=pp, tmp=tmp_path)
    assert out["proposal"]["proposal_state"] in ("READY", "DEGRADED")
    assert out["persistence"]["status"] == "CREATED"


def test_23_degraded_proposal(tmp_path):
    out = _rp(hoc=_hoc(gaps=["LIQUIDITY_UNAVAILABLE"]), tmp=tmp_path)
    assert out["proposal"]["proposal_state"] == "DEGRADED"
    gaps = {g["code"] for g in out["proposal"]["data_gaps"]}
    assert "HOC_LIQUIDITY_UNAVAILABLE" in gaps  # source gap carried forward


def test_24_blocked_proposal(tmp_path):
    out = _rp(hoc={"assessment_hash": "H", "assessment_state": "BLOCKED",
                   "holding_reviews": [], "data_quality": {"data_gaps": []}}, tmp=tmp_path)
    assert out["proposal"]["proposal_state"] == "BLOCKED"
    assert out["persistence"]["persisted"] is False  # BLOCKED not persisted


def test_25_active_book_alignment(tmp_path):
    out = _rp(tmp=tmp_path)
    assert out["proposal"]["active_book_id"] == "alpha_paper_book_1"
    ident = out["persistence"]["identity"]
    assert ident["active_book_id"] == "alpha_paper_book_1"


def test_26_eligible_date_alignment(tmp_path):
    out = _rp(tmp=tmp_path)
    assert out["proposal"]["eligible_market_date"] == "2026-08-06"
    assert out["persistence"]["identity"]["eligible_market_date"] == "2026-08-06"


def test_27_hoc_hash_alignment(tmp_path):
    out = _rp(tmp=tmp_path)
    assert out["input_contract"]["hoc_assessment_hash"] == "HOC1"
    assert out["persistence"]["identity"]["hoc_assessment_hash"] == "HOC1"


def test_28_immutable_artifact(tmp_path):
    out = _rp(tmp=tmp_path)
    path = Path(out["persistence"]["path"])
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["proposal_id"] == out["persistence"]["proposal_id"]
    assert body["proposal"]["proposal_hash"] == out["proposal"]["proposal_hash"]


def test_29_atomic_persistence_no_partial(tmp_path):
    _rp(tmp=tmp_path)
    leftovers = list(Path(tmp_path).rglob("*.tmp"))
    assert leftovers == []


def test_30_index_update(tmp_path):
    out = _rp(tmp=tmp_path)
    idx = json.loads((Path(tmp_path) / "index.json").read_text(encoding="utf-8"))
    key = "alpha_paper_book_1|2026-08-06"
    assert idx[key]["proposal_id"] == out["persistence"]["proposal_id"]


def test_31_read_back(tmp_path):
    out = _rp(tmp=tmp_path)
    r = A.load_reallocation_proposal(portfolio_state=_ps(), reallocation_dir=str(tmp_path))
    assert r["state"] in ("READY", "DEGRADED")
    assert r["artifact"]["proposal_id"] == out["persistence"]["proposal_id"]
    assert r["proposal_hash"] == out["proposal"]["proposal_hash"]


def test_32_idempotent_reuse(tmp_path):
    a = _rp(tmp=tmp_path)
    b = _rp(tmp=tmp_path)
    assert b["persistence"]["status"] == "REUSED_EXISTING"
    assert b["persistence"]["proposal_id"] == a["persistence"]["proposal_id"]


def test_33_different_hoc_hash_supersedes_not_reuse(tmp_path):
    a = _rp(tmp=tmp_path)
    hoc2 = _hoc()
    hoc2["assessment_hash"] = "HOC2_DIFFERENT"
    b = _rp(hoc=hoc2, tmp=tmp_path)
    assert b["persistence"]["status"] == "SUPERSEDED_PRIOR"
    assert b["persistence"]["proposal_id"] != a["persistence"]["proposal_id"]
    assert b["persistence"]["superseded_proposal_id"] == a["persistence"]["proposal_id"]
    # the read now points to the fresh proposal, never the stale one.
    r = A.load_reallocation_proposal(portfolio_state=_ps(), reallocation_dir=str(tmp_path))
    assert r["artifact"]["proposal_id"] == b["persistence"]["proposal_id"]


def test_34_no_provider_call():
    for tok in ("requests.get(", "requests.post(", "urlopen(", "httpx."):
        assert tok not in OWNER_SRC, tok


def test_35_no_prediction_call():
    assert "predict(" not in OWNER_SRC
    assert ":9000" not in OWNER_SRC


def test_36_no_db_write():
    for tok in ("sqlalchemy", "sessionmaker", "session.add(", "session.commit("):
        assert tok not in OWNER_SRC, tok


def test_37_no_operational_ledger_write():
    # persists ONLY under the reallocation research root; never the operational ledger.
    assert "PAPER_TRADER_REALLOC_DIR" in OWNER_SRC
    assert ".paper_trader" not in OWNER_SRC


def test_38_no_target_confirmation():
    for tok in ("confirm_target(", "confirm_snapshot(", "run_daily_close("):
        assert tok not in OWNER_SRC, tok


def test_39_no_order_creation():
    for tok in ("place_order(", "submit_order(", "create_order(", "run_fill_cycle("):
        assert tok not in OWNER_SRC, tok


def test_40_no_fill_or_nav_mutation():
    assert "book_nav(" not in OWNER_SRC
    out = R.build_proposal(input_contract=_kic(), policy=_pol())
    s = out["safety"]
    assert s["created_orders"] is False and s["created_fills"] is False
    assert s["changed_holdings"] is False and s["changed_nav"] is False
    assert s["confirmed_alpha_target"] is False
    assert s["created_operational_target"] is False


# =========================================================================== #
# C. DRC (41-48)
# =========================================================================== #
def _real_proposal():
    res = R.build_proposal(input_contract=_kic(), policy=_pol())
    return {"proposal": res,
            "persistence": {"status": "CREATED", "proposal_id": "reap_test",
                            "persisted": True, "superseded_proposal_id": None}}


def _fake_hoc_run(**_):
    s6 = importlib.import_module("tests.test_slice6_holding_opportunity_cost")
    return {"assessment": s6._run(), "persistence": {"status": "CREATED",
            "artifact_id": "hoc_test", "persisted": True}}


def _fake_realloc_run(**_):
    return _real_proposal()


def _drc_run(tmp, **kw):
    s3 = importlib.import_module("tests.test_slice3_daily_research_cycle")
    return s3._run(tmp, holding_opp_cost_fn=_fake_hoc_run,
                   reallocation_proposal_fn=_fake_realloc_run, **kw)


def test_41_build_reallocation_step_selected(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert "BUILD_REALLOCATION_PROPOSAL" in rec["completed_steps"]


def test_42_runs_after_hoc(tmp_path):
    rec, _ = _drc_run(tmp_path)
    steps = rec["completed_steps"]
    assert steps.index("ASSESS_HOLDING_OPPORTUNITY_COST") < steps.index("BUILD_REALLOCATION_PROPOSAL")
    assert steps.index("BUILD_REALLOCATION_PROPOSAL") < steps.index("RUN_PORTFOLIO_ASSESSMENT")


def test_43_proposal_id_in_manifest(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["reallocation_proposal_owner"] == "api.reallocation_proposal"
    assert rec["reallocation_proposal_id"] == "reap_test"
    assert rec["reallocation_proposal_selected"] is True


def test_44_proposal_hash_in_manifest(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["reallocation_proposal_hash"]
    assert rec["reallocation_action_counts"] is not None


def test_45_idempotent_rerun(tmp_path):
    rec1, _ = _drc_run(tmp_path)
    rec2, _ = _drc_run(tmp_path)
    assert rec2["reused_existing_run"] is True
    assert rec2["reallocation_proposal_hash"] == rec1["reallocation_proposal_hash"]


def test_46_terminal_persistence_correct(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["state"] in ("COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP")
    assert rec["terminal"] is True
    # the terminal manifest carries the reallocation reference (validated + read back).
    assert rec["reallocation_proposal_id"] and rec["reallocation_proposal_hash"]


def test_47_proposal_failure_does_not_fabricate_completion(tmp_path):
    def _failing(**_):
        return {"proposal": {"proposal_state": "BLOCKED"}, "persistence": {"status": "NOT_PERSISTED"}}
    s3 = importlib.import_module("tests.test_slice3_daily_research_cycle")
    rec, _ = s3._run(tmp_path, holding_opp_cost_fn=_fake_hoc_run,
                     reallocation_proposal_fn=_failing)
    step = next(s for s in rec["step_results"] if s["step_id"] == "BUILD_REALLOCATION_PROPOSAL")
    assert step["status"] == "FAILED"
    # a non-available reallocation is NOT a durability requirement → the run still completes,
    # but the reallocation reference is honestly absent (never fabricated).
    assert rec["reallocation_proposal_selected"] is False


def test_48_existing_hoc_artifact_immutable(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["opportunity_cost_assessment_hash"]
    # the reallocation step never mutates the HOC reference.
    assert rec["opportunity_cost_owner"] == "api.holding_opportunity_cost"


# =========================================================================== #
# D. WORKFLOW / UI (49-64)
# =========================================================================== #
def test_49_not_run_presentation():
    p = ws.build_reallocation_proposal_presentation(state="NOT_RUN", available=False,
                                                    eligible_date="2026-08-06")
    assert p["canonical_operator_state"] == "REALLOCATION_PROPOSAL_NOT_RUN"
    assert p["summary_label"] == "NONE YET"
    assert p["execution_available"] is False


def test_50_ready_presentation():
    p = ws.build_reallocation_proposal_presentation(
        state="READY", available=True, eligible_date="2026-08-06",
        action_counts={"RETAIN": 12, "EXIT": 8, "ADD": 9}, proposal_hash="h",
        proposed_holding_count=25)
    assert p["canonical_operator_state"] == "REALLOCATION_PROPOSAL_READY"
    assert p["has_proposal"] is True
    assert p["is_primary_decision"] is False  # HOC stays the primary decision


def test_51_degraded_presentation():
    p = ws.build_reallocation_proposal_presentation(state="DEGRADED", available=True,
                                                    eligible_date="2026-08-06")
    assert p["canonical_operator_state"] == "REALLOCATION_PROPOSAL_DEGRADED"
    assert p["severity"] == "ATTENTION"


def test_52_blocked_presentation():
    p = ws.build_reallocation_proposal_presentation(state="BLOCKED", available=False,
                                                    eligible_date="2026-08-06")
    assert p["canonical_operator_state"] == "REALLOCATION_PROPOSAL_BLOCKED"


def test_53_command_center_status_node():
    ui = UI.read_text(encoding="utf-8")
    for node in ("cc-realloc-card", "cc-realloc-badge", "cc-realloc-headline"):
        assert 'id="%s"' % node in ui


def test_54_daily_workflow_status_node():
    ui = UI.read_text(encoding="utf-8")
    for node in ("dw-realloc-card", "dw-realloc-badge", "dw-realloc-headline"):
        assert 'id="%s"' % node in ui


def test_55_portfolio_manager_detailed_card():
    ui = UI.read_text(encoding="utf-8")
    for node in ("realloc-card", "realloc-summary", "realloc-banner"):
        assert 'id="%s"' % node in ui


def test_56_allocation_table():
    ui = UI.read_text(encoding="utf-8")
    assert 'id="realloc-table"' in ui
    for col in ("Ticker", "Current %", "Proposed %", "Delta", "Action", "Reason"):
        assert col in ui


def test_57_one_ui_loader():
    ui = UI.read_text(encoding="utf-8")
    assert ui.count("function loadReallocationProposal") == 1


def test_58_no_js_portfolio_math():
    ui = UI.read_text(encoding="utf-8")
    start = ui.find("function loadReallocationProposal")
    end = ui.find("window.renderReallocationProposal")
    assert start != -1 and end != -1 and end > start
    region = ui[start:end]
    for tok in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.",
                "cost_rate", "COST_BPS", "compute"):
        assert tok not in region, tok


def test_59_review_only_badges():
    ui = UI.read_text(encoding="utf-8")
    assert "MANUAL REVIEW REQUIRED" in ui
    # the reallocation card carries the review-only safety badges.
    start = ui.find('id="realloc-card"')
    region = ui[start:start + 3000]
    assert "REVIEW ONLY" in region and "NO ORDERS" in region and "NO AUTOMATION" in region


def test_60_no_apply_button():
    ui = UI.read_text(encoding="utf-8")
    start = ui.find('id="realloc-card"')
    region = ui[start:start + 3000]
    for bad in ("Apply Rebalance", "Apply Reallocation", "Confirm Target", "Execute"):
        assert bad not in region, bad


def test_61_no_create_order_button():
    ui = UI.read_text(encoding="utf-8")
    start = ui.find("function loadReallocationProposal")
    end = ui.find("window.renderReallocationProposal")
    region = ui[start:end]
    for bad in ("create-orders", "createOrders", "place_order", "submitOrder"):
        assert bad not in region, bad


def test_62_no_broker_action():
    for tok in ("broker", "route_order", "run_fill_cycle"):
        assert tok not in OWNER_SRC.lower() or tok == "broker"  # 'broker' only in safety flags
    s = R._safety()
    assert s["performed_broker_execution"] is False


def test_63_daily_close_remains_separate():
    # workflow_state P6 (READY_FOR_DAILY_CLOSE) must not depend on the proposal existing.
    src = (ROOT / "api" / "workflow_state.py").read_text(encoding="utf-8")
    # the reallocation review action is INFO-only and routes to the portfolio manager.
    assert "ACTION_REVIEW_REALLOCATION" in src
    assert "REVIEW_REALLOCATION_PROPOSAL" in src


def test_64_automation_off():
    s = R._safety()
    assert s["automation_off"] is True
    assert s["automatic_promotion_allowed"] is False


def test_64b_gate_label_flipped_and_reallocation_landed():
    assert "NOT YET IMPLEMENTED" not in dag.PROPOSAL_REVIEW_LABEL
    g = dag.load_daily_action_gate(
        today="2026-08-06", current={"status": "MHZ_INPUTS_UNAVAILABLE"}, operational={},
        opportunity_cost={"opportunity_cost_available": True, "opportunity_cost_state": "READY"},
        reallocation={"reallocation_proposal_available": True,
                      "reallocation_proposal_state": "READY"})
    assert g["reallocation_engine_implemented"] is True
    assert g["reallocation_proposal_available"] is True
    # a research proposal never becomes execution authority.
    assert g["decision_authority"] == "NONE" and g["execution_available"] is False


def test_64c_workflow_exposes_reallocation_state_via_gate():
    g = dag.load_daily_action_gate(
        today="2026-08-06", current={"status": "MHZ_INPUTS_UNAVAILABLE"}, operational={},
        opportunity_cost={"opportunity_cost_available": True, "opportunity_cost_state": "READY"},
        reallocation={"reallocation_proposal_available": True,
                      "reallocation_proposal_state": "DEGRADED",
                      "reallocation_action_counts": {"RETAIN": 12}})
    assert g["reallocation_proposal_state"] == "DEGRADED"
    assert g["reallocation_action_counts"] == {"RETAIN": 12}


# =========================================================================== #
# E. ARCHITECTURE (65-71)
# =========================================================================== #
def _audit():
    aud = importlib.import_module("scripts.audit_architecture")
    files = aud._iter_source_files()
    return aud, aud.check_reallocation_proposal_ownership(files)


def test_65_exact_owners():
    aud, rp = _audit()
    assert rp["kernel_present"] and rp["owner_present"]
    assert rp["second_calculation_owner_modules"] == []
    assert rp["second_composition_owner_modules"] == []


def test_66_no_duplicate_calculations():
    _, rp = _audit()
    assert rp["kernel_forks_hoc"] is False
    assert rp["kernel_forks_scoring"] is False
    assert rp["missing_delegation"] == []


def test_67_no_mutation_path():
    _, rp = _audit()
    assert rp["owner_forbidden_calls"] == []
    assert rp["kernel_forbidden_calls"] == []
    assert rp["forbidden_routes_present"] == []
    assert rp["forbidden_route_methods_present"] is False


def test_68_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_69_inventory_drift_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    drift = aud.check_inventory_drift(aud._iter_source_files())
    assert drift["on_disk_not_in_inventory"] == []
    assert drift["in_inventory_not_on_disk"] == []


def test_70_slice8_research_agent_landed_no_second_registry():
    # Slice 8 (Persistent Alpha Research Agent, Milestone 4) has LANDED as its two canonical
    # owners; `slice8_present_modules` now guards that NO second/unified model registry was
    # created (the Research Agent reads the existing registries; it never forks them).
    _, rp = _audit()
    assert rp["slice8_present_modules"] == []          # no second/unified model registry
    assert not (ROOT / "api" / "model_registry.py").exists()
    assert (ROOT / "engine" / "research_agent.py").exists()
    assert (ROOT / "api" / "research_agent.py").exists()


def test_71_cadence_disabled():
    _, rp = _audit()
    assert rp["cadence_enabled"] is False
    assert rp["automatic_model_promotion_allowed"] is False
    # GET-only read route exactly once.
    assert rp["route_get_count"] == 1
    assert rp["reallocation_route_methods"] == ["GET"]


# =========================================================================== #
# Performance — bounded warm read paths (a GET must NEVER recompute the proposal).
# =========================================================================== #
def test_perf_warm_get_reallocation_proposal(tmp_path):
    _rp(tmp=tmp_path)  # produce once
    t0 = time.perf_counter()
    for _ in range(20):
        r = A.load_reallocation_proposal(portfolio_state=_ps(), reallocation_dir=str(tmp_path))
    dt = time.perf_counter() - t0
    assert r["state"] in ("READY", "DEGRADED")
    assert dt < 2.0, "20 warm reads took %.3fs" % dt


def test_perf_get_never_recomputes(monkeypatch, tmp_path):
    _rp(tmp=tmp_path)

    def _boom(*a, **k):
        raise AssertionError("GET must not recompute the proposal (kernel called).")
    monkeypatch.setattr(R, "build_proposal", _boom)
    r = A.load_reallocation_proposal(portfolio_state=_ps(), reallocation_dir=str(tmp_path))
    assert r["state"] in ("READY", "DEGRADED")  # served from the immutable artifact


def test_perf_warm_proposal_summary(tmp_path):
    _rp(tmp=tmp_path)
    t0 = time.perf_counter()
    for _ in range(50):
        s = A.load_proposal_summary(active_book_id="alpha_paper_book_1",
                                    eligible_market_date="2026-08-06",
                                    reallocation_dir=str(tmp_path))
    dt = time.perf_counter() - t0
    assert s["reallocation_proposal_available"] is True
    assert dt < 2.0, "50 warm summary reads took %.3fs" % dt
