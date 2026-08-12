r"""Stage 19.1 — Corporate-action PROPAGATION integrity.

Stage 19 made ``api.corporate_actions`` the sole split authority and gave the desk an
OPTIONAL corporate-action parameter, but every CURRENT-state consumer kept calling the
economic primitives WITHOUT it. A registered split therefore corrected the Stage-19
reconciliation report and nothing else: the operational book, the portfolio state, the
per-holding cost basis, cumulative P&L, drawdown, contributors, the HOC / reallocation
inputs and the next Daily Research Cycle all still read the raw, unadjusted 42-share
representation.

This suite is the acceptance for the repair:

  * CURRENT economic reads apply the registered corporate actions BY DEFAULT, through the
    ONE canonical owner (nobody re-derives split arithmetic);
  * IMMUTABLE historical evidence — fills, orders, recorded forward-performance /
    Daily Close rows — is never rewritten and stays readable in its raw form;
  * a registered corporate action is part of the portfolio-state identity, so a proposal
    produced before the registration is STALE and can neither be approved nor turned into
    an order plan;
  * an EMPTY registry is an exact no-op (full backward compatibility).

Every test is hermetic: temp desk / corporate-action / proposal / decision roots and
injected read models. No live endpoint, ledger, holding, cash, NAV or provider is touched.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import corporate_actions as ca
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import operational_book as obk
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_state as pstate
from paper_trader.api import reallocation_proposal as rp
from paper_trader.api import rebalance_execution as rb

REPO_ROOT = Path(__file__).resolve().parents[1]
COST = desk.COST_RATE_PER_SIDE
BOOK_ID = "alpha_paper_book_1"

# The live Aug-2026 MNST forensic case, reproduced exactly on hermetic fixtures.
MNST_QTY = 42
MNST_FILL_PRICE = 95.67          # pre-split basis, as immutably recorded
MNST_EX_DATE = "2026-08-11"
MNST_RATIO = 2.0


# --------------------------------------------------------------------------- #
# Hermetic fixtures
# --------------------------------------------------------------------------- #
def _buy_fill(tk, qty, price, date, fid, book_id=BOOK_ID):
    gross = qty * price
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": fid, "order_id": "ord_%s" % fid, "book_id": book_id, "ticker": tk,
        "side": desk.SIDE_BUY, "quantity": qty, "fill_date": date,
        "fill_price": price, "gross_value": round(gross, 2),
        "transaction_cost": round(cost, 4), "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": "NEXT_CLOSE", "immutable": True}}


def _seed_desk(tmp: Path, *, fills, series, latest, initial_capital=100000.0):
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": BOOK_ID, "book_number": 1, "display_name": "Alpha Paper Book #1",
            "initial_capital": float(initial_capital), "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "model_id": "fundamental_momentum_50_50_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, list(fills))
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger", "series": series,
        "latest_completed_date": latest, "updated_at": "2026-01-01T00:00:00+00:00"})
    return sdir, book


def _split_world(tmp: Path):
    """The exact live shape: MNST bought 42 @ 95.67 pre-split; the provider has since
    BACK-ADJUSTED the whole series (every historical close halved), so the raw 42 shares
    now price against a halved mark. HLD is a control name that never split."""
    fills = [_buy_fill("MNST", MNST_QTY, MNST_FILL_PRICE, "2026-07-22", "f_MNST"),
             _buy_fill("HLD", 40, 100.0, "2026-07-22", "f_HLD")]
    series = {
        # back-adjusted: 2026-07-22 close was 95.67 pre-split -> 47.835 after
        "MNST": [["2026-07-22", 47.835], ["2026-08-10", 47.90], ["2026-08-11", 45.53]],
        "HLD": [["2026-07-22", 100.0], ["2026-08-10", 104.0], ["2026-08-11", 105.0]],
        "SPY": [["2026-07-22", 700.0], ["2026-08-10", 720.0], ["2026-08-11", 721.0]],
    }
    return _seed_desk(tmp, fills=fills, series=series, latest="2026-08-11")


def _register(tmp: Path, monkeypatch, *, ticker="MNST", ex_date=MNST_EX_DATE,
              ratio=MNST_RATIO):
    """Register a corporate action in a TEMP registry root and point the canonical owner
    at it (the confirm token is required, exactly as in production)."""
    cadir = tmp / "ca_registry"
    monkeypatch.setenv(ca.CA_DIR_ENV, str(cadir))
    res = ca.register_action(confirm=ca.CONFIRM_TOKEN, ticker=ticker, ex_date=ex_date,
                             ratio=ratio, action_type=ca.ACTION_FORWARD_SPLIT,
                             actions_dir=cadir)
    assert res["status"] == "REGISTERED" and res["registered"] is True
    return cadir


def _empty_registry(tmp: Path, monkeypatch):
    cadir = tmp / "ca_registry_empty"
    cadir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(ca.CA_DIR_ENV, str(cadir))
    return cadir


def _portfolio_state(*, nav, cash, cost_basis, perf, book_id=BOOK_ID):
    """Compose the canonical portfolio state from injected read models only (no live
    loader runs). The corporate-action block is resolved from the env-pointed registry."""
    operational = {"operational_book": {
        "book_id": book_id, "book_label": "Alpha Paper Book #1", "initialized": True,
        "current_status": "TRACKING", "currency": "USD_PAPER",
        "initial_capital": 100000.0, "starting_capital": 100000.0,
        "nav": nav, "cash": cash, "invested": round(nav - cash, 2),
        "nav_as_of_date": "2026-08-11", "holdings_count": 2,
        "canonical_state": {"nav": nav, "cash": cash,
                            "invested_value": round(nav - cash, 2),
                            "cost_basis": cost_basis,
                            "valuation_date": "2026-08-11"},
    }}
    return pstate.load_portfolio_state(
        operational=operational, freshness={"eligible_market_date": "2026-08-11"},
        performance=perf, gate={}, forward_status={},
        fills={"rows_count": 2, "count": 2, "fills": []})


# =========================================================================== #
# 1-5  CURRENT ACTIVE BOOK — the projection itself
# =========================================================================== #
def test_1_registered_split_projects_42_to_84_shares(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    _register(tmp_path, monkeypatch)
    _cash, held = desk.book_cash_holdings(book, desk._fills(sdir))
    assert held["MNST"] == 84, "the registered 2:1 split must project 42 -> 84 shares"
    assert held["HLD"] == 40, "a name with no registered action is untouched"


def test_2_current_nav_uses_adjusted_holdings(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    raw = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir),
                        corporate_actions=desk.RAW_LEDGER_VIEW)
    _register(tmp_path, monkeypatch)
    cur = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))
    # the raw view under-counts one whole MNST position (42 x 45.53)
    assert cur["nav"] == pytest.approx(raw["nav"] + MNST_QTY * 45.53, abs=0.02)
    assert cur["holdings"]["MNST"] == 84


def test_3_current_cash_is_unchanged_by_the_split(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    raw_cash, _ = desk.book_cash_holdings(book, desk._fills(sdir),
                                          corporate_actions=desk.RAW_LEDGER_VIEW)
    _register(tmp_path, monkeypatch)
    cur_cash, _ = desk.book_cash_holdings(book, desk._fills(sdir))
    assert cur_cash == pytest.approx(raw_cash), "a split never moves cash"


def test_4_current_total_cost_basis_is_unchanged_by_the_split(tmp_path, monkeypatch):
    sdir, _book = _split_world(tmp_path)
    raw = desk._fills(sdir)
    _register(tmp_path, monkeypatch)
    cur = desk.current_fills(sdir, book_id=BOOK_ID)
    raw_basis = sum(-f["net_cash_delta"] for f in raw)
    cur_basis = sum(-f["net_cash_delta"] for f in cur)
    assert cur_basis == pytest.approx(raw_basis), "total cost basis is split-invariant"
    m = [f for f in cur if f["ticker"] == "MNST"][0]
    assert m["quantity"] == 84
    assert m["fill_price"] == pytest.approx(MNST_FILL_PRICE / MNST_RATIO)
    assert m["quantity"] * m["fill_price"] == pytest.approx(MNST_QTY * MNST_FILL_PRICE)


def test_5_split_itself_contributes_zero_economic_pnl(tmp_path, monkeypatch):
    """Priced on ONE consistent (back-adjusted) basis, the corrected book is worth the
    same the day before and the day of the ex-date apart from the real price move."""
    sdir, book = _split_world(tmp_path)
    _register(tmp_path, monkeypatch)
    marks = desk.read_marks(sdir)
    fills = desk._fills(sdir)
    before = desk.book_nav(book, fills, marks, as_of="2026-08-10")
    on_ex = desk.book_nav(book, fills, marks, as_of="2026-08-11")
    # the ONLY difference is the genuine per-share move on each name
    expected = (84 * (45.53 - 47.90)) + (40 * (105.0 - 104.0))
    assert on_ex["nav"] - before["nav"] == pytest.approx(expected, abs=0.02)
    # and the split-driven ~50% one-day collapse is gone
    assert on_ex["nav"] > before["nav"] * 0.98


# =========================================================================== #
# 6-8  CURRENT PERFORMANCE vs IMMUTABLE HISTORICAL EVIDENCE
# =========================================================================== #
def _seed_raw_performance(sdir, book):
    """Append the two forward-performance rows exactly as the desk recorded them BEFORE
    the split was known: the 08-10 row on the PRE-split provider basis (MNST ~95.7), the
    08-11 row on the POST-split basis (MNST 45.53) — both against the RAW 42 shares. That
    pair is the phantom ~50% one-day loss."""
    rows = [
        {"row": {"book_id": BOOK_ID, "date": "2026-08-10",
                 "nav": 100000.0 + 0.0, "cash": 91772.35, "invested": 8180.0,
                 "holdings": {"MNST": 42, "HLD": 40}, "holdings_count": 2,
                 "missing_marks": [], "benchmark_ticker": "SPY", "benchmark_close": 720.0,
                 "benchmark_cumulative_return_pct": 0.0, "daily_return_pct": 0.5,
                 "cumulative_return_pct": -0.05, "drawdown_pct": -0.05,
                 "turnover_pct": 0.0, "transaction_cost": 0.0}},
        {"row": {"book_id": BOOK_ID, "date": "2026-08-11",
                 "nav": 97897.61, "cash": 91772.35, "invested": 6112.26,
                 "holdings": {"MNST": 42, "HLD": 40}, "holdings_count": 2,
                 "missing_marks": [], "benchmark_ticker": "SPY", "benchmark_close": 721.0,
                 "benchmark_cumulative_return_pct": 0.14, "daily_return_pct": -2.09,
                 "cumulative_return_pct": -2.10, "drawdown_pct": -2.10,
                 "turnover_pct": 0.0, "transaction_cost": 0.0}},
    ]
    desk._append_ledger(sdir, desk.PERFORMANCE_FILE, rows)
    return [r["row"] for r in rows]


def test_6_current_performance_removes_the_phantom_split_loss(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    _seed_raw_performance(sdir, book)
    _register(tmp_path, monkeypatch)
    perf = desk.load_performance(desk_dir=sdir)

    assert perf["current_economic_state"]["corporate_action_correction_applied"] is True
    cur = {r["date"]: r for r in perf["current_rows"]}
    # the corrected 08-11 daily move is the real price move, NOT a ~50% collapse
    assert cur["2026-08-11"]["daily_return_pct"] > -1.0
    assert cur["2026-08-11"]["holdings"]["MNST"] == 84
    # current drawdown / cumulative return are strictly better than the phantom figures
    raw = {r["date"]: r for r in perf["rows"]}
    assert cur["2026-08-11"]["drawdown_pct"] > raw["2026-08-11"]["drawdown_pct"]
    assert (perf["current_summary"]["cumulative_return_pct"]
            > perf["summary"]["cumulative_return_pct"])
    assert perf["current_economic_state"]["economic_pnl_from_corporate_action"] == 0.0


def test_7_historical_performance_rows_are_unchanged(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    seeded = _seed_raw_performance(sdir, book)
    on_disk_before = (sdir / desk.PERFORMANCE_FILE).read_text(encoding="utf-8")

    _register(tmp_path, monkeypatch)
    perf = desk.load_performance(desk_dir=sdir)

    # the returned raw rows equal what was appended, field for field
    for expected, got in zip(seeded, perf["rows"]):
        for k, v in expected.items():
            assert got[k] == v, f"immutable row field {k} was rewritten"
    # ...and nothing on disk moved
    assert (sdir / desk.PERFORMANCE_FILE).read_text(encoding="utf-8") == on_disk_before
    assert perf["historical_rows_never_recomputed"] is True
    assert perf["current_economic_state"]["historical_rows_rewritten"] is False
    # the corrected row still carries the untouched recorded values
    cur_811 = [r for r in perf["current_rows"] if r["date"] == "2026-08-11"][0]
    assert cur_811["raw"]["nav"] == 97897.61
    assert cur_811["raw"]["holdings"]["MNST"] == 42


def test_8_historical_fill_is_never_rewritten(tmp_path, monkeypatch):
    sdir, _book = _split_world(tmp_path)
    on_disk_before = (sdir / desk.FILLS_FILE).read_text(encoding="utf-8")
    _register(tmp_path, monkeypatch)

    # every current-state read path, exercised against the same ledger
    desk.current_fills(sdir, book_id=BOOK_ID)
    desk.book_nav(desk.open_book(sdir), desk._fills(sdir), desk.read_marks(sdir))
    desk.load_performance(desk_dir=sdir)
    desk.load_fills(desk_dir=sdir)

    assert (sdir / desk.FILLS_FILE).read_text(encoding="utf-8") == on_disk_before
    raw = [f for f in desk._fills(sdir) if f["ticker"] == "MNST"][0]
    assert raw["quantity"] == MNST_QTY and raw["fill_price"] == MNST_FILL_PRICE
    assert desk.verify_ledger(sdir, desk.FILLS_FILE)["intact"] is True


# =========================================================================== #
# 9-10  PORTFOLIO MANAGER / OPERATIONAL BOOK current surfaces
# =========================================================================== #
def test_9_10_operational_holdings_detail_uses_corrected_state(tmp_path, monkeypatch):
    """The per-holding dashboard (the Portfolio Manager's authoritative table, and the
    source of the operational book's portfolio summary) must show 84 shares AND an
    unchanged cost basis. Feeding it the RAW fills against corrected quantities would
    double the split name's cost basis — the regression this pins."""
    sdir, book = _split_world(tmp_path)
    _register(tmp_path, monkeypatch)
    marks = desk.read_marks(sdir)
    valuation = desk.book_nav(book, desk._fills(sdir), marks)

    rows, _prev = obk.build_holdings_detail(
        book=book, valuation=valuation,
        fills=desk.current_fills(sdir, book_id=BOOK_ID),
        marks=marks, plan_orders=[], target_weights={})
    m = {r["ticker"]: r for r in rows}["MNST"]

    assert m["quantity"] == 84
    assert m["average_cost"] == pytest.approx(
        (MNST_QTY * MNST_FILL_PRICE * (1 + COST)) / 84, abs=0.01)
    assert m["cost_basis"] == pytest.approx(MNST_QTY * MNST_FILL_PRICE * (1 + COST),
                                            abs=0.05), "cost basis is split-invariant"
    assert m["market_value"] == pytest.approx(84 * 45.53, abs=0.01)
    # the phantom ~-50% detractor is gone: the real loss is the genuine price move only
    assert m["unrealized_pnl_pct"] > -0.10

    # and the wrong wiring (raw fills + corrected quantities) is provably different
    wrong, _ = obk.build_holdings_detail(
        book=book, valuation=valuation, fills=desk._fills(sdir),
        marks=marks, plan_orders=[], target_weights={})
    assert {r["ticker"]: r for r in wrong}["MNST"]["cost_basis"] != pytest.approx(
        m["cost_basis"], abs=0.05)


def test_10_operational_book_wires_the_current_fill_view(tmp_path):
    """The canonical operational payload must obtain its per-holding fills from the ONE
    current-state view, not from the raw immutable ledger reader."""
    src = (REPO_ROOT / "api" / "operational_book.py").read_text(encoding="utf-8")
    assert "desk.current_fills(" in src
    assert "fills = [f for f in desk._fills(sdir)" not in src


# =========================================================================== #
# 11-13  HOC / REALLOCATION / DAILY RESEARCH CYCLE inputs
# =========================================================================== #
def _corrected_state(tmp_path, monkeypatch, *, register=True):
    sdir, book = _split_world(tmp_path)
    _seed_raw_performance(sdir, book)
    if register:
        _register(tmp_path, monkeypatch)
    else:
        _empty_registry(tmp_path, monkeypatch)
    perf = desk.load_performance(desk_dir=sdir)
    val = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))
    cur_fills = desk.current_fills(sdir, book_id=BOOK_ID)
    cost_basis = round(sum(-f["net_cash_delta"] for f in cur_fills), 2)
    ps = _portfolio_state(nav=val["nav"], cash=val["cash"], cost_basis=cost_basis,
                          perf=perf)
    return sdir, book, ps, perf


def test_11_hoc_input_contract_uses_corrected_state(tmp_path, monkeypatch):
    _sdir, _book, ps, _perf = _corrected_state(tmp_path, monkeypatch)
    ic = hoc.build_input_contract(
        portfolio_state=ps, scoring={"rankings": [], "output_hash": "USH",
                                     "input_contract_hash": "UIC"})
    assert ic["nav"] == ps["capital"]["nav"]
    assert ic["nav"] == pytest.approx(desk._r2(ps["capital"]["nav"]))
    assert ic["corporate_actions_hash"] == ps["corporate_actions"]["registry_fingerprint"]
    assert ic["corporate_actions_hash"] != ca.EMPTY_REGISTRY_FINGERPRINT


def test_12_reallocation_input_contract_uses_corrected_state(tmp_path, monkeypatch):
    _sdir, _book, ps, _perf = _corrected_state(tmp_path, monkeypatch)
    ic = rp.build_input_contract(
        portfolio_state=ps,
        scoring={"rankings": [], "output_hash": "USH", "input_contract_hash": "UIC"},
        hoc_assessment={}, price_panel=None, policy=rp.resolve_policy())
    assert ic["nav"] == ps["capital"]["nav"]
    assert ic["cash"] == ps["capital"]["cash"]
    assert ic["corporate_actions_hash"] == ps["corporate_actions"]["registry_fingerprint"]


def test_13_next_daily_research_cycle_uses_corrected_state(tmp_path, monkeypatch):
    """The DRC's sole composition path is ``reallocation_proposal.run_proposal`` over the
    portfolio-state loader. Prove the contract it would build carries the CORRECTED NAV
    and the 84-share economic position — never the raw 42-share representation."""
    _sdir, _book, ps, _perf = _corrected_state(tmp_path, monkeypatch)
    run = rp.run_proposal(
        portfolio_state_loader=lambda: ps,
        scoring={"rankings": [], "output_hash": "USH", "input_contract_hash": "UIC"},
        hoc_assessment={}, price_panel=None)
    ic = run["input_contract"]
    assert ic["nav"] == ps["capital"]["nav"]
    assert ic["corporate_actions_hash"] == ps["corporate_actions"]["registry_fingerprint"]
    # the identity the next proposal is persisted under binds the registry state
    ident = rp.proposal_identity(input_contract=ic, result=run["proposal"])
    assert ident["corporate_actions_hash"] == ic["corporate_actions_hash"]


# =========================================================================== #
# 14-17  IDENTITY + STALENESS (a registration invalidates older evidence)
# =========================================================================== #
def test_14_corporate_action_registry_affects_portfolio_state_hash(tmp_path, monkeypatch):
    _s1, _b1, ps_empty, _p1 = _corrected_state(tmp_path / "a", monkeypatch, register=False)
    _s2, _b2, ps_reg, _p2 = _corrected_state(tmp_path / "b", monkeypatch, register=True)

    assert ps_empty["corporate_actions"]["n_registered"] == 0
    assert (ps_empty["corporate_actions"]["registry_fingerprint"]
            == ca.EMPTY_REGISTRY_FINGERPRINT)
    assert ps_reg["corporate_actions"]["n_registered"] == 1
    assert ps_reg["state_hash"] != ps_empty["state_hash"], (
        "registering a corporate action must change the portfolio-state hash")
    assert ps_reg["source_hashes"]["corporate_actions"] == \
        ps_reg["corporate_actions"]["registry_fingerprint"]


def _pre_registration_artifact(eligible="2026-08-11", proposal_hash="HASH_PRE"):
    """A proposal persisted BEFORE the corporate-action identity contract existed: it
    carries no corporate_actions_hash at all (the real Aug-11 artifact's shape)."""
    return {
        "proposal_id": "reap_%s_%s_%s" % (eligible, BOOK_ID, proposal_hash[:6]),
        "identity": {"active_book_id": BOOK_ID, "eligible_market_date": eligible,
                     "proposal_hash": proposal_hash, "portfolio_state_hash": "PSH_OLD",
                     "hoc_assessment_hash": "HOC", "universe_scoring_hash": "USH",
                     "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "input_contract": {"eligible_market_date": eligible, "active_book_id": BOOK_ID,
                           "nav": 97479.79, "cash": 4630.31},
        "proposal": {"proposal_state": "READY", "proposal_hash": proposal_hash,
                     "portfolio": {"nav": 97479.79, "proposed_holding_count": 2},
                     "action_counts": {"EXIT": 1, "ADD": 1, "RETAIN": 1},
                     "turnover": {"one_way_turnover": 0.2},
                     "allocations": [
                         {"ticker": "MNST", "action": "EXIT", "sector": "Staples",
                          "proposed_weight": 0.0},
                         {"ticker": "HLD", "action": "RETAIN", "sector": "Tech",
                          "proposed_weight": 0.05}]},
    }


def test_15_pre_registration_proposal_becomes_stale(tmp_path, monkeypatch):
    _sdir, _book, ps, _perf = _corrected_state(tmp_path, monkeypatch)
    art = _pre_registration_artifact()

    read = rp.load_reallocation_proposal(portfolio_state=ps, artifact=art)
    assert read["state"] == rp.STATE_STALE
    assert read["stale"] is True
    assert read["approvable"] is False and read["executable"] is False
    st = read["staleness"]
    assert st["reason"] == ca.STALE_REASON_CORPORATE_ACTION
    assert st["bound_corporate_actions_hash"] == ca.EMPTY_REGISTRY_FINGERPRINT
    assert st["current_corporate_actions_hash"] == \
        ps["corporate_actions"]["registry_fingerprint"]

    summ = rp.load_proposal_summary(active_book_id=BOOK_ID,
                                    eligible_market_date="2026-08-11", artifact=art)
    assert summ["reallocation_proposal_stale"] is True
    assert summ["reallocation_proposal_state"] == rp.STATE_STALE


def test_16_stale_proposal_cannot_be_approved(tmp_path, monkeypatch):
    _sdir, _book, _ps, _perf = _corrected_state(tmp_path, monkeypatch)
    art = _pre_registration_artifact()
    ddir = tmp_path / "decisions"

    res = pdec.record_decision(
        decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN, artifact=art,
        active_book_id=BOOK_ID, eligible_market_date="2026-08-11", decision_dir=ddir)

    assert res["status"] == pdec.PDS_STALE
    assert res["recorded"] is False
    assert res["created_orders"] is False and res["changed_holdings"] is False
    assert res["stale_reason"] == ca.STALE_REASON_CORPORATE_ACTION
    # nothing durable was written
    assert not (ddir / "decision_records.json").exists() or \
        json.loads((ddir / "decision_records.json").read_text(encoding="utf-8")) == []

    # ...and the operator-facing review state says so
    summ = rp.load_proposal_summary(active_book_id=BOOK_ID,
                                    eligible_market_date="2026-08-11", artifact=art)
    view = pdec.derive_decision_state(has_active_book=True, proposal_summary=summ,
                                      decision_record=None)
    assert view["portfolio_decision_state"] == pdec.PDS_STALE
    assert view["approvable"] is False
    assert view["corporate_action_stale"] is True


def test_17_stale_proposal_cannot_create_an_order_plan(tmp_path, monkeypatch):
    sdir, _book, ps, _perf = _corrected_state(tmp_path, monkeypatch)
    art = _pre_registration_artifact()
    # even WITH a recorded APPROVE bound to the same (unchanged) proposal hash
    dec = {"record_id": "pdec_x", "decision": pdec.DECISION_APPROVE,
           "proposal_hash": "HASH_PRE",
           "binding": {"active_book_id": BOOK_ID, "eligible_market_date": "2026-08-11",
                       "proposal_hash": "HASH_PRE"}}
    kw = dict(desk_dir=sdir, active_book_id=BOOK_ID, eligible_market_date="2026-08-11",
              artifact=art, decision_record=dec, portfolio_state=ps,
              plan_dir=tmp_path / "plans")

    state = rb.load_rebalance_state(**kw)
    assert state["rebalance_state"] == rb.RB_STALE
    assert state["order_plan"] is None
    assert state["order_plan_buildable"] is False
    assert state["stale_reason"] == ca.STALE_REASON_CORPORATE_ACTION

    fills_before = len(desk._fills(sdir))
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kw)
    assert res["status"] == rb.C_STALE
    assert res["created_orders"] is False and res["performed_write"] is False
    assert res["changed_holdings"] is False and res["changed_cash"] is False
    assert len(desk._fills(sdir)) == fills_before
    assert desk.load_orders(desk_dir=sdir)["n_orders"] == 0


# =========================================================================== #
# 18-19  ONE OWNER — no duplicated split math anywhere, none in the UI
# =========================================================================== #
def test_18_no_duplicate_corporate_action_math_exists():
    """Split arithmetic may be DEFINED in exactly one module."""
    owners = []
    for path in sorted(list((REPO_ROOT / "api").rglob("*.py"))
                       + list((REPO_ROOT / "engine").rglob("*.py"))):
        src = path.read_text(encoding="utf-8", errors="replace")
        if "def split_position(" in src or "def adjust_fills(" in src:
            owners.append(path.relative_to(REPO_ROOT).as_posix())
    assert owners == ["api/corporate_actions.py"], (
        "split arithmetic must live in exactly one canonical owner; found %s" % owners)


def test_19_ui_performs_no_split_calculations():
    ui = (REPO_ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                             errors="replace")
    for token in ("adjust_fills", "split_position", "shares_after",
                  "* ratio", "/ ratio", "quantity * 2"):
        assert token not in ui, f"the UI must not perform split math (found {token!r})"


# =========================================================================== #
# 20  BACKWARD COMPATIBILITY — an empty registry is an EXACT no-op
# =========================================================================== #
def test_20_empty_registry_is_an_exact_no_op(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    _seed_raw_performance(sdir, book)
    _empty_registry(tmp_path, monkeypatch)

    marks, fills = desk.read_marks(sdir), desk._fills(sdir)
    assert desk.book_nav(book, fills, marks) == desk.book_nav(
        book, fills, marks, corporate_actions=desk.RAW_LEDGER_VIEW)
    assert desk.current_fills(sdir, book_id=BOOK_ID) == [
        dict(f) for f in desk._fills(sdir) if f["book_id"] == BOOK_ID]

    perf = desk.load_performance(desk_dir=sdir)
    assert perf["current_economic_state"]["corporate_action_correction_applied"] is False
    assert perf["current_rows"] == perf["rows"]
    assert perf["current_summary"] == perf["summary"]

    # an artifact with no recorded fingerprint is NOT stale against an empty registry
    art = _pre_registration_artifact()
    ps = _portfolio_state(nav=desk.book_nav(book, fills, marks)["nav"],
                          cash=desk.book_nav(book, fills, marks)["cash"],
                          cost_basis=8250.0, perf=perf)
    read = rp.load_reallocation_proposal(portfolio_state=ps, artifact=art)
    assert read["stale"] is False
    assert read["state"] != rp.STATE_STALE


# =========================================================================== #
# 21-24  SAFETY — no broker, no live orders, no automation, no evidence rewrite
# =========================================================================== #
def test_21_22_23_no_broker_no_live_orders_no_automation(tmp_path, monkeypatch):
    sdir, book = _split_world(tmp_path)
    _register(tmp_path, monkeypatch)
    safety = desk.desk_safety()
    assert safety["broker_enabled"] is False
    assert safety["orders_enabled"] is False and safety["live_orders_enabled"] is False
    assert safety["automation_enabled"] is False
    assert safety["background_execution"] is False and safety["scheduled_tasks"] is False

    rbs = rb.load_rebalance_state(desk_dir=sdir, active_book_id=BOOK_ID,
                                  eligible_market_date="2026-08-11", artifact=None,
                                  plan_dir=tmp_path / "plans")
    assert rbs.get("broker_enabled") is False
    assert rbs.get("live_orders_enabled") is False
    assert rbs.get("automatic_rebalance_allowed") is False

    # No broker/scheduler INTEGRATION exists in the owners this hotfix touched. (The
    # string "broker" appears only inside the negative safety flags, so match real
    # integration surfaces — imports, client SDKs, schedulers — not the word.)
    for rel in ("api/corporate_actions.py", "api/paper_trading_desk.py",
                "api/rebalance_execution.py"):
        src = (REPO_ROOT / rel).read_text(encoding="utf-8").lower()
        for token in ("import alpaca", "import ib_insync", "ibapi", "brokerclient",
                      "broker_api", "place_order(", "submit_order(", "schedule.every",
                      "crontab", "apscheduler", "auto_approve", "auto_rebalance",
                      "auto_confirm"):
            assert token not in src, f"{rel} must contain no {token!r} path"


def test_24_no_historical_evidence_is_rewritten_by_any_current_read(tmp_path, monkeypatch):
    """Fingerprint EVERY append-only desk ledger, exercise every current-state read the
    hotfix touches, then prove not one byte moved and every chain hash still verifies."""
    sdir, book = _split_world(tmp_path)
    _seed_raw_performance(sdir, book)
    _register(tmp_path, monkeypatch)

    before = {f: (sdir / f).read_bytes() for f in desk.LEDGER_FILES
              if (sdir / f).exists()}
    assert desk.FILLS_FILE in before and desk.PERFORMANCE_FILE in before

    marks, fills = desk.read_marks(sdir), desk._fills(sdir)
    desk.book_nav(book, fills, marks)
    desk.book_cash_holdings(book, fills)
    desk.current_fills(sdir, book_id=BOOK_ID)
    desk.load_performance(desk_dir=sdir)
    desk.load_attribution(desk_dir=sdir, window="daily")
    desk.load_status(desk_dir=sdir)
    desk.load_books(desk_dir=sdir)
    desk.load_fills(desk_dir=sdir)
    desk.load_execution_preview(desk_dir=sdir)
    ca.load_corporate_action_report(desk_dir=sdir)

    after = {f: (sdir / f).read_bytes() for f in desk.LEDGER_FILES
             if (sdir / f).exists()}
    assert after == before, "a read path rewrote immutable append-only evidence"
    integrity = desk.verify_all_ledgers(desk_dir=sdir)
    assert integrity["all_intact"] is True


def test_24b_attribution_reads_the_corrected_curve(tmp_path, monkeypatch):
    """Attribution must be internally consistent on the CURRENT economic basis: the window
    NAVs come from the corrected curve (not the raw rows carrying the phantom ~2% one-day
    collapse) and each name's contribution is measured on its corrected share count."""
    sdir, book = _split_world(tmp_path)
    _seed_raw_performance(sdir, book)

    _empty_registry(tmp_path, monkeypatch)
    before = desk.load_attribution(desk_dir=sdir, window="daily")
    mnst_before = {c["ticker"]: c["pnl"] for c in before["all_contributors"]}["MNST"]
    # the raw rows report a ~2% one-day loss the book did not economically suffer
    assert before["portfolio_return_pct"] < -1.5
    assert mnst_before == pytest.approx(MNST_QTY * (45.53 - 47.90), abs=0.5)
    assert before["current_economic_state"][
        "corporate_action_correction_applied"] is False

    _register(tmp_path, monkeypatch)
    after = desk.load_attribution(desk_dir=sdir, window="daily")
    mnst_after = {c["ticker"]: c["pnl"] for c in after["all_contributors"]}["MNST"]

    # the phantom whole-book collapse is gone...
    assert after["portfolio_return_pct"] > -1.0
    assert after["portfolio_return_pct"] > before["portfolio_return_pct"]
    # ...and MNST is attributed on its true 84-share economic position
    assert mnst_after == pytest.approx(84 * (45.53 - 47.90), abs=0.5)
    assert after["current_economic_state"]["corporate_action_correction_applied"] is True
    assert after["current_economic_state"]["historical_rows_rewritten"] is False
