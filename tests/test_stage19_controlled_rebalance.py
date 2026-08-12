r"""Stage 19 — Controlled paper-rebalance execution + corporate-action P&L integrity.

Hermetic acceptance for:
  * corporate-action split economic invariance (shares/cost-basis/NAV/P&L), read-time
    projection, no silent immutable rewrite, detection scan, historical auditability;
  * the APPROVED-proposal -> deterministic order plan -> SECOND confirmation -> paper
    orders -> NEXT_CLOSE (no-hindsight) -> reconciliation loop.

Every test uses temp desk / proposal / decision / corporate-action roots and injected
stores. No live endpoint, ledger, holding, cash or NAV is touched.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import corporate_actions as ca
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import rebalance_execution as rb


# --------------------------------------------------------------------------- #
# Hermetic desk / proposal / decision fixtures
# --------------------------------------------------------------------------- #
COST = desk.COST_RATE_PER_SIDE


def _buy_fill(book_id, tk, qty, price, date, fid):
    gross = qty * price
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": fid, "order_id": "ord_%s" % fid, "book_id": book_id, "ticker": tk,
        "side": desk.SIDE_BUY, "quantity": qty, "fill_date": date,
        "fill_price": price, "gross_value": round(gross, 2),
        "transaction_cost": round(cost, 4), "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": "NEXT_CLOSE", "immutable": True}}


def _write_marks(sdir: Path, series: dict, latest: str):
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger", "series": series,
        "latest_completed_date": latest, "updated_at": "2026-01-01T00:00:00+00:00"})


def _seed_desk(tmp: Path, *, initial_capital=100000.0, fills=(), series=None, latest=None):
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": "alpha_paper_book_1", "book_number": 1, "display_name": "Paper Book #1",
            "initial_capital": float(initial_capital), "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "model_id": "fundamental_momentum_50_50_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    if fills:
        desk._append_ledger(sdir, desk.FILLS_FILE, list(fills))
    if series is not None:
        _write_marks(sdir, series, latest)
    return sdir, book


def _proposal_artifact(*, book_id, eligible, nav, allocations, proposal_hash="HASH_A"):
    return {"proposal_id": "reap_%s_%s_%s" % (eligible, book_id, proposal_hash[:6]),
            "schema_version": "reallocation_proposal.v1",
            "identity": {"active_book_id": book_id, "eligible_market_date": eligible,
                         "proposal_hash": proposal_hash, "portfolio_state_hash": "PSH",
                         "hoc_assessment_hash": "HOC", "universe_scoring_hash": "USH",
                         "allocation_policy_version": "reallocation_allocation_policy.v1"},
            "proposal": {"proposal_state": "READY", "portfolio": {"nav": nav},
                         "allocations": allocations, "proposal_hash": proposal_hash}}


def _decision(*, book_id, eligible, proposal_hash, decision=pdec.DECISION_APPROVE,
              record_id="pdec_test_1"):
    return {"record_id": record_id, "decision": decision, "proposal_id": "reap_x",
            "proposal_hash": proposal_hash,
            "binding": {"active_book_id": book_id, "eligible_market_date": eligible,
                        "proposal_hash": proposal_hash}}


def _standard_setup(tmp: Path):
    """Held: AAA 40, BBB 50. Marks latest 2026-01-10. Proposal (all target weights under the
    10% position cap): INCREASE AAA (0.08), ADD CCC (0.06), EXIT BBB. Returns
    (sdir, book, artifact, decision, kwargs)."""
    fills = [_buy_fill("alpha_paper_book_1", "AAA", 40, 100.0, "2026-01-05", "f_AAA"),
             _buy_fill("alpha_paper_book_1", "BBB", 50, 200.0, "2026-01-05", "f_BBB")]
    series = {
        "AAA": [["2026-01-05", 100.0], ["2026-01-10", 110.0]],
        "BBB": [["2026-01-05", 200.0], ["2026-01-10", 190.0]],
        "CCC": [["2026-01-05", 50.0], ["2026-01-10", 52.0]],
        "SPY": [["2026-01-05", 400.0], ["2026-01-10", 405.0]],
    }
    sdir, book = _seed_desk(tmp, fills=fills, series=series, latest="2026-01-10")
    nav_blk = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))
    nav = nav_blk["nav"]
    allocations = [
        {"ticker": "AAA", "action": "INCREASE", "sector": "Tech", "proposed_weight": 0.08,
         "current_weight": 0.044, "current_market_value": 4400.0, "proposed_market_value": 8000.0},
        {"ticker": "CCC", "action": "ADD", "sector": "Health", "proposed_weight": 0.06,
         "current_weight": 0.0, "current_market_value": 0.0, "proposed_market_value": 6000.0},
        {"ticker": "BBB", "action": "EXIT", "sector": "Energy", "proposed_weight": 0.0,
         "current_weight": 0.095, "current_market_value": 9500.0, "proposed_market_value": 0.0},
    ]
    art = _proposal_artifact(book_id="alpha_paper_book_1", eligible="2026-01-10", nav=nav,
                             allocations=allocations, proposal_hash="HASH_A")
    dec = _decision(book_id="alpha_paper_book_1", eligible="2026-01-10", proposal_hash="HASH_A")
    kwargs = dict(desk_dir=sdir, active_book_id="alpha_paper_book_1",
                  eligible_market_date="2026-01-10", artifact=art, decision_record=dec,
                  plan_dir=tmp / "plans", actions_dir=tmp / "ca")
    return sdir, book, art, dec, kwargs


# =========================================================================== #
# WORKSTREAM A — corporate-action split economic invariance
# =========================================================================== #
def test_split_position_preserves_economic_value():
    sp = ca.split_position(42, 95.67, 2.0)
    assert sp["shares_after"] == 84.0
    assert sp["per_share_cost_after"] == pytest.approx(47.835)
    assert sp["total_cost_basis_before"] == pytest.approx(sp["total_cost_basis_after"])
    assert sp["cost_basis_invariant"] is True


def test_adjust_fills_quantity_and_cost_basis_invariant():
    fills = [_buy_fill("B", "MNST", 42, 95.67, "2026-07-22", "f1")["fill"]]
    action = {"action_id": "a1", "ticker": "MNST", "ex_date": "2026-08-11", "ratio": 2.0,
              "action_type": ca.ACTION_FORWARD_SPLIT, "book_id": None}
    adj = ca.adjust_fills(fills, [action])
    assert adj[0]["quantity"] == 84                 # shares doubled
    assert adj[0]["fill_price"] == pytest.approx(47.835)   # price halved
    # cash / cost basis inputs are UNCHANGED by a split
    assert adj[0]["net_cash_delta"] == fills[0]["net_cash_delta"]
    assert adj[0]["gross_value"] == fills[0]["gross_value"]


def test_adjust_fills_no_silent_immutable_rewrite():
    fills = [_buy_fill("B", "MNST", 42, 95.67, "2026-07-22", "f1")["fill"]]
    original = json.loads(json.dumps(fills))
    ca.adjust_fills(fills, [{"ticker": "MNST", "ex_date": "2026-08-11", "ratio": 2.0}])
    assert fills == original                        # input list never mutated


def test_adjust_fills_post_exdate_fill_not_scaled():
    # a fill ON/AFTER the ex-date is already on the post-split basis -> untouched
    fills = [_buy_fill("B", "MNST", 84, 47.835, "2026-08-11", "f2")["fill"]]
    adj = ca.adjust_fills(fills, [{"ticker": "MNST", "ex_date": "2026-08-11", "ratio": 2.0}])
    assert adj[0]["quantity"] == 84
    assert adj[0]["fill_price"] == pytest.approx(47.835)


def test_reconcile_book_nav_delta_is_pure_phantom(tmp_path):
    sdir, book = _seed_desk(tmp_path,
                            fills=[_buy_fill("alpha_paper_book_1", "MNST", 42, 95.67,
                                             "2026-07-22", "f1")],
                            series={"MNST": [["2026-07-22", 47.835], ["2026-08-11", 45.53]],
                                    "SPY": [["2026-08-11", 770.0]]}, latest="2026-08-11")
    fills = desk._fills(sdir)
    marks = desk.read_marks(sdir)
    action = {"ticker": "MNST", "ex_date": "2026-08-11", "ratio": 2.0,
              "action_type": ca.ACTION_FORWARD_SPLIT}
    rec = ca.reconcile_book(book=book, fills=fills, marks=marks, actions=[action])
    assert rec["cash_invariant"] is True
    assert rec["cost_basis_invariant"] is True
    # BEFORE = 42*45.53 phantom; AFTER = 84*45.53; delta = one whole MNST position
    assert rec["nav_delta"] == pytest.approx(42 * 45.53, abs=0.01)
    mnst = [p for p in rec["per_name_delta"] if p["ticker"] == "MNST"][0]
    assert mnst["quantity_before"] == 42.0 and mnst["quantity_after"] == 84.0
    assert mnst["cost_basis_before"] == pytest.approx(mnst["cost_basis_after"])


def test_book_nav_corporate_action_corrected(tmp_path):
    sdir, book = _seed_desk(tmp_path,
                            fills=[_buy_fill("alpha_paper_book_1", "MNST", 42, 95.67,
                                             "2026-07-22", "f1")],
                            series={"MNST": [["2026-07-22", 47.835], ["2026-08-11", 45.53]],
                                    "SPY": [["2026-08-11", 770.0]]}, latest="2026-08-11")
    fills = desk._fills(sdir)
    marks = desk.read_marks(sdir)
    nav_raw = desk.book_nav(book, fills, marks)
    action = {"ticker": "MNST", "ex_date": "2026-08-11", "ratio": 2.0,
              "action_type": ca.ACTION_FORWARD_SPLIT}
    nav_fix = desk.book_nav(book, fills, marks, corporate_actions=[action])
    assert nav_raw["holdings"]["MNST"] == 42
    assert nav_fix["holdings"]["MNST"] == 84
    assert nav_fix["nav"] - nav_raw["nav"] == pytest.approx(42 * 45.53, abs=0.01)


def test_scan_detects_only_split_artifact(tmp_path):
    sdir, book = _seed_desk(tmp_path, fills=[
        _buy_fill("alpha_paper_book_1", "MNST", 42, 95.67, "2026-07-22", "f1"),
        _buy_fill("alpha_paper_book_1", "AAA", 10, 100.0, "2026-07-22", "f2")],
        series={"MNST": [["2026-07-22", 47.835]], "AAA": [["2026-07-22", 100.0]],
                "SPY": [["2026-07-22", 770.0]]}, latest="2026-07-22")
    scan = ca.scan_fill_mark_artifacts(desk._fills(sdir), desk.read_marks(sdir))
    assert scan["n_suspects"] == 1
    assert scan["suspects"][0]["ticker"] == "MNST"
    assert scan["suspects"][0]["suggested_ratio"] == 2.0


def test_register_action_requires_confirm_and_is_auditable(tmp_path):
    d = tmp_path / "ca"
    bad = ca.register_action(confirm="WRONG", ticker="MNST", ex_date="2026-08-11",
                             ratio=2.0, actions_dir=d)
    assert bad["registered"] is False and bad["status"] == "CONFIRMATION_REQUIRED"
    assert ca.load_actions(actions_dir=d) == []          # nothing written
    ok = ca.register_action(confirm=ca.CONFIRM_TOKEN, ticker="MNST", ex_date="2026-08-11",
                            ratio=2.0, actions_dir=d, evidence={"implied_ratio": 2.0})
    assert ok["registered"] is True and ok["status"] == "REGISTERED"
    assert ok["action"]["immutable"] is True and ok["action"]["content_hash"]
    # idempotent: identical action reuses, no duplicate
    again = ca.register_action(confirm=ca.CONFIRM_TOKEN, ticker="MNST", ex_date="2026-08-11",
                               ratio=2.0, actions_dir=d)
    assert again["status"] == "REUSED_EXISTING"
    assert len(ca.load_actions(actions_dir=d)) == 1


# =========================================================================== #
# WORKSTREAM C/D/E — approved plan, second confirmation, paper-order write
# =========================================================================== #
def test_approved_proposal_required_reject_and_hold_create_nothing(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    for bad_decision in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
        kwargs2 = dict(kwargs)
        kwargs2["decision_record"] = _decision(book_id="alpha_paper_book_1",
                                               eligible="2026-01-10", proposal_hash="HASH_A",
                                               decision=bad_decision)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs2)
        assert res["performed_write"] is False
        assert res["status"] == rb.C_NOT_APPROVED
    # and no orders exist on the desk
    assert not desk._orders_state(sdir)


def test_stale_proposal_rejected(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    # decision was approved for a DIFFERENT (older) proposal hash than the current artifact
    kwargs2 = dict(kwargs)
    kwargs2["decision_record"] = _decision(book_id="alpha_paper_book_1", eligible="2026-01-10",
                                           proposal_hash="OLD_HASH")
    st = rb.load_rebalance_state(**kwargs2)
    assert st["rebalance_state"] == rb.RB_STALE
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs2)
    assert res["performed_write"] is False and res["status"] == rb.C_STALE
    assert not desk._orders_state(sdir)


def test_order_plan_and_hash_deterministic(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    s1 = rb.load_rebalance_state(**kwargs)
    s2 = rb.load_rebalance_state(**kwargs)
    assert s1["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    assert s1["order_plan"]["order_plan_hash"] == s2["order_plan"]["order_plan_hash"]
    assert s1["order_plan"]["order_plan_id"] == s2["order_plan"]["order_plan_id"]
    kinds = {o["ticker"]: o["order_kind"] for o in s1["order_plan"]["orders"]}
    assert kinds == {"AAA": "INCREASE", "CCC": "ADD", "BBB": "EXIT"}
    sides = {o["ticker"]: o["side"] for o in s1["order_plan"]["orders"]}
    assert sides["BBB"] == desk.SIDE_SELL and sides["CCC"] == desk.SIDE_BUY


def test_second_confirmation_required_wrong_token_creates_nothing(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    res = rb.confirm_rebalance_order_plan(confirm="NOPE", **kwargs)
    assert res["performed_write"] is False and res["status"] == rb.C_CONFIRM_REQUIRED
    assert not desk._orders_state(sdir)


def test_exact_plan_creates_paper_orders_with_lineage(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    plan = rb.load_rebalance_state(**kwargs)["order_plan"]
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                          expected_order_plan_hash=plan["order_plan_hash"],
                                          today="2026-01-10", **kwargs)
    assert res["status"] == rb.C_CREATED and res["performed_write"] is True
    assert res["n_orders_created"] == 3
    orders = desk._orders_state(sdir)
    assert len(orders) == 3
    for o in orders.values():
        lin = o["rebalance_lineage"]
        assert lin["proposal_id"] == art["proposal_id"]
        assert lin["proposal_hash"] == "HASH_A"
        assert lin["order_plan_id"] == plan["order_plan_id"]
        assert lin["order_plan_hash"] == plan["order_plan_hash"]
        assert lin["decision_id"] == "pdec_test_1"
        assert lin["paper_book_id"] == "alpha_paper_book_1"
        assert lin["execution_model"] == "NEXT_CLOSE"
        assert o["status"] == desk.ST_SUBMITTED       # submitted, not yet filled


def test_duplicate_confirm_creates_zero_duplicate_orders(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    plan = rb.load_rebalance_state(**kwargs)["order_plan"]
    r1 = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    assert r1["status"] == rb.C_CREATED
    n_after_first = len(desk._orders_state(sdir))
    r2 = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    assert r2["status"] == rb.C_REUSED and r2["performed_write"] is False
    assert len(desk._orders_state(sdir)) == n_after_first   # ZERO duplicates


# =========================================================================== #
# WORKSTREAM F/G — NEXT_CLOSE no-hindsight + reconciliation
# =========================================================================== #
def test_same_close_fill_impossible_then_future_close_fills(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    # settlement ran inside confirm at approval date -> NO same-close fill (guarded)
    assert res["settlement"]["n_filled"] == 0
    assert all(o["status"] == desk.ST_SUBMITTED for o in desk._orders_state(sdir).values())
    # a NEW owned close (2026-01-12, strictly after the approval store date) becomes available
    _write_marks(sdir, {
        "AAA": [["2026-01-05", 100.0], ["2026-01-10", 110.0], ["2026-01-12", 110.0]],
        "BBB": [["2026-01-05", 200.0], ["2026-01-10", 190.0], ["2026-01-12", 190.0]],
        "CCC": [["2026-01-05", 50.0], ["2026-01-10", 52.0], ["2026-01-12", 52.0]],
        "SPY": [["2026-01-05", 400.0], ["2026-01-10", 405.0], ["2026-01-12", 410.0]],
    }, "2026-01-12")
    settle = desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")
    assert settle["n_filled"] == 3
    assert all(o["status"] == desk.ST_FILLED for o in desk._orders_state(sdir).values())
    for f in desk._fills(sdir):
        if f["order_id"].startswith("ord_alpha_paper_book_1_00"):
            assert f["fill_date"] == "2026-01-12"       # future close, never 2026-01-10


def test_fill_cash_nav_reconciliation(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    plan = rb.load_rebalance_state(**kwargs)["order_plan"]
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    _write_marks(sdir, {
        "AAA": [["2026-01-05", 100.0], ["2026-01-10", 110.0], ["2026-01-12", 110.0]],
        "BBB": [["2026-01-05", 200.0], ["2026-01-10", 190.0], ["2026-01-12", 190.0]],
        "CCC": [["2026-01-05", 50.0], ["2026-01-10", 52.0], ["2026-01-12", 52.0]],
        "SPY": [["2026-01-05", 400.0], ["2026-01-10", 405.0], ["2026-01-12", 410.0]],
    }, "2026-01-12")
    desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")
    fills = desk._fills(sdir)
    cash, holdings = desk.book_cash_holdings(book, fills)
    # BBB fully exited; AAA increased; CCC added
    assert "BBB" not in holdings
    plan_orders = {o["ticker"]: o for o in plan["orders"]}
    assert plan_orders["AAA"]["side"] == desk.SIDE_BUY       # INCREASE
    assert holdings["AAA"] == 40 + plan_orders["AAA"]["quantity"]
    assert holdings["CCC"] == plan_orders["CCC"]["quantity"]
    # NAV = cash + marked holdings, independently recomputed
    nav_blk = desk.book_nav(book, fills, desk.read_marks(sdir))
    manual = cash + holdings["AAA"] * 110.0 + holdings["CCC"] * 52.0
    assert nav_blk["nav"] == pytest.approx(round(manual, 2), abs=0.01)
    # cash reconciles to the sum of immutable fill cash deltas
    assert cash == pytest.approx(book["initial_capital"]
                                 + sum(f["net_cash_delta"] for f in fills), abs=0.01)
    # ledgers remain chain-intact after the whole loop
    assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True
    assert desk.verify_ledger(sdir, desk.FILLS_FILE)["intact"] is True


def test_rebalance_state_progresses_to_executed(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    assert rb.load_rebalance_state(**kwargs)["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    # after submit, pending execution
    assert rb.load_rebalance_state(**kwargs)["rebalance_state"] == rb.RB_PLAN_CONFIRMED
    _write_marks(sdir, {
        "AAA": [["2026-01-10", 110.0], ["2026-01-12", 110.0]],
        "BBB": [["2026-01-10", 190.0], ["2026-01-12", 190.0]],
        "CCC": [["2026-01-10", 52.0], ["2026-01-12", 52.0]],
        "SPY": [["2026-01-10", 405.0], ["2026-01-12", 410.0]],
    }, "2026-01-12")
    desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_EXECUTED
    assert st["primary_action"] is None or st["primary_action"].get("path") is None


def test_one_primary_action_per_state(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    st = rb.load_rebalance_state(**kwargs)
    # exactly one primary action, and it is the second-confirmation path
    pa = st["primary_action"]
    assert pa and pa["path"] == "POST /v1/operations/rebalance/confirm-order-plan"
    # every state maps to at most one primary action
    for state, action in rb._PRIMARY_ACTION.items():
        assert action is None or isinstance(action, dict)


def test_no_broker_no_live_no_automation_flags(tmp_path):
    sdir, book, art, dec, kwargs = _standard_setup(tmp_path)
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-01-10", **kwargs)
    for k in ("broker_enabled", "live_orders_enabled", "automatic_approval_allowed",
              "automatic_rebalance_allowed", "promoted_model", "recalibrated_model",
              "changed_cadence"):
        assert res[k] is False
    assert res["wrote_to_desk_ledgers_only"] is True
