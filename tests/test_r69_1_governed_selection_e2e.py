r"""R69.1 - the WHOLE governed path, end to end, in a hermetic world.

    review -> select -> read back the persisted selection -> approve EXACTLY
    that selection -> order plan -> separate manual confirmation -> next-close
    settlement -> reconciled portfolio

Every test builds its own desk, proposal, decision and selection roots and
injects them. No live endpoint, ledger, holding, cash or NAV is touched, and no
test here runs against the operational book.

The path is proved in two halves, because proving it is what showed they are
different:

* FULL_TARGET runs the whole chain to a reconciled portfolio.
* MINIMUM_REPAIR runs as far as the order plan and then FAILS CLOSED, because
  ``api.rebalance_execution`` reconciles the desk against the proposal
  artifact's ``allocations`` - the full target - and the minimum repair has no
  allocation list anywhere in the artifact. Before this release the operator
  selected 14 names and 21% turnover, approved it, and was handed the full
  target's 20 names and 35% turnover under that approval.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import rebalance_execution as rb

BOOK = "alpha_paper_book_1"
SESSION = "2026-01-10"
PHASH = "HASH_R691"
COST = desk.COST_RATE_PER_SIDE


# --------------------------------------------------------------------------- #
# Hermetic world
# --------------------------------------------------------------------------- #
def _buy_fill(tk, qty, price, date, fid):
    gross = qty * price
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": fid, "order_id": "ord_%s" % fid, "book_id": BOOK, "ticker": tk,
        "side": desk.SIDE_BUY, "quantity": qty, "fill_date": date,
        "fill_price": price, "gross_value": round(gross, 2),
        "transaction_cost": round(cost, 4), "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": "NEXT_CLOSE", "immutable": True}}


def _marks(sdir: Path, series: dict, latest: str):
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger", "series": series,
        "latest_completed_date": latest, "updated_at": "2026-01-01T00:00:00+00:00"})


SERIES = {
    "AAA": [["2026-01-05", 100.0], [SESSION, 110.0]],
    "BBB": [["2026-01-05", 200.0], [SESSION, 190.0]],
    "CCC": [["2026-01-05", 50.0], [SESSION, 52.0]],
    "SPY": [["2026-01-05", 400.0], [SESSION, 405.0]],
}

ALLOCATIONS = [
    {"ticker": "AAA", "action": "INCREASE", "sector": "Tech", "proposed_weight": 0.08,
     "current_weight": 0.044, "current_market_value": 4400.0,
     "proposed_market_value": 8000.0},
    {"ticker": "CCC", "action": "ADD", "sector": "Health", "proposed_weight": 0.06,
     "current_weight": 0.0, "current_market_value": 0.0,
     "proposed_market_value": 6000.0},
    {"ticker": "BBB", "action": "EXIT", "sector": "Energy", "proposed_weight": 0.0,
     "current_weight": 0.095, "current_market_value": 9500.0,
     "proposed_market_value": 0.0},
]


def _world(tmp: Path):
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": BOOK, "book_number": 1, "display_name": "Paper Book #1",
            "initial_capital": 100000.0, "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "model_id": "fundamental_momentum_50_50_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, [
        _buy_fill("AAA", 40, 100.0, "2026-01-05", "f_AAA"),
        _buy_fill("BBB", 50, 200.0, "2026-01-05", "f_BBB")])
    _marks(sdir, SERIES, SESSION)
    nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))["nav"]
    artifact = {
        "proposal_id": "reap_%s_%s_%s" % (SESSION, BOOK, PHASH[:6]),
        "schema_version": "reallocation_proposal.v1",
        "identity": {"active_book_id": BOOK, "eligible_market_date": SESSION,
                     "proposal_hash": PHASH, "portfolio_state_hash": "PSH",
                     "hoc_assessment_hash": "HOC", "universe_scoring_hash": "USH",
                     "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "proposal": {
            "proposal_state": "READY", "portfolio": {"nav": nav},
            "allocations": ALLOCATIONS, "proposal_hash": PHASH,
            # The R63 approval gate reads the proposal kernel's OWN published
            # verdict off the artifact; an artifact without it is unverifiable
            # and refused. This hermetic target resolves every obligation.
            "mandatory_repair": {
                "owner": "engine.holding_opportunity_cost",
                "contract_version": "mandatory_repair_contract.v1",
                "obligations_resolved": True,
                "obligations_open_against_target": [],
                "obligation_count": 1},
        },
    }
    ddir = tmp / "decisions"
    ddir.mkdir(parents=True, exist_ok=True)
    kwargs = dict(desk_dir=sdir, active_book_id=BOOK, eligible_market_date=SESSION,
                  artifact=artifact, plan_dir=tmp / "plans", actions_dir=tmp / "ca",
                  decision_dir=ddir, latest_session=SESSION)
    return sdir, book, artifact, ddir, kwargs


def _envelope(artifact: dict):
    """A review envelope shaped exactly like the live one - including BOTH copies
    of the option list, and the top-level reconciled copy the write gate reads."""
    opts = [
        {"target": "CURRENT", "label": "Current portfolio (do nothing)",
         "selectable": True, "blockers": [], "blocker_codes": [],
         "positions": 2, "changes": 0, "one_way_turnover": 0.0,
         "estimated_cost": 0.0, "score": 0.80, "cash_weight": 0.86,
         "concentration": 0.01, "obligations_remaining": [{"ticker": "BBB"}],
         "mandatory_obligations_remaining": 1, "is_defer": True},
        {"target": "MINIMUM_REPAIR", "label": "Minimum constraint repair",
         "selectable": True, "blockers": [], "blocker_codes": [],
         "positions": 1, "changes": 1, "one_way_turnover": 0.095,
         "estimated_cost": 9.5, "score": 0.88, "cash_weight": 0.955,
         "concentration": 0.002, "obligations_remaining": [],
         "mandatory_obligations_remaining": 0, "is_defer": False},
        {"target": "FULL_TARGET", "label": "Full zero-base target (the proposal)",
         "selectable": True, "blockers": [], "blocker_codes": [],
         "positions": 2, "changes": 3, "one_way_turnover": 0.21,
         "estimated_cost": 21.0, "score": 0.93, "cash_weight": 0.86,
         "concentration": 0.004, "obligations_remaining": [],
         "mandatory_obligations_remaining": 0, "is_defer": False},
    ]
    block = {"options": opts, "recommended_target": "MINIMUM_REPAIR",
             "option_order": ["CURRENT", "MINIMUM_REPAIR", "FULL_TARGET"],
             "selection_is_approval": False}
    ident = artifact["identity"]
    review = {
        "target_selection": block,
        "reviewed_proposal": {
            "proposal_hash": ident["proposal_hash"],
            "eligible_market_date": ident["eligible_market_date"],
            "active_book_id": ident["active_book_id"],
            "hoc_assessment_hash": ident["hoc_assessment_hash"],
            "portfolio_state_hash": ident["portfolio_state_hash"],
            "universe_scoring_hash": ident["universe_scoring_hash"]},
        "review_verdict": {"verdict": "MINIMAL_REPAIR_PREFERRED"},
    }
    return {
        "status": "OK", "proposal_id": artifact["proposal_id"],
        "proposal_hash": ident["proposal_hash"], "review_hash": "RH_R691",
        "eligible_market_date": SESSION, "proposal_read_state": "READY",
        "review": review, "target_selection": block,
        "governance": {"target_selection": block, "actionable": True},
        "inputs": {"hoc_assessment_hash_used": ident["hoc_assessment_hash"]},
    }


SUMMARY = {"reallocation_action_counts": {"EXIT": 1, "ADD": 1, "INCREASE": 1},
           "reallocation_one_way_turnover": 0.21}


def _select(target, ddir, artifact, **over):
    kw = dict(target=target, confirm=pdec.SELECTION_CONFIRM_TOKEN,
              review_envelope=_envelope(artifact), decision_dir=ddir,
              latest_session=SESSION)
    kw.update(over)
    return pdec.record_target_selection(**kw)


def _approve(ddir, artifact, **over):
    kw = dict(decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
              expected_proposal_hash=PHASH, artifact=artifact,
              proposal_summary=SUMMARY, decision_dir=ddir, latest_session=SESSION,
              actor="test-operator")
    kw.update(over)
    return pdec.record_decision(**kw)


# =========================================================================== #
# STAGE 1-3: review -> select -> read back
# =========================================================================== #
def test_01_review_offers_three_targets_and_preselects_none(tmp_path):
    _, _, art, _, _ = _world(tmp_path)
    block = _envelope(art)["target_selection"]
    assert [o["target"] for o in block["options"]] == [
        "CURRENT", "MINIMUM_REPAIR", "FULL_TARGET"]
    assert block["recommended_target"] == "MINIMUM_REPAIR"
    assert block["selection_is_approval"] is False


def test_02_selecting_minimum_repair_is_recorded(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    out = _select("MINIMUM_REPAIR", ddir, art)
    assert out["status"] == pdec.TS_CREATED
    assert out["recorded"] and out["selected"]
    assert out["record"]["selected_target"] == "MINIMUM_REPAIR"
    # a selection is never an approval and never an order
    for flag in ("is_an_approval", "creates_order_plan", "creates_orders",
                 "creates_fills", "executes", "deploys_capital"):
        assert out[flag] is False, flag


def test_03_the_selection_is_read_back_from_the_governed_ledger(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    _select("MINIMUM_REPAIR", ddir, art)
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION,
                                      decision_dir=ddir)
    assert back is not None
    assert back["selected_target"] == "MINIMUM_REPAIR"
    assert back["binding"]["proposal_hash"] == PHASH
    assert back["binding"]["review_hash"] == "RH_R691"
    assert back["selected_target_economics"]["positions"] == 1
    # the economics the operator SAW are frozen with the choice
    assert back["selected_target_economics"]["one_way_turnover"] == 0.095


def test_04_the_selection_survives_a_fresh_process_read(tmp_path):
    """'Survives refresh and navigation' at the durable layer: a second reader
    that shares nothing but the store sees the same record."""
    _, _, art, ddir, _ = _world(tmp_path)
    first = _select("MINIMUM_REPAIR", ddir, art)["record"]
    rows = json.loads((ddir / "target_selections.json").read_text("utf-8"))
    index = json.loads((ddir / "target_selection_index.json").read_text("utf-8"))
    assert rows[0]["selection_id"] == first["selection_id"]
    assert index["%s|%s" % (BOOK, SESSION)]["selected_target"] == "MINIMUM_REPAIR"


def test_05_selecting_again_is_idempotent_and_writes_no_duplicate(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    a = _select("MINIMUM_REPAIR", ddir, art)
    b = _select("MINIMUM_REPAIR", ddir, art)
    assert (a["status"], b["status"]) == (pdec.TS_CREATED, pdec.TS_REUSED)
    assert len(json.loads((ddir / "target_selections.json").read_text("utf-8"))) == 1


def test_06_a_changed_selection_preserves_history(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    _select("MINIMUM_REPAIR", ddir, art)
    rev = _select("FULL_TARGET", ddir, art)
    assert rev["status"] == pdec.TS_REVISED
    rows = json.loads((ddir / "target_selections.json").read_text("utf-8"))
    assert len(rows) == 2
    assert rows[0]["selected_target"] == "MINIMUM_REPAIR"      # never overwritten
    assert rows[1]["supersedes_selection_id"] == rows[0]["selection_id"]


# =========================================================================== #
# STAGE 4: approve EXACTLY the selected target
# =========================================================================== #
def test_10_approval_consumes_exactly_the_governed_selection(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    out = _approve(ddir, art)
    assert out["recorded"] is True
    assert out["binding"]["proposal_hash"] == PHASH
    assert out["created_orders"] is False and out["created_fills"] is False
    assert out["changed_holdings"] is False and out["changed_nav"] is False
    # the durable record is an APPROVE bound to this exact proposal
    back = pdec.load_decision_record(active_book_id=BOOK,
                                     eligible_market_date=SESSION, decision_dir=ddir)
    assert back["decision"] == pdec.DECISION_APPROVE
    assert back["proposal_hash"] == PHASH


def test_11_approval_refuses_when_the_selection_binds_another_proposal(tmp_path):
    """The operator may only approve the proposal they selected against."""
    _, _, art, ddir, _ = _world(tmp_path)
    stale = json.loads(json.dumps(art))
    stale["identity"]["proposal_hash"] = "HASH_OTHER"
    _select("FULL_TARGET", ddir, stale)          # selection bound to HASH_OTHER
    out = _approve(ddir, art)                    # approving HASH_R691
    assert out["recorded"] is False
    assert out["status"] == pdec.PDS_STALE
    assert any(m[0] == "proposal_hash" for m in out["mismatches"])


def test_12_approval_refuses_a_current_selection_as_a_no_change(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    _select("CURRENT", ddir, art)
    out = _approve(ddir, art)
    assert out["recorded"] is False
    assert out["status"] == pdec.PDS_SELECTION_IS_NO_CHANGE


def test_13_a_stale_session_refuses_selection_and_approval_alike(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    sel = _select("FULL_TARGET", ddir, art, latest_session="2026-01-12")
    assert sel["status"] == pdec.TS_SESSION_STALE
    assert sel["next_required_action"] == "RUN_PORTFOLIO_CYCLE"
    assert not (ddir / "target_selections.json").exists()
    app = _approve(ddir, art, latest_session="2026-01-12")
    assert app["recorded"] is False
    assert app["status"] == pdec.PDS_SESSION_STALE


# =========================================================================== #
# STAGE 5: the order plan must implement the SELECTED target
# =========================================================================== #
def test_20_minimum_repair_without_a_frozen_book_is_refused_before_approval(tmp_path):
    """R69.1 found it; R69.2 moved the refusal EARLIER.

    Selecting the minimum repair and approving it used to produce the FULL
    TARGET's order plan. R69.1 refused at the order plan - three steps after the
    operator had been told their choice was approved.

    The envelope this suite builds carries no ``selected_targets``, which is
    exactly the shape of every selection recorded before R69.2: the target's
    economics were frozen, its weights were not. Such a selection can never
    become an order plan, so the APPROVAL itself is now refused and says why.
    The lifecycle for a selection that DOES freeze its book is proved in
    ``test_r69_2_selected_target_lifecycle.py``.
    """
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)
    assert sel["selected"] is True, "the choice is still recordable"
    assert sel["implementable"] is False
    assert sel["implementation_available"] is False

    dec = _approve(ddir, art)
    assert dec["recorded"] is False
    assert dec["status"] == pdec.PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE
    assert dec["selected_target"] == "MINIMUM_REPAIR"
    assert dec["next_required_action"] == "SELECT_AN_IMPLEMENTABLE_TARGET"

    # and with no approval there is no order plan at any point downstream
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_PROPOSAL_REVIEW_REQUIRED
    assert st.get("order_plan") in (None, {}), "no plan may be offered"
    assert rb.RB_SELECTED_TARGET_NOT_IMPLEMENTABLE in rb.NON_CONFIRMABLE_STATES


def test_21_the_confirm_gate_creates_nothing_for_an_unimplementable_target(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    _select("MINIMUM_REPAIR", ddir, art)
    _approve(ddir, art)
    before = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                          today=SESSION, **kwargs)
    assert res["performed_write"] is False
    assert res["created_orders"] is False and res["created_fills"] is False
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == before


def test_22_the_full_target_selection_reaches_a_confirmable_plan(tmp_path):
    _, _, art, ddir, kwargs = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    _approve(ddir, art)
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    plan = st["order_plan"]
    assert plan and plan["orders"]
    assert {o["ticker"] for o in plan["orders"]} == {"AAA", "BBB", "CCC"}


def test_23_a_new_approval_without_a_selection_is_refused(tmp_path):
    """R69.2 - the silent fallback is gone.

    R69.1's gate engaged only when a selection EXISTED, so an APPROVE posted
    without one skipped it entirely and the order-plan owner built the standing
    full target. A missing choice became a choice. It is refused now, by name,
    and nothing is written.
    """
    _, _, art, ddir, kwargs = _world(tmp_path)
    dec = _approve(ddir, art)
    assert dec["recorded"] is False
    assert dec["status"] == pdec.PDS_TARGET_SELECTION_REQUIRED
    assert dec["next_required_action"] == "SELECT_TARGET"
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_PROPOSAL_REVIEW_REQUIRED
    assert st.get("order_plan") in (None, {})


def test_23b_a_legacy_approval_without_a_selection_still_plans_the_full_target(tmp_path):
    """R69.2 - historical compatibility, and it is never silent.

    A decision recorded BEFORE this release carries no ``selected_target``. Its
    order plan is still the artifact's own full target, exactly as it was - but
    the read model, the plan and the persisted plan artifact all say WHICH
    fallback produced it.
    """
    _, _, art, ddir, kwargs = _world(tmp_path)
    # A pre-R69.2 decision record: approved, and naming no target at all.
    legacy = {"record_id": "pdec_legacy", "decision": pdec.DECISION_APPROVE,
              "proposal_hash": PHASH, "recorded_at": "2026-01-09T12:00:00+00:00",
              "binding": {"active_book_id": BOOK, "eligible_market_date": SESSION,
                          "proposal_hash": PHASH}}
    st = rb.load_rebalance_state(**{**kwargs, "decision_record": legacy})
    assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    assert st["target_source"] == rb.TARGET_SOURCE_LEGACY_NO_SELECTION
    assert st["legacy_default_applied"] is True
    assert st["implemented_target"] == "FULL_TARGET"
    assert st["order_plan"]["target_source"] == rb.TARGET_SOURCE_LEGACY_NO_SELECTION
    assert {o["ticker"] for o in st["order_plan"]["orders"]} == {"AAA", "BBB", "CCC"}


# =========================================================================== #
# STAGE 6-8: separate confirmation -> next close -> reconciliation
# =========================================================================== #
def test_30_the_order_plan_needs_its_own_second_confirmation(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    _approve(ddir, art)
    # the approval alone created nothing
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == 0
    # and a wrong token creates nothing either
    bad = rb.confirm_rebalance_order_plan(confirm="CONFIRM_PORTFOLIO_TARGET_SELECTION",
                                          today=SESSION, **kwargs)
    assert bad["performed_write"] is False
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == 0


def test_31_the_whole_chain_reaches_a_reconciled_portfolio(tmp_path):
    sdir, book, art, ddir, kwargs = _world(tmp_path)

    # 1-3  select, read back
    assert _select("FULL_TARGET", ddir, art)["status"] == pdec.TS_CREATED
    sel = pdec.load_target_selection(active_book_id=BOOK,
                                     eligible_market_date=SESSION, decision_dir=ddir)
    assert sel["selected_target"] == "FULL_TARGET"

    # 4  approve exactly that
    assert _approve(ddir, art)["recorded"] is True

    # 5  order plan
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    plan = st["order_plan"]

    # 6  separate manual confirmation
    res = rb.confirm_rebalance_order_plan(
        confirm=rb.CONFIRM_TOKEN, expected_order_plan_hash=plan["order_plan_hash"],
        today=SESSION, **kwargs)
    assert res["created_orders"] is True
    assert res["created_fills"] is False, "confirmation must not fill"
    assert rb.load_rebalance_state(**kwargs)["rebalance_state"] == rb.RB_PLAN_CONFIRMED

    # 7  the NEXT eligible close, never the session the plan was built on
    _marks(sdir, {
        "AAA": [[SESSION, 110.0], ["2026-01-12", 110.0]],
        "BBB": [[SESSION, 190.0], ["2026-01-12", 190.0]],
        "CCC": [[SESSION, 52.0], ["2026-01-12", 52.0]],
        "SPY": [[SESSION, 405.0], ["2026-01-12", 410.0]],
    }, "2026-01-12")
    desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")

    # 8  reconciliation
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_EXECUTED
    fills = desk._fills(sdir)
    cash, holdings = desk.book_cash_holdings(book, fills)
    assert "BBB" not in holdings                       # exited
    po = {o["ticker"]: o for o in plan["orders"]}
    assert holdings["AAA"] == 40 + po["AAA"]["quantity"]
    assert holdings["CCC"] == po["CCC"]["quantity"]
    nav = desk.book_nav(book, fills, desk.read_marks(sdir))["nav"]
    manual = cash + holdings["AAA"] * 110.0 + holdings["CCC"] * 52.0
    assert nav == pytest.approx(round(manual, 2), abs=0.01)
    assert cash == pytest.approx(
        book["initial_capital"] + sum(f["net_cash_delta"] for f in fills), abs=0.01)
    assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True
    assert desk.verify_ledger(sdir, desk.FILLS_FILE)["intact"] is True


def test_32_confirming_the_same_plan_twice_creates_no_duplicate_orders(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    _approve(ddir, art)
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION, **kwargs)
    n = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION, **kwargs)
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n


def test_33_no_broker_no_automation_anywhere_on_the_path(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    _approve(ddir, art)
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                          today=SESSION, **kwargs)
    for flag in ("broker_enabled", "live_orders_enabled",
                 "automatic_approval_allowed", "automatic_rebalance_allowed",
                 "promoted_model", "recalibrated_model", "changed_cadence"):
        assert res[flag] is False, flag
    assert res["wrote_to_desk_ledgers_only"] is True


# =========================================================================== #
# Interruption, restart and session transition
# =========================================================================== #
def test_40_a_session_transition_preserves_history_and_invalidates_action(tmp_path):
    """P0-C: a transition must keep the record, refuse new action, and say what
    to do next."""
    _, _, art, ddir, kwargs = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)

    # the session rolls forward
    rolled = dict(kwargs, latest_session="2026-01-12")
    app = _approve(ddir, art, latest_session="2026-01-12")
    assert app["recorded"] is False
    assert app["status"] == pdec.PDS_SESSION_STALE
    assert app["next_required_action"] == "RUN_PORTFOLIO_CYCLE"

    # the historical selection is untouched and still readable
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION, decision_dir=ddir)
    assert back["selected_target"] == "FULL_TARGET"

    # and no order plan is confirmable in the new session
    st = rb.load_rebalance_state(**rolled)
    assert st["rebalance_state"] in rb.NON_CONFIRMABLE_STATES


def test_41_an_interrupted_selection_leaves_no_partial_record(tmp_path):
    """A refusal at any gate writes nothing at all - there is no half-selection
    for a later read to resurrect."""
    _, _, art, ddir, _ = _world(tmp_path)
    for kw in (dict(confirm="WRONG_TOKEN"),
               dict(target="NOT_A_TARGET"),
               dict(latest_session="2026-01-12")):
        out = _select(kw.pop("target", "FULL_TARGET"), ddir, art, **kw)
        assert out["recorded"] is False, out["status"]
    assert not (ddir / "target_selections.json").exists()
    assert not (ddir / "target_selection_index.json").exists()


def test_42_restart_safety_the_store_is_the_only_state(tmp_path):
    """Nothing in this path depends on in-process memory: a selection made
    before a restart is the selection the approval gate consumes after one."""
    _, _, art, ddir, _ = _world(tmp_path)
    _select("FULL_TARGET", ddir, art)
    # simulate a restart: drop every module-level cache the read path holds
    from paper_trader.api import proposal_decision_review as pdr
    pdr.reset_cache()
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION, decision_dir=ddir)
    assert back and back["selected_target"] == "FULL_TARGET"
    assert _approve(ddir, art)["recorded"] is True
