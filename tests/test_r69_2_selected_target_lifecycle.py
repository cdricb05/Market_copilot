r"""R69.2 - the MINIMUM REPAIR, end to end, in a hermetic world.

    review -> select MINIMUM_REPAIR -> persist -> reload from the store ->
    approve exactly that selection -> the MINIMUM REPAIR's order plan (not the
    full target's) -> separate manual confirmation -> next-close paper
    settlement -> reconciled holdings, cash, NAV and ledgers

R69.1 proved this path was broken and failed it closed: the repaired book's
weights lived only inside the review PROJECTION, were recomputed on every read
and were persisted nowhere, so an approved MINIMUM_REPAIR produced the FULL
TARGET's orders. R69.2 gave those weights an owner
(``engine.selected_target``), froze them into the governed selection, and made
the approval and order-plan paths consume that same identity.

Every test builds its own desk, proposal, opportunity-cost assessment, return
panel, decision root and plan root and injects them. The review is produced by
the REAL kernel and the REAL composition owner - nothing about the target is
hand-written here, because a hand-written block would prove only that the test
can spell JSON. No live endpoint, ledger, holding, cash or NAV is touched, and
no test here runs against the operational book.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as apdr
from paper_trader.api import rebalance_execution as rb
from paper_trader.engine import proposal_decision_review as kernel
from paper_trader.engine import selected_target as stgt

BOOK = "alpha_paper_book_1"
SESSION = "2026-01-10"
NEXT_SESSION = "2026-01-12"
PHASH = "HASH_R692"
COST = desk.COST_RATE_PER_SIDE

#: The hermetic book. AAA and DDD are retained; BBB is the holding the governed
#: retention rule no longer admits, so the minimum repair exits it and nothing
#: else. The full target ALSO exits BBB, but then increases AAA and buys CCC -
#: which is exactly why the two plans must be distinguishable.
HELD = {"AAA": (75, 100.0), "BBB": (40, 200.0), "DDD": (55, 150.0)}
MARKS = {"AAA": 110.0, "BBB": 190.0, "CCC": 52.0, "DDD": 150.0, "SPY": 405.0}

#: The covariance policy the proposal was built with. The review reads a
#: proposal's OWN persisted policy, so a hermetic proposal has to carry one -
#: these are the canonical owner's declared defaults, not values chosen here.
POLICY = {"covariance_lookback": 60, "min_covariance_obs": 40,
          "covariance_variance_floor": 1.0e-12,
          "risk_contribution_excess_multiple": 3.0,
          "min_volatility_coverage": 0.8}


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


def _series(latest=SESSION, marks=None):
    m = marks or MARKS
    return {tk: [["2026-01-05", px * 0.98], [latest, px]] for tk, px in m.items()}


def _returns(n=70):
    """A deterministic, owned-shaped return panel.

    Four series with different amplitudes and phases, generated from a closed
    form, so the covariance kernel measures something stable and the test never
    depends on an RNG seed or a live price store. ``n`` clears the canonical
    ``min_covariance_obs`` of 40; below it the owner reports UNAVAILABLE and the
    repair is correctly refused as unverified.
    """
    dates = ["2025-%02d-%02d" % (10 + (i // 28), 1 + (i % 28)) for i in range(n)]
    amp = {"AAA": 0.012, "BBB": 0.030, "CCC": 0.008, "DDD": 0.006}
    # Distinct, incommensurate FREQUENCIES - not phase shifts of one wave. Two
    # series that differ only by a phase are almost perfectly (anti)correlated,
    # the portfolio variance collapses toward zero and the per-name risk shares
    # blow past 1.0. The covariance kernel is right to reduce such a book; it is
    # simply not the book this suite means to describe.
    freq = {"AAA": 0.37, "BBB": 0.71, "CCC": 1.13, "DDD": 0.23}
    phase = {"AAA": 0.0, "BBB": 1.1, "CCC": 2.2, "DDD": 3.3}
    return {"dates": dates,
            "series": {tk: [amp[tk] * math.sin(i * freq[tk] + phase[tk])
                            for i in range(n)]
                       for tk in amp}}


def _alloc(tk, action, cw, pw, sector, nav=100000.0, **kw):
    row = {"ticker": tk, "action": action, "sector": sector,
           "asset_class": "US_EQUITY", "currency": "USD", "multiplier": 1.0,
           "instrument_type": "CASH_EQUITY", "execution_convention": "NEXT_CLOSE",
           "initial_margin_per_unit": 0.0, "unit_notional_usd": None,
           "cost_bps_per_side": None, "sleeve_id": "hermetic_v1",
           "held": cw > 0.0, "rank": 1, "score": 0.9, "combined_score": 0.9,
           "score_basis": "TEST", "capital_usage_ratio": 1.0,
           "reason_codes": [], "replacement_relationship": None,
           "source_hoc_recommendation": None,
           "current_weight": cw, "proposed_weight": pw,
           "delta_weight": round(pw - cw, 8),
           "current_market_value": round(cw * nav, 2),
           "proposed_market_value": round(pw * nav, 2),
           "capital_change": round((pw - cw) * nav, 2)}
    row.update(kw)
    return row


def _proposal(nav):
    cur = {tk: round(q * MARKS[tk] / nav, 8) for tk, (q, _) in HELD.items()}
    tgt = {"AAA": 0.095, "CCC": 0.06, "DDD": cur["DDD"]}
    allocs = [
        _alloc("AAA", "INCREASE", cur["AAA"], tgt["AAA"], "Tech", nav),
        _alloc("BBB", "EXIT", cur["BBB"], 0.0, "Energy", nav,
               source_hoc_recommendation="EXIT", reason_codes=["HOC_EXIT"]),
        _alloc("CCC", "ADD", 0.0, tgt["CCC"], "Health", nav),
        _alloc("DDD", "RETAIN", cur["DDD"], tgt["DDD"], "Staples", nav),
    ]
    one_way = 0.5 * sum(abs((tgt.get(tk) or 0.0) - (cur.get(tk) or 0.0))
                        for tk in set(cur) | set(tgt))
    return {
        "proposal_hash": PHASH, "proposal_state": "READY",
        "schema_version": "reallocation_proposal.v1",
        "policy_version": "reallocation_allocation_policy.v1",
        "policy": dict(POLICY),
        "allocations": allocs,
        "portfolio": {
            "nav": nav,
            "current_cash_weight": round(1.0 - sum(cur.values()), 8),
            "proposed_cash_weight": round(1.0 - sum(tgt.values()), 8),
            "current_holding_count": len(cur), "proposed_holding_count": len(tgt),
            "current_allocation_by_asset_class": {"US_EQUITY": sum(cur.values())},
            "proposed_allocation_by_asset_class": {"US_EQUITY": sum(tgt.values())},
            "current_allocation_by_sleeve": {"hermetic_v1": sum(cur.values())},
            "proposed_allocation_by_sleeve": {"hermetic_v1": sum(tgt.values())}},
        "turnover": {"one_way_turnover": round(one_way, 6),
                     "two_way_turnover": round(2 * one_way, 6),
                     "estimated_transaction_cost": round(2 * one_way * nav * COST, 2)},
        "switching_economics": {"score_before": 0.80, "score_after": 0.90,
                                "score_improvement": 0.10, "score_cost_hurdle": 0.01,
                                "score_improvement_net_of_cost": 0.09,
                                "switching_hurdle": 0.02,
                                "clears_switching_hurdle": True},
        "risk": {"portfolio_volatility_before": 0.15, "volatility_before_state": "AVAILABLE",
                 "portfolio_volatility_after": 0.13, "volatility_after_state": "AVAILABLE",
                 "concentration_before": 0.013, "concentration_after": 0.010,
                 "largest_position_before": cur["BBB"], "largest_position_after": 0.08,
                 "sector_concentration_before": cur["BBB"],
                 "sector_concentration_after": 0.08,
                 "risk_contributions_before": {}, "risk_contributions_after": {}},
        "risk_contribution_policy": {"limit_held_book": {}, "held_book_breaches": [],
                                     "limit_after_target": {},
                                     "after_target_breaches": []},
        "constraints": {"all_ok": True, "violations": []},
        "complete_target_limits": {"all_ok": True, "breaches": [],
                                   "owner": "engine.reallocation_proposal"},
        "constraint_reoptimization": {"applied": False, "constraints_that_reshaped": [],
                                      "breached_limits": []},
        "mandatory_repair": {"owner": "engine.holding_opportunity_cost",
                             "contract_version": "mandatory_repair_contract.v1",
                             "obligations_resolved": True,
                             "obligations_open_against_target": [],
                             "obligation_count": 1},
        "full_target_reviewable": True,
        "action_counts": {"EXIT": 1, "ADD": 1, "INCREASE": 1, "RETAIN": 1},
        "outcome": "TARGET_READY",
    }


def _hoc():
    """BBB is BROKEN - it fell beyond the governed exit buffer. That one verdict
    is what makes the minimum repair a real, non-empty book."""
    return {"assessment_hash": "HOC_R692",
            "holding_reviews": [
                {"ticker": "AAA", "deterioration_state": "INTACT", "reason_codes": []},
                {"ticker": "BBB", "deterioration_state": "BROKEN",
                 "reason_codes": ["FELL_BELOW_EXIT_BUFFER"],
                 "deterioration_reason_codes": ["FELL_BELOW_EXIT_BUFFER"],
                 "recommendation": "EXIT", "current_rank": 91},
                {"ticker": "DDD", "deterioration_state": "INTACT", "reason_codes": []}]}


def _world(tmp: Path, *, phash=PHASH):
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": BOOK, "book_number": 1, "display_name": "Paper Book #1",
            "initial_capital": 100000.0, "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "model_id": "hermetic_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, [
        _buy_fill(tk, q, px, "2026-01-05", "f_%s" % tk) for tk, (q, px) in HELD.items()])
    _marks(sdir, _series(), SESSION)
    nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))["nav"]

    prop = _proposal(nav)
    prop["proposal_hash"] = phash
    artifact = {
        "proposal_id": "reap_%s_%s_%s" % (SESSION, BOOK, phash[:6]),
        "schema_version": "reallocation_proposal.v1",
        "generated_at": "2026-01-10T21:00:00+00:00",
        # No ``corporate_actions_hash``: this world registers no corporate action,
        # and a hash here would be compared against the LIVE registry fingerprint
        # and read as stale. The hermetic world states what it has, not more.
        "identity": {"active_book_id": BOOK, "eligible_market_date": SESSION,
                     "proposal_hash": phash, "portfolio_state_hash": "PSH",
                     "hoc_assessment_hash": "HOC_R692", "universe_scoring_hash": "USH",
                     "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "proposal": prop,
    }
    ddir = tmp / "decisions"
    ddir.mkdir(parents=True, exist_ok=True)
    kwargs = dict(desk_dir=sdir, active_book_id=BOOK, eligible_market_date=SESSION,
                  artifact=artifact, plan_dir=tmp / "plans", actions_dir=tmp / "ca",
                  decision_dir=ddir, latest_session=SESSION)
    return sdir, book, artifact, ddir, kwargs


def _envelope(artifact, *, latest_session=SESSION, decision_dir=None):
    """The REAL review envelope, from the REAL composition owner.

    Nothing about the three targets is written by this test: the weights come
    from ``engine.proposal_decision_review`` and the implementable blocks from
    ``engine.selected_target``, exactly as the live endpoint serves them.
    """
    ident = artifact["identity"]
    payload = {
        "state": "READY", "proposal_state": "READY", "approvable": True,
        "eligible_market_date": SESSION,
        "active_book": {"book_id": BOOK, "id": BOOK},
        "artifact": {"proposal_id": artifact["proposal_id"], "identity": ident,
                     "generated_at": artifact["generated_at"]},
    }
    return apdr.load_proposal_decision_review(
        proposal_payload=payload, proposal=artifact["proposal"],
        hoc_assessment=_hoc(), outcome_evidence={}, aligned_returns=_returns(),
        decision_dir=(str(decision_dir) if decision_dir is not None else None),
        latest_session=latest_session)


def _select(target, ddir, artifact, **over):
    kw = dict(target=target, confirm=pdec.SELECTION_CONFIRM_TOKEN,
              review_envelope=_envelope(artifact, decision_dir=ddir),
              decision_dir=ddir, latest_session=SESSION)
    kw.update(over)
    return pdec.record_target_selection(**kw)


SUMMARY = {"reallocation_action_counts": {"EXIT": 1, "ADD": 1, "INCREASE": 1},
           "reallocation_one_way_turnover": 0.12}


def _approve(ddir, artifact, **over):
    kw = dict(decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
              expected_proposal_hash=artifact["identity"]["proposal_hash"],
              artifact=artifact, proposal_summary=SUMMARY, decision_dir=ddir,
              latest_session=SESSION, actor="test-operator")
    kw.update(over)
    return pdec.record_decision(**kw)


# =========================================================================== #
# 1. THE OWNER: weights are READ, never computed
# =========================================================================== #
def test_01_the_review_publishes_an_implementable_block_per_target(tmp_path):
    _, _, art, _, _ = _world(tmp_path)
    env = _envelope(art)
    assert env["status"] == "OK"
    blocks = env["selected_targets"]
    assert sorted(blocks) == ["CURRENT", "FULL_TARGET", "MINIMUM_REPAIR"]
    assert env["implementable_targets"] == ["FULL_TARGET", "MINIMUM_REPAIR"]
    assert env["selected_target_owner"] == "engine.selected_target"

    cur = blocks["CURRENT"]
    assert cur["implementable"] is False
    assert cur["not_implementable_reason"] == stgt.NOT_IMPLEMENTABLE_NO_CHANGE

    rep = blocks["MINIMUM_REPAIR"]
    assert rep["implementable"] is True
    assert rep["weights_read_verbatim"] is True
    assert rep["is_an_optimiser"] is False and rep["computes_weights"] is False
    # the repair exits BBB and touches nothing else
    assert sorted(rep["weights"]) == ["AAA", "DDD"]
    assert rep["trading_actions"] == ["EXIT"]


def test_02_every_repair_weight_is_the_review_state_verbatim(tmp_path):
    """The kernel must READ the weights. Not round them, not renormalise them,
    not cap them - read them."""
    _, _, art, _, _ = _world(tmp_path)
    env = _envelope(art)
    review = env["review"]
    for target in ("CURRENT", "MINIMUM_REPAIR", "FULL_TARGET"):
        state_w = {k: v for k, v in
                   (review["states"][target].get("weights") or {}).items() if v > 0}
        block_w = env["selected_targets"][target]["weights"]
        assert block_w == pytest.approx(state_w), target


def test_03_the_full_target_projection_reproduces_the_artifact(tmp_path):
    """The projection is proved equivalent to the artifact rather than replacing
    it. This is what makes it trustworthy for the minimum repair, where there is
    no artifact list to compare against."""
    _, _, art, _, _ = _world(tmp_path)
    prop = art["proposal"]
    policy = kernel.review_policy(prop)
    projected = stgt.projected_full_target_allocations(proposal=prop, policy=policy)
    a_w = {a["ticker"]: round(float(a["proposed_weight"]), 8)
           for a in prop["allocations"]}
    p_w = {r["ticker"]: r["proposed_weight"] for r in projected}
    assert p_w == a_w
    a_act = {a["ticker"]: a["action"] for a in prop["allocations"]}
    p_act = {r["ticker"]: r["action"] for r in projected}
    assert p_act == a_act
    # and the block for FULL_TARGET still carries the ARTIFACT's own rows
    env = _envelope(art)
    full = env["selected_targets"]["FULL_TARGET"]
    assert full["allocation_source"] == stgt.SOURCE_ARTIFACT_VERBATIM
    assert full["allocations"] == prop["allocations"]


def test_04_the_block_is_byte_stable_for_the_same_inputs(tmp_path):
    _, _, art, _, _ = _world(tmp_path)
    a = _envelope(art)["selected_targets"]["MINIMUM_REPAIR"]
    b = _envelope(art)["selected_targets"]["MINIMUM_REPAIR"]
    assert a["selected_target_implementation_hash"] == \
        b["selected_target_implementation_hash"]
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_05_publishing_the_block_does_not_move_the_review_hash(tmp_path):
    """``selected_targets`` lives on the ENVELOPE, not inside ``review``. A
    selection binds ``review_hash``; folding a new block into it would change the
    identity of every review ever made without one input moving."""
    _, _, art, _, _ = _world(tmp_path)
    env = _envelope(art)
    assert "selected_targets" not in env["review"]
    assert env["review_hash"] == apdr.review_hash(env["review"])


# =========================================================================== #
# 2. SELECT -> PERSIST -> RELOAD
# =========================================================================== #
def test_10_selecting_the_repair_freezes_the_whole_book(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    res = _select("MINIMUM_REPAIR", ddir, art)
    assert res["status"] == pdec.TS_CREATED and res["selected"] is True
    assert res["implementable"] is True
    assert res["implementation_available"] is True
    assert res["position_count"] == 2
    rec = res["record"]
    impl = rec["selected_target_implementation"]
    assert impl["target"] == "MINIMUM_REPAIR"
    assert sorted(impl["weights"]) == ["AAA", "DDD"]
    assert rec["selected_target_implementation_hash"] == \
        impl["selected_target_implementation_hash"]
    # selecting is still not approving, and still creates nothing
    assert rec["is_an_approval"] is False and rec["creates_order_plan"] is False
    assert res["creates_orders"] is False and res["creates_fills"] is False


def test_11_the_frozen_book_survives_a_fresh_process_read(tmp_path):
    """Restart safety: the store is the only state. A second, independent read
    of the governed ledger returns the same weights and the same identity."""
    _, _, art, ddir, _ = _world(tmp_path)
    written = _select("MINIMUM_REPAIR", ddir, art)["record"]
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION, decision_dir=ddir)
    assert back["selection_id"] == written["selection_id"]
    assert back["selected_target_implementation"]["weights"] == \
        written["selected_target_implementation"]["weights"]
    assert back["selected_target_implementation_hash"] == \
        written["selected_target_implementation_hash"]
    # and it is on disk, not in memory
    rows = json.loads((ddir / "target_selections.json").read_text(encoding="utf-8"))
    assert rows[-1]["selected_target_implementation"]["weights"] == \
        written["selected_target_implementation"]["weights"]


def test_12_a_current_selection_freezes_a_book_that_is_not_implementable(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    res = _select("CURRENT", ddir, art)
    assert res["selected"] is True
    assert res["implementable"] is False
    assert res["not_implementable_reason"] == stgt.NOT_IMPLEMENTABLE_NO_CHANGE
    assert res["record"]["selected_target_implementation"]["position_count"] == 3


# =========================================================================== #
# 3. APPROVE EXACTLY THE SELECTION
# =========================================================================== #
def test_20_the_approval_binds_the_exact_selected_target(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    dec = _approve(ddir, art,
                   expected_selection_id=sel["selection_id"],
                   expected_selected_target="MINIMUM_REPAIR")
    assert dec["recorded"] is True and dec["status"] == "CREATED"
    rec = dec["record"]
    assert rec["selected_target"] == "MINIMUM_REPAIR"
    assert rec["selection_id"] == sel["selection_id"]
    assert rec["selected_target_implementation_hash"] == \
        sel["selected_target_implementation_hash"]
    assert rec["selected_target_implementable"] is True
    assert dec["created_orders"] is False and dec["created_fills"] is False


def test_21_approving_a_target_the_operator_was_not_shown_is_refused(tmp_path):
    """The browser says which target it was looking at. If the governed ledger
    holds another one, the selection was revised under the operator."""
    _, _, art, ddir, _ = _world(tmp_path)
    _select("MINIMUM_REPAIR", ddir, art)
    dec = _approve(ddir, art, expected_selected_target="FULL_TARGET")
    assert dec["recorded"] is False and dec["status"] == pdec.PDS_STALE
    assert any(m[0] == "selected_target" for m in dec["mismatches"])


def test_22_a_changed_proposal_hash_refuses_selection_and_approval_alike(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    _select("MINIMUM_REPAIR", ddir, art)
    moved = _select("MINIMUM_REPAIR", ddir, art,
                    expected_proposal_hash="A_DIFFERENT_PROPOSAL")
    assert moved["status"] == pdec.TS_STALE and moved["recorded"] is False
    # and the approval refuses a selection bound to another proposal
    _, _, art2, _, _ = _world(tmp_path / "second", phash="OTHER_HASH")
    dec = _approve(ddir, art2, expected_proposal_hash="OTHER_HASH")
    assert dec["recorded"] is False and dec["status"] == pdec.PDS_STALE


# =========================================================================== #
# 4. THE ORDER PLAN IMPLEMENTS THE SELECTED TARGET
# =========================================================================== #
def test_30_a_minimum_repair_approval_produces_the_minimum_repair_plan(tmp_path):
    """THE regression this release exists for.

    On the live 2026-09-22 book the difference was 14 names / 21.1% turnover
    against 20 names / 35.0%. Here it is one order against three.
    """
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"],
             expected_selected_target="MINIMUM_REPAIR")

    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    assert st["implemented_target"] == "MINIMUM_REPAIR"
    assert st["target_source"] == rb.TARGET_SOURCE_SELECTED_FROZEN
    assert st["legacy_default_applied"] is False
    assert st["target_selection_id"] == sel["selection_id"]
    assert st["selected_target_implementation_hash"] == \
        sel["selected_target_implementation_hash"]

    plan = st["order_plan"]
    # EXACTLY the repair: BBB leaves, nothing is bought.
    assert [(o["ticker"], o["side"]) for o in plan["orders"]] == [("BBB", desk.SIDE_SELL)]
    assert plan["n_buy"] == 0 and plan["n_sell"] == 1
    assert plan["implemented_target"] == "MINIMUM_REPAIR"
    assert plan["order_plan_buildable"] is True
    assert st["confirmation_available"] is True


def test_31_the_two_targets_produce_demonstrably_different_plans(tmp_path):
    a_dir = tmp_path / "repair"
    b_dir = tmp_path / "full"
    _, _, art_a, dd_a, kw_a = _world(a_dir)
    _, _, art_b, dd_b, kw_b = _world(b_dir)

    sel_a = _select("MINIMUM_REPAIR", dd_a, art_a)["record"]
    _approve(dd_a, art_a, expected_selection_id=sel_a["selection_id"])
    sel_b = _select("FULL_TARGET", dd_b, art_b)["record"]
    _approve(dd_b, art_b, expected_selection_id=sel_b["selection_id"])

    pa = rb.load_rebalance_state(**kw_a)["order_plan"]
    pb = rb.load_rebalance_state(**kw_b)["order_plan"]
    assert {o["ticker"] for o in pa["orders"]} == {"BBB"}
    assert {o["ticker"] for o in pb["orders"]} == {"AAA", "BBB", "CCC"}
    assert pa["order_plan_hash"] != pb["order_plan_hash"]
    assert pa["one_way_turnover"] < pb["one_way_turnover"]
    # each plan is measured against ITS OWN target's turnover, never the other's
    assert abs(pa["turnover_gap"]) <= pa["executability_envelope"]
    assert abs(pb["turnover_gap"]) <= pb["executability_envelope"]


def test_32_the_mark_universe_follows_the_selected_target(tmp_path):
    """A repair that exits names needs marks for a different universe than the
    full target. Hydrating the wrong one would block a plan that is fine."""
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    uni = rb.load_rebalance_state(**kwargs)["order_plan"]["target_mark_universe"]
    assert "CCC" not in uni["target_tickers"], "CCC belongs to the FULL target only"
    assert set(uni["target_tickers"]) == {"AAA", "DDD"}


def test_33_a_selection_revised_after_approval_blocks_the_plan(tmp_path):
    """An approval names a selection. If the ledger then holds another one,
    NEITHER target may be built under it."""
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    assert rb.load_rebalance_state(**kwargs)["rebalance_state"] == \
        rb.RB_PLAN_REVIEW_REQUIRED

    revised = _select("FULL_TARGET", ddir, art)
    assert revised["status"] == pdec.TS_REVISED

    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_SELECTION_SUPERSEDED
    assert st.get("order_plan") in (None, {})
    assert st["next_required_action"] == "RE_APPROVE_THE_CURRENT_SELECTION"
    assert rb.RB_SELECTION_SUPERSEDED in rb.NON_CONFIRMABLE_STATES


def test_34_the_confirm_gate_refuses_a_superseded_selection(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    _select("FULL_TARGET", ddir, art)
    before = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION,
                                          **kwargs)
    assert res["performed_write"] is False and res["created_orders"] is False
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == before


def test_35_a_current_selection_never_reaches_an_order_plan(tmp_path):
    _, _, art, ddir, kwargs = _world(tmp_path)
    _select("CURRENT", ddir, art)
    dec = _approve(ddir, art)
    assert dec["recorded"] is False
    assert dec["status"] == pdec.PDS_SELECTION_IS_NO_CHANGE
    st = rb.load_rebalance_state(**kwargs)
    assert st.get("order_plan") in (None, {})


# =========================================================================== #
# 5. CONFIRM -> NEXT CLOSE -> RECONCILE
# =========================================================================== #
def test_40_the_whole_repair_chain_reaches_a_reconciled_portfolio(tmp_path):
    sdir, book, art, ddir, kwargs = _world(tmp_path)

    # 1-3  select, persist, read back from the store
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION, decision_dir=ddir)
    assert back["selected_target"] == "MINIMUM_REPAIR"

    # 4  approve EXACTLY that selection
    dec = _approve(ddir, art, expected_selection_id=sel["selection_id"],
                   expected_selected_target="MINIMUM_REPAIR")
    assert dec["record"]["selected_target"] == "MINIMUM_REPAIR"

    # 5  the MINIMUM REPAIR's order plan
    st = rb.load_rebalance_state(**kwargs)
    plan = st["order_plan"]
    assert st["implemented_target"] == "MINIMUM_REPAIR"
    assert [o["ticker"] for o in plan["orders"]] == ["BBB"]

    # 6  a SEPARATE manual confirmation, with its own token
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == 0
    res = rb.confirm_rebalance_order_plan(
        confirm=rb.CONFIRM_TOKEN, expected_order_plan_hash=plan["order_plan_hash"],
        today=SESSION, **kwargs)
    assert res["created_orders"] is True and res["created_fills"] is False
    assert res["n_orders_created"] == 1
    assert rb.load_rebalance_state(**kwargs)["rebalance_state"] == rb.RB_PLAN_CONFIRMED

    # 7  the NEXT eligible close, never the session the plan was built on
    _marks(sdir, _series(latest=NEXT_SESSION), NEXT_SESSION)
    desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")

    # 8  reconciliation: holdings, cash, NAV and the ledgers
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_EXECUTED
    fills = desk._fills(sdir)
    cash, holdings = desk.book_cash_holdings(book, fills)
    assert "BBB" not in holdings, "the repair exits BBB"
    assert holdings["AAA"] == HELD["AAA"][0], "the repair touches nothing else"
    assert holdings["DDD"] == HELD["DDD"][0]
    assert "CCC" not in holdings, "CCC belongs to the target the operator did NOT pick"
    nav = desk.book_nav(book, fills, desk.read_marks(sdir))["nav"]
    manual = cash + holdings["AAA"] * MARKS["AAA"] + holdings["DDD"] * MARKS["DDD"]
    assert nav == pytest.approx(round(manual, 2), abs=0.01)
    assert cash == pytest.approx(
        book["initial_capital"] + sum(f["net_cash_delta"] for f in fills), abs=0.01)
    assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True
    assert desk.verify_ledger(sdir, desk.FILLS_FILE)["intact"] is True


def test_41_confirming_the_same_repair_plan_twice_creates_no_duplicates(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION, **kwargs)
    n = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    again = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION,
                                            **kwargs)
    assert again["status"] == rb.C_REUSED
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n


def test_42_the_executed_order_carries_the_selected_target_lineage(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION,
                                          **kwargs)
    frozen = json.loads(
        (tmp_path / "plans" / "order_plans.json").read_text(encoding="utf-8"))[-1]
    assert frozen["implemented_target"] == "MINIMUM_REPAIR"
    assert frozen["target_source"] == rb.TARGET_SOURCE_SELECTED_FROZEN
    assert frozen["target_selection_id"] == sel["selection_id"]
    assert frozen["selected_target_implementation_hash"] == \
        sel["selected_target_implementation_hash"]
    assert [o["ticker"] for o in frozen["orders"]] == ["BBB"]
    assert res["created_orders"] is True


def test_43_no_broker_no_automation_anywhere_on_the_repair_path(tmp_path):
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today=SESSION,
                                          **kwargs)
    for flag in ("broker_enabled", "live_orders_enabled",
                 "automatic_approval_allowed", "automatic_rebalance_allowed",
                 "promoted_model", "recalibrated_model", "changed_cadence"):
        assert res[flag] is False, flag
    assert res["wrote_to_desk_ledgers_only"] is True


# =========================================================================== #
# 6. Session rollover, partial marks, interruption
# =========================================================================== #
def test_50_a_session_rollover_preserves_the_frozen_book_and_stops_action(tmp_path):
    _, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]

    app = _approve(ddir, art, latest_session=NEXT_SESSION)
    assert app["recorded"] is False and app["status"] == pdec.PDS_SESSION_STALE

    rolled = dict(kwargs, latest_session=NEXT_SESSION)
    st = rb.load_rebalance_state(**rolled)
    assert st["order_plan_confirmation_allowed"] is False
    assert st["confirmation_available"] is False

    # the frozen book is untouched historical evidence and still fully readable
    back = pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION, decision_dir=ddir)
    assert back["selected_target_implementation"]["weights"] == \
        sel["selected_target_implementation"]["weights"]


def test_51_a_partial_mark_blocks_the_repair_plan_rather_than_omitting_a_name(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    _approve(ddir, art, expected_selection_id=sel["selection_id"])
    # BBB - the one name the repair must sell - loses its owned mark
    partial = _series()
    partial.pop("BBB")
    _marks(sdir, partial, SESSION)
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_BLOCKED_MARKS
    assert "BBB" in st["missing_marks"]
    assert st["order_plan_buildable"] is False
    assert st["confirmation_available"] is False


def test_52_an_interrupted_selection_leaves_no_partial_record(tmp_path):
    """A refused selection writes nothing at all - not a stub, not a marker."""
    _, _, art, ddir, _ = _world(tmp_path)
    bad = _select("MINIMUM_REPAIR", ddir, art, confirm="NOT_THE_TOKEN")
    assert bad["recorded"] is False
    assert not (ddir / "target_selections.json").exists()
    assert pdec.load_target_selection(active_book_id=BOOK,
                                      eligible_market_date=SESSION,
                                      decision_dir=ddir) is None


def test_53_selecting_the_repair_twice_is_idempotent(tmp_path):
    _, _, art, ddir, _ = _world(tmp_path)
    first = _select("MINIMUM_REPAIR", ddir, art)
    second = _select("MINIMUM_REPAIR", ddir, art)
    assert second["status"] == pdec.TS_REUSED
    assert second["record"]["selection_id"] == first["record"]["selection_id"]
    rows = json.loads((ddir / "target_selections.json").read_text(encoding="utf-8"))
    assert len(rows) == 1


def test_54_a_pre_r69_2_selection_is_revised_not_replayed(tmp_path):
    """A selection recorded before this release froze economics but no book, so
    no order plan could ever be built from it. Re-selecting the same target now
    that the book IS available is a governed revision - and history is kept."""
    _, _, art, ddir, _ = _world(tmp_path)
    legacy_env = _envelope(art, decision_dir=ddir)
    legacy_env.pop("selected_targets")
    old = pdec.record_target_selection(
        target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
        review_envelope=legacy_env, decision_dir=ddir, latest_session=SESSION)
    assert old["implementable"] is False
    assert old["record"]["selected_target_implementation"] is None

    new = _select("MINIMUM_REPAIR", ddir, art)
    assert new["status"] == pdec.TS_REVISED
    assert new["implementable"] is True
    assert new["record"]["supersedes_selection_id"] == old["record"]["selection_id"]
    rows = json.loads((ddir / "target_selections.json").read_text(encoding="utf-8"))
    assert len(rows) == 2, "both immutable records preserved"


# =========================================================================== #
# 7. The risk-contribution sight line (DISPLAY ONLY - no policy moved)
# =========================================================================== #
def test_60_the_block_publishes_both_limits_and_both_risk_bases(tmp_path):
    _, _, art, _, _ = _world(tmp_path)
    risk = _envelope(art)["selected_targets"]["MINIMUM_REPAIR"]["risk_contribution"]
    assert risk["policy_changed_by_this_release"] is False
    assert risk["thresholds_changed_by_this_release"] is False
    assert risk["measured_here"] is False
    assert risk["policy_owner"] == "engine.holding_opportunity_cost"
    for side in ("before", "after"):
        s = risk[side]
        for key in ("limit", "n_covariance_names", "contributions", "breaches",
                    "portfolio_volatility_invested_basis",
                    "portfolio_volatility_capital_basis",
                    "concentration", "largest_position", "cash_weight"):
            assert key in s, (side, key)


def test_61_a_breach_discharged_by_a_shrinking_universe_is_named(tmp_path):
    """The 2026-09-22 finding, in miniature: a name whose obligation closes while
    its weight does not move is reported by name, with both limits.

    Nothing here changes the cap. The policy question stays open for manual
    review; this test proves it is no longer INVISIBLE.
    """
    before_state = {
        "weights": {"AAA": 0.05, "BBB": 0.04, "CCC": 0.03},
        "risk_contributions": {"AAA": 0.1497, "BBB": 0.1387, "CCC": 0.05},
        "risk_contribution_limit": {"limit": 0.12, "n_covariance_names": 25,
                                    "basis": "TEST"},
        "risk_contribution_breaches": [{"ticker": "AAA"}, {"ticker": "BBB"}],
        "portfolio_volatility": 0.1196, "portfolio_volatility_capital_basis": 0.1142,
        "concentration": 0.0372, "cash_weight": 0.045,
    }
    after_state = {
        "weights": {"AAA": 0.05, "BBB": 0.04},
        "risk_contributions": {"AAA": 0.2073, "BBB": 0.2065},
        "risk_contribution_limit": {"limit": 0.21428571, "n_covariance_names": 14,
                                    "basis": "TEST"},
        "risk_contribution_breaches": [],
        "portfolio_volatility": 0.1680, "portfolio_volatility_capital_basis": 0.0895,
        "concentration": 0.0209, "cash_weight": 0.467,
    }
    cmp_ = stgt.risk_contribution_comparison(
        before_state=before_state, after_state=after_state,
        policy={"material_weight_delta": 1.0e-4})
    assert cmp_["limit_changed"] is True
    assert cmp_["covariance_universe_changed"] is True
    assert cmp_["breaches_closed"] == ["AAA", "BBB"]
    assert cmp_["discharged_without_reduction_count"] == 2
    names = {d["ticker"] for d in cmp_["discharged_without_reduction"]}
    assert names == {"AAA", "BBB"}
    for d in cmp_["discharged_without_reduction"]:
        assert d["weight_change"] == 0.0
        assert d["risk_contribution_after"] > d["risk_contribution_before"]
        assert d["code"] == stgt.DISCHARGE_WITHOUT_REDUCTION
    assert cmp_["two_largest_risk_shares_after"] == pytest.approx(0.4138, abs=1e-3)
    assert cmp_["volatility_invested_basis_rose"] is True
    assert cmp_["volatility_capital_basis_fell"] is True


def test_62_a_reduction_is_not_reported_as_a_free_discharge(tmp_path):
    """The counter-case: a breach repaired by actually reducing the position is
    NOT listed. Otherwise the warning would fire on every successful repair."""
    before_state = {
        "weights": {"AAA": 0.20}, "risk_contributions": {"AAA": 0.30},
        "risk_contribution_limit": {"limit": 0.12, "n_covariance_names": 25},
        "risk_contribution_breaches": [{"ticker": "AAA"}],
    }
    after_state = {
        "weights": {"AAA": 0.08}, "risk_contributions": {"AAA": 0.11},
        "risk_contribution_limit": {"limit": 0.12, "n_covariance_names": 25},
        "risk_contribution_breaches": [],
    }
    cmp_ = stgt.risk_contribution_comparison(
        before_state=before_state, after_state=after_state,
        policy={"material_weight_delta": 1.0e-4})
    assert cmp_["breaches_closed"] == ["AAA"]
    assert cmp_["discharged_without_reduction"] == []
    assert cmp_["limit_changed"] is False


# =========================================================================== #
# 8. Safety
# =========================================================================== #
def test_70_nothing_on_this_path_creates_an_order_before_its_own_gate(tmp_path):
    sdir, _, art, ddir, kwargs = _world(tmp_path)
    n0 = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    sel = _select("MINIMUM_REPAIR", ddir, art)
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n0
    _approve(ddir, art, expected_selection_id=sel["record"]["selection_id"])
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n0
    rb.load_rebalance_state(**kwargs)
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n0
    bad = rb.confirm_rebalance_order_plan(confirm="WRONG", today=SESSION, **kwargs)
    assert bad["performed_write"] is False
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n0


def test_71_the_kernel_declares_what_it_is_not(tmp_path):
    _, _, art, _, _ = _world(tmp_path)
    b = _envelope(art)["selected_targets"]["MINIMUM_REPAIR"]
    for flag in ("is_an_optimiser", "computes_weights", "recomputes_economics",
                 "second_proposal_engine", "second_risk_engine", "is_an_approval",
                 "creates_order_plan", "creates_orders", "deploys_capital",
                 "decided_by_llm"):
        assert b[flag] is False, flag
