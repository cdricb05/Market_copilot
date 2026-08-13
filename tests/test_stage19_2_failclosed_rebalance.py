r"""Stage 19.2 — FAIL-CLOSED complete rebalance execution.

Hermetic acceptance for the August-12 incident and the general invariant behind it:

    An APPROVED portfolio proposal must NEVER be converted into a materially incomplete
    paper rebalance. The executable order plan may differ from the approved target ONLY
    through the explicitly supported mechanics — whole shares, transaction cost, available
    cash, the concentration policy and the minimum-order policy. Anything else (most
    importantly a MISSING OWNED MARK) fails the plan CLOSED.

The incident reproduced here: eight ADD names of the approved 2026-08-12 proposal
(ABNB AIZ CVS DXCM EXPE ITW LH SPG) had no owned execution mark. All eight were correctly
classified NO_OWNED_MARK — and the implementation still returned ``order_plan_buildable =
True``, offering a 22-order plan that sold/exited/reduced the old book at 19.33% one-way
turnover against an approved 35.55% proposal, leaving ~36% residual cash.

Every test uses temp desk / proposal / decision / corporate-action / plan roots and an
injected offline downloader. No live endpoint, ledger, mark, holding, cash or NAV is
touched, and no provider is ever contacted.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import rebalance_execution as rb


# =========================================================================== #
# The August-12 fixture — 25 held names, an approved 25-name target, 8 additions
# =========================================================================== #
BOOK_ID = "alpha_paper_book_1"
ELIGIBLE = "2026-08-12"
PRIOR = "2026-08-11"
COST = desk.COST_RATE_PER_SIDE
INITIAL_CAPITAL = 100_000.0

#: The real August-12 book (25 names).
HELD = ["ALAB", "AMD", "ANET", "APD", "CAH", "CAT", "DDOG", "DVA", "DVN", "EOG", "EXPD",
        "FANG", "FTNT", "GWW", "HST", "KEYS", "LYV", "MNST", "MO", "NDSN", "RCL", "TECH",
        "TT", "VLO", "XYZ"]
#: The eight approved ADD names that had no owned mark — the incident.
ADDS = ["ABNB", "AIZ", "CVS", "DXCM", "EXPE", "ITW", "LH", "SPG"]
#: The eight approved EXIT names.
EXITS = ["APD", "CAH", "EXPD", "MO", "NDSN", "RCL", "TECH", "TT"]
RETAINED = [t for t in HELD if t not in EXITS]          # 17 names
TARGET = RETAINED + ADDS                                # 25 names
TARGET_WEIGHT = 0.039                                   # 25 x 3.9% = 97.5% invested

_ALL = sorted(set(HELD) | set(ADDS) | {desk.BENCHMARK_TICKER})
#: Deterministic, varied prices so whole-share rounding is genuinely exercised.
PRICE = {tk: round(41.0 + 7.3 * i, 2) for i, tk in enumerate(_ALL)}


def _bars(tk: str) -> list[list]:
    px = PRICE[tk]
    return [[PRIOR, round(px * 0.995, 4)], [ELIGIBLE, px]]


def _buy_fill(tk: str, qty: int, price: float, fid: str) -> dict:
    gross = qty * price
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": fid, "order_id": "ord_%s" % fid, "book_id": BOOK_ID, "ticker": tk,
        "side": desk.SIDE_BUY, "quantity": qty, "fill_date": "2026-07-01",
        "fill_price": price, "gross_value": round(gross, 2),
        "transaction_cost": round(cost, 4), "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": "NEXT_CLOSE", "immutable": True}}


def _seed_desk(tmp: Path, *, marked: list[str]) -> tuple[Path, dict]:
    """A 25-name book worth ~$100k. ``marked`` is the set of tickers the owned mark store
    can price — the incident is reproduced by leaving the eight additions OUT of it."""
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": BOOK_ID, "book_number": 1, "display_name": "Alpha Paper Book #1",
            "initial_capital": INITIAL_CAPITAL, "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "snapshot_market_date": "2026-07-01",
            "model_id": "fundamental_momentum_50_50_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    fills = []
    for i, tk in enumerate(HELD):
        qty = int(3_800.0 / PRICE[tk]) or 1
        fills.append(_buy_fill(tk, qty, PRICE[tk], "f_%02d_%s" % (i, tk)))
    desk._append_ledger(sdir, desk.FILLS_FILE, fills)
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger",
        "series": {tk: _bars(tk) for tk in marked},
        "latest_completed_date": ELIGIBLE, "updated_at": "2026-08-12T22:00:00+00:00"})
    return sdir, book


def _allocations(nav: float, holdings: dict) -> list[dict]:
    """The approved 2026-08-12 allocation rows: 17 retained resized to the target weight,
    8 additions at the target weight, 8 exits to zero."""
    rows = []
    cur_w = {tk: (int(holdings.get(tk, 0)) * PRICE[tk]) / nav for tk in HELD}
    for tk in RETAINED:
        cw = cur_w[tk]
        rows.append({"ticker": tk, "sector": "Sector%d" % (len(tk) % 5),
                     "action": "INCREASE" if TARGET_WEIGHT > cw else "REDUCE",
                     "current_weight": round(cw, 6), "proposed_weight": TARGET_WEIGHT,
                     "held": True})
    for tk in ADDS:
        rows.append({"ticker": tk, "sector": "Sector%d" % (len(tk) % 5), "action": "ADD",
                     "current_weight": 0.0, "proposed_weight": TARGET_WEIGHT, "held": False})
    for tk in EXITS:
        rows.append({"ticker": tk, "sector": "Sector%d" % (len(tk) % 5), "action": "EXIT",
                     "current_weight": round(cur_w[tk], 6), "proposed_weight": 0.0,
                     "held": True})
    return rows


def _artifact(nav: float, allocations: list[dict], proposal_hash="AUG12_HASH") -> dict:
    two_way = sum(abs(float(a["proposed_weight"]) - float(a["current_weight"]))
                  for a in allocations)
    one_way = round(two_way / 2.0, 6)
    return {"proposal_id": "reap_%s_%s_%s" % (ELIGIBLE, BOOK_ID, proposal_hash[:12]),
            "schema_version": "reallocation_proposal.v1",
            "identity": {"active_book_id": BOOK_ID, "eligible_market_date": ELIGIBLE,
                         "proposal_hash": proposal_hash, "portfolio_state_hash": "PSH",
                         "hoc_assessment_hash": "HOC", "universe_scoring_hash": "USH",
                         "allocation_policy_version": "reallocation_allocation_policy.v1"},
            "proposal": {"proposal_state": "READY", "portfolio": {"nav": nav},
                         "allocations": allocations, "proposal_hash": proposal_hash,
                         "turnover": {"one_way_turnover": one_way,
                                      "two_way_turnover": round(two_way, 6),
                                      "estimated_transaction_cost": round(
                                          two_way * nav * COST, 2)}}}


def _decision(proposal_hash="AUG12_HASH", decision=pdec.DECISION_APPROVE) -> dict:
    return {"record_id": "pdec_%s_%s" % (ELIGIBLE, BOOK_ID), "decision": decision,
            "proposal_id": "reap_x", "proposal_hash": proposal_hash,
            "binding": {"active_book_id": BOOK_ID, "eligible_market_date": ELIGIBLE,
                        "proposal_hash": proposal_hash}}


def _aug12(tmp: Path, *, marked=None, proposal_hash="AUG12_HASH",
           decision=pdec.DECISION_APPROVE):
    """The complete August-12 scenario. By default the eight additions are ABSENT from the
    desk mark cache — exactly the live incident."""
    marked = HELD + [desk.BENCHMARK_TICKER] if marked is None else marked
    sdir, book = _seed_desk(tmp, marked=marked)
    nav_blk = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))
    nav, holdings = nav_blk["nav"], nav_blk["holdings"]
    art = _artifact(nav, _allocations(nav, holdings), proposal_hash=proposal_hash)
    dec = _decision(proposal_hash=proposal_hash, decision=decision)
    kwargs = dict(desk_dir=sdir, active_book_id=BOOK_ID, eligible_market_date=ELIGIBLE,
                  artifact=art, decision_record=dec, plan_dir=tmp / "plans",
                  actions_dir=tmp / "ca")
    return sdir, book, art, dec, kwargs


def _downloader(tickers: list[str]):
    """An OFFLINE injected downloader in the canonical desk shape. It is the ONLY way marks
    enter these tests — no provider, no network, no key."""
    table = {tk: [{"date": d, "adjusted_close": v} for d, v in _bars(tk)] for tk in tickers}

    def _get(symbol: str, _start: str):
        return table.get(desk._clean_symbol(symbol), [])
    return _get


def _hydrate(tmp: Path, kwargs: dict, tickers: list[str]) -> dict:
    """Supply the missing owned marks through the CANONICAL seam: the Stage 19.2 hydration
    entry point, which delegates to ``desk.refresh_desk``."""
    return rb.refresh_target_marks(
        confirm=rb.HYDRATE_CONFIRM_TOKEN, desk_dir=kwargs["desk_dir"],
        actions_dir=kwargs["actions_dir"], ledger_dir=tmp / "mhz",
        active_book_id=BOOK_ID, eligible_market_date=ELIGIBLE,
        artifact=kwargs["artifact"], decision_record=kwargs["decision_record"],
        downloader=_downloader(tickers), today="2026-08-13",
        completed_through=ELIGIBLE)


def _desk_snapshot(sdir: Path) -> dict:
    """Everything the mutating path could possibly move."""
    book = desk.open_book(sdir)
    fills = desk._fills(sdir)
    nav = desk.book_nav(book, fills, desk.read_marks(sdir))
    return {"orders": sorted(desk._orders_state(sdir)), "n_fills": len(fills),
            "cash": round(nav["cash"], 6), "nav": round(nav["nav"], 6),
            "holdings": {k: v for k, v in sorted(nav["holdings"].items()) if v}}


# =========================================================================== #
# (1) THE EXACT AUGUST-12 EIGHT-NAME MISSING-MARK REGRESSION
# =========================================================================== #
def test_aug12_eight_missing_marks_block_the_plan(tmp_path):
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    st = rb.load_rebalance_state(**kwargs)

    assert st["rebalance_state"] == rb.RB_BLOCKED_MARKS
    assert st["order_plan_buildable"] is False
    assert st["confirmation_available"] is False
    assert st["blocked_count"] == 8
    assert st["missing_marks"] == sorted(ADDS)
    assert st["blocked_tickers"] == sorted(ADDS)
    # every one of the eight is reported explicitly, by name
    for tk in ADDS:
        assert tk in st["missing_marks"]
        assert tk in (st["message"] or "")
    assert rb.BR_NO_OWNED_MARK in st["block_reason_codes"]
    assert st["missing_mark_count"] == 8
    assert st["available_mark_count"] == st["required_mark_count"] - 8


def test_aug12_blocked_plan_is_the_incident_shape(tmp_path):
    """The partial plan is still returned as the EXPLANATION — and it reproduces the
    incident: it only sells, its turnover collapses, and it strands the cash."""
    _sdir, _book, art, _dec, kwargs = _aug12(tmp_path)
    plan = rb.load_rebalance_state(**kwargs)["order_plan"]
    approved = art["proposal"]["turnover"]["one_way_turnover"]

    # every one of the eight approved ADDitions is missing from the executable plan — the
    # plan disproportionately exits/reduces the old book and buys none of its replacements
    planned = {o["ticker"] for o in plan["orders"]}
    assert not (planned & set(ADDS))
    assert not any(o["order_kind"] == "ADD" for o in plan["orders"])
    assert plan["n_sell"] > 0
    assert plan["planned_action_count"] < plan["proposal_action_count"]
    assert plan["planned_one_way_turnover"] < 0.6 * approved
    assert plan["turnover_gap"] < 0
    assert plan["residual_cash"] > 0.25 * plan["sizing_nav_basis"]
    assert plan["target_tracking_error"] > plan["executability_envelope"]
    assert plan["order_plan_buildable"] is False


def test_aug12_second_confirmation_refuses_and_writes_nothing(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    before = _desk_snapshot(sdir)

    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)

    assert res["status"] == rb.C_BLOCKED
    assert res["performed_write"] is False
    assert res["created_orders"] is False
    assert res["created_fills"] is False
    assert res["refused_before_any_write"] is True
    assert res["revalidated_server_side"] is True
    assert sorted(res["blocked_tickers"]) == sorted(ADDS)
    assert _desk_snapshot(sdir) == before                    # zero events, zero drift
    assert desk._orders_state(sdir) == {}                    # no order was ever created
    assert len(desk._fills(sdir)) == len(HELD)               # only the seeded fills remain
    assert not (tmp_path / "plans").exists()                 # no plan artifact persisted


def test_aug12_repaired_after_marks_are_supplied(tmp_path):
    """Supply the eight missing marks through the canonical seam, rebuild, and prove the
    plan becomes a faithful implementation of the approved proposal."""
    sdir, _book, art, _dec, kwargs = _aug12(tmp_path)
    blocked = rb.load_rebalance_state(**kwargs)
    hydration = _hydrate(tmp_path, kwargs, _ALL)

    assert hydration["status"] == rb.H_DONE
    assert hydration["missing_marks_after"] == []

    st = rb.load_rebalance_state(**kwargs)
    plan = st["order_plan"]
    approved = art["proposal"]["turnover"]["one_way_turnover"]

    assert st["rebalance_state"] == rb.RB_PLAN_REVIEW_REQUIRED
    assert st["order_plan_buildable"] is True
    assert st["confirmation_available"] is True
    assert st["blocked_count"] == 0
    assert st["missing_marks"] == []

    # (11) all eight additions produce an executable PAPER_BUY with quantity > 0
    buys = {o["ticker"]: o for o in plan["orders"] if o["side"] == desk.SIDE_BUY}
    for tk in ADDS:
        assert tk in buys, "approved addition %s produced no executable buy" % tk
        assert buys[tk]["quantity"] > 0
        assert buys[tk]["order_kind"] == "ADD"
    # every approved EXIT is a full sell
    sells = {o["ticker"]: o for o in plan["orders"] if o["side"] == desk.SIDE_SELL}
    for tk in EXITS:
        assert tk in sells and sells[tk]["target_shares"] == 0

    # (12) turnover reconciles to the APPROVED proposal, not to the broken partial plan
    assert plan["planned_one_way_turnover"] == pytest.approx(
        approved, abs=plan["executability_envelope"])
    # ... and is materially larger than the broken partial plan, which reconciled to nothing
    partial = blocked["order_plan"]["planned_one_way_turnover"]
    assert plan["planned_one_way_turnover"] - partial > 0.10
    assert abs(partial - approved) > plan["executability_envelope"]

    # (13) residual cash is the intended ~2.5% target cash, not the ~36% collapse
    assert plan["residual_cash"] < 0.06 * plan["sizing_nav_basis"]

    # (14) tracking error is only the bounded whole-share / cost / cash residual
    assert plan["target_tracking_error"] <= plan["executability_envelope"]
    assert plan["policy_target_tracking_error"] <= plan["executability_envelope"]

    # the plan hash MUST move once the mark set changes
    assert plan["order_plan_hash"] != blocked["order_plan"]["order_plan_hash"]
    assert plan["order_plan_id"] != blocked["order_plan"]["order_plan_id"]


# =========================================================================== #
# (2)/(3) a nonempty blocked set / a missing proposed target => NOT buildable
# =========================================================================== #
def test_nonempty_blocked_set_never_yields_buildable(tmp_path):
    """One single unpriced target name is enough. Executability is never a majority vote."""
    _sdir, _book, _art, _dec, kwargs = _aug12(
        tmp_path, marked=HELD + [desk.BENCHMARK_TICKER] + ADDS[1:])
    st = rb.load_rebalance_state(**kwargs)
    assert st["blocked_count"] == 1
    assert st["missing_marks"] == [ADDS[0]]
    assert st["order_plan_buildable"] is False
    assert st["rebalance_state"] == rb.RB_BLOCKED_MARKS


def test_missing_mark_on_a_HELD_name_also_blocks(tmp_path):
    """Sizing a SELL needs a mark too — a held name without one is equally fail-closed."""
    marked = [t for t in HELD if t != "MNST"] + [desk.BENCHMARK_TICKER] + ADDS
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path, marked=marked)
    st = rb.load_rebalance_state(**kwargs)
    assert "MNST" in st["missing_marks"]
    assert st["order_plan_buildable"] is False


def test_target_mark_universe_names_every_required_execution_mark(tmp_path):
    _sdir, book, art, _dec, kwargs = _aug12(tmp_path)
    holdings = desk.book_nav(book, desk._fills(kwargs["desk_dir"]),
                             desk.read_marks(kwargs["desk_dir"]))["holdings"]
    uni = rb.target_mark_universe(artifact=art, holdings=holdings)
    assert set(uni["target_tickers"]) == set(TARGET)
    assert set(uni["held_tickers"]) == set(HELD)
    assert uni["benchmark"] == desk.BENCHMARK_TICKER
    assert set(uni["required"]) == set(TARGET) | set(HELD) | {desk.BENCHMARK_TICKER}
    assert uni["n_target"] == 25


# =========================================================================== #
# (4)/(5) the confirmation refuses a partial plan and writes ZERO orders
# =========================================================================== #
def test_confirmation_refuses_partial_plan_for_every_block_reason(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    before = _desk_snapshot(sdir)
    for token in (rb.CONFIRM_TOKEN, "WRONG", None, ""):
        res = rb.confirm_rebalance_order_plan(confirm=token, **kwargs)
        assert res["performed_write"] is False
        assert res["created_orders"] is False
        assert res["status"] in (rb.C_BLOCKED, rb.C_CONFIRM_REQUIRED)
    assert _desk_snapshot(sdir) == before


def test_refused_confirmation_leaves_ledgers_untouched(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    sizes = {f: (sdir / f).stat().st_size if (sdir / f).exists() else 0
             for f in desk.LEDGER_FILES}
    rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    after = {f: (sdir / f).stat().st_size if (sdir / f).exists() else 0
             for f in desk.LEDGER_FILES}
    assert after == sizes


# =========================================================================== #
# (6) target-mark hydration DELEGATES to the existing mark owner
# =========================================================================== #
def test_hydration_delegates_to_the_canonical_desk_mark_owner(tmp_path, monkeypatch):
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    seen = {}
    real = desk.refresh_desk

    def _spy(**kw):
        seen.update(kw)
        return real(**kw)

    monkeypatch.setattr(desk, "refresh_desk", _spy)
    res = _hydrate(tmp_path, kwargs, _ALL)

    assert res["delegated_to_mark_owner"] == "api.paper_trading_desk.refresh_desk"
    assert seen, "hydration did not call the canonical desk mark owner"
    assert seen["confirm"] == desk.REFRESH_CONFIRM_TOKEN
    # the eight not-yet-held additions are exactly what the desk could not infer on its own
    assert set(ADDS).issubset(set(seen["extra_tickers"]))
    assert set(seen["extra_tickers"]) == set(_ALL)
    assert res["desk_refresh"]["requested_extra_tickers"] == sorted(_ALL)


def test_hydration_requires_its_own_explicit_token(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    before = _desk_snapshot(sdir)
    marks_before = json.dumps(desk.read_marks(sdir), sort_keys=True)
    for token in (None, "", "WRONG", rb.CONFIRM_TOKEN):
        res = rb.refresh_target_marks(confirm=token, desk_dir=sdir,
                                      actions_dir=kwargs["actions_dir"],
                                      artifact=kwargs["artifact"],
                                      decision_record=kwargs["decision_record"],
                                      active_book_id=BOOK_ID, eligible_market_date=ELIGIBLE)
        assert res["status"] == rb.H_CONFIRM_REQUIRED
        assert res["performed_write"] is False
    assert json.dumps(desk.read_marks(sdir), sort_keys=True) == marks_before
    assert _desk_snapshot(sdir) == before


def test_hydration_creates_no_order_and_no_fill(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    fills_before = list(desk._fills(sdir))
    res = _hydrate(tmp_path, kwargs, _ALL)
    assert res["created_orders"] is False and res["created_fills"] is False
    assert res["changed_holdings"] is False and res["changed_cash"] is False
    assert desk._orders_state(sdir) == {}
    assert desk._fills(sdir) == fills_before               # fill ledger byte-identical
    assert len(desk._fills(sdir)) == len(HELD)


def test_hydration_refuses_without_an_approved_proposal(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(
        tmp_path, decision=pdec.DECISION_HOLD if hasattr(pdec, "DECISION_HOLD") else "HOLD")
    marks_before = json.dumps(desk.read_marks(sdir), sort_keys=True)
    res = rb.refresh_target_marks(confirm=rb.HYDRATE_CONFIRM_TOKEN, **{
        k: v for k, v in kwargs.items() if k != "plan_dir"},
        downloader=_downloader(_ALL), today="2026-08-13", completed_through=ELIGIBLE)
    assert res["status"] == rb.H_NOT_APPROVED
    assert res["performed_write"] is False
    assert json.dumps(desk.read_marks(sdir), sort_keys=True) == marks_before


# =========================================================================== #
# (7) the GET read contract is read-only and provider-free
# =========================================================================== #
def test_read_contract_never_calls_the_provider_or_writes(tmp_path, monkeypatch):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)

    def _boom(*_a, **_k):
        raise AssertionError("a GET reached the owned-EODHD transport")

    monkeypatch.setattr(desk, "_live_downloader", _boom)
    monkeypatch.setattr(desk, "sync_marks", _boom)
    monkeypatch.setattr(desk, "refresh_desk", _boom)

    before = _desk_snapshot(sdir)
    marks_before = json.dumps(desk.read_marks(sdir), sort_keys=True)
    for _ in range(3):
        st = rb.load_rebalance_state(**kwargs)
        assert st["provider_called"] is False
        assert st["performed_write"] is False
        assert st["created_orders"] is False
    assert json.dumps(desk.read_marks(sdir), sort_keys=True) == marks_before
    assert _desk_snapshot(sdir) == before


def test_read_contract_is_deterministic(tmp_path):
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    a = rb.load_rebalance_state(**kwargs)
    b = rb.load_rebalance_state(**kwargs)
    for k in ("rebalance_state", "order_plan_buildable", "blocked_tickers",
              "missing_marks", "planned_one_way_turnover"):
        assert a[k] == b[k]
    assert a["order_plan"]["order_plan_hash"] == b["order_plan"]["order_plan_hash"]


# =========================================================================== #
# (8)/(9) the UI blocked state disables confirmation and names the blocked set
# =========================================================================== #
def _ui_source() -> str:
    return (Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html").read_text(
        encoding="utf-8", errors="replace")


def test_ui_renders_the_blocked_state_and_names_the_blocked_tickers():
    ui = _ui_source()
    assert "ORDER PLAN BLOCKED" in ui
    assert "OWNED MARKS REQUIRED" in ui
    assert "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS" in ui
    assert "stage19-blocked" in ui
    assert "missing_marks" in ui and "blocked_tickers" in ui
    assert "blocked_reasons" in ui
    # the review screen shows the reconciliation the operator has to judge
    for token in ("Proposal turnover", "Executable turnover", "Turnover gap",
                  "Residual cash", "Tracking error", "Blocked names", "Marks date"):
        assert token in ui, token


def test_ui_exposes_confirmation_only_when_the_backend_says_confirmable():
    ui = _ui_source()
    assert "confirmation_available" in ui
    assert "ORDER PLAN READY FOR REVIEW" in ui
    # no browser control invokes the order-creating gate, in any form
    for form in ("call('POST', '/v1/operations/rebalance/confirm-order-plan'",
                 "fetch('/v1/operations/rebalance/confirm-order-plan'",
                 "createOrders("):
        assert form not in ui
    # and no native browser dialog anywhere on the page
    assert "window._showWriteConfirm" in ui


def test_ui_has_one_primary_action_out_of_the_blocked_state():
    ui = _ui_source()
    assert ui.count("function rebalanceRefreshTargetMarks") == 1
    assert "rebalanceRefreshTargetMarks(this)" in ui
    assert "CONFIRM_REBALANCE_TARGET_MARK_REFRESH" in ui


def test_ui_surfaces_the_blocked_gate_at_the_top_of_the_portfolio_view():
    """The blocked set must not be discoverable only by scrolling to the rebalance card,
    and never only inside Audit / Advanced. It is mirrored into the top-of-view decision
    bar alongside HOC and Reallocation."""
    ui = _ui_source()
    assert 'id="pa-dec-rebalance"' in ui                 # the decision-bar slot exists
    assert 'id="pa-dec-rebalance-btn"' in ui             # with a deep link to the card
    assert ui.count("function focusRebalanceCard") == 1
    assert "window._stage19Data" in ui                   # fed from the canonical payload
    # the bar states the verdict in the operator's own words
    bar = ui[ui.find("Stage 19.2 execution gate"):]
    for token in ("ORDER PLAN BLOCKED", "missing marks", "confirmable",
                  "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS"):
        assert token in bar[:2500], token


def test_ui_blocked_state_is_never_hidden_inside_the_collapsed_detail():
    """The Controlled Paper Rebalance card lives inside the collapsed 'Portfolio decision
    detail' disclosure. A blocked execution gate must not be discoverable only by opening
    a diagnostics section, so the renderer opens it and the deep link opens it too."""
    ui = _ui_source()
    render = ui[ui.find("function renderRebalanceLifecycle"):
                ui.find("function renderCorporateActionIntegrity")]
    assert "closest('details')" in render
    assert "det.open = true" in render
    jump = ui[ui.find("function focusRebalanceCard"):]
    assert "closest('details')" in jump[:900]


# =========================================================================== #
# (10) after marks are supplied the plan becomes buildable — one primary action each
# =========================================================================== #
def test_every_state_has_exactly_one_primary_action(tmp_path):
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    blocked = rb.load_rebalance_state(**kwargs)
    assert blocked["primary_action"]["path"].endswith("/rebalance/refresh-target-marks")
    _hydrate(tmp_path, kwargs, _ALL)
    ready = rb.load_rebalance_state(**kwargs)
    assert ready["primary_action"]["path"].endswith("/rebalance/confirm-order-plan")
    for state in rb.STATE_VOCAB:
        assert state in rb._PRIMARY_ACTION
    assert set(rb.NON_CONFIRMABLE_STATES).issubset(set(rb.STATE_VOCAB))


def test_blocked_states_are_in_the_public_vocabulary():
    assert rb.RB_BLOCKED_MARKS in rb.STATE_VOCAB
    assert rb.RB_BLOCKED_INCOMPLETE in rb.STATE_VOCAB
    assert rb.RB_BLOCKED_MARKS in rb.NON_CONFIRMABLE_STATES
    assert rb.RB_BLOCKED_INCOMPLETE in rb.NON_CONFIRMABLE_STATES
    assert rb.RB_PLAN_REVIEW_REQUIRED not in rb.NON_CONFIRMABLE_STATES


# =========================================================================== #
# (15)/(16)/(17)/(18) the confirmation revalidates SERVER-SIDE
# =========================================================================== #
def test_confirmation_revalidates_against_current_state_not_the_caller(tmp_path):
    """The operator reviewed a buildable plan; the marks then regress. The confirmation
    rebuilds from the CURRENT stores and refuses — a browser boolean is never trusted."""
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    reviewed = rb.load_rebalance_state(**kwargs)
    assert reviewed["order_plan_buildable"] is True
    reviewed_hash = reviewed["order_plan"]["order_plan_hash"]

    # the mark store regresses (a later refresh dropped the additions)
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger",
        "series": {tk: _bars(tk) for tk in HELD + [desk.BENCHMARK_TICKER]},
        "latest_completed_date": ELIGIBLE, "updated_at": "2026-08-12T23:00:00+00:00"})
    before = _desk_snapshot(sdir)

    res = rb.confirm_rebalance_order_plan(
        confirm=rb.CONFIRM_TOKEN, expected_order_plan_hash=reviewed_hash, **kwargs)
    assert res["status"] == rb.C_BLOCKED
    assert res["refused_before_any_write"] is True
    assert res["performed_write"] is False
    assert _desk_snapshot(sdir) == before


def test_stale_proposal_is_blocked(tmp_path):
    sdir, _book, art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    kwargs["decision_record"] = _decision(proposal_hash="A_DIFFERENT_HASH")
    before = _desk_snapshot(sdir)
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_STALE
    assert st["order_plan_buildable"] is False
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert res["status"] == rb.C_STALE
    assert res["performed_write"] is False
    assert _desk_snapshot(sdir) == before


def test_stale_corporate_action_registry_is_blocked(tmp_path, monkeypatch):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    from paper_trader.api import reallocation_proposal as realloc

    monkeypatch.setattr(realloc, "corporate_action_staleness",
                        lambda **_k: {"stale": True, "reason": "REGISTRY_MOVED"})
    before = _desk_snapshot(sdir)
    st = rb.load_rebalance_state(**kwargs)
    assert st["rebalance_state"] == rb.RB_STALE
    assert st["order_plan_buildable"] is False
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert res["status"] == rb.C_STALE
    assert res["performed_write"] is False
    assert _desk_snapshot(sdir) == before


def test_changed_order_plan_hash_is_blocked(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    before = _desk_snapshot(sdir)
    res = rb.confirm_rebalance_order_plan(
        confirm=rb.CONFIRM_TOKEN, expected_order_plan_hash="A_PLAN_THE_DESK_NEVER_BUILT",
        **kwargs)
    assert res["status"] == rb.C_STALE
    assert res["performed_write"] is False
    assert _desk_snapshot(sdir) == before


# =========================================================================== #
# (19)-(22) CANCELLED-PLAN RECOVERY
# =========================================================================== #
def _cancel_all(sdir: Path) -> list[str]:
    ids = sorted(desk._orders_state(sdir))
    out = desk.cancel_orders(confirm=desk.CANCEL_CONFIRM_TOKEN, order_ids=ids,
                             desk_dir=sdir)
    assert out.get("performed_write") is True
    return ids


def test_cancelled_defective_orders_stay_immutable_and_do_not_veto_recovery(tmp_path):
    """The August-12 shape: a plan is confirmed, then CANCELLED before any fill. The
    cancelled evidence must survive verbatim AND must not block the repaired plan."""
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    first = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert first["status"] == rb.C_CREATED
    defective_ids = _cancel_all(sdir)
    rows_before = desk._read_ledger(sdir, desk.ORDERS_FILE)
    assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True

    # a repaired plan for the same proposal, after the desk moved (one name re-priced)
    marks = desk.read_marks(sdir)
    marks["series"]["AMD"] = [[PRIOR, 100.0], [ELIGIBLE, 137.5]]
    desk._atomic_write_json(sdir / desk.MARKS_FILE, marks)

    repaired = rb.load_rebalance_state(**kwargs)
    assert repaired["order_plan_buildable"] is True
    assert repaired["order_plan"]["order_plan_hash"] != first["order_plan_hash"]

    second = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert second["status"] == rb.C_CREATED
    assert second["order_plan_hash"] != first["order_plan_hash"]

    orders = desk._orders_state(sdir)
    # (19) every cancelled order is still exactly CANCELLED
    for oid in defective_ids:
        assert orders[oid]["status"] == desk.ST_CANCELLED
    # (20)/(21) the repaired set is new, active, and collides with nothing
    new_ids = second["orders_created"]
    assert not (set(new_ids) & set(defective_ids))
    assert len(set(new_ids)) == len(new_ids)
    assert all(orders[o]["status"] == desk.ST_SUBMITTED for o in new_ids)
    # exactly ONE active lineage exists
    active = [o for o in orders.values()
              if o["status"] not in (desk.ST_CANCELLED, desk.ST_EXPIRED)]
    assert {(o.get("rebalance_lineage") or {}).get("order_plan_id") for o in active} == {
        second["order_plan_id"]}
    # append-only: every prior row survives byte-identically as a prefix of the new ledger
    rows_after = desk._read_ledger(sdir, desk.ORDERS_FILE)
    assert len(rows_after) > len(rows_before)
    assert rows_after[:len(rows_before)] == rows_before
    assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True


def test_a_fully_cancelled_plan_can_be_reconfirmed_without_duplicate_active_lineage(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    first = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    cancelled = _cancel_all(sdir)

    again = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert again["status"] == rb.C_CREATED
    assert again["recovered_from_cancelled_plan"] is True
    assert sorted(again["cancelled_prior_order_ids"]) == sorted(cancelled)
    assert not (set(again["orders_created"]) & set(cancelled))
    orders = desk._orders_state(sdir)
    active = [o for o in orders.values()
              if o["status"] not in (desk.ST_CANCELLED, desk.ST_EXPIRED)]
    assert len(active) == len(again["orders_created"])
    assert first["order_plan_id"] == again["order_plan_id"]      # same deterministic plan


def test_retrying_the_repaired_confirmation_is_idempotent(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    first = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
    assert first["status"] == rb.C_CREATED
    n_after_first = len(desk._orders_state(sdir))

    for _ in range(3):
        again = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs)
        assert again["status"] == rb.C_REUSED
        assert again["performed_write"] is False
        assert again["created_orders"] is False
        assert sorted(again["existing_order_ids"]) == sorted(first["orders_created"])
    assert len(desk._orders_state(sdir)) == n_after_first


# =========================================================================== #
# (23)-(26) no duplicated fill engine / NEXT_CLOSE only / no broker / no automation
# =========================================================================== #
def test_no_second_fill_or_order_engine_is_defined_in_the_rebalance_owner():
    src = (Path(rb.__file__)).read_text(encoding="utf-8")
    for marker in ("def settle_due_orders(", "def book_nav(", "def _append_ledger(",
                   "def run_fill_cycle(", "def _row_hash(", "def confirm_orders(",
                   "def sync_marks(", "def refresh_desk(", "def _live_downloader("):
        assert marker not in src, "%s forks a canonical owner" % marker
    for provider in ("requests.", "httpx.", "urlopen(", "eodhd"):
        assert provider not in src


def test_next_close_remains_the_sole_settlement_model(tmp_path):
    sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, today="2026-08-13",
                                          **kwargs)
    assert res["status"] == rb.C_CREATED
    assert res["execution_model"] == desk.EXECUTION_MODEL_DEFAULT == "NEXT_CLOSE"
    orders = desk._orders_state(sdir)
    for oid in res["orders_created"]:
        assert orders[oid]["execution_model"] == "NEXT_CLOSE"
        assert orders[oid]["status"] == desk.ST_SUBMITTED       # no same-close hindsight fill
    assert res["settlement"]["n_filled"] == 0
    assert len(desk._fills(sdir)) == len(HELD)                  # no new fill was invented


def test_safety_flags_on_every_payload(tmp_path):
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    payloads = [rb.load_rebalance_state(**kwargs),
                rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **kwargs),
                rb.confirm_rebalance_order_plan(confirm="NOPE", **kwargs),
                rb.refresh_target_marks(confirm=None, desk_dir=kwargs["desk_dir"])]
    for p in payloads:
        assert p["paper_only"] is True
        assert p["broker_enabled"] is False
        assert p["live_orders_enabled"] is False
        assert p["automation_off"] is True
        assert p["automatic_approval_allowed"] is False
        assert p["automatic_rebalance_allowed"] is False
        assert p["changed_cadence"] is False
        assert p["promoted_model"] is False and p["recalibrated_model"] is False
        assert p["second_confirmation_required"] is True


def test_supported_execution_mechanics_are_closed_and_named(tmp_path):
    """The envelope is an explicit, finite list — not an open-ended tolerance."""
    _sdir, _book, _art, _dec, kwargs = _aug12(tmp_path)
    _hydrate(tmp_path, kwargs, _ALL)
    plan = rb.load_rebalance_state(**kwargs)["order_plan"]
    assert plan["supported_execution_mechanics"] == [
        "WHOLE_SHARES", "TRANSACTION_COST", "AVAILABLE_CASH", "CONCENTRATION_POLICY",
        "MIN_ORDER_POLICY"]
    comp = plan["envelope_components"]
    assert set(comp) == {"whole_share_slack", "transaction_cost", "capital_shortfall",
                         "nav_basis"}
    for row in plan["omitted_actions"]:
        assert row["reason"] in rb.SUPPORTED_OMISSIONS
        assert row["supported"] is True
