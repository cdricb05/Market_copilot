r"""The FX carry cadence TRUE_FORWARD producer, and the two accrual seams it needs.

WHAT IS PROVED
    ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7 was registered on 2026-09-14 and
    could never accrue: nothing froze its per-session decisions, the accrual
    valued every book on the equity panel (which carries no FX future), and a
    realised-calendar grid learns of an entry session only after its window has
    shut. These tests pin the producer
    (``alpha_agent.alpha_recovery.fx_carry_cadence_runtime``), the declared
    valuation marks and the armed entry session - and that every registration
    that declares neither is valued and gridded exactly as before.

WHAT THEY MAY NOT DO
    Every test gets a PRIVATE research root and a private accrual store; the
    conftest already isolates the registry and the accrual store. Nothing here
    reads the vendor, scores the real store or writes the campaign's own store.
"""
from __future__ import annotations

import csv
import inspect
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import cadence as CAD
from paper_trader.alpha_agent.alpha_recovery import fx_carry_cadence_challenger as FXC
from paper_trader.alpha_agent.alpha_recovery import fx_carry_cadence_runtime as FXR
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.api import canonical_forward_accrual as CFA

SCOPE = ["6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "DX"]
REG = "2026-09-14"
SESSIONS = ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
            "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21",
            "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25", "2026-09-28",
            "2026-09-29", "2026-09-30"]
WINDOW_OPEN = "2026-09-22T05:00:00+00:00"      # 01:00 ET on the entry session
AFTER_WINDOW = "2026-09-22T14:00:00+00:00"     # 10:00 ET - the window has shut

RUNTIME_SRC = Path(inspect.getfile(__import__("paper_trader.alpha_agent.r52.runtime",
                                              fromlist=["runtime"]))).read_text(encoding="utf-8")
FXR_SRC = Path(FXR.__file__).read_text(encoding="utf-8")

IDENT = {"challenger_id": FXC.CHALLENGER_ID, "freeze_record_hash": FXC.FREEZE_RECORD_HASH,
         "identity_hash": "fx-identity", "release": "ALPHA_RECOVERY_OFFENSIVE",
         "registration_session": REG, "model_spec_hash": "spec"}


@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A private research root; the cost policy never reads the real R38 panel."""
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    monkeypatch.setattr(FXR, "cost_policy", lambda scope: {
        "bps_per_side": 2.0, "per_market_bps_per_side": {m: 2.0 for m in scope}})
    return tmp_path


def _write_store(through: str, *, provisional_last: bool = True, oi_zero_on: str = None):
    store = FXR.curves_dir()
    store.mkdir(parents=True, exist_ok=True)
    days = [d for d in SESSIONS if d <= through]
    for k, m in enumerate(SCOPE):
        with (store / ("%s_daily.csv" % m)).open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "ret1", "c1", "oi1", "v1"])
            for i, d in enumerate(days):
                last = i == len(days) - 1
                oi = 0.0 if (provisional_last and last) or d == oi_zero_on else 1000.0 + i
                w.writerow([d, 0.001 * ((i + k) % 5 - 2), 1.0 + 0.01 * i, oi, 500.0 + i])
    return store


def _declare():
    return PD.declare_policy(**FXR.policy_declaration(IDENT, scope=SCOPE),
                             now="2026-09-14T20:00:00+00:00")


def _registration() -> dict:
    return {"challenger_id": FXC.CHALLENGER_ID, "registration_session": REG,
            "freeze_record_hash": FXC.FREEZE_RECORD_HASH, "asset_class": "FX_FUTURES",
            "horizon_sessions": 1, "instrument_scope": list(SCOPE),
            "identity": {"challenger_id": FXC.CHALLENGER_ID, "release": "ALPHA_RECOVERY_OFFENSIVE",
                         "identity_hash": "fx-identity",
                         "freeze_record_hash": FXC.FREEZE_RECORD_HASH},
            "observation_clock": {"first_eligible_observation_session": None}}


def _fake_scorer(calls: list):
    def scorer(plan, prev):
        calls.append((dict(plan), dict(prev)))
        return {"ok": True, "entry_session": plan["entry_session"],
                "information_session": plan["information_session"],
                "newest_published_session": plan["newest_session_required"],
                "weights": {"6A": 0.3, "6B": -0.2, "6J": 0.25, "DX": -0.35},
                "source_data_hash": "src", "feature_state_hash": "feat", "leverage": 1.5,
                "leverage_state": "TARGETED", "gross_unlevered": 2.0,
                "names_held_by_the_band": [], "checks": {"ok": True}}
    return scorer


def _fake_refresher(kind):
    return {"ok": True, "kind": kind}


def _plan(entry, published, rows):
    return FXR.plan_entry(entry_session=entry, registration_session=REG, scope=SCOPE,
                          published=[d for d in published if d < entry], rows_by_market=rows)


# --------------------------------------------------------------------------- #
# 1-2. THE IDENTITY IS READ, NEVER RESTATED
# --------------------------------------------------------------------------- #
def test_01_child_environment_names_are_the_owners_own():
    from paper_trader.alpha_agent import r41
    from paper_trader.alpha_agent import r63
    from paper_trader.alpha_agent.r63 import panels as P
    assert FXR.R41_ROOT_ENV == r41.RESEARCH_ROOT_ENV
    assert FXR.CURVES_DIR_ENV == P.CURVES_DIR_ENV
    # unset in this process, the R63 store is the one every result was measured on
    assert P.R41_CURVES == r63.R41_ROOT / "_data_curves"
    assert Path(FXR.FROZEN_CURVES_DIR) == Path(P.R41_CURVES)


def test_02_the_frozen_cadence_is_read_from_the_record_owner():
    assert FXR.CHALLENGER_ID == "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"
    assert FXR.TRADE_EVERY == FXC.FROZEN_TRADE_EVERY == 5
    assert FXR.NO_TRADE_BAND == FXC.FROZEN_NO_TRADE_BAND == 0.25
    assert FXR.LABEL_HORIZON == FXC.FROZEN_HORIZON == 1
    assert FXR.EVALUATION_HORIZON_SESSIONS == FXR.TRADE_EVERY
    for literal in ("TRADE_EVERY = 5", "NO_TRADE_BAND = 0.25"):
        assert literal not in FXR_SRC


# --------------------------------------------------------------------------- #
# 3-7. THE WINDOW, THE BOUNDARY AND THE INFORMATION SESSION
# --------------------------------------------------------------------------- #
def test_03_the_window_sits_on_the_entry_session():
    w = FXR.locate_window(WINDOW_OPEN)
    assert w["in_window"] and w["session"] == "2026-09-22"
    late = FXR.locate_window(AFTER_WINDOW)
    assert not late["in_window"]
    assert late["last_session"] == "2026-09-22" and late["next_session"] == "2026-09-23"
    evening = FXR.locate_window("2026-09-21T23:00:00+00:00")
    assert not evening["in_window"] and evening["next_session"] == "2026-09-22"


def test_04_the_producer_and_the_accrual_name_the_same_boundaries(root):
    _write_store("2026-09-29", provisional_last=False)
    rows = {m: FXR.daily_rows(m) for m in SCOPE}
    pub = FXR.published_sessions(SCOPE, rows_by_market=rows)
    grid = CFA.decision_grid(_registration(), sessions=pub, cadence_sessions=5)
    assert grid[:3] == ["2026-09-15", "2026-09-22", "2026-09-29"]
    for entry in grid[1:3]:
        plan = _plan(entry, pub, rows)
        assert plan["state"] == "DUE", plan
        assert plan["rebalance_index"] % 5 == 0
    # the first boundary would read 2026-09-11, a session before the registration
    assert _plan("2026-09-15", pub, rows)["state"] == FXR.ST_BEFORE_FIRST_LEGAL
    assert _plan("2026-09-23", pub, rows)["state"] == FXR.ST_NOT_A_REBALANCE


def test_04b_the_operator_schedule_names_the_same_boundaries():
    published = [d for d in SESSIONS if d <= "2026-09-11"]
    rows = FXR.schedule_preview(registration_session=REG, published=published)
    assert [r["entry_session"] for r in rows[:3]] == ["2026-09-15", "2026-09-22", "2026-09-29"]
    assert [r["legal"] for r in rows[:2]] == [False, True]
    assert rows[1]["information_session_expected"] == "2026-09-18"


def test_05_the_information_session_is_the_newest_with_final_inputs(root):
    _write_store("2026-09-21")          # the newest row carries provisional OI, as measured
    rows = {m: FXR.daily_rows(m) for m in SCOPE}
    pub = FXR.published_sessions(SCOPE, rows_by_market=rows)
    plan = _plan("2026-09-22", pub, rows)
    assert plan["state"] == "DUE"
    assert plan["newest_session_required"] == "2026-09-21"
    assert plan["information_session"] == "2026-09-18"
    assert FXR.information_is_final(SCOPE, "2026-09-21", rows)["final"] is False


def test_06_incomplete_information_is_refused(root):
    _write_store("2026-09-21", oi_zero_on="2026-09-18")
    rows = {m: FXR.daily_rows(m) for m in SCOPE}
    pub = FXR.published_sessions(SCOPE, rows_by_market=rows)
    assert _plan("2026-09-22", pub, rows)["state"] == FXR.ST_AWAITING_FINAL


def test_07_an_unpublished_session_waits(root):
    _write_store("2026-09-18")
    rows = {m: FXR.daily_rows(m) for m in SCOPE}
    pub = FXR.published_sessions(SCOPE, rows_by_market=rows)
    assert _plan("2026-09-22", pub, rows)["state"] == FXR.ST_AWAITING_PUBLICATION


# --------------------------------------------------------------------------- #
# 8. THE BOOK IS THE FROZEN CADENCE BOOK
# --------------------------------------------------------------------------- #
def test_08_the_forward_rebalance_reproduces_build_book_cadence():
    rng = np.random.default_rng(7)
    n_i, n_p = 9, 90
    gid = np.repeat(np.arange(n_p), n_i)
    iid = np.tile(np.arange(n_i), n_p)
    pred = rng.normal(size=gid.size)
    y = rng.normal(0.0, 0.006, size=gid.size)
    vol = rng.uniform(0.05, 0.15, size=gid.size)
    cost = np.full(gid.size, 0.0002)
    book = CAD.build_book_cadence(pred, y, gid, iid, vol, cost, horizon=1, trade_every=5,
                                  band=0.25)
    pol = FXR.construction_policy()
    prev, k_since, hist, gross = {}, 5, [], []
    for g in range(n_p):
        m = gid == g
        pp, yy, vv, ii = pred[m], y[m], vol[m], iid[m]
        w_u = FXR.unlevered_target(pp, vv, ii, policy=pol)
        ret_by = {int(j): float(x) for j, x in zip(ii, yy)}
        r_u = sum(w_u[x] * ret_by.get(x, 0.0) for x in w_u)
        if k_since >= 5 or not prev:
            w = FXR.forward_weights(w_u=w_u, history=hist, prev=prev, policy=pol)["weights"]
            k_since = 1
        else:
            w = {x: v for x, v in prev.items() if x in ret_by}
            k_since += 1
        hist.append(r_u)
        gross.append(sum(w[x] * ret_by.get(x, 0.0) for x in w))
        prev = w
    assert len(gross) == len(book["gross"])
    assert np.array_equal(np.array(gross), book["gross"])


# --------------------------------------------------------------------------- #
# 9-14. THE ADVANCE
# --------------------------------------------------------------------------- #
def test_09_no_policy_no_decision(root):
    res = FXR.advance(now=WINDOW_OPEN, scorer=_fake_scorer([]), refresher=_fake_refresher)
    assert res["state"] == FXR.ST_AWAITING_POLICY


def test_10_the_policy_declares_the_boundary_the_cadence_and_the_marks(root):
    assert _declare()["outcome"] == PD.FROZEN
    pol = PD.load_policy(FXC.CHALLENGER_ID)
    assert pol["declared_information_cutoff_et"] == [0, 0]
    assert pol["entry_mark_et"] == [9, 30]
    assert pol["emission_window"]["declaration_state"] == "DECLARATION_ACCEPTED"
    assert pol["rebalance_cadence_sessions"] == 5
    assert pol["evaluation_horizon_sessions"] == 5
    ec = pol["execution_contract"]
    assert ec["accrual_arms_the_owner_frozen_entry_session"] is True
    assert ec["valuation_marks"]["store_relative_to_research_root"] == FXR.marks_store_relative()
    # it may never shift the accrual grid the way a next-open release does
    assert ec["execution_boundary"] != "NEXT_ELIGIBLE_SESSION_OPEN"
    assert pol["identity"]["identity_hash"] == "fx-identity"
    assert pol["instrument_scope"] == SCOPE
    assert _declare()["outcome"] == PD.ALREADY_FROZEN


def test_11_one_decision_inside_the_window_and_a_retry_appends_nothing(root):
    _declare()
    _write_store("2026-09-21")
    calls = []
    a = FXR.advance(now=WINDOW_OPEN, scorer=_fake_scorer(calls), refresher=_fake_refresher)
    assert a["state"] == FXR.ST_FROZEN, a
    rec = PD.load_decision(FXC.CHALLENGER_ID, "2026-09-22")
    ctx = rec["decision_context"]
    assert ctx["information_session"] == "2026-09-18"
    assert ctx["newest_published_session"] == "2026-09-21"
    assert ctx["previous_book"]["weights"] == {}
    assert rec["is_true_forward"] is True and rec["backfilled"] is False
    assert rec["evaluation_horizon_sessions"] == 5
    b = FXR.advance(now="2026-09-22T06:00:00+00:00", scorer=_fake_scorer(calls),
                    refresher=_fake_refresher)
    assert b["state"] == FXR.ST_ALREADY_FROZEN
    assert len(calls) == 1


def test_12_a_missed_boundary_is_never_backfilled(root):
    _declare()
    _write_store("2026-09-21")
    calls = []
    res = FXR.advance(now=AFTER_WINDOW, scorer=_fake_scorer(calls), refresher=_fake_refresher)
    assert res["state"] == FXR.ST_MISSED
    assert res["backfill_refused"] is True
    assert PD.load_decision(FXC.CHALLENGER_ID, "2026-09-22") is None
    assert calls == []


def test_13_a_holding_session_freezes_nothing(root):
    _declare()
    _write_store("2026-09-22")
    calls = []
    res = FXR.advance(now="2026-09-23T05:00:00+00:00", scorer=_fake_scorer(calls),
                      refresher=_fake_refresher)
    assert res["state"] == FXR.ST_NOT_A_REBALANCE
    assert PD.load_decision(FXC.CHALLENGER_ID, "2026-09-23") is None
    assert calls == []


def test_14_under_pytest_without_injected_workers_nothing_is_scored(root):
    _declare()
    _write_store("2026-09-21")
    res = FXR.advance(now=WINDOW_OPEN)
    assert res["state"] == FXR.ST_DUE_DRY_RUN
    assert PD.load_decision(FXC.CHALLENGER_ID, "2026-09-22") is None


# --------------------------------------------------------------------------- #
# 15-19. THE ACCRUAL SEAMS
# --------------------------------------------------------------------------- #
def test_15_every_registration_that_declares_nothing_keeps_its_panel(root):
    series = {"SPY": {"dates": ["2026-09-11"], "adj": [1.0]}}
    for reg in ({"challenger_id": "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1",
                 "identity": {"release": "ALPHA_RECOVERY_OFFENSIVE"}},
                {"challenger_id": "R58_FCF_PURE_V1", "identity": {"release": "R58"}}):
        assert CFA.valuation_series_for(reg, series) is series
        assert CFA.armed_owner_sessions(reg, ["2026-09-11"]) == []


def test_16_declared_marks_value_the_fx_book_and_nothing_else_can(root):
    _declare()
    _write_store("2026-09-21")
    equity_dx = {"DX": {"dates": ["2026-09-21"], "adj": [99.0]}}
    s = CFA.valuation_series_for(_registration(), equity_dx)
    assert sorted(s) == sorted(SCOPE)
    assert s["DX"]["adj"] != [99.0]
    rows = FXR.daily_rows("6E")
    level = 1.0
    for d in s["6E"]["dates"]:
        level *= 1.0 + float(rows[d]["ret1"])
    assert abs(s["6E"]["adj"][-1] - level) < 1e-12
    assert s["6E"]["dates"][-1] == "2026-09-21"


def test_17_only_the_owner_decided_next_session_is_armed(root):
    _declare()
    reg = _registration()
    realised = [d for d in SESSIONS if d <= "2026-09-21"]
    assert CFA.armed_owner_sessions(reg, realised) == []
    PD.freeze_decision(challenger_id=FXC.CHALLENGER_ID, eligible_session="2026-09-23",
                       weights={"6A": 0.1}, source_data_hash="s", feature_state_hash="f",
                       feature_observed_at="2026-09-19T21:00:00+00:00",
                       now="2026-09-23T05:00:00+00:00")
    assert CFA.armed_owner_sessions(reg, realised) == []      # not the NEXT session
    PD.freeze_decision(challenger_id=FXC.CHALLENGER_ID, eligible_session="2026-09-22",
                       weights={"6A": 0.1}, source_data_hash="s", feature_state_hash="f",
                       feature_observed_at="2026-09-18T21:00:00+00:00", now=WINDOW_OPEN)
    assert CFA.armed_owner_sessions(reg, realised) == ["2026-09-22"]


def test_18_a_release_that_declares_no_arming_is_never_armed(root):
    PD.declare_policy(challenger_id="SKEW_LIKE", information_cutoff_et=(0, 0),
                      entry_mark_et=(9, 30), rebalance_cadence_sessions=5,
                      evaluation_horizon_sessions=5, cost_policy={"bps_per_side": 1.0},
                      instrument_scope=["SPY"],
                      execution_contract={"decision_session_is": "the ENTRY session",
                                          "execution_boundary": "NEXT_ELIGIBLE_SESSION_OPEN"})
    PD.freeze_decision(challenger_id="SKEW_LIKE", eligible_session="2026-09-22",
                       weights={"SPY": 1.0}, source_data_hash="s", feature_state_hash="f",
                       feature_observed_at="2026-09-21T19:45:00+00:00", now=WINDOW_OPEN)
    reg = {"challenger_id": "SKEW_LIKE", "identity": {"release": "ALPHA_RECOVERY_OFFENSIVE"}}
    series = {"SPY": {"dates": ["2026-09-21"], "adj": [1.0]}}
    assert CFA.armed_owner_sessions(reg, ["2026-09-21"]) == []
    assert CFA.valuation_series_for(reg, series) is series


def test_19_the_accrual_emits_inside_the_window_and_matures_on_the_marks(root, tmp_path):
    _declare()
    _write_store("2026-09-21")
    FXR.advance(now=WINDOW_OPEN, scorer=_fake_scorer([]), refresher=_fake_refresher)
    acc = tmp_path / "accrual"
    adv = CFA.advance_canonical_forward_accrual(
        now=datetime(2026, 9, 22, 5, 30, tzinfo=timezone.utc), registrations=[_registration()],
        price_panel={"series": {}}, store_dir_override=acc)
    row = adv["challengers"][0]
    assert adv["n_emitted_this_run"] == 1, row
    assert row["decision_grid"][:2] == ["2026-09-15", "2026-09-22"]
    assert row["armed_owner_decided_session"] == "2026-09-22"
    assert row["latest_realised_session"] == "2026-09-21"
    assert adv["n_forfeitures_recorded_this_run"] == 0
    _write_store("2026-09-29")
    later = CFA.assess_registration(registration=_registration(), series={},
                                    now="2026-09-30T05:30:00+00:00", today="2026-09-30",
                                    store_dir_override=acc)
    mat = later["maturation"][0]
    assert mat["decision_session"] == "2026-09-22"
    assert mat["matured"] is True and mat["maturity_session"] == "2026-09-29"
    assert later["backfilled"] is False


# --------------------------------------------------------------------------- #
# 20-23. OWNERSHIP AND SAFETY
# --------------------------------------------------------------------------- #
def test_20_the_canonical_runtime_owns_the_flow():
    assert '"fx_carry_cadence_prospective_decision"' in RUNTIME_SRC
    assert "FXR.advance(" in RUNTIME_SRC
    lock = RUNTIME_SRC.index("RL.acquire_path(_lock_file()")
    mine = RUNTIME_SRC.index("fx_carry_cadence_prospective_decision")
    accrual = RUNTIME_SRC.index("advance_canonical_forward_accrual(")
    assert lock < mine < accrual
    assert "Register-ScheduledTask" not in FXR_SRC


def test_21_no_order_promotion_or_registration_path():
    for token in ("place_order", "submit_order", "create_order", "apply_fill", "promote_model(",
                  "activate_sleeve(", "approve_proposal(", "adopt_prospective_freeze(",
                  "register_forward_challenger(", "import requests", "import httpx"):
        assert token not in FXR_SRC, token
    for key in ("creates_orders", "creates_fills", "allocates_capital", "promotes_model",
                "backfill_allowed", "writes_the_frozen_r41_store"):
        assert FXR.SAFETY[key] is False


def test_22_the_forward_store_is_never_the_frozen_store(root):
    assert AR.research_root() in FXR.curves_dir().parents
    assert Path(FXR.curves_dir()) != Path(FXR.FROZEN_CURVES_DIR)


def test_23_a_child_refuses_a_store_it_was_not_given(root, monkeypatch):
    monkeypatch.setenv(FXR.R41_ROOT_ENV, str(root / "elsewhere"))
    assert FXR.refresh_store_in_child("scope", SCOPE)["reason"] == \
        "CHILD_ENVIRONMENT_NOT_THE_STAGING_ROOT"
