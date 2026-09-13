r"""Release 62.3 - the SAME-SESSION decision boundary, SIGNED forward evidence,
and the currency of the marks a prospective book is entered against.

WHAT THIS RELEASE HAD TO FIX BEFORE ``REVERSED_SPY_PUT_CALL_SKEW_H5`` COULD
ACCRUE HONEST TRUE_FORWARD EVIDENCE AT ALL

1. THE EMISSION CLOCK ASSUMED THE DECISION PRE-DATED THE SESSION.
   ``api.canonical_forward_accrual`` encoded one rule inline - emit strictly
   before the session or FORFEIT. That is right for a freeze formed from data
   complete before the session opens, and it makes a strategy whose declared
   inputs arrive DURING the session forfeit every session for ever. The rule now
   belongs to ``engine.forward_emission_window``, which answers from a DECLARED
   boundary; an undeclared challenger keeps the strictest rule unchanged.

2. THE KERNEL DELETED SHORT LEGS.
   ``make_inception_record`` kept a weight only when ``fv > 1e-9``, so
   ``{"SPY": -1.0}`` became an EMPTY book holding 100 % cash: a short strategy
   measured as flat, at zero cost, with no error anywhere. Coverage had the same
   shape - ``priced_share`` divided by the SIGNED sum, so a fully priced short
   reported 0 % coverage.

3. COVERAGE WAS NOT CURRENCY.
   A book is "100 % covered" at its own latest session even when that session is
   three months old. SPY - this challenger's only instrument and the estate's
   declared benchmark - was priced to 2026-06-22 while 1008 of 1092 panel
   tickers reached 2026-09-11, so a September decision would have been entered
   against a June close and nothing would have objected.

NOTHING HERE BACKFILLS. A session whose window closed without a decision is
MISSED, permanently, and every test below that could create one asserts it is
refused instead.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.api import canonical_forward_accrual as CFA
from paper_trader.api import forward_challenger_registry as FCR
from paper_trader.engine import forward_emission_window as EW
from paper_trader.engine import shadow_portfolio_evidence as K

REPO = Path(__file__).resolve().parents[1]

CID = "REVERSED_SPY_PUT_CALL_SKEW_H5"
SESSION = "2026-09-14"
MATURITY = "2026-09-21"
IDENTITY_HASH = "4e2d1027f9aa2a664f70befcc0c0f27ead000000000000000000000000000000"
FREEZE_HASH = "3bd21160a42be246150e37ff1c7cd07277a0810a92ab312ca6254f855670c471"

#: The declared boundary: 15:45 ET snapshot, 16:00 ET entry mark.
CUTOFF_ET = (15, 45)
MARK_ET = (16, 0)
#: The same two instants in UTC on 2026-09-14 (EDT, UTC-4).
BEFORE_CUTOFF = "2026-09-14T19:44:59+00:00"
AT_CUTOFF = "2026-09-14T19:45:00+00:00"
INSIDE = "2026-09-14T19:50:00+00:00"
AT_MARK = "2026-09-14T20:00:00+00:00"
NEXT_DAY = "2026-09-15T14:00:00+00:00"

SESSIONS = ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
            "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
            "2026-09-18", "2026-09-21", "2026-09-22"]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    monkeypatch.setenv(FCR.REGISTRY_DIR_ENV, str(tmp_path / "registry"))
    monkeypatch.setenv(CFA.STORE_DIR_ENV, str(tmp_path / "accrual"))
    return {"research": tmp_path / "research",
            "registry": tmp_path / "registry",
            "accrual": tmp_path / "accrual"}


def declare(**kw):
    kw.setdefault("challenger_id", CID)
    kw.setdefault("information_cutoff_et", CUTOFF_ET)
    kw.setdefault("entry_mark_et", MARK_ET)
    kw.setdefault("rebalance_cadence_sessions", 5)
    kw.setdefault("evaluation_horizon_sessions", 5)
    kw.setdefault("cost_policy", {"bps_per_side": 1.0})
    kw.setdefault("instrument_scope", ["SPY"])
    kw.setdefault("identity", {"identity_hash": IDENTITY_HASH,
                               "freeze_record_hash": FREEZE_HASH,
                               "model_spec_hash": "spec", "challenger_id": CID})
    return PD.declare_policy(**kw)


def freeze(session=SESSION, weights=None, now=INSIDE, **kw):
    kw.setdefault("source_data_hash", "src")
    kw.setdefault("feature_state_hash", "feat")
    kw.setdefault("feature_observed_at", AT_CUTOFF)
    return PD.freeze_decision(challenger_id=CID, eligible_session=session,
                              weights=weights or {"SPY": -1.0}, now=now, **kw)


def registration(**kw):
    """A registration shaped exactly as the canonical registrar writes one."""
    rec = {
        "schema_version": FCR.SCHEMA_VERSION, "owner": FCR.COMPOSITION_OWNER,
        "registered_at": "2026-09-11T16:38:15+00:00",
        "registration_session": "2026-09-11",
        "identity": {"challenger_id": CID, "freeze_id": "F1",
                     "asset_class": "US_ETF", "identity_hash": IDENTITY_HASH,
                     "release": "ALPHA_RECOVERY_OFFENSIVE",
                     "freeze_record_hash": FREEZE_HASH,
                     "inception": "2026-09-11", "horizon_sessions": 5},
        "challenger_id": CID, "freeze_id": "F1",
        "freeze_record_hash": FREEZE_HASH, "model_spec_hash": "spec",
        "asset_class": "US_ETF", "instrument_scope": ["SPY"],
        "horizon_sessions": 5,
        "lifecycle_at_registration": {"lifecycle_state": "ACTIVE",
                                      "adoptable": True},
        "challenger_class": "FORWARD_SIGNAL_CANONICAL",
        "observation_clock": {
            "state": FCR.CLOCK_RESOLVED,
            "calendar_owner": FCR.CAL_EXCHANGE_NYSE,
            "effective_from_session": "2026-09-11",
            "first_eligible_observation_session": SESSION,
            "next_expected_maturity_session": MATURITY},
        "predictions_emitted": 0, "matured_observations": 0,
        "backfilled": False,
    }
    rec.update(kw)
    return rec


def panel(spy_last="2026-09-11", other_last="2026-09-11"):
    """A panel where SPY's currency can be set independently of the rest."""
    def ser(last):
        d = [s for s in SESSIONS if s <= last]
        return {"dates": d, "adj": [100.0 + i for i in range(len(d))]}
    return {"SPY": ser(spy_last), "AAPL": ser(other_last)}


# =========================================================================== #
# 1-2. NOTHING BEFORE THE FIRST ELIGIBLE SESSION; THE 15:45 DECISION IS LEGAL
# =========================================================================== #
def test_01_no_observation_exists_before_the_first_eligible_session(stores):
    declare()
    reg = registration()
    for now in ("2026-09-11T20:00:00+00:00", "2026-09-13T12:00:00+00:00"):
        a = CFA.assess_registration(registration=reg, series=panel(),
                                    today=now[:10], now=now)
        assert a["predictions_emitted"] == 0
        assert a["forfeitures_recorded"] == 0
        earlier = [c for c in a["cells"] if c["decision_session"] < SESSION]
        assert earlier == [], "no decision session precedes the first eligible one"
    assert PD.list_decisions(CID) == []


def test_02_the_same_session_1545_decision_is_legitimate(stores):
    declare()
    cls = EW.classify(policy=PD.load_policy(CID)["emission_window"],
                      session=SESSION, now=AT_CUTOFF)
    assert cls["state"] == EW.WINDOW_OPEN and cls["emittable"] is True
    out = freeze(now=AT_CUTOFF)
    assert out["outcome"] == PD.FROZEN and out["frozen"] is True
    rec = out["decision"]
    assert rec["eligible_session"] == SESSION
    assert rec["declared_information_cutoff"] == AT_CUTOFF
    assert rec["emission_window_closes_at"] == AT_MARK
    assert rec["weights"] == {"SPY": -1.0}
    assert rec["is_true_forward"] is True and rec["backfilled"] is False


def test_02b_the_window_is_exactly_the_declared_interval(stores):
    declare()
    pol = PD.load_policy(CID)["emission_window"]
    states = {n: EW.classify(policy=pol, session=SESSION, now=n)["state"]
              for n in (BEFORE_CUTOFF, AT_CUTOFF, INSIDE, AT_MARK, NEXT_DAY)}
    assert states[BEFORE_CUTOFF] == EW.WINDOW_NOT_OPEN
    assert states[AT_CUTOFF] == EW.WINDOW_OPEN
    assert states[INSIDE] == EW.WINDOW_OPEN
    assert states[AT_MARK] == EW.WINDOW_CLOSED      # the entry mark itself
    assert states[NEXT_DAY] == EW.WINDOW_CLOSED


# =========================================================================== #
# 3-4. TOO EARLY, AND CONTAMINATED
# =========================================================================== #
def test_03_a_pre_cutoff_decision_is_rejected(stores):
    declare()
    out = freeze(now=BEFORE_CUTOFF)
    assert out["outcome"] == PD.REFUSED_TOO_EARLY
    assert out["frozen"] is False
    assert PD.load_decision(CID, SESSION) is None


def test_04_information_observed_after_the_cutoff_is_rejected(stores):
    declare()
    out = freeze(now=INSIDE, feature_observed_at="2026-09-14T19:46:00+00:00")
    assert out["outcome"] == PD.REFUSED_CONTAMINATED
    assert out["frozen"] is False
    assert PD.load_decision(CID, SESSION) is None


def test_04b_an_input_with_no_observation_instant_fails_closed(stores):
    declare()
    out = freeze(now=INSIDE, feature_observed_at=None)
    assert out["outcome"] == PD.REFUSED_CONTAMINATED
    assert out["information_cutoff_check"]["failed_closed"] is True


# =========================================================================== #
# 5-6. NO RETROSPECTIVE CREATION, NO BACKFILL
# =========================================================================== #
def test_05_retrospective_creation_for_a_missed_session_is_rejected(stores):
    declare()
    out = freeze(now=NEXT_DAY)
    assert out["outcome"] == PD.REFUSED_TOO_LATE
    assert out["frozen"] is False and out["backfill_refused"] is True
    assert PD.load_decision(CID, SESSION) is None


def test_06_nothing_in_this_release_backfills(stores):
    declare()
    # A session that closed WITHOUT a decision stays without one for ever.
    assert freeze(session="2026-09-08", now=INSIDE)["outcome"] == PD.REFUSED_TOO_LATE
    assert freeze(session="2026-09-09", now=NEXT_DAY)["outcome"] == PD.REFUSED_TOO_LATE
    assert PD.list_decisions(CID) == []
    for mod in (PD, EW, CFA):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        code = "\n".join(l for l in src.splitlines()
                         if not l.strip().startswith("#"))
        assert "backfill(" not in code
    assert CFA.load_canonical_forward_accrual(
        registrations=[], price_panel={"series": {}})["backfill_allowed"] is False


# =========================================================================== #
# 7-8. FIRST WRITE WINS
# =========================================================================== #
def test_07_a_duplicate_identical_decision_is_idempotent(stores):
    declare()
    first = freeze()
    assert first["outcome"] == PD.FROZEN
    body = Path(first["path"]).read_text(encoding="utf-8")
    again = freeze(now="2026-09-14T19:55:00+00:00")     # later, same decision
    assert again["outcome"] == PD.ALREADY_FROZEN
    assert again["frozen"] is True and again["idempotent"] is True
    assert Path(first["path"]).read_text(encoding="utf-8") == body
    assert len(PD.list_decisions(CID)) == 1


def test_08_a_conflicting_second_decision_is_rejected(stores):
    declare()
    first = freeze(weights={"SPY": -1.0})
    body = Path(first["path"]).read_text(encoding="utf-8")
    clash = freeze(weights={"SPY": 1.0})
    assert clash["outcome"] == PD.REFUSED_CONFLICT
    assert clash["frozen"] is False
    assert clash["held_weights"] == {"SPY": -1.0}
    assert clash["offered_weights"] == {"SPY": 1.0}
    assert Path(first["path"]).read_text(encoding="utf-8") == body
    assert len(PD.list_decisions(CID)) == 1


# =========================================================================== #
# 9-12. SIGNED / LONG-SHORT EVIDENCE
# =========================================================================== #
def record(weights, bps=1.0, session="2026-09-14"):
    return K.make_inception_record(
        challenger_id=CID, label="l", family="f", strategy_identity={},
        weights=weights, inception_session=session,
        inception_timestamp="%sT20:00:00+00:00" % session,
        starting_capital=100000.0, pit_input_identity={},
        cost_bps_per_side=bps, valuation_source=K.VALUATION_PRICE_PANEL)


def moving_panel(pct):
    return {"SPY": {"dates": ["2026-09-14", "2026-09-15"],
                    "adj": [100.0, 100.0 * (1.0 + pct)]}}


def test_09_the_short_return_sign_is_correct(stores):
    up = K.accrue_forward(record={"SPY": -1.0} and record({"SPY": -1.0}),
                          price_series=moving_panel(+0.02))
    assert up["gross_cumulative_return"] == pytest.approx(-0.02, abs=1e-9)
    down = K.accrue_forward(record=record({"SPY": -1.0}),
                            price_series=moving_panel(-0.02))
    assert down["gross_cumulative_return"] == pytest.approx(+0.02, abs=1e-9)
    # ... and the long leg is the mirror image, not the same number.
    lng = K.accrue_forward(record=record({"SPY": 1.0}),
                           price_series=moving_panel(+0.02))
    assert lng["gross_cumulative_return"] == pytest.approx(+0.02, abs=1e-9)


def test_09b_a_short_book_survives_the_record_at_all(stores):
    r = record({"SPY": -1.0})
    assert r["weights"] == {"SPY": -1.0}, "the short leg must not be deleted"
    assert r["position_count"] == 1
    assert r["gross_exposure"] == 1.0
    assert r["net_exposure"] == -1.0
    assert r["has_short_leg"] is True
    assert r["short_positions"] == ["SPY"]


def test_09c_gross_uses_absolute_weight_and_net_is_explicit(stores):
    ls = record({"A": 0.5, "B": -0.5})
    assert ls["gross_exposure"] == 1.0
    assert ls["net_exposure"] == 0.0
    assert K.gross_exposure({"A": 0.5, "B": -0.5}) == 1.0
    assert K.net_exposure({"A": 0.5, "B": -0.5}) == 0.0


def test_10_opening_and_closing_a_short_both_cost_a_positive_amount(stores):
    short = record({"SPY": -1.0})
    long_ = record({"SPY": 1.0})
    cm = short["cost_model"]
    assert cm["entry_cost_weight"] > 0
    assert cm["entry_cost_usd"] > 0
    assert cm["entry_cost_weight"] == long_["cost_model"]["entry_cost_weight"]
    # closing costs the same again: the round trip is twice the gross
    assert cm["round_trip_cost_weight"] == pytest.approx(
        2 * cm["entry_cost_weight"], rel=1e-12)
    assert K.turnover_cost_weight(-1.0, 1.0) > 0
    assert K.turnover_cost_weight(1.0, -1.0) > 0


def test_10b_a_negative_transaction_cost_is_unreachable(stores):
    for traded in (-5.0, -1.0, 0.0, 1.0, 5.0):
        for bps in (-10.0, 0.0, 1.0, 12.5):
            assert K.turnover_cost_weight(traded, bps) >= 0.0


def test_11_a_long_to_short_reversal_costs_on_absolute_turnover(stores):
    legs = K.implied_turnover([record({"SPY": 1.0}, session="2026-09-14"),
                               record({"SPY": -1.0}, session="2026-09-21")])
    leg = legs["legs"][0]
    assert leg["two_way_traded_weight"] == 2.0, "sell the long AND open the short"
    assert leg["one_way_turnover"] == 1.0
    assert leg["transition_cost_weight"] > 0
    # ... strictly more than a same-signed move of the same net size
    hold = K.implied_turnover([record({"SPY": 1.0}, session="2026-09-14"),
                               record({"SPY": 1.0}, session="2026-09-21")])
    assert hold["legs"][0]["transition_cost_weight"] == 0.0
    assert leg["transition_cost_weight"] > hold["legs"][0]["transition_cost_weight"]


def test_12_existing_long_only_evidence_is_unchanged(stores):
    """Every number a long-only book produced before the signed vocabulary."""
    r = record({"AAA": 0.6, "BBB": 0.4})
    assert r["invested_weight"] == 1.0
    assert r["cash_weight"] == 0.0
    assert r["position_count"] == 2
    assert r["has_short_leg"] is False
    assert r["cost_model"]["entry_cost_weight"] == pytest.approx(0.0001)
    # gross and net coincide for a long-only book, which is why nothing moved
    assert r["gross_exposure"] == r["net_exposure"] == r["invested_weight"]
    series = {"AAA": {"dates": ["2026-09-14", "2026-09-15"], "adj": [10.0, 11.0]},
              "BBB": {"dates": ["2026-09-14", "2026-09-15"], "adj": [20.0, 20.0]}}
    acc = K.accrue_forward(record=r, price_series=series)
    assert acc["gross_cumulative_return"] == pytest.approx(0.06, abs=1e-9)
    assert acc["sessions_scored"] == 1
    assert CFA.priced_share({"AAA": 0.6, "BBB": 0.4}, series, "2026-09-15") == 1.0


def test_12b_a_record_frozen_before_the_split_still_scores(stores):
    """An emission already in the store has no ``gross_exposure`` key."""
    r = record({"AAA": 1.0})
    del r["gross_exposure"]
    del r["net_exposure"]
    acc = K.accrue_forward(
        record=r, price_series={"AAA": {"dates": ["2026-09-14", "2026-09-15"],
                                        "adj": [10.0, 11.0]}})
    assert acc["sessions_scored"] == 1
    assert acc["gross_cumulative_return"] == pytest.approx(0.10, abs=1e-9)


def test_12c_a_fully_priced_short_is_fully_covered(stores):
    series = {"SPY": {"dates": ["2026-09-14"], "adj": [100.0]}}
    assert CFA.priced_share({"SPY": -1.0}, series, "2026-09-14") == 1.0
    assert CFA.priced_share({"SPY": 1.0}, series, "2026-09-14") == 1.0


# =========================================================================== #
# 13-14. REGISTRATION AND ACCRUAL GRANT NOTHING
# =========================================================================== #
def test_13_registration_cannot_promote_a_model(stores):
    declare()
    freeze()
    out = CFA.load_canonical_forward_accrual(
        registrations=[registration()], price_panel={"series": panel()})
    assert out["automatic_promotion_allowed"] is False
    s = out["safety"]
    assert s["promoted_model"] is False
    assert s["automatic_model_promotion_allowed"] is False
    assert s["activated_sleeve"] is False
    assert PD.SAFETY["promotes_model"] is False
    assert "PROMOTION_READY" not in FCR.EVIDENCE_STATES


def test_14_registration_cannot_allocate_capital(stores):
    declare()
    freeze()
    out = CFA.load_canonical_forward_accrual(
        registrations=[registration()], price_panel={"series": panel()})
    s = out["safety"]
    for flag in ("allocated_capital", "changed_holdings", "changed_cash",
                 "changed_nav", "created_proposal", "approved_anything"):
        assert s[flag] is False, flag
    assert PD.SAFETY["allocates_capital"] is False
    rec = PD.load_decision(CID, SESSION)
    assert rec["safety"]["allocates_capital"] is False
    assert "capital" not in json.dumps(rec.get("weights"))


# =========================================================================== #
# 15-16. THE RESOLVER READS; IT NEVER COMPUTES
# =========================================================================== #
def test_15_the_resolver_cannot_calculate_the_signal(stores):
    src = Path(CFA.__file__).read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines()
                     if not l.strip().startswith("#"))
    # CALL shapes, not bare words: the module legitimately NAMES the things it
    # refuses ("never_computes_a_zscore"), and a substring scan that tripped on
    # its own prohibition would be testing the spelling of a comment.
    for tok in ("_implied_vol(", "black_scholes(", "_z(", "zscore(",
                "get_range_csv(", "timeseries.get_range", "metadata.get_cost",
                "np.sign(", ".rolling(", ".shift("):
        assert tok not in code, "the accrual owner must not contain %r" % tok
    imports = "\n".join(l for l in src.splitlines()
                        if l.lstrip().startswith(("import ", "from ")))
    for tok in ("options_acquisition", "options_surface", "reversed_skew",
                "numpy", "pandas", "urllib", "requests"):
        assert tok not in imports, "the accrual owner must not import %r" % tok
    assert CFA.RESOLVER_CONTRACT["never_computes_a_feature"] is True
    assert CFA.RESOLVER_CONTRACT["never_chooses_a_position"] is True


def test_16_a_missing_decision_fails_closed(stores):
    declare()                                   # policy, but NO decision
    reg = registration()
    a = CFA.assess_registration(registration=reg, series=panel(),
                                today=SESSION, now=INSIDE)
    cell = next(c for c in a["cells"] if c["decision_session"] == SESSION)
    assert cell["state"] == CFA.ACC_NOT_DUE
    assert cell["reason"] == CFA.NOT_DUE_AWAITING_NEW_FREEZE
    assert a["predictions_emitted"] == 0
    # ... and with no POLICY either, it is blocked rather than assumed
    PD.policy_path(CID).unlink()
    b = CFA.assess_registration(registration=reg, series=panel(),
                                today=SESSION, now=INSIDE)
    assert b["state"] == CFA.ACC_DATA_BLOCKED
    assert b["latest_blocker"] == CFA.BLOCK_SPEC_UNRESOLVABLE


def test_16b_before_the_cutoff_the_state_is_awaiting_not_forfeited(stores):
    """The defect this release exists to remove."""
    declare()
    a = CFA.assess_registration(registration=registration(), series=panel(),
                                today=SESSION, now=BEFORE_CUTOFF)
    cell = next(c for c in a["cells"] if c["decision_session"] == SESSION)
    assert cell["state"] == CFA.ACC_NOT_DUE
    assert cell["reason"] == CFA.NOT_DUE_AWAITING_DECISION_BOUNDARY
    assert cell["state"] != CFA.ACC_FORFEITED


def test_16c_an_undeclared_release_keeps_the_strictest_rule(stores):
    """R58 and every other existing registration are untouched."""
    r58 = registration()
    r58["identity"] = {**r58["identity"], "release": "R58"}
    pol = CFA.emission_policy(r58)
    assert pol["boundary"] == EW.BOUNDARY_PRIOR_SESSION
    assert pol["declaration_state"] == EW.DECL_ABSENT
    bounds = EW.window_bounds(pol, SESSION)
    assert bounds["opens_at"] is None
    assert bounds["closes_at"] == "2026-09-14T00:00:00+00:00"


def test_16d_a_malformed_declaration_never_widens_the_window(stores):
    for bad in ({"boundary": "WHATEVER"},
                {"boundary": EW.BOUNDARY_SAME_SESSION},
                {"boundary": EW.BOUNDARY_SAME_SESSION,
                 "information_cutoff_et": (16, 0), "entry_mark_et": (15, 45)}):
        pol = EW.normalise_policy(bad)
        assert pol["boundary"] == EW.BOUNDARY_PRIOR_SESSION
        assert pol["declaration_state"] != EW.DECL_ACCEPTED
        assert pol["widened_by_this_module"] is False


# =========================================================================== #
# 17. THE MARKS MUST BE CURRENT, NOT MERELY PRESENT
# =========================================================================== #
def test_17_a_stale_spy_blocks_the_decision(stores):
    declare()
    freeze()
    stale = panel(spy_last="2026-06-22", other_last="2026-09-11")
    stale["SPY"] = {"dates": ["2026-06-22"], "adj": [100.0]}
    a = CFA.assess_registration(registration=registration(), series=stale,
                                today=SESSION, now=INSIDE)
    cell = next(c for c in a["cells"] if c["decision_session"] == SESSION)
    assert cell["state"] == CFA.ACC_DATA_BLOCKED
    assert cell["reason"] == CFA.BLOCK_STALE_MARK
    assert cell["book_latest_session"] == "2026-06-22"
    assert cell["panel_latest_session_before_decision"] == "2026-09-11"


def test_17b_a_current_spy_passes_the_same_gate(stores):
    declare()
    freeze()
    a = CFA.assess_registration(registration=registration(), series=panel(),
                                today=SESSION, now=INSIDE)
    cell = next(c for c in a["cells"] if c["decision_session"] == SESSION)
    assert cell["state"] == CFA.ACC_DUE
    assert cell["priced_share"] == 1.0


def test_17c_coverage_and_currency_are_different_questions(stores):
    """A book can be 100 % covered and still be stale."""
    stale = {"SPY": {"dates": ["2026-06-22"], "adj": [100.0]},
             "AAPL": {"dates": SESSIONS[:4], "adj": [1.0, 2.0, 3.0, 4.0]}}
    assert CFA.priced_share({"SPY": -1.0}, stale, "2026-06-22") == 1.0
    assert CFA.panel_session_before(SESSION, stale) == "2026-09-11"


def test_17d_the_benchmark_is_fetched_into_the_owned_trailing_panel():
    """The refresh owner must put the declared benchmark in the panel."""
    src = (REPO / "api" / "alpha_target.py").read_text(encoding="utf-8")
    assert "BENCHMARK_TICKER" in src
    assert "panel_extra" in src
    assert "build_owned_panel_rows({**series, **panel_extra})" in src
    # ... and it must NOT join the universe that decides coverage or momentum
    assert "series[bench_tk]" not in src


# =========================================================================== #
# 18. NO ORDER OR FILL PATH IS REACHABLE
# =========================================================================== #
def test_18_no_order_or_fill_path_is_reachable(stores):
    for mod in (PD, EW, CFA, K):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        code = "\n".join(l for l in src.splitlines()
                         if not l.strip().startswith("#"))
        # Again CALL shapes: every one of these modules declares
        # ``broker_enabled: False``, and a bare "broker" scan would fail on the
        # flag that states the guarantee.
        for tok in ("create_order(", "submit_order(", "apply_fill(",
                    "place_order(", "record_fill(", "paper_orders.json",
                    "paper_fills.json", "import paper_trading_desk",
                    "import operational_book"):
            assert tok not in code, "%s reaches %s" % (mod.__name__, tok)
    declare()
    freeze()
    out = CFA.load_canonical_forward_accrual(
        registrations=[registration()], price_panel={"series": panel()})
    for flag in ("created_orders", "created_order_plan", "created_fills",
                 "broker_enabled", "automation_enabled"):
        assert out["safety"][flag] is False, flag
    assert PD.SAFETY["creates_orders"] is False
    assert PD.SAFETY["creates_fills"] is False


# =========================================================================== #
# 19. A RESTART CHANGES NOTHING
# =========================================================================== #
def test_19_a_restart_preserves_the_immutable_decision_and_evidence(stores):
    declare()
    first = freeze()
    path = Path(first["path"])
    before = path.read_text(encoding="utf-8")
    pol_before = PD.policy_path(CID).read_text(encoding="utf-8")

    reg = registration()
    run1 = CFA.advance_canonical_forward_accrual(
        registrations=[reg], price_panel={"series": panel()},
        today=SESSION, now=INSIDE, execute=True)
    emitted = run1["predictions_emitted_total"]
    assert emitted == 1

    # "Restart": every module-level cache is irrelevant because the state is on
    # disk. Re-running must resolve to the SAME records, not new ones.
    run2 = CFA.advance_canonical_forward_accrual(
        registrations=[reg], price_panel={"series": panel()},
        today=SESSION, now=INSIDE, execute=True)
    assert run2["predictions_emitted_total"] == emitted
    assert run2["n_emitted_this_run"] == 0
    # The cell now reads EMITTED, so it is never offered for emission again -
    # which is why nothing is even attempted, let alone duplicated.
    cell = next(c for c in run2["challengers"][0]["cells"]
                if c["decision_session"] == SESSION)
    assert cell["state"] == CFA.ACC_EMITTED
    assert run2["n_forfeitures_recorded_this_run"] == 0

    assert path.read_text(encoding="utf-8") == before
    assert PD.policy_path(CID).read_text(encoding="utf-8") == pol_before
    rows = CFA.load_emissions(IDENTITY_HASH)
    assert len(rows) == 1
    assert rows[0]["decision_session"] == SESSION
    assert rows[0]["backfilled"] is False
    # the emitted book is the SHORT one the research owner froze
    assert rows[0]["prediction"]["weights"] == {"SPY": -1.0}
    assert rows[0]["prediction"]["gross_exposure"] == 1.0
    assert rows[0]["prediction"]["net_exposure"] == -1.0


def test_19b_the_emission_binds_the_registered_identity(stores):
    declare()
    freeze()
    CFA.advance_canonical_forward_accrual(
        registrations=[registration()], price_panel={"series": panel()},
        today=SESSION, now=INSIDE, execute=True)
    row = CFA.load_emissions(IDENTITY_HASH)[0]
    ident = row["prediction"]["strategy_identity"]
    assert ident["identity_hash"] == IDENTITY_HASH
    assert ident["freeze_record_hash"] == FREEZE_HASH
    assert ident["release"] == "ALPHA_RECOVERY_OFFENSIVE"
    assert row["prediction"]["pit_input_identity"][
        "no_information_after_the_freeze_was_used"] is True


def test_19c_a_decision_whose_hash_does_not_match_the_registration_is_refused(
        stores):
    declare(identity={"identity_hash": IDENTITY_HASH,
                      "freeze_record_hash": "a" * 64,
                      "model_spec_hash": "spec", "challenger_id": CID})
    freeze()
    a = CFA.assess_registration(registration=registration(), series=panel(),
                                today=SESSION, now=INSIDE)
    assert a["state"] == CFA.ACC_INTEGRITY_BLOCKED
    assert a["latest_blocker"] == CFA.INTEGRITY_HASH_MISMATCH
