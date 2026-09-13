"""Regressions for the preregistered SEC Form 8-K Item-code event axis.

These tests exist to make the honesty claims CHECKABLE rather than asserted:
that the census and the event construction never read a price, that the frozen
constants are the frozen constants, that no second scorer was written, that
the tradable window starts strictly after publication, and that each measured
EDGAR trap stays fixed.

Everything here is pure and local. No network, no store, no live path.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.alpha_recovery import control_block_alpha as CBA
from alpha_agent.alpha_recovery import event_8k_alpha as A
from alpha_agent.alpha_recovery import event_8k_data as D
from alpha_agent.alpha_recovery import event_8k_events as EV

REPO = Path(__file__).resolve().parents[1]
PREREG = REPO / "research" / "preregistration" / "EVENT_8K_ITEM_PREREGISTRATION.md"

#: sha256 of the preregistration BLOB as committed at 998a3f9, before any real
#: forward return for this family existed. Read from git rather than from the
#: working tree, so the test cannot be satisfied by a later edit.
PREREG_SHA256 = "9858805c89321e3ef9b91582918119e88bdc09c3e19bccc8a6551b765538ea54"
PREREG_COMMIT = "998a3f9"


# --------------------------------------------------------------------------- #
# The preregistration was not moved after the results
# --------------------------------------------------------------------------- #
def test_preregistration_blob_is_unchanged_since_it_was_frozen():
    import hashlib
    import subprocess
    out = subprocess.run(
        ["git", "-C", str(REPO), "cat-file", "blob",
         "%s:research/preregistration/EVENT_8K_ITEM_PREREGISTRATION.md" % PREREG_COMMIT],
        capture_output=True)
    if out.returncode != 0:
        pytest.skip("git object not available in this checkout")
    assert hashlib.sha256(out.stdout).hexdigest() == PREREG_SHA256
    # a checkout may rewrite line endings; the CONTENT must still be the frozen one
    wt = PREREG.read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(wt).hexdigest() == PREREG_SHA256, \
        "the working-tree preregistration differs from the one that was frozen"


def test_runner_points_at_the_frozen_commit():
    assert A.PREREGISTRATION_COMMIT == PREREG_COMMIT
    assert A.PREREGISTRATION.endswith("EVENT_8K_ITEM_PREREGISTRATION.md")


def test_frozen_family_shape():
    """One cell, three horizons, m = 3 - and no cell added after results."""
    assert EV.CELLS == (EV.CELL_RESTRUCTURING_IMPAIRMENT,)
    assert A.HORIZONS_FROZEN == (5, 21, 63)
    assert A.declared_m() == 3
    assert EV.EVENT_WINDOW_START == "2011-07-01"


# --------------------------------------------------------------------------- #
# No return and no price before the preregistration
# --------------------------------------------------------------------------- #
def test_the_census_and_the_events_never_read_a_price():
    for mod in (D, EV):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert '["price"]' not in src and "forward_compound" not in src, \
            "%s must not touch prices or returns" % mod.__name__


def test_no_second_scorer_was_written():
    for mod in (A, D, EV):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "def run_cell" not in src
        assert "def _book_returns" not in src
        assert "def nw_tstat" not in src


def test_the_event_block_does_not_overwrite_the_intensity_control():
    """The owned 8-K intensity block is the control this family must beat.
    Scoring the event under the same key would silently delete it."""
    assert A.BLOCK != A.INTENSITY_CONTROL
    ds = {"blocks": {d: None for d in ("PRICE_RETURN_STATE", "MOMENTUM",
                                       "DISCLOSURE_INTENSITY_LANGUAGE")}}
    dims = A.control_dims(ds)
    assert "DISCLOSURE_INTENSITY_LANGUAGE" in dims
    assert A.BLOCK not in dims


# --------------------------------------------------------------------------- #
# Frozen constants
# --------------------------------------------------------------------------- #
def test_cost_ladder_and_stopping_rules_are_the_estates():
    assert A.COST_LADDER_BPS == (1.0, 2.0, 5.0)
    assert A.DESK_EQUITY_COST_BPS == pytest.approx(12.5)
    assert A.MIN_DATE_COVERAGE == 0.95
    assert A.MAX_UNCOVERED_SHARE == 0.20


def test_the_family_fits_the_existing_primary_budget():
    from alpha_agent import alpha_recovery as AR
    assert A.declared_m() <= AR.FAMILY_PRIMARY_MAX


def test_every_frozen_cell_carries_a_sign():
    for spec in EV.CELL_SPECS.values():
        assert spec["sign"] in (-1, 1)


def test_restructuring_impairment_excludes_earnings_and_completed_deals():
    spec = EV.CELL_SPECS[EV.CELL_RESTRUCTURING_IMPAIRMENT]
    assert set(spec["items_any"]) == {"2.05", "2.06"}
    assert set(spec["exclude_cofiled"]) == {"2.01", "2.02"}
    assert spec["sign"] == EV.SIGN_NEGATIVE


# --------------------------------------------------------------------------- #
# TRAP - two Item vocabularies
# --------------------------------------------------------------------------- #
def test_only_modern_item_tokens_are_items():
    modern, legacy = D.parse_items("2.02,9.01")
    assert modern == ("2.02", "9.01") and legacy == ()
    modern, legacy = D.parse_items("5,7")
    assert modern == () and legacy == ("5", "7"), \
        "legacy item 5 (Other Events) must never become modern 5.xx"
    assert D.parse_items("") == ((), ())
    assert D.parse_items(None) == ((), ())


def test_cofiling_rules_match_exactly():
    assert D.matches(("2.05", "9.01"), ("2.05", "2.06"), ("2.02",))
    assert not D.matches(("2.05", "2.02"), ("2.05", "2.06"), ("2.02",))
    assert not D.matches(("9.01",), ("2.05", "2.06"), ())


def test_successor_registration_forms_are_not_current_reports():
    assert D.FORMS == ("8-K", "8-K/A")
    assert "8-K12B" not in D.FORMS


# --------------------------------------------------------------------------- #
# The acceptance instant
# --------------------------------------------------------------------------- #
DATES = np.array(["2019-01-07", "2019-01-08", "2019-01-09", "2019-01-10",
                  "2019-01-11", "2019-01-14"], dtype="datetime64[ns]")


def test_session_class_uses_eastern_wall_clock():
    assert D.session_class(DATES, "2019-01-09T13:00:00.000Z") == "PRE_OPEN"     # 08:00 ET
    assert D.session_class(DATES, "2019-01-09T18:00:00.000Z") == "INTRADAY"     # 13:00 ET
    assert D.session_class(DATES, "2019-01-09T21:30:00.000Z") == "AFTER_CLOSE"  # 16:30 ET
    assert D.session_class(DATES, "2019-01-12T18:00:00.000Z") == "NON_SESSION_DAY"
    assert D.session_class(DATES, "") == "UNREADABLE"


def test_a_filing_before_the_calendar_is_refused_not_stacked_on_day_one():
    """searchsorted maps every earlier instant to position 0."""
    assert D.decision_session(DATES, "2005-03-01T18:00:00.000Z") is None
    assert D.decision_session(DATES, "2019-01-09T18:00:00.000Z") == 2
    assert D.decision_session(DATES, "2019-01-09T22:30:00.000Z") == 3


def _stream_row(items, when, form="8-K", cik="111", acc="0001"):
    return {"issuer_cik": cik, "form": form, "filing_date": when[:10],
            "acceptance_utc": when, "accession": acc, "items": list(items),
            "legacy_items": [], "items_raw_present": True, "primary_document": "x.htm"}


def test_eligible_originals_drops_amendments_ineligibles_and_cofiled_exclusions():
    E = {"symbols": np.array(["AAA", "BBB"]), "dates": DATES}
    elig = np.ones((2, len(DATES)), dtype=bool)
    elig[1, :] = False
    rows = [_stream_row(("2.05", "9.01"), "2019-01-08T18:00:00.000Z", acc="a"),
            _stream_row(("2.05",), "2019-01-08T18:00:00.000Z", form="8-K/A", acc="b"),
            _stream_row(("2.06", "2.02"), "2019-01-09T18:00:00.000Z", acc="c"),
            _stream_row(("2.06",), "2019-01-10T18:00:00.000Z", cik="222", acc="d")]
    sel = D.eligible_originals(E, elig, rows, {"111": [0], "222": [1]},
                               require_any=("2.05", "2.06"), exclude_any=("2.02",),
                               since="2019-01-01")
    assert [r["accession"] for r in sel] == ["a"]
    assert sel[0]["t"] == 1 and sel[0]["rows"] == [0]


def test_events_collapse_on_the_same_row_and_session():
    E = {"symbols": np.array(["AAA"]), "dates": DATES}
    elig = np.ones((1, len(DATES)), dtype=bool)
    rows = [_stream_row(("2.05",), "2019-01-08T15:00:00.000Z", acc="a"),
            _stream_row(("2.06",), "2019-01-08T19:00:00.000Z", acc="b"),
            _stream_row(("2.06",), "2019-01-10T19:00:00.000Z", acc="c")]
    orig = EV.EVENT_WINDOW_START
    try:
        EV.EVENT_WINDOW_START = "2019-01-01"
        built = EV.build_events(E, elig, rows=rows, cik2rows={"111": [0]}, verbose=False)
    finally:
        EV.EVENT_WINDOW_START = orig
    ev = built["events"][EV.CELL_RESTRUCTURING_IMPAIRMENT]
    assert [(e["t"], e["n_collapsed"]) for e in ev] == [(1, 2), (3, 1)]


# --------------------------------------------------------------------------- #
# Item sections
# --------------------------------------------------------------------------- #
def test_item_section_takes_the_operative_narrative_not_a_cross_reference():
    txt = ("Item 1.01 Entry. The information set forth under Item 5.02 is incorporated. "
           "Item 5.02 Departure of Directors. On May 1 the Board appointed Jane Doe as "
           "interim Chief Executive Officer. Item 9.01 Exhibits.")
    s = D.item_section(txt, "5.02")
    assert s.startswith("Item 5.02 Departure") and "interim" in s and "9.01" not in s
    assert D.item_section(txt, "2.02") == ""
    assert D.item_section(None, "5.02") == ""


# --------------------------------------------------------------------------- #
# The Item 5.02 interim-CEO rule, on the measured misreadings
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text", [
    "Item 5.02 On March 3 the Board appointed Jane Doe, the Company's Chief Financial "
    "Officer, as interim Chief Executive Officer, effective immediately.",
    "Item 5.02 The Board has named Mr. H. Smith to serve as acting President and CEO.",
    "Item 5.02 Ms. Roe will serve as Chief Executive Officer on an interim basis.",
])
def test_interim_ceo_appointments_are_read(text):
    assert EV.interim_ceo_text(text) is True


@pytest.mark.parametrize("text", [
    # a sitting CEO covering a division on an interim basis (measured: eBay 2012)
    "Item 5.02 John Donahoe, eBay's President and Chief Executive Officer, will serve as "
    "President of PayPal on an interim basis until a successor is named.",
    # a biography of past interim service
    "Item 5.02 Mr. Smith was elected a director. He served as interim Chief Executive "
    "Officer of Acme Corp. from 2008 to 2009.",
    # the interim tenure ENDS (measured: Yahoo 2012)
    "Item 5.02 Effective January 9, Tim Morse will no longer serve as interim Chief "
    "Executive Officer and President of the Company.",
    # the interim is made permanent
    "Item 5.02 Jane Doe, who has been serving as interim Chief Executive Officer since "
    "March, was appointed Chief Executive Officer.",
    # a subsidiary seat
    "Item 5.02 The Board appointed Bob Lee as interim Chief Executive Officer of the "
    "Company's Industrial segment.",
    # a restatement of an earlier announcement
    "Item 5.02 As previously announced, the Board appointed Jane Doe as interim CEO.",
    # compensation for the role, not the appointment (measured: First Solar 2011)
    "Item 5.02 The Committee approved a grant to Mr. Ahearn in connection with his "
    "appointment as interim Chief Executive Officer.",
])
def test_interim_ceo_misreadings_are_refused(text):
    assert EV.interim_ceo_text(text) is False


def test_no_document_is_none_not_false():
    assert EV.interim_ceo_text(None) is None


# --------------------------------------------------------------------------- #
# Sufficiency in the scorer's own units
# --------------------------------------------------------------------------- #
def _calendar():
    """The substrate spells its calendar as ``<U10`` strings, and the canonical
    grid compares it with a string start date - so must the test."""
    return pd.bdate_range("2011-07-01", "2020-12-31").strftime("%Y-%m-%d").values.astype("<U10")


def test_informative_periods_count_only_tested_periods_that_hold_an_event():
    dates = _calendar()
    every = np.arange(len(dates))
    full = D.oos_sufficiency(dates, every, horizons=(21, 63))
    for h in ("21", "63"):
        assert full[h]["oos_periods_with_event"] == full[h]["oos_periods"] > 0
    assert full["63"]["effective_informative_periods"] == \
        int(full["63"]["oos_periods"] * 21 / 63)
    none = D.oos_sufficiency(dates, [], horizons=(21,))
    assert none["21"]["effective_informative_periods"] == 0
    assert none["21"]["meets_floor"] is False


def test_an_event_before_the_first_test_fold_informs_nothing():
    dates = _calendar()
    early = [int(np.searchsorted(dates, "2012-03-01"))]
    assert D.oos_sufficiency(dates, early, horizons=(21,))["21"]["oos_periods_with_event"] == 0


# --------------------------------------------------------------------------- #
# THE PIT WINDOW: entry strictly after publication
# --------------------------------------------------------------------------- #
def test_the_tradable_window_starts_at_close_t_plus_one():
    """Every name jumps +1% on its decision session t (the session that
    CONTAINS publication) and +2% on t+1 (legal to trade, forgone by the
    canonical broadcast lag), then is flat. The market is flat. The frozen
    qualification input must therefore be EXACTLY zero, and each diagnostic
    must recover its own session - so neither jump can leak into the other."""
    n_i, n_t, h = 40, 420, 5
    tr = np.ones((n_i, n_t))
    events = []
    for k in range(n_i):
        t = 60 + 8 * k
        r = np.zeros(n_t)
        r[t], r[t + 1] = 0.01, 0.02
        tr[k] = np.cumprod(1.0 + r)
        events.append({"row": k, "t": t, "date": "d%d" % t, "is_13d": False})
    E = {"symbols": np.array(["S%d" % k for k in range(n_i)]),
         "dates": pd.bdate_range("2015-01-01", periods=n_t).strftime("%Y-%m-%d").values,
         "price": {"tr": tr, "spy_tr": np.ones(n_t)}}
    elig = np.ones((n_i, n_t), dtype=bool)
    out = A.event_study(E, elig, events, h=h, vol63=np.full((n_i, n_t), 0.2),
                        sign=EV.SIGN_NEGATIVE)
    assert out["state"] == "OK"
    assert out["market_adj"]["mean_per_event"] == pytest.approx(0.0, abs=1e-12)
    assert out["announcement_market_adj"]["mean_per_event"] == pytest.approx(0.01)
    assert out["forgone_first_post_publication_session"]["mean_per_event"] == pytest.approx(0.02)


# --------------------------------------------------------------------------- #
# Verdict order
# --------------------------------------------------------------------------- #
_GOOD_EV = {"state": "OK", "market_adj": {"mean_per_session": -0.01, "t": -3.0}}
_GOOD_CELL = {"effective_periods": 100, "conditional": {"t": 3.0},
              "redundancy": {"redundancy": "DISTINCT"},
              "economics": {"ann_net_increment": 0.05}}
_GOOD_DESC = {"rank_ic_orthogonalised": {"t": 3.0}}
_GOOD_INC = {"conditional": {"t": 3.0}}
_OK_COV = {"uncovered_share": 0.0}
_OK_BIAS = {"biased": False}


def _v(**kw):
    args = dict(cell=_GOOD_CELL, inc_cell=_GOOD_INC, desc=_GOOD_DESC, ev=_GOOD_EV,
                coverage=_OK_COV, bias=_OK_BIAS)
    opts = dict(informative_eff=100, fdr_pass=True, sign=-1)
    for k in list(kw):
        if k in args:
            args[k] = kw.pop(k)
    opts.update(kw)
    return A.verdict_for(args["cell"], args["inc_cell"], args["desc"], args["ev"],
                         args["coverage"], args["bias"], **opts)


def test_every_gate_passing_is_the_only_route_to_qualified():
    assert _v()["verdict"] == A.V_QUALIFIED


def test_coverage_is_judged_first():
    assert _v(coverage={"uncovered_share": 0.49})["verdict"] == A.V_DATA_HOLD


def test_missingness_bias_is_judged_second():
    assert _v(bias={"biased": True, "ann_diff": -0.05})["verdict"] == A.V_DATA_HOLD


def test_periods_without_an_event_do_not_count_as_evidence():
    v = _v(informative_eff=20)
    assert v["verdict"] == A.V_NEED_MORE and v["gate"] == "EFFECTIVE_PERIODS"


def test_a_contradicted_sign_closes_the_cell_and_is_never_reversed():
    ev = {"state": "OK", "market_adj": {"mean_per_session": +0.01, "t": 4.0}}
    v = _v(ev=ev)
    assert v["verdict"] == A.V_NO_EDGE and v["gate"] == "FROZEN_SIGN"


def test_the_post_publication_effect_must_clear_the_standalone_floor():
    ev = {"state": "OK", "market_adj": {"mean_per_session": -0.01, "t": -1.2}}
    assert _v(ev=ev)["gate"] == "POST_PUBLICATION_T"


def test_a_negative_conditional_increment_is_not_an_edge():
    """|t| would have let a harmful dimension through; the increment must help."""
    cell = dict(_GOOD_CELL, conditional={"t": -3.0})
    assert _v(cell=cell)["gate"] == "CONDITIONAL_T"


def test_multiplicity_then_incrementality_then_materiality():
    assert _v(fdr_pass=False)["gate"] == "MULTIPLICITY"
    assert _v(desc={"rank_ic_orthogonalised": {"t": 1.0}})["verdict"] == A.V_NO_INCREMENTAL
    cell = dict(_GOOD_CELL, economics={"ann_net_increment": 0.001})
    assert _v(cell=cell)["gate"] == "MATERIALITY"


def test_announcement_and_forgone_sessions_can_never_qualify():
    src = Path(A.__file__).read_text(encoding="utf-8")
    body = src.split("def verdict_for", 1)[1].split("def run", 1)[0]
    for word in ("announcement", "forgone", "prior_21d_runup", "sector_adj"):
        assert word not in body, "%s must stay a diagnostic" % word


def test_verdicts_are_the_existing_five():
    assert set(A.VERDICTS) == {CBA.V_QUALIFIED, CBA.V_NO_EDGE, CBA.V_NO_INCREMENTAL,
                               CBA.V_DATA_HOLD, CBA.V_NEED_MORE}


def test_capital_is_never_eligible_from_this_module():
    src = Path(A.__file__).read_text(encoding="utf-8")
    assert '"capital_eligible": False' in src
    assert "capital_eligible\": True" not in src


def test_fetch_rate_is_the_canonical_gate():
    src = Path(D.__file__).read_text(encoding="utf-8")
    assert "CBD._RateGate(ACQ.MIN_INTERVAL_S)" in src
