"""Regressions for the preregistered SEC Form 4 clustered insider-buying axis.

These tests exist to make the honesty claims CHECKABLE rather than asserted:
that the stream and the event construction never read a price, that the frozen
constants are the frozen constants, that no second scorer was written, that the
transaction date is never the availability boundary, that the tradable window
starts strictly after publication, that the inherited burden can only raise the
bar, and that each measured SEC trap stays fixed.

Everything here is pure and local. No network, no store, no live path.
"""
from __future__ import annotations

import zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.alpha_recovery import control_block_alpha as CBA
from alpha_agent.alpha_recovery import insider_form4_alpha as A
from alpha_agent.alpha_recovery import insider_form4_data as D
from alpha_agent.alpha_recovery import insider_form4_events as EV

REPO = Path(__file__).resolve().parents[1]
PREREG = REPO / "research" / "preregistration" / "INSIDER_FORM4_PREREGISTRATION.md"

#: sha256 of the preregistration BLOB as committed at 944deb6, before any real
#: forward return for this family existed. Read from git rather than from the
#: working tree, so the test cannot be satisfied by a later edit.
PREREG_SHA256 = "9aa2886b91f4af15c56c44a2a7f93021a099c0092189c9a94981c68420d21cfc"
PREREG_COMMIT = "944deb6"


# --------------------------------------------------------------------------- #
# The preregistration was not moved after the results
# --------------------------------------------------------------------------- #
def test_preregistration_blob_is_unchanged_since_it_was_frozen():
    import hashlib
    import subprocess
    out = subprocess.run(
        ["git", "-C", str(REPO), "cat-file", "blob",
         "%s:research/preregistration/INSIDER_FORM4_PREREGISTRATION.md" % PREREG_COMMIT],
        capture_output=True)
    if out.returncode != 0:
        pytest.skip("git object not available in this checkout")
    assert hashlib.sha256(out.stdout).hexdigest() == PREREG_SHA256
    wt = PREREG.read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(wt).hexdigest() == PREREG_SHA256, \
        "the working-tree preregistration differs from the one that was frozen"


def test_runner_points_at_the_frozen_commit():
    assert A.PREREGISTRATION_COMMIT == PREREG_COMMIT
    assert A.PREREGISTRATION.endswith("INSIDER_FORM4_PREREGISTRATION.md")


def test_frozen_family_shape():
    """One cell, three horizons, m = 3, inherited m = 21 - no cell added."""
    assert EV.CELLS == (EV.CELL_CLUSTER,)
    assert A.HORIZONS_FROZEN == (5, 21, 63)
    assert A.declared_m() == 3
    assert len(A.PRIOR_INSIDER_TESTS) == 18
    assert A.inherited_m() == 21
    assert EV.EVENT_WINDOW_START == "2011-07-01"
    assert EV.CLUSTER_MIN_DISTINCT_INSIDERS == 2
    assert EV.CLUSTER_WINDOW_DAYS == 30
    spec = EV.CELL_SPECS[EV.CELL_CLUSTER]
    assert spec["sign"] == EV.SIGN_POSITIVE == 1
    assert spec["refractory_calendar_days"] == spec["window_calendar_days"] == 30
    assert spec["transaction_code"] == "P"
    assert tuple(spec["document_types"]) == ("4", "5")


def test_the_family_fits_the_existing_primary_budget():
    from alpha_agent import alpha_recovery as AR
    assert A.declared_m() <= AR.FAMILY_PRIMARY_MAX


def test_every_prior_insider_test_is_recorded_with_its_source():
    releases = [p["release"] for p in A.PRIOR_INSIDER_TESTS]
    assert releases.count("R27") == 6 and releases.count("R35") == 4
    assert releases.count("R39") == 3 and releases.count("R63") == 4
    assert releases.count("PHASE_11BC") == 1
    ids = {p["id"] for p in A.PRIOR_INSIDER_TESTS}
    assert "r27_insider_cluster_buy" in ids
    for p in A.PRIOR_INSIDER_TESTS:
        assert p["result"] and p["source"]


# --------------------------------------------------------------------------- #
# No return and no price before the preregistration; no second scorer
# --------------------------------------------------------------------------- #
def test_the_stream_and_the_events_never_read_a_price():
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


def test_the_value_field_is_never_read_as_a_number():
    """The estate records TRANS_SHARES x TRANS_PRICEPERSHARE as BLOCKED_SOURCE."""
    src = Path(D.__file__).read_text(encoding="utf-8")
    assert '"price_present"' in src
    assert '"price":' not in src and '"value":' not in src and '"dollars"' not in src


def test_the_event_block_does_not_overwrite_the_insider_control():
    assert A.BLOCK != A.INSIDER_CONTROL
    ds = {"blocks": {d: None for d in ("PRICE_RETURN_STATE", "MOMENTUM", "INSIDER_BEHAVIOUR")}}
    dims = A.control_dims(ds)
    assert "INSIDER_BEHAVIOUR" in dims and A.BLOCK not in dims
    assert A.SHORT_POSITIONING_CONTROL not in dims


def test_short_positioning_becomes_a_control_the_moment_it_exists():
    ds = {"blocks": {d: None for d in ("PRICE_RETURN_STATE", "INSIDER_BEHAVIOUR",
                                       "SHORT_POSITIONING")}}
    assert "SHORT_POSITIONING" in A.control_dims(ds)


def test_cost_ladder_and_stopping_rules_are_the_estates():
    assert A.COST_LADDER_BPS == (1.0, 2.0, 5.0)
    assert A.DESK_EQUITY_COST_BPS == pytest.approx(12.5)
    assert A.MIN_DATE_COVERAGE == 0.95
    assert A.MAX_UNCOVERED_SHARE == 0.20


def test_fetch_rate_is_the_canonical_gate():
    src = Path(D.__file__).read_text(encoding="utf-8")
    assert "CBD._RateGate(ACQ.MIN_INTERVAL_S)" in src


# --------------------------------------------------------------------------- #
# TRAPS - the SEC vocabularies
# --------------------------------------------------------------------------- #
def test_roles_are_read_by_containment_because_old_files_glue_tokens():
    f = D.relationship_flags("TenPercentOwnerOther")
    assert f["ten_percent_owner"] and f["other"] and not f["director"] and not f["officer"]
    f = D.relationship_flags("Director,OfficerOther")
    assert f["director"] and f["officer"] and f["other"]
    assert not any(D.relationship_flags("").values())


def test_the_10b5_1_flag_has_three_states():
    assert D.boolean_flag("1") is True and D.boolean_flag("true") is True
    assert D.boolean_flag("0") is False and D.boolean_flag("false") is False
    assert D.boolean_flag("") is None and D.boolean_flag(None) is None


@pytest.mark.parametrize("title,klass", [
    ('"DEPOSITARY SHARES ""A"" PREFERRED"', "PREFERRED"),
    ("Depositary Shares", "DEPOSITARY"),
    ("American Depositary Shares", "DEPOSITARY"),
    ("6.25% Senior Notes due 2030", "DEBT"),
    ("Warrants to purchase Common Stock", "WARRANT_OR_RIGHT"),
    ("Common Stock, par value $.01", "COMMON"),
    ("Class A Common Shares", "COMMON"),
    ("Ordinary Shares", "COMMON"),
    ("Common Units", "COMMON"),
    ("Series B", "OTHER"),
])
def test_non_common_securities_are_classed_before_shares_is_read(title, klass):
    assert D.title_class(title) == klass


def _tsv(header, rows):
    return "\n".join(["\t".join(header)] + ["\t".join(r) for r in rows]) + "\n"


def test_parse_archive_keeps_only_purchases_and_counts_every_other_code(tmp_path):
    z = tmp_path / "2019q1_form345.zip"
    with zipfile.ZipFile(z, "w") as zf:
        # NOTE: no AFF10B5ONE column - it did not exist before 2023
        zf.writestr("SUBMISSION.tsv", _tsv(
            ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"],
            [["0001-19-000001", "09-JAN-2019", "4", "0000000111"],
             ["0001-19-000002", "10-JAN-2019", "4", "0000000999"]]))
        zf.writestr("REPORTINGOWNER.tsv", _tsv(
            ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"],
            [["0001-19-000001", "0000000900", "Director,OfficerOther", "CEO"]]))
        zf.writestr("NONDERIV_TRANS.tsv", _tsv(
            ["ACCESSION_NUMBER", "TRANS_DATE", "TRANS_CODE", "TRANS_SHARES",
             "TRANS_PRICEPERSHARE", "TRANS_ACQUIRED_DISP_CD", "DIRECT_INDIRECT_OWNERSHIP",
             "EQUITY_SWAP_INVOLVED", "SECURITY_TITLE", "TRANS_TIMELINESS",
             "SHRS_OWND_FOLWNG_TRANS"],
            [["0001-19-000001", "07-JAN-2019", "P", "100", "12.5", "A", "D", "0",
              "Common Stock", "", "1100"],
             ["0001-19-000001", "07-JAN-2019", "S", "50", "12.5", "D", "D", "0",
              "Common Stock", "", "1050"],
             ["0001-19-000001", "07-JAN-2019", "M", "10", "", "A", "D", "0",
              "Common Stock", "", "1060"],
             ["0001-19-000002", "08-JAN-2019", "P", "100", "12.5", "A", "D", "0",
              "Common Stock", "", "100"]]))
    from collections import Counter
    c = Counter()
    rows = D.parse_archive(z, {"111"}, c, quarter="2019q1")
    assert len(rows) == 1, "only the panel issuer's P row is kept"
    r = rows[0]
    assert r["accession"] == "000119000001" and r["issuer_cik"] == "111"
    assert r["filing_date"] == "2019-01-09" and r["trans_date"] == "2019-01-07"
    assert r["rule_10b5_1"] is None and r["rule_10b5_1_column_present"] is False
    assert r["owners"][0]["director"] and r["owners"][0]["officer"]
    assert r["price_present"] is True and "price" not in r
    assert c["nonderivative_code:S"] == 1 and c["nonderivative_code:M"] == 1
    assert c["nonderivative_code:P"] == 1


def test_join_prefers_the_issuer_history_and_refuses_an_ambiguous_one():
    rows = [{"issuer_cik": "1", "accession": "a"}, {"issuer_cik": "1", "accession": "b"},
            {"issuer_cik": "1", "accession": "c"}, {"issuer_cik": "1", "accession": "d"}]
    acc = {"by_issuer": {("1", "a"): "2019-01-09T15:00:00.000Z"},
           "by_accession": {"a": {"2019-01-09T15:00:00.000Z"},
                            "b": {"2019-01-10T15:00:00.000Z"},
                            "c": {"2019-01-11T15:00:00.000Z", "2019-01-11T16:00:00.000Z"}}}
    src = D.join_acceptance(rows, acc)
    assert [r["acceptance_source"] for r in rows] == [
        D.SRC_ISSUER_HISTORY, D.SRC_OTHER_HISTORY, D.SRC_FILING_DATE_BOUND,
        D.SRC_FILING_DATE_BOUND]
    assert rows[2]["acceptance_utc"] == "" and src[D.SRC_FILING_DATE_BOUND] == 2


def test_quarter_end_and_the_observable_tail():
    assert D.quarter_end("2026q1") == "2026-03-31"
    assert D.quarter_end("2025q4") == "2025-12-31"
    assert D.observable_through({"2008q1": "x", "2026q1": "y"}) == "2026-03-31"
    assert D.observable_through({}) is None


# --------------------------------------------------------------------------- #
# THE PIT CONTRACT: the acceptance instant, never the transaction date
# --------------------------------------------------------------------------- #
DATES = np.array(["2019-01-07", "2019-01-08", "2019-01-09", "2019-01-10",
                  "2019-01-11", "2019-01-14"], dtype="datetime64[ns]")


def test_after_close_acceptance_waits_for_the_next_session():
    r = {"acceptance_utc": "2019-01-09T21:30:00.000Z", "filing_date": "2019-01-09"}  # 16:30 ET
    assert D.decision_session(DATES, r) == 3
    r = {"acceptance_utc": "2019-01-09T18:00:00.000Z", "filing_date": "2019-01-09"}  # 13:00 ET
    assert D.decision_session(DATES, r) == 2


def test_the_filing_date_bound_is_never_earlier_than_the_truth():
    """Without an instant, 17:30 ET on the filing date: EDGAR gives anything
    accepted at or after 17:30 the NEXT business day, so the true instant is
    earlier or equal - and the decision session can only be later."""
    bound = {"acceptance_utc": "", "filing_date": "2019-01-09"}
    assert D.availability_instant(bound) == datetime(2019, 1, 9, 17, 30)
    assert D.decision_session(DATES, bound) == 3
    for utc in ("2019-01-09T14:00:00.000Z", "2019-01-09T20:59:00.000Z",
                "2019-01-09T22:29:00.000Z"):
        truth = {"acceptance_utc": utc, "filing_date": "2019-01-09"}
        assert D.availability_instant(truth) <= D.availability_instant(bound)
        assert D.decision_session(DATES, truth) <= D.decision_session(DATES, bound)


def test_an_instant_before_the_calendar_is_refused():
    assert D.decision_session(DATES, {"acceptance_utc": "2005-03-01T18:00:00.000Z"}) is None
    assert D.decision_session(DATES, {"acceptance_utc": "", "filing_date": "2005-03-01"}) is None


def _own(cik, director=True, officer=False, ten=False):
    return {"cik": cik, "director": director, "officer": officer,
            "ten_percent_owner": ten, "other": False}


def _row(acc, owners, utc, trans, **kw):
    r = {"issuer_cik": "111", "accession": acc, "document_type": "4",
         "acquired_disposed": "A", "shares": 100.0, "equity_swap": False,
         "title_class": "COMMON", "rule_10b5_1": None, "owners": owners,
         "acceptance_utc": utc, "acceptance_source": D.SRC_ISSUER_HISTORY,
         "filing_date": utc[:10] if utc else kw.get("filing_date"), "trans_date": trans}
    r.update(kw)
    return r


@pytest.mark.parametrize("kw,reason", [
    ({"document_type": "4/A"}, EV.X_FORM),
    ({"document_type": "3"}, EV.X_FORM),
    ({"acquired_disposed": "D"}, EV.X_FLAG),
    ({"shares": 0.0}, EV.X_SHARES),
    ({"equity_swap": True}, EV.X_SWAP),
    ({"title_class": "PREFERRED"}, EV.X_TITLE),
    ({"rule_10b5_1": True}, EV.X_PLAN),
    ({"owners": [_own("9", director=False, ten=True)]}, EV.X_ROLE),
    ({"trans_date": "2019-01-10"}, EV.X_TDATE),
    ({"trans_date": None}, EV.X_TDATE),
    ({"trans_date": "2018-12-01"}, EV.X_STALE),
])
def test_each_cleaning_rule_fires_in_its_frozen_order(kw, reason):
    r = _row("a", [_own("900")], "2019-01-09T15:00:00.000Z", "2019-01-08")
    assert EV.exclusion_reason(r) is None
    r.update(kw)
    assert EV.exclusion_reason(r) == reason
    assert reason in EV.EXCLUSION_ORDER


def test_an_unobserved_plan_flag_is_not_an_exclusion():
    r = _row("a", [_own("900")], "2019-01-09T15:00:00.000Z", "2019-01-08", rule_10b5_1=False)
    assert EV.exclusion_reason(r) is None
    r["rule_10b5_1"] = None
    assert EV.exclusion_reason(r) is None


def _build(rows, start="2019-01-01"):
    dates = pd.bdate_range("2019-01-01", "2019-06-28").strftime("%Y-%m-%d").values.astype("<U10")
    E = {"symbols": np.array(["AAA"]), "dates": dates}
    elig = np.ones((1, len(dates)), dtype=bool)
    orig = EV.EVENT_WINDOW_START
    try:
        EV.EVENT_WINDOW_START = start
        return EV.build_events(E, elig, rows=rows, cik2rows={"111": [0]}, verbose=False)
    finally:
        EV.EVENT_WINDOW_START = orig


def test_one_insider_buying_twice_is_not_a_cluster():
    b = _build([_row("a", [_own("900")], "2019-01-08T15:00:00.000Z", "2019-01-07"),
                _row("b", [_own("900")], "2019-01-10T15:00:00.000Z", "2019-01-09")])
    assert b["events"][EV.CELL_CLUSTER] == []


def test_a_director_and_the_directors_fund_on_one_filing_are_one_insider():
    b = _build([_row("a", [_own("900"), _own("777", director=False, ten=True)],
                     "2019-01-08T15:00:00.000Z", "2019-01-07"),
                _row("b", [_own("900"), _own("778", director=False, ten=True)],
                     "2019-01-10T15:00:00.000Z", "2019-01-09")])
    assert b["events"][EV.CELL_CLUSTER] == []


def test_two_insiders_inside_the_window_fire_at_the_completing_instant():
    b = _build([_row("a", [_own("900")], "2019-01-08T15:00:00.000Z", "2019-01-07"),
                _row("b", [_own("901", director=False, officer=True)],
                     "2019-01-15T22:00:00.000Z", "2019-01-14")])        # 17:00 ET
    ev = b["events"][EV.CELL_CLUSTER]
    assert [(e["date"], e["accessions"], e["distinct_insiders"]) for e in ev] == \
        [("2019-01-16", ["b"], 2)]


def test_the_refractory_period_and_the_window_edge():
    rows = [_row("a", [_own("900")], "2019-01-08T15:00:00.000Z", "2019-01-07"),
            _row("b", [_own("901")], "2019-01-09T15:00:00.000Z", "2019-01-08"),
            _row("c", [_own("902")], "2019-01-20T15:00:00.000Z", "2019-01-18"),   # refractory
            _row("d", [_own("903")], "2019-03-01T15:00:00.000Z", "2019-02-28"),   # alone
            _row("e", [_own("904")], "2019-04-05T15:00:00.000Z", "2019-04-04")]   # d is 35 days old
    ev = _build(rows)["events"][EV.CELL_CLUSTER]
    assert [e["accessions"] for e in ev] == [["b"]]


def test_a_member_whose_purchase_left_the_window_does_not_count():
    """Published inside the window, but bought more than 30 days before the
    completing instant: not a same-window act."""
    rows = [_row("a", [_own("900")], "2019-02-01T15:00:00.000Z", "2019-01-10"),
            _row("b", [_own("901")], "2019-02-14T15:00:00.000Z", "2019-02-13")]
    assert _build(rows)["events"][EV.CELL_CLUSTER] == []


def test_a_later_filing_never_creates_an_earlier_event():
    """The second insider's filing is public only on 01-20. No event may be
    dated before that instant, whatever its transaction date says."""
    rows = [_row("a", [_own("900")], "2019-01-08T15:00:00.000Z", "2019-01-07"),
            _row("b", [_own("901")], "2019-01-20T15:00:00.000Z", "2019-01-08")]
    ev = _build(rows)["events"][EV.CELL_CLUSTER]
    assert [e["date"] for e in ev] == ["2019-01-21"]


def test_events_before_the_window_build_state_but_are_not_counted():
    rows = [_row("a", [_own("900")], "2019-01-08T15:00:00.000Z", "2019-01-07"),
            _row("b", [_own("901")], "2019-01-09T15:00:00.000Z", "2019-01-08")]
    b = _build(rows, start="2019-03-01")
    assert b["events"][EV.CELL_CLUSTER] == [] and b["stats"]["before_event_window"] == 1


# --------------------------------------------------------------------------- #
# The panel signal: unobserved is NaN, never 0
# --------------------------------------------------------------------------- #
def test_the_signal_is_nan_past_the_archive_and_zero_before_it():
    dates = pd.bdate_range("2019-01-01", periods=60).strftime("%Y-%m-%d").values.astype("<U10")
    E = {"symbols": np.array(["AAA", "BBB"]), "dates": dates}
    elig = np.ones((2, len(dates)), dtype=bool)
    dec = np.arange(10, 60, 10)
    events = [{"row": 0, "t": 18, "date": dates[18]}]
    sig, _per, dropped = A.signal_matrix(E, elig, events, dec, 10, +1, end_ix=35)
    assert sig[0, 20] == 1.0 and sig[1, 20] == 0.0
    assert sig[1, 10] == 0.0 and sig[1, 30] == 0.0
    assert np.isnan(sig[:, 40]).all() and np.isnan(sig[:, 50]).all()
    assert dropped == 2


# --------------------------------------------------------------------------- #
# THE PIT WINDOW: entry strictly after publication; diagnostics stay diagnostics
# --------------------------------------------------------------------------- #
def _jump_panel(jumps: dict, n_i: int = 40, n_t: int = 420, first: int = 60, step: int = 8):
    tr = np.ones((n_i, n_t))
    events = []
    for k in range(n_i):
        t = first + step * k
        r = np.zeros(n_t)
        for off, size in jumps.items():
            r[t + off] = size
        tr[k] = np.cumprod(1.0 + r)
        events.append({"row": k, "t": t, "date": "d%d" % t, "is_13d": False})
    E = {"symbols": np.array(["S%d" % k for k in range(n_i)]),
         "dates": pd.bdate_range("2015-01-01", periods=n_t).strftime("%Y-%m-%d").values,
         "price": {"tr": tr, "spy_tr": np.ones(n_t)}}
    return E, np.ones((n_i, n_t), dtype=bool), events


def test_the_tradable_window_starts_at_close_t_plus_one():
    E, elig, events = _jump_panel({0: 0.01, 1: 0.02})
    out = A.event_study(E, elig, events, h=5, vol63=np.full(elig.shape, 0.2), sign=+1)
    assert out["state"] == "OK"
    assert out["market_adj"]["mean_per_event"] == pytest.approx(0.0, abs=1e-12)
    assert out["announcement_market_adj"]["mean_per_event"] == pytest.approx(0.01)
    assert out["forgone_first_post_publication_session"]["mean_per_event"] == pytest.approx(0.02)


def test_the_pre_publication_diagnostic_stops_before_the_publication_session():
    E, _elig, events = _jump_panel({-5: 0.01, 0: 0.02})
    out = A.pre_publication_market_adj(E, events)
    assert out["mean_per_event"] == pytest.approx(0.01)
    assert out["window_sessions"] == [-21, -1]
    E, _elig, events = _jump_panel({-22: 0.03})
    assert A.pre_publication_market_adj(E, events)["mean_per_event"] == pytest.approx(0.0, abs=1e-12)


# --------------------------------------------------------------------------- #
# The inherited burden can only raise the bar
# --------------------------------------------------------------------------- #
def test_prior_tests_enter_at_p_one_and_raise_the_bar():
    _res, own = A._bh([0.02, 0.5, 0.9])
    assert own[0] is True
    _res, inh = A._bh([0.02, 0.5, 0.9], extra_nulls=len(A.PRIOR_INSIDER_TESTS))
    assert inh[0] is False
    res, inh = A._bh([0.001, 0.5, 0.9], extra_nulls=len(A.PRIOR_INSIDER_TESTS))
    assert inh[0] is True and res["m"] == 21


def test_a_missing_p_value_never_shrinks_the_denominator():
    res, _ = A._bh([None, 0.5, 0.9])
    assert res["m"] == 3


# --------------------------------------------------------------------------- #
# Verdict order
# --------------------------------------------------------------------------- #
_GOOD_EV = {"state": "OK", "market_adj": {"mean_per_session": 0.01, "t": 3.0}}
_GOOD_CELL = {"effective_periods": 100, "conditional": {"t": 3.0},
              "redundancy": {"redundancy": "DISTINCT"},
              "economics": {"ann_net_increment": 0.05}}
_GOOD_DESC = {"rank_ic_orthogonalised": {"t": 3.0}}
_GOOD_INC = {"conditional": {"t": 3.0}}


def _v(**kw):
    args = dict(cell=_GOOD_CELL, inc_cell=_GOOD_INC, desc=_GOOD_DESC, ev=_GOOD_EV,
                coverage={"uncovered_share": 0.0}, bias={"biased": False})
    opts = dict(informative_eff=100, fdr_pass=True, inherited_fdr_pass=True, sign=+1,
                short_positioning_controlled=True)
    for k in list(kw):
        if k in args:
            args[k] = kw.pop(k)
    opts.update(kw)
    return A.verdict_for(args["cell"], args["inc_cell"], args["desc"], args["ev"],
                         args["coverage"], args["bias"], **opts)


def test_every_gate_passing_is_the_only_route_to_qualified():
    assert _v()["verdict"] == A.V_QUALIFIED


def test_data_gates_come_first():
    assert _v(coverage={"uncovered_share": 0.46})["gate"] == "COVERAGE"
    assert _v(bias={"biased": True, "ann_diff": -0.05})["gate"] == "MISSINGNESS_BIAS"


def test_periods_without_an_event_do_not_count_as_evidence():
    v = _v(informative_eff=20)
    assert v["verdict"] == A.V_NEED_MORE and v["gate"] == "EFFECTIVE_PERIODS"


def test_a_contradicted_sign_closes_the_cell_and_is_never_reversed():
    v = _v(ev={"state": "OK", "market_adj": {"mean_per_session": -0.01, "t": -4.0}})
    assert v["verdict"] == A.V_NO_EDGE and v["gate"] == "FROZEN_SIGN"


def test_the_post_publication_effect_must_clear_the_standalone_floor():
    ev = {"state": "OK", "market_adj": {"mean_per_session": 0.01, "t": 1.2}}
    assert _v(ev=ev)["gate"] == "POST_PUBLICATION_T"


def test_a_negative_conditional_increment_is_not_an_edge():
    assert _v(cell=dict(_GOOD_CELL, conditional={"t": -3.0}))["gate"] == "CONDITIONAL_T"


def test_family_then_inherited_multiplicity_then_incremental_then_materiality():
    assert _v(fdr_pass=False)["gate"] == "MULTIPLICITY"
    assert _v(inherited_fdr_pass=False)["gate"] == "INHERITED_MULTIPLICITY"
    assert _v(desc={"rank_ic_orthogonalised": {"t": 1.0}})["verdict"] == A.V_NO_INCREMENTAL
    assert _v(cell=dict(_GOOD_CELL, economics={"ann_net_increment": 0.001}))["gate"] == "MATERIALITY"


def test_without_the_short_positioning_control_nothing_qualifies():
    v = _v(short_positioning_controlled=False)
    assert v["verdict"] == A.V_NEED_MORE and v["gate"] == "REQUIRED_CONTROL_SHORT_POSITIONING"


def test_diagnostics_can_never_qualify():
    src = Path(A.__file__).read_text(encoding="utf-8")
    body = src.split("def verdict_for", 1)[1].split("def _bh", 1)[0]
    for word in ("announcement", "forgone", "pre_publication", "prior_21d_runup", "sector_adj"):
        assert word not in body, "%s must stay a diagnostic" % word


def test_verdicts_are_the_existing_five():
    assert set(A.VERDICTS) == {CBA.V_QUALIFIED, CBA.V_NO_EDGE, CBA.V_NO_INCREMENTAL,
                               CBA.V_DATA_HOLD, CBA.V_NEED_MORE}


def test_capital_is_never_eligible_from_this_module():
    src = Path(A.__file__).read_text(encoding="utf-8")
    assert '"capital_eligible": False' in src
    assert "capital_eligible\": True" not in src


def test_the_campaign_runner_registers_the_stage():
    src = (REPO / "scripts" / "run_alpha_recovery_offensive.py").read_text(encoding="utf-8")
    assert '"insider_form4"' in src and "_stage_insider_form4" in src
