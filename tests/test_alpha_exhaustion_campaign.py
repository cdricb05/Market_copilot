"""Release 27 — Autonomous Alpha Exhaustion Campaign.

Hermetic regressions. No network, no provider, no live price service, no
database beyond temporary SQLite files, no operational store. Every external
surface is injected or written into a tmp_path.

The tests are grouped by the property they defend. Almost everything that can go
wrong in this campaign is silent: a retroactive SEC flag that leaks a quarter of
the future, a fourth-quarter figure compared against a full year and called a
restatement, an insider trade dated before the market could see it, a split
counted as issuance, a degenerate all-ties cross-section reported as a null, a
pre-registration mutated after the numbers were read, or a frontier audit that
lets the campaign commit while free research is still runnable.
"""
from __future__ import annotations

import json
import sqlite3
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from alpha_agent import sec_filing_behavior as sfb
from alpha_agent import sec_financial_statement_sets as fsds
from alpha_agent import stage24_pit_fundamental as s24
from alpha_agent import stage25_alpha_discovery as s25
from alpha_agent import stage26_challenger_expansion as s26
from alpha_agent import stage27_alpha_exhaustion as r27


# =========================================================================== #
# Fixtures
# =========================================================================== #
SUB_HEADER = ("adsh\tcik\tname\tsic\tafs\twksi\tfye\tform\tperiod\tfy\tfp\t"
              "filed\taccepted\tprevrpt\tdetail\tinstance\tnciks\taciks")


def _sub_row(adsh, cik, form, period, filed, accepted, *, afs="1-LAF",
             fye="1231", prevrpt="0", detail="1", nciks="1"):
    return "\t".join([adsh, cik, "ACME INC", "3674", afs, "0", fye, form,
                      period, "2015", "FY", filed, accepted, prevrpt, detail,
                      "x.xml", nciks, ""])


def _write_sub(root: Path, quarter: str, rows) -> None:
    d = root / quarter
    d.mkdir(parents=True, exist_ok=True)
    (d / "sub.txt").write_text(
        "%s\n%s\n" % (SUB_HEADER, "\n".join(rows)), encoding="utf-8")


def _filing_history(tmp_path, rows, quarter="2015q1"):
    root = tmp_path / "fsds"
    _write_sub(root, quarter, rows)
    fh = sfb.FilingHistory(root)
    fh.load()
    return fh


def _cf_index(tmp_path, facts) -> Path:
    """A minimal companyfacts index carrying only what the revision reader reads."""
    p = tmp_path / "cf.sqlite"
    conn = sqlite3.connect(p)
    conn.execute(
        "CREATE TABLE cf_fact (id INTEGER PRIMARY KEY AUTOINCREMENT, cik TEXT, "
        "taxonomy TEXT, concept_tag TEXT, label TEXT, unit TEXT, value REAL, "
        "period_start TEXT, period_end TEXT, filed TEXT, accession TEXT, "
        "form TEXT, fy TEXT, fp TEXT, frame TEXT, source_archive_hash TEXT, "
        "source_member TEXT, parser_version TEXT, created_at TEXT)")
    conn.executemany(
        "INSERT INTO cf_fact (cik, taxonomy, concept_tag, unit, value, "
        "period_start, period_end, filed, accession, form, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,'t')", facts)
    conn.commit()
    conn.close()
    return p


class _Shares:
    def __init__(self, series):
        self.series = series          # [(filed, shares)]
        self.load_status = {"ok": True}

    def shares_as_of(self, cik, as_of):
        best = None
        for filed, shares in sorted(self.series):
            if filed <= str(as_of)[:10]:
                best = {"concept": "dei", "filed": filed, "period_end": filed,
                        "shares": float(shares), "age_days": 0}
        return best


class _Prices:
    def __init__(self, factors):
        self.factors = factors        # {date: capital_factor}
        self.load_status = {"ok": True}

    def closes_as_of(self, symbol, as_of):
        cut = str(as_of)[:10]
        best = None
        for d in sorted(self.factors):
            if d <= cut:
                best = d
        if best is None:
            return None
        f = self.factors[best]
        return {"date": best, "close_none": 10.0, "close_capital": 10.0 * f,
                "capital_factor": f}


# =========================================================================== #
# 1. SEC acceptance timestamp is the point-in-time cutoff
# =========================================================================== #
def test_submission_is_invisible_until_its_acceptance_timestamp(tmp_path):
    fh = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0")])
    assert fh.submissions_as_of("77", "2015-02-09") == []
    assert len(fh.submissions_as_of("77", "2015-02-10")) == 1


def test_period_end_is_not_the_availability_date(tmp_path):
    """The fiscal period ended in December; nothing was knowable until February."""
    fh = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0")])
    assert fh.observables("77", "2015-01-15") is None
    obs = fh.observables("77", "2015-03-01")
    assert obs["annual_lag_days"] == 41.0


def test_filed_date_used_only_when_acceptance_absent(tmp_path):
    fh = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150210", "")])
    subs = fh.submissions_as_of("77", "2015-03-01")
    assert subs[0]["availability_basis"] == "SEC_FILED_DATE"
    assert subs[0]["observable_at"] == "2015-02-10"


# =========================================================================== #
# 2. prevrpt is a look-ahead flag and is never a signal input
# =========================================================================== #
def test_prevrpt_is_diagnostic_only_and_never_reaches_an_observable(tmp_path):
    fh = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0", prevrpt="1")])
    diag = fh.prevrpt_diagnostic()
    assert diag["classification"] == "LOOK_AHEAD_FLAG"
    assert diag["policy"].startswith("DIAGNOSTIC_ONLY")
    assert diag["rows_flagged"] == 1
    obs = fh.observables("77", "2015-03-01")
    assert not any("prevrpt" in k for k in obs)
    # and no registered hypothesis may reference it
    for spec in r27.ALL_R27_FACTORS:
        assert not any("prevrpt" in r for r in spec.required)


def test_first_observation_of_an_accession_wins(tmp_path):
    """The same submission re-listed in a later quarterly data set carries that
    data set's retroactive prevrpt; the earlier listing is authoritative."""
    root = tmp_path / "fsds"
    _write_sub(root, "2015q1", [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0", prevrpt="0")])
    _write_sub(root, "2015q2", [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0", prevrpt="1")])
    fh = sfb.FilingHistory(root)
    st = fh.load()
    assert st["submissions"] == 1
    assert fh.submissions_as_of("77", "2016-01-01")[0]["prevrpt"] is False


# =========================================================================== #
# 3. Amendments enter only at their OWN acceptance; no backward leakage
# =========================================================================== #
def test_amendment_does_not_leak_back_to_the_original_filing_date(tmp_path):
    fh = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150210",
                 "2015-02-10 17:30:00.0"),
        _sub_row("a2", "77", "10-K/A", "20141231", "20151101",
                 "2015-11-01 09:00:00.0"),
    ])
    before = fh.observables("77", "2015-06-01")
    assert before["amendment_count_1y"] == 0.0
    assert before["amendment_recent_1y"] == 0.0
    after = fh.observables("77", "2015-12-01")
    assert after["amendment_count_1y"] == 1.0
    assert after["annual_amendment_count_3y"] == 1.0
    assert after["amendment_latency_days"] == 264.0


def test_deadline_uses_the_filer_status_carried_by_that_submission(tmp_path):
    """A large accelerated filer is held to 60 days; a non-accelerated one to 90.
    A 79-day lag therefore gives opposite verdicts, which is correct."""
    laf = _filing_history(tmp_path, [
        _sub_row("a1", "77", "10-K", "20141231", "20150320",
                 "2015-03-20 09:00:00.0", afs="1-LAF")])
    non = _filing_history(tmp_path / "b", [
        _sub_row("a1", "78", "10-K", "20141231", "20150320",
                 "2015-03-20 09:00:00.0", afs="4-NON")])
    assert laf.observables("77", "2015-05-01")["deadline_miss_latest"] == 1.0
    assert non.observables("78", "2015-05-01")["deadline_miss_latest"] == 0.0


def test_weekend_roll_grace_is_declared_not_tuned():
    assert sfb.DEADLINE_GRACE_DAYS == 4
    assert sfb.DEADLINE_DAYS["1-LAF"] == {"annual": 60, "quarterly": 40}
    assert sfb.DEADLINE_DAYS["4-NON"] == {"annual": 90, "quarterly": 45}


# =========================================================================== #
# 4. Fact revisions: duration-aware, materiality-aware, forward-stamped
# =========================================================================== #
def test_same_period_end_different_duration_is_not_a_restatement(tmp_path):
    """Q4 revenue and full-year revenue share a period_end. Comparing them would
    report a 300 % restatement that never happened."""
    p = _cf_index(tmp_path, [
        ("0000000077", "us-gaap", "Revenues", "USD", 250.0,
         "2014-10-01", "2014-12-31", "2015-02-10", "a1", "10-K"),
        ("0000000077", "us-gaap", "Revenues", "USD", 1000.0,
         "2014-01-01", "2014-12-31", "2015-02-10", "a1", "10-K"),
    ])
    rv = sfb.FactRevisionHistory(p)
    st = rv.load()
    assert st["revision_events"] == 0
    assert rv.events_as_of("77", "2020-01-01") == []


def test_identical_value_restated_in_a_later_filing_is_not_an_event(tmp_path):
    p = _cf_index(tmp_path, [
        ("0000000077", "us-gaap", "Assets", "USD", 1000.0,
         None, "2014-12-31", "2015-02-10", "a1", "10-K"),
        ("0000000077", "us-gaap", "Assets", "USD", 1000.0,
         None, "2014-12-31", "2016-02-10", "a2", "10-K"),
    ])
    rv = sfb.FactRevisionHistory(p)
    assert rv.load()["revision_events"] == 0


def test_material_value_change_is_an_event_stamped_at_the_later_filing(tmp_path):
    p = _cf_index(tmp_path, [
        ("0000000077", "us-gaap", "Assets", "USD", 1000.0,
         None, "2014-12-31", "2015-02-10", "a1", "10-K"),
        ("0000000077", "us-gaap", "Assets", "USD", 1400.0,
         None, "2014-12-31", "2016-02-10", "a2", "10-K"),
    ])
    rv = sfb.FactRevisionHistory(p)
    assert rv.load()["revision_events"] == 1
    assert rv.events_as_of("77", "2016-02-09") == []
    ev = rv.events_as_of("77", "2016-02-10")
    assert len(ev) == 1 and ev[0]["observable_at"] == "2016-02-10"
    assert rv.observables("77", "2015-06-01")["revision_count_1y"] == 0.0
    assert rv.observables("77", "2016-06-01")["revision_count_1y"] == 1.0


def test_sub_materiality_rounding_is_not_a_restatement(tmp_path):
    p = _cf_index(tmp_path, [
        ("0000000077", "us-gaap", "Assets", "USD", 1000.0,
         None, "2014-12-31", "2015-02-10", "a1", "10-K"),
        ("0000000077", "us-gaap", "Assets", "USD", 1001.0,
         None, "2014-12-31", "2016-02-10", "a2", "10-K"),
    ])
    assert sfb.FactRevisionHistory(p).load()["revision_events"] == 0


def test_issuer_with_no_revision_scores_zero_not_missing(tmp_path):
    p = _cf_index(tmp_path, [
        ("0000000077", "us-gaap", "Assets", "USD", 1000.0,
         None, "2014-12-31", "2015-02-10", "a1", "10-K"),
    ])
    rv = sfb.FactRevisionHistory(p)
    rv.load()
    obs = rv.observables("77", "2016-06-01")
    assert obs is not None and obs["revision_count_1y"] == 0.0
    assert rv.observables("999", "2016-06-01") is None


# =========================================================================== #
# 5. Share dynamics: a split is not issuance
# =========================================================================== #
def test_two_for_one_split_produces_zero_net_issuance(tmp_path):
    """The reported count doubles and the capital factor doubles with it."""
    shares = _Shares([("2014-02-01", 1_000_000), ("2015-02-01", 2_000_000)])
    prices = _Prices({"2014-01-01": 0.5, "2014-02-01": 0.5,
                      "2015-02-01": 1.0, "2015-06-01": 1.0})
    sd = sfb.ShareDynamicsHistory(shares, prices)
    obs = sd.observables(symbol="ACME", cik="77", as_of="2015-06-01")
    assert obs["net_issuance_1y"] == pytest.approx(0.0, abs=1e-9)


def test_real_dilution_survives_the_split_normalisation(tmp_path):
    shares = _Shares([("2014-02-01", 1_000_000), ("2015-02-01", 2_200_000)])
    prices = _Prices({"2014-01-01": 0.5, "2014-02-01": 0.5,
                      "2015-02-01": 1.0, "2015-06-01": 1.0})
    sd = sfb.ShareDynamicsHistory(shares, prices)
    obs = sd.observables(symbol="ACME", cik="77", as_of="2015-06-01")
    assert obs["net_issuance_1y"] == pytest.approx(0.10, abs=1e-9)


def test_both_endpoints_resolving_to_one_filing_yields_none_not_zero(tmp_path):
    shares = _Shares([("2013-01-01", 1_000_000)])
    prices = _Prices({"2013-01-01": 1.0, "2015-06-01": 1.0})
    sd = sfb.ShareDynamicsHistory(shares, prices)
    obs = sd.observables(symbol="ACME", cik="77", as_of="2015-06-01")
    assert obs["net_issuance_1y"] is None


def test_share_count_is_never_read_from_a_later_filing(tmp_path):
    shares = _Shares([("2014-02-01", 1_000_000), ("2016-02-01", 9_000_000)])
    prices = _Prices({"2014-02-01": 1.0, "2015-06-01": 1.0})
    sd = sfb.ShareDynamicsHistory(shares, prices)
    n = sd.normalised_shares(symbol="ACME", cik="77", as_of="2015-06-01")
    assert n["shares"] == 1_000_000


# =========================================================================== #
# 6. Insider transactions: filing date, open-market only
# =========================================================================== #
def _insider_cache(tmp_path, *, quarter="2015q1", trans_code="P",
                   filing_date="10-MAR-2015", trans_date="01-MAR-2015",
                   disp="A", relationship="Officer"):
    d = tmp_path / "insider" / quarter
    d.mkdir(parents=True, exist_ok=True)
    (d / "SUBMISSION.tsv").write_text(
        "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\t"
        "ISSUERCIK\tISSUERTRADINGSYMBOL\n"
        "x1\t%s\t%s\t4\t0000000077\tACME\n" % (filing_date, trans_date),
        encoding="utf-8")
    (d / "REPORTINGOWNER.tsv").write_text(
        "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\n"
        "x1\t0000000501\tJane Doe\t%s\n" % relationship, encoding="utf-8")
    (d / "NONDERIV_TRANS.tsv").write_text(
        "ACCESSION_NUMBER\tTRANS_DATE\tTRANS_CODE\tTRANS_SHARES\t"
        "TRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\n"
        "x1\t%s\t%s\t1000\t50.0\t%s\n" % (trans_date, trans_code, disp),
        encoding="utf-8")
    return tmp_path / "insider"


def test_insider_event_is_stamped_at_the_form4_filing_date(tmp_path):
    root = _insider_cache(tmp_path, trans_date="01-MAR-2015",
                          filing_date="10-MAR-2015")
    ins = sfb.InsiderTransactionHistory(root)
    ins.load()
    # the trade happened on the 1st and was filed on the 10th; nothing is
    # observable in between
    assert ins.events_as_of("77", "2015-03-05") == []
    assert len(ins.events_as_of("77", "2015-03-10")) == 1


def test_only_open_market_codes_are_kept(tmp_path):
    for code in ("A", "M", "F", "G"):
        root = _insider_cache(tmp_path / code, trans_code=code)
        st = sfb.InsiderTransactionHistory(root).load()
        assert st["open_market_events_kept"] == 0, code
    root = _insider_cache(tmp_path / "P", trans_code="P")
    assert sfb.InsiderTransactionHistory(root).load()[
        "open_market_events_kept"] == 1


def test_direction_flag_contradicting_the_code_is_dropped(tmp_path):
    root = _insider_cache(tmp_path, trans_code="P", disp="D")
    st = sfb.InsiderTransactionHistory(root).load()
    assert st["open_market_events_kept"] == 0
    assert st["dropped_direction_flag_mismatch"] == 1


def test_quiet_issuer_has_no_buyer_ratio_rather_than_a_zero_one(tmp_path):
    root = _insider_cache(tmp_path)
    ins = sfb.InsiderTransactionHistory(root)
    ins.load()
    quiet = ins.observables(cik="77", as_of="2020-01-01",
                            shares_outstanding=1e6, market_equity=1e8)
    assert quiet["buyer_ratio_6m"] is None
    assert quiet["insider_events_6m"] == 0.0
    active = ins.observables(cik="77", as_of="2015-03-15",
                             shares_outstanding=1e6, market_equity=1e8)
    assert active["buyer_ratio_6m"] == 1.0
    assert active["net_buy_share_fraction_6m"] == pytest.approx(0.001)


def test_insider_parse_validation_reports_the_buy_share_by_quarter(tmp_path):
    root = _insider_cache(tmp_path, trans_code="P")
    ins = sfb.InsiderTransactionHistory(root)
    ins.load()
    v = ins.parse_validation()
    assert v["overall_buy_share"] == 1.0
    assert v["total_open_market_buys"] == 1
    assert v["quarterly_buy_share"]["2015Q1"] == 1.0
    assert v["highest_buy_share_quarters"][0]["quarter"] == "2015Q1"


def test_officer_flag_separates_insiders_from_ten_percent_holders(tmp_path):
    off = _insider_cache(tmp_path / "o", relationship="Officer")
    ten = _insider_cache(tmp_path / "t", relationship="TenPercentOwner")
    a = sfb.InsiderTransactionHistory(off)
    a.load()
    b = sfb.InsiderTransactionHistory(ten)
    b.load()
    kw = dict(cik="77", as_of="2015-03-15", shares_outstanding=1e6,
              market_equity=1e8)
    assert a.observables(**kw)["officer_net_buy_share_fraction_6m"] > 0
    assert b.observables(**kw)["officer_net_buy_share_fraction_6m"] == 0.0


# =========================================================================== #
# 7. Filing stream (EDGAR full index)
# =========================================================================== #
def _stream_cache(tmp_path, lines, quarter="2015q1"):
    d = tmp_path / "fullindex" / quarter
    d.mkdir(parents=True, exist_ok=True)
    (d / "master.idx").write_text(
        "Description\n---\n" + "\n".join(lines), encoding="utf-8")
    return tmp_path / "fullindex"


def test_only_registered_form_groups_are_retained(tmp_path):
    root = _stream_cache(tmp_path, [
        "77|ACME|NT 10-K|2015-03-01|a.txt",
        "77|ACME|SC 13D|2015-04-01|b.txt",
        "77|ACME|DEF 14A|2015-05-01|c.txt",     # not registered
        "77|ACME|8-K|2015-06-01|d.txt",
    ])
    st = sfb.EdgarFilingStreamHistory(root)
    st.load()
    assert st.load_status["events_kept"] == 3
    obs = st.observables("77", "2015-07-01")
    assert obs["late_notification_1y"] == 1.0
    assert obs["activist_stake_1y"] == 1.0
    assert obs["current_report_count_1y"] == 1.0
    assert obs["shelf_offering_1y"] == 0.0


def test_filing_stream_respects_the_as_of_cutoff(tmp_path):
    root = _stream_cache(tmp_path, ["77|ACME|NT 10-K|2015-06-01|a.txt"])
    st = sfb.EdgarFilingStreamHistory(root)
    st.load()
    assert st.observables("77", "2015-05-31")["late_notification_1y"] == 0.0
    assert st.observables("77", "2015-06-01")["late_notification_1y"] == 1.0


# =========================================================================== #
# 8. Pre-registration immutability
# =========================================================================== #
def test_every_registered_hypothesis_declares_a_sign_and_never_fits_it():
    for spec in r27.ALL_R27_FACTORS:
        d = spec.as_dict()
        assert d["expected_sign"] in (1, -1), spec.name
        assert d["sign_fitted_from_data"] is False, spec.name
        assert d["economic_hypothesis"], spec.name
        assert d["economic_rationale"], spec.name
        assert d["factor_definition"], spec.name


def test_manifest_declares_the_family_and_the_hypothesis_count_it_ran():
    m = r27.hypothesis_manifest()
    assert m["family_count"] == len(r27.REGISTERED_FAMILIES)
    assert m["hypothesis_count"] == len(r27.ALL_R27_FACTORS)
    assert m["signs_fixed_before_evaluation"] is True
    assert m["brute_force_parameter_search_performed"] is False
    for fam in m["families"]:
        assert len(fam["experiments"]) == fam["hypotheses"]
        assert 3 <= fam["hypotheses"] <= 15, fam["family"]


def test_hypothesis_names_are_unique_across_the_whole_campaign():
    names = [s.name for s in r27.ALL_R27_FACTORS]
    assert len(names) == len(set(names))


def test_no_registered_hypothesis_reopens_a_closed_family():
    closed = {"s24_rnd_intensity", "s25_operating_profitability"}
    for spec in r27.ALL_R27_FACTORS:
        assert spec.name not in closed
        assert not spec.name.startswith(("s24_", "s25_", "s26_"))


def test_declared_thresholds_come_from_the_readers_not_a_copy():
    m = r27.hypothesis_manifest()
    t = m["thresholds_declared_in_source_before_results"]
    assert t["deadline_grace_days"] == sfb.DEADLINE_GRACE_DAYS
    assert t["revision_materiality"] == sfb.REVISION_MATERIALITY
    assert t["insider_window_days"] == sfb.INSIDER_WINDOW_DAYS
    assert t["insider_cluster_min_buyers"] == sfb.INSIDER_CLUSTER_MIN_BUYERS


# =========================================================================== #
# 9. Multiple-testing scope is frozen
# =========================================================================== #
def _result(name, family, t, periods=60, gate="REJECT", fdr=False,
            off_mode=0.5):
    return {"name": name, "family": family,
            "row": {"rank_ic_t": t, "periods": periods},
            "gate": {"target_state": gate},
            "survives_fdr_10pct": fdr,
            "cross_section_breadth": {"median_off_mode_share": off_mode,
                                      "periods": periods}}


def test_campaign_wide_fdr_is_strictly_harsher_than_per_family():
    rows = [_result("a", "f1", 3.0), _result("b", "f1", 0.2)]
    rows += [_result("x%d" % i, "f2", 0.1) for i in range(40)]
    fam = s25.apply_fdr(rows, family="f1")
    camp = r27.apply_campaign_fdr(rows)
    assert camp["family_size"] == len(rows)
    assert camp["family_fixed_before_evaluation"] is True
    assert set(camp["survivors_q10"]) <= set(fam["survivors_q10"]) | {
        r["name"] for r in rows if r.get("survives_fdr_10pct")}


def test_campaign_fdr_family_covers_every_executed_hypothesis():
    rows = [_result("a", "f1", 1.0), _result("b", "f2", 1.0)]
    camp = r27.apply_campaign_fdr(rows)
    assert {m["name"] for m in camp["members"]} == {"a", "b"}


# =========================================================================== #
# 10. A degenerate cross-section is INSUFFICIENT_SAMPLE, not a null
# =========================================================================== #
def test_all_ties_cross_section_is_not_reported_as_a_measured_null():
    r = _result("tied", "f1", 0.1, periods=60, off_mode=0.001)
    a = r27.sample_adequacy(r)
    assert a["adequate"] is False
    assert any("OFF_MODE_SHARE" in x for x in a["reasons"])
    r["sample_adequacy"] = a
    v = r27.classify_hypothesis(r)
    assert v["terminal_state"] == r27.T_INSUFFICIENT


def test_too_few_periods_is_insufficient_sample():
    r = _result("thin", "f1", 3.0, periods=4)
    r["sample_adequacy"] = r27.sample_adequacy(r)
    assert r27.classify_hypothesis(r)["terminal_state"] == r27.T_INSUFFICIENT


class _Panel:
    """Minimal stand-in exposing only what raw_breadth reads."""
    def __init__(self, rows):
        self.rows = rows

    def months_for(self, horizon):
        return sorted(self.rows)


def test_event_rarer_than_the_winsor_fraction_is_named_not_silently_dropped():
    """A 0/1 indicator whose positive rate is below the released 1 % winsor
    fraction is clipped to a constant, Spearman becomes undefined and the period
    is dropped. The result must SAY that rather than report an empty null."""
    spec = r27.factor_by_name("r27_forward_split_1y")
    rows = {"2015-01": {
        "s%d" % i: {"factors": {spec.name: (1.0 if i < 2 else 0.0)},
                    "forward": {"h3m": 0.01}}
        for i in range(500)}}
    raw = r27.raw_breadth(_Panel(rows), spec, horizon="h3m")
    assert raw["median_off_mode_share_raw"] == pytest.approx(0.004)
    assert raw["flattened_by_winsorizer"] is True
    res = {"name": spec.name, "family": "f", "row": {"periods": 0},
           "cross_section_breadth": {"median_off_mode_share": None},
           "raw_breadth": raw}
    a = r27.sample_adequacy(res)
    assert a["adequate"] is False
    assert any("WINSOR_FRACTION" in x for x in a["reasons"])
    assert any("present in 1 months" in x for x in a["reasons"])


def test_a_common_event_is_not_flagged_as_winsor_flattened():
    spec = r27.factor_by_name("r27_forward_split_1y")
    rows = {"2015-01": {
        "s%d" % i: {"factors": {spec.name: (1.0 if i < 100 else 0.0)},
                    "forward": {"h3m": 0.01}}
        for i in range(500)}}
    raw = r27.raw_breadth(_Panel(rows), spec, horizon="h3m")
    assert raw["flattened_by_winsorizer"] is False


def test_breadth_measures_the_share_away_from_the_modal_value():
    periods = [{"as_of": "2015-01-01",
                "names": [("a", 0.0, 0.1)] * 98 + [("b", 1.0, 0.2)] * 2}]
    b = r27._breadth(periods)
    assert b["median_off_mode_share"] == pytest.approx(0.02)


# =========================================================================== #
# 11. Terminal classification is total and uses the released verdicts
# =========================================================================== #
def test_a_gate_clearing_fdr_surviving_redundant_result_is_redundant():
    r = _result("x", "f1", 3.0, gate="KEEP_FOR_RESEARCH", fdr=True)
    r["sample_adequacy"] = r27.sample_adequacy(r)
    incr = {"candidates": {"x": {"classification": "REDUNDANT",
                                 "not_independent_reason":
                                     "REDUNDANT_WITH_EXISTING_SIGNAL"}}}
    assert r27.classify_hypothesis(r, incr)["terminal_state"] == r27.T_REDUNDANT


def test_independent_survivor_of_both_scopes_is_a_challenger():
    r = _result("x", "f1", 4.0, gate="KEEP_FOR_RESEARCH", fdr=True)
    r["survives_campaign_fdr_10pct"] = True
    r["sample_adequacy"] = r27.sample_adequacy(r)
    incr = {"candidates": {"x": {"classification": "INDEPENDENT_ALPHA"}}}
    assert r27.classify_hypothesis(r, incr)["terminal_state"] == r27.T_CHALLENGER


def test_family_of_only_inadequate_hypotheses_is_not_called_rejected():
    vs = [{"name": "a", "family": "f", "terminal_state": r27.T_INSUFFICIENT,
           "reason": "", "sample_adequacy": {"reasons": ["OFF_MODE"],
                                             "periods": 3}}]
    assert r27.classify_family("f", vs)["terminal_state"] == r27.T_INSUFFICIENT


def test_every_terminal_state_is_in_the_released_vocabulary():
    assert set(r27._STRENGTH) == set(r27.TERMINAL_STATES)
    for forbidden in r27.FORBIDDEN_STATES:
        assert forbidden not in r27.TERMINAL_STATES


# =========================================================================== #
# 12. The frontier audit is a hard gate
# =========================================================================== #
def test_commit_is_impossible_while_a_free_family_is_still_executable():
    inv = {"entries": [{"family": "z", "state": r27.T_REJECTED,
                        "q7_executable_now": True, "reason": "runnable"}],
           "executable_free_owned_high_priority_families": 1}
    audit = r27.final_frontier_audit(inv)
    assert audit["commit_ok"] is False
    assert audit["blocker"] == "EXECUTABLE_FREE_ALPHA_RESEARCH_REMAINS"


def test_commit_is_impossible_with_a_non_terminal_family_state():
    inv = {"entries": [{"family": "z", "state": "NEW_FREE_INFORMATION",
                        "q7_executable_now": False, "reason": ""}],
           "executable_free_owned_high_priority_families": 0}
    audit = r27.final_frontier_audit(inv)
    assert audit["commit_ok"] is False
    assert audit["blocker"] == "NON_TERMINAL_FAMILY_STATE"


def test_every_not_run_frontier_entry_is_non_executable_and_terminal():
    for spec in r27.NOT_RUN_FRONTIER:
        e = r27._frontier_entry(**spec)
        assert e["q7_executable_now"] is False, spec["family"]
        assert e["state"] in r27.TERMINAL_STATES, spec["family"]
        assert e["state"] not in r27.FORBIDDEN_STATES
        assert len(e["reason"]) > 40, spec["family"]


def test_seven_question_rule_makes_an_unanswered_free_family_executable():
    e = r27._frontier_entry(
        "hypothetical", mechanism="m", available=True, pit_safe=True,
        survivorship_ok=True, sample_ok=True, distinct=True,
        already_tested=False, state=r27.T_REJECTED, reason="x" * 50)
    assert e["q7_executable_now"] is True
    assert e["executability"] == r27.EXECUTABLE


# =========================================================================== #
# 13. Exhaustion memory prevents duplicate families
# =========================================================================== #
def test_exhaustion_state_records_a_reopen_condition_for_every_concept():
    fv = {r27.FAM_FILING: {"terminal_state": r27.T_REJECTED, "hypotheses": 10,
                           "survivors": [], "reason": "r"}}
    inv = {"entries": [r27._frontier_entry(**r27.NOT_RUN_FRONTIER[0])]}
    st = r27.research_exhaustion_update(
        family_verdicts=fv, inventory=inv,
        hypothesis_verdicts=[{"name": "a", "family": r27.FAM_FILING,
                              "terminal_state": r27.T_REJECTED, "reason": "r",
                              "gate": "REJECT", "survives_family_fdr": False,
                              "survives_campaign_fdr": False,
                              "independence": None}])
    assert st["hypotheses_registered"] == 1
    for concept in st["concepts"].values():
        assert concept["reopen_if"], concept
    assert "duplicate_family_prevention" in st


# =========================================================================== #
# 14. The frozen forward challenger is never touched
# =========================================================================== #
def test_challenger_continuity_detects_a_changed_spec_hash():
    ok = r27.forward_challenger_continuity(
        shadow_books=[{"shadow_book_id": "sb_c9_qualityprofi_e490533606"}],
        book_payload={"candidate_id": s26.FROZEN_CHALLENGER_CANDIDATE_ID,
                      "read_only": True, "marks": [],
                      "inception": {"membership": [{}] * 100,
                                    "frozen_spec": {
                                        "spec_hash":
                                            r27.FROZEN_CHALLENGER_SPEC_HASH}}})
    assert ok["continuity_ok"] is True
    assert ok["forward_marks"] == 0

    bad = r27.forward_challenger_continuity(
        shadow_books=[],
        book_payload={"candidate_id": s26.FROZEN_CHALLENGER_CANDIDATE_ID,
                      "read_only": True, "marks": [],
                      "inception": {"membership": [{}] * 100,
                                    "frozen_spec": {"spec_hash": "deadbeef"}}})
    assert bad["continuity_ok"] is False
    assert bad["checks"]["frozen_spec_hash_unchanged"] is False


def test_challenger_continuity_detects_a_truncated_membership():
    bad = r27.forward_challenger_continuity(
        shadow_books=[],
        book_payload={"candidate_id": s26.FROZEN_CHALLENGER_CANDIDATE_ID,
                      "read_only": True, "marks": [],
                      "inception": {"membership": [{}] * 3,
                                    "frozen_spec": {
                                        "spec_hash":
                                            r27.FROZEN_CHALLENGER_SPEC_HASH}}})
    assert bad["checks"]["inception_membership_intact"] is False


def test_campaign_never_declares_a_promotion_path():
    """The campaign may DESCRIBE the promotion boundary in prose; it may not
    CALL across it. The check is on executable code, with strings and comments
    stripped, so documenting the rule cannot fail the test that enforces it."""
    import ast
    from alpha_agent import stage27_alpha_exhaustion as mod
    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    forbidden = {"maybe_activate_shadow_books", "create_shadow_book",
                 "record_mark", "append_mark", "set_lifecycle_state",
                 "promote", "register_candidate", "upsert_candidate"}
    called, attrs = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            attrs.add(node.attr)
        if isinstance(node, ast.Call):
            fn = node.func
            called.add(getattr(fn, "attr", None) or getattr(fn, "id", None))
    assert not (forbidden & called), sorted(forbidden & called)
    assert "AUTOMATIC_PROMOTION_ALLOWED" not in attrs
    # The registry is opened, but only for the two read methods the continuity
    # proof needs.
    assert {"list_shadow_books", "counts_by_state", "close"} >= (
        called & {"list_shadow_books", "counts_by_state", "close",
                  "create_shadow_book", "record_mark"})


# =========================================================================== #
# 15. The released panel hook stays byte-identical without an enricher
# =========================================================================== #
def test_stage26_build_panel_default_path_is_unchanged():
    import inspect
    sig = inspect.signature(s26.build_panel)
    assert sig.parameters["enrich"].default is None
    src = inspect.getsource(s26.build_panel)
    assert "if enrich is not None:" in src


def test_enricher_attaches_primitives_only():
    """The enricher may add observables; it may never add a computed hypothesis."""
    enr = r27.BehaviourEnricher()
    rec = {"cur": {}, "prior": {}}
    enr(rec, symbol="ACME", cik="77", formation_date="2015-06-30",
        as_of="2015-06-28")
    assert rec["cur"] == {}
    assert enr.stats["rows"] == 1
    for spec in r27.ALL_R27_FACTORS:
        assert spec.value(rec) is None, spec.name


# =========================================================================== #
# 16. Purchase gate applies the released rule
# =========================================================================== #
def test_purchase_gate_authorises_nothing_and_says_why():
    audit = {"executable_free_owned_high_priority_families": 0}
    inv = {"entries": [{"family": "analyst_estimate_revisions",
                        "state": r27.T_PAID}]}
    g = r27.external_data_purchase_gate(audit=audit, inventory=inv)
    assert g["purchase_authorised"] is False
    assert g["decision"] == "REJECT"
    assert g["condition_a_satisfied"] is True
    assert g["is_analyst_revisions_still_highest"] is False
    assert all(c["recommendation"] in ("BUY_CANDIDATE", "WAIT", "REJECT")
               for c in g["ranked_candidates"])


# =========================================================================== #
# 17. The generic SEC acquirer keeps the released one byte-identical
# =========================================================================== #
def _zip_bytes(members: dict) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, payload in members.items():
            zf.writestr(name, payload)
    return buf.getvalue()


def _range_transport(archive: bytes):
    def _t(request, timeout):
        if request["method"] == "HEAD":
            return {"status": 200,
                    "headers": {"Content-Length": str(len(archive)),
                                "Accept-Ranges": "bytes", "ETag": "e"}}
        rng = request["headers"].get("Range", "")
        lo, hi = rng.replace("bytes=", "").split("-")
        return {"status": 206, "headers": {},
                "body": archive[int(lo):int(hi) + 1]}
    return _t


def test_generic_acquirer_fetches_only_the_named_members(tmp_path):
    archive = _zip_bytes({"SUBMISSION.tsv": b"a\n", "REPORTINGOWNER.tsv": b"b\n",
                          "NONDERIV_TRANS.tsv": b"c\n",
                          "DERIV_TRANS.tsv": b"x" * 500_000})
    acq = fsds.QuarterlyDataSetAcquirer(
        "insider_transactions_data_sets", cache_root=tmp_path,
        user_agent="t (a@b.c)", transport=_range_transport(archive),
        request_delay_seconds=0.0, sleep=lambda _s: None)
    res = acq.acquire_quarter(2015, 1)
    assert res["disposition"] == fsds.D_COMPLETE
    assert set(res["member_sha256"]) == {"SUBMISSION.tsv", "REPORTINGOWNER.tsv",
                                         "NONDERIV_TRANS.tsv"}
    assert res["bytes_fetched_over_network"] < 100_000
    assert acq.cached(2015, 1)["disposition"] == fsds.D_CACHED


def test_released_fsds_acquirer_signature_is_untouched():
    import inspect
    sig = inspect.signature(fsds.FinancialStatementDataSetsAcquirer.__init__)
    assert set(sig.parameters) == {
        "self", "cache_root", "user_agent", "transport", "timeout",
        "request_delay_seconds", "sleep", "clock"}
    assert fsds.MEMBER_NAME == "sub.txt"
    assert fsds.SOURCE_PATH_TEMPLATE == (
        "/files/dera/data/financial-statement-data-sets/%dq%d.zip")


def test_every_registered_dataset_declares_members_and_a_first_quarter():
    for name, spec in fsds.QUARTERLY_DATASETS.items():
        assert spec["members"], name
        assert len(spec["first"]) == 2, name
        assert spec["path_template"].count("%d") == 2, name
        assert spec["description"], name


# =========================================================================== #
# 18. Artifacts contract
# =========================================================================== #
def test_every_executed_family_has_a_named_manifest_and_results_artifact():
    for fam, _specs, _origin in r27.REGISTERED_FAMILIES:
        assert fam in r27.FAMILY_ARTIFACT_NAMES, fam
        names = r27.FAMILY_ARTIFACT_NAMES[fam]
        assert names[0].endswith("_manifest.json")
        assert names[1].endswith("_results.json")


def test_required_campaign_artifacts_are_all_declared():
    required = {
        "campaign_start_state.json", "family_frontier_inventory.json",
        "family_execution_ledger.json", "all_candidate_incrementality.json",
        "ensemble_comparison.json", "alpha_rankings.json",
        "challenger_status.json", "forward_evidence_status.json",
        "hoc_relevance.json", "research_exhaustion_state.json",
        "paid_data_gate.json", "final_frontier_audit.json",
        "campaign_summary.json",
    }
    assert required <= set(r27.ARTIFACT_MAP)
