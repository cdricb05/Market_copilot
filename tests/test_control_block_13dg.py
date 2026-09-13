"""Regressions for the preregistered Schedule 13D/G control-block axis.

These tests exist to make the honesty claims CHECKABLE rather than asserted:
that the preregistration frozen before the results is byte-for-byte the one
executed, that the frozen constants are the frozen constants, that no second
scorer was written, and that each of the four measured EDGAR traps stays fixed.

Everything here is pure and local. No network, no store, no live path.
"""
from __future__ import annotations

import hashlib
import subprocess
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from alpha_agent.alpha_recovery import control_block_alpha as CBA
from alpha_agent.alpha_recovery import control_block_data as CBD
from alpha_agent.alpha_recovery import control_block_events as CBE

REPO = Path(__file__).resolve().parents[1]
PREREG = REPO / "research" / "preregistration" / "CONTROL_BLOCK_13DG_PREREGISTRATION.md"

#: sha256 of the preregistration BLOB as committed at 1d3f491, before any
#: result for this family existed. Read from git rather than from the working
#: tree, so the test cannot be satisfied by a later edit.
PREREG_SHA256 = "dc950cf44f9ed71343be3566575b22a202183fbf31f78ce720c5798f4602f016"
PREREG_COMMIT = "1d3f491"


# --------------------------------------------------------------------------- #
# The preregistration was not moved after the results
# --------------------------------------------------------------------------- #
def test_preregistration_blob_is_unchanged_since_it_was_frozen():
    out = subprocess.run(
        ["git", "-C", str(REPO), "cat-file", "blob",
         "%s:research/preregistration/CONTROL_BLOCK_13DG_PREREGISTRATION.md" % PREREG_COMMIT],
        capture_output=True)
    if out.returncode != 0:
        pytest.skip("git object not available in this checkout")
    assert hashlib.sha256(out.stdout).hexdigest() == PREREG_SHA256
    assert PREREG.exists()
    assert hashlib.sha256(PREREG.read_bytes()).hexdigest() == PREREG_SHA256, \
        "the working-tree preregistration differs from the one that was frozen"


def test_runner_points_at_the_frozen_commit():
    assert CBA.PREREGISTRATION_COMMIT == PREREG_COMMIT
    assert CBA.PREREGISTRATION.endswith("CONTROL_BLOCK_13DG_PREREGISTRATION.md")


# --------------------------------------------------------------------------- #
# Frozen constants
# --------------------------------------------------------------------------- #
def test_frozen_cells_and_horizons():
    assert CBA.HORIZONS_FROZEN == (5, 21, 63)
    assert CBE.CELLS == (CBE.CELL_A, CBE.CELL_B)
    assert len(CBE.CELLS) * len(CBA.HORIZONS_FROZEN) == 6


def test_material_increase_threshold_is_the_legal_one():
    """Rule 13d-2(a) deems 1% material. A threshold taken from law cannot have
    been chosen by looking at returns."""
    assert CBE.MATERIAL_INCREASE_PP == 1.00


def test_cost_ladder_and_stopping_rules_are_frozen():
    assert CBA.COST_LADDER_BPS == (1.0, 2.0, 5.0)
    assert CBA.DESK_EQUITY_COST_BPS == pytest.approx(12.5)
    assert CBA.MIN_DATE_COVERAGE == 0.95
    assert CBA.MAX_UNCOVERED_SHARE == 0.20


def test_no_second_scorer_was_written():
    for mod in (CBA, CBE, CBD):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "def run_cell" not in src, \
            "%s defines a scorer; the canonical one must be reused" % mod.__name__


# --------------------------------------------------------------------------- #
# TRAP 1 - EDGAR renamed the form at the structured mandate
# --------------------------------------------------------------------------- #
def test_both_edgar_spellings_are_the_same_schedule():
    assert CBD.FORM_MAP["SC 13D"] == CBD.FORM_MAP["SCHEDULE 13D"] == "13D"
    assert CBD.FORM_MAP["SC 13D/A"] == CBD.FORM_MAP["SCHEDULE 13D/A"] == "13D/A"
    assert CBD.FORM_MAP["SC 13G"] == CBD.FORM_MAP["SCHEDULE 13G"] == "13G"
    assert CBD.FORM_MAP["SC 13G/A"] == CBD.FORM_MAP["SCHEDULE 13G/A"] == "13G/A"


def test_going_private_schedule_is_not_a_beneficial_ownership_schedule():
    assert "SC 13E3" not in CBD.FORM_MAP
    assert "SC 13E3/A" not in CBD.FORM_MAP


def test_the_canonical_keep_list_would_have_dropped_the_modern_schedules():
    """The reason the rename matters, asserted rather than remembered."""
    from alpha_agent.r63 import acquire as ACQ
    assert ACQ.keep_form("SC 13D") is True
    assert ACQ.keep_form("SCHEDULE 13D") is False


# --------------------------------------------------------------------------- #
# TRAP 2 - the acceptance stamp is UTC
# --------------------------------------------------------------------------- #
def test_acceptance_is_converted_from_utc_to_eastern():
    # January -> standard time, UTC-5
    assert CBD.acceptance_et("2019-01-10T21:30:00.000Z") == datetime(2019, 1, 10, 16, 30)
    # July -> daylight time, UTC-4
    assert CBD.acceptance_et("2019-07-10T21:30:00.000Z") == datetime(2019, 7, 10, 17, 30)
    # the conversion can move the DAY, which is the whole point
    assert CBD.acceptance_et("2019-01-11T02:30:00.000Z") == datetime(2019, 1, 10, 21, 30)


def test_unreadable_acceptance_is_none_not_a_guess():
    assert CBD.acceptance_et("") is None
    assert CBD.acceptance_et("not-a-timestamp") is None


# --------------------------------------------------------------------------- #
# TRAP 3 - the index names the XSL-rendered document
# --------------------------------------------------------------------------- #
def test_root_leaf_strips_the_renderer_directory():
    assert CBD._root_leaf("xslSCHEDULE_13G_X02/primary_doc.xml") == "primary_doc.xml"
    assert CBD._root_leaf("xslSCHEDULE_13D_X01/primary_doc.xml") == "primary_doc.xml"
    assert CBD._root_leaf("d664251dsc13ga.htm") == "d664251dsc13ga.htm"


# --------------------------------------------------------------------------- #
# TRAP 4 - the two structured schedules use different element names
# --------------------------------------------------------------------------- #
G_XML = b"""<?xml version="1.0"?><edgarSubmission><formData><coverPageHeader>
<issuerInfo><issuerCik>0000078239</issuerCik><issuerName>PVH CORP</issuerName>
<issuerCusips><issuerCusipNumber>693656100</issuerCusipNumber></issuerCusips></issuerInfo>
<coverPageHeaderReportingPersonDetails><reportingPersonName>FMR LLC</reportingPersonName>
<classPercent>14.7</classPercent></coverPageHeaderReportingPersonDetails>
</coverPageHeader></formData></edgarSubmission>"""

D_XML = b"""<?xml version="1.0"?><edgarSubmission><formData><coverPageHeader>
<issuerInfo><issuerCIK>0000320193</issuerCIK><issuerCUSIP>037833100</issuerCUSIP></issuerInfo>
<reportingPersons><reportingPersonInfo><reportingPersonCIK>0001067983</reportingPersonCIK>
<reportingPersonName>BERKSHIRE HATHAWAY INC</reportingPersonName>
<percentOfClass>13.1</percentOfClass></reportingPersonInfo></reportingPersons>
<dateOfEvent>2025-01-02</dateOfEvent></coverPageHeader></formData></edgarSubmission>"""


def test_structured_13g_vocabulary_is_read():
    r = CBD.parse_document(G_XML, structured=True)
    assert r["reader"] == "STRUCTURED_XML"
    assert r["percents"] == [14.7]
    assert r["cusip"] == "693656100"
    assert r["issuer_cik_stated"] == "78239"
    assert "FMR LLC" in r["reporting_persons"]


def test_structured_13d_vocabulary_is_read_too():
    """A 13D says percentOfClass / issuerCUSIP. Reading only the 13G spelling
    would silently drop every structured 13D - the activist half of the axis."""
    r = CBD.parse_document(D_XML, structured=True)
    assert r["percents"] == [13.1]
    assert r["cusip"] == "037833100"
    assert r["issuer_cik_stated"] == "320193"
    assert r["reporting_person_ciks"] == ["0001067983"]
    assert r["event_date"] == "2025-01-02"


TEXT_COVER = (b"<html><body><table><tr><td>CUSIP No. 037833 10 0</td></tr>"
              b"<tr><td>11</td><td>PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW (9)"
              b"</td></tr><tr><td>&nbsp;6.37%</td></tr></table></body></html>")


def test_text_cover_page_is_read():
    r = CBD.parse_document(TEXT_COVER, structured=False)
    assert r["reader"] == "TEXT_COVER_PAGE"
    assert r["percents"] == [6.37]
    assert r["cusip"].startswith("037833")


BARE_COVER = (b"<html><body>11. PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW (9)"
              b" 7.91 <br>12. TYPE OF REPORTING PERSON* BD, IA</body></html>")


def test_a_percent_printed_without_a_per_cent_sign_is_still_read():
    """Measured: 389 of 400 sampled failures were exactly this shape. Requiring
    the sign would discard 6% of the stream and break those filers' chains."""
    assert CBD.parse_document(BARE_COVER, structured=False)["percents"] == [7.91]


def test_the_next_boxes_row_number_is_never_read_as_a_percentage():
    """Row 12's caption ends row 11's value, and a bare integer is not a
    percent - otherwise '12. TYPE OF REPORTING PERSON' becomes 12%."""
    empty = (b"<html>11. PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW (9) "
             b"Not Applicable 12. TYPE OF REPORTING PERSON HC</html>")
    assert CBD.parse_document(empty, structured=False)["percents"] == []


def test_a_percent_above_one_hundred_is_dropped_not_clipped():
    bad = TEXT_COVER.replace(b"6.37%", b"637%")
    assert CBD.parse_document(bad, structured=False)["percents"] == []


# --------------------------------------------------------------------------- #
# The decision session
# --------------------------------------------------------------------------- #
DATES = np.array(["2019-01-07", "2019-01-08", "2019-01-09", "2019-01-10",
                  "2019-01-11", "2019-01-14"], dtype="datetime64[ns]")


def test_a_filing_before_the_close_is_in_that_session():
    # 18:30Z in January = 13:30 Eastern, before the 16:00 close
    assert CBE.decision_index(DATES, "2019-01-09T18:30:00.000Z") == 2


def test_a_filing_after_the_close_waits_for_the_next_session():
    # 22:30Z = 17:30 Eastern, after the close
    assert CBE.decision_index(DATES, "2019-01-09T22:30:00.000Z") == 3


def test_a_filing_at_the_close_instant_waits():
    # exactly 16:00 Eastern is NOT in that close
    assert CBE.decision_index(DATES, "2019-01-09T21:00:00.000Z") == 3


def test_a_weekend_filing_waits_for_the_next_session():
    # Saturday 2019-01-12, any hour -> Monday the 14th
    assert CBE.decision_index(DATES, "2019-01-12T18:00:00.000Z") == 5


def test_an_event_past_the_panel_is_none_not_clamped():
    assert CBE.decision_index(DATES, "2030-01-01T18:00:00.000Z") is None


# --------------------------------------------------------------------------- #
# Cell construction
# --------------------------------------------------------------------------- #
def _substrate():
    E = {"symbols": np.array(["AAA", "BBB", "CCC"]), "dates": DATES,
         "sym2cik": {"AAA": "111", "BBB": "222"}}
    elig = np.ones((3, len(DATES)), dtype=bool)
    return E, elig


def _sched(form, acc, when, cik="111", pcts=None):
    return {"issuer_cik": cik, "form": form, "accession": acc,
            "acceptance_utc": when, "filing_date": when[:10],
            "percents": list(pcts or []), "reporting_person_ciks": ["999"],
            "primary_document": "x.htm"}


def test_cell_a_admits_only_initial_schedules():
    E, elig = _substrate()
    recs = [_sched("13G", "a-1", "2019-01-07T18:00:00.000Z"),
            _sched("13G/A", "a-2", "2019-01-08T18:00:00.000Z"),
            _sched("13D/A", "a-3", "2019-01-09T18:00:00.000Z"),
            _sched("13D", "a-4", "2019-01-10T18:00:00.000Z")]
    ev = CBE.build_events(E, elig, verbose=False, recs=recs)["events"][CBE.CELL_A]
    assert sorted(e["form"] for e in ev) == ["13D", "13G"]


def test_same_issuer_same_session_collapses_to_one_event():
    """Several filers reporting the same block on the same day are ONE event,
    not several - otherwise a crowded name is counted repeatedly."""
    E, elig = _substrate()
    recs = [_sched("13G", "a-1", "2019-01-07T18:00:00.000Z"),
            _sched("13G", "a-2", "2019-01-07T19:00:00.000Z"),
            _sched("13G", "a-3", "2019-01-07T14:00:00.000Z")]
    ev = CBE.build_events(E, elig, verbose=False, recs=recs)["events"][CBE.CELL_A]
    assert len(ev) == 1
    assert ev[0]["n_collapsed"] == 3


def test_an_unidentified_issuer_is_dropped_and_counted():
    E, elig = _substrate()
    recs = [_sched("13G", "a-1", "2019-01-07T18:00:00.000Z", cik="999999")]
    built = CBE.build_events(E, elig, verbose=False, recs=recs)
    assert built["events"][CBE.CELL_A] == []
    assert built["stats"]["unidentified_issuer"] == 1


def test_cell_b_needs_a_prior_observation_and_the_full_threshold():
    E, elig = _substrate()
    recs = [_sched("13G", "b-1", "2019-01-07T18:00:00.000Z", pcts=[5.0]),
            _sched("13G/A", "b-2", "2019-01-08T18:00:00.000Z", pcts=[5.9]),
            _sched("13G/A", "b-3", "2019-01-09T18:00:00.000Z", pcts=[7.0])]
    ev = CBE.build_events(E, elig, verbose=False, recs=recs)["events"][CBE.CELL_B]
    # 5.0 -> 5.9 is 0.9pp and does NOT qualify; 5.9 -> 7.0 is 1.1pp and does
    assert [e["date"] for e in ev] == ["2019-01-09"]
    assert ev[0]["delta"] == pytest.approx(1.1)


def test_a_decrease_is_not_an_event_but_still_advances_the_state():
    E, elig = _substrate()
    recs = [_sched("13G", "b-1", "2019-01-07T18:00:00.000Z", pcts=[9.0]),
            _sched("13G/A", "b-2", "2019-01-08T18:00:00.000Z", pcts=[5.0]),
            _sched("13G/A", "b-3", "2019-01-09T18:00:00.000Z", pcts=[6.5])]
    ev = CBE.build_events(E, elig, verbose=False, recs=recs)["events"][CBE.CELL_B]
    # the rise is measured from 5.0, the state after the decrease - not from 9.0
    assert len(ev) == 1 and ev[0]["delta"] == pytest.approx(1.5)


def test_a_joint_filings_block_is_the_maximum_cover_page():
    assert CBE._block_percent({"percents": [5.34, 5.34, 2.69]}) == 5.34
    assert CBE._block_percent({"percents": []}) is None


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #
def test_class_share_punctuation_is_normalised_both_ways():
    v = CBE._ticker_variants("BF.B")
    assert "BF-B" in v and "BFB" in v and "BF.B" in v


def test_identity_never_uses_issuer_names():
    E, _elig = _substrate()
    _rows, meta = CBE.identity_map(E, use_sec_ticker_map=False)
    assert meta["issuer_name_matching_used"] is False
    src = Path(CBE.__file__).read_text(encoding="utf-8")
    assert "issuer_name" not in src.replace("issuer_name_matching_used", "")


def test_a_cik_on_several_panel_rows_is_reported_not_silently_picked():
    E = {"symbols": np.array(["AAA", "AAA-201811"]), "dates": DATES,
         "sym2cik": {"AAA": "111", "AAA-201811": "111"}}
    rows, meta = CBE.identity_map(E, use_sec_ticker_map=False)
    assert rows["111"] == [0, 1]
    assert meta["ciks_mapping_to_multiple_rows"] == 1


# --------------------------------------------------------------------------- #
# The panel signal
# --------------------------------------------------------------------------- #
def test_signal_is_zero_not_nan_for_an_eligible_name_without_an_event():
    """A NaN would restrict the scorer's covered rows to event names only and
    silently destroy the cross-section."""
    E, elig = _substrate()
    events = [{"row": 0, "t": 2, "date": "2019-01-09"}]
    sig, per = CBA.signal_matrix(E, elig, events, np.array([2, 4]), 2)
    assert sig[0, 2] == 1.0
    assert sig[1, 2] == 0.0 and not np.isnan(sig[1, 2])
    assert per[0]["names_with_event"] == 1


def test_every_event_is_counted_once_across_the_grid():
    E, elig = _substrate()
    events = [{"row": 0, "t": 1, "date": "2019-01-08"},
              {"row": 1, "t": 2, "date": "2019-01-09"}]
    sig, _ = CBA.signal_matrix(E, elig, events, np.array([2, 4]), 2)
    assert sig[0, 2] == 1.0 and sig[1, 2] == 1.0
    assert sig[0, 4] == 0.0 and sig[1, 4] == 0.0


# --------------------------------------------------------------------------- #
# Verdict order
# --------------------------------------------------------------------------- #
def test_coverage_is_judged_before_anything_else():
    v = CBA.verdict_for({}, {}, {}, {}, {"uncovered_share": 0.47},
                        {"biased": False}, fdr_pass=True)
    assert v["verdict"] == CBA.V_DATA_HOLD


def test_missingness_bias_is_judged_before_the_scorer():
    v = CBA.verdict_for({}, {}, {}, {}, {"uncovered_share": 0.0},
                        {"biased": True, "ann_diff": 0.05}, fdr_pass=True)
    assert v["verdict"] == CBA.V_DATA_HOLD
    assert "cannot be assessed" in v["why"]


def test_the_post_hoc_runup_can_never_qualify_anything():
    """It explains a result; it must never BE one."""
    src = Path(CBA.__file__).read_text(encoding="utf-8")
    body = src.split("def verdict_for", 1)[1]
    assert "prior_21d_runup" not in body
    assert "announcement" not in body


# --------------------------------------------------------------------------- #
# The acquisition honours the SEC's rate, not merely its serialisation
# --------------------------------------------------------------------------- #
def test_one_accession_spelling_is_used_everywhere():
    """EDGAR's quarterly index spells an accession WITH dashes and the
    submissions history WITHOUT them. A key that differs only in punctuation
    joins nothing and raises nothing - the filer would simply be 'unidentified'
    on every filing, which reads as missing data rather than as a bug."""
    assert CBD._norm_accession("0000004405-19-000001") == "000000440519000001"
    assert CBD._norm_accession("000000440519000001") == "000000440519000001"
    assert CBD._norm_accession(None) == ""
    src = Path(CBE.__file__).read_text(encoding="utf-8")
    assert 'replace("-", "")' not in src, \
        "accession normalisation must go through the one helper"


def test_the_rate_gate_bounds_the_global_request_rate():
    """Concurrency hides latency; it must not raise the request rate. Three
    workers behind ONE gate issue requests no faster than the serial loop."""
    import time as _t
    gate = CBD._RateGate(0.02)
    t0 = _t.time()
    for _ in range(10):
        gate.wait()
    assert _t.time() - t0 >= 0.02 * 9


def test_worker_count_does_not_appear_in_the_rate():
    src = Path(CBD.__file__).read_text(encoding="utf-8")
    assert "_RateGate(ACQ.MIN_INTERVAL_S)" in src, \
        "the gate must be built from the canonical interval, not a local number"


def test_a_dead_signal_is_not_rescued_by_a_kind_gate():
    cell = {"effective_periods": 100, "conditional": {"t": 0.3}}
    v = CBA.verdict_for(cell, {}, {}, {}, {"uncovered_share": 0.0},
                        {"biased": False}, fdr_pass=True)
    assert v["verdict"] == CBA.V_NO_EDGE
