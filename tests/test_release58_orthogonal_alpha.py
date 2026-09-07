"""Release 58 - the orthogonal alpha offensive: regressions that keep it honest.

These tests protect the properties that make an R58 verdict worth anything:

* the point-in-time reader cannot see a fact filed after the decision date, and
  the four defects found during its pre-experiment correctness check stay fixed
* the lockbox really was untouched when the variant selection was persisted
* no threshold moved between the protocol and the code
* the multiple-testing denominator is the one the protocol fixed in advance
* a missing fundamental is NaN (out of the universe), never a silent zero
* the frozen forward challengers are immutable, carry zero forward observations,
  and no challenger was frozen for a family whose cross-section is empty
* nothing in the release creates an order, a fill or a promotion

Artifact-dependent tests skip cleanly when the R58 research root is absent, so
the suite still runs on a machine without the licensed data.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from alpha_agent import r58
from alpha_agent.r58 import fundamentals as FU

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")


# --------------------------------------------------------------------------- #
# Protocol integrity - no threshold may drift between document and code
# --------------------------------------------------------------------------- #
def test_protocol_exists_and_parses():
    p = r58.protocol()
    assert p["release"] == "R58"
    assert p["campaign_id"] == r58.CAMPAIGN_ID
    assert p["registered_before_any_experiment_ran"] is True


def test_protocol_thresholds_match_code():
    p = r58.protocol()
    assert p["partition"]["layers"]["HISTORICAL_IN_SAMPLE"].startswith(
        r58.DISCOVERY_START)
    assert p["partition"]["layers"]["HISTORICAL_VALIDATION"].startswith(
        r58.VALIDATION_START)
    assert p["partition"]["layers"]["HISTORICAL_FINAL_OOS"].startswith(
        r58.LOCKBOX_START)
    assert p["conventions"]["equity_cost_bps_per_side"] == \
        r58.EQ_COST_RATE_PER_SIDE * 1e4
    assert p["conventions"]["equity_decision_cadence_sessions"] == r58.CADENCE
    assert p["conventions"]["equity_horizon_sessions"] == r58.HORIZON
    assert "%.3f" % r58.GATE_MATERIALITY in p["gates"]["economic_materiality"] \
        or "1.5%" in p["gates"]["economic_materiality"]


def test_fdr_family_set_matches_protocol():
    p = r58.protocol()
    assert tuple(p["fdr_family_set"]) == r58.FDR_FAMILIES
    assert len(r58.FDR_FAMILIES) == 13
    assert "B0" not in r58.FDR_FAMILIES, "the diagnostic reference must not " \
                                         "consume FDR budget"


def test_safety_envelope_is_declared_off():
    for flag in ("creates_orders", "creates_fills", "broker_enabled",
                 "promotes_model", "activates_sleeve",
                 "mutates_operational_store", "automation_enabled"):
        assert r58.SAFETY[flag] is False
    assert r58.protocol()["safety"][flag] is False


# --------------------------------------------------------------------------- #
# The point-in-time reader
# --------------------------------------------------------------------------- #
def _state(rows):
    st = FU.CompanyState()
    for r in rows:
        st.absorb(*r)
    return st


def test_reader_never_sees_a_fact_filed_later():
    """A fact absorbed later cannot change an earlier snapshot."""
    early = [("2020-02-01", "Assets", None, "2019-12-31", None, 100.0)]
    st = _state(early)
    snap_before = st.snapshot()
    st.absorb("2021-02-01", "Assets", None, "2020-12-31", None, 999.0)
    snap_after = st.snapshot()
    assert snap_before["assets"] == 100.0
    assert snap_after["assets"] == 999.0, "absorbing IS how time advances"


def test_restatement_uses_latest_filed_so_far():
    rows = [("2020-02-01", "Assets", None, "2019-12-31", None, 100.0),
            ("2020-08-01", "Assets", None, "2019-12-31", None, 110.0)]
    assert _state(rows).snapshot()["assets"] == 110.0


def test_freshest_synonym_wins_not_the_first_in_the_ladder():
    """The abandoned-tag trap: a filer stops using ``Revenues`` after 2018."""
    rows = [
        ("2018-02-01", "Revenues", "2017-01-01", "2017-12-31", 364, 50.0),
        ("2019-02-01", "RevenueFromContractWithCustomerExcludingAssessedTax",
         "2018-01-01", "2018-12-31", 364, 90.0),
    ]
    v, _path, _pe, tag = _state(rows).ttm(FU.FLOW_CONCEPTS["revenue"])
    assert v == 90.0
    assert tag == "RevenueFromContractWithCustomerExcludingAssessedTax"


def test_ytd_diff_produces_a_trailing_twelve_months():
    """A + YTD_current - YTD_prior, with full-year durations excluded."""
    rows = [
        # FY2023 annual and its Q1 year-to-date
        ("2024-02-01", "Revenues", "2023-01-01", "2023-12-31", 364, 400.0),
        ("2023-05-01", "Revenues", "2023-01-01", "2023-03-31", 89, 90.0),
        # FY2024 Q1 year-to-date
        ("2024-05-01", "Revenues", "2024-01-01", "2024-03-31", 89, 110.0),
    ]
    v, path, _pe, _t = _state(rows).ttm(FU.FLOW_CONCEPTS["revenue"])
    assert path == "YTD_DIFF"
    assert v == pytest.approx(400.0 + 110.0 - 90.0)


def test_full_year_facts_never_telescope_the_ytd_extension():
    """Admitting a year-length fact as a YTD makes A_prior + FY - FY_prior == FY,
    which silently zeroes every year-on-year change."""
    rows = [
        ("2023-02-01", "Revenues", "2022-01-01", "2022-12-31", 364, 300.0),
        ("2024-02-01", "Revenues", "2023-01-01", "2023-12-31", 364, 400.0),
    ]
    st = _state(rows)
    v, path, _pe, _t = st.ttm(FU.FLOW_CONCEPTS["revenue"])
    assert (v, path) == (400.0, "ANNUAL")
    vp, _p, _pe2, _t2 = st.ttm_prior(FU.FLOW_CONCEPTS["revenue"], "Revenues")
    assert vp == 300.0, "the prior term must be the previous YEAR, not the same year"


def test_prior_anchor_is_a_year_back_not_a_quarter_back():
    """Many filers publish a genuine 'twelve months ended <quarter date>' fact."""
    rows = [
        ("2023-02-01", "Revenues", "2022-01-01", "2022-12-31", 364, 300.0),
        ("2024-02-01", "Revenues", "2023-01-01", "2023-12-31", 364, 400.0),
        # a trailing-twelve-months fact ending one QUARTER before the anchor
        ("2023-11-01", "Revenues", "2022-10-01", "2023-09-30", 364, 380.0),
    ]
    st = _state(rows)
    vp, _p, pe, _t = st.ttm_prior(FU.FLOW_CONCEPTS["revenue"], "Revenues")
    assert vp == 300.0 and pe == "2022-12-31"


def test_ytd_tolerance_admits_a_52_53_week_filer():
    """272 days one year, 279 the next: a 5-day tolerance refuses the match."""
    assert FU.YTD_LEN_TOL >= 7
    rows = [
        ("2024-02-01", "Revenues", "2023-01-01", "2023-12-31", 364, 400.0),
        ("2023-11-01", "Revenues", "2023-01-01", "2023-09-30", 272, 300.0),
        ("2024-11-01", "Revenues", "2024-01-01", "2024-09-28", 279, 330.0),
    ]
    v, path, _pe, _t = _state(rows).ttm(FU.FLOW_CONCEPTS["revenue"])
    assert path == "YTD_DIFF"
    assert v == pytest.approx(400.0 + 330.0 - 300.0)


def test_missing_fundamental_is_nan_not_zero():
    from alpha_agent.r58 import panel_f
    snap = FU.CompanyState().snapshot()
    vec = panel_f._derive(snap, "2020-01-02", {})
    assert vec[panel_f.F_IX["has_core"]] == 0.0
    import numpy as np
    assert np.isnan(vec[panel_f.F_IX["fcf_to_assets"]]), \
        "an unknown fundamental must leave the name OUT of the universe, " \
        "never rank it as if its cash flow were zero"


def test_cik_bridge_excludes_ambiguous_mappings():
    src = Path(FU.__file__).read_text(encoding="utf-8")
    assert "status='RESOLVED'" in src
    assert "AMBIGUOUS" in src, "the exclusion must be stated, not implicit"


# --------------------------------------------------------------------------- #
# Campaign artifacts
# --------------------------------------------------------------------------- #
def _artifact(name):
    a = r58.read_artifact(name)
    if a is None:
        pytest.skip("R58 research artifact %s not present" % name)
    return a


def test_lockbox_was_untouched_when_selection_was_persisted():
    sel = _artifact("r58_validation_selection.json")
    lb = _artifact("r58_lockbox_results.json")
    assert sel["layers_evaluated"] == ["D", "V"]
    assert sel["lockbox_untouched_at_selection_time"] is True
    t_sel = datetime.fromisoformat(sel["generated_at"])
    t_lb = datetime.fromisoformat(lb["generated_at"])
    assert t_sel < t_lb, "the selection artifact must PRE-DATE the lockbox one"
    assert lb["selection_artifact_generated_at"] == sel["generated_at"]


def test_every_family_selected_one_variant_on_validation_alone():
    sel = _artifact("r58_validation_selection.json")
    for fam, s in sel["selection"].items():
        assert s["selected_variant"] in sel["families"][fam]["variants"]
        assert "VALIDATION" in s["selection_rule"]


def test_lockbox_evaluated_once_per_family():
    lb = _artifact("r58_lockbox_results.json")
    sel = _artifact("r58_validation_selection.json")
    assert set(lb["families"]) == set(sel["selection"])
    for fam, row in lb["families"].items():
        assert row["selected_variant"] == sel["selection"][fam]["selected_variant"]


def test_bh_denominator_is_the_pre_registered_thirteen():
    v = _artifact("r58_campaign_verdicts.json")
    assert v["bh_denominator"] == 13
    assert v["bh_denominator_fixed_before_lockbox"] is True
    assert v["bh_q"] == r58.BH_Q


def test_effective_observation_floor_is_met_or_declared():
    v = _artifact("r58_campaign_verdicts.json")
    for fam, row in v["campaign_verdicts"].items():
        n = row.get("lockbox_periods") or 0
        if row.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE":
            assert n >= r58.OBS_FLOOR


def test_diagnostic_reference_never_receives_an_alpha_verdict():
    v = _artifact("r58_campaign_verdicts.json")
    b0 = v["campaign_verdicts"].get("B0")
    if b0 is None:
        pytest.skip("B0 not evaluated")
    assert b0["verdict"] == "DIAGNOSTIC_REFERENCE_NOT_AN_ALPHA_CLAIM"
    assert b0.get("bh_survived") is None


def test_coverage_blocked_families_get_no_alpha_verdict():
    v = _artifact("r58_campaign_verdicts.json")
    for fam, row in v["campaign_verdicts"].items():
        if row["verdict"] == "DATA_HOLD_COVERAGE":
            assert row["no_alpha_verdict_issued"] is True
            assert row["diagnostic_only"] is True


def test_within_coverage_diagnostics_are_labelled_as_such():
    labs = _artifact("r58_labs.json")
    for fam, row in labs["within_coverage_diagnostics"].items():
        assert row["status"].endswith("NOT_AN_ALPHA_VERDICT")
    assert labs["momentum_attribution"]["status"] == \
        "POST_HOC_DIAGNOSTIC_NOT_AN_ALPHA_CLAIM"


def test_calibration_not_attempted_without_a_qualified_signal():
    labs = _artifact("r58_labs.json")
    v = _artifact("r58_campaign_verdicts.json")
    if v["n_historical_alpha_candidates"] == 0:
        assert labs["calibration"]["status"] == \
            "CALIBRATION_NOT_ATTEMPTED_NO_QUALIFIED_SIGNAL"
        assert labs["calibration"]["expected_return_state"] == "NOT_CALIBRATED"


# --------------------------------------------------------------------------- #
# Forward challengers
# --------------------------------------------------------------------------- #
def test_frozen_challengers_have_zero_forward_observations_and_reproduce():
    body = _artifact("r58_forward_challengers.json")
    assert body["forward_observations_at_freeze"] == 0
    assert body["backfill"] == "FORBIDDEN"
    assert body["frozen"], "at least one challenger must be frozen"
    for name, meta in body["frozen"].items():
        rec = json.loads(Path(meta["path"]).read_text(encoding="utf-8"))
        assert rec["forward_observations_at_freeze"] == 0
        assert rec["status"] == "FORWARD_PENDING_ALPHA_CANDIDATE"
        assert rec["n_held"] > 0, "an empty book is not a challenger"
        assert abs(sum(rec["weights"].values()) - 1.0) < 1e-6
        assert r58.stable_hash(rec["spec"]) == rec["spec_hash"]
        assert r58.stable_hash(rec["weights"]) == rec["weights_hash"]
        for flag in ("creates_orders", "creates_fills", "promotes_model",
                     "activates_sleeve", "mutates_operational_store"):
            assert rec["safety"][flag] is False


def test_post_hoc_challenger_discloses_its_selection_bias():
    body = _artifact("r58_forward_challengers.json")
    veto = body["frozen"].get("R58_FUND_MOMENTUM_VETO_V1")
    if veto is None:
        pytest.skip("veto challenger not frozen")
    rec = json.loads(Path(veto["path"]).read_text(encoding="utf-8"))
    assert rec["spec"]["post_hoc_selection_disclosed"] is True
    assert "selection bias" in rec["spec"]["why_prospective"]


def test_refused_information_families_are_recorded_with_reasons():
    body = _artifact("r58_forward_challengers.json")
    refused = body["refused_information_families"]
    assert "NEWS_EVENT" in refused and "INSIDER_FILING" in refused
    for fam, row in refused.items():
        assert row["reason"] and row["detail"]
    assert refused["INSIDER_FILING"]["reason"] == "FIELD_UNPOPULATED"


# --------------------------------------------------------------------------- #
# Evidence handling
# --------------------------------------------------------------------------- #
def test_prior_release_evidence_is_unmodified():
    c = _artifact("r58_capital_and_gate.json")
    im = c["evidence_immutability"]
    assert im["verdict"] == "R56_AND_R57_EVIDENCE_UNMODIFIED"
    for k in ("r56_shadow_portfolios", "r57_alpha_discovery"):
        assert im[k]["files_modified_after_r58_started"] == []


def test_capital_frontier_only_admits_gate_passers():
    c = _artifact("r58_capital_and_gate.json")
    v = _artifact("r58_campaign_verdicts.json")
    cf = c["capital_frontier"]
    passers = [k for k, row in v["campaign_verdicts"].items()
               if row.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE"]
    assert cf["qualified_candidates"] == passers


def test_purchase_gate_answers_all_twelve_questions():
    c = _artifact("r58_capital_and_gate.json")
    for name, g in c["data_purchase_gate"].items():
        if name == "general":
            continue
        numbered = [k for k in g if k[0].isdigit()]
        assert len(numbered) == 12, "%s answered %d questions" % (name, len(numbered))
        assert g["verdict"].startswith(("DO_NOT_BUY", "NO_PURCHASE_REQUIRED",
                                        "BUY"))


def test_coverage_bias_is_published_whatever_it_says():
    d = _artifact("r58_diagnostics.json")
    cb = d["coverage_bias"]
    assert "interpretation_rule" in cb
    for lay in ("D", "V", "L"):
        assert lay in cb


def test_frozen_operational_panel_is_object_of_study_not_evidence():
    d = _artifact("r58_diagnostics.json")
    f = d["frozen_operational_panel_forensics"]
    if f.get("status") == "PANEL_NOT_FOUND":
        pytest.skip("frozen operational panel not reachable")
    assert f["pseudo_date_collapse"]["exact_duplicate_rows"] > 0
    assert "object of study" in f["role_in_r58"]
