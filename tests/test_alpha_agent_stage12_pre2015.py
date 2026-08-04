"""Deterministic targeted tests for the Stage 12 pre-2015 (2009-2014) independent-
time confirmation pass. Pure logic only (no Norgate / no heavy panel): classification,
panel epoch, temporal split, frozen 3-hypothesis registry, unchanged gates, decision
gate, multiple-testing family size, terminal logic, canonical queue helpers.
"""
import json

import pytest

from alpha_agent import stage12_pre2015 as P
from alpha_agent import stage12_registry as R
from alpha_agent import stage12_event_study as ES


# --------------------------------------------------------------------------- #
# WORKSTREAM A -- eligibility classification.
# --------------------------------------------------------------------------- #
def _rec(assetid, licensed, fq, lq, cik, resolved, membership=True):
    return {"assetid": assetid, "licensed": licensed, "first_quoted": fq,
            "last_quoted": lq, "cik": cik, "resolved": resolved,
            "membership": membership}


def test_price_status_materialized_available_nobar_nolicense():
    mat = {"10"}
    assert P.price_status_pre2015(assetid="10", licensed=True, first_quoted="2005-01-01",
                                  last_quoted="2020-01-01", materialized=mat) == P.PRE2015_ALREADY_MATERIALIZED
    assert P.price_status_pre2015(assetid="11", licensed=True, first_quoted="2005-01-01",
                                  last_quoted="2020-01-01", materialized=mat) == P.PRE2015_AVAILABLE_NOT_MATERIALIZED
    # delisted BEFORE the window -> no window bars (delisted preservation as NO_BAR)
    assert P.price_status_pre2015(assetid="12", licensed=True, first_quoted="1999-01-01",
                                  last_quoted="2008-06-01", materialized=mat) == P.PRE2015_NO_BAR_HISTORY
    # IPO'd after the window
    assert P.price_status_pre2015(assetid="13", licensed=True, first_quoted="2016-01-01",
                                  last_quoted="2020-01-01", materialized=mat) == P.PRE2015_NO_BAR_HISTORY
    assert P.price_status_pre2015(assetid=None, licensed=False, first_quoted=None,
                                  last_quoted=None, materialized=mat) == P.PRE2015_NO_LICENSED_HISTORY


def test_event_status_cik_resolution_and_companyfacts():
    cf = {"C1"}
    assert P.event_status_pre2015(cik="C1", resolved=True, cf_event_ciks=cf) == (P.PRE2015_CIK_RESOLVED, True)
    assert P.event_status_pre2015(cik="C2", resolved=True, cf_event_ciks=cf) == (P.PRE2015_NO_COMPANYFACTS_EVENTS, False)
    assert P.event_status_pre2015(cik=None, resolved=False, cf_event_ciks=cf) == (P.PRE2015_CIK_UNRESOLVED, False)


def test_classify_universe_counts_and_event_eligible_span():
    recs = [
        _rec("10", True, "2005-01-01", "2020-01-01", "C1", True),   # materialized + eligible + span
        _rec("11", True, "2005-01-01", "2020-01-01", "C2", True),   # available + no cf events
        _rec("12", True, "1999-01-01", "2008-06-01", "C3", True),   # delisted pre-window -> no bar
        _rec(None, False, None, None, None, False),                 # no license / unresolved
    ]
    agg = P.classify_pre2015_universe(recs, materialized={"10"}, cf_event_ciks={"C1"})
    c = agg["counts"]
    assert c[P.PRE2015_ALREADY_MATERIALIZED] == 1
    assert c[P.PRE2015_AVAILABLE_NOT_MATERIALIZED] == 1
    assert c[P.PRE2015_NO_BAR_HISTORY] == 1
    assert c[P.PRE2015_NO_LICENSED_HISTORY] == 1
    assert agg["materialized_pre2015"] == 1
    assert agg["available_not_materialized"] == 1
    # only C1 (materialized, cf-eligible, first_quoted<=2009) counts to the span
    assert agg["event_eligible_span_full_window"] == 1


# --------------------------------------------------------------------------- #
# WORKSTREAM C -- pre-2015 panel epoch.
# --------------------------------------------------------------------------- #
def _epoch(**kw):
    base = dict(date_min="2009-01-02", date_max="2014-12-31",
                materialized_assetids=["1", "2"], sources=["norgate_local"],
                cik_mapping_version="m1", norgate_universe_fingerprint="u1",
                event_source_fingerprint="e1", registry_version="r1",
                study_window=["2009-01-01", "2014-12-31"])
    base.update(kw)
    return P.compute_pre2015_panel_epoch(**base)


def test_panel_epoch_prefix_stable_and_order_independent():
    e = _epoch()
    assert e.startswith("p") and len(e) == 17
    assert _epoch(materialized_assetids=["2", "1"]) == e  # order-independent set hash


@pytest.mark.parametrize("field,val", [
    ("materialized_assetids", ["1", "2", "3"]),
    ("date_min", "2008-01-02"),
    ("date_max", "2015-01-01"),
    ("cik_mapping_version", "m2"),
    ("norgate_universe_fingerprint", "u2"),
    ("event_source_fingerprint", "e2"),
    ("registry_version", "r2"),
    ("study_window", ["2009-01-01", "2013-12-31"]),
])
def test_panel_epoch_changes_on_every_component(field, val):
    assert _epoch(**{field: val}) != _epoch()


# --------------------------------------------------------------------------- #
# WORKSTREAM E -- temporal split / final-holdout isolation / window isolation.
# --------------------------------------------------------------------------- #
def _p(m):
    return {"as_of": m, "names": []}


def test_temporal_split_dev_holdout_and_excludes_out_of_window():
    periods = [_p("2008-12"), _p("2009-01"), _p("2011-06"), _p("2012-12"),
               _p("2013-01"), _p("2014-12"), _p("2015-06")]
    dev, hold = P.split_dev_holdout(periods)
    assert [x["as_of"] for x in dev] == ["2009-01", "2011-06", "2012-12"]
    assert [x["as_of"] for x in hold] == ["2013-01", "2014-12"]
    # pre-2009 and post-2014 formation cohorts are excluded from BOTH
    allc = {x["as_of"] for x in dev} | {x["as_of"] for x in hold}
    assert "2008-12" not in allc and "2015-06" not in allc


def test_final_holdout_and_dev_are_disjoint():
    periods = [_p("2010-03"), _p("2012-12"), _p("2013-01"), _p("2014-06")]
    dev, hold = P.split_dev_holdout(periods)
    assert not ({x["as_of"] for x in dev} & {x["as_of"] for x in hold})
    assert all(x["as_of"] <= P.DEV_COHORT_MAX for x in dev)
    assert all(x["as_of"] >= P.HOLDOUT_COHORT_MIN for x in hold)


# --------------------------------------------------------------------------- #
# WORKSTREAM D -- frozen 3-hypothesis registry + unchanged gates.
# --------------------------------------------------------------------------- #
def test_frozen_registry_exactly_three_with_released_directions():
    reg = P.build_pre2015_registry()
    assert reg["n_hypotheses"] == 3
    assert list(reg["builder_keys"]) == list(P.PRE2015_BUILDER_KEYS)
    assert reg["selected_before_evidence"] is True
    released = {h["builder_key"]: h for h in R._hypotheses()}
    for h in reg["hypotheses"]:
        rel = released[h["builder_key"]]
        assert h["expected_direction"] == rel["expected_direction"]
        assert h["formula"] == rel["formula"]
        # UNCHANGED gates -- pulled verbatim from the released registry
        assert list(h["disqualifying_gates"]) == list(R._DEFAULT_GATES)


def test_registry_version_deterministic_and_content_addressed():
    assert P.build_pre2015_registry()["registry_version"] == P.build_pre2015_registry()["registry_version"]


def test_multiple_testing_family_size_exactly_three():
    fams = [{"hypothesis_id": "a", "dev": {"rank_ic_t": 3.0, "periods": 30}},
            {"hypothesis_id": "b", "dev": {"rank_ic_t": 0.5, "periods": 30}},
            {"hypothesis_id": "c", "dev": {"rank_ic_t": 0.2, "periods": 30}}]
    mt = P._mt_three(fams, len(P.PRE2015_BUILDER_KEYS))
    assert len(mt["rows"]) == 3
    # BH q-value uses m = 3 exactly
    assert len(P.PRE2015_BUILDER_KEYS) == 3


def test_freeze_registry_immutable(tmp_path):
    path = tmp_path / "reg.json"
    reg = P.freeze_pre2015_registry(path)
    assert path.exists()
    # idempotent re-freeze of identical content
    assert P.freeze_pre2015_registry(path)["registry_version"] == reg["registry_version"]
    # different content -> raises
    path.write_text(json.dumps({"registry_version": "DIFFERENT"}), encoding="utf-8")
    with pytest.raises(R.RegistryImmutabilityError):
        P.freeze_pre2015_registry(path)


# --------------------------------------------------------------------------- #
# WORKSTREAM G -- decision gate + qualification rules.
# --------------------------------------------------------------------------- #
def _qualifying_family():
    return {"dev": {"rank_ic_t": 3.0, "overlap_clustered_t": 2.5, "spread_t": 2.4,
                    "net25": 0.1, "turnover": 1.0, "target_state": "KEEP_FOR_RESEARCH",
                    "direction_matches": True, "fdr_survived": True},
            "holdout": {"direction_confirms": True},
            "concentration": {"concentrated": False}}


def test_decision_gate_qualifies_only_when_all_conditions_hold():
    assert P.pre2015_decision_gate(_qualifying_family())["qualifies"] is True


def test_decision_gate_negative_direction_fails_standalone():
    f = _qualifying_family()
    f["dev"]["direction_matches"] = False
    f["dev"]["rank_ic_t"] = -3.0
    g = P.pre2015_decision_gate(f)
    assert g["qualifies"] is False
    assert g["weakest_gate"] == "standalone_direction_matches"


def test_decision_gate_holdout_must_confirm():
    f = _qualifying_family()
    f["holdout"]["direction_confirms"] = False
    assert P.pre2015_decision_gate(f)["qualifies"] is False


def test_decision_gate_combined_sample_cannot_qualify_alone():
    # A family with a strong COMBINED story but weak standalone dev never qualifies:
    # the gate reads only the dev (standalone 2009-2014) metrics + holdout confirm.
    f = _qualifying_family()
    f["dev"]["rank_ic_t"] = 1.0      # weak standalone
    f["dev"]["direction_matches"] = True
    assert P.pre2015_decision_gate(f)["qualifies"] is False


def test_decision_gate_negative_spread_or_cost_fails():
    f = _qualifying_family()
    f["dev"]["spread_t"] = -0.5
    f["dev"]["net25"] = -0.1
    g = P.pre2015_decision_gate(f)
    assert g["qualifies"] is False


def test_decision_gate_concentration_blocks():
    f = _qualifying_family()
    f["concentration"]["concentrated"] = True
    assert P.pre2015_decision_gate(f)["qualifies"] is False


# --------------------------------------------------------------------------- #
# Terminal logic + no-auto-promotion + queue helpers.
# --------------------------------------------------------------------------- #
def _adequate_family(qual=False):
    return {"hypothesis_id": "h", "qualifies": qual,
            "study_coverage": {"distinct_issuers": ES.MIN_ISSUERS + 100,
                               "n_cohorts": ES.MIN_COHORTS + 10},
            "dev": {"data_hold_reason": None}}


def test_terminal_no_defensible_when_adequate_and_rejected():
    study = {"families": [_adequate_family()], "qualified_count": 0}
    assert P.pre2015_terminal({"available_not_materialized": 1}, study) == "STAGE12_PRE2015_NO_DEFENSIBLE_ALPHA"


def test_terminal_qualified_when_candidate_passes():
    study = {"families": [_adequate_family(qual=True)], "qualified_count": 1}
    assert P.pre2015_terminal({}, study) == "STAGE12_PRE2015_SHADOW_CANDIDATE_QUALIFIED"


def test_terminal_resumable_when_no_study():
    assert P.pre2015_terminal({}, None) == "STAGE12_PRE2015_CAMPAIGN_RESUMABLE"
    assert P.pre2015_terminal({}, {"families": []}) == "STAGE12_PRE2015_CAMPAIGN_RESUMABLE"


def test_terminal_data_insufficient_when_underpowered():
    under = {"hypothesis_id": "h", "study_coverage": {"distinct_issuers": 5, "n_cohorts": 3},
             "dev": {"data_hold_reason": "DATA_HOLD_INSUFFICIENT_DEV_COHORTS(3<12)"}}
    study = {"families": [under], "qualified_count": 0}
    assert P.pre2015_terminal({}, study) == "STAGE12_PRE2015_DATA_INSUFFICIENT"


def test_no_automatic_promotion_everywhere():
    assert P.pre2015_decision_gate(_qualifying_family())["no_automatic_promotion"] is True
    assert P._pre2015_shadow_decision([])["no_automatic_promotion"] is True
    assert P._pre2015_shadow_decision([])["status"] == "NO_DEFENSIBLE_ALPHA"
    for badge in ("SHADOW ONLY", "NO LIVE BROKER ORDERS", "AUTOMATION OFF",
                  "MANUAL REVIEW", "NO AUTO-PROMOTION"):
        assert badge in P.SAFETY_BADGES


def test_lane_helpers_next_incomplete_and_prefix():
    epoch = "pre2015:abc:def"
    assert P.next_incomplete_lane({}, epoch) == P.LANE_INVENTORY
    assert P.next_incomplete_lane({P.LANE_INVENTORY: epoch}, epoch) == P.LANE_MATERIALIZE
    assert P.next_incomplete_lane({P.LANE_INVENTORY: epoch, P.LANE_MATERIALIZE: epoch}, epoch) is None
    assert all(ln.startswith(P.PRE2015_LANE_PREFIX) for ln in P.LANE_ORDER)


def test_shadow_qualified_requires_manual_review():
    sh = P._pre2015_shadow_decision(["x"])
    assert sh["status"] == "SHADOW_ELIGIBLE_PENDING_MANUAL_REVIEW"
    assert sh["active_strategy"] is None
    assert sh["no_automatic_promotion"] is True
