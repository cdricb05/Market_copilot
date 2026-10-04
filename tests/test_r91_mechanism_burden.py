"""R91 - the HIERARCHICAL research burden (alpha_agent.agents_v2.mechanism_burden).

Tests A-I of the R91 contract:
  A  two different physical mechanisms under one economic family are NOT
     blocked by each other's campaign-local mechanism cap;
  B  a sign flip is the same burden unit;
  C  a threshold change is the same burden unit;
  D  a horizon change is the same burden unit; a genuinely different economic
     object is a new one;
  E  a cosmetic rename does not reset the burden (R91 rows AND legacy rows);
  F  prior settled experiments remain settled;
  G  the global FDR denominator still sees every experiment in every family;
  H  a new campaign does not reset historical mechanism burden;
  I  the R89/R90 failures remain recorded and cannot be re-tested.
"""
from __future__ import annotations

import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import mechanism_burden as MB
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.r59 import engines as E
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.api import prospective_adoption as PA

FUT_COST = {"rate_per_side": 0.0002, "basis": "traded notional"}
CAMPAIGN = "R91_TEST_CAMPAIGN"
GDELT_OBJ = {"source": "gdelt_2_event_and_gkg_country_theme_tone_v1",
             "variable": "commodity theme supply-disruption co-occurrence share",
             "mapping": "24 commodity legs by frozen theme set"}
USDM_OBJ = {"source": "usdm_state_weekly_area_percent_v1",
            "variable": "production-weighted crop area in D1+ drought, 4-week change",
            "mapping": "10 US crop legs by NASS 2010 state production shares"}
ERS_OBJ = {"source": "usda_ers_commodity_costs_and_returns",
           "variable": "log(full economic cost of production per unit / front price)",
           "mapping": "8 crop and livestock legs"}


@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    monkeypatch.setenv(PA.ADOPTION_DIR_ENV, str(tmp_path / "adoption"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _foundation(pipe, dataset="r91_test_commodity_panel",
                universe="r91_test_commodity_universe",
                features="r91_test_features") -> str:
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id=dataset, asset_classes=[r59.AC_COMMODITY],
        pit_status=P.PIT_SAFE, availability_rule="stamped at release",
        survivorship="expired contracts retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id=universe, dataset_id=dataset, asset_class=r59.AC_COMMODITY,
        execution_representation="LONG_SHORT", short_leg_expressible=True,
        rules="certified legs"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id=features, universe_id=universe,
        features=[{"name": "f1", "lag": 1, "source": dataset}],
        leakage_check="PASS"))
    return features


def _payload(features, *, family="FUNDAMENTAL_MOMENTUM", owner=A.MOMENTUM,
             info="GDELT_SUPPLY_DISRUPTION_COOCCURRENCE",
             mech="NEWS_INFORMATION_DIFFUSION", obj=GDELT_OBJ, sign=1,
             horizon=5, params=None, cap=2, campaign=CAMPAIGN,
             hypothesis="disruption attention predicts 5-session returns") -> dict:
    body = dict(
        owning_agent=owner, hypothesis=hypothesis, asset_class=r59.AC_COMMODITY,
        family=family, feature_set_id=features, horizon_sessions=horizon,
        parameters=params or {"z_window": 52, "threshold": 0.0},
        long_short=True,
        discovery_sample={"start": r59.DISCOVERY_START, "end": r59.VALIDATION_START},
        evaluation_sample={"validation": r59.VALIDATION_START,
                           "lockbox": r59.LOCKBOX_START},
        cost_model=FUT_COST, expected_sign=sign, information_family=info,
        instrument_scope=["&ZC"], venue="RESEARCH",
        mechanism_family=mech, economic_object=obj)
    if campaign:
        body["campaign_id"] = campaign
    if cap is not None:
        body["mechanism_family_cap"] = cap
    return body


def _settle_negative(pipe, reg: dict) -> None:
    pipe.mem.record_result(reg["experiment_id"], outcome=r59.HO_NO_ALPHA_EVIDENCE,
                           reason_rejected="test", reopen_condition="owner defect only")


def _refused(code: str):
    return pytest.raises(P.PipelineRefusal, match=code)


# --------------------------------------------------------------------------- #
def test_00_vocabulary_is_frozen_and_the_object_identity_ignores_expression():
    assert "WEATHER_PRODUCTION" in MB.MECHANISM_FAMILIES
    assert "NEWS_INFORMATION_DIFFUSION" in MB.MECHANISM_FAMILIES
    assert len(set(MB.MECHANISM_FAMILIES)) == len(MB.MECHANISM_FAMILIES)
    a = MB.economic_object_id(GDELT_OBJ)
    b = MB.economic_object_id({k: "  %s  " % v.upper() for k, v in GDELT_OBJ.items()})
    assert a == b, "whitespace and case are not identity"
    with pytest.raises(MB.MechanismBurdenRefusal):
        MB.canonical_economic_object({"source": "x", "variable": "y"})
    with pytest.raises(MB.MechanismBurdenRefusal):
        MB.burden_unit(asset_class=r59.AC_COMMODITY, mechanism_family="MADE_UP",
                       economic_object=GDELT_OBJ)
    for k in ("expected_sign", "horizon_sessions", "threshold", "z_window",
              "universe_subset", "theme_subset", "ranking_method",
              "residualisation", "cadence_sessions", "information_family"):
        assert k in MB.EXPRESSION_PARAMETER_KEYS


def test_a_two_physical_mechanisms_are_not_blocked_by_each_others_cap(pipe):
    f = _foundation(pipe)
    pipe.perform(A.DIRECTOR, "preregister",
                 _payload(f, info="GDELT_A", obj=GDELT_OBJ, hypothesis="a"))
    pipe.perform(A.DIRECTOR, "preregister", _payload(
        f, info="GDELT_B", hypothesis="b",
        obj={**GDELT_OBJ, "variable": "producer-country material conflict intensity"}))
    # The NEWS cap (2) is full: a THIRD news object is refused ...
    with _refused("MECHANISM_FAMILY_CAP_REACHED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, info="GDELT_C", hypothesis="c",
            obj={**GDELT_OBJ, "variable": "importer-country tone"}))
    # ... but a WEATHER object under the SAME economic family is not.
    r3 = pipe.perform(A.DIRECTOR, "preregister", _payload(
        f, info="USDM_DROUGHT", mech="WEATHER_PRODUCTION", obj=USDM_OBJ,
        hypothesis="drought"))
    assert r3["state"] == "PREREGISTERED"
    row = pipe.mem.get(r3["experiment_id"])
    assert row["economic_family"] == "FUNDAMENTAL_MOMENTUM"      # routing unchanged
    assert row["spec"]["mechanism_family"] == "WEATHER_PRODUCTION"
    assert row["spec"]["campaign_id"] == CAMPAIGN
    assert row["spec"]["burden_unit"].startswith(
        "%s|WEATHER_PRODUCTION|" % r59.AC_COMMODITY)
    chk = MB.cap_check(pipe.mem, campaign_id=CAMPAIGN, asset_class=r59.AC_COMMODITY,
                       mechanism_family="NEWS_INFORMATION_DIFFUSION", cap=2)
    assert chk["used"] == 2 and not chk["allowed"]
    chk_w = MB.cap_check(pipe.mem, campaign_id=CAMPAIGN, asset_class=r59.AC_COMMODITY,
                         mechanism_family="WEATHER_PRODUCTION", cap=2)
    assert chk_w["used"] == 1 and chk_w["allowed"]
    # Re-registering the SAME experiment (a resume) is idempotent, not a slot.
    again = pipe.perform(A.DIRECTOR, "preregister", _payload(
        f, info="USDM_DROUGHT", mech="WEATHER_PRODUCTION", obj=USDM_OBJ,
        hypothesis="drought"))
    assert again["experiment_id"] == r3["experiment_id"] and again["already_registered"]


def test_b_a_sign_flip_is_the_same_burden_unit(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, sign=1, cap=None))
    _settle_negative(pipe, r1)
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister",
                     _payload(f, sign=-1, cap=None, hypothesis="the flip"))
    chk = MB.re_expression_check(
        pipe.mem, asset_class=r59.AC_COMMODITY,
        mechanism_family="NEWS_INFORMATION_DIFFUSION", economic_object=GDELT_OBJ)
    assert chk["verdict"] == MB.UNIT_SETTLED
    assert chk["settled_negative"][0]["hypothesis_id"] == r1["experiment_id"]


def test_c_a_threshold_change_is_the_same_burden_unit(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None))
    _settle_negative(pipe, r1)
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, cap=None, params={"z_window": 26, "threshold": 0.5},
            hypothesis="tighter threshold"))


def test_d_a_horizon_change_is_the_same_unit_but_a_new_object_is_not(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None, horizon=5))
    _settle_negative(pipe, r1)
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister",
                     _payload(f, cap=None, horizon=21, hypothesis="monthly"))
    # The economic mechanism genuinely changes -> a new unit, allowed.
    other = {**GDELT_OBJ, "variable": "producer-country material conflict intensity"}
    chk = MB.re_expression_check(
        pipe.mem, asset_class=r59.AC_COMMODITY,
        mechanism_family="NEWS_INFORMATION_DIFFUSION", economic_object=other,
        information_family="GDELT_PRODUCER_CONFLICT")
    assert chk["verdict"] == MB.UNIT_NEW
    r2 = pipe.perform(A.DIRECTOR, "preregister", _payload(
        f, cap=None, info="GDELT_PRODUCER_CONFLICT", obj=other, hypothesis="conflict"))
    assert r2["state"] == "PREREGISTERED" and r2["experiment_id"] != r1["experiment_id"]


def test_e_a_cosmetic_rename_does_not_reset_the_burden(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None))
    _settle_negative(pipe, r1)
    # R91 row: a new information_family LABEL over the same declared object.
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, cap=None, info="GDELT_SUPPLY_DISRUPTION_COOCCURRENCE_V2",
            hypothesis="renamed"))
    # LEGACY row (registered before R91, no mechanism fields): its name binds.
    legacy = pipe.mem.register(
        title="legacy tone", release="R89", origin="test",
        generation_method="AGENTS_V2", information_family="LEGACY_TONE_DIFFUSION",
        economic_family="FUNDAMENTAL_MOMENTUM", asset_class=r59.AC_COMMODITY,
        model_family="XS_LONG_SHORT", horizon_sessions=5,
        input_data_identity="fs", spec={"lookback": 21})
    pipe.mem.record_result(legacy, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                           reason_rejected="legacy", reopen_condition="none")
    hits = MB.same_unit_rows(
        pipe.mem, asset_class=r59.AC_COMMODITY,
        mechanism_family="NEWS_INFORMATION_DIFFUSION",
        economic_object={"source": "gdelt", "variable": "anything", "mapping": "any"},
        information_family="legacy tone diffusion")
    assert [h["hypothesis_id"] for h in hits] == [legacy]
    assert hits[0]["legacy_row"] and hits[0]["matched_by"].startswith("LEGACY")
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, cap=None, info="LEGACY_TONE_DIFFUSION",
            obj={"source": "gdelt", "variable": "fresh label", "mapping": "any"},
            hypothesis="relabelled legacy"))
    # Declaring the settled NAME as the economic variable is caught too.
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, cap=None, info="BRAND_NEW_LABEL",
            obj={"source": "gdelt", "variable": "LEGACY_TONE_DIFFUSION", "mapping": "any"},
            hypothesis="variable is the old name"))


def test_f_prior_settled_experiments_remain_settled(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None))
    _settle_negative(pipe, r1)
    for _ in range(3):
        with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
            pipe.perform(A.DIRECTOR, "preregister",
                         _payload(f, cap=None, sign=-1, hypothesis="again"))
    row = pipe.mem.get(r1["experiment_id"])
    assert row["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
    assert row["reopen_condition"] == "owner defect only"
    assert pipe.mem.is_novel(family=row["family_key"], spec=row["spec"])["novel"] is False
    assert pipe.mem.burden()["total"] == 1


def test_g_the_global_fdr_denominator_still_sees_every_family(pipe):
    f = _foundation(pipe)
    regs = [
        pipe.perform(A.DIRECTOR, "preregister", _payload(f, hypothesis="news")),
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, info="USDM", mech="WEATHER_PRODUCTION", obj=USDM_OBJ, hypothesis="drought")),
        pipe.perform(A.DIRECTOR, "preregister", _payload(
            f, family="VALUATION", owner=A.REVERSAL, info="ERS_COST",
            mech="PRODUCTION_COST_VALUE", obj=ERS_OBJ, hypothesis="cost value")),
    ]
    assert pipe.mem.burden()["total"] == 0          # unsettled rows do not count yet
    for r in regs:
        _settle_negative(pipe, r)
    b = pipe.mem.burden()
    assert b["total"] == 3 and b["distinct_families"] == 3
    assert MB.global_burden(pipe.mem) == b
    layers = {"V": {"ann_net_excess": 0.02, "t_net": 2.0, "periods": 60},
              "L": {"ann_net_excess": 0.03, "t_net": 2.5, "p_one_sided": 0.01,
                    "periods": 60}}
    g = E.gate({"layers": layers}, prior_burden=b["total"], family_tests=1)
    assert g["burden_denominator"] == 4            # every family, one denominator
    assert g["burden_corrected_p"] == pytest.approx(0.04)
    # The cap is per mechanism family and campaign-local; the denominator is not.
    for mech in ("NEWS_INFORMATION_DIFFUSION", "WEATHER_PRODUCTION",
                 "PRODUCTION_COST_VALUE"):
        c = MB.cap_check(pipe.mem, campaign_id=CAMPAIGN, asset_class=r59.AC_COMMODITY,
                         mechanism_family=mech, cap=2)
        assert c["used"] == 1 and c["allowed"]
        assert c["global_burden_owner_unchanged"] == MB.GLOBAL_BURDEN_OWNER


def test_h_a_new_campaign_does_not_reset_historical_mechanism_burden(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f))
    _settle_negative(pipe, r1)
    nxt = MB.campaign_local_count(pipe.mem, campaign_id="R92_TEST",
                                  asset_class=r59.AC_COMMODITY,
                                  mechanism_family="NEWS_INFORMATION_DIFFUSION")
    assert nxt["used"] == 0, "the CAP is campaign-local ..."
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister",
                     _payload(f, campaign="R92_TEST", hypothesis="next campaign"))
    assert pipe.mem.burden()["total"] == 1, "... the BURDEN is not"


def test_h2_an_unknown_family_or_incomplete_object_is_refused(pipe):
    f = _foundation(pipe)
    with _refused("MECHANISM_FAMILY_UNKNOWN"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(f, mech="PHYSICAL_STUFF"))
    with _refused("ECONOMIC_OBJECT_INCOMPLETE"):
        pipe.perform(A.DIRECTOR, "preregister",
                     _payload(f, obj={"source": "gdelt", "variable": ""}))
    with _refused("CAMPAIGN_ID_REQUIRED_FOR_CAP"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(f, campaign=None, cap=2))
    # A pre-R91 payload (no mechanism fields) still registers exactly as before.
    body = _payload(f, cap=None, campaign=None)
    body.pop("mechanism_family")
    body.pop("economic_object")
    r = pipe.perform(A.DIRECTOR, "preregister", body)
    assert r["state"] == "PREREGISTERED"
    assert "mechanism_family" not in pipe.mem.get(r["experiment_id"])["spec"]


def test_j_a_settled_unit_reopens_only_on_an_owner_certified_defect(pipe):
    f = _foundation(pipe)
    r1 = pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None))
    _settle_negative(pipe, r1)
    # no record, incomplete record, wrong kind, wrong id: all refused
    with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
        pipe.perform(A.DIRECTOR, "preregister", _payload(f, cap=None, hypothesis="again"))
    for bad in ({"hypothesis_ids": [r1["experiment_id"]], "defect": "DATA_DEFECT"},
                {"hypothesis_ids": [r1["experiment_id"]], "defect": "BAD_LUCK",
                 "certified_by": "data-foundation-agent", "evidence": "x.json"},
                {"hypothesis_ids": ["H_other"], "defect": "DATA_DEFECT",
                 "certified_by": "data-foundation-agent", "evidence": "x.json"}):
        body = _payload(f, cap=None, hypothesis="again")
        body["reopen_defect"] = bad
        with _refused("SAME_BURDEN_UNIT_ALREADY_SETTLED"):
            pipe.perform(A.DIRECTOR, "preregister", body)
    body = _payload(f, cap=None, hypothesis="re-measured on the corrected calendar")
    body["reopen_defect"] = {"hypothesis_ids": [r1["experiment_id"]], "defect": "DATA_DEFECT",
                             "certified_by": "data-foundation-agent",
                             "evidence": "R91_SKEPTIC_VERDICTS.json D1: the event rows were minutes dates"}
    r2 = pipe.perform(A.DIRECTOR, "preregister", body)
    assert r2["state"] == "PREREGISTERED" and r2["experiment_id"] != r1["experiment_id"]
    spec = pipe.mem.get(r2["experiment_id"])["spec"]
    assert spec["reopens"] == [r1["experiment_id"]]
    assert spec["burden_unit"] == pipe.mem.get(r1["experiment_id"])["spec"]["burden_unit"]
    # the defective row stays settled and stays in the burden
    assert pipe.mem.get(r1["experiment_id"])["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
    assert pipe.mem.burden()["total"] == 1


R89_R90_SETTLED = {
    "H_0ec37048_ee99657f4aec": "R89 P1 GDELT cross-asset tone diffusion",
    "H_7feb019a_ba3f760a2186": "R89 P2 macro vintage momentum",
    "H_e028271c_5212bcf7a7b1": "R89 SW2_01 GDELT no-news reversal",
    "H_fa98fcd6_15448a192318": "R90 Lane A GDELT supply-disruption co-occurrence",
    "H_b515e588_e0788458a9d2": "R90 Lane B GDELT producer-country conflict",
}


def test_i_the_r89_r90_failures_remain_recorded_and_cannot_be_retested():
    if not M.memory_present():
        pytest.skip("the live research memory is not present on this machine")
    mem = M.open_memory_readonly()
    for hid, what in R89_R90_SETTLED.items():
        row = mem.get(hid)
        assert row is not None, what
        assert row["outcome"] == r59.HO_NO_ALPHA_EVIDENCE, what
        assert mem.is_novel(family=row["family_key"], spec=row["spec"])["novel"] is False
    lane_a = mem.get("H_fa98fcd6_15448a192318")
    chk = MB.re_expression_check(
        mem, asset_class=lane_a["asset_class"],
        mechanism_family="NEWS_INFORMATION_DIFFUSION",
        economic_object={"source": "gdelt", "variable": lane_a["information_family"],
                         "mapping": "any"},
        information_family=lane_a["information_family"],
        economic_family=lane_a["economic_family"], model_family=lane_a["model_family"])
    assert chk["verdict"] == MB.UNIT_SETTLED
    assert "H_fa98fcd6_15448a192318" in [h["hypothesis_id"] for h in chk["settled_negative"]]
