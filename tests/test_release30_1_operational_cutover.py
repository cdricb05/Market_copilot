"""Release 30.1 - the operational forecast lane, its model-identity contract and
its current-session freshness contract.

The defect this suite exists to prevent is the one Release 30 shipped: a frozen
artifact carrying the CURRENT APPROVED model's name, a NEGATIVE calibration
slope, and therefore a "target portfolio" that is the approved model turned
upside down. Every test below is about keeping an approved ranking, an owned
input date, or a refusal, exactly where it belongs.
"""
from __future__ import annotations

import ast
import json
import math
import re
from pathlib import Path

import pytest

from paper_trader.api import external_references as xr
from paper_trader.api import material_information as mi
from paper_trader.api import return_forecast as api_rf
from paper_trader.api import universe_scoring as us
from paper_trader.api import zero_base_target as zbt
from paper_trader.engine import return_forecast as fk
from paper_trader.engine import zero_base_allocator as zk

REPO = Path(__file__).resolve().parents[1]
FEATURE = api_rf.OPERATIONAL_FEATURE


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #
def _operational_artifact(*, slopes, state="CALIBRATED"):
    """A minimal approved-model adapter: one feature, rank blend, weight 1.0."""
    return {
        "contract": "release30_forecast_model/1",
        "universe_tag": "operational_v2",
        "activation": "CURRENT_OPERATIONAL_MODEL",
        "model_identity_contract": "APPROVED_MODEL_RANKING_IS_PRESERVED",
        "feature_names": [FEATURE],
        "feature_transform": fk.FEATURE_TRANSFORM,
        "target": fk.TARGET_QUANTITY,
        "automatic_promotion_allowed": False,
        "model_spec_hash": "test_operational_hash",
        "horizons": {
            str(h): {
                "horizon_sessions": h,
                "model": {"kind": "rank_blend", "weights": {FEATURE: 1.0}},
                "member_ids": [FEATURE],
                "weights": {FEATURE: 1.0},
                "weighting_method": "FROZEN_OPERATIONAL_CHAMPION_NO_FITTING",
                "calibration": ({"state": state, "slope": s,
                                 "residual_sigma": 0.05, "n_rows": 1000,
                                 "basis": "WALK_FORWARD_VALIDATION_BLOCKS"}
                                if s is not None else
                                {"state": "NOT_CALIBRATED", "slope": None,
                                 "reasons": ["SLOPE_NOT_DISTINGUISHABLE_FROM_ZERO"],
                                 "basis": "WALK_FORWARD_VALIDATION_BLOCKS"}),
                "training_cutoff": "2026-05-29",
            } for h, s in slopes.items()
        },
    }


def _cross_section(scores, as_of="2026-08-18"):
    return {
        "input_schema_version": fk.INPUT_SCHEMA_VERSION,
        "as_of_date": as_of,
        "requested_eligible_market_date": as_of,
        "feature_names": [FEATURE],
        "rows": [{"ticker": t, "features": {FEATURE: v},
                  "adv_dollar": 5.0e8, "sector": "Information Technology"}
                 for t, v in sorted(scores.items())],
        "point_in_time_status": fk.PIT_OK,
        "point_in_time_controls": ["test"],
    }


_SCORES = {"AAA": 0.95, "BBB": 0.80, "CCC": 0.60, "DDD": 0.40,
           "EEE": 0.20, "FFF": 0.05}


# --------------------------------------------------------------------------- #
# A. CURRENT MODEL IDENTITY
# --------------------------------------------------------------------------- #
def test_01_the_declared_model_id_is_the_live_owners_model_id():
    from paper_trader.alpha_agent import release30_1_operational_calibration as oc
    assert oc.OPERATIONAL_MODEL_ID == us.PRIMARY_MODEL_ID
    assert oc.OPERATIONAL_MODEL_ID == us.STRATEGY_ID


def test_02_the_calibration_admits_only_the_approved_models_own_components():
    from paper_trader.alpha_agent import release30_1_operational_calibration as oc
    assert set(oc.OPERATIONAL_COMPONENTS) == {"composite_sn", "mom_6_1"}
    assert oc.OPERATIONAL_WEIGHTS == {"composite_sn": 0.5, "mom_6_1": 0.5}
    # No adaptive-candidate component and no new predictor family may appear in
    # the operational calibration's CODE - not even as a commented-out
    # convenience. The module docstring is exempt: it must be free to name the
    # families it excludes and say why.
    import ast
    path = REPO / "alpha_agent" / "release30_1_operational_calibration.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tree.body = [n for n in tree.body
                 if not (isinstance(n, ast.Expr)
                         and isinstance(n.value, ast.Constant)
                         and isinstance(n.value.value, str))]
    code = ast.unparse(tree)
    for forbidden in ("s25_operating_profitability", "fcf_to_assets",
                      "operating_accruals", "gbrt", "extra_trees", "ridge"):
        assert forbidden not in code, forbidden


def test_03_the_percentile_and_normalisation_transforms_match_the_live_owners():
    from paper_trader.alpha_agent import release30_1_operational_calibration as oc
    from paper_trader.api import multi_horizon_engine as eng
    vals = {"A": 3.0, "B": 1.0, "C": 2.0, "D": 2.0}
    assert oc.percentiles(vals) == eng._percentiles(vals)[0]
    raw = [0.9, 0.1, 0.5, None, 0.5]
    assert oc.rank_normalise(raw) == fk.rank_normalise(raw)
    assert oc.standardise([1.0, 2.0, 3.0]) == fk.standardise([1.0, 2.0, 3.0])


# --------------------------------------------------------------------------- #
# B. RANK IDENTITY - the contract Release 30 lacked
# --------------------------------------------------------------------------- #
def test_10_a_positive_slope_preserves_the_approved_ranking():
    art = _operational_artifact(slopes={20: 0.002})
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20,))
    assert out["state"] == fk.STATE_READY
    assert out["represents_approved_model"] is True
    blk = out["by_horizon"]["20"]
    assert blk["disposition"] == fk.HORIZON_APPLIED
    assert blk["rank_identity"]["verdict"] == fk.RANK_IDENTITY_PRESERVED
    forecast_order = [r["ticker"] for r in blk["forecasts"]]
    approved_order = [t for t, _ in sorted(_SCORES.items(),
                                           key=lambda kv: -kv[1])]
    assert forecast_order == approved_order


def test_11_a_negative_slope_is_refused_not_applied():
    """The Release-30 defect, reproduced and now blocked."""
    art = _operational_artifact(slopes={20: -0.000848})
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20,))
    blk = out["by_horizon"]["20"]
    assert blk["disposition"] == fk.HORIZON_SUPPRESSED
    assert blk["rank_identity"]["verdict"] == fk.RANK_IDENTITY_VIOLATED
    assert fk.SUPPRESSED_RANK_IDENTITY in blk["rank_identity"]["reasons"]
    assert blk["forecasts"] == []
    assert out["state"] == fk.STATE_BLOCKED
    assert out["horizons"] == []
    assert out["suppressed_horizons"] == [20]
    # and nothing downstream can allocate against it
    assert fk.expected_returns(out, 20) == {}
    assert fk.uncertainties(out, 20) == {}
    assert fk.downside(out, 20) == {}


def test_12_a_zero_slope_is_refused_too():
    art = _operational_artifact(slopes={20: 0.0})
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20,))
    assert out["by_horizon"]["20"]["disposition"] == fk.HORIZON_SUPPRESSED
    assert fk.expected_returns(out, 20) == {}


def test_13_one_bad_horizon_does_not_suppress_a_good_sibling():
    art = _operational_artifact(slopes={5: -0.001, 20: 0.002})
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(5, 20))
    assert out["horizons"] == [20]
    assert out["suppressed_horizons"] == [5]
    assert out["state"] == fk.STATE_DEGRADED
    assert len(fk.expected_returns(out, 20)) == len(_SCORES)
    assert fk.expected_returns(out, 5) == {}


def test_14_an_explicitly_uncalibrated_horizon_supplies_no_expected_return():
    """Declared alone, an uncalibrated horizon leaves the artifact with nothing
    to apply, so the whole forecast is BLOCKED and names its reason."""
    art = _operational_artifact(slopes={20: None})
    chk = fk.validate_artifact(art)
    assert not chk["ok"]
    assert chk["declared_uncalibrated_horizons"] == [20]
    assert any(r["code"] == "NO_CALIBRATED_HORIZON" for r in chk["reasons"])
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20,))
    assert out["state"] == fk.STATE_BLOCKED
    assert fk.expected_returns(out, 20) == {}


def test_14b_an_uncalibrated_horizon_beside_a_calibrated_one_is_suppressed():
    art = _operational_artifact(slopes={20: None, 60: 0.002})
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20, 60))
    blk = out["by_horizon"]["20"]
    assert blk["disposition"] == fk.HORIZON_SUPPRESSED
    assert fk.SUPPRESSED_NOT_CALIBRATED in blk["rank_identity"]["reasons"]
    assert fk.expected_returns(out, 20) == {}
    assert len(fk.expected_returns(out, 60)) == len(_SCORES)


def test_15_the_contract_does_not_bind_a_research_candidate():
    """A candidate is allowed to disagree with the incumbent in either
    direction - it is not claiming to BE the incumbent."""
    art = _operational_artifact(slopes={20: -0.002})
    art.pop("activation")
    art["horizons"]["20"]["weighting_method"] = "VALIDATION_SHRUNK_IC"
    out = fk.build_forecast(cross_section=_cross_section(_SCORES), artifact=art,
                            horizons=(20,))
    assert fk.represents_approved_model(art) is False
    assert out["by_horizon"]["20"]["disposition"] == fk.HORIZON_APPLIED
    assert out["by_horizon"]["20"]["rank_identity"]["verdict"] == \
        fk.RANK_IDENTITY_NOT_APPLICABLE


def test_16_the_released_release30_operational_artifact_is_caught():
    """The historical artifact that produced the Aug-18 inversion must be
    recognised by the guard, or the contract binds only future mistakes."""
    root = Path(api_rf.r30_root())
    art_path = root / "model_artifact_operational.json"
    if not art_path.exists():
        pytest.skip("Release 30 research root not present")
    art = json.loads(art_path.read_text(encoding="utf-8"))
    assert fk.represents_approved_model(art) is True
    for h in ("5", "20"):
        v = fk.rank_identity(artifact=art, block=art["horizons"][h])
        assert v["verdict"] == fk.RANK_IDENTITY_VIOLATED, h
        assert v["disposition"] == fk.HORIZON_SUPPRESSED, h


# --------------------------------------------------------------------------- #
# C. CURRENT-SESSION FRESHNESS
# --------------------------------------------------------------------------- #
def test_20_the_operational_cross_section_comes_from_the_scoring_owner():
    scoring = {
        "primary_model_id": us.PRIMARY_MODEL_ID,
        "eligible_market_date": "2026-08-18",
        "universe_id": "u", "output_hash": "h",
        "rankings": [
            {"ticker": "AAA", "combined_score": 0.9, "eligible": True,
             "sector": "Information Technology", "adv_dollar": 1e9},
            {"ticker": "BBB", "combined_score": 0.4, "eligible": True,
             "sector": "Health Care", "adv_dollar": 1e9},
            {"ticker": "CCC", "combined_score": 0.7, "eligible": False,
             "exclusion_reason": "LIQUIDITY_FILTER_FAILED"},
        ],
    }
    ic = api_rf.build_operational_cross_section(scoring=scoring)
    assert ic["as_of_date"] == "2026-08-18"
    assert ic["requested_eligible_market_date"] == "2026-08-18"
    assert ic["feature_panel_behind_eligible_session"] is False
    assert ic["feature_panel_gap_calendar_days"] == 0
    assert [r["ticker"] for r in ic["rows"]] == ["AAA", "BBB"]
    assert ic["excluded"] == {"CCC": "LIQUIDITY_FILTER_FAILED"}
    assert ic["provenance"]["operational_score_owner"] == "api.universe_scoring"
    assert ic["provenance"]["live_input_policy"] == api_rf.LIVE_INPUT_POLICY


def test_21_a_periodic_research_snapshot_is_not_in_the_live_operational_path():
    src = (REPO / "api" / "return_forecast.py").read_text(encoding="utf-8")
    i = src.index("def build_operational(")
    j = src.index("def load_operational_return_forecast(")
    body = src[i:j]
    assert "load_forecast_input" not in body
    assert "_INPUT_FILE" not in body
    assert "build_operational_cross_section" in body


def test_22_a_stale_required_input_blocks_the_operational_forecast():
    freshness = {
        "eligible_market_date": "2026-08-18",
        "source_freshness": [
            {"source_id": "price_score_refresh", "status": "STALE",
             "as_of_date": "2026-08-05", "cadence": "DAILY",
             "required_for_signal_refresh": True,
             "blocks_current_operation": True,
             "authoritative_owner": "api.multi_horizon_engine"},
        ],
    }
    out = api_rf.build_operational(
        scoring={"eligible_market_date": "2026-08-18",
                 "primary_model_id": us.PRIMARY_MODEL_ID,
                 "rankings": [{"ticker": "AAA", "combined_score": 0.5,
                               "eligible": True, "sector": "X",
                               "adv_dollar": 1e9}]},
        artifact=_operational_artifact(slopes={20: 0.002}),
        freshness=freshness)
    assert out["state"] == api_rf.STATE_BLOCKED
    codes = {b["code"] for b in out["blockers"]}
    assert api_rf.BLOCK_STALE_REQUIRED_INPUT in codes
    assert out["operational_use"] == "DATA_BLOCKED"


def test_23_a_slower_cadence_input_is_judged_by_its_own_owner_not_by_today():
    """The quarterly fundamental panel is not stale for being older than
    today; only the canonical freshness owner decides, and it declares that
    source not required for signal refresh."""
    freshness = {
        "eligible_market_date": "2026-08-18",
        "source_freshness": [
            {"source_id": "price_score_refresh", "status": "FRESH",
             "as_of_date": "2026-08-18", "cadence": "DAILY",
             "required_for_signal_refresh": True,
             "blocks_current_operation": False},
            {"source_id": "fundamental_quarterly", "status": "NOT_DUE",
             "as_of_date": "2026-05-22", "cadence": "QUARTERLY",
             "required_for_signal_refresh": False,
             "blocks_current_operation": False},
        ],
    }
    fresh = api_rf.required_input_freshness(freshness=freshness)
    assert fresh["state"] == "FRESH"
    assert [r["source_id"] for r in fresh["required"]] == ["price_score_refresh"]
    assert fresh["stale"] == []


def test_24_the_live_operational_forecast_is_stamped_with_the_current_session():
    scoring = us.load_universe_scoring()
    eligible = scoring.get("eligible_market_date")
    if not eligible:
        pytest.skip("no live scoring")
    out = api_rf.load_operational_return_forecast(scoring=scoring)
    assert out.get("eligible_market_date") == eligible
    st = out.get("input_staleness") or {}
    assert st.get("behind_eligible_session") is False
    assert st.get("feature_as_of_date") == eligible


# --------------------------------------------------------------------------- #
# D. ONE AUTHORITATIVE LANE
# --------------------------------------------------------------------------- #
def test_30_the_research_lane_declares_itself_research():
    payload = zbt.load_zero_base_target()
    auth = payload.get("authority") or {}
    assert auth.get("lane") == zbt.LANE_RESEARCH_PREVIEW
    assert auth.get("can_become_a_proposal") is False


def test_31_the_governed_lane_declares_itself_governed():
    payload = zbt.load_operational_zero_base_target()
    auth = payload.get("authority") or {}
    assert auth.get("lane") == zbt.LANE_GOVERNED_OPERATIONAL
    assert auth.get("operational_model_id") in (None, us.PRIMARY_MODEL_ID)


def test_32_the_governed_lane_never_falls_back_to_the_research_forecast():
    src = (REPO / "api" / "zero_base_target.py").read_text(encoding="utf-8")
    i = src.index("def run_operational_allocation(")
    j = src.index("def load_operational_zero_base_target(")
    body = src[i:j]
    assert "load_model_artifact" not in body
    assert "rfc.build(" not in body
    assert "rfc.build_operational(" in body


def test_33_an_uncalibrated_policy_horizon_blocks_the_governed_target():
    ps = {"dates": {"eligible_market_date": "2026-08-18"},
          "active_book": {"book_id": "B1"},
          "capital": {"nav": 100000.0, "cash": 4000.0}, "positions": []}
    out = zbt.run_operational_allocation(
        portfolio_state=ps,
        scoring={"eligible_market_date": "2026-08-18",
                 "primary_model_id": us.PRIMARY_MODEL_ID, "rankings": []},
        artifact=_operational_artifact(slopes={5: 0.001, 20: None, 60: 0.001}))
    assert out["state"] == zk.STATE_BLOCKED
    codes = {b["code"] for b in out["blockers"]}
    assert "POLICY_HORIZON_NOT_CALIBRATED" in codes
    assert not (out.get("zero_base_target") or {}).get("rows")


def test_34_no_target_owner_but_the_allocator_kernel():
    """The zero-base composition owner delegates the allocation; it never
    builds one, approves one, or writes a decision."""
    import ast
    src = (REPO / "api" / "zero_base_target.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
              for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert not (called & {"record_decision", "approve", "build_proposal",
                          "run_proposal", "persist_proposal", "create_order",
                          "confirm_order_plan", "optimise"})
    assert "kernel.build_allocation" in src


# --------------------------------------------------------------------------- #
# E. THE CALIBRATION CONTRACT ITSELF
# --------------------------------------------------------------------------- #
def test_40_the_walk_forward_split_is_contiguous_ordered_and_embargoed():
    from paper_trader.alpha_agent import release30_1_operational_calibration as oc
    vd = oc.validation_dates(81, 60, (24, 6, 6))
    assert vd == sorted(vd)
    assert len(set(vd)) == len(vd)
    embargo = math.ceil(60 / oc.STEP_SESSIONS)
    assert min(vd) >= 24 + embargo
    # a longer horizon must never validate on an earlier date than a shorter one
    assert min(oc.validation_dates(81, 5, (24, 6, 6))) <= min(vd)


def test_41_an_uncalibrated_horizon_carries_no_usable_slope():
    from paper_trader.alpha_agent import release30_1_operational_calibration as oc
    hist = {"sections": [], "n_sections": 0, "first_date": None,
            "last_date": None, "skipped": {},
            "sources": {}}
    cal = oc.calibrate_horizon(hist, 20)
    assert cal["state"] == oc.CAL_NOT_CALIBRATED
    assert cal["slope"] is None
    assert oc.REASON_NO_FOLDS in cal["reasons"]


def test_42_the_frozen_operational_artifact_states_its_verdict():
    root = Path(api_rf.r30_1_root())
    path = root / ("model_artifact_%s.json" % api_rf.OPERATIONAL_TAG)
    if not path.exists():
        pytest.skip("Release 30.1 calibration not yet run")
    art = json.loads(path.read_text(encoding="utf-8"))
    assert art["operational_model_id"] == us.PRIMARY_MODEL_ID
    assert art["no_new_predictor_family"] is True
    assert art["adaptive_candidate_components_admitted"] == []
    assert art["automatic_promotion_allowed"] is False
    assert fk.represents_approved_model(art) is True
    for key, blk in art["horizons"].items():
        cal = blk["calibration"]
        assert cal["state"] in ("CALIBRATED", "NOT_CALIBRATED")
        if cal["state"] == "NOT_CALIBRATED":
            assert cal["slope"] is None, key
            assert cal["reasons"], key


# --------------------------------------------------------------------------- #
# F. SAFETY
# --------------------------------------------------------------------------- #
def test_50_the_operational_lane_creates_nothing():
    for payload in (api_rf.load_operational_return_forecast(),
                    zbt.load_operational_zero_base_target()):
        safety = payload.get("safety") or {}
        assert safety.get("creates_orders") is False
        assert safety.get("creates_decisions") is False
        assert safety.get("mutates_holdings") is False


def test_51_no_operational_read_path_writes():
    import ast
    src = (REPO / "api" / "return_forecast.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for name in ("build_operational", "build_operational_cross_section",
                 "load_operational_return_forecast", "required_input_freshness",
                 "load_operational_artifact"):
        body = ast.unparse(fns[name])
        for token in ("_atomic_write_json", "write_text", "mkdir", "replace("):
            assert token not in body, "%s:%s" % (name, token)


def test_52_the_adaptive_candidate_remains_not_activated():
    act = api_rf.activation_state()
    assert act["state"] == api_rf.ACTIVATION_NOT_ACTIVATED
    assert act["automatic_promotion_allowed"] is False
    research = zbt.load_zero_base_target()
    assert (research.get("authority") or {}).get("lane") == zbt.LANE_RESEARCH_PREVIEW


def test_53_the_research_calibration_lane_never_imports_the_api_package():
    src = (REPO / "alpha_agent" / "release30_1_operational_calibration.py").read_text(
        encoding="utf-8")
    assert "from paper_trader.api" not in src
    assert "import paper_trader.api" not in src


# --------------------------------------------------------------------------- #
# G. SOURCE LINKS - the evidence behind a signal is one click away
# --------------------------------------------------------------------------- #
def _event(**kw):
    base = {"event_id": "e", "primary_ticker": "MSFT",
            "decision_authority": "EVENT_TRIGGER_ONLY", "source_id": "news_rss",
            "event_type": "NEWS", "ingested_at": "2026-08-18T12:00:00Z",
            "materiality_inputs": {}}
    base.update(kw)
    return base


def _feed(events, **kw):
    ev = {"material_events": events, "holdings": ["MSFT"],
          "affected_holdings": ["MSFT"], "eligible_market_date": "2026-08-18",
          "last_run": {"state": "REASSESSED_NO_CHANGE"}}
    ev.update(kw)
    return mi.build(event_refresh=ev)


def test_60_a_canonical_http_reference_becomes_a_source_url():
    out = _feed([_event(payload_reference="https://example.com/a?x=1&y=2",
                        materiality_inputs={"title": "Example headline"})])
    row = out["rows"][0]
    assert row["source_url"] == "https://example.com/a?x=1&y=2"
    assert row["source_url_state"] == xr.URL_OK
    assert row["source_host"] == "example.com"
    assert row["source_title"] == "Example headline"
    assert row["source_reference"] is None


@pytest.mark.parametrize("ref,state", [
    ("javascript:alert(1)", xr.URL_SCHEME_REFUSED),
    ("data:text/html;base64,PHNjcmlwdD4=", xr.URL_SCHEME_REFUSED),
    ("vbscript:msgbox(1)", xr.URL_SCHEME_REFUSED),
    ("ftp://example.com/x", xr.URL_SCHEME_REFUSED),
    ("/relative/path", xr.URL_NOT_A_URL),
    ("www.example.com", xr.URL_NOT_A_URL),
    ("eodhd|AAPL|2026-08-18", xr.URL_NOT_A_URL),
    ("java\tscript:alert(1)", xr.URL_MALFORMED),
    ("https://", xr.URL_MALFORMED),
    ("", xr.URL_ABSENT),
    (None, xr.URL_ABSENT),
])
def test_61_an_unsafe_or_non_url_reference_never_becomes_a_link(ref, state):
    out = _feed([_event(payload_reference=ref)])
    row = out["rows"][0]
    assert row["source_url"] is None, ref
    assert row["source_url_state"] == state, ref


def test_62_a_non_url_reference_is_still_shown_as_plain_text():
    out = _feed([_event(payload_reference="eodhd|AAPL|2026-08-18")])
    row = out["rows"][0]
    assert row["source_url"] is None
    assert row["source_reference"] == "eodhd|AAPL|2026-08-18"


def test_63_an_over_long_reference_is_refused():
    out = _feed([_event(payload_reference="https://x.com/" + "a" * 4000)])
    assert out["rows"][0]["source_url"] is None
    assert out["rows"][0]["source_url_state"] == xr.URL_MALFORMED


def test_64_the_url_is_never_constructed_by_the_read_model():
    """No literal URL and no URL assembly may exist in the capital-impact feed."""
    tree = ast.parse((REPO / "api" / "material_information.py").read_text(
        encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not node.value.startswith(("http://", "https://")), node.value[:60]
    # and an event with no reference at all yields no link rather than a guess
    out = _feed([_event(materiality_inputs={"title": "no link here"})])
    assert out["rows"][0]["source_url"] is None


def test_65_the_safe_url_decision_has_exactly_one_owner():
    hits = []
    for path in sorted((REPO / "api").glob("*.py")) + sorted((REPO / "engine").glob("*.py")):
        if re.search(r"^def safe_external_url\s*\(", path.read_text(encoding="utf-8"), re.M):
            hits.append(path.name)
    assert hits == ["external_references.py"]
    assert "safe_external_url" in (REPO / "api" / "material_information.py").read_text(
        encoding="utf-8")


def test_66_a_source_link_changes_no_authority_and_no_interpretation():
    """The same event with and without a URL must classify identically."""
    with_url = _feed([_event(payload_reference="https://example.com/a")])["rows"][0]
    without = _feed([_event(payload_reference=None)])["rows"][0]
    for field in ("signal_authority", "authority_reach", "forecast_affected",
                  "risk_affected", "hoc_affected", "what_changed",
                  "portfolio_reassessed", "result"):
        assert with_url[field] == without[field], field


def test_67_an_external_article_is_never_operational_alpha():
    out = _feed([_event(decision_authority="EVENT_TRIGGER_ONLY",
                        payload_reference="https://example.com/a",
                        materiality_inputs={"title": "Huge news"})])
    row = out["rows"][0]
    assert row["forecast_affected"] is False
    assert out["external_article_is_not_alpha"] is True


def test_68_the_link_policy_is_declared_by_the_backend():
    out = _feed([_event(payload_reference="https://example.com/a")])
    pol = out["link_policy"]
    assert pol["target"] == "_blank"
    assert pol["rel"] == "noopener noreferrer"
    assert pol["allowed_schemes"] == ["http", "https"]


# --------------------------------------------------------------------------- #
# H. EXTERNAL MARKET REFERENCES
# --------------------------------------------------------------------------- #
def test_70_the_three_declared_reference_sites_are_present_and_linkable():
    payload = xr.load_external_market_references()
    assert payload["state"] == "READY"
    urls = {r["label"]: r["url"] for r in payload["rows"]}
    assert urls == {
        "FinancialJuice": "https://www.financialjuice.com/home",
        "Trading Economics - Indicators": "https://tradingeconomics.com/indicators",
        "Investing.com - Economic Calendar": "https://www.investing.com/economic-calendar",
    }
    for row in payload["rows"]:
        assert row["url_state"] == xr.URL_OK
        assert row["opens_in_new_tab"] is True
        assert row["link_target"] == "_blank"
        assert row["link_rel"] == "noopener noreferrer"


def test_71_a_reference_site_is_not_ingested_and_influences_nothing():
    payload = xr.load_external_market_references()
    assert payload["any_ingested"] is False
    for row in payload["rows"]:
        assert row["reference_state"] == xr.REF_REFERENCE_ONLY
        assert row["ingested"] is False
        assert row["signal_authorities"] == []
        assert row["influences_portfolio_decisions"] is False
    assert payload["safety"]["creates_signals"] is False
    assert payload["creates_no_event"] is True


def test_72_the_ingestion_claim_is_read_from_the_canonical_registries():
    """The answer must come from the owners, not from a caption. Proven by
    making a reference site resolve to a genuinely ingested source id."""
    from paper_trader.api import source_capability as sc
    real = sc.INGESTED_SOURCE_IDS[0]
    state = xr._ingestion_state(real)
    assert state["ingested"] is True
    assert state["state"] == xr.REF_INGESTED
    assert state["registered_source"] is True
    # an id nothing declares stays REFERENCE_ONLY
    absent = xr._ingestion_state("definitely_not_a_registered_source")
    assert absent["ingested"] is False
    assert absent["state"] == xr.REF_REFERENCE_ONLY


def test_73_the_reference_module_assigns_no_authority_of_its_own():
    src = (REPO / "api" / "external_references.py").read_text(encoding="utf-8")
    assert not re.search(r"^(ALPHA|RISK|TRIGGER)_BEARING", src, re.M)
    assert not re.search(r"^_?AUTHORITY_[A-Z_]*\s*=\s*\{", src, re.M)
    assert '"owns_no_calculation": True' in src
    tree = ast.parse(src)
    called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
              for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert not (called & {"classify_event", "build_event", "record_decision",
                          "build_proposal", "run_proposal", "create_order"})


def test_74_the_reference_module_makes_no_network_call():
    src = (REPO / "api" / "external_references.py").read_text(encoding="utf-8")
    for token in ("urlopen", "requests.", "httpx", "socket", "urlretrieve"):
        assert token not in src, token


def test_75_the_reference_surface_is_markets_only():
    payload = xr.load_external_market_references()
    assert payload["surface"] == "MARKETS"
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    start = ui.index('id="tab-markets"')
    end = ui.index("end tab-markets")
    card = ui.index('id="ext-refs-card"')
    assert start < card < end, "the region must live inside the Markets tab"
    # It must be REACHABLE only from the Markets route activation. Checking the
    # markup around the Today card is not enough - a loader is wired in the
    # bootstrap, far from the region it fills - so this checks CALL SITES.
    spans = []
    m_def = re.search(r"function loadExternalReferences\([\s\S]{0,1200}?\n\}", ui)
    assert m_def
    spans.append((m_def.start(), m_def.end()))
    m_win = ui.index("window.loadExternalReferences = loadExternalReferences;")
    spans.append((m_win, m_win + 80))
    m_mkt = re.search(r"tabName === 'markets'[\s\S]{0,1600}?\n  \}", ui)
    assert m_mkt, "the markets route activation block was not found"
    assert "loadExternalReferences(" in m_mkt.group(0), \
        "the reading list is not loaded from the Markets route"
    spans.append((m_mkt.start(), m_mkt.end()))

    stray = [ui[max(0, m.start() - 60):m.start() + 40].replace("\n", " ").strip()
             for m in re.finditer(r"loadExternalReferences\(", ui)
             if not any(lo <= m.start() < hi for lo, hi in spans)]
    assert stray == [], stray


def test_76_the_reference_route_is_get_only():
    app_src = (REPO / "api" / "app.py").read_text(encoding="utf-8")
    assert '"/v1/market/external-references"' in app_src
    for verb in ("post", "put", "delete", "patch"):
        assert not re.search(
            r'@app\.%s\(\s*"/v1/market/external-references"' % verb, app_src)


# --------------------------------------------------------------------------- #
# I. SIGNAL TRANSPARENCY - and no duplicate calculation in the browser
# --------------------------------------------------------------------------- #
def test_80_every_declared_transparency_field_is_present_on_a_row():
    out = _feed([_event(payload_reference="https://example.com/a",
                        materiality_inputs={"title": "t"})])
    row = out["rows"][0]
    missing = [f for f in out["transparency_fields"] if f not in row]
    assert missing == []
    for required in ("source", "timestamp", "ticker", "event_type",
                     "signal_authority", "source_url", "what_changed",
                     "forecast_affected", "risk_affected", "hoc_affected",
                     "portfolio_reassessed", "result"):
        assert required in out["transparency_fields"], required


def test_81_the_browser_computes_no_verdict_for_these_regions():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    for fn in ("renderMaterialInformation", "renderExternalReferences"):
        m = re.search(r"function %s\([\s\S]{0,9000}?\n\}" % fn, ui)
        assert m, fn
        block = m.group(0)
        # no hand-rolled anchor, no constructed URL, no authority decision
        assert not re.search(r"<a\s+href=", block), fn
        assert "http://" not in block and "https://" not in block, fn
        for token in ("ALPHA_BEARING", "OPERATIONAL_ALPHA'", "INGESTED_SOURCE_IDS"):
            assert token not in block, "%s:%s" % (fn, token)


def test_82_the_link_helper_always_emits_target_and_rel():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    m = re.search(r"function _r30srcLink\([\s\S]{0,1600}?\n\}", ui)
    assert m
    helper = m.group(0)
    assert "if (!url) return label;" in helper, "no URL -> plain text"
    assert 'target="' in helper
    assert "noopener noreferrer" in helper
    assert "_r30attr(url)" in helper, "the href must be attribute-escaped"


def test_83_the_attribute_escape_neutralises_a_quote_break_out():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    m = re.search(r"function _r30attr\(s\) \{[\s\S]{0,600}?\n\}", ui)
    assert m
    body = m.group(0)
    for ch in ("&amp;", "&lt;", "&gt;", "&quot;", "&#39;"):
        assert ch in body, ch


def test_84_the_forbidden_browser_dialogs_are_still_absent():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    for fn in ("renderMaterialInformation", "renderExternalReferences",
               "_r30srcLink"):
        m = re.search(r"function %s\([\s\S]{0,9000}?\n\}" % fn, ui)
        assert m, fn
        assert "alert(" not in m.group(0), fn
        assert "confirm(" not in m.group(0), fn
