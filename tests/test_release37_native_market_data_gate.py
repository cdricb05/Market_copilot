"""Release 37 - Native-Market Data Expansion & Purchase Gate regressions.

The invariants that matter here are commercial and epistemic rather than
statistical: nothing may be bought, no gate may be duplicated, no vendor claim
may become a cell unlock without an instrument mapping, and no score may
outrank a hard gate. Every test runs offline with injected transports.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from paper_trader.alpha_agent import r37  # noqa: E402
from paper_trader.alpha_agent.r37 import campaign as r37_campaign  # noqa: E402
from paper_trader.alpha_agent.r37 import compute as r37_compute  # noqa: E402
from paper_trader.alpha_agent.r37 import contract as r37_contract  # noqa: E402
from paper_trader.alpha_agent.r37 import (  # noqa: E402
    market_structure as r37_structure,
)
from paper_trader.alpha_agent.r37 import ml_readiness as r37_ml  # noqa: E402
from paper_trader.alpha_agent.r37 import providers as r37_providers  # noqa: E402
from paper_trader.alpha_agent.r37 import purchase as r37_purchase  # noqa: E402
from paper_trader.alpha_agent.r37 import samples as r37_samples  # noqa: E402
from paper_trader.alpha_agent.r37 import scoring as r37_scoring  # noqa: E402
from paper_trader.alpha_agent.r37 import unlock as r37_unlock  # noqa: E402

R37_DIR = REPO_ROOT / "alpha_agent" / "r37"


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def research_root(tmp_path, monkeypatch):
    monkeypatch.setenv(r37.RESEARCH_ROOT_ENV, str(tmp_path / "r37"))
    return tmp_path / "r37"


CFE_SAMPLE = (
    b"CFE data is compiled for the convenience of site visitors, and, further,"
    b" is furnished without responsibility\r\n"
    b"Date,VOLATILITY INDEX Volume,VOLATILITY INDEX OI,"
    b"S&P 500 Variance Volume,S&P 500 Variance OI\r\n"
    b"3/26/2004,461,368,,\r\n"
    b"3/29/2004,117,349,,\r\n"
    b"3/30/2004,191,448,,\r\n"
)
#: Deliberately larger than the released acquisition owner's 256-byte floor:
#: a payload below it is recorded as PAYLOAD_TOO_SMALL and never written, which
#: is correct behaviour and would otherwise make these fixtures untestable.
LBMA_SAMPLE = json.dumps(
    [{"is_cms_locked": 0, "d": "1968-04-01", "v": [37.7, 15.68, None]},
     {"is_cms_locked": 0, "d": "1968-04-02", "v": [37.3, 37.3, None]},
     {"is_cms_locked": 0, "d": "1968-04-03", "v": [None, None, None]}]
    + [{"is_cms_locked": 0, "d": "1970-%02d-01" % (m + 1),
        "v": [35.0 + m, 14.0 + m, None]} for m in range(12)]
).encode()
NYFED_SAMPLE = (
    b'"As Of Date","Time Series","Value (millions)"\r\n'
    + b"".join(b'"2026-08-%02d","PDST5TSSI-TC","%d"\r\n' % (d, 100 + d)
               for d in range(1, 29))
)


def fetch_transport(url: str) -> bytes:
    """A download transport: url -> bytes."""
    if "cfevoloi" in url:
        return CFE_SAMPLE
    if "lbma" in url or "prices.lbma" in url:
        return LBMA_SAMPLE
    if "newyorkfed" in url:
        return NYFED_SAMPLE
    return b"x" * 4096


def probe_transport(url: str):
    """A probe transport: url -> (status, head)."""
    return 403, b""


class FakeNorgate:
    """Stands in for the vendor client: every dated-contract call present."""

    __version__ = "1.0.74"

    def futures_market_symbols(self):
        return ["ES"]

    def futures_market_session_symbols(self):
        return ["ES"]

    def futures_market_session_contracts(self, symbol):
        raise ValueError("not entitled")

    def futures_market_session_type(self, symbol):
        raise ValueError("not entitled")

    def futures_market_name(self, symbol):
        return "E-mini S&P 500"

    def first_notice_date(self, symbol, **kw):
        return None

    def last_quoted_date(self, symbol, **kw):
        return None

    def first_quoted_date(self, symbol, **kw):
        return "1997-09-09"

    def point_value(self, symbol):
        return 50.0

    def tick_size(self, symbol):
        return 0.25

    def lowest_ever_tick_size(self, symbol):
        return 0.25

    def margin(self, symbol):
        return 16060.0

    def currency(self, symbol):
        return "USD"

    def exchange_name(self, symbol):
        return "CME"

    def price_timeseries(self, *a, **kw):
        return None

    def unadjusted_close_timeseries(self, *a, **kw):
        return None

    def status(self):
        return True

    def databases(self):
        return ["Continuous Futures", "US Equities"]

    def database_symbols(self, db):
        return ["&ES"] if db == "Continuous Futures" else ["AAPL"]


def fake_gpu():
    return {"state": "MEASURED", "returncode": 0, "devices": [
        {"name": "NVIDIA GeForce GTX 1650", "vram_mib": 4096, "vram_gb": 4.0,
         "driver_version": "566.36", "compute_capability": "7.5"}]}


# --------------------------------------------------------------------------- #
# 1. Contract - nothing is bought, and nothing can be
# --------------------------------------------------------------------------- #
def test_every_commercial_flag_is_false():
    for flag in ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
                 "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
                 "MAY_ACCEPT_LICENCE_AGREEMENT", "MAY_SUBMIT_PAYMENT_DETAILS",
                 "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
                 "MAY_DOWNLOAD_MODEL_WEIGHTS"):
        assert getattr(r37_contract, flag) is False, flag


def test_package_safety_block_is_exhaustively_false():
    block = r37.safety_block()
    for key, value in block.items():
        if key == "safety":
            continue
        assert value is False, key


def test_no_gate_state_grants_purchase_authority():
    for state in r37_contract.GATE_STATES:
        authority = r37_contract.purchase_authority(state)
        assert authority["purchase_authorised"] is False
        assert authority["money_spent_usd"] == 0.0
    recommended = r37_contract.purchase_authority(
        r37_contract.STATE_BUY_RECOMMENDED)
    assert recommended["human_decision_required"] is True


def test_alpha_result_is_structurally_not_tested():
    assert r37_contract.alpha_result() == "NOT_TESTED"
    assert r37_contract.ALPHA_RESULT_IS_STRUCTURALLY_NOT_TESTED is True
    assert r37_contract.MAY_LAUNCH_ALPHA_CAMPAIGN is False
    assert r37_contract.ML_TRAINING_CAMPAIGN_IN_SCOPE is False
    assert r37_contract.MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE is False


def test_exhausted_campaigns_may_not_be_rerun():
    assert r37_contract.MAY_RERUN_EXHAUSTED_CAMPAIGNS is False
    joined = " ".join(r37_contract.SETTLED_AND_NOT_REOPENED)
    for release in ("R31", "R32", "R33", "R34", "R35", "R36"):
        assert release in joined


def test_superseded_campaigns_are_declared_with_their_defects():
    superseded = r37_contract.SUPERSEDED_CAMPAIGNS
    assert superseded, "a corrected run must record what it corrected"
    for name, row in superseded.items():
        assert name != r37_contract.CAMPAIGN_ID
        assert row["defects"], name
        assert row["artifacts_retained"] is True


def test_contract_hash_is_stable_for_a_fixed_timestamp():
    a = r37_contract.build(created_at="2026-08-22T00:00:00+00:00")
    b = r37_contract.build(created_at="2026-08-22T00:00:00+00:00")
    assert a["contract_hash"] == b["contract_hash"]


# --------------------------------------------------------------------------- #
# 2. Provider long list
# --------------------------------------------------------------------------- #
def test_long_list_validates():
    result = r37_providers.validate()
    assert result["missing_fields"] == []
    assert result["states_outside_vocabulary"] == []
    assert result["classifications_outside_vocabulary"] == []
    assert result["rows_without_evidence"] == []
    assert result["rows_without_a_reason"] == []
    assert result["valid"] is True


def test_every_lane_is_represented():
    by_lane = r37_providers.by_lane()
    for lane in r37_providers.LANES:
        assert by_lane[lane], lane


def test_the_long_list_contains_losing_candidates():
    """A review with only a winner is a justification, not a review."""
    states = {r["gate_state"] for r in r37_providers.rows()}
    assert r37_contract.STATE_BUY_RECOMMENDED in states
    do_not_buy = {s for s in states if s.startswith("DO_NOT_BUY")}
    assert len(do_not_buy) >= 3, states


def test_every_candidate_state_is_terminal():
    for row in r37_providers.rows():
        assert row["gate_state"] in r37_contract.GATE_STATES
        assert row["gate_state"] not in ("INTERESTING", "FUTURE_WORK", None)


# --------------------------------------------------------------------------- #
# 3. Cell unlock - derived from Release 36, never typed
# --------------------------------------------------------------------------- #
def test_blocked_frontier_comes_from_release36():
    frontier = r37_unlock.blocked_frontier()
    assert frontier["source"] in (r37_unlock.SOURCE_FROZEN,
                                  r37_unlock.SOURCE_DERIVED)
    assert frontier["n_blocked_cells"] > 0
    assert frontier["n_markets"] > 0


def test_no_candidate_claims_a_market_that_is_not_blocked():
    built = r37_unlock.build()
    assert built["claims_without_a_blocked_market"] == []


def test_a_proxy_or_signal_dataset_never_unlocks_a_native_cell():
    frontier = r37_unlock.blocked_frontier()
    market = sorted(frontier["markets"])[0]
    row = {"dataset_id": "fake_proxy", "provider": "test",
           "implementation_level": r37_contract.LEVEL_PROXY,
           "dated_contracts_available": False,
           "markets_covered": [market], "markets_partial": []}
    result = r37_unlock.for_dataset(row, frontier)
    assert result["cells_unlocked_full"] == 0
    assert result["proxy_credit_refused"] is True
    assert market in result["markets_unlocked_partial"]


def test_partial_unlocks_never_enter_the_headline():
    assert r37_contract.PARTIAL_UNLOCK_COUNTS_IN_HEADLINE is False
    built = r37_unlock.build()
    for row in built["rows"]:
        assert row["cells_unlocked_full"] <= row["cells_unlocked_ceiling"]
        assert row["headline_counts_partial"] is False


def test_the_recommended_dataset_unlocks_a_majority_of_the_frontier():
    built = r37_unlock.build()
    best = next(r for r in built["rows"]
                if r["dataset_id"] == "norgate_futures_package")
    assert best["cells_unlocked_full"] >= 40
    assert best["share_of_blocked_frontier"] > 0.5
    assert set(best["asset_classes_unlocked_full"]) >= {
        "COMMODITY", "RATES", "VOLATILITY"}


def test_some_blocked_markets_remain_out_of_reach():
    """A release claiming every gap is purchasable would be selling."""
    built = r37_unlock.build()
    assert built["markets_no_candidate_reaches"]
    assert built["cells_no_candidate_reaches"] > 0


# --------------------------------------------------------------------------- #
# 4. Scoring
# --------------------------------------------------------------------------- #
def test_score_arithmetic_matches_the_declared_formula():
    row = {"dataset_id": "x", "provider": "p", "lane": "L",
           "gate_state": r37_contract.STATE_BUY_RECOMMENDED,
           "implementation_level": r37_contract.LEVEL_NATIVE,
           "dated_contracts_available": True, "history_years": 40.0,
           "pit_class": "OBSERVED_AS_PUBLISHED",
           "survivorship_class": "DISCONTINUED_RETAINED",
           "licence_class": "RESEARCH_USE_CLEAR", "identity_class": "STRONG",
           "opacity_class": "RAW_AS_PUBLISHED",
           "annual_cost_usd": 100.0, "one_time_cost_usd": 0.0}
    unlock_row = {"cells_unlocked_full": 10, "cells_unlocked_ceiling": 10,
                  "n_asset_classes_unlocked_full": 2,
                  "native_markets_unlocked_full": 2}
    scored = r37_scoring.score_row(row, unlock_row)
    expected_multiplier = 1.0 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0 * (
        r37_contract.BREADTH_BASE + 2 * r37_contract.BREADTH_PER_ASSET_CLASS)
    assert scored["research_value_points"] == pytest.approx(
        10 * expected_multiplier, rel=1e-6)
    assert scored["research_value_per_dollar_score"] == pytest.approx(
        10 * expected_multiplier / 100.0, rel=1e-6)
    assert scored["cost_per_r36_cell_unlocked"] == 10.0
    assert scored["cost_per_year_of_history"] == 2.5


def test_a_one_time_cost_is_amortised():
    row = {"dataset_id": "x", "provider": "p", "lane": "L",
           "gate_state": r37_contract.STATE_BUY_RECOMMENDED,
           "implementation_level": r37_contract.LEVEL_NATIVE,
           "dated_contracts_available": True, "history_years": 20.0,
           "pit_class": "UNKNOWN", "survivorship_class": "UNKNOWN",
           "licence_class": "UNKNOWN", "identity_class": "UNKNOWN",
           "opacity_class": "UNKNOWN",
           "annual_cost_usd": 0.0, "one_time_cost_usd": 500.0}
    cost = r37_scoring.annualised_cost(row)
    assert cost["annualised_cost_usd"] == pytest.approx(
        500.0 / r37_contract.COST_AMORTISATION_YEARS)


def test_a_survivor_only_dataset_cannot_be_ranked_however_cheap():
    """The exact failure a naive value-per-dollar metric would produce."""
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    ranked = scorecard["ranked_investable"]
    assert "binance_public_data_archive" not in ranked
    naive = scorecard["naive_ranking_ignoring_hard_gates"]
    assert "binance_public_data_archive" in naive
    assert r37_contract.SCORE_MAY_OVERRIDE_A_HARD_GATE is False


def test_a_dataset_with_an_unknown_cost_has_no_score():
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    for row in scorecard["rows"]:
        if not row["cost"]["cost_known"]:
            assert row["research_value_per_dollar_score"] is None
            assert row["dataset_id"] in scorecard["cost_unknown"]


def test_the_recommended_dataset_ranks_first():
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    assert scorecard["best"] == "norgate_futures_package"


# --------------------------------------------------------------------------- #
# 5. Purchase gate composition - Release 37 defines no gate
# --------------------------------------------------------------------------- #
def test_release37_defines_no_second_dataset_gate():
    forbidden = ("def evaluate_dataset(", "def evaluate_gap(",
                 "def load_data_expansion(", "def purchase_decision(")
    for path in sorted(R37_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, "%s defines %s" % (path.name, token)


def test_no_release37_purchase_gate_module_exists():
    for name in ("purchase_gate.py", "data_expansion_gate.py",
                 "information_purchase_gate.py"):
        assert not (R37_DIR / name).exists(), name


def test_the_slice9_gate_is_called_and_its_verdict_recorded_verbatim():
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    results = r37_purchase.build(built, scorecard,
                                 campaign_id="test_campaign")
    states = results["slice9_states"]
    assert set(states) == {r["dataset_id"] for r in r37_providers.rows()}
    assert all(v is not None for v in states.values()), states
    assert set(states.values()) <= {"REJECT", "INSUFFICIENT_EVIDENCE",
                                    "RESEARCH_ONLY", "CANDIDATE",
                                    "PURCHASE_RECOMMENDED",
                                    "INTEGRATION_RECOMMENDED", "UNMEASURABLE",
                                    "BLOCKED"}


def test_the_slice9_gate_is_not_overridden_by_the_release37_state():
    """The recommendation and the measured-lift gate answer different questions."""
    assert r37_purchase.SLICE9_RESULT_MAY_BE_OVERRIDDEN is False
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    results = r37_purchase.build(built, scorecard, campaign_id="test_campaign")
    row = next(r for r in results["rows"]
               if r["dataset_id"] == "norgate_futures_package")
    assert row["r37_state"] == r37_contract.STATE_BUY_RECOMMENDED
    # No measured lift exists for blocked data, so the lift gate must not pass.
    assert row["slice9"]["slice9_state"] != "PURCHASE_RECOMMENDED"
    assert results["slice9_purchase_recommendations"] == []


def test_the_gate_composition_persists_nothing_to_the_slice9_store():
    built = r37_unlock.build()
    scorecard = r37_scoring.build(built)
    results = r37_purchase.build(built, scorecard, campaign_id="test_campaign")
    for row in results["rows"]:
        assert row["slice9"].get("persisted_to_slice9_store") is False
    assert results["information_gate"]["written_to_r32_root"] is False


def test_the_information_gate_conditions_are_the_released_ten():
    from paper_trader.alpha_agent.r32 import purchase_gate as r32_gate
    built = r37_unlock.build()
    results = r37_purchase.build(built, r37_scoring.build(built),
                                 campaign_id="test_campaign")
    assert results["information_gate"]["conditions"] == list(
        r32_gate.CONDITIONS)


def test_no_candidate_reports_a_cost_condition_as_satisfied():
    """The tenth condition needs a human, and this release is not one."""
    built = r37_unlock.build()
    results = r37_purchase.build(built, r37_scoring.build(built),
                                 campaign_id="test_campaign")
    for gap in results["information_gate"]["gaps"]:
        assert gap["purchase_authorised"] is False
        assert gap["money_spent_usd"] == 0.0


# --------------------------------------------------------------------------- #
# 6. Samples
# --------------------------------------------------------------------------- #
def test_acquisition_uses_the_released_http_owner():
    text = (R37_DIR / "samples.py").read_text(encoding="utf-8")
    assert "from ..r35 import acquisition as _r35_acquisition" in text
    assert "_r35_acquisition.fetch(" in text
    # No second downloader anywhere in the package.
    for path in sorted(R37_DIR.glob("*.py")):
        body = path.read_text(encoding="utf-8")
        assert "def fetch(" not in body, path.name


def test_free_samples_are_acquired_and_validated(research_root):
    acquired = r37_samples.acquire_free_samples(transport=fetch_transport)
    assert set(acquired) == set(r37_samples.SAMPLE_FILES)
    for record in acquired.values():
        assert record["ok"] is True
        assert record["cost_usd"] == 0.0
        assert record["account_required"] is False
        assert str(research_root) in record["path"]
    validated = r37_samples.validate_samples(acquired)
    assert validated["LBMA_GOLD_PM"]["verdict"] in (
        r37_samples.SAMPLE_OK, r37_samples.SAMPLE_THIN)
    assert validated["LBMA_GOLD_PM"]["first_date"] == "1968-04-01"


def test_the_cboe_sample_is_measured_as_product_level(research_root):
    acquired = r37_samples.acquire_free_samples(
        transport=fetch_transport, names=["CBOE_CFE_VOLUME_OPEN_INTEREST"])
    validated = r37_samples.validate_samples(acquired)
    row = validated["CBOE_CFE_VOLUME_OPEN_INTEREST"]
    assert row["granularity"] == "PRODUCT_LEVEL_NOT_CONTRACT_LEVEL"
    assert row["carries_per_contract_expiry"] is False
    assert row["carries_settlement_price"] is False
    assert row["first_date"] == "3/26/2004"
    assert row["distinct_products"] >= 2


def test_the_scorecard_agrees_with_what_the_sample_measured():
    """The claim the sample disproved must be gone from the long list."""
    row = r37_providers.DATASETS["cboe_cfe_volume_open_interest_free"]
    assert row["dated_contracts_available"] is False
    assert "PRODUCT" in (row["instruments"] or "").upper() or \
        "not by" in (row["instruments"] or "")
    assert row["implementation_level"] == r37_contract.LEVEL_SIGNAL


def test_an_unreachable_probe_is_unmeasured_not_open():
    def dead(url):
        raise OSError("network down")

    blocks = r37_samples.confirm_blocks(transport=dead)
    for row in blocks.values():
        assert row["still_blocked"] is None
        assert row["state"] == "UNMEASURED"


def test_a_403_confirms_the_block_stands():
    blocks = r37_samples.confirm_blocks(transport=probe_transport)
    for name, row in blocks.items():
        assert row["still_blocked"] is True, name
        assert row["state"] == "BLOCKED_LICENSING"


def test_the_owned_client_can_express_a_dated_contract_but_is_not_entitled():
    """The measurement that decides the whole purchase case."""
    measured = r37_samples.measure_owned_futures_client(vendor=FakeNorgate())
    assert measured["client_supports_dated_contracts"] is True
    assert measured["dated_contract_api_missing"] == []
    assert measured["entitled_futures_markets"] == 1
    assert measured["dated_contract_enumeration"]["ok"] is False
    assert "ENTITLEMENT" in measured["finding"]


def test_a_sample_may_never_carry_an_alpha_claim():
    assert r37_samples.A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM is False


def test_no_credential_reaches_a_sample_artifact(research_root):
    acquired = r37_samples.acquire_free_samples(transport=fetch_transport)
    body = r37_samples.registry_artifact(
        acquired, {}, {}, campaign_id="t", created_at="2026-01-01T00:00:00Z")
    text = json.dumps(body)
    assert "api_key=" not in text.lower().replace("api_key=redacted", "")
    assert body["credentials_written_to_artifacts"] is False


# --------------------------------------------------------------------------- #
# 7. Compute inventory - read only
# --------------------------------------------------------------------------- #
def test_compute_inventory_installs_nothing():
    inventory = r37_compute.measure(gpu_runner=fake_gpu)
    body = r37_compute.artifact(inventory, campaign_id="t",
                                created_at="2026-01-01T00:00:00Z")
    assert body["read_only"] is True
    assert body["installed_anything"] is False
    assert body["downloaded_model_weights"] is False
    assert body["purchased_cloud_compute"] is False


def test_compute_inventory_reads_metadata_rather_than_importing():
    text = (R37_DIR / "compute.py").read_text(encoding="utf-8")
    for token in ("import torch", "import tensorflow", "import sklearn",
                  "import xgboost", "pip install"):
        assert token not in text, token


def test_installed_memory_is_measured_on_this_platform():
    """os.sysconf does not exist on Windows, which is where this estate runs."""
    total, source = r37_compute._total_ram_gb()
    assert source != "UNMEASURED_ON_THIS_PLATFORM", source
    assert total and total > 1.0


def test_constraints_are_computed_from_the_measurement():
    inventory = r37_compute.measure(gpu_runner=fake_gpu)
    constraints = r37_compute.constraints(inventory)
    assert constraints["max_vram_gb"] == 4.0
    assert constraints["gpu_present"] is True
    assert constraints["gpu_usable_for_foundation_model_inference"] is False
    assert "VRAM" in constraints["binding_constraints"]


# --------------------------------------------------------------------------- #
# 8. ML readiness - declared requirements, computed feasibility
# --------------------------------------------------------------------------- #
def test_readiness_trains_nothing():
    assert r37_ml.TRAINS_A_MODEL is False
    assert r37_ml.SELECTS_A_MODEL is False
    assert r37_ml.NEWER_IMPLIES_BETTER is False


def test_every_family_has_a_row_and_every_row_names_its_risks():
    rows = r37_ml.matrix({"max_vram_gb": 4.0, "total_ram_gb": 64.0,
                          "gpu_present": True})
    families = {r["family"] for r in rows}
    assert families == set(r37_ml.FAMILIES)
    for row in rows:
        assert row["pit_risks"]
        assert row["survivorship_risks"]
        assert row["use_cases"]
        assert row["training_cost_class"] in r37_ml.COST_CLASSES


def test_feasibility_follows_the_measured_hardware():
    small = {"max_vram_gb": 4.0, "total_ram_gb": 64.0, "gpu_present": True}
    large = {"max_vram_gb": 80.0, "total_ram_gb": 512.0, "gpu_present": True}
    small_rows = r37_ml.summarise(r37_ml.matrix(small))
    large_rows = r37_ml.summarise(r37_ml.matrix(large))
    assert large_rows["feasible_count"] > small_rows["feasible_count"]
    assert "TIMESFM_AND_MOIRAI_CLASS" in small_rows["not_feasible_locally"]
    assert "TIMESFM_AND_MOIRAI_CLASS" in large_rows["feasible_locally"]


def test_cpu_only_families_are_feasible_without_a_gpu():
    none = {"max_vram_gb": 0.0, "total_ram_gb": 64.0, "gpu_present": False}
    summary = r37_ml.summarise(r37_ml.matrix(none))
    assert "GRADIENT_BOOSTED_TREES" in summary["feasible_locally"]
    assert "REGULARISED_LINEAR" in summary["feasible_locally"]


def test_the_recommended_purchase_improves_most_model_families():
    rows = r37_ml.matrix({"max_vram_gb": 4.0, "total_ram_gb": 64.0,
                          "gpu_present": True})
    summary = r37_ml.summarise(rows)
    assert len(summary["improved_by_recommended_purchase"]) >= 10


def test_the_data_contract_composes_owners_and_outputs_more_than_a_mean():
    contract = r37_ml.data_contract()
    assert contract["composes_existing_owners"] is True
    assert contract["creates_a_second_market_data_owner"] is False
    for field in ("decision_timestamp", "feature_observation_timestamp",
                  "embargo_sessions", "survivorship_state",
                  "point_in_time_state", "transaction_cost_bps",
                  "passive_control_return", "missingness_mask"):
        assert field in contract["input_fields"], field
    for field in ("expected_excess_return", "quantiles",
                  "probability_of_positive_excess", "expected_volatility",
                  "uncertainty", "model_disagreement", "abstain"):
        assert field in contract["output_fields"], field


# --------------------------------------------------------------------------- #
# 9. Market structure backlog - designed, not run
# --------------------------------------------------------------------------- #
def test_the_backlog_is_not_executed():
    assert r37_structure.EXECUTED_IN_THIS_RELEASE is False
    assert r37_structure.READS_A_PRICE is False
    assert r37_structure.COMPUTES_A_FEATURE is False
    assert r37_structure.VISUAL_EXPERIMENT_IN_SCOPE is False


def test_a_swing_point_may_only_be_used_after_confirmation():
    assert r37_structure.PIVOT_CONFIRMATION_REQUIRED is True
    assert r37_structure.PIVOT_TIMESTAMP_IS_THE_CONFIRMATION_DATE is True
    assert r37_structure.FUTURE_KNOWN_EXTREMA_ALLOWED is False
    assert r37_structure.PIVOT_CONFIRMATION_SESSIONS >= 1
    assert r37_structure.PIVOT_PARAMETER_SEARCH_ALLOWED is False


def test_fibonacci_is_a_hypothesis_with_a_placebo_arm():
    assert r37_structure.FIBONACCI_IS_DOCTRINE is False
    assert r37_structure.PLACEBO_ARM_REQUIRED is True
    canonical = set(r37_structure.FIBONACCI_LEVELS)
    placebo = set(r37_structure.FIBONACCI_PLACEBO_LEVELS)
    assert canonical & placebo == set(), "a placebo may not be a real level"
    assert len(placebo) >= len(canonical) - 2
    for level in (0.236, 0.382, 0.5, 0.618, 0.786, 1.272, 1.618):
        assert level in canonical, level
    design = r37_structure.FIBONACCI_DESIGN
    assert design["denominator_includes_placebo_levels"] is True
    assert "pullback" in design["failure_reading"] or \
        "retracement" in design["failure_reading"]


def test_every_structural_hypothesis_declares_a_control():
    for hypothesis in r37_structure.HYPOTHESES:
        assert hypothesis["control"], hypothesis["hypothesis_id"]
        assert hypothesis["why_it_might_fail"], hypothesis["hypothesis_id"]
        assert hypothesis["leakage_risk"], hypothesis["hypothesis_id"]


def test_the_visual_lane_declares_its_leakage_rules():
    backlog = r37_structure.backlog()
    assert len(backlog["visual"]["representation_arms"]) == 3
    joined = " ".join(backlog["visual"]["leakage_rules"]).lower()
    assert "decision timestamp" in joined
    assert "axis" in joined


# --------------------------------------------------------------------------- #
# 10. Campaign - every write lands under the research root
# --------------------------------------------------------------------------- #
def test_campaign_writes_only_under_the_research_root(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    assert outcome["artifacts"]
    for name, path in outcome["artifacts"].items():
        assert str(research_root) in path, "%s escaped the root: %s" % (name,
                                                                        path)
        assert Path(path).exists(), name


def test_campaign_produces_every_required_artifact(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    for required in ("research_contract", "provider_long_list",
                     "provider_scorecard", "dataset_purchase_scorecard",
                     "r36_cell_unlock_map", "data_entitlement_matrix_updated",
                     "sample_registry", "sample_validation_report",
                     "purchase_gate_results", "recommended_data_investment",
                     "blocked_vendor_actions", "compute_inventory",
                     "advanced_ml_readiness_matrix",
                     "advanced_ml_data_contract",
                     "market_structure_visual_intelligence_backlog",
                     "final_verdict"):
        assert required in outcome["artifacts"], required


def test_campaign_verdict_spends_nothing(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    verdict = outcome["verdict"]
    assert verdict["money_spent_usd"] == 0.0
    assert verdict["trials_started"] == 0
    assert verdict["accounts_created"] == 0
    assert verdict["subscriptions_changed"] == 0
    assert verdict["operational_writes"] == 0
    assert verdict["portfolio_mutations"] == 0
    assert verdict["model_promotions"] == 0
    assert verdict["purchase_authorised"] is False
    assert verdict["ALPHA_RESULT"] == "NOT_TESTED"
    assert verdict["verdict"] in r37_contract.VERDICTS


def test_campaign_recommends_the_measured_best_dataset(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    verdict = outcome["verdict"]
    assert verdict["verdict"] == r37_contract.VERDICT_RECOMMENDED
    assert verdict["best_dataset"] == "norgate_futures_package"
    assert verdict["best_dataset_cells_unlocked"] >= 40


def test_every_blocked_candidate_carries_a_named_human_action(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    actions = outcome["blocked_actions"]
    assert actions
    for row in actions:
        assert row["action_declared"] is True, row["dataset_id"]
        assert row["action"]["exact_step"]
        assert row["action"]["who"]


def test_the_recommendation_answers_analyst_versus_futures(research_root):
    outcome = r37_campaign.run(campaign_id="test_campaign",
                               fetch_transport=fetch_transport,
                               probe_transport=probe_transport,
                               vendor=FakeNorgate(), gpu_runner=fake_gpu)
    path = Path(outcome["artifacts"]["recommended_data_investment"])
    body = json.loads(path.read_text(encoding="utf-8"))
    comparison = body["analyst_versus_futures"]
    assert comparison["answer"].startswith("NO")
    assert comparison["what_would_change_it"]
    assert body["best"]["purchase_authorised"] is False
    assert body["spend_now_recommendation"]["authorised_by_this_release"] \
        is False


def test_a_rerun_refuses_to_overwrite_a_frozen_artifact(research_root):
    r37_campaign.run(campaign_id="test_campaign",
                     fetch_transport=fetch_transport,
                     probe_transport=probe_transport,
                     vendor=FakeNorgate(), gpu_runner=fake_gpu)
    with pytest.raises(r37.ArtifactImmutable):
        r37_campaign.run(campaign_id="test_campaign",
                         fetch_transport=fetch_transport,
                         probe_transport=probe_transport,
                         vendor=FakeNorgate(), gpu_runner=fake_gpu)


# --------------------------------------------------------------------------- #
# 11. Boundaries - the research lane did not move
# --------------------------------------------------------------------------- #
def test_no_module_touches_an_operational_owner():
    forbidden = ("api.operational_book", "api.daily_close",
                 "api.rebalance_execution", "api.portfolio_decision",
                 "engine.normal_cycle", "paper_trader.broker")
    for path in sorted(R37_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, "%s references %s" % (path.name, token)


def test_no_module_contains_an_execution_call():
    forbidden = ("create_order", "place_order", "submit_order",
                 "confirm_target", "apply_proposal", "approve_proposal",
                 "promote_champion", "activate_model", "write_holdings",
                 "write_cash", "execute_rebalance")
    for path in sorted(R37_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8").lower()
        for token in forbidden:
            assert token not in text, "%s contains %s" % (path.name, token)


def test_the_runner_declares_itself_research_only():
    text = (REPO_ROOT / "scripts"
            / "run_release37_native_market_data_gate.py").read_text(
                encoding="utf-8").lower()
    flat = " ".join(text.split())
    assert "research only" in flat
    assert "no order" in flat
    assert "spends no money" in flat


def test_the_architecture_audit_passes_for_release37():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import audit_architecture  # noqa: PLC0415

    files = audit_architecture._iter_source_files()
    report = audit_architecture.check_release37_native_market_data_gate(files)
    assert report["modules_missing"] == []
    assert report["second_owner_modules"] == []
    assert report["forbidden_calls"] == []
    assert report["forbidden_owner_refs"] == []
    assert report["defines_no_second_gate"] is True
    assert report["spending_refused"] is True
    assert report["alpha_result_is_not_tested"] is True
