"""Release 38 regression - native futures information frontier.

Pins the invariants Release 38 must never lose:

* the provider-call taxonomy - a programmer/API error can never again be
  classified as an entitlement limitation (the '&ES' near-miss);
* the frozen research contract - configurations, roll policy, costs,
  vocabularies, result separation;
* ownership refusals - no second acquisition gate, no second coverage
  authority, no purchase or renewal authority anywhere in the release;
* the verdict rule - ALPHA_RESULT may be PASS only alongside the qualified
  verdict, and never while the entitlement is unsynchronized;
* commercial safety - every flag False, zero spend, renewal never authorised.

Hermetic: no test here touches the Norgate Data Updater, the network, or the
research root.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from paper_trader.alpha_agent import r38
from paper_trader.alpha_agent.r38 import contract as C
from paper_trader.alpha_agent.r38 import entitlement as E
from paper_trader.alpha_agent.r38 import enumeration as EN
from paper_trader.alpha_agent.r38 import campaign as CAMP
from paper_trader.alpha_agent.r38 import experiments as EX
from paper_trader.alpha_agent.r38 import ml_contract as ML
from paper_trader.alpha_agent.r38 import research_layer as RL
from paper_trader.alpha_agent.r38 import steele as ST

REPO = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# The provider-call taxonomy - the release's mandated regression
# --------------------------------------------------------------------------- #
class TestProviderCallTaxonomy:
    SESSIONS = ["ES", "CL"]
    CONTINUOUS = ["&ES"]

    def _classify(self, symbol, outcome, *, dated=False):
        return E.classify_session_contracts_call(
            symbol, outcome,
            delivered_session_symbols=self.SESSIONS,
            continuous_database_symbols=self.CONTINUOUS,
            dated_database_present=dated)

    def test_continuous_symbol_raising_is_parameter_error_not_entitlement(self):
        outcome = {"ok": False, "error_type": "ValueError"}
        assert self._classify("&ES", outcome, dated=False) \
            == C.CALL_PARAMETER_ERROR

    def test_continuous_symbol_empty_answer_is_still_parameter_error(self):
        outcome = {"ok": True, "value": []}
        assert self._classify("&ES", outcome, dated=True) \
            == C.CALL_PARAMETER_ERROR

    def test_valid_session_raising_without_dated_db_is_entitlement_error(self):
        outcome = {"ok": False, "error_type": "ValueError"}
        assert self._classify("ES", outcome, dated=False) \
            == C.CALL_ENTITLEMENT_ERROR

    def test_valid_session_raising_with_dated_db_is_other_provider_error(self):
        outcome = {"ok": False, "error_type": "ValueError"}
        assert self._classify("ES", outcome, dated=True) \
            == C.CALL_OTHER_PROVIDER_ERROR

    def test_unknown_symbol_is_unsupported_market(self):
        outcome = {"ok": False, "error_type": "ValueError"}
        assert self._classify("ZZTOP", outcome, dated=True) \
            == C.CALL_UNSUPPORTED_MARKET

    def test_data_answer_is_valid_request(self):
        outcome = {"ok": True, "value": ["ES-2026U", "ES-2026Z"]}
        assert self._classify("ES", outcome, dated=True) \
            == C.CALL_VALID_WITH_DATA

    def test_empty_answer_on_valid_session_is_empty_history(self):
        outcome = {"ok": True, "value": []}
        assert self._classify("ES", outcome, dated=True) \
            == C.CALL_EMPTY_HISTORY

    def test_vocabulary_is_exactly_the_six_states(self):
        assert C.PROVIDER_CALL_CLASSIFICATION_VOCAB == (
            "VALID_REQUEST_WITH_DATA", "PARAMETER_ERROR",
            "ENTITLEMENT_ERROR", "EMPTY_HISTORY", "UNSUPPORTED_MARKET",
            "OTHER_PROVIDER_ERROR")
        assert C.A_PROGRAMMER_ERROR_IS_NOT_AN_ENTITLEMENT_LIMITATION is True


# --------------------------------------------------------------------------- #
# Frozen research contract
# --------------------------------------------------------------------------- #
class TestFrozenContract:
    def test_thirteen_primary_configurations_under_the_ceiling(self):
        assert C.FROZEN_PRIMARY_COUNT == 13
        assert C.FROZEN_PRIMARY_COUNT == len(C.FROZEN_PRIMARY_CONFIGURATIONS)
        assert C.FROZEN_PRIMARY_COUNT <= C.CONFIGURATION_CEILING
        assert C.NO_OPTIMIZER_CAMPAIGN and C.NO_GENETIC_SEARCH
        assert C.NO_RESULT_DRIVEN_EXPANSION
        assert C.DENOMINATOR_COUNTS_ALL_EXECUTED is True

    def test_every_configuration_carries_a_declared_control(self):
        controls = {C.CONTROL_PASSIVE_ROLL_BASKET, C.CONTROL_RISK_MATCHED_CASH}
        for cfg in C.FROZEN_PRIMARY_CONFIGURATIONS:
            assert cfg["control"] in controls, cfg["name"]
            assert cfg["cadence_sessions"] in (5, 21), cfg["name"]

    def test_roll_policy_is_observable_and_never_searched(self):
        assert C.ROLL_POLICY == "OBSERVABLE_FIRST_NOTICE_LAST_TRADE"
        assert C.NO_ROLL_RULE_SEARCH is True
        assert C.ROLL_RULE_MAY_REFERENCE_OUTCOMES is False
        assert C.NO_HINDSIGHT_ROLL is True
        assert C.NO_SILENT_CONTINUOUS_SUBSTITUTION is True

    def test_every_market_group_has_a_priced_cost_group(self):
        for market, (_, _, cost_group) in C.MARKET_GROUPS.items():
            assert cost_group in C.COST_BPS_PER_SIDE, market
        assert C.COST_MODEL_STATE == "MODELLED_NOT_OBSERVED"
        assert C.COST_BASE == "TRADED_NOTIONAL"

    def test_duplicate_exclusions_reference_declared_markets(self):
        for micro, parent in C.DUPLICATE_UNDERLYING_EXCLUSIONS.items():
            assert micro in C.MARKET_GROUPS, micro
            assert parent in C.MARKET_GROUPS, parent

    def test_cell_recomputation_vocabulary(self):
        assert C.CELL_RECOMPUTATION_VOCAB == (
            "NATIVE_DATA_VERIFIED_RESEARCHABLE", "PARTIALLY_UNLOCKED",
            "PROXY_ONLY_REMAINS", "STILL_BLOCKED_ENTITLEMENT",
            "STILL_BLOCKED_HISTORY", "STILL_BLOCKED_METADATA",
            "STILL_BLOCKED_PIT", "STILL_BLOCKED_SURVIVORSHIP",
            "NOT_ECONOMICALLY_APPLICABLE")

    def test_expectation_is_not_measurement(self):
        assert C.EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS is True
        assert C.TRUTH_WINS_OVER_EXPECTATION is True
        assert C.R37_EXPECTED_FULL_UNLOCK_CELLS_FOR_CROSS_CHECK == 53

    def test_result_axes_are_never_collapsed(self):
        assert C.RESULT_AXES == (
            "SYSTEM_RESULT", "DATA_ENTITLEMENT_RESULT",
            "DATA_CAPABILITY_RESULT", "RESEARCH_CANDIDATE_RESULT",
            "ALPHA_RESULT", "POST_ACQUISITION_VALUE_RESULT")
        assert C.A_WORKING_PIPELINE_IS_NOT_ALPHA is True
        assert C.HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE is True
        assert C.A_RENEWAL_RECOMMENDATION_IS_NOT_AUTOMATIC_RENEWAL is True


# --------------------------------------------------------------------------- #
# Ownership refusals
# --------------------------------------------------------------------------- #
class TestOwnershipRefusals:
    def test_no_second_gate_and_no_second_coverage_authority(self):
        assert not (REPO / "alpha_agent" / "r38" / "purchase_gate.py").exists()
        assert not (REPO / "alpha_agent" / "r38" / "coverage.py").exists()
        assert C.R38_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY is False
        assert C.R38_DEFINES_ITS_OWN_COVERAGE_AUTHORITY is False
        assert C.ACQUISITION_DECISION_OWNER \
            == "engine.data_expansion_gate (POST_ACQUISITION_VALUE)"
        assert C.COVERAGE_MATRIX_OWNER == "alpha_agent.r36.coverage"
        assert C.UNLOCK_EXPECTATION_OWNER == "alpha_agent.r37.unlock"

    def test_campaign_delegates_to_the_canonical_gate(self):
        src = (REPO / "alpha_agent" / "r38" / "campaign.py").read_text(
            encoding="utf-8")
        assert "_slice9.run_evaluation(" in src
        assert "CONTEXT_POST_ACQUISITION_VALUE" in src
        assert '"persisted_to_slice9_store": False' in src

    def test_purchase_and_renewal_authority_do_not_exist(self):
        assert C.PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE is False
        assert C.RENEWAL_AUTHORITY_GRANTED_BY_THIS_RELEASE is False
        authority = C.purchase_authority()
        assert authority["purchase_authorised"] is False
        assert authority["renewal_authorised"] is False

    def test_commercial_safety_flags_are_all_false(self):
        for flag in ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
                     "MAY_CREATE_PROVIDER_ACCOUNT",
                     "MAY_CHANGE_SUBSCRIPTION_TIER", "MAY_RENEW_SUBSCRIPTION",
                     "MAY_ACCEPT_LICENCE_AGREEMENT",
                     "MAY_SUBMIT_PAYMENT_DETAILS",
                     "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
                     "MAY_DOWNLOAD_MODEL_WEIGHTS",
                     "MAY_UPGRADE_NORGATE_PACKAGES"):
            assert getattr(C, flag) is False, flag
        assert C.MONEY_SPENT_BY_R38_USD == 0.0
        assert C.PURCHASE_MADE_BY_THIS_RELEASE is False
        assert C.RENEWAL_DECIDED_BY_THIS_RELEASE is False
        assert C.INHERITED_PURCHASE["purchased_by_release38"] is False

    def test_safety_block_flags_are_all_false(self):
        block = r38.safety_block()
        for key, value in block.items():
            if key == "safety":
                continue
            assert value is False, key
        assert "renews_subscription" in block


# --------------------------------------------------------------------------- #
# Verdict rules
# --------------------------------------------------------------------------- #
def _verdict(sync_state, *, survivors, qualified):
    outcome = {
        "rows": [{"name": "X", "executed": True, "economics": {}}],
        "executed_count": 1,
        "not_executed": [],
        "denominator": 1,
        "bh": {"q": 0.10, "threshold": None, "n_rejected": 0, "rejected": []},
        "positive_survivors": (
            [{"name": "X", "p_value": 0.001, "direction": "POSITIVE"}]
            if survivors else []),
    }
    qualification = ({"configuration": "X", "conditions": {},
                      "qualified": True} if qualified else None)
    registry = {"total_futures_markets": 105,
                "total_dated_contracts_primary_sessions": 23805,
                "total_dated_contracts_distinct": 27357,
                "markets_by_exchange": {"CME": 28}}
    actual = {"expected_full_unlocks_r37": 53,
              "r38_actual_native_verified": 0,
              "r38_actual_partially_unlocked": 0}
    return CAMP.build_verdict(
        outcome=outcome, qualification=qualification,
        entitlement_body={"sync_state": sync_state}, registry=registry,
        actual_unlocks=actual, quality={"states": {"PASS": 30}},
        gate={"state": "CANDIDATE"}, ml_body={},
        campaign_id="r38_test")


class TestVerdictRules:
    def test_alpha_pass_requires_the_qualified_verdict(self):
        body = _verdict(C.SYNC_SYNCHRONIZED, survivors=True, qualified=True)
        assert body["verdict"] == C.VERDICT_QUALIFIED
        assert body["ALPHA_RESULT"] == "PASS"

    def test_survivor_without_qualification_is_not_alpha(self):
        body = _verdict(C.SYNC_SYNCHRONIZED, survivors=True, qualified=False)
        assert body["verdict"] == C.VERDICT_NO_QUALIFIED_ALPHA
        assert body["ALPHA_RESULT"] == "FAIL"
        assert body["RESEARCH_CANDIDATE_RESULT"] == "PASS"

    def test_no_survivor_is_fail_never_pass(self):
        body = _verdict(C.SYNC_SYNCHRONIZED, survivors=False, qualified=False)
        assert body["ALPHA_RESULT"] == "FAIL"
        assert body["RESEARCH_CANDIDATE_RESULT"] == "FAIL"

    def test_unsynchronized_entitlement_blocks_alpha_even_if_qualified(self):
        body = _verdict(C.SYNC_NOT_SYNCHRONIZED, survivors=True,
                        qualified=True)
        assert body["verdict"] == C.VERDICT_NOT_SYNCED
        assert body["ALPHA_RESULT"] != "PASS"

    def test_verdict_reports_zero_spend_and_no_renewal(self):
        body = _verdict(C.SYNC_SYNCHRONIZED, survivors=False, qualified=False)
        assert body["money_spent_during_r38_usd"] == 0.0
        assert body["new_subscriptions"] == 0
        assert body["renewal_authorised"] is False
        assert body["purchase_authority"]["renewal_authorised"] is False
        assert body["POST_ACQUISITION_VALUE_RESULT"] == "CANDIDATE"


# --------------------------------------------------------------------------- #
# Enumeration and layer mechanics
# --------------------------------------------------------------------------- #
class TestMechanics:
    def test_contract_symbol_parsing(self):
        parsed = EN.parse_contract_symbol("ES-2026U")
        assert parsed["parsed"] and parsed["delivery_year"] == 2026
        assert parsed["delivery_month"] == 9
        assert EN.parse_contract_symbol("&ES")["parsed"] is False

    def test_session_grouping_is_digit_suffix_only(self):
        sessions = ["FDAX", "FDAX9", "ES", "ETH", "YAP", "YAP4", "YAP10"]
        assert EN.sessions_for_market("FDAX", sessions) == ["FDAX", "FDAX9"]
        assert EN.sessions_for_market("ES", sessions) == ["ES"]
        assert EN.sessions_for_market("YAP", sessions) \
            == ["YAP", "YAP4", "YAP10"]

    def test_roll_exit_uses_the_earlier_observable_date(self):
        import pandas as pd
        exit_day = RL.roll_exit_date("2026-06-15", "2026-07-20")
        assert exit_day == pd.Timestamp("2026-06-15") \
            - pd.tseries.offsets.BDay(C.ROLL_FIRST_NOTICE_BUFFER_SESSIONS)
        exit_day = RL.roll_exit_date(None, "2026-07-20")
        assert exit_day == pd.Timestamp("2026-07-20") \
            - pd.tseries.offsets.BDay(C.ROLL_LAST_TRADE_BUFFER_SESSIONS)
        assert RL.roll_exit_date(None, None) is None

    def test_period_returns_respect_the_coverage_floor(self):
        import numpy as np
        import pandas as pd
        dates = pd.bdate_range("2026-01-01", periods=42)
        ret = pd.Series(0.01, index=dates)
        ret.iloc[22:] = np.nan  # second window nearly empty
        daily = pd.DataFrame({"ret": ret})
        decisions = pd.DatetimeIndex([dates[0], dates[21], dates[41]])
        out = RL.period_returns(daily, decisions)
        assert np.isfinite(out.iloc[0])
        assert np.isnan(out.iloc[1])

    def test_xs_thirds_weights_are_balanced_and_unit_gross(self):
        import pandas as pd
        idx = pd.DatetimeIndex(["2026-01-30"])
        cols = list("ABCDEF")
        signals = pd.DataFrame([[1, 2, 3, 4, 5, 6]], index=idx, columns=cols,
                               dtype=float)
        forward = pd.DataFrame([[0.0] * 6], index=idx, columns=cols)
        W = EX.xs_thirds_weights(signals, forward)
        row = W.loc[idx[0]]
        assert abs(row.sum()) < 1e-12
        assert abs(row.abs().sum() - 1.0) < 1e-12
        assert row["F"] > 0 and row["A"] < 0

    def test_cot_mapping_only_names_declared_markets(self):
        for market in EX.COT_CODE_MAPPING:
            assert market in C.MARKET_GROUPS, market
            assert C.MARKET_GROUPS[market][0] == "COMMODITY", market


# --------------------------------------------------------------------------- #
# Steele and ML artifacts
# --------------------------------------------------------------------------- #
class TestParallelArtifacts:
    def test_steele_sample_is_schema_validation_only(self):
        assert ST.SAMPLE_PURPOSE == "SCHEMA_AND_PIT_VALIDATION_ONLY"
        assert ST.SAMPLE_IS_ALPHA_EVIDENCE is False
        tickers = [t["ticker"] for t in ST.PROPOSED_TICKERS]
        assert tickers == ["AAPL", "MON", "META", "HTZ", "CALM"]
        message = ST.operator_message()
        for ticker in tickers:
            assert ticker in message
        assert "point-in-time validation only" in message

    def test_ml_contract_trains_nothing_and_partitions_chronologically(self):
        assert ML.TRAINS_A_MODEL is False
        assert abs(sum(ML.PARTITION_SHARES) - 1.0) < 1e-12
        for column in ("fwd_return", "decision_date", "control_fwd_return",
                       "partition", "cost_bps_per_side", "has_cot"):
            assert column in ML.PANEL_COLUMNS, column

    def test_artifact_paths_stay_under_the_research_root(self):
        root = str(r38.research_root())
        from paper_trader.alpha_agent.r38 import quality as QQ
        from paper_trader.alpha_agent.r38 import steele as SS
        from paper_trader.alpha_agent.r38 import unlock_actual as UA
        for path in (C.path_for(), E.path_for(), EN.market_path(),
                     EN.contract_path(), QQ.path_for(), UA.path_for(),
                     UA.coverage_path(), SS.path_for(), ML.path_for()):
            assert str(path).startswith(root), path
