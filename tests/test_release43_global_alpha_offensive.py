"""Release 43 - Global Alpha Offensive: the blocking regression.

These tests protect the four things a later release could quietly break:

* the FROZEN CONTRACT and the inherited SEARCH BURDEN (230 + 59 = 289,
  re-derived from the R41 ledger's bytes, never typed from memory);
* the UNIVERSAL JUDGE's exact equivalence to both prior conventions, which
  is what makes R41-, R42- and R43-era numbers comparable at all;
* the CAUSALITY of every signal construction - the look-ahead that produced
  a t-statistic of -50 in the equity lane must not be able to come back;
* the SAFETY boundary - research only, $0, no promotion, no operational or
  prior-release write.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.r43 import burden as B
from alpha_agent.r43 import closeout as CL
from alpha_agent.r43 import contract as C
from alpha_agent.r43 import judge as J
from alpha_agent.r43 import panels as P

pytestmark = pytest.mark.filterwarnings("ignore")


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_declares_research_only_and_zero_spend():
    assert C.RESEARCH_ONLY is True
    for flag in ("MAY_CREATE_ORDER", "MAY_CREATE_PAPER_ORDER",
                 "MAY_CHANGE_HOLDINGS", "MAY_PROMOTE_MODEL",
                 "MAY_ACTIVATE_SLEEVE", "MAY_MODIFY_PRODUCTION_SCHEDULER",
                 "MAY_RESTART_PRODUCTION", "MAY_CONNECT_BROKER",
                 "MAY_SPEND_MONEY", "MAY_PURCHASE_DATA",
                 "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_ACCEPT_LICENCE_AGREEMENT",
                 "MAY_SUBMIT_PAYMENT_DETAILS", "MAY_START_PROVIDER_TRIAL",
                 "MAY_PURCHASE_COMPUTE", "MAY_MUTATE_OPERATIONAL_STORE",
                 "MAY_MUTATE_PRIOR_RELEASE_ARTIFACT"):
        assert getattr(C, flag) is False, flag


def test_contract_is_not_another_single_family_release():
    assert C.IS_ANOTHER_CRYPTO_RELEASE is False
    assert C.IS_ANOTHER_CARRY_RELEASE is False
    assert C.CENTRED_ON_ONE_STRATEGY_FAMILY is False
    assert len(C.LANES) >= 12


def test_every_lane_has_a_frozen_cap_and_advance_rule():
    for name, spec in C.LANES.items():
        assert spec["cap"] >= 1, name
        assert spec["advance_t"] >= 1.0, name
        assert spec["family"] in C.BURDEN_FAMILIES, name
        assert spec["question"], name
    assert C.TOTAL_ZONE_B_BUDGET == sum(v["cap"] for v in C.LANES.values())


def test_collateral_classes_are_declared_and_exhaustive():
    for expr, cls in C.PRIMARY_COLLATERAL_BY_EXPRESSION.items():
        assert cls in C.COLLATERAL_CLASSES, expr
    rho = {k: v["collateral_earns_rf"]
           for k, v in C.COLLATERAL_CLASSES.items()}
    # The whole Release-43 correction depends on these three numbers.
    assert rho["UNREMUNERATED_FULLY_FUNDED"] == 0.0
    assert rho["REMUNERATED_MARGIN"] == 1.0
    assert 0.0 < rho["FUNDED_LONG_SHORT_EQUITY"] < 1.0
    assert C.COLLATERAL_CHOICE_IS_PREDECLARED is True


def test_placebo_levels_are_distinct_from_named_levels():
    assert not (set(C.FIB_NAMED_LEVELS) & set(C.FIB_PLACEBO_LEVELS))
    assert len(C.FIB_PLACEBO_LEVELS) == len(C.FIB_NAMED_LEVELS)


def test_kill_battery_is_declared_before_results():
    assert C.KILL_TESTS_ARE_CHOSEN_BEFORE_RESULTS is True
    assert C.DO_NOT_PROTECT_PROMISING_RESULTS is True
    for t in ("COLLATERAL_REMUNERATION_ZERO", "CAPITAL_HURDLE_X2",
              "ALTERNATIVE_ECONOMIC_CONTROL", "PARAMETER_NEIGHBOURHOOD",
              "PLACEBO_FEATURE", "FACTOR_RESIDUALISATION"):
        assert t in C.ALPHA_KILLER_TESTS, t


def test_blocker_vocabulary_covers_every_branch_state():
    assert C.BRANCH_STATES[0] == "EXECUTED"
    assert set(C.BLOCKER_VOCAB).issubset(set(C.BRANCH_STATES))
    assert C.NO_ALPHA_FOUND_IS_NOT_A_GLOBAL_STOP is True


# --------------------------------------------------------------------------- #
# Search burden
# --------------------------------------------------------------------------- #
def test_inherited_burden_is_verified_from_the_r41_ledger_bytes():
    got = B.verify_inherited()
    assert got["verified"] is True
    assert got["pre_r41_effective_trials"] == 230
    assert got["r41_distinct_zone_b_candidates"] == 59
    assert got["inherited_global_cumulative"] == 289
    assert got["read_only"] is True


def test_burden_never_resets_and_reports_both_denominators():
    s = B.summary()
    assert s["never_reset"] is True
    assert s["r41_ledger_mutated"] is False
    assert s["global_inherited"] == 289
    assert s["global_cumulative"] >= 289
    assert s["global_cumulative"] == 289 + s["r43_distinct_zone_b_candidates"]
    for fam, n in s["cumulative_family_counts"].items():
        assert n >= C.INHERITED_FAMILY_COUNTS.get(fam, 0), fam


def test_r41_ledger_is_not_writable_through_r43():
    src = Path("alpha_agent/r43/burden.py").read_text(encoding="utf-8")
    assert "R41_LEDGER" in src
    # The only use of the R41 ledger path is a read.
    for line in src.splitlines():
        if "R41_LEDGER" in line and "write" in line.lower():
            pytest.fail("R43 must never write the R41 ledger: %r" % line)


def test_lane_cap_is_enforced():
    lane = "M_CREDIT"
    cap = C.LANES[lane]["cap"]
    assert cap >= 1
    # A spec beyond the cap must raise rather than silently overspend.
    src = Path("alpha_agent/r43/burden.py").read_text(encoding="utf-8")
    assert "has exhausted its FROZEN ZONE_B cap" in src


# --------------------------------------------------------------------------- #
# The universal judge - the two exact equivalences
# --------------------------------------------------------------------------- #
def _toy():
    idx = pd.date_range("2020-01-01", periods=600, freq="B")
    rng = np.random.default_rng(43)
    gross = pd.Series(rng.normal(0.0002, 0.004, len(idx)), index=idx)
    pos = pd.Series(np.sign(rng.normal(size=len(idx))), index=idx)
    cost = pd.Series(np.abs(pos.diff().fillna(0.0)) * 0.0005, index=idx)
    return gross, pos, cost


def test_judge_reproduces_the_r41_convention_exactly():
    gross, pos, cost = _toy()
    bk = J.implementable_book(gross, pos, committed_capital=1.0,
                              collateral_class="REMUNERATED_MARGIN",
                              cost=cost)
    # K = 1, rho = 1: per-notional return against a ZERO control.
    assert float(np.nanmax(np.abs(bk["benchmark"]))) == 0.0
    assert float(np.nanmax(np.abs(bk["excess"]
                                  - bk["pnl_on_notional"]))) == 0.0


def test_judge_reproduces_the_r42_convention_exactly():
    """The decisive equivalence: fed the crypto panel R42 judged, the R43
    judge must reproduce alpha_agent.r42.capital term for term."""
    r42cap = pytest.importorskip("alpha_agent.r42.capital")
    from alpha_agent.r42 import contract as C42
    from alpha_agent.r42 import execution as EX
    from alpha_agent.r42 import pnl_audit as PA
    try:
        df = PA.r41_panel("BTCUSDT")
    except Exception:
        pytest.skip("R42 crypto panel not present on this machine")
    sig = df["signal"]
    K = C42.CAPITAL_MODELS["CONSERVATIVE_COLLATERAL"]["denominator"]
    a = r42cap.implementable_book(
        df, sig, capital_model="CONSERVATIVE_COLLATERAL",
        execution_model=C42.PRIMARY_EXECUTION_MODEL, charge_financing=True)
    b = J.implementable_book(
        (df["funding"] + df["basis_ret"]), sig.shift(1),
        committed_capital=K,
        collateral_class="UNREMUNERATED_FULLY_FUNDED",
        cost=EX.cost_stream(sig.diff().abs(), C42.PRIMARY_EXECUTION_MODEL,
                            1.0),
        day_count=365.0)
    for col in ("gross", "pnl_on_notional", "pnl_on_capital", "benchmark",
                "excess"):
        worst = float(np.nanmax(np.abs(a[col].to_numpy(float)
                                       - b[col].to_numpy(float))))
        assert worst == 0.0, "%s differs by %r" % (col, worst)


def test_judge_risk_free_convention_is_inherited_not_invented():
    from alpha_agent.r42 import contract as C42
    assert tuple(J.RISK_FREE_SERIES_PREFERENCE) == \
        tuple(C42.RISK_FREE_SERIES_PREFERENCE)


def test_judge_rejects_an_undeclared_collateral_class():
    gross, pos, _ = _toy()
    with pytest.raises(ValueError):
        J.implementable_book(gross, pos, committed_capital=1.0,
                             collateral_class="MADE_UP_CLASS")


def test_capital_rescale_leaves_the_t_statistic_unchanged():
    """The formal statement of why R42's kill does not transfer."""
    gross, pos, cost = _toy()
    cards = []
    for K in (1.0, 0.05, 0.5):
        bk = J.implementable_book(gross, pos, committed_capital=K,
                                  collateral_class="REMUNERATED_MARGIN",
                                  cost=cost)
        cards.append(J.score(bk))
    ts = [c["excess_t_hac"] for c in cards]
    sharpes = [c["sharpe"] for c in cards]
    assert max(abs(t - ts[0]) for t in ts) < 1e-9
    assert max(abs(s - sharpes[0]) for s in sharpes) < 1e-9
    # ... and the LEVEL does change, which is the whole point.
    assert cards[1]["excess_ann"] != pytest.approx(cards[0]["excess_ann"])


def test_risk_sized_capital_is_measured_on_the_fitting_zone_only():
    gross, pos, cost = _toy()
    idx = gross.index
    fit = idx[:300]
    out = J.risk_sized_capital(gross, fit, floor=0.05)
    assert out["measured_on"] == "FITTING_ZONE_ONLY"
    assert out["post_freeze_disclosed"] is True
    # Feeding it the judged zone instead must change the answer, proving it
    # really only used the dates it was given.
    other = J.risk_sized_capital(gross * 3.0, fit, floor=0.05)
    assert other["book_vol_ann_on_notional"] > out[
        "book_vol_ann_on_notional"]


def test_futures_capital_is_never_below_the_declared_floor():
    tab = J.capital_table(["TREASURY_FUTURES", "TREASURY_FUTURES"],
                          [1.0, 1.0])
    assert tab["COMMITTED_MARGIN_X2"] >= C.MARGIN_FLOOR_FRACTION_OF_GROSS
    assert tab["GROSS_EXPOSURE"] >= tab["COMMITTED_MARGIN_X2"]
    assert tab["TRADED_NOTIONAL"] == 1.0


# --------------------------------------------------------------------------- #
# Causality - the bugs this release found must stay fixed
# --------------------------------------------------------------------------- #
def test_expression_never_uses_the_same_day_signal():
    from alpha_agent.r43.rv import apply_expression
    idx = pd.date_range("2020-01-01", periods=200, freq="B")
    z = pd.Series(np.zeros(len(idx)), index=idx)
    z.iloc[100] = 9.0                      # one huge signal on one day
    for expr in ("CONTINUOUS", "BAND_15_05", "BAND_20_10"):
        pos = apply_expression(z, expr)
        assert pos.iloc[100] == 0.0, expr   # not tradable on its own day
        assert pos.iloc[101] != 0.0, expr   # tradable on the next


def test_extra_lag_delays_the_position_by_one_more_session():
    from alpha_agent.r43.rv import apply_expression
    idx = pd.date_range("2020-01-01", periods=200, freq="B")
    z = pd.Series(np.zeros(len(idx)), index=idx)
    z.iloc[100] = 9.0
    assert apply_expression(z, "CONTINUOUS", extra_lag=1).iloc[101] == 0.0
    assert apply_expression(z, "CONTINUOUS", extra_lag=1).iloc[102] != 0.0


def test_equity_decile_selection_uses_the_lagged_signal():
    """The look-ahead that produced -304%/yr at t -50 before it was found:
    ranking the cross-section on TODAY's signal while holding YESTERDAY's
    sign selects the names that moved today."""
    src = Path("alpha_agent/r43/equity.py").read_text(encoding="utf-8")
    assert "zl = z.shift(1 + int(extra_lag))" in src
    assert "rank = zl.rank(axis=1, pct=True)" in src
    assert "rank = z.rank(axis=1, pct=True)" not in src


def test_equity_returns_are_not_padded_across_gaps():
    src = Path("alpha_agent/r43/equity.py").read_text(encoding="utf-8")
    assert "close.pct_change()" not in src
    assert "close.notna() & prev.notna()" in src


def test_causal_pivots_are_never_reported_before_confirmation():
    from alpha_agent.r43.crossasset import _causal_pivots, CONFIRM
    idx = pd.date_range("2020-01-01", periods=400, freq="B")
    v = np.ones(len(idx))
    v[200] = 5.0                            # a unique, unmistakable high
    px = pd.Series(v, index=idx)
    hi, _ = _causal_pivots(px, confirm=CONFIRM)
    assert not (hi.iloc[:200 + CONFIRM] == 5.0).any()
    assert hi.iloc[200 + CONFIRM] == 5.0


def test_risk_scaling_target_is_expanding_not_full_sample():
    src = Path("alpha_agent/r43/rv.py").read_text(encoding="utf-8")
    assert "v.expanding(min_periods=250).median()" in src
    assert "np.nanmedian(v)" not in src


def test_carry_signal_level_is_not_demeaned():
    from alpha_agent.r43 import rv as RV
    assert RV.CARRY_Z_IS_DEMEANED is False
    idx = pd.date_range("2015-01-01", periods=900, freq="B")
    x = pd.Series(np.full(len(idx), 0.05), index=idx)   # constant POSITIVE
    z = RV._scale_only(x)
    # A de-meaning z-score would send a constant positive carry to zero or
    # NaN; the level construction must keep its sign.
    assert z.dropna().empty or (z.dropna() >= 0).all()


# --------------------------------------------------------------------------- #
# Panels / provenance
# --------------------------------------------------------------------------- #
def test_panels_are_read_only_against_prior_release_roots():
    src = Path("alpha_agent/r43/panels.py").read_text(encoding="utf-8")
    for bad in ("to_csv(", "write_text(", "to_pickle(", "mkdir("):
        assert bad not in src, bad


def test_owned_inventory_is_measured_not_asserted():
    inv = P.inventory()
    assert inv["futures_markets"] > 50
    assert inv["all_read_only"] is True
    assert inv["written_to_prior_release_roots"] is False
    assert set(inv["futures_by_asset_class"]) >= {"RATES", "COMMODITY", "FX"}


def test_declared_margin_is_compared_against_the_exchange_table():
    ms = P.margin_sanity()
    assert ms["pit_state"] == "CURRENT_ONLY_NOT_POINT_IN_TIME"
    assert ms["n_markets_compared"] > 50
    # The comparison must be REPORTED, whatever it says - the release does
    # not get to hide a market where its declared fraction is optimistic.
    assert 0.0 <= ms["fraction_conservative"] <= 1.0


# --------------------------------------------------------------------------- #
# Acquisition - keys never leak, nothing is bought
# --------------------------------------------------------------------------- #
def test_acquisition_redacts_api_keys():
    from alpha_agent.r43 import acquisition as AQ
    u = AQ._redact("https://x.test/v1/thing?apiKey=SUPERSECRET&limit=3")
    assert "SUPERSECRET" not in u
    assert "REDACTED" in u


def test_acquisition_never_purchases_or_accepts_anything():
    from alpha_agent.r43 import acquisition as AQ
    src = Path("alpha_agent/r43/acquisition.py").read_text(encoding="utf-8")
    for bad in ("urlopen(req, data=", "method=\"POST\"", "'POST'", '"POST"'):
        assert bad not in src, bad
    assert AQ.POLYGON_RPM_SLEEP >= 12.0     # respects the measured limit


def test_shell_policy_disclosures_are_never_erased():
    assert C.WINDOWS_POWERSHELL_ONLY is True
    prior = {d["release"] for d in C.INHERITED_SHELL_POLICY_DISCLOSURES}
    assert "release42" in prior


# --------------------------------------------------------------------------- #
# Frontier / freeze
# --------------------------------------------------------------------------- #
def test_zone_c_pregate_and_single_access():
    from alpha_agent.r43 import frontier as FR
    below = FR.may_open_zone_c("c43_test_below", C.ZONE_C_PREGATE_T - 0.01)
    assert below["may_open"] is False
    above = FR.may_open_zone_c("c43_test_above_unique", 9.99)
    assert above["eligible"] is True


def test_freeze_requires_every_declared_check():
    req = C.FREEZE_REQUIRES
    assert req["zone_b_excess_t_hac_min"] >= 2.0
    assert req["positive_on_committed_capital"] is True
    assert req["survives_full_kill_battery"] is True
    assert C.MAX_NEW_SHADOWS <= 4
    assert C.PROMOTION_ALLOWED is False
    assert C.NEVER_BACKFILL_PROSPECTIVE_ROWS is True


def test_economic_value_penalises_a_signal_with_no_increment():
    from alpha_agent.r43.frontier import _economic_value
    base = {"NET_RESIDUAL_ALPHA": 0.04, "T_STAT": 3.0,
            "ROBUSTNESS": "SURVIVES_FULL_BATTERY"}
    strong = dict(base, _increment_t=3.0)
    weak = dict(base, _increment_t=0.4)
    assert _economic_value(strong) > _economic_value(weak) * 3
    losing = dict(base, NET_RESIDUAL_ALPHA=-0.01)
    assert _economic_value(losing) < 0


def test_placebo_seed_is_reproducible_across_processes():
    """A placebo seeded from Python's randomised hash() flipped this
    release's headline kill verdict between two runs of identical code."""
    from alpha_agent.r43 import killer as K
    src = Path("alpha_agent/r43/killer.py").read_text(encoding="utf-8")
    assert "abs(hash(candidate_id))" not in src
    assert "def _stable_seed" in src
    assert K._stable_seed("c43_example") == K._stable_seed("c43_example")
    assert K._stable_seed("c43_a") != K._stable_seed("c43_b")
    # A 40-draw p95 was itself noise for a candidate near the threshold.
    assert K.PLACEBO_DRAWS >= 200


def test_unmeasured_control_does_not_outrank_a_measured_one():
    """Scoring 'no increment measured' as a pass rewarded candidates for not
    having been attacked, and put a gate-rejected book above the survivor."""
    from alpha_agent.r43.frontier import _economic_value
    base = {"NET_RESIDUAL_ALPHA": 0.04, "T_STAT": 3.0,
            "ROBUSTNESS": "SURVIVES_FULL_BATTERY",
            "QUALIFICATION_STATE": "QUALIFIED_ALPHA_CANDIDATE"}
    measured_strong = dict(base, _increment_t=3.0)
    unmeasured = dict(base, _increment_t=None)
    measured_weak = dict(base, _increment_t=0.4)
    assert _economic_value(measured_strong) > _economic_value(unmeasured)
    assert _economic_value(unmeasured) > _economic_value(measured_weak)


def test_qualification_state_dominates_raw_return_in_the_ranking():
    from alpha_agent.r43.frontier import _economic_value
    survivor = {"NET_RESIDUAL_ALPHA": 0.04, "T_STAT": 2.7,
                "ROBUSTNESS": "SURVIVES_FULL_BATTERY", "_increment_t": 0.4,
                "QUALIFICATION_STATE": "RESEARCH_CANDIDATE_ZONE_C_NOT_OPENED"}
    bigger_but_rejected = {
        "NET_RESIDUAL_ALPHA": 0.31, "T_STAT": 2.1,
        "ROBUSTNESS": "NOT_ATTACKED", "_increment_t": None,
        "QUALIFICATION_STATE": "REJECTED_AT_RESEARCH_CANDIDATE_GATE"}
    assert _economic_value(survivor) > _economic_value(bigger_but_rejected)


def test_partial_battery_is_not_reported_as_a_kill():
    from alpha_agent.r43.frontier import _robustness
    assert _robustness({}) == "NOT_ATTACKED"
    assert _robustness({"state": "PARTIAL", "survives": False}) == \
        "PARTIAL_BATTERY_ONLY"
    assert _robustness({"survives": True}) == "SURVIVES_FULL_BATTERY"
    assert _robustness({"survives": False, "killed_by": ["COST_X2"]}) == \
        "KILLED_BY_COST_X2"


def test_zone_c_refutation_outranks_every_other_interpretation():
    from alpha_agent.r43.frontier import _qualification
    a = {"gate": {"passes": True},
         "zone_b": {"excess_ann": 0.04},
         "zone_c": {"excess_ann": -0.002, "excess_t_hac": -0.16}}
    assert _qualification(a, {}, {"increment_t_hac": 0.4}) == \
        "REFUTED_ON_ZONE_C_SIGN_FLIP"


def test_zone_c_result_survives_a_rerun_without_a_second_access():
    """One access per lineage - but the RECORDED result must still be
    reported, or a re-run turns a refuted candidate into an untested one."""
    src = Path("alpha_agent/r43/campaign.py").read_text(encoding="utf-8")
    assert "zone_c_from_recorded_access" in src
    assert "recorded_access_reused" in src


def test_terminal_state_cannot_claim_strength_after_a_refutation():
    from alpha_agent.r43 import campaign as CP
    refuted = [{"FORWARD_READY": True, "NET_RESIDUAL_ALPHA": 0.04,
                "QUALIFICATION_STATE": "REFUTED_ON_ZONE_C_SIGN_FLIP",
                "_zone_c": {"excess_ann": -0.002}, "_increment_t": 0.4}]
    assert CP.terminal_state(refuted, {}, {}) == \
        "R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE"
    decoration = [{"FORWARD_READY": True, "NET_RESIDUAL_ALPHA": 0.04,
                   "QUALIFICATION_STATE": "RESEARCH_CANDIDATE_ZONE_C_NOT_"
                                          "OPENED",
                   "_zone_c": None, "_increment_t": 0.4}]
    assert CP.terminal_state(decoration, {}, {}) == \
        "R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE"
    real = [{"FORWARD_READY": True, "NET_RESIDUAL_ALPHA": 0.04,
             "QUALIFICATION_STATE": "RESEARCH_CANDIDATE_ZONE_C_NOT_OPENED",
             "_zone_c": None, "_increment_t": 3.0}]
    assert CP.terminal_state(real, {}, {}) == \
        "R43_STRONG_CANDIDATE_FORWARD_PENDING"


def test_frontier_is_not_ranked_by_sharpe():
    src = Path("alpha_agent/r43/frontier.py").read_text(encoding="utf-8")
    assert "ECONOMIC_VALUE_SCORE" in src
    assert "rows.sort(key=lambda r: -(r[\"ECONOMIC_VALUE_SCORE\"]" in src
    assert C.RANK_BY.startswith("evidence-weighted")


# --------------------------------------------------------------------------- #
# Artifacts, if the campaign has been run on this machine
# --------------------------------------------------------------------------- #
def _artifact(name):
    from alpha_agent.r43 import CAMPAIGN_ID, campaign_dir, read_json
    p = campaign_dir(CAMPAIGN_ID) / name
    if not p.exists():
        pytest.skip("%s not produced on this machine" % name)
    return read_json(p)


def test_frozen_contract_artifact_matches_the_source():
    _artifact("r43_frozen_contract.json")
    got = CL.verify_contract_unchanged()
    assert got["unchanged"] is True, (
        "the contract was edited after it was frozen: %r" % got)


def test_verdict_reports_every_result_axis_and_never_collapses_them():
    v = _artifact("R43_FINAL_VERDICT.json")
    axes = v["result_axes"]
    for axis in C.RESULT_AXES:
        assert axis in axes, axis
    assert v["terminal_state"] in C.TERMINAL_STATES


def test_verdict_answers_all_twenty_questions():
    v = _artifact("R43_FINAL_VERDICT.json")
    assert len(v["twenty_answers"]) == 20


def test_every_declared_lane_terminated():
    v = _artifact("R43_FINAL_VERDICT.json")
    bm = v["branch_matrix"]
    assert bm["every_lane_terminated"] is True
    assert not bm["missing"]
    for lane, row in bm["lanes"].items():
        assert row["state"] in C.BRANCH_STATES, (lane, row["state"])


def test_verdict_safety_block_is_all_zero():
    v = _artifact("R43_FINAL_VERDICT.json")
    s = v["safety"]
    for k in ("money_spent", "accounts_created", "trials_started",
              "licences_accepted", "payment_details_submitted",
              "subscriptions", "cloud_compute_spend", "orders",
              "paper_orders", "broker_connections", "operational_writes",
              "portfolio_mutations", "model_promotions",
              "sleeve_activations", "scheduler_changes",
              "production_restarts", "prior_release_artifacts_mutated"):
        assert s[k] == 0, k
    assert s["research_only"] is True
    assert s["promotion_allowed"] is False


def test_verdict_burden_never_below_the_inheritance():
    v = _artifact("R43_FINAL_VERDICT.json")
    assert v["search_burden"]["global_cumulative"] >= 289
    assert v["search_burden"]["never_reset"] is True


def test_shadow_registry_is_promotion_locked():
    reg = _artifact("r43_shadow_registry.json")
    assert reg["promotion_allowed"] is False
    assert reg["prior_release_shadows_mutated"] is False
    assert reg["n_frozen"] <= C.MAX_NEW_SHADOWS
    for sid, sh in (reg.get("shadows") or {}).items():
        assert sh["promotion_allowed"] is False
        assert sh["state"] == C.SHADOW_STATE
        assert sh["rows_captured"] == 0
        assert "paired_control" in sh, sid


def test_prior_release_witnesses_are_byte_identical():
    """R43 must not have touched R41's or R42's artifacts."""
    before = _artifact("R42_CLOSEOUT_IMPORT.json")["immutable_witnesses_before"]
    now = CL.witness_fingerprint()
    for name, row in before.items():
        if not row.get("exists"):
            continue
        assert now[name]["sha256"] == row["sha256"], name
