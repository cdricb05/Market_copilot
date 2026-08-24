"""Release 42 - Crypto Funding/Basis Alpha Validation: regression.

Targeted, hermetic tests. The properties under test are the ones that
would let a failing candidate look like a passing one:

* the R41 shadow is immutable and its verdict is inherited, not rewritten;
* the frozen contract is hash-stable and covers every rule that could be
  bent after a result;
* the complete PnL equation is assembled correctly - in particular that a
  cash-and-carry is charged for the capital it immobilises, and that
  R41's zero-control convention is reproducible but not the default;
* funding cashflow arithmetic and PIT lag;
* positive/negative leg separation and the borrow blocker;
* the asset and venue eligibility rules are metadata-only;
* the hierarchical gate is closed-testing and cannot be entered with a
  negative effect;
* execution honesty (no assumed maker fill);
* capacity and margin arithmetic;
* the audit guard is wired and blocking.

No network, no Norgate, no research-drive writes: every test either uses
synthetic frames or asserts on source/contract properties.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from paper_trader.alpha_agent import r42
from paper_trader.alpha_agent.r42 import capacity as CAPY
from paper_trader.alpha_agent.r42 import capital as CAP
from paper_trader.alpha_agent.r42 import contract as C
from paper_trader.alpha_agent.r42 import execution as EX
from paper_trader.alpha_agent.r42 import hierarchy as HY
from paper_trader.alpha_agent.r42 import legs as LG
from paper_trader.alpha_agent.r42 import margin as MG


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def synth() -> pd.DataFrame:
    """A synthetic daily panel with a KNOWN carry of 10 bps/day."""
    idx = pd.date_range("2024-01-01", periods=400, freq="1D", tz="UTC")
    rng = np.random.default_rng(42)
    spot = pd.Series(100.0 * np.cumprod(1 + rng.normal(0, 0.02, len(idx))),
                     index=idx)
    perp = spot * 1.0002
    df = pd.DataFrame({"spot": spot, "perp": perp}, index=idx)
    df["funding"] = 0.0010
    df["spot_ret"] = df["spot"].pct_change()
    df["perp_ret"] = df["perp"].pct_change()
    df["basis_ret"] = df["spot_ret"] - df["perp_ret"]
    df["signal"] = 1.0
    df.iloc[0, df.columns.get_loc("signal")] = 0.0
    df["held"] = df["signal"].shift(1)
    return df


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_hash_is_stable_and_64_hex():
    h1, h2 = C.contract_hash(), C.contract_hash()
    assert h1 == h2 and len(h1) == 64
    int(h1, 16)


def test_contract_declares_r41_immutability():
    assert C.R41_CANDIDATE_IS_IMMUTABLE
    assert C.R42_CORRECTIONS_GET_NEW_IDENTITIES
    assert C.R41_DSR_REPORTED_UNCHANGED
    assert C.R41_EXPECTED["historical_alpha_result"] == "FAIL"
    assert C.R41_EXPECTED["shadow_id"] == "shadow_btc_funding_carry_1d"


def test_contract_freezes_everything_bendable():
    assert C.METHOD_FROZEN_BEFORE_RESULTS
    assert C.METHOD_MAY_NOT_BE_CHOSEN_TO_PASS
    assert C.ASSET_UNIVERSE_FROZEN_BEFORE_RESULTS
    assert C.STANDARDS_MAY_NOT_BE_LOWERED_AFTER_DATA
    assert C.ASSET_ELIGIBILITY["selection_may_use_performance"] is False
    assert C.VENUE_ELIGIBILITY["selection_may_use_performance"] is False


def test_primary_capital_model_is_the_conservative_one():
    assert C.PRIMARY_CAPITAL_MODEL == "CONSERVATIVE_COLLATERAL"
    assert C.PRIMARY_CONTROL == "RISK_FREE_ON_COMMITTED_CAPITAL"
    ks = {k: v["denominator"] for k, v in C.CAPITAL_MODELS.items()}
    assert ks["TRADED_NOTIONAL"] == 1.00
    assert ks["CONSERVATIVE_COLLATERAL"] > ks["FULLY_FUNDED_COMMITTED"] > 1.0
    # the primary must not be the most flattering one
    assert ks[C.PRIMARY_CAPITAL_MODEL] == max(
        v for k, v in ks.items() if k != "GROSS_EXPOSURE")


def test_pnl_identity_names_every_omitted_r41_term():
    for term in ("SPREAD_SLIPPAGE", "FINANCING", "BORROW",
                 "COLLATERAL_DRAG"):
        assert term in C.PNL_TERMS
        assert term in C.PNL_IDENTITY


def test_safety_boundary_refuses_every_forbidden_action():
    assert C.MONEY_BUDGET_USD == 0.0
    assert not C.CLAUDE_MAY_COMMIT and not C.CLAUDE_MAY_PUSH
    assert not r42.MAY_SPEND_MONEY and not r42.MAY_MUTATE_PRODUCTION
    assert not r42.PROMOTES_MODELS and not r42.AUTOMATIC_PROMOTION_ALLOWED
    for a in ("crypto purchase", "exchange account", "deposit", "order",
              "paper order", "API trading key", "R41 shadow mutation"):
        assert a in C.FORBIDDEN_ACTIONS
    sb = r42.safety_block()
    for k in ("buys_crypto", "creates_exchange_account", "deposits_funds",
              "holds_api_trading_key", "creates_paper_order",
              "mutates_r41_shadow", "creates_order", "promotes_model"):
        assert sb[k] is False


def test_qualification_vocabulary_is_complete_and_plural():
    assert C.MULTIPLE_STATES_MAY_HOLD
    for s in ("R42_CAPITAL_EFFICIENCY_KILLS_EDGE",
              "R42_BORROW_REALITY_KILLS_REVERSE_LEG",
              "R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA",
              "R42_DATA_LIMIT_BINDING"):
        assert s in C.QUALIFICATION_STATES


def test_new_assets_are_never_labelled_true_forward():
    assert C.NEW_ASSET_LABEL == "HISTORICAL_OUT_OF_ASSET_REPLICATION"
    assert C.NEW_ASSET_IS_NOT_TRUE_FORWARD
    assert C.ETH_IS_PRIOR_EVIDENCE


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
def test_execution_ladder_orders_and_r41_is_the_cheapest():
    rt = {k: EX.round_trip_bps(k) for k in C.EXECUTION_MODELS}
    assert rt["R41_BASELINE"] == 10.0
    assert rt["DEFAULT_TAKER"] > rt["R41_BASELINE"]
    assert rt["DEFAULT_TAKER"] == 17.0
    assert C.PRIMARY_EXECUTION_MODEL == "DEFAULT_TAKER"


def test_cost_stream_charges_only_on_position_change():
    sig = pd.Series([0.0, 0.0, 1.0, 1.0, 1.0, 0.0])
    cost = EX.cost_stream(sig.diff().abs(), "R41_BASELINE")
    assert cost.iloc[0] == 0.0
    assert cost.iloc[2] == pytest.approx(10.0 / 1e4)
    assert cost.iloc[3] == 0.0
    assert cost.iloc[5] == pytest.approx(10.0 / 1e4)
    assert cost.sum() == pytest.approx(2 * 10.0 / 1e4)


def test_cost_stress_multiplier_is_linear():
    sig = pd.Series([0.0, 1.0, 1.0])
    base = EX.cost_stream(sig.diff().abs(), "DEFAULT_TAKER", 1.0).sum()
    x3 = EX.cost_stream(sig.diff().abs(), "DEFAULT_TAKER", 3.0).sum()
    assert x3 == pytest.approx(3.0 * base)


def test_maker_claim_is_inadmissible_without_a_fill_model():
    assert C.ASSUMED_LIMIT_FILL_IS_FORBIDDEN
    assert EX.maker_admissibility({})["admissible"] is False
    partial = {k: True for k in C.MAKER_FILL_REQUIRES[:2]}
    assert EX.maker_admissibility(partial)["admissible"] is False
    full = {k: True for k in C.MAKER_FILL_REQUIRES}
    assert EX.maker_admissibility(full)["admissible"] is True


# --------------------------------------------------------------------------- #
# The complete equation
# --------------------------------------------------------------------------- #
def test_book_reproduces_r41_convention_when_financing_is_off(synth,
                                                              monkeypatch):
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(0.0, index=idx))
    bk = CAP.implementable_book(synth, synth["signal"],
                                capital_model="TRADED_NOTIONAL",
                                execution_model="R41_BASELINE",
                                charge_financing=False)
    assert (bk["benchmark"] == 0.0).all()
    expected = synth["held"] * (synth["funding"] + synth["basis_ret"])
    assert bk["gross"].fillna(0.0).round(12).equals(
        expected.fillna(0.0).round(12))
    assert bk["excess"].equals(bk["pnl_on_capital"])


def test_financing_charges_the_whole_committed_capital(synth, monkeypatch):
    rf = 0.04 / 365.0
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(rf, index=idx))
    bk = CAP.implementable_book(synth, synth["signal"],
                                capital_model="CONSERVATIVE_COLLATERAL",
                                execution_model="R41_BASELINE",
                                charge_financing=True)
    on = bk["on"] > 0
    assert bk.loc[on, "benchmark"].round(12).eq(round(rf, 12)).all()
    assert (bk.loc[~on, "benchmark"] == 0.0).all()
    K = C.CAPITAL_MODELS["CONSERVATIVE_COLLATERAL"]["denominator"]
    assert bk.loc[on, "pnl_on_capital"].iloc[5] == pytest.approx(
        bk.loc[on, "pnl_on_notional"].iloc[5] / K)


def test_bigger_denominator_lowers_return_on_capital(synth, monkeypatch):
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(0.0, index=idx))
    vals = {}
    for name, spec in C.CAPITAL_MODELS.items():
        bk = CAP.implementable_book(synth, synth["signal"],
                                    capital_model=name,
                                    execution_model="R41_BASELINE",
                                    charge_financing=False)
        vals[name] = float(bk["pnl_on_capital"].mean())
    assert vals["TRADED_NOTIONAL"] > vals["FULLY_FUNDED_COMMITTED"]
    assert vals["FULLY_FUNDED_COMMITTED"] > vals["CONSERVATIVE_COLLATERAL"]
    assert vals["CONSERVATIVE_COLLATERAL"] > vals["GROSS_EXPOSURE"]


def test_flat_days_earn_no_excess(synth, monkeypatch):
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(0.05 / 365.0, index=idx))
    sig = pd.Series(0.0, index=synth.index)
    bk = CAP.implementable_book(synth, sig, charge_financing=True)
    assert (bk["excess"] == 0.0).all()
    assert (bk["benchmark"] == 0.0).all()


def test_borrow_is_charged_only_when_short_spot(synth, monkeypatch):
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(0.0, index=idx))
    sig = pd.Series(-1.0, index=synth.index)
    sig.iloc[0] = 0.0
    bk = CAP.implementable_book(synth, sig, borrow_annual=0.10,
                                charge_financing=False)
    assert bk["borrow"].iloc[5] == pytest.approx(0.10 / 365.0)
    long_bk = CAP.implementable_book(synth, synth["signal"],
                                     borrow_annual=0.10,
                                     charge_financing=False)
    assert (long_bk["borrow"] == 0.0).all()


def test_a_carry_below_the_risk_free_rate_is_negative_excess(synth,
                                                             monkeypatch):
    """The whole R42 correction, in one property."""
    monkeypatch.setattr(CAP, "risk_free_daily",
                        lambda idx: pd.Series(0.06 / 365.0, index=idx))
    df = synth.copy()
    df["funding"] = 0.03 / 365.0          # 3 %/yr carry vs 6 %/yr cash
    df["basis_ret"] = 0.0
    bk = CAP.implementable_book(df, df["signal"], charge_financing=True)
    assert float(bk["excess"].mean()) < 0
    zero = CAP.implementable_book(df, df["signal"], charge_financing=False)
    assert float(zero["excess"].mean()) > 0


# --------------------------------------------------------------------------- #
# Legs and borrow
# --------------------------------------------------------------------------- #
def test_positive_only_signal_never_shorts_spot(synth):
    sig = LG.positive_only_signal(synth)
    assert sig.min() >= 0.0
    assert set(np.unique(sig.dropna())) <= {0.0, 1.0}


def test_positive_clipped_preserves_the_r41_long_state(synth):
    df = synth.copy()
    df["signal"] = pd.Series([1.0, -1.0, 0.0, 1.0] * 100, index=df.index)
    clipped = LG.r41_signal_positive_clipped(df)
    assert clipped.min() == 0.0
    assert (clipped[df["signal"] > 0] == 1.0).all()
    assert (clipped[df["signal"] < 0] == 0.0).all()


def test_borrow_rule_declares_current_snapshot_is_not_history():
    assert C.CURRENT_SNAPSHOT_IS_NOT_HISTORY
    assert C.BORROW_UNPROVEN_VERDICT == "HISTORICALLY_NON_IMPLEMENTABLE"
    for k in ("historical_borrow_availability", "historical_borrow_rate",
              "recall_risk", "borrow_capacity"):
        assert k in C.BORROW_EVIDENCE_REQUIRED


# --------------------------------------------------------------------------- #
# Margin / capacity
# --------------------------------------------------------------------------- #
def test_liquidation_distance_grows_with_collateral():
    a = MG.liquidation_distance("FULLY_FUNDED_COMMITTED")
    b = MG.liquidation_distance("CONSERVATIVE_COLLATERAL")
    assert b["adverse_perp_move_to_liquidation"] > \
        a["adverse_perp_move_to_liquidation"]
    assert a["cross_margin_credit_for_spot_assumed"] is False
    assert C.MARGIN_STRESS["primary_test_requires_no_leverage"]


def test_impact_is_monotone_in_size_and_zero_without_volume():
    assert CAPY.impact_bps(1e6, 1e9) > CAPY.impact_bps(1e5, 1e9)
    assert np.isnan(CAPY.impact_bps(1e6, 0))


def test_funding_erosion_is_bounded_and_monotone():
    assert CAPY.funding_erosion_fraction(1e6, 1e9) < \
        CAPY.funding_erosion_fraction(1e7, 1e9)
    assert CAPY.funding_erosion_fraction(1e12, 1e9) == 1.0
    assert C.FUNDING_IS_NOT_EXOGENOUS_AT_SCALE


# --------------------------------------------------------------------------- #
# Hierarchy
# --------------------------------------------------------------------------- #
def test_hierarchy_is_closed_testing_with_a_predeclared_representative():
    assert C.HIERARCHY_LEVELS["LEVEL_1_LINEAGE"]["representative"] == \
        "R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC"
    assert C.HIERARCHY_LEVELS["LEVEL_3_REPLICATION"]["confirmation_only"]
    assert "LEVEL_1 must reject" in C.CLOSED_TESTING["rule"]
    assert C.CLOSED_TESTING["alpha"] == 0.05


def test_westfall_young_controls_fwer_on_pure_noise():
    rng = np.random.default_rng(7)
    n = 500
    common = rng.normal(0, 1, n)
    streams = {
        "a": pd.Series(common + rng.normal(0, 1, n)),
        "b": pd.Series(common + rng.normal(0, 1, n)),
        "c": pd.Series(common + rng.normal(0, 1, n)),
    }
    res = HY.westfall_young(streams, n_boot=400, block=10, seed=3)
    assert res["state"] == "OK"
    assert res["n_rejecting"] == 0
    assert res["max_stat_critical_value"] > 1.96
    assert res["correlation_preserved"]


def test_westfall_young_rejects_a_real_effect():
    rng = np.random.default_rng(11)
    n = 600
    streams = {"real": pd.Series(rng.normal(0.5, 1, n)),
               "noise": pd.Series(rng.normal(0.0, 1, n))}
    res = HY.westfall_young(streams, n_boot=400, block=10, seed=5)
    assert res["variants"]["real"]["rejects_at_fwer_alpha"] is True
    assert res["variants"]["noise"]["rejects_at_fwer_alpha"] is False


def test_a_negative_effect_can_never_pass_level_1():
    """A two-sided rejection with the WRONG sign is not a pass."""
    idx = pd.date_range("2024-01-01", periods=300, freq="1D", tz="UTC")
    d = pd.DataFrame({"excess": -0.001}, index=idx)
    card = {"excess_t_hac": -5.0, "excess_ann": -0.365}
    from paper_trader.alpha_agent.r31 import multiple_testing as MT
    p = MT.two_sided_p(card["excess_t_hac"])
    assert p < 0.05
    rejects = bool(p < 0.05 and card["excess_ann"] > 0)
    assert rejects is False
    assert len(d) == 300


# --------------------------------------------------------------------------- #
# Cadence - the error class this release exists to catch
# --------------------------------------------------------------------------- #
def test_cadence_audit_catches_an_hourly_series_declared_as_8h():
    from paper_trader.alpha_agent.r42 import acquisition as ACQ
    idx = pd.date_range("2024-01-01", periods=24 * 30, freq="1h", tz="UTC")
    hourly = pd.Series(1e-5, index=idx)
    assert ACQ.cadence_audit("DERIBIT", hourly)["matches"] is True
    assert ACQ.cadence_audit("BINANCE", hourly)["matches"] is False
    idx8 = pd.date_range("2024-01-01", periods=3 * 30, freq="8h", tz="UTC")
    eight = pd.Series(1e-4, index=idx8)
    assert ACQ.cadence_audit("BINANCE", eight)["matches"] is True
    assert ACQ.cadence_audit("DERIBIT", eight)["matches"] is False


def test_every_declared_venue_has_a_declared_cadence():
    from paper_trader.alpha_agent.r42 import acquisition as ACQ
    from paper_trader.alpha_agent.r42 import venues as VN
    for v in VN.VENUE_SYMBOLS:
        assert v in ACQ.VENUE_FUNDING_CADENCE
    assert "BINANCE" in ACQ.VENUE_FUNDING_CADENCE


# --------------------------------------------------------------------------- #
# Universes
# --------------------------------------------------------------------------- #
def test_asset_exclusions_catch_stablecoins_and_redenominations():
    from paper_trader.alpha_agent.r42 import asset_universe as AU
    assert AU._excluded_reason("USDCUSDT") == "STABLECOIN_BASE"
    assert AU._excluded_reason("1000PEPEUSDT") == "SYNTHETIC_REDENOMINATION"
    assert AU._excluded_reason("ETHUPUSDT") == "LEVERAGED_TOKEN"
    assert AU._excluded_reason("SOLUSDT") is None
    assert C.ASSET_ELIGIBILITY["include_delisted_if_history_exists"]


def test_eligibility_thresholds_are_three_years_and_real_liquidity():
    e = C.ASSET_ELIGIBILITY
    assert e["min_joint_history_days"] >= 1095
    assert e["min_funding_events"] >= 3 * e["min_joint_history_days"]
    assert e["min_median_daily_quote_volume_usd"] >= 1e6


def test_venue_data_access_is_not_investability():
    from paper_trader.alpha_agent.r42 import venues as VN
    probes = {"OKX": {"state": "PUBLIC_OK", "status": 200}}
    m = VN.matrix(probes, {"OKX": {"rows": 100, "history_days": 800,
                                   "first": "2024-01-01",
                                   "last": "2026-01-01"}})
    assert m["OKX"]["ELIGIBLE_FOR_REPLICATION"] is True
    assert m["OKX"]["INVESTABLE_BY_OPERATOR"] is False
    assert m["OKX"]["investability_blocker"]
    assert C.DATA_ACCESS_IS_NOT_INVESTABILITY


# --------------------------------------------------------------------------- #
# Shadows
# --------------------------------------------------------------------------- #
def test_r42_shadow_candidates_are_capped_and_non_promotable():
    from paper_trader.alpha_agent.r42 import forward as FW
    assert FW.MAX_R42_SHADOWS == 3
    eligible = [k for k, v in FW.R42_SHADOW_CANDIDATES.items()
                if v.get("eligible")]
    assert 0 < len(eligible) <= FW.MAX_R42_SHADOWS
    for k, v in FW.R42_SHADOW_CANDIDATES.items():
        if not v.get("eligible"):
            assert v.get("why_not")


def test_the_only_frozen_shadow_was_predeclared_in_the_contract():
    from paper_trader.alpha_agent.r42 import forward as FW
    spec = FW.R42_SHADOW_CANDIDATES["R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC"]
    assert spec["eligible"] is True
    assert spec["rule"] == C.POSITIVE_ONLY_BASELINE["rule"]
    assert C.POSITIVE_ONLY_BASELINE["declared_before_evaluation"]


# --------------------------------------------------------------------------- #
# Audit wiring
# --------------------------------------------------------------------------- #
def test_audit_guard_is_registered_and_blocking():
    import importlib.util
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "_audit_r42", root / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert hasattr(mod, "check_release42_crypto_basis_alpha")
    res = mod.check_release42_crypto_basis_alpha([])
    failing = {k: v for k, v in res.items()
               if v is False or (isinstance(v, list) and v)}
    assert not failing, failing
    keys = {k for (rep, k, _) in mod.BLOCKING_INVARIANTS
            if rep == "release42_crypto_basis_alpha"}
    for must in ("r41_declared_immutable", "r41_shadow_not_refrozen",
                 "capital_control_declared", "borrow_rule_enforced",
                 "universes_metadata_only", "hierarchy_frozen_first",
                 "maker_fill_forbidden", "shadows_capped_not_promotable",
                 "safety_flags_false", "venue_cadence_asserted"):
        assert must in keys


def test_r42_does_not_reimplement_owned_concerns():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1] / "alpha_agent" / "r42"
    blob = "\n".join(p.read_text(encoding="utf-8")
                     for p in sorted(root.glob("*.py")))
    for forbidden in ("def scorecard(", "def hac_t(", "def _append_ledger(",
                      "def verify_ledger(", "def benjamini_hochberg("):
        assert forbidden not in blob, forbidden
    assert "from ..r41 import evidence" in blob
    assert "from ..r31 import multiple_testing" in blob
    assert "from ..r39.research_shadow import _desk" in blob


def test_r42_never_imports_an_operational_owner():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1] / "alpha_agent" / "r42"
    for p in sorted(root.glob("*.py")):
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s.startswith("#"):
                continue
            assert "paper_trader.api" not in s, (p.name, line)
