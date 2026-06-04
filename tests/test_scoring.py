"""
tests/test_scoring.py — Unit tests for engine/scoring.py (Decision Model v2).

All tests are pure: no DB, no network, no fixtures.  Deterministic inputs only.

Test coverage plan:
  1.  high confidence + positive expected return → positive score
  2.  negative expected return → reduces (and penalises) score
  3.  positive momentum improves score but is capped at _CAP_AUX
  4.  extreme relative strength is capped and does not dominate
  5.  scan_score 0-100 is normalised correctly to 0-1 scale
  6.  scan_score already on 0-1 scale is handled unchanged
  7.  holding with prediction uses same scoring logic as score_candidate_v2
  8.  holding without prediction → prediction_missing=True, neutral score 0.0
  9.  rotation compares forward scores, not PnL
 10.  rotation blocked when holding PnL is negative
 11.  rotation eligible when PnL positive and candidate exceeds holding + threshold
 12.  explanation string includes factor breakdown
 13.  normalize_score maps correctly and clamps
 14.  safe_float handles None, NaN, strings, valid values
 15.  low-confidence suppression fires below threshold
 16.  zero confidence → zero-ish score
"""
from __future__ import annotations

import math

import pytest

from paper_trader.engine.scoring import (
    ScoreFactors,
    RotationResult,
    normalize_score,
    safe_float,
    score_candidate_v2,
    score_candidate_balanced_preview,
    score_candidate_quality_preview,
    score_candidate_risk_adjusted_preview,
    build_score_breakdown,
    score_holding_v2,
    score_rotation_v2,
    explain_score_factors,
    _CAP_AUX,
    _LOW_CONF_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cand(
    *,
    confidence: float = 0.80,
    expected_return_pct: float = 0.10,
    momentum_5d_pct: float = 0.0,
    momentum_20d_pct: float = 0.0,
    relative_strength_vs_spy_20d: float = 0.0,
    scan_score: float = 0.0,
) -> dict:
    return {
        "prediction_confidence":         confidence,
        "expected_return_pct":           expected_return_pct,
        "momentum_5d_pct":               momentum_5d_pct,
        "momentum_20d_pct":              momentum_20d_pct,
        "relative_strength_vs_spy_20d":  relative_strength_vs_spy_20d,
        "scan_score":                    scan_score,
    }


# ---------------------------------------------------------------------------
# 1. High confidence + positive expected return → positive score
# ---------------------------------------------------------------------------

def test_high_confidence_positive_return_gives_positive_score():
    f = score_candidate_v2(_cand(confidence=0.90, expected_return_pct=0.12))
    assert f.total_score > 0.0
    # base = 0.90 * 0.12 = 0.108
    assert abs(f.base_score - 0.108) < 1e-9


# ---------------------------------------------------------------------------
# 2. Negative expected return reduces and penalises score
# ---------------------------------------------------------------------------

def test_negative_expected_return_gives_lower_score():
    pos = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=0.10))
    neg = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=-0.10))
    assert neg.total_score < pos.total_score


def test_negative_expected_return_penalised_double():
    # base = conf * (2 * exp_ret) when exp_ret < 0
    f = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=-0.10))
    # base = 0.80 * (2 * -0.10) = -0.16
    assert abs(f.base_score - (-0.16)) < 1e-9
    assert f.total_score < 0.0


# ---------------------------------------------------------------------------
# 3. Positive momentum improves score but cannot exceed _CAP_AUX
# ---------------------------------------------------------------------------

def test_positive_momentum_improves_score():
    base = score_candidate_v2(_cand(momentum_5d_pct=0.0))
    with_mom = score_candidate_v2(_cand(momentum_5d_pct=0.05))
    assert with_mom.total_score > base.total_score


def test_extreme_momentum_capped_at_cap_aux():
    # Even 1000% momentum should not contribute more than _CAP_AUX
    f = score_candidate_v2(_cand(momentum_5d_pct=10.0, momentum_20d_pct=10.0))
    assert f.momentum_adj <= _CAP_AUX + 1e-9
    assert f.momentum_adj >= -_CAP_AUX - 1e-9


def test_negative_momentum_reduces_score():
    base = score_candidate_v2(_cand(momentum_5d_pct=0.0))
    neg  = score_candidate_v2(_cand(momentum_5d_pct=-0.20))
    assert neg.total_score < base.total_score


# ---------------------------------------------------------------------------
# 4. Extreme relative strength is capped and does not dominate
# ---------------------------------------------------------------------------

def test_extreme_rs_spy_capped():
    f = score_candidate_v2(_cand(relative_strength_vs_spy_20d=999.0))
    assert f.rs_adj <= _CAP_AUX + 1e-9


def test_extreme_negative_rs_spy_capped():
    f = score_candidate_v2(_cand(relative_strength_vs_spy_20d=-999.0))
    assert f.rs_adj >= -_CAP_AUX - 1e-9


def test_positive_rs_spy_improves_score():
    base = score_candidate_v2(_cand(relative_strength_vs_spy_20d=0.0))
    strong = score_candidate_v2(_cand(relative_strength_vs_spy_20d=0.20))
    assert strong.total_score > base.total_score


# ---------------------------------------------------------------------------
# 5. scan_score 0-100 is normalised correctly
# ---------------------------------------------------------------------------

def test_scan_score_100_normalises_to_1():
    f = score_candidate_v2(_cand(scan_score=100.0))
    assert abs(f.scan_score_normalised - 1.0) < 1e-9


def test_scan_score_50_normalises_to_half():
    f = score_candidate_v2(_cand(scan_score=50.0))
    assert abs(f.scan_score_normalised - 0.50) < 1e-9


def test_scan_score_100_scale_contributes_positively():
    # score of 100/100 → normalised 1.0 → above midpoint 0.5 → positive adj
    f = score_candidate_v2(_cand(scan_score=100.0))
    assert f.scan_adj > 0.0


def test_scan_score_0_scale_contributes_negatively():
    # score of 0/100 → normalised 0.0 → below midpoint 0.5 → negative adj
    f = score_candidate_v2(_cand(scan_score=0.0))
    assert f.scan_adj <= 0.0


# ---------------------------------------------------------------------------
# 6. scan_score already on 0-1 scale handled correctly
# ---------------------------------------------------------------------------

def test_scan_score_01_scale_treated_as_normalised():
    f = score_candidate_v2(_cand(scan_score=0.75))
    # 0.75 ≤ 1.0 → treated as already normalised
    assert abs(f.scan_score_normalised - 0.75) < 1e-9


def test_scan_score_01_midpoint_neutral():
    # scan_score=0.5 → centred, scan_adj ≈ 0
    f = score_candidate_v2(_cand(scan_score=0.5))
    assert abs(f.scan_adj) < 1e-9


# ---------------------------------------------------------------------------
# 7. Holding with prediction uses same scoring logic
# ---------------------------------------------------------------------------

def test_holding_with_prediction_matches_candidate_score():
    pred = _cand(confidence=0.75, expected_return_pct=0.08)
    holding_factors = score_holding_v2({"ticker": "AAPL"}, holding_prediction=pred)
    candidate_factors = score_candidate_v2(pred)
    assert abs(holding_factors.total_score - candidate_factors.total_score) < 1e-9
    assert not holding_factors.prediction_missing


# ---------------------------------------------------------------------------
# 8. Holding without prediction → prediction_missing=True, neutral score
# ---------------------------------------------------------------------------

def test_holding_without_prediction_returns_prediction_missing():
    f = score_holding_v2({"ticker": "MSFT"}, holding_prediction=None)
    assert f.prediction_missing is True


def test_holding_without_prediction_neutral_score():
    f = score_holding_v2({"ticker": "MSFT"}, holding_prediction=None)
    assert f.total_score == 0.0


# ---------------------------------------------------------------------------
# 9. Rotation compares forward scores, not PnL
# ---------------------------------------------------------------------------

def test_rotation_uses_forward_scores_not_pnl():
    # High positive PnL but candidate clearly better → eligible
    result = score_rotation_v2(
        candidate_score=0.10,
        holding_score=0.02,
        holding_pnl_pct=0.50,    # large historical PnL — must not block
        min_improvement_score=0.02,
    )
    assert result.eligible is True
    assert result.improvement_score == pytest.approx(0.08, abs=1e-9)


def test_rotation_uses_forward_score_improvement_exactly():
    result = score_rotation_v2(
        candidate_score=0.08,
        holding_score=0.05,
        holding_pnl_pct=0.10,
        min_improvement_score=0.02,
    )
    assert result.improvement_score == pytest.approx(0.03, abs=1e-9)
    assert result.eligible is True


# ---------------------------------------------------------------------------
# 10. Rotation blocked when holding PnL is negative
# ---------------------------------------------------------------------------

def test_rotation_blocked_negative_pnl():
    result = score_rotation_v2(
        candidate_score=0.20,
        holding_score=0.01,
        holding_pnl_pct=-0.05,   # small loss — hard block
        min_improvement_score=0.01,
    )
    assert result.eligible is False
    assert result.blocked_reason == "holding_negative_pnl"


def test_rotation_blocked_zero_pnl_is_allowed():
    # Zero PnL means break-even — not a loss; should not be blocked
    result = score_rotation_v2(
        candidate_score=0.10,
        holding_score=0.02,
        holding_pnl_pct=0.0,
        min_improvement_score=0.02,
    )
    assert result.eligible is True


# ---------------------------------------------------------------------------
# 11. Rotation eligible when PnL positive and candidate exceeds threshold
# ---------------------------------------------------------------------------

def test_rotation_eligible_positive_pnl_exceeds_threshold():
    result = score_rotation_v2(
        candidate_score=0.12,
        holding_score=0.05,
        holding_pnl_pct=0.03,
        min_improvement_score=0.02,
    )
    assert result.eligible is True
    assert result.blocked_reason is None


def test_rotation_blocked_insufficient_improvement():
    result = score_rotation_v2(
        candidate_score=0.06,
        holding_score=0.05,
        holding_pnl_pct=0.10,
        min_improvement_score=0.02,
    )
    assert result.eligible is False
    assert result.blocked_reason == "insufficient_improvement"


def test_rotation_blocked_just_below_threshold():
    # improvement (0.019) < min_improvement (0.02) → blocked
    result = score_rotation_v2(
        candidate_score=0.069,
        holding_score=0.05,
        holding_pnl_pct=0.10,
        min_improvement_score=0.02,
    )
    assert result.eligible is False
    assert result.blocked_reason == "insufficient_improvement"


# ---------------------------------------------------------------------------
# 12. Explanation string includes factor breakdown
# ---------------------------------------------------------------------------

def test_explain_includes_total_score():
    f = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=0.10))
    s = explain_score_factors(f)
    assert "score=" in s
    assert "base=" in s


def test_explain_includes_momentum_when_nonzero():
    f = score_candidate_v2(_cand(momentum_5d_pct=0.05))
    s = explain_score_factors(f)
    assert "momentum" in s


def test_explain_marks_prediction_missing():
    f = score_holding_v2({"ticker": "X"}, holding_prediction=None)
    s = explain_score_factors(f)
    assert "no prediction" in s.lower()


def test_explain_marks_low_conf_suppressed():
    f = score_candidate_v2(_cand(confidence=0.40, expected_return_pct=0.10))
    assert f.low_conf_suppressed is True
    s = explain_score_factors(f)
    assert "low-confidence" in s.lower()


# ---------------------------------------------------------------------------
# 13. normalize_score maps correctly and clamps
# ---------------------------------------------------------------------------

def test_normalize_score_midpoint():
    assert normalize_score(5.0, 0.0, 10.0) == pytest.approx(0.5)


def test_normalize_score_min_is_zero():
    assert normalize_score(0.0, 0.0, 10.0) == pytest.approx(0.0)


def test_normalize_score_max_is_one():
    assert normalize_score(10.0, 0.0, 10.0) == pytest.approx(1.0)


def test_normalize_score_clamps_below_zero():
    assert normalize_score(-5.0, 0.0, 10.0) == pytest.approx(0.0)


def test_normalize_score_clamps_above_one():
    assert normalize_score(15.0, 0.0, 10.0) == pytest.approx(1.0)


def test_normalize_score_equal_bounds_returns_zero():
    assert normalize_score(5.0, 5.0, 5.0) == 0.0


# ---------------------------------------------------------------------------
# 14. safe_float handles edge cases
# ---------------------------------------------------------------------------

def test_safe_float_none_returns_default():
    assert safe_float(None) == 0.0
    assert safe_float(None, default=99.0) == 99.0


def test_safe_float_nan_returns_default():
    assert safe_float(float("nan")) == 0.0


def test_safe_float_inf_returns_default():
    assert safe_float(float("inf")) == 0.0
    assert safe_float(float("-inf")) == 0.0


def test_safe_float_valid_values():
    assert safe_float(0.5) == pytest.approx(0.5)
    assert safe_float("0.75") == pytest.approx(0.75)
    assert safe_float(0) == pytest.approx(0.0)


def test_safe_float_bad_string_returns_default():
    assert safe_float("not-a-number") == 0.0


# ---------------------------------------------------------------------------
# 15. Low-confidence suppression fires below threshold
# ---------------------------------------------------------------------------

def test_low_confidence_suppression_fires():
    below = _LOW_CONF_THRESHOLD - 0.01
    f = score_candidate_v2(_cand(confidence=below, expected_return_pct=0.10))
    assert f.low_conf_suppressed is True


def test_at_threshold_no_suppression():
    f = score_candidate_v2(_cand(confidence=_LOW_CONF_THRESHOLD, expected_return_pct=0.10))
    assert f.low_conf_suppressed is False


def test_low_confidence_score_lower_than_normal():
    normal = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=0.10))
    low    = score_candidate_v2(_cand(confidence=0.40, expected_return_pct=0.10))
    assert low.total_score < normal.total_score


# ---------------------------------------------------------------------------
# 16. Zero confidence produces near-zero score
# ---------------------------------------------------------------------------

def test_zero_confidence_near_zero_score():
    # Use scan_score=0.5 (neutral midpoint) so scan_adj=0 — base=0, total≈0
    f = score_candidate_v2(_cand(confidence=0.0, expected_return_pct=0.10, scan_score=0.5))
    assert abs(f.base_score) < 1e-9
    # With all auxiliary inputs at zero/neutral, total should be near zero
    # (low-conf suppression of 0.0 is still 0.0)
    assert abs(f.total_score) < 1e-9


# ---------------------------------------------------------------------------
# as_dict / RotationResult.as_dict smoke tests
# ---------------------------------------------------------------------------

def test_score_factors_as_dict_keys():
    f = score_candidate_v2(_cand())
    d = f.as_dict()
    for key in ("total_score", "base_score", "momentum_adj", "rs_adj", "scan_adj",
                "confidence", "expected_return_pct", "prediction_missing",
                "low_conf_suppressed"):
        assert key in d, f"Missing key: {key}"


def test_rotation_result_as_dict_keys():
    r = score_rotation_v2(0.10, 0.05, 0.02)
    d = r.as_dict()
    for key in ("candidate_score", "holding_score", "improvement_score",
                "eligible", "blocked_reason", "notes"):
        assert key in d, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# Phase 4C — quality_preview
# ---------------------------------------------------------------------------

def _qcand(**kwargs) -> dict:
    """Helper for quality_preview inputs — adds volatility."""
    base = {
        "prediction_confidence": 0.80,
        "expected_return_pct": 0.10,
        "momentum_5d_pct": 0.0,
        "momentum_20d_pct": 0.0,
        "relative_strength_vs_spy_20d": 0.0,
        "scan_score": 0.0,
        "volatility_20d_pct": 0.0,
        "is_current_holding": False,
    }
    base.update(kwargs)
    return base


def test_quality_preview_positive_return_positive_score():
    f = score_candidate_quality_preview(_qcand(prediction_confidence=0.80, expected_return_pct=0.10))
    assert f.total_score > 0.0
    assert f.formula_profile == "quality_preview"


def test_quality_preview_missing_prediction_returns_minus_0_01():
    f = score_candidate_quality_preview({})
    assert f.prediction_missing is True
    assert abs(f.total_score - (-0.01)) < 1e-9
    assert f.formula_profile == "quality_preview"


def test_quality_preview_spike_penalty_applied():
    normal = score_candidate_quality_preview(_qcand(momentum_5d_pct=0.05))
    spike  = score_candidate_quality_preview(_qcand(momentum_5d_pct=0.10))  # 10% > 8% threshold
    assert spike.total_score < normal.total_score


def test_quality_preview_high_rs_weight():
    low_rs  = score_candidate_quality_preview(_qcand(relative_strength_vs_spy_20d=0.0))
    high_rs = score_candidate_quality_preview(_qcand(relative_strength_vs_spy_20d=0.30))
    assert high_rs.total_score > low_rs.total_score


def test_quality_preview_volatility_penalty():
    low_vol  = score_candidate_quality_preview(_qcand(volatility_20d_pct=0.01))
    high_vol = score_candidate_quality_preview(_qcand(volatility_20d_pct=0.15))
    assert high_vol.total_score < low_vol.total_score


def test_quality_preview_formula_profile_field():
    f = score_candidate_quality_preview(_qcand())
    assert f.formula_profile == "quality_preview"


# ---------------------------------------------------------------------------
# Phase 4C — risk_adjusted_preview
# ---------------------------------------------------------------------------

def test_risk_adjusted_preview_positive_return_positive_score():
    f = score_candidate_risk_adjusted_preview(_qcand(prediction_confidence=0.80, expected_return_pct=0.10))
    assert f.total_score > 0.0
    assert f.formula_profile == "risk_adjusted_preview"


def test_risk_adjusted_preview_missing_prediction_returns_minus_0_02():
    f = score_candidate_risk_adjusted_preview({})
    assert f.prediction_missing is True
    assert abs(f.total_score - (-0.02)) < 1e-9


def test_risk_adjusted_preview_discounts_positive_return():
    current = score_candidate_v2(_qcand(prediction_confidence=0.80, expected_return_pct=0.10))
    risk    = score_candidate_risk_adjusted_preview(_qcand(prediction_confidence=0.80, expected_return_pct=0.10))
    # Risk profile discounts positive return (×0.8) — base will be lower
    assert risk.base_score < current.base_score


def test_risk_adjusted_preview_aggressive_vol_penalty():
    # Use 5% vol where balanced hasn't hit _CAP_AUX yet (penalty=-0.025)
    # but risk_adjusted does hit it (penalty=-0.05), confirming larger penalty.
    balanced = score_candidate_balanced_preview(_qcand(volatility_20d_pct=0.05))
    risk     = score_candidate_risk_adjusted_preview(_qcand(volatility_20d_pct=0.05))
    assert risk.volatility_penalty < balanced.volatility_penalty


def test_risk_adjusted_preview_holding_penalty_applied():
    not_held = score_candidate_risk_adjusted_preview(_qcand(is_current_holding=False))
    held     = score_candidate_risk_adjusted_preview(_qcand(is_current_holding=True))
    assert held.holding_penalty < 0.0
    assert not_held.holding_penalty == 0.0
    assert held.total_score < not_held.total_score


def test_risk_adjusted_preview_formula_profile_field():
    f = score_candidate_risk_adjusted_preview(_qcand())
    assert f.formula_profile == "risk_adjusted_preview"


# ---------------------------------------------------------------------------
# Phase 4C — build_score_breakdown
# ---------------------------------------------------------------------------

def test_build_score_breakdown_required_keys():
    f = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=0.10))
    bd = build_score_breakdown(f)
    for key in (
        "formula_profile",
        "prediction_return_component",
        "prediction_confidence_component",
        "momentum_5d_component",
        "momentum_20d_component",
        "momentum_total_adj",
        "relative_strength_component",
        "scan_adj",
        "volatility_penalty_component",
        "already_held_penalty_component",
        "stale_or_missing_prediction_penalty",
        "low_conf_suppression_applied",
        "final_score",
    ):
        assert key in bd, f"Missing key in score_breakdown: {key}"


def test_build_score_breakdown_final_score_matches_factors():
    f = score_candidate_v2(_cand(confidence=0.80, expected_return_pct=0.12, momentum_5d_pct=0.02))
    bd = build_score_breakdown(f)
    assert abs(bd["final_score"] - round(f.total_score, 6)) < 1e-9


def test_build_score_breakdown_formula_profile_matches():
    f = score_candidate_quality_preview(_qcand())
    bd = build_score_breakdown(f)
    assert bd["formula_profile"] == "quality_preview"


def test_build_score_breakdown_risk_profile_volatility_nonzero():
    f = score_candidate_risk_adjusted_preview(_qcand(volatility_20d_pct=0.08))
    bd = build_score_breakdown(f)
    assert bd["volatility_penalty_component"] < 0.0


def test_build_score_breakdown_current_profile_vol_zero():
    """Current profile has no volatility penalty."""
    f = score_candidate_v2(_cand())
    bd = build_score_breakdown(f)
    assert bd["volatility_penalty_component"] == 0.0
    assert bd["already_held_penalty_component"] == 0.0


def test_build_score_breakdown_low_conf_flag():
    f = score_candidate_v2(_cand(confidence=0.30, expected_return_pct=0.10))
    bd = build_score_breakdown(f)
    assert bd["low_conf_suppression_applied"] is True


def test_score_factors_as_dict_includes_phase4c_fields():
    f = score_candidate_balanced_preview(
        _qcand(volatility_20d_pct=0.05, is_current_holding=True)
    )
    d = f.as_dict()
    assert "volatility_penalty" in d
    assert "holding_penalty" in d
    assert "momentum_5d_weighted" in d
    assert "momentum_20d_weighted" in d
    assert "formula_profile" in d
    assert d["formula_profile"] == "balanced_preview"
