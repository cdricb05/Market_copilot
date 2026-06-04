"""
engine/scoring.py — Pure, stateless scoring functions for Decision Model v2.

All functions accept plain dicts or typed dataclasses; no DB sessions, no ORM
objects, no side effects.  Safe to call from app.py, tests, or notebooks.

Scoring design principles
--------------------------
* base_score     = confidence × expected_return_pct  (same as v1)
* momentum_adj   = weighted blend of 5-day and 20-day momentum, capped
* rs_adj         = relative strength vs SPY, capped
* scan_adj       = scan_score, normalised to 0-1 scale, capped
* Negative expected_return is penalised by doubling its weight in base_score.
* Low confidence (< 0.50) applies a mild suppression multiplier.
* No single auxiliary factor can contribute more than ±CAP_AUX points to the
  total score, preventing any raw field from dominating.

Rotation eligibility rules
----------------------------
1. holding_pnl_pct >= 0  (hard block — no sell-at-loss)
2. candidate_forward_score > holding_forward_score + min_improvement_score
3. Both scores must be based on fresh/valid data (caller is responsible for
   staleness; functions accept the data as-is and flag prediction_missing).

Scoring profiles (Phase 4B/4C)
-------------------------------
* current            — production formula, unchanged
* balanced_preview   — reduced momentum weights, volatility + holding penalties
* quality_preview    — favours sustained RS and prediction quality; spike penalty
* risk_adjusted_preview — most conservative; aggressive vol + holding penalties
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum absolute contribution any single auxiliary factor may add/subtract.
_CAP_AUX: float = 0.05

# Clip boundaries for raw field values (before weighting)
_MOM_CLIP: float  = 0.30   # ±30 % momentum cap before normalisation
_RS_CLIP: float   = 0.50   # ±50 % relative-strength cap
_SCAN_MAX: float  = 100.0  # scan_score on 0-100 scale is normalised → 0-1

# Weights for auxiliary factors (all between 0 and 1; sum < 1 by design)
_W_MOM_5D:  float = 0.15
_W_MOM_20D: float = 0.10
_W_RS:      float = 0.15
_W_SCAN:    float = 0.10

# Low-confidence suppression threshold
_LOW_CONF_THRESHOLD: float = 0.50
_LOW_CONF_FACTOR:    float = 0.80   # score × 0.80 when conf < threshold

# Balanced-preview scoring weights (Phase 4B) — less momentum-heavy
_W_MOM_5D_BAL:    float = 0.07   # vs 0.15 in current
_W_MOM_20D_BAL:   float = 0.05   # vs 0.10 in current
_W_RS_BAL:        float = 0.10   # vs 0.15 in current
_W_SCAN_BAL:      float = 0.08   # vs 0.10 in current
_W_VOL_PENALTY:   float = 0.05   # penalise high-volatility tickers (new)
_VOL_CLIP:        float = 0.10   # clip 20d volatility pct at ±10 %
_HOLDING_PENALTY: float = 0.015  # slight discount for already-held positions

# Quality-preview scoring weights (Phase 4C) — favour sustained RS over short spikes
_W_MOM_5D_QUAL:        float = 0.05   # less short-term chasing vs current 0.15
_W_MOM_20D_QUAL:       float = 0.07   # slightly lower vs current 0.10
_W_RS_QUAL:            float = 0.20   # higher weight — sustained market leadership
_W_SCAN_QUAL:          float = 0.08
_W_VOL_QUAL:           float = 0.04   # mild volatility penalty
_SPIKE_THRESHOLD_QUAL: float = 0.08   # ±8% 5D move signals exhausted spike
_SPIKE_PENALTY_QUAL:   float = 0.03   # penalty subtracted when spike detected

# Risk-adjusted-preview scoring weights (Phase 4C) — most conservative
_W_MOM_5D_RISK:          float = 0.04
_W_MOM_20D_RISK:         float = 0.04
_W_RS_RISK:              float = 0.08
_W_SCAN_RISK:            float = 0.06
_W_VOL_RISK:             float = 0.10   # aggressive volatility penalty (2× balanced)
_HOLDING_PENALTY_RISK:   float = 0.025  # stronger holding penalty vs 0.015 balanced
_MISSING_PENALTY_RISK:   float = -0.02  # stronger missing-prediction penalty
_RETURN_FACTOR_POS_RISK: float = 0.80   # discount positive expected return
_RETURN_FACTOR_NEG_RISK: float = 2.50   # more punishing for negative return


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce value to float; return default on None, NaN, or conversion error."""
    if value is None:
        return default
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    import math
    return default if math.isnan(v) or math.isinf(v) else v


def normalize_score(value: float, min_value: float, max_value: float) -> float:
    """
    Map value from [min_value, max_value] to [0.0, 1.0].

    Returns 0.0 when min_value == max_value to avoid division by zero.
    Clamps output to [0.0, 1.0].
    """
    if max_value == min_value:
        return 0.0
    raw = (value - min_value) / (max_value - min_value)
    return max(0.0, min(1.0, raw))


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _cap(value: float) -> float:
    """Clamp an auxiliary contribution to ±_CAP_AUX."""
    return _clip(value, -_CAP_AUX, _CAP_AUX)


def _detect_scan_score_scale(raw: float) -> float:
    """
    Normalise scan_score to 0-1 regardless of whether it arrived on 0-1 or
    0-100 scale.  Heuristic: if raw > 1.0 assume 0-100 scale.
    """
    if raw > 1.0:
        return _clip(raw / _SCAN_MAX, 0.0, 1.0)
    return _clip(raw, 0.0, 1.0)


# ---------------------------------------------------------------------------
# Factor containers
# ---------------------------------------------------------------------------

@dataclass
class ScoreFactors:
    """Detailed breakdown of how a score was computed."""
    base_score:       float = 0.0
    momentum_adj:     float = 0.0
    rs_adj:           float = 0.0
    scan_adj:         float = 0.0
    total_score:      float = 0.0

    # Phase 4C component tracking
    volatility_penalty:      float = 0.0   # vol penalty applied (negative or 0)
    holding_penalty:         float = 0.0   # holding discount (negative or 0)
    momentum_5d_weighted:    float = 0.0   # pre-cap weighted 5D contribution
    momentum_20d_weighted:   float = 0.0   # pre-cap weighted 20D contribution
    formula_profile:         str   = "current"

    # Metadata
    confidence:              float = 0.0
    expected_return_pct:     float = 0.0
    momentum_5d_raw:         float = 0.0
    momentum_20d_raw:        float = 0.0
    rs_spy_raw:              float = 0.0
    scan_score_raw:          float = 0.0
    scan_score_normalised:   float = 0.0
    low_conf_suppressed:     bool  = False
    prediction_missing:      bool  = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "base_score":             round(self.base_score, 6),
            "momentum_adj":           round(self.momentum_adj, 6),
            "rs_adj":                 round(self.rs_adj, 6),
            "scan_adj":               round(self.scan_adj, 6),
            "total_score":            round(self.total_score, 6),
            "volatility_penalty":     round(self.volatility_penalty, 6),
            "holding_penalty":        round(self.holding_penalty, 6),
            "momentum_5d_weighted":   round(self.momentum_5d_weighted, 6),
            "momentum_20d_weighted":  round(self.momentum_20d_weighted, 6),
            "formula_profile":        self.formula_profile,
            "confidence":             round(self.confidence, 4),
            "expected_return_pct":    round(self.expected_return_pct, 4),
            "momentum_5d_raw":        round(self.momentum_5d_raw, 4),
            "momentum_20d_raw":       round(self.momentum_20d_raw, 4),
            "rs_spy_raw":             round(self.rs_spy_raw, 4),
            "scan_score_raw":         round(self.scan_score_raw, 4),
            "scan_score_normalised":  round(self.scan_score_normalised, 4),
            "low_conf_suppressed":    self.low_conf_suppressed,
            "prediction_missing":     self.prediction_missing,
        }


@dataclass
class RotationResult:
    """Output of score_rotation_v2()."""
    candidate_score:   float
    holding_score:     float
    improvement_score: float
    eligible:          bool
    blocked_reason:    str | None = None
    notes:             list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_score":   round(self.candidate_score, 6),
            "holding_score":     round(self.holding_score, 6),
            "improvement_score": round(self.improvement_score, 6),
            "eligible":          self.eligible,
            "blocked_reason":    self.blocked_reason,
            "notes":             self.notes,
        }


# ---------------------------------------------------------------------------
# Core scoring functions
# ---------------------------------------------------------------------------

def score_candidate_v2(candidate: dict[str, Any]) -> ScoreFactors:
    """
    Score a buy candidate using v2 multi-factor formula.

    Expected keys in candidate dict (all optional; missing → 0):
        prediction_confidence      float 0-1
        expected_return_pct        float (e.g. 0.08 = 8 %)
        momentum_5d_pct            float (e.g. 0.03 = 3 %)
        momentum_20d_pct           float
        relative_strength_vs_spy_20d  float
        scan_score                 float (0-1 or 0-100)
    """
    conf       = safe_float(candidate.get("prediction_confidence"), 0.0)
    exp_ret    = safe_float(candidate.get("expected_return_pct"), 0.0)
    mom_5d_raw = safe_float(candidate.get("momentum_5d_pct"), 0.0)
    mom_20d_raw= safe_float(candidate.get("momentum_20d_pct"), 0.0)
    rs_raw     = safe_float(candidate.get("relative_strength_vs_spy_20d"), 0.0)
    scan_raw   = safe_float(candidate.get("scan_score"), 0.0)

    factors = ScoreFactors(
        confidence          = conf,
        expected_return_pct = exp_ret,
        momentum_5d_raw     = mom_5d_raw,
        momentum_20d_raw    = mom_20d_raw,
        rs_spy_raw          = rs_raw,
        scan_score_raw      = scan_raw,
    )

    # --- base score ---
    # Penalise negative expected return: weight it twice as heavy
    if exp_ret < 0:
        base = conf * (2.0 * exp_ret)
    else:
        base = conf * exp_ret
    factors.base_score = base

    # --- momentum adjustment ---
    mom_5d_c  = _clip(mom_5d_raw,  -_MOM_CLIP, _MOM_CLIP)
    mom_20d_c = _clip(mom_20d_raw, -_MOM_CLIP, _MOM_CLIP)
    factors.momentum_5d_weighted  = _W_MOM_5D  * mom_5d_c
    factors.momentum_20d_weighted = _W_MOM_20D * mom_20d_c
    raw_mom_adj = factors.momentum_5d_weighted + factors.momentum_20d_weighted
    factors.momentum_adj = _cap(raw_mom_adj)

    # --- relative-strength adjustment ---
    rs_c = _clip(rs_raw, -_RS_CLIP, _RS_CLIP)
    factors.rs_adj = _cap(_W_RS * rs_c)

    # --- scan_score adjustment ---
    scan_norm = _detect_scan_score_scale(scan_raw)
    factors.scan_score_normalised = scan_norm
    # Centre around 0.5 so a middling scan_score is neutral
    factors.scan_adj = _cap(_W_SCAN * (scan_norm - 0.5))

    # --- total ---
    total = (
        factors.base_score
        + factors.momentum_adj
        + factors.rs_adj
        + factors.scan_adj
    )

    # Low-confidence suppression
    if conf < _LOW_CONF_THRESHOLD:
        total *= _LOW_CONF_FACTOR
        factors.low_conf_suppressed = True

    factors.total_score = total
    factors.formula_profile = "current"
    return factors


def score_holding_v2(
    holding: dict[str, Any],
    holding_prediction: dict[str, Any] | None = None,
) -> ScoreFactors:
    """
    Score a currently-held position for forward outlook.

    Parameters
    ----------
    holding:
        Dict with at minimum {"ticker": str}.  PnL fields are used only for
        eligibility metadata, NOT as the forward score.
    holding_prediction:
        If the ticker has a fresh CandidateReview row, pass its fields here.
        When None the position has no model prediction; forward score is neutral.

    Returns
    -------
    ScoreFactors where prediction_missing=True signals the caller that a fresh
    prediction is needed before confident rotation.
    """
    if holding_prediction is not None:
        factors = score_candidate_v2(holding_prediction)
        return factors

    # No prediction available — return a neutral/unknown forward score
    factors = ScoreFactors(prediction_missing=True)
    # Neutral score: 0.0 (neither bullish nor bearish, caller decides policy)
    factors.total_score = 0.0
    return factors


def score_rotation_v2(
    candidate_score: float,
    holding_score: float,
    holding_pnl_pct: float,
    min_improvement_score: float = 0.02,
) -> RotationResult:
    """
    Determine whether rotating from a holding into a candidate is eligible.

    Rules (all must pass):
    1. holding_pnl_pct >= 0  — no sell-at-loss (hard business rule)
    2. candidate_score > holding_score + min_improvement_score

    Parameters
    ----------
    candidate_score:
        Result of score_candidate_v2().total_score for the incoming ticker.
    holding_score:
        Result of score_holding_v2().total_score for the outgoing position.
        If prediction_missing is True on the holding, caller should pass 0.0
        and include a note; this function does not inspect the ScoreFactors
        object — it receives float values to keep the API simple.
    holding_pnl_pct:
        Current unrealized PnL % of the held position (e.g. 0.05 = 5 %).
        Used only for the hard sell-at-loss gate.
    min_improvement_score:
        Minimum forward-score improvement required for rotation to be
        eligible.  Default 0.02 (2 percentage-point equivalent).

    Returns
    -------
    RotationResult with eligible=True only when both rules pass.
    """
    improvement = candidate_score - holding_score
    notes: list[str] = []

    if holding_pnl_pct < 0:
        return RotationResult(
            candidate_score=candidate_score,
            holding_score=holding_score,
            improvement_score=improvement,
            eligible=False,
            blocked_reason="holding_negative_pnl",
            notes=["Sell blocked: would realize a loss on existing position."],
        )

    if improvement <= min_improvement_score:
        return RotationResult(
            candidate_score=candidate_score,
            holding_score=holding_score,
            improvement_score=improvement,
            eligible=False,
            blocked_reason="insufficient_improvement",
            notes=[
                f"Candidate score {candidate_score:.4f} does not exceed "
                f"holding score {holding_score:.4f} by required margin "
                f"{min_improvement_score:.4f}."
            ],
        )

    notes.append(
        f"Rotation eligible: forward improvement {improvement:.4f} "
        f"exceeds threshold {min_improvement_score:.4f}."
    )
    return RotationResult(
        candidate_score=candidate_score,
        holding_score=holding_score,
        improvement_score=improvement,
        eligible=True,
        blocked_reason=None,
        notes=notes,
    )


def score_candidate_balanced_preview(candidate: dict[str, Any]) -> ScoreFactors:
    """
    Balanced-preview scoring for Phase 4B side-by-side comparison.

    Differences vs score_candidate_v2:
      * Reduced momentum weights (0.07 / 0.05 vs 0.15 / 0.10)
      * Reduced relative-strength weight (0.10 vs 0.15)
      * Volatility penalty: high-volatility tickers lose up to _CAP_AUX points
      * Already-held penalty: _HOLDING_PENALTY subtracted for held positions
      * Missing-prediction penalty: sets total to -0.01 instead of 0.0

    Extra candidate keys consumed (all optional):
        volatility_20d_pct   float  (e.g. 0.025 = 2.5 % daily vol)
        is_current_holding   bool
    """
    conf        = safe_float(candidate.get("prediction_confidence"), 0.0)
    exp_ret     = safe_float(candidate.get("expected_return_pct"), 0.0)
    mom_5d_raw  = safe_float(candidate.get("momentum_5d_pct"), 0.0)
    mom_20d_raw = safe_float(candidate.get("momentum_20d_pct"), 0.0)
    rs_raw      = safe_float(candidate.get("relative_strength_vs_spy_20d"), 0.0)
    scan_raw    = safe_float(candidate.get("scan_score"), 0.0)
    vol_raw     = safe_float(candidate.get("volatility_20d_pct"), 0.0)
    is_held     = bool(candidate.get("is_current_holding", False))

    # Missing-prediction guard
    pred_missing = (conf == 0.0 and exp_ret == 0.0
                    and not candidate.get("prediction_confidence")
                    and not candidate.get("expected_return_pct"))

    factors = ScoreFactors(
        confidence          = conf,
        expected_return_pct = exp_ret,
        momentum_5d_raw     = mom_5d_raw,
        momentum_20d_raw    = mom_20d_raw,
        rs_spy_raw          = rs_raw,
        scan_score_raw      = scan_raw,
        prediction_missing  = pred_missing,
    )

    if pred_missing:
        factors.total_score = -0.01
        factors.formula_profile = "balanced_preview"
        return factors

    # base score (same penalty for negative return as v2)
    base = conf * (2.0 * exp_ret if exp_ret < 0 else exp_ret)
    factors.base_score = base

    # momentum (reduced weights)
    mom_5d_c  = _clip(mom_5d_raw,  -_MOM_CLIP, _MOM_CLIP)
    mom_20d_c = _clip(mom_20d_raw, -_MOM_CLIP, _MOM_CLIP)
    factors.momentum_5d_weighted  = _W_MOM_5D_BAL  * mom_5d_c
    factors.momentum_20d_weighted = _W_MOM_20D_BAL * mom_20d_c
    factors.momentum_adj = _cap(factors.momentum_5d_weighted + factors.momentum_20d_weighted)

    # relative strength (reduced weight)
    factors.rs_adj = _cap(_W_RS_BAL * _clip(rs_raw, -_RS_CLIP, _RS_CLIP))

    # scan score (reduced weight)
    scan_norm = _detect_scan_score_scale(scan_raw)
    factors.scan_score_normalised = scan_norm
    factors.scan_adj = _cap(_W_SCAN_BAL * (scan_norm - 0.5))

    # volatility penalty: higher vol → negative contribution
    vol_abs = _clip(abs(vol_raw), 0.0, _VOL_CLIP)
    factors.volatility_penalty = -_cap(_W_VOL_PENALTY * (vol_abs / max(_VOL_CLIP, 1e-9)))

    # already-held penalty
    factors.holding_penalty = -_HOLDING_PENALTY if is_held else 0.0

    total = (
        factors.base_score
        + factors.momentum_adj
        + factors.rs_adj
        + factors.scan_adj
        + factors.volatility_penalty
        + factors.holding_penalty
    )

    if conf < _LOW_CONF_THRESHOLD:
        total *= _LOW_CONF_FACTOR
        factors.low_conf_suppressed = True

    factors.total_score = total
    factors.formula_profile = "balanced_preview"
    return factors


def score_candidate_quality_preview(candidate: dict[str, Any]) -> ScoreFactors:
    """
    Quality-preview scoring for Phase 4C calibration workbench.

    Design intent: favour sustained outperformance and prediction quality over
    short-term momentum.  Penalises extreme 5-day spikes (exhausted momentum)
    and high volatility.

    Differences vs score_candidate_v2:
      * Very low 5D momentum weight (0.05 vs 0.15) — avoids chasing spikes
      * Higher RS weight (0.20 vs 0.15) — rewards sustained market leadership
      * Spike penalty: |5D mom| > 8% subtracts flat penalty instead of adding
      * Mild volatility penalty (0.04)
      * No holding penalty (focus on fundamental quality, not position status)
      * Missing-prediction penalty: -0.01

    Extra candidate keys consumed (all optional):
        volatility_20d_pct   float
    """
    conf        = safe_float(candidate.get("prediction_confidence"), 0.0)
    exp_ret     = safe_float(candidate.get("expected_return_pct"), 0.0)
    mom_5d_raw  = safe_float(candidate.get("momentum_5d_pct"), 0.0)
    mom_20d_raw = safe_float(candidate.get("momentum_20d_pct"), 0.0)
    rs_raw      = safe_float(candidate.get("relative_strength_vs_spy_20d"), 0.0)
    scan_raw    = safe_float(candidate.get("scan_score"), 0.0)
    vol_raw     = safe_float(candidate.get("volatility_20d_pct"), 0.0)

    pred_missing = (conf == 0.0 and exp_ret == 0.0
                    and not candidate.get("prediction_confidence")
                    and not candidate.get("expected_return_pct"))

    factors = ScoreFactors(
        confidence          = conf,
        expected_return_pct = exp_ret,
        momentum_5d_raw     = mom_5d_raw,
        momentum_20d_raw    = mom_20d_raw,
        rs_spy_raw          = rs_raw,
        scan_score_raw      = scan_raw,
        prediction_missing  = pred_missing,
    )

    if pred_missing:
        factors.total_score  = -0.01
        factors.formula_profile = "quality_preview"
        return factors

    base = conf * (2.0 * exp_ret if exp_ret < 0 else exp_ret)
    factors.base_score = base

    # momentum: low weight; spike penalty replaces bonus for extreme moves
    mom_5d_c  = _clip(mom_5d_raw,  -_MOM_CLIP, _MOM_CLIP)
    mom_20d_c = _clip(mom_20d_raw, -_MOM_CLIP, _MOM_CLIP)
    if abs(mom_5d_raw) > _SPIKE_THRESHOLD_QUAL:
        # Extreme 5D spike — subtract penalty rather than add momentum bonus
        factors.momentum_5d_weighted = -_SPIKE_PENALTY_QUAL
    else:
        factors.momentum_5d_weighted = _W_MOM_5D_QUAL * mom_5d_c
    factors.momentum_20d_weighted = _W_MOM_20D_QUAL * mom_20d_c
    factors.momentum_adj = _cap(factors.momentum_5d_weighted + factors.momentum_20d_weighted)

    # relative strength — higher weight than current
    factors.rs_adj = _cap(_W_RS_QUAL * _clip(rs_raw, -_RS_CLIP, _RS_CLIP))

    # scan score
    scan_norm = _detect_scan_score_scale(scan_raw)
    factors.scan_score_normalised = scan_norm
    factors.scan_adj = _cap(_W_SCAN_QUAL * (scan_norm - 0.5))

    # volatility penalty
    vol_abs = _clip(abs(vol_raw), 0.0, _VOL_CLIP)
    factors.volatility_penalty = -_cap(_W_VOL_QUAL * (vol_abs / max(_VOL_CLIP, 1e-9)))

    total = (
        factors.base_score
        + factors.momentum_adj
        + factors.rs_adj
        + factors.scan_adj
        + factors.volatility_penalty
    )

    if conf < _LOW_CONF_THRESHOLD:
        total *= _LOW_CONF_FACTOR
        factors.low_conf_suppressed = True

    factors.total_score  = total
    factors.formula_profile = "quality_preview"
    return factors


def score_candidate_risk_adjusted_preview(candidate: dict[str, Any]) -> ScoreFactors:
    """
    Risk-adjusted-preview scoring for Phase 4C calibration workbench.

    Design intent: most conservative profile.  Discounts positive returns,
    applies an aggressive volatility penalty, and strongly penalises held
    positions and missing predictions.

    Differences vs score_candidate_v2:
      * Positive return weighted at 0.8× (conservative realism)
      * Negative return weighted at 2.5× (more punishing than v2's 2×)
      * Aggressive volatility penalty (0.10 vs 0.05 balanced)
      * Stronger holding penalty (0.025 vs 0.015 balanced)
      * Stronger missing-prediction penalty: -0.02
      * Minimal momentum sensitivity (0.04/0.04)

    Extra candidate keys consumed (all optional):
        volatility_20d_pct   float
        is_current_holding   bool
    """
    conf        = safe_float(candidate.get("prediction_confidence"), 0.0)
    exp_ret     = safe_float(candidate.get("expected_return_pct"), 0.0)
    mom_5d_raw  = safe_float(candidate.get("momentum_5d_pct"), 0.0)
    mom_20d_raw = safe_float(candidate.get("momentum_20d_pct"), 0.0)
    rs_raw      = safe_float(candidate.get("relative_strength_vs_spy_20d"), 0.0)
    scan_raw    = safe_float(candidate.get("scan_score"), 0.0)
    vol_raw     = safe_float(candidate.get("volatility_20d_pct"), 0.0)
    is_held     = bool(candidate.get("is_current_holding", False))

    pred_missing = (conf == 0.0 and exp_ret == 0.0
                    and not candidate.get("prediction_confidence")
                    and not candidate.get("expected_return_pct"))

    factors = ScoreFactors(
        confidence          = conf,
        expected_return_pct = exp_ret,
        momentum_5d_raw     = mom_5d_raw,
        momentum_20d_raw    = mom_20d_raw,
        rs_spy_raw          = rs_raw,
        scan_score_raw      = scan_raw,
        prediction_missing  = pred_missing,
    )

    if pred_missing:
        factors.total_score  = _MISSING_PENALTY_RISK
        factors.formula_profile = "risk_adjusted_preview"
        return factors

    # Conservative return weighting
    if exp_ret >= 0:
        base = conf * _RETURN_FACTOR_POS_RISK * exp_ret
    else:
        base = conf * _RETURN_FACTOR_NEG_RISK * exp_ret
    factors.base_score = base

    # Minimal momentum sensitivity
    mom_5d_c  = _clip(mom_5d_raw,  -_MOM_CLIP, _MOM_CLIP)
    mom_20d_c = _clip(mom_20d_raw, -_MOM_CLIP, _MOM_CLIP)
    factors.momentum_5d_weighted  = _W_MOM_5D_RISK  * mom_5d_c
    factors.momentum_20d_weighted = _W_MOM_20D_RISK * mom_20d_c
    factors.momentum_adj = _cap(factors.momentum_5d_weighted + factors.momentum_20d_weighted)

    factors.rs_adj = _cap(_W_RS_RISK * _clip(rs_raw, -_RS_CLIP, _RS_CLIP))

    scan_norm = _detect_scan_score_scale(scan_raw)
    factors.scan_score_normalised = scan_norm
    factors.scan_adj = _cap(_W_SCAN_RISK * (scan_norm - 0.5))

    # Aggressive volatility penalty
    vol_abs = _clip(abs(vol_raw), 0.0, _VOL_CLIP)
    factors.volatility_penalty = -_cap(_W_VOL_RISK * (vol_abs / max(_VOL_CLIP, 1e-9)))

    # Stronger holding penalty
    factors.holding_penalty = -_HOLDING_PENALTY_RISK if is_held else 0.0

    total = (
        factors.base_score
        + factors.momentum_adj
        + factors.rs_adj
        + factors.scan_adj
        + factors.volatility_penalty
        + factors.holding_penalty
    )

    if conf < _LOW_CONF_THRESHOLD:
        total *= _LOW_CONF_FACTOR
        factors.low_conf_suppressed = True

    factors.total_score  = total
    factors.formula_profile = "risk_adjusted_preview"
    return factors


def build_score_breakdown(factors: ScoreFactors) -> dict[str, Any]:
    """
    Return a structured per-component score breakdown dict from ScoreFactors.

    Intended for the Phase 4C score_breakdown field on CandidatePreview.
    """
    return {
        "formula_profile": factors.formula_profile,
        "prediction_return_component":  round(factors.base_score, 6),
        "prediction_confidence_component": round(factors.confidence, 4),
        "momentum_5d_component":        round(factors.momentum_5d_weighted, 6),
        "momentum_20d_component":       round(factors.momentum_20d_weighted, 6),
        "momentum_total_adj":           round(factors.momentum_adj, 6),
        "relative_strength_component":  round(factors.rs_adj, 6),
        "scan_adj":                     round(factors.scan_adj, 6),
        "volatility_penalty_component": round(factors.volatility_penalty, 6),
        "already_held_penalty_component": round(factors.holding_penalty, 6),
        "stale_or_missing_prediction_penalty": round(
            factors.total_score if factors.prediction_missing else 0.0, 6
        ),
        "low_conf_suppression_applied": factors.low_conf_suppressed,
        "final_score": round(factors.total_score, 6),
    }


def explain_score_factors(factors: ScoreFactors) -> str:
    """
    Return a human-readable one-liner summarising the score breakdown.

    Intended for use in Daily Plan preview output (what_would_change field).
    """
    parts: list[str] = []

    parts.append(f"base={factors.base_score:.4f}")

    if abs(factors.momentum_adj) > 1e-6:
        sign = "+" if factors.momentum_adj >= 0 else ""
        parts.append(f"momentum{sign}{factors.momentum_adj:.4f}")

    if abs(factors.rs_adj) > 1e-6:
        sign = "+" if factors.rs_adj >= 0 else ""
        parts.append(f"rs{sign}{factors.rs_adj:.4f}")

    if abs(factors.scan_adj) > 1e-6:
        sign = "+" if factors.scan_adj >= 0 else ""
        parts.append(f"scan{sign}{factors.scan_adj:.4f}")

    summary = f"score={factors.total_score:.4f} ({', '.join(parts)})"

    if factors.low_conf_suppressed:
        summary += " [low-confidence suppressed]"
    if factors.prediction_missing:
        summary += " [no prediction — neutral forward score]"

    return summary
