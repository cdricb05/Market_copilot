"""alpha_agent.r31.calibration - the ONE Track-A score-to-expected-return owner.

The canonical zero-base allocator consumes ECONOMIC RETURN QUANTITIES. It prices
a name's expected return against its variance, its forecast uncertainty and the
cost of getting there, and every one of those trades is denominated in return
units. Handing it an arbitrary model score - a z-score, a rank, a tree's leaf
value - and calling the result a portfolio produces weights whose arithmetic is
meaningless even when the ranking underneath was excellent.

So a Track-A candidate may reach the allocator by exactly two routes:

  A. it already forecasts a forward excess return in economic units, or
  B. its raw score passes a PRE-REGISTERED MONOTONIC calibration into economic
     units, fitted only on evidence the candidate was entitled to see.

Why the calibration is monotonic
--------------------------------
Release 30.1 is the reason this module exists. An operational calibration was
fitted whose slope came out NEGATIVE, which silently INVERTED the approved model:
the names the ranking liked best received the lowest expected returns, and the
allocator - behaving perfectly - bought the ones the model liked least. The model
was sound and the mapping destroyed it. A calibration is a change of UNITS, and a
change of units that reorders the thing being measured is not a calibration; it is
a different model wearing the original's name.

This module therefore refuses to emit a mapping that does not preserve the
candidate's own ordering, and reports ``FORECAST_RANK_IDENTITY_VIOLATION`` instead
of returning numbers. Fitting a calibration is allowed to FAIL. Manufacturing a mu
is not.

What it may read
----------------
DISCOVERY and VALIDATION only. The lockbox is invisible here, as it is everywhere
else: a mapping tuned on the lockbox would make the lockbox a training set with
extra steps, and the campaign's one-shot guarantee would be gone.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np

from .. import r31

CALCULATION_OWNER = "alpha_agent.r31.calibration"
CALIBRATION_SCHEMA = "r31_forecast_calibration/1"

#: The two admissible routes to an expected return.
UNIT_NATIVE = "NATIVE_ECONOMIC_RETURN_UNITS"
UNIT_CALIBRATED = "MONOTONIC_CALIBRATED_TO_RETURN_UNITS"

#: Refusal states. Each is a legitimate, reportable outcome.
RANK_IDENTITY_VIOLATION = "FORECAST_RANK_IDENTITY_VIOLATION"
NOT_CALIBRATABLE = "FORECAST_NOT_ECONOMICALLY_CALIBRATABLE"
CALIBRATION_OK = "FORECAST_ECONOMICALLY_CALIBRATED"

#: A slope this small is not a mapping into return units, it is a constant. A
#: constant mu makes every name identical to the allocator, which then allocates
#: on variance alone - a result that would be reported as the candidate's.
MIN_ABS_SLOPE = 1.0e-9

#: Minimum paired observations before a mapping may be claimed at all.
MIN_FIT_OBS = 200

#: Minimum fraction of fitting dates whose within-date slope agrees in sign with
#: the pooled slope: the fitted direction must hold on AT LEAST HALF of them.
#:
#: This is deliberately NOT a significance test, and an earlier draft's 0.55 made
#: it one by accident. Under the usual approximation the positive-slope fraction
#: is Phi(t / sqrt(N)), so at this campaign's 167 fitting dates a 0.55 floor is
#: equivalent to about t >= 1.9 - it simply duplicated the t-gate below, slightly
#: tighter, and two gates measuring one thing means the looser one is decorative
#: while the tighter one silently sets the real bar.
#:
#: What sign stability uniquely catches is an average driven by a handful of
#: dates. The t-statistic uses slope MAGNITUDES, so three violent months can carry
#: it; the sign count cannot be moved by any single date. A floor at 0.50 makes
#: that the whole job: a mapping whose direction fails on most of the dates it was
#: fitted on is refused however impressive its mean looks, and everything else is
#: left to the t-gate, which is the statistic designed for it.
MIN_SIGN_STABILITY = 0.50

#: Minimum t-statistic on the MEAN of the per-date slopes.
#:
#: Sign stability alone is not enough, and the failure is quantitative: across 60
#: fitting dates a pure-noise score clears a 0.55 agreement bar by chance roughly
#: a quarter of the time. A calibration fitted on noise would then hand the
#: allocator confident-looking expected returns built from nothing. Estimating the
#: slope once per date and testing the mean of those estimates is the standard
#: Fama-MacBeth treatment: it prices the fact that observations within a date share
#: that date's shocks, which pooled OLS ignores and is exactly why a pooled t on
#: 3000 stacked observations looks impressive when 167 independent dates do not.
#:
#: THE FLOOR IS THE CONVENTIONAL 2.0, AND THE REASON IS POWER. An earlier draft
#: set it at 3.0, chosen only against the FALSE-POSITIVE rate; measured against
#: real effect sizes that floor rejects genuine alpha. A good real-world equity
#: factor has a monthly cross-sectional rank IC around 0.03 with a standard
#: deviation around 0.10, which over N fitting dates produces an expected t of
#: 0.3*sqrt(N): 2.3 at 60 dates, 3.9 at 167. A 3.0 floor therefore refuses to
#: price ANY modest genuine factor measured on a decade of monthly cross-sections,
#: and a campaign whose every candidate is refused has measured its own gate
#: rather than the evidence.
#:
#: The division of labour matters more than the number. THIS gate exists to stop
#: a score with no defensible relationship to returns from becoming an expected
#: return - a units check, whose sharpest component is the exact ``slope < 0``
#: refusal below. DATA-MINING risk across many candidates is controlled where it
#: belongs: the Benjamini-Hochberg correction over the whole executed denominator,
#: the Hansen SPA test, the paired block bootstrap and the one-shot lockbox. A
#: candidate that squeaks through this gate on noise still has to survive all
#: four, and its small fitted slope produces a small mu, which the allocator
#: answers by holding cash - so it is judged and it loses.
MIN_SLOPE_T = 2.0


def contract() -> dict:
    """The guard values, as data.

    Bound into the judge's behaviour hash and therefore into every candidate's
    specification hash: changing a calibration floor changes WHICH candidates are
    admissible capital allocators and what their expected returns mean, so
    results measured under two different floors must not share a leaderboard.
    """
    return {
        "schema": CALIBRATION_SCHEMA,
        "owner": CALCULATION_OWNER,
        "family": "AFFINE_MONOTONIC",
        "min_abs_slope": MIN_ABS_SLOPE,
        "min_fit_obs": MIN_FIT_OBS,
        "min_sign_stability": MIN_SIGN_STABILITY,
        "min_slope_t": MIN_SLOPE_T,
        "slope_t_basis": "FAMA_MACBETH_MEAN_OF_PER_DATE_SLOPES",
        "negative_slope_refused": True,
        "fitted_on": "DISCOVERY_ONLY",
        "lockbox_visible": False,
    }


class CalibrationRefused(RuntimeError):
    """The candidate's score could not be defensibly mapped into return units.

    Carries the MEASUREMENTS that produced the refusal, not only a sentence. A
    campaign in which every Track-A candidate is refused has to be able to show
    that the refusals are a property of the evidence rather than of the gate, and
    "slope +0.0003, per-date t = 1.24 over 142 dates, direction held on 51 % of
    them" is that showing. A bare state string is not.
    """

    def __init__(self, state: str, detail: str, diagnostics=None):
        super().__init__("%s: %s" % (state, detail))
        self.state = state
        self.detail = detail
        self.diagnostics = dict(diagnostics or {})


class Calibration:
    """A frozen, monotonic, rank-preserving map from raw score to expected return.

    Deliberately AFFINE. A richer monotonic family (isotonic, splines) would fit
    the fitting sample better and would buy that fit with shape freedom the
    campaign cannot audit at every future cross-section. An affine map has one
    slope and one intercept, its monotonicity is decidable by inspecting a single
    sign, and it cannot reorder anything - which is the whole property being
    protected here.
    """

    def __init__(self, slope: float, intercept: float, *, unit: str,
                 diagnostics: dict):
        self.slope = float(slope)
        self.intercept = float(intercept)
        self.unit = str(unit)
        self.diagnostics = dict(diagnostics)

    def apply(self, score: np.ndarray) -> np.ndarray:
        return self.intercept + self.slope * np.asarray(score, dtype=np.float64)

    def to_dict(self) -> dict:
        return {"schema": CALIBRATION_SCHEMA,
                "calculation_owner": CALCULATION_OWNER,
                "unit": self.unit,
                "slope": self.slope,
                "intercept": self.intercept,
                "preserves_rank_identity": True,
                "monotone_increasing": self.slope > 0.0,
                "diagnostics": dict(self.diagnostics),
                "state": CALIBRATION_OK}

    def hash(self) -> str:
        return r31.sha({k: v for k, v in self.to_dict().items()
                        if k != "diagnostics"})


def native(*, unit_note: str = "") -> Calibration:
    """The identity map, for a candidate that already forecasts return units."""
    return Calibration(1.0, 0.0, unit=UNIT_NATIVE,
                       diagnostics={"identity": True, "note": unit_note})


def fit(scores, realised, *, dates=None) -> Calibration:
    """Fit the pre-registered monotonic map on entitled evidence only.

    ``scores`` and ``realised`` are POOLED across the fitting dates, already
    cross-sectionally demeaned by the caller so that a date's overall market move
    cannot masquerade as predictive power. ``dates`` labels each observation and
    is used only to measure whether the fitted direction is stable.

    Raises ``CalibrationRefused`` rather than returning a mapping it cannot
    defend.
    """
    s = np.asarray(scores, dtype=np.float64)
    y = np.asarray(realised, dtype=np.float64)
    if s.shape != y.shape:
        raise CalibrationRefused(
            NOT_CALIBRATABLE,
            "score/realised shape mismatch %s vs %s" % (s.shape, y.shape))

    m = np.isfinite(s) & np.isfinite(y)
    n = int(m.sum())
    if n < MIN_FIT_OBS:
        raise CalibrationRefused(
            NOT_CALIBRATABLE,
            "only %d paired observations; %d required" % (n, MIN_FIT_OBS))
    s, y = s[m], y[m]
    if dates is not None:
        d = np.asarray(dates)[m]
    else:
        d = None

    sd = float(s.std())
    if not math.isfinite(sd) or sd <= 0.0:
        raise CalibrationRefused(
            NOT_CALIBRATABLE, "score has no cross-sectional variation")

    # Ordinary least squares: the slope IS the change of units.
    sc = s - s.mean()
    yc = y - y.mean()
    denom = float((sc * sc).sum())
    if denom <= 0.0:
        raise CalibrationRefused(NOT_CALIBRATABLE, "degenerate score variance")
    slope = float((sc * yc).sum() / denom)
    intercept = float(y.mean() - slope * s.mean())

    # Per-date slopes, measured BEFORE any refusal so every outcome - accepted or
    # refused - reports the same statistics on the same basis.
    sign_stability = None
    slope_t = None
    n_dates = 0
    if d is not None:
        per_date = []
        for u in np.unique(d):
            k = d == u
            if int(k.sum()) < 10:
                continue
            a = s[k] - s[k].mean()
            b = y[k] - y[k].mean()
            den = float((a * a).sum())
            if den <= 0:
                continue
            per_date.append(float((a * b).sum() / den))
        if per_date:
            arr = np.asarray(per_date, dtype=np.float64)
            n_dates = int(arr.size)
            sign_stability = float(np.mean(arr > 0))
            if arr.size >= 3:
                se = float(arr.std(ddof=1) / math.sqrt(arr.size))
                slope_t = float(arr.mean() / se) if se > 0 else float("inf")

    measured = {"pooled_slope": slope, "intercept": intercept,
                "observations": n, "fitting_dates": n_dates,
                "sign_stability": sign_stability, "per_date_slope_t": slope_t,
                "min_sign_stability_required": MIN_SIGN_STABILITY,
                "min_slope_t_required": MIN_SLOPE_T,
                "score_sd": sd}

    if not math.isfinite(slope) or abs(slope) < MIN_ABS_SLOPE:
        raise CalibrationRefused(
            NOT_CALIBRATABLE,
            "slope %.3e is indistinguishable from zero; the mapping would make "
            "every name identical to the allocator" % (slope,), measured)

    # THE Release-30.1 GUARD. A negative slope reverses the candidate's own
    # ranking, so the allocator would buy the names the model ranked worst.
    if slope < 0.0:
        raise CalibrationRefused(
            RANK_IDENTITY_VIOLATION,
            "fitted slope %.6g is negative, which would invert the candidate's "
            "own ordering before it reaches the allocator" % (slope,), measured)

    # Direction stability AND significance across fitting dates. Both are
    # measured on per-date slopes, so a date's common shock cannot be counted
    # once per stock.
    if sign_stability is not None:
        if sign_stability < MIN_SIGN_STABILITY:
            raise CalibrationRefused(
                NOT_CALIBRATABLE,
                "fitted direction holds on only %.2f of %d fitting dates; "
                "below the %.2f stability floor"
                % (sign_stability, n_dates, MIN_SIGN_STABILITY), measured)

        if slope_t is not None:
            if not math.isfinite(slope_t) or slope_t < MIN_SLOPE_T:
                raise CalibrationRefused(
                    NOT_CALIBRATABLE,
                    "per-date slope t=%.2f over %d dates is below the %.2f "
                    "floor; the score/return relationship is not "
                    "distinguishable from noise, so mapping it into return "
                    "units would price capital on nothing"
                    % (slope_t, n_dates, MIN_SLOPE_T), measured)

    fitted = intercept + slope * s
    ss_res = float(((y - fitted) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = (1.0 - ss_res / ss_tot) if ss_tot > 0 else float("nan")

    return Calibration(
        slope, intercept, unit=UNIT_CALIBRATED,
        diagnostics={
            "observations": n,
            "fitting_dates": (int(np.unique(d).size) if d is not None else None),
            "slope": slope,
            "intercept": intercept,
            "r_squared": (None if not math.isfinite(r2) else float(r2)),
            "score_sd": sd,
            "realised_sd": float(y.std()),
            "sign_stability": sign_stability,
            "min_sign_stability_required": MIN_SIGN_STABILITY,
            "per_date_slope_t": slope_t,
            "min_slope_t_required": MIN_SLOPE_T,
            "expected_return_sd_implied": float(abs(slope) * sd),
            "lockbox_used": False,
            "fitted_on": "DISCOVERY_AND_VALIDATION_ONLY",
        })


def verify_rank_identity(score: np.ndarray, mu: np.ndarray) -> bool:
    """Prove the mapping did not reorder anything.

    Compares the ORDERING of the raw score with the ordering of the expected
    returns handed to the allocator. Called on live cross-sections, not only at
    fit time, because the guarantee that matters is the one holding at the moment
    capital is priced.
    """
    s = np.asarray(score, dtype=np.float64)
    m = np.asarray(mu, dtype=np.float64)
    k = np.isfinite(s) & np.isfinite(m)
    if int(k.sum()) < 2:
        return True
    a, b = s[k], m[k]
    order = np.argsort(a, kind="stable")
    return bool(np.all(np.diff(b[order]) >= -1e-12))
