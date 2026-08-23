"""alpha_agent.r40.sequential - PROSPECTIVE_VALIDATION_DESIGNS +
CANDIDATE_FAMILY_ERROR_BUDGET (Track C).

The persistent research agent reviews evidence after EVERY eligible update.
That is only admissible with always-valid inference, so this module
extends the Release-39 pre-registered framework without replacing it:

* the test martingale is the R39 owner's capped-bet mixture e-process
  (``alpha_agent.r39.prospective_design.e_process`` / ``decide``) - imported,
  never re-implemented; its type-I guarantee holds at any stopping time;
* an ANYTIME-VALID CONFIDENCE SEQUENCE for the per-period mean is obtained
  by inverting the same e-process family over a grid of null means (the
  Waudby-Smith/Ramdas construction): the CS at level alpha is the set of
  means whose two-sided e-process is below 1/alpha;
* each NEW shadow receives a frozen design (success boundary, futility
  boundary, minimum evidence before futility, maximum horizon, primary
  statistic, supporting statistics, cost/control assumptions, the
  factor-residual requirement) with sigma0 taken from its Zone-B stream
  BEFORE any forward outcome - the three Release-39 designs stay immutable
  and are referenced by hash;
* the CANDIDATE-FAMILY ERROR BUDGET is declared: per-candidate alpha = 0.05
  (e >= 20, the R39 boundary kept for every member so no threshold moves),
  the union bound over the family (<= 0.25 at five members) is REPORTED,
  and any family-level "the family found alpha" claim uses the AVERAGED
  e-process (valid under arbitrary dependence) at the same 1/20 level.

Evaluated and rejected for this release (named, not silent): sequential
likelihood ratios (need a parametric alternative the shrunk prior cannot
honestly supply); sequential probability ratio tests with fixed
alternatives (the alternative is the quantity under test); plain
Bonferroni over peeks (dominated by the e-process at any stopping time).
"""
from __future__ import annotations

import math

import numpy as np

from .. import r39 as _r39
from ..r39 import prospective_design as PD
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.sequential"
DESIGNS_NAME = "prospective_validation_designs.json"
BUDGET_NAME = "candidate_family_error_budget.json"

ALPHA_PER_CANDIDATE = 1.0 / PD.E_SUCCESS          # 0.05
E_SUCCESS = PD.E_SUCCESS                          # 20 - kept, never reset
E_FUTILITY = PD.E_FUTILITY
MIN_FUTILITY_N = PD.MIN_FUTILITY_N
FAMILY_E_SUCCESS = PD.E_SUCCESS                   # averaged e-process
MAX_HORIZON_MONTHLY = 60
MAX_HORIZON_WEEKLY = 260
REGISTERED_EFFECT_SHRINKAGE = 0.5                 # R39 convention


def confidence_sequence(returns, *, sigma0: float, alpha: float = 0.05,
                        grid_half_width_sigmas: float = 3.0,
                        n_grid: int = 241) -> dict:
    """Anytime-valid CS for the per-period mean by e-process inversion.

    For each candidate mean m on a grid, the two-sided e-process is the
    average of the R39 capped-bet mixture applied to (x - m) and to
    (m - x); m is in the CS when that e-value is below 1/alpha.
    """
    x = np.asarray(list(returns), dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {"n": 0, "lower": None, "upper": None, "alpha": alpha}
    width = grid_half_width_sigmas * sigma0
    centre = float(x.mean())
    grid = np.linspace(centre - width, centre + width, n_grid)
    inside = []
    for m in grid:
        e_up = PD.e_process(x - m, sigma0=sigma0)["e_value"]
        e_dn = PD.e_process(m - x, sigma0=sigma0)["e_value"]
        if 0.5 * (e_up + e_dn) < 1.0 / alpha:
            inside.append(float(m))
    if not inside:
        return {"n": int(x.size), "lower": None, "upper": None,
                "alpha": alpha, "state": "EMPTY_ON_GRID"}
    return {"n": int(x.size), "lower": min(inside), "upper": max(inside),
            "alpha": alpha, "anytime_valid": True,
            "excludes_zero": bool(min(inside) > 0 or max(inside) < 0),
            "family": "inverted capped-bet mixture e-process "
                      "(Waudby-Smith/Ramdas class)"}


def family_e_value(e_values: list) -> float:
    """The averaged e-process: valid for the family claim under any
    dependence between members."""
    vals = [float(v) for v in e_values if v is not None
            and math.isfinite(float(v))]
    return float(np.mean(vals)) if vals else 1.0


def evaluate_shadow(returns, *, sigma0: float, shadow_id: str,
                    max_horizon: int) -> dict:
    """Primary decision for one shadow: the R39 decide() plus the CS."""
    dec = PD.decide(returns, sigma0=sigma0, shadow_id=shadow_id)
    n = dec["n_observations"]
    if dec["decision_state"] == "ACCUMULATING" and n >= max_horizon:
        dec["decision_state"] = "HORIZON_REACHED_WITHOUT_DECISION"
    dec["confidence_sequence"] = confidence_sequence(returns, sigma0=sigma0)
    dec["boundaries"]["max_horizon_obs"] = max_horizon
    return dec


def _mde_periods(sigma: float, effect: float) -> int:
    return PD._mde_months(sigma, effect)


def expected_log_growth(mu: float, sigma0: float) -> float:
    """Expected per-observation log-wealth growth of the capped-bet mixture
    under a Gaussian alternative with mean mu (computed by quadrature over
    the standardised move; the same bets the e-process uses)."""
    z_grid = np.linspace(-6, 6, 1201)
    w = np.exp(-0.5 * z_grid ** 2) / math.sqrt(2 * math.pi)
    w /= w.sum()
    shift = mu / max(sigma0, 1e-12)
    g = []
    for lam in PD.BET_GRID:
        bet = lam * np.clip(z_grid + shift, -PD.BET_CAP / lam,
                            PD.BET_CAP / lam)
        g.append(float((w * np.log1p(bet)).sum()))
    # the mixture's growth is at least the best component's growth minus
    # log(#components); report the best component (the mixture tracks it)
    return max(g)


def design_for(shadow: dict, *, sigma0: float, mu_point: float,
               cadence: str) -> dict:
    """One frozen design from a Zone-B stream (pre-registered effect =
    50% shrinkage of the selection-stage point estimate)."""
    ppy = 12.0 if cadence == "monthly" else 52.0
    mu_reg = mu_point * REGISTERED_EFFECT_SHRINKAGE
    max_h = MAX_HORIZON_MONTHLY if cadence == "monthly" else \
        MAX_HORIZON_WEEKLY
    return {
        "shadow_id": shadow["shadow_id"],
        "candidate_id": shadow["candidate_id"],
        "primary_statistic": "per-period after-cost net book return vs the "
                             "declared control (%s)" % shadow.get("control"),
        "decision_cadence": cadence,
        "control": shadow.get("control"),
        "cost_assumption": shadow.get("cost_model", {}).get("base")
        or "per-market modelled bps on traded notional, both sides",
        "sigma0_per_period": float(sigma0),
        "sigma0_source": "Zone-B after-cost net stream of the frozen "
                         "candidate, fixed before any forward observation",
        "expected_effect_point_estimate_per_period": float(mu_point),
        "expected_effect_point_estimate_annualised":
            float(mu_point * ppy),
        "expected_effect_registered_per_period": float(mu_reg),
        "expected_effect_registered_annualised": float(mu_reg * ppy),
        "registered_effect_shrinkage": REGISTERED_EFFECT_SHRINKAGE,
        "success_boundary_e": E_SUCCESS,
        "futility_boundary": "e <= %s AND n >= %d AND running mean < 0"
                             % (E_FUTILITY, MIN_FUTILITY_N),
        "minimum_evidence_before_futility": MIN_FUTILITY_N,
        "max_horizon_observations": max_h,
        "supporting_statistics": list(C.SUPPORTING_EVIDENCE_CHANNELS),
        "factor_residual_requirement": "at every 12th maturation the "
                                       "forward stream is regressed on the "
                                       "frozen R39 known-premia factor set; "
                                       "exposure is never claimed as alpha",
        "expected_log_evidence_growth_per_obs": {
            "at_point_estimate": expected_log_growth(mu_point, sigma0),
            "at_registered_effect": expected_log_growth(mu_reg, sigma0),
            "under_null": expected_log_growth(0.0, sigma0)},
        "periods_to_success_boundary": {
            "at_point_estimate": int(math.ceil(
                math.log(E_SUCCESS) / max(expected_log_growth(mu_point,
                                                              sigma0),
                                          1e-9))),
            "at_registered_effect": int(math.ceil(
                math.log(E_SUCCESS) / max(expected_log_growth(mu_reg,
                                                              sigma0),
                                          1e-9)))},
        "fixed_sample_80pct_power_periods": {
            "at_point_estimate": _mde_periods(sigma0, mu_point),
            "at_registered_effect": _mde_periods(sigma0, mu_reg)},
        "thresholds_immutable_after_first_observation": True,
    }


def freeze_designs(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Write the designs for every registry-v2 shadow: R39 members by
    reference to their immutable design, new members frozen here."""
    from . import shadow_registry as SR
    reg = SR.load(campaign_id)
    if not reg:
        raise RuntimeError("shadow registry v2 must be frozen first")
    r39_design = _r39.read_json(
        _r39.campaign_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
        / PD.ARTIFACT_NAME) or {}
    designs = {}
    for sh in reg["shadows"]:
        if sh.get("origin_release") == "release39":
            designs[sh["shadow_id"]] = {
                "immutable_r39_design": True,
                "design_hash": r39_design.get("prospective_design_hash"),
                **(r39_design.get("designs") or {}).get(sh["shadow_id"], {}),
                "success_boundary_e": E_SUCCESS,
                "max_horizon_observations": PD.MAX_HORIZON_OBS.get(
                    sh["shadow_id"]),
            }
        else:
            zb = sh.get("zone_b_stream_stats") or {}
            designs[sh["shadow_id"]] = design_for(
                sh, sigma0=float(zb["sigma_per_period"]),
                mu_point=float(zb["mu_per_period"]),
                cadence="weekly" if sh.get("lane") == "VX" else "monthly")
    n = len(designs)
    budget = {
        "family_size": n,
        "family_cap": C.MAX_RESEARCH_SHADOW_FAMILY,
        "per_candidate_alpha": ALPHA_PER_CANDIDATE,
        "per_candidate_success_e": E_SUCCESS,
        "union_bound_family_alpha": min(1.0, n * ALPHA_PER_CANDIDATE),
        "family_claim_rule": "a family-level claim ('some member has "
                             "positive after-cost mean') requires the "
                             "AVERAGED e-process over all members to reach "
                             "%.0f (alpha = %.2f under arbitrary "
                             "dependence)" % (FAMILY_E_SUCCESS,
                                              1.0 / FAMILY_E_SUCCESS),
        "individual_claim_rule": "an individual member's claim is made at "
                                 "alpha = 0.05 and is ALWAYS reported next "
                                 "to the union bound",
        "thresholds_never_reset": True,
        "r39_members_keep_their_immutable_boundaries": True,
        "promotion_on_boundary_crossing": False,
    }
    body = artifact_body("r40_prospective_validation_designs/1", {
        "calculation_owner": CALCULATION_OWNER,
        "e_process_owner": PD.CALCULATION_OWNER,
        "confidence_sequence": "inverted capped-bet mixture e-process, "
                               "alpha 0.05, anytime-valid",
        "designs": designs,
        "family_error_budget": budget,
        "methods_evaluated_and_rejected": {
            "sequential_likelihood_ratio": "needs a parametric alternative "
                                           "the shrunk prior cannot honestly "
                                           "supply",
            "sprt_fixed_alternative": "the alternative is the quantity "
                                      "under test",
            "bonferroni_over_peeks": "dominated by the e-process at any "
                                     "stopping time",
        },
        "registered_before_first_forward_observation": True,
    })
    body["designs_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / DESIGNS_NAME, body)
    bb = artifact_body("r40_candidate_family_error_budget/1", {
        "calculation_owner": CALCULATION_OWNER, **budget,
        "designs_hash": body["designs_hash"]})
    bb["budget_hash"] = _r39.sha(bb)
    _r39.write_json(campaign_dir(campaign_id) / BUDGET_NAME, bb)
    return body
