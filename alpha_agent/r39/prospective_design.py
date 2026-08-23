"""alpha_agent.r39.prospective_design - Track I: pre-registered prospective
validation for the research shadows.

Everything here is REGISTERED BEFORE the first forward observation exists,
so no threshold can be moved after peeking:

* the primary metric, cadence, cost assumptions and control per shadow;
* an ALWAYS-VALID sequential test (a capped-bet e-process in the
  Waudby-Smith/Ramdas family): the e-value may be inspected after EVERY
  new observation without optional-stopping abuse, because the type-I
  guarantee holds at any stopping time;
* explicit success, failure and horizon boundaries;
* the honest sample-size arithmetic: at the WIDE book's own volatility a
  24-month sample can only detect roughly three times its estimated
  effect, so the shadows are cumulative evidence streams, not a fast
  verdict.

The evaluator is a plain function over the outcome ledger so the eventual
near-real-time persistent agent can call it after every maturation.
"""
from __future__ import annotations

import math

import numpy as np

from .. import r39
from .continuation import CONTINUATION_CAMPAIGN_ID

CALCULATION_OWNER = "alpha_agent.r39.prospective_design"
ARTIFACT_NAME = "prospective_validation_design.json"

#: One-sided H0: the true after-cost mean return is <= 0.
E_SUCCESS = 20.0        # rejects H0 at alpha = 1/20 = 0.05, any stop time
E_FUTILITY = 0.05       # with >= MIN_FUTILITY_N obs and a negative mean
MIN_FUTILITY_N = 36
MAX_HORIZON_OBS = {"shadow_wide_xs": 60, "shadow_carry_rule_xs": 60,
                   "shadow_vx_carry_ts": 260}
#: Capped-bet grid; the e-process is the MIXTURE (mean) over the grid.
BET_GRID = (0.05, 0.10, 0.20, 0.40)
BET_CAP = 0.5


def e_process(returns, *, sigma0: float) -> dict:
    """Mixture capped-bet e-process for H0: mean <= 0.

    ``returns`` are per-period after-cost net returns in decision order;
    ``sigma0`` is the PRE-REGISTERED per-period volatility scale (from the
    locked historical stream, never re-fit on forward data). Each bet
    multiplies wealth by (1 + lambda * x / sigma0), with the standardised
    move clipped to +-BET_CAP/lambda so wealth can never go negative.
    """
    x = np.asarray(list(returns), dtype=float)
    x = x[np.isfinite(x)]
    log_wealth = {lam: 0.0 for lam in BET_GRID}
    for xi in x:
        z = xi / max(sigma0, 1e-12)
        for lam in BET_GRID:
            bet = lam * float(np.clip(z, -BET_CAP / lam, BET_CAP / lam))
            log_wealth[lam] += math.log1p(bet)
    e_by_lam = {lam: math.exp(lw) for lam, lw in log_wealth.items()}
    e_mix = float(np.mean(list(e_by_lam.values())))
    return {"n_observations": int(x.size),
            "e_value": e_mix,
            "e_by_bet": {str(k): v for k, v in e_by_lam.items()},
            "mean_per_period": float(x.mean()) if x.size else None,
            "anytime_valid": True}


def decide(returns, *, sigma0: float, shadow_id: str) -> dict:
    ep = e_process(returns, sigma0=sigma0)
    n, e = ep["n_observations"], ep["e_value"]
    mean = ep["mean_per_period"] or 0.0
    if e >= E_SUCCESS:
        state = "SUCCESS_BOUNDARY_CROSSED"
    elif n >= MIN_FUTILITY_N and e <= E_FUTILITY and mean < 0:
        state = "FAILURE_BOUNDARY_CROSSED"
    elif n >= MAX_HORIZON_OBS.get(shadow_id, 60):
        state = "HORIZON_REACHED_WITHOUT_DECISION"
    else:
        state = "ACCUMULATING"
    return {**ep, "decision_state": state,
            "boundaries": {"success_e": E_SUCCESS,
                           "futility_e": E_FUTILITY,
                           "min_futility_n": MIN_FUTILITY_N,
                           "max_horizon_obs":
                               MAX_HORIZON_OBS.get(shadow_id, 60)}}


def _mde_months(sigma_monthly: float, monthly_effect: float,
                *, z_alpha: float = 1.645, z_beta: float = 0.84) -> int:
    if monthly_effect <= 0:
        return -1
    return int(math.ceil(((z_alpha + z_beta) * sigma_monthly
                          / monthly_effect) ** 2))


def freeze(campaign_id: str = CONTINUATION_CAMPAIGN_ID) -> dict:
    """The pre-registered design, one block per shadow."""
    wide_sigma_m = 0.0528 / math.sqrt(12.0)   # locked Zone-C ann. vol
    wide_mu_m = 0.03469932560862894 / 12.0    # locked Zone-C point est.
    carry_sigma_m = 0.0602 / math.sqrt(12.0)
    carry_mu_m = 0.027176682617514665 / 12.0
    vx_sigma_w = 0.7674 / math.sqrt(52.0)
    vx_mu_w = 0.6399567435934317 / 52.0
    designs = {
        "shadow_wide_xs": {
            "primary_metric": "monthly after-cost net book return "
                              "(self-financed L/S; excess of cash by "
                              "construction)",
            "decision_cadence": "monthly",
            "control": "RISK_MATCHED_CASH",
            "cost_assumption": "per-market frozen R38 bps on traded "
                               "notional, both sides",
            "expected_effect_point_estimate_annualised": 0.0347,
            "expected_effect_registered_annualised": 0.017,
            "expectation_note": "the locked estimate is registered at "
                                "50% shrinkage; a selected near-miss is "
                                "expected to regress",
            "sigma0_per_period": wide_sigma_m,
            "minimum_useful_sample_months": {
                "to_detect_point_estimate_80pct_power":
                    _mde_months(wide_sigma_m, wide_mu_m),
                "to_detect_registered_effect_80pct_power":
                    _mde_months(wide_sigma_m, wide_mu_m * 0.5),
                "honest_reading": "a 24-month sample detects only ~3x "
                                  "the estimated effect; this stream is "
                                  "cumulative evidence, not a fast "
                                  "verdict"},
            "confidence_interval": "anytime-valid e-process CI implied by "
                                   "the capped-bet family",
            "factor_residual_alpha_test": "at every 12th maturation, the "
                                          "forward stream is regressed on "
                                          "the SAME frozen factor set as "
                                          "wide_factor_residual_alpha; "
                                          "exposure is never claimed as "
                                          "alpha",
        },
        "shadow_carry_rule_xs": {
            "primary_metric": "monthly after-cost net book return",
            "decision_cadence": "monthly",
            "control": "RISK_MATCHED_CASH",
            "cost_assumption": "as WIDE",
            "expected_effect_point_estimate_annualised": 0.0272,
            "expected_effect_registered_annualised": 0.014,
            "sigma0_per_period": carry_sigma_m,
            "minimum_useful_sample_months": {
                "to_detect_point_estimate_80pct_power":
                    _mde_months(carry_sigma_m, carry_mu_m)},
        },
        "shadow_vx_carry_ts": {
            "primary_metric": "weekly after-cost net return vs the "
                              "vol-matched passive VX control",
            "decision_cadence": "weekly",
            "control": "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
            "cost_assumption": "15 bps/side on traded notional",
            "expected_effect_point_estimate_annualised": 0.64,
            "expected_effect_registered_annualised": 0.32,
            "sigma0_per_period": vx_sigma_w,
            "minimum_useful_sample_weeks": {
                "to_detect_point_estimate_80pct_power":
                    _mde_months(vx_sigma_w, vx_mu_w)},
        },
    }
    body = r39.artifact_body("r39_prospective_validation_design/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "sequential_rule": {
            "family": "capped-bet e-process mixture "
                      "(Waudby-Smith/Ramdas class)",
            "null": "true after-cost mean <= 0",
            "bet_grid": list(BET_GRID), "bet_cap": BET_CAP,
            "success_boundary_e": E_SUCCESS,
            "failure_boundary": "e <= %s AND n >= %d AND running mean < 0"
                                % (E_FUTILITY, MIN_FUTILITY_N),
            "max_horizon_observations": dict(MAX_HORIZON_OBS),
            "anytime_valid": True,
            "no_peeking_abuse": "the e-value may be read after every "
                                "observation; the guarantee holds at any "
                                "stopping time, which is what makes this "
                                "suitable for the near-real-time "
                                "persistent agent",
        },
        "designs": designs,
        "registered_before_first_forward_observation": True,
    })
    body["prospective_design_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
