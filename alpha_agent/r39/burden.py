"""alpha_agent.r39.burden - search-burden-adjusted inference.

A ``t > 2`` after the machine searched thousands of alternatives is not a
finding, and this module is where that sentence becomes arithmetic:

* Benjamini-Hochberg, Hansen SPA and the stationary bootstrap are IMPORTED
  from the canonical owner :mod:`alpha_agent.r31.multiple_testing` and run
  over every LOCKED confirmation (rejected candidates included);
* the Deflated Sharpe Ratio (Bailey & Lopez de Prado) is computed with the
  effective trial count read from the Zone-B reuse ledger - the number of
  DISTINCT candidates the selection stage ever scored, never the finalist
  count;
* the EFFECTIVE SEARCH BURDEN is reported in the artifact so the final
  report cannot quote a p-value without its denominator.
"""
from __future__ import annotations

import math

import numpy as np

from .. import r39
from ..r31 import multiple_testing as _mt
from . import contract as C
from . import zones

CALCULATION_OWNER = "alpha_agent.r39.burden"
SCHEMA = "r39_multiple_discovery_results/1"
ARTIFACT_NAME = "multiple_discovery_results.json"

benjamini_hochberg = _mt.benjamini_hochberg
superior_predictive_ability = _mt.superior_predictive_ability
two_sided_p = _mt.two_sided_p


def _norm_ppf(p: float) -> float:
    from scipy.stats import norm
    return float(norm.ppf(p))


def _norm_cdf(x: float) -> float:
    from scipy.stats import norm
    return float(norm.cdf(x))


def deflated_sharpe(net: np.ndarray, *, n_trials: int,
                    trial_sharpe_variance: float) -> dict:
    """P[true Sharpe > 0 | the best of ``n_trials`` searches].

    ``net`` is the per-period net series of the CANDIDATE being judged;
    ``trial_sharpe_variance`` is the cross-candidate variance of per-period
    Sharpe estimates over the search population (Zone B), which prices how
    wide the search's luck distribution was.
    """
    x = np.asarray(net, dtype=np.float64)
    x = x[np.isfinite(x)]
    T = x.size
    if T < 24:
        return {"state": "INSUFFICIENT_PERIODS", "dsr": None}
    sd = x.std(ddof=1)
    if sd <= 0:
        return {"state": "DEGENERATE", "dsr": None}
    sr = float(x.mean() / sd)  # per-period Sharpe
    g3 = float(((x - x.mean()) ** 3).mean() / sd ** 3)
    g4 = float(((x - x.mean()) ** 4).mean() / sd ** 4)
    n = max(int(n_trials), 1)
    v = max(float(trial_sharpe_variance), 1e-12)
    emc = 0.5772156649015329
    sr0 = math.sqrt(v) * ((1.0 - emc) * _norm_ppf(1.0 - 1.0 / n)
                          + emc * _norm_ppf(1.0 - 1.0 / (n * math.e))) \
        if n > 1 else 0.0
    denom = 1.0 - g3 * sr + (g4 - 1.0) / 4.0 * sr * sr
    if denom <= 0:
        return {"state": "DEGENERATE_MOMENTS", "dsr": None,
                "sharpe_per_period": sr, "expected_max_sharpe": sr0}
    z = (sr - sr0) * math.sqrt(T - 1.0) / math.sqrt(denom)
    return {"state": "OK", "dsr": _norm_cdf(z), "z": z,
            "sharpe_per_period": sr, "expected_max_sharpe": sr0,
            "n_trials": n, "periods": T, "skew": g3, "kurtosis": g4,
            "null": "TRUE_SHARPE_IS_ZERO_AND_THIS_IS_THE_BEST_OF_N"}


def effective_search_burden(*, stage1_screened: int,
                            campaign_id: str = C.CAMPAIGN_ID) -> dict:
    reuse = zones.reuse_summary(campaign_id)
    return {
        "stage1_candidates_screened_zone_a": int(stage1_screened),
        "distinct_candidates_scored_on_zone_b":
            reuse["distinct_candidates_on_zone_b"],
        "total_zone_b_evaluations": reuse["total_zone_b_evaluations"],
        "dsr_effective_trials":
            max(reuse["distinct_candidates_on_zone_b"], 1),
        "rationale": "Zone A screening cannot mine Zone B or C, so the "
                     "deflation count is the distinct candidates the "
                     "SELECTION zone scored; the Zone-A count is reported "
                     "so the full funnel is visible.",
    }


def build(*, campaign_id: str, per_candidate: list, bh: dict, spa: dict,
          dsr_rows: list, burden_summary: dict) -> dict:
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "policy": C.MULTIPLE_TESTING_POLICY,
        "bh_owner": _mt.CALCULATION_OWNER,
        "benjamini_hochberg": bh,
        "superior_predictive_ability": spa,
        "deflated_sharpe": dsr_rows,
        "per_candidate": per_candidate,
        "effective_search_burden": burden_summary,
        "t_above_2_is_not_qualification": C.T_ABOVE_2_IS_NOT_QUALIFICATION,
        "rejected_candidates_stay_in_denominator": True,
    })
    body["multiple_discovery_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
