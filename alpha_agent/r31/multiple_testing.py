"""alpha_agent.r31.multiple_testing - campaign-wide multiple-testing control.

Search hundreds of candidates against noisy financial data and the best one will
look good whether or not any of them has an edge. This module is the correction,
and it takes its denominator from the candidate registry rather than from a
curated shortlist, so a candidate cannot improve the campaign's statistics by
being forgotten.

Three complementary controls, because they fail differently:

* **Benjamini-Hochberg FDR** over every executed candidate's own t-statistic.
  Cheap, and it answers "how many of these could be noise?".
* **Hansen's Superior Predictive Ability (SPA)**, computed with a stationary
  bootstrap over the per-period excess-return series. Answers the harder
  question - "is the BEST candidate better than the benchmark, once I account
  for having picked the best of many?" - which BH does not.
* **Paired block bootstrap** of the candidate-minus-incumbent economic
  difference. The two are evaluated on the same dates, so pairing removes the
  common market component and tests the thing the campaign actually cares about:
  net economics relative to the model the operator runs today.

No scipy. The normal CDF is ``math.erf``; the bootstrap is
``numpy.random.default_rng`` with a seed recorded in the campaign contract, so
every p-value here is reproducible.
"""
from __future__ import annotations

import math

import numpy as np

from .. import r31
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r31.multiple_testing"
SCHEMA = "r31_multiple_testing_results/1"
ARTIFACT_NAME = "multiple_testing_results.json"


def normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(float(z) / math.sqrt(2.0)))


def two_sided_p(t: float) -> float:
    if not math.isfinite(t):
        return 1.0
    return float(2.0 * (1.0 - normal_cdf(abs(float(t)))))


# --------------------------------------------------------------------------- #
# Benjamini-Hochberg
# --------------------------------------------------------------------------- #
def benjamini_hochberg(p_values: list, q: float = _contract.FDR_Q) -> dict:
    """BH step-up. Returns the rejection threshold and the rejected indices."""
    items = [(float(p), i) for i, p in enumerate(p_values)
             if p is not None and math.isfinite(float(p))]
    m = len(items)
    if m == 0:
        return {"m": 0, "q": q, "threshold": None, "rejected": [],
                "n_rejected": 0}
    items.sort()
    thresh = None
    cut = -1
    for rank, (p, _i) in enumerate(items, start=1):
        if p <= q * rank / m:
            thresh = p
            cut = rank
    rejected = [i for _p, i in items[:cut]] if cut > 0 else []
    return {"m": m, "q": float(q), "threshold": thresh,
            "rejected": sorted(rejected), "n_rejected": len(rejected)}


# --------------------------------------------------------------------------- #
# Stationary bootstrap (Politis & Romano 1994)
# --------------------------------------------------------------------------- #
def stationary_bootstrap_indices(n: int, *, block_mean: float, rng) -> np.ndarray:
    """One resampled index path preserving short-range serial dependence."""
    if n <= 0:
        return np.zeros(0, dtype=np.int64)
    p = 1.0 / max(float(block_mean), 1.0)
    idx = np.empty(n, dtype=np.int64)
    cur = int(rng.integers(0, n))
    for i in range(n):
        idx[i] = cur
        if rng.random() < p:
            cur = int(rng.integers(0, n))
        else:
            cur = (cur + 1) % n
    return idx


def paired_block_bootstrap(diff: np.ndarray, *,
                           resamples: int = _contract.BOOTSTRAP_RESAMPLES,
                           block_mean: float = _contract.BOOTSTRAP_BLOCK_MEAN,
                           seed: int = _contract.SEEDS["bootstrap"]) -> dict:
    """One-sided bootstrap p-value for ``mean(diff) <= 0``.

    ``diff`` is the per-period candidate-minus-incumbent net excess return, on
    the SAME dates. Resampling under the recentred null preserves the dependence
    structure a plain t-test would ignore.
    """
    d = np.asarray(diff, dtype=np.float64)
    d = d[np.isfinite(d)]
    n = d.size
    if n < 8:
        return {"n": int(n), "mean": None, "p_value": None,
                "state": "INSUFFICIENT_PERIODS"}
    obs = float(d.mean())
    centred = d - obs
    rng = np.random.default_rng(int(seed))
    ge = 0
    for _ in range(int(resamples)):
        idx = stationary_bootstrap_indices(n, block_mean=block_mean, rng=rng)
        if float(centred[idx].mean()) >= obs:
            ge += 1
    return {"n": int(n), "mean": obs,
            "p_value": float((ge + 1) / (int(resamples) + 1)),
            "resamples": int(resamples), "block_mean": float(block_mean),
            "state": "OK"}


# --------------------------------------------------------------------------- #
# Hansen SPA
# --------------------------------------------------------------------------- #
def superior_predictive_ability(series: dict, *,
                                resamples: int = _contract.BOOTSTRAP_RESAMPLES,
                                block_mean: float = _contract.BOOTSTRAP_BLOCK_MEAN,
                                seed: int = _contract.SEEDS["bootstrap"]) -> dict:
    """Hansen (2005) SPA over every candidate's excess-return series.

    ``series`` maps candidate_id -> per-period excess return over the benchmark,
    all on the SAME dates. The null is that NO candidate beats the benchmark. A
    small p-value says the best candidate's performance is not explained by
    having searched this many.
    """
    ids = [k for k, v in series.items()
           if v is not None and np.isfinite(np.asarray(v, dtype=np.float64)).sum() >= 8]
    if not ids:
        return {"n_candidates": 0, "p_value": None, "state": "NO_SERIES"}
    L = min(int(np.asarray(series[k], dtype=np.float64).size) for k in ids)
    D = np.vstack([np.asarray(series[k], dtype=np.float64)[:L] for k in ids])
    D = np.where(np.isfinite(D), D, 0.0)
    n = D.shape[1]
    means = D.mean(axis=1)
    sd = D.std(axis=1, ddof=1)
    sd = np.where(sd > 0, sd, np.inf)
    t_obs = math.sqrt(n) * float(np.max(means / sd))

    rng = np.random.default_rng(int(seed))
    # Hansen's recentring: only candidates that are not badly inferior
    # contribute to the null, which is what makes SPA less conservative than a
    # plain Reality Check without letting a hopeless candidate inflate it.
    thresh = -sd * math.sqrt(2.0 * math.log(math.log(max(n, 3))) / n)
    g = np.where(means >= thresh, means, 0.0)
    ge = 0
    for _ in range(int(resamples)):
        idx = stationary_bootstrap_indices(n, block_mean=block_mean, rng=rng)
        bm = D[:, idx].mean(axis=1) - g
        t_b = math.sqrt(n) * float(np.max(bm / sd))
        if t_b >= t_obs:
            ge += 1
    best = ids[int(np.argmax(means / sd))]
    return {"n_candidates": len(ids), "periods": int(n),
            "t_observed": t_obs, "best_candidate": best,
            "p_value": float((ge + 1) / (int(resamples) + 1)),
            "resamples": int(resamples), "block_mean": float(block_mean),
            "null": "NO_CANDIDATE_BEATS_THE_BENCHMARK", "state": "OK"}


# --------------------------------------------------------------------------- #
# Campaign artifact
# --------------------------------------------------------------------------- #
def build(*, campaign_id: str = _contract.CAMPAIGN_ID, denominator: int,
          per_candidate: list, spa: dict, paired: dict,
          bh: dict) -> dict:
    body = {
        "contract": SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "policy": _contract.MULTIPLE_TESTING_POLICY,
        "denominator_executed_candidates": int(denominator),
        "denominator_source": "alpha_agent.r31.registry append-only log",
        "rejected_candidates_stay_in_denominator": True,
        "benjamini_hochberg": bh,
        "superior_predictive_ability": spa,
        "paired_vs_incumbent": paired,
        "per_candidate": per_candidate,
        "seeds": {"bootstrap": _contract.SEEDS["bootstrap"]},
    }
    body["multiple_testing_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r31.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r31.write_json(path_for(body["campaign_id"]), body)
