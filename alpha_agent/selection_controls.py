"""
alpha_agent.selection_controls - Stage 9.4 Track D: selection-bias and
multiple-testing controls for an EXPANDING candidate catalogue.

As the pre-registered factor catalogue grows, the probability of a spuriously
"significant" candidate grows with it. These pure, deterministic, stdlib-only
controls quantify and discount that risk so a candidate is never retained merely
because it was the best of a large search:

  * purged, embargoed walk-forward folds (train observations whose label window
    overlaps - or sits inside the embargo around - a test fold are removed, so a
    neighbouring observation never leaks across the split);
  * circular block bootstrap confidence intervals (deterministic under a fixed
    seed; blocks preserve serial dependence);
  * family-level multiple-testing correction (Bonferroni + Benjamini-Hochberg
    FDR) whose adjustment strictly increases with the family size;
  * a deflated / probabilistic Sharpe ratio that discounts for the number of
    trials and the sample length;
  * a single, once-usable final holdout ledger (a reserved holdout can never be
    reused, and there is exactly one);
  * experiment-family budgets + a selection-path record and a search-size
    penalty.

Byte-reproducible: no clock read; the only randomness is an explicitly seeded
``random.Random`` used by the block bootstrap.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
from pathlib import Path
from typing import Optional, Sequence

from . import experiment_contracts as ec

__all__ = [
    "purged_walk_forward_folds", "verify_no_leakage", "block_bootstrap_means",
    "block_bootstrap_ci", "bonferroni_adjust", "benjamini_hochberg",
    "selection_adjusted_pvalue", "deflated_sharpe_ratio",
    "probabilistic_sharpe_ratio", "expected_max_sharpe", "selection_penalty",
    "ExperimentFamilyBudget", "HoldoutLedger", "selection_path_record",
]


# --------------------------------------------------------------------------- #
# Purged, embargoed walk-forward cross-validation.
# --------------------------------------------------------------------------- #
def purged_walk_forward_folds(n_periods: int, *, n_folds: int = 4,
                              embargo: int = 1, purge: int = 1,
                              label_horizon: int = 1,
                              test_size: Optional[int] = None,
                              min_train: Optional[int] = None) -> list[dict]:
    """GENUINE chronological (anchored / forward-only) walk-forward folds.

    Test folds are contiguous blocks that step FORWARD through the sample. For
    every fold, training uses ONLY periods strictly BEFORE that fold's test
    block, with the ``g = purge + (label_horizon - 1)`` periods immediately
    preceding the test start removed (``purged``) so that no training
    observation's forward label window (of ``label_horizon`` periods) can reach
    into - or sit inside the purge gap before - the test block. Because every
    training index is ``< min(test) - g`` and the fold geometry is front-anchored
    (``seed = test_size + g`` and a fixed ``test_size``, both independent of any
    period AFTER a fold's test block), appended FUTURE data can only ADD later
    folds - it can never alter an earlier fold. Folds with fewer than
    ``min_train`` eligible prior periods are OMITTED honestly (their id is not
    reused). Returns one dict per RETAINED fold:
    ``{fold, test, train, purged, embargoed}`` (index lists), the original ``fold``
    id preserved so identities are deterministic."""
    n = int(n_periods)
    k = max(1, int(n_folds))
    g = max(0, int(purge)) + max(0, int(label_horizon) - 1)  # pre-test gap
    w = int(test_size) if (test_size and int(test_size) > 0) else max(1, n // (k + 1))
    seed = w + g                       # first test starts after w+g history
    min_tr = int(min_train) if min_train is not None else 1
    min_tr = max(1, min_tr)
    folds: list[dict] = []
    m = 0
    while True:
        lo = seed + m * w
        hi = lo + w
        if hi > n:                     # the test block must fully fit in [0, n)
            break
        cut = max(0, lo - g)
        train = list(range(0, cut))
        purged = list(range(cut, lo))
        embargoed = list(range(hi, min(n, hi + max(0, int(embargo)))))
        if len(train) < min_tr:        # insufficient prior history -> skip
            m += 1
            continue
        folds.append({"fold": m, "test": list(range(lo, hi)),
                      "train": train, "purged": purged, "embargoed": embargoed})
        m += 1
    return folds


def verify_no_leakage(folds: Sequence[dict], *, embargo: int = 1,
                      purge: int = 1, label_horizon: int = 1) -> bool:
    """True iff every fold is genuinely forward-only: every training index is
    strictly before its test block AND respects the ``purge + (label_horizon-1)``
    gap, train/test are disjoint, and no embargoed index leaks into training.
    Used by the leakage test and as a runtime assertion."""
    g = max(0, int(purge)) + max(0, int(label_horizon) - 1)
    for f in folds:
        test = f.get("test", [])
        if not test:
            continue
        lo = min(test)
        train = f.get("train", [])
        if set(test) & set(train):
            return False
        if train:
            if max(train) >= lo:            # a future/overlapping training index
                return False
            if max(train) > lo - 1 - g:     # purge + label-window gap violated
                return False
        if set(f.get("embargoed", [])) & set(train):
            return False
    return True


# --------------------------------------------------------------------------- #
# Deterministic circular block bootstrap.
# --------------------------------------------------------------------------- #
def _resample_block_means(series: list[float], *, block_size: int,
                          n_resamples: int, seed: int) -> list[float]:
    n = len(series)
    rng = random.Random(int(seed))
    bs = max(1, int(block_size))
    means: list[float] = []
    n_blocks = max(1, math.ceil(n / bs))
    for _ in range(int(n_resamples)):
        sample: list[float] = []
        for _b in range(n_blocks):
            start = rng.randrange(n)
            for o in range(bs):
                sample.append(series[(start + o) % n])
        sample = sample[:n]
        means.append(sum(sample) / len(sample))
    return means


def block_bootstrap_means(series: Sequence[float], *, block_size: int = 4,
                          n_resamples: int = 1000, seed: int = 12345
                          ) -> list[float]:
    """Deterministic circular-block-bootstrap distribution of the mean. Same
    ``seed`` -> byte-identical output (blocks preserve serial dependence)."""
    vals = ec._finite(series)
    if not vals:
        return []
    return _resample_block_means(vals, block_size=block_size,
                                 n_resamples=n_resamples, seed=seed)


def block_bootstrap_ci(series: Sequence[float], *, block_size: int = 4,
                       n_resamples: int = 1000, seed: int = 12345,
                       alpha: float = 0.05) -> dict:
    """Percentile CI of the mean from the deterministic block bootstrap.
    Returns ``{mean, lo, hi, positive_fraction, n_resamples, seed}``."""
    means = block_bootstrap_means(series, block_size=block_size,
                                  n_resamples=n_resamples, seed=seed)
    if not means:
        return {"mean": None, "lo": None, "hi": None,
                "positive_fraction": None, "n_resamples": 0, "seed": seed}
    s = sorted(means)
    m = len(s)
    lo_i = max(0, int((alpha / 2.0) * m) - 0)
    hi_i = min(m - 1, int((1.0 - alpha / 2.0) * m))
    pos = sum(1 for x in means if x > 0) / m
    return {"mean": sum(means) / m, "lo": s[lo_i], "hi": s[hi_i],
            "positive_fraction": pos, "n_resamples": m, "seed": seed}


# --------------------------------------------------------------------------- #
# Multiple-testing correction (family-level).
# --------------------------------------------------------------------------- #
def bonferroni_adjust(pvalue: float, family_size: int) -> float:
    """Bonferroni-adjusted p-value; strictly non-decreasing in family size."""
    p = max(0.0, min(1.0, float(pvalue)))
    m = max(1, int(family_size))
    return min(1.0, p * m)


def benjamini_hochberg(pvalues: Sequence[float]) -> list[float]:
    """Benjamini-Hochberg FDR-adjusted q-values (monotone, in [0,1]), returned
    in the input order."""
    ps = [max(0.0, min(1.0, float(p))) for p in pvalues]
    m = len(ps)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: ps[i])
    q = [0.0] * m
    prev = 1.0
    for rank in range(m - 1, -1, -1):
        i = order[rank]
        val = ps[i] * m / (rank + 1)
        prev = min(prev, val)
        q[i] = min(1.0, prev)
    return q


def selection_adjusted_pvalue(pvalue: float, *, family_size: int,
                              method: str = "bonferroni") -> float:
    """Selection-adjusted p-value for a candidate chosen as the best of a family
    of ``family_size`` variants. Increases with the family size (the price of a
    larger search)."""
    if method == "sidak":
        p = max(0.0, min(1.0, float(pvalue)))
        m = max(1, int(family_size))
        return 1.0 - (1.0 - p) ** m
    return bonferroni_adjust(pvalue, family_size)


def selection_penalty(family_size: int, *, ref_size: int = 1) -> float:
    """A [0,1] multiplicative penalty that shrinks a candidate's credit as the
    size of the search family it was selected from grows (1.0 for a single
    pre-registered hypothesis, decreasing thereafter)."""
    m = max(1, int(family_size))
    r = max(1, int(ref_size))
    if m <= r:
        return 1.0
    return 1.0 / math.sqrt(m / r)


# --------------------------------------------------------------------------- #
# Deflated / probabilistic Sharpe ratio.
# --------------------------------------------------------------------------- #
def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """Inverse standard-normal CDF (Acklam's rational approximation)."""
    if p <= 0.0:
        return -1e9
    if p >= 1.0:
        return 1e9
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                 + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])\
        * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


_EULER = 0.5772156649015329


def expected_max_sharpe(n_trials: int) -> float:
    """Expected maximum of ``n_trials`` independent standard normals (in Z / SR-
    estimator-standard-error units). Scale by the Sharpe estimator's standard
    error to get the deflation benchmark in Sharpe units."""
    m = max(1, int(n_trials))
    if m == 1:
        return 0.0
    return ((1 - _EULER) * _norm_ppf(1 - 1.0 / m)
            + _EULER * _norm_ppf(1 - 1.0 / (m * math.e)))


def _sharpe_estimator_se(sharpe: float, *, n_obs: int, skew: float,
                         kurtosis: float) -> Optional[float]:
    """Standard error of the (per-period) Sharpe estimator, adjusted for
    non-normal returns (Mertens / Bailey-Lopez de Prado)."""
    n = int(n_obs)
    if n < 2:
        return None
    sr = float(sharpe)
    var = 1.0 - skew * sr + ((kurtosis - 1.0) / 4.0) * sr * sr
    if var <= 0:
        return None
    return math.sqrt(var / (n - 1))


def probabilistic_sharpe_ratio(sharpe: float, *, n_obs: int,
                               benchmark_sr: float = 0.0,
                               skew: float = 0.0, kurtosis: float = 3.0
                               ) -> Optional[float]:
    """P(true SR > benchmark_sr) given the estimated (per-period) Sharpe, sample
    length and higher moments. In [0,1]."""
    n = int(n_obs)
    if n < 2:
        return None
    sr = float(sharpe)
    denom = 1.0 - skew * sr + ((kurtosis - 1.0) / 4.0) * sr * sr
    if denom <= 0:
        return None
    z = (sr - benchmark_sr) * math.sqrt(n - 1) / math.sqrt(denom)
    return _norm_cdf(z)


def deflated_sharpe_ratio(sharpe: float, *, n_obs: int, n_trials: int,
                          skew: float = 0.0, kurtosis: float = 3.0) -> dict:
    """Deflated Sharpe ratio: the probabilistic Sharpe evaluated against the
    expected maximum Sharpe from ``n_trials`` trials, so a candidate selected
    from many trials must clear a HIGHER bar. Returns the raw PSR (vs 0), the
    deflation benchmark, and the deflated probability."""
    psr0 = probabilistic_sharpe_ratio(sharpe, n_obs=n_obs, benchmark_sr=0.0,
                                      skew=skew, kurtosis=kurtosis)
    se = _sharpe_estimator_se(sharpe, n_obs=n_obs, skew=skew, kurtosis=kurtosis)
    z_max = expected_max_sharpe(n_trials)
    # Deflation benchmark in Sharpe units = SR-estimator SE * expected max Z.
    sr0 = None if se is None else se * z_max
    dsr = (None if sr0 is None else
           probabilistic_sharpe_ratio(sharpe, n_obs=n_obs, benchmark_sr=sr0,
                                       skew=skew, kurtosis=kurtosis))
    return {"probabilistic_sharpe": psr0,
            "expected_max_sharpe_z": z_max,
            "sharpe_estimator_se": se,
            "deflation_benchmark_sr": sr0,
            "deflated_sharpe": dsr, "n_trials": int(n_trials),
            "n_obs": int(n_obs)}


# --------------------------------------------------------------------------- #
# Experiment-family budgets + selection path.
# --------------------------------------------------------------------------- #
class ExperimentFamilyBudget:
    """Bounds how many variants a hypothesis family may spend and counts what was
    actually considered (so the selection-family size is auditable and the
    multiple-testing correction is honest)."""

    def __init__(self, budgets: Optional[dict] = None, *, default_budget: int = 3):
        self._budgets = dict(budgets or {})
        self._default = int(default_budget)
        self._considered: dict[str, int] = {}

    def budget(self, family: str) -> int:
        return int(self._budgets.get(family, self._default))

    def considered(self, family: str) -> int:
        return int(self._considered.get(family, 0))

    def can_consider(self, family: str) -> bool:
        return self.considered(family) < self.budget(family)

    def record_consideration(self, family: str) -> bool:
        """Record one variant considered in ``family``. Returns False (and does
        not increment) once the family budget is exhausted."""
        if not self.can_consider(family):
            return False
        self._considered[family] = self.considered(family) + 1
        return True

    def summary(self) -> dict:
        return {f: {"considered": self.considered(f), "budget": self.budget(f)}
                for f in sorted(set(self._considered) | set(self._budgets))}


def selection_path_record(*, candidate_id: str, family: str,
                          family_size: int, variants_considered: int,
                          raw_stat: Optional[float] = None,
                          raw_pvalue: Optional[float] = None,
                          selected_reason: str = "best_of_family") -> dict:
    """A durable, auditable record of HOW a candidate was selected - the family
    it belongs to, how many variants were considered, its raw statistic and the
    selection-adjusted statistic + search-size penalty. Recorded on every
    lifecycle decision so no candidate's provenance is hidden."""
    fam_size = max(1, int(family_size))
    adj_p = (selection_adjusted_pvalue(raw_pvalue, family_size=fam_size)
             if raw_pvalue is not None else None)
    return {
        "candidate_id": candidate_id, "family": family,
        "search_family_size": fam_size,
        "variants_considered": int(variants_considered),
        "selected_reason": selected_reason,
        "raw_statistic": raw_stat, "raw_pvalue": raw_pvalue,
        "selection_adjusted_pvalue": adj_p,
        "search_size_penalty": selection_penalty(fam_size),
    }


# --------------------------------------------------------------------------- #
# Single, once-usable final holdout ledger.
# --------------------------------------------------------------------------- #
class HoldoutReuseError(RuntimeError):
    """Raised when the reserved final holdout is used a second time, or a second
    distinct final holdout is reserved."""


class HoldoutLedger:
    """A durable JSON ledger enforcing EXACTLY ONE final holdout that can be
    consumed AT MOST ONCE. Reserving is idempotent for the same key; reserving a
    different key, or using an already-used holdout, raises. Prevents the final
    holdout from being silently re-mined across cycles."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return {}
        return {}

    def _write(self, doc: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(doc, sort_keys=True, separators=(",", ":")),
                       encoding="utf-8")
        tmp.replace(self.path)

    def reserve(self, holdout_key: str, *, description: str = "",
                reserved_at: str = "") -> dict:
        doc = self._load()
        cur = doc.get("holdout")
        if cur is not None:
            if cur.get("key") != holdout_key:
                raise HoldoutReuseError(
                    "a different final holdout is already reserved: %s"
                    % cur.get("key"))
            return cur  # idempotent re-reserve of the same key
        rec = {"key": holdout_key, "description": description,
               "reserved_at": reserved_at, "used": False, "used_at": None,
               "fingerprint": hashlib.sha256(
                   holdout_key.encode("utf-8")).hexdigest()[:16]}
        doc["holdout"] = rec
        self._write(doc)
        return rec

    def is_reserved(self) -> bool:
        return self._load().get("holdout") is not None

    def is_used(self, holdout_key: Optional[str] = None) -> bool:
        rec = self._load().get("holdout")
        if rec is None:
            return False
        if holdout_key is not None and rec.get("key") != holdout_key:
            return False
        return bool(rec.get("used"))

    def can_use(self, holdout_key: str) -> bool:
        rec = self._load().get("holdout")
        return bool(rec and rec.get("key") == holdout_key
                    and not rec.get("used"))

    def mark_used(self, holdout_key: str, *, used_at: str = "") -> dict:
        doc = self._load()
        rec = doc.get("holdout")
        if rec is None or rec.get("key") != holdout_key:
            raise HoldoutReuseError("holdout not reserved: %s" % holdout_key)
        if rec.get("used"):
            raise HoldoutReuseError(
                "final holdout already used - cannot be reused: %s"
                % holdout_key)
        rec["used"] = True
        rec["used_at"] = used_at
        doc["holdout"] = rec
        self._write(doc)
        return rec
