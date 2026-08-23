"""alpha_agent.r39.model_registry - MODEL_TECHNOLOGY_REGISTRY and adapters.

Two registries, one module, because the second is the executable subset of
the first:

* the TECHNOLOGY registry inventories the 2026 model landscape relevant to
  this campaign - licence, version, local requirements, probabilistic
  support, constraints - and classifies each family against THIS machine
  (4-core i3-10105F, 64 GB RAM, GTX 1650 4 GB, 6.6 GB free on the venv
  drive) using the frozen compute classes. A family is admitted or deferred
  for a NAMED reason; fashion admits nothing and absence from previous
  releases excludes nothing.

* the ADAPTER registry wraps every admitted family in one uniform
  fit/predict contract (NaN-aware, train-statistics-only preprocessing) so
  the director can treat models as interchangeable search atoms.

``MAY_DOWNLOAD_MODEL_WEIGHTS = False`` (contract) keeps foundation-model
weights out of this release without operator approval; that lane exits
through COMPUTE_ESCALATION_REQUEST, not through a quiet download.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.model_registry"
SCHEMA = "r39_model_technology_registry/1"
ARTIFACT_NAME = "model_technology_registry.json"
ADAPTER_ARTIFACT_NAME = "model_adapter_registry.json"


def _versions() -> dict:
    out = {}
    for mod in ("sklearn", "scipy", "xgboost", "lightgbm", "statsmodels",
                "numpy", "pandas"):
        try:
            out[mod] = __import__(mod).__version__
        except Exception:
            out[mod] = None
    return out


def technology_inventory() -> list:
    """The 2026 landscape, classified for THIS machine."""
    v = _versions()
    cpu = "CPU_FEASIBLE_NOW"
    return [
        {"family": "BASELINES", "impl": "numpy (historical mean, random "
         "walk, zero)", "licence": "BSD", "version": v["numpy"],
         "compute_class": cpu, "admitted": True,
         "probabilistic": False, "reason": "anchors; every learner must "
         "beat them"},
        {"family": "LINEAR_ROBUST", "impl": "scikit-learn Ridge/ElasticNet/"
         "HuberRegressor/LogisticRegression", "licence": "BSD-3-Clause",
         "version": v["sklearn"], "compute_class": cpu, "admitted": True,
         "probabilistic": "logistic only",
         "reason": "transparent, strong small-signal baselines"},
        {"family": "BOOSTED_TREES", "impl": "XGBoost (hist), LightGBM, "
         "sklearn HistGradientBoosting", "licence": "Apache-2.0 / MIT / BSD",
         "version": "%s / %s / %s" % (v["xgboost"], v["lightgbm"],
                                      v["sklearn"]),
         "compute_class": cpu, "admitted": True,
         "probabilistic": "quantile objectives",
         "reason": "state of the art on small/medium tabular panels; "
         "native missing-value handling matches the mask policy"},
        {"family": "BAGGED_TREES", "impl": "RandomForest / ExtraTrees",
         "licence": "BSD-3-Clause", "version": v["sklearn"],
         "compute_class": cpu, "admitted": True, "probabilistic": False,
         "reason": "variance-reduced nonlinear baseline; ensemble "
         "disagreement input"},
        {"family": "KERNEL_NEIGHBOUR", "impl": "KNeighborsRegressor "
         "(analogue retrieval)", "licence": "BSD-3-Clause",
         "version": v["sklearn"], "compute_class": cpu, "admitted": True,
         "probabilistic": False, "reason": "non-parametric analogue lens"},
        {"family": "QUANTILE_DISTRIBUTIONAL", "impl": "LightGBM quantile "
         "objective; sklearn GradientBoosting quantile", "licence": "MIT/BSD",
         "version": v["lightgbm"], "compute_class": cpu, "admitted": True,
         "probabilistic": True,
         "reason": "tail targets (Q10) and abstention support"},
        {"family": "REGIME", "impl": "sklearn GaussianMixture gate + "
         "per-regime experts; statsmodels MarkovRegression available",
         "licence": "BSD-3-Clause", "version": v["statsmodels"],
         "compute_class": cpu, "admitted": True, "probabilistic": True,
         "reason": "regime-conditional structure the flat models miss"},
        {"family": "ENSEMBLE_STACKING", "impl": "ridge stack over base "
         "adapter predictions; disagreement (std) overlay",
         "licence": "BSD-3-Clause", "version": v["sklearn"],
         "compute_class": cpu, "admitted": True, "probabilistic": False,
         "reason": "evidence-weighted model market (Track 13)"},
        {"family": "SYMBOLIC_PROGRAM_SEARCH", "impl": "native seeded "
         "expression-tree search (representation_factory)", "licence":
         "this repository", "version": "r39", "compute_class": cpu,
         "admitted": True, "probabilistic": False,
         "reason": "machine-invented features with full lineage"},
        {"family": "GAUSSIAN_PROCESS", "impl": "sklearn GPR",
         "licence": "BSD-3-Clause", "version": v["sklearn"],
         "compute_class": "NOT_WORTH_COMPUTE", "admitted": False,
         "probabilistic": True,
         "reason": "O(n^3) at 15k+ rows; sparse approximations add no "
         "expected lift over the boosted/quantile stack at this panel size"},
        {"family": "CATBOOST", "impl": "CatBoost", "licence": "Apache-2.0",
         "version": None, "compute_class": "NOT_WORTH_COMPUTE",
         "admitted": False,
         "reason": "a THIRD boosted-tree implementation duplicates two "
         "already-admitted ones; redundancy is not diversity"},
        {"family": "DEEP_SEQUENCE", "impl": "TCN / LSTM / TFT / PatchTST / "
         "Mamba-class state-space (PyTorch)", "licence": "BSD-3-Clause "
         "(torch)", "version": None,
         "compute_class": "LOCAL_GPU_SLOW_BUT_POSSIBLE", "admitted": False,
         "gpu_requirements": "GTX 1650 4GB is below common TFT/PatchTST "
         "training footprints; CPU training is feasible only for tiny nets",
         "reason": "torch (~2-4 GB installed) does not fit the venv "
         "drive's 6.6 GB free space beside the existing stack; deferred "
         "via COMPUTE_ESCALATION_REQUEST, not silently dropped"},
        {"family": "FOUNDATION_TIME_SERIES", "impl": "TimesFM-class, "
         "Chronos-class (Bolt), Moirai-class zero-shot forecasters",
         "licence": "Apache-2.0 (typ.)", "version": None,
         "compute_class": "EXTERNAL_GPU_HIGH_VALUE", "admitted": False,
         "input_limitations": "univariate or limited covariates at monthly "
         "financial cross-section granularity; zero-shot",
         "reason": "requires torch plus multi-hundred-MB weight downloads; "
         "MAY_DOWNLOAD_MODEL_WEIGHTS = False is a frozen contract flag - "
         "the lane exits through the escalation request"},
        {"family": "TABULAR_FOUNDATION", "impl": "TabPFN-class / "
         "TabICL-class in-context tabular learners", "licence":
         "varies (research licences common)", "version": None,
         "compute_class": "EXTERNAL_GPU_HIGH_VALUE", "admitted": False,
         "input_limitations": "context-length caps below this panel's "
         "training-row count; licence review required",
         "reason": "same weight-download and torch constraints"},
        {"family": "GRAPH_NEURAL", "impl": "temporal GNNs "
         "(torch-geometric)", "licence": "MIT", "version": None,
         "compute_class": "LOCAL_GPU_SLOW_BUT_POSSIBLE", "admitted": False,
         "reason": "torch constraint; the GRAPH_LEAD_LAG representation "
         "carries the relational hypothesis to admitted models instead"},
        {"family": "SELF_SUPERVISED_SEQ", "impl": "masked/contrastive "
         "sequence encoders", "licence": "n/a", "version": None,
         "compute_class": "EXTERNAL_GPU_HIGH_VALUE", "admitted": False,
         "reason": "meaningful only at data/compute scales beyond this "
         "machine; named in the escalation request"},
        {"family": "VISION_CHART", "impl": "chart-image encoders",
         "licence": "n/a", "version": None,
         "compute_class": "EXTERNAL_GPU_HIGH_VALUE", "admitted": False,
         "reason": "image rendering is CPU-cheap but useful encoders are "
         "torch models under the same constraints; deferred with the "
         "market-structure family carrying the geometric hypothesis "
         "numerically in the meantime"},
    ]


# --------------------------------------------------------------------------- #
# Adapters
# --------------------------------------------------------------------------- #
class Adapter:
    """Uniform fit/predict wrapper. Preprocessing statistics come from the
    TRAINING matrix only."""

    def __init__(self, name: str, family: str, needs_impute: bool,
                 builder, classifier: bool = False):
        self.name = name
        self.family = family
        self.needs_impute = needs_impute
        self.builder = builder
        self.classifier = classifier
        self._model = None
        self._med = None
        self._mu = None
        self._sd = None

    def _prep(self, X: np.ndarray, fit: bool) -> np.ndarray:
        X = np.asarray(X, dtype=np.float64)
        if not self.needs_impute:
            return X
        if fit:
            self._med = np.nanmedian(X, axis=0)
            self._med = np.where(np.isfinite(self._med), self._med, 0.0)
        Xi = np.where(np.isfinite(X), X, self._med)
        if fit:
            self._mu = Xi.mean(axis=0)
            sd = Xi.std(axis=0, ddof=1)
            self._sd = np.where(sd > 0, sd, 1.0)
        return (Xi - self._mu) / self._sd

    def fit(self, X, y):
        y = np.asarray(y, dtype=np.float64)
        Xp = self._prep(X, fit=True)
        self._model = self.builder()
        if self.classifier:
            yb = (y > 0).astype(int)
            if len(set(yb.tolist())) < 2:
                self._model = None
                return self
            self._model.fit(Xp, yb)
        else:
            self._model.fit(Xp, y)
        return self

    def predict(self, X) -> np.ndarray:
        if self._model is None:
            return np.zeros(len(X))
        Xp = self._prep(X, fit=False)
        if self.classifier:
            return self._model.predict_proba(Xp)[:, 1] - 0.5
        return np.asarray(self._model.predict(Xp), dtype=np.float64)


class BaselineMean:
    name = "baseline_hist_mean"
    family = "BASELINES"

    def fit(self, X, y):
        y = np.asarray(y, dtype=np.float64)
        self.mu = float(np.nanmean(y)) if np.isfinite(y).any() else 0.0
        return self

    def predict(self, X):
        return np.full(len(X), self.mu)


class RegimeGatedRidge:
    """GaussianMixture regime gate on declared observable state columns,
    with one ridge expert per regime, blended by posterior."""

    name = "gmm_regime_ridge"
    family = "REGIME"

    def __init__(self, gate_idx, n_regimes: int = 2, seed: int = 3903):
        self.gate_idx = list(gate_idx)
        self.n = n_regimes
        self.seed = seed

    def fit(self, X, y):
        from sklearn.linear_model import Ridge
        from sklearn.mixture import GaussianMixture
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64)
        self.med = np.nanmedian(X, axis=0)
        self.med = np.where(np.isfinite(self.med), self.med, 0.0)
        Xi = np.where(np.isfinite(X), X, self.med)
        self.mu = Xi.mean(axis=0)
        sd = Xi.std(axis=0, ddof=1)
        self.sd = np.where(sd > 0, sd, 1.0)
        Z = (Xi - self.mu) / self.sd
        G = Z[:, self.gate_idx]
        self.gmm = GaussianMixture(n_components=self.n,
                                   random_state=self.seed).fit(G)
        post = self.gmm.predict_proba(G)
        self.experts = []
        for k in range(self.n):
            w = post[:, k]
            m = Ridge(alpha=10.0)
            sw = w + 1e-6
            m.fit(Z, y, sample_weight=sw)
            self.experts.append(m)
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=np.float64)
        Xi = np.where(np.isfinite(X), X, self.med)
        Z = (Xi - self.mu) / self.sd
        post = self.gmm.predict_proba(Z[:, self.gate_idx])
        preds = np.column_stack([e.predict(Z) for e in self.experts])
        return (post * preds).sum(axis=1)


def make_adapter(name: str, *, seed: int = 3903, gate_idx=None):
    from sklearn.ensemble import (ExtraTreesRegressor,
                                  HistGradientBoostingRegressor,
                                  RandomForestRegressor)
    from sklearn.linear_model import (ElasticNet, HuberRegressor,
                                      LogisticRegression, Ridge)
    from sklearn.neighbors import KNeighborsRegressor
    import lightgbm as lgb
    import xgboost as xgb

    common_lgb = dict(n_estimators=300, learning_rate=0.03, num_leaves=15,
                      min_child_samples=60, subsample=0.8,
                      colsample_bytree=0.8, random_state=seed, n_jobs=4,
                      verbose=-1)
    table = {
        "baseline_hist_mean": lambda: BaselineMean(),
        "ridge": lambda: Adapter("ridge", "LINEAR_ROBUST", True,
                                 lambda: Ridge(alpha=10.0)),
        "elastic_net": lambda: Adapter(
            "elastic_net", "LINEAR_ROBUST", True,
            lambda: ElasticNet(alpha=0.001, l1_ratio=0.5, max_iter=5000)),
        "huber": lambda: Adapter("huber", "LINEAR_ROBUST", True,
                                 lambda: HuberRegressor(alpha=0.001,
                                                        max_iter=500)),
        "logistic_sign": lambda: Adapter(
            "logistic_sign", "LINEAR_ROBUST", True,
            lambda: LogisticRegression(C=0.5, max_iter=2000),
            classifier=True),
        "knn": lambda: Adapter("knn", "KERNEL_NEIGHBOUR", True,
                               lambda: KNeighborsRegressor(
                                   n_neighbors=60, weights="distance")),
        "rf": lambda: Adapter("rf", "BAGGED_TREES", False,
                              lambda: RandomForestRegressor(
                                  n_estimators=200, max_depth=6,
                                  min_samples_leaf=50, n_jobs=4,
                                  random_state=seed)),
        "extra_trees": lambda: Adapter(
            "extra_trees", "BAGGED_TREES", False,
            lambda: ExtraTreesRegressor(
                n_estimators=200, max_depth=6, min_samples_leaf=50,
                n_jobs=4, random_state=seed)),
        "hist_gbm": lambda: Adapter(
            "hist_gbm", "BOOSTED_TREES", False,
            lambda: HistGradientBoostingRegressor(
                max_iter=250, learning_rate=0.05, max_leaf_nodes=15,
                min_samples_leaf=60, random_state=seed)),
        "xgboost": lambda: Adapter(
            "xgboost", "BOOSTED_TREES", False,
            lambda: xgb.XGBRegressor(
                n_estimators=300, learning_rate=0.03, max_depth=4,
                min_child_weight=30, subsample=0.8, colsample_bytree=0.8,
                tree_method="hist", random_state=seed, n_jobs=4,
                verbosity=0)),
        "lightgbm": lambda: Adapter(
            "lightgbm", "BOOSTED_TREES", False,
            lambda: lgb.LGBMRegressor(**common_lgb)),
        "lgbm_q10": lambda: Adapter(
            "lgbm_q10", "QUANTILE_DISTRIBUTIONAL", False,
            lambda: lgb.LGBMRegressor(objective="quantile", alpha=0.10,
                                      **common_lgb)),
    }
    if name in table:
        return table[name]()
    if name == "gmm_regime_ridge":
        return RegimeGatedRidge(gate_idx or [0], seed=seed)
    raise KeyError("unknown adapter %r" % name)


#: Stage-1 screening zoo (cheap) and the wider Stage-2/3 zoo.
STAGE1_MODELS = ("ridge", "lightgbm")
STAGE2_MODELS = ("ridge", "elastic_net", "huber", "knn", "rf",
                 "extra_trees", "hist_gbm", "xgboost", "lightgbm",
                 "logistic_sign")
STAGE3_EXTRA_MODELS = ("gmm_regime_ridge", "lgbm_q10")


def build_registry(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    inv = technology_inventory()
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "inventory": inv,
        "n_families": len(inv),
        "admitted": [e["family"] for e in inv if e["admitted"]],
        "deferred": {e["family"]: e["reason"] for e in inv
                     if not e["admitted"]},
        "installed_versions": _versions(),
        "packages_installed_for_r39": [
            {"package": p, "licence": lic}
            for p, lic in C.PACKAGES_INSTALLED_FOR_R39],
        "compute_classes": list(C.COMPUTE_CLASSES),
        "machine": {"cpu": "Intel i3-10105F 4C/8T", "ram_gb": 64,
                    "gpu": "NVIDIA GTX 1650 4GB",
                    "venv_drive_free_gb": "~6",
                    "may_install_cuda": False,
                    "may_download_model_weights":
                        C.MAY_DOWNLOAD_MODEL_WEIGHTS},
    })
    body["model_technology_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)

    adapters = r39.artifact_body("r39_model_adapter_registry/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "stage1_models": list(STAGE1_MODELS),
        "stage2_models": list(STAGE2_MODELS),
        "stage3_extra_models": list(STAGE3_EXTRA_MODELS),
        "preprocessing": "train-only medians/standardisation for linear "
                         "and neighbour models; native NaN handling for "
                         "tree models; masks never filled upstream",
    })
    adapters["model_adapter_hash"] = r39.sha(adapters)
    r39.write_json(r39.campaign_dir(campaign_id) / ADAPTER_ARTIFACT_NAME,
                   adapters)
    return body
