"""alpha_agent.r40.open_models - the free model-weight frontier (Track G).

Release 39 kept ``MAY_DOWNLOAD_MODEL_WEIGHTS = False``. Release 40 changes
that research policy under TEN conditions that must ALL hold for a weight
set to be acquired (``contract.MODEL_WEIGHT_DOWNLOAD_CONDITIONS``): $0, no
subscription, no card, no provider account, no restricted-data upload, a
licence that permits local research use, no licence accepted on the
operator's behalf, storage on the research drive, provenance recorded,
and no click-through gate.

Three outputs, all artifacts:

* OPEN_MODEL_TECHNOLOGY_REGISTRY - the 2026 inventory of genuinely
  distinct intelligence architectures, each with the facts read from the
  Hugging Face model API (licence tag, gated flag, revision sha), the
  ten-condition verdict, and a SELECTION decision with a named reason.
  A small set is selected; redundancy is not diversity.
* MODEL_WEIGHT_PROVENANCE - for every acquired weight file: repository,
  revision, file, size, SHA-256, local path (research drive), licence,
  acquisition time.
* PRETRAINING_CONTAMINATION_REGISTRY - every model classified as
  PRETRAINING_DATA_KNOWN_CLEAN / OVERLAP_POSSIBLE / OVERLAP_LIKELY /
  UNKNOWN / NOT_APPLICABLE_TRAINED_FROM_SCRATCH, with the evidence, and
  the evidence ROLE it may play. A zero-shot model that may have seen
  public financial series is REPRESENTATION_RESEARCH, never
  CLEAN_HISTORICAL_OOS.

What was learned acquiring them (recorded, not hidden): the ``tabpfn``
PyPI package's default path demands an interactive Prior Labs login plus a
click-through licence for the v2.5 weights - that FAILS conditions 4 and 7
and is refused; the v2 checkpoint on the ungated Hugging Face repository
loads through an explicit local ``model_path`` with no account, under the
Prior Labs License 1.1 (Apache-2.0 + attribution on distribution; internal
benchmarking requires no attribution). Built with PriorLabs-TabPFN.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39.continuation import TORCH_LIB_DIR
from . import CAMPAIGN_ID, artifact_body, campaign_dir, research_root
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.open_models"
REGISTRY_NAME = "open_model_technology_registry.json"
PROVENANCE_NAME = "model_weight_provenance.json"
CONTAMINATION_NAME = "pretraining_contamination_registry.json"

#: Research-drive locations (C: is never used for weights or caches).
R40_LIB_DIR = research_root() / "_r40_lib"
HF_HOME = research_root() / "_hf_cache"

PACKAGES_INSTALLED = (
    ("tabpfn 8.4.0 (--no-deps, research drive)", "Apache-2.0 (package)"),
    ("chronos-forecasting 2.3.1 (--no-deps, research drive)", "Apache-2.0"),
    ("transformers 5.15.1 / tokenizers 0.22.2 / safetensors / "
     "huggingface-hub / einops / tqdm / regex (--no-deps, research drive)",
     "Apache-2.0 / MIT"),
)

#: The inventory. ``hf`` facts are those read from the HF model API on
#: 2026-08-23 (licence tag, gated flag, revision sha); ``decision`` is the
#: selection with its reason.
INVENTORY = {
    "TABULAR_FOUNDATION::TabPFN-v2-reg": {
        "family": "tabular foundation model (prior-data fitted in-context "
                  "learner, transformer over rows x features)",
        "repo": "Prior-Labs/TabPFN-v2-reg", "file": "tabpfn-v2-regressor.ckpt",
        "hf": {"license": "other (Prior Labs License 1.1 = Apache-2.0 + "
                          "attribution on distribution)", "gated": False,
               "revision": "4972a65a1b30806315c6f92499959ffbfc69a673"},
        "params_m": 11,
        "contamination_class": "PRETRAINING_DATA_KNOWN_CLEAN",
        "contamination_evidence": "pretrained exclusively on SYNTHETIC "
                                  "tabular datasets drawn from structural "
                                  "causal models (Hollmann et al., Nature "
                                  "2025, 'Accurate predictions on small "
                                  "data with a tabular foundation model'); "
                                  "no real-world series enters the prior",
        "evidence_role": "CLEAN_HISTORICAL_OOS",
        "decision": "SELECTED",
        "reason": "the only foundation-model lane whose pretraining is "
                  "provably free of the target series; zero-shot in-context "
                  "learning is a genuinely different intelligence from "
                  "every fitted learner in the R39 zoo",
        "limits": "CPU inference ~0.27 s/row at a 5,000-row context; the "
                  "Zone-A context is subsampled to 5,000 rows (seeded), "
                  "n_estimators=1 (no feature-permutation ensembling) - "
                  "declared, and part of what the result measures",
    },
    "TIME_SERIES_FOUNDATION::chronos-bolt-small": {
        "family": "time-series foundation model (T5-encoder/decoder over "
                  "patched, scaled context; direct multi-quantile head)",
        "repo": "amazon/chronos-bolt-small", "file": "model.safetensors",
        "hf": {"license": "apache-2.0", "gated": False,
               "revision": "772f3d25d38aec6d914c8949dab4462e2d46f5d8"},
        "params_m": 48,
        "contamination_class": "PRETRAINING_OVERLAP_LIKELY",
        "contamination_evidence": "the Chronos pretraining corpus is built "
                                  "from PUBLIC datasets plus synthetic "
                                  "KernelSynth/TSMixup augmentation; the "
                                  "public part (Ansari et al. 2024, "
                                  "Table/Appendix) includes the daily "
                                  "'Exchange Rate' FX dataset and the "
                                  "Monash collection with FRED-MD macro "
                                  "series - the same public series that "
                                  "feed this estate's FX futures and macro "
                                  "overlay; Bolt adds further public "
                                  "corpora",
        "evidence_role": "REPRESENTATION_RESEARCH",
        "decision": "SELECTED",
        "reason": "cheap ($0, 0.75 s per 512 forecasts on CPU) and a "
                  "materially different representation (a pretrained "
                  "forecaster's next-period distribution as a feature); "
                  "its Zone-B result can never carry a clean historical "
                  "OOS label and is labelled accordingly",
    },
    "TIME_SERIES_FOUNDATION::timesfm-2.5-200m": {
        "family": "decoder-only patched time-series foundation model",
        "repo": "google/timesfm-2.5-200m-pytorch",
        "hf": {"license": "apache-2.0", "gated": False,
               "revision": "1d952420fba87f3c6dee4f240de0f1a0fbc790e3"},
        "params_m": 200,
        "contamination_class": "PRETRAINING_OVERLAP_POSSIBLE",
        "contamination_evidence": "pretraining mixes Google Trends, "
                                  "Wikipedia pageviews, synthetic data and "
                                  "public time-series corpora (Das et al. "
                                  "2024; 2.x adds more public data); "
                                  "financial-series exposure is plausible "
                                  "and not excluded by the authors",
        "evidence_role": "REPRESENTATION_RESEARCH",
        "decision": "NOT_SELECTED_REDUNDANT",
        "reason": "same lane and same evidence role as chronos-bolt at "
                  "4x the parameters and a heavier package stack; "
                  "redundancy is not diversity",
    },
    "TIME_SERIES_FOUNDATION::moirai-2.0-R-small": {
        "family": "masked-encoder universal time-series transformer",
        "repo": "Salesforce/moirai-2.0-R-small",
        "hf": {"license": "cc-by-nc-4.0", "gated": False,
               "revision": "30f43ff08c8494f4943ae1521e9d4e94a0fbb389"},
        "contamination_class": "PRETRAINING_OVERLAP_LIKELY",
        "contamination_evidence": "pretrained on LOTSA (Woo et al. 2024), "
                                  "which contains public financial "
                                  "datasets (exchange rate, bitcoin, "
                                  "banking) alongside energy/transport",
        "evidence_role": "REPRESENTATION_RESEARCH",
        "decision": "NOT_SELECTED_REDUNDANT",
        "reason": "non-commercial licence permits research use, but the "
                  "lane is already covered by chronos-bolt and the uni2ts "
                  "stack is heavy",
    },
    "TIME_SERIES_FOUNDATION::granite-ttm-r2": {
        "family": "tiny time mixer (MLP-mixer forecaster, ~1M params)",
        "repo": "ibm-granite/granite-timeseries-ttm-r2",
        "hf": {"license": "apache-2.0", "gated": False,
               "revision": "d6a79570cac0f33d526601cd3a0fc7c80a8f9a2f"},
        "contamination_class": "PRETRAINING_OVERLAP_POSSIBLE",
        "contamination_evidence": "pretrained on public Monash/LOTSA-style "
                                  "corpora (Ekambaram et al. 2024)",
        "evidence_role": "REPRESENTATION_RESEARCH",
        "decision": "NOT_SELECTED_REDUNDANT",
        "reason": "same lane as chronos-bolt; requires the granite-tsfm "
                  "stack",
    },
    "REPRESENTATION_ENCODER::MOMENT-1-small": {
        "family": "pretrained time-series representation encoder "
                  "(masked reconstruction, T5 backbone)",
        "repo": "AutonLab/MOMENT-1-small",
        "hf": {"license": "mit", "gated": False,
               "revision": "411e288267f82cce86296dbe4d6c8bc533cc162f"},
        "contamination_class": "PRETRAINING_OVERLAP_POSSIBLE",
        "contamination_evidence": "pretrained on the Time Series Pile "
                                  "(Goswami et al. 2024), which includes "
                                  "public Monash and UCR collections",
        "evidence_role": "REPRESENTATION_RESEARCH",
        "decision": "NOT_SELECTED_THIS_RELEASE",
        "reason": "the self-supervised encoder hypothesis is carried "
                  "from scratch by R39's masked autoencoder (re-scored "
                  "in Track H); a pretrained encoder is the natural next "
                  "step if any sequence lane survives",
    },
    "TABULAR_FOUNDATION::TabICL-clf": {
        "family": "tabular in-context learner (classification only, v1)",
        "repo": "jingang/TabICL-clf",
        "hf": {"license": "bsd-3-clause", "gated": False,
               "revision": "eaf789a9b25ee8486d6f48997ba076f850bbc30b"},
        "contamination_class": "PRETRAINING_DATA_KNOWN_CLEAN",
        "contamination_evidence": "synthetic prior (Qu et al. 2025)",
        "evidence_role": "CLEAN_HISTORICAL_OOS",
        "decision": "NOT_SELECTED_REDUNDANT",
        "reason": "same synthetic-prior in-context family as TabPFN-v2, "
                  "classification-only; held as the second pass if the "
                  "TabPFN lane shows anything",
    },
    "STATE_SPACE::mamba-130m-hf": {
        "family": "selective state-space LANGUAGE model",
        "repo": "state-spaces/mamba-130m-hf",
        "hf": {"license": "(none declared)", "gated": False},
        "contamination_class": "NOT_APPLICABLE_TRAINED_FROM_SCRATCH",
        "contamination_evidence": "the published weights are a text model; "
                                  "the state-space HYPOTHESIS is executed "
                                  "from random initialisation in Track H "
                                  "(diagonal SSM-lite in pure torch)",
        "evidence_role": "MODEL_CAPABILITY_RESEARCH",
        "decision": "ARCHITECTURE_ONLY_NO_WEIGHTS",
        "reason": "no relevant pretrained weights exist for a numeric "
                  "panel; the architecture is tested from scratch",
    },
    "VISION_CHART::clip-vit-base-patch32": {
        "family": "pretrained image-text encoder (chart-vision hypothesis)",
        "repo": "openai/clip-vit-base-patch32",
        "hf": {"license": "(none declared on the hub; MIT in the source "
                          "repository)", "gated": False},
        "contamination_class": "PRETRAINING_UNKNOWN",
        "contamination_evidence": "web-scale image-text pretraining "
                                  "certainly contains rendered financial "
                                  "charts of public series; the exposure "
                                  "is unquantifiable",
        "evidence_role": "MODEL_CAPABILITY_RESEARCH",
        "decision": "NOT_SELECTED_COMPUTE_AND_EVIDENCE",
        "reason": "rendering ~30k chart images and encoding them is a "
                  "multi-hour CPU job whose evidence could never be "
                  "clean; carried into the compute-escalation request "
                  "with the market-structure family holding the "
                  "geometric hypothesis numerically",
    },
}

FROM_SCRATCH_LANES = {
    "STATE_SPACE::ssm_lite_seq": "diagonal linear state-space sequence "
                                 "learner (S4D-style decay kernels) + MLP "
                                 "head, random init, pure torch",
    "TEMPORAL_TRANSFORMER::patchtst_lite_seq": "patch embedding (3-step "
                                               "patches) + one transformer "
                                               "encoder layer + linear "
                                               "head, random init",
    "TEMPORAL_GRAPH::graph_mlp": "one-hop message passing over the "
                                 "walk-forward sparse lead-lag graph "
                                 "(causal, annually re-estimated from "
                                 "training windows) + MLP head",
}


def _paths() -> None:
    for p in (str(TORCH_LIB_DIR), str(R40_LIB_DIR)):
        if p not in sys.path:
            sys.path.insert(0, p)
    os.environ.setdefault("HF_HOME", str(HF_HOME))
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("TABPFN_ALLOW_CPU_LARGE_DATASET", "1")


def conditions_verdict(entry: dict) -> dict:
    hf = entry.get("hf") or {}
    lic = str(hf.get("license", "")).lower()
    permits = any(k in lic for k in ("apache", "mit", "bsd", "cc-by",
                                     "prior labs"))
    out = {
        "ZERO_MONETARY_COST": True,
        "NO_COMMERCIAL_SUBSCRIPTION": True,
        "NO_CREDIT_CARD": True,
        "NO_PROVIDER_ACCOUNT_REQUIRED": not bool(hf.get("gated")),
        "NO_RESTRICTED_DATA_UPLOAD": True,
        "LICENCE_PERMITS_LOCAL_RESEARCH_USE": permits,
        "NO_AUTOMATIC_LICENCE_ACCEPTANCE_ON_OPERATORS_BEHALF":
            not bool(hf.get("gated")),
        "STORAGE_ON_RESEARCH_DRIVE": True,
        "PROVENANCE_VERSION_HASH_RECORDED": bool(hf.get("revision")),
        "NOT_GATED_BEHIND_CLICK_THROUGH": not bool(hf.get("gated")),
    }
    out["all_conditions_pass"] = all(out.values())
    return out


def build_registry(campaign_id: str = CAMPAIGN_ID) -> dict:
    models = {}
    for key, e in INVENTORY.items():
        models[key] = {**{k: v for k, v in e.items()},
                       "conditions": conditions_verdict(e)}
    body = artifact_body("r40_open_model_technology_registry/1", {
        "calculation_owner": CALCULATION_OWNER,
        "policy": {"may_download_model_weights": C.MAY_DOWNLOAD_MODEL_WEIGHTS,
                   "conditions": list(C.MODEL_WEIGHT_DOWNLOAD_CONDITIONS),
                   "storage": {"packages": str(R40_LIB_DIR),
                               "hf_cache": str(HF_HOME)}},
        "inventory_date": "2026-08-23",
        "inventory_source": "Hugging Face model API (read-only, no token)",
        "models": models,
        "from_scratch_lanes": FROM_SCRATCH_LANES,
        "selected": [k for k, v in models.items()
                     if v["decision"] == "SELECTED"],
        "tabpfn_package_path_refused": {
            "what": "tabpfn 8.x default weight path requires an "
                    "interactive Prior Labs login + click-through licence "
                    "acceptance for v2.5 weights",
            "verdict": "REFUSED - fails NO_PROVIDER_ACCOUNT_REQUIRED and "
                       "NO_AUTOMATIC_LICENCE_ACCEPTANCE_ON_OPERATORS_BEHALF",
            "used_instead": "the ungated v2 checkpoint from the public "
                            "Hugging Face repository through an explicit "
                            "local model_path",
        },
        "packages_installed": [{"package": p, "licence": lic}
                               for p, lic in PACKAGES_INSTALLED],
        "attribution": "Built with PriorLabs-TabPFN (internal research "
                       "use; attribution recorded voluntarily)",
    })
    body["open_model_registry_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / REGISTRY_NAME, body,
                    immutable=False)
    return body


def acquire(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Download (if absent) every SELECTED weight set into the research
    drive cache and record provenance with SHA-256."""
    _paths()
    from huggingface_hub import hf_hub_download
    weights = {}
    for key, e in INVENTORY.items():
        if e.get("decision") != "SELECTED":
            continue
        try:
            t0 = time.time()
            files = []
            names = [e["file"]] if e["file"] != "model.safetensors" else \
                ["config.json", "model.safetensors"]
            for fname in names:
                p = hf_hub_download(e["repo"], fname, token=False,
                                    revision=e["hf"]["revision"])
                files.append({"file": fname, "path": str(p),
                              "size_bytes": int(os.path.getsize(p)),
                              "sha256": _r39.sha_file(Path(p))})
            weights[key] = {"state": "ACQUIRED", "repo": e["repo"],
                            "revision": e["hf"]["revision"],
                            "licence": e["hf"]["license"],
                            "files": files,
                            "on_research_drive": all(
                                f["path"].upper().startswith("D:")
                                for f in files),
                            "seconds": round(time.time() - t0, 1),
                            "acquired_at": time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "monetary_cost": 0.0,
                            "account_used": False,
                            "licence_accepted_on_operators_behalf": False}
        except Exception as ex:  # pragma: no cover - network
            weights[key] = {"state": "ACQUISITION_FAILED",
                            "error": str(ex)[:300]}
    body = artifact_body("r40_model_weight_provenance/1", {
        "calculation_owner": CALCULATION_OWNER,
        "weights": weights,
        "hf_cache": str(HF_HOME),
        "money_spent": 0.0,
    })
    body["provenance_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / PROVENANCE_NAME, body,
                    immutable=False)
    contamination = {k: {"contamination_class": v["contamination_class"],
                         "evidence": v["contamination_evidence"],
                         "evidence_role": v["evidence_role"],
                         "executed": v["decision"] == "SELECTED",
                         "clean_historical_oos_label_admissible":
                             v["contamination_class"] ==
                             "PRETRAINING_DATA_KNOWN_CLEAN"}
                     for k, v in INVENTORY.items()}
    for k, v in FROM_SCRATCH_LANES.items():
        contamination[k] = {"contamination_class":
                            "NOT_APPLICABLE_TRAINED_FROM_SCRATCH",
                            "evidence": v,
                            "evidence_role": "MODEL_CAPABILITY_RESEARCH",
                            "executed": True,
                            "clean_historical_oos_label_admissible": True}
    cb = artifact_body("r40_pretraining_contamination_registry/1", {
        "calculation_owner": CALCULATION_OWNER,
        "classes": list(C.PRETRAINING_CONTAMINATION_CLASSES),
        "rule": "a zero-shot model with possible training exposure to the "
                "target series is NOT fresh out-of-sample evidence; it may "
                "be REPRESENTATION_RESEARCH or MODEL_CAPABILITY_RESEARCH "
                "and never receives a clean historical-OOS label without "
                "evidence",
        "models": contamination,
    })
    cb["contamination_registry_hash"] = _r39.sha(cb)
    _r39.write_json(campaign_dir(campaign_id) / CONTAMINATION_NAME, cb,
                    immutable=False)
    return body


# --------------------------------------------------------------------------- #
# Adapters
# --------------------------------------------------------------------------- #
TABPFN_CONTEXT_ROWS = 5000
TABPFN_PREDICT_BATCH = 512


class TabPFNAdapter:
    """Zero-shot in-context regression with the ungated TabPFN-v2
    checkpoint. ``fit`` stores a seeded subsample of the training rows as
    the context; nothing is trained."""

    family = "TABULAR_FOUNDATION"
    name = "tabpfn_v2"

    def __init__(self, *, seed: int = 3903,
                 context_rows: int = TABPFN_CONTEXT_ROWS):
        self.seed = seed
        self.context_rows = context_rows

    def _model(self):
        _paths()
        from huggingface_hub import hf_hub_download
        from tabpfn import TabPFNRegressor
        e = INVENTORY["TABULAR_FOUNDATION::TabPFN-v2-reg"]
        p = hf_hub_download(e["repo"], e["file"], token=False,
                            revision=e["hf"]["revision"])
        return TabPFNRegressor(device="cpu", n_estimators=1,
                               random_state=self.seed, model_path=p,
                               ignore_pretraining_limits=True)

    def fit(self, X, y):
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64)
        rng = np.random.default_rng(self.seed)
        n = X.shape[0]
        idx = np.arange(n)
        if n > self.context_rows:
            idx = np.sort(rng.choice(n, self.context_rows, replace=False))
        self.n_context = int(idx.size)
        self.reg = self._model()
        self.reg.fit(X[idx], y[idx])
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=np.float64)
        out = np.empty(X.shape[0])
        for k in range(0, X.shape[0], TABPFN_PREDICT_BATCH):
            out[k: k + TABPFN_PREDICT_BATCH] = self.reg.predict(
                X[k: k + TABPFN_PREDICT_BATCH])
        return out


CHRONOS_CONTEXT = 64
CHRONOS_MIN_CONTEXT = 24
CHRONOS_FEATURES = ("chronos_fc_q50", "chronos_fc_mean",
                    "chronos_fc_spread", "chronos_fc_z")


def chronos_features(panel: pd.DataFrame, *, batch: int = 512) -> tuple:
    """Zero-shot next-month forecast features from chronos-bolt-small over
    each market's calendar-month ret_1m history (context <= 64 months, all
    strictly at or before the decision month - the decision month's own
    ret_1m is observable at the decision date)."""
    _paths()
    import torch
    from chronos import BaseChronosPipeline
    torch.set_num_threads(4)
    e = INVENTORY["TIME_SERIES_FOUNDATION::chronos-bolt-small"]
    pipe = BaseChronosPipeline.from_pretrained(
        e["repo"], revision=e["hf"]["revision"], torch_dtype=torch.float32)
    p = panel.copy()
    p["_per"] = pd.to_datetime(p["decision_date"]).dt.to_period("M")
    wide = p.pivot_table(index="_per", columns="market_id", values="ret_1m",
                         aggfunc="last").sort_index()
    X = wide.to_numpy(dtype=float)
    n_per, n_mk = X.shape
    q50 = np.full((n_per, n_mk), np.nan)
    mean = np.full((n_per, n_mk), np.nan)
    spread = np.full((n_per, n_mk), np.nan)
    jobs = []
    for t in range(CHRONOS_MIN_CONTEXT - 1, n_per):
        lo = max(0, t - CHRONOS_CONTEXT + 1)
        for j in range(n_mk):
            ctx = X[lo: t + 1, j]
            ctx = ctx[np.isfinite(ctx)]
            if ctx.size >= CHRONOS_MIN_CONTEXT and np.isfinite(X[t, j]):
                jobs.append((t, j, ctx))
    n_done = 0
    for k in range(0, len(jobs), batch):
        chunk = jobs[k: k + batch]
        L = max(c[2].size for c in chunk)
        arr = np.full((len(chunk), L), np.nan, dtype=np.float32)
        for i, (_, _, ctx) in enumerate(chunk):
            arr[i, L - ctx.size:] = ctx
        with torch.no_grad():
            q, m = pipe.predict_quantiles(torch.tensor(arr),
                                          prediction_length=1,
                                          quantile_levels=[0.1, 0.5, 0.9])
        q = q.numpy()[:, 0, :]
        m = m.numpy()[:, 0]
        for i, (t, j, _) in enumerate(chunk):
            q50[t, j] = q[i, 1]
            mean[t, j] = m[i]
            spread[t, j] = q[i, 2] - q[i, 0]
        n_done += len(chunk)
    frames = {"chronos_fc_q50": q50, "chronos_fc_mean": mean,
              "chronos_fc_spread": spread}
    cols = list(wide.columns)
    for name, arr in frames.items():
        longf = pd.DataFrame(arr, index=wide.index, columns=cols) \
            .stack().rename(name).reset_index()
        longf.columns = ["_per", "market_id", name]
        p = p.merge(longf, on=["_per", "market_id"], how="left")
    p["chronos_fc_z"] = p["chronos_fc_mean"] / \
        p["chronos_fc_spread"].replace(0.0, np.nan)
    p = p.drop(columns=["_per"])
    info = {"forecasts": int(n_done), "context_months": CHRONOS_CONTEXT,
            "min_context_months": CHRONOS_MIN_CONTEXT,
            "coverage": float(np.isfinite(p["chronos_fc_mean"]
                                          .to_numpy(dtype=float)).mean())}
    return p.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True), list(CHRONOS_FEATURES), info
