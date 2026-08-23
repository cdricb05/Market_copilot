"""alpha_agent.r41.model_scale - Track 11: the sequence-model SCALE test.

R40 left model capability open on exactly one axis: scale (TCN 2.07 >
SSM-lite 1.80 > PatchTST-lite 1.77 > ridge 1.62, all tiny models). The R40
compute-escalation request priced a GPU run; Release 41 first executes the
strongest FREE local scaling: the same from-scratch TCN family at 2-8x the
parameters (channels 32/64/128, one extra dilation level, dropout, 60
epochs), on the same frozen R39 universal state, through the same
Director protocol (fit ZONE_A / judge ZONE_B, screening on Zone A only,
ONE Zone-B run for the best config).

If the scaled family fails to beat t 2.07 materially here, the remaining
scale hypothesis is the GPU-sized one and stays a priced request
(COMPUTE_REQUIRES_OPERATOR_SPEND), with this measurement attached.
"""
from __future__ import annotations

import time

import numpy as np

from ..r39.models_ext import SEQ_N_LAGS, _Standardiser
from ..r40 import director as D
from ..r40 import model_challenge as MC
from ..r39.continuation_director import new_cand
from . import burden as BURDEN

CALCULATION_OWNER = "alpha_agent.r41.model_scale"
STAGE = "R41_MODEL_SCALE"

CONFIGS = ("c32_d3", "c64_d3", "c128_d4")
EPOCHS = 60
BATCH = 1024
R39_TCN_ZONE_B_T = 2.07


class ScaledTCN(MC._SeqBase):
    """The R39 TCN architecture family at 2-8x width and +1 dilation."""

    name = "tcn_scaled_seq"

    def _build(self, torch):
        nn = torch.nn
        f = self.n_feats
        ch = 128 if "c128" in self.cfg else (64 if "c64" in self.cfg else 32)
        dil = (1, 2, 4) if "_d4" in self.cfg else (1, 2)

        layers = []
        in_ch = f
        for d in dil:
            layers += [nn.Conv1d(in_ch, ch, kernel_size=3, padding=d,
                                 dilation=d),
                       nn.ReLU(), nn.Dropout(0.1)]
            in_ch = ch

        class TCN(nn.Module):
            def __init__(self):
                super().__init__()
                self.body = nn.Sequential(*layers)
                self.head = nn.Sequential(nn.Linear(ch, ch // 2), nn.ReLU(),
                                          nn.Linear(ch // 2, 1))

            def forward(self, x):            # (N, steps, feats)
                z = self.body(x.transpose(1, 2))
                return self.head(z[:, :, -1])
        return TCN()

    def fit(self, X, y):
        # same protocol as the R40 sequence learners, longer schedule
        old_e, old_b = MC.TORCH_EPOCHS, MC.TORCH_BATCH
        MC.TORCH_EPOCHS, MC.TORCH_BATCH = EPOCHS, BATCH
        try:
            return super().fit(X, y)
        finally:
            MC.TORCH_EPOCHS, MC.TORCH_BATCH = old_e, old_b


class Director4(MC.Director3):
    def _make_model(self, cand: dict, seed_shift: int = 0):
        if cand["model"] == "tcn_scaled_seq":
            cols = self.bundles[cand["bundle"]]
            n_feats = len(cols) // (SEQ_N_LAGS + 1)
            return ScaledTCN(n_feats, str(cand.get("hyper") or "c32_d3"),
                             seed=3903 + seed_shift)
        return super()._make_model(cand, seed_shift)


def run(*, progress=None) -> dict:
    d2 = D.session()
    d2.__class__ = Director4
    out = {"configs": {}, "epochs": EPOCHS,
           "baseline_r39_tcn_zone_b_t": R39_TCN_ZONE_B_T}

    def log(s):
        if progress:
            progress(s)

    # baseline re-score (reuse count, exactness check)
    base = new_cand("FUT", "ALL_FUT", "SEQ_CLS", "FUT:MODEL_DEEP", "tcn_seq",
                    "XS_LONG_SHORT")
    t0 = time.time()
    rep = D.zone_b(base, stage=STAGE, d2=d2)
    out["baseline_rescore"] = {**D.summarise(rep),
                               "seconds": round(time.time() - t0)}
    log("baseline TCN re-score t=%s (%.0fs)" % (
        out["baseline_rescore"].get("after_cost_excess_t_stat"),
        time.time() - t0))

    screens = {}
    for cfg in CONFIGS:
        cand = new_cand("FUT", "ALL_FUT", "SEQ_CLS", "FUT:MODEL_R41",
                        "tcn_scaled_seq", "XS_LONG_SHORT", hyper=cfg)
        t0 = time.time()
        res = d2._screen_one(cand)
        screens[cfg] = {"candidate_id": cand["candidate_id"],
                        "state": res.get("state"), "score": res.get("score"),
                        "seconds": round(time.time() - t0)}
        log("screen %s: score=%s (%.0fs)" % (cfg, res.get("score"),
                                             time.time() - t0))
    out["screens"] = screens
    ok = [(c, r) for c, r in screens.items() if r.get("score") is not None]
    if not ok:
        out["state"] = "SCREEN_FAILED"
        return out
    best_cfg = max(ok, key=lambda cr: cr[1]["score"])[0]
    cand = new_cand("FUT", "ALL_FUT", "SEQ_CLS", "FUT:MODEL_R41",
                    "tcn_scaled_seq", "XS_LONG_SHORT", hyper=best_cfg)
    spec = {"information_family": "UNIVERSAL_FUTURES_XS",
            "asset_family": "FUTURES_68", "horizon": "21s",
            "economic_expression": "XS_LONG_SHORT",
            "representation": "SEQ_CLS",
            "model": "TCN_SCALED_%s" % best_cfg,
            "hyperparameter_budget": len(CONFIGS),
            "parent_hypotheses": ["R39 TCN t 2.07; R40 scale axis open"],
            "validation_touches": 1}
    cid = BURDEN.record_zone_b(spec, family="MODEL_FAMILY")
    t0 = time.time()
    rep = D.zone_b(cand, stage=STAGE, d2=d2)
    out["best_cfg"] = best_cfg
    out["burden_candidate_id"] = cid
    out["zone_b"] = {**D.summarise(rep), "seconds": round(time.time() - t0)}
    out["halves"] = D.halves_same_sign(rep)
    t_new = out["zone_b"].get("after_cost_excess_t_stat")
    t_base = out["baseline_rescore"].get("after_cost_excess_t_stat") \
        or R39_TCN_ZONE_B_T
    out["improves_baseline"] = (t_new is not None and t_base is not None
                                and t_new > t_base + 0.1)
    out["state"] = "OK"
    log("BEST %s Zone-B t=%s vs baseline %s -> improves=%s" % (
        best_cfg, t_new, t_base, out["improves_baseline"]))
    return out
