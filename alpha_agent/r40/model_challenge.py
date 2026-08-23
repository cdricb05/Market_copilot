"""alpha_agent.r40.model_challenge - R40_MODEL_RESULTS (Tracks G/H/I-model).

Release 39's from-scratch TCN reaching Zone-B t 2.07 means model capability
is NOT closed. This module runs materially different challengers under the
SAME economic target / control / cost / chronology protocol (fit Zone A,
judge Zone B, Zone C unreachable, every Zone-B evaluation counted in the
cumulative ledger), and maps every output to an after-cost economic
expression - never an RMSE:

* re-scored baselines under their R39 candidate ids (ridge WIDE, ridge
  CLASSICAL, TCN, GRU, masked-AE ridge) - reuse counts, not new trials;
* STATE_SPACE  - ``ssm_lite_seq``: a diagonal linear state-space layer
  (S4D-style learnable decay kernels over the 12-step window) + MLP head,
  random init, pure torch;
* TEMPORAL_TRANSFORMER - ``patchtst_lite_seq``: 3-step patch embedding +
  one transformer encoder layer + linear head, random init;
* TEMPORAL_GRAPH - ``graph_mlp``: one-hop message passing over the
  walk-forward sparse lead-lag graph (edges re-estimated annually from
  TRAINING windows only) + MLP head over own and neighbour features;
* TABULAR_FOUNDATION - ``tabpfn_v2``: zero-shot in-context regression with
  the ungated TabPFN-v2 checkpoint (PRETRAINING_DATA_KNOWN_CLEAN);
* TIME_SERIES_FOUNDATION - chronos-bolt next-month forecast features under
  ridge and as a transparent rule (PRETRAINING_OVERLAP_LIKELY ->
  REPRESENTATION_RESEARCH only).

Search discipline: hierarchical fidelity. Each from-scratch family exposes
at most ``MAX_CONFIGS_PER_MODEL_FAMILY`` configurations; they are screened
on ZONE A ONLY (purged 3-fold CV through the v1 director's own stage-1
screen, which never touches Zone B), and exactly ONE configuration per
family - the best Zone-A screen - receives a Zone-B evaluation. No unlimited
hyper-parameter search; no Zone-C redesign.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import models_ext as MX
from ..r39.continuation_director import Director2, new_cand
from ..r39.models_ext import SEQ_N_LAGS, _Standardiser
from ..r39.representation_factory import CLASSICAL_FUT
from ..r39.wide_prosecution import WIDE_ID
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C
from . import director as D
from . import open_models as OM

CALCULATION_OWNER = "alpha_agent.r40.model_challenge"
ARTIFACT_NAME = "r40_model_results.json"
STAGE = "R40_MODELS"

R39_BASELINES = {
    "ridge_WIDE": ("FUT_WIDE", "FUT:WIDE", "ridge"),
    "ridge_CLASSICAL": ("FUT_CLASSICAL", "FUT:CLASSICAL", "ridge"),
    "lightgbm_CLASSICAL": ("FUT_CLASSICAL", "FUT:CLASSICAL", "lightgbm"),
    "tcn_seq": ("SEQ_CLS", "FUT:MODEL_DEEP", "tcn_seq"),
    "gru_seq": ("SEQ_CLS", "FUT:MODEL_DEEP", "gru_seq"),
    "ssl_embed_ridge_CLASSICAL": ("FUT_CLASSICAL", "FUT:MODEL_EXT",
                                  "ssl_embed_ridge"),
}

#: <= MAX_CONFIGS_PER_MODEL_FAMILY per family; screened on Zone A only.
CONFIGS = {
    "ssm_lite_seq": ("h16_lr1e-3", "h32_lr1e-3", "h16_lr3e-4"),
    "patchtst_lite_seq": ("d32_h2", "d48_h2", "d32_h4"),
    "graph_mlp": ("k5_h32",),
}

TORCH_EPOCHS = 40
TORCH_BATCH = 1024


def _torch():
    return MX._torch()


# --------------------------------------------------------------------------- #
# From-scratch sequence learners (same (N, n_feats, n_steps) interface)
# --------------------------------------------------------------------------- #
class _SeqBase:
    family = "DEEP_SEQUENCE"

    def __init__(self, n_feats: int, cfg: str, *, seed: int = 3903):
        self.n_feats = n_feats
        self.n_steps = SEQ_N_LAGS + 1
        self.cfg = cfg
        self.seed = seed
        self.prep = _Standardiser()
        self.lr = 3e-4 if "lr3e-4" in cfg else 1e-3

    def _seq(self, torch, Z):
        N = Z.shape[0]
        seq = Z.reshape(N, self.n_feats, self.n_steps)
        seq = np.transpose(seq, (0, 2, 1))          # (N, steps, feats)
        return torch.tensor(seq, dtype=torch.float32)

    def fit(self, X, y):
        torch = _torch()
        torch.manual_seed(self.seed)
        y = np.asarray(y, dtype=np.float64)
        Z = self.prep.fit(X)
        ys = (y - np.nanmean(y)) / (np.nanstd(y) + 1e-12)
        xt = self._seq(torch, Z)
        yt = torch.tensor(ys, dtype=torch.float32).view(-1, 1)
        self.net = self._build(torch)
        opt = torch.optim.Adam(self.net.parameters(), lr=self.lr)
        loss_fn = torch.nn.MSELoss()
        N = Z.shape[0]
        idx = np.arange(N)
        rng = np.random.default_rng(self.seed)
        self.net.train()
        for _ in range(TORCH_EPOCHS):
            rng.shuffle(idx)
            for k in range(0, N, TORCH_BATCH):
                b = idx[k: k + TORCH_BATCH]
                opt.zero_grad()
                loss = loss_fn(self.net(xt[b]), yt[b])
                loss.backward()
                opt.step()
        return self

    def predict(self, X):
        torch = _torch()
        self.net.eval()
        with torch.no_grad():
            out = self.net(self._seq(torch, self.prep.apply(X))).numpy()
        return np.asarray(out, dtype=np.float64).ravel()


class SSMLiteSeq(_SeqBase):
    """Diagonal linear state space: h_t = a * h_{t-1} + B x_t with a =
    exp(-softplus(log_dt)) per channel (S4D-style learnable decay), read
    out at the last step into an MLP."""

    name = "ssm_lite_seq"

    def _build(self, torch):
        nn = torch.nn
        f, hidden = self.n_feats, (32 if "h32" in self.cfg else 16)

        class SSM(nn.Module):
            def __init__(self):
                super().__init__()
                self.B = nn.Linear(f, hidden)
                self.log_dt = nn.Parameter(torch.linspace(-2.0, 1.0, hidden))
                self.head = nn.Sequential(nn.Linear(hidden, hidden),
                                          nn.ReLU(), nn.Linear(hidden, 1))

            def forward(self, x):            # x: (N, steps, feats)
                a = torch.exp(-torch.nn.functional.softplus(self.log_dt))
                u = self.B(x)                # (N, steps, hidden)
                h = torch.zeros(x.shape[0], hidden)
                for t in range(x.shape[1]):
                    h = a * h + u[:, t, :]
                return self.head(h)
        return SSM()


class PatchTSTLiteSeq(_SeqBase):
    """3-step patches -> linear embedding -> 1 transformer encoder layer ->
    flatten -> linear head (channel-mixing variant, declared)."""

    name = "patchtst_lite_seq"

    def _build(self, torch):
        nn = torch.nn
        f, steps = self.n_feats, self.n_steps
        d = 48 if "d48" in self.cfg else 32
        heads = 4 if "h4" in self.cfg else 2
        plen = 3
        n_patch = steps // plen

        class PTST(nn.Module):
            def __init__(self):
                super().__init__()
                self.embed = nn.Linear(f * plen, d)
                self.pos = nn.Parameter(torch.zeros(1, n_patch, d))
                self.enc = nn.TransformerEncoderLayer(
                    d_model=d, nhead=heads, dim_feedforward=2 * d,
                    dropout=0.1, batch_first=True)
                self.head = nn.Linear(d * n_patch, 1)

            def forward(self, x):            # (N, steps, feats)
                N = x.shape[0]
                p = x[:, : n_patch * plen, :].reshape(N, n_patch, plen * f)
                z = self.enc(self.embed(p) + self.pos)
                return self.head(z.reshape(N, -1))
        return PTST()


class GraphMLP:
    """MLP over own + one-hop neighbour-aggregated features (the graph
    aggregation is a panel feature computed causally; see
    ``add_graph_aggregates``)."""

    family = "TEMPORAL_GRAPH"
    name = "graph_mlp"

    def __init__(self, cfg: str = "k5_h32", *, seed: int = 3903):
        self.seed = seed
        self.hidden = 32
        self.prep = _Standardiser()

    def fit(self, X, y):
        torch = _torch()
        torch.manual_seed(self.seed)
        Z = self.prep.fit(X)
        y = np.asarray(y, dtype=np.float64)
        ys = (y - y.mean()) / (y.std() + 1e-12)
        nn = torch.nn
        self.net = nn.Sequential(nn.Linear(Z.shape[1], self.hidden),
                                 nn.ReLU(),
                                 nn.Linear(self.hidden, self.hidden // 2),
                                 nn.ReLU(), nn.Linear(self.hidden // 2, 1))
        opt = torch.optim.Adam(self.net.parameters(), lr=1e-3,
                               weight_decay=1e-4)
        loss_fn = nn.MSELoss()
        xt = torch.tensor(Z, dtype=torch.float32)
        yt = torch.tensor(ys, dtype=torch.float32).view(-1, 1)
        idx = np.arange(Z.shape[0])
        rng = np.random.default_rng(self.seed)
        for _ in range(TORCH_EPOCHS):
            rng.shuffle(idx)
            for k in range(0, Z.shape[0], TORCH_BATCH):
                b = idx[k: k + TORCH_BATCH]
                opt.zero_grad()
                loss = loss_fn(self.net(xt[b]), yt[b])
                loss.backward()
                opt.step()
        return self

    def predict(self, X):
        torch = _torch()
        self.net.eval()
        with torch.no_grad():
            out = self.net(torch.tensor(self.prep.apply(X),
                                        dtype=torch.float32)).numpy()
        return np.asarray(out, dtype=np.float64).ravel()


# --------------------------------------------------------------------------- #
# Causal graph aggregation (feature construction, training windows only)
# --------------------------------------------------------------------------- #
GRAPH_TOP_K = 5
GRAPH_WINDOW = 60
NBR_FEATURES = tuple("nbr_" + c for c in CLASSICAL_FUT)


def add_graph_aggregates(panel: pd.DataFrame, *, window: int = GRAPH_WINDOW,
                         top_k: int = GRAPH_TOP_K) -> tuple:
    """Neighbour-aggregated CLASSICAL features. Edges: for each calendar
    year, the top-k |lead-lag correlation| partners from the trailing
    ``window`` months of ret_1m ending BEFORE that year (walk-forward,
    causal). Aggregate = |corr|-weighted mean of partners' feature values
    at the same calendar month."""
    p = panel.copy()
    p["_per"] = pd.to_datetime(p["decision_date"]).dt.to_period("M")
    wide = p.pivot_table(index="_per", columns="market_id", values="ret_1m",
                         aggfunc="last").sort_index()
    periods, cols = wide.index, list(wide.columns)
    X = wide.to_numpy(dtype=float)
    n_per, n_mk = X.shape
    feats = {c: p.pivot_table(index="_per", columns="market_id", values=c,
                              aggfunc="last").reindex(index=periods,
                                                      columns=cols)
             .to_numpy(dtype=float) for c in CLASSICAL_FUT}
    out = {c: np.full((n_per, n_mk), np.nan) for c in CLASSICAL_FUT}
    edges, edge_year = None, None
    n_years = 0
    for t in range(window, n_per):
        yr = periods[t].year
        if edge_year != yr:
            edge_year = yr
            n_years += 1
            W = X[t - window: t]
            A = np.full((n_mk, n_mk), np.nan)
            for j in range(n_mk):
                xj = W[:-1, j]
                for i in range(n_mk):
                    if i == j:
                        continue
                    yi = W[1:, i]
                    m = np.isfinite(xj) & np.isfinite(yi)
                    if m.sum() < 36:
                        continue
                    if xj[m].std(ddof=1) <= 0 or yi[m].std(ddof=1) <= 0:
                        continue
                    A[i, j] = float(np.corrcoef(xj[m], yi[m])[0, 1])
            edges = []
            for i in range(n_mk):
                row = A[i]
                order = np.argsort(-np.abs(np.where(np.isfinite(row), row,
                                                    0.0)))[:top_k]
                edges.append([(int(j), abs(float(row[j]))) for j in order
                              if np.isfinite(row[j])])
        for c in CLASSICAL_FUT:
            F = feats[c]
            for i in range(n_mk):
                s = wsum = 0.0
                for j, w in edges[i]:
                    v = F[t, j]
                    if np.isfinite(v):
                        s += w * v
                        wsum += w
                out[c][t, i] = s / wsum if wsum > 0 else np.nan
    names = []
    for c in CLASSICAL_FUT:
        name = "nbr_" + c
        longf = pd.DataFrame(out[c], index=periods, columns=cols) \
            .stack().rename(name).reset_index()
        longf.columns = ["_per", "market_id", name]
        p = p.merge(longf, on=["_per", "market_id"], how="left")
        names.append(name)
    p = p.drop(columns=["_per"])
    cov = float(np.isfinite(p[names[0]].to_numpy(dtype=float)).mean())
    return p.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True), names, {"coverage": cov,
                                         "edge_years": n_years,
                                         "top_k": top_k, "window": window}


# --------------------------------------------------------------------------- #
# Director extension (model construction only)
# --------------------------------------------------------------------------- #
R40_MODELS = {"ssm_lite_seq": "STATE_SPACE (from scratch)",
              "patchtst_lite_seq": "TEMPORAL_TRANSFORMER (from scratch)",
              "graph_mlp": "TEMPORAL_GRAPH (one-hop, from scratch)",
              "tabpfn_v2": "TABULAR_FOUNDATION (zero-shot, open weights)"}


def make_r40_model(d2: Director2, cand: dict, seed_shift: int = 0):
    m = cand["model"]
    cfg = str(cand.get("hyper") or "default")
    seed = 3903 + seed_shift
    if m in ("ssm_lite_seq", "patchtst_lite_seq"):
        cols = d2.bundles[cand["bundle"]]
        n_feats = len(cols) // (SEQ_N_LAGS + 1)
        cls = SSMLiteSeq if m == "ssm_lite_seq" else PatchTSTLiteSeq
        return cls(n_feats, cfg, seed=seed)
    if m == "graph_mlp":
        return GraphMLP(cfg, seed=seed)
    if m == "tabpfn_v2":
        return OM.TabPFNAdapter(seed=seed)
    return None


class Director3(Director2):
    def _make_model(self, cand: dict, seed_shift: int = 0):
        model = make_r40_model(self, cand, seed_shift)
        if model is not None:
            return model
        return super()._make_model(cand, seed_shift)


def _upgrade(d2: Director2) -> Director3:
    """Re-class the prepared session director in place (same state, same
    bundles, same ledger binding)."""
    d2.__class__ = Director3
    return d2


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def _screen(d3: Director3, cand: dict) -> dict:
    """Zone-A purged 3-fold screen through the v1 director's own stage-1
    (never touches Zone B; not ledger-counted)."""
    res = d3._screen_one(cand)
    return {"state": res.get("state"), "score": res.get("score"),
            **{k: v for k, v in res.items() if k not in ("state", "score")
               and not isinstance(v, (list, dict))}}


def run(d2=None, campaign_id: str = CAMPAIGN_ID,
        *, run_tabpfn: bool = True, run_chronos: bool = True) -> dict:
    d3 = _upgrade(d2 or D.session())
    fut = d3.state["fut"]
    results, screens, streams = {}, {}, {}
    t_start = time.time()

    def go(key, cand, note="", role="MODEL_CAPABILITY_RESEARCH"):
        t0 = time.time()
        rep = D.zone_b(cand, stage=STAGE, d2=d3)
        row = {"candidate_id": cand["candidate_id"], "model": cand["model"],
               "bundle": cand["bundle"], "expression": cand["expression"],
               "hyper": cand.get("hyper"), "note": note,
               "evidence_role": role, "zone_b": D.summarise(rep),
               "seconds": round(time.time() - t0, 1)}
        if rep.get("state") == "OK":
            row["halves"] = D.halves_same_sign(rep)
            streams[cand["candidate_id"]] = D.stream(rep)
        results[key] = row
        D.log("  %s: t=%s (%.0fs)" % (
            key, row["zone_b"].get("after_cost_excess_t_stat"),
            row["seconds"]))
        return rep

    # --- baselines under their R39 ids (reuse counts only) ---------------- #
    for key, (bundle, family, model) in R39_BASELINES.items():
        if bundle not in d3.bundles:
            continue
        cand = new_cand("FUT", "ALL_FUT", bundle, family, model,
                        "XS_LONG_SHORT")
        go("baseline_" + key, cand, note="R39 candidate id re-scored")

    # --- from-scratch challengers: Zone-A screen, ONE Zone-B run --------- #
    for model, cfgs in CONFIGS.items():
        bundle = "SEQ_CLS" if model.endswith("_seq") else "GRAPH_AGG"
        if model == "graph_mlp":
            if "nbr_ret_3m" not in fut.columns:
                fut2, names, info = add_graph_aggregates(fut)
                d3.state["fut"] = fut2
                fut = fut2
                results["_graph_aggregation"] = info
            d3.bundles["GRAPH_AGG"] = list(CLASSICAL_FUT) + list(NBR_FEATURES)
        rows = {}
        for cfg in cfgs[: C.MAX_CONFIGS_PER_MODEL_FAMILY]:
            cand = new_cand("FUT", "ALL_FUT", bundle, "FUT:MODEL_R40", model,
                            "XS_LONG_SHORT", hyper=cfg)
            t0 = time.time()
            rows[cfg] = {"candidate_id": cand["candidate_id"],
                         **_screen(d3, cand),
                         "seconds": round(time.time() - t0, 1)}
            D.log("  screen %s/%s: %s" % (model, cfg, rows[cfg].get(
                "score")))
        screens[model] = rows
        ok = [(c, r) for c, r in rows.items() if r.get("score") is not None]
        if not ok:
            results[model] = {"state": "SCREEN_FAILED", "screens": rows}
            continue
        best_cfg = max(ok, key=lambda cr: cr[1]["score"])[0]
        cand = new_cand("FUT", "ALL_FUT", bundle, "FUT:MODEL_R40", model,
                        "XS_LONG_SHORT", hyper=best_cfg)
        go(model + "_xs", cand, note="best Zone-A screen of %d configs"
           % len(rows))
        if model != "graph_mlp":
            cand_ts = new_cand("FUT", "ALL_FUT", bundle, "FUT:MODEL_R40",
                               model, "TS_OUTRIGHT", hyper=best_cfg)
            go(model + "_ts", cand_ts, note="same config, outright")
    # graph: ridge over the same aggregated features isolates the
    # representation from the learner
    if "GRAPH_AGG" in d3.bundles:
        go("graph_agg_ridge_xs",
           new_cand("FUT", "ALL_FUT", "GRAPH_AGG", "FUT:MODEL_R40", "ridge",
                    "XS_LONG_SHORT"), note="ridge over own+neighbour features")

    # --- chronos-bolt features (contaminated -> representation only) ------ #
    if run_chronos:
        try:
            fut2, names, info = OM.chronos_features(d3.state["fut"])
            d3.state["fut"] = fut2
            results["_chronos_features"] = info
            d3.bundles["CLS_CHRONOS"] = list(CLASSICAL_FUT) + names
            d3.bundles["CHRONOS_ONLY"] = names + ["vol_63"]
            base = new_cand("FUT", "ALL_FUT", "FUT_CLASSICAL",
                            "FUT:CLASSICAL", "ridge", "XS_LONG_SHORT")
            rep_b = D.zone_b(base, stage=STAGE, d2=d3)
            var = new_cand("FUT", "ALL_FUT", "CLS_CHRONOS", "FUT:MODEL_R40",
                           "ridge", "XS_LONG_SHORT")
            rep_v = go("chronos_ridge_xs", var,
                       note="CLASSICAL + chronos forecast features",
                       role="REPRESENTATION_RESEARCH")
            from ..r39.wide_prosecution import _paired_increment
            results["chronos_ridge_xs"]["paired_increment_vs_classical"] = \
                _paired_increment(rep_b, rep_v) \
                if rep_b.get("state") == "OK" and rep_v.get("state") == "OK" \
                else {"state": "NOT_COMPARABLE"}
            go("chronos_rule_ts",
               new_cand("FUT", "ALL_FUT", "CHRONOS_ONLY", "FUT:MODEL_R40",
                        "rule:chronos_fc_z", "TS_OUTRIGHT"),
               note="sign of the zero-shot forecast z, outright",
               role="REPRESENTATION_RESEARCH")
            go("chronos_rule_xs",
               new_cand("FUT", "ALL_FUT", "CHRONOS_ONLY", "FUT:MODEL_R40",
                        "rule:chronos_fc_z", "XS_LONG_SHORT"),
               note="zero-shot forecast z ranked cross-sectionally",
               role="REPRESENTATION_RESEARCH")
        except Exception as e:  # pragma: no cover - environment
            results["chronos"] = {"state": "LANE_FAILED:%s" % type(e).__name__,
                                  "error": str(e)[:300]}

    # --- TabPFN-v2 (clean prior) ------------------------------------------- #
    if run_tabpfn:
        d3.bundles["CLS_ADMISSIBLE"] = [c for c in CLASSICAL_FUT
                                        if c != "cot_commercial_z"]
        try:
            go("tabpfn_v2_classical_xs",
               new_cand("FUT", "ALL_FUT", "CLS_ADMISSIBLE", "FUT:MODEL_R40",
                        "tabpfn_v2", "XS_LONG_SHORT"),
               note="zero-shot in-context regression, 5,000-row seeded "
                    "Zone-A context, n_estimators=1",
               role="CLEAN_HISTORICAL_OOS")
        except Exception as e:  # pragma: no cover - environment
            results["tabpfn_v2_classical_xs"] = {
                "state": "LANE_FAILED:%s" % type(e).__name__,
                "error": str(e)[:300]}

    # --- comparison table --------------------------------------------------- #
    def t_of(k):
        r = results.get(k) or {}
        return (r.get("zone_b") or {}).get("after_cost_excess_t_stat")
    ridge_t = t_of("baseline_ridge_WIDE")
    tcn_t = t_of("baseline_tcn_seq")
    table = []
    for k, r in results.items():
        if k.startswith("_") or "zone_b" not in r:
            continue
        t = t_of(k)
        table.append({"key": k, "candidate_id": r["candidate_id"],
                      "zone_b_t": t,
                      "zone_b_excess": r["zone_b"].get(
                          "after_cost_excess_annualised"),
                      "mean_ic": r["zone_b"].get("mean_ic"),
                      "evidence_role": r.get("evidence_role"),
                      "beats_ridge_wide": (t is not None and ridge_t
                                           is not None and t > ridge_t),
                      "beats_tcn": (t is not None and tcn_t is not None
                                    and t > tcn_t),
                      "materially_beats_both": (
                          t is not None and ridge_t is not None
                          and tcn_t is not None
                          and t >= max(ridge_t, tcn_t) + 0.5)})
    table.sort(key=lambda r: -(r["zone_b_t"] if r["zone_b_t"] is not None
                               else -9.9))
    body = artifact_body("r40_model_results/1", {
        "calculation_owner": CALCULATION_OWNER,
        "protocol": "fit ZONE_A / judge ZONE_B / Zone C unreachable; after-"
                    "cost excess vs RISK_MATCHED_CASH (XS) or the "
                    "vol-matched passive basket (TS); costs on traded "
                    "notional; every Zone-B evaluation ledger-counted",
        "search_discipline": {"max_configs_per_family":
                              C.MAX_CONFIGS_PER_MODEL_FAMILY,
                              "screen": "Zone-A purged 3-fold CV (v1 "
                                        "stage-1 screen), not ledger-"
                                        "counted, never touches Zone B",
                              "one_zone_b_run_per_family": True,
                              "zone_c_redesign": False},
        "families": dict(R40_MODELS),
        "zone_a_screens": screens,
        "results": results,
        "comparison": table,
        "baselines": {"ridge_wide_zone_b_t": ridge_t,
                      "tcn_zone_b_t": tcn_t,
                      "material_margin_t": 0.5},
        "best_r40_model": table[0] if table else None,
        "seconds_total": round(time.time() - t_start, 1),
        "contamination_labels_applied": True,
    })
    body["model_results_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    body["_streams"] = streams
    return body
