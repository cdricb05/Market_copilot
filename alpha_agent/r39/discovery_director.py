"""alpha_agent.r39.discovery_director - the autonomous discovery director.

Generates candidate experiments without a human enumerating hypotheses,
and keeps generation strictly separated from confirmation:

* every candidate carries full lineage (id, parents, generation method,
  information used, assets, target, expression, model, budget, zones,
  control, cost model, the reason it exists, and whether it is
  machine-generated or a human template);
* Stage 1 screens on ZONE A only, with purged contiguous-fold CV;
* Stage 2 fits on Zone A and is judged economically on ZONE B - every
  evaluation is recorded in the reuse ledger;
* Stage 3 refines survivors (hyper variants, ensembles, regime gates,
  abstention overlays) - still Zone B, still counted;
* the finalist set is FROZEN (hashes first, results later) and Zone C is
  executed exactly once per finalist through the lockbox.

Rejected candidates are never deleted; the full registry is the
multiple-discovery denominator.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C
from . import judge as J
from . import model_registry as M
from . import representation_factory as R
from . import trade_space as T
from . import zones
from .search_budget import Ledger

CALCULATION_OWNER = "alpha_agent.r39.discovery_director"
REGISTRY_SCHEMA = "r39_autonomous_candidate_registry/1"
REGISTRY_NAME = "autonomous_candidate_registry.json"
LINEAGE_NAME = "candidate_lineage.json"
DISCOVERY_NAME = "discovery_results.json"
VALIDATION_NAME = "validation_results.json"
CONFIRMATION_NAME = "locked_confirmation_results.json"
ROBUSTNESS_NAME = "robustness_results.json"

FUT_SCOPES = ("ALL_FUT", "COMMODITY", "RATES", "FX", "INTERNATIONAL_EQUITY")
MIN_TRAIN_ROWS = 400
MIN_BOOK_PERIODS = 36


def _cid(spec: dict) -> str:
    return "c39_" + r39.sha(spec)[:12]


class RuleFeature:
    """A transparent hand rule: the prediction IS one feature's value."""

    def __init__(self, idx: int):
        self.idx = idx

    def fit(self, X, y):
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=np.float64)
        return np.where(np.isfinite(X[:, self.idx]), X[:, self.idx], 0.0)


class EnsembleMean:
    """Equal-weight prediction ensemble over already-fitted members."""

    def __init__(self, members):
        self.members = members

    def fit(self, X, y):
        return self

    def predict(self, X):
        return np.nanmean(np.column_stack(
            [m.predict(X) for m in self.members]), axis=1)


class Director:
    def __init__(self, state: dict, campaign_id: str = C.CAMPAIGN_ID):
        self.state = state
        self.campaign_id = campaign_id
        self.ledger = Ledger(campaign_id)
        self.bundles: dict = {}
        self.lineage: list = []
        self.candidates: list = []
        self.results: dict = {}      # candidate_id -> per-stage results
        self.fitted: dict = {}       # candidate_id -> fitted model (stage 2+)
        self.rng = np.random.default_rng(C.SEEDS["director"])

    # ------------------------------------------------------------------ #
    # Representations
    # ------------------------------------------------------------------ #
    def prepare_representations(self) -> dict:
        fut = self.state["fut"]
        fut, auto_names, lin1 = R.generate_auto_transforms(
            fut, base_cols=R.BASE_FUT + R.MACRO_COLS, k=60,
            seed=C.SEEDS["representation"])
        fut, sym_names, lin2 = R.generate_symbolic(
            fut, base_cols=R.BASE_FUT, k=60,
            seed=C.SEEDS["representation"] + 1)
        fut, spec_names, lin3 = R.add_spectral(fut)
        fut, lg_names, lin4 = R.add_latent_and_graph(fut)
        fut, sf_names, lin5 = R.add_structure_and_fib(fut,
                                                      self.state["layer"])
        self.state["fut"] = fut.sort_values(
            ["decision_date", "market_id"]).reset_index(drop=True)

        eq = self.state["eq"]
        eq, eq_auto, lin6 = R.generate_auto_transforms(
            eq, base_cols=R.EQ_FEATURES_BASE, k=30,
            seed=C.SEEDS["representation"] + 2, id_col="security_id")
        eq, eq_sym, lin7 = R.generate_symbolic(
            eq, base_cols=R.EQ_FEATURES_BASE, k=30,
            seed=C.SEEDS["representation"] + 3)
        self.state["eq"] = eq

        self.lineage = lin1 + lin2 + lin3 + lin4 + lin5 + lin6 + lin7
        auto_sets = [auto_names[i::3] for i in range(3)]
        sym_sets = [sym_names[i::3] for i in range(3)]
        self.bundles = {
            "FUT_RAW": ["ret_1m", "ret_3m", "ret_6m", "ret_12m", "vol_63",
                        "volume_z_252", "oi_z_252"],
            "FUT_CLASSICAL": list(R.CLASSICAL_FUT),
            "FUT_CLASSICAL_MACRO": list(R.CLASSICAL_FUT) +
            [c for c in R.MACRO_COLS if c in fut.columns],
            "FUT_POSITIONING": ["cot_commercial_z", "oi_z_252",
                                "volume_z_252", "ret_3m", "vol_63"],
            "FUT_AUTO_0": auto_sets[0], "FUT_AUTO_1": auto_sets[1],
            "FUT_AUTO_2": auto_sets[2],
            "FUT_SYMBOLIC_0": sym_sets[0], "FUT_SYMBOLIC_1": sym_sets[1],
            "FUT_SYMBOLIC_2": sym_sets[2],
            "FUT_SPECTRAL": list(spec_names) + ["vol_63", "ret_1m"],
            "FUT_LATENT": [c for c in R.LATENT_FEATURES if c in fut.columns],
            "FUT_GRAPH": [c for c in R.GRAPH_FEATURES if c in fut.columns] +
            ["ret_1m", "vol_63"],
            "FUT_MSTRUCT": [c for c in R.MSTRUCT_FEATURES
                            if c in fut.columns],
            "FUT_FIB": [c for c in R.FIB_FEATURES if c in fut.columns],
            "FUT_PLACEBO": [c for c in R.PLACEBO_FEATURES
                            if c in fut.columns],
            "FUT_WIDE": (list(R.CLASSICAL_FUT)
                         + [c for c in R.MACRO_COLS if c in fut.columns]
                         + list(spec_names)
                         + [c for c in R.LATENT_FEATURES
                            if c in fut.columns]
                         + [c for c in R.GRAPH_FEATURES
                            if c in fut.columns]
                         + [c for c in R.MSTRUCT_FEATURES
                            if c in fut.columns]),
            "EQ_RAW14": list(R.EQ_FEATURES_BASE),
            "EQ_AUTO": eq_auto, "EQ_SYMBOLIC": eq_sym,
            "EQ_WIDE": list(R.EQ_FEATURES_BASE) + eq_auto[:15],
            "ETF_CLASSICAL": ["ret_1m", "ret_3m", "ret_12m", "mom_12_1",
                              "vol_63", "high_252_prox"],
            "ETF_MACRO": ["ret_3m", "mom_12_1", "vol_63"] +
            [c for c in R.MACRO_COLS],
            "VX_CLASSICAL": ["carry_slope_ann", "ret_1m", "vol_63"],
            "VX_MACRO": ["carry_slope_ann", "vix", "vix_term", "vol_63",
                         "credit_baa10y"],
        }
        return self.bundles

    # ------------------------------------------------------------------ #
    # Candidate generation
    # ------------------------------------------------------------------ #
    def _add(self, spec: dict, *, origin: str, method: str, reason: str,
             parents=()):
        spec = dict(spec)
        cid = _cid(spec)
        if any(c["candidate_id"] == cid for c in self.candidates):
            return None
        rec = {"candidate_id": cid, "parent_candidate_ids": list(parents),
               "generation_method": method, "origin": origin,
               "reason": reason, **spec}
        self.candidates.append(rec)
        self.ledger.add("CANDIDATES_GENERATED", family=spec["family"],
                        stage="GENERATED")
        return rec

    def generate_candidates(self) -> list:
        fut_bundle_family = {
            "FUT_RAW": "FUT:RAW", "FUT_CLASSICAL": "FUT:CLASSICAL",
            "FUT_CLASSICAL_MACRO": "FUT:MACRO_OVERLAY",
            "FUT_POSITIONING": "FUT:POSITIONING",
            "FUT_AUTO_0": "FUT:AUTO", "FUT_AUTO_1": "FUT:AUTO",
            "FUT_AUTO_2": "FUT:AUTO",
            "FUT_SYMBOLIC_0": "FUT:SYMBOLIC",
            "FUT_SYMBOLIC_1": "FUT:SYMBOLIC",
            "FUT_SYMBOLIC_2": "FUT:SYMBOLIC",
            "FUT_SPECTRAL": "FUT:SPECTRAL", "FUT_LATENT": "FUT:LATENT",
            "FUT_GRAPH": "FUT:GRAPH", "FUT_MSTRUCT": "FUT:MSTRUCT",
            "FUT_FIB": "FUT:FIB", "FUT_PLACEBO": "FUT:FIB_PLACEBO",
            "FUT_WIDE": "FUT:WIDE",
        }
        machine_bundles = {"FUT:AUTO", "FUT:SYMBOLIC", "FUT:LATENT",
                           "FUT:GRAPH", "FUT:SPECTRAL", "FUT:WIDE"}
        # ---- futures lane ------------------------------------------------
        for bundle, family in fut_bundle_family.items():
            for scope in FUT_SCOPES:
                for target, hor in (("tgt_excess_21", 21),
                                    ("tgt_rank_21", 21),
                                    ("tgt_excess_63", 63)):
                    if target == "tgt_excess_63" and scope == "ALL_FUT":
                        exprs = ("TS_OUTRIGHT",)
                    elif target.startswith("tgt_rank"):
                        exprs = ("XS_LONG_SHORT",)
                    else:
                        exprs = ("TS_OUTRIGHT", "XS_LONG_SHORT")
                    if target == "tgt_excess_63" and scope != "ALL_FUT":
                        continue
                    for expr in exprs:
                        if expr == "XS_LONG_SHORT" and scope in ("RATES",
                                                                 "FX"):
                            pass  # small cross-sections are allowed; the
                            # expression itself enforces >= 2 per leg
                        for model in M.STAGE1_MODELS:
                            origin = ("MACHINE_GENERATED"
                                      if family in machine_bundles
                                      else "HUMAN_TEMPLATE")
                            self._add(
                                {"lane": "FUT", "scope": scope,
                                 "target": target, "horizon": hor,
                                 "expression": expr, "model": model,
                                 "bundle": bundle, "family": family,
                                 "hyper": "default"},
                                origin=origin,
                                method="STRUCTURED_ENUMERATION",
                                reason="cover representation x scope x "
                                       "target x expression grid at "
                                       "stage-1 cost")
        # transparent hand rules (the R31-38 baselines, same zones)
        for feat, nm in (("mom_12_1", "trend"), ("carry_slope_ann", "carry"),
                         ("seasonality_next_month", "seasonality"),
                         ("cot_commercial_z", "cot")):
            for scope in ("ALL_FUT", "COMMODITY"):
                for expr in ("TS_OUTRIGHT", "XS_LONG_SHORT"):
                    self._add(
                        {"lane": "FUT", "scope": scope,
                         "target": "tgt_excess_21", "horizon": 21,
                         "expression": expr, "model": "rule:" + feat,
                         "bundle": "FUT_CLASSICAL",
                         "family": "FUT:HAND_RULE", "hyper": "none"},
                        origin="HUMAN_TEMPLATE", method="HAND_RULE_ANCHOR",
                        reason="the %s rule every learner must beat" % nm)
        # ---- VX lane -----------------------------------------------------
        for bundle in ("VX_CLASSICAL", "VX_MACRO"):
            for model in ("rule:carry_slope_ann",) + M.STAGE1_MODELS:
                self._add(
                    {"lane": "VX", "scope": "VX", "target": "tgt_excess_5",
                     "horizon": 5, "expression": "TS_OUTRIGHT",
                     "model": model, "bundle": bundle,
                     "family": "VX:TERM_STRUCTURE", "hyper": "default"},
                    origin="MACHINE_GENERATED" if model != "rule:"
                    "carry_slope_ann" else "HUMAN_TEMPLATE",
                    method="STRUCTURED_ENUMERATION",
                    reason="volatility curve information at weekly cadence")
        # ---- equity lane ---------------------------------------------------
        for bundle, family in (("EQ_RAW14", "EQ:RAW"),
                               ("EQ_AUTO", "EQ:AUTO"),
                               ("EQ_SYMBOLIC", "EQ:SYMBOLIC"),
                               ("EQ_WIDE", "EQ:WIDE")):
            for target in ("tgt_rank_21", "tgt_excess_21"):
                for model in M.STAGE1_MODELS:
                    self._add(
                        {"lane": "EQ", "scope": "US_SINGLE_NAME",
                         "target": target, "horizon": 21,
                         "expression": "XS_LONG_SHORT", "model": model,
                         "bundle": bundle, "family": family,
                         "hyper": "default"},
                        origin="MACHINE_GENERATED"
                        if family in ("EQ:AUTO", "EQ:SYMBOLIC")
                        else "HUMAN_TEMPLATE",
                        method="STRUCTURED_ENUMERATION",
                        reason="survivorship-safe single-name "
                               "cross-section")
        # ---- ETF lane ------------------------------------------------------
        if self.state["etf"] is not None and not self.state["etf"].empty:
            for bundle in ("ETF_CLASSICAL", "ETF_MACRO"):
                for expr in ("TS_OUTRIGHT", "XS_LONG_SHORT",
                             "GROUP_SPREAD"):
                    for model in M.STAGE1_MODELS:
                        self._add(
                            {"lane": "ETF", "scope": "ALL_ETF",
                             "target": "tgt_excess_21", "horizon": 21,
                             "expression": expr, "model": model,
                             "bundle": bundle, "family": "ETF:CROSS_ASSET",
                             "hyper": "default"},
                            origin="MACHINE_GENERATED",
                            method="STRUCTURED_ENUMERATION",
                            reason="credit/REIT/duration cells futures "
                                   "cannot express")
        return self.candidates

    # ------------------------------------------------------------------ #
    # Panel access helpers
    # ------------------------------------------------------------------ #
    def _panel(self, cand: dict) -> pd.DataFrame:
        p = self.state[{"FUT": "fut", "VX": "vx", "EQ": "eq",
                        "ETF": "etf"}[cand["lane"]]]
        if cand["lane"] == "FUT" and cand["scope"] != "ALL_FUT":
            p = p[p["asset_class"] == cand["scope"]]
        return p

    def _id_col(self, cand: dict) -> str:
        return "security_id" if cand["lane"] == "EQ" else "market_id"

    def _make_model(self, cand: dict, seed_shift: int = 0):
        m = cand["model"]
        if m.startswith("rule:"):
            feat = m.split(":", 1)[1]
            cols = self.bundles[cand["bundle"]]
            if feat not in cols:
                cols = list(cols) + [feat]
                self.bundles[cand["bundle"]] = cols
            return RuleFeature(cols.index(feat))
        if m == "gmm_regime_ridge":
            cols = self.bundles[cand["bundle"]]
            gate = [i for i, c in enumerate(cols)
                    if c in ("vol_63", "vix", "credit_baa10y")] or [0]
            return M.make_adapter(m, seed=C.SEEDS["models"] + seed_shift,
                                  gate_idx=gate)
        return M.make_adapter(m, seed=C.SEEDS["models"] + seed_shift)

    def _matrices(self, cand: dict, rows: pd.DataFrame):
        cols = [c for c in self.bundles[cand["bundle"]] if c in rows.columns]
        X = rows[cols].to_numpy(dtype=np.float64)
        y = rows[cand["target"]].to_numpy(dtype=np.float64)
        return X, y, cols

    def _book_from_predictions(self, cand: dict, rows: pd.DataFrame,
                               preds: np.ndarray, *,
                               cost_multiplier: float = 1.0,
                               drop_market: str = None) -> dict:
        rows = rows.copy()
        rows["_pred"] = preds
        if cand["target"].startswith("tgt_rank"):
            rows["_pred"] = rows["_pred"] - 0.5
        idc = self._id_col(cand)
        if drop_market is not None:
            rows = rows[rows[idc] != drop_market]
        fwd_col = "fwd_%d" % cand["horizon"] if cand["lane"] != "VX" \
            else "fwd_5"
        pred_m = rows.pivot_table(index="decision_date", columns=idc,
                                  values="_pred", aggfunc="last")
        fwd_m = rows.pivot_table(index="decision_date", columns=idc,
                                 values=fwd_col, aggfunc="last")
        cost = rows.groupby(idc)["cost_bps_per_side"].median()
        expr = cand["expression"]
        if expr == "TS_OUTRIGHT":
            book = T.ts_outright(pred_m, fwd_m, cost,
                                 cost_multiplier=cost_multiplier)
            ctrl = T.passive_ew_control(fwd_m, cost,
                                        cost_multiplier=cost_multiplier)
            control_net = ctrl["net"]
            control_name = "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        elif expr == "XS_LONG_SHORT":
            book = T.xs_long_short(pred_m, fwd_m, cost,
                                   cost_multiplier=cost_multiplier)
            control_net = np.zeros(len(book["net"]))
            control_name = "RISK_MATCHED_CASH"
        elif expr == "GROUP_SPREAD":
            groups: dict = {}
            for mk, grp in rows.groupby(idc)["economic_group"].first(
            ).items():
                groups.setdefault(str(grp), []).append(mk)
            book = T.group_spread(pred_m, fwd_m, cost, groups,
                                  cost_multiplier=cost_multiplier)
            control_net = np.zeros(len(book["net"]))
            control_name = "RISK_MATCHED_CASH"
        else:
            raise ValueError(expr)
        book["pred_matrix"] = pred_m
        book["fwd_matrix"] = fwd_m
        return {"book": book, "control_net": control_net,
                "control_name": control_name}

    # ------------------------------------------------------------------ #
    # Stage 1 - Zone A purged CV screen
    # ------------------------------------------------------------------ #
    def stage1_screen(self) -> list:
        out = []
        for cand in self.candidates:
            res = self._screen_one(cand)
            self.results.setdefault(cand["candidate_id"], {})["stage1"] = res
            self.ledger.add("CANDIDATES_SCREENED",
                            family=cand["family"], stage="STAGE1")
            out.append({"candidate_id": cand["candidate_id"], **res})
        return out

    def _screen_one(self, cand: dict) -> dict:
        p = self._panel(cand)
        if p is None or p.empty:
            return {"state": "NO_PANEL", "score": None}
        a = p[p["zone"] == "ZONE_A"].copy()
        dates = np.sort(a["decision_date"].unique())
        if len(dates) < 60:
            return {"state": "INSUFFICIENT_ZONE_A", "score": None}
        folds = np.array_split(dates, 3)
        rows_list, preds_list = [], []
        for k, fold in enumerate(folds):
            hold_mask = a["decision_date"].isin(fold)
            # purge one decision date on each side of the held fold
            fold_min, fold_max = fold.min(), fold.max()
            train = a[~hold_mask]
            train = train[(train["decision_date"] <
                           fold_min - np.timedelta64(25, "D")) |
                          (train["decision_date"] >
                           fold_max + np.timedelta64(25, "D"))]
            Xtr, ytr, _ = self._matrices(cand, train)
            ok = np.isfinite(ytr)
            if ok.sum() < MIN_TRAIN_ROWS and cand["lane"] != "VX":
                continue
            if ok.sum() < (40 if cand["lane"] == "VX" else 80):
                continue
            model = self._make_model(cand, seed_shift=k)
            try:
                model.fit(Xtr[ok], ytr[ok])
            except Exception:
                continue
            hold = a[hold_mask]
            Xh, _, _ = self._matrices(cand, hold)
            try:
                ph = model.predict(Xh)
            except Exception:
                continue
            rows_list.append(hold)
            preds_list.append(np.asarray(ph, dtype=np.float64))
        if not rows_list:
            return {"state": "NO_FOLD_EXECUTED", "score": None}
        rows = pd.concat(rows_list, ignore_index=True)
        preds = np.concatenate(preds_list)
        try:
            built = self._book_from_predictions(cand, rows, preds)
        except Exception as e:
            return {"state": "BOOK_FAILED:%s" % type(e).__name__,
                    "score": None}
        net = np.asarray(built["book"]["net"], dtype=np.float64)
        net = net[np.isfinite(net)]
        if net.size < MIN_BOOK_PERIODS and cand["lane"] != "VX":
            return {"state": "INSUFFICIENT_BOOK", "score": None}
        sd = net.std(ddof=1)
        score = float(net.mean() / sd) if sd > 0 else 0.0
        ic = J.rank_ic(built["book"]["pred_matrix"],
                       built["book"]["fwd_matrix"])
        return {"state": "OK", "score": score,
                "zone_a_cv_sharpe_per_period": score,
                "zone_a_cv_mean_ic": ic.get("mean_ic"),
                "periods": int(net.size)}

    # ------------------------------------------------------------------ #
    # Promotion
    # ------------------------------------------------------------------ #
    def promote_stage1(self) -> list:
        scored = [c for c in self.candidates
                  if (self.results[c["candidate_id"]]["stage1"].get("score")
                      is not None)]
        scored.sort(key=lambda c: -self.results[c["candidate_id"]][
            "stage1"]["score"])
        n_top = max(int(len(scored) * C.STAGE1_PROMOTION_FRACTION), 1)
        chosen = scored[:n_top]
        # family diversity floor: best k of every family survive
        by_family: dict = {}
        for c in scored:
            by_family.setdefault(c["family"], []).append(c)
        for fam, lst in by_family.items():
            for c in lst[:C.FAMILY_DIVERSITY_FLOOR]:
                if c not in chosen:
                    chosen.append(c)
        chosen = chosen[:C.STAGE2_MAX_CANDIDATES]
        for c in chosen:
            self.ledger.add("CANDIDATES_PROMOTED_TO_STAGE2",
                            family=c["family"], stage="STAGE2")
        return chosen

    # ------------------------------------------------------------------ #
    # Stage 2 - Zone B economic validation
    # ------------------------------------------------------------------ #
    def evaluate_on_zone(self, cand: dict, *, fit_zones: tuple,
                         eval_zone: str, cost_multiplier: float = 1.0,
                         drop_market: str = None,
                         record_reuse: bool = True) -> dict:
        p = self._panel(cand)
        fit_rows = p[p["zone"].isin(fit_zones)]
        eval_rows = p[p["zone"] == eval_zone]
        if eval_zone == "ZONE_C" and record_reuse:
            raise zones.LockboxViolation(
                "Zone C evaluations go through confirm_finalist only")
        Xtr, ytr, _ = self._matrices(cand, fit_rows)
        ok = np.isfinite(ytr)
        if ok.sum() < 80:
            return {"state": "INSUFFICIENT_FIT_ROWS"}
        model = self._make_model(cand)
        try:
            model.fit(Xtr[ok], ytr[ok])
        except Exception as e:
            return {"state": "FIT_FAILED:%s" % type(e).__name__}
        Xe, _, _ = self._matrices(cand, eval_rows)
        try:
            preds = model.predict(Xe)
        except Exception as e:
            return {"state": "PREDICT_FAILED:%s" % type(e).__name__}
        built = self._book_from_predictions(
            cand, eval_rows, preds, cost_multiplier=cost_multiplier,
            drop_market=drop_market)
        horizon = cand["horizon"]
        rep = J.judge_candidate(built["book"], built["control_net"],
                                horizon=horizon,
                                control_name=built["control_name"])
        rep["state"] = "OK"
        rep["ic"] = J.rank_ic(built["book"]["pred_matrix"],
                              built["book"]["fwd_matrix"])
        rep["book_net"] = built["book"]["net"]
        rep["book_dates"] = [str(d) for d in built["book"]["dates"]]
        if record_reuse and eval_zone == "ZONE_B":
            zones.record_zone_b(cand["candidate_id"], stage="STAGE2_3",
                                campaign_id=self.campaign_id)
        self.fitted[cand["candidate_id"]] = model
        return rep

    def stage2_validate(self, promoted: list) -> list:
        out = []
        for cand in promoted:
            rep = self.evaluate_on_zone(cand, fit_zones=("ZONE_A",),
                                        eval_zone="ZONE_B")
            self.results[cand["candidate_id"]]["stage2"] = _strip(rep)
            self.results[cand["candidate_id"]]["stage2_series"] = rep
            out.append(cand)
        return out

    # ------------------------------------------------------------------ #
    # Stage 3 - refinement: hyper variants, ensembles, gated variants
    # ------------------------------------------------------------------ #
    def stage3_refine(self, validated: list) -> list:
        def s2_t(c):
            r = self.results[c["candidate_id"]].get("stage2") or {}
            return r.get("after_cost_excess_t_stat") or -9.9

        pool = [c for c in validated
                if (self.results[c["candidate_id"]].get("stage2") or {})
                .get("state") == "OK"]
        pool.sort(key=lambda c: -s2_t(c))
        top = pool[: min(len(pool), C.STAGE3_MAX_CANDIDATES - 8)]
        new = []
        # ensembles: for each (lane, scope, target, expression) group with
        # >= 3 members in the top pool, an equal-weight prediction ensemble
        groups: dict = {}
        for c in top:
            key = (c["lane"], c["scope"], c["target"], c["expression"])
            groups.setdefault(key, []).append(c)
        for key, members in groups.items():
            if len(members) < 3:
                continue
            lane, scope, target, expr = key
            parents = [m["candidate_id"] for m in members[:5]]
            rec = self._add(
                {"lane": lane, "scope": scope, "target": target,
                 "horizon": members[0]["horizon"], "expression": expr,
                 "model": "ensemble_mean", "bundle": "ENSEMBLE:" +
                 "+".join(sorted({m["bundle"] for m in members[:5]})),
                 "family": lane + ":ENSEMBLE", "hyper": "equal_weight"},
                origin="MACHINE_GENERATED", method="ENSEMBLE_OF_SURVIVORS",
                reason="evidence-weighted model market over stage-2 "
                       "survivors", parents=parents)
            if rec is not None:
                rec["_ensemble_members"] = parents
                new.append(rec)
        # regime/abstain/quantile variants of the best few
        for c in top[:6]:
            for variant, model in (("regime", "gmm_regime_ridge"),
                                   ("quantile_floor", "lgbm_q10")):
                if c["lane"] == "VX":
                    continue
                rec = self._add(
                    {"lane": c["lane"], "scope": c["scope"],
                     "target": c["target"], "horizon": c["horizon"],
                     "expression": c["expression"], "model": model,
                     "bundle": c["bundle"],
                     "family": c["family"].split(":")[0] + ":REGIME_TAIL",
                     "hyper": variant},
                    origin="MACHINE_GENERATED",
                    method="VARIANT_OF_SURVIVOR",
                    reason="regime-gated / tail-aware variant of a "
                           "stage-2 survivor",
                    parents=[c["candidate_id"]])
                if rec is not None:
                    new.append(rec)
        promoted3 = top + new
        promoted3 = promoted3[:C.STAGE3_MAX_CANDIDATES]
        for c in promoted3:
            self.ledger.add("CANDIDATES_PROMOTED_TO_STAGE3",
                            family=c["family"], stage="STAGE3")
        # evaluate the NEW candidates on Zone B (members already have s2)
        for cand in promoted3:
            if "stage2" in self.results.get(cand["candidate_id"], {}):
                continue
            if cand["model"] == "ensemble_mean":
                rep = self._evaluate_ensemble(cand)
            else:
                rep = self.evaluate_on_zone(cand, fit_zones=("ZONE_A",),
                                            eval_zone="ZONE_B")
            self.results.setdefault(cand["candidate_id"], {})
            self.results[cand["candidate_id"]]["stage2"] = _strip(rep)
            self.results[cand["candidate_id"]]["stage2_series"] = rep
        return promoted3

    def _evaluate_ensemble(self, cand: dict, *, fit_zones=("ZONE_A",),
                           eval_zone="ZONE_B",
                           cost_multiplier: float = 1.0,
                           drop_market: str = None,
                           record_reuse: bool = True) -> dict:
        members = [next(c for c in self.candidates
                        if c["candidate_id"] == mid)
                   for mid in cand["_ensemble_members"]]
        p = self._panel(cand)
        eval_rows = p[p["zone"] == eval_zone]
        preds = []
        for m in members:
            pm = self._panel(m)
            fit_rows = pm[pm["zone"].isin(fit_zones)]
            Xtr, ytr, _ = self._matrices(m, fit_rows)
            ok = np.isfinite(ytr)
            if ok.sum() < 80:
                continue
            mm = self._make_model(m)
            try:
                mm.fit(Xtr[ok], ytr[ok])
                Xe, _, _ = self._matrices(m, eval_rows)
                pr = np.asarray(mm.predict(Xe), dtype=np.float64)
            except Exception:
                continue
            # rank-normalise so members mix on comparable scales
            s = pd.Series(pr).rank(pct=True) - 0.5
            preds.append(s.to_numpy())
        if len(preds) < 2:
            return {"state": "ENSEMBLE_UNDERPOPULATED"}
        mean_pred = np.nanmean(np.column_stack(preds), axis=1)
        built = self._book_from_predictions(
            cand, eval_rows, mean_pred, cost_multiplier=cost_multiplier,
            drop_market=drop_market)
        rep = J.judge_candidate(built["book"], built["control_net"],
                                horizon=cand["horizon"],
                                control_name=built["control_name"])
        rep["state"] = "OK"
        rep["ic"] = J.rank_ic(built["book"]["pred_matrix"],
                              built["book"]["fwd_matrix"])
        rep["book_net"] = built["book"]["net"]
        rep["book_dates"] = [str(d) for d in built["book"]["dates"]]
        if record_reuse and eval_zone == "ZONE_B":
            zones.record_zone_b(cand["candidate_id"], stage="STAGE3",
                                campaign_id=self.campaign_id)
        return rep

    # ------------------------------------------------------------------ #
    # Freeze + locked confirmation
    # ------------------------------------------------------------------ #
    def spec_hash(self, cand: dict) -> str:
        cols = list(self.bundles.get(cand["bundle"], []))
        return r39.sha({
            "candidate_id": cand["candidate_id"], "lane": cand["lane"],
            "scope": cand["scope"], "target": cand["target"],
            "horizon": cand["horizon"], "expression": cand["expression"],
            "model": cand["model"], "hyper": cand["hyper"],
            "features": cols, "refit_zones": ["ZONE_A", "ZONE_B"],
            "cost_base": C.COST_BASE,
            "cost_state": C.COST_MODEL_STATE})

    def select_finalists(self, pool: list) -> list:
        def key(c):
            r = self.results[c["candidate_id"]].get("stage2") or {}
            t = r.get("after_cost_excess_t_stat")
            return -(t if t is not None else -9.9)

        ok_pool = [c for c in pool
                   if (self.results[c["candidate_id"]].get("stage2") or {})
                   .get("state") == "OK"]
        ok_pool.sort(key=key)
        chosen, per_family = [], {}
        budget = C.MAX_LOCKBOX_CANDIDATES - 1  # one slot reserved for the
        # diversified combination book (frontier)
        for c in ok_pool:
            fam = c["family"]
            if per_family.get(fam, 0) >= C.MAX_LOCKBOX_PER_FAMILY:
                continue
            chosen.append(c)
            per_family[fam] = per_family.get(fam, 0) + 1
            if len(chosen) >= budget:
                break
        return chosen

    def freeze_finalists(self, finalists: list,
                         extra_entries: tuple = ()) -> dict:
        body = zones.freeze_finalists(
            [{"candidate_id": c["candidate_id"],
              "spec_hash": self.spec_hash(c), "family": c["family"],
              "sample": c["lane"], "horizon_sessions": c["horizon"]}
             for c in finalists] + list(extra_entries),
            campaign_id=self.campaign_id,
            selected_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            selection_basis="ZONE_B after-cost excess t-statistic with the "
                            "per-family lockbox cap; Zone C untouched")
        for c in finalists:
            self.ledger.add("FINALISTS_FROZEN", family=c["family"],
                            stage="FREEZE")
        for e in extra_entries:
            self.ledger.add("FINALISTS_FROZEN", family=e["family"],
                            stage="FREEZE")
        return body

    def _predict_for(self, cand: dict, fit_zones: tuple, eval_zone: str):
        """(eval_rows, predictions) with the model(s) fitted once - the
        cached-prediction path robustness reuses so a leave-one-market-out
        sweep does not refit anything."""
        p = self._panel(cand)
        eval_rows = p[p["zone"] == eval_zone]
        if cand["model"] == "ensemble_mean":
            members = [next(c for c in self.candidates
                            if c["candidate_id"] == mid)
                       for mid in cand["_ensemble_members"]]
            preds = []
            for m in members:
                pm = self._panel(m)
                fit_rows = pm[pm["zone"].isin(fit_zones)]
                Xtr, ytr, _ = self._matrices(m, fit_rows)
                ok = np.isfinite(ytr)
                if ok.sum() < 80:
                    continue
                mm = self._make_model(m)
                try:
                    mm.fit(Xtr[ok], ytr[ok])
                    Xe, _, _ = self._matrices(m, eval_rows)
                    pr = np.asarray(mm.predict(Xe), dtype=np.float64)
                except Exception:
                    continue
                preds.append((pd.Series(pr).rank(pct=True) - 0.5)
                             .to_numpy())
            if len(preds) < 2:
                return eval_rows, None
            return eval_rows, np.nanmean(np.column_stack(preds), axis=1)
        fit_rows = p[p["zone"].isin(fit_zones)]
        Xtr, ytr, _ = self._matrices(cand, fit_rows)
        ok = np.isfinite(ytr)
        if ok.sum() < 80:
            return eval_rows, None
        model = self._make_model(cand)
        try:
            model.fit(Xtr[ok], ytr[ok])
            Xe, _, _ = self._matrices(cand, eval_rows)
            return eval_rows, np.asarray(model.predict(Xe),
                                         dtype=np.float64)
        except Exception:
            return eval_rows, None

    def confirm_finalist(self, cand: dict) -> dict:
        zones.authorise(self.spec_hash(cand),
                        campaign_id=self.campaign_id,
                        family=cand["family"],
                        candidate_id=cand["candidate_id"],
                        at=time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                         time.gmtime()))
        if cand["model"] == "ensemble_mean":
            rep = self._evaluate_ensemble(
                cand, fit_zones=("ZONE_A", "ZONE_B"), eval_zone="ZONE_C",
                record_reuse=False)
        else:
            rep = self.evaluate_on_zone(
                cand, fit_zones=("ZONE_A", "ZONE_B"), eval_zone="ZONE_C",
                record_reuse=False)
        self.ledger.add("LOCKED_CONFIRMATIONS_RUN", family=cand["family"],
                        stage="ZONE_C")
        self.results[cand["candidate_id"]]["zone_c"] = _strip(rep)
        self.results[cand["candidate_id"]]["zone_c_series"] = rep
        return rep

    # ------------------------------------------------------------------ #
    # Robustness (finalists only; Zone C series already exist)
    # ------------------------------------------------------------------ #
    def robustness(self, cand: dict) -> dict:
        base = self.results[cand["candidate_id"]].get("zone_c_series") or {}
        if base.get("state") != "OK":
            return {"state": "NO_ZONE_C_RESULT"}
        diff = base.get("excess_diff_series")
        out = {"state": "OK",
               "sign_splits": J.sign_split_robustness(
                   np.asarray(diff) if diff is not None else np.zeros(0))}
        # cost stress x2
        if cand["model"] == "ensemble_mean":
            stressed = self._evaluate_ensemble(
                cand, fit_zones=("ZONE_A", "ZONE_B"), eval_zone="ZONE_C",
                cost_multiplier=C.COST_STRESS_MULTIPLIER,
                record_reuse=False)
        else:
            stressed = self.evaluate_on_zone(
                cand, fit_zones=("ZONE_A", "ZONE_B"), eval_zone="ZONE_C",
                cost_multiplier=C.COST_STRESS_MULTIPLIER,
                record_reuse=False)
        out["cost_stress_x2"] = {
            "after_cost_excess_annualised":
                stressed.get("after_cost_excess_annualised"),
            "t_stat": stressed.get("after_cost_excess_t_stat")}
        # leave-one-market-out sign flips (multi-market books only):
        # predictions are computed ONCE, then each market is dropped from
        # the BOOK - the fitted model is unchanged, which is exactly the
        # concentration question being asked.
        p = self._panel(cand)
        idc = self._id_col(cand)
        markets = sorted(p[p["zone"] == "ZONE_C"][idc].unique().tolist())
        base_sign = np.sign(base.get("after_cost_excess_annualised") or 0)
        flips = []
        if cand["lane"] in ("FUT", "ETF") and 3 <= len(markets) <= 70 \
                and base_sign != 0:
            rows, preds = self._predict_for(cand, ("ZONE_A", "ZONE_B"),
                                            "ZONE_C")
            if preds is None:
                out["leave_one_market_out"] = {"state": "PREDICT_FAILED"}
            else:
                for mk in markets:
                    try:
                        built = self._book_from_predictions(
                            cand, rows, preds, drop_market=mk)
                    except Exception:
                        continue
                    r = J.judge_candidate(built["book"],
                                          built["control_net"],
                                          horizon=cand["horizon"],
                                          control_name="LOMO")
                    ex = r.get("after_cost_excess_annualised")
                    if ex is not None and np.sign(ex) != base_sign:
                        flips.append(mk)
                out["leave_one_market_out"] = {
                    "n_markets": len(markets), "sign_flips": flips,
                    "n_sign_flips": len(flips)}
        else:
            out["leave_one_market_out"] = {"state": "NOT_APPLICABLE",
                                           "n_markets": len(markets)}
        # regime slices on the Zone-C excess series
        vix = None
        macro = self.state.get("macro")
        if macro is not None and "vix" in macro.columns:
            vix = macro["vix"].dropna()
        dates = pd.DatetimeIndex(base.get("book_dates") or [])
        out["regime_slices"] = J.regime_slices(
            np.asarray(diff) if diff is not None else np.zeros(0),
            dates, vix)
        self.results[cand["candidate_id"]]["robustness"] = out
        return out


def _strip(rep: dict) -> dict:
    """Result rows persisted to JSON: drop the numpy series."""
    return {k: v for k, v in rep.items()
            if k not in ("book_net", "excess_diff_series", "book_dates")
            and not isinstance(v, (np.ndarray, pd.DataFrame, pd.Series))}
