"""alpha_agent.r39.continuation_director - Tracks E/F/G execution.

``Director2`` extends the frozen v1 Director with exactly what the
continuation slates need - group scopes, the Track-F expressions, the
Track-G adapters, masked (sub-split) evaluation for information families
without Zone-A coverage - and NOTHING that touches Zone C. Every Zone-B
evaluation is recorded in the CUMULATIVE reuse ledger.

The international-rates extension builds the 11 R38-delivered markets
(CGB, FBTP, FGBL, FGBM, FGBS, FGBX, FOAT, LLG, SJB, YXT, YYT) through the
CANONICAL R38 layer builder (``alpha_agent.r38.research_layer``) from the
frozen R38 contract registry, written under the CONTINUATION campaign
directory - the frozen R38/v1 artifacts are never modified. Cost:
3.0 bps/side, the figure the frozen R38 contract already declared for
``INTL_RATES_FUTURE``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C
from . import judge as J
from . import models_ext as MX
from . import trade_space_ext as TX
from . import zones
from .continuation import CONTINUATION_CAMPAIGN_ID
from .discovery_director import Director, _cid
from .info_expansion import SUBSPLIT_FIT_END
from .representation_factory import CLASSICAL_FUT, MACRO_COLS

CALCULATION_OWNER = "alpha_agent.r39.continuation_director"

INTL_RATES_MARKETS = ("CGB", "FBTP", "FGBL", "FGBM", "FGBS", "FGBX",
                      "FOAT", "LLG", "SJB", "YXT", "YYT")
INTL_RATES_COST_BPS = 3.0  # frozen R38 contract: INTL_RATES_FUTURE

SEQ_BASE_FEATURES = ("ret_1m", "ret_3m", "ret_12m", "mom_12_1", "vol_63",
                     "carry_slope_ann", "high_252_prox", "cot_commercial_z")


class Director2(Director):
    """Continuation director: v1 machinery + the continuation surfaces."""

    # ------------------------------------------------------------------ #
    # Panels and scopes
    # ------------------------------------------------------------------ #
    def _panel(self, cand: dict) -> pd.DataFrame:
        scope = cand.get("scope") or ""
        if cand["lane"] == "FUT":
            if scope == "INTL_RATES":
                return self.state["fut_intl_rates"]
            if scope == "RATES_XCOUNTRY":
                fut = self.state["fut"]
                us = fut[fut["asset_class"] == "RATES"]
                intl = self.state["fut_intl_rates"]
                cols = [c for c in us.columns if c in intl.columns]
                return pd.concat([us[cols], intl[cols]],
                                 ignore_index=True).sort_values(
                    ["decision_date", "market_id"])
            if scope.startswith("GROUP:"):
                fut = self.state["fut"]
                return fut[fut["economic_group"] == scope[6:]]
        if cand["lane"] == "ETF" and scope == "SPY_ONLY":
            etf = self.state["etf"]
            return etf[etf["market_id"] == "SPY"]
        if cand["lane"] == "EQF":
            return self.state["eqf"]
        if cand["lane"] == "EQ":
            return self.state["eq"]
        return super()._panel(cand)

    def _id_col(self, cand: dict) -> str:
        return "security_id" if cand["lane"] in ("EQ", "EQF") else \
            "market_id"

    # ------------------------------------------------------------------ #
    # Models
    # ------------------------------------------------------------------ #
    def _make_model(self, cand: dict, seed_shift: int = 0):
        m = cand["model"]
        if m in MX.EXT_MODEL_FAMILIES:
            cols = self.bundles[cand["bundle"]]
            n_feats = None
            if m in ("tcn_seq", "gru_seq"):
                n_feats = len(cols) // (MX.SEQ_N_LAGS + 1)
            return MX.make_ext_adapter(
                m, seed=C.SEEDS["models"] + seed_shift, n_feats=n_feats)
        return super()._make_model(cand, seed_shift)

    # ------------------------------------------------------------------ #
    # Expressions
    # ------------------------------------------------------------------ #
    def _book_from_predictions(self, cand: dict, rows: pd.DataFrame,
                               preds: np.ndarray, *,
                               cost_multiplier: float = 1.0,
                               drop_market: str = None) -> dict:
        expr = cand["expression"]
        if expr in ("TS_OUTRIGHT", "XS_LONG_SHORT", "GROUP_SPREAD"):
            return super()._book_from_predictions(
                cand, rows, preds, cost_multiplier=cost_multiplier,
                drop_market=drop_market)
        rows = rows.copy()
        rows["_pred"] = preds
        if cand["target"].startswith("tgt_rank"):
            rows["_pred"] = rows["_pred"] - 0.5
        idc = self._id_col(cand)
        if drop_market is not None:
            rows = rows[rows[idc] != drop_market]
        fwd_col = "fwd_%d" % cand["horizon"]
        pred_m = rows.pivot_table(index="decision_date", columns=idc,
                                  values="_pred", aggfunc="last")
        fwd_m = rows.pivot_table(index="decision_date", columns=idc,
                                 values=fwd_col, aggfunc="last")
        cost = rows.groupby(idc)["cost_bps_per_side"].median()
        if expr == "GROUP_RV":
            groups: dict = {}
            if cand.get("hyper") == "one_group":
                groups["ALL"] = sorted(rows[idc].unique().tolist())
            else:
                for mk, grp in rows.groupby(idc)["economic_group"] \
                        .first().items():
                    groups.setdefault(str(grp), []).append(mk)
            vol_m = rows.pivot_table(index="decision_date", columns=idc,
                                     values="vol_63", aggfunc="last")
            book = TX.vol_scaled_group_rv(pred_m, fwd_m, cost, groups,
                                          vol_m,
                                          cost_multiplier=cost_multiplier)
            return {"book": {**book, "pred_matrix": pred_m,
                             "fwd_matrix": fwd_m},
                    "control_net": np.zeros(len(book["net"])),
                    "control_name": "RISK_MATCHED_CASH"}
        if expr == "XS_LS_SECTOR_NEUTRAL":
            if "sector" in rows.columns:
                rows["_pred"] = rows["_pred"] - rows.groupby(
                    ["decision_date", "sector"])["_pred"].transform("mean")
                pred_m = rows.pivot_table(index="decision_date",
                                          columns=idc, values="_pred",
                                          aggfunc="last")
            from .trade_space import xs_long_short
            book = xs_long_short(pred_m, fwd_m, cost,
                                 cost_multiplier=cost_multiplier)
            book["expression"] = "XS_LS_SECTOR_NEUTRAL"
            return {"book": {**book, "pred_matrix": pred_m,
                             "fwd_matrix": fwd_m},
                    "control_net": np.zeros(len(book["net"])),
                    "control_name": "RISK_MATCHED_CASH"}
        if expr == "XS_LS_ABSTAIN":
            book = TX.xs_abstain(pred_m, fwd_m, cost,
                                 cost_multiplier=cost_multiplier)
            return {"book": {**book, "pred_matrix": pred_m,
                             "fwd_matrix": fwd_m},
                    "control_net": np.zeros(len(book["net"])),
                    "control_name": "RISK_MATCHED_CASH"}
        if expr in ("XS_LS_GATED", "TS_GATED"):
            _tag, col, mode = str(cand["hyper"]).split(":")
            macro = self.state["macro"][col].dropna()
            gate = TX.regime_gate(macro, pred_m.index, pred_m.columns,
                                  mode)
            if expr == "XS_LS_GATED":
                book = TX.xs_gated(pred_m, fwd_m, cost, gate,
                                   cost_multiplier=cost_multiplier)
                return {"book": {**book, "pred_matrix": pred_m,
                                 "fwd_matrix": fwd_m},
                        "control_net": np.zeros(len(book["net"])),
                        "control_name": "RISK_MATCHED_CASH"}
            book = TX.ts_gated(pred_m, fwd_m, cost, gate,
                               cost_multiplier=cost_multiplier)
            return {"book": {**book, "pred_matrix": pred_m,
                             "fwd_matrix": fwd_m},
                    "control_net": np.asarray(book["gated_control_net"]),
                    "control_name":
                        "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE_SAME_GATE"}
        raise ValueError(expr)

    # ------------------------------------------------------------------ #
    # Masked evaluation (sub-splits; Zone C is structurally unreachable)
    # ------------------------------------------------------------------ #
    def evaluate_masked(self, cand: dict, fit_rows: pd.DataFrame,
                        eval_rows: pd.DataFrame, *, stage: str) -> dict:
        if (eval_rows["zone"] == "ZONE_C").any():
            raise zones.LockboxViolation(
                "masked evaluation may never touch Zone C")
        is_rule = str(cand["model"]).startswith("rule:")
        Xtr, ytr, _ = self._matrices(cand, fit_rows)
        ok = np.isfinite(ytr)
        if not is_rule and ok.sum() < 60:
            return {"state": "INSUFFICIENT_FIT_ROWS",
                    "n_fit_rows": int(ok.sum())}
        model = self._make_model(cand)
        try:
            if is_rule:
                model.fit(None, None)
            else:
                model.fit(Xtr[ok], ytr[ok])
        except Exception as e:
            return {"state": "FIT_FAILED:%s" % type(e).__name__}
        Xe, _, _ = self._matrices(cand, eval_rows)
        try:
            preds = model.predict(Xe)
        except Exception as e:
            return {"state": "PREDICT_FAILED:%s" % type(e).__name__}
        try:
            built = self._book_from_predictions(cand, eval_rows,
                                                np.asarray(preds,
                                                           dtype=float))
        except Exception as e:
            return {"state": "BOOK_FAILED:%s" % type(e).__name__}
        rep = J.judge_candidate(built["book"], built["control_net"],
                                horizon=cand["horizon"],
                                control_name=built["control_name"])
        rep["state"] = "OK"
        rep["ic"] = J.rank_ic(built["book"]["pred_matrix"],
                              built["book"]["fwd_matrix"])
        rep["book_net"] = built["book"]["net"]
        rep["book_dates"] = [str(d) for d in built["book"]["dates"]]
        rep["n_fit_rows"] = int(ok.sum()) if not is_rule else 0
        zones.record_zone_b(cand["candidate_id"], stage=stage,
                            campaign_id=self.campaign_id)
        return rep

    def eval_zone_b(self, cand: dict, *, stage: str,
                    fit_zones=("ZONE_A",)) -> dict:
        p = self._panel(cand)
        return self.evaluate_masked(
            cand, p[p["zone"].isin(fit_zones)],
            p[p["zone"] == "ZONE_B"], stage=stage)

    def eval_subsplit(self, cand: dict, *, stage: str) -> dict:
        """Fit on early Zone B, evaluate on late Zone B - the declared
        protocol for information families without Zone-A coverage."""
        p = self._panel(cand)
        b = p[p["zone"] == "ZONE_B"]
        cut = pd.Timestamp(SUBSPLIT_FIT_END)
        rep = self.evaluate_masked(cand, b[b["decision_date"] <= cut],
                                   b[b["decision_date"] > cut], stage=stage)
        rep["protocol"] = "ZONE_B_SUBSPLIT fit<=%s" % SUBSPLIT_FIT_END
        return rep


def new_cand(lane, scope, bundle, family, model, expression, *,
             target="tgt_excess_21", horizon=21, hyper="default") -> dict:
    spec = {"lane": lane, "scope": scope, "target": target,
            "horizon": horizon, "expression": expression, "model": model,
            "bundle": bundle, "family": family, "hyper": hyper}
    cand = dict(spec)
    cand["candidate_id"] = _cid(spec)
    return cand


# --------------------------------------------------------------------------- #
# International-rates extension (canonical R38 builder, $0, read-only)
# --------------------------------------------------------------------------- #
def build_intl_rates_extension(state: dict,
                               campaign_id: str = CONTINUATION_CAMPAIGN_ID
                               ) -> dict:
    from ..r38 import contract as C38
    from ..r38 import enumeration as EN38
    from ..r38 import research_layer as RL38
    from .universal_state import build_futures_panel

    cache = r39.campaign_dir(campaign_id) / "intl_rates_panel.csv"
    if cache.exists():
        panel = pd.read_csv(cache, parse_dates=["decision_date"])
        from .target_factory import materialise
        state["fut_intl_rates"] = materialise(panel,
                                              scope_col="asset_class")
        return {"state": "CACHE_HIT", "rows": int(len(panel)),
                "markets": sorted(panel["market_id"].unique())}

    registry = EN38.load_contract_registry(C38.CAMPAIGN_ID)
    if registry is None:
        return {"state": "R38_REGISTRY_UNAVAILABLE"}
    layer, built = {}, {}
    ldir = r39.campaign_dir(campaign_id) / "intl_rates_layer"
    ldir.mkdir(parents=True, exist_ok=True)
    for mkt in INTL_RATES_MARKETS:
        lists = registry["contract_symbols"].get(mkt, {})
        primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
        symbols = lists.get(primary, []) if primary else []
        series = RL38.build_market_series(mkt, symbols)
        if series is None or not len(series):
            built[mkt] = {"state": "NO_USABLE_SERIES"}
            continue
        df = series.reset_index()
        df = df.rename(columns={df.columns[0]: "Date"})
        df.to_csv(ldir / ("%s.csv" % mkt), index=False)
        layer[mkt] = df
        built[mkt] = {"state": "OK", "rows": int(len(df)),
                      "first": str(df["Date"].min().date()),
                      "last": str(df["Date"].max().date())}
    if not layer:
        return {"state": "NO_MARKET_BUILT", "markets": built}
    meta = pd.DataFrame([
        {"market_id": m, "asset_class": "RATES",
         "economic_group": "INTL_RATES_FUTURES", "currency": "LOCAL",
         "cost_bps_per_side": INTL_RATES_COST_BPS} for m in layer])
    cot = pd.DataFrame(columns=["market_id", "decision_date",
                                "cot_commercial_z"])
    cot["decision_date"] = pd.to_datetime(cot["decision_date"])
    panel = build_futures_panel(layer, meta, cot, state["macro"])
    panel.to_csv(cache, index=False)
    from .target_factory import materialise
    state["fut_intl_rates"] = materialise(panel, scope_col="asset_class")
    return {"state": "BUILT", "rows": int(len(panel)), "markets": built,
            "builder": "alpha_agent.r38.research_layer (canonical)",
            "cost_bps_per_side": INTL_RATES_COST_BPS,
            "written_under": str(ldir),
            "frozen_r38_layer_untouched": True}


# --------------------------------------------------------------------------- #
# Continuation bundles
# --------------------------------------------------------------------------- #
def register_bundles(d2: Director2) -> None:
    fut_cols = set(d2.state["fut"].columns)
    d2.state["fut"]["rev_1m"] = -d2.state["fut"]["ret_1m"]
    if "fut_intl_rates" in d2.state:
        d2.state["fut_intl_rates"]["rev_1m"] = \
            -d2.state["fut_intl_rates"]["ret_1m"]
    d2.bundles["FUT_REV"] = ["rev_1m", "ret_1m", "vol_63",
                             "high_252_prox", "ms_range_pos"]
    d2.bundles["CLS"] = list(CLASSICAL_FUT)
    d2.bundles["CLS_MACRO"] = list(CLASSICAL_FUT) + \
        [c for c in MACRO_COLS if c in fut_cols]
    for name, feats in (
            ("CLS_NYFED", ["nyfed_net_ust_z", "nyfed_net_ust_chg13_z"]),
            ("CLS_EIA", ["eia_crude_z", "eia_gasoline_z",
                         "eia_distillate_z", "eia_crude_chg4_z"]),
            ("CLS_INSIDER", ["insider_net_breadth_z"])):
        d2.bundles[name] = list(CLASSICAL_FUT) + feats
    from .representation_factory import EQ_FEATURES_BASE
    d2.bundles["EQ_PRICE14"] = list(EQ_FEATURES_BASE)
    d2.bundles["EQ_PRICE14_INSIDER"] = d2.bundles["EQ_PRICE14"] + \
        ["insider_net_6m_z"]
    from .info_expansion import FUND_COLS
    d2.bundles["EQF_PRICE14"] = d2.bundles["EQ_PRICE14"]
    d2.bundles["EQF_ALL21"] = d2.bundles["EQ_PRICE14"] + list(FUND_COLS)
    d2.bundles["ETF_INSIDER_RULE"] = ["insider_net_breadth_z", "ret_3m",
                                      "vol_63"]


def add_latent_graph_repaired(panel: pd.DataFrame, *, window: int = 60,
                              n_factors: int = 3, top_k: int = 5) -> tuple:
    """The REPAIRED latent/graph representations, on the calendar-month
    grid.

    The frozen v1 generator pivoted on exact decision dates; because each
    market's month-end lands on its own session, per-window column
    coverage sits at the knife edge of the 80% rule and the v1
    latent/graph columns exist only ERA-PATCHILY - measured empty across
    the entire SELECTION zone (Zone B), partially live in Zones A and C.
    Selection-stage family verdicts therefore judged filler features,
    while the confirmed WIDE book carried live latent loadings into
    Zone C. That generator stays byte-identical (Track A reconstruction
    depends on it); this repaired variant aligns on MONTH PERIODS so the
    trailing window is dense, and ships as NEW features under NEW names -
    a continuation discovery, not a rewrite of v1 history.
    """
    p = panel.copy()
    p["_per"] = p["decision_date"].dt.to_period("M")
    wide = p.pivot_table(index="_per", columns="market_id",
                         values="ret_1m", aggfunc="last").sort_index()
    periods = wide.index
    X = wide.to_numpy(dtype=float)
    n_per, n_mkts = X.shape
    load = np.full((n_per, n_mkts, n_factors), np.nan)
    resid_mom = np.full((n_per, n_mkts), np.nan)
    leadlag = np.full((n_per, n_mkts), np.nan)
    graph_edges, graph_year = None, None
    for t in range(window, n_per):
        W = X[t - window: t]
        ok = np.isfinite(W).sum(axis=0) >= int(window * 0.8)
        if ok.sum() < 12:
            continue
        Wo = np.where(np.isfinite(W[:, ok]), W[:, ok], 0.0)
        mu = Wo.mean(axis=0)
        sd = Wo.std(axis=0, ddof=1)
        sd[sd <= 0] = 1.0
        Z = (Wo - mu) / sd
        try:
            U, S, Vt = np.linalg.svd(Z, full_matrices=False)
        except np.linalg.LinAlgError:
            continue
        V = Vt[:n_factors].T
        idx_ok = np.flatnonzero(ok)
        load[t, idx_ok, :] = V[:, :n_factors]
        F = Z @ V
        resid = Z - F @ V.T
        resid_mom[t, idx_ok] = resid[-12:].sum(axis=0)
        yr = periods[t].year
        if graph_year != yr:
            graph_year = yr
            A = np.full((n_mkts, n_mkts), np.nan)
            for j in range(n_mkts):
                xj = X[t - window: t - 1, j]
                for i in range(n_mkts):
                    if i == j:
                        continue
                    yi = X[t - window + 1: t, i]
                    m = np.isfinite(xj) & np.isfinite(yi)
                    if m.sum() < 36:
                        continue
                    if xj[m].std(ddof=1) <= 0 or yi[m].std(ddof=1) <= 0:
                        continue
                    A[i, j] = float(np.corrcoef(xj[m], yi[m])[0, 1])
            graph_edges = []
            for i in range(n_mkts):
                row = A[i]
                order = np.argsort(-np.abs(np.where(np.isfinite(row), row,
                                                    0.0)))[:top_k]
                graph_edges.append([(int(j), float(row[j])) for j in order
                                    if np.isfinite(row[j])])
        if graph_edges is not None:
            last = X[t - 1]
            for i in range(n_mkts):
                s = wsum = 0.0
                for j, cij in graph_edges[i]:
                    if np.isfinite(last[j]):
                        s += cij * last[j]
                        wsum += abs(cij)
                leadlag[t, i] = s / wsum if wsum > 0 else np.nan
    cols = list(wide.columns)
    frames = {"latent2_resid_mom": resid_mom, "graph2_leadlag": leadlag}
    for f in range(n_factors):
        frames["latent2_load_pc%d" % (f + 1)] = load[:, :, f]
    names = []
    for name, arr in frames.items():
        longf = pd.DataFrame(arr, index=periods, columns=cols) \
            .stack().rename(name).reset_index()
        longf.columns = ["_per", "market_id", name]
        p = p.merge(longf, on=["_per", "market_id"], how="left")
        names.append(name)
    p = p.drop(columns=["_per"])
    coverage = {n: float(np.isfinite(p[n].to_numpy(dtype=float)).mean())
                for n in names}
    return p.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True), names, coverage


def register_sequence_bundle(d2: Director2) -> None:
    fut, seq_cols = MX.add_sequence_lags(
        d2.state["fut"], [c for c in SEQ_BASE_FEATURES
                          if c in d2.state["fut"].columns])
    d2.state["fut"] = fut
    d2.bundles["SEQ_CLS"] = seq_cols
