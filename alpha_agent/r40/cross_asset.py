"""alpha_agent.r40.cross_asset - CROSS_ASSET_RELATIONSHIP_RESULTS (Track I).

Sparse lead/lag relationships across the universal estate, built from
TRAINING evidence only and judged economically on Zone B:

* NODES (monthly, calendar grid): equal-weight returns of each futures
  economic group; the ETF sleeves (credit = HYG-IEF, duration TLT, REIT
  VNQ, gold GLD, EM-vs-US = EEM-SPY); the VX front return and curve
  slope; monthly changes of the market-quoted macro overlay (credit
  spread, curve, bills, breakevens, real yield, VIX, VIX term, BTC);
  equal-weight returns of the US equity sectors (phase-24 sector axis).
* EDGE CLASSES are DECLARED (asset->asset, macro->asset, curve->equity
  sector, rates->equity, credit->volatility, FX->commodity,
  commodity->equity industry, volatility->dispersion); pairs outside the
  declared classes are never screened - no N^2 explosion.
* SCREEN on ZONE A only: target_{t+1} ~ own lag + source_t with HAC
  inference; Benjamini-Hochberg (the R31 owner) at q = 0.10 over all
  screened pairs; at most ``MAX_EDGES_PER_TARGET`` edges per target.
* Zone-B PREDICTIVE check of every kept edge (out of selection) - each is
  a validation evaluation and is added to the cumulative ledger under a
  synthetic candidate id (the conservative direction).
* ECONOMIC test: the kept edges become point-in-time features on the
  futures / ETF / VX panels; paired BASE(+cross-asset) increments under
  ridge (fit Zone A, judge Zone B) and a transparent per-group directional
  rule; every evaluation ledger-counted; Zone C untouched.

Causality is never claimed; the artifact reports predictive and economic
evidence, not a diagram.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r31 import multiple_testing as _mt
from ..r34 import economics as _econ
from ..r39 import info_expansion as IE
from ..r39.continuation_director import new_cand
from ..r39.representation_factory import CLASSICAL_FUT
from ..r39.wide_prosecution import _paired_increment
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import burden_ledger as BL
from . import director as D

CALCULATION_OWNER = "alpha_agent.r40.cross_asset"
ARTIFACT_NAME = "cross_asset_relationship_results.json"
STAGE = "R40_CROSS_ASSET"

FDR_Q = 0.10
MAX_EDGES_PER_TARGET = 3
MIN_ZONE_A_MONTHS = 120

MACRO_NODES = ("credit_baa10y", "curve_10_2", "DTB3", "T10YIE", "DFII10",
               "vix", "vix_term", "btc_ret_21")

#: node class -> allowed source classes (declared before screening)
EDGE_CLASSES = {
    "FUT_COMMODITY": ("FUT_COMMODITY", "FUT_FX", "FUT_RATES", "MACRO",
                      "VOL", "CREDIT", "EQUITY_SECTOR"),
    "FUT_RATES": ("FUT_RATES", "MACRO", "CREDIT", "VOL", "FUT_COMMODITY"),
    "FUT_FX": ("FUT_FX", "MACRO", "FUT_COMMODITY", "FUT_RATES", "VOL"),
    "FUT_INTL_EQUITY": ("FUT_INTL_EQUITY", "MACRO", "CREDIT", "VOL",
                        "FUT_RATES", "EQUITY_SECTOR"),
    "EQUITY_SECTOR": ("MACRO", "FUT_RATES", "FUT_COMMODITY", "CREDIT",
                      "VOL", "EQUITY_SECTOR"),
    "VOL": ("CREDIT", "MACRO", "FUT_RATES", "EQUITY_SECTOR"),
    "CREDIT": ("VOL", "MACRO", "FUT_RATES", "EQUITY_SECTOR"),
    "ETF_SLEEVE": ("MACRO", "CREDIT", "VOL", "FUT_RATES", "FUT_COMMODITY"),
    "DISPERSION": ("VOL", "CREDIT", "MACRO"),
}
EDGE_CLASS_NAMES = ("asset->asset", "macro->asset", "curve->equity sector",
                    "rates->equity", "credit->volatility", "FX->commodity",
                    "commodity->equity industry",
                    "volatility->cross-sectional dispersion")


# --------------------------------------------------------------------------- #
# Nodes
# --------------------------------------------------------------------------- #
def _month_end(s: pd.Series) -> pd.Series:
    s = s.dropna().sort_index()
    return s.groupby(s.index.to_period("M")).last()


def build_nodes(state: dict) -> tuple:
    """(monthly node frame indexed by Period, node class map)."""
    fut = state["fut"]
    nodes, classes = {}, {}
    p = fut.copy()
    p["_per"] = pd.to_datetime(p["decision_date"]).dt.to_period("M")
    grp = p.groupby(["_per", "economic_group"])["ret_1m"].mean().unstack()
    cls_of_group = p.groupby("economic_group")["asset_class"].first()
    for g in grp.columns:
        name = "FUT_" + str(g)
        nodes[name] = grp[g]
        ac = str(cls_of_group.get(g, ""))
        classes[name] = ("FUT_COMMODITY" if ac == "COMMODITY" else
                         "FUT_RATES" if ac == "RATES" else
                         "FUT_FX" if ac == "FX" else "FUT_INTL_EQUITY")
    # cross-sectional dispersion of futures returns (a volatility proxy)
    disp = p.groupby("_per")["ret_1m"].std()
    nodes["FUT_DISPERSION"] = disp
    classes["FUT_DISPERSION"] = "DISPERSION"
    macro = state.get("macro")
    if macro is not None and not macro.empty:
        for c in MACRO_NODES:
            if c in macro.columns:
                m = _month_end(macro[c])
                nodes["MACRO_" + c] = m.diff() if c != "btc_ret_21" else m
                classes["MACRO_" + c] = "MACRO"
        if "vix" in macro.columns:
            classes["MACRO_vix"] = "VOL"
            classes["MACRO_vix_term"] = "VOL"
        if "credit_baa10y" in macro.columns:
            classes["MACRO_credit_baa10y"] = "CREDIT"
    vx = state.get("vx")
    if vx is not None and not vx.empty:
        v = vx.copy()
        v["_per"] = pd.to_datetime(v["decision_date"]).dt.to_period("M")
        if "ret_1m" in v.columns:
            nodes["VX_RET"] = v.groupby("_per")["ret_1m"].last()
            classes["VX_RET"] = "VOL"
        if "carry_slope_ann" in v.columns:
            nodes["VX_SLOPE"] = v.groupby("_per")["carry_slope_ann"].last()
            classes["VX_SLOPE"] = "VOL"
    etf = state.get("etf")
    if etf is not None and not etf.empty:
        e = etf.copy()
        e["_per"] = pd.to_datetime(e["decision_date"]).dt.to_period("M")
        w = e.pivot_table(index="_per", columns="market_id", values="ret_1m",
                          aggfunc="last")
        if {"HYG", "IEF"} <= set(w.columns):
            nodes["ETF_CREDIT"] = w["HYG"] - w["IEF"]
            classes["ETF_CREDIT"] = "CREDIT"
        for tkr, nm in (("TLT", "ETF_DURATION"), ("VNQ", "ETF_REIT"),
                        ("GLD", "ETF_GOLD")):
            if tkr in w.columns:
                nodes[nm] = w[tkr]
                classes[nm] = "ETF_SLEEVE"
        if {"EEM", "SPY"} <= set(w.columns):
            nodes["ETF_EM_VS_US"] = w["EEM"] - w["SPY"]
            classes["ETF_EM_VS_US"] = "ETF_SLEEVE"
    eq = state.get("eq")
    if eq is not None and not eq.empty and "rev_1m" in eq.columns:
        try:
            q = IE.attach_eq_sector(eq[["security_id", "decision_date",
                                        "rev_1m"]])
            q["_per"] = pd.to_datetime(q["decision_date"]).dt.to_period("M")
            q["_ret"] = -q["rev_1m"]
            sec = q[q["sector"].astype(str).str.len() > 0] \
                .groupby(["_per", "sector"])["_ret"].mean().unstack()
            for s in sec.columns:
                nm = "EQSEC_" + str(s).replace(" ", "_")
                nodes[nm] = sec[s]
                classes[nm] = "EQUITY_SECTOR"
        except Exception:
            pass
    frame = pd.DataFrame(nodes).sort_index()
    return frame, classes


# --------------------------------------------------------------------------- #
# Zone-A screen
# --------------------------------------------------------------------------- #
def _hac_t(y: np.ndarray, X: np.ndarray, lags: int = 3) -> tuple:
    """OLS with Newey-West HAC; returns (beta, t) for the LAST column."""
    n, k = X.shape
    Xc = np.column_stack([np.ones(n), X])
    beta, *_ = np.linalg.lstsq(Xc, y, rcond=None)
    e = y - Xc @ beta
    XtX_inv = np.linalg.pinv(Xc.T @ Xc)
    S = (Xc * e[:, None]).T @ (Xc * e[:, None])
    for lag in range(1, lags + 1):
        w = 1.0 - lag / (lags + 1.0)
        G = (Xc[lag:] * e[lag:, None]).T @ (Xc[:-lag] * e[:-lag, None])
        S += w * (G + G.T)
    V = XtX_inv @ S @ XtX_inv
    se = np.sqrt(max(V[-1, -1], 1e-18))
    return float(beta[-1]), float(beta[-1] / se)


def screen_edges(nodes: pd.DataFrame, classes: dict, zone_a_end: str) -> dict:
    a = nodes[nodes.index <= pd.Period(zone_a_end, "M")]
    pairs = []
    for tgt in a.columns:
        tcls = classes.get(tgt)
        allowed = EDGE_CLASSES.get(tcls)
        if not allowed:
            continue
        y_full = a[tgt]
        for src in a.columns:
            if src == tgt or classes.get(src) not in allowed:
                continue
            df = pd.DataFrame({"y": y_full.shift(-1), "own": y_full,
                               "x": a[src]}).dropna()
            if len(df) < MIN_ZONE_A_MONTHS:
                continue
            b, t = _hac_t(df["y"].to_numpy(), df[["own", "x"]].to_numpy())
            pairs.append({"target": tgt, "source": src,
                          "target_class": tcls,
                          "source_class": classes.get(src),
                          "months": int(len(df)), "beta": b, "t": t,
                          "p": _mt.two_sided_p(t)})
    if not pairs:
        return {"n_pairs_screened": 0, "edges": [], "n_edges_kept": 0}
    ps = [p["p"] for p in pairs]
    bh = _mt.benjamini_hochberg(ps, q=FDR_Q)
    rejected = set(bh.get("rejected_indices") or bh.get("rejected") or [])
    if not rejected and isinstance(bh, dict) and "flags" in bh:
        rejected = {i for i, f in enumerate(bh["flags"]) if f}
    kept = []
    by_target: dict = {}
    for i, p in enumerate(pairs):
        if i in rejected:
            by_target.setdefault(p["target"], []).append(p)
    for tgt, rows in by_target.items():
        rows.sort(key=lambda r: -abs(r["t"]))
        kept.extend(rows[:MAX_EDGES_PER_TARGET])
    return {"n_pairs_screened": len(pairs), "fdr_q": FDR_Q,
            "bh_owner": _mt.CALCULATION_OWNER,
            "n_bh_rejections": len(rejected),
            "max_edges_per_target": MAX_EDGES_PER_TARGET,
            "edges": kept, "n_edges_kept": len(kept),
            "screened_pairs_top": sorted(pairs, key=lambda r: -abs(r["t"]))
            [:25]}


def zone_b_predictive(nodes: pd.DataFrame, edges: list, zone_b: tuple,
                      campaign_id: str) -> list:
    lo, hi = pd.Period(zone_b[0], "M"), pd.Period(zone_b[1], "M")
    b = nodes[(nodes.index >= lo) & (nodes.index <= hi)]
    out = []
    for e in edges:
        df = pd.DataFrame({"y": b[e["target"]].shift(-1), "own": b[e["target"]],
                           "x": b[e["source"]]}).dropna()
        cid = "c40_xa_" + _r39.sha({"s": e["source"], "t": e["target"]})[:12]
        BL.record(cid, stage=STAGE, campaign_id=campaign_id)
        if len(df) < 36:
            out.append({**e, "zone_b_state": "INSUFFICIENT",
                        "candidate_id": cid})
            continue
        bb, tt = _hac_t(df["y"].to_numpy(), df[["own", "x"]].to_numpy())
        out.append({**e, "candidate_id": cid, "zone_b_months": int(len(df)),
                    "zone_b_beta": bb, "zone_b_t": tt,
                    "same_sign": bool(np.sign(bb) == np.sign(e["beta"])),
                    "zone_b_state": "OK"})
    return out


# --------------------------------------------------------------------------- #
# Economic test on the panels
# --------------------------------------------------------------------------- #
def _attach_edge_features(d2, nodes: pd.DataFrame, edges: list) -> list:
    """Per-market cross-asset features: for a market in group g, the kept
    sources of target FUT_<g> at the decision month, plus a signed
    composite (sum of beta-signed standardised sources)."""
    fut = d2.state["fut"].copy()
    fut["_per"] = pd.to_datetime(fut["decision_date"]).dt.to_period("M")
    by_tgt: dict = {}
    for e in edges:
        by_tgt.setdefault(e["target"], []).append(e)
    z = (nodes - nodes.expanding(min_periods=24).mean()) / \
        nodes.expanding(min_periods=24).std()
    comp = pd.Series(np.nan, index=fut.index)
    srcs = sorted({e["source"] for t, es in by_tgt.items()
                   if t.startswith("FUT_") for e in es})
    feats = []
    for s in srcs:
        name = "xa_" + s.lower()
        fut[name] = z[s].reindex(fut["_per"]).to_numpy()
        feats.append(name)
    for g, grp_rows in fut.groupby("economic_group"):
        es = by_tgt.get("FUT_" + str(g)) or []
        if not es:
            continue
        val = np.zeros(len(grp_rows))
        for e in es:
            val = val + np.sign(e["beta"]) * z[e["source"]].reindex(
                grp_rows["_per"]).fillna(0.0).to_numpy()
        comp.loc[grp_rows.index] = val / max(len(es), 1)
    fut["xa_group_composite"] = comp
    fut = fut.drop(columns=["_per"])
    d2.state["fut"] = fut
    d2.bundles["CLS_XA"] = list(CLASSICAL_FUT) + feats + ["xa_group_composite"]
    d2.bundles["XA_RULE"] = ["xa_group_composite", "ret_1m", "vol_63"]
    return feats


def run(d2=None, campaign_id: str = CAMPAIGN_ID) -> dict:
    from ..r39 import contract as C39
    d2 = d2 or D.session()
    t0 = time.time()
    nodes, classes = build_nodes(d2.state)
    zone_a_end = C39.ZONE_A_DISCOVERY_END[:7]
    screen = screen_edges(nodes, classes, zone_a_end)
    edges = screen["edges"]
    pred = zone_b_predictive(nodes, edges,
                             (C39.ZONE_B_VALIDATION_START[:7],
                              C39.ZONE_B_VALIDATION_END[:7]), campaign_id)
    n_same = sum(1 for e in pred if e.get("same_sign"))
    n_ok = sum(1 for e in pred if e.get("zone_b_state") == "OK")
    n_sig = sum(1 for e in pred if e.get("zone_b_state") == "OK"
                and abs(e["zone_b_t"]) >= 2.0 and e["same_sign"])
    results = {}
    if edges:
        feats = _attach_edge_features(d2, nodes, edges)
        results["features"] = feats + ["xa_group_composite"]
        for expr in ("XS_LONG_SHORT", "TS_OUTRIGHT"):
            base = new_cand("FUT", "ALL_FUT", "FUT_CLASSICAL", "FUT:CLASSICAL",
                            "ridge", expr)
            var = new_cand("FUT", "ALL_FUT", "CLS_XA", "FUT:CROSS_ASSET",
                           "ridge", expr)
            rb = D.zone_b(base, stage=STAGE, d2=d2)
            rv = D.zone_b(var, stage=STAGE, d2=d2)
            results["ridge_" + expr.lower()] = {
                "base": {"candidate_id": base["candidate_id"],
                         **D.summarise(rb)},
                "variant": {"candidate_id": var["candidate_id"],
                            **D.summarise(rv)},
                "paired_increment": _paired_increment(rb, rv)
                if rb.get("state") == "OK" and rv.get("state") == "OK"
                else {"state": "NOT_COMPARABLE"}}
        for expr in ("TS_OUTRIGHT", "XS_LONG_SHORT"):
            rule = new_cand("FUT", "ALL_FUT", "XA_RULE", "FUT:CROSS_ASSET",
                            "rule:xa_group_composite", expr)
            rr = D.zone_b(rule, stage=STAGE, d2=d2)
            results["rule_" + expr.lower()] = {
                "candidate_id": rule["candidate_id"], **D.summarise(rr)}
    incs = [(k, v["paired_increment"].get("incremental_t"))
            for k, v in results.items()
            if isinstance(v, dict) and isinstance(v.get("paired_increment"),
                                                  dict)
            and v["paired_increment"].get("incremental_t") is not None]
    best_inc = max(incs, key=lambda x: x[1], default=None)
    rule_ts = [(k, v.get("after_cost_excess_t_stat"))
               for k, v in results.items() if k.startswith("rule_")
               and v.get("after_cost_excess_t_stat") is not None]
    best_rule = max(rule_ts, key=lambda x: x[1], default=None)
    headline = {
        "zone_a_pairs_screened": screen["n_pairs_screened"],
        "edges_kept_after_fdr": screen["n_edges_kept"],
        "zone_b_edges_tested": n_ok,
        "zone_b_edges_same_sign": n_same,
        "zone_b_edges_same_sign_and_|t|>=2": n_sig,
        "best_paired_economic_increment": None if best_inc is None else
        {"pair": best_inc[0], "incremental_t": best_inc[1]},
        "best_rule_zone_b_t": None if best_rule is None else
        {"rule": best_rule[0], "t": best_rule[1]},
        "new_edge": bool((best_inc is not None and best_inc[1] >= 2.0)
                         or (best_rule is not None and best_rule[1] >= 2.0)),
        "result": "CROSS_ASSET_EDGE_CANDIDATE" if (
            (best_inc is not None and best_inc[1] >= 2.0)
            or (best_rule is not None and best_rule[1] >= 2.0))
        else "CROSS_ASSET_NO_ROBUST_EDGE",
    }
    body = artifact_body("r40_cross_asset_relationship_results/1", {
        "calculation_owner": CALCULATION_OWNER,
        "nodes": {"n": int(nodes.shape[1]), "months": int(len(nodes)),
                  "classes": classes,
                  "first": str(nodes.index.min()),
                  "last": str(nodes.index.max())},
        "edge_classes_declared": EDGE_CLASS_NAMES,
        "edge_class_whitelist": {k: list(v) for k, v in EDGE_CLASSES.items()},
        "screen": screen,
        "zone_b_predictive": pred,
        "economic_results": results,
        "headline": headline,
        "causality_claimed": False,
        "ledger_counted": "every Zone-B predictive edge test (synthetic "
                          "candidate id) and every Zone-B book",
        "seconds": round(time.time() - t0, 1),
    })
    body["cross_asset_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    return body
