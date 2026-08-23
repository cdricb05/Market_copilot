"""alpha_agent.r39.frontier - ALPHA_FRONTIER, redundancy, combination.

The release does not return "a best model". It returns a frontier: every
finalist with its full economics, statistical and search-burden-adjusted
evidence, robustness, correlation with every other finalist, and its
fresh-evidence state - because several economically distinct 0.5-Sharpe
edges are worth more than one fragile 2.0.

The DIVERSIFIED combination book (Track 20, zero-base: if every candidate
sleeve were cash, which mix best uses research capital?) occupies the ONE
lockbox slot reserved for it at freeze time: its weights come from ZONE B
excess streams only, and its Zone-C series is a weighted sum of the
finalists' already-executed Zone-C streams - no new model execution, one
declared additional test, counted in the BH denominator. RESEARCH ONLY:
it is not, and does not become, the operational portfolio.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39
from ..r34 import economics as _econ
from ..r31.judge import max_drawdown
from . import contract as C
from . import zones

CALCULATION_OWNER = "alpha_agent.r39.frontier"
SCHEMA = "r39_alpha_frontier/1"
ARTIFACT_NAME = "alpha_frontier.json"
CORR_NAME = "candidate_correlation_matrix.json"
COMBO_NAME = "diversified_alpha_book_research.json"

COMBINATION_CANDIDATE_ID = "c39_diversified_combination"


def _aligned_diffs(finalists: list, results: dict, key: str) -> pd.DataFrame:
    """Finalist excess-vs-control streams on a shared date axis."""
    cols = {}
    for c in finalists:
        rep = results[c["candidate_id"]].get(key) or {}
        diff = rep.get("excess_diff_series")
        dates = rep.get("book_dates")
        if diff is None or not dates:
            continue
        n = min(len(diff), len(dates))
        s = pd.Series(np.asarray(diff[:n], dtype=float),
                      index=pd.DatetimeIndex(dates[:n]))
        cols[c["candidate_id"]] = s[~s.index.duplicated(keep="last")]
    return pd.DataFrame(cols)


def correlation_matrix(finalists: list, results: dict,
                       campaign_id: str = C.CAMPAIGN_ID) -> dict:
    zb = _aligned_diffs(finalists, results, "stage2_series")
    corr = zb.corr(min_periods=24)
    body = r39.artifact_body("r39_candidate_correlation_matrix/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "basis": "ZONE_B after-control excess streams",
        "candidates": list(corr.columns),
        "matrix": [[None if not np.isfinite(v) else round(float(v), 4)
                    for v in row] for row in corr.to_numpy()],
        "mean_absolute_offdiagonal": _mean_abs_offdiag(corr),
    })
    body["correlation_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / CORR_NAME, body)
    return body


def _mean_abs_offdiag(corr: pd.DataFrame):
    a = corr.to_numpy(dtype=float)
    n = a.shape[0]
    if n < 2:
        return None
    mask = ~np.eye(n, dtype=bool)
    vals = a[mask]
    vals = vals[np.isfinite(vals)]
    return round(float(np.abs(vals).mean()), 4) if vals.size else None


def combination_spec(director, finalists: list) -> dict:
    """The combination's frozen spec, computed from ZONE B ONLY so it can be
    frozen into the finalist set BEFORE Zone C opens."""
    zb = _aligned_diffs(finalists, director.results, "stage2_series")
    usable = [c for c in zb.columns if zb[c].notna().sum() >= 24]
    if len(usable) < 2:
        return {"state": "INSUFFICIENT_COMPONENTS",
                "n_components": len(usable)}
    vols = zb[usable].std(ddof=1)
    w = (1.0 / vols.replace(0.0, np.nan)).fillna(0.0)
    w = w / w.sum()
    spec = {"candidate_id": COMBINATION_CANDIDATE_ID,
            "components": {k: round(float(v), 6) for k, v in w.items()},
            "weighting": "inverse ZONE-B volatility",
            "leverage": "none; weights sum to 1 across sleeves"}
    return {"state": "OK", "spec": spec, "spec_hash": r39.sha(spec),
            "usable": usable,
            "weights": {k: float(v) for k, v in w.items()}}


def diversified_combination(director, finalists: list, spec_info: dict,
                            campaign_id: str = C.CAMPAIGN_ID) -> dict:
    """Evaluate the FROZEN combination spec on Zone C through its reserved
    lockbox slot."""
    if spec_info.get("state") != "OK":
        return spec_info
    results = director.results
    spec = spec_info["spec"]
    spec_hash = spec_info["spec_hash"]
    usable = spec_info["usable"]
    w = pd.Series(spec_info["weights"])
    zb = _aligned_diffs(finalists, results, "stage2_series")
    zones.authorise(spec_hash, campaign_id=campaign_id,
                    family="COMBINATION", candidate_id=spec["candidate_id"],
                    at=pd.Timestamp.now(tz="UTC").isoformat())
    zc = _aligned_diffs(finalists, results, "zone_c_series")
    zc = zc[[c for c in usable if c in zc.columns]]
    combo_c = (zc * w.reindex(zc.columns)).sum(axis=1, min_count=2).dropna()
    combo_b = (zb[usable] * w.reindex(usable)).sum(axis=1,
                                                   min_count=2).dropna()
    horizon = 21
    sig = _econ.excess_significance(
        combo_c.to_numpy(), np.zeros(len(combo_c)), horizon=horizon)
    per_component = {}
    for cid in usable:
        s = zc[cid].dropna()
        per_component[cid] = {
            "weight": round(float(w[cid]), 4),
            "zone_c_mean_ann": float(s.mean()) * 12.0 if len(s) else None,
            "risk_contribution": round(
                float(w[cid] * zc[cid].std(ddof=1) /
                      max(combo_c.std(ddof=1), 1e-12)), 4)
            if len(s) >= 8 else None}
    body = r39.artifact_body("r39_diversified_alpha_book_research/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "zero_base_framing": "if all candidate sleeves were cash, this is "
                             "the best-evidence mix of them; RESEARCH ONLY",
        "spec": spec, "spec_hash": spec_hash,
        "weights_zone": "ZONE_B",
        "evaluation_zone": "ZONE_C",
        "zone_c_label": C.ZONE_C_EVIDENCE_LABEL,
        "zone_b": _series_stats(combo_b, horizon),
        "zone_c": _series_stats(combo_c, horizon),
        "zone_c_t_stat": sig.get("t_stat"),
        "zone_c_annualised_excess": sig.get("annualised_excess"),
        "per_component": per_component,
        "not_an_operational_portfolio": True,
    })
    body["combination_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / COMBO_NAME, body)
    body["_zone_c_series"] = combo_c
    body["_sig"] = sig
    return body


def _series_stats(s: pd.Series, horizon: int) -> dict:
    x = s.to_numpy(dtype=float)
    x = x[np.isfinite(x)]
    if x.size < 8:
        return {"periods": int(x.size)}
    ppy = _econ.periods_per_year(horizon)
    sd = x.std(ddof=1)
    return {"periods": int(x.size),
            "annualised_mean": float(x.mean() * ppy),
            "annualised_vol": float(sd * np.sqrt(ppy)),
            "sharpe": float(x.mean() / sd * np.sqrt(ppy)) if sd > 0
            else None,
            "max_drawdown": max_drawdown(x),
            "hit_rate": float((x > 0).mean())}


def build_frontier(director, finalists: list, *, qualification: dict,
                   dsr_by_candidate: dict,
                   campaign_id: str = C.CAMPAIGN_ID) -> dict:
    results = director.results
    rows = []
    for c in finalists:
        cid = c["candidate_id"]
        s2 = results[cid].get("stage2") or {}
        zc = results[cid].get("zone_c") or {}
        rb = results[cid].get("robustness") or {}
        rows.append({
            "candidate_id": cid,
            "origin": c.get("origin"),
            "generation_method": c.get("generation_method"),
            "parent_candidate_ids": c.get("parent_candidate_ids", []),
            "lane": c["lane"], "scope": c["scope"],
            "assets": c["scope"], "representation": c["bundle"],
            "family": c["family"], "model": c["model"],
            "trade_expression": c["expression"],
            "target": c["target"], "horizon_sessions": c["horizon"],
            "zone_b": {k: s2.get(k) for k in (
                "net_return_annualised", "sharpe",
                "after_cost_excess_annualised",
                "after_cost_excess_t_stat", "annualised_turnover",
                "annualised_cost", "max_drawdown")},
            "zone_c": {k: zc.get(k) for k in (
                "net_return_annualised", "sharpe", "sortino",
                "after_cost_excess_annualised",
                "after_cost_excess_t_stat", "annualised_turnover",
                "annualised_cost", "max_drawdown", "cvar_5pct",
                "hit_rate", "minimum_detectable_excess",
                "mean_gross_exposure")},
            "deflated_sharpe": dsr_by_candidate.get(cid),
            "robustness": {
                "sign_splits": rb.get("sign_splits"),
                "cost_stress_x2": rb.get("cost_stress_x2"),
                "leave_one_market_out": rb.get("leave_one_market_out"),
                "regime_slices": rb.get("regime_slices")},
            "qualified_historical_alpha": bool(
                qualification.get(cid, {}).get("qualified")),
            "qualification_detail": qualification.get(cid),
            "fresh_evidence_state": C.ZONE_C_EVIDENCE_LABEL,
        })
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frontier": rows,
        "n_finalists": len(rows),
        "qualification_conditions": C.QUALIFICATION_CONDITIONS,
        "we_prefer": "multiple economically distinct 0.5-1.0 Sharpe edges "
                     "over one fragile 2.0 historical Sharpe backtest",
    })
    body["alpha_frontier_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
