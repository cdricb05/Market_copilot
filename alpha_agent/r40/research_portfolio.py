"""alpha_agent.r40.research_portfolio - PROSPECTIVE_RESEARCH_PORTFOLIO
(Track K).

The five shadows are treated as a PORTFOLIO OF EXPERIMENTS: what matters
is evidence diversification as much as return diversification. Measured
on the same Zone-B evaluation streams the registry froze (never on Zone C,
never on TRUE_FORWARD rows):

* candidate return correlation (monthly-aggregated net streams);
* prediction correlation where both candidates rank the same universe;
* common factor exposure (each stream regressed on the R39 known-premia
  factor streams - trend, XS commodity momentum, carry, betas, short-VX,
  credit, seasonality, positioning - HAC inference);
* information-family overlap, asset-class overlap, regime overlap
  (calm/stress excess by the observable VIX split);
* a redundancy verdict per pair and a research-attention priority that
  favours the candidate adding the most incremental information.

Nothing here allocates capital; it prioritises research attention.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import judge as J
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import director as D
from . import evidence_velocity as EV
from . import shadow_registry as SR

CALCULATION_OWNER = "alpha_agent.r40.research_portfolio"
ARTIFACT_NAME = "prospective_research_portfolio.json"
STAGE = "R40_PORTFOLIO"
REDUNDANT_CORR = 0.70

INFO_FAMILY = {
    "shadow_wide_xs": {"CLASSICAL", "MACRO", "SPECTRAL", "LATENT", "GRAPH",
                       "MSTRUCT"},
    "shadow_carry_rule_xs": {"CARRY"},
    "shadow_vx_carry_ts": {"VX_TERM_STRUCTURE"},
    "shadow_intl_rates_carry_rv": {"CARRY"},
}
ASSET_CLASSES = {
    "shadow_wide_xs": {"COMMODITY", "RATES", "FX", "INTERNATIONAL_EQUITY"},
    "shadow_carry_rule_xs": {"COMMODITY", "RATES", "FX",
                             "INTERNATIONAL_EQUITY"},
    "shadow_vx_carry_ts": {"VOLATILITY"},
    "shadow_intl_rates_carry_rv": {"INTL_RATES"},
}


def _monthly(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    return s.groupby(s.index.to_period("M")).sum()


def _factor_exposure(stream: pd.Series, factors: pd.DataFrame) -> dict:
    m = _monthly(stream).to_frame("y").join(
        factors.groupby(factors.index.to_period("M")).sum(), how="inner") \
        .dropna()
    if len(m) < 36:
        return {"state": "INSUFFICIENT", "n": int(len(m))}
    y = m["y"].to_numpy()
    X = m.drop(columns=["y"]).to_numpy()
    Xc = np.column_stack([np.ones(len(y)), X])
    beta, *_ = np.linalg.lstsq(Xc, y, rcond=None)
    resid = y - Xc @ beta
    r2 = 1.0 - resid.var() / y.var() if y.var() > 0 else None
    return {"state": "OK", "n_months": int(len(m)), "r_squared": r2,
            "betas": {c: float(b) for c, b in zip(m.columns[1:], beta[1:])},
            "dominant_factor": str(m.columns[1:][int(np.argmax(
                np.abs(beta[1:])))]) if X.shape[1] else None}


def build(d2=None, campaign_id: str = CAMPAIGN_ID) -> dict:
    from ..r39.wide_prosecution import _factor_books
    from .model_challenge import _upgrade
    reg = SR.load(campaign_id)
    if not reg:
        raise RuntimeError("shadow registry v2 must be frozen first")
    d3 = _upgrade(d2 or D.session())
    streams, preds, regimes = {}, {}, {}
    vix = d3.state["macro"]["vix"] if "vix" in d3.state["macro"].columns \
        else None
    for sh in reg["shadows"]:
        cand = EV._candidate_for(sh, d3)
        if cand is None:
            continue
        rep = D.zone_b(cand, stage=STAGE, d2=d3)
        if rep.get("state") != "OK":
            continue
        streams[sh["shadow_id"]] = D.stream(rep)
        fitted = d3.fitted.get(cand["candidate_id"])
        if fitted is not None and cand["lane"] == "FUT" and \
                cand["scope"] == "ALL_FUT":
            p = d3._panel(cand)
            b = p[p["zone"] == "ZONE_B"]
            X, _, _ = d3._matrices(cand, b)
            try:
                preds[sh["shadow_id"]] = pd.Series(
                    np.asarray(fitted.predict(X), dtype=float),
                    index=pd.MultiIndex.from_arrays(
                        [pd.to_datetime(b["decision_date"]), b["market_id"]]))
            except Exception:
                pass
        diff = rep.get("excess_diff_series")
        if diff is not None and vix is not None:
            regimes[sh["shadow_id"]] = J.regime_slices(
                np.asarray(diff, dtype=float),
                pd.DatetimeIndex(rep["book_dates"]), vix)
    ids = list(streams)
    corr = {}
    for i, a in enumerate(ids):
        for b2 in ids[i + 1:]:
            ma, mb = _monthly(streams[a]), _monthly(streams[b2])
            j = ma.to_frame("a").join(mb.to_frame("b"), how="inner").dropna()
            corr["%s|%s" % (a, b2)] = float(j["a"].corr(j["b"])) \
                if len(j) >= 24 else None
    pred_corr = {}
    pids = list(preds)
    for i, a in enumerate(pids):
        for b2 in pids[i + 1:]:
            j = preds[a].to_frame("a").join(preds[b2].to_frame("b"),
                                            how="inner").dropna()
            pred_corr["%s|%s" % (a, b2)] = float(j["a"].corr(j["b"])) \
                if len(j) >= 100 else None
    try:
        fb = _factor_books(d3, "ZONE_B")
        cols = {}
        for k, s in fb.items():
            s = pd.Series(s).dropna()
            if isinstance(s.index, pd.PeriodIndex):
                s.index = s.index.to_timestamp(how="end")
            cols[k] = s
        factors = pd.DataFrame(cols)
    except Exception:
        factors = pd.DataFrame()
    exposure = {sid: _factor_exposure(s, factors) if not factors.empty
                else {"state": "NO_FACTORS"} for sid, s in streams.items()}
    overlap = {}
    fam = dict(INFO_FAMILY)
    cls = dict(ASSET_CLASSES)
    for sh in reg["shadows"]:
        if sh["shadow_id"] not in fam:
            fam[sh["shadow_id"]] = {str(sh.get("family"))}
            cls[sh["shadow_id"]] = {"COMMODITY", "RATES", "FX",
                                    "INTERNATIONAL_EQUITY"}
    for i, a in enumerate(ids):
        for b2 in ids[i + 1:]:
            fa, fb = fam.get(a, set()), fam.get(b2, set())
            ca, cb = cls.get(a, set()), cls.get(b2, set())
            overlap["%s|%s" % (a, b2)] = {
                "information_family_jaccard":
                    len(fa & fb) / max(len(fa | fb), 1),
                "asset_class_jaccard": len(ca & cb) / max(len(ca | cb), 1),
                "regime_same_sign_both": bool(
                    (regimes.get(a) or {}).get("same_sign_across_regimes")
                    and (regimes.get(b2) or {}).get(
                        "same_sign_across_regimes"))}
    redundant = [k for k, v in corr.items()
                 if v is not None and abs(v) >= REDUNDANT_CORR]
    # attention priority: lower max |corr| with the others and a faster
    # expected information rate rank higher
    vel = (_r39.read_json(campaign_dir(campaign_id) / EV.REGISTRY_NAME)
           or {}).get("registry") or {}
    priority = []
    for sid in ids:
        others = [abs(v) for k, v in corr.items() if sid in k.split("|")
                  and v is not None]
        rate = (vel.get(sid) or {}).get("success_information_rate_per_year")
        priority.append({"shadow_id": sid,
                         "max_abs_corr_with_others": max(others, default=0.0),
                         "success_information_rate_per_year": rate,
                         "score": (rate or 0.0) * (1.0 - max(others,
                                                             default=0.0))})
    priority.sort(key=lambda r: -r["score"])
    body = artifact_body("r40_prospective_research_portfolio/1", {
        "calculation_owner": CALCULATION_OWNER,
        "members": ids,
        "return_correlation_monthly": corr,
        "prediction_correlation": pred_corr,
        "factor_exposure": exposure,
        "overlap": overlap,
        "regimes": regimes,
        "redundancy_threshold": REDUNDANT_CORR,
        "redundant_pairs": redundant,
        "economically_redundant_statement": (
            "pairs above the threshold share most of their variance; "
            "their evidence is not independent and the family e-process "
            "treats them accordingly") if redundant else
        "no pair is economically redundant at the declared threshold",
        "research_attention_priority": priority,
        "allocates_capital": False,
        "evidence_source": "Zone-B selection streams only; Zone C and "
                           "TRUE_FORWARD rows never read",
    })
    body["portfolio_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    return body
