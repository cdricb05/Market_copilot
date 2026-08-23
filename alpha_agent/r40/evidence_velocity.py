"""alpha_agent.r40.evidence_velocity - EVIDENCE_VELOCITY_REGISTRY +
EFFECTIVE_SAMPLE_ANALYSIS (Track B).

Release 39 estimated that relying on WIDE's monthly economic stream alone
could take an operationally unacceptable amount of calendar time. This
engine quantifies, per shadow, how fast legitimate evidence can accrue -
and refuses the shortcuts that fake it:

* the PRIMARY channel is the after-cost economic excess per primary
  decision period; its serial dependence is measured (autocorrelations,
  Bartlett-weighted HAC) and turned into an effective-sample ratio;
* cross-sectional rows sharing a date are NOT independent markets: the
  effective number of markets is the participation ratio of the return
  correlation matrix (plus the mean pairwise correlation);
* daily marks of a fixed monthly position carry ZERO additional
  information about the mean (the sum is sufficient for the mean of
  i.i.d. increments with a common variance); they do sharpen the variance
  estimate and drawdown detection - stated analytically, not simulated
  into existence;
* the cross-sectional rank IC is a genuinely different statistic (every
  ranked market, not only the traded terciles) and its information per
  observation relative to the book's is measured from the same Zone-B
  evaluation: that is the one supporting channel that can legitimately
  accelerate the PREDICTIVE question - never the economic one;
* expected log-evidence growth per observation comes from the
  pre-registered capped-bet e-process under the frozen historical
  alternative (point estimate, 50% and 25% shrinkage) and under the null,
  and is converted to CALENDAR time using the effective observations per
  year.

Nothing here reports markets x days as independent samples.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39.wide_prosecution import RECON_STREAMS_NAME
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C
from . import director as D
from . import sequential as SQ
from . import shadow_registry as SR

CALCULATION_OWNER = "alpha_agent.r40.evidence_velocity"
REGISTRY_NAME = "evidence_velocity_registry.json"
ESS_NAME = "effective_sample_analysis.json"
STAGE = "R40_VELOCITY"
HAC_LAGS = 6
EFFECT_SCALES = {"point_estimate": 1.0, "registered_50pct": 0.5,
                 "shrunk_25pct": 0.25}


# --------------------------------------------------------------------------- #
# Dependence arithmetic
# --------------------------------------------------------------------------- #
def autocorrelations(x: np.ndarray, lags: int = HAC_LAGS) -> list:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size < lags + 8:
        return []
    xc = x - x.mean()
    v = float((xc * xc).sum())
    if v <= 0:
        return []
    return [float((xc[k:] * xc[:-k]).sum() / v) for k in range(1, lags + 1)]


def ess_ratio(x: np.ndarray, lags: int = HAC_LAGS) -> dict:
    """n_eff / n for the mean under Bartlett-weighted serial dependence."""
    rho = autocorrelations(x, lags)
    if not rho:
        return {"ratio": None, "autocorrelations": []}
    s = sum((1.0 - k / (lags + 1.0)) * r for k, r in enumerate(rho, 1))
    ratio = 1.0 / max(1.0 + 2.0 * s, 0.2)
    return {"ratio": float(min(ratio, 1.5)), "autocorrelations": rho,
            "bartlett_sum": float(s), "lags": lags}


def effective_markets(ret_wide: pd.DataFrame) -> dict:
    """Participation ratio of the correlation matrix + mean pairwise corr."""
    R = ret_wide.dropna(axis=1, thresh=int(len(ret_wide) * 0.5))
    Cm = R.corr(min_periods=36).to_numpy()
    n = Cm.shape[0]
    if n < 2:
        return {"n_markets": n, "effective_number": float(n)}
    Cm = np.where(np.isfinite(Cm), Cm, 0.0)
    np.fill_diagonal(Cm, 1.0)
    ev = np.linalg.eigvalsh(Cm)
    ev = ev[ev > 0]
    pr = float(ev.sum() ** 2 / (ev ** 2).sum())
    off = Cm[~np.eye(n, dtype=bool)]
    return {"n_markets": int(n), "effective_number_participation_ratio": pr,
            "mean_pairwise_correlation": float(off.mean()),
            "top_eigenvalue_share": float(ev.max() / ev.sum())}


def periods_per_year(stream: pd.Series) -> float:
    if stream.empty or len(stream) < 2:
        return float("nan")
    yrs = (stream.index.max() - stream.index.min()).days / 365.25
    return float(len(stream) / max(yrs, 1e-9))


# --------------------------------------------------------------------------- #
# Per-shadow velocity
# --------------------------------------------------------------------------- #
def velocity(shadow: dict, stream: pd.Series, ic: dict,
             eff_markets: dict, zone_c_stream: pd.Series = None) -> dict:
    x = stream.to_numpy(dtype=float)
    n = int(np.isfinite(x).sum())
    ess = ess_ratio(x)
    ppy = periods_per_year(stream)
    sigma0 = float(np.nanstd(x, ddof=1)) if n > 2 else None
    mu_b = float(np.nanmean(x)) if n else None
    # the frozen alternative: Zone-C for WIDE (its locked estimate), Zone-B
    # point estimate otherwise
    if zone_c_stream is not None and not zone_c_stream.empty:
        mu_alt = float(zone_c_stream.mean())
        alt_source = "locked Zone-C stream"
    else:
        mu_alt, alt_source = mu_b, "Zone-B selection stream"
    eff_ppy = ppy * (ess["ratio"] or 1.0)
    growth = {k: SQ.expected_log_growth(mu_alt * s, sigma0)
              for k, s in EFFECT_SCALES.items()} if sigma0 else {}
    growth_null = SQ.expected_log_growth(0.0, sigma0) if sigma0 else None
    ttd = {k: (math.log(SQ.E_SUCCESS) / (g * eff_ppy) if g > 0 else None)
           for k, g in growth.items()}
    # time to futility under the null (growth is negative under H0)
    ttf_null = (math.log(SQ.E_FUTILITY) / (growth_null * eff_ppy)
                if growth_null and growth_null < 0 else None)
    # supporting channel: rank IC information per observation vs book
    ic_t, ic_n = ic.get("t_stat"), ic.get("n_dates")
    book_t = (mu_b / (sigma0 / math.sqrt(n))) if sigma0 and n else None
    info_book = (book_t ** 2 / n) if book_t is not None and n else None
    info_ic = (ic_t ** 2 / ic_n) if ic_t is not None and ic_n else None
    return {
        "shadow_id": shadow["shadow_id"],
        "candidate_id": shadow["candidate_id"],
        "primary_decision_cadence": shadow.get("cadence"),
        "primary_economic_evidence_cadence_obs_per_year": ppy,
        "n_markets": eff_markets.get("n_markets"),
        "cross_sectional_dependence": eff_markets,
        "serial_dependence": ess,
        "effective_obs_per_year": eff_ppy,
        "effective_sample_ratio": ess["ratio"],
        "historical_stream": {"periods": n, "sigma_per_period": sigma0,
                              "mu_per_period": mu_b,
                              "t_stat": book_t,
                              "first": str(stream.index.min().date())
                              if n else None,
                              "last": str(stream.index.max().date())
                              if n else None},
        "frozen_alternative": {"mu_per_period": mu_alt,
                               "source": alt_source,
                               "annualised": mu_alt * ppy
                               if mu_alt is not None else None},
        "expected_log_evidence_growth_per_obs": {**growth,
                                                 "under_null": growth_null},
        "expected_log_evidence_growth_per_year": {
            k: g * eff_ppy for k, g in growth.items()},
        "time_to_decision": {
            "success_years": ttd,
            "success_obs": {k: (math.log(SQ.E_SUCCESS) / g if g > 0
                                else None) for k, g in growth.items()},
            "futility_years_under_null": ttf_null,
            "max_horizon_obs": 60 if shadow.get("lane") != "VX" else 260,
        },
        "success_information_rate_per_year":
            growth.get("point_estimate", 0.0) * eff_ppy,
        "failure_information_rate_per_year":
            (-growth_null * eff_ppy) if growth_null else None,
        "supporting_channels": {
            "daily_marks_of_fixed_position": {
                "mean_information_gain": 0.0,
                "why": "for a fixed position the period sum is sufficient "
                       "for the mean; %d daily increments carry the same "
                       "mean information as their sum" % (
                           shadow.get("horizon_sessions") or 21),
                "variance_estimation_speedup_x":
                    shadow.get("horizon_sessions") or 21,
                "drawdown_detection": "earlier by up to one period",
                "counted_as_independent_trades": False},
            "cross_sectional_rank_ic": {
                "mean_ic": ic.get("mean_ic"), "t_stat": ic_t,
                "n_dates": ic_n,
                "information_per_obs_book": info_book,
                "information_per_obs_ic": info_ic,
                "relative_information_ic_over_book":
                    (info_ic / info_book) if info_ic and info_book else None,
                "answers": "PREDICTIVE skill, never after-cost economics"},
            "sign_accuracy": {"role": "supporting, same observations as "
                                      "the book - no extra independent "
                                      "information"},
        },
        "never_report_markets_times_days": True,
    }


def build(d2=None, campaign_id: str = CAMPAIGN_ID) -> dict:
    from ..r39.wide_prosecution import wide_candidate
    from .model_challenge import _upgrade
    reg = SR.load(campaign_id)
    if not reg:
        raise RuntimeError("shadow registry v2 must be frozen first")
    d3 = _upgrade(d2 or D.session())
    fut = d3.state["fut"]
    sel = fut[fut["zone"].isin(("ZONE_A", "ZONE_B"))]
    wide_ret = sel.assign(_per=pd.to_datetime(sel["decision_date"])
                          .dt.to_period("M")).pivot_table(
        index="_per", columns="market_id", values="ret_1m", aggfunc="last")
    eff_all = effective_markets(wide_ret)
    intl = d3.state.get("fut_intl_rates")
    eff_intl = None
    if intl is not None and not intl.empty:
        si = intl[intl["zone"].isin(("ZONE_A", "ZONE_B"))]
        eff_intl = effective_markets(si.assign(
            _per=pd.to_datetime(si["decision_date"]).dt.to_period("M"))
            .pivot_table(index="_per", columns="market_id", values="ret_1m",
                         aggfunc="last"))
    registry = {}
    zone_c_wide = None
    recon = _r39.campaign_dir(C.R39_CONTINUATION_CAMPAIGN_ID) / \
        RECON_STREAMS_NAME
    if recon.exists():
        zc = pd.read_csv(recon, parse_dates=["decision_date"])
        zone_c_wide = pd.Series(zc["net_after_cost"].to_numpy(),
                                index=pd.DatetimeIndex(zc["decision_date"]))
    for sh in reg["shadows"]:
        cand = _candidate_for(sh, d3)
        if cand is None:
            registry[sh["shadow_id"]] = {"state": "NO_CANDIDATE_SPEC"}
            continue
        rep = D.zone_b(cand, stage=STAGE, d2=d3)
        if rep.get("state") != "OK":
            registry[sh["shadow_id"]] = {"state": rep.get("state")}
            continue
        stream = D.stream(rep)
        eff = eff_intl if sh.get("lane") == "FUT_INTL_RATES" else \
            ({"n_markets": 1, "effective_number_participation_ratio": 1.0,
              "note": "single instrument (VX front)"}
             if sh.get("lane") == "VX" else eff_all)
        registry[sh["shadow_id"]] = velocity(
            sh, stream, rep.get("ic") or {}, eff,
            zone_c_stream=zone_c_wide
            if sh["candidate_id"] == wide_candidate()["candidate_id"]
            else None)
    # what legitimately shortens time-to-decision
    levers = {
        "LEGITIMATE": [
            "a broader cross-section for a NEW candidate (more effective "
            "markets lowers book volatility for the same per-name skill)",
            "a higher-cadence NEW candidate with non-overlapping horizons "
            "(more independent periods per year; it is a different spec)",
            "the rank-IC channel for the PREDICTIVE question (measured "
            "information ratio above)",
            "family-level pooling through the averaged e-process (valid "
            "under dependence; answers the family question, not WIDE's)",
            "contiguous catch-up so no eligible date is lost",
        ],
        "ILLEGITIMATE_AND_REFUSED": [
            "counting daily marks of a monthly position as trades",
            "counting markets x days as independent samples",
            "refitting or re-tuning a frozen candidate",
            "resetting a boundary after observing outcomes",
            "reading Zone C or TRUE_FORWARD rows to choose among shadows",
        ],
    }
    body = artifact_body("r40_evidence_velocity_registry/1", {
        "calculation_owner": CALCULATION_OWNER,
        "e_process_owner": SQ.CALCULATION_OWNER,
        "effect_scales": EFFECT_SCALES,
        "registry": registry,
        "levers": levers,
        "dependence_treatments": list(C.DEPENDENCE_TREATMENTS),
    })
    body["velocity_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / REGISTRY_NAME, body,
                    immutable=False)
    ess = artifact_body("r40_effective_sample_analysis/1", {
        "calculation_owner": CALCULATION_OWNER,
        "all_futures_effective_markets": eff_all,
        "intl_rates_effective_markets": eff_intl,
        "per_shadow": {k: {"periods": (v.get("historical_stream") or {}).get(
            "periods"), "effective_sample_ratio": v.get(
                "effective_sample_ratio"),
            "effective_obs_per_year": v.get("effective_obs_per_year"),
            "autocorrelations": (v.get("serial_dependence") or {}).get(
                "autocorrelations")}
            for k, v in registry.items() if "historical_stream" in v},
        "daily_marks_rule": C.DAILY_MARKS_OF_A_MONTHLY_POSITION_ARE_NOT_INDEPENDENT_TRADES,
        "date_sharing_rule": C.CROSS_SECTIONAL_ROWS_SHARING_A_DATE_ARE_NOT_INDEPENDENT_MARKETS,
    })
    ess["ess_hash"] = _r39.sha(ess)
    _r39.write_json(campaign_dir(campaign_id) / ESS_NAME, ess,
                    immutable=False)
    return body


def _candidate_for(sh: dict, d3):
    from ..r39.continuation_director import new_cand
    from ..r39.wide_prosecution import wide_candidate
    sid = sh["shadow_id"]
    if sid == "shadow_wide_xs":
        return wide_candidate()
    if sid == "shadow_carry_rule_xs":
        return SR.carry_rule_candidate()
    if sid == "shadow_vx_carry_ts":
        return {"lane": "VX", "scope": "VX", "target": "tgt_excess_5",
                "horizon": 5, "expression": "TS_OUTRIGHT",
                "model": "rule:carry_slope_ann", "bundle": "VX_CLASSICAL",
                "family": "VX:TERM_STRUCTURE", "hyper": "default",
                "candidate_id": sh["candidate_id"]}
    if sid == "shadow_intl_rates_carry_rv":
        return SR.slot4_candidate()
    if sh.get("bundle"):
        if sh["bundle"] not in d3.bundles and sh.get("frozen_model"):
            d3.bundles[sh["bundle"]] = list(sh["frozen_model"]["features"])
        return new_cand(sh["lane"], sh["scope"], sh["bundle"], sh["family"],
                        sh["model"], sh["expression"],
                        hyper=sh.get("hyper") or "default")
    return None
