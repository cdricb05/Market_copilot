"""alpha_agent.r44.portfolio - ENGINE 2D, portfolio-level qualification.

A portfolio may qualify where no single sleeve does. It may NOT qualify by
being large, smooth or profitable. This module applies the contract's
fourteen predeclared kill tests to the combined book and reports every one
of them, including the ones it fails.

The tests that actually decide the release:

``STRUCTURAL_PREMIUM_CONTROL_INCREMENT``
    the residual portfolio minus the premium portfolio, volatility matched.
    If this is not positive with a real t, the answer to "did diversifying
    weak edges create Alpha" is NO and the honest label is a smoother
    package of premia.

``LEAVE_ONE_FAMILY_OUT`` / ``LEAVE_ONE_STREAM_OUT``
    a portfolio that dies when one sleeve is removed was that sleeve.

``PBO_COMBINATORIAL_SPLIT``
    the probability that the combination rule that looked best in sample is
    below median out of sample. Eight rules were tried; this is what that
    costs.
"""
from __future__ import annotations

import itertools

import numpy as np
import pandas as pd

from ..r31 import multiple_testing as MT
from ..r41 import evidence as EV
from ..r43 import judge as J
from . import combine as CB
from . import contract as C
from . import control as CTL

CALCULATION_OWNER = "alpha_agent.r44.portfolio"
TRADING_DAYS = J.TRADING_DAYS


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #
def score(ret: pd.Series, dates=None, *, lags: int = 21) -> dict:
    s = ret if dates is None else ret.reindex(pd.DatetimeIndex(dates))
    s = s.dropna()
    if len(s) < 24:
        return {"n": int(len(s)), "insufficient": True, "excess_ann": None,
                "t_hac": None}
    a = s.to_numpy(dtype=float)
    hac = EV.hac_t(a, lags=lags)
    mu = float(np.nanmean(a) * TRADING_DAYS)
    sd = float(np.nanstd(a, ddof=1) * np.sqrt(TRADING_DAYS))
    ess = EV.effective_sample(a)
    return {
        "n": int(len(s)),
        "first": str(s.index[0])[:10], "last": str(s.index[-1])[:10],
        "excess_ann": mu,
        "vol_ann": sd,
        "sharpe": (mu / sd) if sd else None,
        "t_hac": hac.get("t"),
        "max_drawdown": EV.max_drawdown(a),
        "cvar_5": EV.cvar(a),
        "effective_sample": ess,
        "hit_rate": float(np.mean(a > 0)),
    }


def _weight_meta(inv: dict) -> dict:
    return {sid: {"family": rec.get("family"),
                  "asset_class": rec.get("asset_class"),
                  "role": rec.get("role")}
            for sid, rec in inv.items()}


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def build(frame: pd.DataFrame, zones: dict, inv: dict, *,
          ids=None, rule: str = None, label: str = "PORTFOLIO",
          cost_multiplier: float = 1.0) -> dict:
    """Fit on A+B, apply to everything, score each zone separately."""
    rule = rule or C.PRIMARY_COMBINATION_RULE
    meta = _weight_meta(inv)
    ids = list(ids) if ids is not None else list(frame.columns)
    sub = frame[[c for c in ids if c in frame.columns]]
    fit_dates = pd.DatetimeIndex(zones["A"]).union(pd.DatetimeIndex(zones["B"]))
    fitted = CB.fit_weights(sub, fit_dates, meta, rule)
    if fitted.get("state") != "FITTED":
        return {"label": label, "rule": rule, "state": fitted.get("state"),
                "n_streams": fitted.get("n_streams", 0)}
    ret = CB.portfolio_returns(sub, fitted["weights"])
    out = {
        "label": label, "rule": rule, "state": "BUILT",
        "cost_multiplier": float(cost_multiplier),
        "weights": fitted["weights"],
        "risk_contribution": fitted["risk_contribution"],
        "n_streams": fitted["n_streams"],
        "effective_n_streams": fitted["effective_n_streams"],
        "shrinkage_intensity": fitted["shrinkage_intensity"],
        "streams_dropped_for_short_history": sorted(
            set(sub.columns) - set(fitted["weights"])),
        "fit": score(ret, fit_dates),
        "zone_a": score(ret, zones["A"]),
        "zone_b": score(ret, zones["B"]),
        "lock": score(ret, zones["C"]),
        "_returns": ret,
    }
    f, l = out["fit"].get("excess_ann"), out["lock"].get("excess_ann")
    out["same_sign_fit_and_lock"] = bool(
        f is not None and l is not None and np.sign(f) == np.sign(l))
    return out


# --------------------------------------------------------------------------- #
# The kill battery
# --------------------------------------------------------------------------- #
def _rebuild(frame, zones, inv, ids, rule, cost_multiplier=1.0):
    return build(frame, zones, inv, ids=ids, rule=rule,
                 cost_multiplier=cost_multiplier)


def kill_battery(frame: pd.DataFrame, zones: dict, inv: dict,
                 base: dict, *, ids=None, rule: str = None,
                 frames_by_cost: dict = None,
                 controls: dict = None) -> dict:
    """Every predeclared portfolio kill test, run on the LOCK zone."""
    rule = rule or C.PRIMARY_COMBINATION_RULE
    ids = list(ids) if ids is not None else list(base["weights"])
    meta = _weight_meta(inv)
    lock = pd.DatetimeIndex(zones["C"])
    base_ret = base["_returns"]
    base_lock = base["lock"].get("excess_ann")
    sign = np.sign(base_lock) if base_lock is not None else 0.0
    tests = {}

    # --- leave-one-out families -------------------------------------------- #
    for key, group_of in (("LEAVE_ONE_STREAM_OUT", lambda i: i),
                          ("LEAVE_ONE_FAMILY_OUT",
                           lambda i: (meta.get(i) or {}).get("family")),
                          ("LEAVE_ONE_ASSET_CLASS_OUT",
                           lambda i: (meta.get(i) or {}).get("asset_class"))):
        rows, flips = [], 0
        groups = sorted({group_of(i) for i in ids})
        for g in groups:
            keep = [i for i in ids if group_of(i) != g]
            if len(keep) < 2:
                continue
            b = _rebuild(frame, zones, inv, keep, rule)
            v = (b.get("lock") or {}).get("excess_ann")
            t = (b.get("lock") or {}).get("t_hac")
            flip = bool(v is None or (sign and np.sign(v) != sign))
            flips += int(flip)
            rows.append({"dropped": g, "lock_excess_ann": v, "lock_t": t,
                         "sign_flipped": flip})
        tests[key] = {"n": len(rows), "sign_flips": flips,
                      "passed": flips == 0, "rows": rows}

    # --- leave-one-year-out ------------------------------------------------ #
    years = sorted({d.year for d in lock})
    rows, flips = [], 0
    for y in years:
        keep = pd.DatetimeIndex([d for d in lock if d.year != y])
        s = score(base_ret, keep)
        v = s.get("excess_ann")
        flip = bool(v is None or (sign and np.sign(v) != sign))
        flips += int(flip)
        rows.append({"dropped_year": y, "lock_excess_ann": v,
                     "lock_t": s.get("t_hac"), "sign_flipped": flip})
    tests["LEAVE_ONE_YEAR_OUT"] = {"n": len(rows), "sign_flips": flips,
                                   "passed": flips == 0, "rows": rows}

    # --- weight perturbation ----------------------------------------------- #
    rng = np.random.default_rng(20260824)
    w0 = np.array([base["weights"][i] for i in base["weights"]])
    keys = list(base["weights"])
    vals, flips = [], 0
    for _ in range(200):
        noise = rng.normal(0.0, C.WEIGHT_PERTURBATION_SIGMA, size=len(w0))
        w = np.clip(w0 * (1.0 + noise), 0.0, None)
        w = w / w.sum() if w.sum() else w0
        r = CB.portfolio_returns(frame[keys], dict(zip(keys, w)))
        v = score(r, lock).get("excess_ann")
        if v is not None:
            vals.append(v)
            flips += int(bool(sign and np.sign(v) != sign))
    tests["WEIGHT_PERTURBATION"] = {
        "draws": len(vals), "sigma": C.WEIGHT_PERTURBATION_SIGMA,
        "sign_flips": flips, "flip_rate": (flips / len(vals)) if vals else None,
        "median_lock_excess_ann": float(np.median(vals)) if vals else None,
        "p05": float(np.percentile(vals, 5)) if vals else None,
        "p95": float(np.percentile(vals, 95)) if vals else None,
        "passed": bool(vals and flips / len(vals) <= 0.05)}

    # --- cost stress -------------------------------------------------------- #
    frames_by_cost = frames_by_cost or {}
    for mult in (2.0, 3.0):
        key = "COST_X%d" % int(mult)
        fr = frames_by_cost.get(mult)
        if fr is None:
            tests[key] = {"state": "NOT_RUN"}
            continue
        b = _rebuild(fr, zones, inv, ids, rule, cost_multiplier=mult)
        v = (b.get("lock") or {}).get("excess_ann")
        tests[key] = {"lock_excess_ann": v,
                      "lock_t": (b.get("lock") or {}).get("t_hac"),
                      "passed": bool(v is not None and v > 0)}

    # --- correlation stress ------------------------------------------------ #
    fit_dates = pd.DatetimeIndex(zones["A"]).union(pd.DatetimeIndex(zones["B"]))
    cov = CB.shrunk_covariance(frame[keys].reindex(fit_dates))
    d = np.sqrt(np.maximum(np.diag(cov.to_numpy(dtype=float)), 1e-24))
    R = cov.to_numpy(dtype=float) / np.outer(d, d)
    rows = []
    for rho in (0.25, 0.50):
        Rs = R * (1.0 - rho) + rho
        np.fill_diagonal(Rs, 1.0)
        cov_s = pd.DataFrame(Rs * np.outer(d, d), index=keys, columns=keys)
        w = CB.apply_constraints(
            pd.Series(CB._erc_weights(cov_s.to_numpy(dtype=float)),
                      index=keys), meta)
        r = CB.portfolio_returns(frame[keys], w.to_dict())
        s = score(r, lock)
        rows.append({"forced_min_correlation": rho,
                     "lock_excess_ann": s.get("excess_ann"),
                     "lock_t": s.get("t_hac")})
    tests["CORRELATION_STRESS"] = {
        "rows": rows,
        "passed": all(r["lock_excess_ann"] is not None
                      and (not sign or np.sign(r["lock_excess_ann"]) == sign)
                      for r in rows)}

    # --- block bootstrap ---------------------------------------------------- #
    a = base_ret.reindex(lock).dropna().to_numpy(dtype=float)
    if len(a) >= C.BOOTSTRAP_BLOCK * 4:
        nb = int(np.ceil(len(a) / C.BOOTSTRAP_BLOCK))
        starts = np.arange(0, len(a) - C.BOOTSTRAP_BLOCK)
        draws = np.empty(C.BOOTSTRAP_DRAWS)
        for i in range(C.BOOTSTRAP_DRAWS):
            s0 = rng.choice(starts, size=nb, replace=True)
            samp = np.concatenate(
                [a[s:s + C.BOOTSTRAP_BLOCK] for s in s0])[:len(a)]
            draws[i] = float(np.mean(samp) * TRADING_DAYS)
        tests["BLOCK_BOOTSTRAP"] = {
            "draws": int(C.BOOTSTRAP_DRAWS), "block": int(C.BOOTSTRAP_BLOCK),
            "mean": float(np.mean(draws)),
            "p05": float(np.percentile(draws, 5)),
            "p95": float(np.percentile(draws, 95)),
            "p_le_zero": float(np.mean(draws <= 0)),
            "passed": bool(float(np.mean(draws <= 0)) < 0.05)}
    else:
        tests["BLOCK_BOOTSTRAP"] = {"state": "NOT_RUN",
                                    "reason": "lock zone too short"}

    # --- concentration ------------------------------------------------------ #
    contrib = {}
    for sid in keys:
        w = float(base["weights"][sid])
        s = frame[sid].reindex(lock).fillna(0.0)
        contrib[sid] = float(np.nanmean(s) * TRADING_DAYS * w)
    tot = sum(contrib.values())
    share = {k: (v / tot if tot else None) for k, v in contrib.items()}
    top = max(share.items(), key=lambda kv: (kv[1] or -9)) if share else \
        (None, None)
    tests["CONCENTRATION"] = {
        "lock_excess_contribution_ann": contrib,
        "share_of_excess": share,
        "largest_contributor": top[0],
        "largest_share": top[1],
        "cap": C.PORTFOLIO_CONSTRAINTS[
            "max_single_lineage_contribution_to_excess"],
        "passed": bool(top[1] is not None and top[1] <= C.
                       PORTFOLIO_CONSTRAINTS[
                           "max_single_lineage_contribution_to_excess"])}

    # --- controls ------------------------------------------------------------ #
    controls = controls or {}
    prem_ret = controls.get("structural_premium_returns")
    if prem_ret is not None:
        inc = CTL.volatility_matched_increment(base_ret, prem_ret, lock)
        inc["passed"] = bool(
            inc.get("increment_ann") is not None
            and inc["increment_ann"] > 0
            and (inc.get("increment_t_hac") or 0.0)
            >= C.PORTFOLIO_ALPHA_GATE["control_increment_t_min"])
        tests["STRUCTURAL_PREMIUM_CONTROL_INCREMENT"] = inc
    else:
        tests["STRUCTURAL_PREMIUM_CONTROL_INCREMENT"] = {"state": "NOT_RUN"}

    pas = controls.get("passive_long_returns")
    if pas is not None and len(pas):
        inc = CTL.volatility_matched_increment(base_ret, pas, lock)
        inc["passed"] = bool(inc.get("increment_ann") is not None
                             and inc["increment_ann"] > 0)
        tests["VOLATILITY_MATCHED_PASSIVE_INCREMENT"] = inc
    else:
        tests["VOLATILITY_MATCHED_PASSIVE_INCREMENT"] = {"state": "NOT_RUN"}

    # --- equal-weight degradation ------------------------------------------- #
    ew = _rebuild(frame, zones, inv, ids, "EQUAL_WEIGHT")
    tests["EQUAL_WEIGHT_DEGRADATION"] = {
        "equal_weight_lock_excess_ann": (ew.get("lock") or {}).get(
            "excess_ann"),
        "primary_lock_excess_ann": base_lock,
        "primary_beats_equal_weight": bool(
            base_lock is not None
            and (ew.get("lock") or {}).get("excess_ann") is not None
            and base_lock >= (ew["lock"]["excess_ann"])),
        "passed": None, "gated": False,
        "note": "a primary rule that cannot beat equal weight has not earned "
                "its complexity; this is reported, not gated"}

    return tests


# --------------------------------------------------------------------------- #
# The sign-selected DIAGNOSTIC - never a qualification
# --------------------------------------------------------------------------- #
def sign_selected_diagnostic(frame: pd.DataFrame, zones: dict, inv: dict, *,
                             ids=None, rule: str = None) -> dict:
    """Let the FIT ZONE choose each stream's sign, then open the lockbox.

    This is not a candidate and cannot become one. It answers exactly one
    question: do these streams carry information that our predeclared
    economic signs pointed the wrong way at, or do they carry none? If the
    sign-selected portfolio is ALSO flat or negative out of sample, the
    streams are empty and no weighting scheme could have rescued them.
    """
    from . import streams as ST
    rule = rule or C.PRIMARY_COMBINATION_RULE
    ids = list(ids) if ids is not None else list(frame.columns)
    fit_dates = pd.DatetimeIndex(zones["A"]).union(pd.DatetimeIndex(zones["B"]))
    sub = frame[[c for c in ids if c in frame.columns]]

    # The sign is chosen on each stream's FIT-ZONE GROSS return, not on its
    # net return, so a stream is never flipped merely because its costs are
    # large. Costs are then re-charged in full on the flipped book.
    signs = {}
    for c in sub.columns:
        rec = inv.get(c) or {}
        g = rec.get("gross")
        if g is None:
            signs[c] = 1.0
            continue
        gf = pd.Series(g).reindex(fit_dates).dropna()
        m = float(np.nanmean(gf.to_numpy(dtype=float))) if len(gf) >= 250 \
            else 0.0
        signs[c] = -1.0 if m < 0 else 1.0

    flipped = ST.excess_frame_signed(inv, signs, ids=list(sub.columns))
    b = build(flipped, zones, inv, ids=list(sub.columns), rule=rule,
              label="SIGN_SELECTED_DIAGNOSTIC")
    b["signs"] = signs
    b["n_flipped"] = int(sum(1 for v in signs.values() if v < 0))
    b["may_qualify"] = False
    b["may_be_frozen_as_a_shadow"] = False
    b["declared_as"] = "DIAGNOSTIC"
    b["interpretation"] = (
        "a positive lockbox here with a flat predeclared-sign portfolio "
        "would mean the streams carry information and the economic signs "
        "were backwards; a flat lockbox here means the streams are empty")
    return b


# --------------------------------------------------------------------------- #
# PBO - what eight combination rules cost
# --------------------------------------------------------------------------- #
def pbo(frame: pd.DataFrame, zones: dict, inv: dict, *, ids=None,
        n_blocks: int = 8) -> dict:
    """Combinatorially-symmetric cross-validation over the COMBINATION RULES.

    The eight rules are the "strategies"; the fit zone is cut into blocks and
    every balanced in/out split is examined. PBO is the fraction of splits in
    which the rule that ranked first in sample ranked below median out of
    sample.
    """
    meta = _weight_meta(inv)
    ids = list(ids) if ids is not None else list(frame.columns)
    fit_dates = pd.DatetimeIndex(zones["A"]).union(pd.DatetimeIndex(zones["B"]))
    sub = frame[[c for c in ids if c in frame.columns]]

    rets = {}
    for rule in C.COMBINATION_RULES:
        f = CB.fit_weights(sub, fit_dates, meta, rule)
        if f.get("state") != "FITTED":
            continue
        rets[rule] = CB.portfolio_returns(sub, f["weights"]).reindex(fit_dates)
    if len(rets) < 3:
        return {"state": "NOT_RUN", "n_rules": len(rets)}

    R = pd.DataFrame(rets).dropna(how="all")
    blocks = np.array_split(np.arange(len(R)), n_blocks)
    half = n_blocks // 2
    logits, below = [], 0
    combos = list(itertools.combinations(range(n_blocks), half))
    for cmb in combos:
        is_rows = np.concatenate([blocks[i] for i in cmb])
        oos_rows = np.concatenate([blocks[i] for i in range(n_blocks)
                                   if i not in cmb])
        A, B = R.iloc[is_rows], R.iloc[oos_rows]
        is_sharpe = A.mean() / A.std(ddof=1)
        oos_sharpe = B.mean() / B.std(ddof=1)
        best = is_sharpe.idxmax()
        rank = float(oos_sharpe.rank(pct=True).get(best, np.nan))
        if np.isfinite(rank):
            below += int(rank < 0.5)
            rank = min(max(rank, 1e-6), 1 - 1e-6)
            logits.append(np.log(rank / (1 - rank)))
    n = len(logits)
    return {
        "state": "MEASURED",
        "n_rules": int(R.shape[1]),
        "n_splits": n,
        "n_blocks": int(n_blocks),
        "pbo": (below / n) if n else None,
        "median_logit": float(np.median(logits)) if logits else None,
        "interpretation": "PBO is the probability that the combination rule "
                          "that looked best in sample is below median out of "
                          "sample. It prices the eight-rule search directly.",
    }


# --------------------------------------------------------------------------- #
# Search adjustment
# --------------------------------------------------------------------------- #
def search_adjustment(rows: list, *, q: float = 0.10) -> dict:
    """Benjamini-Hochberg inside the PORTFOLIO_SYNTHESIS family."""
    from scipy import stats
    keep = [r for r in rows if r.get("t") is not None
            and np.isfinite(r.get("t"))]
    if not keep:
        return {"state": "NOT_RUN"}
    p = [float(2.0 * (1.0 - stats.norm.cdf(abs(r["t"])))) for r in keep]
    bh = MT.benjamini_hochberg(p, q=q)
    rejected = set(bh.get("rejected") or [])
    return {
        "calculation_owner": "alpha_agent.r31.multiple_testing."
                             "benjamini_hochberg",
        "q": q,
        "m": len(p),
        "threshold": bh.get("threshold"),
        "rows": [{"label": r.get("label"), "t": r.get("t"), "p": pi,
                  "bh_survivor": i in rejected}
                 for i, (r, pi) in enumerate(zip(keep, p))],
        "n_survivors": len(rejected),
    }


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #
def qualification(base: dict, tests: dict, *, bh_survivor: bool = None) -> dict:
    """Apply PORTFOLIO_ALPHA_GATE, item by item, and report every item."""
    lock = base.get("lock") or {}
    gate = {
        "streams_independently_discovered": True,
        "weighting_rule_predeclared": True,
        "no_holdout_optimisation": True,
        "positive_lock_excess": bool((lock.get("excess_ann") or 0.0) > 0),
        "t_min_lock": bool((lock.get("t_hac") or -9.0)
                           >= C.PORTFOLIO_ALPHA_GATE["t_min_lock"]),
        "same_sign_fit_and_lock": bool(base.get("same_sign_fit_and_lock")),
        "positive_at_2x_cost": bool(
            (tests.get("COST_X2") or {}).get("passed")),
        # Split deliberately: "the increment is positive" and "the increment
        # is distinguishable from zero" are different claims, and folding
        # them into one line hides which of the two failed.
        "beats_structural_premium_control": bool(
            ((tests.get("STRUCTURAL_PREMIUM_CONTROL_INCREMENT") or {})
             .get("increment_ann") or 0.0) > 0),
        "control_increment_t_min": bool(
            ((tests.get("STRUCTURAL_PREMIUM_CONTROL_INCREMENT") or {})
             .get("increment_t_hac") or -9.0)
            >= C.PORTFOLIO_ALPHA_GATE["control_increment_t_min"]),
        "no_single_stream_above_fraction_of_excess": bool(
            (tests.get("CONCENTRATION") or {}).get("passed")),
        "leave_one_family_out_preserves_sign": bool(
            (tests.get("LEAVE_ONE_FAMILY_OUT") or {}).get("passed")),
        "leave_one_stream_out_preserves_sign": bool(
            (tests.get("LEAVE_ONE_STREAM_OUT") or {}).get("passed")),
        "leave_one_asset_class_out_preserves_sign": bool(
            (tests.get("LEAVE_ONE_ASSET_CLASS_OUT") or {}).get("passed")),
        "min_lock_days": bool((lock.get("n") or 0)
                              >= C.PORTFOLIO_ALPHA_GATE["min_lock_days"]),
    }
    if bh_survivor is not None:
        gate["survives_search_adjustment"] = bool(bh_survivor)
    failed = [k for k, v in gate.items() if not v]
    return {
        "gate": gate,
        "failed_checks": failed,
        "qualifies_as_portfolio_alpha": not failed,
        "qualification_level": ("PORTFOLIO_ALPHA" if not failed else
                                "RESEARCH_CANDIDATE"
                                if gate["positive_lock_excess"]
                                and gate["t_min_lock"] else "NOT_QUALIFIED"),
    }
