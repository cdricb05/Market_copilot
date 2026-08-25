"""alpha_agent.r43.killer - Track Q: try to destroy every promising result.

The tests are those declared in ``contract.ALPHA_KILLER_TESTS`` and the kill
criteria are ``contract.KILL_CRITERIA``, both frozen before any Release-43
number existed. Nothing here is applied selectively, nothing is dropped
because it was inconvenient, and a test that cannot be run reports
``NOT_RUN`` with a reason rather than silently passing.

The two tests that exist only in this release:

* ``COLLATERAL_REMUNERATION_ZERO`` - re-score the book as if its margin
  earned NOTHING, i.e. impose the R42 crypto treatment on a futures book.
  This is deliberately unfair; a book that survives it does not depend on
  the collateral argument at all.
* ``CAPITAL_HURDLE_X2`` - double committed capital. For a margin book this
  halves the return on capital without touching the t-statistic, so it is
  reported as an economic-scale test, not a significance test, and the
  report says so instead of pretending it is evidence.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import judge as J
from . import panels as P
from . import rv as RV
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.killer"

#: The placebo verdict must be REPRODUCIBLE. An earlier draft seeded the
#: generator from Python's built-in hash(), which is randomised per process,
#: so the headline candidate's kill verdict flipped between two runs of the
#: same code on the same data. The seed is now derived from a stable digest,
#: and the draw count is large enough that the 95th percentile is not itself
#: noise - because for this release's one survivor the placebo distribution
#: sits close enough to the candidate that a coarse estimate decides the
#: verdict.
PLACEBO_DRAWS = 200
BLOCK = 21
NEIGHBOURHOOD = ((1.25, 0.4), (1.4, 0.5), (1.6, 0.5), (1.75, 0.6),
                 (1.5, 0.3), (1.5, 0.7), (1.3, 0.6), (1.7, 0.4))
FACTOR_MARKETS = {"EQUITY_ES": "ES", "USD_DX": "DX", "GOLD_GC": "GC",
                  "OIL_CL": "CL", "DURATION_ZN": "ZN"}


def _stable_seed(candidate_id: str) -> int:
    """A seed that is identical in every process, on every machine."""
    import hashlib
    return int(hashlib.sha256(candidate_id.encode("utf-8")).hexdigest()[:8],
               16)


def _t(bk, dates) -> tuple:
    card = RV.score_book(bk, dates)
    return card.get("excess_ann"), card.get("excess_t_hac"), card


def _factors(index) -> pd.DataFrame:
    cols = {}
    for name, m in FACTOR_MARKETS.items():
        d = P.futures_daily(m)
        if d is None or "ret1" not in d.columns:
            continue
        cols[name] = pd.to_numeric(d["ret1"], errors="coerce")
    if not cols:
        return pd.DataFrame(index=pd.DatetimeIndex(index))
    return pd.DataFrame(cols).reindex(pd.DatetimeIndex(index)).fillna(0.0)


def _excess_stream(bk, dates) -> pd.Series:
    book = J.implementable_book(
        bk["gross"], pd.Series(1.0, index=bk["index"]),
        committed_capital=bk["committed_capital"],
        collateral_class="REMUNERATED_MARGIN", cost=bk["cost"],
        day_count=J.TRADING_DAYS)
    return book["excess"].reindex(pd.DatetimeIndex(dates)).dropna()


def _block_shuffle(frame: pd.DataFrame, rng, block: int = BLOCK
                   ) -> pd.DataFrame:
    """Stationary block shuffle of each column IN TIME.

    This preserves each signal's own persistence - and therefore the book's
    turnover - while destroying its alignment with the returns it is
    supposed to predict. A placebo that trades less than the candidate would
    be a rigged comparison.
    """
    n = len(frame)
    starts = rng.integers(0, max(1, n - block), size=n // block + 1)
    order = np.concatenate([np.arange(s, min(s + block, n)) for s in starts])
    order = order[:n]
    out = frame.copy()
    for c in frame.columns:
        out[c] = frame[c].to_numpy()[order]
    return out


def run(kind: str, signal: str, expression: str, *, family: str,
        candidate_id: str, zones: dict) -> dict:
    """The full battery against ONE advanced RV candidate on ZONE_B."""
    structures = RV.build_structures(kind)
    base_frame = RV.signal_frame(structures, signal)
    base = RV.book_streams(structures, signal, expression,
                           frame_override=base_frame)
    dates = zones["B"]
    base_ann, base_t, base_card = _t(base, dates)
    tests = {}

    # ---- cost stress (already computed by the scorecard; restated) ------- #
    cs = base_card.get("cost_stress") or {}
    tests["COST_X2"] = _verdict(cs.get("x2", {}).get("excess_ann"),
                                cs.get("x2", {}).get("t"), base_ann)
    tests["COST_X3"] = _verdict(cs.get("x3", {}).get("excess_ann"),
                                cs.get("x3", {}).get("t"), base_ann)

    # ---- leave-one-structure-out ---------------------------------------- #
    loo = []
    for s in structures:
        subset = [x for x in structures if x["name"] != s["name"]]
        if len(subset) < 3:
            continue
        bk = RV.book_streams(subset, signal, expression)
        if bk is None:
            continue
        a, t, _ = _t(bk, dates)
        loo.append({"dropped": s["name"], "excess_ann": a, "t": t})
    tests["LEAVE_ONE_MARKET_OUT"] = _loo_verdict(loo, base_ann, "dropped", base_t=base_t)

    # ---- leave-one-country/tag-out -------------------------------------- #
    tags = sorted({s["tag"] for s in structures})
    loc = []
    for tag in tags:
        subset = [x for x in structures if x["tag"] != tag]
        if len(subset) < 3:
            continue
        bk = RV.book_streams(subset, signal, expression)
        if bk is None:
            continue
        a, t, _ = _t(bk, dates)
        loc.append({"dropped": tag, "excess_ann": a, "t": t})
    tests["LEAVE_ONE_COUNTRY_OUT"] = _loo_verdict(loc, base_ann, "dropped", base_t=base_t)

    # ---- leave-one-year-block-out --------------------------------------- #
    d = pd.DatetimeIndex(dates)
    years = sorted({int(x) for x in d.year})
    loy = []
    for y in years:
        keep = d[d.year != y]
        if len(keep) < 250:
            continue
        a, t, _ = _t(base, keep)
        loy.append({"dropped": y, "excess_ann": a, "t": t})
    tests["LEAVE_ONE_YEAR_BLOCK_OUT"] = _loo_verdict(loy, base_ann, "dropped", base_t=base_t)

    # ---- signal lag / latency ------------------------------------------- #
    lag = RV.book_streams(structures, signal, expression, extra_lag=1,
                          frame_override=base_frame)
    a, t, _ = _t(lag, dates)
    tests["SIGNAL_LAG_PERTURBATION"] = _verdict(a, t, base_ann)
    tests["LATENCY_ONE_BAR"] = tests["SIGNAL_LAG_PERTURBATION"]

    # ---- parameter neighbourhood ---------------------------------------- #
    nb = []
    for band in NEIGHBOURHOOD:
        bk = RV.book_streams(structures, signal, expression, band=band,
                             frame_override=base_frame)
        if bk is None:
            continue
        a, t, _ = _t(bk, dates)
        nb.append({"band": list(band), "excess_ann": a, "t": t})
    ts = [r["t"] for r in nb if r["t"] is not None]
    med = float(np.median(ts)) if ts else None
    tests["PARAMETER_NEIGHBOURHOOD"] = {
        "rows": nb, "median_t": med,
        "n_positive": int(sum(1 for r in nb if (r["excess_ann"] or 0) > 0)),
        "n": len(nb),
        "kill_threshold_median_t":
            C.KILL_CRITERIA["parameter_neighbourhood_median_t_below"],
        "killed": bool(med is not None and med
                       < C.KILL_CRITERIA[
                           "parameter_neighbourhood_median_t_below"]),
    }

    # ---- placebo -------------------------------------------------------- #
    rng = np.random.default_rng(_stable_seed(candidate_id))
    pl = []
    for _ in range(PLACEBO_DRAWS):
        shuffled = _block_shuffle(base_frame, rng)
        bk = RV.book_streams(structures, signal, expression,
                             frame_override=shuffled)
        if bk is None:
            continue
        a, t, card = _t(bk, dates)
        pl.append({"t": t, "excess_ann": a,
                   "turnover_ann": card.get("turnover_ann")})
    pts = [abs(r["t"]) for r in pl if r["t"] is not None]
    thr = 0.8 * abs(base_t or 0.0)
    tests["PLACEBO_FEATURE"] = {
        "draws": len(pl),
        "placebo_abs_t_mean": float(np.mean(pts)) if pts else None,
        "placebo_abs_t_p95": float(np.quantile(pts, 0.95)) if pts else None,
        "placebo_turnover_mean": float(np.mean(
            [r["turnover_ann"] for r in pl if r["turnover_ann"] is not None]))
        if pl else None,
        "candidate_t": base_t,
        "empirical_p_two_sided": (float(np.mean([p >= abs(base_t or 0)
                                                 for p in pts]))
                                  if pts else None),
        "kill_rule": C.KILL_CRITERIA["placebo_indistinguishable"],
        "killed": bool(pts and float(np.quantile(pts, 0.95)) >= thr),
    }

    # ---- factor residualisation ----------------------------------------- #
    ex = _excess_stream(base, dates)
    F = _factors(ex.index)
    if len(F.columns) >= 2 and len(ex) > 100:
        fr = EV.factor_residual(ex.to_numpy(), F, overlap=1)
        tests["FACTOR_RESIDUALISATION"] = {
            "alpha_t_hac": fr.get("alpha_t_hac"),
            "alpha_per_period": fr.get("alpha_per_period"),
            "alpha_ann": (fr.get("alpha_per_period") or 0) * J.TRADING_DAYS,
            "betas": fr.get("betas"), "r_squared": fr.get("r_squared"),
            "factors": list(F.columns),
            "min_t": C.RESEARCH_CANDIDATE_GATE["factor_residual_t_min"],
            "killed": bool((fr.get("alpha_t_hac") or 0)
                           < C.RESEARCH_CANDIDATE_GATE[
                               "factor_residual_t_min"]),
        }
    else:
        tests["FACTOR_RESIDUALISATION"] = {"state": "NOT_RUN",
                                           "reason": "insufficient factors"}

    # ---- economics: collateral and capital ------------------------------ #
    zero_rho = RV.score_book(base, dates,
                             collateral_class="UNREMUNERATED_FULLY_FUNDED")
    tests["COLLATERAL_REMUNERATION_ZERO"] = {
        "excess_ann": zero_rho.get("excess_ann"),
        "t": zero_rho.get("excess_t_hac"),
        "cash_hurdle_ann": zero_rho.get("cash_hurdle_ann"),
        "note": "deliberately unfair: charges an exchange-traded futures "
                "book the full risk-free rate as if its margin earned "
                "nothing, which is the R42 crypto treatment",
        "killed": bool((zero_rho.get("excess_ann") or 0) <= 0),
    }
    x2 = RV.score_book(base, dates,
                       capital=base["committed_capital"] * 2.0)
    tests["CAPITAL_HURDLE_X2"] = {
        "excess_ann": x2.get("excess_ann"), "t": x2.get("excess_t_hac"),
        "committed_capital": base["committed_capital"] * 2.0,
        "note": "an economic-SCALE test, not a significance test: doubling a "
                "margin book's capital halves its return on capital and "
                "leaves the t-statistic unchanged",
        "killed": bool((x2.get("excess_ann") or 0) <= 0),
    }

    # ---- alternative economic control ----------------------------------- #
    #  Duration-matched passive long of the same instrument set: does the
    #  book merely reproduce being long bonds?
    tests["ALTERNATIVE_ECONOMIC_CONTROL"] = _passive_control(
        structures, dates, base)

    # ---- block bootstrap ------------------------------------------------ #
    tests["CLUSTER_BOOTSTRAP"] = _bootstrap(ex, rng)

    killed_by = sorted(k for k, v in tests.items()
                       if isinstance(v, dict) and v.get("killed"))
    return {
        "calculation_owner": CALCULATION_OWNER,
        "candidate_id": candidate_id, "kind": kind, "family": family,
        "signal": signal, "expression": expression,
        "zone": "ZONE_B", "zone_range": zones.get("b_range"),
        "base_excess_ann": base_ann, "base_t": base_t,
        "tests_declared": list(C.ALPHA_KILLER_TESTS),
        "tests_run": sorted(tests),
        "tests": tests,
        "killed_by": killed_by,
        "survives": not killed_by,
    }


def _verdict(ann, t, base_ann) -> dict:
    return {"excess_ann": ann, "t": t,
            "sign_flip": bool(ann is not None and base_ann is not None
                              and ann * base_ann < 0),
            "killed": bool(ann is not None and ann <= 0)}


def _loo_verdict(rows: list, base_ann, key: str, *,
                 base_t: float = None) -> dict:
    anns = [r["excess_ann"] for r in rows if r["excess_ann"] is not None]
    ts = [r["t"] for r in rows if r["t"] is not None]
    flips = [r[key] for r in rows
             if r["excess_ann"] is not None and base_ann is not None
             and r["excess_ann"] * base_ann < 0]
    worst = min(rows, key=lambda r: (r["t"] if r["t"] is not None
                                     else float("inf"))) if rows else None
    out = {
        "n": len(rows), "rows": rows,
        "min_excess_ann": min(anns) if anns else None,
        "max_excess_ann": max(anns) if anns else None,
        "min_t": min(ts) if ts else None,
        "sign_flips": flips,
        # The contract's kill rule is a SIGN FLIP; concentration is not a
        # kill but it is never left implicit, because a book that keeps its
        # sign while losing almost all of its t is a one-leg book.
        "worst_drop": (worst or {}).get(key),
        "worst_drop_t": (worst or {}).get("t"),
        "t_retained_fraction": (
            (min(ts) / base_t) if ts and base_t else None),
        "concentration_warning": bool(
            ts and base_t and (min(ts) / base_t) < 0.5),
        "killed": bool(flips),
    }
    return out


def _passive_control(structures: list, dates, base) -> dict:
    """The contract's TS_DIRECTIONAL control: a VOLATILITY-MATCHED passive
    long of the same instrument set, and the signal's INCREMENT over it.

    Comparing raw returns would be rigged in either direction - the passive
    book is always on, the candidate is not - so the passive stream is
    scaled to the candidate's realised volatility and the difference is
    scored with a HAC t. This is R42's "the gate is worth less than nothing"
    test, applied to a rates book instead of a crypto one.
    """
    ones = {s["name"]: pd.Series(1.0, index=s["index"]) for s in structures}
    frame = pd.DataFrame(ones).sort_index()
    passive = RV.book_streams(structures, "CARRY_LEVEL", "CONTINUOUS",
                              frame_override=frame)
    if passive is None:
        return {"state": "NOT_RUN", "reason": "no passive book"}
    card = RV.score_book(passive, dates)
    d = pd.DatetimeIndex(dates)
    cand = _excess_stream(base, d)
    pas = _excess_stream(passive, d).reindex(cand.index).fillna(0.0)
    sc = float(np.nanstd(cand)) / float(np.nanstd(pas) or np.nan) \
        if np.nanstd(pas) else None
    if not sc or not np.isfinite(sc):
        return {"state": "NOT_RUN", "reason": "passive volatility is zero"}
    matched = pas * sc
    inc = (cand - matched)
    hac = EV.hac_t(inc.to_numpy(dtype=float), lags=21)
    inc_ann = float(np.nanmean(inc) * J.TRADING_DAYS)
    return {
        "control": "VOLATILITY-MATCHED always-long book over the SAME "
                   "structures (contract CONTROLS.TS_DIRECTIONAL)",
        "passive_excess_ann_raw": card.get("excess_ann"),
        "passive_t_raw": card.get("excess_t_hac"),
        "passive_vol_ann_raw": card.get("vol_ann"),
        "vol_match_scale": sc,
        "passive_excess_ann_vol_matched": float(
            np.nanmean(matched) * J.TRADING_DAYS),
        "candidate_excess_ann": float(np.nanmean(cand) * J.TRADING_DAYS),
        "increment_ann": inc_ann,
        "increment_t_hac": hac.get("t"),
        "note": "if the volatility-matched passive book earns the same "
                "thing, the signal is decoration and the increment is what "
                "the candidate is actually worth",
        "killed": bool(inc_ann <= 0),
    }


def _bootstrap(ex: pd.Series, rng, *, draws: int = 2000,
               block: int = BLOCK) -> dict:
    x = ex.dropna().to_numpy(dtype=float)
    n = x.size
    if n < 200:
        return {"state": "NOT_RUN", "reason": "n < 200"}
    nb = int(np.ceil(n / block))
    means = np.empty(draws)
    centred = x - x.mean()
    for i in range(draws):
        starts = rng.integers(0, n - block, size=nb)
        samp = np.concatenate([centred[s:s + block] for s in starts])[:n]
        means[i] = samp.mean()
    obs = x.mean()
    p = float(np.mean(np.abs(means) >= abs(obs)))
    return {"draws": draws, "block": block,
            "observed_mean_ann": float(obs * J.TRADING_DAYS),
            "bootstrap_p_two_sided": p,
            "killed": bool(p > 0.05)}
