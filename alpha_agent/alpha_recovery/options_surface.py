r"""alpha_agent.alpha_recovery.options_surface - information axis A: the owned
SPY option / implied-volatility surface.

WHY THIS AXIS, AND WHY NOW
    The campaign's own information frontier ranks VOLATILITY_EXPECTATIONS_IV
    third among owned needs. It has been attempted twice and never satisfied:

        R63/R64   VOLATILITY|TS|21|VOLATILITY_EXPECTATIONS_IV returned DATA_HOLD
                  on 196 covered rows against sensitivity.MIN_ROWS = 200.
        earlier   the same cell run at h = 1 and h = 5 answered the row floor by
        this      CADENCE (the floor was never moved) and came back with a
        campaign  NEGATIVE conditional statistic and REPRODUCTION_FAILED against
                  the persisted R63 matrix, because its FRED / Cboe index inputs
                  had drifted.

    Both attempts used an INDEX PROXY for implied volatility. This module uses
    an ACTUAL OPTION SURFACE - per-contract implied volatilities with strike,
    expiry, type, moneyness and time to expiry - which is the instrument the
    need always described. Under contract rule 13 that is a genuine coverage
    improvement resolving a NAMED binding failure, not a reopened transform.

WHAT IS OWNED (acquired by R45, left on disk, nothing purchased)
    ``_data_options/polygon_spy_option_surface_r45_extension.csv.gz``
    6,216 contract-days, 265 dates, 2025-06-27 -> 2026-07-17, 6 monthly
    expiries, 40 strikes (654-720), calls and puts, with iv, moneyness, T_years,
    rf and the underlying close.

THE LIMITS, STATED BEFORE THE RESULTS
    * 265 dates is ~13 months and ONE regime. No regime partition is possible
      and none is claimed.
    * The surface is narrow: a median of 22 contracts and 3 expiries per date,
      moneyness 0.91-1.139. Deep wings do not exist here, so no tail-risk or
      25-delta construct is attempted.
    * The sample lies entirely after the R63 lockbox start (2023-01-01), so the
      estate's lockbox does not partition it. A chronological split is declared
      instead, BEFORE running: first 60 % of dates SELECTION, last 40 % HOLDOUT.
    * h = 21 is NOT attempted: at cadence 21 the sample yields ~12
      non-overlapping periods against the frozen floor of 36. The floor is not
      moved and the horizon is not run.

RESEARCH ONLY. Reads one frozen CSV, writes one artifact. No purchase.
"""
from __future__ import annotations

import json
import math
import re
import time

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, GATE_HALF_FLOOR, HOLM_ALPHA, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS,
               SPY_PROXY_COST_BPS, research_root, write_artifact)
from . import intraday_alpha as IA
from . import intraday_data as ID
from . import tournament as T

CALCULATION_OWNER = "alpha_agent.alpha_recovery.options_surface"
CELLS_DIR = "cells"
ARTIFACT_NAME = "options_surface.json"
#: The R45 surface this axis was first attempted on: a FIXED strike band of
#: 654-720 that the underlying rallied through, leaving 25 of 264 dates whose
#: near-dated strikes bracket the money against a frozen floor of 36.
R45_SURFACE_PATH = ID.OPTION_SURFACE


def surface_path():
    """The surface to run on: the MONEYNESS-ANCHORED one when it exists.

    The axis's recorded blocker was never "not enough dates" - it was that an
    ATM implied-volatility series cannot be built from a strike band the spot
    has left. A band that tracks the underlying removes exactly that defect, so
    running on it is the NAMED binding failure being resolved rather than a
    closed axis being reopened on a new parameter. The R45 surface remains the
    fallback so this module still works wherever the new one has not been
    acquired.
    """
    from .options_acquisition import surface_path as anchored
    p = anchored()
    return p if p.exists() else R45_SURFACE_PATH


SURFACE_PATH = ID.OPTION_SURFACE          # kept for readers that import the name
PPY = 252.0

FAMILY = "OPTIONS_IMPLIED_VOLATILITY_SURFACE"
DIMENSION = "VOLATILITY_EXPECTATIONS_IV"

#: Cost ladder. The estate's canonical SPY proxy rate is the headline; the
#: intraday ladder's stress rate is carried so both axes are comparable.
COST_PRIMARY_BPS = SPY_PROXY_COST_BPS          # 1.0 bp per side, canonical
COST_STRESS_BPS = ID.COST_STRESS_BPS           # 5.0 bp per side
COST_LADDER_BPS = (COST_PRIMARY_BPS, ID.COST_PRIMARY_BPS, COST_STRESS_BPS)

#: Fixed before running.
SELECTION_FRACTION = 0.60
ZSCORE_LOOKBACK = 60
RV_LOOKBACK = 21
MIN_T_YEARS_NEAR = 0.05                        # ~18 calendar days
HORIZONS = (1, 5)
#: Re-derived when the moneyness-anchored surface replaced the R45 one, because
#: the old justification quoted a sample size that no longer applies. The
#: CONCLUSION is unchanged and the floor is still not moved: a two-year surface
#: is ~490 dates, which at cadence 21 yields ~23 non-overlapping periods against
#: the frozen floor of 36. Clearing it at h = 21 needs roughly three years, not
#: two, so h = 21 remains un-run and says so with the right arithmetic.
HORIZON_NOT_RUN = {21: "at cadence 21 a two-year surface (~490 dates) yields ~23 non-overlapping "
                       "periods against the frozen MIN_EFFECTIVE_PERIODS floor of 36; ~756 dates "
                       "would be needed. The floor is not moved and the horizon is not run"}

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# The surface -> daily feature panel
# --------------------------------------------------------------------------- #
def _atm_iv(g: pd.DataFrame) -> float:
    """IV at moneyness 1.0 for one (date, expiry), averaging the call and put
    sides and interpolating linearly in moneyness when both sides bracket 1.0."""
    g = g.dropna(subset=["iv", "moneyness"])
    if g.empty:
        return float("nan")
    out = []
    for _, side in g.groupby("type"):
        side = side.sort_values("moneyness")
        m, v = side["moneyness"].to_numpy(), side["iv"].to_numpy()
        if len(m) == 0:
            continue
        if m.min() <= 1.0 <= m.max():
            out.append(float(np.interp(1.0, m, v)))
        else:
            out.append(float(v[int(np.argmin(np.abs(m - 1.0)))]))
    return float(np.mean(out)) if out else float("nan")


def features(*, rebuild: bool = False) -> pd.DataFrame:
    """One row per surface date: ATM IV near and far, term slope, put-call skew,
    trailing realised volatility and the variance risk premium.

    Every field is computed from information available ON that date; the
    realised-volatility leg uses STRICTLY PRIOR closes.
    """
    if "feat" in _CACHE and not rebuild:
        return _CACHE["feat"]
    df = pd.read_csv(surface_path(), parse_dates=["date", "expiration"])
    df = df[np.isfinite(df["iv"]) & (df["iv"] > 0)]
    rows = []
    for d, g in df.groupby("date"):
        und = float(g["underlying_close"].iloc[0])
        exps = sorted(g["expiration"].unique())
        near = None
        for e in exps:
            ge = g[g["expiration"] == e]
            if float(ge["T_years"].max()) >= MIN_T_YEARS_NEAR:
                near = (e, ge)
                break
        far_e = exps[-1] if exps else None
        far = g[g["expiration"] == far_e] if far_e is not None else None
        # A date is USABLE only if the near expiry's strike grid BRACKETS the
        # money. Without that, "ATM implied volatility" would be the implied
        # volatility of whatever strike happened to be least far from the
        # money that day - a series whose moneyness drifts with the rally, not
        # a volatility series. Such a date contributes nothing rather than a
        # plausible-looking proxy.
        brackets = False
        if near is not None:
            m = near[1]["moneyness"]
            brackets = bool(m.min() <= 1.0 <= m.max())
        iv_near = _atm_iv(near[1]) if (near is not None and brackets) else float("nan")
        iv_far = _atm_iv(far) if far is not None and len(far) else float("nan")
        # put-call skew on the SAME expiry: OTM puts minus OTM calls
        sk = float("nan")
        if near is not None and brackets:
            ge = near[1]
            p = ge[(ge["type"] == "put") & (ge["moneyness"] <= 0.98)]["iv"]
            c = ge[(ge["type"] == "call") & (ge["moneyness"] >= 1.02)]["iv"]
            if len(p) and len(c):
                sk = float(p.mean() - c.mean())
        rows.append({"date": d, "underlying_close": und, "atm_iv_near": iv_near,
                     "atm_iv_far": iv_far, "term_slope": iv_far - iv_near, "skew": sk,
                     "n_contracts": int(len(g)), "n_expiries": int(len(exps)),
                     "has_near_expiry": bool(near is not None), "brackets_the_money": brackets,
                     "moneyness_min": float(g["moneyness"].min()),
                     "moneyness_max": float(g["moneyness"].max()),
                     "t_years_near": (float(near[1]["T_years"].max()) if near is not None
                                      else float("nan"))})
    f = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    lr = np.log(f["underlying_close"]).diff()
    f["rv21"] = lr.shift(1).rolling(RV_LOOKBACK, min_periods=RV_LOOKBACK // 2).std() * math.sqrt(PPY)
    f["vrp"] = f["atm_iv_near"] - f["rv21"]
    _CACHE["feat"] = f
    return f


def usability() -> dict:
    """Can an honest ATM implied-volatility series be built from this surface
    at all? Decided by MEASUREMENT, before any signal is computed."""
    f = features()
    n = int(len(f))
    n_near = int(f["has_near_expiry"].sum())
    n_ok = int((f["has_near_expiry"] & f["brackets_the_money"]).sum())
    n_skew = int(np.isfinite(f["skew"]).sum())
    ok = n_ok >= MIN_EFFECTIVE_PERIODS
    return {
        "state": "USABLE" if ok else "DATA_INSUFFICIENT",
        "dates": n, "dates_with_near_expiry": n_near,
        "dates_whose_strikes_bracket_the_money": n_ok,
        "dates_supporting_a_skew": n_skew,
        "floor": MIN_EFFECTIVE_PERIODS,
        "why": (None if ok else
                "The surface is a FIXED strike band of %d-%d acquired for a single R45 event study. "
                "Over the sample the underlying rallied from %.0f to %.0f, so the band drifted out "
                "of the money: only %d of %d dates carry a near-dated expiry whose strikes bracket "
                "the money, against the frozen floor of %d. An ATM implied-volatility series cannot "
                "be built without silently substituting a drifting-moneyness proxy, which would "
                "manufacture a result rather than measure one. The floor is NOT moved and no cell "
                "is scored."
                % (654, 720, float(f["underlying_close"].iloc[0]), float(f["underlying_close"].iloc[-1]),
                   n_ok, n, MIN_EFFECTIVE_PERIODS)),
        "exact_missing_requirement": (
            "a daily SPY option chain anchored on MONEYNESS rather than on fixed strikes - at "
            "minimum a +/-10 % moneyness band with two expiries beyond 18 days - over >= 2 years "
            "(~500 dates). The owned surface is 6 fixed expiries x 40 fixed strikes."),
        "acquisition_options": [
            {"source": "Polygon options aggregates", "state": "OWNED_PLAN_NOT_ENTITLED",
             "detail": "the same plan that answers 403 for current-session equity aggregates"},
            {"source": "CBOE DataShop / OptionMetrics IvyDB", "state": "PURCHASE_REQUIRED",
             "detail": "not requested; no purchase is proposed by this campaign"}],
    }


def _z(x: pd.Series) -> np.ndarray:
    """Rolling z-score against STRICTLY PRIOR observations only."""
    m = x.shift(1).rolling(ZSCORE_LOOKBACK, min_periods=ZSCORE_LOOKBACK // 2).mean()
    s = x.shift(1).rolling(ZSCORE_LOOKBACK, min_periods=ZSCORE_LOOKBACK // 2).std()
    return ((x - m) / s.replace(0.0, np.nan)).to_numpy()


#: The three signals, each with its economic sign fixed here, BEFORE running,
#: from theory rather than from the data. One sign per signal - the mirror is
#: NOT executed as a separate specification, which would double the multiplicity
#: denominator for nothing.
SIGNALS = {
    "ATM_IV_LEVEL": {
        "field": "atm_iv_near", "sign": +1,
        "economics": "a high implied-volatility level is compensation for bearing variance risk; "
                     "the variance risk premium predicts HIGHER subsequent equity returns"},
    "PUT_CALL_SKEW": {
        "field": "skew", "sign": +1,
        "economics": "a steep put-over-call skew is hedging demand / crash fear; elevated fear has "
                     "historically been paid, so the sign is POSITIVE"},
    "VARIANCE_RISK_PREMIUM": {
        "field": "vrp", "sign": +1,
        "economics": "implied minus trailing realised volatility is the premium itself, measured "
                     "directly; a wide premium predicts HIGHER subsequent returns"},
}

#: A construct the MONEYNESS-ANCHORED surface makes expressible for the first
#: time and which is DELIBERATELY NOT RUN.
#:
#: ``features`` already computes ``term_slope``, and the fixed R45 band could
#: never have supported it - that band gave an ATM volatility on 25 of 264 dates
#: and a skew on ZERO. The new surface keeps about three expiries live on every
#: date, so the slope is finally measurable.
#:
#: It is still not run, because this family's research budget is SIX primary
#: specifications and three signals at two horizons already spend all six.
#: Adding a fourth signal would take the family to eight, and the budget is
#: frozen exactly so that "we found something new we could test" cannot quietly
#: become "so we tested more things". Promoting the term structure to a family
#: of its own to win six fresh slots would be the same breach wearing a
#: different label. It is recorded here as available and unspent.
TERM_SLOPE_NOT_RUN = {
    "field": "term_slope",
    "newly_expressible_because": "the moneyness-anchored surface keeps ~3 expiries live per date; "
                                 "the R45 fixed band supported a skew on 0 of 264 dates",
    "declared_sign_if_it_were_run": -1,
    "economics": "far-dated minus near-dated ATM implied volatility inverts when near-term fear "
                 "spikes, and the stressed state has historically been paid",
    "why_not_run": "the family's frozen budget is 6 primaries and 3 signals x 2 horizons already "
                   "spends all of them; the budget is not raised to admit a new idea",
}


def default_grid() -> list:
    g = []
    for name, sp in SIGNALS.items():
        for h in HORIZONS:
            g.append({"family": FAMILY, "dimension": DIMENSION, "name": name, "horizon": h,
                      "field": sp["field"], "sign": sp["sign"], "economics": sp["economics"],
                      "tag": "PRIMARY", "instrument": "SPY",
                      "cell_id": "OPTIONS|SPY|%s|h%d" % (name, h)})
    return g


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #
def path(sp: dict, *, cost_bps: float) -> dict:
    """Non-overlapping per-decision NET returns at cadence = horizon."""
    f = features()
    z = _z(f[sp["field"]])
    pos = float(sp["sign"]) * np.sign(z)
    px = f["underlying_close"].to_numpy()
    h = int(sp["horizon"])
    n = len(f)
    rate = float(cost_bps) * 1e-4
    idx, net, gross, dates = [], [], [], []
    t = 0
    while t + h < n:
        p = pos[t]
        if np.isfinite(p):
            r = px[t + h] / px[t] - 1.0
            g = p * r
            c = abs(p) * 2.0 * rate
            idx.append(t)
            gross.append(g)
            net.append(g - c)
            dates.append(f["date"].iloc[t])
        t += h
    return {"net": np.array(net), "gross": np.array(gross), "dates": dates,
            "engaged": np.array([1.0 if np.isfinite(pos[i]) and pos[i] != 0 else 0.0 for i in idx]),
            "cadence": h}


def _stats(net: np.ndarray, *, h: int, label: str, cost_bps: float) -> dict:
    x = net[np.isfinite(net)]
    if len(x) < 3:
        return {"layer": label, "periods": int(len(x))}
    ppy = PPY / float(h)
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"layer": label, "periods": int(len(x)), "effective_periods": int(len(x)),
            "ann_net": float(x.mean() * ppy),
            "ann_vol": float(sd * math.sqrt(ppy)) if sd > 0 else None,
            "sharpe": float(x.mean() / sd * math.sqrt(ppy)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"],
            "max_dd": S._max_dd(x), "hit_rate": float((x > 0).mean()),
            "ann_cost_drag": float(2.0 * cost_bps * 1e-4 * ppy)}


def measure_cell(sp: dict) -> dict:
    cell = dict(sp)
    cell["calculation_owner"] = CALCULATION_OWNER
    cell["evidence_label"] = ("POST_SELECTION: the surface was acquired by R45 and this axis was "
                              "opened after the intraday axis failed; the signal signs were fixed "
                              "from theory before any arm ran")
    h = int(sp["horizon"])
    by_cost = {}
    for c in COST_LADDER_BPS:
        p = path(sp, cost_bps=c)
        n = len(p["net"])
        cut = int(round(n * SELECTION_FRACTION))
        lay = {"all": _stats(p["net"], h=h, label="ALL", cost_bps=c),
               "selection": _stats(p["net"][:cut], h=h, label="SELECTION", cost_bps=c),
               "holdout": _stats(p["net"][cut:], h=h, label="HOLDOUT", cost_bps=c)}
        ho = p["net"][cut:]
        half = len(ho) // 2
        lay["holdout_halves_ann_net"] = ([float(np.mean(ho[:half]) * PPY / h),
                                          float(np.mean(ho[half:]) * PPY / h)]
                                         if half >= 5 else None)
        by_cost["%.1f" % c] = lay
    cell["by_cost_bps_per_side"] = by_cost
    p0 = path(sp, cost_bps=0.0)
    st0 = S.nw_tstat(p0["gross"], 0)
    cell["gross"] = {"ann_gross": float(np.nanmean(p0["gross"]) * PPY / h), "t_gross": st0["t"],
                     "reaches_t2_at_zero_cost": bool((st0["t"] or 0) >= 2.0),
                     "why_this_matters": "a gross t below 2.0 cannot be rescued by any cost "
                                         "assumption; the failure would be information, not execution"}
    cell["construction"] = {
        "instrument": "SPY", "cadence": h, "overlapping": False,
        "position": "long/short, gross 1.0, sign of a strictly-trailing %d-observation z-score"
                    % ZSCORE_LOOKBACK,
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "selection_fraction": SELECTION_FRACTION,
        "horizons_not_run": HORIZON_NOT_RUN}
    prim = path(sp, cost_bps=COST_PRIMARY_BPS)
    cell["capital_applicability"] = _capital(sp, prim)
    cell["gates"] = gates(cell)
    cell["verdict"] = verdict(cell)
    return T._jsonable(cell)


def _capital(sp: dict, p: dict) -> dict:
    inc = IA.incumbent_daily_path()
    if inc is None or len(inc) == 0 or len(p["net"]) == 0:
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    # spread each decision's net return across its holding sessions so the two
    # legs are compared on one calendar
    h = int(sp["horizon"])
    s = pd.Series(p["net"], index=pd.to_datetime(p["dates"]))
    daily = s.apply(lambda r: (1.0 + r) ** (1.0 / h) - 1.0)
    daily = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="B"))
    if h > 1:
        daily = daily.ffill(limit=h - 1)
    daily = daily.dropna()
    return IA.equal_risk_daily(daily, inc, label=sp["cell_id"])


def _lvl(cell: dict, bps: float, layer: str) -> dict:
    return ((cell.get("by_cost_bps_per_side") or {}).get("%.1f" % bps) or {}).get(layer) or {}


def gates(cell: dict, *, fdr_pass: bool | None = None, holm_pass: bool | None = None) -> dict:
    a = _lvl(cell, COST_PRIMARY_BPS, "all")
    stress = _lvl(cell, COST_STRESS_BPS, "all")
    sel = _lvl(cell, COST_PRIMARY_BPS, "selection")
    hold = _lvl(cell, COST_PRIMARY_BPS, "holdout")
    halves = ((cell.get("by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS)
              or {}).get("holdout_halves_ann_net")
    cap = cell.get("capital_applicability") or {}
    ann, t = a.get("ann_net"), a.get("t_net")
    return {
        "materiality_ge_1p5pct": bool(ann is not None and ann >= MATERIALITY_ANN_NET),
        "t_ge_2": bool(t is not None and t >= 2.0),
        "survives_stress_cost": bool(stress.get("ann_net") is not None
                                     and stress["ann_net"] >= MATERIALITY_ANN_NET),
        "holdout_sign_agrees": bool(sel.get("ann_net") is not None and hold.get("ann_net") is not None
                                    and np.sign(sel["ann_net"]) == np.sign(hold["ann_net"])
                                    and hold["ann_net"] > 0),
        "holdout_halves_ge_floor": bool(halves and min(halves) >= GATE_HALF_FLOOR),
        "effective_sample_ge_floor": bool((a.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS),
        "positive_equal_risk_utility": (None if cap.get("state") != "OK"
                                        else bool(cap.get("positive_incremental_utility_after_costs"))),
        "benjamini_hochberg": fdr_pass,
        "family_holm": holm_pass,
    }


def verdict(cell: dict) -> str:
    a = _lvl(cell, COST_PRIMARY_BPS, "all")
    if (a.get("periods") or 0) < MIN_EFFECTIVE_PERIODS:
        return T.V_DATA_HOLD
    g = cell.get("gates") or {}
    ann, t = a.get("ann_net"), a.get("t_net")
    if ann is not None and ann < 0 and t is not None and t <= -2.0:
        return T.V_WORSE
    decided = {k: v for k, v in g.items() if v is not None}
    if decided and all(decided.values()) and g.get("benjamini_hochberg") is True \
            and g.get("family_holm") is not False:
        return T.V_MATERIAL
    if g.get("materiality_ge_1p5pct") and g.get("t_ge_2"):
        return T.V_NOT_QUALIFIED
    return T.V_NO_ADVANTAGE


# --------------------------------------------------------------------------- #
# Grid
# --------------------------------------------------------------------------- #
def _cell_path(cell_id: str):
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("OPTS_%s.json" % re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id))


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("OPTS_*.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER:
            out.append(c)
    return out


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    u = usability()
    if u["state"] != "USABLE":
        if verbose:
            print("OPTIONS SURFACE %s - no cell is scored.\n  %s" % (u["state"], u["why"]), flush=True)
        return []
    grid = grid or default_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            prior = json.loads(p.read_text(encoding="utf-8"))
            if not prior.get("error"):
                cells.append(prior)
                continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid), sp["cell_id"]), flush=True)
        t0 = time.time()
        try:
            cell = measure_cell(sp)
        except Exception as exc:                                        # noqa: BLE001
            cell = {**sp, "verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc),
                    "calculation_owner": CALCULATION_OWNER}
        cell["seconds"] = round(time.time() - t0, 1)
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            a = _lvl(cell, COST_PRIMARY_BPS, "all")
            print("    %-22s ann=%s t=%s periods=%s gross_t=%s" % (
                cell.get("verdict"),
                None if a.get("ann_net") is None else round(a["ann_net"], 4),
                None if a.get("t_net") is None else round(a["t_net"], 2),
                a.get("periods"),
                None if (cell.get("gross") or {}).get("t_gross") is None
                else round(cell["gross"]["t_gross"], 2)), flush=True)
    return cells


def brief(c: dict) -> dict:
    a = _lvl(c, COST_PRIMARY_BPS, "all")
    st = _lvl(c, COST_STRESS_BPS, "all")
    sel = _lvl(c, COST_PRIMARY_BPS, "selection")
    ho = _lvl(c, COST_PRIMARY_BPS, "holdout")
    cap = c.get("capital_applicability") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "name": c.get("name"),
            "dimension": c.get("dimension"), "horizon": c.get("horizon"),
            "verdict": c.get("verdict"), "economics": c.get("economics"),
            "ann_net": a.get("ann_net"), "t_net": a.get("t_net"), "sharpe": a.get("sharpe"),
            "max_dd": a.get("max_dd"), "periods": a.get("periods"),
            "ann_gross": (c.get("gross") or {}).get("ann_gross"),
            "t_gross": (c.get("gross") or {}).get("t_gross"),
            "ann_net_at_stress_cost": st.get("ann_net"),
            "selection_ann_net": sel.get("ann_net"), "holdout_ann_net": ho.get("ann_net"),
            "incremental_ann_net_return": cap.get("incremental_ann_net_return"),
            "t_incremental": cap.get("t_incremental"),
            "correlation_incumbent_sleeve": cap.get("correlation_incumbent_sleeve"),
            "fdr_pass": c.get("fdr_pass"), "holm_pass_family": c.get("holm_pass_family"),
            "failed_gates": sorted(k for k, v in (c.get("gates") or {}).items() if v is False),
            "error": c.get("error")}


def contradicted_signs(cells: list) -> dict:
    """Arms whose PRE-REGISTERED sign the data contradicts at |t| >= 2.

    This exists because the alternative is worse. An arm can be strongly
    significant in the OPPOSITE direction to the one declared from theory, and
    the campaign then faces the single most dangerous choice in empirical
    finance: flip the sign and report a winner. Flipping is forbidden - it is
    post-hoc sign selection, and a sign chosen after seeing the result carries
    no evidence at all. But silently reporting only "0 qualified" would hide a
    real, decision-relevant measurement.

    So the finding is COMPUTED, reported in full, and explicitly NOT ADOPTED.
    What it would be worth with the other sign is stated exactly, including
    which gates it would then pass, so nobody has to take the campaign's word
    for the size of what is being declined.

    The disciplined way to test a direction discovered this way is PROSPECTIVE:
    declare it now, freeze it, and let TRUE_FORWARD evidence on data that does
    not yet exist decide. That is human-gated and nothing here registers it.
    """
    out = []
    for c in cells:
        a = _lvl(c, COST_PRIMARY_BPS, "all")
        t = a.get("t_net")
        if t is None or t >= -2.0:
            continue
        sel = _lvl(c, COST_PRIMARY_BPS, "selection")
        hold = _lvl(c, COST_PRIMARY_BPS, "holdout")
        stress = _lvl(c, COST_STRESS_BPS, "all")
        halves = ((c.get("by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS)
                  or {}).get("holdout_halves_ann_net") or []
        cap = c.get("capital_applicability") or {}
        gross = (c.get("gross") or {}).get("ann_gross")
        cost_drag = abs((a.get("ann_net") or 0.0) - (gross if gross is not None else 0.0))
        flipped = {
            "ann_net": -(gross or 0.0) - cost_drag,
            "t_net": -float(t),
            "ann_net_at_stress_cost": -(gross or 0.0) - abs((stress.get("ann_net") or 0.0)
                                                            - (gross or 0.0)),
            "selection_ann_net": -(sel.get("ann_net") or 0.0),
            "holdout_ann_net": -(hold.get("ann_net") or 0.0),
            "holdout_halves_ann_net": [-h for h in halves],
            "equal_risk_increment": (None if cap.get("state") != "OK"
                                     else -(cap.get("incremental_ann_net_return") or 0.0)),
            "t_incremental": (None if cap.get("state") != "OK"
                              else -(cap.get("t_incremental") or 0.0)),
        }
        would_pass = {
            "materiality_ge_1p5pct": flipped["ann_net"] >= MATERIALITY_ANN_NET,
            "t_ge_2": flipped["t_net"] >= 2.0,
            "survives_stress_cost": flipped["ann_net_at_stress_cost"] >= MATERIALITY_ANN_NET,
            "holdout_sign_agrees": bool(
                np.sign(flipped["selection_ann_net"]) == np.sign(flipped["holdout_ann_net"])
                and flipped["holdout_ann_net"] > 0),
            "holdout_halves_ge_floor": bool(flipped["holdout_halves_ann_net"]
                                            and min(flipped["holdout_halves_ann_net"]) >= GATE_HALF_FLOOR),
            "effective_sample_ge_floor": bool((a.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS),
            "positive_equal_risk_utility": (None if flipped["equal_risk_increment"] is None
                                            else flipped["equal_risk_increment"] > 0),
        }
        out.append({
            "cell_id": c["cell_id"], "declared_sign": c.get("sign"),
            "as_declared": {"ann_net": a.get("ann_net"), "t_net": t,
                            "verdict": c.get("verdict")},
            "with_the_sign_the_data_prefers": flipped,
            "gates_it_would_then_pass": would_pass,
            "would_pass_every_gate": all(v for v in would_pass.values() if v is not None),
            "adopted": False,
            "why_not_adopted": (
                "the sign was pre-registered from theory BEFORE any arm ran and the data "
                "contradicts it. Flipping a declared sign after seeing the result is post-hoc "
                "sign selection: the flipped number is not evidence, because the direction was "
                "chosen by the same sample that scores it. No threshold was met as registered, "
                "so the campaign reports 0 qualified."),
            "the_only_honest_way_to_test_it": (
                "declare the opposite direction NOW, freeze it, and let TRUE_FORWARD evidence on "
                "data that does not yet exist decide. That is human-gated and nothing here "
                "registers it."),
            "why_it_should_be_treated_sceptically": (
                "two years is one regime, the arm has %s non-overlapping periods, and an effect "
                "this large in a heavily-traded, widely-published SPY signal is more consistent "
                "with a sample artefact than with an edge that survived everyone else looking "
                "for it" % (a.get("effective_periods"))),
        })
    return {"n": len(out), "arms": out,
            "rule": "reported whenever a PRE-REGISTERED sign is contradicted at t <= -2.0; "
                    "never acted on"}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    scored = [c for c in cells if _lvl(c, COST_PRIMARY_BPS, "all").get("p_net_one_sided") is not None]
    p_one = {c["cell_id"]: _lvl(c, COST_PRIMARY_BPS, "all")["p_net_one_sided"] for c in scored}
    bh = S.bh_fdr(p_one, BH_Q) if p_one else {"per_test": {}}
    holm = FAM.holm(p_one, HOLM_ALPHA) if p_one else {"rejected": {}}
    for c in scored:
        c["fdr_pass"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get("rejected", {}).get(c["cell_id"])
        c["gates"] = gates(c, fdr_pass=c["fdr_pass"], holm_pass=c["holm_pass_family"])
        c["verdict"] = verdict(c)
    counts: dict = {}
    for c in cells:
        counts[c.get("verdict")] = counts.get(c.get("verdict"), 0) + 1
    f = features()
    best = max(scored, key=lambda c: _lvl(c, COST_PRIMARY_BPS, "all").get("ann_net") or -9,
               default=None) if scored else None
    body = {
        "schema": "alpha_recovery_options_surface/1", "calculation_owner": CALCULATION_OWNER,
        "axis": "OPTIONS_IMPLIED_VOLATILITY_SURFACE",
        "question": "does an ACTUAL option surface satisfy the VOLATILITY_EXPECTATIONS_IV need that "
                    "two index-proxy attempts could not?",
        "resolves_named_binding_failure": (
            "VOLATILITY|TS|21|VOLATILITY_EXPECTATIONS_IV returned DATA_HOLD on 196 rows against "
            "sensitivity.MIN_ROWS = 200, and its cadence rescue failed reproduction against the "
            "persisted R63 matrix because the FRED / Cboe index inputs had drifted. This axis "
            "replaces the index proxy with per-contract implied volatilities. The row floor was "
            "never moved."),
        "usability": usability(),
        # Reported, never acted on. See contradicted_signs.
        "pre_registered_signs_the_data_contradicts": contradicted_signs(cells),
        "surface": {"path": str(surface_path()),
                    "is_moneyness_anchored": surface_path() != R45_SURFACE_PATH,
                    "dates": int(len(f)),
                    "first": str(f["date"].min())[:10], "last": str(f["date"].max())[:10],
                    "median_contracts_per_date": int(f["n_contracts"].median()),
                    "median_expiries_per_date": int(f["n_expiries"].median()),
                    "atm_iv_near_median": float(np.nanmedian(f["atm_iv_near"])),
                    "skew_median": float(np.nanmedian(f["skew"])),
                    "vrp_median": float(np.nanmedian(f["vrp"])),
                    "purchased_anything": False},
        "limits": {"single_regime": "265 dates, ~13 months; no regime partition is possible",
                   "narrow_surface": "median 22 contracts and 3 expiries per date, moneyness "
                                     "0.91-1.139; no deep wings, so no 25-delta or tail construct",
                   "post_lockbox": "the whole sample post-dates the lockbox start 2023-01-01; a "
                                   "chronological split was declared instead, before running",
                   "horizons_not_run": HORIZON_NOT_RUN},
        "signals": SIGNALS, "grid": default_grid(), "n_cells": len(cells), "counts": counts,
        "budget": {"primary_max": 6, "n_primary": len([c for c in cells if c.get("tag") != "RESCUE"]),
                   "rescue_max": 2, "n_rescue": len([c for c in cells if c.get("tag") == "RESCUE"]),
                   "mirror_signs_not_executed": "each signal carries ONE economically pre-registered "
                                                "sign; running the mirror would double the "
                                                "multiplicity denominator for no information"},
        "cost_ladder_bps_per_side": {"primary_canonical_spy": COST_PRIMARY_BPS,
                                     "intraday_primary": ID.COST_PRIMARY_BPS,
                                     "stress": COST_STRESS_BPS},
        "multiple_testing": {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
                             "holm": {k: v for k, v in holm.items() if k != "adjusted"},
                             "denominator": len(p_one)},
        "best_by_ann_net": brief(best) if best else None,
        "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
        "cells": cells,
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
