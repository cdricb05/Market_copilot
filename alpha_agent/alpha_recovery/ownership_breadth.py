"""alpha_agent.alpha_recovery.ownership_breadth - the preregistered 13F
institutional-ownership-breadth experiment.

Executes ``research/preregistration/INSTITUTIONAL_OWNERSHIP_BREADTH_PREREGISTRATION.md``
(frozen at commit 38453b9, BEFORE any result existed) and changes nothing in it.

    hypothesis   the one-quarter CHANGE in the number of distinct 13F filers
                 reporting a position carries cross-sectional information about
                 subsequent excess return that the estate's owned price,
                 fundamental and short-positioning families do not already carry
    direction    TWO-SIDED. No sign is pre-specified, so a sign read off the
                 sample is a DISCOVERY and is labelled one; it cannot
                 retroactively qualify this experiment
    horizons     21 and 63 sessions, non-overlapping
    rebalance    quarterly, the first session after each 13F deadline
    scorer       alpha_agent.r63.sensitivity.run_cell - the ONE scorer
    PIT engine   alpha_agent.r63.pit
    burden       alpha_agent.r39.burden -> alpha_agent.r31.multiple_testing,
                 ONE declared family, the two horizons as its cells

TWO CONSTRUCTION DECISIONS the preregistration left open, both fixed here
BEFORE any result was computed and both point-in-time safe either way:

* **Equal-age breadth.** Each quarter's breadth is counted at ITS OWN decision
  date, never both quarters at the later one. Only 87 % of 13F filings are in by
  day 45 and 3.7 % arrive after day 60 (measured, not assumed), so counting the
  current quarter at day ~46 against a prior quarter that has had a further
  three months to fill up would manufacture a negative drift whose size depends
  on how promptly a name's holders happen to file. The alternative is reported
  as a sensitivity, never as the headline.
* **The cost ladder.** The preregistration froze 1 / 2 / 5 bp per side. That
  ladder was written for a SPY-class instrument; the estate's own single-name
  equity rate is ``EQ_COST_RATE_PER_SIDE`` = 12.5 bp. Both are reported. The
  frozen gate is judged at the frozen ladder, and the desk rate is reported
  alongside because it is the honest cost of this asset - a stricter number can
  only make qualification harder, never easier.

RESEARCH ONLY. No purchase, no promotion, no capital, no proposal, no order,
no fill, no backfill, no live write.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from alpha_agent.r31 import multiple_testing as MT
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import ontology as ONT
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, CONDITIONAL_T_FLOOR, EQ_COST_RATE_PER_SIDE, LOCKBOX_START,
               MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, now_iso, write_artifact)
from . import incumbent as INC
from . import ownership_13f as O13
from . import ownership_identity as OI

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ownership_breadth"
ARTIFACT_NAME = "ownership_breadth_13f.json"

DIMENSION = "OWNERSHIP_INSTITUTIONAL_FLOW"
FAMILY_ID = "OWNERSHIP_INSTITUTIONAL_FLOW_13F_BREADTH_CHANGE_V1"

#: FROZEN: the two cells of the ONE declared family.
HORIZONS_FROZEN = (21, 63)
#: A reporting quarter is ~63 sessions; the scorer uses this for the
#: Newey-West lag and the effective-period discount, and at this cadence both
#: frozen horizons are strictly non-overlapping.
CADENCE_QUARTERLY = 63

#: 13F is due 45 calendar days after the period end (17 CFR 240.13f-1).
DEADLINE_DAYS = 45
QUARTER_ENDS = ((3, 31), (6, 30), (9, 30), (12, 31))

#: FROZEN by the preregistration, plus the estate's own single-name rate.
COST_LADDER_BPS = (1.0, 2.0, 5.0)
DESK_EQUITY_COST_BPS = EQ_COST_RATE_PER_SIDE * 1e4

#: FROZEN stopping rule.
MIN_DATE_COVERAGE = 0.95
MAX_UNCOVERED_SHARE = 0.20

V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_NO_INCREMENTAL = "NO_INCREMENTAL_INFORMATION_EDGE"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
VERDICTS = (V_QUALIFIED, V_NO_EDGE, V_NO_INCREMENTAL, V_DATA_HOLD, V_NEED_MORE)

SIGN_PRESPECIFIED = "PRE_SPECIFIED"
SIGN_DISCOVERY = "DISCOVERY_ONLY"
SIGN_NONE = "NONE"


# --------------------------------------------------------------------------- #
# The quarterly decision grid: the first session after each 13F deadline
# --------------------------------------------------------------------------- #
def reporting_periods(first: str, last: str) -> list:
    out = []
    for y in range(int(first[:4]), int(last[:4]) + 1):
        for m, d in QUARTER_ENDS:
            p = pd.Timestamp(year=y, month=m, day=d)
            if first <= str(p)[:10] <= last:
                out.append(p)
    return sorted(out)


def deadline_for(period) -> pd.Timestamp:
    return pd.Timestamp(period) + pd.Timedelta(days=DEADLINE_DAYS)


def decision_grid(dates: np.ndarray, periods: list) -> list:
    """One decision per reporting period, on the first session STRICTLY after
    that period's filing deadline."""
    ds = pd.DatetimeIndex(pd.to_datetime(np.asarray(dates)))
    grid = []
    for p in periods:
        dl = deadline_for(p)
        pos = int(np.searchsorted(ds.values, np.datetime64(dl), side="right"))
        if pos >= len(ds):
            continue
        grid.append({"period": p, "deadline": dl, "t": pos,
                     "date": str(ds[pos])[:10]})
    return grid


# --------------------------------------------------------------------------- #
# The signal
# --------------------------------------------------------------------------- #
def breadth_by_period(grid: list, sub: pd.DataFrame, *, verbose: bool = True) -> dict:
    """period tag -> (cusip8 -> distinct filer count), each counted AT ITS OWN
    decision date so every quarter is observed at the same filing age."""
    out = {}
    for g in grid:
        tag = O13._period_tag(g["period"])
        b = O13.breadth_as_of(sub, g["period"], g["date"])
        out[tag] = b
        if verbose:
            print("  breadth %s as of %s -> %d names, %d holdings-weighted"
                  % (tag, g["date"], len(b), int(b.sum()) if len(b) else 0), flush=True)
    return out


def signal_matrix(E: dict, elig: np.ndarray, grid: list, breadth: dict,
                  resolve, *, verbose: bool = True) -> dict:
    """The frozen signal on the daily grid: at each decision date, the
    cross-sectional rank in [0,1] of the quarter-over-quarter CHANGE in distinct
    13F holders, over eligible names the point-in-time bridge resolves."""
    n_i, n_t = len(E["symbols"]), len(E["dates"])
    sig = np.full((n_i, n_t), np.nan)
    raw = np.full((n_i, n_t), np.nan)
    per_date, held_by_date = [], {}
    for k, g in enumerate(grid):
        if k == 0:
            continue
        tag, prev_tag = O13._period_tag(g["period"]), O13._period_tag(grid[k - 1]["period"])
        cur, prv = breadth.get(tag), breadth.get(prev_tag)
        if cur is None or prv is None or not len(cur) or not len(prv):
            continue
        t, d = g["t"], g["date"]
        br = resolve(d)
        c2r = br["cusip_to_row"]
        names = set(cur.index) | set(prv.index)
        held_by_date[d] = set(names)
        # Aggregate to the PANEL ROW before differencing. A security that
        # changed CUSIP between the two quarters would otherwise show a phantom
        # exit on the old identifier and a phantom entry on the new one - two
        # large opposite changes on what is one security - and differencing per
        # CUSIP would score whichever the tie-break happened to pick.
        cur_row: dict = {}
        prv_row: dict = {}
        for c in names:
            r = c2r.get(c)
            if r is None or not elig[r, t]:
                continue
            cur_row[r] = cur_row.get(r, 0.0) + float(cur.get(c, 0))
            prv_row[r] = prv_row.get(r, 0.0) + float(prv.get(c, 0))
        if len(cur_row) < 50:
            per_date.append({"date": d, "period": tag, "n_scored": len(cur_row),
                             "state": "TOO_FEW_NAMES"})
            continue
        rows = np.asarray(sorted(cur_row))
        dv = np.asarray([cur_row[r] - prv_row[r] for r in rows], dtype=float)
        rk = pd.Series(dv).rank(method="average").to_numpy()
        rk = (rk - 1.0) / max(len(rk) - 1.0, 1.0)
        sig[rows, t] = rk
        raw[rows, t] = dv
        universe = int(elig[:, t].sum())
        per_date.append({
            "date": d, "period": tag, "n_scored": int(len(rows)),
            "universe": universe,
            "coverage": float(len(rows) / universe) if universe else 0.0,
            "cusips_reported": len(names),
            "cusips_bridged": int(sum(1 for c in names if c in c2r)),
            "bridge_ambiguous": br["n_ambiguous_cusip"],
            "mean_breadth_change": float(dv.mean()),
            "median_breadth_change": float(np.median(dv)),
            "state": "OK"})
        if verbose:
            print("  signal %s n=%d universe=%d coverage=%.3f"
                  % (d, len(rows), universe, per_date[-1]["coverage"]), flush=True)
    return {"signal": sig, "raw": raw, "per_date": per_date,
            "held_by_date": held_by_date}


# --------------------------------------------------------------------------- #
# Dataset assembly on the frozen grid
# --------------------------------------------------------------------------- #
def assemble(h: int, sig: np.ndarray, dec: np.ndarray, *,
             cost_bps: float | None = None) -> dict:
    """The canonical R63 equity dataset, on the FROZEN quarterly grid, with the
    ownership block added. Nothing about the substrate, eligibility, returns or
    baseline is re-implemented."""
    base = X.assemble_equity(int(h))
    ds = dict(base)
    ds["blocks"] = dict(base["blocks"])
    ds["blocks"][DIMENSION] = sig[:, :, None]
    ds["dec"] = np.asarray(dec, dtype=int)
    ds["cadence"] = CADENCE_QUARTERLY
    if cost_bps is not None:
        ds["cost"] = np.full(len(ds["inst"]), float(cost_bps) / 1e4)
    return ds


def baseline_dims(ds: dict) -> tuple:
    return tuple(d for d in ONT.baseline_for("US_EQUITY") if d in ds["blocks"])


# --------------------------------------------------------------------------- #
# Descriptive measurement, through the canonical owners only
# --------------------------------------------------------------------------- #
def _xs_residual(x: np.ndarray, Z: np.ndarray) -> np.ndarray:
    """Residual of ``x`` on ``Z`` within ONE cross-section. A contemporaneous
    cross-sectional regression uses no future information, so orthogonalising
    this way cannot leak; residualising on a time-series fit could."""
    ok = np.isfinite(x) & np.isfinite(Z).all(axis=1)
    if ok.sum() < 30:
        return np.full(len(x), np.nan)
    A = np.column_stack([np.ones(ok.sum()), Z[ok]])
    beta = np.linalg.lstsq(A, x[ok], rcond=None)[0]
    out = np.full(len(x), np.nan)
    out[ok] = x[ok] - A @ beta
    return out


def raw_and_orthogonal(ds: dict, sig: np.ndarray, *, h: int,
                       incumbent_score: np.ndarray) -> dict:
    """Per-period rank IC and long-short book of the RAW signal, and of the
    signal residualised on the owned baseline plus the incumbent, in the SAME
    cross-sections. The book is :func:`sensitivity._book_returns`, the canonical
    one; nothing here is a second scorer."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims_b = baseline_dims(ds)
    rows = {"gid": [], "iid": [], "pred_raw": [], "pred_orth": [], "y": [],
            "vol": [], "cost": []}
    ic_raw, ic_orth, per = [], [], []
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        x = sig[ix, t]
        y = ds["y"][ix, t]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims_b]
                            + [incumbent_score[ix, t][:, None]])
        xo = _xs_residual(x, Z)
        a = S.spearman(x, y)
        b = S.spearman(xo, y)
        ic_raw.append(a)
        ic_orth.append(b)
        keep = np.isfinite(xo)
        rows["gid"].append(np.full(ok.sum(), j))
        rows["iid"].append(ix)
        rows["pred_raw"].append(x)
        rows["pred_orth"].append(np.where(keep, xo, np.nan))
        rows["y"].append(y)
        rows["vol"].append(ds["vol"][ix, t])
        rows["cost"].append(ds["cost"][ix])
        per.append({"date": str(np.asarray(ds["dates"])[t])[:10], "n": int(ok.sum()),
                    "rank_ic_raw": a, "rank_ic_orth": b,
                    "layer": "LOCKBOX" if str(np.asarray(ds["dates"])[t])[:10] >= LOCKBOX_START
                    else "SELECTION"})
    if not per:
        return {"state": "NO_PERIODS"}
    cat = {k: np.concatenate(v) for k, v in rows.items()}
    out = {"state": "OK", "n_periods": len(per), "per_period": per,
           "rank_ic_raw": _stat(ic_raw), "rank_ic_orthogonalised": _stat(ic_orth)}
    for label, pred in (("raw", cat["pred_raw"]), ("orthogonalised", cat["pred_orth"])):
        out["book_%s" % label] = {}
        for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
            bk = S._book_returns(pred, cat["y"], cat["gid"], cat["iid"], cat["vol"],
                                 np.full(len(cat["y"]), bps / 1e4), "XS", "XS_LONG_SHORT",
                                 1.0)
            out["book_%s" % label]["%.1fbp" % bps] = _book_stats(bk, h)
    return out


def incremental_vs_incumbent(ds: dict, sig: np.ndarray, *, h: int,
                             incumbent_score: np.ndarray, top_n: int = 50) -> dict:
    """Does the information ADD to the incumbent, in economic units?

    Two long-only top-N books on the identical rows, dates, costs and
    construction: the incumbent alone, and the incumbent blended equally (in
    cross-sectional z units, the estate's own 50/50 convention) with the part of
    the ownership signal the owned families and the incumbent do NOT already
    explain. The difference is the incremental utility. Both books are priced by
    :func:`sensitivity._book_returns`; no second book is written here.

    The blend weight is NOT preregistered, so this is reported as a descriptive
    measurement and can never qualify the experiment on its own.
    """
    dec = np.asarray(ds["dec"], dtype=int)
    dims_b = baseline_dims(ds)
    acc = {k: [] for k in ("gid", "iid", "y", "vol", "cost", "inc", "blend")}
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t]) \
            & np.isfinite(incumbent_score[:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims_b]
                            + [incumbent_score[ix, t][:, None]])
        xo = _xs_residual(sig[ix, t], Z)
        zi = _z(incumbent_score[ix, t])
        zs = _z(xo)
        blend = np.where(np.isfinite(zs), 0.5 * zi + 0.5 * zs, zi)
        acc["gid"].append(np.full(len(ix), j))
        acc["iid"].append(ix)
        acc["y"].append(ds["y"][ix, t])
        acc["vol"].append(ds["vol"][ix, t])
        acc["cost"].append(ds["cost"][ix])
        acc["inc"].append(zi)
        acc["blend"].append(blend)
    if not acc["gid"]:
        return {"state": "NO_PERIODS"}
    c = {k: np.concatenate(v) for k, v in acc.items()}
    out = {"state": "OK", "blend": "0.5 z(incumbent) + 0.5 z(orthogonalised ownership)",
           "book": "EQ_LONG_ONLY_TOPN vs the equal-weight scored universe",
           "top_n": top_n}
    for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
        cost = np.full(len(c["y"]), bps / 1e4)
        a = S._book_returns(c["inc"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        b = S._book_returns(c["blend"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        n = min(len(a["net"]), len(b["net"]))
        d = b["net"][:n] - a["net"][:n]
        st = S.nw_tstat(d, 0)
        ppy = 252.0 / float(h)
        out["%.1fbp" % bps] = {
            "incumbent": _book_stats(a, h), "incumbent_plus_ownership": _book_stats(b, h),
            "incremental_ann_net": float(d.mean() * ppy) if n else None,
            "t_incremental": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "periods": int(n)}
    return out


def _z(v: np.ndarray) -> np.ndarray:
    x = np.asarray(v, dtype=float)
    ok = np.isfinite(x)
    if ok.sum() < 5:
        return np.full(len(x), np.nan)
    sd = x[ok].std()
    out = np.full(len(x), np.nan)
    if sd > 1e-12:
        out[ok] = (x[ok] - x[ok].mean()) / sd
    return out


def _stat(v: list) -> dict:
    a = np.asarray([x for x in v], dtype=float)
    a = a[np.isfinite(a)]
    if a.size < 3:
        return {"n": int(a.size), "mean": None, "t": None, "p_two_sided": None}
    st = S.nw_tstat(a, 0)
    return {"n": st["n"], "mean": st["mean"], "t": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "positive_fraction": float((a > 0).mean())}


def _book_stats(bk: dict, h: int) -> dict:
    g, n = bk["gross"], bk["net"]
    if not len(n):
        return {"periods": 0}
    ppy = 252.0 / float(h)
    st = S.nw_tstat(n, 0)
    sd = float(np.std(n, ddof=1)) if len(n) > 1 else 0.0
    return {"periods": int(len(n)),
            "gross_ann": float(g.mean() * ppy), "net_ann": float(n.mean() * ppy),
            "t_net": st["t"], "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "sharpe_ann": float(n.mean() / sd * np.sqrt(ppy)) if sd > 0 else None,
            "max_drawdown": S._max_dd(n),
            "mean_oneway_turnover": float(np.mean(bk["turnover"])),
            "cost_drag_ann": float(np.mean(bk["cost"]) * ppy)}


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def verdict_for(cell: dict, inc_cell: dict, desc: dict, coverage: dict,
                *, fdr_pass: bool | None) -> dict:
    """Only the preregistered gates. No threshold is invented here."""
    why = []
    if coverage.get("uncovered_share", 0.0) > MAX_UNCOVERED_SHARE:
        return {"verdict": V_DATA_HOLD,
                "why": "%.0f%% of rebalance dates resolve under %.0f%% of the universe"
                       % (100 * coverage["uncovered_share"], 100 * MIN_DATE_COVERAGE)}
    eff = cell.get("effective_periods")
    if cell.get("verdict") == S.V_DATA_HOLD or eff is None:
        n = cell.get("rows_covered")
        return {"verdict": V_NEED_MORE,
                "why": "the canonical scorer returned DATA_HOLD (%s); a quarterly "
                       "rebalance cannot reach its floor of %d scorable periods on "
                       "this substrate" % (cell.get("why"), 2 * MIN_EFFECTIVE_PERIODS),
                "rows_covered": n}
    if eff < MIN_EFFECTIVE_PERIODS:
        return {"verdict": V_NEED_MORE,
                "why": "%d effective independent periods, floor %d" % (eff, MIN_EFFECTIVE_PERIODS)}
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or abs(t) < CONDITIONAL_T_FLOOR:
        why.append("conditional |t| %s below the %.1f floor"
                   % ("None" if t is None else round(t, 2), CONDITIONAL_T_FLOOR))
        return {"verdict": V_NO_EDGE, "why": "; ".join(why)}
    if fdr_pass is False:
        return {"verdict": V_NO_EDGE,
                "why": "does not survive the declared multiple-testing burden"}
    ic_o = (desc.get("rank_ic_orthogonalised") or {}).get("t")
    red = (cell.get("redundancy") or {}).get("redundancy")
    inc_t = ((inc_cell or {}).get("conditional") or {}).get("t")
    if red == "REDUNDANT" or ic_o is None or abs(ic_o) < CONDITIONAL_T_FLOOR \
            or inc_t is None or abs(inc_t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_INCREMENTAL,
                "why": "the effect does not survive orthogonalisation against the "
                       "owned families and the incumbent (redundancy=%s, "
                       "orthogonalised IC t=%s, incremental-vs-incumbent t=%s)"
                       % (red, None if ic_o is None else round(ic_o, 2),
                          None if inc_t is None else round(inc_t, 2))}
    e = cell.get("economics") or {}
    if (e.get("ann_net_increment") or 0.0) < MATERIALITY_ANN_NET:
        return {"verdict": V_NO_EDGE,
                "why": "net annual increment %.4f below the %.3f materiality floor"
                       % (e.get("ann_net_increment") or 0.0, MATERIALITY_ANN_NET)}
    return {"verdict": V_QUALIFIED, "why": "every preregistered gate passed"}


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run(*, verbose: bool = True, write: bool = True,
        bridge_mode: str = OI.BRIDGE_CUSIP_ANCHORED) -> dict:
    if not any(O13.holdings_dir().glob("holdings_*.npz")):
        return {"state": "NOT_ACQUIRED", "calculation_owner": CALCULATION_OWNER,
                "why": "no 13F holdings store under %s; run the free acquisition "
                       "first (ownership_data.acquire, then "
                       "ownership_13f.build_holdings)" % O13.holdings_dir()}
    E, elig = INC.equity_substrate()
    dates = np.asarray(E["dates"])
    sub = O13.build_submission_index(verbose=False)
    obs = OI.build_observations(verbose=False)
    resolve, bridge_meta = OI.make_resolver(E, obs, bridge_mode)
    periods = reporting_periods("2013-03-31", str(sub["period"].max())[:10])
    grid = decision_grid(dates, periods)
    if verbose:
        print("decision grid: %d quarterly dates %s -> %s"
              % (len(grid), grid[0]["date"] if grid else None,
                 grid[-1]["date"] if grid else None), flush=True)
    breadth = breadth_by_period(grid, sub, verbose=verbose)
    sm = signal_matrix(E, elig, grid, breadth, resolve, verbose=verbose)
    sig = sm["signal"]
    ok_dates = [r for r in sm["per_date"] if r.get("state") == "OK"]
    uncovered = [r for r in ok_dates if r["coverage"] < MIN_DATE_COVERAGE]
    coverage = {
        "n_rebalance_dates": len(ok_dates),
        "coverage_mean": float(np.mean([r["coverage"] for r in ok_dates])) if ok_dates else 0.0,
        "coverage_min": float(np.min([r["coverage"] for r in ok_dates])) if ok_dates else 0.0,
        "n_uncovered_dates": len(uncovered),
        "uncovered_share": float(len(uncovered) / len(ok_dates)) if ok_dates else 1.0,
        "min_date_coverage_rule": MIN_DATE_COVERAGE,
        "max_uncovered_share_rule": MAX_UNCOVERED_SHARE,
        "bridge_mode": bridge_meta,
        "bridge_mode_chosen_before_any_alpha_was_computed": True,
    }
    blocks = INC.incumbent_blocks(E, elig)
    inc_score = blocks[INC.BLOCK_SCORE][:, :, 0]
    dec_all = np.array([g["t"] for g in grid if any(
        r["date"] == g["date"] and r.get("state") == "OK" for r in sm["per_date"])], dtype=int)

    cells, cells_inc, descs = {}, {}, {}
    for h in HORIZONS_FROZEN:
        ds = assemble(h, sig, dec_all)
        dims_b = baseline_dims(ds)
        if verbose:
            print("h=%d baseline dims=%d decisions=%d" % (h, len(dims_b), len(ds["dec"])),
                  flush=True)
        cells[h] = S.run_cell(ds, dims_b, DIMENSION, keep_predictions=False)
        ds_i = assemble(h, sig, dec_all)
        ds_i["blocks"][INC.BLOCK_SCORE] = blocks[INC.BLOCK_SCORE]
        cells_inc[h] = S.run_cell(ds_i, (INC.BLOCK_SCORE,), DIMENSION, keep_predictions=False)
        descs[h] = raw_and_orthogonal(ds, sig, h=h, incumbent_score=inc_score)
        descs[h]["incremental_vs_incumbent"] = incremental_vs_incumbent(
            ds, sig, h=h, incumbent_score=inc_score)

    # ONE declared family, the two horizons as its cells: the denominator is 2
    pvals = [(descs[h].get("rank_ic_raw") or {}).get("p_two_sided") for h in HORIZONS_FROZEN]
    bh = MT.benjamini_hochberg(pvals, BH_Q)
    fdr = {h: (i in bh["rejected"]) for i, h in enumerate(HORIZONS_FROZEN)}

    results = {}
    for h in HORIZONS_FROZEN:
        v = verdict_for(cells[h], cells_inc[h], descs[h], coverage, fdr_pass=fdr[h])
        mean_ic = (descs[h].get("rank_ic_raw") or {}).get("mean")
        results[str(h)] = {
            "horizon": h, "verdict": v["verdict"], "why": v["why"],
            "sign_result": SIGN_DISCOVERY if (mean_ic is not None and abs(mean_ic) > 0)
            else SIGN_NONE,
            "sign_observed": None if mean_ic is None else ("POSITIVE" if mean_ic > 0 else "NEGATIVE"),
            "cell": cells[h], "cell_vs_incumbent": cells_inc[h], "descriptive": descs[h],
            "fdr_pass": fdr[h],
        }
    body = {
        "schema": "alpha_recovery_ownership_breadth_13f/1",
        "calculation_owner": CALCULATION_OWNER,
        "family_id": FAMILY_ID,
        "dimension": DIMENSION,
        "preregistration": "research/preregistration/"
                           "INSTITUTIONAL_OWNERSHIP_BREADTH_PREREGISTRATION.md",
        "preregistration_commit": "38453b9",
        "preregistration_altered_after_results": False,
        "direction": "TWO_SIDED_NO_SIGN_PRESPECIFIED",
        "sign_policy": "a sign read off the sample is DISCOVERY_ONLY and cannot "
                       "retroactively qualify this experiment",
        "horizons": list(HORIZONS_FROZEN),
        "cadence_sessions": CADENCE_QUARTERLY,
        "rebalance_rule": "first session strictly after the 45-day 13F deadline",
        "breadth_construction": "EQUAL_AGE: each quarter counted at its own decision date",
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "desk_single_name_cost_bps_per_side": DESK_EQUITY_COST_BPS,
        "scorer": S.CALCULATION_OWNER,
        "pit_engine": pit.CALCULATION_OWNER,
        "multiplicity_owner": MT.CALCULATION_OWNER,
        "multiplicity": {"family": FAMILY_ID, "m_declared": len(HORIZONS_FROZEN),
                         "benjamini_hochberg": bh, "q": BH_Q,
                         "denominator_reset": False, "horizons_added_after_results": False},
        "coverage": coverage,
        "signal_per_date": sm["per_date"],
        "decision_dates": [g["date"] for g in grid],
        "results": results,
        "generated_at": now_iso(),
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
