"""alpha_agent.alpha_recovery.control_block_alpha - the preregistered Schedule
13D/13G control-block experiment.

Executes ``research/preregistration/CONTROL_BLOCK_13DG_PREREGISTRATION.md``
(frozen at commit 1d3f491, BEFORE any return existed) and changes nothing in it.

    hypothesis   a meaningful NEW or INCREASED control block reported on
                 Schedule 13D/G carries information about subsequent equity
                 return that the estate's owned families do not already carry
    direction    TWO-SIDED. A sign read off the sample is DISCOVERY_ONLY and
                 cannot retroactively qualify this experiment
    cells        A NEW_CONTROL_BLOCK, B MATERIAL_BLOCK_INCREASE (>= 1.00 pp,
                 Rule 13d-2(a)'s own materiality standard)
    horizons     5, 21, 63 sessions -> six tests in ONE declared family, m = 6
    PIT          the SEC ACCEPTANCE INSTANT (UTC -> Eastern), first session
                 whose 16:00 close is strictly after it, entry the NEXT close
    scorer       alpha_agent.r63.sensitivity.run_cell - the ONE scorer
    burden       alpha_agent.r31.multiple_testing, the denominator NOT reset

TWO ARMS, both frozen, because they answer different questions and neither is
dropped after seeing which is kinder:

* the EVENT STUDY enters at the event's own next close and measures magnitude;
* the CROSS-SECTIONAL PANEL places the same events on the canonical R63 grid and
  measures whether the information is INCREMENTAL to the owned families. It
  dilutes entry by up to one cadence and a sparse binary signal ties heavily -
  declared in the preregistration, not discovered here.

RESEARCH ONLY. No purchase, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from alpha_agent.r31 import multiple_testing as MT
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import ontology as ONT
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, CONDITIONAL_T_FLOOR, EQ_COST_RATE_PER_SIDE, LOCKBOX_START,
               MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, now_iso, write_artifact)
from . import control_block_events as CBE
from . import incumbent as INC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.control_block_alpha"
ARTIFACT_NAME = "control_block_13dg.json"

DIMENSION = "OWNERSHIP_INSTITUTIONAL_FLOW"
FAMILY_ID = "OWNERSHIP_CONTROL_BLOCK_13DG_EVENT_V1"
PREREGISTRATION = ("research/preregistration/"
                   "CONTROL_BLOCK_13DG_PREREGISTRATION.md")
PREREGISTRATION_COMMIT = "1d3f491"

#: FROZEN. Three horizons, two cells -> six cells in ONE declared family.
HORIZONS_FROZEN = (5, 21, 63)
COST_LADDER_BPS = (1.0, 2.0, 5.0)
DESK_EQUITY_COST_BPS = EQ_COST_RATE_PER_SIDE * 1e4

#: FROZEN stopping rules.
MIN_DATE_COVERAGE = 0.95
MAX_UNCOVERED_SHARE = 0.20

PPY = 252.0

V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_NO_INCREMENTAL = "NO_INCREMENTAL_INFORMATION_EDGE"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"

SIGN_PRESPECIFIED = "PRE_SPECIFIED"
SIGN_DISCOVERY = "DISCOVERY_ONLY"
SIGN_NONE = "NONE"


# --------------------------------------------------------------------------- #
# Returns the event study needs (the substrate's own, never re-derived)
# --------------------------------------------------------------------------- #
def _session_returns(E: dict) -> np.ndarray:
    tr = E["price"]["tr"]
    r = np.full(tr.shape, np.nan)
    r[:, 1:] = tr[:, 1:] / tr[:, :-1] - 1.0
    return r


def _spy_forward(E: dict, h: int) -> np.ndarray:
    spy = np.asarray(E["price"]["spy_tr"], dtype=float)[None, :]
    sr = np.full(spy.shape, np.nan)
    sr[:, 1:] = spy[:, 1:] / spy[:, :-1] - 1.0
    return pit.forward_compound(sr, h)[0]


def _sector_forward(E: dict, elig: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Equal-weight forward return of the eligible same-sector names.

    DECLARED LIMITATION, inherited from R58 and not invented here: the estate
    owns a CURRENT GICS snapshot and no point-in-time sector history, so a 2013
    event is adjusted by the sector the issuer is classified in TODAY. That is
    why sector adjustment is reported as a DIAGNOSTIC and is never a
    qualification input.
    """
    sec = np.asarray(E["price"].get("sectors")
                     if isinstance(E["price"], dict) else None)
    n_i, n_t = y.shape
    out = np.full((n_i, n_t), np.nan)
    if sec is None or sec.shape != (n_i,):
        return out
    for s in np.unique(sec):
        rows = np.where(sec == s)[0]
        if len(rows) < 3:
            continue
        blk = y[rows, :]
        msk = elig[rows, :] & np.isfinite(blk)
        tot = np.where(msk, blk, 0.0).sum(axis=0)
        cnt = msk.sum(axis=0)
        # leave-one-out, so a name is never benchmarked against itself
        with np.errstate(invalid="ignore", divide="ignore"):
            loo = (tot[None, :] - np.where(msk, blk, 0.0)) / np.maximum(cnt[None, :] - 1, 1)
        out[rows, :] = np.where(cnt[None, :] > 1, loo, np.nan)
    return out


# --------------------------------------------------------------------------- #
# ARM 1 - the event study
# --------------------------------------------------------------------------- #
def event_study(E: dict, elig: np.ndarray, events: list, *, h: int,
                vol63: np.ndarray) -> dict:
    """Entry at the event's OWN next close; hold h sessions.

    Aggregation clusters by decision session and the t-statistic is Newey-West
    at the canonical lag for (h, cadence=1), because two events h sessions apart
    share a window and are not independent observations.
    """
    y = pit.forward_compound(_session_returns(E), h)
    spy = _spy_forward(E, h)
    sec = _sector_forward(E, elig, y)
    ret1 = _session_returns(E)
    # POST-HOC DESCRIPTIVE, and excluded from every gate: the 21 sessions of
    # return BEFORE the disclosure session. It qualifies nothing; it exists so
    # that "the announcement session moved the price" can be told apart from
    # "the price had already moved and the filing followed it", which is the
    # difference between information and a report of past accumulation.
    lg = np.log1p(np.clip(np.where(np.isfinite(ret1), ret1, 0.0), -0.999999, None))
    cum = np.concatenate([np.zeros((lg.shape[0], 1)), np.cumsum(lg, axis=1)], axis=1)
    rows = []
    for e in events:
        i, t = e["row"], e["t"]
        if t >= y.shape[1] or not np.isfinite(y[i, t]):
            continue
        raw = float(y[i, t])
        mkt = raw - float(spy[t]) if np.isfinite(spy[t]) else np.nan
        sct = raw - float(sec[i, t]) if np.isfinite(sec[i, t]) else np.nan
        v = float(vol63[i, t]) if np.isfinite(vol63[i, t]) and vol63[i, t] > 0 else np.nan
        eqr = mkt / (v * np.sqrt(h)) if np.isfinite(mkt) and np.isfinite(v) else np.nan
        run = np.expm1(cum[i, t] - cum[i, max(t - 21, 0)]) if t >= 1 else np.nan
        rows.append({"t": t, "date": e["date"], "raw": raw, "market_adj": mkt,
                     "sector_adj": sct, "equal_risk": eqr,
                     "announcement": float(ret1[i, t]) if np.isfinite(ret1[i, t]) else np.nan,
                     "prior_21d_runup_post_hoc": float(run) if np.isfinite(run) else np.nan,
                     "is_13d": bool(e.get("is_13d"))})
    if len(rows) < 30:
        return {"state": "TOO_FEW_EVENTS", "n_events": len(rows)}
    df = pd.DataFrame(rows)
    lag = pit.nw_lag(h, 1)
    out = {"state": "OK", "n_events": int(len(df)),
           "n_decision_sessions": int(df["t"].nunique()),
           "effective_observations": float(df["t"].nunique()) / float(h),
           "first": df["date"].min(), "last": df["date"].max(),
           "nw_lag": lag, "horizon": h}
    for col in ("raw", "market_adj", "sector_adj", "equal_risk", "announcement",
                "prior_21d_runup_post_hoc"):
        per = df.groupby("t")[col].mean().dropna()
        if len(per) < 3:
            out[col] = {"periods": int(len(per))}
            continue
        st = S.nw_tstat(per.to_numpy(), lag=lag)
        block = {"periods": int(len(per)), "mean_per_event": float(df[col].mean()),
                 "mean_per_session": st["mean"], "t": st["t"],
                 "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
                 "positive_fraction": float((df[col].dropna() > 0).mean())}
        if col not in ("equal_risk", "announcement", "prior_21d_runup_post_hoc"):
            block["ann_gross"] = float(st["mean"] * PPY / h) if st["mean"] is not None else None
        out[col] = block
    # the cost ladder, paid on ENTRY and EXIT
    base = df.groupby("t")["market_adj"].mean().dropna()
    out["net_by_cost"] = {}
    for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
        n = base.to_numpy() - 2.0 * bps / 1e4
        st = S.nw_tstat(n, lag=lag)
        sd = float(np.std(n, ddof=1)) if len(n) > 2 else 0.0
        out["net_by_cost"]["%.1fbp" % bps] = {
            "ann_net": float(n.mean() * PPY / h), "t": st["t"],
            "sharpe_ann": float(n.mean() / sd * np.sqrt(PPY / h)) if sd > 0 else None,
            "max_drawdown": S._max_dd(n)}
    d13 = df[df["is_13d"]]
    if len(d13) >= 30:
        per = d13.groupby("t")["market_adj"].mean().dropna()
        st = S.nw_tstat(per.to_numpy(), lag=lag)
        out["diagnostic_13d_only"] = {"n_events": int(len(d13)),
                                      "ann_gross": float(st["mean"] * PPY / h)
                                      if st["mean"] is not None else None,
                                      "t": st["t"]}
    return out


# --------------------------------------------------------------------------- #
# ARM 2 - the cross-sectional panel, through the canonical scorer
# --------------------------------------------------------------------------- #
def signal_matrix(E: dict, elig: np.ndarray, events: list, dec: np.ndarray,
                  cadence: int) -> tuple:
    """Events as a cross-sectional signal on the CANONICAL grid.

    ``sig[i, t]`` is the number of qualifying events for name i whose decision
    session falls in ``(t - cadence, t]`` - so every event is counted exactly
    once, with no gap and no double count. The signal is 0, never NaN, for an
    eligible name with no event: a NaN would restrict the scorer's covered rows
    to event names only and silently destroy the cross-section.
    """
    n_i, n_t = len(E["symbols"]), len(E["dates"])
    sig = np.full((n_i, n_t), np.nan)
    by_t: dict = {}
    for e in events:
        by_t.setdefault(e["t"], []).append(e["row"])
    per = []
    for t in np.asarray(dec, dtype=int):
        u = elig[:, t]
        if not u.any():
            continue
        col = np.zeros(n_i, dtype=float)
        n_ev = 0
        for tt in range(max(t - int(cadence) + 1, 0), t + 1):
            for i in by_t.get(tt, ()):
                col[i] += 1.0
                n_ev += 1
        sig[u, t] = col[u]
        per.append({"date": str(np.asarray(E["dates"])[t])[:10],
                    "universe": int(u.sum()),
                    "names_with_event": int(((col > 0) & u).sum()),
                    "events": n_ev})
    return sig, per


_BASE_CACHE: dict = {}


def _base_dataset(h: int) -> dict:
    """The canonical equity dataset for one horizon, built ONCE per process.

    Six cells re-use three horizons, and rebuilding the forward-return matrix
    each time is pure repetition - never a different answer.
    """
    if h not in _BASE_CACHE:
        _BASE_CACHE[h] = X.assemble_equity(int(h))
    return _BASE_CACHE[h]


def assemble(h: int, sig: np.ndarray) -> dict:
    """The canonical R63 equity dataset with the control-block block added.
    Nothing about the substrate, eligibility, returns, costs or baseline is
    re-implemented here."""
    base = _base_dataset(int(h))
    ds = dict(base)
    ds["blocks"] = dict(base["blocks"])
    ds["blocks"][DIMENSION] = sig[:, :, None]
    return ds


def baseline_dims(ds: dict) -> tuple:
    return tuple(d for d in ONT.baseline_for("US_EQUITY") if d in ds["blocks"])


def _xs_residual(x: np.ndarray, Z: np.ndarray) -> np.ndarray:
    """Residual of ``x`` on ``Z`` within ONE cross-section. A contemporaneous
    cross-sectional regression uses no future information; residualising on a
    time-series fit could."""
    ok = np.isfinite(x) & np.isfinite(Z).all(axis=1)
    if ok.sum() < 30:
        return np.full(len(x), np.nan)
    A = np.column_stack([np.ones(ok.sum()), Z[ok]])
    beta = np.linalg.lstsq(A, x[ok], rcond=None)[0]
    out = np.full(len(x), np.nan)
    out[ok] = x[ok] - A @ beta
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


def _stat(v: list, lag: int) -> dict:
    a = np.asarray([x for x in v], dtype=float)
    a = a[np.isfinite(a)]
    if a.size < 3:
        return {"n": int(a.size), "mean": None, "t": None, "p_two_sided": None}
    st = S.nw_tstat(a, lag=lag)
    return {"n": st["n"], "mean": st["mean"], "t": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "positive_fraction": float((a > 0).mean())}


def _book_stats(bk: dict, h: int, lag: int) -> dict:
    g, n = bk["gross"], bk["net"]
    if not len(n):
        return {"periods": 0}
    ppy = PPY / float(h)
    st = S.nw_tstat(n, lag=lag)
    sd = float(np.std(n, ddof=1)) if len(n) > 1 else 0.0
    return {"periods": int(len(n)),
            "gross_ann": float(g.mean() * ppy), "net_ann": float(n.mean() * ppy),
            "t_net": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "sharpe_ann": float(n.mean() / sd * np.sqrt(ppy)) if sd > 0 else None,
            "max_drawdown": S._max_dd(n),
            "mean_oneway_turnover": float(np.mean(bk["turnover"])),
            "cost_drag_ann": float(np.mean(bk["cost"]) * ppy)}


def raw_and_orthogonal(ds: dict, sig: np.ndarray, *, h: int,
                       incumbent_score: np.ndarray) -> dict:
    """Per-period rank IC and long-short book of the RAW signal, and of the
    signal residualised on the owned baseline plus the incumbent, in the SAME
    cross-sections. The book is the canonical :func:`sensitivity._book_returns`;
    nothing here is a second scorer."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims_b = baseline_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
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
                    "n_with_event": int((x > 0).sum()),
                    "rank_ic_raw": a, "rank_ic_orth": b,
                    "layer": "LOCKBOX" if str(np.asarray(ds["dates"])[t])[:10] >= LOCKBOX_START
                    else "SELECTION"})
    if not per:
        return {"state": "NO_PERIODS"}
    cat = {k: np.concatenate(v) for k, v in rows.items()}
    out = {"state": "OK", "n_periods": len(per), "nw_lag": lag,
           "per_period": per[:400],
           "rank_ic_raw": _stat(ic_raw, lag),
           "rank_ic_orthogonalised": _stat(ic_orth, lag)}
    for label, pred in (("raw", cat["pred_raw"]), ("orthogonalised", cat["pred_orth"])):
        out["book_%s" % label] = {}
        for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
            bk = S._book_returns(pred, cat["y"], cat["gid"], cat["iid"], cat["vol"],
                                 np.full(len(cat["y"]), bps / 1e4), "XS",
                                 "XS_LONG_SHORT", 1.0)
            out["book_%s" % label]["%.1fbp" % bps] = _book_stats(bk, h, lag)
    return out


def incremental_vs_incumbent(ds: dict, sig: np.ndarray, *, h: int,
                             incumbent_score: np.ndarray, top_n: int = 50) -> dict:
    """Does the information ADD to the incumbent, in economic units?

    Two long-only top-N books on identical rows, dates, costs and construction:
    the incumbent alone, and the incumbent blended 50/50 (in cross-sectional z
    units, the estate's own convention) with the part of the control-block
    signal the owned families and the incumbent do NOT already explain.

    The blend weight is NOT preregistered, so this is a descriptive measurement
    and can never qualify the experiment on its own.
    """
    dec = np.asarray(ds["dec"], dtype=int)
    dims_b = baseline_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
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
    out = {"state": "OK",
           "blend": "0.5 z(incumbent) + 0.5 z(orthogonalised control-block)",
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
        st = S.nw_tstat(d, lag=lag)
        ppy = PPY / float(h)
        out["%.1fbp" % bps] = {
            "incumbent": _book_stats(a, h, lag),
            "incumbent_plus_control_block": _book_stats(b, h, lag),
            "incremental_ann_net": float(d.mean() * ppy) if n else None,
            "t_incremental": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "periods": int(n)}
    return out


# --------------------------------------------------------------------------- #
# The declared missingness-bias gate
# --------------------------------------------------------------------------- #
def missingness_bias(ds: dict, cik2rows: dict, *, h: int) -> dict:
    """Do the names that CANNOT be assessed earn a different forward return?

    The coverage share says how much is unseen; this says whether what is unseen
    is different. If it is, the estimate is biased by the blindness measured in
    section 3.2 of the preregistration, and the frozen rule returns DATA_HOLD.
    """
    identified = np.zeros(len(ds["inst"]), dtype=bool)
    for rows in cik2rows.values():
        for i in rows:
            identified[i] = True
    lag = pit.nw_lag(h, int(ds["cadence"]))
    diffs, n_seen, n_unseen = [], 0, 0
    for t in np.asarray(ds["dec"], dtype=int):
        ok = ds["elig"][:, t] & np.isfinite(ds["y"][:, t])
        a = ds["y"][ok & identified, t]
        b = ds["y"][ok & ~identified, t]
        if len(a) < 20 or len(b) < 3:
            continue
        diffs.append(float(b.mean() - a.mean()))
        n_seen += len(a)
        n_unseen += len(b)
    if len(diffs) < 5:
        return {"state": "TOO_FEW", "periods": len(diffs)}
    st = S.nw_tstat(np.asarray(diffs), lag=lag)
    ann = float(st["mean"] * PPY / h) if st["mean"] is not None else None
    return {"state": "OK", "periods": len(diffs),
            "rows_assessable": n_seen, "rows_unassessable": n_unseen,
            "mean_diff_per_period": st["mean"], "ann_diff": ann,
            "t": st["t"], "materiality_floor": MATERIALITY_ANN_NET,
            "biased": bool(ann is not None and abs(ann) > MATERIALITY_ANN_NET)}


# --------------------------------------------------------------------------- #
# Verdict - only the preregistered gates, in the preregistered order
# --------------------------------------------------------------------------- #
def verdict_for(cell: dict, inc_cell: dict, desc: dict, ev: dict,
                coverage: dict, bias: dict, *, fdr_pass: bool | None) -> dict:
    """Section 10 of the preregistration, in order. No threshold is invented."""
    if coverage.get("uncovered_share", 0.0) > MAX_UNCOVERED_SHARE:
        return {"verdict": V_DATA_HOLD,
                "why": "%.0f%% of decision sessions resolve under %.0f%% of the "
                       "universe (limit %.0f%%)"
                       % (100 * coverage["uncovered_share"], 100 * MIN_DATE_COVERAGE,
                          100 * MAX_UNCOVERED_SHARE)}
    if bias.get("biased"):
        return {"verdict": V_DATA_HOLD,
                "why": "the names that cannot be assessed earn %.2f%%/yr more than "
                       "those that can (floor %.1f%%), so the estimate is biased by "
                       "what cannot be seen"
                       % (100 * (bias.get("ann_diff") or 0.0), 100 * MATERIALITY_ANN_NET)}
    eff = cell.get("effective_periods")
    if cell.get("verdict") == S.V_DATA_HOLD or eff is None:
        return {"verdict": V_NEED_MORE,
                "why": "the canonical scorer returned DATA_HOLD (%s)" % cell.get("why"),
                "rows_covered": cell.get("rows_covered")}
    if eff < MIN_EFFECTIVE_PERIODS:
        return {"verdict": V_NEED_MORE,
                "why": "%d effective independent periods, floor %d"
                       % (eff, MIN_EFFECTIVE_PERIODS)}
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or abs(t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_EDGE,
                "why": "conditional |t| %s below the %.1f floor"
                       % ("None" if t is None else round(t, 2), CONDITIONAL_T_FLOOR)}
    if fdr_pass is False:
        return {"verdict": V_NO_EDGE,
                "why": "does not survive the declared multiple-testing burden "
                       "(BH q=%.2f over m=%d)" % (BH_Q, 2 * len(HORIZONS_FROZEN))}
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
def run(*, verbose: bool = True, write: bool = True) -> dict:
    E, elig = INC.equity_substrate()
    built = CBE.build_events(E, elig, verbose=verbose)
    events, cik2rows = built["events"], built["cik2rows"]
    blocks = INC.incumbent_blocks(E, elig)
    inc_score = blocks[INC.BLOCK_SCORE][:, :, 0]
    feats_vol = X.assemble_equity(HORIZONS_FROZEN[0])["vol"]

    results, coverage_by_h = {}, {}
    pvals, pkeys = [], []
    cells, cells_inc, descs, evs, biases = {}, {}, {}, {}, {}
    for h in HORIZONS_FROZEN:
        ds0 = X.assemble_equity(h)
        dec = np.asarray(ds0["dec"], dtype=int)
        cadence = int(ds0["cadence"])
        cov = CBE.coverage_report(E, elig, cik2rows, dec)
        coverage_by_h[h] = cov
        for cell_name in CBE.CELLS:
            key = (cell_name, h)
            evs[key] = event_study(E, elig, events[cell_name], h=h, vol63=feats_vol)
            sig, per = signal_matrix(E, elig, events[cell_name], dec, cadence)
            ds = assemble(h, sig)
            dims_b = baseline_dims(ds)
            if verbose:
                print("%s h=%d cadence=%d baseline dims=%d decisions=%d events=%d"
                      % (cell_name, h, cadence, len(dims_b), len(dec),
                         len(events[cell_name])), flush=True)
            cells[key] = S.run_cell(ds, dims_b, DIMENSION, keep_predictions=False)
            ds_i = assemble(h, sig)
            ds_i["blocks"][INC.BLOCK_SCORE] = blocks[INC.BLOCK_SCORE]
            cells_inc[key] = S.run_cell(ds_i, (INC.BLOCK_SCORE,), DIMENSION,
                                        keep_predictions=False)
            descs[key] = raw_and_orthogonal(ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["incremental_vs_incumbent"] = incremental_vs_incumbent(
                ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["signal_per_date"] = per[:200]
            biases[key] = missingness_bias(ds, cik2rows, h=h)
            p = (evs[key].get("market_adj") or {}).get("p_two_sided")
            pvals.append(p)
            pkeys.append(key)

    # ONE declared family, six cells: the denominator is 6 and is not reset
    bh = MT.benjamini_hochberg(pvals, BH_Q)
    fdr = {pkeys[i]: (i in bh["rejected"]) for i in range(len(pkeys))}

    for key in pkeys:
        cell_name, h = key
        v = verdict_for(cells[key], cells_inc[key], descs[key], evs[key],
                        coverage_by_h[h], biases[key], fdr_pass=fdr[key])
        m = (evs[key].get("market_adj") or {}).get("mean_per_session")
        results["%s|%d" % (cell_name, h)] = {
            "cell": cell_name, "horizon": h,
            "verdict": v["verdict"], "why": v["why"],
            "sign_result": SIGN_DISCOVERY if (m is not None and m != 0) else SIGN_NONE,
            "sign_observed": None if m is None else ("POSITIVE" if m > 0 else "NEGATIVE"),
            "event_study": evs[key], "scorer_cell": cells[key],
            "scorer_cell_vs_incumbent": cells_inc[key],
            "descriptive": descs[key], "missingness_bias": biases[key],
            "fdr_pass": fdr[key],
        }

    body = {
        "schema": "alpha_recovery_control_block_13dg/1",
        "calculation_owner": CALCULATION_OWNER,
        "family_id": FAMILY_ID, "dimension": DIMENSION,
        "preregistration": PREREGISTRATION,
        "preregistration_commit": PREREGISTRATION_COMMIT,
        "preregistration_altered_after_results": False,
        "direction": "TWO_SIDED_NO_SIGN_PRESPECIFIED",
        "sign_policy": "a sign read off the sample is DISCOVERY_ONLY and cannot "
                       "retroactively qualify this experiment",
        "cells": list(CBE.CELLS), "horizons": list(HORIZONS_FROZEN),
        "material_increase_pp": CBE.MATERIAL_INCREASE_PP,
        "material_increase_source": "Rule 13d-2(a) materiality standard, not a "
                                    "search over returns",
        "pit_boundary": "SEC acceptance instant (UTC -> Eastern); first session "
                        "whose 16:00 ET close is strictly after it; entry the "
                        "NEXT close",
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "desk_single_name_cost_bps_per_side": DESK_EQUITY_COST_BPS,
        "scorer": S.CALCULATION_OWNER, "pit_engine": pit.CALCULATION_OWNER,
        "multiplicity_owner": MT.CALCULATION_OWNER,
        "multiplicity": {"family": FAMILY_ID,
                         "m_declared": len(CBE.CELLS) * len(HORIZONS_FROZEN),
                         "benjamini_hochberg": bh, "q": BH_Q,
                         "denominator_reset": False,
                         "cells_added_after_results": False},
        "identity": built["identity"], "stream_stats": built["stats"],
        "coverage": {str(h): coverage_by_h[h] for h in HORIZONS_FROZEN},
        "event_counts": {c: len(events[c]) for c in CBE.CELLS},
        "results": results,
        "generated_at": now_iso(),
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
