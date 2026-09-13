"""alpha_agent.alpha_recovery.event_8k_alpha - the preregistered SEC Form 8-K
Item-code event experiment.

Executes ``research/preregistration/EVENT_8K_ITEM_PREREGISTRATION.md`` (frozen
BEFORE any return existed) and changes nothing in it.

    hypothesis   a filer-declared 8-K Item that reports a specific economic
                 event carries information about the issuer's return AFTER the
                 first legitimate entry, that the estate's owned families -
                 including its own 8-K disclosure-intensity block - do not
                 already carry
    direction    PRE-SPECIFIED per cell. A contradicted sign is reported and
                 the cell closes; it is never reversed
    PIT          the SEC ACCEPTANCE INSTANT (UTC -> Eastern); decision session
                 t = first session whose 16:00 close is strictly after it;
                 entry at close t+1 (canonical NEXT_CLOSE)
    scorer       alpha_agent.r63.sensitivity.run_cell - the ONE scorer
    burden       alpha_agent.r31.multiple_testing, the denominator NOT reset

REUSED, NOT REWRITTEN: the event study, the panel signal, the residualiser, the
book statistics and the missingness gate are the 13D/G experiment's frozen
owners in :mod:`control_block_alpha`. What this module adds is only what the
8-K preregistration adds: a declared sign, the owned 8-K intensity block as a
required control, the post-publication diagnostics, and a count of effective
periods that ignores periods in which no event could be scored.

RESEARCH ONLY. No purchase, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from alpha_agent.r31 import multiple_testing as MT
from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, CONDITIONAL_T_FLOOR, LOCKBOX_START, MATERIALITY_ANN_NET,
               MIN_EFFECTIVE_PERIODS, now_iso, write_artifact)
from . import control_block_alpha as CBA
from . import control_block_events as CBE
from . import event_8k_data as D
from . import event_8k_events as EV
from . import incumbent as INC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.event_8k_alpha"
ARTIFACT_NAME = "event_8k_item.json"

FAMILY_ID = "DISCLOSURE_8K_ITEM_EVENT_V1"
#: The ontology dimension this information belongs to ...
ONTOLOGY_DIMENSION = "DISCLOSURE_INTENSITY_LANGUAGE"
#: ... and the block key it is scored under. It may NOT reuse the ontology
#: key: that block already exists in the dataset as the owned 8-K intensity
#: control, and overwriting it would delete the control this family must beat.
BLOCK = "SEC_8K_ITEM_EVENT"
INTENSITY_CONTROL = "DISCLOSURE_INTENSITY_LANGUAGE"

PREREGISTRATION = "research/preregistration/EVENT_8K_ITEM_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "998a3f9"

HORIZONS_FROZEN = (5, 21, 63)
COST_LADDER_BPS = CBA.COST_LADDER_BPS
DESK_EQUITY_COST_BPS = CBA.DESK_EQUITY_COST_BPS
MIN_DATE_COVERAGE = CBA.MIN_DATE_COVERAGE
MAX_UNCOVERED_SHARE = CBA.MAX_UNCOVERED_SHARE
PPY = CBA.PPY

V_QUALIFIED = CBA.V_QUALIFIED
V_NO_EDGE = CBA.V_NO_EDGE
V_NO_INCREMENTAL = CBA.V_NO_INCREMENTAL
V_DATA_HOLD = CBA.V_DATA_HOLD
V_NEED_MORE = CBA.V_NEED_MORE
VERDICTS = (V_QUALIFIED, V_NO_EDGE, V_NO_INCREMENTAL, V_DATA_HOLD, V_NEED_MORE)


def declared_m() -> int:
    return len(EV.CELLS) * len(HORIZONS_FROZEN)


def control_dims(ds: dict) -> tuple:
    """The owned US_EQUITY baseline PLUS the owned 8-K intensity block. An
    Item-coded event is mechanically correlated with how often a company files
    8-Ks, so information that the intensity block already carries cannot be
    credited to the Item."""
    dims = CBA.baseline_dims(ds)
    if INTENSITY_CONTROL in ds["blocks"] and INTENSITY_CONTROL not in dims:
        dims = dims + (INTENSITY_CONTROL,)
    return dims


# --------------------------------------------------------------------------- #
# ARM 1 - the event study (the frozen 13D/G owner) + the 8-K diagnostics
# --------------------------------------------------------------------------- #
def _spy_session_returns(E: dict) -> np.ndarray:
    spy = np.asarray(E["price"]["spy_tr"], dtype=float)
    r = np.full(spy.shape, np.nan)
    r[1:] = spy[1:] / spy[:-1] - 1.0
    return r


def _clustered(values: list, ts: list, lag: int) -> dict:
    df = pd.DataFrame({"t": ts, "v": values}).dropna()
    per = df.groupby("t")["v"].mean()
    if len(per) < 3:
        return {"periods": int(len(per))}
    st = S.nw_tstat(per.to_numpy(), lag=lag)
    return {"periods": int(len(per)), "events": int(len(df)),
            "mean_per_event": float(df["v"].mean()),
            "mean_per_session": st["mean"], "t": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None}


def event_study(E: dict, elig: np.ndarray, events: list, *, h: int,
                vol63: np.ndarray, sign: int) -> dict:
    """The frozen event study, then the declared diagnostics.

    ``market_adj`` from :func:`control_block_alpha.event_study` is the
    QUALIFICATION input: entry at close t+1, strictly after publication.

    Added, and excluded from every gate:

    * ``announcement_market_adj`` - close t-1 to close t, the session that
      CONTAINS publication. Information spent here is not tradable alpha.
    * ``forgone_first_post_publication_session`` - close t to close t+1. Legal
      to trade, deliberately forgone by the canonical one-session broadcast
      lag. Reported so "no information after entry" can be told apart from
      "information spent in the one session we declined to claim". It can
      never qualify a cell and it is not a rescue.
    * ``signed_net_by_cost`` - the market-adjusted return in the FROZEN
      direction, paying the cost ladder on entry and exit.
    * a SELECTION / LOCKBOX split of the primary statistic.
    """
    base = CBA.event_study(E, elig, events, h=h, vol63=vol63)
    if base.get("state") != "OK":
        return base
    ret1 = CBA._session_returns(E)
    spy1 = _spy_session_returns(E)
    y = pit.forward_compound(ret1, h)
    spy_h = CBA._spy_forward(E, h)
    lock_t = int(np.searchsorted(np.asarray(E["dates"], dtype="datetime64[ns]"),
                                 np.datetime64(LOCKBOX_START, "D").astype("datetime64[ns]")))
    n_t = ret1.shape[1]
    ann, forgone, mkt, ts, ts_next, layer = [], [], [], [], [], []
    for e in events:
        i, t = int(e["row"]), int(e["t"])
        if t >= y.shape[1] or not np.isfinite(y[i, t]) or not np.isfinite(spy_h[t]):
            continue
        mkt.append(float(y[i, t] - spy_h[t]))
        ts.append(t)
        layer.append("LOCKBOX" if t >= lock_t else "SELECTION")
        a = ret1[i, t] - spy1[t] if np.isfinite(ret1[i, t]) and np.isfinite(spy1[t]) else np.nan
        ann.append(float(a))
        if t + 1 < n_t and np.isfinite(ret1[i, t + 1]) and np.isfinite(spy1[t + 1]):
            forgone.append(float(ret1[i, t + 1] - spy1[t + 1]))
        else:
            forgone.append(np.nan)
        ts_next.append(t)
    lag = pit.nw_lag(h, 1)
    out = dict(base)
    out["sign_frozen"] = int(sign)
    out["announcement_market_adj"] = _clustered(ann, ts, 0)
    out["forgone_first_post_publication_session"] = _clustered(forgone, ts_next, 0)
    out["market_adj_by_layer"] = {
        lay: _clustered([v for v, l in zip(mkt, layer) if l == lay],
                        [t for t, l in zip(ts, layer) if l == lay], lag)
        for lay in ("SELECTION", "LOCKBOX")}
    per = pd.DataFrame({"t": ts, "v": mkt}).groupby("t")["v"].mean().dropna()
    out["signed_net_by_cost"] = {}
    for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
        n = float(sign) * per.to_numpy() - 2.0 * bps / 1e4
        st = S.nw_tstat(n, lag=lag)
        sd = float(np.std(n, ddof=1)) if len(n) > 2 else 0.0
        out["signed_net_by_cost"]["%.1fbp" % bps] = {
            "ann_net": float(n.mean() * PPY / h), "t": st["t"],
            "sharpe_ann": float(n.mean() / sd * np.sqrt(PPY / h)) if sd > 0 else None,
            "max_drawdown": S._max_dd(n)}
    m = base.get("market_adj") or {}
    if m.get("ann_gross") is not None:
        out["signed_market_adj_ann_gross"] = float(sign) * float(m["ann_gross"])
    return out


# --------------------------------------------------------------------------- #
# ARM 2 - the cross-sectional panel, through the canonical scorer
# --------------------------------------------------------------------------- #
def signal_matrix(E: dict, elig: np.ndarray, events: list, dec: np.ndarray,
                  cadence: int, sign: int) -> tuple:
    """The 13D/G owner's event-count signal on the canonical grid, multiplied
    by the frozen sign, so that a larger value always means the direction the
    preregistration predicts is BETTER. 0 - never NaN - for an eligible name
    with no event."""
    sig, per = CBA.signal_matrix(E, elig, events, dec, cadence)
    return float(sign) * sig, per


_BASE_CACHE: dict = {}


def _base_dataset(h: int) -> dict:
    if h not in _BASE_CACHE:
        _BASE_CACHE[h] = X.assemble_equity(int(h))
    return _BASE_CACHE[h]


def assemble(h: int, sig: np.ndarray) -> dict:
    base = _base_dataset(int(h))
    ds = dict(base)
    ds["blocks"] = dict(base["blocks"])
    ds["blocks"][BLOCK] = sig[:, :, None]
    return ds


def raw_and_orthogonal(ds: dict, sig: np.ndarray, *, h: int,
                       incumbent_score: np.ndarray) -> dict:
    """Per-period rank IC and the canonical long-short book of the signal, raw
    and residualised on the controls plus the incumbent, in the SAME
    cross-sections. The residualiser, the book and its statistics are the
    frozen 13D/G owners; only the control set differs."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims = control_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
    rows = {"gid": [], "iid": [], "pred_raw": [], "pred_orth": [], "y": [], "vol": []}
    ic_raw, ic_orth, per = [], [], []
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        x, yv = sig[ix, t], ds["y"][ix, t]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims]
                            + [incumbent_score[ix, t][:, None]])
        xo = CBA._xs_residual(x, Z)
        a, b = S.spearman(x, yv), S.spearman(xo, yv)
        ic_raw.append(a)
        ic_orth.append(b)
        rows["gid"].append(np.full(ok.sum(), j))
        rows["iid"].append(ix)
        rows["pred_raw"].append(x)
        rows["pred_orth"].append(xo)
        rows["y"].append(yv)
        rows["vol"].append(ds["vol"][ix, t])
        per.append({"date": str(np.asarray(ds["dates"])[t])[:10], "n": int(ok.sum()),
                    "n_with_event": int((x != 0).sum()), "rank_ic_raw": a,
                    "rank_ic_orth": b})
    if not per:
        return {"state": "NO_PERIODS"}
    cat = {k: np.concatenate(v) for k, v in rows.items()}
    out = {"state": "OK", "n_periods": len(per), "nw_lag": lag, "controls": list(dims),
           "per_period": per[:400], "rank_ic_raw": CBA._stat(ic_raw, lag),
           "rank_ic_orthogonalised": CBA._stat(ic_orth, lag)}
    for label, pred in (("raw", cat["pred_raw"]), ("orthogonalised", cat["pred_orth"])):
        out["book_%s" % label] = {}
        for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
            bk = S._book_returns(pred, cat["y"], cat["gid"], cat["iid"], cat["vol"],
                                 np.full(len(cat["y"]), bps / 1e4), "XS",
                                 "XS_LONG_SHORT", 1.0)
            out["book_%s" % label]["%.1fbp" % bps] = CBA._book_stats(bk, h, lag)
    return out


def incremental_vs_incumbent(ds: dict, sig: np.ndarray, *, h: int,
                             incumbent_score: np.ndarray, top_n: int = 50) -> dict:
    """The incumbent's top-N book alone against the incumbent blended 50/50 (z
    units, the estate's convention) with the part of the signal the controls
    and the incumbent do NOT explain. Identical rows, dates, costs and
    construction. The blend weight is not a preregistered parameter, so this
    is descriptive and never qualifies a cell on its own."""
    dec = np.asarray(ds["dec"], dtype=int)
    dims = control_dims(ds)
    lag = pit.nw_lag(h, int(ds["cadence"]))
    acc = {k: [] for k in ("gid", "iid", "y", "vol", "inc", "blend")}
    for j, t in enumerate(dec):
        ok = ds["elig"][:, t] & np.isfinite(sig[:, t]) & np.isfinite(ds["y"][:, t]) \
            & np.isfinite(incumbent_score[:, t])
        if ok.sum() < 50:
            continue
        ix = np.where(ok)[0]
        Z = np.column_stack([ds["blocks"][d][ix, t, :] for d in dims]
                            + [incumbent_score[ix, t][:, None]])
        xo = CBA._xs_residual(sig[ix, t], Z)
        zi, zs = CBA._z(incumbent_score[ix, t]), CBA._z(xo)
        acc["gid"].append(np.full(len(ix), j))
        acc["iid"].append(ix)
        acc["y"].append(ds["y"][ix, t])
        acc["vol"].append(ds["vol"][ix, t])
        acc["inc"].append(zi)
        acc["blend"].append(np.where(np.isfinite(zs), 0.5 * zi + 0.5 * zs, zi))
    if not acc["gid"]:
        return {"state": "NO_PERIODS"}
    c = {k: np.concatenate(v) for k, v in acc.items()}
    out = {"state": "OK", "controls": list(dims), "top_n": top_n,
           "blend": "0.5 z(incumbent) + 0.5 z(orthogonalised signed 8-K event signal)",
           "book": "EQ_LONG_ONLY_TOPN vs the equal-weight scored universe"}
    for bps in tuple(COST_LADDER_BPS) + (DESK_EQUITY_COST_BPS,):
        cost = np.full(len(c["y"]), bps / 1e4)
        a = S._book_returns(c["inc"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        b = S._book_returns(c["blend"], c["y"], c["gid"], c["iid"], c["vol"], cost,
                            "XS", "EQ_LONG_ONLY_TOPN", 1.0, top_n)
        n = min(len(a["net"]), len(b["net"]))
        d = b["net"][:n] - a["net"][:n]
        st = S.nw_tstat(d, lag=lag)
        out["%.1fbp" % bps] = {
            "incumbent": CBA._book_stats(a, h, lag),
            "incumbent_plus_8k_event": CBA._book_stats(b, h, lag),
            "incremental_ann_net": float(d.mean() * PPY / float(h)) if n else None,
            "t_incremental": st["t"],
            "p_two_sided": MT.two_sided_p(st["t"]) if st["t"] is not None else None,
            "periods": int(n)}
    return out


# --------------------------------------------------------------------------- #
# Verdict - only the preregistered gates, in the preregistered order
# --------------------------------------------------------------------------- #
def verdict_for(cell: dict, inc_cell: dict, desc: dict, ev: dict, coverage: dict,
                bias: dict, *, informative_eff: int | None, fdr_pass: bool | None,
                sign: int) -> dict:
    """Section 10 of the preregistration, in order. Every threshold already
    exists in the estate; none is invented here."""
    if coverage.get("uncovered_share", 0.0) > MAX_UNCOVERED_SHARE:
        return {"verdict": V_DATA_HOLD, "gate": "COVERAGE",
                "why": "%.0f%% of decision sessions resolve under %.0f%% of the universe "
                       "(limit %.0f%%)" % (100 * coverage["uncovered_share"],
                                           100 * MIN_DATE_COVERAGE, 100 * MAX_UNCOVERED_SHARE)}
    if bias.get("biased"):
        return {"verdict": V_DATA_HOLD, "gate": "MISSINGNESS_BIAS",
                "why": "the names that cannot be assessed earn %.2f%%/yr differently from those "
                       "that can (floor %.1f%%)" % (100 * (bias.get("ann_diff") or 0.0),
                                                    100 * MATERIALITY_ANN_NET)}
    eff = cell.get("effective_periods")
    if cell.get("verdict") == S.V_DATA_HOLD or eff is None:
        return {"verdict": V_NEED_MORE, "gate": "SCORER_FLOOR",
                "why": "the canonical scorer returned DATA_HOLD (%s)" % cell.get("why")}
    used = int(min(eff, informative_eff)) if informative_eff is not None else int(eff)
    if used < MIN_EFFECTIVE_PERIODS:
        return {"verdict": V_NEED_MORE, "gate": "EFFECTIVE_PERIODS",
                "why": "%d effective independent periods that carry an event (scorer %d), "
                       "floor %d" % (used, eff, MIN_EFFECTIVE_PERIODS)}
    if ev.get("state") != "OK":
        return {"verdict": V_NEED_MORE, "gate": "EVENT_STUDY_FLOOR",
                "why": "event study state %s" % ev.get("state")}
    m = ev.get("market_adj") or {}
    mean, t_ev = m.get("mean_per_session"), m.get("t")
    if mean is None or float(sign) * float(mean) <= 0.0:
        return {"verdict": V_NO_EDGE, "gate": "FROZEN_SIGN",
                "why": "post-entry market-adjusted return %s has the sign opposite to the "
                       "frozen direction %+d; the sign is not reversed"
                       % (None if mean is None else round(float(mean), 5), sign)}
    if t_ev is None or abs(float(t_ev)) < STANDALONE_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "POST_PUBLICATION_T",
                "why": "post-entry market-adjusted |t| %s below the %.1f floor"
                       % (None if t_ev is None else round(float(t_ev), 2), STANDALONE_T_FLOOR)}
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or float(t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "CONDITIONAL_T",
                "why": "conditional t %s below the %.1f floor"
                       % (None if t is None else round(float(t), 2), CONDITIONAL_T_FLOOR)}
    if fdr_pass is False:
        return {"verdict": V_NO_EDGE, "gate": "MULTIPLICITY",
                "why": "does not survive BH q=%.2f over the declared m=%d" % (BH_Q, declared_m())}
    ic_o = (desc.get("rank_ic_orthogonalised") or {}).get("t")
    red = (cell.get("redundancy") or {}).get("redundancy")
    inc_t = ((inc_cell or {}).get("conditional") or {}).get("t")
    if red == "REDUNDANT" or ic_o is None or float(ic_o) < CONDITIONAL_T_FLOOR \
            or inc_t is None or float(inc_t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_INCREMENTAL, "gate": "INCREMENTAL",
                "why": "does not survive the controls and the incumbent in the frozen direction "
                       "(redundancy=%s, orthogonalised IC t=%s, vs-incumbent t=%s)"
                       % (red, None if ic_o is None else round(float(ic_o), 2),
                          None if inc_t is None else round(float(inc_t), 2))}
    e = cell.get("economics") or {}
    if (e.get("ann_net_increment") or 0.0) < MATERIALITY_ANN_NET:
        return {"verdict": V_NO_EDGE, "gate": "MATERIALITY",
                "why": "net annual increment %.4f below the %.3f floor"
                       % (e.get("ann_net_increment") or 0.0, MATERIALITY_ANN_NET)}
    return {"verdict": V_QUALIFIED, "gate": None, "why": "every preregistered gate passed"}


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run(*, verbose: bool = True, write: bool = True) -> dict:
    E, elig = INC.equity_substrate()
    built = EV.build_events(E, elig, verbose=verbose)
    events = built["events"]
    hist = {r["issuer_cik"] for r in D.load_stream()}
    cik2rows = {c: r for c, r in built["cik2rows"].items() if c in hist}
    blocks = INC.incumbent_blocks(E, elig)
    inc_score = blocks[INC.BLOCK_SCORE][:, :, 0]
    vol63 = _base_dataset(HORIZONS_FROZEN[0])["vol"]

    coverage_by_h, pvals, pkeys = {}, [], []
    cells, cells_inc, descs, evs, biases, informative = {}, {}, {}, {}, {}, {}
    for h in HORIZONS_FROZEN:
        ds0 = _base_dataset(h)
        dec = np.asarray(ds0["dec"], dtype=int)
        cadence = int(ds0["cadence"])
        cov = CBE.coverage_report(E, elig, cik2rows, dec)
        cov.pop("per_session", None)
        coverage_by_h[h] = cov
        for cell_name in EV.CELLS:
            sign = EV.CELL_SPECS[cell_name]["sign"]
            key = (cell_name, h)
            evs[key] = event_study(E, elig, events[cell_name], h=h, vol63=vol63, sign=sign)
            suff = D.oos_sufficiency(E["dates"], [e["t"] for e in events[cell_name]],
                                     horizons=(h,))[str(h)]
            informative[key] = suff
            sig, per = signal_matrix(E, elig, events[cell_name], dec, cadence, sign)
            ds = assemble(h, sig)
            dims = control_dims(ds)
            if verbose:
                print("%s h=%d cadence=%d controls=%d decisions=%d events=%d informative_eff=%d"
                      % (cell_name, h, cadence, len(dims), len(dec), len(events[cell_name]),
                         suff["effective_informative_periods"]), flush=True)
            cells[key] = S.run_cell(ds, dims, BLOCK, keep_predictions=False)
            ds_i = assemble(h, sig)
            ds_i["blocks"][INC.BLOCK_SCORE] = blocks[INC.BLOCK_SCORE]
            cells_inc[key] = S.run_cell(ds_i, (INC.BLOCK_SCORE,), BLOCK, keep_predictions=False)
            descs[key] = raw_and_orthogonal(ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["incremental_vs_incumbent"] = incremental_vs_incumbent(
                ds, sig, h=h, incumbent_score=inc_score)
            descs[key]["signal_per_date"] = per[:200]
            biases[key] = CBA.missingness_bias(ds, cik2rows, h=h)
            pvals.append((evs[key].get("market_adj") or {}).get("p_two_sided"))
            pkeys.append(key)

    # ONE declared family: the denominator is cells x horizons and is not reset
    bh = MT.benjamini_hochberg(pvals, BH_Q)
    fdr = {pkeys[i]: (i in bh["rejected"]) for i in range(len(pkeys))}

    results = {}
    for key in pkeys:
        cell_name, h = key
        sign = EV.CELL_SPECS[cell_name]["sign"]
        kw = dict(informative_eff=informative[key]["effective_informative_periods"],
                  fdr_pass=fdr[key], sign=sign)
        v = verdict_for(cells[key], cells_inc[key], descs[key], evs[key],
                        coverage_by_h[h], biases[key], **kw)
        # DECLARED IN ADVANCE: the same frozen function with both DATA gates
        # satisfied, so the axis is answered on its merits and not only on a gate.
        v_merits = verdict_for(cells[key], cells_inc[key], descs[key], evs[key],
                               {"uncovered_share": 0.0}, {"biased": False}, **kw)
        m = (evs[key].get("market_adj") or {}).get("mean_per_session")
        results["%s|%d" % (cell_name, h)] = {
            "cell": cell_name, "horizon": h, "sign_frozen": sign,
            "verdict": v["verdict"], "gate": v["gate"], "why": v["why"],
            "verdict_if_data_gates_passed": v_merits,
            "sign_observed": None if m is None else ("POSITIVE" if m > 0 else "NEGATIVE"),
            "sign_agrees_with_frozen": None if m is None else bool(float(sign) * m > 0),
            "event_study": evs[key], "informative_periods": informative[key],
            "scorer_cell": cells[key], "scorer_cell_vs_incumbent": cells_inc[key],
            "descriptive": descs[key], "missingness_bias": biases[key],
            "fdr_pass": fdr[key]}

    body = {
        "schema": "alpha_recovery_event_8k_item/1",
        "calculation_owner": CALCULATION_OWNER, "family_id": FAMILY_ID,
        "ontology_dimension": ONTOLOGY_DIMENSION, "block": BLOCK,
        "preregistration": PREREGISTRATION, "preregistration_commit": PREREGISTRATION_COMMIT,
        "preregistration_altered_after_results": False,
        "cells": {c: {k: v for k, v in s.items() if k != "text_rule"}
                  for c, s in EV.CELL_SPECS.items()},
        "horizons": list(HORIZONS_FROZEN),
        "pit_boundary": "SEC acceptance instant (UTC -> Eastern); decision session = first "
                        "session whose 16:00 ET close is strictly after it; entry at the NEXT "
                        "close (canonical NEXT_CLOSE)",
        "event_window_start": EV.EVENT_WINDOW_START,
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "desk_single_name_cost_bps_per_side": DESK_EQUITY_COST_BPS,
        "scorer": S.CALCULATION_OWNER, "pit_engine": pit.CALCULATION_OWNER,
        "multiplicity_owner": MT.CALCULATION_OWNER,
        "multiplicity": {"family": FAMILY_ID, "m_declared": declared_m(),
                         "benjamini_hochberg": bh, "q": BH_Q, "denominator_reset": False,
                         "cells_added_after_results": False},
        "identity": built["identity"], "event_stats": built["stats"],
        "coverage": {str(h): coverage_by_h[h] for h in HORIZONS_FROZEN},
        "event_counts": {c: len(events[c]) for c in EV.CELLS},
        "results": results, "capital_eligible": False, "generated_at": now_iso()}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
