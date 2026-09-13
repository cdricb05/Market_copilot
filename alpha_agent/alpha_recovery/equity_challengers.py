"""alpha_agent.alpha_recovery.equity_challengers - same-domain challengers built
from the incumbent's OWN information.

Workstream 12, opened after two measured facts:

    * the incumbent baseline verdict is INCUMBENT_WEAK_OR_UNPROVEN at the
      OPERATIONAL 21-session horizon (net excess +2.7 %/yr, t 0.99), and
    * its own horizon attribution scores the SAME blend at +4.5 %/yr (t 2.14)
      when the forward window is 63 sessions.

Two questions follow that the head-to-head tournament cannot ask, because both
change the CONSTRUCTION rather than the information:

    A. CADENCE   the desk rebalances every 21 sessions. The information is
                 stronger at 63. Is the operational cadence paying alpha away
                 in turnover? Arms: rebalance every 21 (the operational arm),
                 42, 63 and 126 sessions.
    B. LEGS      the blend is fixed at 0.5 fundamental / 0.5 momentum. Which
                 leg carries the alpha, and does the other one earn its cost?
                 Arms: momentum only and fundamental only, at 21 and 63.

WHY THE ATTRIBUTION ROW IS NOT ALREADY THE ANSWER
    The h = 63 attribution row is a DIAGNOSTIC, not a book: it charges the
    turnover of a 21-session rebalance against a 63-session forward return
    window, so it under-charges cost, and its overlapping windows are not a
    capital path anyone can hold. This module builds the REAL daily path of
    each arm and charges the real cost at the real rebalance dates.

CONSTRUCTION (identical for every arm; one treatment at a time)
    long-only equal-weight top-25 of the R63 PIT S&P 500 eligible universe,
    12.5 bp per side charged on the summed absolute weight change at each
    rebalance, positions drifting with prices in between, a name whose return
    stops printing held at a zero return until the next rebalance sells it.
    The benchmark is the equal-weight scored universe rebalanced every 21
    sessions, FIXED across arms so every excess is on the same ruler.

    Arms are compared on non-overlapping 21-session blocks of their own daily
    net paths: the comparison is paired, on the same capital, over the same
    calendar. The frozen tournament gates decide; Holm runs inside the family
    and the p-values join the campaign's Benjamini-Hochberg denominator.

EVIDENCE LABEL
    POST_SELECTION. The horizon attribution that motivated the cadence ladder
    was read before this family was written, and the R63/R64 lockbox of this
    substrate was viewed by earlier releases. No threshold is moved for it.
"""
from __future__ import annotations

import json
import math
import re
import time

import numpy as np
import pandas as pd

from alpha_agent.r63 import EQUITY_DISCOVERY_START, LOCKBOX_START, pit
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, EQ_COST_RATE_PER_SIDE, EQ_TOP_N_OPERATIONAL, GATE_DD_MULTIPLE,
               GATE_HALF_FLOOR, GATE_MAX_TURNOVER, HOLM_ALPHA, MATERIALITY_ANN_NET,
               MIN_EFFECTIVE_PERIODS, read_artifact, research_root, write_artifact)
from . import incumbent as INC
from . import tournament as T

CALCULATION_OWNER = "alpha_agent.alpha_recovery.equity_challengers"
CELLS_DIR = "cells"
ARTIFACT_NAME = "equity_challengers.json"
PPY = 252.0
BLOCK_SESSIONS = 21
POST_SELECTION = ("POST_SELECTION: the horizon attribution that motivated the cadence ladder "
                  "was measured before this family was written")

FAM_CADENCE = "EQUITY_INCUMBENT_CADENCE"
FAM_LEGS = "INCUMBENT_DECOMPOSITION"

LEG_BLEND = "blend"
LEG_MOM = "momentum_only"
LEG_FUND = "fundamental_only"
LEG_BLOCK = {LEG_BLEND: INC.BLOCK_SCORE, LEG_MOM: INC.BLOCK_MOM, LEG_FUND: INC.BLOCK_FUND}

#: the operational arm every other arm is measured against
REFERENCE = (LEG_BLEND, 21)

_CACHE: dict = {}


def default_grid() -> list:
    """The whole grid, fixed here before any arm ran."""
    g = [{"leg": LEG_BLEND, "trade_every": k, "family": FAM_CADENCE, "tag": "PRIMARY"}
         for k in (21, 42, 63, 126)]
    g += [{"leg": leg, "trade_every": k, "family": FAM_LEGS, "tag": "PRIMARY"}
          for k in (21, 63) for leg in (LEG_MOM, LEG_FUND)]
    for sp in g:
        sp["scope"] = "US_EQUITY"
        sp["top_n"] = EQ_TOP_N_OPERATIONAL
        sp["cell_id"] = "US_EQUITY|TOP%d|%s|k%d" % (EQ_TOP_N_OPERATIONAL, sp["leg"], sp["trade_every"])
        sp["is_reference"] = (sp["leg"], sp["trade_every"]) == REFERENCE
    return g


# --------------------------------------------------------------------------- #
# The daily capital path of one arm
# --------------------------------------------------------------------------- #
def daily_path_book(score: np.ndarray, *, elig: np.ndarray, ret: np.ndarray, dates: np.ndarray,
                    top_n: int, trade_every: int, cost: float = EQ_COST_RATE_PER_SIDE,
                    start_date: str = EQUITY_DISCOVERY_START,
                    whole_universe: bool = False) -> dict:
    """Daily NET return path of a long-only equal-weight top-N book rebalanced
    every ``trade_every`` sessions.

    ``score`` and ``elig`` are (n_sym x n_dates) and are read ONLY at the
    decision session; ``ret`` is the simple daily total return, so the return
    credited to a decision at t is ``ret[:, t + 1]``. Weights drift with prices
    between rebalances. A held name whose return stops printing is carried at a
    zero return until the next rebalance sells it (never dropped for free).
    Cost is the summed absolute weight change times ``cost``, charged on the
    session the rebalance happens - the same convention as
    ``incumbent.run_score``.
    """
    n_d = len(dates)
    t0 = int(np.searchsorted(dates, start_date))
    w: dict = {}
    cash = 1.0
    daily = np.full(n_d, np.nan)
    turn = np.zeros(n_d)
    cst = np.zeros(n_d)
    n_held = np.zeros(n_d)
    rebalanced = np.zeros(n_d, dtype=bool)
    live = False
    for t in range(t0, n_d - 1):
        cost_t = 0.0
        if (t - t0) % int(trade_every) == 0:
            s = score[:, t]
            ok = elig[:, t] & np.isfinite(s)
            n = int(ok.sum())
            if n >= INC.MIN_NAMES:
                ix = np.where(ok)[0]
                if whole_universe:
                    held = ix
                else:
                    order = np.argsort(-s[ix], kind="mergesort")
                    held = ix[order[:min(int(top_n), n)]]
                k = len(held)
                w_new = {int(i): 1.0 / k for i in held}
                traded = sum(abs(w_new.get(i, 0.0) - w.get(i, 0.0)) for i in set(w_new) | set(w))
                cost_t = traded * cost
                turn[t] = traded / 2.0
                cst[t] = cost_t
                rebalanced[t] = True
                w, cash = w_new, 0.0
                live = True
        if not live:
            continue
        port = 0.0
        for i, wi in w.items():
            r = ret[i, t + 1]
            port += wi * (float(r) if np.isfinite(r) else 0.0)
        daily[t + 1] = port - cost_t
        n_held[t + 1] = len(w)
        scale = 1.0 + port
        if scale <= 0:
            w, cash = {}, 1.0
            live = False
            continue
        nw = {}
        for i, wi in w.items():
            r = ret[i, t + 1]
            nw[i] = wi * (1.0 + (float(r) if np.isfinite(r) else 0.0)) / scale
        w, cash = nw, cash / scale
    return {"daily_net": daily, "turnover": turn, "cost": cst, "n_held": n_held,
            "rebalanced": rebalanced, "trade_every": int(trade_every), "top_n": int(top_n),
            "first_live": int(np.argmax(np.isfinite(daily))) if np.isfinite(daily).any() else None}


def blocks(daily: np.ndarray, dates: np.ndarray, *, first: int, last: int,
           sessions: int = BLOCK_SESSIONS) -> pd.DataFrame:
    """Non-overlapping ``sessions``-session compounded blocks of a daily path,
    on a grid shared by every arm (``first`` and ``last`` are common)."""
    recs = []
    t = int(first)
    while t + sessions <= last + 1:
        seg = daily[t:t + sessions]
        if np.isfinite(seg).all():
            recs.append({"start": str(dates[t]), "end": str(dates[t + sessions - 1]),
                         "ret": float(np.prod(1.0 + seg) - 1.0),
                         "layer": "LOCKBOX" if str(dates[t]) >= LOCKBOX_START else "SELECTION"})
        t += sessions
    return pd.DataFrame(recs)


def _arm_stats(bl: pd.DataFrame, bench: pd.DataFrame, book: dict, *, lag: int) -> dict:
    if len(bl) == 0:
        return {"periods": 0}
    ppy = PPY / BLOCK_SESSIONS
    r = bl["ret"].to_numpy()
    b = bench["ret"].to_numpy()[:len(r)]
    ex = r - b
    sd = float(np.std(r, ddof=1)) if len(r) > 2 else float("nan")
    sde = float(np.std(ex, ddof=1)) if len(ex) > 2 else float("nan")
    reb = book["rebalanced"]
    return {"periods": int(len(r)), "effective_periods": int(len(r)),
            "first": str(bl["start"].iloc[0]), "last": str(bl["end"].iloc[-1]),
            "ann_net": float(r.mean() * ppy), "ann_net_excess": float(ex.mean() * ppy),
            "t_net_excess": S.nw_tstat(ex, lag)["t"],
            "ann_vol": float(sd * math.sqrt(ppy)) if np.isfinite(sd) else None,
            "sharpe": float(r.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None,
            "sharpe_excess": float(ex.mean() / sde * math.sqrt(ppy)) if sde and sde > 0 else None,
            "max_dd": S._max_dd(r), "max_dd_excess": S._max_dd(ex),
            "hit_rate_excess": float((ex > 0).mean()),
            "mean_oneway_turnover_per_21s": float(book["turnover"].sum() / len(r)),
            "ann_oneway_turnover": float(book["turnover"].sum() / len(r) * ppy),
            "ann_cost_drag": float(book["cost"].sum() / len(r) * ppy),
            "rebalances": int(reb.sum()), "mean_names_held": float(book["n_held"][book["n_held"] > 0].mean())}


def _paired(bl: pd.DataFrame, ref: pd.DataFrame, *, lag: int) -> dict:
    n = min(len(bl), len(ref))
    if n < 4:
        return {"periods": int(n)}
    ppy = PPY / BLOCK_SESSIONS
    adv = bl["ret"].to_numpy()[:n] - ref["ret"].to_numpy()[:n]
    st = S.nw_tstat(adv, lag)
    sd = float(np.std(adv, ddof=1)) if n > 2 else float("nan")
    half = n // 2
    return {"periods": int(n), "effective_periods": int(n),
            "ann_advantage": float(adv.mean() * ppy), "t_advantage": st["t"],
            "p_advantage_one_sided": st["p_one_sided"],
            "sharpe_advantage": float(adv.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None,
            "halves_ann_advantage": [float(adv[:half].mean() * ppy), float(adv[half:].mean() * ppy)],
            "hit_rate_advantage": float((adv > 0).mean())}


# --------------------------------------------------------------------------- #
# The grid
# --------------------------------------------------------------------------- #
def _substrate() -> dict:
    if "sub" in _CACHE:
        return _CACHE["sub"]
    E, elig = INC.equity_substrate()
    blk = INC.incumbent_blocks(E, elig)
    _CACHE["sub"] = {"E": E, "elig": elig, "blocks": blk, "ret": blk["_ret"], "dates": E["dates"]}
    return _CACHE["sub"]


def _books() -> dict:
    """Every arm's daily path plus the fixed benchmark, on one common window."""
    if "books" in _CACHE:
        return _CACHE["books"]
    sub = _substrate()
    dates, elig, ret = sub["dates"], sub["elig"], sub["ret"]
    out: dict = {}
    for sp in default_grid():
        sc = sub["blocks"][LEG_BLOCK[sp["leg"]]][..., 0]
        out[sp["cell_id"]] = daily_path_book(sc, elig=elig, ret=ret, dates=dates,
                                             top_n=sp["top_n"], trade_every=sp["trade_every"])
    bench = daily_path_book(sub["blocks"][INC.BLOCK_SCORE][..., 0], elig=elig, ret=ret, dates=dates,
                            top_n=0, trade_every=BLOCK_SESSIONS, whole_universe=True)
    firsts = [b["first_live"] for b in list(out.values()) + [bench] if b["first_live"] is not None]
    first = max(firsts) if firsts else 0
    last = len(dates) - 2
    _CACHE["books"] = {"arms": out, "benchmark": bench, "first": int(first), "last": int(last),
                       "dates": dates}
    return _CACHE["books"]


def measure_cell(sp: dict) -> dict:
    bk = _books()
    dates = bk["dates"]
    ref_id = "US_EQUITY|TOP%d|%s|k%d" % (EQ_TOP_N_OPERATIONAL, REFERENCE[0], REFERENCE[1])
    bench_bl = blocks(bk["benchmark"]["daily_net"], dates, first=bk["first"], last=bk["last"])
    ref_bl = blocks(bk["arms"][ref_id]["daily_net"], dates, first=bk["first"], last=bk["last"])
    arm_bl = blocks(bk["arms"][sp["cell_id"]]["daily_net"], dates, first=bk["first"], last=bk["last"])
    lag_arm = pit.nw_lag(int(sp["trade_every"]), BLOCK_SESSIONS)
    lag_pair = pit.nw_lag(max(int(sp["trade_every"]), REFERENCE[1]), BLOCK_SESSIONS)
    cell = dict(sp)
    cell["calculation_owner"] = CALCULATION_OWNER
    cell["evidence_label"] = POST_SELECTION
    cell["reference_cell_id"] = ref_id
    cell["construction"] = {
        "book": "EQ_LONG_ONLY_TOP%d_EW_DAILY_PATH" % EQ_TOP_N_OPERATIONAL,
        "benchmark": "equal-weight scored universe rebalanced every %d sessions (fixed across arms)"
                     % BLOCK_SESSIONS,
        "cost_per_side": EQ_COST_RATE_PER_SIDE, "block_sessions": BLOCK_SESSIONS,
        "delisting_rule": "a held name whose return stops printing is carried at zero until the "
                          "next rebalance sells it",
        "nw_lag_arm": lag_arm, "nw_lag_paired": lag_pair}
    for layer, mask in (("all", None), ("selection", "SELECTION"), ("lockbox", "LOCKBOX")):
        a = arm_bl if mask is None else arm_bl[arm_bl["layer"] == mask]
        b = bench_bl if mask is None else bench_bl[bench_bl["layer"] == mask]
        r = ref_bl if mask is None else ref_bl[ref_bl["layer"] == mask]
        cell[layer] = {"arm": _arm_stats(a, b, bk["arms"][sp["cell_id"]], lag=lag_arm),
                       "reference": _arm_stats(r, b, bk["arms"][ref_id], lag=lag_arm),
                       "paired": _paired(a, r, lag=lag_pair)}
    cell["gates"] = gates(cell)
    cell["verdict"] = verdict(cell)
    return T._jsonable(cell)


def gates(cell: dict, *, fdr_pass: bool | None = None, holm_pass: bool | None = None) -> dict:
    """The frozen tournament gates, unchanged, on the paired 21-session blocks.

    The turnover cap is the SAME economic bar as the tournament's: at most
    ``GATE_MAX_TURNOVER`` one-way per 21 sessions. A slower cadence passes it
    by trading less, never by moving it.
    """
    a, sel, lock = cell.get("all") or {}, cell.get("selection") or {}, cell.get("lockbox") or {}
    p, arm, ref = a.get("paired") or {}, a.get("arm") or {}, a.get("reference") or {}
    ps, pl = sel.get("paired") or {}, lock.get("paired") or {}
    adv, t = p.get("ann_advantage"), p.get("t_advantage")
    return {
        "materiality_ge_1p5pct": bool(adv is not None and adv >= MATERIALITY_ANN_NET),
        "paired_t_ge_2": bool(t is not None and t >= 2.0),
        "lockbox_sign_agrees": bool(ps.get("ann_advantage") is not None
                                    and pl.get("ann_advantage") is not None
                                    and np.sign(ps["ann_advantage"]) == np.sign(pl["ann_advantage"])
                                    and pl["ann_advantage"] > 0),
        "lockbox_halves_ge_floor": bool(pl.get("halves_ann_advantage")
                                        and min(pl["halves_ann_advantage"]) >= GATE_HALF_FLOOR),
        "turnover_le_cap": bool(arm.get("mean_oneway_turnover_per_21s") is not None
                                and arm["mean_oneway_turnover_per_21s"] <= GATE_MAX_TURNOVER),
        "drawdown_within_multiple": bool(arm.get("max_dd") is not None and ref.get("max_dd") is not None
                                         and arm["max_dd"] >= GATE_DD_MULTIPLE * ref["max_dd"]),
        "effective_sample_ge_floor": bool((p.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS),
        "benjamini_hochberg": fdr_pass,
        "family_holm": holm_pass,
    }


def verdict(cell: dict) -> str:
    if cell.get("is_reference"):
        return "REFERENCE_ARM"
    p = ((cell.get("all") or {}).get("paired")) or {}
    if not p or (p.get("periods") or 0) < 4:
        return T.V_DATA_HOLD
    g = cell.get("gates") or {}
    adv, t = p.get("ann_advantage"), p.get("t_advantage")
    if adv is not None and adv < 0 and t is not None and t <= -2.0:
        return T.V_WORSE
    decided = {k: v for k, v in g.items() if v is not None}
    if decided and all(decided.values()) and g.get("benjamini_hochberg") is True \
            and g.get("family_holm") is not False:
        return T.V_MATERIAL
    if g.get("materiality_ge_1p5pct") and g.get("paired_t_ge_2"):
        return T.V_NOT_QUALIFIED
    return T.V_NO_ADVANTAGE


def _cell_path(cell_id: str):
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("EQCH_%s.json" % re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id))


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("EQCH_*.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER:
            out.append(c)
    return out


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    grid = grid or default_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            prior = json.loads(p.read_text(encoding="utf-8"))
            if not prior.get("error"):
                cells.append(prior)
                if verbose:
                    print("[%d/%d] %s (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
                continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid), sp["cell_id"]), flush=True)
        t0 = time.time()
        try:
            cell = measure_cell(sp)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc),
                    "calculation_owner": CALCULATION_OWNER}
        cell["seconds"] = round(time.time() - t0, 1)
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            a = (cell.get("all") or {}).get("arm") or {}
            pr = (cell.get("all") or {}).get("paired") or {}
            print("    %s ann_net_ex=%s sharpe=%s dd=%s turn21=%s | adv=%s t=%s" % (
                cell.get("verdict"),
                None if a.get("ann_net_excess") is None else round(a["ann_net_excess"], 4),
                None if a.get("sharpe_excess") is None else round(a["sharpe_excess"], 3),
                None if a.get("max_dd") is None else round(a["max_dd"], 3),
                None if a.get("mean_oneway_turnover_per_21s") is None else round(a["mean_oneway_turnover_per_21s"], 3),
                None if pr.get("ann_advantage") is None else round(pr["ann_advantage"], 4),
                None if pr.get("t_advantage") is None else round(pr["t_advantage"], 2)), flush=True)
    return cells


def brief(c: dict) -> dict:
    a = c.get("all") or {}
    arm, ref, p = a.get("arm") or {}, a.get("reference") or {}, a.get("paired") or {}
    lk = ((c.get("lockbox") or {}).get("paired")) or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "leg": c.get("leg"),
            "trade_every": c.get("trade_every"), "verdict": c.get("verdict"),
            "ann_net_excess": arm.get("ann_net_excess"), "t_net_excess": arm.get("t_net_excess"),
            "sharpe_excess": arm.get("sharpe_excess"), "max_dd": arm.get("max_dd"),
            "ann_oneway_turnover": arm.get("ann_oneway_turnover"),
            "mean_oneway_turnover_per_21s": arm.get("mean_oneway_turnover_per_21s"),
            "ann_cost_drag": arm.get("ann_cost_drag"), "rebalances": arm.get("rebalances"),
            "reference_ann_net_excess": ref.get("ann_net_excess"),
            "reference_max_dd": ref.get("max_dd"), "reference_sharpe_excess": ref.get("sharpe_excess"),
            "reference_oneway_turnover_per_21s": ref.get("mean_oneway_turnover_per_21s"),
            "ann_advantage": p.get("ann_advantage"), "t_advantage": p.get("t_advantage"),
            "sharpe_advantage": p.get("sharpe_advantage"),
            "lockbox_ann_advantage": lk.get("ann_advantage"),
            "periods": p.get("periods") or (arm.get("periods")),
            "fdr_pass_paired": c.get("fdr_pass_paired"), "holm_pass_family": c.get("holm_pass_family"),
            "failed_gates": sorted(k for k, v in (c.get("gates") or {}).items() if v is False),
            "evidence_label": c.get("evidence_label"), "error": c.get("error")}


def reconciliation(cells: list) -> dict:
    """The reference arm against the incumbent baseline artifact: the same
    score, the same universe, the same cost, a daily path instead of forward
    windows. They are different measurements of one portfolio and must agree
    in sign and order of magnitude."""
    ref = next((c for c in cells if c.get("is_reference")), None)
    base = read_artifact(INC.ARTIFACT_NAME) or {}
    op = (((base.get("historical_oos") or {}).get("horizons") or {}).get("21") or {}).get(
        "top%d" % EQ_TOP_N_OPERATIONAL) or {}
    b_all = op.get("all") or {}
    r_all = ((ref or {}).get("all") or {}).get("arm") or {}
    a, b = r_all.get("ann_net_excess"), b_all.get("ann_net_excess")
    return {"reference_cell_id": (ref or {}).get("cell_id"),
            "daily_path_ann_net_excess": a, "incumbent_baseline_ann_net_excess": b,
            "daily_path_periods": r_all.get("periods"), "baseline_periods": b_all.get("periods"),
            "same_sign": (None if a is None or b is None else bool(np.sign(a) == np.sign(b))),
            "difference": (None if a is None or b is None else float(a - b)),
            "why_they_differ": "the baseline measures overlapping forward windows at the R63 decision "
                               "cadence; this arm compounds the realised daily path and charges cost "
                               "at the rebalance session"}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    scored = [c for c in cells if not c.get("is_reference") and ((c.get("all") or {}).get("paired") or {}).get("p_advantage_one_sided") is not None]
    p_adv = {c["cell_id"]: c["all"]["paired"]["p_advantage_one_sided"] for c in scored}
    bh = S.bh_fdr(p_adv, BH_Q)
    fam_p: dict = {}
    for c in scored:
        fam_p.setdefault(c.get("family"), {})[c["cell_id"]] = p_adv[c["cell_id"]]
    holm = {f: FAM.holm(ps, HOLM_ALPHA) for f, ps in fam_p.items()}
    for c in scored:
        c["fdr_pass_paired"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get(c.get("family"), {}).get("rejected", {}).get(c["cell_id"])
        c["gates"] = gates(c, fdr_pass=c["fdr_pass_paired"], holm_pass=c["holm_pass_family"])
        c["verdict"] = verdict(c)
    counts: dict = {}
    for c in cells:
        counts[c.get("verdict")] = counts.get(c.get("verdict"), 0) + 1
    best = None
    for c in scored:
        adv = c["all"]["paired"].get("ann_advantage")
        if adv is None:
            continue
        if best is None or adv > best["all"]["paired"]["ann_advantage"]:
            best = c
    body = {"schema": "alpha_recovery_equity_challengers/1", "calculation_owner": CALCULATION_OWNER,
            "question": "does the incumbent's OWN information do better under a different "
                        "construction: a slower rebalance, or one leg instead of the blend?",
            "grid": default_grid(), "n_cells": len(cells), "counts": counts,
            "reference": {"leg": REFERENCE[0], "trade_every": REFERENCE[1],
                          "why": "the operational construction of fundamental_momentum_50_50_v1"},
            "gates": "protocol head_to_head_tournament.gates_frozen (unchanged)",
            "multiple_testing": {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
                                 "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"}
                                                    for f, h in holm.items()},
                                 "denominator": len(p_adv)},
            "evidence_label": POST_SELECTION,
            "reconciliation_with_incumbent_baseline": reconciliation(cells),
            "best_by_advantage": brief(best) if best else None,
            "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
            "cells": cells}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
