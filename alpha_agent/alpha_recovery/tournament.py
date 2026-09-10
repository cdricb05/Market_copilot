"""alpha_agent.alpha_recovery.tournament - head-to-head against the incumbent.

Workstream 6, the heart of the campaign. Same-domain (US equity) candidates
are measured on the IDENTICAL sample: the same eligible universe, the same
PIT decision dates, the same realised returns, the same folds, the same
forced ridge penalty, the same costs and the same book construction, with
the ONE scorer (``alpha_agent.r63.sensitivity.run_cell``) asked to keep its
out-of-sample scores. The baseline arm is the INCUMBENT_SCORE block alone -
a single feature, so the baseline ranking IS the incumbent's ranking (the
sign of the fitted coefficient is verified and published) - and the
augmented arm is the incumbent plus the candidate information. The paired
per-period difference between the two top-N books is the head-to-head.

Frozen gates (protocol section head_to_head_tournament): materiality
>= 1.5 %/yr net advantage, paired Newey-West t >= 2, lockbox sign
agreement, lockbox halves >= -0.5 %/yr, turnover <= 0.40 one-way, drawdown
>= 1.5 x the incumbent's, Benjamini-Hochberg q = 0.10 over every executed
same-domain paired p-value, Holm within the family, and the R63
information gate (conditional t >= 2, residual share >= 0.35).

Cross-domain sleeves are never compared as stock rankings: the incumbent-
only book is compared with incumbent + sleeve under EQUAL TOTAL RISK.
"""
from __future__ import annotations

import json
import math
import re
import time

import numpy as np
import pandas as pd

from alpha_agent.r63 import AC_US_EQUITY, LOCKBOX_START, ontology as ONT, pit
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, CONDITIONAL_T_FLOOR, EQ_COST_RATE_PER_SIDE, EQ_TOP_N_OPERATIONAL,
               EQ_TOP_N_SHADOW, GATE_DD_MULTIPLE, GATE_HALF_FLOOR, GATE_MAX_TURNOVER, HOLM_ALPHA,
               MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, research_root, write_artifact)
from . import earnings_events as EE
from . import incumbent as INC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.tournament"
CELLS_DIR = "cells"
ARTIFACT_NAME = "tournament.json"
PPY = 252.0
PARTIAL_RESIDUAL_SHARE_MAX = 0.35

V_MATERIAL = "MATERIALLY_BEATS_INCUMBENT"
V_NOT_QUALIFIED = "BEATS_INCUMBENT_NOT_QUALIFIED"
V_NO_ADVANTAGE = "NO_ADVANTAGE"
V_WORSE = "WORSE_THAN_INCUMBENT"
V_DATA_HOLD = "DATA_HOLD"
VERDICTS = (V_MATERIAL, V_NOT_QUALIFIED, V_NO_ADVANTAGE, V_WORSE, V_DATA_HOLD)

_DS: dict = {}


# --------------------------------------------------------------------------- #
# Datasets: the R63 equity dataset plus the campaign blocks
# --------------------------------------------------------------------------- #
def inject_blocks(ds: dict, blocks: dict) -> dict:
    out = dict(ds)
    out["blocks"] = {**ds["blocks"], **{k: v for k, v in blocks.items() if not k.startswith("_")}}
    return out


def equity_dataset(h: int, *, extra_builders: tuple = ()) -> dict:
    key = ("eq", int(h), tuple(b.__name__ for b in extra_builders))
    if key in _DS:
        return _DS[key]
    ds = X.assemble_equity(h)
    E, elig = INC.equity_substrate()
    blocks = dict(INC.incumbent_blocks(E, elig))
    blocks.update(EE.build_blocks(E))
    for b in extra_builders:
        blocks.update(b(E))
    out = inject_blocks(ds, blocks)
    _DS[key] = out
    if extra_builders:
        # an enriched dataset is a superset: later plain look-ups for the same
        # horizon (the cell runner's) must see the extra blocks too
        _DS[("eq", int(h), ())] = out
    return out


def spec(h: int, dimension: str, *, baseline: tuple, family: str, tag: str) -> dict:
    return {"scope": AC_US_EQUITY, "mode": "XS", "horizon": int(h), "dimension": dimension,
            "baseline": list(baseline), "family": family, "tag": tag,
            "cell_id": "%s|XS|%d|%s|vs|%s" % (AC_US_EQUITY, int(h), dimension,
                                             "+".join(baseline))}


def default_grid() -> list:
    inc = (INC.BLOCK_SCORE,)
    g = []
    for h in (1, 5, 21, 63):
        g.append(spec(h, EE.BLOCK, baseline=inc, family="EARNINGS_EVENT_REACTION", tag="PRIMARY"))
    g.append(spec(21, EE.BLOCK_EAR_ONLY, baseline=inc, family="EARNINGS_EVENT_REACTION", tag="PRIMARY"))
    g.append(spec(21, EE.BLOCK_SUE_ONLY, baseline=inc, family="EARNINGS_EVENT_REACTION", tag="PRIMARY"))
    base_r63 = tuple(ONT.baseline_for(AC_US_EQUITY))
    g.append(spec(21, EE.BLOCK, baseline=base_r63, family="EARNINGS_EVENT_REACTION",
                  tag="FRONTIER_CONDITIONAL"))
    return g


# --------------------------------------------------------------------------- #
# Books from the kept OOS scores: identical rows for both arms
# --------------------------------------------------------------------------- #
#: a period needs this many scored names for a top-N book; the news SAMPLE
#: (120 names by rule, fewer acquired within the bounded budget) uses a lower
#: floor, fixed here before any news cell ran
MIN_NAMES_SAMPLE = 20


def paired_books(preds: dict, *, top_n: int, cost: float = EQ_COST_RATE_PER_SIDE,
                 min_names: int = INC.MIN_NAMES) -> pd.DataFrame:
    """Per period: top-N EW book net return of arm B (incumbent ranking) and
    arm BD (incumbent + information), the EW benchmark of the scored rows,
    turnover of each, and the layer."""
    gid, iid, y = preds["gid"], preds["iid"], preds["y_raw"]
    pB, pBD, kind = preds["pred_B"], preds["pred_BD"], preds["fold_kind"]
    slot_dates = preds["slot_dates"]
    order = np.lexsort((iid, gid))
    gid, iid, y, pB, pBD, kind = gid[order], iid[order], y[order], pB[order], pBD[order], kind[order]
    periods = np.unique(gid)
    starts = np.searchsorted(gid, periods, side="left")
    ends = np.searchsorted(gid, periods, side="right")
    prev = {"B": {}, "BD": {}, "bench": {}}
    recs = []
    for g, a, b in zip(periods, starts, ends):
        yy, ii = y[a:b], iid[a:b]
        ok = np.isfinite(pB[a:b]) & np.isfinite(pBD[a:b]) & np.isfinite(yy)
        if ok.sum() < min_names:
            prev = {"B": {}, "BD": {}, "bench": {}}
            continue
        yy, ii = yy[ok], ii[ok]
        n = len(yy)
        bench_w = {int(i): 1.0 / n for i in ii}
        bt = sum(abs(bench_w.get(i, 0.0) - prev["bench"].get(i, 0.0)) for i in set(bench_w) | set(prev["bench"]))
        bench = float(yy.mean()) - bt * cost
        rec = {"gid": int(g), "date": str(slot_dates[int(g)]), "n": n,
               "layer": "LOCKBOX" if str(slot_dates[int(g)]) >= LOCKBOX_START else "SELECTION",
               "fold_kind": str(kind[a:b][ok][0]), "bench_net": bench}
        for arm, p in (("B", pB[a:b][ok]), ("BD", pBD[a:b][ok])):
            k = min(top_n, n)
            hi = np.argsort(-p, kind="mergesort")[:k]
            w = {int(i): 1.0 / k for i in ii[hi]}
            traded = sum(abs(w.get(i, 0.0) - prev[arm].get(i, 0.0)) for i in set(w) | set(prev[arm]))
            gross = float(yy[hi].mean())
            rec["%s_gross" % arm] = gross
            rec["%s_net" % arm] = gross - traded * cost
            rec["%s_turnover" % arm] = traded / 2.0
            rec["%s_rank_ic" % arm] = S.spearman(p, yy)
            prev[arm] = w
        prev["bench"] = bench_w
        rec["B_net_excess"] = rec["B_net"] - bench
        rec["BD_net_excess"] = rec["BD_net"] - bench
        rec["advantage"] = rec["BD_net"] - rec["B_net"]
        recs.append(rec)
    return pd.DataFrame(recs)


def _stats(rows: pd.DataFrame, *, h: int, cad: int) -> dict:
    if len(rows) == 0:
        return {"periods": 0}
    ppy = PPY / h            # annualise by the horizon a period's return covers
    lag = pit.nw_lag(h, cad)
    adv = rows["advantage"].to_numpy()
    st = S.nw_tstat(adv, lag)
    sd = float(np.std(adv, ddof=1)) if len(adv) > 2 else float("nan")

    def _arm(arm):
        ne = rows["%s_net_excess" % arm].to_numpy()
        net = rows["%s_net" % arm].to_numpy()
        s = float(np.std(ne, ddof=1)) if len(ne) > 2 else float("nan")
        return {"ann_net_excess": float(ne.mean() * ppy),
                "t_net_excess": S.nw_tstat(ne, lag)["t"],
                "sharpe_excess": float(ne.mean() / s * math.sqrt(ppy)) if s and s > 0 else None,
                "max_dd": S._max_dd(net), "max_dd_excess": S._max_dd(ne),
                "mean_oneway_turnover": float(rows["%s_turnover" % arm].mean()),
                "mean_rank_ic": float(np.nanmean(rows["%s_rank_ic" % arm].to_numpy())),
                "hit_rate_excess": float((ne > 0).mean())}
    half = len(adv) // 2
    return {"periods": int(len(rows)), "effective_periods": int(len(rows) * min(1.0, cad / float(h))),
            "first": str(rows["date"].iloc[0]), "last": str(rows["date"].iloc[-1]),
            "incumbent": _arm("B"), "challenger": _arm("BD"),
            "ann_advantage": float(adv.mean() * ppy), "t_advantage": st["t"],
            "p_advantage_one_sided": st["p_one_sided"],
            "sharpe_advantage": float(adv.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None,
            "halves_ann_advantage": ([float(adv[:half].mean() * ppy), float(adv[half:].mean() * ppy)]
                                     if len(adv) >= 4 else None),
            "hit_rate_advantage": float((adv > 0).mean())}


def ranking_identity(preds: dict, ds: dict) -> dict:
    """Is the baseline arm's ranking the incumbent's ranking? Mean per-period
    Spearman between pred_B and the INCUMBENT_SCORE rows."""
    blk = ds["blocks"].get(INC.BLOCK_SCORE)
    if blk is None:
        return {"state": "NOT_APPLICABLE"}
    dec = np.asarray(ds["dec"])
    sc = blk[preds["iid"], dec[preds["gid"]], 0]
    df = pd.DataFrame({"g": preds["gid"], "p": preds["pred_B"], "s": sc})
    df = df[np.isfinite(df["p"]) & np.isfinite(df["s"])]
    rhos = [S.spearman(g["p"].to_numpy(), g["s"].to_numpy()) for _, g in df.groupby("g")
            if len(g) >= MIN_NAMES_SAMPLE]
    rhos = [r for r in rhos if np.isfinite(r)]
    mean_rho = float(np.mean(rhos)) if rhos else None
    return {"mean_spearman_predB_vs_incumbent_score": mean_rho, "periods": len(rhos),
            "identical_ranking": bool(mean_rho is not None and mean_rho > 0.999),
            "sign_flipped": bool(mean_rho is not None and mean_rho < -0.999)}


# --------------------------------------------------------------------------- #
# One cell
# --------------------------------------------------------------------------- #
def measure_cell(sp: dict, *, verbose: bool = True) -> dict:
    h = int(sp["horizon"])
    ds = equity_dataset(h)
    dims_b = tuple(d for d in sp["baseline"] if d in ds["blocks"])
    if sp["dimension"] not in ds["blocks"] or not dims_b:
        return {**sp, "verdict": V_DATA_HOLD, "why": "dimension or baseline block missing"}
    t0 = time.time()
    cell = S.run_cell(ds, dims_b, sp["dimension"], keep_predictions=True, top_n=EQ_TOP_N_SHADOW)
    preds = cell.pop("_predictions", None)
    cell.update({k: v for k, v in sp.items() if k not in cell or k == "cell_id"})
    cell["cell_id"] = sp["cell_id"]
    cell["seconds"] = round(time.time() - t0, 1)
    cell["calculation_owner"] = CALCULATION_OWNER
    cell["n_folds"] = len(cell.get("folds") or [])
    cell.pop("folds", None)
    if preds is not None and cell.get("conditional"):
        cad = int(ds["cadence"])
        cell["ranking_identity"] = ranking_identity(preds, ds)
        h2h = {}
        min_names = MIN_NAMES_SAMPLE if str(sp.get("family", "")).startswith("NEWS") else INC.MIN_NAMES
        cell["book_min_names"] = min_names
        for top_n in (EQ_TOP_N_OPERATIONAL, EQ_TOP_N_SHADOW):
            rows = paired_books(preds, top_n=top_n, min_names=min_names)
            h2h["top%d" % top_n] = {
                "all": _stats(rows, h=h, cad=cad),
                "selection": _stats(rows[rows["layer"] == "SELECTION"] if len(rows) else rows, h=h, cad=cad),
                "lockbox": _stats(rows[rows["layer"] == "LOCKBOX"] if len(rows) else rows, h=h, cad=cad)}
        cell["head_to_head"] = h2h
        cell["gates"] = gates(cell)
    return _jsonable(cell)


def gates(cell: dict, *, fdr_pass: bool | None = None, holm_pass: bool | None = None) -> dict:
    """The frozen gates on the operational top-25 head-to-head."""
    op = ((cell.get("head_to_head") or {}).get("top%d" % EQ_TOP_N_OPERATIONAL)) or {}
    a, sel, lock = op.get("all") or {}, op.get("selection") or {}, op.get("lockbox") or {}
    c, r = cell.get("conditional") or {}, cell.get("redundancy") or {}
    ch, inc = a.get("challenger") or {}, a.get("incumbent") or {}
    adv, t = a.get("ann_advantage"), a.get("t_advantage")
    checks = {
        "materiality_ge_1p5pct": bool(adv is not None and adv >= MATERIALITY_ANN_NET),
        "paired_t_ge_2": bool(t is not None and t >= 2.0),
        "lockbox_sign_agrees": bool(sel.get("ann_advantage") is not None and lock.get("ann_advantage") is not None
                                    and np.sign(sel["ann_advantage"]) == np.sign(lock["ann_advantage"])
                                    and lock["ann_advantage"] > 0),
        "lockbox_halves_ge_floor": bool(lock.get("halves_ann_advantage")
                                        and min(lock["halves_ann_advantage"]) >= GATE_HALF_FLOOR),
        "turnover_le_cap": bool(ch.get("mean_oneway_turnover") is not None
                                and ch["mean_oneway_turnover"] <= GATE_MAX_TURNOVER),
        "drawdown_within_multiple": bool(ch.get("max_dd") is not None and inc.get("max_dd") is not None
                                         and ch["max_dd"] >= GATE_DD_MULTIPLE * inc["max_dd"]),
        "effective_sample_ge_floor": bool((a.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS),
        "information_conditional_t_ge_2": bool((c.get("t") or -99) >= CONDITIONAL_T_FLOOR),
        "information_distinct": bool((r.get("residual_share") or 0) >= PARTIAL_RESIDUAL_SHARE_MAX),
        "benjamini_hochberg": fdr_pass,
        "family_holm": holm_pass,
    }
    return checks


def verdict(cell: dict) -> str:
    if "head_to_head" not in cell or not cell.get("conditional"):
        return V_DATA_HOLD
    g = cell.get("gates") or {}
    op = ((cell.get("head_to_head") or {}).get("top%d" % EQ_TOP_N_OPERATIONAL)) or {}
    adv = (op.get("all") or {}).get("ann_advantage")
    t = (op.get("all") or {}).get("t_advantage")
    if adv is not None and adv < 0 and t is not None and t <= -2.0:
        return V_WORSE
    decided = {k: v for k, v in g.items() if v is not None}
    if decided and all(decided.values()) and g.get("benjamini_hochberg") is True \
            and g.get("family_holm") is not False:
        return V_MATERIAL
    if g.get("materiality_ge_1p5pct") and g.get("paired_t_ge_2"):
        return V_NOT_QUALIFIED
    return V_NO_ADVANTAGE


def _jsonable(o):
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, np.ndarray):
        return [_jsonable(v) for v in o.tolist()]
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        v = float(o)
        return v if np.isfinite(v) else None
    if isinstance(o, float) and not np.isfinite(o):
        return None
    return o


# --------------------------------------------------------------------------- #
# Grid, checkpointed
# --------------------------------------------------------------------------- #
def _cell_path(cell_id: str):
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id)
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("%s.json" % safe)


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER or "|vs|" in str(c.get("cell_id")):
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
        try:
            cell = measure_cell(sp, verbose=verbose)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc)}
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            op = ((cell.get("head_to_head") or {}).get("top25") or {}).get("all") or {}
            c = cell.get("conditional") or {}
            print("    cond t=%s adv=%s t_adv=%s lock_adv=%s %ss" % (
                None if c.get("t") is None else round(c["t"], 2),
                None if op.get("ann_advantage") is None else round(op["ann_advantage"], 4),
                None if op.get("t_advantage") is None else round(op["t_advantage"], 2),
                None if not ((cell.get("head_to_head") or {}).get("top25") or {}).get("lockbox") else
                round((cell["head_to_head"]["top25"]["lockbox"].get("ann_advantage") or 0.0), 4),
                cell.get("seconds")), flush=True)
    return cells


# --------------------------------------------------------------------------- #
# Multiplicity and the tournament artifact
# --------------------------------------------------------------------------- #
def apply_multiplicity(cells: list) -> dict:
    scored = [c for c in cells if c.get("head_to_head")]
    p_adv = {}
    for c in scored:
        op = (c["head_to_head"].get("top%d" % EQ_TOP_N_OPERATIONAL) or {}).get("all") or {}
        p_adv[c["cell_id"]] = op.get("p_advantage_one_sided")
    bh = S.bh_fdr(p_adv, BH_Q)
    families: dict = {}
    for c in scored:
        families.setdefault(c.get("family"), {})[c["cell_id"]] = p_adv.get(c["cell_id"])
    holm = {f: FAM.holm(ps, HOLM_ALPHA) for f, ps in families.items()}
    for c in scored:
        fp = bh["per_test"].get(c["cell_id"])
        hp = holm.get(c.get("family"), {}).get("rejected", {}).get(c["cell_id"])
        c["gates"] = gates(c, fdr_pass=fp, holm_pass=hp)
        c["fdr_pass_paired"] = fp
        c["holm_pass_family"] = hp
        c["tournament_verdict"] = verdict(c)
    return {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
            "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"} for f, h in holm.items()},
            "denominator": len(p_adv)}


def brief(c: dict) -> dict:
    op = (c.get("head_to_head") or {}).get("top%d" % EQ_TOP_N_OPERATIONAL) or {}
    a, lock = op.get("all") or {}, op.get("lockbox") or {}
    co, r = c.get("conditional") or {}, c.get("redundancy") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "tag": c.get("tag"),
            "horizon": c.get("horizon"), "dimension": c.get("dimension"),
            "tournament_verdict": c.get("tournament_verdict") or c.get("verdict"),
            "conditional_t": co.get("t"), "conditional_increment": co.get("increment"),
            "residual_share": r.get("residual_share"), "r63_verdict": c.get("verdict"),
            "ann_advantage_top25": a.get("ann_advantage"), "t_advantage_top25": a.get("t_advantage"),
            "sharpe_advantage_top25": a.get("sharpe_advantage"),
            "lockbox_ann_advantage_top25": lock.get("ann_advantage"),
            "incumbent_ann_net_excess": (a.get("incumbent") or {}).get("ann_net_excess"),
            "challenger_ann_net_excess": (a.get("challenger") or {}).get("ann_net_excess"),
            "incumbent_max_dd": (a.get("incumbent") or {}).get("max_dd"),
            "challenger_max_dd": (a.get("challenger") or {}).get("max_dd"),
            "incumbent_turnover": (a.get("incumbent") or {}).get("mean_oneway_turnover"),
            "challenger_turnover": (a.get("challenger") or {}).get("mean_oneway_turnover"),
            "effective_periods": a.get("effective_periods"),
            "fdr_pass_paired": c.get("fdr_pass_paired"), "holm_pass_family": c.get("holm_pass_family"),
            "failed_gates": sorted(k for k, v in (c.get("gates") or {}).items() if v is False),
            "ranking_identity": c.get("ranking_identity"), "coverage": c.get("coverage"),
            "error": c.get("error")}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    mt = apply_multiplicity(cells)
    briefs = sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"]))
    counts: dict = {}
    for b in briefs:
        counts[b["tournament_verdict"]] = counts.get(b["tournament_verdict"], 0) + 1
    best = None
    for b in briefs:
        if b.get("ann_advantage_top25") is None:
            continue
        if best is None or (b["ann_advantage_top25"] or -9) > (best["ann_advantage_top25"] or -9):
            best = b
    body = {"schema": "alpha_recovery_tournament/1", "calculation_owner": CALCULATION_OWNER,
            "scorer": "alpha_agent.r63.sensitivity.run_cell (keep_predictions=True)",
            "identical_sample": "both arms share rows, dates, realised returns, folds, forced penalty, "
                                "universe, costs and construction; baseline arm = INCUMBENT_SCORE",
            "gates": "protocol head_to_head_tournament.gates_frozen",
            "grid": default_grid(), "n_cells": len(cells), "counts": counts,
            "multiple_testing": mt, "best_by_advantage": best, "brief": briefs,
            "cells": cells}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


# --------------------------------------------------------------------------- #
# Cross-domain: incumbent-only versus incumbent + sleeve at equal total risk
# --------------------------------------------------------------------------- #
def cross_domain(incumbent_rows: pd.DataFrame, sleeve: pd.Series, *, horizon: int = 21,
                 sleeve_risk_share: float = 0.5, label: str = "sleeve") -> dict:
    """``incumbent_rows``: per-period rows with ``date`` (decision date), ``t``
    (decision index), ``strat_net`` (the incumbent-only book's net return
    over the forward window) - from ``incumbent.run_score``. ``sleeve``: the
    sleeve's per-session NET returns indexed by date. The sleeve is compounded
    over each incumbent window (sessions strictly after the decision date up
    to and including the window end), sized to ``sleeve_risk_share`` of the
    incumbent's realised volatility, and the combination is rescaled to the
    incumbent-only realised volatility so utility is compared at EQUAL RISK."""
    if incumbent_rows is None or len(incumbent_rows) == 0 or sleeve is None or len(sleeve) == 0:
        return {"state": "DATA_HOLD", "why": "no incumbent periods or no sleeve returns"}
    s = pd.Series(sleeve.values.astype(float), index=pd.to_datetime(sleeve.index)).sort_index()
    lg = np.log1p(np.clip(s, -0.999999, None))
    cum = lg.cumsum()
    dates = pd.to_datetime(incumbent_rows["date"])
    ends = []
    # window end = decision date + (horizon + 1) sessions on the incumbent's own calendar;
    # approximate on the sleeve's calendar by calendar days: horizon sessions ~ 1.45 x days
    for d in dates:
        ends.append(d + pd.Timedelta(days=int(round((horizon + 1) * 1.45))))
    sl = []
    for d, e in zip(dates, ends):
        seg = cum[(cum.index > d) & (cum.index <= e)]
        base = cum[cum.index <= d]
        if len(seg) == 0 or len(base) == 0:
            sl.append(np.nan)
            continue
        sl.append(float(np.expm1(seg.iloc[-1] - base.iloc[-1])))
    sl = np.array(sl)
    inc = incumbent_rows["strat_net"].to_numpy().astype(float)
    ok = np.isfinite(sl) & np.isfinite(inc)
    if ok.sum() < MIN_EFFECTIVE_PERIODS:
        return {"state": "DATA_HOLD", "why": "fewer than %d overlapping periods" % MIN_EFFECTIVE_PERIODS,
                "periods": int(ok.sum())}
    inc, sl = inc[ok], sl[ok]
    cad = 21
    ppy = PPY / cad
    v_inc = float(np.std(inc, ddof=1))
    v_sl = float(np.std(sl, ddof=1))
    if v_sl <= 0 or v_inc <= 0:
        return {"state": "DATA_HOLD", "why": "degenerate volatility"}
    size = sleeve_risk_share * v_inc / v_sl
    combo = inc + size * sl
    lam = v_inc / float(np.std(combo, ddof=1))
    combo_eq = lam * combo
    diff = combo_eq - inc
    st = S.nw_tstat(diff, 0)
    corr = float(np.corrcoef(inc, sl)[0, 1])

    def _m(x):
        sd = float(np.std(x, ddof=1))
        return {"ann_net": float(x.mean() * ppy), "ann_vol": float(sd * math.sqrt(ppy)),
                "sharpe": float(x.mean() / sd * math.sqrt(ppy)) if sd > 0 else None,
                "max_dd": S._max_dd(x)}
    a, b = _m(inc), _m(combo_eq)
    return {"state": "OK", "label": label, "periods": int(ok.sum()), "sleeve_risk_share": sleeve_risk_share,
            "sleeve_size_multiplier": float(size), "equal_risk_rescale": float(lam),
            "correlation_incumbent_sleeve": corr,
            "incumbent_only": a, "incumbent_plus_sleeve_equal_risk": b,
            "incremental_ann_net_return": b["ann_net"] - a["ann_net"],
            "sharpe_delta": (b["sharpe"] or 0.0) - (a["sharpe"] or 0.0),
            "drawdown_delta": (b["max_dd"] or 0.0) - (a["max_dd"] or 0.0),
            "t_incremental": st["t"], "p_incremental_one_sided": st["p_one_sided"],
            "positive_incremental_utility_after_costs": bool(b["ann_net"] - a["ann_net"] > 0 and (st["t"] or 0) >= 2.0),
            "rule": "protocol head_to_head_tournament.cross_domain"}
