r"""alpha_agent.alpha_recovery.orthogonal_composite - executor for the Alpha Agent mechanism
``ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1``.

Contract: ``research/preregistration/ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_PREREGISTRATION.md`` (committed
69d267f before any composite return existed).

Gate 1 re-derives the sleeve inventory from the ONE canonical global frontier
(``alpha_agent.r59.global_frontier``) under the frozen mechanical reading of E1-E10, applies the frozen
selection rule and holds on any drift from ``FROZEN_ELIGIBLE``. Only when at least three sleeves with three
mechanism classes are selected are their paths loaded through their own owners and aligned on the NYSE
sessions, and the ONE preregistered composite built: monthly 126-session inverse volatility, 35 % cap,
lagged covariance, 10 % volatility target, 1.5x cap, 50 bp over the bill above 1x, 2 bp / 4 bp overlay.
The qualification book is built on sessions up to the frozen split only; the confirmation is built and
read only after every qualification gate passes.

Existing owners only: ``alpha_agent.r63.sensitivity.nw_tstat`` / ``_max_dd``; cash and SPY through
``scheduled_event_equity_premium.load_dtb3`` / ``load_spy_total_return`` / ``daily_frame``.
RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

import json
import math
import sys
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.orthogonal_composite"
MECHANISM_ID = "ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1"
CANDIDATE_ID = "GC_XA_ORTHOGONAL_MULTI_ASSET_COMPOSITE"
ARTIFACT_NAME = "orthogonal_multi_asset_alpha_composite.json"
PREREGISTRATION = "research/preregistration/ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "69d267f"
SCORER = "alpha_agent.r63.sensitivity.nw_tstat/_max_dd"

KILL_RULE_FROZEN = (
    "DATA_HOLD if the inventory re-derived from the canonical global frontier differs from the preregistered "
    "eligible set, fewer than 3 sleeves or 3 distinct mechanism classes are selected, a selected return path is "
    "unavailable, the bill rate is missing on more than 1 % of sessions, or common history is below 2,268 "
    "sessions with qualification below 1,500 or confirmation below 600 sessions. NO_EDGE if on qualification "
    "at 2 bp overlay the composite has net annualised excess below 5.0 %, Sharpe below 1.00, NW t < 2.50, "
    "maximum drawdown worse than -15 %, a non-positive half, SPY-regression alpha below 3.0 %/yr or its HAC "
    "t < 2.00, Sharpe below volatility-matched SPY Sharpe + 0.25, absolute SPY correlation above 0.60, a sleeve "
    "above 60 % of absolute contribution, or at 4 bp net below 4.0 % or Sharpe below 0.80; or if the untouched "
    "confirmation has net below 4.0 %, Sharpe below 0.80, NW t < 2.00, drawdown worse than -15 %, SPY alpha "
    "below 2.5 %/yr or HAC t < 1.50, non-positive net at 4 bp, or a sleeve above 60 %; or if full common "
    "history has net below 5.0 %, Sharpe below 1.0, drawdown worse than -15 % or non-positive net at 4 bp.")

# --------------------------------------------------------------------------- #
# Frozen inventory reading (preregistration s2)
# --------------------------------------------------------------------------- #
GOOD_EVIDENCE = ("GOOD_STRATEGY_INCOMPLETE_EVIDENCE", "GOOD_STRATEGY_MATURE_EVIDENCE")
BLOCKING_STATES = ("BLOCKED", "CLOSED", "CONTROL")
PIT_OK = ("PIT_TRUE", "PIT_MARKET_OBSERVABLE", "PIT_BY_DECLARED_LAG")
#: Mirror of ``alpha_agent.r59.mechanisms.CLOSED_CLASSES``; a test pins the two together.
CLOSED_CLASSES = ("PRICE_STATE_TRANSFORMATION",)
INCUMBENT_STATE = "LIVE_CANDIDATE"
HUMAN_GATE_STATE = "HUMAN_GATE"
PURCHASE_ACTION = "DATA_PURCHASE_DECISION"
FROZEN_ELIGIBLE = ("GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW", "GC_FX_XS_CARRY_DATED_CONTRACT")
SIMILAR_SCORE = 0.02
MIN_SLEEVES, MAX_SLEEVES, MIN_MECHANISMS = 3, 5, 3

FX_CELL_ID = "FX_FUTURES|XS|1|CARRY|k5|b0.25"
SPY_SKEW_CONFIRMATION = "reversed_skew_challenger_confirmation.json"

#: Declared return-path owners (s3.2). A candidate absent here has no declared path (E7/E8).
RETURN_PATH_OWNERS = {
    "GC_FX_XS_CARRY_DATED_CONTRACT": {
        "state": "AVAILABLE", "identity": "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",
        "owner": "alpha_agent.alpha_recovery.cadence.sleeve_series", "cell_id": FX_CELL_ID,
        "resolution": "PER_SESSION", "loader": "load_fx_carry",
        "return_type": "net futures excess return of the risk-controlled cadence-5 cross-sectional book",
        "cost_owner": "alpha_agent.r64.construction per-instrument cost_bps_per_side",
        "normalisation": "already an excess return; a non-NYSE-dated return is compounded into the next NYSE session"},
    "GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW": {
        "state": "AVAILABLE", "identity": "REVERSED_SPY_PUT_CALL_SKEW_H5",
        "owner": "alpha_agent.alpha_recovery.options_surface.path", "resolution": "PER_PERIOD_5_SESSION",
        "loader": "load_spy_skew",
        "return_type": "frozen sign x SPY return over 5 sessions, gross 1.0, net of 1 bp per side",
        "cost_owner": "alpha_agent.alpha_recovery.options_surface.COST_PRIMARY_BPS",
        "normalisation": "sign x (SPY total-return session return - bill) on held sessions, 1 bp per side on the "
                         "entry and exit sessions",
        "evidence_label": "SIGN DISCOVERED POST HOC on 2024-09-10..2026-08-20; 2022-09-09..2024-08-15 independent"},
    "GC_XA_R39_R40_MACHINE_SHADOWS": {
        "state": "UNAVAILABLE", "resolution": "MONTH_END_ONLY",
        "why": "the owner stores month-end Zone-C rows only (wide_zone_c_streams.csv, 140 rows 2016-12..2026-06); "
               "a month-end return cannot be split into sessions without a construction the owner never froze"},
}

# --------------------------------------------------------------------------- #
# Frozen construction (s4)
# --------------------------------------------------------------------------- #
PPY = 252.0
LOOKBACK = 126
CAP = 0.35
TARGET_VOL = 0.10
MAX_SCALE = 1.50
FIN_SPREAD = 0.0050
OVERLAY_PRIMARY_BPS, OVERLAY_STRESS_BPS = 2.0, 4.0
NW_LAG = 10
QUAL_SHARE = 0.70
MIN_COMMON, MIN_QUAL, MIN_CONF = 2268, 1500, 600
MAX_BILL_MISSING = 0.01
MAX_CONTRIBUTION_SHARE = 0.60
QUAL = {"ann_net": 0.05, "sharpe": 1.00, "t": 2.50, "max_dd": -0.15, "alpha": 0.03, "alpha_t": 2.00,
        "sharpe_over_spy": 0.25, "abs_corr": 0.60, "stress_ann_net": 0.04, "stress_sharpe": 0.80}
CONF = {"ann_net": 0.04, "sharpe": 0.80, "t": 2.00, "max_dd": -0.15, "alpha": 0.025, "alpha_t": 1.50}
FULL = {"ann_net": 0.05, "sharpe": 1.0, "max_dd": -0.15}


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


# --------------------------------------------------------------------------- #
# Gate 1: inventory and selection
# --------------------------------------------------------------------------- #
def _catalog_candidates(catalog: Optional[dict]) -> dict:
    rec = (catalog or {}).get("global_reconciliation") or {}
    return {str(c.get("candidate_id")): c for c in rec.get("candidates") or [] if isinstance(c, dict)}


def inventory(frontier: dict, catalog: Optional[dict]) -> list:
    """Every frontier candidate in global rank order with its E1-E10 failures (s2.1)."""
    declared = _catalog_candidates(catalog)
    ranked = sorted((c for c in frontier.get("candidates") or [] if isinstance(c, dict)),
                    key=lambda c: (c.get("global_rank") is None, c.get("global_rank") or 10 ** 6,
                                   str(c.get("candidate_id"))))
    rows = []
    for c in ranked:
        cid = str(c.get("candidate_id"))
        hist = (declared.get(cid) or {}).get("historical") or {}
        members = list(c.get("members") or [])
        nba = c.get("next_best_action") or {}
        path = RETURN_PATH_OWNERS.get(cid) or {}
        fails = []
        if cid == CANDIDATE_ID or MECHANISM_ID in members:
            fails.append("E10_THIS_COMPOSITE")
        if not members:
            fails.append("E1_NO_FROZEN_IDENTITY")
        if (c.get("disposition") != "OPEN" or c.get("current_state") in BLOCKING_STATES
                or c.get("evidence_quality") not in GOOD_EVIDENCE):
            fails.append("E2_CLOSED_NO_EDGE_OR_HOLD")
        if str(c.get("data_requirement") or "").strip().upper().startswith("PAID"):
            fails.append("E3_PAID_DATA")
        if c.get("current_state") == HUMAN_GATE_STATE or nba.get("kind") == PURCHASE_ACTION:
            fails.append("E4_HUMAN_PURCHASE_GATE")
        if c.get("mechanism_class") in CLOSED_CLASSES:
            fails.append("E5_CLOSED_MECHANISM_CLASS")
        if hist.get("pit") not in PIT_OK or _f(hist.get("t_stat")) is None:
            fails.append("E6_NO_PIT_HISTORICAL_OWNER")
        if path.get("state") != "AVAILABLE":
            fails.append("E7_E8_RETURN_PATH_%s" % (path.get("state") or "NOT_DECLARED"))
        if c.get("current_state") == INCUMBENT_STATE:
            fails.append("E9_INCUMBENT")
        rows.append({"candidate_id": cid, "global_rank": c.get("global_rank"), "asset_class": c.get("asset_class"),
                     "mechanism_class": c.get("mechanism_class"), "members": members,
                     "current_state": c.get("current_state"), "evidence_quality": c.get("evidence_quality"),
                     "score": _f(c.get("opportunity_cost_score")), "pit": hist.get("pit"),
                     "historical_t": _f(hist.get("t_stat")), "next_action_kind": nba.get("kind"),
                     "return_path": path or None, "failures": fails, "eligible": not fails})
    return rows


def select(rows: list) -> list:
    """The frozen selection rule (s3.1) over eligible rows already in rank order."""
    pool = [r for r in rows if r["eligible"]]
    chosen, classes, assets = [], set(), set()
    while pool and len(chosen) < MAX_SLEEVES:
        head = pool[0]
        if head["mechanism_class"] in classes:
            pool.pop(0)
            continue
        if len(pool) > 1:
            nxt = pool[1]
            if (nxt["mechanism_class"] not in classes and head["asset_class"] in assets
                    and nxt["asset_class"] not in assets
                    and abs((head["score"] or 0.0) - (nxt["score"] or 0.0)) < SIMILAR_SCORE):
                head = nxt
        pool.remove(head)
        chosen.append(head)
        classes.add(head["mechanism_class"])
        assets.add(head["asset_class"])
    return chosen


def gate_inventory(frontier: dict, catalog: Optional[dict]) -> dict:
    rows = inventory(frontier, catalog)
    eligible = tuple(r["candidate_id"] for r in rows if r["eligible"])
    sel = select(rows)
    problems = []
    if frontier.get("state") != "COMPLETE":
        problems.append("FRONTIER_STATE_%s" % frontier.get("state"))
    if eligible != FROZEN_ELIGIBLE:
        problems.append("INVENTORY_DRIFT: derived %s, preregistered %s" % (list(eligible), list(FROZEN_ELIGIBLE)))
    if len(sel) < MIN_SLEEVES:
        problems.append("SLEEVES_%d_BELOW_%d" % (len(sel), MIN_SLEEVES))
    n_mech = len({r["mechanism_class"] for r in sel})
    if n_mech < MIN_MECHANISMS:
        problems.append("MECHANISM_CLASSES_%d_BELOW_%d" % (n_mech, MIN_MECHANISMS))
    return {"frontier_state": frontier.get("state"), "n_candidates": len(rows), "eligible": list(eligible),
            "selection": [{k: r[k] for k in ("candidate_id", "global_rank", "asset_class", "mechanism_class")}
                          for r in sel],
            "rows": rows, "problems": problems}


def load_frontier() -> tuple:
    """The canonical frontier, built by its owner from the committed catalog and the estate."""
    from alpha_agent.r59 import global_frontier as GF
    from alpha_agent.r59 import mechanisms as MX
    from alpha_agent.r59 import memory as M
    cat = MX.load_catalog()
    mem = M.ResearchMemory(read_only=True)
    return GF.build(cat, GF.load_estate(mem, catalog=cat)), cat


# --------------------------------------------------------------------------- #
# Return paths through their owners (s3.2)
# --------------------------------------------------------------------------- #
def to_sessions(series: pd.Series, sessions) -> pd.Series:
    """Compound every observation into the first session on or after its date; NaN where none."""
    sess = pd.DatetimeIndex(sessions).normalize()
    s = series.dropna().sort_index()
    idx = pd.DatetimeIndex(s.index).normalize()
    keep = (idx >= sess[0]) & (idx <= sess[-1])
    s, idx = s[keep], idx[keep]
    pos = sess.searchsorted(idx, side="left")
    g = pd.Series(np.log1p(s.to_numpy(dtype=float)), index=sess[pos]).groupby(level=0).sum()
    return pd.Series(np.expm1(g), index=g.index).reindex(sess)


def periods_from_positions(dates, pos, horizon: int) -> list:
    """The owner's non-overlapping decisions (options_surface.path): (entry date, exit date, position)."""
    dates = pd.DatetimeIndex(dates)
    pos = np.asarray(pos, dtype=float)
    out, t, n, h = [], 0, len(dates), int(horizon)
    while t + h < n:
        if np.isfinite(pos[t]):
            out.append((dates[t], dates[t + h], float(pos[t])))
        t += h
    return out


def sign_book_daily(periods: list, frame: pd.DataFrame, *, cost_bps: float) -> pd.Series:
    """Daily net excess return over the bill of a held sign position; NaN on sessions no period covers."""
    sess = pd.DatetimeIndex(frame.index)
    ex = (frame["r"] - frame["c"]).to_numpy(dtype=float)
    out = np.full(len(sess), np.nan)
    rate = float(cost_bps) * 1e-4
    for a, b, p in periods:
        i0 = int(sess.searchsorted(pd.Timestamp(a), side="right"))
        i1 = int(sess.searchsorted(pd.Timestamp(b), side="right")) - 1
        if i1 < i0 or not np.isfinite(p):
            continue
        seg = p * ex[i0:i1 + 1]
        seg[0] -= abs(p) * rate
        seg[-1] -= abs(p) * rate
        cur = out[i0:i1 + 1]
        out[i0:i1 + 1] = np.where(np.isnan(cur), seg, cur + seg)
    return pd.Series(out, index=sess)


def load_fx_carry(frame: pd.DataFrame) -> pd.Series:
    from . import cadence as CD
    s = CD.sleeve_series(FX_CELL_ID)
    if s is None or not len(s):
        raise ValueError("the FX carry cadence owner returned no path for %s" % FX_CELL_ID)
    return to_sessions(s, frame.index)


def load_spy_skew(frame: pd.DataFrame) -> pd.Series:
    from . import options_surface as OS
    conf = json.loads((research_root() / "results" / SPY_SKEW_CONFIRMATION).read_text(encoding="utf-8"))
    sp, dj = conf["cell"], conf["disjointness"]
    periods = []
    for surface in (dj["confirmation_surface"], dj["discovery_surface"]):
        f = OS.features(surface=surface)
        pos = float(sp["sign"]) * np.sign(OS._z(f[sp["field"]]))
        periods += periods_from_positions(pd.DatetimeIndex(f["date"]), pos, int(sp["horizon"]))
    return sign_book_daily(periods, frame, cost_bps=float(OS.COST_PRIMARY_BPS))


def nyse_sessions(index) -> pd.DatetimeIndex:
    """SPY sessions, minus any the authoritative rule-based calendar marks as a full-day closure."""
    idx = pd.DatetimeIndex(index).normalize()
    try:
        from paper_trader.engine import exchange_calendar as EC
    except ImportError:
        from engine import exchange_calendar as EC
    keep = [not (EC.is_supported(d.date()) and EC.is_non_session(d.date())) for d in idx]
    return idx[np.asarray(keep, dtype=bool)]


# --------------------------------------------------------------------------- #
# The composite (s4.2)
# --------------------------------------------------------------------------- #
def cap_weights(raw, cap: float = CAP) -> np.ndarray:
    raw = np.asarray(raw, dtype=float)
    if not len(raw) or not np.all(np.isfinite(raw)) or np.any(raw <= 0.0):
        raise ValueError("raw weights must be positive and finite")
    if cap * len(raw) < 1.0 - 1e-12:
        raise ValueError("a %.2f cap cannot hold %d sleeves at 100 %%" % (cap, len(raw)))
    capped = np.zeros(len(raw), dtype=bool)
    w = raw / raw.sum()
    for _ in range(len(raw)):
        over = (w > cap + 1e-12) & ~capped
        if not over.any():
            break
        capped |= over
        free = ~capped
        w = np.where(capped, cap, 0.0)
        if free.any():
            w[free] = raw[free] / raw[free].sum() * (1.0 - cap * capped.sum())
    return w


def first_rebalance_position(index) -> Optional[int]:
    idx = pd.DatetimeIndex(index)
    n = len(idx)
    if n <= LOOKBACK:
        return None
    ym = np.asarray(idx.year * 12 + idx.month)
    first = np.r_[True, ym[1:] != ym[:-1]]
    cand = np.flatnonzero(first & (np.arange(n) >= LOOKBACK))
    return int(cand[0]) if len(cand) else None


def build_composite(R: pd.DataFrame, *, overlay_bps: float) -> pd.DataFrame:
    """One row per portfolio session. Weights at a rebalance use only the prior 126 common sessions."""
    R = R.sort_index()
    X = R.to_numpy(dtype=float)
    idx = pd.DatetimeIndex(R.index)
    n, k = X.shape
    p0 = first_rebalance_position(idx)
    if p0 is None:
        return pd.DataFrame()
    ym = np.asarray(idx.year * 12 + idx.month)
    first = np.r_[True, ym[1:] != ym[:-1]]
    cols = list(R.columns)
    e, scale, rows = np.zeros(k), float("nan"), []
    for i in range(p0, n):
        turn, pv, reb = 0.0, float("nan"), bool(first[i])
        if reb:
            hist = X[i - LOOKBACK:i]
            vol = hist.std(axis=0, ddof=1) * math.sqrt(PPY)
            if not np.all(np.isfinite(vol)) or np.any(vol <= 0.0):
                raise ValueError("DEGENERATE_SLEEVE_VOLATILITY at %s" % idx[i].date())
            w = cap_weights(1.0 / vol, CAP)
            cov = np.atleast_2d(np.cov(hist, rowvar=False, ddof=1)) * PPY
            pv = float(math.sqrt(max(float(w @ cov @ w), 0.0)))
            scale = min(MAX_SCALE, TARGET_VOL / pv) if pv > 0.0 else MAX_SCALE
            new = scale * w
            turn = 0.5 * float(np.abs(new - e).sum())
            e = new
        contrib = e * X[i]
        gross = float(contrib.sum())
        fin = max(0.0, scale - 1.0) * FIN_SPREAD / PPY
        cost = turn * float(overlay_bps) * 1e-4
        row = {"date": idx[i], "gross": gross, "financing": fin, "turnover": turn, "overlay_cost": cost,
               "net": gross - fin - cost, "scale": scale, "rebalance": reb, "predicted_vol": pv}
        for j, c in enumerate(cols):
            row["e:%s" % c] = float(e[j])
            row["c:%s" % c] = float(contrib[j])
        rows.append(row)
    return pd.DataFrame(rows).set_index("date")


# --------------------------------------------------------------------------- #
# Measurement (s4.4)
# --------------------------------------------------------------------------- #
def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"sessions": int(len(x)), "ann_net": None, "ann_vol": None, "sharpe": None, "t_nw": None,
                "p_one_sided": None, "max_dd": None}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, NW_LAG)
    return {"sessions": int(len(x)), "ann_net": float(x.mean() * PPY),
            "ann_vol": sd * math.sqrt(PPY) if sd > 0 else None,
            "sharpe": float(x.mean() / sd * math.sqrt(PPY)) if sd > 0 else None,
            "t_nw": _f(st["t"]), "p_one_sided": _f(st["p_one_sided"]), "max_dd": _f(S._max_dd(x)),
            "hit_rate": float((x > 0).mean())}


def spy_regression(net, spy_ex) -> dict:
    y, x = np.asarray(net, dtype=float), np.asarray(spy_ex, dtype=float)
    ok = np.isfinite(y) & np.isfinite(x)
    y, x = y[ok], x[ok]
    if len(y) < 3 or float(np.var(x, ddof=1)) <= 0.0:
        return {"sessions": int(len(y)), "beta": None, "alpha_ann": None, "alpha_t_hac": None,
                "correlation": None, "vol_matched_spy_sharpe": None}
    beta = float(np.cov(y, x, ddof=1)[0, 1] / np.var(x, ddof=1))
    a = y - beta * x
    st = S.nw_tstat(a, NW_LAG)
    sdx = float(np.std(x, ddof=1))
    return {"sessions": int(len(y)), "beta": beta, "alpha_ann": float(a.mean() * PPY), "alpha_t_hac": _f(st["t"]),
            "correlation": float(np.corrcoef(y, x)[0, 1]),
            "vol_matched_spy_sharpe": float(x.mean() / sdx * math.sqrt(PPY)) if sdx > 0 else None,
            "spy_ann_excess": float(x.mean() * PPY), "definition": "a = net - beta_hat * SPY_EXCESS; nw_tstat(a, %d)"
                                                                     % NW_LAG}


def contributions(book: pd.DataFrame, sleeves: list) -> dict:
    tot = {s: float(book["c:%s" % s].sum()) for s in sleeves}
    den = sum(abs(v) for v in tot.values())
    share = {s: (abs(v) / den if den > 0 else None) for s, v in tot.items()}
    vals = [v for v in share.values() if v is not None]
    return {"cumulative": tot, "abs_share": share, "max_share": max(vals) if vals else None}


def _window(book: pd.DataFrame, stress: pd.DataFrame, spy_ex: pd.Series, sleeves: list) -> dict:
    return {"first": str(book.index.min().date()) if len(book) else None,
            "last": str(book.index.max().date()) if len(book) else None,
            "stats": stats(book["net"]), "stats_stress_4bp": stats(stress["net"]),
            "spy_regression": spy_regression(book["net"], spy_ex.reindex(book.index)),
            "contributions": contributions(book, sleeves),
            "mean_scale": float(book["scale"].mean()), "turnover_per_year": float(book["turnover"].sum() * PPY / len(book))}


def evaluate(R: pd.DataFrame, frame: pd.DataFrame) -> dict:
    """Gates 1 (history) to 5 on sleeve paths ``R`` and the SPY/bill ``frame`` (columns r, c)."""
    out: dict = {"confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"},
                 "full_history": {"state": "UNREAD"}, "untouched_confirmation": "NOT_READ"}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "kill_rule_fired": gate, "why": why})
        return out

    sleeves = list(R.columns)
    common = R.reindex(pd.DatetimeIndex(frame.index)).dropna(how="any")
    p0 = first_rebalance_position(common.index)
    n_port = len(common) - p0 if p0 is not None else 0
    q = int(math.floor(QUAL_SHARE * n_port))
    out["split"] = {"common_sessions": int(len(common)),
                    "common_first": str(common.index.min().date()) if len(common) else None,
                    "first_portfolio_session": str(common.index[p0].date()) if p0 is not None else None,
                    "portfolio_sessions": n_port, "qualification_sessions": q, "confirmation_sessions": n_port - q,
                    "confirmation_first_session": (str(common.index[p0 + q].date())
                                                   if p0 is not None and n_port - q > 0 else None),
                    "last_session": str(common.index.max().date()) if len(common) else None}
    problems = []
    if len(common) < MIN_COMMON:
        problems.append("COMMON_HISTORY_%d_BELOW_%d" % (len(common), MIN_COMMON))
    if q < MIN_QUAL:
        problems.append("QUALIFICATION_%d_BELOW_%d" % (q, MIN_QUAL))
    if n_port - q < MIN_CONF:
        problems.append("CONFIRMATION_%d_BELOW_%d" % (n_port - q, MIN_CONF))
    if p0 is not None:
        bill = frame["c"].reindex(common.index[p0:]).to_numpy(dtype=float)
        miss = float((~np.isfinite(bill)).mean()) if len(bill) else 1.0
        out["split"]["bill_missing_share"] = miss
        if miss > MAX_BILL_MISSING:
            problems.append("BILL_RATE_MISSING_SHARE_%.4f" % miss)
    if problems:
        return done("DATA_HOLD", "DATA", "DATA gate: %s" % "; ".join(problems))

    spy_ex = (frame["r"] - frame["c"]).reindex(common.index)
    qual_rows = common.iloc[:p0 + q]
    qb = build_composite(qual_rows, overlay_bps=OVERLAY_PRIMARY_BPS)
    qs = build_composite(qual_rows, overlay_bps=OVERLAY_STRESS_BPS)
    wq = _window(qb, qs, spy_ex, sleeves)
    h = len(qb) // 2
    wq["halves_ann_net"] = [stats(qb["net"].iloc[:h]).get("ann_net"), stats(qb["net"].iloc[h:]).get("ann_net")]
    wq["sleeves_on_the_same_sessions"] = {s: stats(qual_rows[s].iloc[p0:]) for s in sleeves}
    out["qualification"] = wq
    st, reg, stress = wq["stats"], wq["spy_regression"], wq["stats_stress_4bp"]

    def lt(v, floor) -> bool:
        return v is None or v < floor

    checks = [
        ("QUAL_NET", lt(st["ann_net"], QUAL["ann_net"]), "net annualised excess %s < 5.0 %%" % st["ann_net"]),
        ("QUAL_SHARPE", lt(st["sharpe"], QUAL["sharpe"]), "Sharpe %s < 1.00" % st["sharpe"]),
        ("QUAL_T", lt(st["t_nw"], QUAL["t"]), "NW t %s < 2.50" % st["t_nw"]),
        ("QUAL_DRAWDOWN", lt(st["max_dd"], QUAL["max_dd"]), "maximum drawdown %s worse than -15 %%" % st["max_dd"]),
        ("QUAL_HALVES", any(lt(v, 1e-12) for v in wq["halves_ann_net"]), "halves %s" % wq["halves_ann_net"]),
        ("QUAL_SPY_ALPHA", lt(reg["alpha_ann"], QUAL["alpha"]), "SPY alpha %s < 3.0 %%/yr" % reg["alpha_ann"]),
        ("QUAL_SPY_ALPHA_T", lt(reg["alpha_t_hac"], QUAL["alpha_t"]), "SPY alpha HAC t %s < 2.00" % reg["alpha_t_hac"]),
        ("QUAL_SHARPE_VS_SPY", st["sharpe"] is None or reg["vol_matched_spy_sharpe"] is None
         or st["sharpe"] < reg["vol_matched_spy_sharpe"] + QUAL["sharpe_over_spy"],
         "Sharpe %s < volatility-matched SPY %s + 0.25" % (st["sharpe"], reg["vol_matched_spy_sharpe"])),
        ("QUAL_SPY_CORRELATION", reg["correlation"] is None or abs(reg["correlation"]) > QUAL["abs_corr"],
         "|correlation| %s > 0.60" % reg["correlation"]),
        ("QUAL_CONCENTRATION", wq["contributions"]["max_share"] is None
         or wq["contributions"]["max_share"] > MAX_CONTRIBUTION_SHARE,
         "largest absolute contribution share %s > 60 %%" % wq["contributions"]["max_share"]),
        ("QUAL_STRESS_COST", lt(stress["ann_net"], QUAL["stress_ann_net"]) or lt(stress["sharpe"], QUAL["stress_sharpe"]),
         "at 4 bp net %s Sharpe %s" % (stress["ann_net"], stress["sharpe"])),
    ]
    for gate, failed, why in checks:
        if failed:
            return done("NO_EDGE", gate, why)

    fb = build_composite(common, overlay_bps=OVERLAY_PRIMARY_BPS)
    fs = build_composite(common, overlay_bps=OVERLAY_STRESS_BPS)
    if not np.allclose(fb["net"].iloc[:q].to_numpy(), qb["net"].to_numpy(), rtol=0.0, atol=1e-12):
        raise ValueError("LOOKAHEAD: the qualification rows changed when later sessions were added")
    cb, cs = fb.iloc[q:], fs.iloc[q:]
    wc = _window(cb, cs, spy_ex, sleeves)
    out["confirmation"] = {"state": "READ", **wc}
    cst, creg = wc["stats"], wc["spy_regression"]
    cchecks = [
        ("CONF_NET", lt(cst["ann_net"], CONF["ann_net"]), "confirmation net %s < 4.0 %%" % cst["ann_net"]),
        ("CONF_SHARPE", lt(cst["sharpe"], CONF["sharpe"]), "confirmation Sharpe %s < 0.80" % cst["sharpe"]),
        ("CONF_T", lt(cst["t_nw"], CONF["t"]), "confirmation NW t %s < 2.00" % cst["t_nw"]),
        ("CONF_DRAWDOWN", lt(cst["max_dd"], CONF["max_dd"]), "confirmation drawdown %s" % cst["max_dd"]),
        ("CONF_SPY_ALPHA", lt(creg["alpha_ann"], CONF["alpha"]), "confirmation SPY alpha %s" % creg["alpha_ann"]),
        ("CONF_SPY_ALPHA_T", lt(creg["alpha_t_hac"], CONF["alpha_t"]), "confirmation alpha t %s" % creg["alpha_t_hac"]),
        ("CONF_STRESS_COST", lt(wc["stats_stress_4bp"]["ann_net"], 1e-12),
         "confirmation net at 4 bp %s" % wc["stats_stress_4bp"]["ann_net"]),
        ("CONF_CONCENTRATION", wc["contributions"]["max_share"] is None
         or wc["contributions"]["max_share"] > MAX_CONTRIBUTION_SHARE,
         "confirmation largest share %s" % wc["contributions"]["max_share"]),
    ]
    failed_conf = next(((g, w) for g, f, w in cchecks if f), None)
    out["untouched_confirmation"] = "FAILED" if failed_conf else "CONFIRMED"
    if failed_conf:
        return done("NO_EDGE", failed_conf[0], failed_conf[1])
    full = _window(fb, fs, spy_ex, sleeves)
    out["full_history"] = {"state": "READ", **full}
    fst = full["stats"]
    fchecks = [
        ("FULL_NET", lt(fst["ann_net"], FULL["ann_net"]), "full-history net %s < 5.0 %%" % fst["ann_net"]),
        ("FULL_SHARPE", lt(fst["sharpe"], FULL["sharpe"]), "full-history Sharpe %s < 1.0" % fst["sharpe"]),
        ("FULL_DRAWDOWN", lt(fst["max_dd"], FULL["max_dd"]), "full-history drawdown %s" % fst["max_dd"]),
        ("FULL_STRESS_COST", lt(full["stats_stress_4bp"]["ann_net"], 1e-12),
         "full-history net at 4 bp %s" % full["stats_stress_4bp"]["ann_net"]),
    ]
    for gate, failed, why in fchecks:
        if failed:
            return done("NO_EDGE", gate, why)
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched composite "
                                   "confirmation; a HUMAN gate governs a portfolio-level TRUE_FORWARD registration")


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #
def design() -> dict:
    return {"inventory_owner": "alpha_agent.r59.global_frontier", "frozen_eligible": list(FROZEN_ELIGIBLE),
            "selection": {"min": MIN_SLEEVES, "max": MAX_SLEEVES, "min_mechanism_classes": MIN_MECHANISMS,
                          "similar_score": SIMILAR_SCORE},
            "return_path_owners": RETURN_PATH_OWNERS, "calendar": "SPY sessions minus engine.exchange_calendar closures",
            "cash": "FRED DTB3, last observation strictly before the session / 100 / 252",
            "lookback_sessions": LOOKBACK, "cap": CAP, "target_vol": TARGET_VOL, "max_scale": MAX_SCALE,
            "financing_spread_above_1x": FIN_SPREAD, "overlay_bps": [OVERLAY_PRIMARY_BPS, OVERLAY_STRESS_BPS],
            "nw_lag": NW_LAG, "split": {"qualification_share": QUAL_SHARE, "min_common": MIN_COMMON,
                                        "min_qualification": MIN_QUAL, "min_confirmation": MIN_CONF},
            "gates": {"qualification": QUAL, "confirmation": CONF, "full_history": FULL,
                      "max_contribution_share": MAX_CONTRIBUTION_SHARE}}


def run(*, verbose: bool = True, write: bool = True, frontier: Optional[dict] = None,
        catalog: Optional[dict] = None, spy_tr: Optional[pd.Series] = None,
        dtb3: Optional[pd.Series] = None) -> dict:
    if frontier is None:
        frontier, catalog = load_frontier()
    inv = gate_inventory(frontier, catalog)
    ident = "GLOBAL_FRONTIER:%s:%d candidates:eligible=%s" % (inv["frontier_state"], inv["n_candidates"],
                                                             ",".join(inv["eligible"]))
    if inv["problems"]:
        res = {"verdict": "DATA_HOLD", "gate": "DATA", "kill_rule_fired": "DATA",
               "why": "DATA gate (inventory): %s" % "; ".join(inv["problems"]),
               "confirmation": {"state": "UNREAD", "why": "gate 1 held; no return path was loaded"},
               "untouched_confirmation": "NOT_READ", "paths_loaded": []}
    else:
        from . import scheduled_event_equity_premium as SE
        spy_tr = SE.load_spy_total_return() if spy_tr is None else spy_tr
        dtb3 = SE.load_dtb3() if dtb3 is None else dtb3
        frame = SE.daily_frame(spy_tr, dtb3)
        frame = frame.reindex(nyse_sessions(frame.index))
        me = sys.modules[__name__]
        R = pd.DataFrame({r["candidate_id"]: getattr(me, RETURN_PATH_OWNERS[r["candidate_id"]]["loader"])(frame)
                          for r in inv["selection"]})
        res = evaluate(R, frame)
        res["paths_loaded"] = list(R.columns)
        ident += ";NORGATE:SPY(TOTALRETURN);FRED:DTB3"
    res["inventory"] = {k: inv[k] for k in ("frontier_state", "n_candidates", "eligible", "selection", "problems")}
    res["inventory_rows"] = inv["rows"]
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "candidate_id": CANDIDATE_ID, "preregistration": PREREGISTRATION,
            "preregistration_commit": PREREGISTRATION_COMMIT, "kill_rule_frozen": KILL_RULE_FROZEN,
            "design": design(), "input_data_identity": ident, "result": res, "scorer": SCORER,
            "capital_eligible": False}
    if write:
        body["artifact_path"] = str(write_artifact(ARTIFACT_NAME, body))
    if verbose:
        print("%s -> %s (%s)" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def executor_result(body: dict) -> dict:
    res = body["result"]
    wq = res.get("qualification") or {}
    st, reg = wq.get("stats") or {}, wq.get("spy_regression") or {}
    inv = res.get("inventory") or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("kill_rule_fired"),
            "statistic": {"lockbox_t": st.get("t_nw"), "lockbox_window": "QUALIFICATION %s" % (res.get("split") or {}),
                          "p_one_sided": st.get("p_one_sided"), "sessions": st.get("sessions"),
                          "spy_alpha_t_hac": reg.get("alpha_t_hac"),
                          "untouched_confirmation": res.get("untouched_confirmation"),
                          "eligible": inv.get("eligible"), "selection": inv.get("selection"),
                          "inventory_problems": inv.get("problems")},
            "economics": {"ann_net_2bp": st.get("ann_net"), "sharpe": st.get("sharpe"), "max_dd": st.get("max_dd"),
                          "spy_alpha_ann": reg.get("alpha_ann"), "spy_correlation": reg.get("correlation"),
                          "ann_net_4bp": (wq.get("stats_stress_4bp") or {}).get("ann_net"),
                          "max_contribution_share": (wq.get("contributions") or {}).get("max_share")},
            "multiplicity": {"m": 1, "state": "ONE_PREREGISTERED_COMPOSITE; components keep their own labels"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body.get("input_data_identity")}


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses another mechanism or a changed kill rule."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    if ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE") != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor does not "
                         "judge a changed contract")
    try:
        body = run(verbose=True, write=True)
    except (OSError, KeyError, ValueError, ImportError) as exc:
        return {"verdict": "DATA_HOLD", "why": "an owned input is unavailable or unreadable: %r" % (exc,),
                "kill_rule_fired": "DATA", "statistic": {}, "economics": {},
                "multiplicity": {"m": 1, "state": "NOT_REACHED"}, "artifact": None, "capital_eligible": False,
                "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    return executor_result(body)


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "inventory", "select", "gate_inventory", "cap_weights",
           "build_composite", "evaluate", "run", "run_mechanism"]
