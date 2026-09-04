"""alpha_agent.r57.futures_tournament - trend / breakout / cross-market
momentum on Norgate Continuous Futures, in DOLLARS against declared risk
capital, with the pre-registered roll-methodology contamination check.

Sizing (protocol): each market carries an equal risk budget; position size is
set so the market's 63-session realised $-P&L volatility hits a 10% annualised
target on its share of capital; the portfolio return is aggregate $P&L over
total capital. Rebalanced every 5 sessions with NEXT_CLOSE effect. Costs: 2bp
of traded notional per side, plus two sides on the held notional at every
detected roll.
"""
from __future__ import annotations

import math

import numpy as np

from . import (DISCOVERY_START, FUT_COST_RATE_PER_SIDE, now_iso,
               read_artifact, write_artifact)
from . import engine as E

SELECTION_ARTIFACT = "futures_validation_selection.json"
LOCKBOX_ARTIFACT = "futures_lockbox_results.json"

CADENCE = 5
TARGET_VOL = 0.10
VOL_LOOKBACK = 63
CAPITAL = 1_000_000.0
HAC_LAG = 10

#: Implementation constraint, applied uniformly to every family and variant
#: BEFORE any lockbox was evaluated: a market's notional may not exceed this
#: multiple of its capital slice. Without it the inverse-vol sizing rule
#: divides by a degenerate 63-day dollar-vol estimate (a short-rate contract in
#: the zero-rate era moves ~$1/day) and manufactures million-contract
#: positions whose transaction costs are astronomically larger than the
#: capital. This is a robustness bound on the REGISTERED sizing rule, not a
#: new signal parameter; the first (bugged) validation artifact was discarded
#: and the discard is disclosed in the release document.
MAX_LEVERAGE_PER_MARKET = 10.0

MATERIALITY_SHARPE = 0.40
MIN_DAILY_MARKS = 500
HALF_SHARPE_FLOOR = -0.10
MAX_DD_OF_CAPITAL = -0.25

FAMILIES = {
    "F1_TS_TREND": {
        "variants": ["tsmom_63_sign", "tsmom_126_sign", "tsmom_252_sign",
                     "tsmom_63_volscaled", "tsmom_126_volscaled",
                     "tsmom_252_volscaled"],
        "neighbour_order": ["tsmom_63_sign", "tsmom_126_sign", "tsmom_252_sign",
                            "tsmom_63_volscaled", "tsmom_126_volscaled",
                            "tsmom_252_volscaled"],
    },
    "F2_CHANNEL_BREAKOUT": {
        "variants": ["donchian_50", "donchian_100", "donchian_200"],
        "neighbour_order": ["donchian_50", "donchian_100", "donchian_200"],
    },
    "F3_XS_MOMENTUM": {
        "variants": ["xsmom_126", "xsmom_252"],
        "neighbour_order": ["xsmom_126", "xsmom_252"],
    },
}


# --------------------------------------------------------------------------- #
# Signals: matrix (markets x dates) of position DIRECTION/STRENGTH in [-1, 1]
# using data through each column's date.
# --------------------------------------------------------------------------- #
def _tsmom(close, lookback, volscaled):
    n_mkt, n_d = close.shape
    sig = np.zeros((n_mkt, n_d))
    with np.errstate(invalid="ignore", divide="ignore"):
        past = np.full_like(close, np.nan)
        past[:, lookback:] = close[:, lookback:] - close[:, :-lookback]
        if volscaled:
            d = np.diff(close, axis=1, prepend=close[:, :1])
            sd = _rolling_std(d, VOL_LOOKBACK)
            strength = past / (sd * math.sqrt(lookback))
            sig = np.clip(np.where(np.isfinite(strength), strength, 0.0), -1, 1)
        else:
            sig = np.sign(np.where(np.isfinite(past), past, 0.0))
    return sig


def _rolling_std(x, w):
    n_mkt, n_d = x.shape
    out = np.full((n_mkt, n_d), np.nan)
    c = np.nancumsum(x, axis=1)
    c2 = np.nancumsum(x * x, axis=1)
    cnt = np.cumsum(np.isfinite(x), axis=1).astype(float)
    for j in range(w, n_d):
        m = (c[:, j] - c[:, j - w]) / w
        v = (c2[:, j] - c2[:, j - w]) / w - m * m
        nn = cnt[:, j] - cnt[:, j - w]
        out[:, j] = np.where(nn >= w * 0.8, np.sqrt(np.maximum(v, 1e-18)), np.nan)
    return out


def _donchian(close, w):
    n_mkt, n_d = close.shape
    sig = np.zeros((n_mkt, n_d))
    state = np.zeros(n_mkt)
    hi = np.full(n_mkt, np.nan)
    lo = np.full(n_mkt, np.nan)
    for j in range(n_d):
        if j >= w:
            seg = close[:, j - w:j]
            hi = np.nanmax(seg, axis=1)
            lo = np.nanmin(seg, axis=1)
            cur = close[:, j]
            up = np.isfinite(cur) & np.isfinite(hi) & (cur > hi)
            dn = np.isfinite(cur) & np.isfinite(lo) & (cur < lo)
            state = np.where(up, 1.0, np.where(dn, -1.0, state))
        sig[:, j] = state
    return sig


def _xsmom(close, lookback):
    """Vol-adjusted trailing $ move, ranked across markets: top third +1,
    bottom third -1."""
    d = np.diff(close, axis=1, prepend=close[:, :1])
    sd = _rolling_std(d, VOL_LOOKBACK)
    with np.errstate(invalid="ignore", divide="ignore"):
        past = np.full_like(close, np.nan)
        past[:, lookback:] = close[:, lookback:] - close[:, :-lookback]
        strength = past / (sd * math.sqrt(lookback))
    n_mkt, n_d = close.shape
    sig = np.zeros((n_mkt, n_d))
    for j in range(lookback, n_d):
        s = strength[:, j]
        fin = np.isfinite(s)
        k = int(fin.sum())
        if k < 9:
            continue
        third = max(1, k // 3)
        order = np.argsort(s[fin])
        rows = np.where(fin)[0]
        sig[rows[order[-third:]], j] = 1.0
        sig[rows[order[:third]], j] = -1.0
    return sig


def signal_matrix(close: np.ndarray, variant: str) -> np.ndarray:
    if variant.startswith("tsmom_"):
        parts = variant.split("_")
        return _tsmom(close, int(parts[1]), parts[2] == "volscaled")
    if variant.startswith("donchian_"):
        return _donchian(close, int(variant.split("_")[1]))
    if variant.startswith("xsmom_"):
        return _xsmom(close, int(variant.split("_")[1]))
    raise ValueError(variant)


# --------------------------------------------------------------------------- #
# Simulation
# --------------------------------------------------------------------------- #
def simulate(fp: dict, variant: str, methodology: str = "a") -> dict:
    """Daily portfolio return series for one variant on one continuous
    methodology ('a' = '&MKT', 'b' = '&MKT_CCB')."""
    close = fp["close_" + methodology].astype(np.float64)
    pv = fp["point_values"][:, None]
    rolls = fp["rolls"]
    dates = fp["dates"]
    n_mkt, n_d = close.shape

    dollar = close * pv                              # $ value of one contract
    dpnl = np.diff(dollar, axis=1, prepend=dollar[:, :1])
    dpnl[:, 0] = 0.0
    sd = _rolling_std(dpnl, VOL_LOOKBACK)            # $ vol per contract
    sig = signal_matrix(close, variant)

    cap_per_mkt = CAPITAL / n_mkt
    target_daily = TARGET_VOL / math.sqrt(252.0) * cap_per_mkt

    start = int(np.searchsorted(dates, DISCOVERY_START))
    pos = np.zeros(n_mkt)                            # contracts, effective now
    pending = None                                   # NEXT_CLOSE, entered at
    pending_day = -1                                 # close `pending_day`
    pnl = np.zeros(n_d)
    costs = np.zeros(n_d)
    for j in range(start, n_d):
        # 1. the day's P&L on the position that EXISTED through this day
        held_notional = np.abs(pos) * np.abs(dollar[:, j - 1])
        roll_cost = float(np.where(rolls[:, j] > 0,
                                   np.where(np.isfinite(held_notional),
                                            held_notional, 0.0), 0.0).sum()) \
            * FUT_COST_RATE_PER_SIDE * 2.0
        costs[j] += roll_cost
        day = pos * np.where(np.isfinite(dpnl[:, j]), dpnl[:, j], 0.0)
        # 2. a trade scheduled for this close executes NOW: cost charged
        #    today, position effective for TOMORROW's P&L (no lookahead)
        if pending is not None and j >= pending_day:
            ref = np.where(np.isfinite(dollar[:, j]), np.abs(dollar[:, j]), 0.0)
            traded = np.abs(pending - pos) * ref
            costs[j] += float(traded.sum()) * FUT_COST_RATE_PER_SIDE
            pos = pending
            pending = None
        pnl[j] = float(day.sum()) - costs[j]
        # 3. rebalance decision from data through THIS close, entered next close
        if (j - start) % CADENCE == 0:
            with np.errstate(invalid="ignore", divide="ignore"):
                unit = target_daily / sd[:, j]
            new = sig[:, j] * np.where(np.isfinite(unit), unit, 0.0)
            new = np.where(np.isfinite(close[:, j]), new, 0.0)
            ref = np.where(np.isfinite(dollar[:, j]) & (np.abs(dollar[:, j]) > 0),
                           np.abs(dollar[:, j]), np.inf)
            with np.errstate(invalid="ignore", divide="ignore"):
                lev = np.abs(new) * ref / cap_per_mkt
                scale = np.where(lev > MAX_LEVERAGE_PER_MARKET,
                                 MAX_LEVERAGE_PER_MARKET
                                 / np.maximum(lev, 1e-12), 1.0)
            new = new * np.where(np.isfinite(scale), scale, 0.0)
            pending = new
            pending_day = j + 1
    ret = pnl / CAPITAL
    return {"dates": dates, "ret": ret, "start": start,
            "gross": (pnl + costs) / CAPITAL, "cost": costs / CAPITAL}


def layer_slice(dates: np.ndarray, start: int):
    from . import LOCKBOX_START, VALIDATION_START
    v0 = int(np.searchsorted(dates, VALIDATION_START))
    l0 = int(np.searchsorted(dates, LOCKBOX_START))
    emb = HAC_LAG
    return {"D": slice(start, v0 - emb), "V": slice(v0, l0 - emb),
            "L": slice(l0, len(dates))}


def stats(sim: dict, layer: str) -> dict:
    sl = layer_slice(sim["dates"], sim["start"])[layer]
    r = sim["ret"][sl]
    g = sim["gross"][sl]
    if len(r) < 30:
        return {"days": len(r)}
    ann = float(r.mean() * 252.0)
    vol = float(r.std() * math.sqrt(252.0))
    st = E.nw_tstat(r, lag=HAC_LAG)
    nav = np.cumprod(1.0 + r)
    dd = float((nav / np.maximum.accumulate(nav) - 1.0).min())
    return {"days": len(r),
            "first": str(sim["dates"][sl][0]), "last": str(sim["dates"][sl][-1]),
            "ann_net_return": ann, "ann_gross_return": float(g.mean() * 252.0),
            "ann_vol": vol, "net_sharpe": (ann / vol) if vol > 0 else None,
            "max_dd": dd, "t_net": st["t"], "p_one_sided": st["p_one_sided"],
            "halves_sharpe": [
                _sh(r[:len(r) // 2]), _sh(r[len(r) // 2:])]}


def _sh(r):
    if len(r) < 30 or r.std() == 0:
        return None
    return float(r.mean() / r.std() * math.sqrt(252.0))


def run_validation_pass(fp: dict) -> dict:
    existing = read_artifact(SELECTION_ARTIFACT)
    if existing:
        return existing
    out = {"stage": "VALIDATION_SELECTION", "families": {}}
    for fam_id, spec in FAMILIES.items():
        rows = {}
        for v in spec["variants"]:
            sim = simulate(fp, v, "a")
            rows[v] = {"discovery": stats(sim, "D"), "validation": stats(sim, "V")}
            print("  %s / %-18s V sharpe %s  D sharpe %s"
                  % (fam_id, v, rows[v]["validation"].get("net_sharpe"),
                     rows[v]["discovery"].get("net_sharpe")), flush=True)
        best = max(rows, key=lambda v: rows[v]["validation"].get("net_sharpe")
                   if rows[v]["validation"].get("net_sharpe") is not None else -9)
        order = spec["neighbour_order"]
        bi = order.index(best)
        neighbours = [order[j] for j in (bi - 1, bi + 1) if 0 <= j < len(order)]
        bsign = np.sign(rows[best]["validation"].get("net_sharpe") or 0)
        neigh_ok = all(np.sign(rows[nb]["validation"].get("net_sharpe") or 0)
                       == bsign for nb in neighbours) if neighbours else True
        out["families"][fam_id] = {
            "selected_variant": best,
            "validation_net_sharpe": rows[best]["validation"].get("net_sharpe"),
            "neighbour_sign_ok": bool(neigh_ok),
            "variants_evaluated": len(rows), "all_variants": rows}
    out["selection_completed_at"] = now_iso()
    write_artifact(SELECTION_ARTIFACT, out)
    return out


def run_lockbox_pass(fp: dict) -> dict:
    existing = read_artifact(LOCKBOX_ARTIFACT)
    if existing:
        return existing
    selection = read_artifact(SELECTION_ARTIFACT)
    assert selection, "futures validation selection must exist first"
    results, pvals = {}, {}
    for fam_id, sel in selection["families"].items():
        v = sel["selected_variant"]
        sim_a = simulate(fp, v, "a")
        sim_b = simulate(fp, v, "b")
        La, Lb = stats(sim_a, "L"), stats(sim_b, "L")
        results[fam_id] = {
            "selected_variant": v, "lockbox": La,
            "lockbox_ccb_methodology": {"net_sharpe": Lb.get("net_sharpe"),
                                        "ann_net_return": Lb.get("ann_net_return")},
            "roll_methodology_sign_match": (
                np.sign(La.get("net_sharpe") or 0)
                == np.sign(Lb.get("net_sharpe") or 0)),
            "validation_net_sharpe": sel["validation_net_sharpe"],
            "neighbour_sign_ok": sel["neighbour_sign_ok"]}
        pvals[fam_id] = La.get("p_one_sided")
        print("LOCKBOX %s / %s  sharpe %s (t %s)  CCB sharpe %s"
              % (fam_id, v, La.get("net_sharpe"), La.get("t_net"),
                 Lb.get("net_sharpe")), flush=True)
    out = {"stage": "LOCKBOX", "results": results, "p_values": pvals,
           "lockbox_evaluated_at": now_iso(),
           "selection_completed_at": selection.get("selection_completed_at")}
    write_artifact(LOCKBOX_ARTIFACT, out)
    return out


def verdicts(lockbox: dict, bh_pass: dict) -> dict:
    out = {}
    for fam_id, r in lockbox["results"].items():
        L = r["lockbox"]
        gates = {
            "effective_observations": (L.get("days", 0) >= MIN_DAILY_MARKS),
            "bh_fdr_q10": bool(bh_pass.get(fam_id, False)),
            "materiality_sharpe_0p40": (L.get("net_sharpe") or -9) >= MATERIALITY_SHARPE,
            "validation_lockbox_same_sign": (
                np.sign(r.get("validation_net_sharpe") or 0)
                == np.sign(L.get("net_sharpe") or 0)),
            "halves_floor": all((h if h is not None else -1) >= HALF_SHARPE_FLOOR
                                for h in (L.get("halves_sharpe") or [-1])),
            "neighbour_sign": bool(r.get("neighbour_sign_ok")),
            "roll_methodology_sign_match": bool(r.get("roll_methodology_sign_match")),
            "drawdown": (L.get("max_dd") or -1) >= MAX_DD_OF_CAPITAL,
        }
        ok = all(gates.values())
        out[fam_id] = {
            "verdict": ("HISTORICAL_ALPHA_CANDIDATE" if ok else "NO_ALPHA_EVIDENCE"),
            "failed_gates": sorted(k for k, v in gates.items() if not v),
            "gates": gates,
            "lockbox_net_sharpe": L.get("net_sharpe"),
            "lockbox_ann_net_return": L.get("ann_net_return"),
            "lockbox_days": L.get("days")}
    return out
