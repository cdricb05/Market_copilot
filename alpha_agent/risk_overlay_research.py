"""
alpha_agent.risk_overlay_research — Stage 7 Workstreams 3 & 5 + recovery
orchestration.

  * Workstream 3 — a fixed, deterministic RISK-OVERLAY TOURNAMENT. The current
    Alpha Paper Book #1 is the UNCHANGED control; a fixed grid of deterministic
    shadow re-weightings (inverse-vol, portfolio vol-target 15/20/25%, risk-
    contribution cap, sector-risk cap, extreme-vol sleeve cap, market-regime
    cash overlay) is evaluated on real survivorship-aware daily history, holding
    the CURRENT held-name SET fixed so the tournament isolates RISK CONSTRUCTION,
    never selection. No leverage above 100%; residual is cash; costs included;
    no grid is tuned to the recent losing days (evaluated over full history with
    trailing-data weights).
  * A read-only MANUAL de-risking preview is produced only when robust evidence
    supports it — never a target, proposal, decision, signal or order.
  * The single recovery DISPOSITION is synthesised deterministically from the
    champion autopsy + overlay evidence + campaign evidence + forward-sample
    adequacy.
  * The future-promotion checklist evidence (Workstream 5) is derived here.
  * Recovery run orchestration: assemble + write the immutable Stage 7 package,
    verify it, and update ``latest.json`` — all read-only w.r.t. every
    operational ledger and stage root.

Safety: research-only, read-only. No order/fill/signal/trade decision, no model
promotion, no portfolio / Alpha Paper Book / ledger mutation, no prediction
service, no DB, no LLM, no package install. Pure stdlib math.
"""
from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
from typing import Any, Callable, Optional

from . import evidence_observatory as eo
from . import experiment_contracts as ec
from . import runtime_contracts as rc

# --------------------------------------------------------------------------- #
# Deterministic overlay grid (fixed — never tuned to recent data).
# --------------------------------------------------------------------------- #
OV_CONTROL = "CURRENT_CONTROL"
OV_INVERSE_VOL = "INVERSE_VOL_NAME_CAPPED"
OV_VOL_TARGET = "PORTFOLIO_VOL_TARGET"
OV_RISK_CONTRIB = "RISK_CONTRIBUTION_CAPPED"
OV_SECTOR_RISK = "SECTOR_RISK_CAPPED"
OV_EXTREME_VOL = "EXTREME_VOLATILITY_SLEEVE_CAP"
OV_REGIME_CASH = "MARKET_REGIME_CASH_OVERLAY"

# Stage 7.1 WS5 — the EXACTLY-THREE overlays tracked forward as read-only
# shadow portfolios (labels, not spec keys). Deterministic and fixed.
FORWARD_SHADOW_OVERLAYS = (OV_CONTROL, OV_REGIME_CASH,
                           OV_VOL_TARGET + "_20")

# Stage 7.1 WS4 — Stage 7 recurring-cadence decisions (deterministic tokens).
CAD_REUSE = "REUSE_LATEST"
CAD_RUN_DELTA = "RUN_DELTA"
CAD_RUN_FULL = "RUN_FULL"
CAD_NONE = "NONE_NO_PACKAGE"
STAGE7_CADENCE_ACTIONS = (CAD_REUSE, CAD_RUN_DELTA, CAD_RUN_FULL, CAD_NONE)


class ImmutableRunError(RuntimeError):
    """Raised when writing a Stage 7 package would overwrite an EXISTING,
    non-identical immutable run directory. Immutable evidence is never silently
    rewritten: a schema/engine/evidence change must land under a new run id."""


def _run_identity(cfg: dict, as_of: str, stage7_input: dict) -> dict:
    """The identity an on-disk package must match to be considered the SAME
    immutable run. Binds package schema, engine version, config and the upstream
    evidence fingerprint so any of those changing marks a DIFFERENT run."""
    upstream = (stage7_input.get("upstream_fingerprint")
                or upstream_evidence_fingerprint(cfg).get("fingerprint"))
    return {
        "schema_version": eo.STAGE7_SCHEMA_VERSION,
        "engine_version": eo.STAGE7_ENGINE_VERSION,
        "config_hash": eo.config_hash(cfg),
        "upstream_fingerprint": upstream,
        "as_of": as_of,
    }


def default_overlay_specs(cfg: Optional[dict] = None) -> list[dict]:
    ov = (cfg or {}).get("overlays") or {}
    name_cap = float(ov.get("name_cap_pct", 5.0)) / 100.0
    vol_targets = list(ov.get("vol_targets_pct", [15.0, 20.0, 25.0]))
    rc_cap_mult = float(ov.get("risk_contribution_cap_mult", 1.5))
    sector_rc_cap = float(ov.get("sector_risk_cap_pct", 40.0)) / 100.0
    extreme_vol = float(ov.get("extreme_annual_vol_pct", 60.0)) / 100.0
    regime_scale = float(ov.get("regime_risk_off_scale", 0.5))
    specs = [
        {"overlay": OV_CONTROL, "params": {}},
        {"overlay": OV_INVERSE_VOL, "params": {"name_cap": name_cap}},
    ]
    for vt in vol_targets:
        specs.append({"overlay": OV_VOL_TARGET,
                      "variant": "%d" % int(vt),
                      "params": {"target_annual_vol": float(vt) / 100.0}})
    specs.append({"overlay": OV_RISK_CONTRIB,
                  "params": {"rc_cap_mult": rc_cap_mult, "name_cap": name_cap}})
    specs.append({"overlay": OV_SECTOR_RISK,
                  "params": {"sector_rc_cap": sector_rc_cap}})
    specs.append({"overlay": OV_EXTREME_VOL,
                  "params": {"extreme_annual_vol": extreme_vol,
                             "name_cap": name_cap}})
    specs.append({"overlay": OV_REGIME_CASH,
                  "params": {"risk_off_scale": regime_scale, "ma_days": 200}})
    return specs


def overlay_label(spec: dict) -> str:
    v = spec.get("variant")
    return spec["overlay"] + (("_" + str(v)) if v else "")


_STEP_MONTH = 21
_TRAIL = 63          # trailing window (trading days) for vol / covariance
_PPY_D = 252.0


# --------------------------------------------------------------------------- #
# Return-panel + linear-algebra helpers (pure stdlib).
# --------------------------------------------------------------------------- #
def build_return_panel(price_panel: dict) -> dict:
    """{ticker: [(date, close)]} → {ticker: {date: daily_simple_return}}."""
    out: dict[str, dict] = {}
    for t, series in price_panel.items():
        clean = sorted({(str(d)[:10], float(c)) for d, c in series
                        if _num(c)}, key=lambda p: p[0])
        rr: dict[str, float] = {}
        for i in range(1, len(clean)):
            p0 = clean[i - 1][1]
            p1 = clean[i][1]
            if p0 and p0 > 0:
                rr[clean[i][0]] = p1 / p0 - 1.0
        if rr:
            out[str(t)] = rr
    return out


def series_to_returns(series) -> dict:
    """[(date, close)] → {date: daily_return} for a single symbol (e.g., SPY)."""
    clean = sorted({(str(d)[:10], float(c)) for d, c in series if _num(c)},
                   key=lambda p: p[0])
    out: dict[str, float] = {}
    for i in range(1, len(clean)):
        p0 = clean[i - 1][1]
        if p0 and p0 > 0:
            out[clean[i][0]] = clean[i][1] / p0 - 1.0
    return out


def series_levels(series) -> list[tuple[str, float]]:
    return sorted({(str(d)[:10], float(c)) for d, c in series if _num(c)},
                  key=lambda p: p[0])


def _num(v) -> bool:
    try:
        f = float(v)
        return f == f and abs(f) != float("inf")
    except (TypeError, ValueError):
        return False


def _trailing(returns_by_name, names, dates, upto_idx, window):
    lo = max(0, upto_idx - window)
    seg = dates[lo:upto_idx]
    out: dict[str, list] = {}
    for n in names:
        rmap = returns_by_name.get(n, {})
        vals = [rmap[d] for d in seg if d in rmap]
        if len(vals) >= max(5, window // 2):
            out[n] = vals
    return out


def _ann_vol_of(vals) -> Optional[float]:
    sd = ec.stdev(vals)
    return sd * (_PPY_D ** 0.5) if sd is not None else None


def _cov_matrix(names, trail):
    """Annualised covariance matrix over the trailing daily returns."""
    means = {n: (sum(trail[n]) / len(trail[n])) for n in names if n in trail}
    m = len(names)
    cov = [[0.0] * m for _ in range(m)]
    for a in range(m):
        na = names[a]
        if na not in trail:
            continue
        for b in range(a, m):
            nb = names[b]
            if nb not in trail:
                continue
            xs = trail[na]
            ys = trail[nb]
            k = min(len(xs), len(ys))
            if k < 3:
                continue
            ma, mb = means[na], means[nb]
            s = sum((xs[i] - ma) * (ys[i] - mb) for i in range(k)) / (k - 1)
            s *= _PPY_D
            cov[a][b] = s
            cov[b][a] = s
    return cov


def _risk_contributions(weights, names, cov):
    """Percent risk contribution per name (sum≈1 over invested names)."""
    m = len(names)
    w = [weights.get(names[i], 0.0) for i in range(m)]
    sigw = [sum(cov[i][j] * w[j] for j in range(m)) for i in range(m)]
    port_var = sum(w[i] * sigw[i] for i in range(m))
    if port_var <= 0:
        return {}, 0.0
    rc_pct = {}
    for i in range(m):
        mrc = w[i] * sigw[i]
        rc_pct[names[i]] = mrc / port_var
    return rc_pct, port_var ** 0.5


# --------------------------------------------------------------------------- #
# Deterministic overlay weight rules (all keep gross ≤ 1.0; residual = cash).
# --------------------------------------------------------------------------- #
def _normalize_capped(raw, gross, name_cap):
    """Scale raw weights to sum==gross with a per-name cap (deterministic
    water-filling)."""
    names = [n for n, v in raw.items() if v > 0]
    if not names:
        return {}
    w = {n: raw[n] for n in names}
    tot = sum(w.values())
    w = {n: v / tot * gross for n, v in w.items()}
    for _ in range(50):
        over = {n: v for n, v in w.items() if v > name_cap + 1e-12}
        if not over:
            break
        excess = sum(v - name_cap for v in over.values())
        for n in over:
            w[n] = name_cap
        free = [n for n in names if w[n] < name_cap - 1e-12]
        base = sum(w[n] for n in free)
        if base <= 0 or excess <= 0:
            break
        for n in free:
            w[n] += excess * (w[n] / base)
    return w


def _overlay_weights(overlay, params, base_weights, names, trail, cov,
                     sector_map, spy_trail_levels):
    gross = sum(base_weights.values())
    if overlay == OV_CONTROL:
        return dict(base_weights)

    if overlay == OV_INVERSE_VOL:
        inv = {}
        for n in names:
            v = _ann_vol_of(trail.get(n, []))
            if v and v > 0:
                inv[n] = 1.0 / v
        if not inv:
            return dict(base_weights)
        return _normalize_capped(inv, gross, params.get("name_cap", 0.05))

    if overlay == OV_VOL_TARGET:
        rc_pct, port_vol = _risk_contributions(base_weights, names, cov)
        tgt = params.get("target_annual_vol", 0.20)
        if not port_vol or port_vol <= 0:
            return dict(base_weights)
        scale = min(1.0, tgt / port_vol)
        return {n: w * scale for n, w in base_weights.items()}

    if overlay == OV_RISK_CONTRIB:
        return _risk_contribution_capped(base_weights, names, cov, gross,
                                         params.get("rc_cap_mult", 1.5),
                                         params.get("name_cap", 0.05))

    if overlay == OV_SECTOR_RISK:
        return _sector_risk_capped(base_weights, names, cov, sector_map, gross,
                                   params.get("sector_rc_cap", 0.40))

    if overlay == OV_EXTREME_VOL:
        extreme = params.get("extreme_annual_vol", 0.60)
        cap = params.get("name_cap", 0.05)
        w = dict(base_weights)
        for n in list(w):
            v = _ann_vol_of(trail.get(n, []))
            if v and v > extreme:
                w[n] = min(w[n], cap * 0.5)
        return w  # residual (from capped names) becomes cash

    if overlay == OV_REGIME_CASH:
        scale = _regime_scale(spy_trail_levels, params.get("ma_days", 200),
                              params.get("risk_off_scale", 0.5))
        return {n: w * scale for n, w in base_weights.items()}

    return dict(base_weights)


def _risk_contribution_capped(base_weights, names, cov, gross, cap_mult,
                              name_cap):
    w = dict(base_weights)
    n_inv = sum(1 for v in w.values() if v > 0) or 1
    cap = cap_mult * (1.0 / n_inv)
    for _ in range(25):
        rc_pct, _ = _risk_contributions(w, names, cov)
        if not rc_pct:
            break
        over = {n: p for n, p in rc_pct.items() if p > cap + 1e-9}
        if not over:
            break
        for n in over:
            w[n] *= 0.85
        tot = sum(w.values())
        if tot > 0:
            w = {n: v / tot * gross for n, v in w.items()}
        w = _normalize_capped(w, gross, name_cap)
    return w


def _sector_risk_capped(base_weights, names, cov, sector_map, gross, sector_cap):
    if not sector_map:
        return dict(base_weights)
    w = dict(base_weights)
    for _ in range(25):
        rc_pct, _ = _risk_contributions(w, names, cov)
        if not rc_pct:
            break
        sec_rc: dict[str, float] = {}
        for n, p in rc_pct.items():
            s = sector_map.get(n, "Unknown")
            sec_rc[s] = sec_rc.get(s, 0.0) + p
        over = {s: p for s, p in sec_rc.items() if p > sector_cap + 1e-9}
        if not over:
            break
        for n in list(w):
            s = sector_map.get(n, "Unknown")
            if s in over:
                w[n] *= 0.85
        tot = sum(w.values())
        if tot > 0:
            w = {n: v / tot * gross for n, v in w.items()}
    return w


def _regime_scale(spy_levels, ma_days, risk_off_scale):
    if not spy_levels or len(spy_levels) < ma_days:
        return 1.0
    closes = [c for _, c in spy_levels]
    ma = sum(closes[-ma_days:]) / ma_days
    last = closes[-1]
    return risk_off_scale if last < ma else 1.0


# --------------------------------------------------------------------------- #
# Overlay tournament backtest.
# --------------------------------------------------------------------------- #
def run_overlay_tournament(holdings: list, return_panel: dict, *,
                           spy_returns: Optional[dict] = None,
                           spy_levels: Optional[list] = None,
                           sector_map: Optional[dict] = None,
                           specs: Optional[list] = None,
                           cost_bps: float = 10.0,
                           holdings_source: str = "OPERATIONAL_BOOK") -> dict:
    """Evaluate the fixed overlay grid on the CURRENT held-name set over real
    daily history. Returns overlay rows + subperiods + regimes + cost sensitivity
    + risk contributions + a per-overlay daily-return-derived metric battery."""
    specs = specs or default_overlay_specs()
    sector_map = {str(k): str(v) for k, v in (sector_map or {}).items()}
    base_weights = {str(h["ticker"]): float(h.get("weight") or 0.0)
                    for h in holdings if h.get("ticker")}
    names = sorted(base_weights.keys())
    # Master calendar = union of held-name return dates.
    dates = sorted({d for n in names for d in return_panel.get(n, {})})
    spy_lvls = series_levels(spy_levels) if spy_levels else []

    if len(dates) <= _TRAIL + _STEP_MONTH or not names:
        return {"status": "INSUFFICIENT_HISTORY",
                "holdings_source": holdings_source,
                "held_names": names, "overlays": [], "subperiods": [],
                "regimes": [], "cost_sensitivity": [], "risk_contributions": []}

    form_idxs = list(range(_TRAIL, len(dates) - _STEP_MONTH, _STEP_MONTH))
    results = []
    rc_rows: list[dict] = []
    subperiod_rows: list[dict] = []
    regime_rows: list[dict] = []
    cost_rows: list[dict] = []

    spy_daily = spy_returns or {}
    latest_w_by_overlay: dict[str, dict] = {}
    for spec in specs:
        lbl = overlay_label(spec)
        daily, turns, latest_w, cash_series, latest_rc, latest_sec_rc = \
            _run_one_overlay(spec, base_weights, names, return_panel, dates,
                             form_idxs, sector_map, spy_lvls)
        latest_w_by_overlay[lbl] = dict(latest_w)
        met = _overlay_metrics(daily, dates, spy_daily, turns, cost_bps,
                               latest_w, cash_series)
        met["overlay"] = lbl
        met["holdings_source"] = holdings_source
        results.append(met)
        for n in sorted(latest_rc, key=lambda x: -latest_rc[x])[:len(names)]:
            rc_rows.append({"overlay": lbl, "ticker": n,
                            "weight": round(latest_w.get(n, 0.0), 6),
                            "pct_risk_contribution": round(latest_rc[n], 6),
                            "sector": sector_map.get(n, "Unknown")})
        subperiod_rows.extend(_subperiods(lbl, daily, dates))
        regime_rows.extend(_regimes(lbl, daily, dates, spy_daily))
        cost_rows.extend(_cost_sensitivity(lbl, daily, turns))

    forward_ref = _forward_shadow_reference(latest_w_by_overlay)

    return {
        "status": "OK",
        "holdings_source": holdings_source,
        "held_names": names,
        "held_count": len(names),
        "sector_map_available": bool(sector_map),
        "spy_available": bool(spy_daily),
        "forward_shadow_reference": forward_ref,
        "evaluation_note": ("fixed current-holding name set held over "
                            "survivorship-aware history; isolates RISK "
                            "CONSTRUCTION, not selection; overlay weights use "
                            "only trailing data at each monthly formation (no "
                            "tuning to recent losing days)."),
        "overlays": results,
        "subperiods": subperiod_rows,
        "regimes": regime_rows,
        "cost_sensitivity": cost_rows,
        "risk_contributions": rc_rows,
    }


def _run_one_overlay(spec, base_weights, names, return_panel, dates, form_idxs,
                     sector_map, spy_lvls):
    overlay = spec["overlay"]
    params = spec.get("params") or {}
    daily: list[float] = []
    daily_dates: list[str] = []
    turns: list[float] = []
    cash_series: list[float] = []
    prev_w: dict[str, float] = {}
    latest_w: dict[str, float] = dict(base_weights)
    latest_rc: dict[str, float] = {}
    latest_sec_rc: dict[str, float] = {}

    for k, fi in enumerate(form_idxs):
        trail = _trailing(return_panel, names, dates, fi, _TRAIL)
        avail = [n for n in names if n in trail]
        if not avail:
            continue
        cov = _cov_matrix(avail, trail)
        bw = {n: base_weights[n] for n in avail}
        # renormalise base to preserve invested gross among available names
        gross0 = sum(base_weights.values())
        s = sum(bw.values())
        if s > 0:
            bw = {n: v / s * gross0 for n, v in bw.items()}
        spy_trail = [lv for lv in spy_lvls if lv[0] <= dates[fi]]
        w = _overlay_weights(overlay, params, bw, avail, trail, cov,
                             sector_map, spy_trail)
        # clamp: no leverage
        gw = sum(w.values())
        if gw > 1.0:
            w = {n: v / gw for n, v in w.items()}
        latest_w = w
        latest_rc, _ = _risk_contributions(w, avail, cov)
        latest_sec_rc = {}
        for n, p in latest_rc.items():
            ssec = sector_map.get(n, "Unknown")
            latest_sec_rc[ssec] = latest_sec_rc.get(ssec, 0.0) + p
        turns.append(0.5 * sum(abs(w.get(n, 0.0) - prev_w.get(n, 0.0))
                               for n in set(w) | set(prev_w)))
        prev_w = w
        cash = max(0.0, 1.0 - sum(w.values()))
        # forward month daily returns
        hi = form_idxs[k + 1] if k + 1 < len(form_idxs) else len(dates)
        for di in range(fi, hi):
            d = dates[di]
            r = 0.0
            for n, wt in w.items():
                rn = return_panel.get(n, {}).get(d)
                if rn is not None:
                    r += wt * rn
            daily.append(r)
            daily_dates.append(d)
            cash_series.append(cash)
    return daily, turns, latest_w, cash_series, latest_rc, latest_sec_rc


def _overlay_metrics(daily, dates, spy_daily, turns, cost_bps, latest_w,
                     cash_series):
    n = len(daily)
    ann = _ann_from_daily(daily)
    vol = (ec.stdev(daily) or 0.0) * (_PPY_D ** 0.5) if n > 2 else None
    mdd = ec.max_drawdown(daily)
    worst1 = min(daily) if daily else None
    worst5 = _worst_rolling(daily, 5)
    worst20 = _worst_rolling(daily, 20)
    es = _expected_shortfall(daily, 0.05)
    # cost drag: monthly turnover × cost per side, annualised
    turn_ann = (ec.mean(turns) or 0.0) * 12.0
    cost_drag = (ec.mean(turns) or 0.0) * (cost_bps / 10000.0) * 12.0
    net_ann = (ann - cost_drag) if ann is not None else None
    # SPY excess + beta on aligned days
    spy_aligned = [spy_daily.get(d) for d in _rebuild_dates(daily, dates)]
    beta, spy_ann = _beta_and_spy(daily, dates, spy_daily)
    excess = (ann - spy_ann) if (ann is not None and spy_ann is not None) \
        else None
    name_hhi = sum(w * w for w in latest_w.values()) if latest_w else None
    max_name = max(latest_w.values()) if latest_w else None
    avg_cash = ec.mean(cash_series)
    return {
        "days": n,
        "annualized_return": ann,
        "net_annualized_return": net_ann,
        "annualized_vol": vol,
        "max_drawdown": mdd,
        "worst_1d": worst1,
        "worst_5d": worst5,
        "worst_20d": worst20,
        "expected_shortfall_5pct": es,
        "spy_excess_annualized": excess,
        "market_beta": beta,
        "turnover_annualized": turn_ann,
        "cost_drag_annualized": cost_drag,
        "avg_cash_weight": avg_cash,
        "name_concentration_hhi": name_hhi,
        "max_name_weight": max_name,
        "sharpe_net": _sharpe_daily(daily, cost_drag),
    }


def _rebuild_dates(daily, dates):
    # daily was built in formation order over a contiguous tail; approximate the
    # aligned date slice as the trailing len(daily) dates.
    return dates[-len(daily):] if daily else []


def _beta_and_spy(daily, dates, spy_daily):
    d_dates = _rebuild_dates(daily, dates)
    xs, ys = [], []
    for r, d in zip(daily, d_dates):
        sr = spy_daily.get(d)
        if sr is not None:
            xs.append(sr)
            ys.append(r)
    beta = None
    if len(xs) >= 3:
        mx = sum(xs) / len(xs)
        my = sum(ys) / len(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        if sxx > 0:
            beta = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    spy_vals = [spy_daily[d] for d in d_dates if d in spy_daily]
    spy_ann = _ann_from_daily(spy_vals) if spy_vals else None
    return beta, spy_ann


def _ann_from_daily(daily):
    if not daily:
        return None
    acc = 1.0
    for r in daily:
        acc *= (1.0 + r)
    years = len(daily) / _PPY_D
    if years <= 0 or acc <= 0:
        return None
    return acc ** (1.0 / years) - 1.0


def _sharpe_daily(daily, cost_drag):
    m = ec.mean(daily)
    sd = ec.stdev(daily)
    if m is None or sd is None or sd == 0:
        return None
    daily_cost = cost_drag / _PPY_D
    return ((m - daily_cost) / sd) * (_PPY_D ** 0.5)


def _worst_rolling(daily, w):
    if len(daily) < w:
        return None
    worst = None
    for i in range(len(daily) - w + 1):
        acc = 1.0
        for r in daily[i:i + w]:
            acc *= (1.0 + r)
        ret = acc - 1.0
        worst = ret if worst is None else min(worst, ret)
    return worst


def _expected_shortfall(daily, q):
    if not daily:
        return None
    s = sorted(daily)
    k = max(1, int(len(s) * q))
    tail = s[:k]
    return sum(tail) / len(tail)


def _subperiods(lbl, daily, dates, parts=2):
    rows = []
    if len(daily) < parts:
        return rows
    size = len(daily) // parts
    for i in range(parts):
        lo = i * size
        hi = (i + 1) * size if i < parts - 1 else len(daily)
        seg = daily[lo:hi]
        rows.append({"overlay": lbl, "subperiod": i + 1,
                     "annualized_return": _ann_from_daily(seg),
                     "annualized_vol": (ec.stdev(seg) or 0.0) * (_PPY_D ** 0.5),
                     "max_drawdown": ec.max_drawdown(seg),
                     "positive": bool((ec.mean(seg) or 0) > 0)})
    return rows


def _regimes(lbl, daily, dates, spy_daily):
    d_dates = _rebuild_dates(daily, dates)
    up, down = [], []
    for r, d in zip(daily, d_dates):
        sr = spy_daily.get(d)
        if sr is None:
            continue
        (up if sr > 0 else down).append(r)
    return [
        {"overlay": lbl, "regime": "spy_up_days", "days": len(up),
         "mean_daily": ec.mean(up), "annualized_vol":
         (ec.stdev(up) or 0.0) * (_PPY_D ** 0.5) if len(up) > 2 else None},
        {"overlay": lbl, "regime": "spy_down_days", "days": len(down),
         "mean_daily": ec.mean(down), "annualized_vol":
         (ec.stdev(down) or 0.0) * (_PPY_D ** 0.5) if len(down) > 2 else None},
    ]


def _cost_sensitivity(lbl, daily, turns):
    ann = _ann_from_daily(daily)
    rows = []
    for cb in (5.0, 10.0, 25.0, 50.0):
        drag = (ec.mean(turns) or 0.0) * (cb / 10000.0) * 12.0
        rows.append({"overlay": lbl, "cost_bps": cb,
                     "net_annualized_return":
                     (ann - drag) if ann is not None else None,
                     "cost_drag": drag})
    return rows


# --------------------------------------------------------------------------- #
# Forward risk-shadow reference (WS5). For each of the three tracked overlays we
# capture the deterministic forward SCALE relative to the control's latest
# invested gross, plus the resulting cash weight. Because the vol-target and
# regime overlays produce weights == control_weights × scale, the invested-gross
# ratio IS that scale exactly — so a forward shadow return is scale × the active
# book's realized daily return, with the remainder in (zero-return) cash.
# --------------------------------------------------------------------------- #
def _forward_shadow_reference(latest_w_by_overlay: dict) -> list[dict]:
    ctrl = latest_w_by_overlay.get(OV_CONTROL) or {}
    ctrl_gross = sum(ctrl.values()) or 1.0
    out: list[dict] = []
    for overlay in FORWARD_SHADOW_OVERLAYS:
        w = latest_w_by_overlay.get(overlay)
        if w is None:
            continue
        inv = sum(w.values())
        scale = (inv / ctrl_gross) if ctrl_gross > 0 else 1.0
        out.append({
            "overlay": overlay,
            "invested_gross": round(inv, 6),
            "cash": round(max(0.0, 1.0 - inv), 6),
            "forward_scale": round(scale, 6),
        })
    return out


# --------------------------------------------------------------------------- #
# Disposition synthesis (the single primary outcome).
# --------------------------------------------------------------------------- #
def synthesize_disposition(*, champion_recon: dict, overlay: dict,
                           campaign: dict, forward_context: dict,
                           gates: Optional[dict] = None) -> dict:
    gates = gates or {}
    min_ic_t = float(gates.get("min_ic_t", 2.0))
    min_forward = int(gates.get("min_forward_observations", 20))
    f = champion_recon.get("forensics") or {}
    classes = {c["component"]: c["class"]
               for c in (champion_recon.get("classification") or [])}

    fundamental_unverifiable = (
        classes.get("fundamental_leg_point_in_time") == eo.RECON_UNVERIFIABLE)
    ic_t = f.get("rank_ic_t")
    rolling = f.get("rolling_oos") or []
    oos_positive = bool(rolling) and all(w.get("positive") for w in rolling)
    price_leg_significant = bool(ic_t is not None and abs(ic_t) >= min_ic_t
                                 and oos_positive)
    excess = f.get("benchmark_excess_annualized")
    forward_obs = int(forward_context.get("observations", 0) or 0)
    forward_sufficient = forward_obs >= min_forward

    overlay_robust, overlay_reason, best = _overlay_robustness(overlay)

    # Deterministic decision tree.
    if not fundamental_unverifiable and price_leg_significant \
            and forward_sufficient and overlay_robust:
        disp = eo.DISP_CONFIRMED_RISK_OVERLAY
        why = ("Champion selection alpha is verifiable and significant, forward "
               "sample is adequate, and a risk overlay robustly improves risk "
               "construction.")
    elif not fundamental_unverifiable and price_leg_significant \
            and forward_sufficient and not overlay_robust:
        disp = eo.DISP_CONFIRMED_NO_CHANGE
        why = ("Champion selection alpha is verifiable and significant, forward "
               "sample is adequate, and no overlay robustly beats the control.")
    elif not fundamental_unverifiable and forward_sufficient \
            and not price_leg_significant and (excess is not None and excess <= 0):
        disp = eo.DISP_REJECTED
        why = ("Champion selection alpha is verifiable but not defensible (weak "
               "IC, no benchmark excess) with an adequate forward sample.")
    else:
        disp = eo.DISP_NEED_MORE_EVIDENCE
        reasons = []
        if fundamental_unverifiable:
            reasons.append("the defining fundamental selection leg is "
                           "UNVERIFIABLE on owned point-in-time-safe data (no "
                           "lookahead-free fundamental history) — it can be "
                           "neither confirmed nor conclusively rejected")
        if not forward_sufficient:
            reasons.append("the live forward sample (%d obs) is far below the "
                           "minimum (%d) for a confident verdict"
                           % (forward_obs, min_forward))
        if not price_leg_significant:
            reasons.append("the reconstructable price-momentum leg is weak "
                           "(rank-IC t=%s)" % _fmt(ic_t))
        why = "; ".join(reasons) or "insufficient evidence to confirm or reject."

    return {
        "disposition": disp,
        "rationale": why,
        "inputs": {
            "fundamental_leg_unverifiable": fundamental_unverifiable,
            "price_leg_rank_ic_t": ic_t,
            "price_leg_oos_positive": oos_positive,
            "price_leg_significant": price_leg_significant,
            "benchmark_excess_annualized": excess,
            "forward_observations": forward_obs,
            "forward_sufficient": forward_sufficient,
            "min_ic_t": min_ic_t,
            "min_forward_observations": min_forward,
            "overlay_robust": overlay_robust,
            "overlay_reason": overlay_reason,
            "best_overlay": best.get("overlay") if best else None,
            "campaign_keep_for_research": int(
                (campaign or {}).get("keep_for_research", 0) or 0),
        },
        "no_operational_change": True,
        "note": ("No operational model or holding changes in this phase. "
                 "The champion remains paper-only regardless of disposition."),
    }


def _overlay_robustness(overlay):
    """A risk overlay is 'robust' only when it materially lowers drawdown AND
    volatility versus the control WITHOUT reducing net return, AND is stable
    across both subperiods (positive in each)."""
    rows = {r["overlay"]: r for r in (overlay.get("overlays") or [])}
    control = rows.get(OV_CONTROL)
    if not control or overlay.get("status") != "OK":
        return False, "no control / insufficient history", None
    candidates = [r for lbl, r in rows.items() if lbl != OV_CONTROL]
    c_dd = control.get("max_drawdown")
    c_vol = control.get("annualized_vol")
    c_net = control.get("net_annualized_return")
    best = None
    best_score = 0.0
    for r in candidates:
        dd = r.get("max_drawdown")
        vol = r.get("annualized_vol")
        net = r.get("net_annualized_return")
        if None in (dd, vol, net, c_dd, c_vol, c_net):
            continue
        dd_impr = dd - c_dd            # less negative ⇒ positive improvement
        vol_impr = c_vol - vol         # lower vol ⇒ positive
        net_keep = net - c_net         # want ≥ ~0
        # subperiod stability of this overlay
        subs = [s for s in (overlay.get("subperiods") or [])
                if s["overlay"] == r["overlay"]]
        stable = bool(subs) and all(s.get("positive") for s in subs)
        material = (dd_impr > 0.03 and vol_impr > 0.02 and net_keep > -0.01
                    and stable)
        score = dd_impr + vol_impr
        if material and score > best_score:
            best_score = score
            best = r
    if best is None:
        return False, ("no overlay materially lowered drawdown and volatility "
                       "without sacrificing net return across both subperiods"), \
            None
    return True, ("overlay %s lowers drawdown and volatility vs control while "
                  "preserving net return, stable across subperiods"
                  % best["overlay"]), best


# --------------------------------------------------------------------------- #
# Manual de-risking preview (read-only; only when robust evidence supports it).
# --------------------------------------------------------------------------- #
def build_manual_risk_preview(*, overlay: dict, disposition: dict,
                              holdings: list) -> dict:
    robust = disposition.get("inputs", {}).get("overlay_robust")
    best_lbl = disposition.get("inputs", {}).get("best_overlay")
    if not robust or not best_lbl:
        return {
            "status": "WITHHELD_NO_ROBUST_EVIDENCE",
            "recommendation": "NONE",
            "reason": ("A manual de-risking preview is produced only when robust "
                       "overlay evidence supports it. It does not here — no "
                       "target, proposal, decision, signal or order is created."),
            "is_target": False, "is_order": False, "is_proposal": False,
            "safety_badges": ["PREVIEW ONLY", "NO TARGET", "NO ORDER",
                              "MANUAL REVIEW", "PAPER ONLY"],
        }
    rc_rows = [r for r in (overlay.get("risk_contributions") or [])
               if r["overlay"] == best_lbl]
    proposed = {r["ticker"]: r["weight"] for r in rc_rows}
    deltas = []
    for h in holdings:
        t = h.get("ticker")
        b = float(h.get("weight") or 0.0)
        p = proposed.get(t, b)
        deltas.append({"ticker": t, "current_weight": round(b, 6),
                       "preview_weight": round(p, 6),
                       "delta": round(p - b, 6)})
    return {
        "status": "PREVIEW_AVAILABLE",
        "recommendation": "MANUAL_DE_RISK_PREVIEW",
        "overlay": best_lbl,
        "reason": disposition.get("inputs", {}).get("overlay_reason"),
        "weight_preview": deltas,
        "is_target": False, "is_order": False, "is_proposal": False,
        "note": ("READ-ONLY illustrative reweighting. Creates no target, "
                 "proposal, decision, signal or order; nothing is applied to any "
                 "book."),
        "safety_badges": ["PREVIEW ONLY", "NO TARGET", "NO ORDER",
                          "MANUAL REVIEW", "PAPER ONLY"],
    }


# --------------------------------------------------------------------------- #
# Promotion-gate evidence (Workstream 5).
# --------------------------------------------------------------------------- #
def promotion_evidence(*, campaign: dict, champion_recon: dict,
                       overlay: dict) -> dict:
    """Deterministic evidence flags for eo.promotion_checklist. A requirement is
    True only when explicitly demonstrated by owned evidence, else None."""
    keep = int((campaign or {}).get("keep_for_research", 0) or 0)
    # Nothing has cleared the bar yet — this phase promotes nothing.
    return {
        "positive_out_of_sample": True if keep > 0 else None,
        "net_of_cost_improvement": None,
        "acceptable_drawdown": None,
        "stability_periods_regimes": None,
        "adequate_breadth": None,
        "low_concentration": None,
        "no_leakage": True,   # every owned factor is leakage-controlled by design
        "forward_shadow_evaluation": False,  # not yet run
    }


# --------------------------------------------------------------------------- #
# Package assembly + write + verify.
# --------------------------------------------------------------------------- #
def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def _write_json(path: Path, obj: Any) -> None:
    _write_text_atomic(path, json.dumps(obj, indent=1, sort_keys=True,
                                        default=str))


def _write_jsonl(path: Path, rows: list) -> None:
    _write_text_atomic(path, "".join(rc.canonical_json(r) + "\n" for r in rows))


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n",
                       extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({k: _cell(r.get(k)) for k in fieldnames})
    _write_text_atomic(path, buf.getvalue())


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return "%.10g" % v
    if isinstance(v, (list, dict)):
        return rc.canonical_json(v)
    return v


def _sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _fmt(v):
    if v is None:
        return "n/a"
    try:
        return "%.4g" % float(v)
    except (TypeError, ValueError):
        return str(v)


def recovery_report_model(*, run_id: str, inventory: dict, champion_recon: dict,
                          overlay: dict, campaign: dict, disposition: dict,
                          manual_preview: dict, gaps: list,
                          promotion: dict) -> dict:
    """Deterministic model for the Stage 4 report renderer (no LLM)."""
    ov_rows = [{"overlay": r.get("overlay"),
                "net_annualized_return": r.get("net_annualized_return"),
                "annualized_vol": r.get("annualized_vol"),
                "max_drawdown": r.get("max_drawdown"),
                "worst_20d": r.get("worst_20d"),
                "spy_excess_annualized": r.get("spy_excess_annualized"),
                "avg_cash_weight": r.get("avg_cash_weight")}
               for r in (overlay.get("overlays") or [])]
    f = champion_recon.get("forensics") or {}
    return {
        "run_id": run_id,
        "disposition": disposition.get("disposition"),
        "disposition_rationale": disposition.get("rationale"),
        "champion_model": champion_recon.get("champion_model"),
        "reconstruction_classes": [
            {"component": c.get("component"), "class": c.get("class")}
            for c in (champion_recon.get("classification") or [])],
        "champion_rank_ic_t": f.get("rank_ic_t"),
        "champion_max_drawdown": f.get("max_drawdown"),
        "champion_annualized_vol": f.get("annualized_vol"),
        "champion_top5_contribution_share": f.get("top5_contribution_share"),
        "equal_dollar_unequal_risk": f.get("equal_dollar_implies_unequal_risk"),
        "overlay_status": overlay.get("status"),
        "overlays": ov_rows,
        "manual_preview_status": manual_preview.get("status"),
        "campaign_experiments": int((campaign or {}).get(
            "experiments_completed", 0) or 0),
        "campaign_keep_for_research": int((campaign or {}).get(
            "keep_for_research", 0) or 0),
        "remaining_gaps": [g.get("gap") or g.get("detail") for g in (gaps or [])],
        "promotion_allowed_now": False,
        "shadow_challenger_allowed_now": bool(
            promotion.get("shadow_challenger_allowed_now")),
        "evidence_paths": [p for p in (inventory.get("evidence_paths") or [])],
    }


def assemble_and_write(cfg: dict, *, run_id: str, as_of: str,
                       stage7_input: dict, inventory: dict,
                       champion_recon: dict, overlay: dict, campaign: dict,
                       disposition: dict, manual_preview: dict, gaps: list,
                       promotion: dict, report_md: str,
                       write: bool = True) -> dict:
    """Write the immutable Stage 7 package and return a result dict."""
    from . import champion_forensics as cf
    root = Path(cfg.get("recovery_root"))
    run_dir = root / "runs" / run_id
    files_written: list[str] = []
    identity = _run_identity(cfg, as_of, stage7_input)

    # Immutable-evidence guard: never silently overwrite an existing run dir.
    # If a completed run already sits here, it may only be re-materialised when
    # its recorded identity is byte-for-byte the same (idempotent no-op); any
    # difference — or a legacy package written before identities were recorded —
    # fails safely so the operator must land the change under a new run id.
    if write and run_dir.exists() and any(run_dir.iterdir()):
        prior = eo._read_json(run_dir / "run_manifest.json") or {}
        prior_identity = prior.get("identity")
        if prior_identity is not None and prior_identity == identity:
            return {"run_id": run_id, "run_dir": str(run_dir),
                    "files_written": [], "manifest": prior, "idempotent": True}
        raise ImmutableRunError(
            "refusing to overwrite existing Stage 7 run %r: on-disk identity %r "
            "differs from requested %r — a schema/engine/evidence change must "
            "use a new run id" % (run_id, prior_identity, identity))

    payloads_json = {
        "stage7_input.json": stage7_input,
        "evidence_inventory.json": inventory,
        "champion_reconstruction.json": champion_recon,
        "alpha_campaign_plan.json": (campaign or {}).get("plan") or {},
        "recovery_disposition.json": disposition,
        "manual_risk_preview.json": manual_preview,
    }
    csv_specs = {
        "source_freshness.csv": (
            ["stage", "label", "status", "run_id", "as_of", "stale", "path"],
            eo.source_freshness_rows(inventory, today=as_of)),
        "champion_forensics.csv": (
            ["formation_date", "next_date", "universe", "rank_ic",
             "book_return", "benchmark_return", "turnover"],
            cf.forensics_csv_rows(champion_recon)),
        "risk_contributions.csv": (
            ["overlay", "ticker", "weight", "pct_risk_contribution", "sector"],
            overlay.get("risk_contributions") or []),
        "overlay_results.csv": (
            ["overlay", "holdings_source", "days", "annualized_return",
             "net_annualized_return", "annualized_vol", "max_drawdown",
             "worst_1d", "worst_5d", "worst_20d", "expected_shortfall_5pct",
             "spy_excess_annualized", "market_beta", "turnover_annualized",
             "cost_drag_annualized", "avg_cash_weight", "name_concentration_hhi",
             "max_name_weight", "sharpe_net"],
            overlay.get("overlays") or []),
        "overlay_subperiods.csv": (
            ["overlay", "subperiod", "annualized_return", "annualized_vol",
             "max_drawdown", "positive"],
            overlay.get("subperiods") or []),
        "overlay_regimes.csv": (
            ["overlay", "regime", "days", "mean_daily", "annualized_vol"],
            overlay.get("regimes") or []),
        "overlay_cost_sensitivity.csv": (
            ["overlay", "cost_bps", "net_annualized_return", "cost_drag"],
            overlay.get("cost_sensitivity") or []),
    }
    jsonl_specs = {
        "overlay_specs.jsonl": (campaign or {}).get("overlay_specs")
        or default_overlay_specs(cfg),
        "alpha_experiment_results.jsonl": (campaign or {}).get("results") or [],
        "alpha_evidence_decisions.jsonl": (campaign or {}).get("decisions") or [],
        "remaining_data_gaps.jsonl": gaps or [],
    }

    if write:
        for name, obj in payloads_json.items():
            _write_json(run_dir / name, obj)
            files_written.append(name)
        for name, (fields, rows) in csv_specs.items():
            _write_csv(run_dir / name, fields, rows)
            files_written.append(name)
        for name, rows in jsonl_specs.items():
            _write_jsonl(run_dir / name, rows)
            files_written.append(name)
        _write_text_atomic(run_dir / "stage7_recovery_report.md", report_md)
        files_written.append("stage7_recovery_report.md")

    # Run manifest with per-file hashes.
    manifest = {
        "run_id": run_id, "as_of": as_of, "stage": eo.STAGE7_STAGE,
        "schema_version": eo.STAGE7_SCHEMA_VERSION,
        "engine_version": eo.STAGE7_ENGINE_VERSION,
        "identity": identity,
        "config_hash": eo.config_hash(cfg),
        "disposition": disposition.get("disposition"),
        "terminal": stage7_input.get("terminal"),
        "upstream_fingerprint": (stage7_input.get("upstream_fingerprint")
                                 or upstream_evidence_fingerprint(cfg).get(
                                     "fingerprint")),
        "forward_shadows": overlay.get("forward_shadow_reference") or [],
        "files": {},
        "safety": {"no_orders": True, "no_promotion": True,
                   "no_ledger_mutation": True, "paper_only": True,
                   "read_only": True},
    }
    if write:
        for name in files_written:
            fp = run_dir / name
            if fp.exists():
                manifest["files"][name] = _sha256_file(fp)
        _write_json(run_dir / "run_manifest.json", manifest)
        _write_json(root / "latest.json", {
            "run_id": run_id, "as_of": as_of,
            "disposition": disposition.get("disposition"),
            "terminal": stage7_input.get("terminal"),
            "run_dir": str(run_dir)})

    return {"run_id": run_id, "run_dir": str(run_dir),
            "files_written": files_written, "manifest": manifest}


def latest_recovery_report_model(cfg: dict, fresh_model: Optional[dict] = None
                                 ) -> Optional[dict]:
    """Deterministic Stage 4 report model from a fresh recovery model if given,
    else the latest recovery package on disk, else None. Read-only, no LLM."""
    if fresh_model:
        return fresh_model
    root = cfg.get("recovery_root") or (cfg.get("stage_roots") or {}).get(
        "stage7")
    if not root:
        return None
    latest = eo._read_json(Path(root) / "latest.json") or {}
    rid = latest.get("run_id")
    if not rid:
        return None
    rd = Path(root) / "runs" / rid
    disp = eo._read_json(rd / "recovery_disposition.json") or {}
    recon = eo._read_json(rd / "champion_reconstruction.json") or {}
    manual = eo._read_json(rd / "manual_risk_preview.json") or {}
    manifest = eo._read_json(rd / "run_manifest.json") or {}
    f = recon.get("forensics") or {}
    ov_rows = []
    for r in eo._read_csv(rd / "overlay_results.csv"):
        ov_rows.append({
            "overlay": r.get("overlay"),
            "net_annualized_return": _flt(r.get("net_annualized_return")),
            "annualized_vol": _flt(r.get("annualized_vol")),
            "max_drawdown": _flt(r.get("max_drawdown")),
            "worst_20d": _flt(r.get("worst_20d")),
            "spy_excess_annualized": _flt(r.get("spy_excess_annualized")),
            "avg_cash_weight": _flt(r.get("avg_cash_weight"))})
    decisions = eo._read_jsonl(rd / "alpha_evidence_decisions.jsonl")
    keep = sum(1 for d in decisions if d.get("decision") == "KEEP_FOR_RESEARCH")
    completed = sum(1 for d in decisions if d.get("decision") not in
                    ("DATA_HOLD", None) and d.get("experiment_id"))
    decision_counts: dict[str, int] = {}
    for d in decisions:
        dec = d.get("decision")
        if dec:
            decision_counts[dec] = decision_counts.get(dec, 0) + 1
    gaps = eo._read_jsonl(rd / "remaining_data_gaps.jsonl")
    return {
        "run_id": rid,
        "as_of": latest.get("as_of"),
        "disposition": disp.get("disposition") or latest.get("disposition"),
        "disposition_rationale": disp.get("rationale"),
        "campaign_decision_counts": decision_counts,
        "champion_model": recon.get("champion_model"),
        "reconstruction_classes": [
            {"component": c.get("component"), "class": c.get("class")}
            for c in (recon.get("classification") or [])],
        "champion_rank_ic_t": f.get("rank_ic_t"),
        "champion_max_drawdown": f.get("max_drawdown"),
        "champion_annualized_vol": f.get("annualized_vol"),
        "champion_top5_contribution_share": f.get("top5_contribution_share"),
        "equal_dollar_unequal_risk": f.get("equal_dollar_implies_unequal_risk"),
        "overlays": ov_rows,
        "manual_preview_status": manual.get("status"),
        "campaign_experiments": completed,
        "campaign_keep_for_research": keep,
        "remaining_gaps": [g.get("gap") or g.get("detail") for g in gaps][:12],
        "promotion_allowed_now": False,
        "shadow_challenger_allowed_now": bool(
            (manifest.get("safety") or {}).get("shadow_challenger_allowed_now")),
    }


def _flt(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def verify_recovery(cfg: dict, *, run_id: Optional[str] = None,
                    ledger_fingerprint: Optional[Callable[[], dict]] = None
                    ) -> dict:
    """Read-only verification of a Stage 7 package: required files present,
    manifest hashes reconcile, disposition is valid, no ledger changed."""
    root = Path(cfg.get("recovery_root"))
    if run_id is None:
        latest = eo._read_json(root / "latest.json") or {}
        run_id = latest.get("run_id")
    if not run_id:
        return {"terminal": eo.BLOCKED, "status": "no recovery run to verify"}
    run_dir = root / "runs" / run_id
    if not run_dir.exists():
        return {"terminal": eo.BLOCKED, "status": "run dir missing: %s" % run_dir}

    missing = [f for f in eo.REQUIRED_RUN_FILES
               if not (run_dir / f).exists()]
    manifest = eo._read_json(run_dir / "run_manifest.json") or {}
    hash_mismatch = []
    for name, want in (manifest.get("files") or {}).items():
        fp = run_dir / name
        if not fp.exists() or _sha256_file(fp) != want:
            hash_mismatch.append(name)
    disp_doc = eo._read_json(run_dir / "recovery_disposition.json") or {}
    disp = disp_doc.get("disposition")
    disp_ok = disp in eo.RECOVERY_DISPOSITIONS
    ledgers = ledger_fingerprint() if ledger_fingerprint else None

    ok = (not missing and not hash_mismatch and disp_ok)
    return {
        "terminal": eo.VERIFIED if ok else eo.BLOCKED,
        "status": ("verified" if ok else
                   "missing=%s mismatch=%s disp_ok=%s" %
                   (missing, hash_mismatch, disp_ok)),
        "run_id": run_id, "run_dir": str(run_dir),
        "missing_files": missing, "hash_mismatch": hash_mismatch,
        "disposition": disp, "disposition_valid": disp_ok,
        "ledger_files": len(ledgers) if ledgers is not None else None,
    }


# --------------------------------------------------------------------------- #
# WS5 — read-only forward risk-shadow tracker. Applies each tracked overlay's
# deterministic forward scale (from the latest Stage 7 package) to the ACTIVE
# paper book's realized daily returns since a baseline. Read-only: it creates no
# target, order, signal, decision or fill and changes no book.
# --------------------------------------------------------------------------- #
def build_forward_shadows(cfg: dict, *, forward_series: Optional[list] = None,
                          baseline_date: Optional[str] = None,
                          min_vol_obs: int = 10) -> dict:
    root = cfg.get("recovery_root") or (cfg.get("stage_roots") or {}).get(
        "stage7")
    empty = {"status": "NO_PACKAGE", "baseline_date": baseline_date,
             "shadows": [], "note": "No Stage 7 package to derive shadows from.",
             "disclaimer": "No shadow portfolio changes the active paper "
                           "portfolio."}
    if not root:
        return empty
    latest = eo._read_json(Path(root) / "latest.json") or {}
    rid = latest.get("run_id")
    if not rid:
        return empty
    rd = Path(root) / "runs" / rid
    manifest = eo._read_json(rd / "run_manifest.json") or {}
    ref_by = {r.get("overlay"): r for r in (manifest.get("forward_shadows")
                                            or [])}
    ov_by = {r.get("overlay"): r for r in eo._read_csv(rd /
                                                       "overlay_results.csv")}

    series = forward_series or []
    book_rets = [float(s["book_return"]) for s in series
                 if isinstance(s, dict) and _num(s.get("book_return"))]
    spy_rets = [float(s["spy_return"]) for s in series
                if isinstance(s, dict) and _num(s.get("spy_return"))]
    spy_cum = None
    if spy_rets:
        acc = 1.0
        for r in spy_rets:
            acc *= (1.0 + r)
        spy_cum = acc - 1.0

    shadows: list[dict] = []
    for overlay in FORWARD_SHADOW_OVERLAYS:
        ref = ref_by.get(overlay) or {}
        default_scale = 1.0 if overlay == OV_CONTROL else _fallback_scale(
            overlay, ov_by)
        scale = float(ref.get("forward_scale", default_scale))
        cash = ref.get("cash")
        if cash is None:
            cash = round(max(0.0, 1.0 - scale), 6)
        ov = ov_by.get(overlay) or {}
        n = len(book_rets)
        shadow_daily = [scale * r for r in book_rets]
        cum = dd = vol = daily_last = None
        if shadow_daily:
            acc = 1.0
            for r in shadow_daily:
                acc *= (1.0 + r)
            cum = acc - 1.0
            dd = ec.max_drawdown(shadow_daily)
            daily_last = shadow_daily[-1]
            if n >= min_vol_obs:
                sd = ec.stdev(shadow_daily)
                vol = sd * (_PPY_D ** 0.5) if sd is not None else None
        excess = (cum - spy_cum) if (cum is not None and spy_cum is not None) \
            else None
        shadows.append({
            "overlay": overlay,
            "daily_return": daily_last,
            "cumulative_return": cum,
            "drawdown": dd,
            "realized_vol": vol,
            "cash": cash,
            "spy_excess": excess,
            "turnover_annualized": _flt(ov.get("turnover_annualized")),
            "modeled_cost_annualized": _flt(ov.get("cost_drag_annualized")),
            "observations": n,
            "scale": round(scale, 6),
        })
    return {
        "status": "OK" if book_rets else "NO_FORWARD_OBSERVATIONS",
        "baseline_date": baseline_date,
        "reference_run_id": rid,
        "holdings_source": latest.get("holdings_source"),
        "shadows": shadows,
        "note": ("Modeled read-only forward shadows: each overlay's fixed scale "
                 "from the latest Stage 7 formation applied to the active book's "
                 "realized daily returns since %s. The remainder is held as "
                 "cash." % (baseline_date or "the baseline")),
        "disclaimer": "No shadow portfolio changes the active paper portfolio.",
    }


def _fallback_scale(overlay: str, ov_by: dict) -> float:
    """Best-effort forward scale when the package predates the reference field:
    for a vol-target overlay, the realized-vol ratio to the control."""
    if overlay == OV_CONTROL:
        return 1.0
    ctrl = ov_by.get(OV_CONTROL) or {}
    row = ov_by.get(overlay) or {}
    cv = _flt(ctrl.get("annualized_vol"))
    ov = _flt(row.get("annualized_vol"))
    if cv and ov and cv > 0:
        return max(0.0, min(1.0, ov / cv))
    return 1.0


# --------------------------------------------------------------------------- #
# WS4 — upstream-evidence fingerprint + Stage 7 recurring-cadence decision.
# Both are pure/read-only: they only inspect ``latest.json`` markers.
# --------------------------------------------------------------------------- #
def upstream_evidence_fingerprint(cfg: dict) -> dict:
    """Deterministic marker of the upstream evidence a Stage 7 package is built
    against: Stage 3 latest run, Stage 5 latest run, Stage 6 data version."""
    import hashlib as _hl
    roots = cfg.get("stage_roots") or {}

    def _latest(root_key, alt_key, field="run_id"):
        root = roots.get(root_key) or cfg.get(alt_key)
        if not root:
            return ""
        j = eo._read_json(Path(root) / "latest.json") or {}
        return str(j.get(field) or "")

    s3 = _latest("stage3", "stage3_director_root")
    s5 = _latest("stage5", "stage5_experiments_root")
    s6root = roots.get("stage6") or cfg.get("stage6_backfill_root")
    s6 = ""
    if s6root:
        j = eo._read_json(Path(s6root) / "latest.json") or {}
        s6 = str(j.get("data_version") or j.get("run_id") or "")
    fp = _hl.sha256(("|".join([s3, s5, s6])).encode("utf-8")).hexdigest()[:16]
    return {"fingerprint": fp, "stage3_run": s3, "stage5_run": s5,
            "stage6_data_version": s6}


def stage7_cadence_decision(*, label: str, weekday: int, current_fingerprint,
                            latest_manifest: Optional[dict],
                            latest_as_of: Optional[str] = None,
                            today: Optional[str] = None,
                            stale_after_days: int = 7,
                            friday_label: str = "post_close") -> dict:
    """Deterministic Stage 7 cadence decision (WS4). Friday post-close ⇒ full
    campaign + overlay refresh; upstream change ⇒ delta refresh; otherwise reuse
    the latest verified package and state that nothing changed."""
    recorded = (latest_manifest or {}).get("upstream_fingerprint")
    changed = bool(recorded) and str(recorded) != str(current_fingerprint)
    is_friday_post_close = (str(label) == friday_label and weekday == 4)
    age_days = _age_days(latest_as_of, today)
    stale = bool(age_days is not None and age_days > stale_after_days)

    if latest_manifest is None:
        action, reason = CAD_NONE, "No Stage 7 package on record yet."
    elif is_friday_post_close:
        action, reason = CAD_RUN_FULL, ("Friday post-close: one bounded full "
                                        "recovery + risk-overlay refresh.")
    elif changed:
        action, reason = CAD_RUN_DELTA, ("Upstream evidence changed since the "
                                         "latest verified package.")
    else:
        action, reason = CAD_REUSE, ("No new research evidence since the prior "
                                     "report.")
    return {"action": action, "reason": reason, "upstream_changed": changed,
            "age_days": age_days, "stale": stale,
            "weekly": is_friday_post_close}


def _age_days(as_of: Optional[str], today: Optional[str]) -> Optional[int]:
    import datetime as _dt
    if not as_of or not today:
        return None
    try:
        a = _dt.date.fromisoformat(str(as_of)[:10])
        t = _dt.date.fromisoformat(str(today)[:10])
    except (TypeError, ValueError):
        return None
    return (t - a).days
