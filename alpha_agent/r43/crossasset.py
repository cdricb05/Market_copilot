"""alpha_agent.r43.crossasset - Tracks H, I and J.

**Track H, event-driven.** Release 43 acquired 2,916 real scheduled macro
release DATES from the owned FRED entitlement (1996 -> 2026, eight release
types). That is a genuine point-in-time event calendar - the estate had none
before - and it makes one question answerable: around a scheduled release,
does a relative-value structure OVER-react and revert, or UNDER-react and
continue? The book trades only in the window after a release and is flat
otherwise, and the identical rule applied to NON-event days is carried
alongside as a placebo. If the effect is not specific to events, the placebo
says so.

**Track I, cross-asset.** Twelve sparse, economically motivated relations,
every one declared with its direction BEFORE it was measured, aggregated
into a single equal-risk relational portfolio. This is deliberately not an
N-squared search: the contract forbids uncontrolled fishing, and a
relationship only matters if it converts into a tradeable expression, so
each relation's target is a futures market the estate can actually trade.
Credit enters here through the owned ICE BofA OAS family, which is real
credit information rather than an ETF proxy - and, being an index level, is
signal-only, which is exactly why the tradeable leg is an equity or rates
future and never HYG.

**Track J, technical structure.** Named Fibonacci retracement levels are
admissible and are tested against the PREDECLARED placebo levels in the
frozen contract, on CAUSAL pivots only: a swing extreme is confirmed only
after the confirmation window has passed, so no level is ever drawn from an
extreme the rule could not yet have seen. There is no human chart
confirmation and no post-hoc level selection.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import burden as B
from . import judge as J
from . import panels as P
from . import acquisition as AQ
from .rv import apply_expression
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.crossasset"

EMBARGO = 21
VOL_WIN = 60
Z_WIN = 252

# --------------------------------------------------------------------------- #
# Track I - the predeclared relation set
# --------------------------------------------------------------------------- #
#: (name, source kind, source key, lag-days of the source change, target
#:  futures market, declared sign, economic story)
#: sign +1 means "source change UP -> go LONG the target".
RELATIONS = [
    ("VOL_TERM_TO_EQUITY", "CBOE", "VIX_TERM", 5, "ES", +1,
     "the VIX curve steepens into contango -> the market is calm -> risk on"),
    ("CURVE_TO_EQUITY", "FRED", "CMT_2s10s", 5, "ES", +1,
     "the Treasury curve steepens -> growth expectations improve"),
    ("CURVE_TO_SMALLCAP", "FRED", "CMT_2s10s", 5, "RTY", +1,
     "a steeper curve helps the lenders and borrowers small caps depend on"),
    ("USD_TO_GOLD", "MARKET", "DX", 5, "GC", -1,
     "the dollar strengthens -> the gold price in dollars falls"),
    ("USD_TO_OIL", "MARKET", "DX", 5, "CL", -1,
     "the dollar strengthens -> dollar-denominated crude falls"),
    ("USD_TO_EM_EQUITY", "MARKET", "DX", 5, "EMD", -1,
     "a stronger dollar tightens global financial conditions"),
    ("BREAKEVEN_TO_OIL", "FRED", "BE_5Y", 5, "CL", +1,
     "inflation expectations rise -> the commodity that drives them bids"),
    ("BREAKEVEN_TO_DURATION", "FRED", "BE_5Y", 5, "ZN", -1,
     "inflation expectations rise -> nominal Treasuries sell off"),
    ("BREAKEVEN_TO_GOLD", "FRED", "BE_5Y", 5, "GC", +1,
     "gold is the classic inflation-expectation hedge"),
    ("OIL_TO_CAD", "MARKET", "CL", 5, "6C", +1,
     "crude rallies -> the Canadian dollar, an oil currency, strengthens"),
    ("VOL_SHOCK_TO_EQUITY", "CBOE", "VIX", 5, "ES", -1,
     "implied volatility spikes -> the equity index falls"),
    ("VOL_SHOCK_TO_DURATION", "CBOE", "VIX", 5, "ZN", +1,
     "a volatility spike bids duration"),
]

#: Credit relations are declared with the same discipline but are held in a
#: SEPARATE tier because the owned credit history is three years long, not
#: three decades. ICE restricted FRED redistribution in April 2026 - the
#: series' own metadata says "Starting in April 2026, this series will only
#: include 3 years of observations" - so these can be measured but can never
#: be a Release-43 candidate: three years cannot carry a 50/30/20 split with
#: a judged zone above the contract's minimum. They are reported, labelled,
#: and excluded from the candidate portfolio.
CREDIT_RELATIONS = [
    ("CREDIT_STRESS_TO_EQUITY", "FRED", "OAS_HY", 5, "ES", -1,
     "high-yield spreads widen -> the equity index falls"),
    ("CREDIT_STRESS_TO_DURATION", "FRED", "OAS_HY", 5, "ZN", +1,
     "high-yield spreads widen -> flight to quality bids Treasuries"),
    ("CREDIT_QUALITY_TO_SMALLCAP", "FRED", "OAS_SPREAD_HY_IG", 5, "RTY", -1,
     "the quality spread widens -> small caps, which carry the weakest "
     "balance sheets, underperform"),
    ("EM_CREDIT_TO_MXN", "FRED", "OAS_EM", 5, "6M", -1,
     "EM credit stress -> the peso weakens"),
    ("CREDIT_TERM_TO_DURATION", "FRED", "OAS_TERM_15P_13", 5, "ZB", -1,
     "the credit curve steepens -> long duration risk is being repriced"),
]
CREDIT_MIN_YEARS_FOR_CANDIDACY = 8.0

EXPRESSIONS = {"CONTINUOUS": None, "BAND_15_05": (1.5, 0.5)}


def _fred_source(key: str) -> pd.Series:
    """Prefer the R43-deepened panel; fall back to R41's. Neither is written."""
    fp = AQ.load_macro_panel()
    if fp is None:
        fp = P.fred_panel()
    if key == "OAS_SPREAD_HY_IG":
        if "OAS_HY" not in fp.columns or "OAS_IG" not in fp.columns:
            return None
        return (fp["OAS_HY"] - fp["OAS_IG"]).rename(key)
    if key == "OAS_TERM_15P_13":
        if "OAS_IG_15P" not in fp.columns or "OAS_IG_1_3" not in fp.columns:
            return None
        return (fp["OAS_IG_15P"] - fp["OAS_IG_1_3"]).rename(key)
    if key == "CMT_2s10s":
        return (fp["CMT_10Y"] - fp["CMT_2Y"]).rename(key)
    if key not in fp.columns:
        return None
    return fp[key].rename(key)


def _cboe_source(key: str) -> pd.Series:
    cb = P.cboe_panel()
    if key == "VIX_TERM":
        if "VIX3M" not in cb.columns or "VIX" not in cb.columns:
            return None
        return (cb["VIX3M"] / cb["VIX"].replace(0.0, np.nan)).rename(key)
    if key not in cb.columns:
        return None
    return cb[key].rename(key)


def relation_signal(rel) -> pd.Series:
    """The standardised source CHANGE, observable at t."""
    name, kind, key, lag, target, sign, story = rel
    if kind == "FRED":
        s = _fred_source(key)
    elif kind == "CBOE":
        s = _cboe_source(key)
    else:
        d = P.futures_daily(key)
        s = (pd.to_numeric(d["ret1"], errors="coerce").add(1.0).cumprod()
             if d is not None else None)
    if s is None:
        return None
    s = pd.to_numeric(s, errors="coerce").dropna()
    chg = s.diff(lag)
    return P.zscore(chg, Z_WIN) * float(sign)


def relation_book(rels, expression: str, *, extra_lag: int = 0,
                  frame_override: pd.DataFrame = None,
                  min_obs: int = 750) -> dict:
    gross, cost, turn, used = None, None, None, []
    caps = []
    reg = P.market_registry()
    frame = frame_override
    if frame is None:
        cols = {}
        for rel in rels:
            z = relation_signal(rel)
            if z is not None:
                cols[rel[0]] = z
        if not cols:
            return None
        frame = pd.DataFrame(cols).sort_index()
    for rel in rels:
        name, kind, key, lag, target, sign, story = rel
        if name not in frame.columns:
            continue
        d = P.futures_daily(target)
        if d is None:
            continue
        ret = pd.to_numeric(d["ret1"], errors="coerce")
        idx = ret.dropna().index.intersection(frame[name].dropna().index)
        if len(idx) < min_obs:
            continue
        pos = apply_expression(frame[name].reindex(idx), expression,
                               extra_lag=extra_lag)
        vol = ret.reindex(idx).rolling(VOL_WIN, min_periods=VOL_WIN // 2) \
            .std().shift(1)
        scale = (vol.expanding(min_periods=250).median()
                 / vol.replace(0.0, np.nan)).clip(0.25, 4.0).fillna(1.0)
        held = pos * scale
        pnl = (held * ret.reindex(idx)).fillna(0.0)
        g = (reg.get(target) or {}).get("cost_group") or "COMMODITY_INDEX"
        bps = C.COST_BPS_PER_SIDE.get(g)
        if not isinstance(bps, (int, float)):
            bps = max(v for v in C.COST_BPS_PER_SIDE.values()
                      if isinstance(v, (int, float)))
        to = held.diff().abs().fillna(0.0)
        c = to * (float(bps) / 1e4)
        frac = C.FUTURES_MARGIN_FRACTION.get(
            g, max(C.FUTURES_MARGIN_FRACTION.values()))
        caps.append(C.MARGIN_STRESS_BUFFER_MULTIPLIER * float(frac))
        gross = pnl if gross is None else gross.add(pnl, fill_value=0.0)
        cost = c if cost is None else cost.add(c, fill_value=0.0)
        turn = to if turn is None else turn.add(to, fill_value=0.0)
        used.append({"relation": name, "target": target, "sign": sign,
                     "story": story, "n_obs": int(len(idx))})
    if gross is None or not used:
        return None
    n = float(len(used))
    return {"gross": (gross / n).rename("gross"),
            "cost": (cost.reindex(gross.index).fillna(0.0) / n)
            .rename("cost"),
            "turnover": (turn.reindex(gross.index).fillna(0.0) / n),
            "committed_capital": max(float(np.mean(caps)),
                                     C.MARGIN_FLOOR_FRACTION_OF_GROSS),
            "index": gross.index, "relations_used": used,
            "n_relations": len(used)}


# --------------------------------------------------------------------------- #
# Track H - event-conditioned relative value
# --------------------------------------------------------------------------- #
EVENT_RULES = ("REVERSAL", "CONTINUATION")
EVENT_HORIZONS = (5, 21)


def event_book(structures: list, calendar: pd.DataFrame, rule: str,
               horizon: int, *, event_days: bool = True,
               shock_z: float = 1.5) -> dict:
    """Trade ONLY in the window after a scheduled release.

    On a release date the structure's OWN move that day is standardised. If
    it exceeds ``shock_z``, a position is opened at that day's close - so the
    signal is the completed session, never the session being traded - and
    held for ``horizon`` sessions. ``event_days=False`` runs the identical
    rule on NON-release days and is the placebo.
    """
    if calendar is None or calendar.empty:
        return None
    ev = pd.DatetimeIndex(sorted(set(calendar["date"])))
    gross, cost, turn, used, caps = None, None, None, [], []
    for s in structures:
        idx = s["index"]
        r = s["spread_ret"].reindex(idx)
        z = P.zscore(r, Z_WIN)
        is_ev = pd.Series(idx.isin(ev), index=idx)
        trigger = (z.abs() >= shock_z) & (is_ev if event_days else ~is_ev)
        direction = -np.sign(z) if rule == "REVERSAL" else np.sign(z)
        raw = (direction.where(trigger, 0.0)).fillna(0.0)
        # Hold for `horizon` sessions; the position is entered on the CLOSE
        # of the signal day, so it earns from the NEXT session onward.
        held = raw.rolling(horizon, min_periods=1).sum().shift(1).fillna(0.0)
        held = held.clip(-3.0, 3.0)
        if held.abs().sum() < 10:
            continue
        vol = r.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
        scale = (vol.expanding(min_periods=250).median()
                 / vol.replace(0.0, np.nan)).clip(0.25, 4.0).fillna(1.0)
        held = held * scale
        pnl = (held * r).fillna(0.0)
        to = held.diff().abs().fillna(0.0)
        g1, g2 = s["leg_groups"]
        b1 = C.COST_BPS_PER_SIDE.get(g1, 5.0)
        b2 = C.COST_BPS_PER_SIDE.get(g2, 5.0)
        b1 = float(b1) if isinstance(b1, (int, float)) else 5.0
        b2 = float(b2) if isinstance(b2, (int, float)) else 5.0
        c = to * ((b1 + b2) / 1e4)
        caps.append(J.futures_committed_capital(
            s["leg_groups"], (1.0, 1.0))["committed_capital"])
        gross = pnl if gross is None else gross.add(pnl, fill_value=0.0)
        cost = c if cost is None else cost.add(c, fill_value=0.0)
        turn = to if turn is None else turn.add(to, fill_value=0.0)
        used.append(s["name"])
    if gross is None or not used:
        return None
    n = float(len(used))
    return {"gross": (gross / n).rename("gross"),
            "cost": (cost.reindex(gross.index).fillna(0.0) / n)
            .rename("cost"),
            "turnover": (turn.reindex(gross.index).fillna(0.0) / n),
            "committed_capital": max(float(np.mean(caps)),
                                     C.MARGIN_FLOOR_FRACTION_OF_GROSS),
            "index": gross.index, "structures_used": used,
            "n_structures": len(used)}


# --------------------------------------------------------------------------- #
# Track J - causal pivots and predeclared placebo levels
# --------------------------------------------------------------------------- #
CONFIRM = 10            # a swing extreme is confirmed only after 10 sessions
TREND_WIN = 63
TOUCH_TOL = 0.0025      # 25 bp band around a level counts as a touch
J_MARKETS = ("ES", "ZN", "CL", "GC", "6E", "NQ", "ZB", "SI")
J_HORIZONS = (5, 21)


def _causal_pivots(px: pd.Series, confirm: int = CONFIRM):
    """Swing highs/lows confirmed ONLY after ``confirm`` later sessions.

    The pivot at index i is reported at index i + confirm, never earlier, so
    a rule reading this series can never use an extreme it could not yet
    have known was an extreme. This is the contract's
    PIVOT_CONFIRMATION_RULE in code.
    """
    v = px.to_numpy(dtype=float)
    n = v.size
    hi = np.full(n, np.nan)
    lo = np.full(n, np.nan)
    for i in range(confirm, n - confirm):
        w = v[i - confirm: i + confirm + 1]
        if not np.isfinite(v[i]):
            continue
        if v[i] == np.nanmax(w):
            if i + confirm < n:
                hi[i + confirm] = v[i]
        if v[i] == np.nanmin(w):
            if i + confirm < n:
                lo[i + confirm] = v[i]
    return (pd.Series(hi, index=px.index).ffill(),
            pd.Series(lo, index=px.index).ffill())


def fib_book(markets, levels, horizon: int, *, tol: float = TOUCH_TOL
             ) -> dict:
    """Retracement-continuation book on CAUSAL pivots at the given levels."""
    reg = P.market_registry()
    gross, cost, turn, used, caps = None, None, None, [], []
    for m in markets:
        d = P.futures_daily(m)
        if d is None or "ret1" not in d.columns:
            continue
        ret = pd.to_numeric(d["ret1"], errors="coerce").dropna()
        if len(ret) < 1500:
            continue
        px = (1.0 + ret).cumprod()
        hi, lo = _causal_pivots(px)
        span = (hi - lo)
        ok = span > 0
        trend = np.sign(px.pct_change(TREND_WIN))
        pos = pd.Series(0.0, index=px.index)
        for lv in levels:
            # Retracement level measured DOWN from the confirmed swing high.
            level = hi - span * float(lv)
            touch = ok & ((px - level).abs() / px.abs() <= tol)
            pos = pos + (touch.astype(float) * trend.fillna(0.0))
        raw = np.sign(pos).fillna(0.0)
        held = raw.rolling(horizon, min_periods=1).sum().shift(1) \
            .fillna(0.0).clip(-2.0, 2.0)
        if held.abs().sum() < 10:
            continue
        vol = ret.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
        scale = (vol.expanding(min_periods=250).median()
                 / vol.replace(0.0, np.nan)).clip(0.25, 4.0).fillna(1.0)
        held = held * scale
        pnl = (held * ret).fillna(0.0)
        to = held.diff().abs().fillna(0.0)
        g = (reg.get(m) or {}).get("cost_group") or "COMMODITY_INDEX"
        bps = C.COST_BPS_PER_SIDE.get(g, 5.0)
        bps = float(bps) if isinstance(bps, (int, float)) else 5.0
        c = to * (bps / 1e4)
        caps.append(C.MARGIN_STRESS_BUFFER_MULTIPLIER
                    * float(C.FUTURES_MARGIN_FRACTION.get(g, 0.1)))
        gross = pnl if gross is None else gross.add(pnl, fill_value=0.0)
        cost = c if cost is None else cost.add(c, fill_value=0.0)
        turn = to if turn is None else turn.add(to, fill_value=0.0)
        used.append(m)
    if gross is None or not used:
        return None
    n = float(len(used))
    return {"gross": (gross / n).rename("gross"),
            "cost": (cost.reindex(gross.index).fillna(0.0) / n)
            .rename("cost"),
            "turnover": (turn.reindex(gross.index).fillna(0.0) / n),
            "committed_capital": max(float(np.mean(caps)),
                                     C.MARGIN_FLOOR_FRACTION_OF_GROSS),
            "index": gross.index, "markets_used": used}


# --------------------------------------------------------------------------- #
# Shared scoring
# --------------------------------------------------------------------------- #
def score(bk: dict, dates=None, *, capital: float = None,
          overlap: int = 1) -> dict:
    K = float(capital if capital is not None else bk["committed_capital"])
    b = J.implementable_book(bk["gross"], pd.Series(1.0, index=bk["index"]),
                             committed_capital=K,
                             collateral_class="REMUNERATED_MARGIN",
                             cost=bk["cost"], day_count=J.TRADING_DAYS)
    card = J.score(b, dates, overlap=overlap, day_count=J.TRADING_DAYS)
    d = b if dates is None else b.reindex(pd.DatetimeIndex(dates))
    card["turnover_ann"] = float(
        np.nanmean(bk["turnover"].reindex(d.index)) * J.TRADING_DAYS)
    card["cost_share_of_gross"] = (
        float(card["cost_ann_on_notional"] / card["gross_ann_on_notional"])
        if card.get("gross_ann_on_notional") else None)
    card.pop("diff_stream", None)
    return card


def _gate(card: dict) -> dict:
    g = C.RESEARCH_CANDIDATE_GATE
    t = card.get("excess_t_hac")
    cs = (card.get("cost_stress") or {}).get("x2") or {}
    checks = {
        "t_min": bool(t is not None
                      and t >= g["after_cost_excess_t_hac_min"]),
        "same_sign_halves": bool(card.get("same_sign_halves")),
        "positive_at_2x_cost": bool((cs.get("excess_ann") or 0) > 0),
        "min_effective_decisions": bool(
            (card.get("effective_sample") or {}).get("ess", 0)
            >= g["min_effective_decisions"]),
        "positive_on_committed_capital": bool(
            (card.get("excess_ann") or 0) > 0),
    }
    return {"checks": checks, "passes": all(checks.values())}


# --------------------------------------------------------------------------- #
# Lanes
# --------------------------------------------------------------------------- #
def run_cross_asset(lane: str = "I_CROSS_ASSET") -> dict:
    spec_lane = C.LANES[lane]
    screened, advanced, per_relation = [], [], []
    cols = {}
    for rel in RELATIONS:
        z = relation_signal(rel)
        if z is None:
            per_relation.append({"relation": rel[0], "state": "NO_SOURCE"})
            continue
        cols[rel[0]] = z
    frame = pd.DataFrame(cols).sort_index()
    full = relation_book(RELATIONS, "CONTINUOUS", frame_override=frame)
    if full is None:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE"}
    # Zones are split on the COVERAGE index - the dates on which at least
    # half the declared relations actually have a live signal - not on the
    # union of the futures histories. Splitting on the union would place the
    # whole fitting zone before the sources existed, and a fitting zone in
    # which the book cannot trade is not a fitting zone.
    live = frame.notna().sum(axis=1)
    need = max(2, int(np.ceil(frame.shape[1] / 2.0)))
    cover = frame.index[live >= need]
    cover = pd.DatetimeIndex(cover).intersection(full["index"])
    if len(cover) < 750:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "coverage index shorter than 750 sessions",
                "coverage_sessions": int(len(cover))}
    zones = EV.zone_split(cover, embargo=EMBARGO)
    coverage = {"sessions": int(len(cover)),
                "span": [str(cover[0].date()), str(cover[-1].date())],
                "relations_required_live": need,
                "note": "zones split on the coverage index, not the union of "
                        "the futures histories"}

    # Per-relation ZONE-A diagnostics (free; never a candidate on their own,
    # which is what stops this becoming twelve cherry-picked candidates).
    for rel in RELATIONS:
        if rel[0] not in frame.columns:
            continue
        bk = relation_book([rel], "CONTINUOUS",
                           frame_override=frame[[rel[0]]])
        if bk is None:
            per_relation.append({"relation": rel[0], "state": "NO_BOOK"})
            continue
        a = score(bk, zones["A"])
        per_relation.append({
            "relation": rel[0], "target": rel[4], "declared_sign": rel[5],
            "story": rel[6], "state": "EXECUTED",
            "zone_a_excess_ann": a.get("excess_ann"),
            "zone_a_t": a.get("excess_t_hac"),
            "zone_a_sharpe": a.get("sharpe")})

    cache = {}
    for expression in EXPRESSIONS:
        bk = relation_book(RELATIONS, expression, frame_override=frame)
        if bk is None:
            continue
        cache[expression] = bk
        a = score(bk, zones["A"])
        screened.append({"expression": expression,
                         "zone_a_excess_ann": a.get("excess_ann"),
                         "zone_a_t": a.get("excess_t_hac"),
                         "zone_a_sharpe": a.get("sharpe"),
                         "zone_a_turnover_ann": a.get("turnover_ann"),
                         "n_relations": bk["n_relations"]})

    adv = sorted([r for r in screened if (r["zone_a_t"] or -9)
                  >= spec_lane["advance_t"]],
                 key=lambda r: -r["zone_a_t"])[:spec_lane["cap"]]
    for r in adv:
        bk = cache[r["expression"]]
        spec = {"information_family": "CROSS_ASSET",
                "asset_family": "MULTI_ASSET_FUTURES",
                "horizon": "5s", "economic_expression":
                    "FUTURES_CROSS_MARKET_RV",
                "representation": "SPARSE_RELATION_PORTFOLIO_%s"
                                  % r["expression"],
                "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R36 cross-asset", "R41 fx_credit_lab"],
                "validation_touches": 1, "lane": lane}
        cid = B.record_zone_b(spec, family="CROSS_ASSET", lane=lane)
        card = score(bk, zones["B"])
        advanced.append({"candidate_id": cid, "spec": spec,
                         "expression": r["expression"],
                         "zone_a_t": r["zone_a_t"], "zone_b": card,
                         "gate": _gate(card),
                         "committed_capital": bk["committed_capital"],
                         "relations_used": bk["relations_used"]})
    return {"lane": lane, "state": "EXECUTED",
            "question": spec_lane["question"],
            "n_relations_declared": len(RELATIONS),
            "relations_predeclared_with_sign": True,
            "n_squared_search": False,
            "coverage": coverage,
            "zones": {k: zones["%s_range" % k.lower()]
                      for k in ("A", "B", "C")},
            "per_relation_zone_a": per_relation,
            "screened": screened, "advanced": advanced,
            "credit_tier": _credit_tier(),
            "advance_rule": {"advance_t": spec_lane["advance_t"],
                             "cap": spec_lane["cap"], "signed": True}}


def _credit_tier() -> dict:
    """Track M's measurable half: the owned credit information, measured on
    the three years the provider still serves, and explicitly NOT a
    candidate."""
    cols = {}
    for rel in CREDIT_RELATIONS:
        z = relation_signal(rel)
        if z is not None and z.notna().sum() > 200:
            cols[rel[0]] = z
    if not cols:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "no credit source resolved"}
    frame = pd.DataFrame(cols).sort_index()
    rows = []
    for rel in CREDIT_RELATIONS:
        if rel[0] not in frame.columns:
            rows.append({"relation": rel[0], "state": "NO_SOURCE"})
            continue
        bk = relation_book([rel], "CONTINUOUS",
                           frame_override=frame[[rel[0]]], min_obs=200)
        if bk is None:
            rows.append({"relation": rel[0], "state": "NO_BOOK",
                         "reason": "fewer than 200 joint sessions"})
            continue
        card = score(bk)
        rows.append({"relation": rel[0], "target": rel[4],
                     "declared_sign": rel[5], "story": rel[6],
                     "state": "MEASURED_FULL_SAMPLE_ONLY",
                     "excess_ann": card.get("excess_ann"),
                     "t": card.get("excess_t_hac"),
                     "n": card.get("n")})
    span = [str(frame.dropna(how="all").index[0].date()),
            str(frame.dropna(how="all").index[-1].date())]
    years = (pd.Timestamp(span[1]) - pd.Timestamp(span[0])).days / 365.25
    return {
        "state": "EXECUTED",
        "n_relations": len(CREDIT_RELATIONS),
        "span": span, "years": round(years, 2),
        "candidate_eligible": bool(years >= CREDIT_MIN_YEARS_FOR_CANDIDACY),
        "blocker": "HISTORICAL_DATA_UNAVAILABLE",
        "why": "the ICE BofA OAS family's own FRED metadata states "
               "'Starting in April 2026, this series will only include 3 "
               "years of observations'. Three years cannot carry a 50/30/20 "
               "split whose judged zone clears the contract's minimum, so "
               "these relations are MEASURED and REPORTED but are never "
               "advanced to ZONE_B and never charged to the burden ledger.",
        "burden_charged": 0,
        "rows": rows,
    }


def run_event_driven(lane: str = "H_EVENT_DRIVEN") -> dict:
    from .rv import build_structures
    spec_lane = C.LANES[lane]
    cal = AQ.load_release_calendar()
    if cal is None or cal.empty:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "no release calendar acquired"}
    structures = build_structures("RATES")
    idx = None
    for s in structures:
        idx = s["index"] if idx is None else idx.union(s["index"])
    # The calendar starts in 1996; the event lane is scoped to the dates it
    # actually covers, and the zones are recomputed on THAT index.
    idx = idx[idx >= pd.Timestamp("1996-01-01")]
    zones = EV.zone_split(idx, embargo=EMBARGO)

    screened, advanced, cache = [], [], {}
    for rule in EVENT_RULES:
        for h in EVENT_HORIZONS:
            bk = event_book(structures, cal, rule, h, event_days=True)
            pb = event_book(structures, cal, rule, h, event_days=False)
            if bk is None:
                continue
            cache[(rule, h)] = bk
            a = score(bk, zones["A"], overlap=h)
            ap = score(pb, zones["A"], overlap=h) if pb else {}
            screened.append({
                "rule": rule, "horizon": h,
                "zone_a_excess_ann": a.get("excess_ann"),
                "zone_a_t": a.get("excess_t_hac"),
                "zone_a_sharpe": a.get("sharpe"),
                "zone_a_turnover_ann": a.get("turnover_ann"),
                "nonevent_placebo_excess_ann": ap.get("excess_ann"),
                "nonevent_placebo_t": ap.get("excess_t_hac"),
                "event_specific": bool(
                    a.get("excess_t_hac") is not None
                    and ap.get("excess_t_hac") is not None
                    and abs(a["excess_t_hac"]) > abs(ap["excess_t_hac"]))})

    adv = sorted([r for r in screened if (r["zone_a_t"] or -9)
                  >= spec_lane["advance_t"]],
                 key=lambda r: -r["zone_a_t"])[:spec_lane["cap"]]
    for r in adv:
        bk = cache[(r["rule"], r["horizon"])]
        spec = {"information_family": "EVENT_DRIVEN",
                "asset_family": "RATES_RV",
                "horizon": "%ds" % r["horizon"],
                "economic_expression": "FUTURES_CROSS_MARKET_RV",
                "representation": "MACRO_RELEASE_%s" % r["rule"],
                "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R43 FRED release calendar"],
                "validation_touches": 1, "lane": lane}
        cid = B.record_zone_b(spec, family="EVENT_DRIVEN", lane=lane)
        card = score(bk, zones["B"], overlap=r["horizon"])
        pb = event_book(structures, cal, r["rule"], r["horizon"],
                        event_days=False)
        pcard = score(pb, zones["B"], overlap=r["horizon"]) if pb else {}
        advanced.append({"candidate_id": cid, "spec": spec,
                         "rule": r["rule"], "horizon": r["horizon"],
                         "zone_a_t": r["zone_a_t"], "zone_b": card,
                         "zone_b_nonevent_placebo": {
                             "excess_ann": pcard.get("excess_ann"),
                             "t": pcard.get("excess_t_hac")},
                         "gate": _gate(card),
                         "committed_capital": bk["committed_capital"]})
    return {"lane": lane, "state": "EXECUTED",
            "question": spec_lane["question"],
            "calendar": {"n_event_dates": int(len(cal)),
                         "event_types": sorted(cal["event"].unique().tolist()),
                         "span": [str(cal["date"].min().date()),
                                  str(cal["date"].max().date())],
                         "source": "FRED release dates (owned entitlement)",
                         "granularity": "DATE_ONLY_NOT_INTRADAY"},
            "n_structures": len(structures),
            "zones": {k: zones["%s_range" % k.lower()]
                      for k in ("A", "B", "C")},
            "screened": screened, "advanced": advanced,
            "gross_vs_cost_decomposition": _event_decomposition(screened),
            "placebo_is_the_same_rule_on_non_event_days": True,
            "advance_rule": {"advance_t": spec_lane["advance_t"],
                             "cap": spec_lane["cap"], "signed": True}}


def _event_decomposition(screened: list) -> dict:
    """REVERSAL and CONTINUATION are the same positions with opposite signs,
    so their gross streams are exact mirrors and their costs are identical.
    That makes the split between signal and cost solvable in closed form:

        rev  = +g - c        cont = -g - c
        =>   c = -(rev + cont) / 2     g = (rev - cont) / 2

    which answers the only question that matters about this lane - is the
    event effect small, or is the cost of trading it large?
    """
    by_h = {}
    for r in screened:
        by_h.setdefault(r["horizon"], {})[r["rule"]] = r
    rows = []
    for h, m in sorted(by_h.items()):
        rev, cont = m.get("REVERSAL"), m.get("CONTINUATION")
        if not rev or not cont:
            continue
        a, b = rev.get("zone_a_excess_ann"), cont.get("zone_a_excess_ann")
        if a is None or b is None:
            continue
        cost = -(a + b) / 2.0
        gross = (a - b) / 2.0
        rows.append({
            "horizon": h,
            "reversal_net_ann": a, "continuation_net_ann": b,
            "implied_cost_ann": cost,
            "implied_gross_reversal_ann": gross,
            "cost_multiple_of_gross": (abs(cost / gross) if gross else None),
            "sign_of_effect": ("REVERSION" if gross > 0 else "CONTINUATION"),
        })
    mult = [r["cost_multiple_of_gross"] for r in rows
            if r["cost_multiple_of_gross"] is not None]
    return {
        "rows": rows,
        "median_cost_multiple_of_gross": (float(np.median(mult)) if mult
                                          else None),
        "reading": "an event effect exists in the sign the decomposition "
                   "reports, but the cost of trading it is the stated "
                   "multiple of its size. This lane is not blocked by "
                   "information - it is blocked by TRANSACTION COST, and "
                   "the intraday feed Track D cannot buy is the only thing "
                   "that would change the arithmetic.",
    }


def run_technical(lane: str = "J_TECHNICAL_STRUCTURE") -> dict:
    spec_lane = C.LANES[lane]
    markets = [m for m in J_MARKETS if P.futures_daily(m) is not None]
    if not markets:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE"}
    probe = fib_book(markets, C.FIB_NAMED_LEVELS, J_HORIZONS[0])
    if probe is None:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE"}
    zones = EV.zone_split(probe["index"], embargo=EMBARGO)

    screened, advanced, cache = [], [], {}
    for h in J_HORIZONS:
        named = fib_book(markets, C.FIB_NAMED_LEVELS, h)
        plac = fib_book(markets, C.FIB_PLACEBO_LEVELS, h)
        if named is None:
            continue
        cache[h] = named
        a = score(named, zones["A"], overlap=h)
        ap = score(plac, zones["A"], overlap=h) if plac else {}
        screened.append({
            "levels": "NAMED_FIBONACCI", "horizon": h,
            "zone_a_excess_ann": a.get("excess_ann"),
            "zone_a_t": a.get("excess_t_hac"),
            "placebo_levels": list(C.FIB_PLACEBO_LEVELS),
            "placebo_zone_a_excess_ann": ap.get("excess_ann"),
            "placebo_zone_a_t": ap.get("excess_t_hac"),
            "named_beats_placebo": bool(
                a.get("excess_t_hac") is not None
                and ap.get("excess_t_hac") is not None
                and abs(a["excess_t_hac"]) > abs(ap["excess_t_hac"]))})

    adv = sorted([r for r in screened if (r["zone_a_t"] or -9)
                  >= spec_lane["advance_t"]],
                 key=lambda r: -r["zone_a_t"])[:spec_lane["cap"]]
    for r in adv:
        bk = cache[r["horizon"]]
        spec = {"information_family": "TECHNICAL_STRUCTURE",
                "asset_family": "LIQUID_FUTURES",
                "horizon": "%ds" % r["horizon"],
                "economic_expression": "FUTURES_OUTRIGHT",
                "representation": "CAUSAL_PIVOT_FIB_RETRACEMENT",
                "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R41 technical structure"],
                "validation_touches": 1, "lane": lane}
        cid = B.record_zone_b(spec, family="TECHNICAL_STRUCTURE", lane=lane)
        card = score(bk, zones["B"], overlap=r["horizon"])
        plac = fib_book(markets, C.FIB_PLACEBO_LEVELS, r["horizon"])
        pcard = score(plac, zones["B"], overlap=r["horizon"]) if plac else {}
        advanced.append({"candidate_id": cid, "spec": spec,
                         "horizon": r["horizon"], "zone_a_t": r["zone_a_t"],
                         "zone_b": card,
                         "zone_b_placebo": {"excess_ann":
                                            pcard.get("excess_ann"),
                                            "t": pcard.get("excess_t_hac")},
                         "gate": _gate(card),
                         "committed_capital": bk["committed_capital"]})
    return {"lane": lane, "state": "EXECUTED",
            "question": spec_lane["question"],
            "markets": markets,
            "named_levels": list(C.FIB_NAMED_LEVELS),
            "placebo_levels": list(C.FIB_PLACEBO_LEVELS),
            "pivot_rule": C.PIVOT_CONFIRMATION_RULE,
            "confirmation_sessions": CONFIRM,
            "no_hindsight_extrema": True,
            "no_human_visual_confirmation": True,
            "zones": {k: zones["%s_range" % k.lower()]
                      for k in ("A", "B", "C")},
            "screened": screened, "advanced": advanced,
            "advance_rule": {"advance_t": spec_lane["advance_t"],
                             "cap": spec_lane["cap"], "signed": True}}
