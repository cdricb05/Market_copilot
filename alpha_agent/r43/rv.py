"""alpha_agent.r43.rv - Tracks E and F: curve relative value, judged on the
capital it actually commits, and attacked where it actually fails.

Release 41 built the same structures and reported, for its best
international-rates carry portfolio, ``excess_ann = 0.115 %/yr`` on traded
notional at ``t 1.78`` with ``cost_ann = 0.117 %/yr`` against
``gross_ann = 0.232 %/yr``. Read that line carefully: **cost consumed 50 %
of gross**, and the book turned over 9 times a year to earn 23 bps.

Two independent errors are buried in it, and Release 43 separates them.

**The quotation error.** A duration-neutral bond-future spread commits
SPAN margin, not notional. Quoting its return per unit of notional
understates the return on the capital it immobilises by an order of
magnitude. But the rescale multiplies gross AND cost by the same factor, so
it moves the LEVEL and never the sign, the t-statistic or the Sharpe.
Nothing R41 killed can be resurrected this way, and this module proves that
rather than asserting it.

**The turnover error, which is the real one.** The binding constraint on an
owned-data RV book is not information; it is TURNOVER. A signal that is
continuously proportional to a noisy state variable trades every time the
noise moves. The economically motivated fix is not a better signal - it is a
different EXPRESSION: enter only on conviction, and hold through the noise
until conviction is genuinely gone. That is a hysteresis band, and it is
declared in the frozen contract's lane questions, not chosen after seeing a
result.

So each family is searched over a small predeclared grid:

    signal   in {CARRY, VALUE, MOMENTUM}
    expression in {CONTINUOUS (the R41 baseline), BAND_15_05, BAND_20_10}

screened on ZONE_A only, advanced under the lane's frozen ``advance_t`` and
``cap``, and every ZONE_B score is charged to the burden ledger.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import burden as B
from . import judge as J
from . import panels as P
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.rv"

BETA_WIN = 120          # rolling hedge-ratio window (sessions), lagged 1
VOL_WIN = 60            # rolling spread-vol window for risk scaling
Z_WIN = 252             # state z-score window
MIN_OBS = 750           # a structure needs three years to be researched
#: Signal constructions. CARRY_LEVEL is scaled by its own volatility but NOT
#: de-meaned: the economically meaningful content of carry is its LEVEL (am
#: I paid to hold this spread?), and a rolling z-score would delete exactly
#: that. CARRY_XS ranks carry ACROSS structures on each date. VALUE and
#: MOMENTUM are genuinely mean-reverting / trending states, so they are
#: de-meaned.
#:
#: DISCLOSURE: the first Zone-A screen of this lane used a de-meaned z-score
#: for carry. That destroyed the level information and was corrected here.
#: ZONE_B was never touched and no burden was spent before the correction -
#: Zone A is the contract's free screening zone precisely so construction
#: errors are found without paying for them.
SIGNALS = ("CARRY_LEVEL", "CARRY_XS", "VALUE", "MOMENTUM")
CARRY_Z_IS_DEMEANED = False
EXPRESSIONS = {
    "CONTINUOUS": None,                 # R41 baseline: sign of the signal
    "BAND_15_05": (1.5, 0.5),           # enter at 1.5 sigma, exit at 0.5
    "BAND_20_10": (2.0, 1.0),           # enter at 2.0 sigma, exit at 1.0
}
MOM_WIN = 63
EMBARGO = 21

# --------------------------------------------------------------------------- #
# Structure declarations
# --------------------------------------------------------------------------- #
#: Rates: (name, market_long, market_short, country_tag). A positive position
#: is long the first leg and short the beta-scaled second leg.
RATES_STRUCTURES = [
    ("US_2s5s", "ZT", "ZF", "US"), ("US_5s10s", "ZF", "ZN", "US"),
    ("US_10s30s", "ZN", "ZB", "US"), ("US_30sUB", "ZB", "UB", "US"),
    ("US_10sTN", "ZN", "TN", "US"), ("US_2s10s", "ZT", "ZN", "US"),
    ("DE_2s5s", "FGBS", "FGBM", "DE"), ("DE_5s10s", "FGBM", "FGBL", "DE"),
    ("DE_10s30s", "FGBL", "FGBX", "DE"), ("DE_2s10s", "FGBS", "FGBL", "DE"),
    ("AU_3s10s", "YYT", "YXT", "AU"),
    ("XC_US_DE_10y", "ZN", "FGBL", "XC"), ("XC_US_UK_10y", "ZN", "LLG", "XC"),
    ("XC_US_CA_10y", "ZN", "CGB", "XC"), ("XC_US_JP_10y", "ZN", "SJB", "XC"),
    ("XC_US_AU_10y", "ZN", "YXT", "XC"), ("XC_DE_UK_10y", "FGBL", "LLG", "XC"),
    ("XC_DE_FR_10y", "FGBL", "FOAT", "XC"),
    ("XC_DE_IT_10y", "FGBL", "FBTP", "XC"),
    ("XC_DE_JP_10y", "FGBL", "SJB", "XC"),
    ("XC_US_DE_2y", "ZT", "FGBS", "XC"),
    ("XC_STIR_US_EU", "ZQ", "LEU", "XC"),
    ("XC_STIR_US_UK", "SR3", "SO3", "XC"),
    ("XC_STIR_US_AU", "SR3", "YIR", "XC"),
]

#: Commodity calendar spreads are built per market from the OWN curve
#: (tenor 1 vs tenor 2 and tenor 1 vs tenor 3) - the cleanest relative-value
#: object in the estate: identical underlying, identical delivery mechanism,
#: no cross-market basis risk and no FX leg.
COMMODITY_ASSET_CLASSES = ("COMMODITY",)
CALENDAR_PAIRS = (("ret1", "ret2", "c1", "c2", "slope_ann", "1v2"),
                  ("ret1", "ret3", "c1", "c3", "slope23_ann", "1v3"))

#: Inter-commodity structures with a genuine production-chain or
#: substitution economics, not statistical pairs: refining margins, the
#: soybean crush, feed substitution, feed-to-livestock and the two precious
#: metals' industrial/monetary split. Declared here, before any result.
INTER_COMMODITY = [
    ("CRACK_CL_HO", "HO", "CL", "ENERGY"),
    ("CRACK_CL_RB", "RB", "CL", "ENERGY"),
    ("SPARK_NG_CL", "NG", "CL", "ENERGY"),
    ("CRUSH_ZS_ZM", "ZM", "ZS", "GRAINS_AND_OILSEEDS"),
    ("CRUSH_ZS_ZL", "ZL", "ZS", "GRAINS_AND_OILSEEDS"),
    ("FEED_ZC_ZW", "ZC", "ZW", "GRAINS_AND_OILSEEDS"),
    ("FEED_ZC_ZM", "ZC", "ZM", "GRAINS_AND_OILSEEDS"),
    ("LIVE_LE_ZC", "LE", "ZC", "LIVESTOCK"),
    ("LIVE_LE_HE", "LE", "HE", "LIVESTOCK"),
    ("METAL_GC_SI", "GC", "SI", "PRECIOUS_METALS"),
    ("METAL_GC_PL", "GC", "PL", "PRECIOUS_METALS"),
    ("METAL_HG_GC", "HG", "GC", "INDUSTRIAL_METALS"),
]


# --------------------------------------------------------------------------- #
# Structure construction
# --------------------------------------------------------------------------- #
def _rolling_beta(y: pd.Series, x: pd.Series, win: int = BETA_WIN) -> pd.Series:
    """Point-in-time hedge ratio: rolling OLS beta of y on x, LAGGED one
    session so the ratio used on date t was computable on t-1."""
    cov = y.rolling(win, min_periods=win // 2).cov(x)
    var = x.rolling(win, min_periods=win // 2).var()
    b = (cov / var.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    return b.shift(1)


def rates_structure(name: str, m_long: str, m_short: str, tag: str):
    a, b = P.futures_daily(m_long), P.futures_daily(m_short)
    if a is None or b is None:
        return None
    ra = pd.to_numeric(a.get("ret1"), errors="coerce")
    rb = pd.to_numeric(b.get("ret1"), errors="coerce")
    ca = pd.to_numeric(a.get("c1"), errors="coerce")
    cb = pd.to_numeric(b.get("c1"), errors="coerce")
    sa = pd.to_numeric(a.get("slope_ann"), errors="coerce")
    sb = pd.to_numeric(b.get("slope_ann"), errors="coerce")
    idx = ra.dropna().index.intersection(rb.dropna().index)
    if len(idx) < MIN_OBS:
        return None
    ra, rb = ra.reindex(idx), rb.reindex(idx)
    beta = _rolling_beta(ra, rb).clip(-5.0, 5.0)
    spread_ret = (ra - beta * rb).rename("spread_ret")
    # State variables, all observable at t.
    with np.errstate(all="ignore"):
        level = (np.log(ca.reindex(idx).replace(0, np.nan))
                 - np.log(cb.reindex(idx).replace(0, np.nan)))
    carry = (sa.reindex(idx) - sb.reindex(idx)).rename("carry")
    reg = P.market_registry()
    return {
        "name": name, "kind": "RATES", "tag": tag,
        "legs": (m_long, m_short),
        "leg_groups": (reg[m_long]["cost_group"], reg[m_short]["cost_group"]),
        "index": idx, "spread_ret": spread_ret, "beta": beta,
        "level": level.rename("level"), "carry": carry,
        "asset_class": "RATES",
    }


def commodity_structure(market: str, pair) -> dict:
    r_near, r_far, c_near, c_far, slope_col, tag = pair
    d = P.futures_daily(market)
    if d is None:
        return None
    ra = pd.to_numeric(d.get(r_near), errors="coerce")
    rb = pd.to_numeric(d.get(r_far), errors="coerce")
    ca = pd.to_numeric(d.get(c_near), errors="coerce")
    cb = pd.to_numeric(d.get(c_far), errors="coerce")
    sl = pd.to_numeric(d.get(slope_col), errors="coerce")
    if ra is None or rb is None:
        return None
    idx = ra.dropna().index.intersection(rb.dropna().index)
    if len(idx) < MIN_OBS:
        return None
    ra, rb = ra.reindex(idx), rb.reindex(idx)
    # A calendar spread on ONE market is naturally one-for-one; the rolling
    # beta corrects only for the tenor's lower volatility.
    beta = _rolling_beta(ra, rb).clip(0.0, 3.0).fillna(1.0)
    spread_ret = (ra - beta * rb).rename("spread_ret")
    with np.errstate(all="ignore"):
        level = (np.log(ca.reindex(idx).replace(0, np.nan))
                 - np.log(cb.reindex(idx).replace(0, np.nan)))
    reg = P.market_registry()
    g = reg[market]["cost_group"]
    return {
        "name": "%s_%s" % (market, tag), "kind": "COMMODITY_CALENDAR",
        "tag": market, "legs": (market, market), "leg_groups": (g, g),
        "index": idx, "spread_ret": spread_ret, "beta": beta,
        "level": level.rename("level"),
        "carry": sl.reindex(idx).rename("carry"),
        "asset_class": reg[market]["asset_class"],
    }


def build_structures(kind: str) -> list:
    out = []
    if kind == "RATES":
        for row in RATES_STRUCTURES:
            s = rates_structure(*row)
            if s is not None:
                out.append(s)
    else:
        for m in P.markets_by(asset_class="COMMODITY"):
            for pair in CALENDAR_PAIRS:
                s = commodity_structure(m, pair)
                if s is not None:
                    out.append(s)
        for name, m_long, m_short, tag in INTER_COMMODITY:
            s = rates_structure(name, m_long, m_short, tag)
            if s is not None:
                s["kind"] = "INTER_COMMODITY"
                s["asset_class"] = "COMMODITY"
                out.append(s)
    return out


# --------------------------------------------------------------------------- #
# Signals and expressions
# --------------------------------------------------------------------------- #
def _scale_only(x: pd.Series, win: int = Z_WIN) -> pd.Series:
    """Standardise the SCALE of a state variable without removing its mean."""
    sd = x.rolling(win, min_periods=max(20, win // 3)).std()
    return (x / sd.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


def raw_signal(struct: dict, signal: str) -> pd.Series:
    """The standardised STATE that decides the position, observable at t."""
    x = pd.Series(dtype=float)
    if signal == "CARRY_LEVEL":
        return _scale_only(
            pd.Series(struct["carry"], index=struct["index"]).astype(float))
    if signal == "CARRY_XS":
        # Filled in by signal_frame, which needs every structure at once.
        return pd.Series(struct["carry"], index=struct["index"]).astype(float)
    if signal == "VALUE":
        x = -struct["level"]          # cheap spread -> long the spread
    elif signal == "MOMENTUM":
        x = struct["spread_ret"].rolling(MOM_WIN, min_periods=MOM_WIN // 2) \
            .sum()
    else:
        raise ValueError("unknown signal %r" % signal)
    return P.zscore(pd.Series(x, index=struct["index"]).astype(float), Z_WIN)


def signal_frame(structures: list, signal: str) -> pd.DataFrame:
    """One standardised state per structure. CARRY_XS is cross-sectional:
    on each date the carries of ALL structures are ranked against each
    other, which is the classic carry expression and a different economic
    claim from 'this spread's own carry is positive'."""
    cols = {s["name"]: raw_signal(s, signal) for s in structures}
    frame = pd.DataFrame(cols).sort_index()
    if signal != "CARRY_XS":
        return frame
    n = frame.notna().sum(axis=1)
    mu = frame.mean(axis=1)
    sd = frame.std(axis=1).replace(0.0, np.nan)
    xs = frame.sub(mu, axis=0).div(sd, axis=0)
    return xs.where(n >= 3)


def apply_expression(z: pd.Series, expression: str, *, extra_lag: int = 0,
                     band=None) -> pd.Series:
    """Turn the state into a HELD position. The position on date t uses only
    information through t-1 (the signal is lagged once, always);
    ``extra_lag`` is the kill battery's latency perturbation."""
    zl = z.shift(1 + int(extra_lag))
    if expression == "CONTINUOUS" and band is None:
        return np.sign(zl).fillna(0.0)
    enter, exit_ = band if band is not None else EXPRESSIONS[expression]
    v = zl.to_numpy(dtype=float)
    pos = np.zeros(len(v))
    cur = 0.0
    for i in range(len(v)):
        x = v[i]
        if not np.isfinite(x):
            pos[i] = cur
            continue
        if cur == 0.0:
            if x >= enter:
                cur = 1.0
            elif x <= -enter:
                cur = -1.0
        elif cur > 0 and x < exit_:
            cur = 0.0
        elif cur < 0 and x > -exit_:
            cur = 0.0
        pos[i] = cur
    return pd.Series(pos, index=z.index)


def risk_scale(struct: dict) -> pd.Series:
    """Scale each structure to a common risk unit so a book of structures is
    not dominated by its most volatile leg.

    The vol is rolling and LAGGED, and the target it is scaled against is an
    EXPANDING median of that same lagged vol - never a full-sample constant.
    A full-sample target would let each structure's weight depend on volatility
    it had not yet realised, which is look-ahead in the portfolio weights even
    though every individual return is causal.
    """
    v = struct["spread_ret"].rolling(VOL_WIN, min_periods=VOL_WIN // 2).std()
    v = v.shift(1).replace(0.0, np.nan)
    target = v.expanding(min_periods=250).median()
    return (target / v).clip(0.25, 4.0).fillna(1.0)


def _leg_cost(turnover: pd.Series, leg_groups, beta_weight: pd.Series
              ) -> pd.Series:
    """Execution cost of a two-leg spread, charged with a TIME-VARYING hedge
    ratio: leg 1 trades one unit, leg 2 trades ``beta_weight`` units."""
    g1, g2 = leg_groups
    b1 = C.COST_BPS_PER_SIDE.get(g1)
    b2 = C.COST_BPS_PER_SIDE.get(g2)
    fallback = max(v for v in C.COST_BPS_PER_SIDE.values()
                   if isinstance(v, (int, float)))
    b1 = float(b1) if isinstance(b1, (int, float)) else fallback
    b2 = float(b2) if isinstance(b2, (int, float)) else fallback
    bw = beta_weight.reindex(turnover.index).ffill().fillna(1.0)
    return (turnover.abs().fillna(0.0)
            * ((b1 + b2 * bw) / 1e4)).rename("cost")


def _causal_beta_weight(beta: pd.Series) -> pd.Series:
    """Expanding-median |beta|, for the COST and CAPITAL constants. Causal for
    the same reason as :func:`risk_scale`."""
    b = beta.abs().replace([np.inf, -np.inf], np.nan)
    return b.expanding(min_periods=250).median().ffill().fillna(1.0) \
        .clip(0.05, 5.0)


# --------------------------------------------------------------------------- #
# Book assembly - a PORTFOLIO of structures is the candidate
# --------------------------------------------------------------------------- #
def book_streams(structures: list, signal: str, expression: str, *,
                 extra_lag: int = 0, band=None,
                 frame_override: pd.DataFrame = None) -> dict:
    """Aggregate one (signal x expression) portfolio across all structures.

    Every structure contributes its own risk-scaled spread P&L and its own
    cost, both quoted per unit of ONE leg's notional, and the portfolio is
    the equal-risk average. Committed capital is the notional-weighted
    average of the structures' committed capital.
    """
    gross, cost, turn, pos_n = None, None, None, None
    cap_num, cap_den, gross_leverage = 0.0, 0.0, []
    used = []
    frame = (signal_frame(structures, signal) if frame_override is None
             else frame_override)
    for s in structures:
        if s["name"] not in frame.columns:
            continue
        z = frame[s["name"]].reindex(s["index"])
        pos = apply_expression(z, expression, extra_lag=extra_lag, band=band)
        if pos.abs().sum() < 5:
            continue
        w = risk_scale(s)
        held = (pos * w).rename(s["name"])
        pnl = (held * s["spread_ret"]).fillna(0.0)
        bw = _causal_beta_weight(s["beta"])
        turnover = held.diff().abs().fillna(0.0)
        # Cost is charged per date against THAT date's causal hedge ratio,
        # so a book whose hedge ratio grows is charged for the bigger leg.
        c = _leg_cost(turnover, s["leg_groups"], bw)
        weights = (1.0, float(bw.median()))
        capt = J.futures_committed_capital(s["leg_groups"], weights)
        gross = pnl if gross is None else gross.add(pnl, fill_value=0.0)
        cost = c if cost is None else cost.add(c, fill_value=0.0)
        turn = turnover if turn is None else turn.add(turnover, fill_value=0.0)
        onv = (held != 0).astype(float)
        pos_n = onv if pos_n is None else pos_n.add(onv, fill_value=0.0)
        cap_num += capt["committed_capital"]
        cap_den += 1.0
        gross_leverage.append(capt["gross_notional_per_leg_unit"])
        used.append({"structure": s["name"], "legs": list(s["legs"]),
                     "leg_groups": list(s["leg_groups"]),
                     "committed_capital": capt["committed_capital"],
                     "n_obs": int(len(s["index"]))})
    if gross is None or not used:
        return None
    n = float(len(used))
    idx = gross.index
    scale = 1.0 / n
    return {
        "gross": (gross * scale).rename("gross"),
        "cost": (cost.reindex(idx).fillna(0.0) * scale).rename("cost"),
        "turnover": (turn.reindex(idx).fillna(0.0) * scale),
        "active_fraction": (pos_n.reindex(idx).fillna(0.0) / n),
        "committed_capital": cap_num / cap_den,
        "gross_notional": float(np.mean(gross_leverage)),
        "structures_used": used, "n_structures": len(used), "index": idx,
    }


def score_book(bk: dict, dates=None, *, overlap: int = 1,
               collateral_class: str = "REMUNERATED_MARGIN",
               capital: float = None, cost_multiplier: float = 1.0) -> dict:
    """Score a portfolio on committed capital.

    ``bk["gross"]`` already embeds the held position of every structure, so
    the judge is handed a unit position and the aggregated stream. Under
    REMUNERATED_MARGIN the benchmark is identically zero, which is exactly
    why the on/off indicator does not enter the arithmetic; it is reported
    separately as ``active_fraction_mean``.
    """
    K = float(capital if capital is not None else bk["committed_capital"])
    book = J.implementable_book(
        bk["gross"], pd.Series(1.0, index=bk["index"]),
        committed_capital=K, collateral_class=collateral_class,
        cost=bk["cost"] * float(cost_multiplier), day_count=J.TRADING_DAYS)
    card = J.score(book, dates, overlap=overlap, day_count=J.TRADING_DAYS)
    d = book if dates is None else book.reindex(pd.DatetimeIndex(dates))
    card["turnover_ann"] = float(
        np.nanmean(bk["turnover"].reindex(d.index)) * J.TRADING_DAYS)
    card["active_fraction_mean"] = float(
        np.nanmean(bk["active_fraction"].reindex(d.index)))
    card["cost_share_of_gross"] = (
        float(card["cost_ann_on_notional"] / card["gross_ann_on_notional"])
        if card.get("gross_ann_on_notional") else None)
    card["n_structures"] = bk["n_structures"]
    return card


# --------------------------------------------------------------------------- #
# The lane
# --------------------------------------------------------------------------- #
def run_lane(lane: str, kind: str, *, family: str) -> dict:
    spec_lane = C.LANES[lane]
    structures = build_structures(kind)
    if not structures:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "n_structures": 0}
    idx = None
    for s in structures:
        idx = s["index"] if idx is None else idx.union(s["index"])
    zones = EV.zone_split(idx, embargo=EMBARGO)

    screened, cache = [], {}
    for signal in SIGNALS:
        for expression in EXPRESSIONS:
            bk = book_streams(structures, signal, expression)
            if bk is None:
                continue
            a = score_book(bk, zones["A"])
            cache[(signal, expression)] = bk
            ex_ann, c_ann = a.get("excess_ann"), a.get("cost_ann_on_notional")
            # Reversing the position reverses gross but NOT cost, so the
            # reversed book's mean is exact arithmetic, not a second search.
            rev = (None if ex_ann is None or c_ann is None
                   else -ex_ann - 2.0 * (c_ann / bk["committed_capital"]))
            screened.append({
                "signal": signal, "expression": expression,
                "zone_a_excess_ann": ex_ann,
                "zone_a_t": a.get("excess_t_hac"),
                "zone_a_sharpe": a.get("sharpe"),
                "zone_a_gross_ann_on_capital": (
                    a.get("gross_ann_on_notional") / bk["committed_capital"]
                    if a.get("gross_ann_on_notional") is not None else None),
                "zone_a_turnover_ann": a.get("turnover_ann"),
                "zone_a_cost_share_of_gross": a.get("cost_share_of_gross"),
                "zone_a_reversed_excess_ann_exact": rev,
                "n_structures": a.get("n_structures"),
            })
    # Advance rule: SIGNED Zone-A t >= advance_t, ranked by t, capped by the
    # lane's FROZEN cap. Signed, not absolute: Release 43 fits no signs, so a
    # book that loses on Zone A is a REJECTED HYPOTHESIS, not a candidate
    # waiting to be flipped. This is strictly stricter than R41's |t| rule
    # and spends strictly less burden. The reversed book of every negative
    # screen is reported below as a ZONE-A-ONLY diagnostic.
    adv = [r for r in screened
           if r["zone_a_t"] is not None
           and r["zone_a_t"] >= spec_lane["advance_t"]]
    adv.sort(key=lambda r: -r["zone_a_t"])
    adv = adv[:spec_lane["cap"]]

    advanced = []
    for r in adv:
        bk = cache[(r["signal"], r["expression"])]
        spec = {
            "information_family": family,
            "asset_family": kind,
            "horizon": "1s_hold_to_exit" if r["expression"] != "CONTINUOUS"
            else "1s",
            "economic_expression": "FUTURES_CURVE_SPREAD"
            if kind != "RATES" else "FUTURES_CROSS_MARKET_RV",
            "representation": "%s_%s" % (r["signal"], r["expression"]),
            "model": "TRANSPARENT_RULE",
            "hyperparameter_budget": 0,
            "parent_hypotheses": ["R41 %s lab" % kind.lower()],
            "validation_touches": 1,
            "lane": lane,
        }
        cid = B.record_zone_b(spec, family=family, lane=lane)
        b_card = score_book(bk, zones["B"])
        advanced.append({
            "candidate_id": cid, "spec": spec,
            "signal": r["signal"], "expression": r["expression"],
            "zone_a_t": r["zone_a_t"],
            "zone_b": _clean(b_card),
            "gate": _gate(b_card),
            "committed_capital": bk["committed_capital"],
            "gross_notional_per_leg_unit": bk["gross_notional"],
            "n_structures": bk["n_structures"],
        })
    return {
        "lane": lane, "state": "EXECUTED", "kind": kind, "family": family,
        "n_structures": len(structures),
        "structure_names": [s["name"] for s in structures],
        "zones": {k: zones["%s_range" % k.lower()] for k in ("A", "B", "C")},
        "zone_embargo": EMBARGO,
        "advance_rule": {"advance_t": spec_lane["advance_t"],
                         "cap": spec_lane["cap"], "frozen": True},
        "screened": screened, "advanced": advanced,
        "turnover_finding": _turnover_finding(screened),
        "advance_rule_is_signed": True,
        "signs_fitted": 0,
        "collateral_class": "REMUNERATED_MARGIN",
        "capital_note": "margin is posted in T-bills and IS remunerated, so "
                        "the correct correction is a RESCALE onto committed "
                        "capital, not a subtraction of the risk-free rate",
    }


def _turnover_finding(screened: list) -> dict:
    """Isolate the effect of the EXPRESSION, holding the signal fixed: does
    a hysteresis band cut turnover and the cost share of gross?"""
    rows = []
    by_sig = {}
    for r in screened:
        by_sig.setdefault(r["signal"], {})[r["expression"]] = r
    for sig, m in sorted(by_sig.items()):
        base = m.get("CONTINUOUS")
        if not base or not base.get("zone_a_turnover_ann"):
            continue
        for expr, r in sorted(m.items()):
            if expr == "CONTINUOUS":
                continue
            rows.append({
                "signal": sig, "expression": expr,
                "turnover_continuous": base["zone_a_turnover_ann"],
                "turnover_banded": r["zone_a_turnover_ann"],
                "turnover_ratio": (r["zone_a_turnover_ann"]
                                   / base["zone_a_turnover_ann"]
                                   if base["zone_a_turnover_ann"] else None),
                "cost_share_continuous": base["zone_a_cost_share_of_gross"],
                "cost_share_banded": r["zone_a_cost_share_of_gross"],
                "t_continuous": base["zone_a_t"],
                "t_banded": r["zone_a_t"],
            })
    ratios = [r["turnover_ratio"] for r in rows
              if r["turnover_ratio"] is not None]
    return {
        "hypothesis": "the binding constraint on an owned-data RV book is "
                      "TURNOVER, not information; a hysteresis band changes "
                      "the ECONOMIC EXPRESSION, not the signal",
        "median_turnover_ratio": (float(np.median(ratios)) if ratios
                                  else None),
        "n_comparisons": len(rows),
        "rows": rows,
        "zone": "ZONE_A_ONLY",
    }


def _clean(card: dict) -> dict:
    out = {k: v for k, v in card.items() if k != "diff_stream"}
    es = out.get("effective_sample")
    if isinstance(es, dict):
        out["effective_sample"] = {k: v for k, v in es.items()}
    return out


def _gate(card: dict) -> dict:
    g = C.RESEARCH_CANDIDATE_GATE
    t = card.get("excess_t_hac")
    cs = (card.get("cost_stress") or {}).get("x2") or {}
    checks = {
        # Signed, not absolute: a book that loses is not a candidate.
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
