"""alpha_agent.r44.niche - ENGINE 3, the less-efficient market frontier.

Releases 31-43 searched hard and searched mostly where everyone else does:
ES, ZN, CL, GC, EURUSD, BTC. If the efficient-market objection to every
negative result is "those markets are crowded", then the objection is a
testable hypothesis, and this module tests it in the only way that isolates
it - by holding the HYPOTHESIS constant and varying the LIQUIDITY.

The design:

  * the owned universe is split into liquidity terciles by median daily
    notional turnover in USD;
  * the SAME three economic hypotheses - carry, momentum, value - are built
    inside each tier, with the same windows and the same expression;
  * cost is LIQUIDITY-SCALED, so the illiquid tier is charged more per
    trade, which is the whole reason a crowded-market edge might not
    survive when it moves downmarket;
  * capacity is computed, not assumed, at a 1% participation cap.

If the efficient-market story is right, the illiquid tier's frontier should
be visibly better than the liquid tier's. If it is not, "the markets we
looked at were too efficient" stops being an available explanation for
R31-R43.

One approximation is declared rather than hidden. Contract notional is
quoted in each exchange's own currency and the owned registry carries no
currency field, so a STATIC exchange-to-USD table is used for TIERING ONLY.
It never touches a P&L. Every market's implied contract notional is
reported so a reader can see whether the conversion produced a sane number.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from ..r43 import carry as KARRY
from ..r43 import judge as J
from ..r43 import panels as P
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.niche"

#: Declared BEFORE any tier result. Approximate long-run averages, used only
#: to place a market in a liquidity tier - never in a return.
FX_TO_USD = {"USD": 1.0, "EUR": 1.10, "GBP": 1.28, "AUD": 0.68,
             "CAD": 0.75, "HKD": 0.128, "KRW": 0.00075, "JPY": 0.0068,
             "SGD": 0.74}
CURRENCY_BY_EXCHANGE = {
    "CME": "USD", "CBOT": "USD", "NYMEX": "USD", "ICE US": "USD",
    "KCBT": "USD", "MIAX": "USD", "CBOE": "USD",
    "Eurex": "EUR", "EURONEXT": "EUR", "ICE Europe": "USD",
    "ASX": "AUD", "ME": "CAD", "HKFE": "HKD", "KRX": "KRW", "SGX": "USD",
}
#: Markets whose denomination differs from their exchange's default.
MARKET_CURRENCY = {
    "NIY": "JPY",           # CME-listed Nikkei quoted in yen
    "FTDX": "EUR", "FESX": "EUR", "FDAX": "EUR",
    "EUA": "EUR",           # ICE Europe emissions are euro-denominated
}

LIQUIDITY_LOOKBACK = 1260          # five years of sessions
MIN_LIQUIDITY_OBS = 250
TIER_NAMES = ("LIQUID", "MID", "ILLIQUID")

#: Cost scaling: a market with a tenth of its group's turnover pays the
#: square root of that ratio in extra spread, capped at five times. Declared
#: before any tier was scored; the contract froze the PRINCIPLE
#: (NICHE_COST_IS_LIQUIDITY_SCALED) and this is its functional form.
COST_SCALE_EXPONENT = 0.5
COST_SCALE_CAP = 5.0

HYPOTHESES = ("CARRY", "MOMENTUM", "VALUE")
MOM_WIN = 252
MOM_SKIP = 21
VALUE_WIN = 1260
MIN_TIER_MARKETS = 6


# --------------------------------------------------------------------------- #
# Liquidity
# --------------------------------------------------------------------------- #
def _currency(market: str, reg: dict) -> str:
    if market in MARKET_CURRENCY:
        return MARKET_CURRENCY[market]
    return CURRENCY_BY_EXCHANGE.get((reg.get(market) or {}).get("exchange"),
                                    "USD")


def liquidity_table() -> pd.DataFrame:
    """Median daily notional turnover in USD, per market."""
    reg = P.market_registry()
    rows = []
    for m in P.available_markets():
        d = P.futures_daily(m)
        if d is None or "v1" not in d.columns or "c1" not in d.columns:
            continue
        meta = reg.get(m) or {}
        pv = float(meta.get("point_value") or 0.0)
        if pv <= 0:
            continue
        ccy = _currency(m, reg)
        fx = FX_TO_USD.get(ccy, 1.0)
        v = pd.to_numeric(d["v1"], errors="coerce")
        c = pd.to_numeric(d["c1"], errors="coerce")
        notional = (c * pv * fx)
        turn = (v * notional).replace([np.inf, -np.inf], np.nan).dropna()
        turn = turn.tail(LIQUIDITY_LOOKBACK)
        if len(turn) < MIN_LIQUIDITY_OBS:
            continue
        rows.append({
            "market": m, "asset_class": meta.get("asset_class"),
            "economic_group": meta.get("economic_group"),
            "exchange": meta.get("exchange"), "currency": ccy,
            "contract_notional_usd": float(
                notional.tail(LIQUIDITY_LOOKBACK).median()),
            "adv_usd": float(turn.median()),
            "n_obs": int(len(turn)),
        })
    f = pd.DataFrame(rows)
    # A market with no recorded volume is not an illiquid opportunity, it is
    # a data gap, and letting it into a tier makes that tier's capacity the
    # capacity of a market nobody can trade.
    f = f[f["adv_usd"] > 0].sort_values("adv_usd").reset_index(drop=True)
    # Sanity: a listed futures contract is worth roughly $5k-$2m. Anything
    # far outside that is a currency-conversion failure and is reported.
    f["notional_implausible"] = ~f["contract_notional_usd"].between(2e3, 5e6)
    return f


def tiers(table: pd.DataFrame = None) -> dict:
    table = table if table is not None else liquidity_table()
    q = table["adv_usd"].quantile([1 / 3, 2 / 3]).to_list()
    out = {t: [] for t in TIER_NAMES}
    for _, r in table.iterrows():
        if r["adv_usd"] <= q[0]:
            out["ILLIQUID"].append(r["market"])
        elif r["adv_usd"] <= q[1]:
            out["MID"].append(r["market"])
        else:
            out["LIQUID"].append(r["market"])
    return {"tiers": out, "cuts_usd": [float(x) for x in q],
            "n": {k: len(v) for k, v in out.items()}}


def cost_multipliers(table: pd.DataFrame) -> dict:
    """Per-market cost scaling from its own turnover against its group."""
    med = table.groupby("economic_group")["adv_usd"].median()
    out = {}
    for _, r in table.iterrows():
        g = med.get(r["economic_group"], r["adv_usd"])
        ratio = (g / r["adv_usd"]) if r["adv_usd"] > 0 else COST_SCALE_CAP
        out[r["market"]] = float(np.clip(ratio ** COST_SCALE_EXPONENT,
                                         1.0, COST_SCALE_CAP))
    return out


# --------------------------------------------------------------------------- #
# Books
# --------------------------------------------------------------------------- #
def _signal(hyp: str, panel: dict) -> pd.DataFrame:
    ret, carry = panel["ret"], panel["carry"]
    if hyp == "CARRY":
        return KARRY.xs_signal(carry)
    # ``min_periods`` is not cosmetic here. Markets from different exchanges
    # sit on different calendars, so a union-indexed panel has a hole in
    # every column on every other market's holidays. A bare rolling(1260)
    # demands 1260 CONSECUTIVE observations and returns an entirely empty
    # signal - which is exactly what the first run of this lane produced for
    # two of the three tiers before the emptiness was traced.
    if hyp == "MOMENTUM":
        raw = np.log1p(ret.clip(-0.5, 0.5)) \
            .rolling(MOM_WIN, min_periods=MOM_WIN // 2).sum().shift(MOM_SKIP)
    elif hyp == "VALUE":
        raw = -np.log1p(ret.clip(-0.5, 0.5)) \
            .rolling(VALUE_WIN, min_periods=VALUE_WIN // 2).sum().shift(1)
    else:
        raise ValueError(hyp)
    n = raw.notna().sum(axis=1)
    mu, sd = raw.mean(axis=1), raw.std(axis=1).replace(0.0, np.nan)
    return raw.sub(mu, axis=0).div(sd, axis=0).where(n >= KARRY.MIN_MARKETS)


def tier_book(markets: list, hyp: str, mults: dict) -> dict:
    """One hypothesis inside one liquidity tier, with liquidity-scaled cost."""
    usable = []
    for m in markets:
        d = P.futures_daily(m)
        if d is None or "slope_ann" not in d.columns:
            continue
        if pd.to_numeric(d["slope_ann"], errors="coerce").notna().sum() \
                >= KARRY.MIN_OBS:
            usable.append(m)
    if len(usable) < MIN_TIER_MARKETS:
        return None
    panel = KARRY.build_panel(sorted(usable))
    if panel is None:
        return None
    z = _signal(hyp, panel)
    cols = [c for c in z.columns if c in panel["ret"].columns]
    if len(cols) < MIN_TIER_MARKETS:
        return None
    pos = KARRY.positions(z[cols], "CONTINUOUS", panel["ret"][cols])
    if pos.abs().to_numpy().sum() < 10:
        return None
    ret = panel["ret"][cols]
    gross = (pos * ret).sum(axis=1).rename("gross")

    reg = P.market_registry()
    turn = pos.diff().abs().fillna(0.0)
    cost = pd.Series(0.0, index=pos.index)
    cap_w = pd.Series(0.0, index=pos.index)
    for c in cols:
        g = P.cost_group((reg.get(c) or {}).get("economic_group"))
        from ..r43 import contract as R43C
        v = R43C.COST_BPS_PER_SIDE.get(g)
        if not isinstance(v, (int, float)):
            v = max(x for x in R43C.COST_BPS_PER_SIDE.values()
                    if isinstance(x, (int, float)))
        cost = cost + turn[c] * (float(v) * float(mults.get(c, 1.0)) / 1e4)
        frac = R43C.FUTURES_MARGIN_FRACTION.get(
            g, max(R43C.FUTURES_MARGIN_FRACTION.values()))
        cap_w = cap_w + pos[c].abs() * float(frac)
    live = cap_w[cap_w > 0]
    committed = float(max(
        R43C.MARGIN_STRESS_BUFFER_MULTIPLIER * float(np.nanmean(live))
        if len(live) else np.nan,
        R43C.MARGIN_FLOOR_FRACTION_OF_GROSS))
    return {"gross": gross, "cost": cost.rename("cost"),
            "turnover": turn.sum(axis=1),
            "committed_capital": committed, "index": pos.index,
            "markets": cols, "n_markets": len(cols),
            "mean_cost_multiplier": float(np.mean(
                [mults.get(c, 1.0) for c in cols]))}


def score(bk: dict, dates=None, *, cost_multiplier: float = 1.0) -> dict:
    book = J.implementable_book(
        bk["gross"], pd.Series(1.0, index=bk["index"]),
        committed_capital=bk["committed_capital"],
        collateral_class="REMUNERATED_MARGIN",
        cost=bk["cost"] * float(cost_multiplier), day_count=J.TRADING_DAYS)
    card = J.score(book, dates, day_count=J.TRADING_DAYS)
    d = book if dates is None else book.reindex(pd.DatetimeIndex(dates))
    card["turnover_ann"] = float(
        np.nanmean(bk["turnover"].reindex(d.index)) * J.TRADING_DAYS)
    card["n_markets"] = bk["n_markets"]
    card["mean_cost_multiplier"] = bk["mean_cost_multiplier"]
    card.pop("diff_stream", None)
    return card


# --------------------------------------------------------------------------- #
# Capacity
# --------------------------------------------------------------------------- #
def capacity(bk: dict, table: pd.DataFrame) -> dict:
    """Capital the book supports at a 1% participation cap.

    Each market can absorb ``cap x its own daily turnover``; the book holds
    it at a weight, so the book's capacity is the smallest capital at which
    ANY market breaches its cap.
    """
    adv = table.set_index("market")["adv_usd"].to_dict()
    per = []
    n = float(bk["n_markets"]) or 1.0
    for m in bk["markets"]:
        a = adv.get(m)
        if not a:
            continue
        weight = 1.0 / n
        per.append({"market": m, "adv_usd": float(a),
                    "capital_at_cap_usd": float(
                        a * C.PARTICIPATION_CAP_OF_DAILY_VOLUME / weight)})
    if not per:
        return {"state": "NOT_MEASURED"}
    per.sort(key=lambda r: r["capital_at_cap_usd"])
    binding = per[0]
    # Two capacities, because they answer different questions. The BINDING
    # one is what the book as actually built can hold - it is bound by its
    # least liquid leg because the book holds every leg equally. The
    # liquidity-weighted one is what the tier could absorb if the book were
    # rebuilt to weight by turnover, which would be a different book.
    lw = sum(r["adv_usd"] for r in per) * \
        C.PARTICIPATION_CAP_OF_DAILY_VOLUME
    return {
        "state": "MEASURED",
        "participation_cap": C.PARTICIPATION_CAP_OF_DAILY_VOLUME,
        "binding_market": binding["market"],
        "capacity_usd": binding["capital_at_cap_usd"],
        "capacity_liquidity_weighted_usd": float(lw),
        "tier_supported": next(
            (t for t in sorted(C.CAPACITY_TIERS_USD, reverse=True)
             if binding["capital_at_cap_usd"] >= t), None),
        "tier_supported_liquidity_weighted": next(
            (t for t in sorted(C.CAPACITY_TIERS_USD, reverse=True)
             if lw >= t), None),
        "tightest_five": per[:5],
    }


# --------------------------------------------------------------------------- #
# Advance - ZONE_B costs burden, ZONE_C needs the inherited pregate
# --------------------------------------------------------------------------- #
#: The advance bar is not chosen here. It is the frozen contract's own
#: ``STANDALONE_ALPHA_GATE["t_min_lock"]``, which existed before any tier
#: was scored, and the lockbox pregate is R43's, inherited unchanged.
ADVANCE_T = C.STANDALONE_ALPHA_GATE["t_min_lock"]
ZONE_C_PREGATE_T = 2.5


def passive_increment(bk: dict, dates) -> dict:
    """The book against a volatility-matched always-long of its OWN markets."""
    panel = KARRY.build_panel(sorted(bk["markets"]))
    if panel is None:
        return {"state": "NOT_RUN"}
    cols = [c for c in bk["markets"] if c in panel["ret"].columns]
    ones = pd.DataFrame(1.0, index=panel["ret"].index, columns=cols)
    pos = KARRY.positions(ones, "CONTINUOUS", panel["ret"][cols],
                          neutralise=False)
    gross = (pos * panel["ret"][cols]).sum(axis=1)
    d = pd.DatetimeIndex(dates)
    cand = ((bk["gross"] - bk["cost"]).reindex(d).dropna()
            / bk["committed_capital"])
    pas = gross.reindex(cand.index).fillna(0.0) / bk["committed_capital"]
    sp, sc = float(np.nanstd(pas)), float(np.nanstd(cand))
    if not sp:
        return {"state": "NOT_RUN", "reason": "passive volatility is zero"}
    matched = pas * (sc / sp)
    inc = cand - matched
    hac = EV.hac_t(inc.to_numpy(dtype=float), lags=21)
    return {
        "state": "MEASURED",
        "control": "VOLATILITY-MATCHED always-long equal-risk book over the "
                   "SAME markets",
        "candidate_excess_ann": float(np.nanmean(cand) * J.TRADING_DAYS),
        "passive_excess_ann_vol_matched": float(
            np.nanmean(matched) * J.TRADING_DAYS),
        "increment_ann": float(np.nanmean(inc) * J.TRADING_DAYS),
        "increment_t_hac": hac.get("t"),
        "signal_is_decoration": bool((hac.get("t") or 0.0) < 2.0),
    }


def advance(screened: list, books: dict, zones: dict,
            table: pd.DataFrame,
            lane: str = "E3_LESS_EFFICIENT_MARKETS") -> list:
    """Every cell at or above the frozen bar goes to ZONE_B and pays burden."""
    from . import burden as B
    out = []
    for r in sorted(screened, key=lambda x: -(x["zone_a_t"] or -9)):
        if (r["zone_a_t"] or -9) < ADVANCE_T:
            continue
        bk = books.get((r["tier"], r["hypothesis"]))
        if bk is None:
            continue
        spec = {"information_family": "LESS_EFFICIENT_MARKETS",
                "asset_family": "MULTI_ASSET_FUTURES_%s" % r["tier"],
                "horizon": "1s",
                "economic_expression": "FUTURES_OUTRIGHT",
                "representation": "%s_%s_CONTINUOUS" % (r["tier"],
                                                        r["hypothesis"]),
                "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R36/R38/R41 multi-asset carry, "
                                      "momentum and value"],
                "validation_touches": 1, "lane": lane}
        try:
            cid = B.record_zone_b(spec, family="LESS_EFFICIENT_MARKETS",
                                  lane=lane)
        except ValueError as exc:
            out.append({"tier": r["tier"], "hypothesis": r["hypothesis"],
                        "state": "LANE_CAP_EXHAUSTED", "detail": str(exc)})
            continue
        b = score(bk, zones["B"])
        stress = {"x%d" % int(m): score(bk, zones["B"], cost_multiplier=m)
                  for m in C.COST_STRESS_MULTIPLIERS}
        inc = passive_increment(bk, zones["B"])
        cap = capacity(bk, table)
        row = {
            "candidate_id": cid, "tier": r["tier"],
            "hypothesis": r["hypothesis"], "spec": spec,
            "zone_a_t": r["zone_a_t"],
            "zone_b_excess_ann": b.get("excess_ann"),
            "zone_b_t": b.get("excess_t_hac"),
            "zone_b_sharpe": b.get("sharpe"),
            "zone_b_max_drawdown": b.get("max_drawdown"),
            "same_sign_a_b": bool(
                (r["zone_a_excess_ann"] or 0) * (b.get("excess_ann") or 0) > 0),
            "cost_stress": {k: {"excess_ann": v.get("excess_ann"),
                                "t": v.get("excess_t_hac")}
                            for k, v in stress.items()},
            "positive_at_2x_cost": bool(
                (stress["x2"].get("excess_ann") or -1) > 0),
            "passive_control": inc,
            "capacity": cap,
            "zone_c_pregate_t": ZONE_C_PREGATE_T,
            "zone_c_opened": False,
        }
        if (b.get("excess_t_hac") or -9) >= ZONE_C_PREGATE_T:
            c = score(bk, zones["C"])
            row["zone_c_opened"] = True
            row["zone_c_excess_ann"] = c.get("excess_ann")
            row["zone_c_t"] = c.get("excess_t_hac")
            row["zone_c_sharpe"] = c.get("sharpe")
            row["zone_c_increment"] = passive_increment(bk, zones["C"])
        out.append(row)
    return out


# --------------------------------------------------------------------------- #
# The lane
# --------------------------------------------------------------------------- #
def run(lane: str = "E3_LESS_EFFICIENT_MARKETS") -> dict:
    table = liquidity_table()
    tt = tiers(table)
    mults = cost_multipliers(table)

    idx = None
    for m in P.available_markets()[:5]:
        d = P.futures_daily(m)
        if d is not None:
            idx = pd.DatetimeIndex(d.index) if idx is None \
                else idx.union(pd.DatetimeIndex(d.index))
    zones = EV.zone_split(idx, embargo=KARRY.EMBARGO)

    screened, books = [], {}
    for tier, markets in tt["tiers"].items():
        for hyp in HYPOTHESES:
            bk = tier_book(markets, hyp, mults)
            if bk is None:
                screened.append({"tier": tier, "hypothesis": hyp,
                                 "state": "INSUFFICIENT_MARKETS"})
                continue
            a = score(bk, zones["A"])
            books[(tier, hyp)] = bk
            screened.append({
                "tier": tier, "hypothesis": hyp, "state": "SCREENED",
                "n_markets": bk["n_markets"],
                "mean_cost_multiplier": bk["mean_cost_multiplier"],
                "zone_a_excess_ann": a.get("excess_ann"),
                "zone_a_t": a.get("excess_t_hac"),
                "zone_a_sharpe": a.get("sharpe"),
                "zone_a_gross_ann": a.get("gross_ann_on_notional"),
                "zone_a_cost_ann": a.get("cost_ann_on_notional"),
                "turnover_ann": a.get("turnover_ann"),
                "capacity": capacity(bk, table),
            })

    ok = [r for r in screened if r.get("state") == "SCREENED"]
    by_tier = {}
    for t in TIER_NAMES:
        rows = [r for r in ok if r["tier"] == t]
        if not rows:
            continue
        by_tier[t] = {
            "n_hypotheses": len(rows),
            "mean_zone_a_excess_ann": float(np.mean(
                [r["zone_a_excess_ann"] for r in rows])),
            "mean_zone_a_t": float(np.mean([r["zone_a_t"] for r in rows])),
            "best_t": max(r["zone_a_t"] for r in rows),
            "n_positive": sum(1 for r in rows if r["zone_a_excess_ann"] > 0),
            "median_capacity_usd": float(np.median(
                [r["capacity"].get("capacity_usd", np.nan) for r in rows])),
        }
    better = None
    if {"LIQUID", "ILLIQUID"} <= set(by_tier):
        better = by_tier["ILLIQUID"]["mean_zone_a_t"] > \
            by_tier["LIQUID"]["mean_zone_a_t"]
    advanced = advance(ok, books, zones, table)
    return {
        "advanced": advanced,
        "lane": lane, "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E3_LESS_EFFICIENT_MARKETS"],
        "liquidity": {
            "n_markets": int(len(table)),
            "tier_cuts_usd": tt["cuts_usd"], "tier_sizes": tt["n"],
            "tiers": tt["tiers"],
            "fx_conversion_is_for_tiering_only": True,
            "n_notional_implausible": int(table["notional_implausible"].sum()),
            "implausible_markets": table.loc[table["notional_implausible"],
                                             "market"].tolist(),
            "least_liquid_ten": table.head(10)[
                ["market", "asset_class", "adv_usd"]].to_dict("records"),
        },
        "cost_scaling": {"exponent": COST_SCALE_EXPONENT,
                         "cap": COST_SCALE_CAP,
                         "declared_before_results": True},
        "zones": {"a_range": zones.get("a_range"),
                  "b_range": zones.get("b_range"),
                  "c_range": zones.get("c_range")},
        "screened_zone_a": screened,
        "by_tier": by_tier,
        "illiquid_beats_liquid": better,
        "_books": books,
        "_zones": zones,
        "_table": table,
    }
