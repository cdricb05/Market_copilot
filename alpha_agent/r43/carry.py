"""alpha_agent.r43.carry - Track A: the capital-adjusted carry rejudgment.

Release 42 closed with one instruction for its successor: *price the carry,
do not search for it*. Specifically, re-score the R36 FX carry survivor
(cross-sectional rank IC 0.155, t 7.97) and the R38/R41 futures curve carry
through the implementable-capital instrument, because neither had ever been
subjected to that treatment.

This module does that, and it does it on the ECONOMIC EXPRESSION those
results were actually about: an OUTRIGHT cross-sectional book in front
contracts, long high carry and short low carry - not the calendar spreads
Track F trades. Carry here is the observable annualised front-to-second
calendar slope of each market's own dated curve, which is the futures
market's own statement of what it pays to hold that exposure.

The answer this lane owes the estate is one sentence: **which historical
carry survivors, if any, beat cash on the capital they immobilise** - and,
because R42's real lesson is about denominators, the answer is quoted on
every declared denominator at once.

Burden note: the frozen lane declares FX as its primary family, but a
commodity carry candidate is charged to COMMODITY_CURVE and a rates carry
candidate to RATES_RV. Charging commodity carry to the FX denominator would
launder one family's history into another's, which is exactly what the
burden ledger exists to prevent. The lane's frozen CAP still binds.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import burden as B
from . import judge as J
from . import panels as P
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.carry"

VOL_WIN = 60
Z_WIN = 252
MIN_MARKETS = 4
MIN_OBS = 750
EMBARGO = 21
EXPRESSIONS = {"CONTINUOUS": None, "BAND_15_05": (1.5, 0.5)}

#: Predeclared carry groups, each a distinct economic claim with its own
#: burden family. VX is a single market, so it is a TERM-STRUCTURE carry
#: (front vs second VX), not a cross-sectional one.
GROUPS = {
    "FX_CARRY": {"asset_class": "FX", "family": "FX",
                 "expression": "FX_FUTURES_CARRY",
                 "lineage": "R36 FX carry survivor (rank IC 0.155, t 7.97)"},
    "COMMODITY_CARRY": {"asset_class": "COMMODITY", "family": "COMMODITY_CURVE",
                        "expression": "FUTURES_OUTRIGHT",
                        "lineage": "R38/R41 futures curve carry"},
    "RATES_CARRY": {"asset_class": "RATES", "family": "RATES_RV",
                    "expression": "FUTURES_OUTRIGHT",
                    "lineage": "R39/R41 international carry"},
    "EQUITY_INDEX_CARRY": {"asset_class": "INTERNATIONAL_EQUITY",
                           "family": "CROSS_ASSET",
                           "expression": "FUTURES_OUTRIGHT",
                           "lineage": "R36 index dividend/financing carry"},
    "ALL_ASSET_CARRY": {"asset_class": None, "family": "CROSS_ASSET",
                        "expression": "FUTURES_OUTRIGHT",
                        "lineage": "R36 global multi-asset carry"},
    "VX_TERM_CARRY": {"asset_class": "VOLATILITY", "family":
                      "VOLATILITY_OPTIONS",
                      "expression": "VX_TERM_STRUCTURE",
                      "lineage": "R38 VX carry"},
}


# --------------------------------------------------------------------------- #
# Panels
# --------------------------------------------------------------------------- #
def group_markets(group: str) -> list:
    spec = GROUPS[group]
    ac = spec["asset_class"]
    if ac is None:
        mk = [m for m in P.available_markets()
              if (P.market_registry().get(m) or {}).get("asset_class")
              not in (None, "CRYPTO")]
    else:
        mk = P.markets_by(asset_class=ac)
    out = []
    for m in mk:
        d = P.futures_daily(m)
        if d is None or "slope_ann" not in d.columns:
            continue
        if pd.to_numeric(d["slope_ann"], errors="coerce").notna().sum() \
                >= MIN_OBS:
            out.append(m)
    return sorted(out)


def build_panel(markets: list) -> dict:
    carry = P.field_frame(markets, "slope_ann", min_obs=MIN_OBS)
    ret = P.field_frame(markets, "ret1", min_obs=MIN_OBS)
    cols = [c for c in carry.columns if c in ret.columns]
    if len(cols) < MIN_MARKETS:
        return None
    carry, ret = carry[cols], ret[cols]
    idx = carry.index.union(ret.index).sort_values()
    return {"carry": carry.reindex(idx), "ret": ret.reindex(idx),
            "markets": cols, "index": idx}


def xs_signal(carry: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional standardised carry: on each date, each market's carry
    relative to the cross-section. This is the classic carry expression."""
    n = carry.notna().sum(axis=1)
    mu = carry.mean(axis=1)
    sd = carry.std(axis=1).replace(0.0, np.nan)
    return carry.sub(mu, axis=0).div(sd, axis=0).where(n >= MIN_MARKETS)


def ts_signal(carry: pd.DataFrame) -> pd.DataFrame:
    """Own-level carry, scale-standardised and NOT de-meaned (the level is
    the economic content). Used for the single-market VX term structure."""
    sd = carry.rolling(Z_WIN, min_periods=Z_WIN // 3).std()
    return (carry / sd.replace(0.0, np.nan)).replace([np.inf, -np.inf],
                                                     np.nan)


def positions(z: pd.DataFrame, expression: str, ret: pd.DataFrame, *,
              neutralise: bool = True) -> pd.DataFrame:
    """Risk-scaled positions HELD on each date.

    ``neutralise`` makes the book a RELATIVE claim by removing its net
    exposure. It must be FALSE for the passive control: subtracting the mean
    from a book that is long everything leaves only the differences between
    the markets' risk scales, which is a volatility-relative book and not a
    passive long at all.
    """
    from .rv import apply_expression
    held = {}
    for c in z.columns:
        held[c] = apply_expression(z[c], expression)
    pos = pd.DataFrame(held, index=z.index)
    vol = ret.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
    target = vol.expanding(min_periods=250).median()
    scale = (target / vol.replace(0.0, np.nan)).clip(0.25, 4.0).fillna(1.0)
    pos = pos * scale
    if neutralise:
        net = pos.sum(axis=1)
        active = (pos != 0).sum(axis=1).replace(0, np.nan)
        pos = pos.sub(net.div(active), axis=0).where(pos != 0, 0.0)
    n = pos.abs().sum(axis=1).replace(0.0, np.nan)
    return pos.div(n, axis=0).fillna(0.0)


def book(panel: dict, expression: str, *, mode: str = "XS",
         markets_subset: list = None, extra_lag: int = 0) -> dict:
    carry, ret = panel["carry"], panel["ret"]
    if markets_subset is not None:
        keep = [c for c in markets_subset if c in carry.columns]
        if len(keep) < 2:
            return None
        carry, ret = carry[keep], ret[keep]
    z = xs_signal(carry) if mode == "XS" else ts_signal(carry)
    if extra_lag:
        z = z.shift(extra_lag)
    pos = positions(z, expression, ret)
    if pos.abs().to_numpy().sum() < 10:
        return None
    gross = (pos * ret).sum(axis=1).rename("gross")
    reg = P.market_registry()
    turn_total = pos.diff().abs().fillna(0.0)
    cost = pd.Series(0.0, index=pos.index)
    cap_w = pd.Series(0.0, index=pos.index)
    for c in pos.columns:
        g = (reg.get(c) or {}).get("cost_group") or "COMMODITY_INDEX"
        bps = C.COST_BPS_PER_SIDE.get(g)
        if not isinstance(bps, (int, float)):
            bps = max(v for v in C.COST_BPS_PER_SIDE.values()
                      if isinstance(v, (int, float)))
        cost = cost + turn_total[c] * (float(bps) / 1e4)
        frac = C.FUTURES_MARGIN_FRACTION.get(
            g, max(C.FUTURES_MARGIN_FRACTION.values()))
        cap_w = cap_w + pos[c].abs() * float(frac)
    committed = float(np.nanmax([
        C.MARGIN_STRESS_BUFFER_MULTIPLIER * float(np.nanmean(
            cap_w[cap_w > 0])) if (cap_w > 0).any() else np.nan,
        C.MARGIN_FLOOR_FRACTION_OF_GROSS]))
    return {"gross": gross, "cost": cost.rename("cost"),
            "turnover": turn_total.sum(axis=1),
            "active_fraction": (pos != 0).sum(axis=1) / max(1, pos.shape[1]),
            "committed_capital": committed,
            "gross_notional": float(np.nanmean(pos.abs().sum(axis=1))),
            "index": pos.index, "n_markets": pos.shape[1],
            "markets": list(pos.columns)}


def score(bk: dict, dates=None, *, capital: float = None,
          collateral_class: str = "REMUNERATED_MARGIN",
          cost_multiplier: float = 1.0) -> dict:
    K = float(capital if capital is not None else bk["committed_capital"])
    b = J.implementable_book(
        bk["gross"], pd.Series(1.0, index=bk["index"]),
        committed_capital=K, collateral_class=collateral_class,
        cost=bk["cost"] * float(cost_multiplier), day_count=J.TRADING_DAYS)
    card = J.score(b, dates, day_count=J.TRADING_DAYS)
    d = b if dates is None else b.reindex(pd.DatetimeIndex(dates))
    card["turnover_ann"] = float(
        np.nanmean(bk["turnover"].reindex(d.index)) * J.TRADING_DAYS)
    card["cost_share_of_gross"] = (
        float(card["cost_ann_on_notional"] / card["gross_ann_on_notional"])
        if card.get("gross_ann_on_notional") else None)
    card["n_markets"] = bk["n_markets"]
    return card


def passive_increment(bk: dict, panel: dict, dates) -> dict:
    """The contract's TS_DIRECTIONAL control: a volatility-matched, always-on
    equal-risk long of the SAME markets, and the carry signal's increment."""
    ones = pd.DataFrame(1.0, index=panel["carry"].index,
                        columns=[c for c in bk["markets"]])
    pos = positions(ones, "CONTINUOUS", panel["ret"][bk["markets"]],
                    neutralise=False)
    gross = (pos * panel["ret"][bk["markets"]]).sum(axis=1)
    d = pd.DatetimeIndex(dates)
    cand = (bk["gross"] - bk["cost"]).reindex(d).dropna() \
        / bk["committed_capital"]
    pas = (gross.reindex(cand.index).fillna(0.0)) / bk["committed_capital"]
    sp, sc = float(np.nanstd(pas)), float(np.nanstd(cand))
    if not sp:
        return {"state": "NOT_RUN", "reason": "passive volatility is zero"}
    matched = pas * (sc / sp)
    inc = cand - matched
    hac = EV.hac_t(inc.to_numpy(dtype=float), lags=21)
    return {
        "control": "VOLATILITY-MATCHED always-long equal-risk book over the "
                   "SAME markets",
        "passive_excess_ann_vol_matched": float(
            np.nanmean(matched) * J.TRADING_DAYS),
        "candidate_excess_ann": float(np.nanmean(cand) * J.TRADING_DAYS),
        "increment_ann": float(np.nanmean(inc) * J.TRADING_DAYS),
        "increment_t_hac": hac.get("t"),
        "signal_is_decoration": bool((hac.get("t") or 0) < 2.0),
    }


# --------------------------------------------------------------------------- #
# The lane
# --------------------------------------------------------------------------- #
def run_lane(lane: str = "A_CARRY_REJUDGMENT") -> dict:
    spec_lane = C.LANES[lane]
    groups, screened, advanced = {}, [], []
    for gname, gspec in GROUPS.items():
        mk = group_markets(gname)
        if len(mk) < (1 if gname == "VX_TERM_CARRY" else MIN_MARKETS):
            groups[gname] = {"state": "HISTORICAL_DATA_UNAVAILABLE",
                             "n_markets": len(mk)}
            continue
        panel = build_panel(mk) if gname != "VX_TERM_CARRY" else \
            build_panel(mk + [m for m in P.available_markets()
                              if m == "VX"][:1])
        if panel is None:
            groups[gname] = {"state": "HISTORICAL_DATA_UNAVAILABLE",
                             "n_markets": len(mk)}
            continue
        zones = EV.zone_split(panel["index"], embargo=EMBARGO)
        mode = "TS" if gname == "VX_TERM_CARRY" else "XS"
        rows = []
        for expression in EXPRESSIONS:
            bk = book(panel, expression, mode=mode)
            if bk is None:
                continue
            a = score(bk, zones["A"])
            rows.append({"group": gname, "expression": expression,
                         "mode": mode,
                         "zone_a_excess_ann": a.get("excess_ann"),
                         "zone_a_t": a.get("excess_t_hac"),
                         "zone_a_sharpe": a.get("sharpe"),
                         "zone_a_turnover_ann": a.get("turnover_ann"),
                         "zone_a_cost_share_of_gross":
                             a.get("cost_share_of_gross"),
                         "committed_capital": bk["committed_capital"],
                         "n_markets": bk["n_markets"]})
        screened.extend(rows)
        groups[gname] = {"state": "EXECUTED", "n_markets": len(panel["markets"]),
                         "markets": panel["markets"],
                         "zones": {k: zones["%s_range" % k.lower()]
                                   for k in ("A", "B", "C")},
                         "family": gspec["family"],
                         "lineage": gspec["lineage"],
                         "screened": rows}

    adv = [r for r in screened if r["zone_a_t"] is not None
           and r["zone_a_t"] >= spec_lane["advance_t"]]
    adv.sort(key=lambda r: -r["zone_a_t"])
    adv = adv[:spec_lane["cap"]]

    for r in adv:
        gspec = GROUPS[r["group"]]
        mk = group_markets(r["group"])
        panel = build_panel(mk)
        zones = EV.zone_split(panel["index"], embargo=EMBARGO)
        bk = book(panel, r["expression"], mode=r["mode"])
        spec = {
            "information_family": gspec["family"],
            "asset_family": r["group"],
            "horizon": "1s_hold_to_exit" if r["expression"] != "CONTINUOUS"
            else "1s",
            "economic_expression": gspec["expression"],
            "representation": "CARRY_XS_%s" % r["expression"],
            "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
            "parent_hypotheses": [gspec["lineage"]],
            "validation_touches": 1, "lane": lane,
        }
        cid = B.record_zone_b(spec, family=gspec["family"], lane=lane)
        b_card = score(bk, zones["B"])
        denom = {}
        for name in ("TRADED_NOTIONAL", "COMMITTED_MARGIN",
                     "COMMITTED_MARGIN_X2", "GROSS_EXPOSURE"):
            K = {"TRADED_NOTIONAL": 1.0,
                 "COMMITTED_MARGIN": bk["committed_capital"] / 2.0,
                 "COMMITTED_MARGIN_X2": bk["committed_capital"],
                 "GROSS_EXPOSURE": bk["gross_notional"]}[name]
            c = score(bk, zones["B"], capital=max(K, 1e-6))
            denom[name] = {"committed_capital": K,
                           "return_on_capital_ann":
                               c.get("return_on_capital_ann"),
                           "excess_ann": c.get("excess_ann"),
                           "t": c.get("excess_t_hac"),
                           "sharpe": c.get("sharpe"),
                           "vol_ann": c.get("vol_ann"),
                           "max_drawdown": c.get("max_drawdown"),
                           "is_primary": name == C.PRIMARY_CAPITAL_MODEL}
        rs = J.risk_sized_capital(bk["gross"] - bk["cost"], zones["A"],
                                  floor=bk["committed_capital"])
        if rs.get("committed_capital"):
            c = score(bk, zones["B"], capital=rs["committed_capital"])
            denom["RISK_SIZED_10PCT_VOL"] = {
                "committed_capital": rs["committed_capital"],
                "return_on_capital_ann": c.get("return_on_capital_ann"),
                "excess_ann": c.get("excess_ann"),
                "t": c.get("excess_t_hac"), "sharpe": c.get("sharpe"),
                "vol_ann": c.get("vol_ann"),
                "max_drawdown": c.get("max_drawdown"),
                "binding": rs["binding"],
                "is_primary": False,
                "post_freeze_disclosed": True,
                "note": "capital sized on the FITTING ZONE so the book runs "
                        "at 10%/yr volatility - the only denominator in this "
                        "table an investor could actually survive"}
        advanced.append({
            "candidate_id": cid, "group": r["group"], "spec": spec,
            "expression": r["expression"], "zone_a_t": r["zone_a_t"],
            "zone_b": {k: v for k, v in b_card.items() if k != "diff_stream"},
            "gate": _gate(b_card),
            "denominator_table": denom,
            "passive_increment": passive_increment(bk, panel, zones["B"]),
            "committed_capital": bk["committed_capital"],
        })

    return {"lane": lane, "state": "EXECUTED",
            "question": spec_lane["question"],
            "groups": groups, "screened": screened, "advanced": advanced,
            "advance_rule": {"advance_t": spec_lane["advance_t"],
                             "cap": spec_lane["cap"], "signed": True,
                             "frozen": True},
            "collateral_class": "REMUNERATED_MARGIN",
            "capital_finding": _capital_finding(advanced),
            "burden_families_used": sorted({a["spec"]["information_family"]
                                            for a in advanced}),
            }


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


def _capital_finding(advanced: list) -> dict:
    """The answer to R42's question, stated as arithmetic rather than
    rhetoric: what does the capital correction actually do to a
    margin-financed carry book?"""
    rows = []
    for a in advanced:
        d = a["denominator_table"]
        tn, cm = d["TRADED_NOTIONAL"], d[C.PRIMARY_CAPITAL_MODEL]
        rows.append({
            "candidate_id": a["candidate_id"], "group": a["group"],
            "excess_ann_on_notional": tn["excess_ann"],
            "excess_ann_on_committed_capital": cm["excess_ann"],
            "leverage": (cm["excess_ann"] / tn["excess_ann"]
                         if tn["excess_ann"] else None),
            "t_on_notional": tn["t"], "t_on_committed_capital": cm["t"],
            "t_unchanged": bool(tn["t"] is not None and cm["t"] is not None
                                and abs(tn["t"] - cm["t"]) < 1e-6),
        })
    return {
        "claim": "for a REMUNERATED_MARGIN book the capital correction is a "
                 "RESCALE: it multiplies gross, cost and the return on "
                 "capital by the same factor and leaves the t-statistic and "
                 "the Sharpe ratio EXACTLY unchanged. R42's kill therefore "
                 "does NOT transfer from crypto to exchange-traded futures - "
                 "and nothing R41 killed on significance can be resurrected "
                 "by re-quoting it either.",
        "rows": rows,
        "all_t_unchanged": all(r["t_unchanged"] for r in rows) if rows
        else None,
    }
