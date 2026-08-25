"""alpha_agent.r45.implementable - Track O. What it is worth, on real capital.

Basis points per event is not an investment result. This turns one into one,
and it keeps Release 43's hard-won correction intact: exchange futures margin
is REMUNERATED. Collateral posted against a futures position earns the
risk-free rate, so the strategy's excess return over cash is the PnL divided
by the committed margin - and subtracting a cash opportunity cost on top of
that, the way an unremunerated crypto-collateral book must, would be
charging the same rent twice. Release 42 learned that the expensive way.

Two capital conventions are always quoted so neither can be chosen after the
fact: traded notional, and 2x initial margin.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r43 import judge as J43
from . import bars as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.implementable"
#: The capital equation belongs to Release 43 and is IMPORTED, never
#: re-derived here. A release that quietly writes its own denominator can
#: quote whatever return it likes.
CAPITAL_EQUATION_OWNER = "alpha_agent.r43.judge"

SLEEVE_COST_GROUP = {
    "RATES": "TREASURY_FUTURES", "EQUITY": "US_INDEX_FUTURES",
    "GOLD": "PRECIOUS_METALS", "FX": "FX_FUTURES", "ENERGY": "ENERGY",
}
SYMBOL_COST_GROUP = {"DEUIDXEUR": "INTL_INDEX_FUTURES",
                     "BUNDTREUR": "INTERNATIONAL_GOVERNMENT"}
COLLATERAL_CLASS = "REMUNERATED_MARGIN"
PARTICIPATION_CAP = 0.01


def cost_group(symbol: str) -> str:
    if symbol in SYMBOL_COST_GROUP:
        return SYMBOL_COST_GROUP[symbol]
    return SLEEVE_COST_GROUP.get(B.sleeve(symbol), "US_INDEX_FUTURES")


def capacity(symbol: str, ev: pd.DataFrame) -> dict:
    """Notional the entry minute can absorb at the declared participation cap."""
    df = B.panel(symbol)
    if df is None or "volume" not in df.columns or ev is None:
        return {"state": "UNKNOWN",
                "why": "the owned broker feed quotes a price and a spread "
                       "but no traded size, so capacity cannot be measured "
                       "from it and is not guessed"}
    vol = pd.to_numeric(df["volume"], errors="coerce")
    px = pd.to_numeric(df["close"], errors="coerce")
    idx = pd.DatetimeIndex(ev["entry_ts"])
    v = vol.reindex(idx).to_numpy(dtype=float)
    p = px.reindex(idx).to_numpy(dtype=float)
    dollars = v * p
    dollars = dollars[np.isfinite(dollars) & (dollars > 0)]
    if dollars.size < 20:
        return {"state": "UNKNOWN", "n": int(dollars.size)}
    med = float(np.median(dollars))
    return {"state": "MEASURED",
            "median_entry_minute_dollar_volume": med,
            "participation_cap": PARTICIPATION_CAP,
            "capacity_usd_per_event": med * PARTICIPATION_CAP,
            "p10_capacity_usd": float(np.percentile(dollars, 10))
            * PARTICIPATION_CAP,
            "note": "one minute of the entry bar only; a real book would "
                    "work the order over several minutes and pay more"}


def score(card: dict, *, symbol: str = None, ev: pd.DataFrame = None,
          legs: int = 1, leg_weights=None) -> dict:
    """Turn a per-event card into an implementable annual result."""
    if not card or card.get("state") != "MEASURED":
        return {"state": "NOT_MEASURABLE"}
    symbol = symbol or card.get("symbol")
    n = int(card["n_events"])
    yr = card.get("year_range") or [0, 0]
    span = max(1.0, float(yr[1] - yr[0]) + 1.0)
    per_year = n / span

    groups = [cost_group(symbol)] * max(1, int(legs))
    cap = J43.futures_committed_capital(groups, leg_weights)
    K = float(cap["committed_capital"])

    net_ann_notional = float(card["net_bps_per_event"]) / 1e4 * per_year
    gross_ann_notional = float(card["gross_bps_per_event"]) / 1e4 * per_year
    cost_ann_notional = float(card["cost_bps_per_event"]) / 1e4 * per_year

    return {
        "state": "MEASURED", "symbol": symbol,
        "calculation_owner": CALCULATION_OWNER,
        "collateral_class": COLLATERAL_CLASS,
        "collateral_note": "futures margin earns the risk-free rate, so the "
                           "excess return over cash is the PnL on committed "
                           "margin and no further cash rent is charged",
        "n_events": n, "years_spanned": span,
        "events_per_year": per_year,
        "gross_annual_return_on_traded_notional": gross_ann_notional,
        "cost_annual_return_on_traded_notional": cost_ann_notional,
        "net_annual_return_on_traded_notional": net_ann_notional,
        "committed_capital_per_leg_unit": K,
        "effective_leverage_on_capital": cap["effective_leverage_on_capital"],
        "net_annual_excess_return_on_committed_margin":
            net_ann_notional / K if K else None,
        "capital_models": {
            "TRADED_NOTIONAL": net_ann_notional,
            "COMMITTED_MARGIN": net_ann_notional / (K / 2.0) if K else None,
            "COMMITTED_MARGIN_X2": net_ann_notional / K if K else None,
        },
        "primary_capital_model": "COMMITTED_MARGIN_X2",
        "max_event_loss_bps": card.get("max_event_loss_bps"),
        "largest_event_share_of_pnl": card.get("largest_event_share_of_pnl"),
        "largest_year_share_of_pnl": card.get("largest_year_share_of_pnl"),
        "capacity": capacity(symbol, ev) if ev is not None else
        {"state": "UNKNOWN"},
        "control": "CASH",
        "beats_control": bool(net_ann_notional > 0),
    }
