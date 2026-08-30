r"""engine/instrument_contract.py - Release 50: the ONE canonical asset-agnostic
POSITION CONTRACT and the instrument-valuation semantics behind it (pure).

The operational portfolio used to be a list of ``{ticker, quantity}`` pairs valued
as ``quantity x adjusted close``. That arithmetic is correct for a US cash equity
and wrong for everything else the estate owns data for: a futures contract has a
point value, an initial margin and a currency; an FX position is a currency
conversion; a crypto contract is a future. Release 50 states the valuation
semantics ONCE, here, as data and pure functions, and every other owner (the desk
NAV replay, the capital pool, the risk state, the frontier, the order plan, the
settlement engine, the decision evidence) reads them from here.

What a position IS, in one contract::

    instrument_id, sleeve_id, asset_class, instrument_type
    quantity, unit_type
    mark, mark_currency, multiplier, currency, fx_to_usd
    market_value_usd, notional_usd, capital_usage_usd, collateral_usd
    unrealized_pnl_usd, realized_pnl_usd
    exposure_weight, capital_usage_weight
    execution_convention, settlement_semantics, collateral_semantics

Valuation semantics, declared (reporting currency USD):

* CASH_EQUITY / FX_SPOT / CRYPTO_SPOT - fully cash-settled at fill. Market value
  is the notional; capital usage is the notional; no collateral.
* FUTURE - NOT valued like a cash equity. The contract's NOTIONAL is
  ``quantity x mark x multiplier x fx``; its contribution to NAV is the
  UNREALISED variation ``quantity x (mark - entry_mark) x multiplier x fx``; the
  cash it encumbers is the initial margin (``collateral_usd``), which is capital
  usage but NOT a cash outflow. Long-only in this release (declared).
* CASH - USD at par; zero return by the declared paper policy.

Backwards compatibility: a fill / order / holding row that carries no
``instrument`` block IS a US cash equity (multiplier 1, USD, fully cash settled),
so every historical ledger row values exactly as before. Historical ledgers are
never rewritten.

Pure stdlib. No I/O, no clock, no provider, no write.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

PHASE = "R50"
CALCULATION_OWNER = "engine.instrument_contract"
SCHEMA_VERSION = "multi_asset_position.v1"
DESCRIPTOR_SCHEMA_VERSION = "instrument_descriptor.v1"
REPORTING_CURRENCY = "USD"

# --------------------------------------------------------------------------- #
# Vocabularies (frozen; every consumer renders these words and nothing else)
# --------------------------------------------------------------------------- #
IT_CASH_EQUITY = "CASH_EQUITY"
IT_FUTURE = "FUTURE"
IT_FX_SPOT = "FX_SPOT"
IT_CRYPTO_SPOT = "CRYPTO_SPOT"
IT_CASH = "CASH"
INSTRUMENT_TYPES = (IT_CASH_EQUITY, IT_FUTURE, IT_FX_SPOT, IT_CRYPTO_SPOT, IT_CASH)

UNIT_TYPE = {
    IT_CASH_EQUITY: "SHARES",
    IT_FUTURE: "CONTRACTS",
    IT_FX_SPOT: "BASE_CURRENCY_UNITS",
    IT_CRYPTO_SPOT: "COINS",
    IT_CASH: "USD",
}

AC_US_EQUITY = "US_EQUITY"
AC_EQUITY_INDEX_FUTURES = "EQUITY_INDEX_FUTURES"
AC_INTL_EQUITY_INDEX_FUTURES = "INTERNATIONAL_EQUITY_INDEX_FUTURES"
AC_RATES_FUTURES = "RATES_FUTURES"
AC_COMMODITY_FUTURES = "COMMODITY_FUTURES"
AC_VOLATILITY_FUTURES = "VOLATILITY_FUTURES"
AC_FX_FUTURES = "FX_FUTURES"
AC_FX_SPOT = "FX_SPOT"
AC_CRYPTO_FUTURES = "CRYPTO_FUTURES"
AC_CASH = "CASH"
ASSET_CLASSES = (AC_US_EQUITY, AC_EQUITY_INDEX_FUTURES, AC_INTL_EQUITY_INDEX_FUTURES,
                 AC_RATES_FUTURES, AC_COMMODITY_FUTURES, AC_VOLATILITY_FUTURES,
                 AC_FX_FUTURES, AC_FX_SPOT, AC_CRYPTO_FUTURES, AC_CASH)

ASSET_CLASS_LABELS = {
    AC_US_EQUITY: "US Equities",
    AC_EQUITY_INDEX_FUTURES: "Equity Index Futures",
    AC_INTL_EQUITY_INDEX_FUTURES: "International Index Futures",
    AC_RATES_FUTURES: "Rates Futures",
    AC_COMMODITY_FUTURES: "Commodity Futures",
    AC_VOLATILITY_FUTURES: "Volatility Futures",
    AC_FX_FUTURES: "FX Futures",
    AC_FX_SPOT: "FX Spot",
    AC_CRYPTO_FUTURES: "Crypto Futures",
    AC_CASH: "Cash",
}

#: The ONE governed, asset-aware execution convention. For a US cash equity it is
#: exactly the desk's NEXT_CLOSE; for every other instrument it is the instrument's
#: OWN first completed daily settlement strictly after the marks known at approval.
#: Same no-hindsight guard, same settlement owner, no ad-hoc timing.
EXECUTION_CONVENTION = "NEXT_SESSION_SETTLEMENT"
EXECUTION_CONVENTION_BY_TYPE = {
    IT_CASH_EQUITY: "NEXT_CLOSE",
    IT_FUTURE: "NEXT_SESSION_SETTLEMENT",
    IT_FX_SPOT: "NEXT_SESSION_SETTLEMENT",
    IT_CRYPTO_SPOT: "NEXT_SESSION_SETTLEMENT",
    IT_CASH: "NONE",
}
SETTLEMENT_SEMANTICS = {
    IT_CASH_EQUITY: "FULL_NOTIONAL_CASH_SETTLED_AT_FILL",
    IT_FUTURE: "DAILY_MARK_TO_MARKET_UNREALISED_VARIATION_REALISED_AT_CLOSE",
    IT_FX_SPOT: "FULL_NOTIONAL_CASH_CONVERTED_AT_FILL",
    IT_CRYPTO_SPOT: "FULL_NOTIONAL_CASH_SETTLED_AT_FILL",
    IT_CASH: "NONE",
}
COLLATERAL_SEMANTICS = {
    IT_CASH_EQUITY: "NONE_FULLY_PAID",
    IT_FUTURE: "INITIAL_MARGIN_ENCUMBERS_CASH_NOT_A_CASH_OUTFLOW",
    IT_FX_SPOT: "NONE_FULLY_PAID",
    IT_CRYPTO_SPOT: "NONE_FULLY_PAID",
    IT_CASH: "NONE",
}
EXECUTION_CONVENTION_DOC = {
    "convention": EXECUTION_CONVENTION,
    "by_instrument_type": dict(EXECUTION_CONVENTION_BY_TYPE),
    "decision_timestamp": ("the UTC instant the second manual confirmation creates the "
                           "paper orders (recorded on every order transition)"),
    "eligible_execution_session": ("the instrument's OWN first completed daily session with "
                                   "a settlement mark strictly after the desk mark store's "
                                   "latest date at approval (never a mark already known)"),
    "mark_fill_convention": ("fill at the recorded owned settlement / close of that session; "
                             "a US equity fills at the owned adjusted close (NEXT_CLOSE), a "
                             "future at its owned daily settlement, an FX spot at its owned "
                             "daily close"),
    "settlement_collateral_semantics": {
        "settlement": dict(SETTLEMENT_SEMANTICS),
        "collateral": dict(COLLATERAL_SEMANTICS),
    },
    "one_convention": ("ONE governed, asset-aware convention; no instrument carries ad-hoc "
                       "timing, and the equity convention is unchanged (NEXT_CLOSE)."),
}

#: Long-only is a DECLARED limitation of this release, not an oversight.
SHORT_EXPOSURE_SUPPORTED = False

#: Cash earns zero on the paper book - the SAME declared policy as
#: ``engine.zero_base_allocator.CASH_RETURN`` and the decision-outcome kernel. It
#: is restated here because futures collateral is cash: pricing it at anything
#: else would let a collateralised sleeve "beat" a fully-paid one by fiat.
CASH_RETURN = 0.0
CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"
COLLATERAL_FINANCING_POLICY = "COLLATERAL_EARNS_THE_DECLARED_CASH_RETURN_ZERO"

# --------------------------------------------------------------------------- #
# Cost policy - DECLARED, conservative, per asset class. One owner.
#
# The US-equity number IS the desk's canonical 12.5 bps per side (Phase 27A) and is
# bound to it at the read seam by the API owner so the two can never diverge. The
# non-equity numbers are restated from the Release-46 research cost contract
# (base per side + 1 bp slippage, deliberately conservative) as an OPERATIONAL
# policy; they are declared here once, folded into every hash that prices a
# switch, and are never tuned on realised outcomes. Holding costs (roll /
# financing) are annualised bps on notional and are reported, never silently
# charged to a mark.
# --------------------------------------------------------------------------- #
COST_POLICY_VERSION = "multi_asset_cost_policy.v1"
COST_BPS_PER_SIDE_BY_CLASS = {
    AC_US_EQUITY: 12.5,                 # desk.COST_BPS_PER_SIDE (canonical; bound live)
    AC_EQUITY_INDEX_FUTURES: 2.0,
    AC_INTL_EQUITY_INDEX_FUTURES: 2.5,
    AC_RATES_FUTURES: 1.75,
    AC_COMMODITY_FUTURES: 3.5,
    AC_VOLATILITY_FUTURES: 13.0,
    AC_FX_FUTURES: 2.0,
    AC_FX_SPOT: 2.5,
    AC_CRYPTO_FUTURES: 6.0,
    AC_CASH: 0.0,
}
HOLDING_COST_BPS_ANNUAL_BY_CLASS = {
    AC_US_EQUITY: 0.0,
    AC_EQUITY_INDEX_FUTURES: 8.0,
    AC_INTL_EQUITY_INDEX_FUTURES: 8.0,
    AC_RATES_FUTURES: 6.0,
    AC_COMMODITY_FUTURES: 21.0,
    AC_VOLATILITY_FUTURES: 156.0,
    AC_FX_FUTURES: 8.0,
    AC_FX_SPOT: 25.0,
    AC_CRYPTO_FUTURES: 100.0,
    AC_CASH: 0.0,
}
COST_POLICY_PROVENANCE = (
    "US_EQUITY = the canonical paper desk cost (12.5 bps/side, Phase 27A). Non-equity "
    "classes are restated from the Release-46 research cost contract (base per side + "
    "1 bp slippage, conservative) as operational policy. Declared once, never tuned "
    "on outcomes; holding costs are reported, never charged to a mark.")

DEFAULT_EQUITY_SLEEVE = "us_equity_fundamental_momentum_50_50_v1"
CASH_INSTRUMENT_ID = "USD_CASH"
CASH_SLEEVE = "cash_usd"

_TRADING_DAYS_YEAR = 252.0


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")).hexdigest()[:32]


def is_cash_settled(instrument_type: str) -> bool:
    return instrument_type in (IT_CASH_EQUITY, IT_FX_SPOT, IT_CRYPTO_SPOT, IT_CASH)


# --------------------------------------------------------------------------- #
# Descriptors
# --------------------------------------------------------------------------- #
def describe(instrument_id: str, *, asset_class: str, instrument_type: str,
             sleeve_id: str, currency: str = REPORTING_CURRENCY,
             multiplier: float = 1.0, initial_margin_per_unit: float = 0.0,
             tick_size: Optional[float] = None, label: Optional[str] = None,
             sector: Optional[str] = None, cost_bps_per_side: Optional[float] = None,
             holding_cost_bps_annual: Optional[float] = None,
             mark_owner: Optional[str] = None,
             reference_data_owner: Optional[str] = None) -> dict:
    """ONE instrument descriptor. Every downstream owner reads instrument
    semantics from this object, never from a ticker's spelling."""
    if instrument_type not in INSTRUMENT_TYPES:
        raise ValueError("unknown instrument_type %r" % (instrument_type,))
    if asset_class not in ASSET_CLASSES:
        raise ValueError("unknown asset_class %r" % (asset_class,))
    cost = (_f(cost_bps_per_side) if cost_bps_per_side is not None
            else float(COST_BPS_PER_SIDE_BY_CLASS[asset_class]))
    hold = (_f(holding_cost_bps_annual) if holding_cost_bps_annual is not None
            else float(HOLDING_COST_BPS_ANNUAL_BY_CLASS[asset_class]))
    return {
        "descriptor_schema_version": DESCRIPTOR_SCHEMA_VERSION,
        "instrument_id": str(instrument_id),
        "label": label or str(instrument_id),
        "asset_class": asset_class,
        "asset_class_label": ASSET_CLASS_LABELS.get(asset_class, asset_class),
        "instrument_type": instrument_type,
        "sleeve_id": sleeve_id,
        "sector": sector or (ASSET_CLASS_LABELS.get(asset_class)
                             if asset_class != AC_US_EQUITY else None),
        "currency": str(currency or REPORTING_CURRENCY).upper(),
        "unit_type": UNIT_TYPE[instrument_type],
        "multiplier": float(multiplier if multiplier is not None else 1.0),
        "initial_margin_per_unit": float(initial_margin_per_unit or 0.0),
        "tick_size": _f(tick_size),
        "execution_convention": EXECUTION_CONVENTION_BY_TYPE[instrument_type],
        "settlement_semantics": SETTLEMENT_SEMANTICS[instrument_type],
        "collateral_semantics": COLLATERAL_SEMANTICS[instrument_type],
        "cost_bps_per_side": cost,
        "holding_cost_bps_annual": hold,
        "cost_policy_version": COST_POLICY_VERSION,
        "mark_owner": mark_owner or ("api.paper_trading_desk (owned EODHD adjusted close)"
                                     if instrument_type == IT_CASH_EQUITY else
                                     "api.paper_trading_desk via api.market_reference_data"
                                     " (owned Norgate daily settlement)"),
        "reference_data_owner": reference_data_owner or (
            "api.universe_scoring" if instrument_type == IT_CASH_EQUITY
            else "api.market_reference_data"),
        "long_only": True,
        "short_exposure_supported": SHORT_EXPOSURE_SUPPORTED,
        "calculation_owner": CALCULATION_OWNER,
    }


def equity_descriptor(ticker: str, *, sleeve_id: str = DEFAULT_EQUITY_SLEEVE,
                      sector: Optional[str] = None,
                      cost_bps_per_side: Optional[float] = None) -> dict:
    """The backwards-compatible descriptor of a US cash equity holding."""
    return describe(ticker, asset_class=AC_US_EQUITY, instrument_type=IT_CASH_EQUITY,
                    sleeve_id=sleeve_id, sector=sector,
                    cost_bps_per_side=cost_bps_per_side)


def cash_descriptor() -> dict:
    return describe(CASH_INSTRUMENT_ID, asset_class=AC_CASH, instrument_type=IT_CASH,
                    sleeve_id=CASH_SLEEVE, label="USD cash")


def descriptor_from_row(row: Optional[dict], *, default_ticker: Optional[str] = None,
                        cost_bps_per_side: Optional[float] = None) -> dict:
    """Recover a descriptor from a ledger row's embedded ``instrument`` block.

    A row with NO block is a US cash equity - the pre-Release-50 contract - so
    every historical order, fill and holding values exactly as it always did.
    """
    row = row or {}
    blk = row.get("instrument") if isinstance(row.get("instrument"), dict) else None
    tk = (blk or {}).get("instrument_id") or row.get("ticker") or default_ticker
    if not blk or not blk.get("instrument_type") or blk.get("instrument_type") == IT_CASH_EQUITY:
        return equity_descriptor(str(tk), sleeve_id=(blk or {}).get("sleeve_id") or DEFAULT_EQUITY_SLEEVE,
                                 sector=row.get("sector"), cost_bps_per_side=cost_bps_per_side)
    return describe(
        str(tk), asset_class=blk.get("asset_class") or AC_EQUITY_INDEX_FUTURES,
        instrument_type=blk["instrument_type"],
        sleeve_id=blk.get("sleeve_id") or blk.get("asset_class") or "unknown_sleeve",
        currency=blk.get("currency") or REPORTING_CURRENCY,
        multiplier=_f(blk.get("multiplier")) or 1.0,
        initial_margin_per_unit=_f(blk.get("initial_margin_per_unit")) or 0.0,
        tick_size=blk.get("tick_size"), label=blk.get("label"),
        cost_bps_per_side=blk.get("cost_bps_per_side"),
        holding_cost_bps_annual=blk.get("holding_cost_bps_annual"))


def instrument_block(descriptor: dict, *, fx_to_usd: Optional[float] = None) -> dict:
    """The compact, immutable block embedded in an order / fill / plan row so that
    replaying a ledger never depends on live reference data."""
    d = descriptor
    return {
        "instrument_id": d["instrument_id"],
        "instrument_type": d["instrument_type"],
        "asset_class": d["asset_class"],
        "sleeve_id": d["sleeve_id"],
        "currency": d["currency"],
        "unit_type": d["unit_type"],
        "multiplier": d["multiplier"],
        "initial_margin_per_unit": d["initial_margin_per_unit"],
        "tick_size": d.get("tick_size"),
        "cost_bps_per_side": d["cost_bps_per_side"],
        "holding_cost_bps_annual": d["holding_cost_bps_annual"],
        "execution_convention": d["execution_convention"],
        "settlement_semantics": d["settlement_semantics"],
        "collateral_semantics": d["collateral_semantics"],
        "fx_to_usd_at_record": _f(fx_to_usd),
        "descriptor_schema_version": DESCRIPTOR_SCHEMA_VERSION,
    }


# --------------------------------------------------------------------------- #
# Unit economics
# --------------------------------------------------------------------------- #
def unit_notional_usd(descriptor: dict, mark: Optional[float],
                      fx_to_usd: Optional[float] = 1.0) -> Optional[float]:
    m, fx = _f(mark), (_f(fx_to_usd) if fx_to_usd is not None else 1.0)
    if m is None or fx is None:
        return None
    if descriptor["instrument_type"] == IT_CASH:
        return 1.0
    return m * float(descriptor["multiplier"]) * fx


def capital_usage_ratio(descriptor: dict, mark: Optional[float],
                        fx_to_usd: Optional[float] = 1.0) -> Optional[float]:
    """Cash encumbered per dollar of notional exposure: 1.0 for a fully-paid
    instrument, the initial-margin ratio for a future."""
    if descriptor["instrument_type"] != IT_FUTURE:
        return 1.0
    un = unit_notional_usd(descriptor, mark, fx_to_usd)
    fx = _f(fx_to_usd) if fx_to_usd is not None else 1.0
    if not un or un <= 0 or fx is None:
        return None
    return float(descriptor["initial_margin_per_unit"]) * fx / un


def units_for_notional(descriptor: dict, target_notional_usd: Optional[float],
                       mark: Optional[float], fx_to_usd: Optional[float] = 1.0) -> int:
    """Whole units (shares / contracts) that a target notional buys at ``mark``."""
    un = unit_notional_usd(descriptor, mark, fx_to_usd)
    t = _f(target_notional_usd)
    if un is None or un <= 0 or t is None or t <= 0:
        return 0
    return int(math.floor(t / un))


def cost_rate_per_side(descriptor: dict) -> float:
    return float(descriptor.get("cost_bps_per_side") or 0.0) / 10000.0


def transaction_cost_usd(descriptor: dict, units: float, mark: Optional[float],
                         fx_to_usd: Optional[float] = 1.0) -> Optional[float]:
    un = unit_notional_usd(descriptor, mark, fx_to_usd)
    if un is None:
        return None
    return abs(float(units)) * un * cost_rate_per_side(descriptor)


def fill_cash_delta(descriptor: dict, *, side_is_buy: bool, units: float,
                    price: float, fx_to_usd: Optional[float] = 1.0,
                    entry_reference_price: Optional[float] = None,
                    cost_rate: Optional[float] = None) -> dict:
    """The cash effect of ONE paper fill, by instrument semantics.

    * fully-paid instruments: BUY pays ``gross + cost``; SELL receives ``gross - cost``;
    * FUTURE: opening pays only the transaction cost (the notional is not cash);
      closing realises ``units x (price - entry) x multiplier x fx`` minus cost.
    """
    units = abs(float(units))
    fx = _f(fx_to_usd) if fx_to_usd is not None else 1.0
    un = unit_notional_usd(descriptor, price, fx) or 0.0
    gross = units * un
    rate = float(cost_rate) if cost_rate is not None else cost_rate_per_side(descriptor)
    cost = gross * rate
    realized = 0.0
    if descriptor["instrument_type"] == IT_FUTURE:
        if side_is_buy:
            delta = -cost
        else:
            entry = _f(entry_reference_price)
            realized = (units * (float(price) - (entry if entry is not None else float(price)))
                        * float(descriptor["multiplier"]) * fx)
            delta = realized - cost
    else:
        delta = -(gross + cost) if side_is_buy else (gross - cost)
    return {"gross_value": gross, "transaction_cost": cost, "net_cash_delta": delta,
            "realized_pnl": realized, "notional_usd": gross,
            "cost_rate_per_side": rate, "fx_to_usd": fx,
            "instrument_type": descriptor["instrument_type"]}


def replay_entry_marks(fills: list, *, book_id: Optional[str] = None,
                       up_to_date: Optional[str] = None) -> dict:
    """Average entry price per OPEN futures position, replayed from immutable fills.

    Opening fills (PAPER_BUY) move the average; closing fills (PAPER_SELL) reduce
    the open quantity and leave the average unchanged. Returns
    ``{instrument_id: {"quantity", "average_entry_price", "multiplier", "currency"}}``
    for instruments still open. Pure replay; never writes.
    """
    state: dict[str, dict] = {}
    for f in sorted(fills or [], key=lambda x: (x.get("fill_date") or "", x.get("fill_id") or "")):
        if book_id is not None and f.get("book_id") != book_id:
            continue
        if up_to_date is not None and (f.get("fill_date") or "") > up_to_date:
            continue
        d = descriptor_from_row(f)
        if d["instrument_type"] != IT_FUTURE:
            continue
        tk = d["instrument_id"]
        try:
            q = float(f.get("quantity") or 0.0)
            px = float(f.get("fill_price"))
        except (TypeError, ValueError):
            continue
        s = state.setdefault(tk, {"quantity": 0.0, "average_entry_price": None,
                                  "multiplier": d["multiplier"], "currency": d["currency"]})
        if str(f.get("side") or "").endswith("BUY"):
            q0 = s["quantity"]
            a0 = s["average_entry_price"] if s["average_entry_price"] is not None else px
            s["average_entry_price"] = ((a0 * q0 + px * q) / (q0 + q)) if (q0 + q) > 0 else px
            s["quantity"] = q0 + q
        else:
            s["quantity"] = s["quantity"] - q
            if s["quantity"] <= 0:
                s["quantity"] = 0.0
                s["average_entry_price"] = None
    return {k: v for k, v in state.items() if v["quantity"] > 0}


# --------------------------------------------------------------------------- #
# The position contract
# --------------------------------------------------------------------------- #
def value_position(descriptor: dict, *, quantity: float, mark: Optional[float],
                   fx_to_usd: Optional[float] = 1.0, entry_mark: Optional[float] = None,
                   cost_basis_usd: Optional[float] = None, nav: Optional[float] = None,
                   realized_pnl_usd: Optional[float] = None,
                   liquidity_state: Optional[str] = None,
                   capacity: Optional[dict] = None,
                   risk_contribution: Optional[float] = None) -> dict:
    """Value ONE position under its declared semantics. Pure.

    ``mark`` is in the instrument's own currency; ``fx_to_usd`` converts it. A
    missing mark yields ``None`` values and a named gap, never a fabricated zero.
    """
    d = descriptor
    it = d["instrument_type"]
    q = float(quantity or 0.0)
    m = _f(mark)
    # USD is identity; a NON-USD instrument without a rate is a named gap, never 1.0.
    fx = 1.0 if d["currency"] == REPORTING_CURRENCY else _f(fx_to_usd)
    gaps: list[str] = []
    mult = float(d["multiplier"])
    notional = market_value = capital_usage = collateral = unreal = None
    if it == IT_CASH:
        notional = market_value = capital_usage = q
        collateral = 0.0
        unreal = 0.0
    elif m is None or fx is None:
        gaps.append("MARK_UNAVAILABLE" if m is None else "FX_UNAVAILABLE")
    elif it == IT_FUTURE:
        notional = q * m * mult * fx
        e = _f(entry_mark)
        if e is None:
            gaps.append("ENTRY_MARK_UNAVAILABLE")
            unreal = None
        else:
            unreal = q * (m - e) * mult * fx
        market_value = unreal
        collateral = q * float(d["initial_margin_per_unit"]) * fx
        capital_usage = collateral
    else:
        notional = q * m * mult * fx
        market_value = notional
        capital_usage = notional
        collateral = 0.0
        cb = _f(cost_basis_usd)
        unreal = (market_value - cb) if cb is not None else None
        if cb is None:
            gaps.append("COST_BASIS_UNAVAILABLE")
    navv = _f(nav)
    exposure_w = (notional / navv) if (notional is not None and navv) else None
    usage_w = (capital_usage / navv) if (capital_usage is not None and navv) else None
    return {
        "schema_version": SCHEMA_VERSION,
        "instrument_id": d["instrument_id"],
        "label": d.get("label"),
        "sleeve_id": d["sleeve_id"],
        "asset_class": d["asset_class"],
        "asset_class_label": d.get("asset_class_label"),
        "instrument_type": it,
        "sector": d.get("sector"),
        "quantity": q,
        "unit_type": d["unit_type"],
        "mark": _r(m, 6),
        "mark_currency": d["currency"],
        "entry_mark": _r(_f(entry_mark), 6),
        "multiplier": mult,
        "currency": d["currency"],
        "fx_to_usd": _r(fx, 8) if fx is not None else None,
        "reporting_currency": REPORTING_CURRENCY,
        "market_value_usd": _money(market_value),
        "notional_usd": _money(notional),
        "capital_usage_usd": _money(capital_usage),
        "collateral_usd": _money(collateral),
        "unrealized_pnl_usd": _money(unreal),
        "realized_pnl_usd": _money(_f(realized_pnl_usd)),
        "exposure_weight": _r(exposure_w, 6),
        "capital_usage_weight": _r(usage_w, 6),
        "cost_basis_usd": _money(_f(cost_basis_usd)),
        "risk_contribution": _r(_f(risk_contribution), 6),
        "liquidity_state": liquidity_state,
        "capacity": dict(capacity or {}),
        "execution_convention": d["execution_convention"],
        "settlement_semantics": d["settlement_semantics"],
        "collateral_semantics": d["collateral_semantics"],
        "valuation_gaps": gaps,
        "valuation_basis": ("VARIATION_MARGIN_UNREALISED" if it == IT_FUTURE
                            else "FULL_NOTIONAL"),
        "calculation_owner": CALCULATION_OWNER,
    }


def aggregate_exposures(positions: list, *, nav: Optional[float],
                        cash: Optional[float] = None) -> dict:
    """Gross / net exposure and the asset-class / sleeve / currency breakdown of a
    list of position contracts, in NAV weight. Long-only: net == gross."""
    navv = _f(nav)
    by_class: dict[str, float] = {}
    by_sleeve: dict[str, float] = {}
    by_ccy: dict[str, float] = {}
    gross = 0.0
    net = 0.0
    collateral = 0.0
    usage = 0.0
    n_unpriced = 0
    for p in positions or []:
        w = _f(p.get("exposure_weight"))
        if w is None:
            n_unpriced += 1
            continue
        gross += abs(w)
        net += w
        by_class[p.get("asset_class") or "UNKNOWN"] = by_class.get(p.get("asset_class") or "UNKNOWN", 0.0) + w
        by_sleeve[p.get("sleeve_id") or "UNKNOWN"] = by_sleeve.get(p.get("sleeve_id") or "UNKNOWN", 0.0) + w
        by_ccy[p.get("currency") or REPORTING_CURRENCY] = by_ccy.get(p.get("currency") or REPORTING_CURRENCY, 0.0) + w
        collateral += _f(p.get("collateral_usd")) or 0.0
        usage += _f(p.get("capital_usage_usd")) or 0.0
    c = _f(cash)
    cash_w = (c / navv) if (c is not None and navv) else None
    return {
        "gross_exposure": _r(gross, 6),
        "net_exposure": _r(net, 6),
        "by_asset_class": {k: _r(v, 6) for k, v in sorted(by_class.items())},
        "by_sleeve": {k: _r(v, 6) for k, v in sorted(by_sleeve.items())},
        "by_currency": {k: _r(v, 6) for k, v in sorted(by_ccy.items())},
        "non_usd_exposure": _r(sum(v for k, v in by_ccy.items() if k != REPORTING_CURRENCY), 6),
        "collateral_usd": _money(collateral),
        "capital_usage_usd": _money(usage),
        "collateral_weight": _r((collateral / navv) if navv else None, 6),
        "cash_weight": _r(cash_w, 6),
        "free_cash_usd": _money((c - collateral) if c is not None else None),
        "unpriced_positions": n_unpriced,
        "long_only": True,
    }


def allocation_by_group(weights: dict, group_of: dict, *, group_key: str = "asset_class",
                        cash_group: str = AC_CASH) -> dict:
    """Aggregate a weight vector by a grouping map. Cash is ``1 - sum(w)``. Only
    groups that actually carry weight are returned - never a cosmetic 0% row."""
    out: dict[str, float] = {}
    total = 0.0
    for tk, w in (weights or {}).items():
        wv = _f(w) or 0.0
        if wv <= 0:
            continue
        g = (group_of or {}).get(tk) or "UNKNOWN"
        out[g] = out.get(g, 0.0) + wv
        total += wv
    cash = max(0.0, 1.0 - total)
    if cash > 1e-9:
        out[cash_group] = cash
    return {k: _r(v, 6) for k, v in sorted(out.items(), key=lambda kv: (-kv[1], kv[0]))}


def safety_block() -> dict:
    return {"paper_only": True, "read_only": True, "pure": True,
            "creates_orders": False, "creates_fills": False,
            "mutates_holdings": False, "mutates_cash": False,
            "broker_enabled": False, "automation_enabled": False,
            "short_exposure_supported": SHORT_EXPOSURE_SUPPORTED,
            "safety_badges": ["PAPER ONLY", "READ ONLY", "NO ORDERS", "NO BROKER",
                              "AUTOMATION OFF", "MANUAL REVIEW"]}


__all__ = [
    "PHASE", "CALCULATION_OWNER", "SCHEMA_VERSION", "DESCRIPTOR_SCHEMA_VERSION",
    "REPORTING_CURRENCY", "INSTRUMENT_TYPES", "ASSET_CLASSES", "ASSET_CLASS_LABELS",
    "IT_CASH_EQUITY", "IT_FUTURE", "IT_FX_SPOT", "IT_CRYPTO_SPOT", "IT_CASH",
    "AC_US_EQUITY", "AC_EQUITY_INDEX_FUTURES", "AC_INTL_EQUITY_INDEX_FUTURES",
    "AC_RATES_FUTURES", "AC_COMMODITY_FUTURES", "AC_VOLATILITY_FUTURES",
    "AC_FX_FUTURES", "AC_FX_SPOT", "AC_CRYPTO_FUTURES", "AC_CASH",
    "UNIT_TYPE", "EXECUTION_CONVENTION", "EXECUTION_CONVENTION_BY_TYPE",
    "EXECUTION_CONVENTION_DOC", "SETTLEMENT_SEMANTICS", "COLLATERAL_SEMANTICS",
    "SHORT_EXPOSURE_SUPPORTED", "CASH_RETURN", "CASH_RETURN_POLICY",
    "COLLATERAL_FINANCING_POLICY", "COST_POLICY_VERSION",
    "COST_BPS_PER_SIDE_BY_CLASS", "HOLDING_COST_BPS_ANNUAL_BY_CLASS",
    "COST_POLICY_PROVENANCE", "DEFAULT_EQUITY_SLEEVE", "CASH_INSTRUMENT_ID", "CASH_SLEEVE",
    "describe", "equity_descriptor", "cash_descriptor", "descriptor_from_row",
    "instrument_block", "unit_notional_usd", "capital_usage_ratio", "units_for_notional",
    "cost_rate_per_side", "transaction_cost_usd", "fill_cash_delta", "replay_entry_marks",
    "value_position", "aggregate_exposures", "allocation_by_group", "is_cash_settled",
    "stable_hash", "safety_block",
]
