"""alpha_agent.r36.strategies - the ONE Release 36 strategy rule owner.

Every rule below is an economically interpretable statement about a market, with
its parameters pre-declared at canonical values in the contract. Nothing here is
fitted, searched, tuned or selected. That is a deliberate answer to the last five
releases: R31 searched a mathematical frontier and found nothing, R34 searched
fifty-five conversion configurations and found nothing, and adding a
thirty-sixth optimiser to a problem whose binding constraint is information
would be repeating the same experiment louder.

Two properties are enforced by construction rather than by review.

**Every statistic is trailing.** There is no full-sample mean, standard
deviation, median or rank anywhere in this module. Each conditioning statistic
is an EXPANDING window that is shifted one period first, so the value used on
date ``t`` was computed from observations strictly before ``t``. A full-sample
z-score would hand every date a threshold that knows its own future, and it is
the single easiest way to manufacture a result that cannot be traded.

**Every signal is stamped on the date whose information produced it.** The panel
stamps ``excess.loc[t]`` with the return earned AFTER ``t``, so a rule that
wants past returns must shift. ``trend`` therefore sums periods ``t-12`` to
``t-2`` and never touches ``excess.loc[t]``, which has not happened yet.

Positions are equal-weighted extreme terciles for a cross-section and the sign
of the signal for a directional rule, gross exposure one, no leverage. Rank
weighting rather than inverse-volatility weighting is not a detail: an
inverse-volatility currency book puts its largest weight on the most tightly
managed currency, which is the one whose realised volatility least reflects its
risk.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r36.strategies"

RETURNS_EXCESS = "excess"


# --------------------------------------------------------------------------- #
# Trailing statistics - shifted BEFORE expanding, always
# --------------------------------------------------------------------------- #
def trailing_mean(values, *, minimum: int = 24):
    """Expanding mean of everything strictly before each row."""
    return values.shift(1).expanding(min_periods=int(minimum)).mean()


def trailing_std(values, *, minimum: int = 24):
    return values.shift(1).expanding(min_periods=int(minimum)).std(ddof=1)


def trailing_z(values, *, minimum: int = 24):
    mean = trailing_mean(values, minimum=minimum)
    std = trailing_std(values, minimum=minimum)
    return (values - mean) / std.replace(0.0, np.nan)


def trailing_percentile(values: pd.Series, *, minimum: int = 24) -> pd.Series:
    """Share of strictly earlier observations below each value."""
    series = pd.Series(values, dtype=float)
    out = pd.Series(np.nan, index=series.index, dtype=float)
    history = []
    for date, value in series.items():
        if len(history) >= int(minimum) and np.isfinite(value):
            past = np.asarray(history, dtype=float)
            out.loc[date] = float(np.mean(past < value))
        if np.isfinite(value):
            history.append(float(value))
    return out


def trailing_sum(frame, *, window: int, skip: int):
    """Sum of the ``window`` realised periods ending ``skip`` periods ago."""
    return frame.shift(int(skip)).rolling(int(window),
                                          min_periods=int(window)).sum()


# --------------------------------------------------------------------------- #
# Position construction
# --------------------------------------------------------------------------- #
def cross_sectional_terciles(scores: pd.DataFrame, *,
                             min_cross_section: int) -> pd.DataFrame:
    """Equal-weight long the top third, short the bottom third, gross one."""
    weights = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
    for date, row in scores.iterrows():
        valid = row.dropna()
        n = int(valid.size)
        if n < int(min_cross_section):
            continue
        k = max(1, int(round(n * _contract.TERCILE_FRACTION)))
        order = valid.sort_values()
        short = order.index[:k]
        long = order.index[-k:]
        if set(short) & set(long):
            continue
        weights.loc[date, long] = 0.5 / len(long)
        weights.loc[date, short] = -0.5 / len(short)
    return weights


def directional(signs: pd.DataFrame) -> pd.DataFrame:
    """Sign of the signal, spread equally over the instruments in play."""
    taken = signs.fillna(0.0)
    active = (taken != 0.0).sum(axis=1).replace(0, np.nan)
    return taken.div(active, axis=0).fillna(0.0)


def single_instrument(index, columns, instrument: str,
                      position) -> pd.DataFrame:
    weights = pd.DataFrame(0.0, index=index, columns=columns)
    if instrument in weights.columns:
        weights[instrument] = pd.Series(position, index=index).fillna(0.0)
    return weights


def duration_neutralise(weights: pd.DataFrame, durations: pd.DataFrame
                        ) -> pd.DataFrame:
    """Scale the long and short legs to equal duration exposure.

    A steepener that is long 2-year notes and short 30-year bonds in equal cash
    is not a curve trade, it is a short duration position wearing a curve
    trade's name.
    """
    out = weights.copy()
    for date in weights.index:
        row = weights.loc[date]
        dur = durations.loc[date] if date in durations.index else None
        if dur is None or not np.isfinite(dur.to_numpy(dtype=float)).any():
            continue
        long_mask = row > 0
        short_mask = row < 0
        long_dur = float((row[long_mask] * dur[long_mask]).sum(skipna=True))
        short_dur = float((row[short_mask] * dur[short_mask]).sum(skipna=True))
        if long_dur <= 0 or short_dur >= 0:
            continue
        scale = long_dur / abs(short_dur)
        out.loc[date, short_mask] = row[short_mask] * scale
        gross = float(out.loc[date].abs().sum())
        if gross > 0:
            out.loc[date] = out.loc[date] / gross * _contract.MAX_GROSS_EXPOSURE
    return out


def _result(weights: pd.DataFrame, *, note: str,
            returns_key: str = RETURNS_EXCESS, cost_scale: float = 1.0
            ) -> dict:
    return {"weights": weights.fillna(0.0), "returns_key": returns_key,
            "cost_scale": float(cost_scale), "note": note}


def _min_cross_section(panel: dict) -> int:
    return int(_contract.LANE_MIN_CROSS_SECTION.get(
        panel.get("lane"), _contract.MIN_CROSS_SECTION))


def _empty(panel: dict, note: str) -> dict:
    excess = panel["excess"]
    return _result(pd.DataFrame(0.0, index=excess.index,
                                columns=excess.columns), note=note)


# --------------------------------------------------------------------------- #
# FX
# --------------------------------------------------------------------------- #
def _fx_trend(panel: dict) -> pd.DataFrame:
    return trailing_sum(panel["excess"],
                        window=_contract.TREND_LOOKBACK_MONTHS
                        - _contract.TREND_SKIP_MONTHS,
                        skip=_contract.TREND_SKIP_MONTHS + 1)


def _global_fx_volatility(panel: dict) -> pd.Series:
    """Menkhoff-style global FX volatility: the cross-sectional mean absolute
    currency move, averaged over the trailing year."""
    absolute = panel["excess"].abs().mean(axis=1, skipna=True)
    return absolute.shift(1).rolling(12, min_periods=12).mean()


def fx_strategy(name: str, panel: dict) -> dict:
    carry = panel["signals"]["carry"]
    min_cs = _min_cross_section(panel)
    if name == "FX_CARRY":
        return _result(cross_sectional_terciles(carry,
                                                min_cross_section=min_cs),
                       note="long the highest interest differentials, short "
                            "the lowest")
    if name == "FX_TREND_12_1":
        return _result(cross_sectional_terciles(_fx_trend(panel),
                                                min_cross_section=min_cs),
                       note="twelve-month currency excess return skipping the "
                            "most recent month")
    if name == "FX_REVERSAL_1M":
        return _result(cross_sectional_terciles(-panel["excess"].shift(1),
                                                min_cross_section=min_cs),
                       note="one-month reversal in the currency excess return")
    if name == "FX_VALUE_REAL_RATE":
        real = panel["signals"].get("real_rate")
        if real is None or real.dropna(how="all").empty:
            return _empty(panel, "no admissible price index for any currency")
        deviation = real - real.shift(_contract.VALUE_LOOKBACK_MONTHS)
        return _result(cross_sectional_terciles(-deviation,
                                                min_cross_section=min_cs),
                       note="short the currencies that have appreciated most "
                            "in real terms over five years")
    if name == "FX_CARRY_TREND":
        score = (carry.rank(axis=1, pct=True)
                 + _fx_trend(panel).rank(axis=1, pct=True)) / 2.0
        score = score.where(carry.notna() & _fx_trend(panel).notna())
        return _result(cross_sectional_terciles(score,
                                                min_cross_section=min_cs),
                       note="the average cross-sectional rank of carry and "
                            "trend")
    if name == "FX_CARRY_CRASH_CONDITIONED":
        weights = cross_sectional_terciles(carry, min_cross_section=min_cs)
        volatility = _global_fx_volatility(panel)
        percentile = trailing_percentile(volatility, minimum=36)
        calm = (percentile < (2.0 / 3.0)).reindex(weights.index).fillna(False)
        return _result(weights.mul(calm.astype(float), axis=0),
                       note="carry, held only when global currency volatility "
                            "is outside the top third of its own trailing "
                            "distribution")
    if name == "FX_CARRY_POSITIONING":
        positioning = panel["signals"].get("positioning")
        if positioning is None or positioning.dropna(how="all").empty:
            return _empty(panel, "no positioning data for any currency")
        columns = [c for c in positioning.columns if c in carry.columns]
        if len(columns) < min_cs:
            return _empty(panel, "positioning covers too few currencies")
        score = (carry[columns].rank(axis=1, pct=True)
                 - positioning[columns].rank(axis=1, pct=True))
        score = score.where(carry[columns].notna()
                            & positioning[columns].notna())
        weights = cross_sectional_terciles(score, min_cross_section=min_cs)
        return _result(weights.reindex(columns=carry.columns).fillna(0.0),
                       note="carry, tilted away from the currencies "
                            "speculators are already most long")
    if name == "FX_DOLLAR_FACTOR_TIMING":
        average = carry.mean(axis=1, skipna=True)
        held = (average > 0).astype(float)
        weights = pd.DataFrame(
            {c: held / max(1, carry.shape[1]) for c in carry.columns},
            index=carry.index)
        return _result(weights.where(carry.notna(), 0.0),
                       note="hold the equal-weight foreign currency basket "
                            "only when its average carry over the dollar is "
                            "positive")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Commodity curve
# --------------------------------------------------------------------------- #
def commodity_strategy(name: str, panel: dict) -> dict:
    carry = panel["signals"]["carry"]
    excess = panel["excess"]
    min_cs = _min_cross_section(panel)
    trend = trailing_sum(excess,
                         window=_contract.TREND_LOOKBACK_MONTHS,
                         skip=1)
    if name == "CMDTY_CURVE_CARRY":
        return _result(directional(np.sign(carry)),
                       note="long a backwardated curve, short a contangoed "
                            "one; the sign of the observed basis")
    if name == "CMDTY_TREND_12M":
        return _result(directional(np.sign(trend)),
                       note="twelve-month time-series momentum in the "
                            "held-contract return")
    if name == "CMDTY_CARRY_TREND":
        agree = np.sign(carry).where(np.sign(carry) == np.sign(trend), 0.0)
        return _result(directional(agree),
                       note="hold only where the curve and the trend agree")
    if name == "CMDTY_CROSS_SECTIONAL_CARRY":
        return _result(cross_sectional_terciles(carry,
                                                min_cross_section=min_cs),
                       note="long the most backwardated market, short the most "
                            "contangoed")
    if name == "CMDTY_CALENDAR_SPREAD":
        return _result(directional(np.sign(carry)),
                       returns_key="spread_return", cost_scale=2.0,
                       note="the nearest contract against the second, by the "
                            "sign of the basis; two legs, so twice the cost")
    if name == "CMDTY_CARRY_POSITIONING":
        positioning = panel["signals"].get("positioning")
        if positioning is None or positioning.dropna(how="all").empty:
            return _empty(panel, "no positioning data for any market")
        sign = np.sign(carry)
        for market in carry.columns:
            if market not in positioning.columns:
                sign[market] = 0.0
                continue
            percentile = trailing_percentile(positioning[market], minimum=36)
            crowded_long = (percentile > 0.9) & (sign[market] > 0)
            crowded_short = (percentile < 0.1) & (sign[market] < 0)
            sign[market] = sign[market].where(
                ~(crowded_long | crowded_short).fillna(False), 0.0)
        return _result(directional(sign),
                       note="the curve signal, stood down when speculators "
                            "are already crowded the same way")
    if name == "CMDTY_SEASONALITY":
        months = pd.Series(excess.index.month, index=excess.index)
        sign = pd.DataFrame(0.0, index=excess.index, columns=excess.columns)
        minimum = int(_contract.SEASONALITY_MIN_TRAILING_YEARS)
        for market in excess.columns:
            values = excess[market]
            for month in range(1, 13):
                mask = months == month
                same = values.where(mask)
                history = same.shift(1).expanding(
                    min_periods=minimum).mean()
                sign.loc[mask, market] = np.sign(
                    history[mask]).fillna(0.0).to_numpy()
        return _result(directional(sign),
                       note="the sign of this calendar month's average return "
                            "in this market, over prior years only")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Rates curve
# --------------------------------------------------------------------------- #
def _steepener(panel: dict, active: pd.Series) -> pd.DataFrame:
    """Long the short bucket, short the long bucket, duration-neutral."""
    excess = panel["excess"]
    weights = pd.DataFrame(0.0, index=excess.index, columns=excess.columns)
    short_bucket = "UST_1_3Y" if "UST_1_3Y" in excess.columns else None
    long_bucket = "UST_20Y_PLUS" if "UST_20Y_PLUS" in excess.columns else (
        "UST_10_20Y" if "UST_10_20Y" in excess.columns else None)
    if short_bucket is None or long_bucket is None:
        return weights
    direction = pd.Series(active, index=excess.index).fillna(0.0)
    weights[short_bucket] = 0.5 * direction
    weights[long_bucket] = -0.5 * direction
    return duration_neutralise(weights, panel["signals"]["duration"])


def rates_strategy(name: str, panel: dict) -> dict:
    excess = panel["excess"]
    curve = panel["signals"]["curve"]
    durations = panel["signals"]["duration"]
    min_cs = _min_cross_section(panel)
    if name == "RATES_CARRY_ROLLDOWN":
        maturity = {"UST_1_3Y": 2.0, "UST_3_7Y": 5.0, "UST_7_10Y": 8.5,
                    "UST_10_20Y": 15.0, "UST_20Y_PLUS": 25.0}
        score = pd.DataFrame(np.nan, index=excess.index,
                             columns=excess.columns)
        ordered = [b for b in sorted(maturity, key=maturity.get)
                   if b in excess.columns]
        for k, bucket in enumerate(ordered):
            point = _contract.RATES_LEGS[bucket][1]
            if point not in curve:
                continue
            level = curve[point]
            if k == 0:
                slide = level - curve.get("%3MTCM", level)
                span = maturity[bucket] - 0.25
            else:
                previous = _contract.RATES_LEGS[ordered[k - 1]][1]
                slide = level - curve.get(previous, level)
                span = maturity[bucket] - maturity[ordered[k - 1]]
            rolldown = (slide / max(span, 0.25)) / 12.0
            duration = durations[bucket].replace(0.0, np.nan)
            score[bucket] = (level / 12.0 + duration * rolldown) / duration
        weights = cross_sectional_terciles(score, min_cross_section=min_cs)
        return _result(duration_neutralise(weights, durations),
                       note="rank the duration buckets by carry plus roll-down "
                            "per unit of duration, then neutralise duration")
    if name == "RATES_LEVEL_TREND":
        bucket = "UST_7_10Y" if "UST_7_10Y" in excess.columns \
            else excess.columns[0]
        trend = trailing_sum(excess[[bucket]],
                             window=_contract.TREND_LOOKBACK_MONTHS, skip=1)
        return _result(single_instrument(excess.index, excess.columns, bucket,
                                         np.sign(trend[bucket]).fillna(0.0)),
                       note="twelve-month time-series momentum in the belly of "
                            "the Treasury curve")
    if name == "RATES_CURVE_VALUE":
        slope = panel["signals"]["slope_10y_2y"]
        active = np.sign(trailing_mean(slope, minimum=36) - slope)
        return _result(_steepener(panel, active.fillna(0.0)),
                       note="steepen when the curve is flatter than its own "
                            "trailing average, flatten when it is steeper")
    if name == "RATES_STEEPENER_CONDITIONAL":
        front = curve.get("%3MTCM")
        if front is None:
            return _empty(panel, "no front curve point")
        active = np.sign(front - trailing_mean(front, minimum=36))
        return _result(_steepener(panel, active.fillna(0.0)),
                       note="steepen when the front rate is above its trailing "
                            "average, which is late in a tightening cycle")
    if name == "RATES_BUTTERFLY":
        fly = panel["signals"]["butterfly_2_5_10"]
        active = np.sign(trailing_mean(fly, minimum=36) - fly).fillna(0.0)
        weights = pd.DataFrame(0.0, index=excess.index, columns=excess.columns)
        if not {"UST_1_3Y", "UST_3_7Y", "UST_10_20Y"} <= set(excess.columns):
            return _empty(panel, "curve buckets for a butterfly are absent")
        weights["UST_3_7Y"] = 0.5 * active
        weights["UST_1_3Y"] = -0.25 * active
        weights["UST_10_20Y"] = -0.25 * active
        return _result(duration_neutralise(weights, durations),
                       note="long the belly against the wings when the "
                            "butterfly is cheap to its trailing average")
    if name == "RATES_BREAKEVEN_RV":
        returns = panel["signals"].get("breakeven_returns")
        breakeven = panel["signals"].get("be_T10YIE")
        if returns is None or breakeven is None:
            return _empty(panel, "no inflation-linked leg or breakeven series")
        active = np.sign(trailing_mean(breakeven, minimum=36)
                         - breakeven).fillna(0.0)
        weights = pd.DataFrame(0.0, index=returns.index,
                               columns=returns.columns)
        legs = _contract.RATES_BREAKEVEN_LEGS
        if not set(legs) <= set(returns.columns):
            return _empty(panel, "inflation-linked legs incomplete")
        weights[legs[0]] = 0.5 * active.reindex(returns.index).fillna(0.0)
        weights[legs[1]] = -0.5 * active.reindex(returns.index).fillna(0.0)
        return _result(weights, returns_key="breakeven_returns",
                       note="long inflation-linked against nominal when the "
                            "breakeven is below its trailing average")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Credit
# --------------------------------------------------------------------------- #
def credit_strategy(name: str, panel: dict) -> dict:
    excess = panel["excess"]
    spreads = panel["signals"]["spreads"]
    column = "BAA10Y" if "BAA10Y" in spreads.columns else (
        spreads.columns[0] if len(spreads.columns) else None)
    if column is None:
        return _empty(panel, "no credit spread series")
    spread = spreads[column]
    instrument = excess.columns[0]
    if name == "CREDIT_SPREAD_CARRY":
        position = (spread > trailing_mean(spread, minimum=36)).astype(float)
        return _result(single_instrument(excess.index, excess.columns,
                                         instrument, position),
                       note="hold duration-hedged credit only when the spread "
                            "is wider than its own trailing average")
    if name == "CREDIT_SPREAD_MOMENTUM":
        change = spread - spread.shift(6)
        return _result(single_instrument(excess.index, excess.columns,
                                         instrument,
                                         -np.sign(change).fillna(0.0)),
                       note="long credit while the spread has been tightening "
                            "over six periods")
    if name == "CREDIT_SPREAD_REVERSION":
        change = spread - spread.shift(1)
        return _result(single_instrument(excess.index, excess.columns,
                                         instrument,
                                         np.sign(change).fillna(0.0)),
                       note="long credit after the spread widens, short after "
                            "it tightens")
    if name == "CREDIT_VS_RATES_RV":
        yields = panel["signals"]["spreads"]
        other = [c for c in yields.columns if c != column]
        if not other:
            return _empty(panel, "no second credit series for relative value")
        score = (trailing_z(spread, minimum=36)
                 - trailing_z(yields[other[0]], minimum=36))
        return _result(single_instrument(excess.index, excess.columns,
                                         instrument,
                                         np.sign(score).fillna(0.0)),
                       note="hold credit when investment-grade compensation is "
                            "rich relative to the lower-quality tier")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Volatility
# --------------------------------------------------------------------------- #
def volatility_strategy(name: str, panel: dict) -> dict:
    excess = panel["excess"]
    slope = panel["signals"]["term_slope"]
    if name == "VOL_TERM_LONG_TIMING":
        position = (slope > 0).astype(float)
        return _result(single_instrument(excess.index, excess.columns,
                                         _contract.VOL_TRADABLE_LEG, position),
                       note="hold long volatility only while the term "
                            "structure is inverted; cash otherwise")
    if name == "VOL_TERM_EQUITY_TIMING":
        position = (slope < trailing_mean(slope, minimum=52)).astype(float)
        return _result(single_instrument(excess.index, excess.columns,
                                         _contract.VOL_EQUITY_LEG, position),
                       note="hold equity only while the volatility term "
                            "structure is calmer than its trailing average")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Cross-asset relative value
# --------------------------------------------------------------------------- #
def cross_asset_strategy(name: str, panel: dict) -> dict:
    excess = panel["excess"]
    signals = panel["signals"]
    if name == "XA_GOLD_VS_REAL_YIELD":
        real = signals.get("DFII10")
        if real is None or "GOLD" not in excess.columns:
            return _empty(panel, "no real yield series or no gold leg")
        position = np.sign(trailing_mean(real, minimum=36) - real).fillna(0.0)
        return _result(single_instrument(excess.index, excess.columns, "GOLD",
                                         position),
                       note="gold is a zero-coupon real asset, so hold it when "
                            "the real yield is below its trailing average")
    if name == "XA_COPPER_GOLD_TO_RATES":
        ratio = signals.get("copper_gold_level")
        if ratio is None or "TREASURY" not in excess.columns:
            return _empty(panel, "no copper-gold ratio or no Treasury leg")
        change = np.log(ratio.replace(0.0, np.nan)) - np.log(
            ratio.replace(0.0, np.nan).shift(12))
        return _result(single_instrument(excess.index, excess.columns,
                                         "TREASURY",
                                         -np.sign(change).fillna(0.0)),
                       note="the copper-gold ratio is a growth proxy, so a "
                            "rising ratio argues against duration")
    if name == "XA_EQUITY_VS_CREDIT":
        if not {"EQUITY", "CREDIT"} <= set(excess.columns):
            return _empty(panel, "no equity or credit leg")
        lead = trailing_sum(excess[["CREDIT"]], window=3, skip=1)["CREDIT"]
        return _result(single_instrument(excess.index, excess.columns,
                                         "EQUITY",
                                         np.sign(lead).fillna(0.0)),
                       note="the credit market is said to lead equity, so take "
                            "equity risk after credit has been paid")
    if name == "XA_CURVE_SLOPE_EQUITY_BOND":
        if not {"EQUITY", "TREASURY"} <= set(excess.columns):
            return _empty(panel, "no equity or Treasury leg")
        slope = signals.get("%10YTCM")
        short = signals.get("%2YTCM")
        if slope is None or short is None:
            return _empty(panel, "no curve slope")
        curve = slope - short
        active = np.sign(curve - trailing_mean(curve, minimum=36)).fillna(0.0)
        weights = pd.DataFrame(0.0, index=excess.index, columns=excess.columns)
        weights["EQUITY"] = 0.5 * active
        weights["TREASURY"] = -0.5 * active
        return _result(weights,
                       note="a steeper curve than usual argues for equity over "
                            "duration, a flatter one for the reverse")
    if name == "XA_FX_CARRY_VS_EQUITY":
        if "FX_CARRY_BOOK" not in excess.columns \
                or "EQUITY" not in excess.columns:
            return _empty(panel, "the currency carry book is not available")
        pair = excess[["FX_CARRY_BOOK", "EQUITY"]]
        mean = trailing_sum(pair, window=12, skip=1) / 12.0
        volatility = pair.shift(1).rolling(12, min_periods=12).std(ddof=1)
        score = mean / volatility.replace(0.0, np.nan)
        weights = pd.DataFrame(0.0, index=excess.index, columns=excess.columns)
        # ``idxmax`` raises on an all-missing row, and the early sample has
        # many: the currency book does not exist until the trailing window
        # fills. No score means no position, which is the correct reading.
        scored = score.dropna(how="all")
        for date, row in scored.iterrows():
            valid = row.dropna()
            if valid.empty:
                continue
            weights.loc[date, valid.idxmax()] = 1.0
        return _result(weights,
                       note="hold whichever of the currency carry book and "
                            "equity has the better trailing risk-adjusted "
                            "return")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Crypto
# --------------------------------------------------------------------------- #
def crypto_strategy(name: str, panel: dict) -> dict:
    excess = panel["excess"]
    trend = trailing_sum(excess,
                         window=_contract.CRYPTO_TREND_LOOKBACK_WEEKS, skip=1)
    if name == "CRYPTO_TREND_TS":
        return _result(directional(np.sign(trend).clip(lower=0.0)),
                       note="hold each asset only while its twelve-week trend "
                            "is positive; no shorting, because a borrow was "
                            "not reliably available")
    if name == "CRYPTO_CROSS_SECTIONAL":
        return _result(cross_sectional_terciles(
            trend, min_cross_section=_min_cross_section(panel)),
            note="long the stronger of the two majors against the weaker")
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #
_BY_LANE = {
    _contract.LANE_FX: fx_strategy,
    _contract.LANE_COMMODITY: commodity_strategy,
    _contract.LANE_RATES: rates_strategy,
    _contract.LANE_CREDIT: credit_strategy,
    _contract.LANE_VOL: volatility_strategy,
    _contract.LANE_CROSS_ASSET: cross_asset_strategy,
    _contract.LANE_CRYPTO: crypto_strategy,
}


def build_weights(name: str, panel: dict) -> dict:
    """The frozen rule named ``name``, applied to its lane's panel."""
    lane = _contract.STRATEGIES[name][0]
    if panel.get("lane") != lane:
        raise ValueError("strategy %s belongs to lane %s, not %s"
                         % (name, lane, panel.get("lane")))
    return _BY_LANE[lane](name, panel)


__all__ = ["CALCULATION_OWNER", "trailing_mean", "trailing_std", "trailing_z",
           "trailing_percentile", "trailing_sum", "cross_sectional_terciles",
           "directional", "single_instrument", "duration_neutralise",
           "build_weights", "fx_strategy", "commodity_strategy",
           "rates_strategy", "credit_strategy", "volatility_strategy",
           "cross_asset_strategy", "crypto_strategy"]
