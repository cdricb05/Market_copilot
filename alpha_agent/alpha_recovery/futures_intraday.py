r"""alpha_agent.alpha_recovery.futures_intraday - the ONE owner of the native
CME futures intraday panel: its session definition, its cost ladder, and the
families that will be run on it.

WHY THIS FILE EXISTS BEFORE THE DATA DOES
    Everything in here is a PRE-REGISTRATION. It is written and committed while
    the panel is still priced-but-not-downloaded, so that not one of these
    choices can have been made after seeing a bar, let alone after seeing a
    result. The campaign's own closed axis is the reason: its single surviving
    arm reached gross t 2.05 only after a magnitude condition was applied, and
    the only thing that made that credible was that the condition was fixed once
    and stated in advance. The same discipline is owed to a panel that costs
    real credits.

    Concretely, three things are fixed here and may not move afterwards:
      1. what a "session" is on a 23-hour venue,
      2. what a round trip costs in each of ten contracts,
      3. which families are allowed to be tested, and how many specifications
         each may spend.

WHAT THIS PANEL CHANGES, AND WHY IT IS NOT ANOTHER PRICE TRANSFORM
    Contract rule 13 forbids reopening price-derived research on another lag,
    transform or parameter, and permits it when COVERAGE materially improves.
    The owned R45 ETF minute panel runs 07:00-12:59 ET. That is the first 150
    minutes of the US day: no afternoon, no closing auction, no overnight, no
    European session. Every intraday family in this campaign was therefore
    tested on a fragment of the day and NONE could be marked to the close.

    A CME session runs ~23 hours. This panel is the first configuration in the
    project's history in which an intraday signal can be held to the settlement
    print, and the first in which the overnight and European sessions are
    observable at all. It also adds ENERGY (CL) and FX (6E/6J), two buckets the
    estate has never held at any frequency.

WHAT IS REUSED AND MUST NOT BE REBUILT
    The scorer, the gates, BH/Holm multiplicity, the equal-risk incremental
    utility against the incumbent, and the verdict vocabulary are all inherited
    unchanged from r63/r64 and `intraday_alpha`. This module owns the PANEL and
    the PRE-REGISTRATION only. It deliberately owns no scoring code.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from . import MIN_EFFECTIVE_PERIODS, write_artifact
from .databento_acquisition import UNIVERSE, acquisition_root

CALCULATION_OWNER = "alpha_agent.alpha_recovery.futures_intraday"
ARTIFACT_NAME = "futures_intraday_preregistration.json"

TZ = "America/New_York"
PPY = 252.0


# --------------------------------------------------------------- the session
#: The hour, in exchange-local time, at which CME rolls the TRADE DATE.
#:
#: This is the single most important definition in the file and the easiest to
#: get silently wrong. ``databento_acquisition.parse_csv`` labels each bar with
#: its ET CALENDAR date, which is correct for a roll schedule driven by RTH
#: volume but WRONG for strategy construction: the bars from 18:00 ET Monday
#: belong to Tuesday's trade date, and grouping them under Monday would split
#: one session across two rows and leak Tuesday's overnight into Monday's
#: "close". The trade date therefore advances at 17:00 ET.
TRADE_DATE_ROLL_HOUR_ET = 17

#: Named windows inside one CME trade date, as (first_minute, last_minute) in
#: minutes since ET midnight ON THE TRADE DATE's own clock. The overnight
#: window starts on the PRIOR calendar evening, which is why it is expressed as
#: a negative offset rather than as a wrapped 18:00 -> 09:29.
#:
#: These are declared as ECONOMIC windows, not as tuning parameters. None of
#: them may be swept.
WINDOWS = {
    # 18:00 ET prior evening -> 09:29 ET, i.e. everything before the US cash open
    "OVERNIGHT": (-6 * 60, 9 * 60 + 29),
    # the European cash session, the one the ETF panel could never observe
    "EUROPE": (3 * 60, 8 * 60),
    # the US cash session
    "RTH": (9 * 60 + 30, 16 * 60),
    # the fragment the owned ETF panel covers, kept so the two panels can be
    # compared on identical ground rather than by assertion
    "ETF_PANEL_EQUIVALENT": (9 * 60 + 30, 12 * 60 + 59),
    # the US afternoon - the half of the day the ETF panel is missing
    "US_AFTERNOON": (13 * 60, 16 * 60),
    # the settlement window: the mark this campaign has never once been able to
    # trade against
    "SETTLEMENT": (15 * 60 + 45, 16 * 60),
}

#: The reference mark. The whole point of buying this panel.
MARK_TO = "RTH_CLOSE"
MARK_MINUTE_ET = 16 * 60


# ------------------------------------------------------- CME contract specs
#: (price tick, contract multiplier). Public CME contract specifications, and
#: the ONLY per-instrument numbers this module hardcodes.
#:
#: tick value in USD = tick * multiplier, which must reproduce the venue's
#: published tick values exactly; `test_futures_tick_values_match_the_venue`
#: pins all ten.
SPECS = {
    "ES": (0.25,        50),          # E-mini S&P 500          -> $12.500/tick
    "NQ": (0.25,        20),          # E-mini Nasdaq-100       -> $5.000/tick
    "GC": (0.10,        100),         # Gold, 100 oz            -> $10.000/tick
    "6E": (0.00005,     125_000),     # Euro FX                 -> $6.250/tick
    "6J": (0.0000005,   12_500_000),  # Japanese Yen            -> $6.250/tick
    "ZN": (1.0 / 64,    1_000),       # 10-year Note            -> $15.625/tick
    "ZF": (1.0 / 128,   1_000),       # 5-year Note             -> $7.8125/tick
    "ZT": (1.0 / 256,   2_000),       # 2-year Note             -> $7.8125/tick
    "ZB": (1.0 / 32,    1_000),       # T-Bond                  -> $31.250/tick
    "CL": (0.01,        1_000),       # WTI Crude, 1,000 bbl    -> $10.000/tick
}

#: The market each root expresses. Four Treasury contracts are ONE rates market
#: at four tenors, not four markets - the same non-additivity the acquisition
#: optimiser enforces, restated here so a cross-sectional family cannot quietly
#: treat ZT/ZF/ZN/ZB as four independent bets.
MARKET = {
    "ES": "US_EQUITY_BETA", "NQ": "US_TECH_BETA", "GC": "GOLD",
    "6E": "EUR_FX", "6J": "JPY_FX",
    "ZT": "US_RATES_2Y", "ZF": "US_RATES_5Y", "ZN": "US_RATES_10Y", "ZB": "US_RATES_30Y",
    "CL": "CRUDE_ENERGY",
}

ROOTS = tuple(SPECS)


# ------------------------------------------------- the pre-registered costs
#: Round-trip cost is NOT the desk's 12.5 bp single-name equity rate. A liquid
#: CME outright is one of the cheapest instruments in the world to trade, and
#: charging it an equity rate would reject a real edge for a reason that is not
#: true. Equally, it is not free.
#:
#: The LADDER IS A FORMULA, not a table of basis points, and that is deliberate:
#: a futures cost in bp depends on the price level (a 1-tick spread on ZB at 118
#: is not the same bp as at 95), so a hardcoded bp table would silently drift
#: with the sample. The formula is fixed here; only the price level comes from
#: the data.
#:
#:     per_side_bps = 10000 * (ticks * tick_size + commission_usd / multiplier)
#:                    / price
#:
#: Note the multiplier cancels out of the spread term entirely, so the spread
#: cost in bp is just ticks * tick_size / price. Only the commission needs it.
COST_LADDER = {
    # headline: a half-tick of spread, retail-competitive all-in commission
    "PRIMARY":   {"ticks": 0.5, "commission_usd": 1.25},
    # capital-eligibility REQUIRES surviving this: a full tick crossed every
    # time, and double commission
    "STRESS":    {"ticks": 1.0, "commission_usd": 2.50},
    # always reported: two ticks, the cost of trading these products at an hour
    # when the book is thin - which an overnight or European-session family
    # will genuinely face
    "CANONICAL": {"ticks": 2.0, "commission_usd": 2.50},
}
COST_ELIGIBILITY_LEVEL = "STRESS"


def per_side_bps(root: str, price: float, level: str = "PRIMARY") -> float:
    """Cost of ONE side, in basis points of notional, at ``price``."""
    tick, mult = SPECS[root]
    cfg = COST_LADDER[level]
    usd = cfg["ticks"] * tick * mult + cfg["commission_usd"]
    notional = price * mult
    return 10000.0 * usd / notional


def round_trip_bps(root: str, price: float, level: str = "PRIMARY") -> float:
    return 2.0 * per_side_bps(root, price, level)


def tick_value_usd(root: str) -> float:
    tick, mult = SPECS[root]
    return tick * mult


# ----------------------------------------------------- the declared families
#: Families that only a FULL session makes possible. These are the reason the
#: panel is worth credits; none of them can be expressed on the owned ETF panel
#: at all.
FAM_CLOSE = "FUT_MARK_TO_CLOSE"
FAM_OVERNIGHT = "FUT_OVERNIGHT_TO_RTH"
FAM_EUROPE = "FUT_EUROPE_LEAD"
#: Families inherited from the closed ETF axis, re-run on the full session and
#: on markets the ETF panel does not contain. They are NOT re-opened as new
#: parameter searches - each is the SAME economic statement measured where it
#: can now be marked to the close.
FAM_CARRY = "FUT_SESSION_CARRY"
FAM_RANGE = "FUT_OPENING_RANGE"
FAM_VOL = "FUT_VOLATILITY_STATE"
FAM_LEAD = "FUT_CROSS_MARKET_LEADLAG"
FAM_RS = "FUT_RELATIVE_STRENGTH"

FAMILIES = (FAM_CLOSE, FAM_OVERNIGHT, FAM_EUROPE,
            FAM_CARRY, FAM_RANGE, FAM_VOL, FAM_LEAD, FAM_RS)

#: What each family claims, and - stated in advance - what would falsify it.
#: A family with no declared falsifier is a fishing licence.
FAMILY_CONTRACT = {
    FAM_CLOSE: {
        "claim": "the US afternoon and the settlement print carry information the "
                 "first 150 minutes of the day do not",
        "only_possible_because": "the ETF panel stops at 12:59 ET",
        "falsified_by": "no arm marked to the 16:00 close beats the same arm marked at "
                        "12:59 ET by more than sampling noise",
    },
    FAM_OVERNIGHT: {
        "claim": "the overnight move (18:00 ET -> 09:29 ET) predicts the RTH move",
        "only_possible_because": "the estate has never held an overnight session at any frequency",
        "falsified_by": "gross Newey-West t below 2.0 with the cost ladder set to ZERO, "
                        "which is how the ETF axis proved it had no edge to lose",
    },
    FAM_EUROPE: {
        "claim": "the European cash session (03:00-08:00 ET) leads the US open",
        "only_possible_because": "the ETF panel's first bar is 07:00 ET",
        "falsified_by": "the lead-lag beta is indistinguishable from the contemporaneous "
                        "overnight beta, i.e. it carries no timing information",
    },
    FAM_CARRY: {
        "claim": "prior-session carry reverts, now measurable to the close",
        "prior_result": "CARRY_REVERSION_SPY_LARGE_ONLY reached gross t 2.05 on the ETF "
                        "panel but its four-market generalisation returned +0.03 %/yr at "
                        "t 0.01, which FALSIFIED the effect",
        "falsified_by": "the same falsification repeating: a single-market arm that does "
                        "not generalise across the ten roots is noise, not an effect",
    },
    FAM_RANGE: {"claim": "opening-range breakout vs failure, held to the close",
                "falsified_by": "no gross t >= 2.0 at zero cost"},
    FAM_VOL: {"claim": "intraday volatility compression / expansion",
              "falsified_by": "no gross t >= 2.0 at zero cost"},
    FAM_LEAD: {"claim": "rates / gold / crude -> equity transmission within the session",
               "falsified_by": "no gross t >= 2.0 at zero cost"},
    FAM_RS: {"claim": "cross-sectional relative strength among economically distinct roots",
             "falsified_by": "no gross t >= 2.0 at zero cost"},
}

#: Contract section 7, restated so the budget cannot drift upward mid-axis.
MAX_PRIMARY_PER_FAMILY = 6
MAX_RESCUES_PER_FAMILY = 2
RESCUE_REQUIRES_NAMED_MEASURED_BINDING_FAILURE = True


# ------------------------------------------------------------ the gates
#: Inherited UNCHANGED. Acquiring data does not buy a weaker threshold, and
#: this dict exists so that claim is checkable rather than asserted.
FROZEN_GATES = {
    "materiality_net_pct_per_year": 1.5,
    "paired_t": 2.0,
    "bh_q": 0.10,
    "family_holm_alpha": 0.05,
    "min_effective_periods": MIN_EFFECTIVE_PERIODS,
    "holdout_halves_floor": -0.005,
    "drawdown_multiple": 1.5,
    "must_survive_cost_level": COST_ELIGIBILITY_LEVEL,
    "positive_equal_risk_utility_vs_incumbent": True,
}


# ------------------------------------------------------------- the panel
def panel_root() -> Path:
    """Where `databento_acquisition.normalise` writes its front-month series."""
    return acquisition_root() / "_normalised"


def panel_path(root: str) -> Path:
    return panel_root() / ("%s_front_1m.csv.gz" % root)


def trade_date(ts_et) -> date:
    """The CME trade date a bar belongs to, given its EXCHANGE-LOCAL stamp."""
    shift = 24 - TRADE_DATE_ROLL_HOUR_ET
    return (ts_et + timedelta(hours=shift)).date()


def window_minutes(name: str) -> tuple:
    return WINDOWS[name]


def in_window(minute_et: int, name: str, *, prior_evening: bool = False) -> bool:
    """Is a minute inside a named window?

    ``prior_evening`` marks a bar stamped on the calendar day BEFORE its trade
    date (18:00-23:59 ET), which the overnight window expresses as a negative
    minute offset.
    """
    lo, hi = WINDOWS[name]
    m = minute_et - 24 * 60 if prior_evening else minute_et
    return lo <= m <= hi


def available_roots() -> list:
    """Which roots are actually on disk. Empty until the panel is acquired -
    and that is reported as a state, never as an empty result."""
    if not panel_root().exists():
        return []
    return sorted(r for r in ROOTS if panel_path(r).exists())


# --------------------------------------------------------- the aligned panel
#: The trade date's own minute axis. Index 0 is 17:00 ET on the PRIOR calendar
#: evening - the instant the CME trade date rolls - and index 1439 is 16:59 ET.
#: A minute is therefore addressed by its offset from ET MIDNIGHT OF THE TRADE
#: DATE, which is exactly the convention ``WINDOWS`` is written in, so a window
#: bound can be turned into a column with no second convention to get wrong.
TD_FIRST_REL = TRADE_DATE_ROLL_HOUR_ET * 60 - 24 * 60          # -420, i.e. 17:00 prior evening
TD_MINUTES = 24 * 60                                            # 1440


def rel_minute(minute_et: int) -> int:
    """ET minute-of-day -> minutes from ET midnight OF ITS TRADE DATE."""
    return minute_et if minute_et < TRADE_DATE_ROLL_HOUR_ET * 60 else minute_et - 24 * 60


def col(rel: int) -> int:
    """A trade-date-relative minute -> its column in the panel grids."""
    if not (TD_FIRST_REL <= rel < TD_FIRST_REL + TD_MINUTES):
        raise ValueError("%d is outside the trade date's own 24h axis" % rel)
    return rel - TD_FIRST_REL


def minute_index(hh: int, mm: int) -> int:
    """Column for an exchange-local wall-clock time on the trade date.

    Deliberately the same call signature as ``intraday_data.minute_index`` so
    the one scorer can be pointed at this panel without being edited.
    """
    return col(rel_minute(hh * 60 + mm))


def window_cols(name: str) -> tuple:
    lo, hi = WINDOWS[name]
    return col(lo), col(hi)


_CACHE: dict = {}


def _cache_path() -> Path:
    return panel_root() / "futures_panel_v1.npz"


def _read_front(root: str):
    """One root's normalised front-month series, on its trade-date axis."""
    import pandas as pd

    df = pd.read_csv(panel_path(root),
                     usecols=lambda c: c in ("session", "minute_et", "symbol",
                                             "open", "high", "low", "close", "volume"))
    m = df["minute_et"].to_numpy()
    # The trade date advances at 17:00 ET, so an evening bar belongs to the NEXT
    # date. This is the one place that correction is applied.
    td = pd.to_datetime(df["session"]) + pd.to_timedelta(
        (m >= TRADE_DATE_ROLL_HOUR_ET * 60).astype("int64"), unit="D")
    df["td"] = td.dt.strftime("%Y-%m-%d").to_numpy()
    df["c"] = [rel_minute(int(x)) - TD_FIRST_REL for x in m]
    return df


def _grid(df, field: str, dates: list, *, dtype=float):
    """(n_trade_dates x 1440) of ``field``.

    A minute with no print is forward-filled from the last print WITHIN the
    same trade date - the last trade is still the price - and minutes before
    the first print of that trade date stay NaN and are never back-filled.
    This is the identical rule the owned ETF panel uses.
    """
    import numpy as np
    import pandas as pd

    piv = df.pivot_table(index="td", columns="c", values=field, aggfunc="last")
    piv = piv.reindex(index=dates, columns=list(range(TD_MINUTES)))
    arr = piv.to_numpy(dtype=float)
    if field == "volume":
        arr = np.nan_to_num(arr, nan=0.0)
    else:
        arr = pd.DataFrame(arr).ffill(axis=1).to_numpy()
    return arr.astype(dtype)


def _held(df, dates: list, *, lo_col: int, hi_col: int):
    """The dated contract held over a column range, per trade date.

    ``None`` where that part of the trade date has no print at all, which is
    itself information: a signal spanning it cannot be formed.
    """
    import numpy as np

    sub = df[(df["c"] >= lo_col) & (df["c"] <= hi_col)]
    s = sub.groupby("td")["symbol"].last().reindex(dates)
    # "" means "no print in this part of the trade date". A plain string array
    # survives the npz round trip, where an object array carrying None does
    # not - and a None that silently became the STRING "None" would compare
    # equal to another one, which is exactly the roll bug this guards against.
    return np.array(["" if v is None or v != v else str(v) for v in s.to_numpy()], dtype="<U12")


def same_contract(pn, legs, *, across: str):
    """(n_dates x n_legs) mask: is the span a single contract, or a spread?

    ``across="midnight"`` covers a signal running from the prior evening into
    the trade date's own morning - the OVERNIGHT window. ``across="trade_date"``
    covers a signal differencing this trade date against the previous one -
    CARRY. Either can straddle a roll, and a price difference taken across a
    roll is a calendar spread, never a return. This is the same rule
    ``databento_acquisition.normalise`` applies to minute returns, restated for
    the two spans that reach beyond a single continuous block of minutes.
    """
    import numpy as np

    ev, dy = pn["held_evening"], pn["held_day"]
    idx = [pn["instruments"].index(s) for s in legs]
    d = dy[:, idx]
    if across == "midnight":
        e = ev[:, idx]
        return (e == d) & (e != "") & (d != "")
    if across == "trade_date":
        prev = np.roll(d, 1, axis=0)
        prev[0, :] = ""
        return (prev == d) & (d != "")
    raise ValueError("unknown span %r" % across)


def panel(*, rebuild: bool = False):
    """The aligned CME futures intraday panel.

    Shape and key names deliberately mirror ``intraday_data.panel`` so the one
    scorer can consume either: ``(n_trade_dates, 1440, n_roots)``. The only
    difference is the minute axis, and that difference is the entire point of
    the acquisition.
    """
    import numpy as np

    if "panel" in _CACHE and not rebuild:
        return _CACHE["panel"]
    cp = _cache_path()
    if cp.exists() and not rebuild:
        z = np.load(cp, allow_pickle=False)
        out = {"dates": [str(d) for d in z["dates"]], "instruments": [str(s) for s in z["instruments"]],
               "close": z["close"], "high": z["high"], "low": z["low"], "volume": z["volume"],
               "held_evening": z["held_evening"], "held_day": z["held_day"]}
        _CACHE["panel"] = out
        return out

    roots = available_roots()
    if not roots:
        raise RuntimeError("the futures panel is not on disk; acquisition must run first")
    frames = {r: _read_front(r) for r in roots}
    # The contract actually held, recorded separately for the two CALENDAR days
    # a single trade date spans. Any signal that reaches across the 00:00 ET
    # boundary (the overnight) or across trade dates (carry) must be able to
    # check that it is not differencing two different contracts - which would
    # be a calendar spread, not a return.
    EVENING_LAST_COL = -TD_FIRST_REL - 1                        # 17:00-23:59 ET
    # The session calendar is ES's: the deepest book and the market whose
    # session defines the others' tradability. A root missing a date keeps NaN
    # on that row rather than borrowing a neighbour's.
    ref = "ES" if "ES" in frames else roots[0]
    dates = sorted(set(frames[ref]["td"].tolist()))
    n_d, n_i = len(dates), len(roots)
    cl = np.full((n_d, TD_MINUTES, n_i), np.nan)
    hi = np.full((n_d, TD_MINUTES, n_i), np.nan, dtype=np.float32)
    lo = np.full((n_d, TD_MINUTES, n_i), np.nan, dtype=np.float32)
    vo = np.zeros((n_d, TD_MINUTES, n_i), dtype=np.float32)
    ev = np.full((n_d, n_i), "", dtype="<U12")
    dy = np.full((n_d, n_i), "", dtype="<U12")
    for j, r in enumerate(roots):
        df = frames[r]
        cl[:, :, j] = _grid(df, "close", dates)
        hi[:, :, j] = _grid(df, "high", dates, dtype=np.float32)
        lo[:, :, j] = _grid(df, "low", dates, dtype=np.float32)
        vo[:, :, j] = _grid(df, "volume", dates, dtype=np.float32)
        ev[:, j] = _held(df, dates, lo_col=0, hi_col=EVENING_LAST_COL)
        dy[:, j] = _held(df, dates, lo_col=EVENING_LAST_COL + 1, hi_col=TD_MINUTES - 1)
    out = {"dates": dates, "instruments": list(roots),
           "close": cl, "high": hi, "low": lo, "volume": vo,
           "held_evening": ev, "held_day": dy}
    cp.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cp, dates=np.array(dates), instruments=np.array(roots),
                        close=cl, high=hi, low=lo, volume=vo,
                        held_evening=ev, held_day=dy)
    _CACHE["panel"] = out
    return out


def ix(root: str) -> int:
    return panel()["instruments"].index(root)


def representative_price(root: str) -> float:
    """The median close used to turn the cost FORMULA into basis points.

    Taken over the whole panel and over every minute, so it is a property of
    the sample rather than of any arm's engaged sessions - an arm cannot get a
    cheaper cost by trading when the contract happens to be expensive.
    """
    import numpy as np

    key = "repr_%s" % root
    if key not in _CACHE:
        pn = panel()
        _CACHE[key] = float(np.nanmedian(pn["close"][:, :, pn["instruments"].index(root)]))
    return _CACHE[key]


def cost_ladder_bps(roots) -> dict:
    """The three pre-registered cost levels, in per-side bp, for a set of legs.

    The MOST EXPENSIVE leg's rate is charged to the whole arm. That is
    deliberately conservative: a cell mixing ES (0.24 bp/side) and ZB (1.43) is
    charged ZB's rate on both legs, so no cell can ever be made to look cheaper
    by pairing a costly market with a cheap one.
    """
    out = {}
    for level in ("PRIMARY", "STRESS", "CANONICAL"):
        out[level] = max(per_side_bps(r, representative_price(r), level) for r in roots)
    return out


# -------------------------------------------------------------- the artifact
def preregistration() -> dict:
    """The whole pre-registration, as one machine-readable record written
    BEFORE the data exists."""
    return {
        "calculation_owner": CALCULATION_OWNER,
        "written_before_any_bar_was_downloaded": True,
        "why_this_is_coverage_not_a_transform": {
            "contract_rule": 13,
            "named_measured_defect": "the owned R45 ETF minute panel runs 07:00-12:59 ET, so "
                                     "every intraday family in this campaign was tested on the "
                                     "first 150 minutes of the US day and none could be marked "
                                     "to the close",
            "what_the_new_panel_adds": ["the US afternoon", "the closing / settlement print",
                                        "the overnight session", "the European cash session",
                                        "ENERGY (CL)", "FX (6E, 6J)"],
            "is_another_lag_or_transform": False,
        },
        "session": {
            "timezone": TZ,
            "trade_date_rolls_at_et_hour": TRADE_DATE_ROLL_HOUR_ET,
            "why": "bars from 18:00 ET belong to the NEXT trade date; grouping them under the "
                   "calendar date splits one session in two and leaks the next session's "
                   "overnight into this session's close",
            "windows_minutes_et": {k: list(v) for k, v in WINDOWS.items()},
            "mark_to": MARK_TO,
            "mark_minute_et": MARK_MINUTE_ET,
        },
        "contract_specs": {r: {"tick": SPECS[r][0], "multiplier": SPECS[r][1],
                               "tick_value_usd": round(tick_value_usd(r), 6),
                               "market": MARKET[r], "bucket": UNIVERSE[r][1]}
                           for r in ROOTS},
        "cost_ladder": {
            "form": "per_side_bps = 10000 * (ticks*tick_size + commission_usd/multiplier) / price",
            "why_a_formula_not_a_table": "a futures cost in bp depends on the price level, so a "
                                         "hardcoded bp table would silently drift with the sample",
            "why_not_the_equity_rate": "the desk's 12.5 bp single-name equity rate is not true "
                                       "for a liquid CME outright; charging it would reject a "
                                       "real edge for a reason that is not true",
            "levels": COST_LADDER,
            "eligibility_requires_surviving": COST_ELIGIBILITY_LEVEL,
        },
        "families": {f: FAMILY_CONTRACT[f] for f in FAMILIES},
        "research_budget": {
            "max_primary_per_family": MAX_PRIMARY_PER_FAMILY,
            "max_rescues_per_family": MAX_RESCUES_PER_FAMILY,
            "rescue_requires_named_measured_binding_failure":
                RESCUE_REQUIRES_NAMED_MEASURED_BINDING_FAILURE,
            "every_executed_specification_counts_in_the_denominator": True,
        },
        "frozen_gates": FROZEN_GATES,
        "gates_unchanged_by_acquiring_data": True,
        "reused_not_rebuilt": ["alpha_agent.r63.sensitivity.run_cell (the ONE scorer)",
                               "alpha_agent.r63.sensitivity.bh_fdr", "alpha_agent.r64.family.holm",
                               "alpha_agent.r64.construction (risk-controlled book)",
                               "alpha_agent.alpha_recovery.tournament.cross_domain"],
        "panel_root": str(panel_root()),
        "roots_declared": list(ROOTS),
        "roots_on_disk": available_roots(),
    }


def build(*, write: bool = True) -> dict:
    body = preregistration()
    on_disk = body["roots_on_disk"]
    body["state"] = "PREREGISTERED_AWAITING_PANEL" if not on_disk else "PANEL_PRESENT"
    if not on_disk:
        body["blocker"] = {
            "kind": "PANEL_NOT_ACQUIRED",
            "what_is_missing": "the priced Databento panel has not been downloaded",
            "owner": "alpha_agent.alpha_recovery.databento_acquisition",
            "remediation": "run the databento stage with --spend-free-credits once the "
                           "free-credit spend is approved",
        }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


if __name__ == "__main__":                                   # pragma: no cover
    print(json.dumps(build(), indent=1, default=str))
