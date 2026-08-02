"""
alpha_agent.price_factor_catalogue - Stage 9.4 Track A: the pre-registered,
economically-motivated price-factor catalogue.

Every candidate is registered BEFORE evaluation with an explicit economic
hypothesis, an exact formula, a horizon, and an expected sign. The catalogue is
deterministic (a pure function of this file), capped at three controlled variants
per hypothesis (no brute-force horizon grid), and deduplicated against the
existing Stage 9 catalogue so no candidate re-registers an already-scored factor.

Factors whose formula needs data the owned close panel does not carry (volume /
turnover, intraday/overnight opens, point-in-time GICS sectors, event calendars)
are pre-registered but flagged with a specific ``data_requirement`` that routes
them to an HONEST DATA_HOLD - never approximated onto the close panel.

Pure stdlib; no clock, no randomness, no state writes.
"""
from __future__ import annotations

from typing import Optional

from . import price_factors as pf
from . import tournament as tt

# Data requirements and the specific DATA_HOLD blocker each maps to when it is
# not satisfiable from the owned close panel.
REQ_OWNED_CLOSE = "owned_close"
REQ_VOLUME = "owned_volume_turnover"
REQ_INTRADAY = "intraday_open"
REQ_PIT_GICS = "point_in_time_gics"
REQ_EVENT_CAL = "event_calendar"

_HELD_REQUIREMENTS = (REQ_VOLUME, REQ_INTRADAY, REQ_PIT_GICS, REQ_EVENT_CAL)

CATALOGUE_VERSION = "stage9_4-price-catalogue-1.0.0"
_UNIVERSE = "S&P 500 Current & Past (Norgate survivorship-safe)"

# Each entry: economic hypothesis with an exact formula, expected sign, the owned
# feature key that computes it (when computable) and the horizon variants (<=3).
# ``data_requirement`` == owned_close => computed here; otherwise DATA_HOLD.
PRE_REGISTERED: tuple[dict, ...] = (
    # ---- 1. TREND QUALITY ------------------------------------------------- #
    {"family": "TREND_QUALITY", "hypothesis_id": "trend_slope_quality",
     "name": "Trend slope t-statistic", "feature": "trend_slope_t",
     "formula": "t-stat of OLS slope of log(close) on time over 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21, 63)},
    {"family": "TREND_QUALITY", "hypothesis_id": "trend_path_efficiency",
     "name": "Path efficiency ratio", "feature": "path_efficiency",
     "formula": "(close_t - close_{t-126}) / sum(|dclose|) over 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21,)},
    {"family": "TREND_QUALITY", "hypothesis_id": "drawdown_adjusted_trend",
     "name": "Drawdown-adjusted trend", "feature": "drawdown_adjusted_trend",
     "formula": "trailing 252d return / (1 + |max drawdown|)",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21, 63)},
    {"family": "TREND_QUALITY", "hypothesis_id": "positive_day_fraction",
     "name": "Fraction of positive-return days", "feature": None,
     "formula": "share of days with positive close-to-close return over 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21,), "note": "diagnostic; folded into trend quality"},
    # ---- 2. BREAKOUT AND COMPRESSION -------------------------------------- #
    {"family": "BREAKOUT_COMPRESSION", "hypothesis_id": "distance_52w_high",
     "name": "Distance from 52-week high", "feature": "distance_from_52w_high",
     "formula": "close_t / max(close, 252d) - 1", "expected_sign": 1,
     "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    {"family": "BREAKOUT_COMPRESSION", "hypothesis_id": "channel_breakout",
     "name": "Channel breakout", "feature": "channel_breakout",
     "formula": "(close_t - max(close, prior 63d)) / close_t",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21,)},
    {"family": "BREAKOUT_COMPRESSION",
     "hypothesis_id": "breakout_post_compression",
     "name": "Breakout after volatility compression",
     "feature": "breakout_post_compression",
     "formula": "channel_breakout * (long_vol / short_vol)",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21,)},
    {"family": "BREAKOUT_COMPRESSION",
     "hypothesis_id": "breakout_dollar_volume",
     "name": "Breakout with dollar-volume confirmation", "feature": None,
     "formula": "channel_breakout gated by dollar-volume expansion",
     "expected_sign": 1, "data_requirement": REQ_VOLUME, "horizons": (21,)},
    # ---- 3. RESIDUAL AND RELATIVE MOMENTUM -------------------------------- #
    {"family": "RESIDUAL_RELATIVE_MOM",
     "hypothesis_id": "market_residual_momentum",
     "name": "Market-residual momentum", "feature": "market_residual_momentum",
     "formula": "cumulative market-regression residual return, 12-1 window",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21, 63)},
    {"family": "RESIDUAL_RELATIVE_MOM", "hypothesis_id": "beta_neutral_momentum",
     "name": "Beta-neutral momentum", "feature": "beta_neutral_momentum",
     "formula": "name 12-1 momentum - beta * market 12-1 momentum",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21,)},
    {"family": "RESIDUAL_RELATIVE_MOM",
     "hypothesis_id": "sector_residual_momentum",
     "name": "Sector-residual momentum", "feature": None,
     "formula": "12-1 momentum minus PIT-GICS sector-mean momentum",
     "expected_sign": 1, "data_requirement": REQ_PIT_GICS, "horizons": (21,)},
    # ---- 4. VOLATILITY DYNAMICS ------------------------------------------- #
    {"family": "VOLATILITY_DYNAMICS", "hypothesis_id": "idiosyncratic_vol",
     "name": "Idiosyncratic volatility (low)",
     "feature": "idiosyncratic_volatility",
     "formula": "-stdev of market-regression residual returns, 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21, 63)},
    {"family": "VOLATILITY_DYNAMICS", "hypothesis_id": "short_long_vol_ratio",
     "name": "Short-vs-long realized-volatility ratio",
     "feature": "short_long_vol_ratio",
     "formula": "-(stdev 21d returns / stdev 126d returns)",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    {"family": "VOLATILITY_DYNAMICS", "hypothesis_id": "vol_of_vol",
     "name": "Volatility of volatility", "feature": "vol_of_vol",
     "formula": "-stdev of rolling 21d realized volatility, 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    # ---- 5. LIQUIDITY AND TURNOVER (owned volume unavailable) ------------- #
    {"family": "LIQUIDITY_TURNOVER", "hypothesis_id": "dollar_volume_shock",
     "name": "Dollar-volume shock", "feature": None,
     "formula": "z-score of dollar volume vs trailing 63d", "expected_sign": 1,
     "data_requirement": REQ_VOLUME, "horizons": (21,)},
    {"family": "LIQUIDITY_TURNOVER", "hypothesis_id": "amihud_illiquidity_change",
     "name": "Amihud-style illiquidity change", "feature": None,
     "formula": "change in |return| / dollar volume vs trailing", "expected_sign": 1,
     "data_requirement": REQ_VOLUME, "horizons": (21,)},
    # ---- 6. RETURN DISTRIBUTION ------------------------------------------- #
    {"family": "RETURN_DISTRIBUTION", "hypothesis_id": "max_daily_return",
     "name": "Maximum daily return effect", "feature": "max_daily_return",
     "formula": "-max(daily return, trailing 21d)", "expected_sign": 1,
     "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    {"family": "RETURN_DISTRIBUTION", "hypothesis_id": "realized_skewness",
     "name": "Realized skewness", "feature": "realized_skewness",
     "formula": "-sample skewness of daily returns, 126d", "expected_sign": 1,
     "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    {"family": "RETURN_DISTRIBUTION", "hypothesis_id": "gap_concentration",
     "name": "Overnight gap concentration", "feature": None,
     "formula": "share of variance from overnight gaps", "expected_sign": 1,
     "data_requirement": REQ_INTRADAY, "horizons": (21,)},
    # ---- 7. MARKET SENSITIVITY -------------------------------------------- #
    {"family": "MARKET_SENSITIVITY", "hypothesis_id": "low_beta",
     "name": "Low beta", "feature": "low_beta",
     "formula": "-OLS beta of daily returns vs equal-weight market, 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE,
     "horizons": (21, 63)},
    {"family": "MARKET_SENSITIVITY", "hypothesis_id": "downside_beta",
     "name": "Low downside beta", "feature": "downside_beta",
     "formula": "-beta computed on down-market days, 126d", "expected_sign": 1,
     "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    {"family": "MARKET_SENSITIVITY", "hypothesis_id": "defensive_residual_return",
     "name": "Defensive residual return", "feature": "defensive_residual_return",
     "formula": "cumulative market-regression residual (alpha) return, 126d",
     "expected_sign": 1, "data_requirement": REQ_OWNED_CLOSE, "horizons": (21,)},
    # ---- 8. OVERNIGHT / INTRADAY (owned opens unavailable) ---------------- #
    {"family": "OVERNIGHT_INTRADAY", "hypothesis_id": "overnight_persistence",
     "name": "Overnight return persistence", "feature": None,
     "formula": "trailing mean overnight (close-to-open) return",
     "expected_sign": 1, "data_requirement": REQ_INTRADAY, "horizons": (21,)},
    {"family": "OVERNIGHT_INTRADAY", "hypothesis_id": "overnight_intraday_divergence",
     "name": "Overnight-vs-intraday divergence", "feature": None,
     "formula": "trailing overnight return minus intraday return",
     "expected_sign": 1, "data_requirement": REQ_INTRADAY, "horizons": (21,)},
    # ---- 9. SEASONALITY (needs PIT event calendar) ------------------------ #
    {"family": "SEASONALITY", "hypothesis_id": "turn_of_month",
     "name": "Turn-of-month seasonality", "feature": None,
     "formula": "name's historical turn-of-month excess return",
     "expected_sign": 1, "data_requirement": REQ_EVENT_CAL, "horizons": (21,)},
    {"family": "SEASONALITY", "hypothesis_id": "earnings_calendar_seasonality",
     "name": "Earnings-calendar seasonality", "feature": None,
     "formula": "expected return in the name's earnings month (PIT dates)",
     "expected_sign": 1, "data_requirement": REQ_EVENT_CAL, "horizons": (21,)},
)

_VARIANT_CAP = 3


def _validate_variant_caps() -> None:
    for h in PRE_REGISTERED:
        if len(h["horizons"]) > _VARIANT_CAP:
            raise ValueError("hypothesis %s exceeds %d variants"
                             % (h["hypothesis_id"], _VARIANT_CAP))


_validate_variant_caps()


def families() -> list[str]:
    seen: list[str] = []
    for h in PRE_REGISTERED:
        if h["family"] not in seen:
            seen.append(h["family"])
    return seen


def variant_counts_by_family() -> dict[str, int]:
    out: dict[str, int] = {}
    for h in PRE_REGISTERED:
        out[h["family"]] = out.get(h["family"], 0) + len(h["horizons"])
    return out


def hypotheses_by_family() -> dict[str, int]:
    out: dict[str, int] = {}
    for h in PRE_REGISTERED:
        out[h["family"]] = out.get(h["family"], 0) + 1
    return out


def search_family_size(hypothesis_id: str) -> int:
    for h in PRE_REGISTERED:
        if h["hypothesis_id"] == hypothesis_id:
            return len(h["horizons"])
    return 1


def _candidate_spec(h: dict, horizon: int) -> dict:
    """One tournament-seed candidate spec (deterministic). Computable factors
    carry the owned close feature; held factors carry a data_requirement that
    routes the tournament to a specific DATA_HOLD blocker."""
    computable = (h["data_requirement"] == REQ_OWNED_CLOSE
                  and h["feature"] in pf.COMPUTABLE_FEATURES)
    feature = h["feature"] if computable else h["hypothesis_id"]
    spec = {
        "feature": feature, "horizon_days": int(horizon), "rebalance": "monthly",
        "template": "campaign_price_factor" if computable
        else "price_factor_data_hold",
        "factor_family": h["family"], "hypothesis_id": h["hypothesis_id"],
        "expected_sign": h["expected_sign"], "formula": h["formula"],
        "catalogue": CATALOGUE_VERSION,
        "search_family_size": len(h["horizons"]),
    }
    if not computable:
        spec["data_requirement"] = h["data_requirement"]
    pit = tt.PIT_OWNED_PRICE if computable else _REQ_PIT_STATUS[h["data_requirement"]]
    return {
        "name": "%s (%dd)" % (h["name"], horizon),
        "family": tt.FAM_PRICE_MOMENTUM,
        "spec": spec,
        "data_dependencies": (["owned_norgate_daily_bars"] if computable
                              else ["owned_norgate_daily_bars",
                                    h["data_requirement"]]),
        "universe": _UNIVERSE, "pit_status": pit,
        "component_signals": [feature],
    }


_REQ_PIT_STATUS = {
    REQ_VOLUME: "REQUIRES_OWNED_VOLUME_TURNOVER",
    REQ_INTRADAY: "REQUIRES_INTRADAY_OPEN",
    REQ_PIT_GICS: tt.PIT_NEEDS_GICS,
    REQ_EVENT_CAL: "REQUIRES_POINT_IN_TIME_EVENT_CALENDAR",
}


def catalogue_specs(*, exclude_existing: bool = True) -> list[dict]:
    """Deterministic list of pre-registered candidate specs (one per hypothesis
    x horizon variant), deduplicated against the existing Stage 9 catalogue by
    (family, spec_hash) so an already-registered factor is never re-added."""
    existing: set[tuple[str, str]] = set()
    if exclude_existing:
        for s in tt.default_candidate_specs():
            existing.add((s["family"], tt.spec_hash(s["spec"])))
    out: list[dict] = []
    seen_local: set[tuple[str, str]] = set()
    for h in PRE_REGISTERED:
        computable = (h["data_requirement"] == REQ_OWNED_CLOSE
                      and h["feature"] in pf.COMPUTABLE_FEATURES)
        # An owned-close idea without a materialized calculator (a diagnostic)
        # is pre-registered metadata but not seeded as a stuck candidate.
        if not computable and h["data_requirement"] == REQ_OWNED_CLOSE:
            continue
        for hz in h["horizons"]:
            cand = _candidate_spec(h, hz)
            key = (cand["family"], tt.spec_hash(cand["spec"]))
            if key in existing or key in seen_local:
                continue
            seen_local.add(key)
            out.append(cand)
    return out


def computable_specs() -> list[dict]:
    """Just the pre-registered candidates computable from the owned close panel
    (used by the bounded live proof)."""
    return [c for c in catalogue_specs()
            if c["spec"].get("template") == "campaign_price_factor"]


def data_hold_specs() -> list[dict]:
    return [c for c in catalogue_specs()
            if c["spec"].get("template") == "price_factor_data_hold"]


def catalogue_summary() -> dict:
    specs = catalogue_specs(exclude_existing=False)
    return {
        "catalogue_version": CATALOGUE_VERSION,
        "families": families(),
        "hypotheses": len(PRE_REGISTERED),
        "total_variants": sum(len(h["horizons"]) for h in PRE_REGISTERED),
        "variant_cap_per_hypothesis": _VARIANT_CAP,
        "hypotheses_by_family": hypotheses_by_family(),
        "variant_counts_by_family": variant_counts_by_family(),
        "computable_from_owned_close": len(computable_specs()),
        "data_hold_requirements": sorted(_HELD_REQUIREMENTS),
    }


__all__ = ["PRE_REGISTERED", "CATALOGUE_VERSION", "families",
           "variant_counts_by_family", "hypotheses_by_family",
           "search_family_size", "catalogue_specs", "computable_specs",
           "data_hold_specs", "catalogue_summary",
           "REQ_OWNED_CLOSE", "REQ_VOLUME", "REQ_INTRADAY", "REQ_PIT_GICS",
           "REQ_EVENT_CAL"]
