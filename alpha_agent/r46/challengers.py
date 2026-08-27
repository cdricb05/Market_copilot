"""alpha_agent.r46.challengers - the frozen seed cohort.

Ten economically distinct challengers across five asset classes and three
horizons. Every parameter in this file is a canonical constant taken from the
published asset-pricing literature and written down before
:mod:`alpha_agent.r46.marketdata` was first called: 12-1 momentum, five-day
reversal, sixty-day volatility, 252-day trend, the 200-day filter, a
one-sigma reversion band, decile portfolios. Nothing here was chosen by
sweeping this estate's data.

That is the whole point. Release 45 re-ran Release 44's sixty-cell screen
separately on three event zones and found a different winner every time - the
last one bigger than the published headline - because the maximum of a noisy
grid always looks locally peaked from the inside. A release that picks no cell
cannot be fooled that way, and it is why R46 charges essentially no new
historical search burden.

Two things this module deliberately does NOT do:

* it does not predict a magnitude. These are transparent rules, not calibrated
  return forecasts, so ``expected_return`` is emitted as ``None`` with
  ``expected_return_state = NOT_CALIBRATED``. The cost, which IS known before
  the fact, is emitted as a number. Inventing an expected return to fill a
  schema field would be the first lie in an evidence chain built to prevent
  them.
* it does not combine anything. Release 44 measured the combination frontier
  over twelve streams and the answer did not depend on the weighting scheme.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import clock as CK
from . import contract as C
from . import marketdata as MD
from . import sha

CALCULATION_OWNER = "alpha_agent.r46.challengers"

K = C.SEED_PARAMETERS_WERE_NOT_SEARCHED["canonical_constants"]

BENCHMARK_EQUITY = "SPY"

#: Declared liquid futures markets, grouped. The grouping is market structure,
#: not return data: an exchange's flagship contract is the flagship contract
#: whatever it returned last year.
FUTURES_GROUPS = {
    "EQUITY_INDEX_FUTURES": ("&ES", "&NQ", "&YM", "&RTY", "&EMD", "&NKD",
                             "&FDAX", "&FESX"),
    "RATES_FUTURES": ("&ZT", "&ZF", "&ZN", "&ZB", "&UB", "&FGBL", "&FGBM",
                      "&FGBS", "&CGB"),
    "FX_FUTURES": ("&6A", "&6B", "&6C", "&6E", "&6J", "&6M", "&6N", "&6S",
                   "&DX"),
    "COMMODITY_FUTURES": ("&CL", "&NG", "&HO", "&RB", "&BRN", "&GC", "&SI",
                          "&HG", "&PL", "&PA", "&ZC", "&ZS", "&ZW", "&ZM",
                          "&ZL", "&KC", "&CT", "&SB", "&CC", "&LE", "&HE"),
    "VOLATILITY_FUTURES": ("&VX",),
}

#: The G10 crosses, all quoted USD-per-foreign so a rise always means the
#: foreign currency appreciated. Mixing AUDUSD with USDJPY in one
#: cross-section is a sign error waiting to happen.
G10_USD_PER_FOREIGN = ("AUDUSD", "CADUSD", "CHFUSD", "EURUSD", "GBPUSD",
                       "JPYUSD", "NZDUSD", "NOKUSD", "SEKUSD")

COMMODITY_MARKETS = FUTURES_GROUPS["COMMODITY_FUTURES"]

MIN_CROSS_SECTION = 30      # names needed before a decile book means anything
MIN_FUTURES_MARKETS = 12
MIN_FX_PAIRS = 6


# --------------------------------------------------------------------------- #
# The frozen specifications
# --------------------------------------------------------------------------- #
def _spec(**kw) -> dict:
    spec = {
        "challenger_version": "v1",
        "promotion_allowed": False,
        "research_shadow_only": True,
        "origin": "R46_SEED",
        "parameters_were_searched": False,
        "expected_return_state": "NOT_CALIBRATED",
    }
    spec.update(kw)
    return spec


SEED_SPECS = (
    _spec(
        challenger_id="r46_eq_xs_mom_12_1",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(5, 20),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="twelve-month winners keep winning over one to four weeks; the "
               "one-month skip removes the short-horizon reversal that would "
               "otherwise contaminate the signal",
        parameters={"formation_days": K["momentum_formation_days"],
                    "skip_days": K["momentum_skip_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_rev_5d",
        family="CROSS_SECTIONAL_REVERSAL",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="a week of one-sided pressure in a large-cap name is mostly "
               "liquidity provision, and it is paid back over the next session",
        parameters={"reversal_days": K["reversal_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_lowvol_60d",
        family="LOW_RISK_ANOMALY",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="leverage-constrained investors bid up high-beta names, so "
               "low-volatility stocks earn more per unit of risk than the "
               "CAPM allows",
        parameters={"volatility_days": K["volatility_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_resid_mom_12_1",
        family="RESIDUAL_MOMENTUM",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="momentum measured on market-beta residuals carries the same "
               "continuation without the embedded market-timing bet, so it "
               "should survive where raw momentum crashes",
        parameters={"formation_days": K["momentum_formation_days"],
                    "skip_days": K["momentum_skip_days"],
                    "beta_days": K["beta_days"],
                    "decile_fraction": K["decile_fraction"],
                    "market": BENCHMARK_EQUITY},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_fut_ts_mom_252",
        family="TIME_SERIES_TREND",
        asset_class="MULTI_ASSET_FUTURES",
        instrument="BOOK:FUTURES_TS_TREND",
        prediction_type="TIME_SERIES_DIRECTIONAL_BASKET",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="MIXED_FUTURES",
        universe="declared liquid continuous futures across equity index, "
                 "rates, FX and commodities",
        thesis="risk transfer from hedgers to speculators pays a premium that "
               "shows up as twelve-month trend persistence, and it is the one "
               "premium documented in every futures market simultaneously",
        parameters={"trend_days": K["trend_days"],
                    "volatility_days": K["volatility_days"],
                    "sizing": "inverse realised volatility, gross notional 1"},
        signal_owner="_futures_trend",
    ),
    _spec(
        challenger_id="r46_fx_xs_mom_252",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="FX",
        instrument="BOOK:G10_FX_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="FX_SPOT",
        universe="G10 crosses quoted USD per foreign unit",
        thesis="currency trends persist because central banks move slowly and "
               "in the same direction for years at a time",
        parameters={"formation_days": K["trend_days"], "n_per_leg": 3},
        signal_owner="_fx_cross_section",
    ),
    _spec(
        challenger_id="r46_vx_term_carry_5d",
        family="VOLATILITY_TERM_CARRY",
        asset_class="VOLATILITY",
        instrument="&VX",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="VOLATILITY_FUTURES",
        universe="front VIX future against VIX spot",
        thesis="the VIX curve is in contango most of the time because "
               "variance is insurance, and a short front future earns the "
               "roll-down when it is",
        parameters={"basis": "front &VX close / $VIX close - 1",
                    "position": "short when basis > 0, long when basis < 0"},
        signal_owner="_vx_carry",
        economic_overlap_with=("R39:shadow_vx_carry_ts",),
        overlap_note="economically related to the adopted R39 shadow, which "
                     "decides on VX Fridays with its own expression; the two "
                     "must not be counted as independent evidence",
    ),
    _spec(
        challenger_id="r46_rates_curve_rv_5d",
        family="RATES_RELATIVE_VALUE",
        asset_class="RATES",
        instrument="BOOK:ZN_ZT_DURATION_NEUTRAL",
        prediction_type="RELATIVE_VALUE_SPREAD",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="RATES_FUTURES",
        universe="&ZN against &ZT, volatility-neutral",
        thesis="the belly of the Treasury curve is pushed around by hedging "
               "flow that reverses within a week once the flow clears",
        parameters={"spread_z_days": K["spread_z_days"],
                    "entry_z": 1.0,
                    "hedge": "inverse realised volatility on each leg"},
        signal_owner="_rates_rv",
        hedge_definition="long &ZN / short &ZT scaled to equal realised "
                         "volatility; both legs charged full cost",
    ),
    _spec(
        challenger_id="r46_comdty_xs_mom_252",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="COMMODITY",
        instrument="BOOK:COMMODITY_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="COMMODITY_FUTURES",
        universe="declared liquid energy, metal and agricultural futures",
        thesis="commodity trends are carried by slow physical inventory "
               "adjustment, which no announcement resets",
        parameters={"formation_days": K["trend_days"], "leg_fraction": 1 / 3.0},
        signal_owner="_commodity_cross_section",
    ),
    _spec(
        challenger_id="r46_spx_trend_200d",
        family="INDEX_TREND_TIMING",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_VS_BENCHMARK",
        horizons=(20,),
        control=C.CONTROL_BENCHMARK,
        benchmark="SPY",
        cost_class="US_ETF",
        universe="SPY",
        thesis="the 200-day filter is the oldest published trend rule there "
               "is; if trend timing adds anything over simply owning the "
               "index, this is where it shows",
        parameters={"filter_days": K["trend_filter_days"],
                    "position": "long SPY above the average, cash below"},
        signal_owner="_index_trend",
    ),
)


# --------------------------------------------------------------------------- #
# Release 46.3 - the EXPANSION cohort. Same door, wider field.
#
# Every rule of the seed cohort applies unchanged: parameters are canonical
# constants declared here BEFORE any of these rules read a bar of this
# estate's data to be selected; nothing was swept, screened or ranked; a
# challenger that starts losing is never edited, only versioned. What the
# expansion adds is BREADTH along the axes evidence velocity actually depends
# on - economic mechanisms, information families, asset structures, horizons -
# and it declares, per challenger, which DEPENDENCE CLUSTER its evidence
# belongs to, so the velocity owner can refuse to count related bets twice.
# --------------------------------------------------------------------------- #
EXPANSION_COHORT = "R46_3_EXPANSION"

EXPANSION_CANONICAL_CONSTANTS = {
    "statement": (
        "every expansion parameter below is a canonical constant from the "
        "published literature, written into this module before any of these "
        "rules was first run on this estate's data. No sweep, screen or "
        "ranking on owned returns selected any of them."),
    "max_window_days": 21,          # Bali, Cakici & Whitelaw's MAX month
    "max_top_days": 5,              # MAX(5): mean of the 5 largest daily moves
    "amihud_days": 252,             # Amihud's one-year illiquidity average
    "seasonal_lag_years": 5,        # Heston & Sadka same-month lags 1..5
    "seasonal_min_years": 3,
    "turn_of_month_window": (-1, 3),   # McConnell & Xu: last day through +3
    "futures_leg_fraction": 1 / 3.0,   # Moskowitz-style thirds
    "curve_skip_spot_month": True,     # delivery distortions are not carry
    "ml_training_sessions": 756,       # three years of dailies
    "ml_training_stride_sessions": 21, # monthly samples: overlapping targets
                                       # inside training would inflate fit
    "ml_target_sessions": 20,          # the challenger's own horizon
    "ml_ridge_lambda": 1.0,            # unit penalty on z-scored features
    "ml_gbt_max_iter": 100,            # library defaults, declared, frozen
    "ml_gbt_max_depth": 3,
    "ml_gbt_learning_rate": 0.05,
    "ml_random_seed": 46,
}

XK = EXPANSION_CANONICAL_CONSTANTS

#: The six features every ML challenger sees. Frozen as a set; hashed via each
#: spec's parameters. All are computable from owned bars at the data cutoff.
ML_FEATURES = ("mom_12_1", "rev_5d", "vol_60d", "beta_60d", "max_21d",
               "amihud_252d")

#: The declared retraining policy for the ML cohort. Refit happens at EVERY
#: emission, deterministically, from the trailing window - predeclared here so
#: it is a contract clause and not a silent habit. A change to this protocol
#: is a MATERIAL change and forces a new version with a new forward clock.
ML_RETRAINING_POLICY = (
    "refit deterministically at each emission from the trailing "
    "%d-session window, sampled every %d sessions; fixed feature set, fixed "
    "preprocessing (cross-sectional z-score, clipped at 3 sigma), fixed "
    "hyperparameters, fixed random seed %d; no hyperparameter search ever; "
    "forward results may never choose the model"
    % (XK["ml_training_sessions"], XK["ml_training_stride_sessions"],
       XK["ml_random_seed"]))

EXPANSION_SPECS = (
    _spec(
        challenger_id="r46_3_eq_xs_max_lottery",
        family="LOTTERY_DEMAND",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_MAX",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="investors overpay for lottery-like payoffs, so names with the "
               "largest recent daily jumps subsequently underperform names "
               "without them",
        parameters={"max_window_days": XK["max_window_days"],
                    "max_top_days": XK["max_top_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_xs_lottery",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_STATE",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_eq_xs_rev_5d", "r46_eq_xs_lowvol_60d"),
        overlap_note="price-state cross-section on the same universe as the "
                     "seed equity cells; counted in the same dependence "
                     "cluster, never as independent evidence",
    ),
    _spec(
        challenger_id="r46_3_eq_xs_amihud_illiq",
        family="LIQUIDITY_PREMIUM",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ILLIQ",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="holding a name that is expensive to trade earns compensation; "
               "|return| per dollar of volume prices that inventory risk even "
               "inside the large-cap universe",
        parameters={"amihud_days": XK["amihud_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_xs_illiquidity",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_VOLUME",
        dependence_cluster="EQ_XS_VOLUME",
    ),
    _spec(
        challenger_id="r46_3_eq_xs_seasonal_month",
        family="RETURN_SEASONALITY",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_SEASONAL",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="names that historically did well in this calendar month do "
               "well in it again - recurring flows and announcement calendars "
               "repeat on an annual clock",
        parameters={"lag_years": XK["seasonal_lag_years"],
                    "min_years": XK["seasonal_min_years"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_xs_seasonal",
        cohort=EXPANSION_COHORT,
        information_family="CALENDAR_STRUCTURE",
        dependence_cluster="EQ_SEASONALITY",
    ),
    _spec(
        challenger_id="r46_3_fut_xs_mom_252",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="MULTI_ASSET_FUTURES",
        instrument="BOOK:FUTURES_XS_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="MIXED_FUTURES",
        universe="declared liquid continuous futures across equity index, "
                 "rates, FX and commodities",
        thesis="RELATIVE twelve-month strength across futures markets "
               "persists; unlike the time-series trend book this is a "
               "dollar-neutral relative bet, not an absolute one",
        parameters={"formation_days": K["trend_days"],
                    "leg_fraction": XK["futures_leg_fraction"]},
        signal_owner="_futures_xs_momentum",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_STATE",
        dependence_cluster="FUTURES_TREND_PRICE",
        economic_overlap_with=("r46_fut_ts_mom_252",),
        overlap_note="same information family and largely the same markets as "
                     "the time-series trend book; same dependence cluster",
    ),
    _spec(
        challenger_id="r46_3_comdty_curve_carry",
        family="FUTURES_CURVE_CARRY",
        asset_class="COMMODITY",
        instrument="BOOK:COMMODITY_CURVE_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(5, 20),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="COMMODITY_FUTURES",
        universe="declared liquid commodity futures with a readable dated "
                 "curve (front and next delivery months)",
        thesis="backwardation pays the long and contango pays the short: the "
               "curve's slope is the price of storage and hedging pressure, "
               "and it accrues to whoever carries the position",
        parameters={"leg_fraction": XK["futures_leg_fraction"],
                    "skip_spot_month": XK["curve_skip_spot_month"],
                    "slope": "ln(front/next) * 12 / months_between"},
        signal_owner="_commodity_curve_carry",
        cohort=EXPANSION_COHORT,
        information_family="FUTURES_CURVE",
        dependence_cluster="COMMODITY_CURVE",
    ),
    _spec(
        challenger_id="r46_3_rates_curve_carry",
        family="TERM_PREMIUM_CARRY",
        asset_class="RATES",
        instrument="&ZN",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="RATES_FUTURES",
        universe="&ZN, signed by the owned 10y-2y constant-maturity spread",
        thesis="a steep curve pays duration holders carry and roll-down; an "
               "inverted curve takes it away - the oldest bond risk premium "
               "there is, read from OWNED yield series rather than prices",
        parameters={"long_series": "%10YTCM", "short_series": "%2YTCM",
                    "position": "long &ZN when 10y-2y > 0, short when < 0",
                    "max_series_lag_sessions": 5},
        signal_owner="_rates_macro_curve",
        cohort=EXPANSION_COHORT,
        information_family="MACRO_RATES_LEVELS",
        dependence_cluster="RATES_MACRO_CARRY",
    ),
    _spec(
        challenger_id="r46_3_spx_turn_of_month",
        family="CALENDAR_SEASONALITY",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_ETF",
        universe="SPY",
        thesis="pension and payroll flows cluster at month turns; equity "
               "returns concentrate in the last session and first three "
               "sessions of the month",
        parameters={"window": list(XK["turn_of_month_window"]),
                    "position": "long SPY only when the entry session falls "
                                "inside the turn-of-month window"},
        signal_owner="_spx_turn_of_month",
        cohort=EXPANSION_COHORT,
        information_family="CALENDAR_STRUCTURE",
        dependence_cluster="CALENDAR_TOM",
    ),
    _spec(
        challenger_id="r46_3_vx_term_carry_1d",
        family="VOLATILITY_TERM_CARRY",
        asset_class="VOLATILITY",
        instrument="&VX",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="VOLATILITY_FUTURES",
        universe="front VIX future against VIX spot",
        thesis="the roll-down of a contango VIX curve accrues session by "
               "session; a one-session horizon reads that accrual on the "
               "fastest legitimate clock this data supports",
        parameters={"basis": "front &VX close / $VIX close - 1",
                    "position": "short when basis > 0, long when basis < 0"},
        signal_owner="_vx_carry",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_STATE",
        dependence_cluster="VX_CARRY",
        economic_overlap_with=("r46_vx_term_carry_5d",
                               "R39:shadow_vx_carry_ts"),
        overlap_note="the same signal as the seed 5-session cell on a faster "
                     "clock; one dependence cluster, never two",
    ),
    _spec(
        challenger_id="r46_3_ens_eq_xs_equal",
        family="PROSPECTIVE_ENSEMBLE",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ENS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="an equal-weight combination of momentum, low-volatility and "
               "residual momentum ranks diversifies rule-level noise; the "
               "weights are frozen at one third each BEFORE any member has a "
               "single matured forward observation, so no forward result "
               "chose them",
        parameters={"members": ["momentum_12_1", "low_volatility_60d",
                                "residual_momentum_12_1"],
                    "weights": [1 / 3.0, 1 / 3.0, 1 / 3.0],
                    "combination": "equal-weight average of cross-sectional "
                                   "percentile ranks",
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_xs_ensemble",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_STATE",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_eq_xs_mom_12_1", "r46_eq_xs_lowvol_60d",
                               "r46_eq_xs_resid_mom_12_1"),
        overlap_note="a fixed combination of three seed signals; same "
                     "dependence cluster as its members",
    ),
    _spec(
        challenger_id="r46_3_ml_eq_xs_ridge",
        family="ML_CROSS_SECTIONAL_LINEAR",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ML_RIDGE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="a regularised linear combination of the declared canonical "
               "features may weight them better than any single rule; if it "
               "cannot beat its own ingredients net of cost, that is worth "
               "knowing on the forward record",
        parameters={"model_class": "RIDGE_CLOSED_FORM",
                    "features": list(ML_FEATURES),
                    "preprocessing": "cross-sectional z-score, clip 3 sigma",
                    "ridge_lambda": XK["ml_ridge_lambda"],
                    "training_sessions": XK["ml_training_sessions"],
                    "training_stride_sessions":
                        XK["ml_training_stride_sessions"],
                    "target_sessions": XK["ml_target_sessions"],
                    "retraining_policy": ML_RETRAINING_POLICY,
                    "random_seed": XK["ml_random_seed"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_ml_eq_cross_section",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_VOLUME",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_eq_xs_mom_12_1", "r46_eq_xs_rev_5d",
                               "r46_eq_xs_lowvol_60d",
                               "r46_eq_xs_resid_mom_12_1"),
        overlap_note="learned from the same price-state features the seed "
                     "cells use; same dependence cluster",
    ),
    _spec(
        challenger_id="r46_3_ml_eq_xs_gbt",
        family="ML_CROSS_SECTIONAL_NONLINEAR",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ML_GBT",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="if the cross-section rewards INTERACTIONS the linear rules "
               "cannot express - momentum conditional on volatility, reversal "
               "conditional on liquidity - a shallow boosted tree is the "
               "bounded way to find out; identical features, identical "
               "protocol, so the comparison against the ridge cell isolates "
               "nonlinearity itself",
        parameters={"model_class": "HIST_GRADIENT_BOOSTING",
                    "features": list(ML_FEATURES),
                    "preprocessing": "cross-sectional z-score, clip 3 sigma",
                    "max_iter": XK["ml_gbt_max_iter"],
                    "max_depth": XK["ml_gbt_max_depth"],
                    "learning_rate": XK["ml_gbt_learning_rate"],
                    "training_sessions": XK["ml_training_sessions"],
                    "training_stride_sessions":
                        XK["ml_training_stride_sessions"],
                    "target_sessions": XK["ml_target_sessions"],
                    "retraining_policy": ML_RETRAINING_POLICY,
                    "random_seed": XK["ml_random_seed"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_ml_eq_cross_section",
        cohort=EXPANSION_COHORT,
        information_family="PRICE_VOLUME",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_3_ml_eq_xs_ridge",),
        overlap_note="same features and protocol as the ridge cell by design; "
                     "same dependence cluster",
    ),
)

# --------------------------------------------------------------------------- #
# Release 46.4 - the P&L OFFENSIVE cohort: four NEW information families.
#
# Same door, same rules. Every parameter below is a declared constant (the
# CFTC windows are Release 35's, the credit windows are canonical 63/21, the
# macro forecast is Release 45's, the calendar rules have no parameter at
# all, the ML hyperparameters are library-style constants), written here
# before any of these rules was first run on this estate's data to select it.
# Nothing was swept. What the cohort adds is INFORMATION that no active cell
# reads: positioning, credit spreads, first-published macro prints and the
# scheduled event calendar.
# --------------------------------------------------------------------------- #
R46_4_COHORT = "R46_4_PNL_OFFENSIVE"

R46_4_CANONICAL_CONSTANTS = {
    "statement": (
        "every Release-46.4 parameter is a declared constant written into "
        "this module before the rule first read a bar of this estate's data "
        "to be selected; no sweep, screen or ranking on owned returns chose "
        "any of them"),
    "cot_z_window_weeks": 156,          # Release 35's declared window
    "cot_change_weeks": 13,             # Release 35's declared window
    "cot_publication_lag_days": 6,      # Release 35's declared PIT lag
    "cot_leg_fraction": 1 / 3.0,        # thirds, as the futures books
    "credit_mean_window": 63,           # one quarter of sessions
    "credit_change_window": 21,         # one month of sessions
    "macro_forecast_window": 12,        # Release 45's declared window
    "macro_min_history": 24,            # Release 45's declared floor
    "ml_extra_trees_n_estimators": 200,
    "ml_extra_trees_max_depth": 4,
    "ml_extra_trees_min_samples_leaf": 50,
    "regime_gate_series": "$VIX",
    "regime_gate_level": 20.0,          # the textbook "elevated VIX" line
}
K4 = R46_4_CANONICAL_CONSTANTS

R46_4_SPECS = (
    _spec(
        challenger_id="r46_4_cot_xs_positioning_reversal",
        family="POSITIONING_REVERSAL",
        asset_class="MULTI_ASSET_FUTURES",
        instrument="BOOK:FUTURES_COT_REVERSAL_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="MIXED_FUTURES",
        universe="CFTC-mapped liquid continuous futures across equity index, "
                 "rates, FX, commodities and volatility",
        thesis="crowded speculative positioning is a liquidity demand that "
               "reverses: the most crowded longs underperform and the most "
               "crowded shorts outperform over the following month",
        parameters={"z_window_weeks": K4["cot_z_window_weeks"],
                    "leg_fraction": K4["cot_leg_fraction"],
                    "publication_lag_days": K4["cot_publication_lag_days"],
                    "signal": "minus the 156-week z-score of speculative net "
                              "position as a share of open interest"},
        signal_owner="_cot_xs_reversal",
        cohort=R46_4_COHORT,
        information_family="POSITIONING",
        dependence_cluster="FUT_POSITIONING",
    ),
    _spec(
        challenger_id="r46_4_cot_xs_positioning_flow",
        family="POSITIONING_FLOW",
        asset_class="MULTI_ASSET_FUTURES",
        instrument="BOOK:FUTURES_COT_FLOW_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="MIXED_FUTURES",
        universe="CFTC-mapped liquid continuous futures across equity index, "
                 "rates, FX, commodities and volatility",
        thesis="speculative flow persists over weeks as positions are built "
               "gradually; the markets speculators are buying keep rising",
        parameters={"change_weeks": K4["cot_change_weeks"],
                    "leg_fraction": K4["cot_leg_fraction"],
                    "publication_lag_days": K4["cot_publication_lag_days"],
                    "signal": "13-week change in speculative net position as "
                              "a share of open interest"},
        signal_owner="_cot_xs_flow",
        cohort=R46_4_COHORT,
        information_family="POSITIONING",
        dependence_cluster="FUT_POSITIONING",
        economic_overlap_with=("r46_4_cot_xs_positioning_reversal",),
        overlap_note="same report, opposite mechanism (level versus change); "
                     "one dependence cluster",
    ),
    _spec(
        challenger_id="r46_4_credit_regime_spx_timing",
        family="CREDIT_REGIME_TIMING",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_VS_BENCHMARK",
        horizons=(5,),
        control=C.CONTROL_BENCHMARK,
        benchmark="SPY",
        cost_class="US_ETF",
        universe="SPY, signed by the ICE BofA US High Yield OAS as published "
                 "(FRED/ALFRED vintage; owned Norgate series as fallback)",
        thesis="the credit market prices the same default risk equities "
               "price and it moves first; a spread below its recent mean is "
               "a benign regime in which owning the index is rewarded",
        parameters={"mean_window": K4["credit_mean_window"],
                    "position": "long SPY when HY OAS < its 63-observation "
                                "mean; cash otherwise"},
        signal_owner="_credit_regime_spx",
        cohort=R46_4_COHORT,
        information_family="CREDIT_SPREADS",
        dependence_cluster="CREDIT_REGIME",
    ),
    _spec(
        challenger_id="r46_4_credit_hy_ig_momentum",
        family="CREDIT_SPREAD_MOMENTUM",
        asset_class="CREDIT",
        instrument="BOOK:HYG_LQD_LS",
        prediction_type="RELATIVE_VALUE_SPREAD",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_ETF",
        universe="HYG against LQD, equal dollar legs",
        thesis="spread moves persist over weeks because credit repricing is "
               "slow and dealer-intermediated; tightening spreads favour high "
               "yield over investment grade",
        parameters={"change_window": K4["credit_change_window"],
                    "position": "long HYG / short LQD when the 21-observation "
                                "change in HY OAS < 0; reversed otherwise"},
        signal_owner="_credit_hy_ig_momentum",
        cohort=R46_4_COHORT,
        information_family="CREDIT_SPREADS",
        dependence_cluster="CREDIT_REGIME",
        hedge_definition="equal dollar legs; both legs charged full cost",
        economic_overlap_with=("r46_4_credit_regime_spx_timing",),
        overlap_note="same spread series read as change rather than level; "
                     "one dependence cluster",
    ),
    _spec(
        challenger_id="r46_4_macro_surprise_rates_5d",
        family="MACRO_RELEASE_SURPRISE",
        asset_class="RATES",
        instrument="&ZN",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="RATES_FUTURES",
        universe="&ZN on CPI and Employment Situation release days, signed "
                 "by the model-based FIRST-PUBLISHED surprise",
        thesis="an upside inflation or payrolls surprise is repriced into "
               "the front of the curve over days as the policy path is "
               "revised, not only in the first minute",
        parameters={"forecast_window": K4["macro_forecast_window"],
                    "min_history": K4["macro_min_history"],
                    "releases": ["CPI", "EMPLOYMENT"],
                    "position": "short &ZN on a positive surprise, long on a "
                                "negative one; flat on every other day; "
                                "sign only, no magnitude threshold"},
        signal_owner="_macro_surprise_rates",
        cohort=R46_4_COHORT,
        information_family="MACRO_RELEASE_SURPRISE",
        dependence_cluster="MACRO_SURPRISE",
    ),
    _spec(
        challenger_id="r46_4_spx_pre_fomc_drift",
        family="PRE_FOMC_DRIFT",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_ETF",
        universe="SPY",
        thesis="the equity premium concentrates in the twenty-four hours "
               "before a scheduled FOMC decision as uncertainty is resolved",
        parameters={"position": "long SPY only when the holding session is a "
                                "scheduled FOMC decision day"},
        signal_owner="_spx_pre_fomc",
        cohort=R46_4_COHORT,
        information_family="SCHEDULED_EVENT_CALENDAR",
        dependence_cluster="CALENDAR_EVENT",
    ),
    _spec(
        challenger_id="r46_4_spx_announcement_day_premium",
        family="ANNOUNCEMENT_DAY_PREMIUM",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_ETF",
        universe="SPY",
        thesis="the equity premium is earned on scheduled macro announcement "
               "days, when systematic risk is resolved, and is close to zero "
               "on other days",
        parameters={"position": "long SPY only when the holding session "
                                "carries a scheduled CPI, Employment "
                                "Situation or FOMC decision"},
        signal_owner="_spx_announcement_day",
        cohort=R46_4_COHORT,
        information_family="SCHEDULED_EVENT_CALENDAR",
        dependence_cluster="CALENDAR_EVENT",
        economic_overlap_with=("r46_4_spx_pre_fomc_drift",),
        overlap_note="FOMC days are in both calendars; one dependence cluster",
    ),
    _spec(
        challenger_id="r46_4_ml_eq_xs_extratrees",
        family="ML_CROSS_SECTIONAL_NONLINEAR",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ML_ET",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="a randomised forest has a different inductive bias from a "
               "boosted tree on the same features; if either nonlinearity "
               "pays after cost it should show in both, and if only one, "
               "that is a finding about variance rather than about the "
               "cross-section",
        parameters={"model_class": "EXTRA_TREES",
                    "features": list(ML_FEATURES),
                    "preprocessing": "cross-sectional z-score, clip 3 sigma",
                    "n_estimators": K4["ml_extra_trees_n_estimators"],
                    "max_depth": K4["ml_extra_trees_max_depth"],
                    "min_samples_leaf": K4["ml_extra_trees_min_samples_leaf"],
                    "training_sessions": XK["ml_training_sessions"],
                    "training_stride_sessions":
                        XK["ml_training_stride_sessions"],
                    "target_sessions": XK["ml_target_sessions"],
                    "retraining_policy": ML_RETRAINING_POLICY,
                    "random_seed": XK["ml_random_seed"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_ml_eq_cross_section",
        cohort=R46_4_COHORT,
        information_family="PRICE_VOLUME",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_3_ml_eq_xs_gbt", "r46_3_ml_eq_xs_ridge"),
        overlap_note="same features and protocol as the R46.3 ML cells; "
                     "same dependence cluster",
    ),
    _spec(
        challenger_id="r46_4_ml_eq_xs_regime_gated",
        family="REGIME_GATED_ENSEMBLE",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE_ML_GATED",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="linear structure may dominate in calm markets and "
               "interactions in stressed ones; a frozen volatility gate "
               "between the ridge and the boosted tree tests that without "
               "letting any forward result choose the gate",
        parameters={"model_class": "REGIME_GATED_RIDGE_GBT",
                    "features": list(ML_FEATURES),
                    "preprocessing": "cross-sectional z-score, clip 3 sigma",
                    "gate_series": K4["regime_gate_series"],
                    "gate_level": K4["regime_gate_level"],
                    "gate_rule": "ridge when the gate series close at the "
                                 "data cutoff is at or below the level; "
                                 "gradient boosting above it",
                    "ridge_lambda": XK["ml_ridge_lambda"],
                    "max_iter": XK["ml_gbt_max_iter"],
                    "max_depth": XK["ml_gbt_max_depth"],
                    "learning_rate": XK["ml_gbt_learning_rate"],
                    "training_sessions": XK["ml_training_sessions"],
                    "training_stride_sessions":
                        XK["ml_training_stride_sessions"],
                    "target_sessions": XK["ml_target_sessions"],
                    "retraining_policy": ML_RETRAINING_POLICY,
                    "random_seed": XK["ml_random_seed"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_ml_eq_cross_section",
        cohort=R46_4_COHORT,
        information_family="PRICE_VOLUME",
        dependence_cluster="EQ_XS_PRICE",
        economic_overlap_with=("r46_3_ml_eq_xs_gbt", "r46_3_ml_eq_xs_ridge"),
        overlap_note="a gate between two existing members; same cluster",
    ),
)

#: The full frozen field: seed cohort plus expansion cohort plus the Release
#: 46.4 P&L-offensive cohort. Registration and emission default to this; each
#: tuple keeps its own name and its own bytes because the earlier
#: specifications are evidence.
# --------------------------------------------------------------------------- #
# Release 46.5 - the forward-harvest cohort. Two information families no
# active cell reads: per-name EARNINGS announcement instants (SEC 8-K Item
# 2.02 acceptance stamps) and daily INSIDER FLOW (SEC Form 4). Every constant
# is the literature's, declared here before any of these rules first read a
# bar of this estate's data. Nothing was swept.
# --------------------------------------------------------------------------- #
R46_5_COHORT = "R46_5_FORWARD_HARVEST"

R46_5_CANONICAL_CONSTANTS = {
    "statement": (
        "every Release-46.5 parameter is a declared constant written into "
        "this module before the rule first read a bar of this estate's data "
        "to be selected; no sweep, screen or ranking on owned returns chose "
        "any of them"),
    "pead_window_sessions": 5,          # announcements in the trailing week
    "pead_leg_fraction": 1 / 3.0,       # terciles, as the event literature
    "pead_min_names": 15,               # five names per leg, at least
    "insider_cluster_window_sessions": 21,   # one month
    "insider_cluster_min_insiders": 2,       # two distinct buyers = a cluster
    "insider_cluster_min_names": 5,
    "insider_npr_window_sessions": 63,       # one quarter
    "insider_npr_leg_fraction": 1 / 3.0,
    "insider_npr_min_names": 15,
}
K5 = R46_5_CANONICAL_CONSTANTS

R46_5_SPECS = (
    _spec(
        challenger_id="r46_5_pead_announcement_return_20d",
        family="POST_EARNINGS_ANNOUNCEMENT_DRIFT",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_EARNINGS_TERCILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission, restricted "
                 "to names whose earnings 8-K (Item 2.02) was accepted by "
                 "EDGAR before the emission instant and whose reaction "
                 "session falls in the trailing five sessions",
        thesis="the market under-reacts to earnings news; the announcement-"
               "window abnormal return sorts the cross-section into names "
               "that keep drifting in the direction of the surprise for "
               "weeks (Chan, Jegadeesh and Lakonishok's earnings-"
               "announcement-return formulation, which needs no consensus)",
        parameters={"window_sessions": K5["pead_window_sessions"],
                    "leg_fraction": K5["pead_leg_fraction"],
                    "min_names": K5["pead_min_names"],
                    "signal": "announcement-window return minus the SPY "
                              "return over the same window (close before "
                              "the reaction session to the reaction close)",
                    "event_source": "SEC 8-K Item 2.02 acceptance instant"},
        signal_owner="_pead_announcement_return",
        cohort=R46_5_COHORT,
        information_family="EARNINGS_EVENTS",
        dependence_cluster="EARNINGS_DRIFT",
    ),
    _spec(
        challenger_id="r46_5_insider_cluster_buy_20d",
        family="INSIDER_CLUSTER_BUYING",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LONG_INSIDER_CLUSTER",
        prediction_type="DIRECTIONAL_VS_BENCHMARK",
        horizons=(20,),
        control=C.CONTROL_BENCHMARK,
        benchmark="SPY",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission, restricted "
                 "to names with at least two distinct insiders making open-"
                 "market purchases (Form 4 code P) accepted by EDGAR within "
                 "the trailing 21 sessions",
        thesis="several insiders buying the same stock in the open market "
               "within a month is informed demand that the price has not "
               "absorbed; the basket outperforms the index over the following "
               "month",
        parameters={"window_sessions": K5["insider_cluster_window_sessions"],
                    "min_insiders": K5["insider_cluster_min_insiders"],
                    "min_names": K5["insider_cluster_min_names"],
                    "signal": "count of distinct open-market buyers; equal "
                              "weight long basket, gross 1.0",
                    "event_source": "SEC Form 4 ACCEPTANCE-DATETIME"},
        signal_owner="_insider_cluster_buy",
        cohort=R46_5_COHORT,
        information_family="INSIDER_FLOW",
        dependence_cluster="INSIDER_FLOW",
    ),
    _spec(
        challenger_id="r46_5_insider_net_purchase_xs_20d",
        family="INSIDER_NET_PURCHASE_RATIO",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_INSIDER_NPR_TERCILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission, restricted "
                 "to names with any open-market insider purchase or sale "
                 "(Form 4 codes P / S) accepted by EDGAR within the trailing "
                 "63 sessions",
        thesis="the net purchase ratio - insider buying minus selling over "
               "buying plus selling - ranks names by the direction of informed "
               "flow; net buyers outperform net sellers over the following "
               "month",
        parameters={"window_sessions": K5["insider_npr_window_sessions"],
                    "leg_fraction": K5["insider_npr_leg_fraction"],
                    "min_names": K5["insider_npr_min_names"],
                    "signal": "(purchase value - sale value) / (purchase "
                              "value + sale value), value = shares x price "
                              "where priced, shares otherwise",
                    "event_source": "SEC Form 4 ACCEPTANCE-DATETIME"},
        signal_owner="_insider_net_purchase_xs",
        cohort=R46_5_COHORT,
        information_family="INSIDER_FLOW",
        dependence_cluster="INSIDER_FLOW",
        economic_overlap_with=("r46_5_insider_cluster_buy_20d",),
        overlap_note="same filings, different aggregation (breadth of "
                     "buyers versus net value); one dependence cluster",
    ),
)

ALL_SPECS = SEED_SPECS + EXPANSION_SPECS + R46_4_SPECS + R46_5_SPECS

#: Dependence clusters and information families for the SEED cohort, declared
#: here rather than edited into the frozen seed dicts. The expansion cohort
#: carries both inline; :func:`cluster_for` and :func:`info_family_for` are
#: the one place either is resolved.
SEED_DEPENDENCE_CLUSTERS = {
    "r46_eq_xs_mom_12_1": "EQ_XS_PRICE",
    "r46_eq_xs_rev_5d": "EQ_XS_PRICE",
    "r46_eq_xs_lowvol_60d": "EQ_XS_PRICE",
    "r46_eq_xs_resid_mom_12_1": "EQ_XS_PRICE",
    "r46_fut_ts_mom_252": "FUTURES_TREND_PRICE",
    "r46_fx_xs_mom_252": "FX_TREND",
    "r46_vx_term_carry_5d": "VX_CARRY",
    "r46_rates_curve_rv_5d": "RATES_RV",
    "r46_comdty_xs_mom_252": "COMMODITY_XS_PRICE",
    "r46_spx_trend_200d": "SPX_TREND",
}

SEED_INFORMATION_FAMILIES = {cid: "PRICE_STATE"
                             for cid in SEED_DEPENDENCE_CLUSTERS}


def cluster_for(spec_or_entry: dict) -> str:
    d = spec_or_entry or {}
    return (d.get("dependence_cluster")
            or SEED_DEPENDENCE_CLUSTERS.get(str(d.get("challenger_id")))
            or "%s|%s" % (d.get("family"), d.get("asset_class")))


def info_family_for(spec_or_entry: dict) -> str:
    d = spec_or_entry or {}
    return (d.get("information_family")
            or SEED_INFORMATION_FAMILIES.get(str(d.get("challenger_id")))
            or "PRICE_STATE")


def spec_by_id(challenger_id: str):
    for s in ALL_SPECS:
        if s["challenger_id"] == challenger_id:
            return s
    return None


def spec_hash(spec: dict) -> str:
    """Hash over everything that changes the challenger's ECONOMICS.

    Deliberately excludes nothing that matters and includes nothing that does
    not: two specs with the same hash must produce the same decisions from the
    same data, and any change to universe, parameters, horizon, control, cost
    class or expression must change it.
    """
    core = {
        "challenger_id": spec["challenger_id"],
        "challenger_version": spec["challenger_version"],
        "family": spec["family"],
        "asset_class": spec["asset_class"],
        "instrument": spec["instrument"],
        "prediction_type": spec["prediction_type"],
        "horizons": sorted(spec["horizons"]),
        "control": spec["control"],
        "benchmark": spec["benchmark"],
        "cost_class": spec["cost_class"],
        "universe": spec["universe"],
        "parameters": spec["parameters"],
        "signal_owner": spec["signal_owner"],
        "hedge_definition": spec.get("hedge_definition"),
    }
    return sha(core)


def parameters_hash(spec: dict) -> str:
    return sha(spec["parameters"])


def feature_set_hash(spec: dict) -> str:
    return sha({"signal_owner": spec["signal_owner"],
                "universe": spec["universe"],
                "inputs": "owned Norgate daily bars, adjusted"})


# --------------------------------------------------------------------------- #
# Book construction helpers
# --------------------------------------------------------------------------- #
def _decile_book(scores: dict, fraction: float,
                 cost_class: str = "US_EQUITY") -> list:
    """Dollar-neutral decile book, gross notional 1.0.

    Long the top ``fraction``, short the bottom ``fraction``, equal weight
    inside each leg, each leg carrying half the gross.

    ``cost_class`` is written onto EVERY leg rather than left to a default.
    The judge reads the leg's own class when it charges the round trip, and a
    leg that does not carry one would be charged at whatever the fallback
    happens to be - correct for equities today and silently wrong the first
    time a non-equity cross-section is added.
    """
    clean = {k: float(v) for k, v in scores.items()
             if v is not None and np.isfinite(v)}
    n = len(clean)
    if n < MIN_CROSS_SECTION:
        return []
    k = max(1, int(round(n * float(fraction))))
    order = sorted(clean.items(), key=lambda kv: kv[1])
    shorts, longs = order[:k], order[-k:]
    legs = []
    for sym, sc in longs:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": cost_class})
    for sym, sc in shorts:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": cost_class})
    return legs


def _normalise_gross(legs: list, gross: float = 1.0) -> list:
    tot = sum(abs(float(l["weight"])) for l in legs)
    if tot <= 0:
        return []
    f = float(gross) / tot
    for l in legs:
        l["weight"] = float(l["weight"]) * f
    return legs


# --------------------------------------------------------------------------- #
# Signal owners
# --------------------------------------------------------------------------- #
def _eq_universe() -> tuple:
    """S&P 500 membership OBSERVED AT EMISSION.

    For a forward prediction this is exactly right and carries no survivorship
    bias: the index's constituents today are a fact known today. The
    survivorship-safe ``Current & Past`` construction exists in
    :func:`alpha_agent.r46.marketdata.sp500_pit` and is reserved for anything
    labelled HISTORICAL_SIMULATION.
    """
    return MD._watchlist("S&P 500")


def _eq_cross_section(spec: dict) -> dict:
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    cid = spec["challenger_id"]
    market = MD.closes(BENCHMARK_EQUITY) if "resid" in cid else None
    mkt_mom = None
    if market is not None:
        mkt_mom = MD.total_return(market, p.get("formation_days", 252),
                                  p.get("skip_days", 0))
    scores, marks, n_seen = {}, {}, 0
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < 5:
            continue
        n_seen += 1
        marks[sym] = float(s.iloc[-1])
        if cid == "r46_eq_xs_mom_12_1":
            v = MD.total_return(s, p["formation_days"], p["skip_days"])
        elif cid == "r46_eq_xs_rev_5d":
            r = MD.total_return(s, p["reversal_days"])
            v = None if r is None else -r
        elif cid == "r46_eq_xs_lowvol_60d":
            vol = MD.realised_vol(s, p["volatility_days"])
            v = None if vol is None else -vol
        elif cid == "r46_eq_xs_resid_mom_12_1":
            raw = MD.total_return(s, p["formation_days"], p["skip_days"])
            b = MD.beta_to(s, market, p["beta_days"])
            v = (None if (raw is None or b is None or mkt_mom is None)
                 else raw - b * mkt_mom)
        else:                                   # pragma: no cover
            v = None
        if v is not None:
            scores[sym] = v
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_priced": n_seen,
            "n_scored": len(scores), "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _futures_group(sym: str) -> str:
    for grp, members in FUTURES_GROUPS.items():
        if sym in members:
            return grp
    return "COMMODITY_FUTURES"


def _futures_trend(spec: dict) -> dict:
    p = spec["parameters"]
    declared = [s for grp, members in FUTURES_GROUPS.items()
                for s in members if grp != "VOLATILITY_FUTURES"]
    available = set(MD.continuous_futures())
    legs, marks, skipped = [], {}, []
    for sym in declared:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        if MD.has_non_positive(s, p["trend_days"] + 1):
            skipped.append({"instrument": sym, "why": MD.NON_POSITIVE_PRICE,
                            "detail": "a percentage return is undefined "
                                      "across this window"})
            continue
        tr = MD.total_return(s, p["trend_days"])
        vol = MD.realised_vol(s, p["volatility_days"])
        if tr is None or vol is None or vol <= 0:
            skipped.append({"instrument": sym, "why": "SHORT_HISTORY"})
            continue
        direction = 1.0 if tr > 0 else -1.0
        legs.append({"instrument": sym, "weight": direction / vol,
                     "score": tr, "side": "LONG" if direction > 0 else "SHORT",
                     "cost_class": _futures_group(sym)})
    if len(legs) < MIN_FUTURES_MARKETS:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_markets": len(legs), "skipped": skipped}
    legs = _normalise_gross(legs, 1.0)
    return {"state": "OK", "legs": legs, "n_markets": len(legs),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: l["cost_class"]
                                  for l in legs}}


def _fx_cross_section(spec: dict) -> dict:
    p = spec["parameters"]
    available = set(MD.fx_spot_symbols())
    scores, marks, skipped = {}, {}, []
    for sym in G10_USD_PER_FOREIGN:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        v = MD.total_return(s, p["formation_days"])
        if v is not None:
            scores[sym] = v
    if len(scores) < MIN_FX_PAIRS:
        return {"state": "INSUFFICIENT_PAIRS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = int(p["n_per_leg"])
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": "FX_SPOT"})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": "FX_SPOT"})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: "FX_SPOT" for l in legs}}


def _vx_carry(spec: dict) -> dict:
    front = MD.closes("&VX")
    spot = MD.closes("$VIX")
    if front is None or spot is None:
        return {"state": "NO_DATA", "legs": []}
    j = front.align(spot, join="inner")
    if not len(j[0]):
        return {"state": "NO_OVERLAP", "legs": []}
    f, sp = float(j[0].iloc[-1]), float(j[1].iloc[-1])
    if sp <= 0:
        return {"state": "BAD_SPOT", "legs": []}
    basis = f / sp - 1.0
    direction = -1.0 if basis > 0 else 1.0
    legs = [{"instrument": "&VX", "weight": direction, "score": basis,
             "side": "SHORT" if direction < 0 else "LONG",
             "cost_class": "VOLATILITY_FUTURES"}]
    return {"state": "OK", "legs": legs, "basis": basis,
            "front": f, "spot": sp, "marks": {"&VX": f, "$VIX": sp},
            "cost_class_by_leg": {"&VX": "VOLATILITY_FUTURES"}}


def _rates_rv(spec: dict) -> dict:
    p = spec["parameters"]
    zn, zt = MD.closes("&ZN"), MD.closes("&ZT")
    if zn is None or zt is None:
        return {"state": "NO_DATA", "legs": []}
    v_zn = MD.realised_vol(zn, K["volatility_days"])
    v_zt = MD.realised_vol(zt, K["volatility_days"])
    if not v_zn or not v_zt:
        return {"state": "SHORT_HISTORY", "legs": []}
    a = np.log(zn).diff()
    b = np.log(zt).diff()
    j = a.align(b, join="inner")
    spread_ret = (j[0] / v_zn - j[1] / v_zt).dropna()
    win = int(p["spread_z_days"])
    if len(spread_ret) < win + 1:
        return {"state": "SHORT_HISTORY", "legs": []}
    level = spread_ret.rolling(win).sum().dropna()
    z = MD.zscore_last(level, win)
    if z is None:
        return {"state": "NO_ZSCORE", "legs": []}
    if abs(z) < float(p["entry_z"]):
        return {"state": "FLAT_NO_SIGNAL", "legs": [], "z": z,
                "marks": {"&ZN": float(zn.iloc[-1]), "&ZT": float(zt.iloc[-1])}}
    direction = -1.0 if z > 0 else 1.0          # fade the stretched spread
    legs = [{"instrument": "&ZN", "weight": direction / v_zn, "score": z,
             "side": "LONG" if direction > 0 else "SHORT",
             "cost_class": "RATES_FUTURES"},
            {"instrument": "&ZT", "weight": -direction / v_zt, "score": z,
             "side": "SHORT" if direction > 0 else "LONG",
             "cost_class": "RATES_FUTURES"}]
    legs = _normalise_gross(legs, 1.0)
    return {"state": "OK", "legs": legs, "z": z,
            "marks": {"&ZN": float(zn.iloc[-1]), "&ZT": float(zt.iloc[-1])},
            "cost_class_by_leg": {"&ZN": "RATES_FUTURES",
                                  "&ZT": "RATES_FUTURES"}}


def _commodity_cross_section(spec: dict) -> dict:
    p = spec["parameters"]
    available = set(MD.continuous_futures())
    scores, marks, skipped = {}, {}, []
    for sym in COMMODITY_MARKETS:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        if MD.has_non_positive(s, p["formation_days"] + 1):
            skipped.append({"instrument": sym, "why": MD.NON_POSITIVE_PRICE,
                            "detail": "a percentage return is undefined "
                                      "across this window"})
            continue
        v = MD.total_return(s, p["formation_days"])
        if v is not None:
            scores[sym] = v
    if len(scores) < 9:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = max(1, int(round(len(scores) * float(p["leg_fraction"]))))
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": "COMMODITY_FUTURES"})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": "COMMODITY_FUTURES"})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: "COMMODITY_FUTURES"
                                  for l in legs}}


def _index_trend(spec: dict) -> dict:
    p = spec["parameters"]
    s = MD.closes(BENCHMARK_EQUITY)
    if s is None or len(s) < int(p["filter_days"]) + 1:
        return {"state": "SHORT_HISTORY", "legs": []}
    ma = float(s.iloc[-int(p["filter_days"]):].mean())
    px = float(s.iloc[-1])
    invested = px > ma
    legs = ([{"instrument": BENCHMARK_EQUITY, "weight": 1.0,
              "score": px / ma - 1.0, "side": "LONG",
              "cost_class": "US_ETF"}] if invested else [])
    return {"state": "OK", "legs": legs, "invested": invested,
            "price": px, "moving_average": ma,
            "marks": {BENCHMARK_EQUITY: px},
            "cost_class_by_leg": {BENCHMARK_EQUITY: "US_ETF"}}


# --------------------------------------------------------------------------- #
# Release 46.3 signal owners
# --------------------------------------------------------------------------- #
def _eq_xs_lottery(spec: dict) -> dict:
    """MAX(5): the mean of the five largest daily moves of the last month."""
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    win, top = int(p["max_window_days"]), int(p["max_top_days"])
    scores, marks = {}, {}
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < win + 2:
            continue
        marks[sym] = float(s.iloc[-1])
        r = s.pct_change().dropna()
        w = r.iloc[-win:]
        if len(w) < win:
            continue
        vals = np.sort(w.to_numpy(dtype=float))
        if not np.isfinite(vals[-top:]).all():
            continue
        scores[sym] = -float(vals[-top:].mean())   # long LOW-MAX names
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_scored": len(scores),
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _eq_xs_illiquidity(spec: dict) -> dict:
    """Amihud: |return| per dollar traded, averaged over one year."""
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    win = int(p["amihud_days"])
    scores, marks = {}, {}
    for sym in syms:
        s, v = MD.closes(sym), MD.volumes(sym)
        if s is None or v is None or len(s) < win + 2:
            continue
        marks[sym] = float(s.iloc[-1])
        j = s.align(v, join="inner")
        px, vol = j[0], j[1]
        dollar = (px * vol).where(lambda x: x > 0)
        illiq = (px.pct_change().abs() / dollar).dropna()
        w = illiq.iloc[-win:]
        if len(w) < win // 2:
            continue
        m = float(w.mean())
        if np.isfinite(m):
            scores[sym] = m                        # long HIGH illiquidity
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_scored": len(scores),
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _eq_xs_seasonal(spec: dict) -> dict:
    """Heston-Sadka: the same calendar month, one to five years back."""
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    lags, min_years = int(p["lag_years"]), int(p["min_years"])
    entry = CK.entry_session_date(CK.now_utc())
    scores, marks = {}, {}
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < 300:
            continue
        marks[sym] = float(s.iloc[-1])
        monthly = s.resample("ME").last().pct_change().dropna()
        vals = [float(r) for ts, r in monthly.items()
                if ts.month == entry.month
                and entry.year - lags <= ts.year <= entry.year - 1
                and np.isfinite(r)]
        if len(vals) >= min_years:
            scores[sym] = float(np.mean(vals))
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_scored": len(scores),
            "target_month": entry.month, "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _futures_xs_momentum(spec: dict) -> dict:
    """Relative twelve-month strength ACROSS futures markets, thirds."""
    p = spec["parameters"]
    declared = [s for grp, members in FUTURES_GROUPS.items()
                for s in members if grp != "VOLATILITY_FUTURES"]
    available = set(MD.continuous_futures())
    scores, marks, skipped = {}, {}, []
    for sym in declared:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        if MD.has_non_positive(s, int(p["formation_days"]) + 1):
            skipped.append({"instrument": sym, "why": MD.NON_POSITIVE_PRICE})
            continue
        v = MD.total_return(s, int(p["formation_days"]))
        if v is not None:
            scores[sym] = v
    if len(scores) < MIN_FUTURES_MARKETS:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = max(1, int(round(len(scores) * float(p["leg_fraction"]))))
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": _futures_group(sym)})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": _futures_group(sym)})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: l["cost_class"]
                                  for l in legs}}


def _commodity_curve_carry(spec: dict) -> dict:
    """Front/next curve slope per commodity, thirds. Signal from the dated
    curve; the tradeable expression stays the continuous series, whose bars
    keep printing through the outcome window."""
    p = spec["parameters"]
    ref = CK.eastern_date(CK.now_utc())
    scores, marks, curves, skipped = {}, {}, [], []
    for sym in COMMODITY_MARKETS:
        root = sym.lstrip("&")
        cv = MD.futures_curve_carry(root, ref)
        if cv.get("state") != "OK":
            skipped.append({"instrument": sym, "why": cv.get("state")})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_CONTINUOUS_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        scores[sym] = float(cv["carry_annualised"])
        curves.append({"instrument": sym, "carry": scores[sym],
                       "front": cv["front"]["symbol"],
                       "next": cv["next"]["symbol"],
                       "months_between": cv["months_between"]})
    if len(scores) < 9:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = max(1, int(round(len(scores) * float(p["leg_fraction"]))))
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:                    # long backwardation
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": "COMMODITY_FUTURES"})
    for sym, sc in order[:k]:                     # short contango
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": "COMMODITY_FUTURES"})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "curves": curves, "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: "COMMODITY_FUTURES"
                                  for l in legs}}


def _rates_macro_curve(spec: dict) -> dict:
    """Sign &ZN by the OWNED 10y-2y constant-maturity spread."""
    p = spec["parameters"]
    y10 = MD.closes(p["long_series"])
    y2 = MD.closes(p["short_series"])
    zn = MD.closes("&ZN")
    if y10 is None or y2 is None or zn is None:
        return {"state": "NO_DATA", "legs": []}
    j = y10.align(y2, join="inner")
    if not len(j[0]):
        return {"state": "NO_OVERLAP", "legs": []}
    last = j[0].index[-1].date()
    ref = CK.eastern_date(CK.now_utc())
    lag, d = 0, last
    while d < ref:
        d += _dt.timedelta(days=1)
        if d.weekday() < 5:
            lag += 1
    if lag > int(p["max_series_lag_sessions"]):
        return {"state": "STALE_SERIES", "legs": [],
                "series_last_session": str(last), "lag_sessions": lag}
    slope = float(j[0].iloc[-1]) - float(j[1].iloc[-1])
    if slope == 0.0:
        return {"state": "OK", "legs": [], "slope": slope,
                "marks": {"&ZN": float(zn.iloc[-1])}}
    direction = 1.0 if slope > 0 else -1.0
    legs = [{"instrument": "&ZN", "weight": direction, "score": slope,
             "side": "LONG" if direction > 0 else "SHORT",
             "cost_class": "RATES_FUTURES"}]
    return {"state": "OK", "legs": legs, "slope": slope,
            "series_last_session": str(last), "lag_sessions": lag,
            "marks": {"&ZN": float(zn.iloc[-1])},
            "cost_class_by_leg": {"&ZN": "RATES_FUTURES"}}


def _tom_window_membership(entry: _dt.date) -> bool:
    """Is ``entry`` the last weekday of its month, or one of the first three?

    Stated on the weekday calendar, like the entry rule itself: no venue in
    this tournament prints a bar on a weekend, and holidays resolve the entry
    forward on the instrument's own realised calendar as they do everywhere
    else in this release.
    """
    first3, d = [], entry.replace(day=1)
    while len(first3) < 3:
        if d.weekday() < 5:
            first3.append(d)
        d += _dt.timedelta(days=1)
    nxt = (entry.replace(day=28) + _dt.timedelta(days=4)).replace(day=1)
    last = nxt - _dt.timedelta(days=1)
    while last.weekday() in CK.WEEKEND:
        last -= _dt.timedelta(days=1)
    return entry in first3 or entry == last


def _spx_turn_of_month(spec: dict) -> dict:
    """Long SPY only when the ENTRY session falls in the turn-of-month window.

    Outside the window the rule holds nothing, which is a valid decision and
    correctly emits no row - there is nothing to score against cash.
    """
    s = MD.closes(BENCHMARK_EQUITY)
    if s is None or not len(s):
        return {"state": "NO_DATA", "legs": []}
    entry = CK.entry_session_date(CK.now_utc())
    inside = _tom_window_membership(entry)
    px = float(s.iloc[-1])
    legs = ([{"instrument": BENCHMARK_EQUITY, "weight": 1.0, "score": 1.0,
              "side": "LONG", "cost_class": "US_ETF"}] if inside else [])
    return {"state": "OK", "legs": legs, "entry_session": str(entry),
            "in_turn_of_month_window": inside,
            "marks": {BENCHMARK_EQUITY: px},
            "cost_class_by_leg": {BENCHMARK_EQUITY: "US_ETF"}}


def _eq_xs_ensemble(spec: dict) -> dict:
    """Equal-weight rank combination of three seed signals. Weights frozen at
    one third each before ANY member holds a matured forward observation."""
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    market = MD.closes(BENCHMARK_EQUITY)
    mkt_mom = (MD.total_return(market, K["momentum_formation_days"],
                               K["momentum_skip_days"])
               if market is not None else None)
    mom, lowvol, resid, marks = {}, {}, {}, {}
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < 5:
            continue
        marks[sym] = float(s.iloc[-1])
        m = MD.total_return(s, K["momentum_formation_days"],
                            K["momentum_skip_days"])
        if m is not None:
            mom[sym] = m
        v = MD.realised_vol(s, K["volatility_days"])
        if v is not None:
            lowvol[sym] = -v
        b = MD.beta_to(s, market, K["beta_days"])
        if m is not None and b is not None and mkt_mom is not None:
            resid[sym] = m - b * mkt_mom

    def _ranks(d: dict) -> dict:
        order = sorted(d.items(), key=lambda kv: kv[1])
        n = max(1, len(order) - 1)
        return {sym: i / n for i, (sym, _) in enumerate(order)}

    r1, r2, r3 = _ranks(mom), _ranks(lowvol), _ranks(resid)
    scores = {sym: (r1[sym] + r2[sym] + r3[sym]) / 3.0
              for sym in set(r1) & set(r2) & set(r3)}
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_scored": len(scores),
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _rolling_top_mean(frame: pd.DataFrame, window: int,
                      top: int) -> pd.DataFrame:
    """Rolling mean of the ``top`` largest values in each window, per column.

    Vectorised: a python-level rolling apply over five hundred columns is
    minutes of wall clock inside a daily cycle, and the daily cycle is where
    this runs.
    """
    out = pd.DataFrame(np.nan, index=frame.index, columns=frame.columns)
    if len(frame) < window:
        return out
    from numpy.lib.stride_tricks import sliding_window_view
    w = sliding_window_view(frame.to_numpy(dtype=float), window, axis=0)
    part = np.partition(w, -top, axis=2)[:, :, -top:]
    out.iloc[window - 1:] = part.mean(axis=2)
    return out


def _ml_eq_cross_section(spec: dict) -> dict:
    """One deterministic ML pipeline, two frozen model classes.

    The training protocol is a contract clause (see ML_RETRAINING_POLICY):
    trailing window, monthly-sampled decision dates so overlapping targets do
    not inflate the fit, cross-sectional z-scores clipped at three sigma,
    frozen hyperparameters, fixed seed. Every training input existed at the
    data cutoff and every training TARGET window closed at or before it, so
    nothing the model saw includes any part of any outcome it will be scored
    on.
    """
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    market = MD.closes(BENCHMARK_EQUITY)
    if market is None:
        return {"state": "NO_DATA", "legs": []}

    px_map, vol_map = {}, {}
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < 300:
            continue
        px_map[sym] = s
        v = MD.volumes(sym)
        if v is not None:
            vol_map[sym] = v
    if len(px_map) < MIN_CROSS_SECTION:
        return {"state": "INSUFFICIENT_CROSS_SECTION", "legs": [],
                "n_priced": len(px_map)}

    px = pd.DataFrame(px_map).sort_index()
    px = px.where(px > 0)
    ret = px.pct_change()
    logret = np.log(px).diff()
    mret = np.log(market).diff().reindex(px.index)

    feats = {
        "mom_12_1": px.shift(21) / px.shift(252) - 1.0,
        "rev_5d": -(px / px.shift(5) - 1.0),
        "vol_60d": logret.rolling(60).std() * np.sqrt(252.0),
        "beta_60d": logret.rolling(60).cov(mret).div(
            mret.rolling(60).var(), axis=0),
        "max_21d": _rolling_top_mean(ret, XK["max_window_days"],
                                     XK["max_top_days"]),
    }
    if vol_map:
        dollar = px * pd.DataFrame(vol_map).reindex(
            index=px.index, columns=px.columns)
        feats["amihud_252d"] = (ret.abs() / dollar.where(dollar > 0)) \
            .rolling(XK["amihud_days"]).mean()
    else:
        feats["amihud_252d"] = pd.DataFrame(np.nan, index=px.index,
                                            columns=px.columns)
    features = list(p["features"])

    def _zrow(pos: int):
        row = pd.DataFrame({f: feats[f].iloc[pos] for f in features})
        row = row.dropna()
        if len(row) < MIN_CROSS_SECTION:
            return None
        for f in features:
            col = row[f]
            sd = float(col.std(ddof=1))
            if not np.isfinite(sd) or sd <= 0:
                return None
            row[f] = ((col - float(col.mean())) / sd).clip(-3.0, 3.0)
        return row

    idx = px.index
    t_target = int(p["target_sessions"])
    stride = int(p["training_stride_sessions"])
    last_train = len(idx) - 1 - t_target
    first_train = max(300, last_train - int(p["training_sessions"]))
    if last_train <= first_train:
        return {"state": "INSUFFICIENT_TRAINING_SAMPLE", "legs": []}
    positions = list(range(last_train, first_train, -stride))[::-1]
    fwd = px.shift(-t_target) / px - 1.0

    x_rows, y_rows, used = [], [], []
    for pos in positions:
        row = _zrow(pos)
        if row is None:
            continue
        target = fwd.iloc[pos].reindex(row.index).dropna()
        row = row.loc[target.index]
        if len(row) < MIN_CROSS_SECTION:
            continue
        x_rows.append(row[features].to_numpy(dtype=float))
        y_rows.append(target.to_numpy(dtype=float))
        used.append(str(idx[pos].date()))
    if len(x_rows) < 10:
        return {"state": "INSUFFICIENT_TRAINING_SAMPLE", "legs": [],
                "n_training_dates": len(x_rows)}
    x = np.vstack(x_rows)
    y = np.concatenate(y_rows)

    def _fit(model_class: str):
        """One frozen model class -> a predict callable, or None."""
        if model_class == "RIDGE_CLOSED_FORM":
            lam = float(p["ridge_lambda"])
            beta = np.linalg.solve(x.T @ x + lam * np.eye(x.shape[1]),
                                   x.T @ y)
            return lambda m: m @ beta
        if model_class == "HIST_GRADIENT_BOOSTING":
            try:
                from sklearn.ensemble import HistGradientBoostingRegressor
            except Exception:
                return None
            model = HistGradientBoostingRegressor(
                max_iter=int(p["max_iter"]), max_depth=int(p["max_depth"]),
                learning_rate=float(p["learning_rate"]),
                random_state=int(p["random_seed"]))
            model.fit(x, y)
            return model.predict
        if model_class == "EXTRA_TREES":
            # Release 46.4 - a randomised forest with frozen depth and leaf
            # size; a different inductive bias on the same frozen protocol.
            try:
                from sklearn.ensemble import ExtraTreesRegressor
            except Exception:
                return None
            model = ExtraTreesRegressor(
                n_estimators=int(p["n_estimators"]),
                max_depth=int(p["max_depth"]),
                min_samples_leaf=int(p["min_samples_leaf"]),
                random_state=int(p["random_seed"]), n_jobs=1)
            model.fit(x, y)
            return model.predict
        return None

    model_class = str(p["model_class"])
    gate = None
    if model_class == "REGIME_GATED_RIDGE_GBT":
        # Release 46.4 - a FROZEN gate between two existing frozen members.
        # The gate reads the declared series at the data cutoff and nothing
        # else; no forward result may move the level.
        g = MD.closes(str(p["gate_series"]))
        if g is None or not len(g):
            return {"state": "NO_DATA", "legs": [],
                    "why": "gate series unavailable"}
        level = float(g.iloc[-1])
        chosen = ("RIDGE_CLOSED_FORM" if level <= float(p["gate_level"])
                  else "HIST_GRADIENT_BOOSTING")
        gate = {"series": p["gate_series"], "level_at_cutoff": level,
                "gate_level": float(p["gate_level"]), "chosen": chosen,
                "gate_session": str(g.index[-1].date())}
        predict = _fit(chosen)
    elif model_class in ("RIDGE_CLOSED_FORM", "HIST_GRADIENT_BOOSTING",
                         "EXTRA_TREES"):
        predict = _fit(model_class)
    else:
        return {"state": "UNKNOWN_MODEL_CLASS", "legs": []}
    if predict is None:
        return {"state": "ML_DEPENDENCY_UNAVAILABLE", "legs": []}

    today = _zrow(len(idx) - 1)
    if today is None:
        return {"state": "INSUFFICIENT_CROSS_SECTION", "legs": []}
    preds = predict(today[features].to_numpy(dtype=float))
    scores = {sym: float(v) for sym, v in zip(today.index, preds)
              if np.isfinite(v)}
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    marks = {sym: float(v) for sym, v in px.iloc[-1].dropna().items()}
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_scored": len(scores),
            "model_class": model_class,
            "regime_gate": gate,
            "n_training_rows": int(len(y)),
            "n_training_dates": len(used),
            "training_data_cutoff": used[-1] if used else None,
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


# --------------------------------------------------------------------------- #
# Release 46.4 signal owners - positioning, credit, macro prints, calendars
# --------------------------------------------------------------------------- #
def _cot_book(spec: dict, score_key: str, sign: float) -> dict:
    """Thirds book across CFTC-mapped futures on one positioning feature."""
    from . import cftc as CF
    p = spec["parameters"]
    ref = CK.eastern_date(CK.now_utc())
    pos = CF.positioning(CF.load_history(), ref)
    available = set(MD.continuous_futures())
    scores, marks, classes, skipped = {}, {}, {}, []
    for sym, m in (pos.get("markets") or {}).items():
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        v = m.get(score_key)
        if v is None:
            skipped.append({"instrument": sym, "why": "NO_FEATURE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        scores[sym] = sign * float(v)
        classes[sym] = m.get("cost_class") or _futures_group(sym)
    if len(scores) < CF.MIN_MARKETS:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_scored": len(scores), "skipped": skipped,
                "positioning_as_of": pos.get("as_of")}
    k = max(1, int(round(len(scores) * float(p["leg_fraction"]))))
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": classes[sym]})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": classes[sym]})
    reports = sorted({m.get("report_as_of")
                      for m in (pos.get("markets") or {}).values()})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "skipped": skipped, "marks": marks,
            "report_as_of": reports[-1] if reports else None,
            "publication_lag_days": pos.get("publication_lag_days"),
            "cost_class_by_leg": {l["instrument"]: l["cost_class"]
                                  for l in legs}}


def _cot_xs_reversal(spec: dict) -> dict:
    """Fade the 156-week z-score of speculative net positioning."""
    return _cot_book(spec, "spec_net_share_z", -1.0)


def _cot_xs_flow(spec: dict) -> dict:
    """Follow the 13-week change in speculative net positioning."""
    return _cot_book(spec, "spec_net_share_change_13w", 1.0)


def _credit_regime_spx(spec: dict) -> dict:
    """Long SPY only when the HY spread sits below its 63-observation mean."""
    from . import credit as CR
    ref = CK.eastern_date(CK.now_utc())
    st = CR.state(ref)
    s = MD.closes(BENCHMARK_EQUITY)
    if st.get("state") != "OK" or s is None or not len(s):
        return {"state": "NO_DATA", "legs": [], "credit_state": st}
    invested = bool(st.get("hy_below_mean"))
    px = float(s.iloc[-1])
    legs = ([{"instrument": BENCHMARK_EQUITY, "weight": 1.0,
              "score": float(st["hy_oas_mean_63"] - st["hy_oas"]),
              "side": "LONG", "cost_class": "US_ETF"}] if invested else [])
    return {"state": "OK", "legs": legs, "invested": invested,
            "credit_state": {k: st.get(k) for k in (
                "source", "series_last_observation", "published_by",
                "hy_oas", "hy_oas_mean_63")},
            "marks": {BENCHMARK_EQUITY: px},
            "cost_class_by_leg": {BENCHMARK_EQUITY: "US_ETF"}}


def _credit_hy_ig_momentum(spec: dict) -> dict:
    """Long HYG / short LQD on a tightening 21-observation spread change."""
    from . import credit as CR
    ref = CK.eastern_date(CK.now_utc())
    st = CR.state(ref)
    hyg, lqd = MD.closes("HYG"), MD.closes("LQD")
    if st.get("state") != "OK" or hyg is None or lqd is None:
        return {"state": "NO_DATA", "legs": [], "credit_state": st}
    direction = 1.0 if st.get("hy_tightening") else -1.0
    sc = -float(st.get("hy_oas_change_21") or 0.0)
    legs = [{"instrument": "HYG", "weight": 0.5 * direction, "score": sc,
             "side": "LONG" if direction > 0 else "SHORT",
             "cost_class": "US_ETF"},
            {"instrument": "LQD", "weight": -0.5 * direction, "score": -sc,
             "side": "SHORT" if direction > 0 else "LONG",
             "cost_class": "US_ETF"}]
    return {"state": "OK", "legs": legs,
            "credit_state": {k: st.get(k) for k in (
                "source", "series_last_observation", "published_by",
                "hy_oas", "hy_oas_change_21")},
            "marks": {"HYG": float(hyg.iloc[-1]), "LQD": float(lqd.iloc[-1])},
            "cost_class_by_leg": {"HYG": "US_ETF", "LQD": "US_ETF"}}


def _macro_surprise_rates(spec: dict) -> dict:
    """Sign &ZN against today's first-published CPI / payrolls surprise."""
    from . import macro as MC
    ref = CK.eastern_date(CK.now_utc())
    sig = MC.rates_signal(ref)
    zn = MD.closes("&ZN")
    if zn is None or not len(zn):
        return {"state": "NO_DATA", "legs": [], "signal": sig}
    px = float(zn.iloc[-1])
    if sig.get("state") != "OK" or sig.get("direction") == "FLAT":
        return {"state": "OK", "legs": [], "signal": sig,
                "marks": {"&ZN": px}, "cost_class_by_leg": {}}
    direction = 1.0 if sig["direction"] == "LONG" else -1.0
    legs = [{"instrument": "&ZN", "weight": direction,
             "score": float(sig["zn_score"]),
             "side": "LONG" if direction > 0 else "SHORT",
             "cost_class": "RATES_FUTURES"}]
    return {"state": "OK", "legs": legs, "signal": sig, "marks": {"&ZN": px},
            "cost_class_by_leg": {"&ZN": "RATES_FUTURES"}}


def _calendar_spy(spec: dict, inside: bool, label: str, holding) -> dict:
    s = MD.closes(BENCHMARK_EQUITY)
    if s is None or not len(s):
        return {"state": "NO_DATA", "legs": []}
    px = float(s.iloc[-1])
    legs = ([{"instrument": BENCHMARK_EQUITY, "weight": 1.0, "score": 1.0,
              "side": "LONG", "cost_class": "US_ETF"}] if inside else [])
    return {"state": "OK", "legs": legs, "holding_session": str(holding),
            label: inside, "marks": {BENCHMARK_EQUITY: px},
            "cost_class_by_leg": {BENCHMARK_EQUITY: "US_ETF"}}


def _spx_pre_fomc(spec: dict) -> dict:
    """Long SPY only when the HOLDING session is a scheduled FOMC decision."""
    from . import events as EVN
    hold = EVN.holding_session_for(CK.now_utc())
    return _calendar_spy(spec, EVN.is_fomc_decision_day(hold),
                         "holding_session_is_fomc_decision_day", hold)


def _spx_announcement_day(spec: dict) -> dict:
    """Long SPY only when the HOLDING session carries a scheduled release."""
    from . import events as EVN
    hold = EVN.holding_session_for(CK.now_utc())
    return _calendar_spy(spec, EVN.is_announcement_day(hold),
                         "holding_session_is_announcement_day", hold)


def _fraction_book(scores: dict, fraction: float, min_names: int,
                   cost_class: str = "US_EQUITY") -> list:
    """Dollar-neutral long-top / short-bottom ``fraction`` book, gross 1.0,
    over a SMALL event cross-section. Refuses below ``min_names``."""
    clean = {k: float(v) for k, v in scores.items()
             if v is not None and np.isfinite(v)}
    n = len(clean)
    if n < int(min_names):
        return []
    k = max(1, int(round(n * float(fraction))))
    order = sorted(clean.items(), key=lambda kv: kv[1])
    shorts, longs = order[:k], order[-k:]
    legs = []
    for sym, sc in longs:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": cost_class})
    for sym, sc in shorts:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": cost_class})
    return legs


def _trailing_sessions(cutoff: _dt.date, n: int) -> list:
    out, d = [], cutoff
    while len(out) < int(n):
        if d.weekday() not in CK.WEEKEND:
            out.append(str(d))
        d -= _dt.timedelta(days=1)
    return out


def _pead_announcement_return(spec: dict) -> dict:
    """Sort last week's announcers by announcement-window abnormal return."""
    from . import earnings as EA
    p = spec["parameters"]
    now = CK.now_utc()
    cutoff = MD.last_session(BENCHMARK_EQUITY)
    spy = MD.closes(BENCHMARK_EQUITY)
    if cutoff is None or spy is None or not len(spy):
        return {"state": "NO_DATA", "legs": []}
    raw_universe = _eq_universe()
    cov = EA.universe_coverage(raw_universe, now)
    if not cov["complete"]:
        return {"state": "LANE_COVERAGE_INCOMPLETE", "legs": [],
                "universe_coverage": cov}
    # Normalise on BOTH sides. The coverage gate compares normalised names; a
    # membership test against the raw watchlist here would silently drop any
    # share class the two sources spell differently.
    universe = {EA.norm_ticker(t) for t in raw_universe}
    by_norm = {EA.norm_ticker(t): t for t in raw_universe}
    ann = EA.recent_announcements(cutoff, p["window_sessions"], now)
    scores, marks, detail = {}, {}, []
    for e in ann:
        sym = EA.norm_ticker(e.get("ticker"))
        if sym not in universe or sym in scores:
            continue
        sym = by_norm.get(sym, sym)       # trade the name the bars are keyed by
        s = MD.closes(sym)
        if s is None or len(s) < 3:
            continue
        rs = str(e.get("reaction_session"))
        idx = [str(ts.date()) for ts in s.index]
        if rs not in idx:
            continue
        i = idx.index(rs)
        if i == 0 or str(s.index[i].date()) > str(cutoff):
            continue
        sidx = [str(ts.date()) for ts in spy.index]
        if rs not in sidx or sidx.index(rs) == 0:
            continue
        j = sidx.index(rs)
        r_name = float(s.iloc[i]) / float(s.iloc[i - 1]) - 1.0
        r_spy = float(spy.iloc[j]) / float(spy.iloc[j - 1]) - 1.0
        scores[sym] = r_name - r_spy
        marks[sym] = float(s.iloc[-1])
        detail.append({"ticker": sym, "reaction_session": rs,
                       "timing": e.get("timing"),
                       "accepted_at_utc": e.get("accepted_at_utc"),
                       "abnormal_return": scores[sym]})
    legs = _fraction_book(scores, p["leg_fraction"], p["min_names"],
                          spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_announcers_in_window": len(ann),
            "n_in_universe_scored": len(scores), "events": detail[:60],
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _insider_window(spec: dict, window_key: str = "window_sessions"):
    """The declared window's transactions - or the reason there are none.

    Returns ``(cutoff, coverage, transactions)``. A window that is not
    COMPLETELY covered by complete daily captures yields no transactions and
    carries its coverage report: a breadth-of-buyers count over a window half
    of which was never captured systematically undercounts, and would read as
    a weak signal rather than as missing data.
    """
    from . import form4 as FM
    now = CK.now_utc()
    cutoff = MD.last_session(BENCHMARK_EQUITY)
    if cutoff is None:
        return None, None, []
    window = _trailing_sessions(cutoff, spec["parameters"][window_key])
    cov = FM.window_coverage(window, now)
    if not cov["complete"]:
        return cutoff, cov, []
    sessions = set(window)
    txs = [t for t in FM.transactions(now, informative_only=True)
           if str(t.get("transaction_date") or "") in sessions
           and t.get("shares")]
    return cutoff, cov, txs


def _insider_cluster_buy(spec: dict) -> dict:
    """Equal-weight long basket of names with clustered open-market buying."""
    p = spec["parameters"]
    cutoff, cov, txs = _insider_window(spec)
    if cutoff is None:
        return {"state": "NO_DATA", "legs": []}
    if not (cov or {}).get("complete"):
        return {"state": "LANE_COVERAGE_INCOMPLETE", "legs": [],
                "window_coverage": cov}
    from . import earnings as EA
    raw_universe = _eq_universe()
    by_norm = {EA.norm_ticker(t): t for t in raw_universe}
    buyers: dict = {}
    for t in txs:
        if t.get("transaction_code") != "P":
            continue
        sym = by_norm.get(t.get("issuer_ticker"))
        if sym is None:
            continue
        buyers.setdefault(sym, set()).add(t.get("insider_cik")
                                          or t.get("insider_name"))
    names = sorted(s for s, b in buyers.items()
                   if len(b) >= int(p["min_insiders"]))
    marks = {}
    for s in list(names):
        px = MD.closes(s)
        if px is None or not len(px):
            names.remove(s)
            continue
        marks[s] = float(px.iloc[-1])
    if len(names) < int(p["min_names"]):
        return {"state": "OK", "legs": [], "n_cluster_names": len(names),
                "n_informative_transactions": len(txs),
                "window_coverage": cov,
                "why_flat": "fewer than %d names carry a buying cluster"
                            % p["min_names"],
                "marks": marks, "cost_class_by_leg": {}}
    legs = [{"instrument": s, "weight": 1.0 / len(names),
             "score": float(len(buyers[s])), "side": "LONG",
             "cost_class": spec["cost_class"]} for s in names]
    return {"state": "OK", "legs": legs, "n_cluster_names": len(names),
            "n_informative_transactions": len(txs), "window_coverage": cov,
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _insider_net_purchase_xs(spec: dict) -> dict:
    """Long top-third / short bottom-third by insider net purchase ratio."""
    p = spec["parameters"]
    cutoff, cov, txs = _insider_window(spec)
    if cutoff is None:
        return {"state": "NO_DATA", "legs": []}
    if not (cov or {}).get("complete"):
        return {"state": "LANE_COVERAGE_INCOMPLETE", "legs": [],
                "window_coverage": cov}
    from . import earnings as EA
    raw_universe = _eq_universe()
    by_norm = {EA.norm_ticker(t): t for t in raw_universe}
    buy: dict = {}
    sell: dict = {}
    for t in txs:
        sym = by_norm.get(t.get("issuer_ticker"))
        if sym is None:
            continue
        val = float(t["shares"]) * float(t.get("price_per_share") or 1.0)
        if t.get("transaction_code") == "P":
            buy[sym] = buy.get(sym, 0.0) + val
        elif t.get("transaction_code") == "S":
            sell[sym] = sell.get(sym, 0.0) + val
    scores, marks = {}, {}
    for sym in set(buy) | set(sell):
        tot = buy.get(sym, 0.0) + sell.get(sym, 0.0)
        if tot <= 0:
            continue
        px = MD.closes(sym)
        if px is None or not len(px):
            continue
        scores[sym] = (buy.get(sym, 0.0) - sell.get(sym, 0.0)) / tot
        marks[sym] = float(px.iloc[-1])
    legs = _fraction_book(scores, p["leg_fraction"], p["min_names"],
                          spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_names_with_flow": len(scores),
            "n_informative_transactions": len(txs), "window_coverage": cov,
            "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


_OWNERS = {
    "_pead_announcement_return": _pead_announcement_return,
    "_insider_cluster_buy": _insider_cluster_buy,
    "_insider_net_purchase_xs": _insider_net_purchase_xs,
    "_cot_xs_reversal": _cot_xs_reversal,
    "_cot_xs_flow": _cot_xs_flow,
    "_credit_regime_spx": _credit_regime_spx,
    "_credit_hy_ig_momentum": _credit_hy_ig_momentum,
    "_macro_surprise_rates": _macro_surprise_rates,
    "_spx_pre_fomc": _spx_pre_fomc,
    "_spx_announcement_day": _spx_announcement_day,
    "_eq_cross_section": _eq_cross_section,
    "_futures_trend": _futures_trend,
    "_fx_cross_section": _fx_cross_section,
    "_vx_carry": _vx_carry,
    "_rates_rv": _rates_rv,
    "_commodity_cross_section": _commodity_cross_section,
    "_index_trend": _index_trend,
    "_eq_xs_lottery": _eq_xs_lottery,
    "_eq_xs_illiquidity": _eq_xs_illiquidity,
    "_eq_xs_seasonal": _eq_xs_seasonal,
    "_futures_xs_momentum": _futures_xs_momentum,
    "_commodity_curve_carry": _commodity_curve_carry,
    "_rates_macro_curve": _rates_macro_curve,
    "_spx_turn_of_month": _spx_turn_of_month,
    "_eq_xs_ensemble": _eq_xs_ensemble,
    "_ml_eq_cross_section": _ml_eq_cross_section,
}


def build(spec: dict) -> dict:
    """Run one challenger's frozen rule over the latest owned data.

    Returns the book it would hold, its per-leg diagnostics and the hashes
    that pin the inputs. Never writes anything.
    """
    owner = _OWNERS.get(spec["signal_owner"])
    if owner is None:                            # pragma: no cover
        return {"state": "NO_SIGNAL_OWNER", "legs": []}
    out = owner(spec)
    legs = out.get("legs") or []
    out["gross_notional"] = float(sum(abs(float(l["weight"])) for l in legs))
    out["net_notional"] = float(sum(float(l["weight"]) for l in legs))
    out["n_legs"] = len(legs)
    out["market_state_snapshot_hash"] = sha(out.get("marks") or {})
    out["input_evidence_hash"] = sha(
        [(l["instrument"], round(float(l["score"]), 10),
          round(float(l["weight"]), 10)) for l in legs])
    out.pop("marks", None)
    return out


def expected_cost_bps(book: dict, spec: dict) -> float:
    """Cost of OPENING the book, in bps of gross notional.

    Charged on traded notional - Release 31's correction - and only for the
    entry side here; the exit side is charged again by the judge when the
    position is closed at maturity.
    """
    legs = book.get("legs") or []
    if not legs:
        return 0.0
    by_leg = book.get("cost_class_by_leg") or {}
    total = 0.0
    for l in legs:
        klass = (l.get("cost_class") or by_leg.get(l["instrument"])
                 or spec.get("cost_class") or "US_EQUITY")
        half = C.COST_BPS_PER_SIDE.get(klass, 5.0) + C.SLIPPAGE_BPS_PER_SIDE
        total += abs(float(l["weight"])) * half
    return float(total)
