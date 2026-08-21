"""alpha_agent.r33.regime - latent market state, and whether it is worth anything.

The hypothesis is not "regimes exist". Regimes always look like they exist: fit
two states to any financial series and one will be calm and one turbulent, and
the picture will be compelling. The hypothesis being TESTED here is narrower and
falsifiable:

    does ``P(S_t | I_t)`` improve a forecast that already has the same
    information WITHOUT the state?

So every regime configuration is run against the identical model on the
identical features minus the state probabilities. If adding the state does not
improve the primary metric, the regime is decoration, however intuitive its
picture.

States are DATA-DERIVED, not hard-coded narratives. Three state sources are
offered so that the answer is not an artifact of one information set:

    MARKET_ONLY        owned market observables - implied volatility, the yield
                       curve, credit spreads, breadth, the dollar, commodities
    PIT_MACRO_ONLY     ALFRED vintage macro state, as it was actually known
    MARKET_PLUS_PIT    both

The HMM is fitted on DISCOVERY observations only and its parameters are then
frozen; later blocks are FILTERED forward through it. State beliefs are always
the filtered ``P(S_t | data up to t)`` - never smoothed. See
:func:`alpha_agent.r33.models.hmm_filter_states` for why that distinction is the
one that decides whether a regime study measured anything.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import models as _models

CALCULATION_OWNER = "alpha_agent.r33.regime"

SOURCE_MARKET = "MARKET_ONLY"
SOURCE_PIT = "PIT_MACRO_ONLY"
SOURCE_BOTH = "MARKET_PLUS_PIT"
STATE_SOURCES = (SOURCE_MARKET, SOURCE_PIT, SOURCE_BOTH)

#: Market state columns, taken from the global state frame.
MARKET_STATE_COLUMNS = ("g_vix_level", "g_vix_change_21", "g_yield_slope",
                        "g_credit_spread", "g_usd_trend_63",
                        "g_commodity_trend_63", "g_breadth")

#: PIT macro state columns, taken from the Lane B state frame.
PIT_STATE_COLUMNS = ("pit_inflation_level_yoy", "pit_payrolls_yoy",
                     "pit_industrial_production_yoy", "pit_unemployment",
                     "pit_real_gdp_yoy", "pit_consumer_sentiment_yoy")


def state_frame(global_state: pd.DataFrame, pit_state: pd.DataFrame, *,
                source: str) -> pd.DataFrame:
    """Assemble the observation series the HMM is fitted on."""
    if source == SOURCE_MARKET:
        cols = [c for c in MARKET_STATE_COLUMNS if c in global_state.columns]
        frame = global_state[cols]
    elif source == SOURCE_PIT:
        cols = [c for c in PIT_STATE_COLUMNS if c in pit_state.columns]
        frame = pit_state[cols]
    else:
        a = [c for c in MARKET_STATE_COLUMNS if c in global_state.columns]
        b = [c for c in PIT_STATE_COLUMNS if c in pit_state.columns]
        frame = pd.concat([global_state[a], pit_state[b]], axis=1)
    return frame.ffill()


def standardise_on_training(frame: pd.DataFrame,
                            training_mask: np.ndarray) -> pd.DataFrame:
    """Centre and scale using TRAINING rows only.

    Standardising a regime observation series on the full sample tells the model
    where the whole period's calm and turbulent levels sit, including the part
    it is about to be evaluated on.
    """
    train = frame.to_numpy()[training_mask]
    mu = np.nanmean(train, axis=0)
    sd = np.nanstd(train, axis=0, ddof=1)
    sd = np.where(np.isfinite(sd) & (sd > 1e-12), sd, 1.0)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    Z = (frame.to_numpy() - mu[None, :]) / sd[None, :]
    Z = np.clip(np.where(np.isfinite(Z), Z, 0.0), -8.0, 8.0)
    return pd.DataFrame(Z, index=frame.index, columns=frame.columns)


def fit_and_filter(frame: pd.DataFrame, training_mask: np.ndarray, *,
                   n_states: int, seed: int = 33) -> dict:
    """Fit on training rows, then FILTER the whole sample forward."""
    Z = standardise_on_training(frame, training_mask)
    values = Z.to_numpy()
    spec = _models.fit_hmm(values[training_mask], n_states=n_states, seed=seed)
    filtered = _models.hmm_filter_states(spec, values)
    probs = pd.DataFrame(
        filtered, index=frame.index,
        columns=[f"state_p{k}" for k in range(int(n_states))])
    return {"spec": spec, "probabilities": probs,
            "columns": list(frame.columns),
            "fitted_on_training_only": True,
            "states_are_filtered_only": True}


def describe_states(spec: dict, frame_columns) -> list:
    """A readable description of what each fitted state looks like.

    Reported so a reader can see whether a "regime" is an economic condition or
    a numerical accident, but this description never qualifies anything.
    """
    mu = np.asarray(spec["mu"], dtype=float)
    trans = np.asarray(spec["trans"], dtype=float)
    out = []
    for k in range(mu.shape[0]):
        loadings = {c: round(float(mu[k, j]), 3)
                    for j, c in enumerate(frame_columns)}
        persistence = float(trans[k, k])
        out.append({"state": k, "mean_standardised_loadings": loadings,
                    "self_transition_probability": round(persistence, 4),
                    "expected_duration_sessions":
                        round(1.0 / max(1e-6, 1.0 - persistence), 1)})
    return out


def attach_state_features(X: np.ndarray, feature_names: list,
                          probabilities: pd.DataFrame,
                          row_dates: pd.DatetimeIndex) -> tuple:
    """Append filtered state probabilities to a design matrix.

    The paired configuration WITHOUT these columns is what the regime candidate
    must beat; that is the only comparison that isolates the state's value.
    """
    aligned = probabilities.reindex(row_dates).to_numpy()
    aligned = np.where(np.isfinite(aligned), aligned, 1.0 / max(
        1, probabilities.shape[1]))
    return (np.column_stack([X, aligned]),
            list(feature_names) + list(probabilities.columns))
