"""alpha_agent.r35.design - base (+) new information, on identical rows.

The two arms of every Release-35 comparison must differ in ONE thing: which
columns the model may see. This module is the only place that guarantees it.

It never rebuilds a design matrix. It takes the context Release 34 already
builds - the same rows, the same decision dates, the same folds, the same
targets, the same tradability - and appends columns to ``X``. The row set is
decided entirely by the BASE features (a row survives when any base feature is
finite), so the base arm and every candidate arm are
**row-identical by construction** rather than by a join that could slip.

That matters more than it sounds. A paired per-date statistic comparing two arms
scored on slightly different rows is not a paired statistic, and the difference
it reports is a sampling artefact wearing the costume of an increment.

The second job here is availability. A family that begins in 2009 has nothing to
say about 2008, and scoring the augmented arm on 2008 dates - where its columns
are all neutral fill - would dilute a real increment toward zero and make an
absent source look like a tested one. :func:`available_dates` returns the dates
on which a family genuinely carries information, and the campaign scores BOTH
arms on exactly those dates.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.design"

#: A family is considered to carry information on a decision date when at least
#: this many instruments have a REAL (not neutral-filled) value. One is enough
#: for a market-level family, which is constant across instruments by nature.
MIN_INSTRUMENTS_WITH_VALUE = 1


def _lookup(frame: pd.DataFrame, decision_index: np.ndarray,
            symbol_position: np.ndarray) -> np.ndarray:
    """Pull one feature frame onto design rows by (session, instrument).

    Positional, and safe to be positional: ``decision_index`` and
    ``symbol_position`` are the calendar row and the panel column the design
    itself recorded, so there is no name matching to get wrong.
    """
    values = frame.to_numpy(dtype=float)
    return values[np.asarray(decision_index, dtype=np.int64),
                  np.asarray(symbol_position, dtype=np.int64)]


def augment_context(base_ctx: dict, *, frames: dict, feature_names) -> dict:
    """A context identical to ``base_ctx`` but with extra feature columns.

    Missing values become the declared neutral zero rather than a
    cross-sectional median. R33 established the reason on ``bond_carry_slope``:
    a median fill would hand a bond's carry to every currency in the panel, so
    a feature declared unavailable would be silently present and would measure
    something other than what it claims. The same argument applies to every
    feature here, all of which are structurally absent somewhere.
    """
    names = list(feature_names)
    if not names:
        return dict(base_ctx)
    design = base_ctx["design"]
    di = design["decision_index"]
    sp = design["symbol_position"]
    columns = []
    for name in names:
        frame = frames.get(name)
        if frame is None:
            columns.append(np.zeros(len(di), dtype=float))
            continue
        raw = _lookup(frame, di, sp)
        columns.append(np.where(np.isfinite(raw), raw, 0.0))
    extra = np.column_stack(columns) if columns else np.zeros((len(di), 0))

    new_design = dict(design)
    new_design["X"] = np.hstack([design["X"], extra])
    new_design["feature_names"] = list(design["feature_names"]) + names
    new_design["added_feature_names"] = names

    ctx = dict(base_ctx)
    ctx["design"] = new_design
    ctx["feature_names"] = new_design["feature_names"]
    ctx["information_set_features"] = names
    return ctx


def only_new_context(base_ctx: dict, *, frames: dict, feature_names) -> dict:
    """A context carrying ONLY the new columns - the standalone diagnostic.

    Reported so the write-up can distinguish "this source predicts" from "this
    source predicts something the base set did not already know". They are
    different claims and only the second one is the release question.
    """
    ctx = augment_context(base_ctx, frames=frames,
                          feature_names=feature_names)
    design = ctx["design"]
    n_base = len(base_ctx["design"]["feature_names"])
    new_design = dict(design)
    new_design["X"] = design["X"][:, n_base:]
    new_design["feature_names"] = list(feature_names)
    new_design["added_feature_names"] = list(feature_names)
    ctx["design"] = new_design
    ctx["feature_names"] = list(feature_names)
    return ctx


def available_dates(base_ctx: dict, presence: pd.DataFrame, *,
                    min_instruments: int = MIN_INSTRUMENTS_WITH_VALUE
                    ) -> pd.DatetimeIndex:
    """Decision dates on which a family genuinely carries information."""
    if presence is None or presence.empty:
        return pd.DatetimeIndex([])
    counts = presence.sum(axis=1)
    live = counts[counts >= int(min_instruments)].index
    return pd.DatetimeIndex(sorted(set(base_ctx["udates"]) & set(live)))


def row_mask_for_dates(ctx: dict, dates) -> np.ndarray:
    """Boolean row mask selecting the design rows struck on ``dates``."""
    wanted = set(pd.DatetimeIndex(dates))
    return np.asarray([d in wanted for d in ctx["row_dates"]], dtype=bool)


def row_mask_for_presence(ctx: dict, presence: pd.DataFrame) -> np.ndarray:
    """Rows where a family carries a REAL value for that (date, instrument).

    Used for the STANDALONE diagnostic and nowhere else, and the reason is
    worth stating. Positioning covers seventeen of forty-seven instruments; the
    other thirty carry an all-zero feature block, so a standalone model gives
    them one identical predicted value and Spearman assigns them one shared
    mid-rank. If the covered instruments happen to out- or under-perform the
    uncovered ones on average, that block placement produces a rank IC out of
    nothing but the coverage pattern. Scoring the standalone arm on covered rows
    only asks the question that is actually meaningful - does this information
    rank the instruments it knows about - and removes the artefact.

    The INCREMENT is deliberately NOT masked this way: there both arms face the
    real forty-seven-instrument decision, which is the problem the release is
    about.
    """
    if presence is None or presence.empty:
        return np.zeros(len(ctx["row_dates"]), dtype=bool)
    design = ctx["design"]
    values = _lookup(presence.astype(float), design["decision_index"],
                     design["symbol_position"])
    return np.asarray(values > 0.5, dtype=bool)


def information_sets(built: dict) -> dict:
    """Every information set this campaign evaluates, in one place.

    ``BASE`` is the reference arm and is never a candidate; the six single
    families and their union are the candidates. Enumerated from the acquired
    families so the plan cannot disagree with what ran.
    """
    families = [f for f in _contract.ACQUIRED_FAMILIES if f in built]
    sets = {"BASE": []}
    for family in families:
        sets[family] = list(_contract.features_of(family))
    if len(families) > 1:
        combined = []
        for family in families:
            combined.extend(_contract.features_of(family))
        sets["ALL_NEW_COMBINED"] = combined
    return sets


__all__ = ["CALCULATION_OWNER", "augment_context", "only_new_context",
           "available_dates", "row_mask_for_dates", "row_mask_for_presence",
           "information_sets", "MIN_INSTRUMENTS_WITH_VALUE"]
