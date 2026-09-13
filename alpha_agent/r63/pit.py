"""alpha_agent.r63.pit - the ONE as-of join and the purged / embargoed split.

Two things in R63 could quietly introduce look-ahead, and both live here so
there is exactly one place to check:

* ``as_of``          projects an ``available_at``-indexed series onto a session
                     calendar with a strict ``available_at <= session`` rule,
                     then shifts by the declared broadcast lag. A value is
                     never visible before it was public, and a publication lag
                     that is CONTRACTED (COT: 6 calendar days; EIA: 7) is added
                     to the raw period date before the join, never after.
* ``walk_forward``   expanding-window folds by calendar year with PURGE (a
                     training decision whose forward window overlaps the test
                     block is dropped) and EMBARGO (h sessions between the last
                     admissible training decision and the first test decision).
                     Folds before LOCKBOX_START are SELECTION folds; the block
                     from LOCKBOX_START is the LOCKBOX, evaluated once.

Pure numpy / pandas; deterministic; no I/O.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from . import (LOCKBOX_START, MARKET_BROADCAST_LAG_SESSIONS, MIN_TRAIN_SESSIONS)

CALCULATION_OWNER = "alpha_agent.r63.pit"


# --------------------------------------------------------------------------- #
# As-of join
# --------------------------------------------------------------------------- #
def as_of(values: pd.Series, calendar, *, lag_sessions: int = MARKET_BROADCAST_LAG_SESSIONS,
          publication_lag_days: int = 0) -> np.ndarray:
    """Project an availability-indexed series onto ``calendar`` (sorted dates).

    ``values`` is indexed by the instant the value became PUBLIC (or by the
    period date when ``publication_lag_days`` carries the declared lag). For
    each session the result holds the most recent value whose availability is
    <= that session's date, shifted ``lag_sessions`` further sessions.
    Missing values are NaN, never filled from the future.
    """
    cal = pd.DatetimeIndex(pd.to_datetime(np.asarray(calendar)))
    if values is None or len(values) == 0:
        return np.full(len(cal), np.nan)
    s = pd.Series(np.asarray(values.values, dtype=float),
                  index=pd.to_datetime(values.index)).dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if publication_lag_days:
        s.index = s.index + pd.Timedelta(days=int(publication_lag_days))
        s = s[~s.index.duplicated(keep="last")].sort_index()
    if s.empty:
        return np.full(len(cal), np.nan)
    # position of the last availability <= session date
    pos = np.searchsorted(s.index.values, cal.values, side="right") - 1
    out = np.where(pos >= 0, s.values[np.clip(pos, 0, None)], np.nan)
    if lag_sessions:
        k = int(lag_sessions)
        out = np.concatenate([np.full(k, np.nan), out[:-k]]) if k < len(out) \
            else np.full(len(out), np.nan)
    return out.astype(float)


def as_of_frame(frame: pd.DataFrame, calendar, **kw) -> pd.DataFrame:
    """Column-wise ``as_of`` for a frame of availability-indexed series."""
    cal = pd.DatetimeIndex(pd.to_datetime(np.asarray(calendar)))
    return pd.DataFrame({c: as_of(frame[c], cal, **kw) for c in frame.columns},
                        index=cal)


def strictly_after(available: pd.Series, calendar) -> np.ndarray:
    """``as_of`` where a value becomes visible at the first session STRICTLY
    after its availability date (used for ALFRED vintages, whose
    ``realtime_start`` is a calendar date and may fall on a session)."""
    cal = pd.DatetimeIndex(pd.to_datetime(np.asarray(calendar)))
    s = pd.Series(np.asarray(available.values, dtype=float),
                  index=pd.to_datetime(available.index)).dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if s.empty:
        return np.full(len(cal), np.nan)
    pos = np.searchsorted(s.index.values, cal.values, side="left") - 1
    return np.where(pos >= 0, s.values[np.clip(pos, 0, None)], np.nan).astype(float)


# --------------------------------------------------------------------------- #
# Forward windows
# --------------------------------------------------------------------------- #
def forward_compound(ret: np.ndarray, horizon: int) -> np.ndarray:
    """Forward compounded return over (t+1 .. t+1+h) per NEXT_CLOSE.

    ``ret`` is a (n_inst x n_dates) matrix of simple session returns where
    ret[:, t] is the return earned on session t. A missing session earns zero
    inside a window that has at least one observed session; a window with no
    observed session is NaN.
    """
    r = np.where(np.isfinite(ret), ret, 0.0)
    seen = np.isfinite(ret)
    lg = np.log1p(np.clip(r, -0.999999, None))
    n_i, n_t = ret.shape
    out = np.full((n_i, n_t), np.nan)
    cum = np.concatenate([np.zeros((n_i, 1)), np.cumsum(lg, axis=1)], axis=1)
    cnt = np.concatenate([np.zeros((n_i, 1), dtype=int),
                          np.cumsum(seen, axis=1)], axis=1)
    h = int(horizon)
    # window sessions t+2 .. t+1+h in return-index terms (entry at close t+1)
    for t in range(0, n_t - h - 1):
        a, b = t + 2, t + 2 + h
        win = cum[:, b] - cum[:, a]
        k = cnt[:, b] - cnt[:, a]
        out[:, t] = np.where(k > 0, np.expm1(win), np.nan)
    return out


# --------------------------------------------------------------------------- #
# Walk-forward folds with purge and embargo
# --------------------------------------------------------------------------- #
def decision_indices(dates: np.ndarray, first_date: str, cadence: int,
                     horizon: int) -> np.ndarray:
    start = int(np.searchsorted(dates, first_date))
    last = len(dates) - int(horizon) - 2
    if last <= start:
        return np.array([], dtype=int)
    return np.arange(start, last + 1, int(cadence))


def walk_forward(dates: np.ndarray, idx: np.ndarray, *, horizon: int,
                 min_train_sessions: int = MIN_TRAIN_SESSIONS,
                 lockbox_start: str = LOCKBOX_START) -> list:
    """Yearly expanding-window folds over decision indices ``idx``.

    Returns a list of dicts: ``{"kind": "SELECTION"|"LOCKBOX", "test_year",
    "train": array of decision positions, "test": array of decision
    positions}``. Purge: a training decision t is admissible only if
    t + 1 + h < first test session index. Embargo: additionally t <= first
    test index - h - 1. Both are enforced together as ``t < t0 - h - 1``.
    """
    if len(idx) == 0:
        return []
    years = np.array([int(str(dates[t])[:4]) for t in idx])
    lock_ix = int(np.searchsorted(dates, lockbox_start))
    first_ok = idx[0] + int(min_train_sessions)
    folds = []
    h = int(horizon)
    for y in sorted(set(years)):
        test_pos = np.where((years == y) & (idx < lock_ix))[0]
        if len(test_pos) == 0:
            continue
        t0 = idx[test_pos[0]]
        if t0 < first_ok:
            continue
        train_pos = np.where(idx < t0 - h - 1)[0]
        if len(train_pos) == 0:
            continue
        folds.append({"kind": "SELECTION", "test_year": y,
                      "train": train_pos, "test": test_pos})
    lock_pos = np.where(idx >= lock_ix)[0]
    if len(lock_pos):
        t0 = idx[lock_pos[0]]
        train_pos = np.where(idx < t0 - h - 1)[0]
        if len(train_pos):
            folds.append({"kind": "LOCKBOX", "test_year": int(str(dates[t0])[:4]),
                          "train": train_pos, "test": lock_pos})
    return folds


def blocked_inner_folds(n_train: int, k: int = 3, *, gap: int = 0) -> list:
    """Contiguous blocked K-fold over training positions, with an optional
    gap of ``gap`` positions on each side of the held-out block (the inner
    embargo). Returns (fit, hold) position arrays in training-local terms."""
    if n_train < 2 * k:
        return []
    edges = np.linspace(0, n_train, k + 1).astype(int)
    out = []
    for i in range(k):
        a, b = edges[i], edges[i + 1]
        hold = np.arange(a, b)
        fit = np.concatenate([np.arange(0, max(0, a - gap)),
                              np.arange(min(n_train, b + gap), n_train)])
        if len(fit) >= 10 and len(hold) >= 5:
            out.append((fit, hold))
    return out


def nw_lag(horizon: int, cadence: int) -> int:
    return max(0, int(math.ceil(int(horizon) / float(max(1, int(cadence))))) - 1)
