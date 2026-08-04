"""Stage 12 execution-lag contract (BLOCKER 3 correction).

The Stage 11 / Stage 12 ``SignalSpec`` carries ``lag = 1`` ("formation-to-first-
forward gap (days)") in its identity, but the shared forward-return primitive
(``fundamental_evidence._forward_return``) enters at the *formation* close
``closes[i]`` -- the very close the signal is formed through. Buying at that same
close is a same-bar execution that cannot be realised in practice and can leak
the formation move into the entry price (e.g. a short-term-reversal or industry-
relative signal that reads the formation close, then "enters" at it).

This module supplies the Stage-12-owned, lag-aware entry/forward-return
primitives so EVERY Stage 12 measurement enters STRICTLY AFTER formation:

* ``entry_index(dates, as_of, lag)`` -> formation_index + max(1, lag); the entry
  index is ALWAYS strictly greater than the formation index, so the entry close
  is never the formation close.
* ``entry_index_after_availability(dates, available_on, lag)`` -> the first
  trading session whose date is STRICTLY AFTER the event availability date (>= 1
  full session lag); an event's forward window can never start on the session
  that supplied or already reflected the event.
* ``forward_return_lagged`` / ``event_forward_return`` -> the horizon return from
  the lagged entry.

It deliberately does NOT touch ``fundamental_evidence._forward_return`` so Stage
11's committed artifacts and any other caller are unaffected (the directive: "Do
not rerun Stage 11. Existing Stage 11 artifacts and conclusions must not be
rewritten." -- so the repair is Stage-12-local).
"""
from __future__ import annotations

import bisect
from typing import Any, Dict, Optional, Sequence

DEFAULT_LAG = 1


def formation_index(dates: Sequence[str], as_of: str) -> int:
    """Index of the last session with ``dates[i] <= as_of`` (or -1)."""
    return bisect.bisect_right(dates, as_of) - 1


def entry_index(dates: Sequence[str], as_of: str, *, lag: int = DEFAULT_LAG
                ) -> Optional[int]:
    """Entry index = formation_index + max(1, lag).

    The ``max(1, lag)`` guarantees the entry index is STRICTLY greater than the
    formation index, so same-close entry is impossible. Returns ``None`` when the
    entry (or its horizon exit) would fall outside the series.
    """
    i = formation_index(dates, as_of)
    if i < 0:
        return None
    e = i + max(1, int(lag))
    if e >= len(dates):
        return None
    return e


def entry_index_after_availability(dates: Sequence[str], available_on: str, *,
                                   lag: int = DEFAULT_LAG) -> Optional[int]:
    """First trading session STRICTLY AFTER ``available_on`` (+ any extra lag).

    ``available_on`` is an *event availability date* (e.g. an SEC ``filed`` date,
    day precision). Because companyfacts carries only day precision, a filing may
    have been accepted intraday on ``available_on``; entering on the FIRST session
    with ``date > available_on`` applies at least one full trading-session lag and
    never trades the close that could already reflect the event.
    """
    i = formation_index(dates, available_on)  # last session on/before availability
    e = i + max(1, int(lag))                  # i+1 = first session strictly after
    if e < 0:
        e = 0
    # Defensive: guarantee the entry session's date is strictly after availability
    # even if the series has an exact-date bar at the availability date.
    while e < len(dates) and dates[e] <= available_on:
        e += 1
    if e >= len(dates):
        return None
    return e


def forward_return_from_entry(closes: Sequence[float], entry_idx: Optional[int],
                              horizon: int) -> Optional[float]:
    """Horizon return measured FROM the entry close (never the formation close)."""
    if entry_idx is None or entry_idx < 0:
        return None
    j = entry_idx + int(horizon)
    if j >= len(closes):
        return None
    if closes[entry_idx] <= 0:
        return None
    return closes[j] / closes[entry_idx] - 1.0


def forward_return_lagged(dates: Sequence[str], closes: Sequence[float],
                          as_of: str, horizon: int, *, lag: int = DEFAULT_LAG
                          ) -> Optional[float]:
    """Return over ``horizon`` sessions from the LAGGED entry (formation + lag)."""
    return forward_return_from_entry(closes, entry_index(dates, as_of, lag=lag),
                                     horizon)


def event_forward_return(dates: Sequence[str], closes: Sequence[float],
                         available_on: str, horizon: int, *,
                         lag: int = DEFAULT_LAG) -> Optional[float]:
    """Return over ``horizon`` sessions from the first session after an event."""
    return forward_return_from_entry(
        closes, entry_index_after_availability(dates, available_on, lag=lag),
        horizon)


def proves_no_same_close_entry(dates: Sequence[str], as_of: str, *,
                               lag: int = DEFAULT_LAG) -> Dict[str, Any]:
    """Diagnostic used by the tests to PROVE same-close entry is impossible.

    Returns the formation index, the entry index, and whether the entry is
    strictly after formation (it always is when both are in range).
    """
    i = formation_index(dates, as_of)
    e = entry_index(dates, as_of, lag=lag)
    return {"formation_index": i, "entry_index": e,
            "strictly_after": (e is not None and i is not None and e > i),
            "lag": max(1, int(lag))}
