"""alpha_agent.r33.partition - the ONE chronological partition owner.

No random train/test split, ever. Financial panels are serially dependent and
cross-sectionally correlated, so a shuffled split trains on the future of the
same episode it is tested on and reports a skill that does not exist.

Three contiguous blocks in time:

    DISCOVERY  ->  VALIDATION  ->  LOCKBOX

and between them an EMBARGO. The embargo is not a decorative gap. A decision
struck near the end of DISCOVERY is still holding its position days later, so
its LABEL is drawn from dates that belong to VALIDATION. Any decision date whose
holding window crosses a boundary - plus ``EMBARGO_EXTRA_SESSIONS`` of buffer -
is dropped from the earlier segment rather than allowed to leak.

The lockbox is the latest contiguous block, is entered once per finalist, and is
never used to select anything. TRUE_FORWARD, the operational forward-evidence
store, is a SEPARATE object that this module neither reads nor writes.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.partition"

SEG_DISCOVERY = _contract.SEGMENT_DISCOVERY
SEG_VALIDATION = _contract.SEGMENT_VALIDATION
SEG_LOCKBOX = _contract.SEGMENT_LOCKBOX
SEG_EMBARGOED = "EMBARGOED"


def assign(dates: pd.DatetimeIndex, *, horizon: int,
           lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS,
           calendar: pd.DatetimeIndex = None,
           decision_index: np.ndarray = None) -> np.ndarray:
    """Label every decision date with its segment, or ``EMBARGOED``.

    ``calendar`` and ``decision_index`` are used to find the true EXIT session
    of each decision, so the embargo is measured in sessions actually traded
    rather than in calendar days.
    """
    dates = pd.DatetimeIndex(dates)
    disc_end = pd.Timestamp(_contract.DISCOVERY_END)
    val_start = pd.Timestamp(_contract.VALIDATION_START)
    val_end = pd.Timestamp(_contract.VALIDATION_END)
    lock_start = pd.Timestamp(_contract.LOCKBOX_START)

    hold = int(lag) + int(horizon) + int(_contract.EMBARGO_EXTRA_SESSIONS)
    if calendar is not None and decision_index is not None:
        cal = pd.DatetimeIndex(calendar)
        exits = []
        for i in np.asarray(decision_index, dtype=int):
            j = min(i + hold, len(cal) - 1)
            exits.append(cal[j])
        exit_dates = pd.DatetimeIndex(exits)
    else:
        exit_dates = dates + pd.Timedelta(days=int(hold * 7 / 5) + 3)

    out = np.full(len(dates), SEG_EMBARGOED, dtype=object)
    for k in range(len(dates)):
        d, x = dates[k], exit_dates[k]
        if d <= disc_end:
            out[k] = SEG_DISCOVERY if x <= disc_end else SEG_EMBARGOED
        elif val_start <= d <= val_end:
            out[k] = SEG_VALIDATION if x <= val_end else SEG_EMBARGOED
        elif d >= lock_start:
            out[k] = SEG_LOCKBOX
    return out


def summarise(dates: pd.DatetimeIndex, segments: np.ndarray) -> dict:
    dates = pd.DatetimeIndex(dates)
    out = {}
    for seg in (SEG_DISCOVERY, SEG_VALIDATION, SEG_LOCKBOX, SEG_EMBARGOED):
        mask = segments == seg
        sel = dates[mask]
        out[seg] = {"forecast_dates": int(mask.sum()),
                    "first_date": str(sel[0].date()) if len(sel) else None,
                    "last_date": str(sel[-1].date()) if len(sel) else None}
    return out


def contract_block(horizon: int) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "random_split_allowed": False,
        "panel_start": _contract.PANEL_START,
        "discovery_end": _contract.DISCOVERY_END,
        "validation_start": _contract.VALIDATION_START,
        "validation_end": _contract.VALIDATION_END,
        "lockbox_start": _contract.LOCKBOX_START,
        "horizon_sessions": int(horizon),
        "implementation_lag_sessions": _contract.IMPLEMENTATION_LAG_SESSIONS,
        "embargo_extra_sessions": _contract.EMBARGO_EXTRA_SESSIONS,
        "embargo_rule": ("a decision whose holding window crosses a segment "
                         "boundary is dropped from the earlier segment"),
        "true_forward_is_separate": _contract.TRUE_FORWARD_IS_SEPARATE,
    }
