"""alpha_agent.r34.walkforward - the ONE temporal-design owner.

No random split, ever. Financial panels are serially dependent and
cross-sectionally correlated, so a shuffled split trains on the future of the
same episode it is tested on and reports a skill that does not exist.

The design is a NESTED chronological walk-forward. Each fold trains on
everything strictly before its evaluation block, minus an embargo, and every
parameter that has to be chosen - which calibration, which turnover band, which
horizon weights - is chosen INSIDE that training block, on an inner split, and
never on the block it will be scored on. Selection that touches the evaluation
block is the most common way a walk-forward turns into an in-sample fit wearing
a chronological costume.

**The embargo is not decoration.** A decision struck near the end of a training
block is still holding its position days later, so its LABEL is drawn from dates
that belong to the evaluation block. Any training decision whose holding window
crosses the boundary - plus ``EMBARGO_EXTRA_SESSIONS`` of buffer - is dropped
from training rather than allowed to leak.

**This module does not create a lockbox and will not pretend to.** The contract
declares ``FRESH_UNSEEN_EVIDENCE_EXISTS = False``, because Releases 31, 32 and
33 have all used evidence through 2026 for selection and R33's lockbox opened
2021 onward and was accessed eight times. The last fold of this walk-forward is
therefore HISTORICAL_WALK_FORWARD_EVIDENCE and calling it a fresh lockbox would
be a fiction. :func:`evidence_state` says so in one place so that no caller can
quietly upgrade the claim.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.walkforward"

SEG_TRAIN = "TRAIN"
SEG_INNER = "INNER_VALIDATION"
SEG_EVAL = "EVALUATION"
SEG_EMBARGOED = "EMBARGOED"
SEG_UNUSED = "UNUSED"


def folds(dates: pd.DatetimeIndex, *, horizon: int,
          calendar: pd.DatetimeIndex = None,
          decision_index: np.ndarray = None,
          lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS) -> list:
    """One chronological fold per declared evaluation block.

    ``calendar`` and ``decision_index`` are used to find each decision's true
    EXIT session, so the embargo is measured in sessions actually traded rather
    than in calendar days.
    """
    dates = pd.DatetimeIndex(dates)
    hold = int(lag) + int(horizon) + int(_contract.EMBARGO_EXTRA_SESSIONS)
    if calendar is not None and decision_index is not None:
        cal = pd.DatetimeIndex(calendar)
        exits = pd.DatetimeIndex(
            [cal[min(int(i) + hold, len(cal) - 1)]
             for i in np.asarray(decision_index, dtype=int)])
    else:
        exits = dates + pd.Timedelta(days=int(hold * 7 / 5) + 3)

    out = []
    for start, end in _contract.WALK_FORWARD_FOLDS:
        t0, t1 = pd.Timestamp(start), pd.Timestamp(end)
        segments = np.full(len(dates), SEG_UNUSED, dtype=object)
        # Training: decided AND fully exited before the evaluation block opens.
        train = (dates < t0) & (exits < t0)
        leaked = (dates < t0) & (exits >= t0)
        segments[train] = SEG_TRAIN
        segments[leaked] = SEG_EMBARGOED
        evaluate = (dates >= t0) & (dates <= t1)
        segments[evaluate] = SEG_EVAL

        # Inner split: the LAST slice of training, chronologically, so the
        # inner validation sits closest to the block it is meant to anticipate.
        train_idx = np.flatnonzero(segments == SEG_TRAIN)
        n_inner = int(len(train_idx) * float(
            _contract.INNER_VALIDATION_FRACTION))
        inner = train_idx[-n_inner:] if n_inner >= 8 else np.zeros(0, int)
        inner_fit = train_idx[:len(train_idx) - len(inner)]

        out.append({
            "evaluation_start": start, "evaluation_end": end,
            "segments": segments,
            "train": train_idx,
            "inner_fit": inner_fit,
            "inner_validation": inner,
            "evaluation": np.flatnonzero(segments == SEG_EVAL),
            "embargoed": int((segments == SEG_EMBARGOED).sum()),
            "usable": bool(len(train_idx) >= 8
                           and (segments == SEG_EVAL).sum() >= 4),
        })
    return out


def summarise(dates: pd.DatetimeIndex, fold: dict) -> dict:
    dates = pd.DatetimeIndex(dates)

    def _span(idx):
        if len(idx) == 0:
            return {"n": 0, "first": None, "last": None}
        sel = dates[idx]
        return {"n": int(len(idx)), "first": str(sel[0].date()),
                "last": str(sel[-1].date())}

    return {"evaluation_start": fold["evaluation_start"],
            "evaluation_end": fold["evaluation_end"],
            "train": _span(fold["train"]),
            "inner_fit": _span(fold["inner_fit"]),
            "inner_validation": _span(fold["inner_validation"]),
            "evaluation": _span(fold["evaluation"]),
            "embargoed_decisions": fold["embargoed"],
            "usable": fold["usable"]}


def row_mask(row_dates, fold_dates_index: np.ndarray,
             date_positions: dict) -> np.ndarray:
    """Boolean mask over DESIGN ROWS for one fold segment.

    Rows are ``(date, instrument)`` pairs and folds are sets of dates, so the
    mapping is by date IDENTITY. A positional join here would be silently wrong
    on every fold after the first.
    """
    wanted = set(int(i) for i in fold_dates_index)
    return np.asarray([date_positions.get(d, -1) in wanted
                       for d in row_dates], dtype=bool)


def evidence_state() -> dict:
    """What kind of evidence this design can and cannot produce.

    One place, so no caller can upgrade the claim by wording it differently.
    """
    return {
        "calculation_owner": CALCULATION_OWNER,
        "design": "NESTED_CHRONOLOGICAL_WALK_FORWARD",
        "random_split_allowed": _contract.RANDOM_SPLIT_ALLOWED,
        "folds": len(_contract.WALK_FORWARD_FOLDS),
        "nested_selection_inside_training_only":
            _contract.NESTED_SELECTION_INSIDE_TRAINING_ONLY,
        "embargo_extra_sessions": _contract.EMBARGO_EXTRA_SESSIONS,
        "evidence_produced": _contract.HISTORICAL_WALK_FORWARD_EVIDENCE,
        "fresh_unseen_evidence_exists":
            _contract.FRESH_UNSEEN_EVIDENCE_EXISTS,
        "fresh_unseen_evidence_reason":
            _contract.FRESH_UNSEEN_EVIDENCE_REASON,
        "evidence_used_by_prior_campaigns":
            list(_contract.EVIDENCE_USED_BY_PRIOR_CAMPAIGNS),
        "a_fold_may_be_called_a_lockbox": False,
        "verdict_ceiling_without_fresh_evidence":
            _contract.VERDICT_NEEDS_FORWARD,
    }
