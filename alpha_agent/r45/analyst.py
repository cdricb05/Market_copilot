"""alpha_agent.r45.analyst - Track M. The one asset that is bought with time.

Release 44 established two things about analyst expectations. The vendor's
backward strip of "30 days ago" consensus reproduces this estate's own
prospectively captured snapshots only about half the time, so it is restated
and cannot be used as history. And the estate's own ledger - captured
forward, never backfilled - does work.

That ledger has a hard point-in-time floor at 2026-07-31. Nothing before it
exists and nothing before it may ever be written. All Release 45 does here
is measure how much longer it has become, how often the estimates actually
move, and how far it still is from a sample anyone could judge.

This module writes nothing into the ledger. It only reads it.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import numpy as np

from ..r44 import acquisition as R44AQ
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.analyst"

VINTAGE_ROOT = R44AQ.VINTAGE_ROOT
#: R44 measured 24 days of ledger. Any growth is measured against that.
R44_LEDGER_SPAN_DAYS = 24
R44_SERIES_REVISED = 18
R44_SERIES_TOTAL = 47
#: A revision study needs at least this many independent estimate changes
#: before it can be judged. Declared, not chosen after counting.
MIN_REVISIONS_TO_JUDGE = 250
READ_ONLY = True


def _f(x):
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def revision_frequency() -> dict:
    """How often a (ticker, fiscal period) estimate actually changes."""
    days = R44AQ._snapshot_dates()
    if len(days) < 2:
        return {"state": "INSUFFICIENT_SNAPSHOTS", "n_snapshots": len(days)}
    snaps = {d: R44AQ._trend_rows(R44AQ._load_snapshot(d)) for d in days}

    series_seen, series_moved, moves = set(), set(), []
    for i in range(1, len(days)):
        prev, cur = days[i - 1], days[i]
        gap = (cur - prev).days
        a, b = snaps[prev], snaps[cur]
        for key in set(a) & set(b):
            va, vb = _f(a[key].get("epsTrendCurrent")), \
                _f(b[key].get("epsTrendCurrent"))
            if va is None or vb is None:
                continue
            series_seen.add(key)
            if va != vb:
                series_moved.add(key)
                moves.append({"gap_days": gap,
                              "abs_change": abs(vb - va),
                              "rel_change": abs(vb - va) / max(1e-9, abs(va))})
    if not series_seen:
        return {"state": "NO_COMPARABLE_SERIES"}
    span = (days[-1] - days[0]).days
    return {
        "state": "MEASURED",
        "n_snapshot_dates": len(days),
        "span_days": span,
        "n_series_tracked": len(series_seen),
        "n_series_ever_revised": len(series_moved),
        "share_of_series_revised": len(series_moved) / len(series_seen),
        "n_observed_revisions": len(moves),
        "median_abs_revision": (float(np.median([m["abs_change"]
                                                 for m in moves]))
                                if moves else None),
        "median_rel_revision": (float(np.median([m["rel_change"]
                                                 for m in moves]))
                                if moves else None),
        "revisions_per_series_per_30d": (
            len(moves) / max(1, len(series_seen)) * 30.0 / max(1, span)),
    }


def run() -> dict:
    ledger = R44AQ.vintage_ledger_state()
    if ledger.get("state") != "PRESENT":
        return {"track": "M", "state": "HISTORICAL_DATA_UNAVAILABLE",
                "ledger": ledger}
    freq = revision_frequency()
    strip = R44AQ.reconcile_backward_strip()

    span = int(ledger.get("span_days") or 0)
    grew = span - R44_LEDGER_SPAN_DAYS
    n_rev = int(freq.get("n_observed_revisions") or 0)
    per_30 = float(freq.get("revisions_per_series_per_30d") or 0.0)
    n_series = int(freq.get("n_series_tracked") or 0)
    monthly = per_30 * max(1, n_series)
    months_needed = ((MIN_REVISIONS_TO_JUDGE - n_rev) / monthly
                     if monthly > 0 else None)

    return {
        "track": "M", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["L14_ANALYST"],
        "read_only": READ_ONLY,
        "wrote_into_the_ledger": False,
        "backfilled": False,
        "hard_pit_floor": ledger.get("hard_pit_floor"),
        "ledger": {k: ledger.get(k) for k in
                   ("first_snapshot", "last_snapshot", "n_snapshot_dates",
                    "span_days", "n_tickers", "tickers_per_snapshot")},
        "growth_since_r44": {
            "r44_span_days": R44_LEDGER_SPAN_DAYS,
            "r45_span_days": span,
            "days_added": grew,
            "snapshots_now": ledger.get("n_snapshot_dates"),
        },
        "revision_frequency": freq,
        "vendor_backward_strip_reconciliation": strip,
        "judgeable_sample": {
            "min_revisions_required": MIN_REVISIONS_TO_JUDGE,
            "observed_revisions": n_rev,
            "still_required": max(0, MIN_REVISIONS_TO_JUDGE - n_rev),
            "estimated_months_of_waiting": (round(months_needed, 1)
                                            if months_needed and
                                            months_needed > 0 else 0),
            "state": "JUDGEABLE" if n_rev >= MIN_REVISIONS_TO_JUDGE
            else "STILL_SHORT",
        },
        "blocker": "FUTURE_TIME_REQUIRED" if n_rev < MIN_REVISIONS_TO_JUDGE
        else "EXECUTED",
        "why_it_is_still_worth_running": "it is the only revision history "
                                         "this estate will ever own that "
                                         "cannot have been restated, and the "
                                         "only thing it costs is time",
        "money_spent_usd": 0.0, "vendor_emails_sent": 0,
    }
