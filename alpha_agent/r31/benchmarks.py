"""alpha_agent.r31.benchmarks - the ONE Release 31 benchmark owner.

Campaign v2 reported every candidate against a single benchmark: the equal-weight
return of the eligible cross-section it was selecting from. It adopted that
benchmark because the frozen panel carries no index security, and it recorded the
substitution honestly - but a substitution made for a DATA reason quietly changed
the BUSINESS question. "Did this beat an equal-weight basket of the same names we
screened?" and "did this beat buying SPY?" are different questions, and only the
second is the one an operator actually faces.

This module therefore serves BOTH, and refuses to let either stand in for the
other:

    SP500_PIT_EQUAL_WEIGHT   equal weight across the SAME point-in-time index
                             members the candidate chose from. Isolates SELECTION
                             skill: it neutralises the universe, so beating it
                             means picking better names, not merely being long.

    SPY_TOTAL_RETURN         the investable alternative. Isolates the DECISION to
                             run the strategy at all: an operator who does not run
                             it buys the ETF.

A candidate that beats the equal-weight basket but loses to SPY has demonstrated
stock selection inside a universe that was itself a bad place to be. That is worth
knowing, and it is exactly what a single blended benchmark would hide.

Availability is a STATE, never a silent fallback
------------------------------------------------
If the SPY series cannot be established this module reports
``SPY_RELATIVE_EVIDENCE_BLOCKED`` for that comparison and leaves it absent. It
never substitutes the equal-weight series into the SPY field. A missing benchmark
that silently becomes a different benchmark is the failure Campaign v2 is
superseded for, and repeating it one module over would not be an improvement.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

from .. import r31

CALCULATION_OWNER = "alpha_agent.r31.benchmarks"
MANIFEST_SCHEMA = "r31_benchmark_manifest/1"
MANIFEST_NAME = "benchmark_manifest.json"
CACHE_NAME = "benchmark_series.npz"

#: The two benchmarks, both always reported.
BENCH_EQUAL_WEIGHT = "SP500_PIT_EQUAL_WEIGHT"
BENCH_SPY = "SPY_TOTAL_RETURN"
BENCHMARKS = (BENCH_EQUAL_WEIGHT, BENCH_SPY)

#: Evidence states for the investable benchmark.
SPY_AVAILABLE = "SPY_TOTAL_RETURN_AVAILABLE"
SPY_BLOCKED = "SPY_RELATIVE_EVIDENCE_BLOCKED"

#: Preference order. ``$SPXTR`` is the S&P 500 TOTAL RETURN index - it reinvests
#: dividends, which is the correct like-for-like against a total-return strategy.
#: ``SPY`` is the tradable ETF and is the fallback; its Norgate history is
#: dividend-adjusted, so it is a defensible second choice. A PRICE-only index is
#: NOT admissible here: comparing a total-return strategy with a price index
#: manufactures roughly two points a year of fake outperformance.
SPY_SOURCE_PREFERENCE = ("$SPXTR", "SPY")
PRICE_ONLY_INADMISSIBLE = ("$SPX",)


def _norgate():
    try:
        logging.disable(logging.WARNING)
        import norgatedata as nd
    except ImportError:                              # pragma: no cover - env
        return None
    try:
        return nd if nd.status() else None
    except Exception:                                # pragma: no cover - env
        return None


# --------------------------------------------------------------------------- #
# Build + cache
# --------------------------------------------------------------------------- #
def build_cache(*, campaign_id: str, panel_dates, force: bool = False) -> Path:
    """Freeze the investable benchmark's daily close on the panel's calendar.

    Alignment is BY DATE, never by position. The index series and the panel do
    not share a calendar - holidays and the panel's own construction differ - so
    positional alignment would shift the benchmark against the strategy by a
    variable number of sessions and quietly invent alpha.
    """
    out = r31.campaign_dir(campaign_id) / CACHE_NAME
    if out.exists() and not force:
        return out

    dates = [str(d) for d in panel_dates]
    pos = {d: i for i, d in enumerate(dates)}
    close = np.full(len(dates), np.nan, dtype=np.float64)
    source = ""

    nd = _norgate()
    if nd is not None:
        for sym in SPY_SOURCE_PREFERENCE:
            try:
                ts = nd.price_timeseries(
                    sym, start_date=dates[0], end_date=dates[-1],
                    format="numpy-recarray", timeseriesformat="numpy-recarray")
            except Exception:
                continue
            if ts is None or len(ts) == 0:
                continue
            hit = 0
            for d, c in zip(np.asarray(ts["Date"]).astype(str),
                            np.asarray(ts["Close"]).astype(np.float64)):
                i = pos.get(str(d)[:10])
                if i is not None:
                    close[i] = float(c)
                    hit += 1
            if hit > 0:
                source = sym
                break

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.stem + ".building.npz")
    with open(tmp, "wb") as fh:
        np.savez_compressed(fh, close=close,
                            dates=np.array(dates, dtype="U10"),
                            source=np.array([source], dtype="U16"))
    Path(tmp).replace(out)
    return out


class Benchmarks:
    """The frozen investable-benchmark series."""

    def __init__(self, close, dates, source):
        self.close = np.asarray(close, dtype=np.float64)
        self.dates = [str(d) for d in dates]
        self.source = str(source)
        self._pos = {d: i for i, d in enumerate(self.dates)}
        self.coverage = float(np.isfinite(self.close).mean()) if self.close.size else 0.0

    @property
    def spy_state(self) -> str:
        # A series present for only part of the window cannot carry the whole
        # comparison; the threshold is declared rather than discovered.
        return SPY_AVAILABLE if (self.source and self.coverage >= 0.95) else SPY_BLOCKED

    def hold_return(self, date_from: str, date_to: str) -> Optional[float]:
        """Total return of the investable benchmark between two panel dates."""
        i, j = self._pos.get(str(date_from)), self._pos.get(str(date_to))
        if i is None or j is None:
            return None
        a, b = self.close[i], self.close[j]
        if not (np.isfinite(a) and np.isfinite(b)) or a <= 0:
            return None
        return float(b / a - 1.0)


def load(*, campaign_id: str) -> Benchmarks:
    path = r31.campaign_dir(campaign_id) / CACHE_NAME
    with np.load(path, allow_pickle=False) as z:
        return Benchmarks(z["close"], z["dates"], z["source"][0])


# --------------------------------------------------------------------------- #
# The equal-weight benchmark
# --------------------------------------------------------------------------- #
def equal_weight_return(holding_returns: np.ndarray,
                        eligible: np.ndarray) -> Optional[float]:
    """Equal-weight return across the point-in-time index members.

    ``eligible`` is the SAME membership mask the candidate selected from, so this
    neutralises the universe and isolates selection skill. Names whose holding
    return is missing - a delisting inside the holding window - are excluded from
    the average rather than treated as zero, because a zero return for a delisted
    name is a fabricated price.
    """
    r = np.asarray(holding_returns, dtype=np.float64)
    m = np.asarray(eligible, dtype=bool) & np.isfinite(r)
    return float(r[m].mean()) if int(m.sum()) > 0 else None


# --------------------------------------------------------------------------- #
# Frozen manifest
# --------------------------------------------------------------------------- #
def build_manifest(*, campaign_id: str) -> dict:
    b = load(campaign_id=campaign_id)
    finite = np.isfinite(b.close)
    body = {
        "manifest": MANIFEST_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "benchmarks_reported": list(BENCHMARKS),
        "both_always_reported": True,
        "equal_weight": {
            "name": BENCH_EQUAL_WEIGHT,
            "definition": "equal weight across the point-in-time S&P 500 members "
                          "the candidate selected from, on the same dates",
            "isolates": "SELECTION_SKILL_UNIVERSE_NEUTRALISED",
            "always_available": True,
        },
        "investable": {
            "name": BENCH_SPY,
            "source_symbol": b.source or None,
            "source_preference": list(SPY_SOURCE_PREFERENCE),
            "price_only_series_inadmissible": list(PRICE_ONLY_INADMISSIBLE),
            "total_return": True,
            "coverage_fraction": round(b.coverage, 6),
            "state": b.spy_state,
            "isolates": "DECISION_TO_RUN_THE_STRATEGY_AT_ALL",
            "first_covered": (b.dates[int(np.argmax(finite))] if finite.any() else None),
            "last_covered": (b.dates[len(finite) - 1 - int(np.argmax(finite[::-1]))]
                             if finite.any() else None),
            "cache_sha256": r31.sha_file(r31.campaign_dir(campaign_id) / CACHE_NAME),
        },
        "substitution_policy": (
            "the equal-weight benchmark is ADDITIONAL, never a replacement; if the "
            "investable series is unavailable the campaign reports "
            "SPY_RELATIVE_EVIDENCE_BLOCKED and leaves that comparison absent"),
        "silent_substitution_permitted": False,
    }
    body["benchmark_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def path_for(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / MANIFEST_NAME


def freeze(body: dict) -> Path:
    return r31.write_json(path_for(body["campaign_id"]), body)
