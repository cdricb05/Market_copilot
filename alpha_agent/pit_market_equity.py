"""alpha_agent/pit_market_equity.py — Stage 26 point-in-time MARKET EQUITY.

Stage 25 recorded PIT market cap as `WAITING_FOR_DATA`, blocked by two
independent gaps, and warned that fixing only one would produce a *plausible but
wrong* number. Both are closed here with **owned** data and no purchase:

1. **Share counts were being discarded.** The released companyfacts indexer
   emits monetary-USD facts only, so a `shares`-unit fact never reached the
   panel. ``sec_companyfacts_index.parse_companyfacts_facts`` now takes an
   explicit, opt-in ``extra_units`` / ``extra_taxonomies`` widening (default
   behaviour byte-identical), and this stage indexes the share concepts.

2. **The only owned price surface was TOTALRETURN adjusted.** Splits *and*
   dividends are baked into it, so it cannot be inverted back to a traded price.
   The same owned, entitled local Norgate installation serves ``NONE`` (raw
   traded price) and ``CAPITAL`` (capital events only); Stage 26 pulls both.

The business concept is **market equity at the formation date** — not today's
shares times a historical adjusted price, and not today's market cap projected
backward. The construction is:

    shares(formation) = shares(report) * f(formation) / f(report)
    market_equity     = shares(formation) * close_none(formation)

where ``f(t) = close_capital(t) / close_none(t)`` is the cumulative
capital-event factor at ``t``. The share ratio term is what makes a split
occurring *between* the report date and the formation date a handled case rather
than a silent error: a 2-for-1 split doubles ``f``, which doubles the carried
share count exactly as it doubled the real one.

Every input is read point-in-time. A share count is used only if the filing that
carried it was **filed on or before** the formation date; a name whose share
count, price or factor is missing stays missing and is never defaulted.

Research-only, read-only. No operational store, no order, no model.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Sequence

CONTRACT_VERSION = "stage26-pit-market-equity-1.0.0"

#: Share-count concepts in preference order. The cover-page `dei` count is first
#: because it is stated as of a date close to the FILING date rather than the
#: fiscal period end, so it is the freshest count that was legitimately knowable.
#: The us-gaap balance-sheet counts are the fallbacks, in the order that puts the
#: economically correct concept (outstanding) ahead of the broader one (issued,
#: which includes treasury stock).
SHARE_CONCEPTS = (
    "EntityCommonStockSharesOutstanding",   # dei — cover page, filing-dated
    "CommonStockSharesOutstanding",         # us-gaap — balance sheet
    "CommonStockSharesIssued",              # us-gaap — includes treasury stock
)

SHARE_TAGS = frozenset(SHARE_CONCEPTS)

#: Sanity envelope on the produced market equity, in USD. These are not tuning
#: knobs: a value outside this range is an identity or unit error (a share count
#: matched to the wrong issuer, or a per-class count paired with a total price),
#: not a small company or a large one. Values outside are dropped as UNRELIABLE
#: rather than winsorized into the cross-section.
MIN_PLAUSIBLE_MARKET_EQUITY = 1e6          # $1 million
MAX_PLAUSIBLE_MARKET_EQUITY = 1e13         # $10 trillion

#: A share count older than this at the formation date is stale enough that the
#: issuer's capital structure may have moved materially. It is still used — a
#: stale count is the honest point-in-time answer — but it is labelled so a
#: coverage report can quantify it.
STALE_SHARES_DAYS = 400

# Dispositions for one (symbol, formation) attempt.
OK = "OK"
NO_SHARES = "NO_PIT_SHARE_COUNT"
NO_PRICE = "NO_UNADJUSTED_PRICE"
NO_FACTOR = "NO_CAPITAL_FACTOR"
IMPLAUSIBLE = "IMPLAUSIBLE_MARKET_EQUITY"


# --------------------------------------------------------------------------- #
# Point-in-time share counts
# --------------------------------------------------------------------------- #
class PitShareCounts:
    """Share counts read from a Stage-26 companyfacts index, point-in-time.

    ``shares_as_of(cik, as_of)`` returns the most recent share count whose
    filing was **filed on or before** ``as_of``, preferring the earlier concepts
    in :data:`SHARE_CONCEPTS`. A concept is never blended with another and a
    missing count is never defaulted.
    """

    def __init__(self, index_path: "str | Path") -> None:
        self.index_path = Path(index_path)
        #: cik -> concept -> sorted [(filed, period_end, value)]
        self._by_cik: "dict[str, dict[str, list]]" = {}
        self.load_status: dict = {}

    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        if not self.index_path.exists():
            self.load_status = {"ok": False, "reason": "SHARES_INDEX_ABSENT",
                                "path": str(self.index_path)}
            return self.load_status
        conn = sqlite3.connect("file:%s?mode=ro" % self.index_path, uri=True)
        conn.row_factory = sqlite3.Row
        wanted = {str(c).zfill(10) for c in (ciks or []) if c} or None
        rows = 0
        try:
            cur = conn.execute(
                "SELECT cik, concept_tag, value, period_end, filed FROM cf_fact "
                "WHERE concept_tag IN (%s) AND value IS NOT NULL AND filed IS NOT NULL"
                % ",".join("?" * len(SHARE_CONCEPTS)), tuple(SHARE_CONCEPTS))
            for r in cur:
                cik = str(r["cik"]).zfill(10)
                if wanted is not None and cik not in wanted:
                    continue
                val = r["value"]
                if val is None or float(val) <= 0:
                    continue
                self._by_cik.setdefault(cik, {}).setdefault(
                    r["concept_tag"], []).append(
                        (str(r["filed"])[:10], str(r["period_end"] or "")[:10],
                         float(val)))
                rows += 1
        finally:
            conn.close()
        for concepts in self._by_cik.values():
            for series in concepts.values():
                series.sort()
        self.load_status = {
            "ok": True, "path": str(self.index_path), "fact_rows": rows,
            "ciks": len(self._by_cik),
            "concepts": sorted(SHARE_CONCEPTS),
            "evidence_class": "OWNED_PIT_SEC_FILED_DATE",
        }
        return self.load_status

    def shares_as_of(self, cik: str, as_of: str) -> Optional[dict]:
        """Most recent share count FILED on or before ``as_of``, or ``None``."""
        concepts = self._by_cik.get(str(cik).zfill(10))
        if not concepts:
            return None
        cutoff = str(as_of)[:10]
        for concept in SHARE_CONCEPTS:
            series = concepts.get(concept)
            if not series:
                continue
            best = None
            for filed, period_end, value in series:
                if filed > cutoff:
                    break            # sorted by filed; nothing later qualifies
                best = (filed, period_end, value)
            if best is not None:
                return {"concept": concept, "filed": best[0],
                        "period_end": best[1], "shares": best[2],
                        "age_days": _day_gap(best[0], cutoff)}
        return None

    def covered_ciks(self) -> set:
        return set(self._by_cik)


def _day_gap(earlier: str, later: str) -> Optional[int]:
    from datetime import date
    try:
        a = date.fromisoformat(str(earlier)[:10])
        b = date.fromisoformat(str(later)[:10])
    except ValueError:
        return None
    return (b - a).days


# --------------------------------------------------------------------------- #
# Unadjusted / capital-adjusted price surface
# --------------------------------------------------------------------------- #
class UnadjustedPriceSurface:
    """The owned ``NONE`` and ``CAPITAL`` daily closes, queried as-of a date.

    Both series are needed: ``close_none`` is the multiplicand for an
    as-reported share count, and the ratio to ``close_capital`` is the
    cumulative capital-event factor that carries a share count between dates.
    """

    def __init__(self, npz_path: "str | Path") -> None:
        self.npz_path = Path(npz_path)
        self._dates = None
        self._sym_index: "dict[str, int]" = {}
        self._none = None
        self._capital = None
        self.load_status: dict = {}

    def load(self) -> dict:
        if not self.npz_path.exists():
            self.load_status = {"ok": False, "reason": "PRICE_SURFACE_ABSENT",
                                "path": str(self.npz_path)}
            return self.load_status
        import numpy as np
        z = np.load(self.npz_path, allow_pickle=False)
        self._dates = [str(d) for d in z["dates"].astype("datetime64[D]").tolist()]
        symbols = [str(s) for s in z["symbols"].tolist()]
        self._sym_index = {s: i for i, s in enumerate(symbols)}
        self._none = z["close_none"]
        self._capital = z["close_capital"]
        self.load_status = {
            "ok": True, "path": str(self.npz_path), "symbols": len(symbols),
            "trading_days": len(self._dates),
            "first_date": self._dates[0] if self._dates else None,
            "last_date": self._dates[-1] if self._dates else None,
            "adjustments": ["NONE", "CAPITAL"],
            "evidence_class": "OWNED_NORGATE_UNADJUSTED",
        }
        return self.load_status

    def _row_asof(self, as_of: str) -> Optional[int]:
        """Index of the last trading day on or before ``as_of``."""
        import bisect
        if not self._dates:
            return None
        i = bisect.bisect_right(self._dates, str(as_of)[:10]) - 1
        return i if i >= 0 else None

    def closes_as_of(self, symbol: str, as_of: str) -> Optional[dict]:
        """``{date, close_none, close_capital, capital_factor}`` or ``None``.

        Walks back over non-trading days and gaps up to a bounded window so a
        formation dated to a month end resolves to that month's last real close,
        never to a fabricated price.
        """
        j = self._sym_index.get(symbol)
        if j is None:
            return None
        i = self._row_asof(as_of)
        if i is None:
            return None
        lo = max(0, i - 10)
        while i >= lo:
            n = float(self._none[i, j])
            c = float(self._capital[i, j])
            if math.isfinite(n) and math.isfinite(c) and n > 0 and c > 0:
                return {"date": self._dates[i], "close_none": n,
                        "close_capital": c, "capital_factor": c / n}
            i -= 1
        return None

    def covered_symbols(self) -> set:
        return set(self._sym_index)


# --------------------------------------------------------------------------- #
# The construction
# --------------------------------------------------------------------------- #
def market_equity(*, shares_rec: Optional[dict], formation_px: Optional[dict],
                  report_px: Optional[dict]) -> dict:
    """Point-in-time market equity from one share observation and two closes.

    ``report_px`` is the price observation at the share count's **filed** date and
    is what supplies ``f(report)``. Without it the capital-event carry cannot be
    computed, and rather than assume no split occurred the result is returned as
    ``NO_CAPITAL_FACTOR`` — assuming would be exactly the plausible-but-wrong
    number Stage 25 warned about.
    """
    if not shares_rec:
        return {"ok": False, "disposition": NO_SHARES}
    if not formation_px:
        return {"ok": False, "disposition": NO_PRICE}
    if not report_px or not report_px.get("capital_factor"):
        return {"ok": False, "disposition": NO_FACTOR}
    f_form = float(formation_px["capital_factor"])
    f_rep = float(report_px["capital_factor"])
    if f_rep <= 0 or f_form <= 0:
        return {"ok": False, "disposition": NO_FACTOR}
    carry = f_form / f_rep
    shares_at_formation = float(shares_rec["shares"]) * carry
    value = shares_at_formation * float(formation_px["close_none"])
    if not math.isfinite(value) or not (
            MIN_PLAUSIBLE_MARKET_EQUITY <= value <= MAX_PLAUSIBLE_MARKET_EQUITY):
        return {"ok": False, "disposition": IMPLAUSIBLE,
                "market_equity": value if math.isfinite(value) else None,
                "shares_reported": shares_rec["shares"],
                "capital_event_carry": carry}
    return {
        "ok": True, "disposition": OK,
        "market_equity": value,
        "shares_reported": float(shares_rec["shares"]),
        "shares_at_formation": shares_at_formation,
        "capital_event_carry": carry,
        "split_between_report_and_formation": abs(carry - 1.0) > 1e-6,
        "share_concept": shares_rec.get("concept"),
        "shares_filed": shares_rec.get("filed"),
        "shares_age_days": shares_rec.get("age_days"),
        "shares_stale": (shares_rec.get("age_days") is not None
                         and shares_rec["age_days"] > STALE_SHARES_DAYS),
        "price_date": formation_px.get("date"),
        "close_unadjusted": formation_px.get("close_none"),
    }


class PitMarketEquity:
    """Compose share counts and the unadjusted price surface into market equity."""

    def __init__(self, shares: "PitShareCounts", prices: "UnadjustedPriceSurface"):
        self.shares = shares
        self.prices = prices

    def at(self, *, symbol: str, cik: str, as_of: str) -> dict:
        rec = self.shares.shares_as_of(cik, as_of)
        formation_px = self.prices.closes_as_of(symbol, as_of)
        report_px = (self.prices.closes_as_of(symbol, rec["filed"])
                     if rec and rec.get("filed") else None)
        out = market_equity(shares_rec=rec, formation_px=formation_px,
                            report_px=report_px)
        out["symbol"] = symbol
        out["cik"] = cik
        out["as_of"] = as_of
        return out

    def capability(self) -> dict:
        return {
            "contract_version": CONTRACT_VERSION,
            "business_concept": "PIT market equity at the formation date",
            "explicitly_not": [
                "today's shares x historical adjusted price",
                "today's market cap projected backward",
                "TOTALRETURN-adjusted price x as-reported shares",
            ],
            "share_source": self.shares.load_status,
            "price_source": self.prices.load_status,
            "share_concepts_in_preference_order": list(SHARE_CONCEPTS),
            "capital_event_carry": (
                "shares(formation) = shares(report) * f(formation)/f(report), "
                "f(t) = close_capital(t)/close_none(t); a split between the "
                "report date and the formation date is carried exactly, never "
                "assumed away"),
            "point_in_time_rule": "share count used only if FILED on or before "
                                  "the formation date",
            "missing_policy": "missing stays missing; never defaulted, never "
                              "zero-filled, never carried from a later filing",
            "plausibility_envelope_usd": [MIN_PLAUSIBLE_MARKET_EQUITY,
                                          MAX_PLAUSIBLE_MARKET_EQUITY],
        }


def coverage_report(results: "Sequence[dict]") -> dict:
    """Coverage / quality summary over many :meth:`PitMarketEquity.at` results."""
    total = len(results)
    if not total:
        return {"attempts": 0, "resolved": 0, "coverage_fraction": 0.0}
    by_disp: "dict[str, int]" = {}
    for r in results:
        d = r.get("disposition") or "UNKNOWN"
        by_disp[d] = by_disp.get(d, 0) + 1
    ok = [r for r in results if r.get("ok")]
    by_concept: "dict[str, int]" = {}
    stale = 0
    split_carried = 0
    ages: list = []
    for r in ok:
        c = r.get("share_concept") or "?"
        by_concept[c] = by_concept.get(c, 0) + 1
        if r.get("shares_stale"):
            stale += 1
        if r.get("split_between_report_and_formation"):
            split_carried += 1
        if r.get("shares_age_days") is not None:
            ages.append(int(r["shares_age_days"]))
    ages.sort()
    return {
        "attempts": total,
        "resolved": len(ok),
        "coverage_fraction": round(len(ok) / total, 6),
        "by_disposition": dict(sorted(by_disp.items(), key=lambda kv: -kv[1])),
        "by_share_concept": dict(sorted(by_concept.items(), key=lambda kv: -kv[1])),
        "stale_share_count_rows": stale,
        "stale_threshold_days": STALE_SHARES_DAYS,
        "split_carried_rows": split_carried,
        "share_age_days_median": ages[len(ages) // 2] if ages else None,
        "share_age_days_p90": ages[int(0.9 * (len(ages) - 1))] if ages else None,
    }


__all__ = [
    "CONTRACT_VERSION", "SHARE_CONCEPTS", "SHARE_TAGS",
    "MIN_PLAUSIBLE_MARKET_EQUITY", "MAX_PLAUSIBLE_MARKET_EQUITY",
    "STALE_SHARES_DAYS", "OK", "NO_SHARES", "NO_PRICE", "NO_FACTOR",
    "IMPLAUSIBLE", "PitShareCounts", "UnadjustedPriceSurface", "market_equity",
    "PitMarketEquity", "coverage_report",
]
