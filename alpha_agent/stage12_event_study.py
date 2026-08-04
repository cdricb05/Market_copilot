"""Stage 12 TRUE event-time evaluator (BLOCKER 2 correction).

The prior event builder (``stage12_events.build_event_periods``) required >= 20
comparable names on the SAME calendar formation date. SEC filings are
ASYNCHRONOUS -- issuers report on their own fiscal cadence -- so a same-day
cross-section is almost always thin, and the calendar builder returned 0 scored
periods even though 924k companyfacts facts across 846 issuers exist.

This module evaluates each fundamental-change signal in EVENT TIME:

* Event identity: every periodic SEC filing (10-K / 10-Q / 20-F / 40-F, with the
  amendment status tracked) is one event, keyed by (cik, accession, form, filed,
  period_end, fiscal_key). The filing calendar is read from the durable
  ``cf_fact`` index (``SecCompanyFactsIndex.facts_for_cik``), which -- unlike the
  in-memory ``PitObservation`` -- retains the accession and form.
* Entry: the FIRST trading session STRICTLY AFTER the ``filed`` availability date
  (>= 1 full session lag; companyfacts is day-precision so we never trade the
  session that could already reflect the filing) -- ``stage12_execution``.
* Forward horizons: 5 / 20 / 63 trading days from the lagged entry close.
* Primary cross-sectional unit: the MONTHLY event COHORT (all events whose entry
  falls in a calendar month); weekly cohorts only when coverage supports it. A
  cohort's cross-sectional rank-IC between the event signal and the forward
  return is one period observation; the series of cohort ICs flows through the
  UNCHANGED ``signal_evaluation.evaluate_periods`` gate battery (purged folds,
  block bootstrap, subperiod/regime), and an OVERLAP-aware / issuer-clustered
  effective-N adjustment (``stage12_power``) is reported alongside.
* Restatement isolation: the primary event set is ORIGINAL filings; every signal
  value is read ``as_of = filed`` so a later 10-K/A (filed after the event) is
  excluded by the availability boundary -- no future-restatement leakage.

Owned data still gates honestly: too few events / issuers / cohorts -> DATA_HOLD
(never a fabricated cohort).
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from . import pit_fundamentals as _pf
    from . import signal_library as _sl
    from . import stage12_events as _events
    from . import stage12_execution as _exec
    from . import stage12_power as _power
except Exception:  # pragma: no cover
    import pit_fundamentals as _pf  # type: ignore
    import signal_library as _sl  # type: ignore
    import stage12_events as _events  # type: ignore
    import stage12_execution as _exec  # type: ignore
    import stage12_power as _power  # type: ignore

ANCHOR_CONCEPT = "assets"

# Periodic financial reports that carry the balance-sheet / income concepts. 8-K,
# DEF 14A, etc. are excluded (they do not report the full statement set).
PERIODIC_FORMS = {"10-K", "10-Q", "10-KT", "10-QT", "20-F", "40-F"}
_AMENDMENT_FORMS = {f + "/A" for f in PERIODIC_FORMS}
# A periodic filing must carry this us-gaap tag to count as a real report.
_REQUIRED_TAG = "Assets"

DEFAULT_HORIZONS = (5, 20, 63)
_MONTH_STEP_TRADING_DAYS = 21  # ~ trading days per monthly cohort step

# Event-study adequacy gates (config can override via
# autonomy.event_min_issuers / event_min_events).
MIN_COHORTS = 12
MIN_ISSUERS = 30
MIN_EVENTS = 30
MIN_NAMES_PER_COHORT = 8


# --------------------------------------------------------------------------- #
# Filing calendar (event identity) from the durable cf_fact index.
# --------------------------------------------------------------------------- #
def build_filing_calendar(cf_index: Any, ciks: Iterable[str], *,
                          include_amendments: bool = False) -> Dict[str, Any]:
    """Per-CIK list of periodic filing events read from ``cf_fact``.

    Returns ``{"calendar": {cik: [event, ...]}, "stats": {...}}`` where each
    ``event`` is ``{filed, form, accession, period_end, fiscal_key,
    is_amendment}``. Original filings only unless ``include_amendments``.
    """
    calendar: Dict[str, List[Dict[str, Any]]] = {}
    n_events = 0
    n_amend = 0
    for cik in {str(c) for c in ciks if c}:
        try:
            rows = cf_index.facts_for_cik(cik)
        except Exception:  # noqa: BLE001 - a locked/partial index degrades to empty
            rows = []
        # group facts by accession -> one filing event
        by_accn: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            form = str(r.get("form") or "")
            is_amend = form.endswith("/A")
            base_ok = form in PERIODIC_FORMS or (is_amend and form in _AMENDMENT_FORMS)
            if not base_ok:
                continue
            accn = r.get("accession")
            filed = r.get("filed")
            if not accn or not filed:
                continue
            ev = by_accn.setdefault(accn, {
                "filed": str(filed)[:10], "form": form, "accession": accn,
                "is_amendment": is_amend, "tags": set(),
                "period_end": None, "fy": None, "fp": None})
            ev["tags"].add(str(r.get("concept") or ""))
            # the filing's primary period = the fact with the latest period_end
            pe = r.get("period_end")
            if pe and (ev["period_end"] is None or str(pe) > str(ev["period_end"])):
                ev["period_end"] = str(pe)
                ev["fy"] = r.get("fy")
                ev["fp"] = r.get("fp")
        events: List[Dict[str, Any]] = []
        for accn, ev in by_accn.items():
            if _REQUIRED_TAG not in ev["tags"]:
                continue  # not a real financial report
            if ev["is_amendment"]:
                n_amend += 1
                if not include_amendments:
                    continue
            fy, fp = ev.get("fy"), ev.get("fp")
            ev["fiscal_key"] = ("%s-%s" % (fy, fp)) if (fy and fp) else str(ev["period_end"])
            ev.pop("tags", None)
            events.append(ev)
        events.sort(key=lambda e: (e["filed"], e["accession"]))
        if events:
            calendar[cik] = events
            n_events += len(events)
    stats = {"issuers_with_filings": len(calendar), "total_events": n_events,
             "amendment_events_seen": n_amend,
             "include_amendments": bool(include_amendments)}
    return {"calendar": calendar, "stats": stats}


def build_event_scaffold(ctx: Mapping[str, Any], cf_index: Any, *,
                         include_amendments: bool = False, lag: int = _exec.DEFAULT_LAG
                         ) -> Dict[str, Any]:
    """Precompute the horizon-independent event scaffold ONCE per campaign.

    For every (cik, assetid, filing) with a priced series, resolve the strictly-
    comparable fiscal keys and the post-availability entry index. Signal builders
    then only evaluate their feature value; the entry/forward machinery is shared.
    Returns ``{"scaffold": [event...], "calendar_stats": {...}}`` and also stores
    the scaffold on ``ctx['event_scaffold']`` when ``ctx`` is mutable.
    """
    store = ctx.get("store")
    cik_to_assetid = ctx.get("cik_to_assetid") or {}
    close_index = ctx.get("close_index") or {}
    if store is None or not cik_to_assetid or not close_index or cf_index is None:
        return {"scaffold": [], "calendar_stats": {"total_events": 0,
                "issuers_with_filings": 0, "reason": "MISSING_STORE_OR_INDEX"}}
    cal = build_filing_calendar(cf_index, cik_to_assetid.keys(),
                                include_amendments=include_amendments)
    calendar = cal["calendar"]
    scaffold: List[Dict[str, Any]] = []
    for cik, events in calendar.items():
        assetid = cik_to_assetid.get(cik)
        if assetid is None or assetid not in close_index:
            continue
        dates, closes = close_index[assetid][0], close_index[assetid][1]
        for ev in events:
            filed = ev["filed"]
            try:
                fk_now = store.latest_fiscal_key(cik, filed, concept=ANCHOR_CONCEPT)
            except Exception:
                fk_now = None
            if not fk_now:
                continue
            try:
                fk_prior = store.prior_fiscal_key(cik, filed, fk_now,
                                                  concept=ANCHOR_CONCEPT)
            except Exception:
                fk_prior = None
            entry = _exec.entry_index_after_availability(dates, filed, lag=lag)
            if entry is None:
                continue
            cohort = str(dates[entry])[:7]  # YYYY-MM of the ENTRY session
            scaffold.append({
                "cik": cik, "assetid": assetid, "filed": filed,
                "form": ev["form"], "accession": ev["accession"],
                "period_end": ev.get("period_end"), "fiscal_key": ev["fiscal_key"],
                "fk_now": fk_now, "fk_prior": fk_prior,
                "is_amendment": ev["is_amendment"],
                "entry_index": entry, "entry_date": str(dates[entry]),
                "cohort": cohort})
    stats = dict(cal["stats"])
    stats["scaffold_events"] = len(scaffold)
    stats["priced_issuers"] = len({e["cik"] for e in scaffold})
    try:
        ctx["event_scaffold"] = scaffold          # type: ignore[index]
        ctx["event_calendar_stats"] = stats        # type: ignore[index]
    except Exception:
        pass
    return {"scaffold": scaffold, "calendar_stats": stats}


def _scaffold(ctx: Mapping[str, Any], cf_index: Any, *,
              include_amendments: bool = False, lag: int = _exec.DEFAULT_LAG
              ) -> List[Dict[str, Any]]:
    sc = ctx.get("event_scaffold")
    if sc is not None:
        return sc
    return build_event_scaffold(ctx, cf_index, include_amendments=include_amendments,
                                lag=lag)["scaffold"]


# --------------------------------------------------------------------------- #
# Event-time cohort periods for one signal.
# --------------------------------------------------------------------------- #
def is_event_study_builder(builder_key: str) -> bool:
    return builder_key in _events.EVENT_BUILDERS


def build_event_cohort_periods(builder_key: str, ctx: Mapping[str, Any], *,
                               cf_index: Any = None, direction: int = 1,
                               horizon_days: int = 63, winsor: float = 0.02,
                               min_names_for_cohort: int = 3,
                               include_amendments: bool = False,
                               lag: int = _exec.DEFAULT_LAG,
                               min_cohorts: int = MIN_COHORTS,
                               min_issuers: int = MIN_ISSUERS,
                               min_events: int = MIN_EVENTS,
                               min_names_per_cohort: int = MIN_NAMES_PER_COHORT
                               ) -> Dict[str, Any]:
    """Build monthly event cohorts for one fundamental-change signal.

    Each event's signal is computed ``as_of = filed`` (no future restatement
    leaks in); each event's forward return enters the session AFTER ``filed``.
    Events are grouped into monthly cohorts (one observation per issuer per
    cohort, latest filing wins). Returns the ``periods`` the unchanged evaluator
    scores plus event/coverage counts and the overlap diagnostic.
    """
    fn = _events.EVENT_BUILDERS.get(builder_key)
    store = ctx.get("store")
    close_index = ctx.get("close_index") or {}
    pit = {"design": "event_time_monthly_cohort", "no_lookahead": True,
           "availability": "SEC filed date (day precision)",
           "entry": "first trading session strictly after filed (>=1 session lag)",
           "restatement": "value read as_of=filed; later amendments excluded by boundary",
           "horizon_days": int(horizon_days), "execution_lag": max(1, int(lag)),
           "cross_sectional_unit": "monthly_event_cohort"}
    if fn is None:
        return _empty(pit, "UNKNOWN_EVENT_BUILDER:%s" % builder_key)
    if store is None:
        return _empty(pit, "DATA_HOLD_NO_PIT_FUNDAMENTALS_STORE")
    scaffold = _scaffold(ctx, cf_index, include_amendments=include_amendments, lag=lag)
    if not scaffold:
        return _empty(pit, "DATA_HOLD_NO_FILING_CALENDAR")

    sign = int(direction) or 1
    # cohort -> {assetid: (filed, signal, fwd)} keeping the latest filing per issuer
    cohorts: Dict[str, Dict[Any, Tuple[str, float, float]]] = {}
    total_events = 0
    issuers: set = set()
    per_year: Dict[str, int] = {}
    for ev in scaffold:
        assetid = ev["assetid"]
        idx = close_index.get(assetid)
        if idx is None:
            continue
        try:
            val = fn(store, ev["cik"], ev["fk_now"], ev["fk_prior"], ev["filed"])
        except Exception:
            val = None
        if val is None:
            continue
        fwd = _exec.forward_return_from_entry(idx[1], ev["entry_index"], horizon_days)
        if fwd is None:
            continue
        total_events += 1
        issuers.add(ev["cik"])
        yr = ev["cohort"][:4]
        per_year[yr] = per_year.get(yr, 0) + 1
        bucket = cohorts.setdefault(ev["cohort"], {})
        prev = bucket.get(assetid)
        if prev is None or ev["filed"] >= prev[0]:
            bucket[assetid] = (ev["filed"], float(val), float(fwd))

    periods: List[Dict[str, Any]] = []
    names_counts: List[int] = []
    for cohort in sorted(cohorts):
        raw = {aid: v[1] for aid, v in cohorts[cohort].items()}
        if len(raw) < min_names_for_cohort:
            continue
        raw = _sl.winsorize(raw, winsor)
        names = [(aid, float(raw[aid]) * sign, float(cohorts[cohort][aid][2]))
                 for aid in raw]
        if len(names) >= min_names_for_cohort:
            periods.append({"as_of": cohort, "names": names})
            names_counts.append(len(names))

    median_names = sorted(names_counts)[len(names_counts) // 2] if names_counts else 0
    coverage = {
        "design": "event_time_monthly_cohort",
        "n_cohorts": len(periods), "total_events": total_events,
        "distinct_issuers": len(issuers), "median_cohort_names": median_names,
        "min_cohort_names": min(names_counts) if names_counts else 0,
        "max_cohort_names": max(names_counts) if names_counts else 0,
        "events_by_year": dict(sorted(per_year.items())),
        "cohort_span": ((periods[0]["as_of"], periods[-1]["as_of"]) if periods else None),
    }
    data_hold_reason = _cohort_data_hold(coverage, min_cohorts, min_issuers,
                                         min_events, min_names_per_cohort)
    overlap = overlap_diagnostics([_cohort_ic(p) for p in periods], horizon_days)
    return {"periods": periods, "coverage": coverage, "pit": pit,
            "data_hold_reason": data_hold_reason, "overlap": overlap,
            "event_counts": {"total_events": total_events,
                             "distinct_issuers": len(issuers),
                             "events_by_year": coverage["events_by_year"]}}


def _empty(pit: dict, reason: str) -> Dict[str, Any]:
    return {"periods": [], "coverage": {"n_cohorts": 0, "total_events": 0,
            "distinct_issuers": 0}, "pit": pit, "data_hold_reason": reason,
            "overlap": {}, "event_counts": {"total_events": 0, "distinct_issuers": 0}}


def _cohort_data_hold(cov: Mapping[str, Any], min_cohorts: int, min_issuers: int,
                      min_events: int, min_names: int) -> Optional[str]:
    if cov["n_cohorts"] < min_cohorts:
        return "DATA_HOLD_INSUFFICIENT_COHORTS(%d<%d)" % (cov["n_cohorts"], min_cohorts)
    if cov["distinct_issuers"] < min_issuers:
        return "DATA_HOLD_INSUFFICIENT_ISSUERS(%d<%d)" % (cov["distinct_issuers"], min_issuers)
    if cov["total_events"] < min_events:
        return "DATA_HOLD_INSUFFICIENT_EVENTS(%d<%d)" % (cov["total_events"], min_events)
    if cov["median_cohort_names"] < min_names:
        return "DATA_HOLD_THIN_COHORTS(median=%d<%d)" % (cov["median_cohort_names"], min_names)
    return None


def _cohort_ic(period: Mapping[str, Any]) -> Optional[float]:
    names = period.get("names") or []
    if len(names) < 3:
        return None
    try:
        from . import fundamental_evidence as _fev
    except Exception:  # pragma: no cover
        import fundamental_evidence as _fev  # type: ignore
    return _fev._spearman([t[1] for t in names], [t[2] for t in names])


def overlap_diagnostics(ic_series: Sequence[Optional[float]], horizon_days: int, *,
                        cohort_step_days: int = _MONTH_STEP_TRADING_DAYS
                        ) -> Dict[str, Any]:
    """Overlap-aware / autocorrelation-robust effective-N for the cohort IC series.

    Monthly cohorts step ~21 trading days; a 63-day horizon overlaps ~2 cohorts,
    so the nominal cohort count overstates independent information. Reports both
    the analytic overlap bound and the Newey-West effective N + clustered t.
    """
    clean = [v for v in ic_series if v is not None]
    n = len(clean)
    analytic = _power.overlap_effective_n(n, horizon_days, cohort_step_days)
    nw = _power.clustered_t_stat(clean) if n >= 3 else {"t_naive": None,
                                                        "t_clustered": None,
                                                        "effective_n": float(n)}
    return {"n_cohorts": n,
            "analytic_overlap_effective_n": analytic.get("effective_n"),
            "overlap_fraction": analytic.get("overlap_fraction"),
            "newey_west_effective_n": nw.get("effective_n"),
            "t_naive": nw.get("t_naive"), "t_clustered": nw.get("t_clustered"),
            "variance_inflation": nw.get("variance_inflation")}


# --------------------------------------------------------------------------- #
# Event counts by family / year (reported BEFORE testing).
# --------------------------------------------------------------------------- #
def event_counts_by_family_year(builder_keys: Sequence[str], ctx: Mapping[str, Any],
                                *, cf_index: Any = None, horizon_days: int = 63,
                                include_amendments: bool = False,
                                lag: int = _exec.DEFAULT_LAG) -> Dict[str, Any]:
    """For each event family, the count of measurable events / issuers / cohorts
    by calendar year -- so the coverage of the study is stated before any test."""
    scaffold = _scaffold(ctx, cf_index, include_amendments=include_amendments, lag=lag)
    store = ctx.get("store")
    close_index = ctx.get("close_index") or {}
    out: Dict[str, Any] = {}
    total_years: Dict[str, int] = {}
    for ev in scaffold:
        total_years[ev["cohort"][:4]] = total_years.get(ev["cohort"][:4], 0) + 1
    for bk in builder_keys:
        fn = _events.EVENT_BUILDERS.get(bk)
        if fn is None or store is None:
            out[bk] = {"total_events": 0, "distinct_issuers": 0, "by_year": {}}
            continue
        by_year: Dict[str, int] = {}
        issuers: set = set()
        cohorts: set = set()
        total = 0
        for ev in scaffold:
            idx = close_index.get(ev["assetid"])
            if idx is None:
                continue
            try:
                val = fn(store, ev["cik"], ev["fk_now"], ev["fk_prior"], ev["filed"])
            except Exception:
                val = None
            if val is None:
                continue
            fwd = _exec.forward_return_from_entry(idx[1], ev["entry_index"], horizon_days)
            if fwd is None:
                continue
            total += 1
            issuers.add(ev["cik"])
            cohorts.add(ev["cohort"])
            by_year[ev["cohort"][:4]] = by_year.get(ev["cohort"][:4], 0) + 1
        out[bk] = {"total_events": total, "distinct_issuers": len(issuers),
                   "n_cohorts": len(cohorts), "by_year": dict(sorted(by_year.items()))}
    return {"scaffold_events": len(scaffold),
            "scaffold_events_by_year": dict(sorted(total_years.items())),
            "per_family": out}
