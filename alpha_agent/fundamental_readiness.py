"""
alpha_agent.fundamental_readiness - Stage 9.5 RELEASE CLOSURE: the survivorship-
safe HISTORICAL readiness contract that separates SAFE ACQUISITION (Stage 9.5A)
from HISTORICAL FUNDAMENTAL EVALUATION (Stage 9.5B).

The Stage 9.5 fundamental EVALUATION path must never run a survivorship-biased
historical backtest merely because enough CURRENT SEC constituents have owned
companyfacts. The free ``company_tickers.json`` map resolves CURRENT entities
only, so the current 503 constituents are NOT a valid historical backtest
universe (they exclude every delisted name that existed on a past rebalance
date). Until an owned, survivorship-safe HISTORICAL ticker->CIK mapping exists,
Stage 9.5B is DEFERRED (release-model OPTION B): the current-universe acquisition
campaign keeps collecting, the collected facts are labelled acquisition /
forward-evidence only, and every fundamental candidate stays DATA_HOLD behind the
single explicit diagnostic ``HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY``.

This module supplies:
  * ``historical_mapping_status`` - whether an owned survivorship-safe historical
    ticker->CIK mapping is available (OPTION B => False, honestly);
  * ``per_rebalance_readiness`` - the PER-DATE historical readiness contract
    (names in the survivorship-safe universe on each date, names with the
    required PIT facts, coverage %, median/min eligible names, valid scored
    periods, earliest/latest usable formation date, missing fiscal comparisons,
    unsupported units, historical mapping coverage) replacing aggregate-CIK-only
    readiness. Current-universe aggregate counts alone can NEVER mark a candidate
    sufficient;
  * ``historical_fundamental_experiment_allowed`` - the SAFETY SWITCH: a Stage
    9.5B historical experiment may be generated/executed ONLY when survivorship-
    safe mapping AND per-rebalance readiness AND the config flag all pass.

Pure stdlib; deterministic; no network, no state writes, no operational mutation.
"""
from __future__ import annotations

from typing import Optional

from . import fundamental_signals as _fsig
from . import pit_fundamentals as _pfd

# The single explicit diagnostic (Blocker 7). Emitted whenever a Stage 9.5B
# historical fundamental experiment is refused for lack of a survivorship-safe
# historical universe / per-rebalance readiness.
HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY = \
    "HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY"

# Per-rebalance readiness thresholds (Blocker 2). All must pass for eligibility.
_DEFAULT_THRESHOLDS = {
    "min_eligible_names_per_rebalance": 20,
    "min_coverage_pct": 60.0,
    "min_scored_periods": 12,
    "min_subperiod_coverage": 0.60,
    "min_historical_mapping_coverage_pct": 60.0,
}


def _median(xs) -> Optional[float]:
    xs = sorted(float(x) for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    m = n // 2
    return xs[m] if n % 2 else (xs[m - 1] + xs[m]) / 2.0


def readiness_thresholds(cfg: dict) -> dict:
    """The configured per-rebalance readiness thresholds (Stage 9.5 block),
    falling back to the deterministic defaults."""
    thr = dict(_DEFAULT_THRESHOLDS)
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    over = s95.get("per_rebalance_readiness") or {}
    for k in thr:
        if over.get(k) is not None:
            thr[k] = over[k]
    return thr


def historical_mapping_status(cfg: dict) -> dict:
    """Whether an owned SURVIVORSHIP-SAFE historical ticker->CIK mapping exists.

    Under the Stage 9.5 release model this is OPTION B (deferred): no such mapping
    is owned (``company_tickers.json`` is current-only), so ``available`` is False
    unless a config-declared mapping artifact is explicitly present AND enabled.
    A current-only map is NEVER treated as a historical mapping."""
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    hu = s95.get("historical_universe") or {}
    available = bool(hu.get("mapping_available"))
    return {
        "available": available,
        "source": hu.get("mapping_source"),
        "mapping_version_hash": hu.get("mapping_version_hash"),
        "current_only_map_is_not_historical": True,
        "reason": None if available else (
            "no owned survivorship-safe historical ticker->CIK mapping (SEC "
            "company_tickers.json resolves CURRENT entities only); the current "
            "constituents are not a valid historical backtest universe - Stage "
            "9.5B historical fundamental evaluation is deferred (OPTION B)"),
    }


def _names_with_facts(store, signal: str, ciks, as_of: str):
    """(count computable, count missing-fiscal-comparison) for ``signal`` as-of
    ``as_of`` over ``ciks`` - leakage-safe (only facts FILED on or before as_of).
    A CIK counts as computable when ``compute_signal`` yields a non-None value."""
    computable = 0
    missing_fiscal = 0
    if store is None:
        return 0, 0
    for cik in ciks:
        fk = store.latest_fiscal_key(cik, as_of, concept="assets")
        if fk is None:
            continue
        prior = None
        if signal == "asset_growth":
            prior = store.prior_fiscal_key(cik, as_of, fk, concept="assets")
            if prior is None:
                missing_fiscal += 1
                continue
        r = _fsig.compute_signal(store, signal, cik=cik, fiscal_key=fk,
                                 as_of=as_of, prior_fiscal_key=prior)
        if r.get("value") is not None:
            computable += 1
    return computable, missing_fiscal


def per_rebalance_readiness(store, signal: str, *, rebalance_dates,
                            historical_universe_by_date: Optional[dict] = None,
                            cik_to_ticker_by_date: Optional[dict] = None,
                            mapping_available: bool = False,
                            thresholds: Optional[dict] = None) -> dict:
    """PER-REBALANCE-DATE historical readiness for one fundamental candidate.

    Replaces aggregate-CIK-only readiness (Blocker 2). For each historical
    rebalance date it measures the SURVIVORSHIP-SAFE universe as it stood then,
    the names mapped to a CIK, the names with the required PIT facts, coverage %
    and historical mapping coverage %. A candidate is eligible ONLY when every
    configured threshold passes AND a survivorship-safe historical mapping is
    available - current-universe aggregate counts alone can NEVER mark it
    sufficient. Under OPTION B (no historical mapping) the inputs are empty, so
    every date reports zero eligible names and the gate returns
    ``HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY``."""
    thr = dict(_DEFAULT_THRESHOLDS)
    thr.update(thresholds or {})
    huniv = historical_universe_by_date or {}
    c2t_by_date = cik_to_ticker_by_date or {}
    dates = sorted(rebalance_dates or [])
    required = _pfd.CANDIDATE_CONCEPTS.get(signal, [])
    min_eligible = int(thr["min_eligible_names_per_rebalance"])

    per_date: list[dict] = []
    for d in dates:
        univ = set(huniv.get(d) or [])
        c2t = c2t_by_date.get(d) or {}
        mapped = len(c2t)
        with_facts, missing_fiscal = _names_with_facts(store, signal,
                                                       list(c2t.keys()), d)
        coverage_pct = (100.0 * with_facts / len(univ)) if univ else 0.0
        mapping_cov = (100.0 * mapped / len(univ)) if univ else 0.0
        per_date.append({
            "as_of": d,
            "universe_names": len(univ),
            "mapped_names": mapped,
            "names_with_facts": with_facts,
            "coverage_pct": round(coverage_pct, 4),
            "historical_mapping_coverage_pct": round(mapping_cov, 4),
            "missing_fiscal_comparisons": missing_fiscal,
            "unsupported_units": 0,   # only USD monetary facts are ingested
            "eligible": with_facts >= min_eligible,
        })

    eligible_dates = [p for p in per_date if p["eligible"]]
    valid_scored_periods = len(eligible_dates)
    names_series = [p["names_with_facts"] for p in per_date]
    coverage_series = [p["coverage_pct"] for p in per_date]
    mapping_series = [p["historical_mapping_coverage_pct"] for p in per_date]
    subperiod_coverage = (valid_scored_periods / len(per_date)) if per_date \
        else 0.0
    earliest = next((p["as_of"] for p in per_date if p["eligible"]), None)
    latest = next((p["as_of"] for p in reversed(per_date) if p["eligible"]),
                  None)

    median_cov = _median(coverage_series) or 0.0
    median_map = _median(mapping_series) or 0.0
    min_eligible_names = min(names_series) if names_series else 0

    # The gate: EVERY threshold must pass AND a survivorship-safe historical
    # mapping must be available. Mapping availability dominates so no volume of
    # current-only coverage can unlock a survivorship-biased historical backtest.
    failed_gate = None
    if not mapping_available:
        failed_gate = HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    elif valid_scored_periods < int(thr["min_scored_periods"]):
        failed_gate = "INSUFFICIENT_SCORED_PERIODS"
    elif median_cov < float(thr["min_coverage_pct"]):
        failed_gate = "INSUFFICIENT_COVERAGE_PCT"
    elif subperiod_coverage < float(thr["min_subperiod_coverage"]):
        failed_gate = "INSUFFICIENT_SUBPERIOD_COVERAGE"
    elif median_map < float(thr["min_historical_mapping_coverage_pct"]):
        failed_gate = "INSUFFICIENT_HISTORICAL_MAPPING_COVERAGE"
    elif min_eligible_names < min_eligible and valid_scored_periods < len(
            per_date):
        # at least the eligible span must clear the per-rebalance name floor
        failed_gate = "INSUFFICIENT_MIN_ELIGIBLE_NAMES"

    sufficient = bool(mapping_available) and failed_gate is None
    return {
        "signal": signal,
        "sufficient": sufficient,
        "blocker": failed_gate,
        "mapping_available": bool(mapping_available),
        "required_concepts": required,
        "rebalance_dates_requested": len(dates),
        "valid_scored_periods": valid_scored_periods,
        "median_eligible_names": _median(names_series) or 0,
        "min_eligible_names": min_eligible_names,
        "median_coverage_pct": round(median_cov, 4),
        "median_historical_mapping_coverage_pct": round(median_map, 4),
        "subperiod_coverage": round(subperiod_coverage, 4),
        "earliest_usable_formation_date": earliest,
        "latest_usable_formation_date": latest,
        "thresholds": thr,
        "per_date": per_date,
    }


def historical_mapping_status_from_store(store, cfg: dict) -> dict:
    """Stage 10 MEASURED mapping status. Derives whether an owned survivorship-
    safe historical security->CIK mapping is available FROM THE MEASURED identity
    store coverage (the latest append-only coverage snapshot), NOT from a hand-set
    config flag. ``available`` becomes True ONLY when the measured historical
    mapping coverage over the SURVIVORSHIP-SAFE universe (current + delisted) is
    at least ``min_historical_mapping_coverage_pct``. Because that denominator
    includes every delisted name, aggregate current-CIK coverage alone can never
    satisfy it — the survivorship gap keeps it honest. No side effects; the
    safety switch is never flipped here."""
    thr = readiness_thresholds(cfg)
    required = float(thr["min_historical_mapping_coverage_pct"])
    snap = None
    try:
        snap = store.latest_coverage_snapshot() if store is not None else None
    except Exception:  # noqa: BLE001 - measurement is best-effort, never fatal
        snap = None
    if not snap:
        return {"available": False, "source": "measured_identity_store",
                "measured_coverage_pct": None, "required_coverage_pct": required,
                "current_only_map_is_not_historical": True,
                "reason": ("no measured historical identity coverage snapshot "
                           "yet; run the Stage 10 identity coverage job")}
    by_date = snap.get("by_date") or {}
    # The measured coverage is the MEDIAN mapping coverage across the requested
    # rebalance dates (a single lucky date can never unlock it); absent any
    # rebalance date it falls back to the store-wide resolved fraction.
    covs = sorted(float(v.get("mapping_coverage_pct", 0.0))
                  for v in by_date.values())
    if covs:
        n = len(covs)
        measured = covs[n // 2] if n % 2 else (covs[n // 2 - 1] +
                                               covs[n // 2]) / 2.0
    else:
        total = int(snap.get("total_securities") or 0)
        measured = (100.0 * int(snap.get("mapped_ciks") or 0) / total) \
            if total else 0.0
    available = measured >= required
    return {"available": available, "source": "measured_identity_store",
            "measured_coverage_pct": round(measured, 4),
            "required_coverage_pct": required,
            "current_only_map_is_not_historical": True,
            "mapping_version_hash": snap.get("mapping_version"),
            "reason": None if available else (
                "measured historical mapping coverage %.2f%% < required %.2f%% "
                "over the survivorship-safe universe (current + delisted); "
                "aggregate current-CIK coverage can never satisfy this"
                % (measured, required))}


def open_identity_store(cfg: dict):
    """Open the Stage 10 canonical identity store from the path declared in the
    stage9 config (``stage9_5.historical_universe.identity_store_db``), or return
    None when Stage 10 is not configured. Lazy import so importing this module
    never pulls the identity layer. Read-only w.r.t. operational state."""
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    hu = s95.get("historical_universe") or {}
    db = hu.get("identity_store_db")
    if not db:
        return None
    try:
        from . import historical_identity as _hi
        return _hi.IdentityStore(db)
    except Exception:  # noqa: BLE001 - store access never fatal
        return None


def measured_readiness_from_store(store, signal: str, cfg: dict, *,
                                  pit_store=None,
                                  rebalance_dates: Optional[list] = None,
                                  index_name: str = "S&P 500") -> dict:
    """Stage 10 MEASURED per-rebalance readiness for one fundamental signal,
    built from the survivorship-safe identity store: the historical universe on
    each rebalance date and the RESOLVED security->CIK map on that date feed the
    existing ``per_rebalance_readiness`` contract. ``pit_store`` (the owned PIT
    companyfacts store) supplies the fact coverage; absent it, fact coverage is
    zero so readiness stays insufficient (correct — all requirements must pass).
    Mapping availability is the MEASURED store coverage, never the config flag."""
    thr = readiness_thresholds(cfg)
    mapping = historical_mapping_status_from_store(store, cfg)
    dates = sorted(rebalance_dates or _default_rebalance_dates(cfg))
    huniv: dict = {}
    c2t: dict = {}
    try:
        for d in dates:
            univ = store.historical_universe_on(d, index_name=index_name)
            huniv[d] = [u["security_id"] for u in univ]
            c2t[d] = {u["cik"]: u.get("ticker_effective") or u.get("ticker")
                      for u in univ
                      if u.get("cik") and u.get("mapping_status") == "RESOLVED"}
    except Exception:  # noqa: BLE001 - measurement never fatal
        huniv, c2t = {}, {}
    return per_rebalance_readiness(
        pit_store, signal, rebalance_dates=dates,
        historical_universe_by_date=huniv, cik_to_ticker_by_date=c2t,
        mapping_available=bool(mapping["available"]), thresholds=thr)


def _default_rebalance_dates(cfg: dict) -> list:
    """Deterministic quarterly rebalance dates from the configured window, else a
    conservative built-in span. Used only for MEASUREMENT (read-only)."""
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    hu = s95.get("historical_universe") or {}
    dates = hu.get("rebalance_dates")
    if isinstance(dates, list) and dates:
        return [str(d) for d in dates]
    out = []
    for y in range(2015, 2025):
        for mm in ("03-31", "06-30", "09-30", "12-31"):
            out.append("%d-%s" % (y, mm))
    return out


def historical_fundamental_experiment_allowed(cfg: dict,
                                              readiness: Optional[dict] = None,
                                              *, store=None,
                                              signal: str = "gross_profitability"
                                              ) -> dict:
    """SAFETY SWITCH (Blocker 7). A Stage 9.5B historical fundamental experiment
    may be generated/executed ONLY when ALL of:
      * ``stage9_5.fundamental_experiments.historical_evaluation_enabled`` is true;
      * an owned survivorship-safe historical ticker->CIK mapping is available;
      * the per-rebalance readiness has been MEASURED sufficient.
    Otherwise ``allowed`` is False and the single diagnostic
    ``HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY`` is returned - zero jobs, all
    candidates stay DATA_HOLD, no transition on current-survivor results.

    Stage 10: when a measured identity ``store`` is supplied, mapping
    availability AND readiness are derived from the MEASURED store coverage
    (never the hand-set config flag) — the honest, automatic unlock path. When
    ``store`` is None the legacy config-flag behaviour is preserved exactly
    (backward compatible). Either way the diagnostic stays until every configured
    requirement genuinely passes; the switch is never flipped by Claude."""
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    fx = s95.get("fundamental_experiments") or {}
    enabled = bool(fx.get("historical_evaluation_enabled"))
    if store is not None:
        mapping = historical_mapping_status_from_store(store, cfg)
        if readiness is None:
            readiness = measured_readiness_from_store(store, signal, cfg)
    else:
        mapping = historical_mapping_status(cfg)
    ready_ok = bool(readiness.get("sufficient")) if readiness else False
    allowed = bool(enabled and mapping["available"] and ready_ok)
    reason = None
    if not allowed:
        if not enabled:
            reason = ("stage9_5.fundamental_experiments."
                      "historical_evaluation_enabled is false (OPTION B defer)")
        elif not mapping["available"]:
            reason = mapping["reason"]
        elif not ready_ok:
            reason = ("per-rebalance readiness not yet sufficient on the "
                      "survivorship-safe historical universe")
    return {
        "allowed": allowed,
        "historical_evaluation_enabled": enabled,
        "mapping_available": mapping["available"],
        "readiness_sufficient": ready_ok,
        "diagnostic": None if allowed
        else HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY,
        "reason": reason,
    }


def evaluate_candidate_readiness(store, signal: str, cfg: dict, *,
                                 rebalance_dates: Optional[list] = None,
                                 historical_universe_by_date: Optional[dict]
                                 = None,
                                 cik_to_ticker_by_date: Optional[dict] = None
                                 ) -> dict:
    """Compose ``historical_mapping_status`` + ``per_rebalance_readiness`` +
    ``historical_fundamental_experiment_allowed`` into ONE honest readiness report
    for a fundamental candidate. Under OPTION B (no owned historical mapping) the
    universe/mapping inputs are empty, so the report is NOT_READY with the single
    diagnostic - the current-universe acquisition is unaffected."""
    mapping = historical_mapping_status(cfg)
    thr = readiness_thresholds(cfg)
    readiness = per_rebalance_readiness(
        store, signal, rebalance_dates=rebalance_dates or [],
        historical_universe_by_date=historical_universe_by_date,
        cik_to_ticker_by_date=cik_to_ticker_by_date,
        mapping_available=mapping["available"], thresholds=thr)
    gate = historical_fundamental_experiment_allowed(cfg, readiness)
    return {"signal": signal, "mapping": mapping, "readiness": readiness,
            "experiment_allowed": gate["allowed"],
            "diagnostic": gate["diagnostic"], "reason": gate["reason"]}


__all__ = ["HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY",
           "historical_mapping_status", "historical_mapping_status_from_store",
           "measured_readiness_from_store", "per_rebalance_readiness",
           "historical_fundamental_experiment_allowed",
           "evaluate_candidate_readiness", "readiness_thresholds"]
