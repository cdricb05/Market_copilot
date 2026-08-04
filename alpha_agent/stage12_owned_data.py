"""Stage 12 Workstream B -- owned-data power inventory (read-only).

Inventories the maximum *survivorship-safe, point-in-time-valid* research power
already available from owned data, so Stage 12 can decide -- BEFORE contemplating
any purchase -- whether an economically-distinct, adequately-powered study is
feasible on data we already hold.

Pure functions operate over already-loaded metadata dicts (Stage 11
``inventory.json`` + ``catalogue.json`` + optional live store summaries), so the
core is deterministic and testable. ``run_owned_data_inventory`` is a thin,
guarded wrapper that adds live store counts when a store is supplied.

USE_OWNED_DATA_FIRST. Nothing here purchases, subscribes to, or back-applies
current metadata as historical evidence. EODHD fundamentals and Norgate GICS are
current-only and are explicitly flagged unusable for confirmatory history.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

# Canonical owned-data source descriptors (facts established by the owned-data
# survey; date spans are re-confirmed live from inventory.json when available).
OWNED_SOURCES: List[Dict[str, Any]] = [
    {
        "source": "norgate_local_bars",
        "kind": "price_ohlcv",
        "identity_key": "norgate_assetid",
        "pit_safe": True,
        "survivorship_safe": True,
        "usable_for_confirmatory_history": True,
        "note": "Adjusted total-return close; delisted retained; membership intervals.",
    },
    {
        "source": "norgate_index_membership",
        "kind": "universe_membership",
        "identity_key": "norgate_assetid",
        "pit_safe": True,
        "survivorship_safe": True,
        "usable_for_confirmatory_history": True,
        "note": "S&P 500 current+past membership intervals for survivorship-safe formation universes.",
    },
    {
        "source": "sec_companyfacts_xbrl",
        "kind": "fundamentals_pit",
        "identity_key": "cik",
        "pit_safe": True,
        "survivorship_safe": False,
        "usable_for_confirmatory_history": True,
        "note": ("Each fact PIT via 'filed' availability date; amendments distinct. "
                 "Archive is a CURRENT snapshot of currently-existing filers -> "
                 "issuer-level survivorship gap (deregistered filers may be absent)."),
    },
    {
        "source": "pit_sector_sic",
        "kind": "classification_pit",
        "identity_key": "cik",
        "pit_safe": True,
        "survivorship_safe": True,
        "usable_for_confirmatory_history": True,
        "note": "Contemporaneous SIC from filing headers (sector_as_of); never current GICS/SIC.",
    },
    {
        "source": "eodhd_fundamentals",
        "kind": "fundamentals_snapshot",
        "identity_key": "ticker",
        "pit_safe": False,
        "survivorship_safe": False,
        "usable_for_confirmatory_history": False,
        "note": "Current snapshot, ticker-keyed, no assetid -> excluded from the canonical panel.",
    },
    {
        "source": "norgate_gics_sector",
        "kind": "classification_snapshot",
        "identity_key": "norgate_assetid",
        "pit_safe": False,
        "survivorship_safe": False,
        "usable_for_confirmatory_history": False,
        "note": "Current-only classification -> diagnostic label only, never confirmatory history.",
    },
]

# Event families and the owned concept(s) each needs (for the event-coverage view).
EVENT_FAMILY_REQUIREMENTS: Dict[str, List[str]] = {
    "revenue_change": ["revenue"],
    "gross_margin_change": ["revenue", "cost_of_revenue"],
    "gross_profitability_change": ["gross_profit", "assets"],
    "operating_margin_change": ["operating_income", "revenue"],
    "cash_flow_change": ["cash"],
    "accrual_change": ["net_income", "assets"],
    "asset_growth_inflection": ["assets"],
    "leverage_change": ["liabilities", "assets"],
    "profitability_inflection": ["net_income", "assets"],
    "equity_issuance": ["stockholders_equity"],
}


def _pct(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if not n or not d:
        return None
    try:
        return round(100.0 * float(n) / float(d), 2)
    except (TypeError, ZeroDivisionError):
        return None


def build_owned_data_inventory(inventory: Mapping[str, Any],
                               catalogue: Optional[Mapping[str, Any]] = None,
                               *,
                               identity_counts: Optional[Mapping[str, Any]] = None,
                               pit_coverage: Optional[Mapping[str, Any]] = None,
                               companyfacts_stats: Optional[Mapping[str, Any]] = None,
                               ) -> Dict[str, Any]:
    """Assemble the owned-data capability matrix + breadth + event coverage."""
    catalogue = catalogue or {}
    cov_sig = inventory.get("coverage_signature") or {}
    date_min = cov_sig.get("date_min") or inventory.get("panel_date_start")
    date_max = cov_sig.get("date_max")
    universe = inventory.get("owned_norgate_universe_count")
    cik_to_assetid = inventory.get("cik_to_assetid")
    cik_with_price = inventory.get("cik_with_owned_price_series")
    assetids = inventory.get("materialized_assetids")
    rebalance_dates = inventory.get("rebalance_dates")

    # ---- capability matrix (date span attached to the price spine) ----------
    matrix: List[Dict[str, Any]] = []
    for src in OWNED_SOURCES:
        row = dict(src)
        if src["kind"] in ("price_ohlcv", "universe_membership"):
            row["date_start"] = date_min
            row["date_end"] = date_max
        matrix.append(row)

    # ---- cross-sectional breadth over the owned rebalance dates -------------
    per_rebalance = inventory.get("per_rebalance_coverage") or []
    breadth = _breadth_series(per_rebalance)

    # ---- unavailable-reason taxonomy ----------------------------------------
    breadth_reasons = (inventory.get("unavailable_reason_counts")
                       or _cc_unavailable(inventory))
    data_hold = catalogue.get("data_hold_specs") or []
    hold_by_reason: Dict[str, int] = {}
    for s in data_hold:
        r = s.get("reason", "UNKNOWN")
        hold_by_reason[r] = hold_by_reason.get(r, 0) + 1

    # ---- max defensible history per feature family --------------------------
    # Price families: bounded only by the owned bar span. Fundamental/event
    # families: bounded by the PIT availability window (filed dates).
    pit_avail_start = (pit_coverage or {}).get("availability_start")
    pit_avail_end = (pit_coverage or {}).get("availability_end")
    families_cfg = (catalogue.get("families") or {})
    max_history: Dict[str, Dict[str, Any]] = {}
    for fam in families_cfg:
        is_fundamental = fam in ("profitability_quality", "growth_investment", "valuation")
        max_history[fam] = {
            "max_history_start": (pit_avail_start if is_fundamental else date_min),
            "max_history_end": (pit_avail_end if is_fundamental else date_max),
            "bounded_by": "pit_filed_availability" if is_fundamental else "owned_bar_span",
        }

    # ---- event coverage by family (owned concepts) --------------------------
    owned_concepts = _owned_concepts(pit_coverage, catalogue)
    event_coverage: List[Dict[str, Any]] = []
    for fam, needs in EVENT_FAMILY_REQUIREMENTS.items():
        missing = [c for c in needs if c not in owned_concepts]
        event_coverage.append({
            "event_family": fam,
            "required_concepts": needs,
            "missing_concepts": missing,
            "supported": not missing,
            "reason": None if not missing else "DATA_HOLD_MISSING_CONCEPT:" + ",".join(missing),
        })

    # ---- recommended highest-power owned universe ---------------------------
    recommendation = {
        "price_spine": "norgate_local assetid panel (survivorship-safe, delisted retained)",
        "formation_universe": "historical_identity.historical_universe_on(as_of)",
        "fundamental_join": "PitFundamentalsStore.as_of by SEC filed date",
        "sector_neutralization": "pit_sector.sector_as_of (leakage-safe SIC)",
        "usable_cross_section_estimate": cik_with_price,
        "usable_periods_estimate": rebalance_dates,
        "cross_sectional_coverage_pct_of_universe": _pct(cik_with_price, universe),
        "caveats": [
            "Cross-section (~%s of %s universe names) is survivorship-biased toward "
            "identity-resolved, price-covered names; align-to-subset is FORBIDDEN as "
            "confirmatory evidence." % (cik_with_price, universe),
            "companyfacts is a current snapshot -> issuer-level survivorship gap; "
            "event studies inherit it.",
        ],
    }

    return {
        "stage": "12",
        "workstream": "B_owned_data_power_inventory",
        "read_only": True,
        "owned_data_capability_matrix": matrix,
        "price_span": {"date_start": date_min, "date_end": date_max},
        "identity_resolution": {
            "owned_norgate_universe": universe,
            "cik_to_assetid_resolved": cik_to_assetid,
            "cik_with_owned_price_series": cik_with_price,
            "materialized_assetids": assetids,
            "rebalance_dates": rebalance_dates,
            "coverage_pct_of_universe": _pct(cik_with_price, universe),
            "unavailable_reason_counts": breadth_reasons,
        },
        "cross_sectional_breadth": breadth,
        "max_defensible_history_per_family": max_history,
        "event_coverage": event_coverage,
        "event_families_supported": sum(1 for e in event_coverage if e["supported"]),
        "event_families_total": len(event_coverage),
        "owned_concepts": sorted(owned_concepts),
        "data_hold_by_reason": hold_by_reason,
        "pit_fundamentals_coverage": _slim_pit(pit_coverage),
        "companyfacts_stats": dict(companyfacts_stats) if companyfacts_stats else None,
        "identity_store_counts": dict(identity_counts) if identity_counts else None,
        "recommended_highest_power_universe": recommendation,
    }


def _breadth_series(per_rebalance: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not per_rebalance:
        return {"n_dates": 0, "min_names": None, "max_names": None, "first": None, "last": None}
    def _names(r: Mapping[str, Any]) -> Optional[int]:
        return r.get("price_assetid_overlap") or r.get("mapped_resolved")
    vals = [_names(r) for r in per_rebalance if _names(r) is not None]
    return {
        "n_dates": len(per_rebalance),
        "min_names": min(vals) if vals else None,
        "max_names": max(vals) if vals else None,
        "first": {"as_of": per_rebalance[0].get("as_of"), "names": _names(per_rebalance[0])},
        "last": {"as_of": per_rebalance[-1].get("as_of"), "names": _names(per_rebalance[-1])},
    }


def _cc_unavailable(inventory: Mapping[str, Any]) -> Dict[str, Any]:
    return inventory.get("unavailable_reason_counts") or {}


def _owned_concepts(pit_coverage: Optional[Mapping[str, Any]],
                    catalogue: Optional[Mapping[str, Any]]) -> set:
    concepts: set = set()
    if pit_coverage:
        for key in ("concepts", "available_concepts", "owned_concepts"):
            v = pit_coverage.get(key)
            if isinstance(v, Mapping):
                concepts |= set(v.keys())
            elif isinstance(v, (list, tuple, set)):
                concepts |= set(v)
    if not concepts:
        # Fall back to the concepts the supported (non-DATA_HOLD) catalogue specs use.
        held = {s.get("spec_id") for s in (catalogue or {}).get("data_hold_specs") or []}
        for s in (catalogue or {}).get("specs") or []:
            if s.get("spec_id") in held:
                continue
            for c in s.get("requires") or []:
                if c not in ("sector", "volume", "market_cap"):
                    concepts.add(c)
    return concepts


def _slim_pit(pit_coverage: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not pit_coverage:
        return None
    keys = ("availability_start", "availability_end", "pit_observations",
            "distinct_ciks", "concepts")
    return {k: pit_coverage.get(k) for k in keys if k in pit_coverage}


# ---------------------------------------------------------------------------
# Guarded live wrapper.
# ---------------------------------------------------------------------------
def run_owned_data_inventory(inventory: Mapping[str, Any],
                             catalogue: Optional[Mapping[str, Any]] = None,
                             *, store: Any = None, id_store: Any = None) -> Dict[str, Any]:
    """Build the inventory, adding live store summaries when stores are supplied.

    Both store queries are wrapped: any failure degrades to the pure inventory
    rather than raising, so a partial/locked store never blocks the report.
    """
    pit_coverage = None
    identity_counts = None
    if store is not None:
        try:
            pit_coverage = store.coverage_summary()
        except Exception:
            pit_coverage = None
    if id_store is not None:
        try:
            identity_counts = id_store.counts()
        except Exception:
            identity_counts = None
    return build_owned_data_inventory(
        inventory, catalogue, identity_counts=identity_counts, pit_coverage=pit_coverage)
