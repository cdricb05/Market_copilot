"""Stage 12 full-universe Norgate inventory + classification (BLOCKER 1).

The prior Stage 12 declared owned evidence "exhausted" while only ~28% of the
1,895-name survivorship-safe Norgate universe had a materialized price series --
so ~1,360 names were unclassified and, for the licensed ones, unmaterialized.
This module classifies EVERY universe assetid (by its STABLE Norgate assetid,
never ticker text) into an explicit availability bucket using ONLY cheap owned
metadata (no bar fetch), so a genuine "available licensed history remains" can be
measured and the resumable Stage 6 backfill can then materialize it.

Classification (per assetid, single most-binding label):
* PRICE_SERIES_ALREADY_MATERIALIZED -- assetid is a key of the owned MARKET_BAR
  panel AND has a resolved CIK identity + index membership (research-ready).
* AVAILABLE_NOT_MATERIALIZED -- Norgate reports quoted bars but the assetid is not
  in the owned panel yet (the ACTIONABLE bucket; drives materialization).
* NO_BAR_HISTORY -- licensed assetid with no quoted bars.
* NO_LICENSED_HISTORY -- the symbol has no Norgate assetid (not licensed).
* IDENTITY_UNRESOLVED -- price materialized but no RESOLVED security->CIK mapping
  (price-only; cannot join fundamentals).
* MEMBERSHIP_UNAVAILABLE -- price materialized + resolved but no index-membership
  interval (excluded from survivorship-safe formation universes).
* OTHER_EXPLICIT_REASON -- any residual case, named explicitly (never silent).

Materialisation reuses the EXISTING Stage 6 ``historical_backfill.run_backfill``
(append-only, idempotent, resumable ``backfill_state.sqlite`` ticker-batch cursor)
-- never a new price writer, never a norgatedata upgrade. Bounded by a cap
increment so it is safe to run as resumable work and stop cleanly.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

CLASS_MATERIALIZED = "PRICE_SERIES_ALREADY_MATERIALIZED"
CLASS_AVAILABLE = "AVAILABLE_NOT_MATERIALIZED"
CLASS_NO_BARS = "NO_BAR_HISTORY"
CLASS_NO_LICENSE = "NO_LICENSED_HISTORY"
CLASS_ID_UNRESOLVED = "IDENTITY_UNRESOLVED"
CLASS_MEMBERSHIP = "MEMBERSHIP_UNAVAILABLE"
CLASS_OTHER = "OTHER_EXPLICIT_REASON"

ALL_CLASSES = [CLASS_MATERIALIZED, CLASS_AVAILABLE, CLASS_NO_BARS, CLASS_NO_LICENSE,
               CLASS_ID_UNRESOLVED, CLASS_MEMBERSHIP, CLASS_OTHER]
_PRICE_STATUSES = [CLASS_MATERIALIZED, CLASS_AVAILABLE, CLASS_NO_BARS, CLASS_NO_LICENSE]


# --------------------------------------------------------------------------- #
# Norgate metadata (cheap; no bar fetch).
# --------------------------------------------------------------------------- #
def open_norgate():
    try:
        import norgatedata as nd  # noqa: PLC0415 - lazy vendor import
        return nd
    except Exception:  # noqa: BLE001
        return None


def _call(nd: Any, name: str, *args: Any):
    fn = getattr(nd, name, None)
    if not callable(fn):
        return None
    try:
        return fn(*args)
    except Exception:  # noqa: BLE001 - metadata probe never raises
        return None


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:  # noqa: BLE001
            return str(v)
    return str(v)


def probe_symbol(nd: Any, symbol: str) -> Dict[str, Any]:
    """Cheap owned-metadata probe (no bar fetch): stable assetid + quoted span."""
    aid = _call(nd, "assetid", symbol)
    fq = _call(nd, "first_quoted_date", symbol)
    lq = _call(nd, "last_quoted_date", symbol)
    return {"symbol": symbol,
            "assetid": (str(aid) if aid is not None else None),
            "first_quoted": _iso(fq), "last_quoted": _iso(lq),
            "licensed": aid is not None, "has_bars": bool(fq and lq)}


# --------------------------------------------------------------------------- #
# Identity-store cross-check (assetid -> resolved CIK + membership).
# --------------------------------------------------------------------------- #
def build_identity_map(id_store: Any) -> Dict[str, Dict[str, Any]]:
    """``str(assetid) -> {security_id, resolved, has_cik, membership}`` using the
    identity store (never ticker text)."""
    try:
        from . import historical_identity as _hi
    except Exception:  # pragma: no cover
        import historical_identity as _hi  # type: ignore
    resolved_status = getattr(_hi, "STATUS_RESOLVED", "RESOLVED")
    out: Dict[str, Dict[str, Any]] = {}
    try:
        secs = id_store.list_securities(limit=1000000)
    except Exception:  # noqa: BLE001
        secs = []
    for s in secs:
        aid = s.get("norgate_assetid")
        if not aid:
            continue
        sid = s.get("security_id")
        resolved = has_cik = False
        try:
            mp = id_store.active_mapping(sid)
            resolved = bool(mp and mp.get("status") == resolved_status)
            has_cik = bool(mp and mp.get("cik"))
        except Exception:  # noqa: BLE001
            pass
        membership = _membership_present(id_store, sid, s)
        out[str(aid)] = {"security_id": sid, "resolved": resolved,
                         "has_cik": has_cik, "membership": membership}
    return out


def _membership_present(id_store: Any, sid: Any, sec_row: Mapping[str, Any]
                        ) -> Optional[bool]:
    iv = sec_row.get("membership_intervals")
    if iv is not None:
        return bool(iv)
    getter = getattr(id_store, "get_security", None)
    if callable(getter):
        try:
            rec = getter(sid) or {}
            iv = rec.get("membership_intervals")
            if iv is not None:
                return bool(iv)
        except Exception:  # noqa: BLE001
            return None
    return None


# --------------------------------------------------------------------------- #
# Classification.
# --------------------------------------------------------------------------- #
def _classify_one(probe: Mapping[str, Any], id_map: Mapping[str, Any],
                  materialized: set) -> str:
    aid = probe.get("assetid")
    materialized_hit = bool(aid and aid in materialized)
    if materialized_hit:
        idrec = id_map.get(aid) or {}
        if not idrec.get("resolved"):
            return CLASS_ID_UNRESOLVED
        if idrec.get("membership") is False:
            return CLASS_MEMBERSHIP
        return CLASS_MATERIALIZED
    if not probe.get("licensed") or not aid:
        return CLASS_NO_LICENSE
    if not probe.get("has_bars"):
        return CLASS_NO_BARS
    return CLASS_AVAILABLE


def _price_status(probe: Mapping[str, Any], materialized: set) -> str:
    aid = probe.get("assetid")
    if aid and aid in materialized:
        return CLASS_MATERIALIZED
    if not probe.get("licensed") or not aid:
        return CLASS_NO_LICENSE
    if not probe.get("has_bars"):
        return CLASS_NO_BARS
    return CLASS_AVAILABLE


def classify_universe(symbols: Sequence[str], *, materialized_assetids: Iterable[str],
                      id_map: Mapping[str, Any], nd: Any = None,
                      offset: int = 0, limit: Optional[int] = None,
                      window_start: Optional[str] = None,
                      keep_records: bool = False) -> Dict[str, Any]:
    """Classify each universe symbol by its stable assetid. Cursor-friendly via
    ``offset``/``limit`` so a large universe can be swept as bounded chunks.

    ``window_start`` (e.g. the Stage 6 ``date_range.start``) splits the AVAILABLE
    bucket into ``available_in_window`` (last quote on/after the research window ->
    materialisable into the CURRENT owned panel now) vs ``available_pre_window_only``
    (licensed history that ends before the window -> would need a widened date
    range). Both are "available licensed history that remains unmaterialised".
    """
    materialized = {str(a) for a in materialized_assetids}
    syms = list(symbols)
    batch = syms[offset:(offset + limit) if limit else None]
    counts = {c: 0 for c in ALL_CLASSES}
    price_counts = {c: 0 for c in _PRICE_STATUSES}
    avail_in_window = 0
    avail_pre_window = 0
    records: List[Dict[str, Any]] = []
    for sym in batch:
        probe = probe_symbol(nd, sym) if nd is not None else {
            "symbol": sym, "assetid": None, "licensed": False, "has_bars": False}
        cls = _classify_one(probe, id_map, materialized)
        ps = _price_status(probe, materialized)
        counts[cls] += 1
        price_counts[ps] += 1
        in_window = None
        if ps == CLASS_AVAILABLE:
            lq = probe.get("last_quoted")
            in_window = (window_start is None or (lq is not None and str(lq)[:10] >= window_start))
            if in_window:
                avail_in_window += 1
            else:
                avail_pre_window += 1
        if keep_records:
            records.append({"symbol": sym, "assetid": probe.get("assetid"),
                            "classification": cls, "price_status": ps,
                            "available_in_window": in_window,
                            "first_quoted": probe.get("first_quoted"),
                            "last_quoted": probe.get("last_quoted")})
    out = {"n_classified": len(batch), "offset": offset,
           "cursor_next": offset + len(batch), "universe_total": len(syms),
           "complete": (offset + len(batch)) >= len(syms),
           "window_start": window_start,
           "counts_by_classification": counts,
           "counts_by_price_status": price_counts,
           "available_not_materialized": price_counts[CLASS_AVAILABLE],
           "available_in_window": avail_in_window,
           "available_pre_window_only": avail_pre_window,
           "materialized": price_counts[CLASS_MATERIALIZED]}
    if keep_records:
        out["records"] = records
    return out


def merge_chunk(agg: Optional[Dict[str, Any]], chunk: Mapping[str, Any]) -> Dict[str, Any]:
    """Accumulate cursor chunks into a single running classification."""
    if agg is None:
        agg = {"n_classified": 0, "universe_total": chunk.get("universe_total"),
               "counts_by_classification": {c: 0 for c in ALL_CLASSES},
               "counts_by_price_status": {c: 0 for c in _PRICE_STATUSES}}
    agg["n_classified"] += int(chunk.get("n_classified") or 0)
    for c in ALL_CLASSES:
        agg["counts_by_classification"][c] += int(
            (chunk.get("counts_by_classification") or {}).get(c, 0))
    for c in _PRICE_STATUSES:
        agg["counts_by_price_status"][c] += int(
            (chunk.get("counts_by_price_status") or {}).get(c, 0))
    agg["cursor_next"] = chunk.get("cursor_next")
    agg["universe_total"] = chunk.get("universe_total") or agg.get("universe_total")
    agg["complete"] = bool(chunk.get("complete"))
    agg["available_not_materialized"] = agg["counts_by_price_status"][CLASS_AVAILABLE]
    agg["materialized"] = agg["counts_by_price_status"][CLASS_MATERIALIZED]
    return agg


# --------------------------------------------------------------------------- #
# Bounded, resumable materialisation via the EXISTING Stage 6 backfill writer.
# --------------------------------------------------------------------------- #
def materialize_bounded(backfill_config_path: str, *, cap_increment: int = 75,
                        as_of: str = "latest") -> Dict[str, Any]:
    """Raise the Stage 6 ``max_universe_symbols`` cap by ``cap_increment`` and run
    ONE bounded, idempotent, resumable Norgate backfill. Already-complete ticker
    batches are skipped by the Stage 6 cursor; only newly-admitted symbols are
    fetched. Never a new writer, never a norgatedata upgrade. Writes ONLY under
    the Stage 6 ingestion tree (never an operational ledger)."""
    try:
        from . import historical_backfill as _hb
    except Exception:  # pragma: no cover
        import historical_backfill as _hb  # type: ignore
    p = Path(backfill_config_path)
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[1] / backfill_config_path
    bcfg = json.loads(p.read_text(encoding="utf-8-sig"))
    base_cap = int((bcfg.get("batch") or {}).get("max_universe_symbols", 800))
    new_cap = base_cap + max(0, int(cap_increment))
    bcfg.setdefault("batch", {})["max_universe_symbols"] = new_cap
    res = _hb.run_backfill(bcfg, mode="backfill", as_of=as_of)
    summary = {"mode": "materialized", "base_cap": base_cap, "new_cap": new_cap,
               "cap_increment": max(0, int(cap_increment))}
    if isinstance(res, dict):
        for k in ("terminal", "records_written", "symbols_completed",
                  "no_data_symbols", "batches_completed"):
            if k in res:
                summary[k] = res[k]
    return summary


def resolve_universe_symbols(cfg: Mapping[str, Any]) -> List[str]:
    """The full survivorship-safe Norgate universe (current + past, incl. delisted)."""
    try:
        from . import production_universe as _pu
    except Exception:  # pragma: no cover
        import production_universe as _pu  # type: ignore
    try:
        res = _pu.resolve_production_universe(cfg, scope="survivorship")
        return list(getattr(res, "symbols", None) or [])
    except Exception:  # noqa: BLE001
        return []
