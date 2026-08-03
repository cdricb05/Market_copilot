"""
alpha_agent.historical_price_panel — Stage 10.3 CANONICAL survivorship-safe
historical price panel over the OWNED normalized MARKET_BAR store, anchored on the
stable Norgate ``assetid`` (NEVER ticker text alone).

Stage 10.2 unlocked deep owned PIT fundamentals (924k observations, 846 CIKs) but
the fundamental EXPERIMENT still reached only 11 scored periods (< 12) because the
evaluator read the price panel through ``experiment_runner.NormalizedStore
(max_files=6000, max_symbols=300)`` — the earliest-6000-files / 300-symbol slice
truncates the already-owned MARKET_BAR history at 2018-12-20 even though the owned
normalized tree spans 2015-01-02 .. 2026-07-31 with 500+ survivorship-safe
securities (current + delisted). This module replaces that truncated reader with a
canonical panel that:

  * reads the FULL owned normalized MARKET_BAR history (no earliest-N-file / symbol
    truncation), streaming and bounded only by an explicit date window;
  * keys each price series on the STABLE Norgate ``assetid`` carried on every owned
    Norgate MARKET_BAR record (``security_id``), so a ticker change preserves one
    identity and ticker reuse keeps identities separate — ticker text is NEVER the
    sole key (a delisted predecessor is never rewritten to a successor);
  * joins the survivorship-safe historical universe to its owned price series
    through the assetid-anchored identity store (``historical_identity``): each
    resolved CIK maps to its security's assetid, and only securities with an owned
    price series appear (missing stays missing — never silently filled);
  * exposes the panel keyed by assetid together with a ``cik -> assetid`` map so the
    EXISTING ``fundamental_evidence.evaluate_fundamental_evidence`` contract runs
    unchanged — its opaque cross-section key becomes the assetid, not the ticker.

It also supplies the PER-REBALANCE PRICE-READINESS contract: the number of REAL
evaluator scored periods (cross-sections of at least the decile minimum names that
have BOTH a leakage-safe PIT fundamental and a strictly-forward return) the owned
panel supports, gated at ``min_scored_periods`` — total price-row counts and
current-company coverage can NEVER open it; only real evaluator periods do.

Pure Python stdlib; deterministic; read-only (never writes an operational ledger,
order, fill, signal, trade decision, model or price row). Uses the owned adjusted
total-return close consistent with the evaluator contract.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Optional

from . import fundamental_evidence as _fev
from . import fundamental_signals as _fsig
from . import historical_identity as _hi

RT_MARKET_BAR = "MARKET_BAR"
# The owned survivorship-safe assetid-bearing price source. Norgate local bars
# carry the stable ``security_id`` = Norgate assetid; ticker-only sources (e.g.
# a current EODHD snapshot) carry no assetid and are excluded from the canonical
# assetid panel so the identity is always the stable assetid, never a reused
# ticker. Configurable, but defaults to the survivorship-safe owned source.
DEFAULT_SOURCES = ("norgate_local",)

# The canonical per-rebalance price-readiness thresholds. min_scored_periods is
# the SAME 12 the evaluator/tournament require and is NEVER lowered here.
DEFAULT_PRICE_READINESS = {
    "min_scored_periods": 12,
    "min_cross_section_names": _fev._MIN_DECILE_NAMES,   # 10 (a real decile leg)
    "min_names_per_period": 20,                          # per-rebalance name floor
}


# --------------------------------------------------------------------------- #
# Owned MARKET_BAR streaming (assetid-keyed).
# --------------------------------------------------------------------------- #
def iter_owned_market_bars(ingestion_root, *, sources: Iterable[str] = DEFAULT_SOURCES,
                           date_start: Optional[str] = None,
                           date_end: Optional[str] = None,
                           max_files: Optional[int] = None):
    """Yield ``(assetid, date, close)`` for every owned normalized MARKET_BAR
    record from ``sources`` within the date window. ``assetid`` is the record's
    top-level ``security_id`` (the stable Norgate assetid); records without an
    assetid (ticker-only sources) are skipped so the panel is always assetid-keyed.
    Streaming and bounded only by the explicit date window (NO earliest-N-file or
    symbol truncation). Deterministic file order (sorted)."""
    base = Path(ingestion_root) / "normalized" / RT_MARKET_BAR
    if not base.exists():
        return
    allow = set(sources)
    files = sorted(base.rglob("*.jsonl"))
    if max_files is not None:
        files = files[:int(max_files)]
    for f in files:
        try:
            text = f.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if allow and (rec.get("source_id") not in allow):
                continue
            assetid = rec.get("security_id")
            if not assetid:
                continue
            pay = rec.get("normalized_payload") or {}
            date = rec.get("effective_at") or pay.get("Date")
            close = pay.get("Close", pay.get("close"))
            if not date or close is None:
                continue
            date = str(date)[:10]
            if date_start and date < date_start:
                continue
            if date_end and date > date_end:
                continue
            try:
                yield str(assetid), date, float(close)
            except (TypeError, ValueError):
                continue


def build_assetid_price_panel(ingestion_root, *, sources: Iterable[str] = DEFAULT_SOURCES,
                              date_start: Optional[str] = None,
                              date_end: Optional[str] = None,
                              max_files: Optional[int] = None) -> dict:
    """The owned survivorship-safe price panel keyed by Norgate assetid:
    ``{assetid: [(date, adj_close)]}`` sorted ascending and de-duplicated. Reads
    the FULL owned MARKET_BAR history within the window — the canonical replacement
    for the truncated ``NormalizedStore.price_panel`` reader."""
    panel: dict = {}
    for assetid, date, close in iter_owned_market_bars(
            ingestion_root, sources=sources, date_start=date_start,
            date_end=date_end, max_files=max_files):
        panel.setdefault(assetid, []).append((date, close))
    for a in panel:
        panel[a] = sorted(set(panel[a]), key=lambda p: p[0])
    return panel


# --------------------------------------------------------------------------- #
# Effective-dated, assetid-anchored identity join (survivorship-safe).
# --------------------------------------------------------------------------- #
def build_cik_to_assetid(id_store) -> dict:
    """``{cik: assetid}`` for every ACTIVE RESOLVED security in the identity store
    (the survivorship-safe historical universe — current + delisted), anchored on
    the stable Norgate ``assetid``. A ticker change never changes the assetid and a
    reused ticker maps to a DIFFERENT assetid, so the price join is identity-stable
    and never rewrites a predecessor to a successor. Deterministic: on the (rare)
    event that two resolved securities share a CIK (share classes) the
    lexicographically-first security_id's assetid wins, recorded once."""
    out: dict = {}
    if id_store is None:
        return out
    for s in sorted(id_store.list_securities(limit=1000000),
                    key=lambda x: str(x.get("security_id"))):
        mp = id_store.active_mapping(s["security_id"])
        if not (mp and mp.get("status") == _hi.STATUS_RESOLVED and mp.get("cik")):
            continue
        assetid = s.get("norgate_assetid")
        cik = _hi.norm_cik(mp["cik"])
        if assetid and cik:
            out.setdefault(cik, str(assetid))
    return out


def assetid_to_effective_ticker(id_store, as_of: str) -> dict:
    """``{assetid: effective_ticker_on(as_of)}`` for reporting/audit only (the
    PIT-correct ticker on the formation date). Never a join key — the join key is
    always the assetid."""
    out: dict = {}
    if id_store is None:
        return out
    for u in id_store.historical_universe_on(as_of):
        aid = u.get("norgate_assetid")
        if aid:
            out[str(aid)] = u.get("ticker_effective_on") or u.get("ticker")
    return out


def build_repaired_panel(ingestion_root, id_store, *,
                         sources: Iterable[str] = DEFAULT_SOURCES,
                         date_start: Optional[str] = None,
                         date_end: Optional[str] = None) -> tuple:
    """Convenience for the experiment handler: return
    ``(panel_by_assetid, cik_to_assetid)`` ready to feed the EXISTING evaluator as
    ``evaluate_fundamental_evidence(store, panel_by_assetid, signal,
    cik_to_ticker=cik_to_assetid)`` — the evaluator's opaque cross-section key
    becomes the stable assetid."""
    panel = build_assetid_price_panel(ingestion_root, sources=sources,
                                      date_start=date_start, date_end=date_end)
    c2a = build_cik_to_assetid(id_store)
    return panel, c2a


# --------------------------------------------------------------------------- #
# Cheap owned-panel coverage signature (epoch for spec de-dup / re-measurement).
# --------------------------------------------------------------------------- #
def panel_coverage_signature(ingestion_root, *,
                             sources: Iterable[str] = DEFAULT_SOURCES) -> dict:
    """A CHEAP deterministic signature of the owned MARKET_BAR coverage derived
    from the partition paths (no line reads): earliest/latest dated partition and
    the dated-file count. It changes only when the owned price history genuinely
    grows, so a repaired/extended panel yields a new epoch and downstream
    measurement + experiment generation re-run exactly once."""
    base = Path(ingestion_root) / "normalized" / RT_MARKET_BAR
    dates = []
    n = 0
    if base.exists():
        for f in base.rglob("*.jsonl"):
            parts = f.parts
            if len(parts) >= 4 and parts[-4].isdigit() and parts[-3].isdigit() \
                    and parts[-2].isdigit():
                dates.append("%s-%s-%s" % (parts[-4], parts[-3], parts[-2]))
                n += 1
    dates.sort()
    sig = {"date_min": dates[0] if dates else None,
           "date_max": dates[-1] if dates else None,
           "dated_files": n, "sources": sorted(set(sources))}
    sig["hash"] = hashlib.sha256(
        json.dumps(sig, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return sig


def panel_coverage_epoch(ingestion_root, *,
                         sources: Iterable[str] = DEFAULT_SOURCES) -> str:
    return panel_coverage_signature(ingestion_root, sources=sources)["hash"]


# --------------------------------------------------------------------------- #
# Per-rebalance PRICE readiness (the number of REAL evaluator scored periods).
# --------------------------------------------------------------------------- #
def per_rebalance_price_readiness(store, panel: dict, cik_to_assetid: dict,
                                  signal: str, *, horizon_days: int = 63,
                                  rebalance_dates: Optional[list] = None,
                                  thresholds: Optional[dict] = None) -> dict:
    """MEASURED per-rebalance PRICE readiness for one fundamental ``signal`` over
    the owned assetid panel. For each panel-derived quarterly formation date it
    reproduces the evaluator cross-section funnel EXACTLY: a name counts only when
    its assetid has an owned price series AND a leakage-safe PIT fundamental (and,
    for asset_growth, a comparable prior period) AND a STRICTLY-forward return; a
    date is a REAL scored period only when its cross-section has at least the decile
    minimum names. ``valid_scored_periods`` is that count of REAL evaluator periods
    — never a total-row or current-company proxy — and the gate opens only when it
    reaches ``min_scored_periods``. Deterministic and read-only.

    ``store`` is the owned PIT fundamentals store; ``panel`` is keyed by assetid;
    ``cik_to_assetid`` maps each resolved CIK to its stable assetid."""
    thr = dict(DEFAULT_PRICE_READINESS)
    thr.update(thresholds or {})
    horizon = int(horizon_days)
    min_cross = int(thr["min_cross_section_names"])
    min_names_floor = int(thr["min_names_per_period"])
    sign = _fsig.EXPECTED_SIGN.get(signal, 1) or 1
    price_idx = {a: _fev._price_index(b) for a, b in panel.items()}
    dates = sorted(rebalance_dates) if rebalance_dates else \
        _fev._default_rebalance_dates(panel, horizon=horizon)

    per_date: list = []
    for d in dates:
        c = {"as_of": d, "mapped": 0, "price_present": 0, "fund_present": 0,
             "prior_present": 0, "value_present": 0, "forward_present": 0,
             "cross_section": 0}
        for cik, assetid in cik_to_assetid.items():
            c["mapped"] += 1
            if assetid not in price_idx:
                continue
            c["price_present"] += 1
            fk = store.latest_fiscal_key(cik, d, concept="assets")
            if fk is None:
                continue
            c["fund_present"] += 1
            prior = (store.prior_fiscal_key(cik, d, fk, concept="assets")
                     if signal == "asset_growth" else None)
            if signal == "asset_growth" and prior is None:
                continue
            c["prior_present"] += 1
            val = _fsig.compute_signal(store, signal, cik=cik, fiscal_key=fk,
                                       as_of=d, prior_fiscal_key=prior).get("value")
            if val is None:
                continue
            c["value_present"] += 1
            pdates, closes = price_idx[assetid]
            if _fev._forward_return(pdates, closes, d, horizon) is None:
                continue
            c["forward_present"] += 1
            c["cross_section"] += 1
        c["scored"] = c["cross_section"] >= min_cross
        per_date.append(c)

    scored_dates = [p for p in per_date if p["scored"]]
    valid = len(scored_dates)
    names_series = [p["cross_section"] for p in per_date]
    min_names = min((p["cross_section"] for p in scored_dates), default=0)
    min_req = int(thr["min_scored_periods"])
    blocker = None
    if valid < min_req:
        blocker = "INSUFFICIENT_PRICE_SCORED_PERIODS"
    elif scored_dates and min_names < min_names_floor:
        blocker = "INSUFFICIENT_MIN_NAMES_PER_PERIOD"
    sufficient = blocker is None and valid >= min_req
    return {
        "signal": signal,
        "sufficient": bool(sufficient),
        "blocker": blocker,
        "valid_scored_periods": valid,
        "min_scored_periods": min_req,
        "formation_dates": len(dates),
        "panel_assetids": len(panel),
        "min_names_in_scored_period": min_names,
        "median_names_in_period": (sorted(names_series)[len(names_series) // 2]
                                   if names_series else 0),
        "earliest_scored_date": scored_dates[0]["as_of"] if scored_dates else None,
        "latest_scored_date": scored_dates[-1]["as_of"] if scored_dates else None,
        "horizon_days": horizon,
        "thresholds": thr,
        "per_date": per_date,
    }


__all__ = [
    "RT_MARKET_BAR", "DEFAULT_SOURCES", "DEFAULT_PRICE_READINESS",
    "iter_owned_market_bars", "build_assetid_price_panel",
    "build_cik_to_assetid", "assetid_to_effective_ticker", "build_repaired_panel",
    "panel_coverage_signature", "panel_coverage_epoch",
    "per_rebalance_price_readiness",
]
