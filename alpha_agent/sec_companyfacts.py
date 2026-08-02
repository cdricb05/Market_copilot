"""
alpha_agent.sec_companyfacts - Stage 9.4 Track E: bounded, restart-safe SEC XBRL
companyfacts acquisition + point-in-time materialization.

STAGE 9.4 RELEASE-CLOSURE STATUS = DEFERRED (OPTION B). This module is a
retained, INACTIVE reusable capability: the parser + campaign scaffolding below
are unit-tested and ready, but NO live per-CIK companyfacts fetch is wired into
the canonical collect tick and there are no owned companyfacts, so PIT
fundamentals acquisition is NOT operational. All fundamental candidates stay
DATA_HOLD and the readiness gate reports honest owned coverage. Wiring a live,
bounded acquisition campaign (Option A) is the next explicit stage; owned
coverage is never fabricated.

Parses the SEC ``companyfacts`` JSON (the free, owned EDGAR XBRL frames) into
leakage-safe point-in-time fact observations and feeds them to
``pit_fundamentals.PitFundamentalsStore``. The acquisition is driven by the
EXISTING Stage 9.3 controlled-campaign framework (``acquisition_campaign``):
bounded CIKs per batch, a daily batch cap, a consecutive-no-progress stop, a
restart-safe cursor, and one immutable batch-history row per executed batch. No
second worker, no operational-ledger write, no model promotion.

Point-in-time contract enforced here:
  * availability boundary = the SEC ``filed`` date on each fact;
  * accession-level deduplication (the same fact from the same filing is counted
    once);
  * amendments / restatements preserved as DISTINCT observations (a later
    restatement never overwrites or leaks backward - ``PitFundamentalsStore``
    returns the latest fact filed on-or-before the as-of date);
  * units/scale normalized deterministically (USD monetary facts kept; other
    units recorded and excluded from the monetary concepts);
  * fiscal identity is ``(fy, fp)`` when present, else the reported period end.

Missing concepts stay EXPLICIT. No point-in-time data is fabricated: when no
owned companyfacts exist for a CIK the batch records honest zero progress and the
campaign reports an exact blocker.
"""
from __future__ import annotations

from typing import Callable, Iterable, Optional

from . import pit_fundamentals as pfd

COMPANYFACTS_CAMPAIGN_ID = "sec_companyfacts"
COMPANYFACTS_KIND = "acq.sec_companyfacts"
COMPANYFACTS_LANE = "acq.sec_companyfacts"

# The Track E first-concept tags (a subset of the CONCEPT_MAP fallbacks).
FIRST_CONCEPTS: tuple[str, ...] = (
    "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
    "CostOfRevenue", "CostOfGoodsAndServicesSold", "GrossProfit", "Assets",
    "Liabilities", "StockholdersEquity", "NetIncomeLoss", "OperatingIncomeLoss",
    "CashAndCashEquivalentsAtCarryingValue")

# All us-gaap tags we ingest (union of the concept-map fallbacks + first list).
TARGET_TAGS: frozenset = frozenset(
    list(FIRST_CONCEPTS)
    + [t for tags in pfd.CONCEPT_MAP.values() for t in tags])

# Monetary units normalized to USD; others are recorded but not treated as a
# monetary concept value.
_MONETARY_UNITS = {"USD"}

# Exact blockers (honest, specific).
NO_OWNED_COMPANYFACTS = "DATA_HOLD_NO_OWNED_COMPANYFACTS"
FETCH_FAILED = "RETRYABLE_COMPANYFACTS_FETCH_FAILED"


def normalize_unit(unit: Optional[str]) -> tuple[str, bool]:
    """Return ``(canonical_unit, is_monetary_usd)``. Deterministic: 'USD' is the
    canonical monetary unit; 'USD/shares', 'shares' etc. are recorded but not
    monetary."""
    u = str(unit or "").strip()
    return (u or "UNKNOWN"), (u in _MONETARY_UNITS)


def parse_companyfacts(doc: dict, *, target_tags: Optional[Iterable[str]] = None,
                       cik: Optional[str] = None) -> list[dict]:
    """Parse an SEC companyfacts document into normalized PIT fact payloads.

    Accession-level deduplication: the same ``(accession, tag, unit, period_end,
    fy, fp)`` fact is emitted once. Only monetary (USD) facts for the target tags
    are emitted; a fact missing a ``filed`` date is dropped (no availability
    boundary). Returns a deterministic, sorted list of payloads."""
    tags = set(target_tags) if target_tags is not None else set(TARGET_TAGS)
    facts = ((doc or {}).get("facts") or {})
    doc_cik = cik or doc.get("cik") or doc.get("entityName")
    if doc_cik is not None:
        doc_cik = _norm_cik(doc_cik)
    out: list[dict] = []
    seen: set = set()
    for taxonomy in ("us-gaap",):
        tax = facts.get(taxonomy) or {}
        for tag, node in tax.items():
            if tag not in tags:
                continue
            units = (node or {}).get("units") or {}
            for unit, arr in units.items():
                canon_unit, monetary = normalize_unit(unit)
                if not monetary:
                    continue
                for f in (arr or []):
                    accn = f.get("accn")
                    end = f.get("end")
                    fy, fp = f.get("fy"), f.get("fp")
                    filed = f.get("filed")
                    val = f.get("val")
                    if filed is None or val is None or accn is None:
                        continue
                    key = (accn, tag, canon_unit, end, fy, fp)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append({
                        "cik": doc_cik, "concept": tag, "unit": canon_unit,
                        "value": val, "period_end": end, "fy": fy, "fp": fp,
                        "form": f.get("form"), "filed": str(filed)[:10],
                        "accession": accn})
    out.sort(key=lambda p: (str(p.get("concept")), str(p.get("filed")),
                            str(p.get("period_end")), str(p.get("accession"))))
    return out


def _norm_cik(cik) -> str:
    s = str(cik).strip()
    if s.isdigit():
        return s.zfill(10)
    return s


def to_pit_records(payloads: Iterable[dict]) -> list[dict]:
    """Wrap parsed payloads as normalized records for ``PitFundamentalsStore``."""
    return [{"normalized_payload": p, "event_type": "XBRL_FACT"}
            for p in payloads]


def materialize(docs: Iterable[dict], *,
                store: Optional[pfd.PitFundamentalsStore] = None) -> dict:
    """Parse companyfacts documents and materialize PIT observations. Returns
    ``{store, payloads, records_added, coverage}``. Deterministic and
    leakage-safe (restatements preserved; filed date is the availability
    boundary)."""
    st = store or pfd.PitFundamentalsStore()
    all_payloads: list[dict] = []
    for doc in docs:
        all_payloads.extend(parse_companyfacts(doc))
    added = st.add_records(to_pit_records(all_payloads))
    return {"store": st, "payloads": all_payloads, "records_added": added,
            "coverage": st.coverage_summary()}


# --------------------------------------------------------------------------- #
# Bounded, restart-safe companyfacts campaign (reuses acquisition_campaign).
# --------------------------------------------------------------------------- #
def ensure_campaign(store, ciks: Iterable[str], *, universe_source: str,
                    batch_size: int = 25,
                    campaign_id: str = COMPANYFACTS_CAMPAIGN_ID) -> dict:
    """Seed/reconcile the companyfacts campaign over the production CIK universe.
    Idempotent and restart-safe (append-only; never resets a COMPLETED CIK)."""
    return store.ensure_campaign(
        campaign_id, kind=COMPANYFACTS_KIND,
        universe=[_norm_cik(c) for c in ciks], universe_source=universe_source,
        batch_size=int(batch_size))


def run_batch(store, *, fetch_fn: Callable[[str], Optional[dict]], run_date: str,
              origin: str = "campaign-continuation",
              batch_size: Optional[int] = None, daily_cap: int = 6,
              no_progress_max: int = 2,
              pit_store: Optional[pfd.PitFundamentalsStore] = None,
              campaign_id: str = COMPANYFACTS_CAMPAIGN_ID) -> dict:
    """Run at most ONE bounded companyfacts batch under the Stage 9.3 controlled-
    continuation contract (daily cap + consecutive-no-progress stop, both derived
    from the append-only batch log). Returns the batch outcome + exact stop/
    blocker state. Restart-safe: a re-run after a crash resumes at the cursor and
    never double-counts a COMPLETED CIK."""
    from .runtime import (sec_continuation_stop_reason,  # reuse exact contract
                          sec_continuation_should_chain)

    cov0 = store.coverage(campaign_id)
    if not cov0.get("exists"):
        return {"status": "NO_CAMPAIGN", "campaign_id": campaign_id}
    done_today = store.batches_on_date(campaign_id, run_date)
    no_prog = store.consecutive_no_progress(campaign_id)
    stop = sec_continuation_stop_reason(
        is_complete=bool(cov0.get("is_complete")),
        completed_batches_today=done_today,
        consecutive_no_progress=no_prog, daily_cap=int(daily_cap),
        no_progress_max=int(no_progress_max))
    if stop is not None:
        return {"status": "STOPPED", "stop_reason": stop,
                "campaign_id": campaign_id, "coverage": cov0}

    batch = store.next_batch(campaign_id, batch_size=batch_size)
    if not batch:
        return {"status": "EMPTY", "campaign_id": campaign_id, "coverage": cov0}

    pit = pit_store or pfd.PitFundamentalsStore()
    succeeded: list[str] = []
    failed: list[tuple] = []
    records_added = 0
    payloads_seen = 0
    concepts: set = set()
    filed_dates: list[str] = []
    for cik in batch:
        try:
            doc = fetch_fn(cik)
        except Exception as exc:  # noqa: BLE001 - one CIK never stops the batch
            failed.append((cik, "%s: %s" % (type(exc).__name__, str(exc)[:120])))
            continue
        if not doc:
            failed.append((cik, NO_OWNED_COMPANYFACTS))
            continue
        payloads = parse_companyfacts(doc, cik=cik)
        payloads_seen += len(payloads)
        added = pit.add_records(to_pit_records(payloads))
        records_added += added
        for p in payloads:
            c = pfd.canonical_concept(p.get("concept"))
            if c:
                concepts.add(c)
            if p.get("filed"):
                filed_dates.append(p["filed"])
        if added > 0:
            succeeded.append(cik)
        else:
            failed.append((cik, NO_OWNED_COMPANYFACTS))

    store.record_results(campaign_id, succeeded=succeeded, failed=failed)
    cov1 = store.coverage(campaign_id)
    progressed = bool(succeeded) or records_added > 0
    metrics = {
        "ciks_attempted": len(batch), "ciks_completed": len(succeeded),
        "records_added": records_added,
        "records_deduped": max(0, payloads_seen - records_added),
        "issuers_covered": len(pit.covered_ciks()),
        "remaining_backlog": cov1.get("remaining_symbol_count"),
        "detail": {"concepts": sorted(concepts),
                   "availability_start": min(filed_dates) if filed_dates else None,
                   "availability_end": max(filed_dates) if filed_dates else None}}
    row = store.record_batch(campaign_id, run_date=run_date, progress=progressed,
                             origin=origin,
                             stop_reason=None if progressed else NO_OWNED_COMPANYFACTS,
                             metrics=metrics)
    should_chain = sec_continuation_should_chain(
        is_complete=bool(cov1.get("is_complete")),
        completed_batches_today=store.batches_on_date(campaign_id, run_date),
        daily_cap=int(daily_cap)) if progressed else False
    return {"status": "RAN", "campaign_id": campaign_id, "progressed": progressed,
            "ciks_attempted": len(batch), "ciks_completed": len(succeeded),
            "records_added": records_added, "batch_seq": row.get("batch_seq"),
            "blocker": None if progressed else NO_OWNED_COMPANYFACTS,
            "should_chain": should_chain, "coverage": cov1,
            "pit_coverage": pit.coverage_summary()}


def owned_companyfacts_fetch(root: str) -> Callable[[str], Optional[dict]]:
    """A fetch_fn that reads an OWNED companyfacts JSON from disk (no network):
    ``<root>/CIK##########.json`` or ``<root>/companyfacts/CIK##########.json``.
    Returns None when the owned file is absent (honest no-progress). No network,
    no provider purchase."""
    import json
    from pathlib import Path
    base = Path(root)

    def _fetch(cik: str) -> Optional[dict]:
        c = _norm_cik(cik)
        for cand in (base / ("CIK%s.json" % c),
                     base / "companyfacts" / ("CIK%s.json" % c),
                     base / ("%s.json" % c)):
            if cand.exists():
                try:
                    return json.loads(cand.read_text(encoding="utf-8-sig"))
                except (OSError, ValueError):
                    return None
        return None
    return _fetch


__all__ = ["COMPANYFACTS_CAMPAIGN_ID", "COMPANYFACTS_KIND", "COMPANYFACTS_LANE",
           "FIRST_CONCEPTS", "TARGET_TAGS", "NO_OWNED_COMPANYFACTS",
           "normalize_unit", "parse_companyfacts", "to_pit_records",
           "materialize", "ensure_campaign", "run_batch",
           "owned_companyfacts_fetch"]
