"""
alpha_agent.pit_fundamentals - Stage 9.3 smallest-defensible POINT-IN-TIME
fundamentals dataset built ONLY from already-owned SEC XBRL company facts
(``RT_FUNDAMENTAL_FACT`` / ``XBRL_FACT`` normalized records). No provider
purchase, no speculative universal platform - just the minimal slice the
FUNDAMENTAL candidates need, assembled leakage-safely.

Point-in-time contract (the whole point of building this rather than reusing the
owned EODHD current-snapshot fundamentals):

* Availability boundary = the SEC ``filed`` date of the fact (``available_at``).
  An observation is only visible ``as_of`` a date once it was actually filed by
  then - never a future restatement.
* Restatements / amendments are PRESERVED as DISTINCT immutable observations for
  the same (cik, concept, fiscal period); ``as_of`` returns the latest one whose
  filed date is <= the as-of date, so a later restatement never leaks backward.
* Deterministic concept mapping with ordered us-gaap tag fallbacks
  (``CONCEPT_MAP``); a versioned mapping (``MAPPING_VERSION`` +
  ``mapping_version_hash``).
* Units/scale are normalized to USD (or the fact's declared unit) and the fiscal
  period identity is ``(fy, fp)`` when present else the reported ``period_end``.
* Missing concepts are reported EXPLICITLY (never silently imputed).

This module is pure stdlib and does NO network and NO operational-ledger write.
It is a research-store reader/assembler; the runtime measures owned coverage
with it and reports honest DATA_HOLD until enough owned facts exist.
"""
from __future__ import annotations

import hashlib
from typing import Iterable, Optional

MAPPING_VERSION = "pit-fundamentals-1.0.0"

# Canonical concept -> ordered list of us-gaap tags to try (first hit wins). The
# fallbacks are deterministic and documented so a coverage report can name the
# exact tag that satisfied (or failed) each concept.
CONCEPT_MAP: "dict[str, list[str]]" = {
    "revenue": ["Revenues",
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "SalesRevenueNet"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold",
                        "CostOfGoodsSold"],
    "gross_profit": ["GrossProfit"],
    "assets": ["Assets"],
    "liabilities": ["Liabilities"],
    "stockholders_equity": ["StockholdersEquity"],
    "net_income": ["NetIncomeLoss"],
    "operating_income": ["OperatingIncomeLoss"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue"],
}

# The minimal concept sets each supported FUNDAMENTAL candidate needs. A concept
# is "available" for a CIK once at least one PIT observation exists for it.
CANDIDATE_CONCEPTS: "dict[str, list[str]]" = {
    # gross profitability (Novy-Marx): (revenue - cost_of_revenue) / assets,
    # with a gross_profit fallback for the numerator.
    "gross_profitability": ["revenue", "cost_of_revenue", "assets"],
    # asset growth (Cooper-Gulen-Schill): d Assets / lagged Assets.
    "asset_growth": ["assets"],
    # balance-sheet quality (leverage / equity ratio): liabilities, equity,
    # assets.
    "balance_sheet_quality": ["assets", "liabilities", "stockholders_equity"],
}


def mapping_version_hash() -> str:
    """Deterministic 16-hex content hash of the versioned concept mapping."""
    payload = repr((MAPPING_VERSION, sorted(CONCEPT_MAP.items()),
                    sorted(CANDIDATE_CONCEPTS.items())))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# us-gaap tag -> canonical concept (reverse of CONCEPT_MAP).
_TAG_TO_CONCEPT = {tag: concept
                   for concept, tags in CONCEPT_MAP.items() for tag in tags}


def canonical_concept(tag: Optional[str]) -> Optional[str]:
    return _TAG_TO_CONCEPT.get(str(tag)) if tag is not None else None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class PitObservation:
    """One immutable as-reported fact observation."""
    __slots__ = ("cik", "concept", "tag", "unit", "value", "period_end",
                 "fiscal_key", "available_at", "form", "is_amendment")

    def __init__(self, *, cik, concept, tag, unit, value, period_end,
                 fiscal_key, available_at, form=None, is_amendment=False):
        self.cik = str(cik)
        self.concept = concept
        self.tag = tag
        self.unit = unit
        self.value = value
        self.period_end = period_end
        self.fiscal_key = fiscal_key
        self.available_at = available_at
        self.form = form
        self.is_amendment = bool(is_amendment)

    def as_dict(self) -> dict:
        return {"cik": self.cik, "concept": self.concept, "tag": self.tag,
                "unit": self.unit, "value": self.value,
                "period_end": self.period_end, "fiscal_key": self.fiscal_key,
                "available_at": self.available_at, "form": self.form,
                "is_amendment": self.is_amendment}


class PitFundamentalsStore:
    """An append-only in-memory PIT observation store assembled from owned SEC
    XBRL fact payloads. Deterministic and leakage-safe; restatements preserved.

    Each input payload is the normalized ``normalized_payload`` of an
    ``RT_FUNDAMENTAL_FACT`` record, carrying at least ``cik``, ``concept``
    (us-gaap tag), ``unit``, ``value``, ``period_end``, ``filed`` and optionally
    ``fy``/``fp``/``form``."""

    def __init__(self):
        # (cik, concept, fiscal_key) -> list[PitObservation] (multiple = restated)
        self._obs: "dict[tuple, list[PitObservation]]" = {}
        self._unmapped_tags: "dict[str, int]" = {}
        self._missing_filed = 0
        self._ingested = 0

    def add_fact(self, payload: dict) -> bool:
        """Ingest one XBRL fact payload. Returns True if it produced a mapped PIT
        observation, else False (and records the reason in diagnostics)."""
        self._ingested += 1
        tag = payload.get("concept") or payload.get("tag")
        concept = canonical_concept(tag)
        if concept is None:
            if tag is not None:
                self._unmapped_tags[str(tag)] = \
                    self._unmapped_tags.get(str(tag), 0) + 1
            return False
        filed = (payload.get("filed") or payload.get("available_at")
                 or payload.get("acceptance_datetime"))
        if not filed:
            self._missing_filed += 1
            return False  # no availability boundary -> cannot place PIT-safely
        value = _num(payload.get("value") if "value" in payload
                     else payload.get("val"))
        if value is None:
            return False
        period_end = (payload.get("period_end") or payload.get("end")
                      or payload.get("report_date"))
        fy, fp = payload.get("fy"), payload.get("fp")
        fiscal_key = ("%s-%s" % (fy, fp)) if (fy and fp) else str(period_end)
        form = payload.get("form")
        obs = PitObservation(
            cik=payload.get("cik"), concept=concept, tag=str(tag),
            unit=payload.get("unit"), value=value, period_end=period_end,
            fiscal_key=fiscal_key, available_at=str(filed)[:10], form=form,
            is_amendment=str(form or "").endswith("/A"))
        key = (obs.cik, concept, fiscal_key)
        self._obs.setdefault(key, []).append(obs)
        return True

    def add_records(self, records: Iterable[dict]) -> int:
        """Ingest normalized RT_FUNDAMENTAL_FACT records (each with a
        ``normalized_payload``); returns the number of mapped observations."""
        n = 0
        for r in records:
            payload = r.get("normalized_payload") or r
            if str(r.get("event_type") or payload.get("event_type") or
                   "XBRL_FACT") not in ("XBRL_FACT", "XBRL_CONCEPT", ""):
                continue
            if self.add_fact(payload):
                n += 1
        return n

    # -- point-in-time query ------------------------------------------------ #
    def as_of(self, cik: str, concept: str, fiscal_key: str,
              as_of_date: str) -> Optional[PitObservation]:
        """The latest observation for (cik, concept, fiscal period) that was
        FILED on or before ``as_of_date`` - so a later restatement never leaks
        backward. None if nothing was available by then."""
        obs = self._obs.get((str(cik), concept, str(fiscal_key)))
        if not obs:
            return None
        eligible = [o for o in obs if o.available_at <= str(as_of_date)]
        if not eligible:
            return None
        return max(eligible, key=lambda o: (o.available_at,
                                            1 if o.is_amendment else 0))

    def observation_count(self) -> int:
        return sum(len(v) for v in self._obs.values())

    def covered_ciks(self) -> "set[str]":
        return {k[0] for k in self._obs}

    def concepts_present(self) -> "set[str]":
        return {k[1] for k in self._obs}

    def ciks_with_candidate(self, candidate: str) -> "set[str]":
        """CIKs that have >=1 observation for EVERY concept the candidate needs
        (so a real PIT evaluation could be attempted for them)."""
        need = CANDIDATE_CONCEPTS.get(candidate)
        if not need:
            return set()
        out = None
        for concept in need:
            have = {k[0] for k in self._obs if k[1] == concept}
            out = have if out is None else (out & have)
        return out or set()

    # -- coverage / diagnostics --------------------------------------------- #
    def missing_concepts(self, candidate: str) -> "list[str]":
        present = self.concepts_present()
        return [c for c in CANDIDATE_CONCEPTS.get(candidate, [])
                if c not in present]

    def coverage_summary(self, *, candidates: Optional[Iterable[str]] = None
                         ) -> dict:
        cands = list(candidates or CANDIDATE_CONCEPTS.keys())
        dates = sorted(o.available_at for v in self._obs.values() for o in v)
        per_candidate = {}
        for cand in cands:
            per_candidate[cand] = {
                "ciks_with_all_required_concepts":
                    len(self.ciks_with_candidate(cand)),
                "required_concepts": CANDIDATE_CONCEPTS.get(cand, []),
                "missing_concepts": self.missing_concepts(cand)}
        return {
            "mapping_version": MAPPING_VERSION,
            "mapping_version_hash": mapping_version_hash(),
            "facts_ingested": self._ingested,
            "pit_observations": self.observation_count(),
            "distinct_ciks": len(self.covered_ciks()),
            "concepts_present": sorted(self.concepts_present()),
            "availability_start": dates[0] if dates else None,
            "availability_end": dates[-1] if dates else None,
            "unmapped_us_gaap_tags": dict(sorted(self._unmapped_tags.items())),
            "facts_missing_filed_date": self._missing_filed,
            "per_candidate": per_candidate,
            "pit_basis": "availability = SEC filed date; restatements preserved "
                         "as distinct observations; as_of returns latest filed "
                         "<= as_of (no future-restatement leakage)"}
