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
import re
from typing import Iterable, Optional

MAPPING_VERSION = "pit-fundamentals-1.0.0"

# A ``FY-FP`` fiscal identity, e.g. ``2025-FY`` or ``2025-Q2`` - the comparable
# prior period is deterministically ``(FY-1)-FP`` (never an adjacent quarter).
_FY_FP_RE = re.compile(r"^(\d{4})-(Q[1-4]|FY|H1|H2)$")

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
        # (cik, concept) -> set[fiscal_key]. A pure performance index over the
        # SAME observations so ``latest_fiscal_key`` / ``prior_fiscal_key`` iterate
        # only the fiscal keys for one (cik, concept) instead of scanning EVERY key
        # in ``self._obs`` (O(all facts) -> O(periods for this cik/concept)); the
        # selected fiscal keys and every leakage-safe as-of resolution are byte-for-
        # byte identical to the exhaustive scan. Deterministic, never lowers a gate.
        self._fks_by_cik_concept: "dict[tuple, set]" = {}
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
        self._fks_by_cik_concept.setdefault(
            (str(obs.cik), concept), set()).add(fiscal_key)
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

    def _period_end_available(self, cik: str, concept: str, fiscal_key: str,
                              as_of: str) -> Optional[str]:
        """The reported period_end of (cik, concept, fiscal_key) IF at least one
        observation was FILED on or before ``as_of`` (else None). Leakage-safe."""
        obs = self._obs.get((str(cik), concept, str(fiscal_key)))
        if not obs:
            return None
        avail = [o for o in obs if o.available_at <= str(as_of)]
        if not avail:
            return None
        return max((o.period_end or "") for o in avail) or None

    def latest_fiscal_key(self, cik: str, as_of: str, *,
                          concept: str = "assets") -> Optional[str]:
        """The fiscal_key of the MOST RECENT fiscal period (by reported
        period_end) for ``cik`` that has an observation of ``concept`` FILED on or
        before ``as_of``. This is the leakage-safe 'current' formation period - it
        never uses a period whose only filing post-dates the formation date. None
        if the concept was never available for this CIK by then."""
        best_fk = None
        best_pe = None
        for fk in self._fks_by_cik_concept.get((str(cik), concept), ()):
            pe = self._period_end_available(cik, concept, fk, as_of)
            if pe is None:
                continue
            if best_pe is None or pe > best_pe:
                best_pe, best_fk = pe, fk
        return best_fk

    def prior_fiscal_key(self, cik: str, as_of: str, current_fiscal_key: str, *,
                         concept: str = "assets", tolerance_days: int = 45
                         ) -> Optional[str]:
        """The COMPARABLE prior-period fiscal_key for a LIKE-FOR-LIKE comparison
        (FY vs prior-year FY, Q2 vs prior-year Q2), using only observations FILED
        on or before ``as_of``. Comparability is enforced deterministically so an
        adjacent, non-comparable quarter is NEVER substituted:

          * when ``current_fiscal_key`` is a ``FY-FP`` identity (e.g. ``2025-Q2``)
            the comparable prior key is EXACTLY ``(FY-1)-FP`` - it must exist and
            be available (filed <= as_of); there is no adjacent-quarter fallback;
          * otherwise (a bare ``period_end`` key - an annual filer without fy/fp)
            the prior ``period_end`` must fall within a TIGHT window around exactly
            one year earlier (``tolerance_days``, default 45), which admits a
            slightly-shifted FY period_end but rejects an adjacent quarter ~91 days
            away.

        Returns None when no comparable prior period is available (no fallback to
        a non-comparable period - a missing prior comparable yields no signal)."""
        import datetime as _d
        m = _FY_FP_RE.match(str(current_fiscal_key))
        if m:
            fy, fp = int(m.group(1)), m.group(2)
            prior_key = "%d-%s" % (fy - 1, fp)
            # Same fiscal period one year earlier; must be available by as_of.
            if self._period_end_available(cik, concept, prior_key,
                                          as_of) is not None:
                return prior_key
            return None
        cur_pe = self._period_end_available(cik, concept, current_fiscal_key,
                                            as_of)
        if not cur_pe:
            return None
        try:
            target = (_d.date.fromisoformat(cur_pe[:10])
                      - _d.timedelta(days=365))
        except ValueError:
            return None
        best_fk = None
        best_gap = None
        for fk in self._fks_by_cik_concept.get((str(cik), concept), ()):
            if fk == current_fiscal_key:
                continue
            # A comparable prior for a bare period_end key must itself be a bare
            # period_end key (never a FY-FP quarterly identity), else the tight
            # window could still admit a same-year quarter with a nearby end date.
            if _FY_FP_RE.match(str(fk)):
                continue
            pe = self._period_end_available(cik, concept, fk, as_of)
            if pe is None:
                continue
            try:
                gap = abs((_d.date.fromisoformat(pe[:10]) - target).days)
            except ValueError:
                continue
            if best_gap is None or gap < best_gap:
                best_gap, best_fk = gap, fk
        return best_fk if (best_gap is not None
                           and best_gap <= int(tolerance_days)) else None

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
