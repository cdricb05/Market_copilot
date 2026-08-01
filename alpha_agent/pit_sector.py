"""
alpha_agent/pit_sector.py — Stage 8 WS5 point-in-time SIC sector classification.

The current Norgate GICS surface is CURRENT-ONLY: using it to exclude financials
(or to sector-neutralize) in a retrospective backtest injects a classification
look-ahead — a name reclassified in 2024 is treated as if it always had its 2024
sector. This module builds the leakage-safe fallback from the ASSIGNED-SIC code in
CONTEMPORANEOUS SEC filing headers: the SIC a filing carried AT its acceptance
time, mapped to a stable, versioned research-sector taxonomy, and queried with a
strict no-look-ahead rule (``sector_as_of(date)`` uses only observations whose
acceptance time is on/before ``date``).

Pure standard library. No network, no database, no operational writes.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Optional

MAPPING_VERSION = "sic-research-sector-1.0.0"

# Stable SIC-range -> research-sector map (SIC major divisions / groups). The
# financials range (6000-6999: Finance, Insurance, Real Estate) is the one that
# drives the "excluding financials" screen and is deliberately explicit.
_SIC_RANGES = (
    (100, 999, "Materials"),          # Agriculture, forestry, fishing
    (1000, 1499, "Materials"),        # Mining
    (1500, 1799, "Industrials"),      # Construction
    (2000, 2199, "ConsumerStaples"),  # Food & kindred
    (2200, 2799, "Industrials"),      # Textiles, apparel, paper, printing
    (2800, 2899, "Materials"),        # Chemicals
    (2900, 2999, "Energy"),           # Petroleum refining
    (3000, 3569, "Industrials"),      # Rubber, metal, machinery
    (3570, 3579, "Technology"),       # Computer & office equipment
    (3580, 3669, "Industrials"),      # Other machinery / electrical
    (3670, 3699, "Technology"),       # Electronic components / semiconductors
    (3700, 3799, "ConsumerDiscretionary"),  # Transportation equipment (autos)
    (3800, 3899, "HealthCare"),       # Instruments (incl. medical)
    (3900, 3999, "ConsumerDiscretionary"),
    (4000, 4799, "Industrials"),      # Transportation
    (4800, 4899, "CommunicationServices"),  # Communications
    (4900, 4999, "Utilities"),        # Electric, gas, sanitary
    (5000, 5199, "Industrials"),      # Wholesale
    (5200, 5999, "ConsumerDiscretionary"),  # Retail
    (6000, 6999, "Financials"),       # Finance, insurance, real estate
    (7000, 7999, "ConsumerDiscretionary"),  # Services
    (8000, 8099, "HealthCare"),       # Health services
    (8100, 8999, "Industrials"),      # Other services
    (9100, 9999, "Government"),        # Public administration
)

FINANCIALS = "Financials"
UNKNOWN = "Unknown"


def mapping_version_hash() -> str:
    """Deterministic 16-hex content hash of the versioned SIC->sector map, so a
    coverage report can pin the exact mapping that produced a classification."""
    payload = repr((MAPPING_VERSION, _SIC_RANGES, FINANCIALS, UNKNOWN))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _sic_int(sic) -> Optional[int]:
    if sic is None:
        return None
    s = "".join(ch for ch in str(sic) if ch.isdigit())
    if not s:
        return None
    try:
        return int(s[:4])
    except ValueError:
        return None


def sic_to_sector(sic) -> dict:
    """Map a SIC code to a research sector + a confidence. Returns
    ``{sector, confidence, sic, mapping_version}``. An unmappable SIC yields
    sector Unknown with confidence 0.0 (never guessed into a real sector)."""
    code = _sic_int(sic)
    if code is None:
        return {"sector": UNKNOWN, "confidence": 0.0, "sic": None,
                "mapping_version": MAPPING_VERSION}
    for lo, hi, sector in _SIC_RANGES:
        if lo <= code <= hi:
            return {"sector": sector, "confidence": 0.9, "sic": code,
                    "mapping_version": MAPPING_VERSION}
    return {"sector": UNKNOWN, "confidence": 0.0, "sic": code,
            "mapping_version": MAPPING_VERSION}


def is_financial_sic(sic) -> bool:
    code = _sic_int(sic)
    return code is not None and 6000 <= code <= 6999


def _as_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    s = str(value).replace("Z", "+00:00")
    for fmt in (None,):  # try fromisoformat first
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None)
        except ValueError:
            break
    try:
        return datetime.fromisoformat(str(value)[:10])
    except ValueError:
        return None


class PitSicSeries:
    """A point-in-time SIC classification series keyed by an entity id (CIK or
    ticker). Each observation is ``(available_at, sic, provenance)``. Queries use
    a STRICT no-look-ahead rule: ``sector_as_of(key, date)`` returns the sector
    from the most recent observation whose availability is on/before ``date`` —
    a future reclassification NEVER leaks backward."""

    def __init__(self) -> None:
        self._obs: dict[str, list[dict]] = {}

    def add(self, key: str, *, sic, available_at, provenance: str = "",
            confidence: Optional[float] = None) -> None:
        dt = _as_dt(available_at)
        if not key or dt is None:
            return
        mapped = sic_to_sector(sic)
        self._obs.setdefault(str(key), []).append({
            "available_at": dt, "sic": mapped["sic"], "sector": mapped["sector"],
            "confidence": mapped["confidence"] if confidence is None else confidence,
            "provenance": provenance})

    def _latest_asof(self, key: str, as_of) -> Optional[dict]:
        dt = _as_dt(as_of)
        obs = self._obs.get(str(key))
        if not obs or dt is None:
            return None
        eligible = [o for o in obs if o["available_at"] <= dt]  # NO look-ahead
        if not eligible:
            return None
        return max(eligible, key=lambda o: o["available_at"])

    def sector_as_of(self, key: str, as_of) -> str:
        o = self._latest_asof(key, as_of)
        return o["sector"] if o else UNKNOWN

    def is_financial_as_of(self, key: str, as_of) -> Optional[bool]:
        """True/False when a contemporaneous classification exists; None when the
        entity has no observation on/before ``as_of`` (unknown, never assumed)."""
        o = self._latest_asof(key, as_of)
        if o is None:
            return None
        return o["sector"] == FINANCIALS

    def covered_keys(self) -> set:
        return set(self._obs)

    @classmethod
    def from_records(cls, records: "list[dict]") -> "PitSicSeries":
        """Build from normalized ASSIGNED_SIC_PIT records (RT_SECURITY_IDENTITY
        with an ``assigned_sic`` payload and ``available_at`` = SEC acceptance)."""
        series = cls()
        for r in records:
            pay = r.get("normalized_payload") or {}
            sic = pay.get("assigned_sic") or pay.get("sic")
            avail = r.get("available_at") or pay.get("acceptance_time")
            if not sic or not avail:
                continue
            prov = r.get("provenance", "SEC assigned-SIC PIT")
            # Index under BOTH the CIK and the ticker so callers can query by
            # either (panel symbols are tickers; the series id is the CIK).
            for key in {r.get("company_id"), pay.get("cik"), r.get("ticker"),
                        pay.get("ticker")}:
                if key:
                    series.add(str(key), sic=sic, available_at=avail,
                               provenance=prov)
        return series


def three_way_financials_comparison(*, symbols: "list[str]",
                                    current_gics: dict,
                                    pit_series: "PitSicSeries",
                                    key_of=None, as_of: str) -> dict:
    """Compare the three financials-exclusion universes over ``symbols``:

      1. full        — no exclusion (baseline);
      2. current_gics — exclude names whose CURRENT GICS sector == Financials
                        (exploratory; classification look-ahead);
      3. pit_sic     — exclude names PIT-classified Financials as of ``as_of``
                        from contemporaneous SEC ASSIGNED-SIC (leakage-safe, but
                        only over names with a PIT observation).

    Returns per-variant universe sizes, exclusion counts, coverage, and an
    explicit evidence label. Only the PIT-SIC variant is leakage-safe."""
    key_of = key_of or (lambda s: s)
    n = len(symbols)

    gics_fin = [s for s in symbols
                if str(current_gics.get(s) or "").strip().lower() == "financials"]
    pit_known = [s for s in symbols
                 if pit_series.is_financial_as_of(key_of(s), as_of) is not None]
    pit_fin = [s for s in symbols
               if pit_series.is_financial_as_of(key_of(s), as_of) is True]

    return {
        "as_of": as_of, "universe_size": n,
        "variants": {
            "full": {"universe": n, "excluded": 0,
                     "leakage_safe": True,
                     "evidence_label": "BASELINE_FULL_UNIVERSE"},
            "current_gics": {
                "universe": n - len(gics_fin), "excluded": len(gics_fin),
                "leakage_safe": False,
                "evidence_label": "PROVISIONAL_CLASSIFICATION_LOOKAHEAD",
                "note": "current Norgate GICS is undated; excluding on it injects "
                        "a classification look-ahead — exploratory only"},
            "pit_sic": {
                "universe": len(pit_known) - len(pit_fin),
                "classified": len(pit_known),
                "excluded_financials": len(pit_fin),
                "coverage_fraction": (len(pit_known) / n) if n else 0.0,
                "leakage_safe": True,
                "evidence_label": ("LEAKAGE_SAFE_PIT_SIC"
                                   if len(pit_known) >= max(20, int(0.6 * n))
                                   else "LEAKAGE_SAFE_PIT_SIC_LOW_COVERAGE"),
                "note": "excluded on contemporaneous SEC ASSIGNED-SIC (available "
                        "at filing acceptance); the only leakage-safe variant"}},
        "mapping_version": MAPPING_VERSION,
    }
