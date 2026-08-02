"""
alpha_agent.historical_identity — Stage 10 HISTORICAL IDENTITY LAYER.

The survivorship-safe, effective-dated security-identity infrastructure that is
the missing prerequisite for the AlphaAgent's historical fundamental / event
evaluation. It closes the exact blocker ``HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_
READY``: the free SEC ``company_tickers.json`` maps ONLY current entities to CIKs,
so the current constituents are not a valid historical backtest universe (they
exclude every delisted name that existed on a past rebalance date). Until an
owned, survivorship-safe, effective-dated security identity AND a security->CIK
mapping with measured historical coverage exist, the safety diagnostic stays on
and every fundamental candidate stays DATA_HOLD.

What this module provides (Stage 10 Parts 2-5), all pure stdlib + deterministic,
no operational mutation, no network beyond the local Norgate vendor library:

  * a CANONICAL, VERSIONED, EFFECTIVE-DATED historical security identity model
    anchored on the owned, survivorship-safe Norgate ``assetid`` (NOT ticker
    text) — company identity, security identity, share class, ticker history,
    name history, exchange, security start / delisting date, index-membership
    intervals, SEC CIK with effective dates, mapping method / confidence /
    evidence / status / version hash / source-artifact hash;
  * a DETERMINISTIC MATCHING CONTRACT with an explicit evidence tier order in
    which ticker text alone is never sufficient and ambiguous matches remain
    UNRESOLVED (never fabricated), preventing ticker reuse, current-ticker /
    current-CIK substitution, merger-successor rewrites and share-class collapse;
  * a HISTORICAL UNIVERSE SERVICE that, for any formation date, returns the exact
    eligible historical securities and their effective identities from owned
    Norgate membership history (never a current-survivor universe);
  * a durable, append-only, versioned CANONICAL IDENTITY STORE on the data drive
    (NOT under any operational-ledger root) with atomic writes, deterministic
    hashes, restart safety, idempotent replay and exact provenance.

The Norgate vendor access is a thin, injectable accessor (mirroring the existing
``production_universe.NorgateUniverseAccessor`` pattern) so every algorithm here
is unit-testable with a fake and the module never installs or upgrades Norgate.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

STAGE = "10"
SCHEMA_VERSION = "1.0.0"
# Bumped whenever the identity extraction / matching algorithm changes so a
# mapping row records the exact algorithm version that produced it.
MAPPING_ALGORITHM_VERSION = "hist-identity-1.0.0"

# --------------------------------------------------------------------------- #
# Mapping status (Part 2) — the lifecycle of a security->CIK relationship.
# --------------------------------------------------------------------------- #
STATUS_RESOLVED = "RESOLVED"
STATUS_AMBIGUOUS = "AMBIGUOUS"
STATUS_UNRESOLVED = "UNRESOLVED"
STATUS_CONFLICT = "CONFLICT"
STATUS_SUPERSEDED = "SUPERSEDED"
MAPPING_STATUSES = (STATUS_RESOLVED, STATUS_AMBIGUOUS, STATUS_UNRESOLVED,
                    STATUS_CONFLICT, STATUS_SUPERSEDED)

# --------------------------------------------------------------------------- #
# Matching tiers (Part 3) — the permitted evidence order. Lower tier == stronger
# evidence. Ticker text alone is DELIBERATELY absent from every resolving tier.
# --------------------------------------------------------------------------- #
TIER_OWNED_AUTHORITATIVE = 1   # authoritative owned security/issuer identifier
TIER_DIRECT_NORGATE_SEC = 2    # direct effective-dated Norgate->SEC identifier
TIER_FILING_TICKER_OVERLAP = 3  # SEC filing identity + date-overlapping ticker
TIER_LEGAL_NAME_OVERLAP = 4    # historical legal-name match w/ date overlap
TIER_AUDITED_REPAIR = 5        # explicit audited repair rule
TIER_UNRESOLVED_BACKLOG = 6    # unresolved manual-review backlog

METHOD_OWNED_AUTHORITATIVE = "owned_authoritative_identifier"
METHOD_DIRECT_NORGATE_SEC = "direct_norgate_sec_identifier"
METHOD_FILING_TICKER_OVERLAP = "sec_filing_date_overlapping_ticker"
METHOD_LEGAL_NAME_OVERLAP = "legal_name_effective_date_overlap"
METHOD_AUDITED_REPAIR = "audited_repair_rule"
METHOD_UNRESOLVED = "unresolved_manual_review"

# Confidence per resolving tier (deterministic; never inflated).
_TIER_CONFIDENCE = {
    TIER_OWNED_AUTHORITATIVE: 1.0,
    TIER_DIRECT_NORGATE_SEC: 0.95,
    TIER_FILING_TICKER_OVERLAP: 0.80,
    TIER_LEGAL_NAME_OVERLAP: 0.70,
    TIER_AUDITED_REPAIR: 0.85,
    TIER_UNRESOLVED_BACKLOG: 0.0,
}

_OPEN_DATE = "9999-12-31"   # sentinel effective-end for a still-open interval


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _digest(*parts: str) -> str:
    return hashlib.sha256("|".join("" if p is None else str(p)
                                   for p in parts).encode("utf-8")).hexdigest()


def content_hash(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def _norm_ticker(t: Any) -> str:
    return str(t or "").strip().upper()


def norm_cik(cik: Any) -> Optional[str]:
    """Zero-pad a numeric CIK to 10 digits (the SEC canonical form). Non-digit /
    empty returns None so a bad value never masquerades as a resolved CIK."""
    s = str(cik or "").strip()
    if not s:
        return None
    s = s.lstrip("0") or "0"
    if not s.isdigit():
        return None
    return s.zfill(10)


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()[:10]
        except Exception:  # noqa: BLE001
            return str(value)[:10]
    s = str(value)
    return s[:10] if s and s != "None" else None


# --------------------------------------------------------------------------- #
# Norgate delisted-symbol convention. Norgate encodes a delisted security as
# ``TICKER-YYYYMM`` (e.g. ``AAMRQ-201312``). The bare ticker is the historical
# ticker; the suffix is the delisting year-month. This is owned, deterministic
# vendor metadata — NOT a guess.
# --------------------------------------------------------------------------- #
def parse_norgate_symbol(symbol: str) -> dict:
    """Split a Norgate symbol into its historical ticker and (optional) delisting
    year-month. ``AAMRQ-201312`` -> {'ticker': 'AAMRQ', 'delist_yyyymm':
    '201312', 'is_delisted': True}. A live symbol -> is_delisted False."""
    s = _norm_ticker(symbol)
    if "-" in s:
        head, _, tail = s.rpartition("-")
        if head and len(tail) == 6 and tail.isdigit() and \
                "01" <= tail[4:6] <= "12":
            return {"ticker": head, "delist_yyyymm": tail, "is_delisted": True,
                    "norgate_symbol": s}
    return {"ticker": s, "delist_yyyymm": None, "is_delisted": False,
            "norgate_symbol": s}


def canonical_security_id(assetid: Any, *, fallback_symbol: str = "") -> Optional[str]:
    """The canonical security identity = the owned survivorship-safe Norgate
    ``assetid`` (a stable per-security integer that is invariant across ticker
    and name changes, and distinct per share class). Ticker text is NEVER the
    identity. Falls back to a symbol-derived id only when no assetid is
    observed (so a security is still trackable), clearly namespaced so it can
    never collide with a real assetid."""
    s = str(assetid or "").strip()
    if s and s.lower() != "none":
        return "ngid:%s" % s
    fb = _norm_ticker(fallback_symbol)
    return ("ngsym:%s" % fb) if fb else None


def derive_share_class(security_name: Optional[str],
                       subtype: Optional[str] = None) -> str:
    """Deterministically derive a share-class label from the Norgate security
    name suffix (Common / Class A / Class B / Preferred / Unit / Warrant / ADR).
    Distinct share classes carry distinct Norgate assetids, so this label is
    descriptive only — the identity is the assetid, never this string."""
    n = (security_name or "").strip().lower()
    for needle, label in (
            ("class a", "CLASS_A"), (" cl a", "CLASS_A"),
            ("class b", "CLASS_B"), (" cl b", "CLASS_B"),
            ("class c", "CLASS_C"), (" cl c", "CLASS_C"),
            ("preferred", "PREFERRED"), (" pfd", "PREFERRED"),
            ("warrant", "WARRANT"), (" unit", "UNIT"),
            ("depositary", "ADR"), (" adr", "ADR"), ("common", "COMMON")):
        if needle in n:
            return label
    return "COMMON"


# --------------------------------------------------------------------------- #
# Norgate identity accessor (injectable; degrades honestly; never installs).
# --------------------------------------------------------------------------- #
class NorgateIdentityAccessor:
    """Thin, lazy wrapper over the licensed ``norgatedata`` vendor library for
    the identity-timeline extraction the existing collectors do NOT expose as a
    reusable function. Reuses the exact proven vendor calls (assetid /
    security_name / exchange_name / first_quoted_date / last_quoted_date /
    subtype1 / base_type / watchlist_symbols / index_constituent_timeseries). If
    the package / NDU is unavailable every method degrades to an empty result
    rather than raising, so the identity jobs record an honest blocker instead
    of fabricating identifiers."""

    def __init__(self, module: Any = None):
        self._nd = module

    def _import(self):
        if self._nd is not None:
            return self._nd
        try:
            import norgatedata  # noqa: PLC0415 — lazy local vendor import
            self._nd = norgatedata
        except Exception:  # noqa: BLE001
            self._nd = None
        return self._nd

    def available(self) -> "tuple[bool, str]":
        nd = self._import()
        if nd is None:
            return False, "norgatedata not importable (Norgate library absent)"
        try:
            if not bool(nd.status()):
                return False, "Norgate Data Updater not running"
        except Exception as exc:  # noqa: BLE001
            return False, "norgatedata.status() failed: %s" % type(exc).__name__
        return True, "NDU running"

    def watchlist_symbols(self, name: str) -> "list[str]":
        nd = self._import()
        if nd is None:
            return []
        try:
            return [str(s) for s in nd.watchlist_symbols(name)]
        except Exception:  # noqa: BLE001
            return []

    @staticmethod
    def _call(nd: Any, fn: str, *args: Any) -> Any:
        f = getattr(nd, fn, None)
        if f is None or not callable(f):
            return None
        try:
            return f(*args)
        except Exception:  # noqa: BLE001 — observed-only; never fabricate
            return None

    def security_identity(self, symbol: str) -> dict:
        """Best-effort CURRENT vendor identity snapshot for one symbol. Only
        fields actually observed are returned. ``assetid`` anchors the identity;
        ``first_quoted_date`` / ``last_quoted_date`` bound the security life;
        a non-null ``last_quoted_date`` (or the Norgate delist suffix) marks a
        delisting."""
        nd = self._import()
        if nd is None:
            return {}
        out: dict[str, Any] = {}
        for fn in ("assetid", "security_name", "exchange_name", "subtype1",
                   "base_type", "first_quoted_date", "last_quoted_date"):
            v = self._call(nd, fn, symbol)
            if v is not None:
                out[fn] = _iso(v) if fn.endswith("_date") else (
                    v if isinstance(v, (str, int, float, bool)) else str(v))
        return out

    def membership_intervals(self, symbol: str, index_name: str,
                             start_date: str = "1990-01-01") -> "list[dict]":
        """Effective-dated index-membership intervals for one symbol, collapsing
        the per-date constituent flag into ``[{'member_start','member_end'}]``
        spans (the same interval algorithm used by the Stage 6 backfill). An open
        span (still a member) has member_end=None."""
        nd = self._import()
        if nd is None:
            return []
        arr = None
        for kwargs in (dict(start_date=start_date,
                            timeseriesformat="numpy-recarray"),
                       dict(start_date=start_date)):
            try:
                arr = nd.index_constituent_timeseries(symbol, index_name,
                                                      **kwargs)
                break
            except TypeError:
                continue
            except Exception:  # noqa: BLE001
                return []
        if arr is None:
            return []
        try:
            names = list(getattr(arr, "dtype", None).names or [])
            flag = next((n for n in names if "constituent" in n.lower()), None)
            rows = []
            for item in arr:
                d = item["Date"] if "Date" in names else None
                rows.append((_iso(d), bool(item[flag]) if flag else False))
        except Exception:  # noqa: BLE001
            return []
        return _collapse_spans(rows, index_name)


def _collapse_spans(rows: "list[tuple]", index_name: str) -> "list[dict]":
    """Collapse ordered (date, is_member) rows into effective-dated intervals."""
    spans: list[dict] = []
    cur_start: Optional[str] = None
    prev_date: Optional[str] = None
    for d, member in rows:
        if d is None:
            continue
        if member and cur_start is None:
            cur_start = d
        elif not member and cur_start is not None:
            spans.append({"index_name": index_name, "member_start": cur_start,
                          "member_end": prev_date})
            cur_start = None
        prev_date = d
    if cur_start is not None:
        # Still a member as of the last observed date -> open interval.
        spans.append({"index_name": index_name, "member_start": cur_start,
                      "member_end": None})
    return spans


# --------------------------------------------------------------------------- #
# Security identity extraction (Part 2). Assembles ONE effective-dated security
# identity record from the owned Norgate accessor. Deterministic and pure given
# the accessor's observations.
# --------------------------------------------------------------------------- #
@dataclass
class SecurityIdentity:
    security_id: str
    norgate_assetid: Optional[str]
    norgate_symbol: str
    ticker: str
    issuer_name: Optional[str]
    share_class: str
    base_type: Optional[str]
    exchange: Optional[str]
    security_start_date: Optional[str]
    security_end_date: Optional[str]
    delisting_date: Optional[str]
    is_current: bool
    ticker_history: "list[dict]" = field(default_factory=list)
    name_history: "list[dict]" = field(default_factory=list)
    membership_intervals: "list[dict]" = field(default_factory=list)
    source: str = "norgate_local"

    def as_dict(self) -> dict:
        return {
            "security_id": self.security_id,
            "norgate_assetid": self.norgate_assetid,
            "norgate_symbol": self.norgate_symbol, "ticker": self.ticker,
            "issuer_name": self.issuer_name, "share_class": self.share_class,
            "base_type": self.base_type, "exchange": self.exchange,
            "security_start_date": self.security_start_date,
            "security_end_date": self.security_end_date,
            "delisting_date": self.delisting_date, "is_current": self.is_current,
            "ticker_history": self.ticker_history,
            "name_history": self.name_history,
            "membership_intervals": self.membership_intervals,
            "source": self.source,
        }

    @property
    def content_hash(self) -> str:
        return content_hash(self.as_dict())


def extract_security_identity(accessor: NorgateIdentityAccessor, symbol: str, *,
                              index_name: str = "S&P 500",
                              membership_start: str = "1990-01-01"
                              ) -> Optional[SecurityIdentity]:
    """Extract ONE survivorship-safe, effective-dated security identity from the
    owned Norgate accessor. Anchors identity on the assetid; derives the security
    life span from first/last quoted date; marks delisting from the last quoted
    date and/or the Norgate delist suffix; and collapses index membership into
    effective-dated intervals. Returns None only when no identity at all is
    observable (accessor degraded) — never a fabricated identity."""
    ident = accessor.security_identity(symbol) or {}
    parsed = parse_norgate_symbol(symbol)
    assetid = ident.get("assetid")
    sec_id = canonical_security_id(assetid, fallback_symbol=parsed["ticker"])
    if sec_id is None:
        return None
    name = ident.get("security_name")
    start = ident.get("first_quoted_date")
    last = ident.get("last_quoted_date")
    # A delisting is evidenced by a non-null last_quoted_date OR the Norgate
    # delist suffix. A live security has neither.
    is_delisted = parsed["is_delisted"] or bool(last)
    delist = last if last else (
        _delist_from_yyyymm(parsed["delist_yyyymm"]) if parsed["is_delisted"]
        else None)
    intervals = accessor.membership_intervals(symbol, index_name,
                                              membership_start)
    ticker = parsed["ticker"]
    # Effective-dated history seeded from the owned observation. The vendor API
    # exposes only current values, so history begins as a single interval over
    # the security's life; later dated observations append genuine intervals
    # (append-only) rather than overwriting.
    end = delist or None
    thist = [{"ticker": ticker, "effective_start": start,
              "effective_end": end, "source": "norgate_local"}]
    nhist = ([{"name": name, "effective_start": start, "effective_end": end,
               "source": "norgate_local"}] if name else [])
    return SecurityIdentity(
        security_id=sec_id, norgate_assetid=(str(assetid) if assetid is not None
                                             else None),
        norgate_symbol=parsed["norgate_symbol"], ticker=ticker,
        issuer_name=name, share_class=derive_share_class(name,
                                                         ident.get("subtype1")),
        base_type=ident.get("base_type"), exchange=ident.get("exchange_name"),
        security_start_date=start, security_end_date=end, delisting_date=delist,
        is_current=not is_delisted, ticker_history=thist, name_history=nhist,
        membership_intervals=intervals)


def _delist_from_yyyymm(yyyymm: Optional[str]) -> Optional[str]:
    if not yyyymm or len(yyyymm) != 6:
        return None
    return "%s-%s-01" % (yyyymm[:4], yyyymm[4:6])


# --------------------------------------------------------------------------- #
# SEC submissions identity parsing (Part 3, genuinely new). Extracts issuer
# name history (formerNames with from/to dates), the current tickers array,
# exchanges, SIC and life-cycle dates from an owned SEC submissions JSON doc.
# Nothing in the existing collectors parses formerNames — this is new capability
# used ONLY as CIK-resolution evidence, never to fabricate a mapping.
# --------------------------------------------------------------------------- #
def parse_submissions_identity(doc: dict) -> Optional[dict]:
    """Parse an SEC submissions JSON document into issuer identity evidence:
    cik, current legal name, formerNames (name, from, to), current tickers /
    exchanges, sic, and first/last filing dates. Returns None if no CIK is
    present. Deterministic; read-only."""
    if not isinstance(doc, dict):
        return None
    cik = norm_cik(doc.get("cik"))
    if cik is None:
        return None
    former = []
    for fn in (doc.get("formerNames") or []):
        if isinstance(fn, dict) and fn.get("name"):
            former.append({"name": str(fn["name"]).strip(),
                           "from": _iso(fn.get("from")),
                           "to": _iso(fn.get("to"))})
    tickers = [t for t in (doc.get("tickers") or []) if t]
    exchanges = [e for e in (doc.get("exchanges") or []) if e]
    return {
        "cik": cik, "name": (str(doc.get("name")).strip()
                             if doc.get("name") else None),
        "former_names": former, "tickers": [str(t).upper() for t in tickers],
        "exchanges": [str(e) for e in exchanges],
        "sic": (str(doc.get("sic")) if doc.get("sic") else None),
        "sic_description": doc.get("sicDescription"),
    }


# --------------------------------------------------------------------------- #
# Deterministic matching contract (Part 3). Ticker text alone is never
# sufficient; ambiguous matches remain UNRESOLVED; every resolved mapping keeps
# its supporting evidence.
# --------------------------------------------------------------------------- #
@dataclass
class MappingResult:
    security_id: str
    cik: Optional[str]
    method: str
    tier: int
    confidence: float
    status: str
    effective_start: Optional[str]
    effective_end: Optional[str]
    evidence: dict
    conflict: Optional[dict] = None

    def as_dict(self) -> dict:
        return {"security_id": self.security_id, "cik": self.cik,
                "method": self.method, "tier": self.tier,
                "confidence": self.confidence, "status": self.status,
                "effective_start": self.effective_start,
                "effective_end": self.effective_end, "evidence": self.evidence,
                "conflict": self.conflict}


def _overlaps(a_start, a_end, b_start, b_end) -> bool:
    """Whether two date intervals overlap (None bounds are open)."""
    a0 = a_start or "0000-00-00"
    a1 = a_end or _OPEN_DATE
    b0 = b_start or "0000-00-00"
    b1 = b_end or _OPEN_DATE
    return a0 <= b1 and b0 <= a1


def match_security_to_cik(security: dict, *,
                          owned_authoritative: Optional[dict] = None,
                          direct_norgate_sec: Optional[dict] = None,
                          ticker_cik_index: Optional[dict] = None,
                          submissions_by_cik: Optional[dict] = None,
                          repair_rules: Optional[dict] = None) -> MappingResult:
    """Resolve one security to a SEC CIK via the deterministic tier order. Every
    resolving tier requires corroborating evidence BEYOND ticker text; ticker
    reuse across unrelated issuers, current-ticker / current-CIK substitution
    without date evidence, and share-class collapse are all prevented because the
    identity is the assetid and every candidate is validated by effective-date
    overlap. When more than one distinct CIK survives, the result is AMBIGUOUS /
    CONFLICT and remains unresolved — a mapping is never guessed.

    Inputs (all owned, all optional — an absent source simply cannot resolve):
      * ``owned_authoritative``: {security_id -> cik} an owned authoritative map.
      * ``direct_norgate_sec``: {assetid -> cik} a direct owned Norgate->SEC id.
      * ``ticker_cik_index``: {ticker -> [cik, ...]} owned ticker->CIK evidence
        (e.g. from owned SEC records); used ONLY with date-overlap corroboration.
      * ``submissions_by_cik``: {cik -> parsed submissions identity} for legal-
        name overlap and to corroborate a ticker candidate by filing dates.
      * ``repair_rules``: {security_id -> {cik, note}} explicit audited repairs.
    """
    sec_id = security.get("security_id")
    assetid = security.get("norgate_assetid")
    ticker = _norm_ticker(security.get("ticker"))
    life_start = security.get("security_start_date")
    life_end = security.get("security_end_date")
    # Normalize the submissions index keys to the canonical 10-digit CIK so
    # corroboration lookups never silently miss on zero-padding differences.
    submissions_by_cik = {norm_cik(k): v
                          for k, v in (submissions_by_cik or {}).items()
                          if norm_cik(k)} if submissions_by_cik else {}
    names = [security.get("issuer_name")] + [
        h.get("name") for h in (security.get("name_history") or [])]
    names = [n for n in names if n]

    # TIER 1 — authoritative owned security/issuer identifier.
    if owned_authoritative and sec_id in owned_authoritative:
        cik = norm_cik(owned_authoritative[sec_id])
        if cik:
            return MappingResult(
                sec_id, cik, METHOD_OWNED_AUTHORITATIVE,
                TIER_OWNED_AUTHORITATIVE, _TIER_CONFIDENCE[
                    TIER_OWNED_AUTHORITATIVE], STATUS_RESOLVED, life_start,
                life_end, {"tier": 1, "source": "owned_authoritative_map"})

    # TIER 2 — direct effective-dated Norgate->SEC identifier.
    if direct_norgate_sec and assetid is not None and \
            str(assetid) in direct_norgate_sec:
        cik = norm_cik(direct_norgate_sec[str(assetid)])
        if cik:
            return MappingResult(
                sec_id, cik, METHOD_DIRECT_NORGATE_SEC, TIER_DIRECT_NORGATE_SEC,
                _TIER_CONFIDENCE[TIER_DIRECT_NORGATE_SEC], STATUS_RESOLVED,
                life_start, life_end,
                {"tier": 2, "source": "direct_norgate_sec_map",
                 "assetid": str(assetid)})

    # TIER 3 — SEC filing identity with a date-overlapping historical ticker.
    # Ticker text is a CANDIDATE GENERATOR only; it resolves solely when the
    # candidate CIK's filing life overlaps the security's life (corroboration).
    if ticker_cik_index and ticker:
        cand = sorted({c for c in (norm_cik(x) for x in
                                   (ticker_cik_index.get(ticker) or [])) if c})
        overlapping = []
        for c in cand:
            sub = (submissions_by_cik or {}).get(c)
            if sub is None:
                # No corroborating filing evidence -> ticker text alone; refuse.
                continue
            f0, f1 = sub.get("first_filing"), sub.get("last_filing")
            if _overlaps(life_start, life_end, f0, f1):
                overlapping.append(c)
        if len(overlapping) == 1:
            return MappingResult(
                sec_id, overlapping[0], METHOD_FILING_TICKER_OVERLAP,
                TIER_FILING_TICKER_OVERLAP,
                _TIER_CONFIDENCE[TIER_FILING_TICKER_OVERLAP], STATUS_RESOLVED,
                life_start, life_end,
                {"tier": 3, "ticker": ticker, "cik": overlapping[0],
                 "corroboration": "filing_date_overlap"})
        if len(overlapping) > 1:
            return MappingResult(
                sec_id, None, METHOD_UNRESOLVED, TIER_UNRESOLVED_BACKLOG, 0.0,
                STATUS_CONFLICT, life_start, life_end,
                {"tier": 3, "ticker": ticker, "reason": "ticker_reuse"},
                conflict={"ticker": ticker, "candidate_ciks": overlapping,
                          "reason": "multiple date-overlapping CIKs share ticker"})
        # A CURRENT (non-delisted) security with a UNIQUE owned current ticker->
        # CIK is authoritatively resolvable from the current listing: it is not
        # survivorship-biased (a delisted name never reaches this branch — it has
        # no current ticker->CIK entry), and it can never unlock the gate because
        # the mapping-coverage denominator is the full survivorship-safe universe
        # (current + delisted). A DELISTED security requires filing corroboration
        # (above) and otherwise stays UNRESOLVED — ticker text alone is never
        # sufficient for a historical (delisted) identity.
        if bool(security.get("is_current")) and len(cand) == 1 and \
                not (submissions_by_cik and cand[0] in submissions_by_cik):
            return MappingResult(
                sec_id, cand[0], METHOD_FILING_TICKER_OVERLAP,
                TIER_FILING_TICKER_OVERLAP,
                _TIER_CONFIDENCE[TIER_FILING_TICKER_OVERLAP], STATUS_RESOLVED,
                life_start, life_end,
                {"tier": 3, "ticker": ticker, "cik": cand[0],
                 "corroboration": "current_listing_unique_ticker_cik"})
        if len(cand) > 1:
            # Multiple CIKs share the ticker but none corroborated -> ambiguous.
            return MappingResult(
                sec_id, None, METHOD_UNRESOLVED, TIER_UNRESOLVED_BACKLOG, 0.0,
                STATUS_AMBIGUOUS, life_start, life_end,
                {"tier": 3, "ticker": ticker,
                 "reason": "ticker_shared_no_corroboration",
                 "candidate_ciks": cand})

    # TIER 4 — historical legal-name match with effective-date overlap.
    if submissions_by_cik and names:
        name_hits = []
        norm_names = {_norm_name(n) for n in names}
        for c, sub in submissions_by_cik.items():
            c = norm_cik(c)
            if c is None:
                continue
            sub_names = [sub.get("name")] + [
                fn.get("name") for fn in (sub.get("former_names") or [])]
            if norm_names & {_norm_name(x) for x in sub_names if x}:
                f0, f1 = sub.get("first_filing"), sub.get("last_filing")
                if _overlaps(life_start, life_end, f0, f1):
                    name_hits.append(c)
        name_hits = sorted(set(name_hits))
        if len(name_hits) == 1:
            return MappingResult(
                sec_id, name_hits[0], METHOD_LEGAL_NAME_OVERLAP,
                TIER_LEGAL_NAME_OVERLAP,
                _TIER_CONFIDENCE[TIER_LEGAL_NAME_OVERLAP], STATUS_RESOLVED,
                life_start, life_end,
                {"tier": 4, "matched_names": sorted(norm_names),
                 "cik": name_hits[0]})
        if len(name_hits) > 1:
            return MappingResult(
                sec_id, None, METHOD_UNRESOLVED, TIER_UNRESOLVED_BACKLOG, 0.0,
                STATUS_AMBIGUOUS, life_start, life_end,
                {"tier": 4, "reason": "legal_name_shared"},
                conflict={"candidate_ciks": name_hits,
                          "reason": "legal name overlaps multiple CIKs"})

    # TIER 5 — explicit audited repair rule (human-audited, recorded).
    if repair_rules and sec_id in repair_rules:
        rr = repair_rules[sec_id] or {}
        cik = norm_cik(rr.get("cik"))
        if cik:
            return MappingResult(
                sec_id, cik, METHOD_AUDITED_REPAIR, TIER_AUDITED_REPAIR,
                _TIER_CONFIDENCE[TIER_AUDITED_REPAIR], STATUS_RESOLVED,
                life_start, life_end,
                {"tier": 5, "audited_repair": rr.get("note", "audited repair")})

    # TIER 6 — unresolved manual-review backlog (no fabrication).
    return MappingResult(
        sec_id, None, METHOD_UNRESOLVED, TIER_UNRESOLVED_BACKLOG, 0.0,
        STATUS_UNRESOLVED, life_start, life_end,
        {"tier": 6, "reason": "no owned survivorship-safe evidence resolves this "
                              "security to a CIK",
         "required_evidence": ["owned direct Norgate->SEC id",
                               "date-overlapping owned ticker->CIK filing",
                               "owned SEC legal-name/formerNames history"]})


def _norm_name(name: Optional[str]) -> str:
    """Normalize a legal name for comparison: upper, strip common suffixes and
    punctuation. Conservative — only collapses obvious corporate-suffix noise so
    genuinely different issuers never collapse together."""
    n = (name or "").upper()
    for ch in ".,/-&":
        n = n.replace(ch, " ")
    tokens = [t for t in n.split() if t]
    drop = {"INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LLC", "LP",
            "PLC", "THE", "HOLDINGS", "HOLDING", "GROUP", "CLASS", "COMMON",
            "NEW", "SA", "NV", "AG"}
    kept = [t for t in tokens if t not in drop]
    return " ".join(kept) if kept else " ".join(tokens)


# --------------------------------------------------------------------------- #
# Canonical identity store (Part 5) — durable, append-only, versioned.
# --------------------------------------------------------------------------- #
_STORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS identity_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS securities (
    security_id TEXT PRIMARY KEY,
    norgate_assetid TEXT,
    norgate_symbol TEXT NOT NULL,
    ticker TEXT NOT NULL,
    issuer_name TEXT,
    share_class TEXT NOT NULL DEFAULT 'COMMON',
    base_type TEXT,
    exchange TEXT,
    security_start_date TEXT,
    security_end_date TEXT,
    delisting_date TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    source TEXT NOT NULL DEFAULT 'norgate_local',
    content_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sec_ticker ON securities(ticker);
CREATE INDEX IF NOT EXISTS ix_sec_current ON securities(is_current);
CREATE TABLE IF NOT EXISTS ticker_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    effective_start TEXT,
    effective_end TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(security_id, ticker, effective_start)
);
CREATE INDEX IF NOT EXISTS ix_th_ticker ON ticker_history(ticker);
CREATE TABLE IF NOT EXISTS name_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT NOT NULL,
    name TEXT NOT NULL,
    effective_start TEXT,
    effective_end TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(security_id, name, effective_start)
);
CREATE TABLE IF NOT EXISTS index_membership (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    member_start TEXT NOT NULL,
    member_end TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(security_id, index_name, member_start)
);
CREATE INDEX IF NOT EXISTS ix_mem_index ON index_membership(index_name);
CREATE TABLE IF NOT EXISTS cik_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT NOT NULL,
    cik TEXT,
    method TEXT NOT NULL,
    tier INTEGER NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    effective_start TEXT,
    effective_end TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    mapping_version_hash TEXT NOT NULL,
    source_artifact_hash TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    superseded_by INTEGER,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_cik_sec ON cik_map(security_id, active);
CREATE INDEX IF NOT EXISTS ix_cik_status ON cik_map(status, active);
CREATE TABLE IF NOT EXISTS mapping_conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS unresolved_backlog (
    security_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    reason TEXT NOT NULL,
    required_evidence TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mapping_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    security_id TEXT,
    before_json TEXT,
    after_json TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS coverage_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    total_securities INTEGER NOT NULL,
    current_securities INTEGER NOT NULL,
    delisted_securities INTEGER NOT NULL,
    resolved INTEGER NOT NULL,
    unresolved INTEGER NOT NULL,
    ambiguous INTEGER NOT NULL,
    conflict INTEGER NOT NULL,
    mapped_ciks INTEGER NOT NULL,
    by_date_json TEXT,
    detail_json TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_jobs (
    job_key TEXT PRIMARY KEY,
    job_id TEXT,
    kind TEXT,
    recorded_at TEXT NOT NULL
);
"""


class IdentityStore:
    """Durable, append-only, versioned canonical identity store. One stdlib
    sqlite file under the Stage 8 research root on the data drive — NEVER under
    an operational-ledger root, so it can never change an operational-ledger
    fingerprint. Atomic writes (BEGIN IMMEDIATE), deterministic hashes, restart-
    safe, idempotent replay (content-hash on securities + UNIQUE history
    constraints + processed_jobs), append-only revisions with exact provenance.
    ``clock`` is an injectable ``() -> iso`` for deterministic tests."""

    def __init__(self, db_path: str | Path, *,
                 clock: Optional[Callable[[], str]] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_STORE_SCHEMA)
                conn.execute(
                    "INSERT OR IGNORE INTO identity_meta(key,value) VALUES(?,?)",
                    ("schema_version", SCHEMA_VERSION))
                conn.execute(
                    "INSERT OR IGNORE INTO identity_meta(key,value) VALUES(?,?)",
                    ("active_mapping_version", MAPPING_ALGORITHM_VERSION))
            finally:
                conn.close()

    # -- meta --------------------------------------------------------------- #
    def get_meta(self, key: str) -> Optional[str]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT value FROM identity_meta WHERE key=?",
                               (key,)).fetchone()
            return row["value"] if row else None
        finally:
            conn.close()

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO identity_meta(key,value) VALUES(?,?) ON "
                    "CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, str(value)))
            finally:
                conn.close()

    # -- idempotent replay -------------------------------------------------- #
    def already_processed(self, job_key: str) -> bool:
        conn = self._connect()
        try:
            return conn.execute("SELECT 1 FROM processed_jobs WHERE job_key=?",
                                (job_key,)).fetchone() is not None
        finally:
            conn.close()

    def mark_processed(self, job_key: str, *, job_id: Optional[str] = None,
                       kind: Optional[str] = None) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_jobs(job_key,job_id,kind,"
                    "recorded_at) VALUES(?,?,?,?)",
                    (job_key, job_id, kind, self._clock()))
            finally:
                conn.close()

    # -- security upsert (append-only history) ------------------------------ #
    def upsert_security(self, ident: SecurityIdentity) -> dict:
        """Idempotently persist one security identity + append its ticker / name
        / membership intervals. A security row is written once per content hash
        (an unchanged re-observation is a no-op); a genuinely-changed observation
        appends a revision and updates the mutable row while the append-only
        history tables retain every prior interval. Returns
        {'created','changed','security_id'}."""
        now = self._clock()
        d = ident.as_dict()
        ch = ident.content_hash
        created = changed = False
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT content_hash FROM securities WHERE security_id=?",
                    (ident.security_id,)).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO securities(security_id,norgate_assetid,"
                        "norgate_symbol,ticker,issuer_name,share_class,base_type,"
                        "exchange,security_start_date,security_end_date,"
                        "delisting_date,is_current,source,content_hash,"
                        "first_seen_at,updated_at) VALUES"
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (ident.security_id, ident.norgate_assetid,
                         ident.norgate_symbol, ident.ticker, ident.issuer_name,
                         ident.share_class, ident.base_type, ident.exchange,
                         ident.security_start_date, ident.security_end_date,
                         ident.delisting_date, 1 if ident.is_current else 0,
                         ident.source, ch, now, now))
                    created = True
                    conn.execute(
                        "INSERT INTO mapping_revisions(kind,security_id,"
                        "before_json,after_json,created_at) VALUES(?,?,?,?,?)",
                        ("SECURITY_CREATED", ident.security_id, None,
                         canonical_json(d), now))
                elif row["content_hash"] != ch:
                    conn.execute(
                        "UPDATE securities SET norgate_assetid=?,norgate_symbol=?,"
                        "ticker=?,issuer_name=?,share_class=?,base_type=?,"
                        "exchange=?,security_start_date=?,security_end_date=?,"
                        "delisting_date=?,is_current=?,source=?,content_hash=?,"
                        "updated_at=? WHERE security_id=?",
                        (ident.norgate_assetid, ident.norgate_symbol,
                         ident.ticker, ident.issuer_name, ident.share_class,
                         ident.base_type, ident.exchange,
                         ident.security_start_date, ident.security_end_date,
                         ident.delisting_date, 1 if ident.is_current else 0,
                         ident.source, ch, now, ident.security_id))
                    changed = True
                    conn.execute(
                        "INSERT INTO mapping_revisions(kind,security_id,"
                        "before_json,after_json,created_at) VALUES(?,?,?,?,?)",
                        ("SECURITY_REVISED", ident.security_id, None,
                         canonical_json(d), now))
                # Append-only history (UNIQUE guards make this idempotent).
                for h in ident.ticker_history:
                    conn.execute(
                        "INSERT OR IGNORE INTO ticker_history(security_id,ticker,"
                        "effective_start,effective_end,source,created_at) VALUES"
                        "(?,?,?,?,?,?)",
                        (ident.security_id, _norm_ticker(h.get("ticker")),
                         h.get("effective_start"), h.get("effective_end"),
                         h.get("source", ident.source), now))
                for h in ident.name_history:
                    if not h.get("name"):
                        continue
                    conn.execute(
                        "INSERT OR IGNORE INTO name_history(security_id,name,"
                        "effective_start,effective_end,source,created_at) VALUES"
                        "(?,?,?,?,?,?)",
                        (ident.security_id, h["name"], h.get("effective_start"),
                         h.get("effective_end"), h.get("source", ident.source),
                         now))
                for m in ident.membership_intervals:
                    if not m.get("member_start"):
                        continue
                    conn.execute(
                        "INSERT OR IGNORE INTO index_membership(security_id,"
                        "index_name,member_start,member_end,source,created_at)"
                        " VALUES(?,?,?,?,?,?)",
                        (ident.security_id, m.get("index_name", "S&P 500"),
                         m["member_start"], m.get("member_end"),
                         m.get("source", ident.source), now))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return {"created": created, "changed": changed,
                "security_id": ident.security_id}

    # -- cik mapping (append-only, versioned, supersession) ----------------- #
    def record_mapping(self, result: MappingResult, *,
                       source_artifact_hash: Optional[str] = None) -> dict:
        """Append one mapping result. The prior ACTIVE mapping for the security
        (if any and if it differs) is SUPERSEDED (active=0, superseded_by=new,
        status stamped SUPERSEDED) — never deleted, so every prior evidence row
        stays queryable. A CONFLICT / AMBIGUOUS result also records a
        mapping_conflicts row and an unresolved_backlog entry. Idempotent: an
        identical active mapping (same cik+status+method) is a no-op. Returns
        {'inserted','superseded_prior','status'}."""
        now = self._clock()
        r = result
        inserted = superseded = False
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                prior = conn.execute(
                    "SELECT id,cik,status,method FROM cik_map WHERE security_id=?"
                    " AND active=1 ORDER BY id DESC LIMIT 1",
                    (r.security_id,)).fetchone()
                same = (prior is not None and (prior["cik"] or "") ==
                        (r.cik or "") and prior["status"] == r.status and
                        prior["method"] == r.method)
                if not same:
                    if prior is not None:
                        conn.execute(
                            "UPDATE cik_map SET active=0, status=?, "
                            "superseded_by=NULL WHERE id=?",
                            (STATUS_SUPERSEDED, prior["id"]))
                        superseded = True
                    cur = conn.execute(
                        "INSERT INTO cik_map(security_id,cik,method,tier,"
                        "confidence,status,effective_start,effective_end,"
                        "evidence_json,mapping_version_hash,source_artifact_hash,"
                        "active,superseded_by,created_at) VALUES"
                        "(?,?,?,?,?,?,?,?,?,?,?,1,NULL,?)",
                        (r.security_id, r.cik, r.method, r.tier, r.confidence,
                         r.status, r.effective_start, r.effective_end,
                         canonical_json(r.evidence), MAPPING_ALGORITHM_VERSION,
                         source_artifact_hash, now))
                    new_id = cur.lastrowid
                    inserted = True
                    if superseded:
                        conn.execute("UPDATE cik_map SET superseded_by=? WHERE "
                                     "id=?", (new_id, prior["id"]))
                    conn.execute(
                        "INSERT INTO mapping_revisions(kind,security_id,"
                        "before_json,after_json,created_at) VALUES(?,?,?,?,?)",
                        ("MAPPING_RECORDED", r.security_id,
                         canonical_json({"prior_id": prior["id"] if prior
                                         else None}),
                         canonical_json(r.as_dict()), now))
                if r.conflict is not None:
                    conn.execute(
                        "INSERT INTO mapping_conflicts(security_id,detail_json,"
                        "created_at) VALUES(?,?,?)",
                        (r.security_id, canonical_json(r.conflict), now))
                if r.status in (STATUS_UNRESOLVED, STATUS_AMBIGUOUS,
                                STATUS_CONFLICT):
                    conn.execute(
                        "INSERT INTO unresolved_backlog(security_id,symbol,"
                        "reason,required_evidence,updated_at) VALUES(?,?,?,?,?) "
                        "ON CONFLICT(security_id) DO UPDATE SET reason=excluded."
                        "reason, required_evidence=excluded.required_evidence, "
                        "updated_at=excluded.updated_at",
                        (r.security_id, _norm_ticker(r.evidence.get("ticker")
                                                     or r.security_id),
                         r.status, canonical_json(
                             r.evidence.get("required_evidence")), now))
                else:
                    conn.execute("DELETE FROM unresolved_backlog WHERE "
                                 "security_id=?", (r.security_id,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return {"inserted": inserted, "superseded_prior": superseded,
                "status": r.status}

    # -- reads -------------------------------------------------------------- #
    def get_security(self, security_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM securities WHERE security_id=?",
                               (security_id,)).fetchone()
            if row is None:
                return None
            out = dict(row)
            out["ticker_history"] = [dict(r) for r in conn.execute(
                "SELECT ticker,effective_start,effective_end,source FROM "
                "ticker_history WHERE security_id=? ORDER BY effective_start",
                (security_id,))]
            out["name_history"] = [dict(r) for r in conn.execute(
                "SELECT name,effective_start,effective_end,source FROM "
                "name_history WHERE security_id=? ORDER BY effective_start",
                (security_id,))]
            out["membership_intervals"] = [dict(r) for r in conn.execute(
                "SELECT index_name,member_start,member_end,source FROM "
                "index_membership WHERE security_id=? ORDER BY member_start",
                (security_id,))]
            out["active_mapping"] = self.active_mapping(security_id)
            return out
        finally:
            conn.close()

    def active_mapping(self, security_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM cik_map WHERE security_id=? AND active=1 ORDER BY "
                "id DESC LIMIT 1", (security_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def mapping_history(self, security_id: str) -> "list[dict]":
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM cik_map WHERE security_id=? ORDER BY id",
                (security_id,))]
        finally:
            conn.close()

    def list_securities(self, *, is_current: Optional[bool] = None,
                        unmapped_only: bool = False, limit: int = 100000
                        ) -> "list[dict]":
        conn = self._connect()
        try:
            sql = "SELECT * FROM securities"
            clauses, params = [], []
            if is_current is not None:
                clauses.append("is_current=?")
                params.append(1 if is_current else 0)
            if unmapped_only:
                clauses.append(
                    "security_id NOT IN (SELECT security_id FROM cik_map WHERE "
                    "active=1 AND status='RESOLVED')")
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY security_id LIMIT ?"
            params.append(int(limit))
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def counts(self) -> dict:
        conn = self._connect()
        try:
            total = int(conn.execute(
                "SELECT COUNT(*) c FROM securities").fetchone()["c"])
            current = int(conn.execute(
                "SELECT COUNT(*) c FROM securities WHERE is_current=1"
            ).fetchone()["c"])
            by_status = {s: 0 for s in MAPPING_STATUSES}
            for row in conn.execute(
                    "SELECT status, COUNT(*) c FROM cik_map WHERE active=1 GROUP "
                    "BY status"):
                by_status[row["status"]] = int(row["c"])
            resolved_ciks = int(conn.execute(
                "SELECT COUNT(DISTINCT cik) c FROM cik_map WHERE active=1 AND "
                "status='RESOLVED' AND cik IS NOT NULL").fetchone()["c"])
            conflicts = int(conn.execute(
                "SELECT COUNT(*) c FROM mapping_conflicts").fetchone()["c"])
            backlog = int(conn.execute(
                "SELECT COUNT(*) c FROM unresolved_backlog").fetchone()["c"])
            return {
                "total_securities": total, "current_securities": current,
                "delisted_securities": total - current,
                "resolved": by_status[STATUS_RESOLVED],
                "unresolved": by_status[STATUS_UNRESOLVED],
                "ambiguous": by_status[STATUS_AMBIGUOUS],
                "conflict": by_status[STATUS_CONFLICT],
                "superseded_active": by_status[STATUS_SUPERSEDED],
                "mapped_ciks": resolved_ciks, "conflict_rows": conflicts,
                "unresolved_backlog": backlog}
        finally:
            conn.close()

    def unresolved(self, *, limit: int = 500) -> "list[dict]":
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM unresolved_backlog ORDER BY security_id LIMIT ?",
                (int(limit),))]
        finally:
            conn.close()

    def conflicts(self, *, limit: int = 500) -> "list[dict]":
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM mapping_conflicts ORDER BY id DESC LIMIT ?",
                (int(limit),))]
        finally:
            conn.close()

    def revisions(self, *, security_id: Optional[str] = None, limit: int = 500
                  ) -> "list[dict]":
        conn = self._connect()
        try:
            if security_id:
                rows = conn.execute(
                    "SELECT * FROM mapping_revisions WHERE security_id=? ORDER BY"
                    " id LIMIT ?", (security_id, int(limit)))
            else:
                rows = conn.execute(
                    "SELECT * FROM mapping_revisions ORDER BY id DESC LIMIT ?",
                    (int(limit),))
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- historical universe service (Part 4) ------------------------------- #
    def historical_universe_on(self, as_of: str, *,
                               index_name: str = "S&P 500") -> "list[dict]":
        """The EXACT eligible historical securities on a formation date, from
        owned Norgate membership intervals with the effective identity. A future
        constituent (member_start > as_of) can never appear; a removed
        constituent (member_end < as_of) disappears; a delisted name stays in the
        prior periods where it was a member. Uses the assetid-anchored security
        identity so a ticker change preserves one identity and ticker reuse keeps
        separate identities. NEVER a current-survivor universe."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT s.security_id,s.ticker,s.issuer_name,s.share_class,"
                "s.norgate_assetid,s.exchange,s.delisting_date,s.is_current,"
                "m.member_start,m.member_end FROM index_membership m JOIN "
                "securities s ON s.security_id=m.security_id WHERE "
                "m.index_name=? AND m.member_start<=? AND (m.member_end IS NULL "
                "OR m.member_end>=?) ORDER BY s.security_id",
                (index_name, as_of, as_of)).fetchall()
            out = []
            seen = set()
            for r in rows:
                if r["security_id"] in seen:
                    continue
                seen.add(r["security_id"])
                mp = self.active_mapping(r["security_id"])
                out.append({
                    "security_id": r["security_id"], "ticker": r["ticker"],
                    "issuer_name": r["issuer_name"],
                    "share_class": r["share_class"],
                    "norgate_assetid": r["norgate_assetid"],
                    "exchange": r["exchange"],
                    "ticker_effective_on": self._ticker_on(conn,
                                                           r["security_id"],
                                                           as_of) or r["ticker"],
                    "member_start": r["member_start"],
                    "member_end": r["member_end"],
                    "delisting_date": r["delisting_date"],
                    "cik": (mp or {}).get("cik"),
                    "mapping_status": (mp or {}).get("status",
                                                     STATUS_UNRESOLVED)})
            return out
        finally:
            conn.close()

    @staticmethod
    def _ticker_on(conn, security_id: str, as_of: str) -> Optional[str]:
        row = conn.execute(
            "SELECT ticker FROM ticker_history WHERE security_id=? AND "
            "(effective_start IS NULL OR effective_start<=?) AND (effective_end "
            "IS NULL OR effective_end>=?) ORDER BY effective_start DESC LIMIT 1",
            (security_id, as_of, as_of)).fetchone()
        return row["ticker"] if row else None

    # -- coverage snapshots (append-only) ----------------------------------- #
    def record_coverage_snapshot(self, *, as_of: str, rebalance_dates=None,
                                 index_name: str = "S&P 500") -> dict:
        """Compute + persist an append-only coverage snapshot: security counts,
        mapping-status counts, distinct resolved CIKs and, for each requested
        rebalance date, the survivorship-safe universe size, mapped-name count
        and mapping-coverage %. Returns the snapshot dict."""
        c = self.counts()
        by_date = {}
        for d in sorted(rebalance_dates or []):
            univ = self.historical_universe_on(d, index_name=index_name)
            n = len(univ)
            mapped = sum(1 for u in univ
                         if u.get("mapping_status") == STATUS_RESOLVED
                         and u.get("cik"))
            by_date[d] = {
                "universe_names": n, "mapped_names": mapped,
                "mapping_coverage_pct": round(100.0 * mapped / n, 4)
                if n else 0.0}
        snap = {
            "as_of": as_of, "mapping_version": MAPPING_ALGORITHM_VERSION,
            "total_securities": c["total_securities"],
            "current_securities": c["current_securities"],
            "delisted_securities": c["delisted_securities"],
            "resolved": c["resolved"], "unresolved": c["unresolved"],
            "ambiguous": c["ambiguous"], "conflict": c["conflict"],
            "mapped_ciks": c["mapped_ciks"], "by_date": by_date}
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO coverage_snapshots(as_of,mapping_version,"
                    "total_securities,current_securities,delisted_securities,"
                    "resolved,unresolved,ambiguous,conflict,mapped_ciks,"
                    "by_date_json,detail_json,created_at) VALUES"
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (as_of, MAPPING_ALGORITHM_VERSION, c["total_securities"],
                     c["current_securities"], c["delisted_securities"],
                     c["resolved"], c["unresolved"], c["ambiguous"],
                     c["conflict"], c["mapped_ciks"], canonical_json(by_date),
                     canonical_json(c), now))
            finally:
                conn.close()
        return snap

    def latest_coverage_snapshot(self) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM coverage_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            out = dict(row)
            out["by_date"] = json.loads(out.pop("by_date_json") or "{}")
            out["detail"] = json.loads(out.pop("detail_json") or "{}")
            return out
        finally:
            conn.close()

    def digest(self) -> str:
        """Deterministic content digest of the resolved identity state (securities
        + active mappings) for restart / idempotency verification."""
        conn = self._connect()
        try:
            secs = [tuple(r) for r in conn.execute(
                "SELECT security_id,content_hash FROM securities ORDER BY "
                "security_id")]
            maps = [tuple(r) for r in conn.execute(
                "SELECT security_id,cik,status,method FROM cik_map WHERE active=1"
                " ORDER BY security_id")]
            return hashlib.sha256(
                canonical_json({"s": secs, "m": maps}).encode()).hexdigest()
        finally:
            conn.close()


__all__ = [
    "STATUS_RESOLVED", "STATUS_AMBIGUOUS", "STATUS_UNRESOLVED", "STATUS_CONFLICT",
    "STATUS_SUPERSEDED", "MAPPING_STATUSES", "MAPPING_ALGORITHM_VERSION",
    "SCHEMA_VERSION", "TIER_OWNED_AUTHORITATIVE", "TIER_DIRECT_NORGATE_SEC",
    "TIER_FILING_TICKER_OVERLAP", "TIER_LEGAL_NAME_OVERLAP", "TIER_AUDITED_REPAIR",
    "TIER_UNRESOLVED_BACKLOG", "NorgateIdentityAccessor", "SecurityIdentity",
    "MappingResult", "IdentityStore", "parse_norgate_symbol",
    "canonical_security_id", "derive_share_class", "extract_security_identity",
    "parse_submissions_identity", "match_security_to_cik", "norm_cik",
    "canonical_json", "content_hash",
]
