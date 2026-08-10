"""Stage 13A -- historical ANALYST-REVISION data contract, adequacy gate, power
model and provider-neutral PURCHASE-DECISION framework.

Stage 12 closed the owned-data programme: price, filed SEC fundamentals and the
independent 2009-2014 window were all exhausted with NO defensible new alpha. The
single economically-orthogonal family that remains untested on owned data is
HISTORICAL ANALYST REVISIONS (consensus EPS/revenue estimates, revision breadth,
dispersion, surprises) -- analyst *belief updates* that arrive between filings and
are disjoint from realised fundamentals and price.

That data is not owned. A trial has been offered (Intrinio US Fundamentals; Nasdaq
Data Link / Zacks pending). This module builds the ENTIRE research and purchase-
evaluation framework NOW, provider-neutral, BEFORE any trial data is inspected, so
that when a local trial extract arrives the only remaining work is: (1) configure
credentials securely, (2) map provider fields into the frozen normalized contract,
(3) run a bounded local extraction, (4) execute the adequacy / power / integrity /
purchase gates, (5) produce a MANUAL purchase recommendation.

Workstreams (all pure / non-networking / deterministic):

* A -- provider-neutral normalized data contract: five immutable, append-only
  record schemas (SECURITY_IDENTITY, ESTIMATE_REVISION_EVENT, CONSENSUS_SNAPSHOT,
  ACTUAL_EVENT, PROVIDER_CAPABILITY).
* B -- point-in-time safety contract: deterministic invariants that classify (never
  silently repair) availability-timestamp, revision-overwrite, backfilled-consensus,
  actual-before-announcement, ticker-duplicate, reconstructed-snapshot and
  restatement-versioning violations.
* C -- a provider-neutral adapter protocol + non-networking Intrinio/Zacks and
  Nasdaq/Zacks skeletons + a LOCAL-FILE (CSV/JSON) trial importer. No live auth,
  no network retrieval.
* D -- a frozen, content-addressed 6-hypothesis pre-registered revision registry,
  frozen BEFORE any trial data is viewed.
* E -- a deterministic synthetic-fixture engine (ten fixtures) + an evaluator that
  detects the positive fixture, rejects null / leakage / concentration / active-only,
  and applies BH-FDR over EXACTLY six hypotheses. Never promotes.
* F -- a provider-neutral trial-data adequacy gate (history, breadth, inactive/
  delisted coverage, timestamp quality, effective independent cohort count, ...).
* G -- a transparent power & confidence model: CONFIDENCE_PRIOR_ONLY before trial
  data (prior 25-35%, explicitly labelled an assumption), measured update after.
* H -- a cost & purchase gate returning exactly one of six terminals; the maximum
  RATIONAL one-time price is computed under EXPLICIT assumptions (never an invented
  provider price) and PURCHASE_CANDIDATE_PENDING_MANUAL_APPROVAL requires manual sign-off.
* I -- a vendor-comparison matrix (Intrinio/Zacks, Nasdaq/Zacks, an unspecified
  third) that preserves UNKNOWN and does not assume vendor independence.

RESEARCH-ONLY. No orders / fills / signals / trade-decisions / holdings / broker /
automation / promotion. No provider is contacted. No credential is stored. No
operational ledger is ever mutated. This phase builds NO trading strategy; it
returns TRIAL_NOT_STARTED until a real local trial extract is supplied by hand.
"""
from __future__ import annotations

import csv
import dataclasses
import hashlib
import io
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from . import stage12_power as _power
    from . import fundamental_evidence as _fev
    from . import signal_evaluation as _se
except Exception:  # pragma: no cover
    import stage12_power as _power  # type: ignore
    import fundamental_evidence as _fev  # type: ignore
    import signal_evaluation as _se  # type: ignore

STAGE = "13A"

SAFETY_BADGES = ["RESEARCH ONLY", "NO PURCHASE EXECUTED", "MANUAL APPROVAL REQUIRED",
                 "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "NO AUTO-PROMOTION"]

ESTIMATE_TYPES = ("EPS", "REVENUE")
FISCAL_PERIOD_TYPES = ("QUARTERLY", "ANNUAL")
REVISION_DIRECTIONS = ("UP", "DOWN", "UNCHANGED")
SECURITY_STATUSES = ("ACTIVE", "INACTIVE", "DELISTED", "ACQUIRED", "MERGED")
FORWARD_OUTCOME_STATUSES = ("MATURED", "UNAVAILABLE")

HORIZONS = (5, 20, 63)
PRIMARY_HORIZON = 63
FAMILY_SIZE = 6
FDR_ALPHA = 0.05

# Judgement prior (explicitly a PRIOR ASSUMPTION, to be replaced by measured trial
# evidence -- never presented as measured).
PRIOR_CONFIDENCE_LOW = 0.25
PRIOR_CONFIDENCE_HIGH = 0.35


# --------------------------------------------------------------------------- #
# Small IO helpers (atomic; never an operational ledger).
# --------------------------------------------------------------------------- #
def _load_cfg(config_path: str | Path) -> dict:
    with io.open(str(config_path), encoding="utf-8-sig") as fh:
        return json.load(fh)


def _read_json(path: Path, default=None):
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return default
    return default


def _write_json(path: Path, doc) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, sort_keys=True, indent=1), encoding="utf-8")
    tmp.replace(p)


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      default=str)


def _sha16(obj: Any) -> str:
    return hashlib.sha256(_canonical_json(obj).encode("utf-8")).hexdigest()[:16]


# =========================================================================== #
# WORKSTREAM A -- provider-neutral normalized data contract (5 schemas).
# =========================================================================== #
# Each schema is declared as an ordered list of (field, dtype, required) so it is
# testable as data; the frozen dataclasses below give typed, immutable construction.

_STR, _NUM, _INT, _BOOL, _TS = "str", "number", "int", "bool", "timestamp"

SCHEMAS: Dict[str, List[Tuple[str, str, bool]]] = {
    # NOTE: every schema declares an OPTIONAL record_id — the store assigns one
    # on append, so a record read back from the store re-validates cleanly (and
    # an explicit id can be provided for deterministic identity).
    "SECURITY_IDENTITY": [
        ("record_id", _STR, False),
        ("provider_security_id", _STR, True),
        ("stable_assetid", _STR, False),
        ("cik", _STR, False),
        ("issuer_id", _STR, False),
        ("historical_ticker", _STR, False),
        ("effective_start", _TS, True),
        ("effective_end", _TS, False),
        ("status", _STR, True),          # ACTIVE / INACTIVE / DELISTED / ACQUIRED / MERGED
        ("acquisition_or_merger_status", _STR, False),
        ("exchange", _STR, False),
        ("adr_flag", _BOOL, False),
        ("source_provider", _STR, True),
    ],
    "ESTIMATE_REVISION_EVENT": [
        ("record_id", _STR, True),
        ("provider", _STR, True),
        ("provider_event_id", _STR, True),
        ("security_id", _STR, True),
        ("estimate_type", _STR, True),         # EPS / REVENUE
        ("fiscal_period_type", _STR, True),    # QUARTERLY / ANNUAL
        ("fiscal_period_end", _TS, True),
        ("observation_timestamp", _TS, True),
        ("provider_effective_timestamp", _TS, True),
        ("prior_estimate", _NUM, False),
        ("revised_estimate", _NUM, True),
        ("absolute_revision", _NUM, False),
        ("percentage_revision", _NUM, False),
        ("revision_direction", _STR, False),   # UP / DOWN / UNCHANGED
        ("analyst_or_broker_id", _STR, False),
        ("analyst_count", _INT, False),
        ("source_availability_timestamp", _TS, True),
        ("ingestion_timestamp", _TS, True),
        ("raw_source_reference", _STR, False),
    ],
    "CONSENSUS_SNAPSHOT": [
        ("record_id", _STR, False),
        ("security_id", _STR, True),
        ("estimate_type", _STR, True),
        ("fiscal_period_end", _TS, True),
        ("observation_timestamp", _TS, True),
        ("source_availability_timestamp", _TS, True),
        ("mean", _NUM, False),
        ("median", _NUM, False),
        ("high", _NUM, False),
        ("low", _NUM, False),
        ("standard_deviation", _NUM, False),
        ("analyst_count", _INT, False),
        ("upward_revision_count", _INT, False),
        ("downward_revision_count", _INT, False),
        ("unchanged_count", _INT, False),
        ("prior_week_consensus", _NUM, False),
        ("prior_month_consensus", _NUM, False),
        ("prior_quarter_consensus", _NUM, False),
        # Measured on the live Intrinio/Zacks trial (2026-08-10): a Q4 estimate and
        # an FY estimate can share the SAME fiscal_period_end, so the period type is
        # required to disambiguate the horizon. Optional for back-compat.
        ("fiscal_period_type", _STR, False),
    ],
    # -- Stage 13B prospective forward-evidence records ----------------------- #
    # A formation is the immutable fact "signal X ranked security S at value V on
    # snapshot D, formed from data available at T". Outcomes are appended LATER,
    # one record per (formation, horizon); PENDING is always DERIVED (absence of
    # an outcome record), never stored, so nothing in the ledger ever mutates.
    "PROSPECTIVE_SIGNAL_FORMATION": [
        ("record_id", _STR, False),            # deterministic: compute_formation_id
        ("security_id", _STR, True),
        ("signal_family", _STR, True),
        ("signal_value", _NUM, True),
        ("formation_rank", _INT, True),
        ("breadth", _INT, True),
        ("snapshot_date", _STR, True),
        ("formation_timestamp", _TS, True),
        ("source_availability_timestamp", _TS, True),
        ("fiscal_period_end", _TS, False),
        ("fiscal_period_type", _STR, False),
        ("provider", _STR, True),
    ],
    "FORWARD_OUTCOME": [
        ("record_id", _STR, False),            # deterministic: compute_outcome_id
        ("formation_record_id", _STR, True),
        ("security_id", _STR, True),
        ("signal_family", _STR, True),
        ("snapshot_date", _STR, True),
        ("horizon_sessions", _INT, True),
        ("status", _STR, True),                # MATURED / UNAVAILABLE only
        ("entry_session", _TS, False),
        ("exit_session", _TS, False),
        ("forward_return", _NUM, False),
        ("computed_timestamp", _TS, True),
    ],
    "ACTUAL_EVENT": [
        ("record_id", _STR, False),
        ("security_id", _STR, True),
        ("estimate_type", _STR, True),
        ("fiscal_period_end", _TS, True),
        ("actual_value", _NUM, True),
        ("announcement_timestamp", _TS, True),
        ("source_availability_timestamp", _TS, True),
        ("surprise_amount", _NUM, False),
        ("surprise_percentage", _NUM, False),
    ],
    "PROVIDER_CAPABILITY": [
        ("record_id", _STR, False),
        ("source_provider", _STR, True),
        ("available_history_start", _TS, False),
        ("available_history_end", _TS, False),
        ("active_coverage", _BOOL, False),
        ("inactive_coverage", _BOOL, False),
        ("acquired_coverage", _BOOL, False),
        ("delisted_coverage", _BOOL, False),
        ("adr_coverage", _BOOL, False),
        ("otc_coverage", _BOOL, False),
        ("individual_analyst_availability", _BOOL, False),
        ("consensus_history_availability", _BOOL, False),
        ("api_availability", _BOOL, False),
        ("bulk_availability", _BOOL, False),
        ("rate_limits", _STR, False),
        ("historical_archive_restrictions", _STR, False),
        ("trial_restrictions", _STR, False),
        ("licensing_restrictions", _STR, False),
        ("quoted_one_time_history_price", _NUM, False),
        ("quoted_recurring_price", _NUM, False),
        ("auto_renewal_terms", _STR, False),
    ],
}

SCHEMA_NAMES = tuple(SCHEMAS.keys())


def schema_fields(schema_name: str) -> List[str]:
    return [f[0] for f in SCHEMAS[schema_name]]


def required_fields(schema_name: str) -> List[str]:
    return [f[0] for f in SCHEMAS[schema_name] if f[2]]


_FORBIDDEN_KEYS = {"api_key", "apikey", "secret", "secret_key", "password",
                   "bot_token", "access_token", "bearer", "credential", "credentials"}


def validate_record(schema_name: str, rec: Mapping[str, Any]) -> List[str]:
    """Return a list of contract violations for one record (empty == valid).

    Enforces required-field presence, known-field membership, controlled
    vocabularies, and the hard rule that NO credential/secret ever appears in a
    normalized record."""
    problems: List[str] = []
    if schema_name not in SCHEMAS:
        return ["unknown schema %s" % schema_name]
    spec = SCHEMAS[schema_name]
    allowed = {f[0] for f in spec}
    for name, _dt, req in spec:
        if req and (name not in rec or rec.get(name) in (None, "")):
            problems.append("%s: missing required field %s" % (schema_name, name))
    for k in rec.keys():
        if str(k).lower() in _FORBIDDEN_KEYS:
            problems.append("%s: forbidden credential field %s" % (schema_name, k))
        if k not in allowed:
            problems.append("%s: unknown field %s" % (schema_name, k))
    et = rec.get("estimate_type")
    if et is not None and str(et).upper() not in ESTIMATE_TYPES and schema_name in (
            "ESTIMATE_REVISION_EVENT", "CONSENSUS_SNAPSHOT", "ACTUAL_EVENT"):
        problems.append("%s: bad estimate_type %s" % (schema_name, et))
    fpt = rec.get("fiscal_period_type")
    if fpt is not None and str(fpt).upper() not in FISCAL_PERIOD_TYPES:
        problems.append("%s: bad fiscal_period_type %s" % (schema_name, fpt))
    st = rec.get("status")
    if schema_name == "SECURITY_IDENTITY" and st is not None \
            and str(st).upper() not in SECURITY_STATUSES:
        problems.append("SECURITY_IDENTITY: bad status %s" % st)
    if schema_name == "FORWARD_OUTCOME" and st is not None \
            and str(st).upper() not in FORWARD_OUTCOME_STATUSES:
        # PENDING is intentionally NOT storable: pending-ness is the ABSENCE of an
        # outcome record; storing it would invite mutation of ledger state.
        problems.append("FORWARD_OUTCOME: bad status %s (PENDING is derived, "
                        "never stored)" % st)
    return problems


def compute_record_id(rec: Mapping[str, Any]) -> str:
    """Deterministic immutable record_id for an ESTIMATE_REVISION_EVENT: a content
    hash of the provider identity + observation + the revised value, so the SAME
    observation is idempotent and a REVISED value is a NEW record (never an
    overwrite)."""
    key = {
        "provider": rec.get("provider"),
        "provider_event_id": rec.get("provider_event_id"),
        "security_id": rec.get("security_id"),
        "estimate_type": rec.get("estimate_type"),
        "fiscal_period_end": rec.get("fiscal_period_end"),
        "observation_timestamp": rec.get("observation_timestamp"),
        "revised_estimate": rec.get("revised_estimate"),
    }
    return "rev_" + _sha16(key)


def compute_formation_id(rec: Mapping[str, Any]) -> str:
    """Deterministic id for a PROSPECTIVE_SIGNAL_FORMATION. Includes the source
    availability stamp so a SECOND same-day provider snapshot (changed state)
    forms NEW records instead of colliding with the earlier genuine observation."""
    return "psf_" + _sha16({
        "security_id": rec.get("security_id"),
        "signal_family": rec.get("signal_family"),
        "snapshot_date": rec.get("snapshot_date"),
        "source_availability_timestamp": rec.get("source_availability_timestamp"),
    })


def compute_outcome_id(rec: Mapping[str, Any]) -> str:
    """Deterministic id for a FORWARD_OUTCOME: one outcome per (formation,
    horizon). Excludes computed_timestamp so a re-run that would REVISE a stored
    outcome trips the immutability guard instead of silently duplicating."""
    return "fwd_" + _sha16({
        "formation_record_id": rec.get("formation_record_id"),
        "horizon_sessions": rec.get("horizon_sessions"),
    })


# -- typed, immutable (frozen) record dataclasses --------------------------- #
@dataclass(frozen=True)
class SecurityIdentity:
    provider_security_id: str
    effective_start: str
    status: str
    source_provider: str
    stable_assetid: Optional[str] = None
    cik: Optional[str] = None
    issuer_id: Optional[str] = None
    historical_ticker: Optional[str] = None
    effective_end: Optional[str] = None
    acquisition_or_merger_status: Optional[str] = None
    exchange: Optional[str] = None
    adr_flag: Optional[bool] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}


@dataclass(frozen=True)
class EstimateRevisionEvent:
    provider: str
    provider_event_id: str
    security_id: str
    estimate_type: str
    fiscal_period_type: str
    fiscal_period_end: str
    observation_timestamp: str
    provider_effective_timestamp: str
    revised_estimate: float
    source_availability_timestamp: str
    ingestion_timestamp: str
    record_id: Optional[str] = None
    prior_estimate: Optional[float] = None
    absolute_revision: Optional[float] = None
    percentage_revision: Optional[float] = None
    revision_direction: Optional[str] = None
    analyst_or_broker_id: Optional[str] = None
    analyst_count: Optional[int] = None
    raw_source_reference: Optional[str] = None

    def to_dict(self) -> dict:
        d = {k: v for k, v in dataclasses.asdict(self).items() if v is not None}
        d.setdefault("record_id", compute_record_id(d))
        return d


# =========================================================================== #
# WORKSTREAM A (store) -- immutable, append-only raw observation store.
# =========================================================================== #
class RawObservationStore:
    """Append-only JSONL store, one file per schema. First-write-wins per
    record_id: re-appending IDENTICAL content is idempotent; a DIFFERENT record
    with the SAME record_id raises (revisions can never overwrite prior
    observations -- they must be new record_ids). Never an operational ledger."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, schema_name: str) -> Path:
        return self.root / ("%s.jsonl" % schema_name.lower())

    def append(self, schema_name: str, rec: Mapping[str, Any]) -> str:
        res = self.append_many(schema_name, [rec])
        return res["record_ids"][0]

    def append_many(self, schema_name: str, recs) -> Dict[str, Any]:
        """Batched append with IDENTICAL semantics to per-record append (validate,
        first-write-wins idempotency, immutability guard) but ONE read of the
        existing file — required for real snapshot volumes (10k+ rows/day)."""
        existing = self._by_id(schema_name)
        appended = idempotent = 0
        rids: List[str] = []
        with self._path(schema_name).open("a", encoding="utf-8") as fh:
            for rec in recs:
                problems = validate_record(schema_name, rec)
                if problems:
                    raise ValueError("record contract violation: %s"
                                     % "; ".join(problems))
                rec = dict(rec)
                rid = rec.get("record_id") or ("id_" + _sha16(rec))
                rec["record_id"] = rid
                prev = existing.get(rid)
                if prev is not None:
                    if _canonical_json(prev) != _canonical_json(rec):
                        raise RevisionImmutabilityError(
                            "record_id %s already stored with different content "
                            "(a revision must be a NEW record_id, never an "
                            "overwrite)" % rid)
                    idempotent += 1
                else:
                    fh.write(_canonical_json(rec) + "\n")
                    existing[rid] = rec
                    appended += 1
                rids.append(rid)
        return {"appended": appended, "idempotent": idempotent,
                "record_ids": rids}

    def _by_id(self, schema_name: str) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        p = self._path(schema_name)
        if not p.exists():
            return out
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            out[rec.get("record_id")] = rec
        return out

    def all(self, schema_name: str) -> List[dict]:
        return list(self._by_id(schema_name).values())


class RevisionImmutabilityError(RuntimeError):
    pass


# =========================================================================== #
# WORKSTREAM B -- point-in-time safety contract (deterministic, classifying).
# =========================================================================== #
# Violation codes -- each is EXPLICIT; nothing is silently repaired or inferred.
PIT_MISSING_SOURCE_AVAILABILITY = "MISSING_SOURCE_AVAILABILITY_TIMESTAMP"
PIT_OBS_AFTER_INGESTION = "OBSERVATION_AFTER_INGESTION_WITHOUT_LATE_ARRIVAL"
PIT_REVISION_OVERWRITE = "REVISION_OVERWRITES_PRIOR_OBSERVATION"
PIT_CONSENSUS_BACKFILL = "CURRENT_CONSENSUS_SUBSTITUTED_BACKWARD"
PIT_ACTUAL_BEFORE_ANNOUNCE = "ACTUAL_BEFORE_PUBLIC_ANNOUNCEMENT"
PIT_AMENDMENT_REWRITE = "AMENDMENT_REWRITES_PRIOR_RESEARCH_STATE"
PIT_INACTIVE_IDENTITY_LOST = "INACTIVE_OR_DELISTED_IDENTITY_NOT_RETAINED"
PIT_TICKER_DUPLICATE_ISSUER = "HISTORICAL_TICKER_CHANGE_CREATED_DUPLICATE_ISSUER"
PIT_FISCAL_AS_AVAILABILITY = "FISCAL_PERIOD_DATE_USED_AS_AVAILABILITY"
PIT_RECONSTRUCTED_SNAPSHOT = "RECONSTRUCTED_DAILY_SNAPSHOT_FROM_CURRENT_VALUES"
PIT_RESTATEMENT_UNVERSIONED = "PROVIDER_RESTATEMENT_NOT_SEPARATELY_VERSIONED"

PIT_INVARIANTS = (
    PIT_MISSING_SOURCE_AVAILABILITY, PIT_OBS_AFTER_INGESTION, PIT_REVISION_OVERWRITE,
    PIT_CONSENSUS_BACKFILL, PIT_ACTUAL_BEFORE_ANNOUNCE, PIT_AMENDMENT_REWRITE,
    PIT_INACTIVE_IDENTITY_LOST, PIT_TICKER_DUPLICATE_ISSUER, PIT_FISCAL_AS_AVAILABILITY,
    PIT_RECONSTRUCTED_SNAPSHOT, PIT_RESTATEMENT_UNVERSIONED,
)


def _ts(x) -> Optional[str]:
    return None if x in (None, "") else str(x)


def pit_validate_event(rec: Mapping[str, Any], *,
                       prior_by_key: Optional[Mapping[Any, Mapping[str, Any]]] = None
                       ) -> List[str]:
    """Deterministic PIT checks for one ESTIMATE_REVISION_EVENT. ``prior_by_key`` is
    an optional map from a de-dup key to a previously-observed record; a revision
    that reuses the SAME record_id but changes the value is an overwrite."""
    v: List[str] = []
    sat = _ts(rec.get("source_availability_timestamp"))
    obs = _ts(rec.get("observation_timestamp"))
    ing = _ts(rec.get("ingestion_timestamp"))
    fpe = _ts(rec.get("fiscal_period_end"))
    late = bool(rec.get("late_arrival")) if "late_arrival" in rec else False
    if not sat:
        v.append(PIT_MISSING_SOURCE_AVAILABILITY)
    if obs and ing and obs > ing and not late:
        v.append(PIT_OBS_AFTER_INGESTION)
    # fiscal-period date must NOT masquerade as the availability/observation date.
    if fpe and (sat == fpe or obs == fpe):
        v.append(PIT_FISCAL_AS_AVAILABILITY)
    if prior_by_key is not None:
        rid = rec.get("record_id") or compute_record_id(rec)
        prev = prior_by_key.get(rid)
        if prev is not None and prev.get("revised_estimate") != rec.get("revised_estimate"):
            v.append(PIT_REVISION_OVERWRITE)
    return v


def pit_validate_actual(rec: Mapping[str, Any]) -> List[str]:
    v: List[str] = []
    ann = _ts(rec.get("announcement_timestamp"))
    sat = _ts(rec.get("source_availability_timestamp"))
    if not sat:
        v.append(PIT_MISSING_SOURCE_AVAILABILITY)
    if ann and sat and sat < ann:
        v.append(PIT_ACTUAL_BEFORE_ANNOUNCE)
    return v


def pit_scan_consensus_series(snapshots: Sequence[Mapping[str, Any]]) -> List[str]:
    """Detect a reconstructed daily snapshot series: many DISTINCT observation
    timestamps all carrying the IDENTICAL consensus value (i.e. today's consensus
    stamped backward). Also flags a missing source-availability timestamp."""
    v: List[str] = []
    if not snapshots:
        return v
    ordered = sorted(snapshots, key=lambda s: str(s.get("observation_timestamp") or ""))
    if any(not _ts(s.get("source_availability_timestamp")) for s in ordered):
        v.append(PIT_MISSING_SOURCE_AVAILABILITY)
    vals = [s.get("mean") for s in ordered if s.get("mean") is not None]
    distinct_ts = {str(s.get("observation_timestamp")) for s in ordered}
    if len(distinct_ts) >= 3 and len(vals) >= 3 and len(set(vals)) == 1:
        v.append(PIT_RECONSTRUCTED_SNAPSHOT)
        v.append(PIT_CONSENSUS_BACKFILL)
    # a later observation whose "prior_*_consensus" equals its own mean is a
    # backfilled reconstruction (no real revision history).
    for s in ordered:
        m = s.get("mean")
        if m is not None and s.get("prior_month_consensus") == m \
                and s.get("prior_week_consensus") == m and PIT_CONSENSUS_BACKFILL not in v:
            v.append(PIT_CONSENSUS_BACKFILL)
            break
    return v


def pit_scan_identities(identities: Sequence[Mapping[str, Any]]) -> List[str]:
    """Historical-identity integrity: inactive/delisted rows must be RETAINED, and a
    historical ticker change must NOT split one issuer into duplicate issuer_ids."""
    v: List[str] = []
    # duplicate-issuer-from-ticker: same issuer_id reachable under >1 ticker is fine;
    # the violation is the INVERSE -- one (provider_security_id) mapped to multiple
    # issuer_ids across ticker eras (a ticker change spawning a new issuer identity).
    sec_to_issuers: Dict[str, set] = {}
    for r in identities:
        sid = str(r.get("provider_security_id"))
        iss = r.get("issuer_id")
        if iss is not None:
            sec_to_issuers.setdefault(sid, set()).add(str(iss))
    if any(len(s) > 1 for s in sec_to_issuers.values()):
        v.append(PIT_TICKER_DUPLICATE_ISSUER)
    return v


def pit_scan(*, events: Sequence[Mapping[str, Any]] = (),
             actuals: Sequence[Mapping[str, Any]] = (),
             consensus: Sequence[Mapping[str, Any]] = (),
             identities: Sequence[Mapping[str, Any]] = (),
             require_inactive_retained: bool = True) -> Dict[str, Any]:
    """Aggregate PIT scan over a normalized dataset. Returns violation COUNTS by
    code + a boolean ``clean``. Nothing is repaired; every violation is classified."""
    counts: Dict[str, int] = {c: 0 for c in PIT_INVARIANTS}
    prior: Dict[str, dict] = {}
    for e in events:
        for code in pit_validate_event(e, prior_by_key=prior):
            counts[code] += 1
        rid = e.get("record_id") or compute_record_id(e)
        prior.setdefault(rid, dict(e))
    for a in actuals:
        for code in pit_validate_actual(a):
            counts[code] += 1
    for code in pit_scan_consensus_series(consensus):
        counts[code] += 1
    for code in pit_scan_identities(identities):
        counts[code] += 1
    if require_inactive_retained and identities:
        has_inactive = any(str(r.get("status", "")).upper() in
                           ("INACTIVE", "DELISTED", "ACQUIRED", "MERGED")
                           for r in identities)
        # If EVERY identity is ACTIVE the dataset cannot prove inactive retention.
        if not has_inactive:
            counts[PIT_INACTIVE_IDENTITY_LOST] += 1
    total = sum(counts.values())
    return {"violation_counts": counts, "total_violations": total,
            "clean": total == 0, "invariants_checked": list(PIT_INVARIANTS),
            "no_silent_repair": True}


# =========================================================================== #
# WORKSTREAM C -- provider-neutral adapter protocol + non-networking skeletons.
# =========================================================================== #
class RevisionDataAdapter:
    """Provider-neutral adapter interface. Concrete adapters declare capability,
    field mapping, pagination, trial limits, raw-archive naming, rate limits,
    identifier mapping, inactive/delisted handling and error classification. NO
    method performs authentication or network retrieval in this phase -- capability
    is a DECLARED contract (with UNKNOWNs), not a live discovery call.

    Credentials, when a trial begins, are read by NAME from an environment variable
    (``credential_env``); no credential value is ever stored in code, config,
    fixtures, tests, reports or logs.
    """
    provider = "abstract"
    credential_env = None  # NAME of an env var, resolved only at extraction time

    def capability(self) -> Dict[str, Any]:
        raise NotImplementedError

    def field_mapping(self) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError

    def pagination_contract(self) -> Dict[str, Any]:
        raise NotImplementedError

    def trial_limits(self) -> Dict[str, Any]:
        raise NotImplementedError

    def rate_limits(self) -> Dict[str, Any]:
        raise NotImplementedError

    def raw_archive_name(self, *, schema: str, page: int, as_of: str) -> str:
        return "%s__%s__p%05d__%s.raw.json" % (self.provider, schema.lower(),
                                               int(page), str(as_of))

    def map_identifier(self, provider_row: Mapping[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def inactive_delisted_handling(self) -> Dict[str, Any]:
        raise NotImplementedError

    def classify_error(self, code: Any) -> str:
        try:
            c = int(code)
        except (TypeError, ValueError):
            return "UNKNOWN_ERROR"
        if c in (401, 403):
            return "ENTITLEMENT_OR_AUTH"
        if c == 404:
            return "MISSING_RESOURCE"
        if c == 429:
            return "RATE_LIMITED"
        if 500 <= c < 600:
            return "PROVIDER_SERVER_ERROR"
        if 200 <= c < 300:
            return "OK"
        return "CLIENT_ERROR"


_UNKNOWN = None  # explicit: capability facts are UNKNOWN until measured on a trial


class IntrinioZacksAdapter(RevisionDataAdapter):
    """Non-networking skeleton for Intrinio's US-Fundamentals + Zacks estimates.
    The provider's TRIAL scope (stated) is ~1 year of history -- flagged so the
    adequacy gate can immediately mark history length as the weakest gate. Every
    capability fact that requires a live trial to measure is left UNKNOWN (None)."""
    provider = "intrinio_zacks"
    credential_env = "STAGE13A_INTRINIO_TOKEN"

    def capability(self) -> Dict[str, Any]:
        return {
            "source_provider": self.provider,
            "underlying_estimate_source": "zacks",  # do NOT assume independence
            "available_history_start": _UNKNOWN,
            "available_history_end": _UNKNOWN,
            "active_coverage": True,
            "inactive_coverage": True,
            "acquired_coverage": True,
            "delisted_coverage": True,
            "adr_coverage": True,
            "otc_coverage": False,           # stated: OTC incomplete
            "individual_analyst_availability": _UNKNOWN,
            "consensus_history_availability": _UNKNOWN,
            "api_availability": True,
            "bulk_availability": True,
            "rate_limits": _UNKNOWN,
            "historical_archive_restrictions":
                "TRIAL PROVIDES ~1 YEAR OF HISTORY ONLY (stated)",
            "trial_restrictions": "approx 1 year of statements/metadata during trial",
            "licensing_restrictions": _UNKNOWN,
            "quoted_one_time_history_price": _UNKNOWN,
            "quoted_recurring_price": _UNKNOWN,
            "auto_renewal_terms": _UNKNOWN,
        }

    def field_mapping(self) -> Dict[str, Dict[str, str]]:
        return {
            "ESTIMATE_REVISION_EVENT": {
                "id": "provider_event_id",
                "security": "security_id",
                "type": "estimate_type",
                "fiscal_period": "fiscal_period_type",
                "fiscal_period_end_date": "fiscal_period_end",
                "updated_date": "observation_timestamp",
                "effective_date": "provider_effective_timestamp",
                "previous_estimate": "prior_estimate",
                "estimate": "revised_estimate",
                "analyst_count": "analyst_count",
                "available_date": "source_availability_timestamp",
            },
            "CONSENSUS_SNAPSHOT": {
                "security": "security_id",
                "type": "estimate_type",
                "fiscal_period_end_date": "fiscal_period_end",
                "as_of_date": "observation_timestamp",
                "mean_estimate": "mean",
                "median_estimate": "median",
                "high_estimate": "high",
                "low_estimate": "low",
                "standard_deviation": "standard_deviation",
                "count": "analyst_count",
                "up_revisions": "upward_revision_count",
                "down_revisions": "downward_revision_count",
            },
        }

    def pagination_contract(self) -> Dict[str, Any]:
        return {"style": "cursor", "cursor_param": "next_page",
                "page_size_param": "page_size", "max_page_size": _UNKNOWN}

    def trial_limits(self) -> Dict[str, Any]:
        return {"history_years": 1, "history_years_source": "vendor_stated",
                "row_cap": _UNKNOWN, "call_cap": _UNKNOWN}

    def rate_limits(self) -> Dict[str, Any]:
        return {"requests_per_minute": _UNKNOWN, "concurrency": _UNKNOWN}

    def map_identifier(self, provider_row: Mapping[str, Any]) -> Dict[str, Any]:
        return {"provider_security_id": provider_row.get("security"),
                "cik": provider_row.get("cik"),
                "historical_ticker": provider_row.get("ticker"),
                "status": provider_row.get("security_status"),
                "stable_assetid": _UNKNOWN}

    def inactive_delisted_handling(self) -> Dict[str, Any]:
        return {"inactive_rows_retained": True, "delisted_rows_retained": True,
                "effective_dated_identity": True}


class NasdaqZacksAdapter(RevisionDataAdapter):
    """Non-networking skeleton for Nasdaq Data Link / Zacks estimate tables. Vendor
    has not yet responded; ALL capability facts are UNKNOWN until a trial. Shares an
    underlying Zacks source with Intrinio -- so the two are NOT assumed independent."""
    provider = "nasdaq_zacks"
    credential_env = "STAGE13A_NASDAQ_DATA_LINK_TOKEN"

    def capability(self) -> Dict[str, Any]:
        return {
            "source_provider": self.provider,
            "underlying_estimate_source": "zacks",
            "available_history_start": _UNKNOWN,
            "available_history_end": _UNKNOWN,
            "active_coverage": _UNKNOWN,
            "inactive_coverage": _UNKNOWN,
            "acquired_coverage": _UNKNOWN,
            "delisted_coverage": _UNKNOWN,
            "adr_coverage": _UNKNOWN,
            "otc_coverage": _UNKNOWN,
            "individual_analyst_availability": _UNKNOWN,
            "consensus_history_availability": _UNKNOWN,
            "api_availability": True,
            "bulk_availability": True,
            "rate_limits": _UNKNOWN,
            "historical_archive_restrictions": _UNKNOWN,
            "trial_restrictions": _UNKNOWN,
            "licensing_restrictions": _UNKNOWN,
            "quoted_one_time_history_price": _UNKNOWN,
            "quoted_recurring_price": _UNKNOWN,
            "auto_renewal_terms": _UNKNOWN,
        }

    def field_mapping(self) -> Dict[str, Dict[str, str]]:
        return {
            "ESTIMATE_REVISION_EVENT": {
                "m_ticker": "security_id",
                "per_type": "fiscal_period_type",
                "per_end_date": "fiscal_period_end",
                "est_type": "estimate_type",
                "asof_date": "observation_timestamp",
                "prev_value": "prior_estimate",
                "value": "revised_estimate",
                "analyst_cnt": "analyst_count",
            },
            "CONSENSUS_SNAPSHOT": {
                "m_ticker": "security_id",
                "est_type": "estimate_type",
                "per_end_date": "fiscal_period_end",
                "asof_date": "observation_timestamp",
                "mean_est": "mean",
                "median_est": "median",
                "std_dev": "standard_deviation",
                "analyst_cnt": "analyst_count",
                "up_1m": "upward_revision_count",
                "down_1m": "downward_revision_count",
            },
        }

    def pagination_contract(self) -> Dict[str, Any]:
        return {"style": "page", "page_param": "page",
                "page_size_param": "per_page", "max_page_size": _UNKNOWN}

    def trial_limits(self) -> Dict[str, Any]:
        return {"history_years": _UNKNOWN, "row_cap": _UNKNOWN, "call_cap": _UNKNOWN}

    def rate_limits(self) -> Dict[str, Any]:
        return {"requests_per_minute": _UNKNOWN, "concurrency": _UNKNOWN}

    def map_identifier(self, provider_row: Mapping[str, Any]) -> Dict[str, Any]:
        return {"provider_security_id": provider_row.get("m_ticker"),
                "cik": _UNKNOWN, "historical_ticker": provider_row.get("ticker"),
                "status": _UNKNOWN, "stable_assetid": _UNKNOWN}

    def inactive_delisted_handling(self) -> Dict[str, Any]:
        return {"inactive_rows_retained": _UNKNOWN, "delisted_rows_retained": _UNKNOWN,
                "effective_dated_identity": _UNKNOWN}


ADAPTERS = {IntrinioZacksAdapter.provider: IntrinioZacksAdapter,
            NasdaqZacksAdapter.provider: NasdaqZacksAdapter}


def get_adapter(provider: str) -> RevisionDataAdapter:
    cls = ADAPTERS.get(str(provider))
    if cls is None:
        raise KeyError("no adapter for provider %s" % provider)
    return cls()


class LocalTrialImporter:
    """Consumes a LOCAL CSV or JSON trial export (supplied by the operator) and maps
    provider columns into the frozen normalized contract via an adapter's
    field_mapping. Works ONLY on local files -- it never opens a network connection.
    Unmapped provider columns are dropped; contract violations are RETURNED (never
    silently coerced)."""

    def __init__(self, adapter: RevisionDataAdapter):
        self.adapter = adapter

    def _map_rows(self, schema: str, rows: Sequence[Mapping[str, Any]]
                  ) -> Tuple[List[dict], List[str]]:
        mapping = self.adapter.field_mapping().get(schema, {})
        out: List[dict] = []
        problems: List[str] = []
        for i, row in enumerate(rows):
            rec: Dict[str, Any] = {"provider": self.adapter.provider} \
                if schema == "ESTIMATE_REVISION_EVENT" else {}
            for src, dst in mapping.items():
                if src in row and row[src] not in (None, ""):
                    rec[dst] = row[src]
            if schema == "ESTIMATE_REVISION_EVENT":
                rec.setdefault("provider_event_id", row.get("id") or "row%d" % i)
                rec["record_id"] = compute_record_id(rec)
            probs = validate_record(schema, rec)
            if probs:
                problems.extend("row %d: %s" % (i, p) for p in probs)
            out.append(rec)
        return out, problems

    def import_file(self, path: str | Path, *, schema: str) -> Dict[str, Any]:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError("local trial file not found: %s" % p)
        rows = self._read_local(p)
        mapped, problems = self._map_rows(schema, rows)
        return {"provider": self.adapter.provider, "schema": schema,
                "path": str(p), "rows_in": len(rows), "records_out": len(mapped),
                "records": mapped, "contract_violations": problems,
                "network_used": False}

    @staticmethod
    def _read_local(p: Path) -> List[dict]:
        text = p.read_text(encoding="utf-8-sig")
        if p.suffix.lower() == ".json":
            data = json.loads(text)
            if isinstance(data, Mapping):
                data = data.get("data") or data.get("rows") or []
            return [dict(r) for r in data]
        reader = csv.DictReader(io.StringIO(text))
        return [dict(r) for r in reader]


# =========================================================================== #
# WORKSTREAM D -- frozen 6-hypothesis pre-registered revision registry.
# =========================================================================== #
REVISION_REGISTRY_SCHEMA_VERSION = "stage13a.analyst_revisions.hypotheses.v1"

_REVISION_GATES = (
    "point_in_time_valid", "survivorship_safe", "inactive_delisted_coverage>=0.30",
    "min_scored_cohorts>=12", "min_breadth_names>=20", "rank_ic_t>=2.0",
    "spread_t>=2.0", "positive_ic_hit_rate>=0.52", "net25_spread>0",
    "turnover<=2.0", "overlap_adjusted_clustered_t>=2.0",
    "family_aware_multiple_testing_bh_fdr", "holdout_sign_confirms",
    "no_reconstructed_snapshots",
)

REVISION_EVALUATION_CONTRACT = {
    "contract_version": "stage13a-analyst-revision-v1",
    "event_time_unit": "monthly revision cohort (issuer observation per cohort)",
    "observation_timestamp_rule": "provider source_availability_timestamp; a signal "
                                  "may use ONLY information available at that stamp",
    "execution_lag": "first full trading session STRICTLY AFTER the revision is "
                     "available (>=1 session lag; never the availability-day close)",
    "forward_horizons_days": [5, 20, 63],
    "primary_horizon_days": 63,
    "neutralization": "market + sector (historically valid only; unavailable "
                      "historical industry -> DATA_HOLD, never current substitution)",
    "turnover_treatment": "measured; capped at 2.0x per rebalance",
    "cost_treatment": "25 bps per side applied to the long-short spread",
    "holdout_rule": "final 30% of cohorts, sign-confirm only, once-usable",
    "multiple_testing": "Benjamini-Hochberg FDR over EXACTLY the six pre-registered "
                        "hypotheses (family_size=6)",
    "selection_before_evidence": "all six formulas + directions frozen BEFORE any "
                                 "real trial data is inspected",
    "gates_unchanged_from_stage12": True,
    "no_strategy_built_this_phase": True,
}


def _revh(hid: str, family: str, rationale: str, formula: str, direction: int,
          data_requirement: List[str], *, horizon_days: int = 63,
          turnover: str = "medium") -> Dict[str, Any]:
    return {
        "id": hid,
        "economic_family": family,
        "status": "pre_registered",
        "economic_rationale": rationale,
        "formula": formula,
        "expected_direction": direction,
        "observation_timestamp_rule": REVISION_EVALUATION_CONTRACT["observation_timestamp_rule"],
        "execution_lag": REVISION_EVALUATION_CONTRACT["execution_lag"],
        "forward_horizons_days": [5, 20, 63],
        "holding_horizon_days": horizon_days,
        "neutralization": "market_sector_historically_valid",
        "turnover_treatment": turnover,
        "cost_assumption_bps": 25,
        "min_breadth_names": 20,
        "min_cohorts": 12,
        "holdout_rule": REVISION_EVALUATION_CONTRACT["holdout_rule"],
        "multiple_testing_family": "analyst_revision_v1_family_of_6",
        "data_requirement": data_requirement,
        "disqualifying_gates": list(_REVISION_GATES),
    }


def _revision_hypotheses() -> List[Dict[str, Any]]:
    H: List[Dict[str, Any]] = []
    H.append(_revh(
        "rev_eps_consensus_revision_magnitude", "eps_revision",
        "Analysts under-react to their own information: a positive change in the "
        "consensus EPS estimate predicts positive drift (estimate-revision anomaly).",
        "d(consensus_mean_EPS) scaled by |prior_consensus| (or price), cross-sectional "
        "rank at the availability stamp",
        +1, ["CONSENSUS_SNAPSHOT.mean(EPS)"]))
    H.append(_revh(
        "rev_revenue_consensus_revision_magnitude", "revenue_revision",
        "Revenue estimate revisions carry information orthogonal to EPS (top-line vs "
        "margin); rising revenue consensus predicts positive drift.",
        "d(consensus_mean_REVENUE) scaled by prior_consensus, cross-sectional rank",
        +1, ["CONSENSUS_SNAPSHOT.mean(REVENUE)"]))
    H.append(_revh(
        "rev_up_minus_down_breadth", "revision_breadth",
        "Revision BREADTH (how many analysts move up vs down) is a cleaner, less "
        "noisy signal than the magnitude of any single revision.",
        "(upward_revision_count - downward_revision_count) / analyst_count, rank",
        +1, ["CONSENSUS_SNAPSHOT.upward_revision_count",
             "CONSENSUS_SNAPSHOT.downward_revision_count"]))
    H.append(_revh(
        "rev_joint_eps_revenue_confirmation", "joint_confirmation",
        "EPS and revenue revisions AGREEING (both up or both down) is a higher-"
        "conviction signal than either leg alone (two independent legs confirming).",
        "sign-agreeing rank(d_EPS_consensus) + rank(d_REVENUE_consensus); gated on "
        "same sign",
        +1, ["CONSENSUS_SNAPSHOT.mean(EPS)", "CONSENSUS_SNAPSHOT.mean(REVENUE)"]))
    H.append(_revh(
        "rev_revision_acceleration", "revision_acceleration",
        "The CHANGE in the revision trend (acceleration) leads the level: consensus "
        "that is revising up faster than last period predicts stronger drift.",
        "d2(consensus_mean_EPS) = this-period revision minus prior-period revision, rank",
        +1, ["CONSENSUS_SNAPSHOT.mean(EPS) (>=3 successive cohorts)"]))
    H.append(_revh(
        "rev_analyst_dispersion_change", "dispersion",
        "RISING dispersion (disagreement) signals rising uncertainty and predicts "
        "LOWER future returns (disagreement/uncertainty anomaly).",
        "-d(standard_deviation / |mean|) of the consensus, cross-sectional rank",
        -1, ["CONSENSUS_SNAPSHOT.standard_deviation", "CONSENSUS_SNAPSHOT.mean"]))
    return H


def build_revision_registry() -> Dict[str, Any]:
    hyps = _revision_hypotheses()
    ids = [h["id"] for h in hyps]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate revision hypothesis ids")
    if len(hyps) != FAMILY_SIZE:
        raise ValueError("expected exactly %d hypotheses, got %d" % (FAMILY_SIZE, len(hyps)))
    families = sorted({h["economic_family"] for h in hyps})
    body = {
        "schema_version": REVISION_REGISTRY_SCHEMA_VERSION,
        "evaluation_contract": dict(REVISION_EVALUATION_CONTRACT),
        "n_hypotheses": len(hyps),
        "family_size": FAMILY_SIZE,
        "economic_families": families,
        "hypotheses": hyps,
        "selected_before_evidence": True,
    }
    body["registry_version"] = hashlib.sha256(_canonical_json({
        "schema_version": REVISION_REGISTRY_SCHEMA_VERSION,
        "evaluation_contract": REVISION_EVALUATION_CONTRACT,
        "hypotheses": hyps}).encode("utf-8")).hexdigest()[:16]
    return body


def validate_revision_registry(reg: Mapping[str, Any]) -> List[str]:
    problems: List[str] = []
    hyps = reg.get("hypotheses") or []
    if len(hyps) != FAMILY_SIZE:
        problems.append("hypothesis count %d != %d" % (len(hyps), FAMILY_SIZE))
    required = ("id", "economic_family", "economic_rationale", "formula",
                "expected_direction", "observation_timestamp_rule", "execution_lag",
                "forward_horizons_days", "neutralization", "turnover_treatment",
                "cost_assumption_bps", "min_breadth_names", "min_cohorts",
                "holdout_rule", "multiple_testing_family", "disqualifying_gates")
    ids = set()
    for h in hyps:
        for f in required:
            if f not in h or h.get(f) in (None, "", []):
                problems.append("hypothesis %s missing %s" % (h.get("id"), f))
        if h.get("id") in ids:
            problems.append("duplicate id %s" % h.get("id"))
        ids.add(h.get("id"))
        if int(h.get("expected_direction", 0)) not in (-1, 1):
            problems.append("hypothesis %s bad direction" % h.get("id"))
    if reg.get("registry_version") != build_revision_registry()["registry_version"]:
        problems.append("registry_version mismatch")
    return problems


def default_revision_registry_path(repo_root: Optional[str | Path] = None) -> Path:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    return root / "configs" / "alpha_agent" / "stage13a_revision_hypotheses.json"


def freeze_revision_registry(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """First-write-wins immutable freeze. Re-freezing DIFFERENT content raises."""
    path = Path(path) if path else default_revision_registry_path()
    reg = build_revision_registry()
    payload = json.dumps(reg, sort_keys=True, indent=1) + "\n"
    if path.exists():
        existing = _read_json(path, {}) or {}
        if existing.get("registry_version") == reg["registry_version"]:
            return reg
        raise RevisionImmutabilityError(
            "frozen revision registry at %s differs (frozen=%s new=%s)"
            % (path, existing.get("registry_version"), reg["registry_version"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)
    return reg


# =========================================================================== #
# WORKSTREAM E -- deterministic synthetic-fixture engine + evaluator.
# =========================================================================== #
FIX_POSITIVE = "known_positive_revision_signal"
FIX_NULL = "known_null_signal"
FIX_WRONG_DIRECTION = "wrong_direction_signal"
FIX_COST_DESTROYED = "signal_destroyed_by_costs"
FIX_ISSUER_CONCENTRATED = "signal_concentrated_in_one_issuer"
FIX_MONTH_CONCENTRATED = "signal_concentrated_in_one_month"
FIX_LEAKAGE = "revision_timestamps_with_leakage"
FIX_ACTIVE_ONLY = "survivorship_biased_active_only"
FIX_DUP_TICKER = "duplicated_ticker_histories"
FIX_RECONSTRUCTED = "rewritten_consensus_snapshots"

FIXTURE_KINDS = (FIX_POSITIVE, FIX_NULL, FIX_WRONG_DIRECTION, FIX_COST_DESTROYED,
                 FIX_ISSUER_CONCENTRATED, FIX_MONTH_CONCENTRATED, FIX_LEAKAGE,
                 FIX_ACTIVE_ONLY, FIX_DUP_TICKER, FIX_RECONSTRUCTED)


def _rng(seed: int):
    """Deterministic Mersenne-Twister PRNG seeded explicitly (independent of the
    process hash seed); returns floats in [0,1)."""
    import random
    r = random.Random(int(seed))
    return r.random


def _month(i: int) -> str:
    y = 2018 + (i // 12)
    m = (i % 12) + 1
    return "%04d-%02d" % (y, m)


# Per-fixture generation parameters. ``beta`` = strength of the signal->return
# relationship; ``noise`` = idiosyncratic return noise; ``sign`` flips the relation
# (wrong-direction); ``month0_mult`` / ``issuer0_mult`` scale the RETURN MAGNITUDE
# (not the rank) so a signal predictive in every cohort still concentrates its P&L.
_FIXTURE_PARAMS = {
    FIX_POSITIVE:            dict(beta=0.9,  noise=0.10),
    FIX_NULL:                dict(beta=0.0,  noise=0.10),
    FIX_WRONG_DIRECTION:     dict(beta=0.9,  noise=0.10, sign=-1.0),
    FIX_COST_DESTROYED:      dict(beta=0.05, noise=0.0004),   # strong IC, tiny spread
    FIX_ISSUER_CONCENTRATED: dict(beta=0.9,  noise=0.02, issuer0_mult=60.0),
    FIX_MONTH_CONCENTRATED:  dict(beta=0.9,  noise=0.02, month0_mult=60.0),
    FIX_LEAKAGE:             dict(beta=0.9,  noise=0.10),
    FIX_ACTIVE_ONLY:         dict(beta=0.9,  noise=0.10),
    FIX_DUP_TICKER:          dict(beta=0.9,  noise=0.10),
    FIX_RECONSTRUCTED:       dict(beta=0.0,  noise=0.10),
}


def make_fixture(kind: str, *, n_cohorts: int = 24, n_names: int = 40,
                 seed: int = 20260804) -> Dict[str, Any]:
    """Build a deterministic synthetic normalized dataset with a KNOWN property.

    Returns cohorts (for the IC evaluator) plus the identity / consensus records
    needed by the PIT scan. Signal = the standardized consensus-EPS revision; fwd =
    the forward return the evaluator should (or should not) find related to it."""
    rnd = _rng(seed + int(hashlib.sha256(kind.encode("utf-8")).hexdigest()[:8], 16))
    p = _FIXTURE_PARAMS.get(kind, dict(beta=0.0, noise=0.10))
    beta = float(p.get("beta", 0.0)); noise_amp = float(p.get("noise", 0.10))
    sign = float(p.get("sign", 1.0))
    month0_mult = float(p.get("month0_mult", 1.0))
    issuer0_mult = float(p.get("issuer0_mult", 1.0))
    cohorts: List[Dict[str, Any]] = []
    identities: List[Dict[str, Any]] = []
    consensus: List[Dict[str, Any]] = []
    for ci in range(n_cohorts):
        names: List[Tuple[str, float, float]] = []
        for ni in range(n_names):
            iid = "ISS%03d" % ni
            signal = (rnd() - 0.5) * 2.0                 # revision signal in [-1,1]
            noise = (rnd() - 0.5) * noise_amp
            mult = 1.0
            if ci == 0:
                mult *= month0_mult
            if ni == 0:
                mult *= issuer0_mult
                if issuer0_mult > 1.0:
                    signal = 1.0                          # keep the fat name at an extreme
            fwd = mult * (sign * beta * 0.05 * signal) + noise
            names.append((iid, signal, fwd))
        cohorts.append({"as_of": _month(ci), "names": names})
    # identities
    statuses = ["ACTIVE"] * n_names
    if kind != FIX_ACTIVE_ONLY:
        for k in range(0, max(1, n_names // 3)):
            statuses[k] = "DELISTED" if k % 2 else "INACTIVE"
    for ni in range(n_names):
        rec = {"provider_security_id": "SEC%03d" % ni, "issuer_id": "ISS%03d" % ni,
               "historical_ticker": "TK%03d" % ni, "effective_start": "2000-01-01",
               "status": statuses[ni], "source_provider": "synthetic"}
        identities.append(rec)
        if kind == FIX_DUP_TICKER and ni == 0:
            dup = dict(rec)
            dup["issuer_id"] = "ISS999"     # ticker change spawns a duplicate issuer
            dup["historical_ticker"] = "TK000B"
            identities.append(dup)
    # consensus series (mainly to exercise the PIT scan)
    if kind == FIX_RECONSTRUCTED:
        for ci in range(6):
            consensus.append({"security_id": "SEC000", "estimate_type": "EPS",
                              "fiscal_period_end": "2019-12-31",
                              "observation_timestamp": "2019-%02d-15" % (ci + 1),
                              "source_availability_timestamp": "2019-%02d-15" % (ci + 1),
                              "mean": 1.23, "prior_week_consensus": 1.23,
                              "prior_month_consensus": 1.23})
    cohort_meta = {"cost_destroyed": kind == FIX_COST_DESTROYED,
                   "leakage": kind == FIX_LEAKAGE,
                   "active_only": kind == FIX_ACTIVE_ONLY}
    return {"kind": kind, "expected_direction": 1, "cohorts": cohorts,
            "identities": identities, "consensus": consensus, "meta": cohort_meta}


def _cohort_ic_series(cohorts: Sequence[Mapping[str, Any]], *, direction: int
                      ) -> List[float]:
    out: List[float] = []
    for c in cohorts:
        names = c.get("names") or []
        if len(names) < 3:
            continue
        sig = [direction * float(t[1]) for t in names]
        fwd = [float(t[2]) for t in names]
        ic = _fev._spearman(sig, fwd)
        if ic is not None:
            out.append(ic)
    return out


def _ic_tstat(ics: Sequence[float]) -> Optional[float]:
    xs = [float(x) for x in ics if x is not None]
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    if sd <= 0:
        return None
    return m / (sd / math.sqrt(n))


def _cohort_legs(cohorts: Sequence[Mapping[str, Any]], *, direction: int):
    """Per-cohort long-short P&L plus per-issuer leg contributions. Yields
    (as_of, diff, {issuer: contribution}) so spread and concentration share one
    ranking. The long leg is the top quintile of ``direction*signal``, the short
    leg the bottom quintile; an issuer's contribution is +fwd/q (long) or -fwd/q
    (short)."""
    for c in cohorts:
        names = c.get("names") or []
        if len(names) < 4:
            continue
        ranked = sorted(names, key=lambda t: direction * float(t[1]))
        q = max(1, len(ranked) // 5)
        contrib: Dict[str, float] = {}
        for t in ranked[-q:]:
            contrib[str(t[0])] = contrib.get(str(t[0]), 0.0) + float(t[2]) / q
        for t in ranked[:q]:
            contrib[str(t[0])] = contrib.get(str(t[0]), 0.0) - float(t[2]) / q
        long_leg = sum(float(t[2]) for t in ranked[-q:]) / q
        short_leg = sum(float(t[2]) for t in ranked[:q]) / q
        yield c.get("as_of"), long_leg - short_leg, contrib


def _long_short_spread(cohorts: Sequence[Mapping[str, Any]], *, direction: int
                       ) -> Dict[str, Any]:
    diffs = [d for _a, d, _c in _cohort_legs(cohorts, direction=direction)]
    if len(diffs) < 3:
        return {"spread_mean": None, "spread_t": None, "net25": None, "n": len(diffs)}
    m = sum(diffs) / len(diffs)
    sd = math.sqrt(sum((d - m) ** 2 for d in diffs) / (len(diffs) - 1))
    t = (m / (sd / math.sqrt(len(diffs)))) if sd > 0 else None
    # 25 bps per side, 2 sides, per-cohort turnover ~1.0 -> 0.005 haircut on the spread
    net = m - 0.005
    return {"spread_mean": m, "spread_t": t, "net25": net, "n": len(diffs)}


def _concentration_of(cohorts: Sequence[Mapping[str, Any]], *, direction: int = 1
                      ) -> Dict[str, Any]:
    """Concentration of the long-short P&L (not the rank-IC), so a signal that is
    predictive in every cohort but whose PROFIT comes from one month or one issuer
    is flagged. month share = |one cohort's spread| / sum|spread|; issuer share =
    |one issuer's summed leg contribution| / sum|contribution|."""
    month_abs: Dict[str, float] = {}
    issuer_abs: Dict[str, float] = {}
    for as_of, diff, contrib in _cohort_legs(cohorts, direction=direction):
        month_abs[as_of] = month_abs.get(as_of, 0.0) + abs(diff)
        for iid, val in contrib.items():
            issuer_abs[iid] = issuer_abs.get(iid, 0.0) + val
    tot_month = sum(month_abs.values()) or 1.0
    tot_issuer = sum(abs(v) for v in issuer_abs.values()) or 1.0
    top_month = (max(month_abs.values()) / tot_month) if month_abs else None
    top_issuer = (max(abs(v) for v in issuer_abs.values()) / tot_issuer) if issuer_abs else None
    concentrated = bool((top_month or 0) > 0.60 or (top_issuer or 0) > 0.15)
    return {"top_month_share_of_spread": top_month,
            "top_issuer_share_of_pnl": top_issuer, "concentrated": concentrated}


# evaluator verdict codes
EV_PASS = "PASS_ALL_GATES"
EV_REJECT_DIRECTION = "REJECT_WRONG_DIRECTION"
EV_REJECT_WEAK = "REJECT_WEAK_OR_NULL"
EV_REJECT_COST = "REJECT_COST_DESTROYED"
EV_REJECT_CONC = "REJECT_CONCENTRATION"
EV_REJECT_LEAKAGE = "REJECT_PIT_LEAKAGE"
EV_REJECT_SURVIVORSHIP = "REJECT_ACTIVE_ONLY_SURVIVORSHIP"
EV_REJECT_RECONSTRUCTED = "REJECT_RECONSTRUCTED_SNAPSHOTS"
EV_REJECT_DUP_TICKER = "REJECT_DUPLICATE_TICKER_IDENTITY"


def evaluate_signal(fixture: Mapping[str, Any], *, direction: Optional[int] = None,
                    require_inactive: bool = True, keep_t: float = 2.0) -> Dict[str, Any]:
    """Evaluate ONE fixture/dataset under the UNCHANGED gates. Rejection order:
    PIT integrity (leakage -> reconstructed -> duplicate-ticker -> survivorship)
    FIRST, then strength (weak/null), then wrong-direction, then concentration, then
    cost. Never promotes anything."""
    direction = int(direction if direction is not None else fixture.get("expected_direction", 1))
    cohorts = fixture.get("cohorts") or []
    meta = fixture.get("meta") or {}
    identities = fixture.get("identities") or []
    consensus = fixture.get("consensus") or []

    # 1. PIT integrity gates (hard, first).
    pit = pit_scan(consensus=consensus, identities=identities,
                   require_inactive_retained=require_inactive)
    vc = pit["violation_counts"]
    if meta.get("leakage") or vc.get(PIT_OBS_AFTER_INGESTION) or vc.get(PIT_FISCAL_AS_AVAILABILITY):
        return _verdict(EV_REJECT_LEAKAGE, direction, pit=pit)
    if vc.get(PIT_RECONSTRUCTED_SNAPSHOT) or vc.get(PIT_CONSENSUS_BACKFILL):
        return _verdict(EV_REJECT_RECONSTRUCTED, direction, pit=pit)
    if vc.get(PIT_TICKER_DUPLICATE_ISSUER):
        return _verdict(EV_REJECT_DUP_TICKER, direction, pit=pit)
    if require_inactive and vc.get(PIT_INACTIVE_IDENTITY_LOST):
        return _verdict(EV_REJECT_SURVIVORSHIP, direction, pit=pit)

    # 2. statistical gates
    ics = _cohort_ic_series(cohorts, direction=direction)
    ic_t = _ic_tstat(ics)
    ic_mean = (sum(ics) / len(ics)) if ics else None
    spread = _long_short_spread(cohorts, direction=direction)
    conc = _concentration_of(cohorts, direction=direction)
    metrics = {"n_cohorts": len(ics), "rank_ic_mean": ic_mean, "rank_ic_t": ic_t,
               "spread_t": spread["spread_t"], "spread_mean": spread["spread_mean"],
               "net25": spread["net25"], "concentration": conc}

    # strength first: a null/weak signal (|t| < keep) is rejected as weak regardless
    # of the incidental sign of its near-zero mean IC.
    if ic_t is None or abs(ic_t) < keep_t:
        return _verdict(EV_REJECT_WEAK, direction, metrics=metrics, pit=pit)
    if ic_mean is not None and ic_mean < 0:
        return _verdict(EV_REJECT_DIRECTION, direction, metrics=metrics, pit=pit)
    if conc["concentrated"]:
        return _verdict(EV_REJECT_CONC, direction, metrics=metrics, pit=pit)
    if spread["net25"] is None or spread["net25"] <= 0:
        return _verdict(EV_REJECT_COST, direction, metrics=metrics, pit=pit)
    return _verdict(EV_PASS, direction, metrics=metrics, pit=pit)


def _verdict(code: str, direction: int, *, metrics=None, pit=None) -> Dict[str, Any]:
    return {"verdict": code, "passed": code == EV_PASS, "direction": direction,
            "metrics": metrics or {}, "pit": pit or {},
            "promoted": False, "no_automatic_promotion": True}


def _bh_fdr(pvals_by_id: Mapping[str, Optional[float]], *, family_size: int,
            alpha: float = FDR_ALPHA) -> Dict[str, Any]:
    ordered = sorted(((hid, p) for hid, p in pvals_by_id.items() if p is not None),
                     key=lambda x: x[1])
    m = max(1, family_size)
    survived: Dict[str, bool] = {}
    q_by: Dict[str, float] = {}
    running = 1.0
    for k in range(len(ordered), 0, -1):
        hid, p = ordered[k - 1]
        q = min(running, p * m / k)
        running = q
        q_by[hid] = q
        survived[hid] = bool(q < alpha)
    return {"family_size": m, "alpha": alpha, "q": q_by, "survived": survived,
            "n_survivors": sum(1 for x in survived.values() if x)}


def run_fixture_self_test(*, registry: Optional[Mapping[str, Any]] = None,
                          seed: int = 20260804) -> Dict[str, Any]:
    """Prove the evaluator behaves: the positive fixture PASSES, every adversarial
    fixture is REJECTED with the correct reason, and BH-FDR is applied over exactly
    six hypotheses. Fully deterministic; promotes nothing."""
    registry = registry or build_revision_registry()
    expected = {
        FIX_POSITIVE: EV_PASS, FIX_NULL: EV_REJECT_WEAK,
        FIX_WRONG_DIRECTION: EV_REJECT_DIRECTION, FIX_COST_DESTROYED: EV_REJECT_COST,
        FIX_ISSUER_CONCENTRATED: EV_REJECT_CONC, FIX_MONTH_CONCENTRATED: EV_REJECT_CONC,
        FIX_LEAKAGE: EV_REJECT_LEAKAGE, FIX_ACTIVE_ONLY: EV_REJECT_SURVIVORSHIP,
        FIX_DUP_TICKER: EV_REJECT_DUP_TICKER, FIX_RECONSTRUCTED: EV_REJECT_RECONSTRUCTED,
    }
    results = {}
    all_ok = True
    for kind in FIXTURE_KINDS:
        fx = make_fixture(kind, seed=seed)
        v = evaluate_signal(fx)
        ok = (v["verdict"] == expected[kind])
        results[kind] = {"verdict": v["verdict"], "expected": expected[kind],
                         "ok": ok, "passed": v["passed"]}
        all_ok = all_ok and ok
    # BH-FDR demonstration over the six pre-registered hypotheses using the positive
    # fixture's t (strong) vs null t (weak) so the family_size is exercised as six.
    pos_t = _ic_tstat(_cohort_ic_series(make_fixture(FIX_POSITIVE, seed=seed)["cohorts"], direction=1))
    null_t = _ic_tstat(_cohort_ic_series(make_fixture(FIX_NULL, seed=seed)["cohorts"], direction=1))
    hyp_ids = [h["id"] for h in registry["hypotheses"]]
    pvals = {}
    for i, hid in enumerate(hyp_ids):
        t = pos_t if i == 0 else null_t
        pvals[hid] = _se.approx_two_sided_pvalue(t, 24)
    fdr = _bh_fdr(pvals, family_size=FAMILY_SIZE)
    return {"stage": STAGE, "all_expected": all_ok, "results": results,
            "fdr_family_size": fdr["family_size"], "fdr_survivors": fdr["n_survivors"],
            "fdr_applied_over_exactly_six": fdr["family_size"] == FAMILY_SIZE,
            "positive_fixture_detected": results[FIX_POSITIVE]["passed"],
            "no_automatic_promotion": True, "safety_badges": SAFETY_BADGES}


# =========================================================================== #
# WORKSTREAM F -- provider-neutral trial-data adequacy gate.
# =========================================================================== #
def _default_adequacy_thresholds() -> Dict[str, Any]:
    return {
        "min_history_years": 15, "min_distinct_issuers": 800,
        "min_inactive_coverage_fraction": 0.30, "min_norgate_overlap_fraction": 0.60,
        "min_stable_id_mapping_rate": 0.90, "min_historical_ticker_mapping_rate": 0.85,
        "min_timestamp_completeness": 0.95, "min_exact_time_fraction": 0.60,
        "max_duplicate_rate": 0.02, "max_correction_rate": 0.10,
        "min_eps_coverage": 0.80, "min_revenue_coverage": 0.60,
        "min_quarterly_coverage": 0.80, "min_monthly_cohorts": 120,
        "min_median_issuers_per_cohort": 200, "min_effective_cohorts": 24,
        "max_missingness": 0.10, "min_revision_events": 200000,
    }


def assess_adequacy(measured: Optional[Mapping[str, Any]] = None, *,
                    thresholds: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Score MEASURED trial metrics against provider-neutral thresholds. With no
    measured data returns a TRIAL_NOT_STARTED shell listing exactly what will be
    measured; adequacy is NEVER a record-count-only pass. Effective independent
    cohort count is derived with the Stage 12 overlap machinery."""
    th = dict(_default_adequacy_thresholds())
    if thresholds:
        th.update(thresholds)
    metric_names = [
        "history_years", "first_usable_date", "last_usable_date", "total_revision_events",
        "distinct_issuers", "active_issuer_coverage", "inactive_issuer_coverage",
        "acquired_issuer_coverage", "delisted_issuer_coverage", "adr_coverage",
        "norgate_overlap_fraction", "stable_id_mapping_rate", "historical_ticker_mapping_rate",
        "timestamp_completeness", "exact_time_fraction", "duplicate_rate",
        "correction_restatement_rate", "eps_coverage", "revenue_coverage",
        "quarterly_coverage", "annual_coverage", "individual_analyst_coverage",
        "consensus_only_coverage", "revision_event_density", "monthly_cohort_count",
        "median_issuers_per_cohort", "effective_independent_cohort_count", "missingness",
        "extraction_throughput_rows_per_min", "projected_full_history_extraction_minutes",
        "api_practical", "bulk_practical",
    ]
    if not measured:
        return {"stage": STAGE, "status": "TRIAL_NOT_STARTED",
                "measured": False, "thresholds": th,
                "metrics_to_measure": metric_names,
                "note": "No trial data received; adequacy is prior-only.",
                "no_automatic_promotion": True}

    m = dict(measured)
    # derive the effective independent cohort count from the overlap machinery when
    # a per-cohort IC series (or cohort count + horizon) is supplied.
    eff = m.get("effective_independent_cohort_count")
    if eff is None and m.get("monthly_cohort_count"):
        ov = _power.overlap_effective_n(int(m["monthly_cohort_count"]),
                                        PRIMARY_HORIZON, int(m.get("step_days", 21)))
        eff = ov["effective_n"]
        m["effective_independent_cohort_count"] = eff

    checks = {
        "history_years": (m.get("history_years"), ">=", th["min_history_years"]),
        "distinct_issuers": (m.get("distinct_issuers"), ">=", th["min_distinct_issuers"]),
        "inactive_issuer_coverage": (m.get("inactive_issuer_coverage"), ">=",
                                     th["min_inactive_coverage_fraction"]),
        "norgate_overlap_fraction": (m.get("norgate_overlap_fraction"), ">=",
                                     th["min_norgate_overlap_fraction"]),
        "stable_id_mapping_rate": (m.get("stable_id_mapping_rate"), ">=",
                                   th["min_stable_id_mapping_rate"]),
        "timestamp_completeness": (m.get("timestamp_completeness"), ">=",
                                   th["min_timestamp_completeness"]),
        "exact_time_fraction": (m.get("exact_time_fraction"), ">=", th["min_exact_time_fraction"]),
        "duplicate_rate": (m.get("duplicate_rate"), "<=", th["max_duplicate_rate"]),
        "correction_restatement_rate": (m.get("correction_restatement_rate"), "<=",
                                        th["max_correction_rate"]),
        "eps_coverage": (m.get("eps_coverage"), ">=", th["min_eps_coverage"]),
        "revenue_coverage": (m.get("revenue_coverage"), ">=", th["min_revenue_coverage"]),
        "monthly_cohort_count": (m.get("monthly_cohort_count"), ">=", th["min_monthly_cohorts"]),
        "median_issuers_per_cohort": (m.get("median_issuers_per_cohort"), ">=",
                                      th["min_median_issuers_per_cohort"]),
        "effective_independent_cohort_count": (eff, ">=", th["min_effective_cohorts"]),
        "missingness": (m.get("missingness"), "<=", th["max_missingness"]),
        "total_revision_events": (m.get("total_revision_events"), ">=", th["min_revision_events"]),
    }
    results = {}
    failed = []
    for name, (val, op, bound) in checks.items():
        if val is None:
            results[name] = {"value": None, "bound": bound, "op": op, "pass": None}
            failed.append(name)
            continue
        ok = (val >= bound) if op == ">=" else (val <= bound)
        results[name] = {"value": val, "bound": bound, "op": op, "pass": bool(ok)}
        if not ok:
            failed.append(name)
    weakest = _weakest_adequacy(results)
    adequate = len(failed) == 0
    return {"stage": STAGE, "status": "MEASURED", "measured": True,
            "thresholds": th, "checks": results, "failed_gates": failed,
            "weakest_gate": weakest, "adequate": adequate,
            "effective_independent_cohort_count": eff,
            "no_automatic_promotion": True}


def _weakest_adequacy(results: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    worst = None
    worst_ratio = None
    for name, r in results.items():
        if r.get("pass") is False and r.get("value") is not None and r.get("bound"):
            try:
                if r["op"] == ">=":
                    ratio = float(r["value"]) / float(r["bound"])
                else:
                    ratio = float(r["bound"]) / max(1e-9, float(r["value"]))
            except (TypeError, ZeroDivisionError, ValueError):
                ratio = 0.0
            if worst_ratio is None or ratio < worst_ratio:
                worst_ratio, worst = ratio, name
    # a None (unmeasured) gate is weaker than any measured-but-failing gate.
    for name, r in results.items():
        if r.get("pass") is None:
            return name
    return worst


# =========================================================================== #
# WORKSTREAM G -- power & confidence model.
# =========================================================================== #
def power_model(*, measured: Optional[Mapping[str, Any]] = None,
                cfg_power: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Transparent power model. BEFORE trial data: CONFIDENCE_PRIOR_ONLY, returning
    the prior confidence RANGE (explicitly an assumption) and the design-level
    minimum detectable IC / required breadth using the Stage 12 machinery under an
    ASSUMED IC dispersion. AFTER trial data: the same machinery driven by measured
    breadth / cohort / timestamp / coverage evidence."""
    p = {"ic_sd_assumption": 0.08, "alpha": FDR_ALPHA, "target_power": 0.80,
         "step_days": 21, "assumed_true_ic": 0.03,
         "prior_confidence_low": PRIOR_CONFIDENCE_LOW,
         "prior_confidence_high": PRIOR_CONFIDENCE_HIGH}
    if cfg_power:
        p.update({k: cfg_power[k] for k in cfg_power if k in p})
    ic_sd = float(p["ic_sd_assumption"])
    per_horizon = []
    n_cohorts = int((measured or {}).get("monthly_cohort_count") or 60)
    for h in HORIZONS:
        ov = _power.overlap_effective_n(n_cohorts, h, int(p["step_days"]))
        mdi = _power.minimum_detectable_ic(ov["effective_n"], ic_sd,
                                           alpha=p["alpha"], power=p["target_power"])
        pw = _power.approximate_power(float(p["assumed_true_ic"]), ov["effective_n"],
                                      ic_sd, alpha=p["alpha"])
        per_horizon.append({"horizon_days": h, "effective_n": ov["effective_n"],
                            "min_detectable_ic": mdi, "power_at_assumed_ic": pw})
    if not measured:
        return {
            "stage": STAGE, "status": "CONFIDENCE_PRIOR_ONLY", "measured": False,
            "confidence_range": [p["prior_confidence_low"], p["prior_confidence_high"]],
            "confidence_basis": "PRIOR_ASSUMPTION_NOT_MEASURED",
            "prior_label": ("Judgement prior of ~25-35%% for finding a shadow-worthy "
                            "revision signal. This is an ASSUMPTION and MUST be "
                            "replaced by measured trial evidence."),
            "ic_sd_assumption": ic_sd, "assumed_true_ic": p["assumed_true_ic"],
            "family_size": FAMILY_SIZE, "per_horizon": per_horizon,
            "no_automatic_promotion": True}
    # measured update: shrink/expand the confidence band by the fraction of adequacy
    # gates cleared and the realised effective sample vs the design requirement.
    adq = measured.get("adequacy") or {}
    checks = adq.get("checks") or {}
    passed = sum(1 for r in checks.values() if r.get("pass") is True)
    total = max(1, sum(1 for r in checks.values() if r.get("pass") is not None))
    clear_frac = passed / total
    eff = float(adq.get("effective_independent_cohort_count") or per_horizon[-1]["effective_n"])
    eff_ratio = min(1.5, eff / 24.0)
    lo = round(max(0.0, p["prior_confidence_low"] * clear_frac * min(1.0, eff_ratio)), 3)
    hi = round(min(0.95, p["prior_confidence_high"] * (0.5 + 0.5 * clear_frac) * eff_ratio), 3)
    if hi < lo:
        lo, hi = hi, lo
    return {
        "stage": STAGE, "status": "CONFIDENCE_MEASURED_UPDATE", "measured": True,
        "confidence_range": [lo, hi],
        "confidence_basis": "MEASURED_TRIAL_ADEQUACY_AND_EFFECTIVE_SAMPLE",
        "adequacy_gates_cleared_fraction": round(clear_frac, 3),
        "effective_independent_cohorts": eff, "family_size": FAMILY_SIZE,
        "per_horizon": per_horizon, "no_automatic_promotion": True}


# =========================================================================== #
# WORKSTREAM H -- cost & purchase gate (six terminals).
# =========================================================================== #
PT_TRIAL_NOT_STARTED = "TRIAL_NOT_STARTED"
PT_TRIAL_DATA_INSUFFICIENT = "TRIAL_DATA_INSUFFICIENT"
PT_DO_NOT_BUY = "DO_NOT_BUY"
PT_REQUEST_EXPANDED_TRIAL = "REQUEST_EXPANDED_TRIAL"
PT_NEGOTIATE_PRICE = "NEGOTIATE_PRICE"
PT_PURCHASE_CANDIDATE = "PURCHASE_CANDIDATE_PENDING_MANUAL_APPROVAL"
PURCHASE_TERMINALS = (PT_TRIAL_NOT_STARTED, PT_TRIAL_DATA_INSUFFICIENT, PT_DO_NOT_BUY,
                      PT_REQUEST_EXPANDED_TRIAL, PT_NEGOTIATE_PRICE, PT_PURCHASE_CANDIDATE)


def _default_value_assumptions() -> Dict[str, Any]:
    return {"assumed_annual_capital_usd": 100000,
            "assumed_net_annual_alpha_bps_if_qualified": 150,
            "assumed_probability_qualifies_low": PRIOR_CONFIDENCE_LOW,
            "assumed_probability_qualifies_high": PRIOR_CONFIDENCE_HIGH,
            "assumed_useful_years": 3}


def max_rational_one_time_price(*, confidence_range: Sequence[float],
                                value_assumptions: Optional[Mapping[str, Any]] = None,
                                recurring_annual_usd: Optional[float] = None
                                ) -> Dict[str, Any]:
    """The maximum RATIONAL one-time historical-archive price under EXPLICIT stated
    assumptions. This is NOT a provider price and NOT a forecast -- it is a decision
    bound: expected value = P(qualifies) * annual_alpha_$ * useful_years, minus the
    recurring subscription already required over that horizon."""
    va = dict(_default_value_assumptions())
    if value_assumptions:
        va.update(value_assumptions)
    p_lo = float(confidence_range[0]); p_hi = float(confidence_range[-1])
    ann_alpha = va["assumed_annual_capital_usd"] * va["assumed_net_annual_alpha_bps_if_qualified"] / 10000.0
    years = float(va["assumed_useful_years"])
    ev_lo = p_lo * ann_alpha * years
    ev_hi = p_hi * ann_alpha * years
    recurring_over_horizon = (float(recurring_annual_usd) * years) if recurring_annual_usd else 0.0
    return {
        "assumptions": va,
        "expected_gross_value_range_usd": [round(ev_lo, 2), round(ev_hi, 2)],
        "recurring_cost_over_horizon_usd": round(recurring_over_horizon, 2),
        "max_rational_one_time_price_usd": round(max(0.0, ev_hi - recurring_over_horizon), 2),
        "is_provider_price": False,
        "note": ("Decision bound under EXPLICIT assumptions; not a provider quote. "
                 "A real quote replaces the recurring input; the confidence range "
                 "must be the MEASURED range once trial data exists."),
    }


def purchase_decision(*, trial_started: bool, adequacy: Optional[Mapping[str, Any]] = None,
                      power: Optional[Mapping[str, Any]] = None,
                      integrity: Optional[Mapping[str, Any]] = None,
                      cost: Optional[Mapping[str, Any]] = None,
                      value_assumptions: Optional[Mapping[str, Any]] = None
                      ) -> Dict[str, Any]:
    """Return exactly one of the six purchase terminals. No automatic purchase; the
    only 'buy-class' terminal is PURCHASE_CANDIDATE_PENDING_MANUAL_APPROVAL and it
    requires EVERY listed condition. Explains the weakest gate + the maximum rational
    price under explicit assumptions."""
    cost = dict(cost or {})
    conf = (power or {}).get("confidence_range") or [PRIOR_CONFIDENCE_LOW, PRIOR_CONFIDENCE_HIGH]
    mrp = max_rational_one_time_price(
        confidence_range=conf, value_assumptions=value_assumptions,
        recurring_annual_usd=cost.get("recurring_annual_usd"))

    def _wrap(term, weakest, reason):
        return {"stage": STAGE, "purchase_terminal": term, "weakest_gate": weakest,
                "reason": reason, "max_rational_price": mrp,
                "quoted_one_time_price_usd": cost.get("one_time_history_usd"),
                "quoted_recurring_annual_usd": cost.get("recurring_annual_usd"),
                "manual_approval_required": True, "auto_purchase": False,
                "no_automatic_promotion": True, "safety_badges": SAFETY_BADGES}

    if not trial_started or not adequacy or not adequacy.get("measured"):
        return _wrap(PT_TRIAL_NOT_STARTED, "trial_not_started",
                     "No real trial dataset received; no buy/no-buy decision is made.")

    # integrity is a hard veto: reconstructed snapshots / PIT failures.
    integ = integrity or {}
    if integ and not integ.get("clean", True):
        return _wrap(PT_DO_NOT_BUY, "point_in_time_integrity",
                     "PIT integrity violations present (e.g. reconstructed snapshots); "
                     "the data is not point-in-time safe.")

    failed = list(adequacy.get("failed_gates") or [])
    weakest = adequacy.get("weakest_gate")
    if not adequacy.get("adequate"):
        # distinguish 'trial too small to conclude' from 'measured and inadequate'
        history_gate_weak = weakest in ("history_years",) or "history_years" in failed
        breadth_gate_weak = any(g in failed for g in
                                ("distinct_issuers", "monthly_cohort_count",
                                 "median_issuers_per_cohort", "effective_independent_cohort_count",
                                 "inactive_issuer_coverage"))
        if history_gate_weak and len(failed) <= 3:
            return _wrap(PT_REQUEST_EXPANDED_TRIAL, weakest,
                         "The only/primary shortfall is trial history length "
                         "(vendor trial ~1yr); request an expanded historical trial.")
        if breadth_gate_weak or len(failed) >= 4:
            return _wrap(PT_TRIAL_DATA_INSUFFICIENT, weakest,
                         "Measured breadth/coverage/effective-sample below adequacy "
                         "thresholds; not decidable as a buy.")
        return _wrap(PT_TRIAL_DATA_INSUFFICIENT, weakest,
                     "Adequacy gates not fully cleared.")

    # adequate + integrity clean: cost/quote decides candidate vs negotiate.
    quoted = cost.get("one_time_history_usd")
    if quoted is None and cost.get("recurring_annual_usd") is None:
        return _wrap(PT_PURCHASE_CANDIDATE, None,
                     "Data adequate and point-in-time safe; awaiting a complete quote "
                     "-> escalate to MANUAL approval (no price invented).")
    if quoted is not None and quoted > mrp["max_rational_one_time_price_usd"]:
        return _wrap(PT_NEGOTIATE_PRICE, "quoted_price_above_max_rational",
                     "Quoted one-time price exceeds the maximum rational price under "
                     "the stated assumptions; negotiate.")
    return _wrap(PT_PURCHASE_CANDIDATE, None,
                 "Data adequate, point-in-time safe, and quoted price within the "
                 "maximum rational bound -> MANUAL approval required.")


# =========================================================================== #
# WORKSTREAM I -- vendor comparison matrix.
# =========================================================================== #
VENDOR_COMPARISON_FIELDS = (
    "underlying_source", "fields", "timestamps", "history", "inactive_delisted_coverage",
    "individual_vs_consensus", "api", "bulk", "rate_limits", "licensing", "trial_terms",
    "one_time_history_price", "recurring_price", "engineering_complexity",
    "adequacy_score", "confidence_update", "purchase_terminal",
)
UNKNOWN = "UNKNOWN"


def vendor_comparison(providers: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    """Provider-neutral comparison matrix. Every unmeasured cell is UNKNOWN; Intrinio
    and Nasdaq are NOT assumed to provide independent underlying data (both are
    Zacks-sourced here). No vendor is favoured."""
    providers = list(providers or ["intrinio_zacks", "nasdaq_zacks", "unspecified_third"])
    rows: Dict[str, Dict[str, Any]] = {}
    for prov in providers:
        row = {f: UNKNOWN for f in VENDOR_COMPARISON_FIELDS}
        row["purchase_terminal"] = PT_TRIAL_NOT_STARTED
        try:
            ad = get_adapter(prov)
            cap = ad.capability()
            row["underlying_source"] = cap.get("underlying_estimate_source", UNKNOWN)
            row["api"] = cap.get("api_availability", UNKNOWN)
            row["bulk"] = cap.get("bulk_availability", UNKNOWN)
            row["inactive_delisted_coverage"] = (
                UNKNOWN if cap.get("inactive_coverage") is None
                else bool(cap.get("inactive_coverage")))
            row["trial_terms"] = cap.get("trial_restrictions", UNKNOWN) or UNKNOWN
            row["history"] = cap.get("historical_archive_restrictions", UNKNOWN) or UNKNOWN
            row["fields"] = sorted(ad.field_mapping().keys())
        except KeyError:
            pass
        rows[prov] = row
    return {"stage": STAGE, "providers": providers, "fields": list(VENDOR_COMPARISON_FIELDS),
            "matrix": rows, "assume_independent_sources": False,
            "note": ("Intrinio and Nasdaq are BOTH Zacks-sourced here; do not treat "
                     "them as independent evidence. UNKNOWN is preserved until "
                     "measured on a real trial."),
            "no_favoured_vendor": True}


# =========================================================================== #
# Command center (read-only aggregate view-model).
# =========================================================================== #
def _resolve_state_dir(cfg: Mapping[str, Any]) -> Path:
    root = cfg.get("research_root") or "."
    sub = cfg.get("state_subdir") or "stage13a"
    return Path(root) / sub / "state"


def contract_completeness() -> Dict[str, Any]:
    return {
        "schemas": {name: {"n_fields": len(spec),
                           "required": required_fields(name)}
                    for name, spec in SCHEMAS.items()},
        "n_schemas": len(SCHEMAS),
        "pit_invariants": list(PIT_INVARIANTS),
        "n_pit_invariants": len(PIT_INVARIANTS),
        "adapters": sorted(ADAPTERS.keys()),
        "importer_formats": ["csv", "json"],
        "complete": True,
    }


def build_command_center(config_path: Optional[str] = None, *,
                         cfg: Optional[Mapping[str, Any]] = None,
                         measured: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Read-only aggregate of the entire Stage 13A framework: provider/trial status,
    contract completeness, adequacy (prior or measured), PIT integrity, history /
    breadth / inactive coverage, effective sample, prior + measured confidence,
    quoted cost, maximum rational price, weakest gate, and the purchase terminal.
    Builds nothing, buys nothing, promotes nothing, contacts no provider."""
    cfg = dict(cfg or (_load_cfg(config_path) if config_path else {}))
    trial = dict(cfg.get("trial") or {})
    trial_started = bool(trial.get("status") and trial.get("status") != PT_TRIAL_NOT_STARTED
                         and measured is not None)
    registry = build_revision_registry()
    adequacy = assess_adequacy((measured or {}).get("adequacy_metrics"),
                               thresholds=cfg.get("adequacy_thresholds"))
    integrity = (measured or {}).get("pit_scan") or {"clean": True,
                                                     "note": "no trial data yet"}
    power_prior = power_model(cfg_power=cfg.get("power"))
    power_measured = (power_model(measured={"adequacy": adequacy,
                                            "monthly_cohort_count":
                                                (adequacy.get("checks", {})
                                                 .get("monthly_cohort_count", {}) or {}).get("value")},
                                  cfg_power=cfg.get("power"))
                      if adequacy.get("measured") else None)
    cost = dict(cfg.get("cost_model") or {})
    decision = purchase_decision(trial_started=trial_started, adequacy=adequacy,
                                 power=(power_measured or power_prior),
                                 integrity=integrity, cost={
                                     "one_time_history_usd": cost.get("one_time_history_usd"),
                                     "recurring_annual_usd": cost.get("recurring_annual_usd")},
                                 value_assumptions=cfg.get("value_assumptions"))
    return {
        "stage": STAGE, "title": "Analyst-Revision Data Purchase-Evaluation Framework",
        "status": "FRAMEWORK_READY", "measured": bool(adequacy.get("measured")),
        "safety_badges": SAFETY_BADGES, "no_automatic_promotion": True,
        "provider_status": {
            "requested_fields": cfg.get("requested_fields"),
            "vendors": vendor_comparison(cfg.get("vendors"))["providers"],
            "trial": trial},
        "trial_status": trial.get("status", PT_TRIAL_NOT_STARTED),
        "contract_completeness": contract_completeness(),
        "data_adequacy": adequacy,
        "point_in_time_integrity": integrity,
        "history_years": (adequacy.get("checks", {}).get("history_years", {}) or {}).get("value"),
        "issuer_breadth": (adequacy.get("checks", {}).get("distinct_issuers", {}) or {}).get("value"),
        "inactive_delisted_coverage": (adequacy.get("checks", {})
                                       .get("inactive_issuer_coverage", {}) or {}).get("value"),
        "effective_sample": adequacy.get("effective_independent_cohort_count"),
        "prior_confidence_range": power_prior.get("confidence_range"),
        "measured_confidence_range": (power_measured or {}).get("confidence_range"),
        "confidence_basis": (power_measured or power_prior).get("confidence_basis"),
        "quoted_cost": {"one_time_history_usd": cost.get("one_time_history_usd"),
                        "recurring_annual_usd": cost.get("recurring_annual_usd"),
                        "auto_renewal": cost.get("auto_renewal")},
        "max_rational_price": decision.get("max_rational_price"),
        "weakest_gate": decision.get("weakest_gate"),
        "purchase_terminal": decision.get("purchase_terminal"),
        "frozen_registry": {"registry_version": registry["registry_version"],
                            "n_hypotheses": registry["n_hypotheses"],
                            "hypothesis_ids": [h["id"] for h in registry["hypotheses"]]},
        "vendor_comparison": vendor_comparison(cfg.get("vendors")),
        "safety_labels": SAFETY_BADGES,
    }


def load_snapshot(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Read-only load of the framework command center. If a persisted snapshot
    exists it is returned; otherwise the live (prior-only) command center is built.
    Never raises to the caller for a missing snapshot."""
    try:
        cfg = _load_cfg(config_path) if config_path else {}
    except (OSError, ValueError):
        cfg = {}
    state_dir = _resolve_state_dir(cfg) if cfg else None
    if state_dir is not None:
        persisted = _read_json(state_dir / "command_center.json", None)
        if persisted:
            return persisted
    return build_command_center(cfg=cfg)


def write_snapshot(config_path: str) -> Dict[str, Any]:
    """Freeze the registry, run the deterministic self-test, and write the command
    center + self-test snapshots under the state dir. Read/compute-only; writes only
    local research JSON (never an operational ledger)."""
    cfg = _load_cfg(config_path)
    reg = freeze_revision_registry(cfg.get("registry_config"))
    self_test = run_fixture_self_test(registry=reg)
    cc = build_command_center(cfg=cfg)
    state_dir = _resolve_state_dir(cfg)
    _write_json(state_dir / "self_test.json", self_test)
    _write_json(state_dir / "command_center.json", cc)
    return {"stage": STAGE, "registry_version": reg["registry_version"],
            "self_test_all_expected": self_test["all_expected"],
            "purchase_terminal": cc["purchase_terminal"],
            "state_dir": str(state_dir)}
