"""api/universe_scoring.py - Slice 4 (Phase 29E) canonical universe-scoring composition & read owner.

This module is the ONE authoritative *operational* interpretation of the current
universe scoring & ranking. It is NOT a scoring engine: all model mathematics
(z-score / percentile / rank / combined blend / book construction) remain owned by
the pure kernel ``api.multi_horizon_engine`` (``build_current`` composing
``load_inputs`` -> ``compute_scores`` -> ``compute_combined`` -> ``build_books``).
This owner:

  * calls the kernel exactly once per build,
  * DEEP-COPIES the (mtime-cached) kernel result before reading it, so the cache
    object is never mutated and no consumer can contaminate a later result,
  * normalises the kernel output into ONE frozen read contract with explicit
    strategy / model / universe identity, deterministic rankings, exclusions with
    reconciled counts, TOP25 / TOP50 books, coverage and point-in-time provenance,
  * derives ONE deterministic *content-level* input-contract hash (over the owned
    input fingerprints + the frozen model / construction / eligibility contract,
    never over wall-clock, object identity, absolute path or file mtime),
  * offers a deterministic cross-consumer consistency validator.

Strictly read-only. No provider / network / prediction / database / operational
ledger / order / signal / decision / fill path exists here. A missing owned input
degrades to an explicit ``*_UNAVAILABLE`` status (HTTP 200), never a stack trace.
Automatic model promotion / recalibration is impossible from here
(``AUTOMATIC_PROMOTION_ALLOWED = False``); the champion model is never replaced.
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_registry as mreg

PHASE = "29E"
CONTRACT_ID = "universe_scoring/1"

STATUS_READY = "UNIVERSE_SCORING_READY"
STATUS_INPUTS_UNAVAILABLE = "UNIVERSE_SCORING_INPUTS_UNAVAILABLE"

# The pure computational kernel this owner composes (never re-implements).
SCORING_KERNEL = "api.multi_horizon_engine"
KERNEL_BUILD_FN = "build_current"

# --------------------------------------------------------------------------- #
# Frozen operational scoring identity (single owner). Consumers re-export these
# so the strategy / model / book identity has exactly one source of truth.
# --------------------------------------------------------------------------- #
STRATEGY_ID = "fundamental_momentum_50_50_v1"
STRATEGY_VERSION = "v1"
PRIMARY_MODEL_ID = "fundamental_momentum_50_50_v1"
PRIMARY_BOOK_ID = "fundamental_momentum_50_50_top25"
TOP50_BOOK_ID = "fundamental_momentum_50_50_top50"
MODEL_REGISTRY_VERSION = mreg.PHASE  # the durable model-contract registry version.

# Workstream L: the current paper champion is NEVER auto-replaced by this owner.
CHAMPION_MODEL_ID = "composite_sn"
CHALLENGER_MODEL_IDS = ("mom_6_1", "fundamental_momentum_50_50_v1")
AUTOMATIC_PROMOTION_ALLOWED = False

# --------------------------------------------------------------------------- #
# Universe identity (Workstream F). The OPERATIONAL universe is the common
# eligible cross-section of the owned fundamental panel and the current PIT
# momentum members - it is deliberately NOT labelled a strict S&P 500 list, and
# membership that the owned inputs do not confirm is UNKNOWN, never substituted.
# --------------------------------------------------------------------------- #
UNIVERSE_ID = "phase8v_combined_eodhd_price_fundamentals_universe"
UNIVERSE_LABEL = "Combined EODHD price + fundamentals operational universe"
UNIVERSE_DEFINITION = (
    "Common eligible cross-section: owned fundamental-panel names (composite_sn, "
    "sector-neutral quality/cash-flow) that are current point-in-time momentum "
    "members (mom_6_1). This is NOT a strict S&P 500 constituent list.")
UNIVERSE_IS_STRICT_SP500 = False
UNIVERSE_SOURCE = (
    "owned frozen Phase 10-L sector-neutral scored panel (fundamental leg) "
    "intersected with owned survivorship-free Russell-1000 PIT momentum members")
UNIVERSE_POINT_IN_TIME_POLICY = (
    "Fundamentals as-of the panel rebalance date; momentum uses only closes "
    "through the prior month; membership as-of the momentum month. No future "
    "fundamentals; delisted names retained in history.")
UNIVERSE_CURRENT_MEMBER_POLICY = (
    "A name is eligible only when it is a current PIT momentum member AND has a "
    "valid fundamental score; names not confirmed as members are UNKNOWN.")
UNIVERSE_UNKNOWN_MEMBERSHIP_POLICY = "explicit"

# Safety-badge vocabulary surfaced with the read contract.
SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "MANUAL REVIEW"]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(payload: Any) -> str:
    """Deterministic short content hash over a JSON-canonicalised payload.

    Matches ``api.daily_research_cycle._hash`` (sha256 hexdigest[:24]) so the two
    owners speak the same hash vocabulary.
    """
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _is_ready_kernel(built: Any) -> bool:
    return isinstance(built, dict) and built.get("status") == eng.STATUS_READY


def _construction_contract() -> dict:
    """The frozen construction / weight / eligibility constants read from the
    kernel (never redefined here). A change to any constant changes the hash."""
    return {
        "book_sizes": list(eng.BOOK_SIZES),
        "sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
        "max_individual_weight": eng.MAX_INDIVIDUAL_WEIGHT,
        "min_adv_dollar": eng.MIN_ADV_DOLLAR,
        "entry_buffer": eng.ENTRY_BUFFER,
        "exit_buffer_fraction": eng.EXIT_BUFFER_FRACTION,
        "cost_bps": eng.COST_BPS,
    }


def _weights_contract() -> dict:
    return {
        "primary_weights": dict(eng.PRIMARY_WEIGHTS),
        "sensitivity_views": {k: dict(v) for k, v in eng.SENSITIVITY_VIEWS.items()},
    }


# --------------------------------------------------------------------------- #
# Input-contract hash (Workstream C). Content-level: over the owned input
# fingerprints + the frozen model / construction / weights / eligibility contract
# and the resolved point-in-time dates. NEVER over evaluated_at, built_at, object
# identity, absolute path alone, or file mtime alone.
# --------------------------------------------------------------------------- #
def input_contract_components(built: dict) -> dict:
    """The deterministic content that defines the scoring input contract."""
    src = built if isinstance(built, dict) else {}
    inputs = src.get("inputs") or {}
    fingerprints = inputs.get("fingerprints") or {}
    # Only the content-level file fingerprints (sha1 of file bytes) participate -
    # explicitly NOT the absolute paths (inputs["paths"]) or any mtime.
    fp = {k: fingerprints.get(k) for k in sorted(fingerprints)}
    return {
        "contract": CONTRACT_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "primary_model_id": PRIMARY_MODEL_ID,
        "primary_book_id": PRIMARY_BOOK_ID,
        "model_registry_version": MODEL_REGISTRY_VERSION,
        "universe_id": UNIVERSE_ID,
        "ranking_date": src.get("market_as_of_date"),
        "momentum_month": src.get("momentum_month"),
        "fundamental_as_of_date": src.get("fundamental_as_of_date"),
        "fundamental_month": src.get("fundamental_month"),
        "input_fingerprints": fp,
        "construction": _construction_contract(),
        "weights": _weights_contract(),
        "eligibility": {
            "min_adv_dollar": eng.MIN_ADV_DOLLAR,
            "requires_current_member": True,
            "requires_eligible_history": True,
            "blocks_extreme_momentum": True,
            "requires_valid_composite_sn": True,
        },
    }


def compute_input_contract_hash(built: dict) -> str:
    return _hash(input_contract_components(built))


# --------------------------------------------------------------------------- #
# Ranking + exclusion + universe normalisation (Workstream B / E / F).
# --------------------------------------------------------------------------- #
def _eligible_count(score_map: dict, declared: Optional[int]) -> int:
    if isinstance(declared, int):
        return declared
    return sum(1 for s in score_map.values()
               if isinstance(s, dict) and s.get("eligible"))


def _rank_rows(combined: dict, fund_scores: dict, mom_scores: dict,
               market_as_of: Optional[str]) -> list[dict]:
    """Full deterministic combined ranking (rank asc, ties already broken by the
    kernel's score-desc / ticker-asc rule). Reads kernel values only."""
    rows: list[dict] = []
    for tk, c in combined.items():
        if not isinstance(c, dict):
            continue
        fsc = fund_scores.get(tk) or {}
        msc = mom_scores.get(tk) or {}
        flags = list(fsc.get("data_quality_flags") or [])
        for f in (msc.get("data_quality_flags") or []):
            if f not in flags:
                flags.append(f)
        rows.append({
            "ticker": tk,
            "rank": c.get("rank"),
            "combined_score": c.get("combined_score"),
            "percentile": c.get("percentile"),
            "fundamental_score": fsc.get("raw_signal"),
            "fundamental_rank": c.get("fund_rank"),
            "fundamental_percentile": c.get("fund_percentile"),
            "momentum_score": msc.get("raw_signal"),
            "momentum_rank": c.get("mom_rank"),
            "momentum_percentile": c.get("mom_percentile"),
            "sector": c.get("sector"),
            "adv_dollar": c.get("adv_dollar"),
            "eligible": True,
            "exclusion_reason": None,
            "data_quality_flags": flags,
            "market_as_of_date": market_as_of,
            "model_id": c.get("model_id") or PRIMARY_MODEL_ID,
            "model_version": c.get("model_version") or STRATEGY_VERSION,
        })
    rows.sort(key=lambda r: (r["rank"] if r.get("rank") is not None else 1_000_000,
                             r["ticker"]))
    return rows


def _book_rows(books: dict, book_id: str, combined: dict,
               market_as_of: Optional[str]) -> list[dict]:
    """A TOP-N book's constituents (deterministic, sector-capped, equal weight)
    enriched with the combined-ranking facts. Order = the kernel's book order."""
    book = books.get(book_id) or {}
    out: list[dict] = []
    for c in (book.get("constituents") or []):
        tk = c.get("ticker")
        cm = combined.get(tk) or {}
        out.append({
            "ticker": tk,
            "rank": c.get("rank"),                 # position within the book (1..N)
            "weight": c.get("weight"),
            "sector": c.get("sector"),
            "adv_dollar": c.get("adv_dollar"),
            "combined_score": cm.get("combined_score"),
            "combined_rank": cm.get("rank"),        # score rank in the full universe
            "percentile": cm.get("percentile"),
            "market_as_of_date": market_as_of,
            "model_id": book.get("model_id") or PRIMARY_MODEL_ID,
            "model_version": STRATEGY_VERSION,
        })
    return out


def _universe_identity(scored_count: Optional[int], eligible_count: Optional[int],
                       excluded_count: Optional[int], unknown_membership: Optional[int],
                       market_as_of: Optional[str]) -> dict:
    return {
        "universe_id": UNIVERSE_ID,
        "universe_label": UNIVERSE_LABEL,
        "universe_definition": UNIVERSE_DEFINITION,
        "is_strict_sp500": UNIVERSE_IS_STRICT_SP500,
        "source": UNIVERSE_SOURCE,
        "as_of_date": market_as_of,
        "point_in_time_policy": UNIVERSE_POINT_IN_TIME_POLICY,
        "current_member_policy": UNIVERSE_CURRENT_MEMBER_POLICY,
        "delisted_treatment": "retained in history; not eligible when not a current member",
        "unknown_membership_policy": UNIVERSE_UNKNOWN_MEMBERSHIP_POLICY,
        "seed_universe_size": None,   # the kernel does not expose a seed index size
        "scored_universe_size": scored_count,
        "eligible_count": eligible_count,
        "excluded_count": excluded_count,
        "unknown_membership_count": unknown_membership,
    }


def _safety_block() -> dict:
    return {
        "paper_only": True,
        "read_only": True,
        "preview_only": True,
        "no_orders": True,
        "no_signals": True,
        "no_trade_decisions": True,
        "no_fills": True,
        "no_broker": True,
        "no_automation": True,
        "calls_prediction_service": False,
        "calls_provider": False,
        "wrote_to_database": False,
        "wrote_to_operational_ledger": False,
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_fills": False,
        "automatic_model_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
        "champion_replaced": False,
        "champion_model_id": CHAMPION_MODEL_ID,
        "safety_badges": list(SAFETY_BADGES),
    }


def _degraded_contract(built: dict) -> dict:
    src = built if isinstance(built, dict) else {}
    return {
        "contract": CONTRACT_ID,
        "phase": PHASE,
        "status": STATUS_INPUTS_UNAVAILABLE,
        "kernel_status": src.get("status"),
        "evaluated_at": _iso_now(),
        "scoring_kernel": SCORING_KERNEL,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "primary_model_id": PRIMARY_MODEL_ID,
        "primary_book_id": PRIMARY_BOOK_ID,
        "universe_id": UNIVERSE_ID,
        "eligible_market_date": None,
        "ranking_date": None,
        "input_contract_hash": compute_input_contract_hash(src),
        "rankings": [],
        "top25": [],
        "top50": [],
        "exclusions": {},
        "warnings": list(src.get("warnings") or []),
        "blockers": [{"code": "SCORING_INPUTS_UNAVAILABLE",
                      "detail": src.get("status")}],
        "consistency_status": "UNKNOWN",
        "consistency_violations": [],
        "safety": _safety_block(),
    }


# --------------------------------------------------------------------------- #
# Canonical normalisation (Workstream B / D). Deep-copies the (cached) kernel
# object FIRST so the cache is never mutated and no later result is contaminated.
# --------------------------------------------------------------------------- #
def normalize_built(built: dict) -> dict:
    """Normalise a kernel ``build_current`` result into the canonical contract.

    ``built`` is deep-copied before any read; the kernel cache object is never
    mutated. Accepts either a live kernel result or the canonical contract (a
    no-op passthrough) so callers can pass either shape.
    """
    if isinstance(built, dict) and built.get("contract") == CONTRACT_ID:
        return built  # already canonical (idempotent)
    if not _is_ready_kernel(built):
        return _degraded_contract(built if isinstance(built, dict) else {})

    src = copy.deepcopy(built)  # cache-safety: never read the cached object directly.

    scores = src.get("scores") or {}
    combined_wrap = src.get("combined") or {}
    combined = combined_wrap.get("combined") or {}
    books_wrap = src.get("books") or {}
    books = books_wrap.get("books") or {}
    inputs = src.get("inputs") or {}
    validations = inputs.get("validations") or {}
    fingerprints = inputs.get("fingerprints") or {}
    counts = scores.get("counts") or {}
    fund_scores = scores.get("composite_sn") or {}
    mom_scores = scores.get("mom_6_1") or {}

    market_as_of = src.get("market_as_of_date")

    # Reconciliation counts (Workstream E).
    scored_count = len(fund_scores)
    fundamental_eligible_count = _eligible_count(fund_scores, counts.get("composite_sn_eligible"))
    momentum_eligible_count = _eligible_count(mom_scores, counts.get("mom_6_1_eligible"))
    combined_eligible_count = combined_wrap.get("n_common")
    if combined_eligible_count is None:
        combined_eligible_count = len(combined)

    # Fundamental-leg exclusions (a scored fundamental name that is not eligible).
    exclusions = {tk: sd.get("exclusion_reason")
                  for tk, sd in fund_scores.items()
                  if isinstance(sd, dict) and not sd.get("eligible")}
    excluded_count = len(exclusions)
    exclusion_summary: dict[str, int] = {}
    for reason in exclusions.values():
        key = reason or "UNSPECIFIED"
        exclusion_summary[key] = exclusion_summary.get(key, 0) + 1

    # Operational combined-universe drop: names scored on the fundamental leg that
    # did NOT enter the combined eligible universe (mostly momentum-leg reasons -
    # not a current member, insufficient history, liquidity, extreme). This is the
    # meaningful reduction that coverage_ratio measures. Reconciles:
    #   scored_count == combined_eligible_count + combined_excluded_count.
    combined_excluded_count = (scored_count - combined_eligible_count
                               if isinstance(combined_eligible_count, int) else None)
    momentum_exclusion_summary: dict[str, int] = {}
    for sd in mom_scores.values():
        if isinstance(sd, dict) and not sd.get("eligible"):
            key = sd.get("exclusion_reason") or "UNSPECIFIED"
            momentum_exclusion_summary[key] = momentum_exclusion_summary.get(key, 0) + 1

    unknown_membership = sum(
        1 for sd in fund_scores.values()
        if isinstance(sd, dict) and (sd.get("sector") in (None, "Unknown")))

    coverage_ratio = (round(combined_eligible_count / scored_count, 6)
                      if scored_count and combined_eligible_count is not None else None)

    rankings = _rank_rows(combined, fund_scores, mom_scores, market_as_of)
    top25 = _book_rows(books, PRIMARY_BOOK_ID, combined, market_as_of)
    top50 = _book_rows(books, TOP50_BOOK_ID, combined, market_as_of)
    # TOP25 is NOT guaranteed to be a subset of TOP50: the greedy per-sector cap is
    # looser at size 50 (max 12/sector) than at size 25 (max 6/sector), so TOP50's
    # early slots can be consumed by names TOP25 sector-capped out. This is exposed
    # explicitly rather than asserted as a false invariant ("where policy permits").
    _t25 = [r.get("ticker") for r in top25]
    _t50 = set(r.get("ticker") for r in top50)
    top25_in_top50 = sum(1 for t in _t25 if t in _t50)
    top25_subset_of_top50 = (top25_in_top50 == len(_t25)) if _t25 else True

    primary_book = books.get(PRIMARY_BOOK_ID) or {}
    construction = {
        **_construction_contract(),
        **_weights_contract(),
        "long_only": True,
        "manual_rebalance_only": True,
        "equal_weight": primary_book.get("equal_weight"),
        "max_per_sector": primary_book.get("max_per_sector"),
        "primary_book_size_target": primary_book.get("size_target"),
        "primary_book_size_actual": primary_book.get("size_actual"),
        "sector_capped_out": list(primary_book.get("sector_capped_out") or []),
    }

    ich = compute_input_contract_hash(src)
    contract = {
        "contract": CONTRACT_ID,
        "phase": PHASE,
        "status": STATUS_READY,
        "compatibility_surface": False,
        "evaluated_at": _iso_now(),
        "scoring_kernel": SCORING_KERNEL,
        "kernel_status": src.get("status"),

        # identity
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "model_registry_version": MODEL_REGISTRY_VERSION,
        "primary_model_id": PRIMARY_MODEL_ID,
        "primary_book_id": books_wrap.get("primary_book_id") or PRIMARY_BOOK_ID,
        "champion_model_id": CHAMPION_MODEL_ID,
        "challenger_model_ids": list(CHALLENGER_MODEL_IDS),

        # dates / provenance
        "eligible_market_date": market_as_of,
        "ranking_date": market_as_of,
        "momentum_market_date": market_as_of,
        "momentum_month": src.get("momentum_month"),
        "fundamental_as_of_date": src.get("fundamental_as_of_date"),
        "fundamental_month": src.get("fundamental_month"),
        "risk_as_of_date": market_as_of if validations.get("risk_available") else None,
        "input_contract_hash": ich,
        "input_fingerprints": {k: fingerprints.get(k) for k in sorted(fingerprints)},
        "source_statuses": {
            "fundamental_available": bool(validations.get("fundamental_available")),
            "momentum_available": bool(validations.get("momentum_available")),
            "risk_available": bool(validations.get("risk_available")),
            "sector_map_available": bool(validations.get("sector_map_available")),
        },

        # universe identity (Workstream F)
        "universe_id": UNIVERSE_ID,
        "universe_label": UNIVERSE_LABEL,
        "universe_definition": UNIVERSE_DEFINITION,
        "universe_as_of_date": market_as_of,
        "universe": _universe_identity(scored_count, fundamental_eligible_count,
                                       excluded_count, unknown_membership, market_as_of),

        # counts (Workstream E reconciliation)
        "universe_size": scored_count,
        "scored_count": scored_count,
        "fundamental_eligible_count": fundamental_eligible_count,
        "momentum_eligible_count": momentum_eligible_count,
        "combined_eligible_count": combined_eligible_count,
        "excluded_count": excluded_count,
        "combined_excluded_count": combined_excluded_count,
        "unknown_membership_count": unknown_membership,
        "coverage_ratio": coverage_ratio,
        "exclusion_summary": exclusion_summary,
        "exclusions": exclusions,
        "momentum_exclusion_summary": momentum_exclusion_summary,

        # rankings + books
        "rankings": rankings,
        "top25": top25,
        "top50": top50,
        "top25_count": len(top25),
        "top50_count": len(top50),
        "top25_in_top50_count": top25_in_top50,
        "top25_subset_of_top50": top25_subset_of_top50,
        "construction": construction,

        # hashes
        "output_hash": _hash({"ranking_date": market_as_of,
                              "top25": [r.get("ticker") for r in top25],
                              "top50": [r.get("ticker") for r in top50],
                              "input_contract_hash": ich}),

        "warnings": list(src.get("warnings") or []),
        "blockers": [],
    }

    consistency = _self_consistency(contract)
    contract["consistency_status"] = consistency["consistency_status"]
    contract["consistency_violations"] = consistency["consistency_violations"]
    contract["safety"] = _safety_block()
    return contract


# --------------------------------------------------------------------------- #
# Canonical build entrypoint (Workstream B / D). Calls the kernel exactly once.
# --------------------------------------------------------------------------- #
def build_universe_scoring(*, panel_path=None, inputs_dir=None, use_cache=True) -> dict:
    """The ONE canonical operational universe-scoring build. Never raises."""
    built = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir,
                              use_cache=use_cache)
    return normalize_built(built)


def load_universe_scoring(*, panel_path=None, inputs_dir=None) -> dict:
    """Read alias used by the canonical GET endpoint."""
    return build_universe_scoring(panel_path=panel_path, inputs_dir=inputs_dir)


# --------------------------------------------------------------------------- #
# Canonical identity subset (used by consumers to reference canonical facts).
# --------------------------------------------------------------------------- #
def canonical_identity(contract_or_built: dict) -> dict:
    c = normalize_built(contract_or_built)
    return {
        "strategy_id": c.get("strategy_id"),
        "strategy_version": c.get("strategy_version"),
        "primary_model_id": c.get("primary_model_id"),
        "primary_book_id": c.get("primary_book_id"),
        "universe_id": c.get("universe_id"),
        "ranking_date": c.get("ranking_date"),
        "input_contract_hash": c.get("input_contract_hash"),
        "top25": [r.get("ticker") for r in (c.get("top25") or [])],
        "top50": [r.get("ticker") for r in (c.get("top50") or [])],
        "status": c.get("status"),
    }


# --------------------------------------------------------------------------- #
# Consistency (Workstream J). Self-consistency (structural) + a deterministic
# cross-consumer validator. Never silently chooses a conflicting value.
# --------------------------------------------------------------------------- #
CONSISTENT = "CONSISTENT"
INCONSISTENT = "INCONSISTENT"
UNKNOWN = "UNKNOWN"
CANONICAL_OWNER = "api.universe_scoring"


def _self_consistency(contract: dict) -> dict:
    """Structural reconciliation of the canonical contract itself."""
    violations: list[dict] = []
    scored = contract.get("scored_count")
    elig = contract.get("fundamental_eligible_count")
    excl = contract.get("excluded_count")
    if isinstance(scored, int) and isinstance(elig, int) and isinstance(excl, int):
        if scored != elig + excl:
            violations.append({
                "concept": "count_reconciliation",
                "canonical_owner": CANONICAL_OWNER,
                "conflicting_owner": CANONICAL_OWNER,
                "canonical_value": scored,
                "conflicting_value": elig + excl,
            })
    top25 = [r.get("ticker") for r in (contract.get("top25") or [])]
    top50 = [r.get("ticker") for r in (contract.get("top50") or [])]
    rank_set = {r.get("ticker") for r in (contract.get("rankings") or [])}
    if len(top25) != len(set(top25)):
        violations.append({"concept": "top25_duplicate_ticker",
                           "canonical_owner": CANONICAL_OWNER,
                           "conflicting_owner": CANONICAL_OWNER,
                           "canonical_value": len(set(top25)),
                           "conflicting_value": len(top25)})
    if rank_set:
        stray = sorted(set(top25) - rank_set)
        if stray:
            violations.append({"concept": "top25_not_in_rankings",
                               "canonical_owner": CANONICAL_OWNER,
                               "conflicting_owner": CANONICAL_OWNER,
                               "canonical_value": sorted(rank_set)[:5],
                               "conflicting_value": stray[:5]})
    status = INCONSISTENT if violations else CONSISTENT
    return {"consistency_status": status, "consistency_violations": violations}


# The consumer fields compared against the canonical contract, and the canonical
# key each maps to. Absent / None consumer fields are treated as UNKNOWN (not a
# violation); a present-and-different value is a violation.
_CONSUMER_FIELD_MAP = {
    "ranking_date": "ranking_date",
    "primary_book_id": "primary_book_id",
    "primary_model_id": "primary_model_id",
    "strategy_version": "strategy_version",
    "strategy_id": "strategy_id",
    "universe_id": "universe_id",
    "input_contract_hash": "input_contract_hash",
    "top25": "__top25__",
    "top50": "__top50__",
}


def check_scoring_consistency(canonical: dict, consumers: dict) -> dict:
    """Deterministic cross-consumer consistency validator (Workstream J).

    ``canonical`` is a universe-scoring contract (or ``canonical_identity`` dict).
    ``consumers`` maps owner-name -> a dict of any of the fields in
    ``_CONSUMER_FIELD_MAP``. Returns CONSISTENT / INCONSISTENT / UNKNOWN plus every
    violation naming both owners and both values. Never chooses a value.
    """
    # Accept a raw kernel result (normalise it), a full canonical contract, or a
    # canonical_identity subset (both used as-is).
    can = normalize_built(canonical) if _is_ready_kernel(canonical) else (canonical or {})
    can_top25 = ([r.get("ticker") if isinstance(r, dict) else r
                  for r in (can.get("top25") or [])])
    can_top50 = ([r.get("ticker") if isinstance(r, dict) else r
                  for r in (can.get("top50") or [])])
    can_vals = {
        "ranking_date": can.get("ranking_date"),
        "primary_book_id": can.get("primary_book_id"),
        "primary_model_id": can.get("primary_model_id"),
        "strategy_version": can.get("strategy_version"),
        "strategy_id": can.get("strategy_id"),
        "universe_id": can.get("universe_id"),
        "input_contract_hash": can.get("input_contract_hash"),
        "__top25__": can_top25,
        "__top50__": can_top50,
    }

    if can.get("status") == STATUS_INPUTS_UNAVAILABLE:
        return {"consistency_status": UNKNOWN, "consistency_violations": [],
                "compared_owners": sorted(consumers or {})}

    violations: list[dict] = []
    compared = 0
    for owner in sorted(consumers or {}):
        facts = consumers[owner] or {}
        for cfield, ckey in _CONSUMER_FIELD_MAP.items():
            if cfield not in facts:
                continue
            cv = facts.get(cfield)
            if cv is None:
                continue  # not supplied -> UNKNOWN for this field
            canonical_value = can_vals.get(ckey)
            if canonical_value is None:
                continue
            if cfield in ("top25", "top50"):
                cv_list = [x.get("ticker") if isinstance(x, dict) else x for x in cv]
                if list(cv_list) != list(canonical_value):
                    violations.append({
                        "concept": cfield,
                        "canonical_owner": CANONICAL_OWNER,
                        "conflicting_owner": owner,
                        "canonical_value": list(canonical_value),
                        "conflicting_value": list(cv_list),
                    })
                compared += 1
                continue
            compared += 1
            if cv != canonical_value:
                violations.append({
                    "concept": cfield,
                    "canonical_owner": CANONICAL_OWNER,
                    "conflicting_owner": owner,
                    "canonical_value": canonical_value,
                    "conflicting_value": cv,
                })
    if violations:
        status = INCONSISTENT
    elif compared == 0:
        status = UNKNOWN
    else:
        status = CONSISTENT
    return {"consistency_status": status, "consistency_violations": violations,
            "compared_owners": sorted(consumers or {})}


__all__ = [
    "PHASE", "CONTRACT_ID", "STATUS_READY", "STATUS_INPUTS_UNAVAILABLE",
    "SCORING_KERNEL", "KERNEL_BUILD_FN",
    "STRATEGY_ID", "STRATEGY_VERSION", "PRIMARY_MODEL_ID", "PRIMARY_BOOK_ID",
    "TOP50_BOOK_ID", "MODEL_REGISTRY_VERSION", "CHAMPION_MODEL_ID",
    "CHALLENGER_MODEL_IDS", "AUTOMATIC_PROMOTION_ALLOWED",
    "UNIVERSE_ID", "UNIVERSE_LABEL", "UNIVERSE_DEFINITION", "UNIVERSE_IS_STRICT_SP500",
    "CONSISTENT", "INCONSISTENT", "UNKNOWN", "CANONICAL_OWNER",
    "input_contract_components", "compute_input_contract_hash",
    "normalize_built", "build_universe_scoring", "load_universe_scoring",
    "canonical_identity", "check_scoring_consistency",
]
