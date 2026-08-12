r"""Phase 29H Slice 7 — Reallocation Proposal composition, persistence & read owner.

This is the ONE canonical orchestration / validation / immutable-artifact /
read-contract owner for the Portfolio Reallocation Proposal Engine (Consolidation
Roadmap Slice 7 / Charter Milestone 3). It performs NO allocation calculation of its
own — the single canonical calculation lives in ``engine.reallocation_proposal``. This
module only:

  1. SOURCES an immutable point-in-time reallocation-input contract from the
     authoritative owners (``api.portfolio_state`` for holdings / weights / NAV / cash /
     sectors; the Slice 6 ``api.holding_opportunity_cost`` assessment for per-holding
     recommendations / replacements / switching costs; ``api.universe_scoring`` for the
     eligible candidate ranking / eligibility / adv_dollar; ``api.price_panel`` for owned
     point-in-time returns used by the before/after covariance), reusing the canonical
     construction constants from ``api.multi_horizon_engine`` and the transaction-cost
     constant from ``api.paper_trading_desk`` (never forked).
  2. RUNS the pure kernel.
  3. PERSISTS a completed production proposal as an immutable artifact under a dedicated
     research root (atomic write, index/manifest, idempotent identical rerun; a proposal
     from a DIFFERENT source HOC assessment hash for the same date SUPERSEDES — never
     silently reuses — the stale proposal, keeping every artifact immutable on disk). It
     NEVER writes an operational ledger, PostgreSQL, an operational target, an alpha
     target, an order, a fill, a holding, cash or NAV.
  4. Exposes ONE read contract for ``GET /v1/operations/reallocation-proposal``
     (read-only) plus a compact summary the Daily Action Gate / workflow state read.

The sole NORMAL execution path is the Daily Research Cycle
(``POST /v1/operations/daily-research-cycle/run``); there is deliberately NO separate
create / apply / confirm / rebalance / order endpoint. This module is preview-first,
paper-only, review-only: no target is confirmed, no order is created, no automation is
enabled.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import reallocation_proposal as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
ALLOCATION_POLICY_VERSION = kernel.ALLOCATION_POLICY_VERSION
COST_POLICY_VERSION = kernel.COST_POLICY_VERSION
ACTION_VOCAB = kernel.ACTION_VOCAB
COMPOSITION_OWNER = "api.reallocation_proposal"
PHASE = "29H-Slice7"

# Read-layer states extend the kernel's proposal states.
STATE_READY = kernel.STATE_READY
STATE_DEGRADED = kernel.STATE_DEGRADED
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NO_ACTIVE_BOOK = kernel.STATE_NO_ACTIVE_BOOK
STATE_NOT_RUN = "NOT_RUN"
STATE_UNAVAILABLE = "UNAVAILABLE"
#: Stage 19.1 — the persisted proposal was produced against a DIFFERENT corporate-action
#: registry state than the current portfolio. Review-only, never approvable, never
#: executable; the Daily Research Cycle must produce a fresh proposal.
STATE_STALE = "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK,
                    STATE_NOT_RUN, STATE_UNAVAILABLE, STATE_STALE)

# --- immutable artifact root (configurable; a research / decision-evidence root, ----- #
# NEVER the operational ledger root). ---------------------------------------------- #
REALLOC_DIR_ENV = "PAPER_TRADER_REALLOC_DIR"
_DEFAULT_REALLOC_DIR = Path(r"D:\Stock_Prediction_app_data\reallocation_proposals")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"


# --------------------------------------------------------------------------- #
# Time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _realloc_dir(reallocation_dir=None) -> Path:
    if reallocation_dir is not None:
        return Path(reallocation_dir)
    env = os.environ.get(REALLOC_DIR_ENV)
    return Path(env) if env else _DEFAULT_REALLOC_DIR


def _artifacts_dir(reallocation_dir=None) -> Path:
    return _realloc_dir(reallocation_dir) / _ARTIFACTS_SUBDIR


def _index_path(reallocation_dir=None) -> Path:
    return _realloc_dir(reallocation_dir) / _INDEX_FILE


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic write: a temp file in the same dir then ``os.replace`` (interrupted
    writes leave the prior file intact and only an orphan .tmp, never a partial)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _f(x: Any) -> Optional[float]:
    return kernel._f(x)


# --------------------------------------------------------------------------- #
# Stage 19.1 — corporate-action identity + staleness (delegated to the ONE owner)
# --------------------------------------------------------------------------- #
def _corporate_actions_hash(ps: Optional[dict]) -> Optional[str]:
    """The registry fingerprint the supplied portfolio state was projected through. Read
    from the portfolio-state contract (its canonical owner); no registry access here."""
    return ((ps or {}).get("corporate_actions") or {}).get("registry_fingerprint")


def _bound_corporate_actions_hash(artifact: Optional[dict]) -> Optional[str]:
    """The registry fingerprint an already-persisted artifact was bound to. Artifacts
    written before this contract existed carry none — treated as the EMPTY registry."""
    art = artifact or {}
    ident = art.get("identity") or {}
    ic = art.get("input_contract") or {}
    return ident.get("corporate_actions_hash") or ic.get("corporate_actions_hash")


def corporate_action_staleness(*, artifact: Optional[dict],
                               portfolio_state: Optional[dict] = None,
                               active_book_id: Optional[str] = None) -> dict:
    """Is a persisted proposal still valid against the CURRENT corporate-action registry?
    Pure delegation to ``api.corporate_actions.staleness_vs_registry``; this module
    recreates no split math and no registry logic."""
    from paper_trader.api import corporate_actions as ca
    bound = _bound_corporate_actions_hash(artifact)
    current_hash = _corporate_actions_hash(portfolio_state)
    if current_hash is not None:
        cur = {"fingerprint": current_hash,
               "n_registered": len(((portfolio_state or {}).get("corporate_actions")
                                    or {}).get("actions") or []),
               "actions": ((portfolio_state or {}).get("corporate_actions")
                           or {}).get("actions") or []}
        return ca.staleness_vs_registry(bound, current=cur)
    return ca.staleness_vs_registry(bound, book_id=active_book_id)


# --------------------------------------------------------------------------- #
# Policy (reuse canonical construction + cost constants — never forked)
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    ov: dict[str, Any] = {}
    try:
        import math as _m
        from paper_trader.api import multi_horizon_engine as eng
        n = eng.BOOK_SIZES[0]
        ov.update({
            "target_position_count": n,
            "entry_rank": n,
            "exit_buffer_rank": _m.ceil(n * (1.0 + eng.EXIT_BUFFER_FRACTION)),
            "sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
            "max_name_weight": eng.MAX_INDIVIDUAL_WEIGHT,
            "min_adv_dollar": eng.MIN_ADV_DOLLAR,
            "candidate_rank_max": 2 * n,
        })
    except Exception:  # noqa: BLE001 - degrade to kernel defaults
        pass
    try:
        from paper_trader.api import paper_trading_desk as desk
        ov.update({
            "cost_bps_per_side": desk.COST_BPS_PER_SIDE,
            "cost_rate_per_side": desk.COST_RATE_PER_SIDE,
            "round_trip_cost_bps": 2.0 * desk.COST_BPS_PER_SIDE,
        })
    except Exception:  # noqa: BLE001
        pass
    return ov


def resolve_policy(policy_overrides: Optional[dict] = None) -> dict:
    pol = dict(kernel.default_policy())
    pol.update(_live_policy_overrides())
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


# --------------------------------------------------------------------------- #
# Input-contract sourcing (every source is injectable for tests)
# --------------------------------------------------------------------------- #
def _positions_from_state(ps: dict) -> list[dict]:
    out = []
    for p in (ps.get("positions") or []):
        out.append({
            "ticker": p.get("ticker"),
            "sector": p.get("sector") or "Unknown",
            "quantity": p.get("quantity"),
            "current_weight": _f(p.get("portfolio_weight")),
            "market_value": _f(p.get("market_value")),
            "price": _f(p.get("price")),
        })
    return out


def _hoc_reviews_from_assessment(assessment: dict) -> list[dict]:
    out = []
    for r in (assessment.get("holding_reviews") or []):
        out.append({
            "ticker": r.get("ticker"),
            "recommendation": r.get("recommendation"),
            "current_rank": r.get("current_rank"),
            "current_score": r.get("current_score"),
            "signal_strength": r.get("signal_strength"),
            "strongest_replacement_ticker": r.get("strongest_replacement_ticker"),
            "replacement_rank": r.get("replacement_rank"),
            "replacement_score": r.get("replacement_score"),
            "gross_score_improvement": r.get("gross_score_improvement"),
            "net_improvement": r.get("net_improvement"),
            "switching_cost_usd": r.get("switching_cost_usd"),
            "deterioration_state": r.get("deterioration_state"),
            "drawdown_60d": r.get("drawdown_60d"),
            "volatility_60d": r.get("volatility_60d"),
            "liquidity_state": r.get("liquidity_state"),
            "risk_contribution_pct": r.get("risk_contribution_pct"),
        })
    return out


def _candidate_tickers(*, scoring: dict, held: set, policy: dict) -> list[str]:
    """Eligible non-held names within the candidate-rank bound (for return sourcing)."""
    out: list[str] = []
    for r in sorted((r for r in (scoring.get("rankings") or [])
                     if r.get("ticker") and r.get("rank") is not None),
                    key=lambda r: (r.get("rank"), r.get("ticker"))):
        tk = r.get("ticker")
        if tk in held or not r.get("eligible", True):
            continue
        if r.get("rank") is not None and r.get("rank") > policy["candidate_rank_max"]:
            continue
        adv = _f(r.get("adv_dollar"))
        if adv is not None and adv < policy["min_adv_dollar"]:
            continue
        out.append(tk)
    return out


def _aligned_returns(*, price_panel: dict, tickers: list, eligible: str,
                     lookback: int) -> dict:
    """Owned point-in-time daily returns for ``tickers`` aligned on a UNION calendar
    (None where a name lacks a date), ending at the eligible session. The Slice-6
    covariance kernel trims each weight-set to its own common non-None index."""
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    per: dict[str, dict] = {}
    all_dates: set = set()
    for tk in tickers:
        s = series.get(tk)
        if not s:
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        if j < 1:
            continue
        dmap = {}
        for i in range(1, j + 1):
            r = s["ret"][i]
            if r is not None:
                dmap[s["dates"][i]] = float(r)
        if dmap:
            per[tk] = dmap
            all_dates |= set(dmap.keys())
    if not per:
        return {"dates": [], "series": {}}
    dates = sorted(all_dates)[-lookback:]
    out = {tk: [per[tk].get(d) for d in dates] for tk in per}
    return {"dates": dates, "series": out}


def build_input_contract(*, portfolio_state: dict, scoring: dict, hoc_assessment: dict,
                         price_panel: Optional[dict] = None,
                         policy: Optional[dict] = None) -> dict:
    """Assemble the immutable point-in-time reallocation-input contract.

    Everything is sourced as of the portfolio-state eligible market date. No expected
    return is ever synthesised. No future rows; no fabricated volume.
    """
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    sc = scoring or {}
    hoc = hoc_assessment or {}
    eligible = ((ps.get("dates") or {}).get("eligible_market_date"))
    active_book = ps.get("active_book") or {}
    active_book_id = active_book.get("book_id")

    positions = _positions_from_state(ps)
    held = {p["ticker"] for p in positions if p.get("ticker")}
    universe_rows = list(sc.get("rankings") or [])
    hoc_reviews = _hoc_reviews_from_assessment(hoc)
    hoc_state = hoc.get("assessment_state")
    hoc_available = bool(hoc) and hoc_state in ("READY", "DEGRADED")
    hoc_gaps = ((hoc.get("data_quality") or {}).get("data_gaps")) or []

    aligned = {"dates": [], "series": {}}
    if price_panel:
        cand = _candidate_tickers(scoring=sc, held=held, policy=pol)
        union = sorted(held | set(cand))
        aligned = _aligned_returns(price_panel=price_panel, tickers=union,
                                   eligible=eligible, lookback=pol["covariance_lookback"])

    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": active_book_id,
        "active_book_label": active_book.get("book_label"),
        "valuation_date": (ps.get("dates") or {}).get("valuation_date"),
        "nav": (ps.get("capital") or {}).get("nav"),
        "cash": (ps.get("capital") or {}).get("cash"),
        "portfolio_state_hash": ps.get("state_hash"),
        # Stage 19.1: the corporate-action registry the CURRENT holdings/NAV in this
        # contract were projected through. Bound into the proposal identity so a later
        # registration provably invalidates this proposal.
        "corporate_actions_hash": _corporate_actions_hash(ps),
        "universe_scoring_hash": sc.get("output_hash"),
        "universe_input_contract_hash": sc.get("input_contract_hash"),
        "hoc_assessment_hash": hoc.get("assessment_hash"),
        "hoc_assessment_state": hoc_state,
        "hoc_available": hoc_available,
        "hoc_data_gaps": list(hoc_gaps),
        "hoc_recommendation_counts": hoc.get("recommendation_counts") or {},
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
        "positions": positions,
        "hoc_reviews": hoc_reviews,
        "universe_rows": universe_rows,
        "aligned_returns": aligned,
    }


# --------------------------------------------------------------------------- #
# Default source loaders (injectable seams)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader() -> dict:
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_price_panel_loader() -> Optional[dict]:
    from paper_trader.api import price_panel as pp
    return pp.load_price_panel()


def _default_hoc_assessment_loader(*, active_book_id, eligible_market_date,
                                   hoc_dir=None) -> dict:
    """The latest persisted Slice-6 assessment for the active book + eligible date."""
    from paper_trader.api import holding_opportunity_cost as hoc
    art = hoc.load_latest_artifact(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date, hoc_dir=hoc_dir)
    return (art or {}).get("assessment") or {}


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_proposal(*, input_contract: Optional[dict] = None,
                 portfolio_state: Optional[dict] = None,
                 scoring: Optional[dict] = None,
                 hoc_assessment: Optional[dict] = None,
                 price_panel: Optional[dict] = None,
                 policy: Optional[dict] = None,
                 hoc_dir=None,
                 portfolio_state_loader: Optional[Callable] = None,
                 scoring_loader: Optional[Callable] = None,
                 price_panel_loader: Optional[Callable] = None,
                 hoc_assessment_loader: Optional[Callable] = None) -> dict:
    """Build the input contract (unless supplied) and run the pure kernel.

    Returns ``{"input_contract": ..., "proposal": <kernel result>}``. Read-only:
    persists nothing.
    """
    pol = policy or resolve_policy()
    if input_contract is None:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
        sc = scoring if scoring is not None else (
            (scoring_loader or _default_scoring_loader)())
        active_book_id = (ps.get("active_book") or {}).get("book_id")
        eligible = (ps.get("dates") or {}).get("eligible_market_date")
        hoc = hoc_assessment
        if hoc is None:
            loader = hoc_assessment_loader or _default_hoc_assessment_loader
            try:
                hoc = loader(active_book_id=active_book_id,
                             eligible_market_date=eligible, hoc_dir=hoc_dir)
            except Exception:  # noqa: BLE001
                hoc = {}
        pp_obj = price_panel
        if pp_obj is None:
            try:
                pp_obj = (price_panel_loader or _default_price_panel_loader)()
            except Exception:  # noqa: BLE001
                pp_obj = None
        input_contract = build_input_contract(
            portfolio_state=ps, scoring=sc, hoc_assessment=hoc or {},
            price_panel=pp_obj, policy=pol)
    result = kernel.build_proposal(input_contract=input_contract, policy=pol)
    return {"input_contract": input_contract, "proposal": result}


# --------------------------------------------------------------------------- #
# Persist (immutable artifact)
# --------------------------------------------------------------------------- #
def proposal_identity(*, input_contract: dict, result: dict) -> dict:
    return {
        "eligible_market_date": input_contract.get("eligible_market_date"),
        "active_book_id": input_contract.get("active_book_id"),
        "portfolio_state_hash": input_contract.get("portfolio_state_hash"),
        "corporate_actions_hash": input_contract.get("corporate_actions_hash"),
        "universe_scoring_hash": input_contract.get("universe_scoring_hash"),
        "hoc_assessment_hash": input_contract.get("hoc_assessment_hash"),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "proposal_hash": result.get("proposal_hash"),
    }


def proposal_id_for(identity: dict) -> str:
    book = (identity.get("active_book_id") or "book")
    date = (identity.get("eligible_market_date") or "nodate")
    h = (identity.get("proposal_hash") or "")[:12]
    return "reap_%s_%s_%s" % (date, book, h)


def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


def _compact_input_contract(ic: dict) -> dict:
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "active_book_label": ic.get("active_book_label"),
        "valuation_date": ic.get("valuation_date"),
        "nav": ic.get("nav"),
        "cash": ic.get("cash"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "hoc_assessment_hash": ic.get("hoc_assessment_hash"),
        "hoc_assessment_state": ic.get("hoc_assessment_state"),
        "hoc_available": ic.get("hoc_available"),
        "hoc_data_gaps": ic.get("hoc_data_gaps"),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
        "positions_count": len(ic.get("positions") or []),
        "universe_rows_count": len(ic.get("universe_rows") or []),
    }


def persist_proposal(*, result: dict, input_contract: dict, reallocation_dir=None,
                     now: Optional[datetime] = None) -> dict:
    """Persist a completed production proposal as an immutable artifact.

    Idempotency contract:
      * identical inputs (same ``proposal_hash``) reuse the existing artifact;
      * a DIFFERENT source HOC assessment hash (or any changed input producing a
        different ``proposal_hash``) for the same book+date SUPERSEDES — the fresh
        proposal is written to its own immutable artifact and the index pointer is
        advanced; the stale proposal is NEVER silently reused. Every artifact on disk
        is immutable (the artifact_id embeds the proposal hash).

    Only production READY / DEGRADED proposals are persisted; BLOCKED / NO_ACTIVE_BOOK
    are not (nothing durable to record).
    """
    state = result.get("proposal_state")
    if state not in (STATE_READY, STATE_DEGRADED):
        return {"status": "NOT_PERSISTED", "reason": "STATE_%s_NOT_PERSISTABLE" % state,
                "proposal_id": None, "persisted": False, "reused": False,
                "superseded": False}

    identity = proposal_identity(input_contract=input_contract, result=result)
    pid = proposal_id_for(identity)
    key = _index_key(identity["active_book_id"], identity["eligible_market_date"])
    index = _load_json(_index_path(reallocation_dir)) or {}
    existing = index.get(key)

    if existing and existing.get("proposal_hash") == identity["proposal_hash"]:
        return {"status": "REUSED_EXISTING", "proposal_id": existing.get("proposal_id"),
                "path": existing.get("path"), "persisted": True, "reused": True,
                "superseded": False, "identity": identity}

    superseded_id = existing.get("proposal_id") if existing else None
    superseded_hoc_hash = existing.get("hoc_assessment_hash") if existing else None

    payload = {
        "proposal_id": pid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "identity": identity,
        "supersedes_proposal_id": superseded_id,
        "supersedes_hoc_assessment_hash": superseded_hoc_hash,
        "input_contract": _compact_input_contract(input_contract),
        "proposal": result,
    }
    path = _artifacts_dir(reallocation_dir) / ("%s.json" % pid)
    _atomic_write_json(path, payload)
    index[key] = {"proposal_id": pid, "path": str(path),
                  "proposal_hash": identity["proposal_hash"],
                  "hoc_assessment_hash": identity["hoc_assessment_hash"],
                  "portfolio_state_hash": identity["portfolio_state_hash"],
                  "universe_scoring_hash": identity["universe_scoring_hash"],
                  "allocation_policy_version": identity["allocation_policy_version"],
                  "eligible_market_date": identity["eligible_market_date"],
                  "active_book_id": identity["active_book_id"],
                  "proposal_state": state,
                  "generated_at": payload["generated_at"]}
    _atomic_write_json(_index_path(reallocation_dir), index)
    return {"status": ("SUPERSEDED_PRIOR" if superseded_id else "CREATED"),
            "proposal_id": pid, "path": str(path), "persisted": True, "reused": False,
            "superseded": bool(superseded_id), "superseded_proposal_id": superseded_id,
            "identity": identity}


def load_latest_artifact(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str],
                         reallocation_dir=None) -> Optional[dict]:
    """Load the current persisted proposal for an exact (active book, eligible date)."""
    index = _load_json(_index_path(reallocation_dir)) or {}
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None:
        art = _load_json(_artifacts_dir(reallocation_dir)
                         / ("%s.json" % entry.get("proposal_id")))
    return art


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None,
                    hoc_assessment: Optional[dict] = None,
                    price_panel: Optional[dict] = None, policy: Optional[dict] = None,
                    reallocation_dir=None, hoc_dir=None, now: Optional[datetime] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None,
                    price_panel_loader: Optional[Callable] = None,
                    hoc_assessment_loader: Optional[Callable] = None) -> dict:
    """The Daily Research Cycle entry: build -> kernel -> persist (idempotent)."""
    run = run_proposal(
        portfolio_state=portfolio_state, scoring=scoring, hoc_assessment=hoc_assessment,
        price_panel=price_panel, policy=policy, hoc_dir=hoc_dir,
        portfolio_state_loader=portfolio_state_loader, scoring_loader=scoring_loader,
        price_panel_loader=price_panel_loader, hoc_assessment_loader=hoc_assessment_loader)
    persist = persist_proposal(result=run["proposal"], input_contract=run["input_contract"],
                               reallocation_dir=reallocation_dir, now=now)
    return {"input_contract": run["input_contract"], "proposal": run["proposal"],
            "persistence": persist}


# --------------------------------------------------------------------------- #
# Read contract — GET /v1/operations/reallocation-proposal
# --------------------------------------------------------------------------- #
def _active_book_block(ps: dict) -> dict:
    ab = (ps or {}).get("active_book") or {}
    return {
        "book_id": ab.get("book_id"),
        "book_label": ab.get("book_label"),
        "status": ab.get("status"),
        "initialized": ab.get("initialized"),
        "holdings_count": ab.get("holdings_count"),
        "is_dormant_legacy_book": ab.get("is_dormant_legacy_book", False),
        "owner_module": "api.operational_book",
    }


def _read_payload(*, state: str, generated_at: str, eligible: Optional[str],
                  active_book: dict, artifact: Optional[dict], message: str,
                  policy: dict, proposal: Optional[dict],
                  input_contract: Optional[dict],
                  staleness: Optional[dict] = None) -> dict:
    p = proposal or {}
    art_meta = None
    if artifact:
        art_meta = {"proposal_id": artifact.get("proposal_id"),
                    "generated_at": artifact.get("generated_at"),
                    "identity": artifact.get("identity"),
                    "supersedes_proposal_id": artifact.get("supersedes_proposal_id"),
                    "immutable": True,
                    "root_env": REALLOC_DIR_ENV}
    stale = bool((staleness or {}).get("stale"))
    return {
        # Stage 19.1 — the explicit approvability contract every surface renders.
        "stale": stale,
        "staleness": staleness,
        "approvable": (not stale) and state in (STATE_READY, STATE_DEGRADED),
        "executable": (not stale) and state in (STATE_READY, STATE_DEGRADED),
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": state,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "message": message,
        "eligible_market_date": eligible,
        "active_book": active_book,
        "input_contract": input_contract,
        "policy": p.get("policy") or policy,
        "policy_version": p.get("policy_version") or ALLOCATION_POLICY_VERSION,
        "portfolio": p.get("portfolio") or {},
        "action_counts": p.get("action_counts") or {a: 0 for a in ACTION_VOCAB},
        "action_vocabulary": list(ACTION_VOCAB),
        "allocations": p.get("allocations") or [],
        "turnover": p.get("turnover") or {},
        "signal": p.get("signal") or {},
        "risk": p.get("risk") or {},
        "constraints": p.get("constraints") or {},
        "diagnostics": p.get("diagnostics") or {},
        "data_gaps": p.get("data_gaps") or [],
        "hoc_reference": p.get("hoc_reference") or {},
        "artifact": art_meta,
        "safety": p.get("safety") or kernel._safety(),
        "provenance": p.get("provenance")
        or {"composition_owner": COMPOSITION_OWNER, "calculation_owner": kernel.CALCULATION_OWNER},
        "proposal_hash": p.get("proposal_hash"),
        "proposal_state": p.get("proposal_state"),
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
        "review_only": True,
    }


def load_reallocation_proposal(*, portfolio_state: Optional[dict] = None,
                               artifact: Optional[dict] = None, reallocation_dir=None,
                               now: Optional[datetime] = None,
                               portfolio_state_loader: Optional[Callable] = None) -> dict:
    """The read contract for the endpoint. READ-ONLY: it NEVER runs the engine — it
    returns the current persisted proposal for the active book + eligible date. When no
    production artifact exists it returns a readable ``NOT_RUN`` payload (the sole
    execution path is the Daily Research Cycle). Always degrade-safe."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return _read_payload(state=STATE_UNAVAILABLE, generated_at=generated_at, eligible=None,
                             active_book={}, artifact=None,
                             message="Portfolio state is unavailable: %s" % str(exc)[:160],
                             policy=resolve_policy(), proposal=None, input_contract=None)

    active_book = _active_book_block(ps)
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    book_id = active_book.get("book_id")

    if not book_id:
        return _read_payload(state=STATE_NO_ACTIVE_BOOK, generated_at=generated_at,
                             eligible=eligible, active_book=active_book, artifact=None,
                             message="No active operational book; no proposal.",
                             policy=resolve_policy(), proposal=None, input_contract=None)

    art = artifact if artifact is not None else load_latest_artifact(
        active_book_id=book_id, eligible_market_date=eligible,
        reallocation_dir=reallocation_dir)
    if not art:
        return _read_payload(
            state=STATE_NOT_RUN, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=None,
            message=("No reallocation proposal has been produced for the current active "
                     "book and eligible session yet. Run the Daily Research Cycle "
                     "(POST /v1/operations/daily-research-cycle/run) to produce one."),
            policy=resolve_policy(), proposal=None, input_contract=None)

    proposal = art.get("proposal") or {}
    # Stage 19.1 — a proposal produced BEFORE a corporate action was registered describes
    # holdings that no longer exist economically. It stays readable (and its artifact stays
    # immutable) but it is explicitly STALE: not approvable, not executable.
    staleness = corporate_action_staleness(artifact=art, portfolio_state=ps,
                                           active_book_id=book_id)
    if staleness.get("stale"):
        return _read_payload(
            state=STATE_STALE, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=art,
            message=("This reallocation proposal was produced BEFORE a corporate action "
                     "was registered, so it was computed against holdings that no longer "
                     "describe the current portfolio. It cannot be approved or turned into "
                     "an order plan. Run the Daily Research Cycle to produce a fresh "
                     "proposal against the corrected portfolio state."),
            policy=proposal.get("policy") or resolve_policy(), proposal=proposal,
            input_contract=art.get("input_contract"), staleness=staleness)
    return _read_payload(
        state=proposal.get("proposal_state") or STATE_READY, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art,
        message="Current reallocation proposal for the active book / eligible session. "
                "Manual review required — review only, no orders.",
        policy=proposal.get("policy") or resolve_policy(), proposal=proposal,
        input_contract=art.get("input_contract"), staleness=staleness)


# --------------------------------------------------------------------------- #
# Compact summary the Daily Action Gate / workflow state read
# --------------------------------------------------------------------------- #
def load_proposal_summary(*, active_book_id: Optional[str] = None,
                          eligible_market_date: Optional[str] = None,
                          artifact: Optional[dict] = None, reallocation_dir=None) -> dict:
    """A compact, read-only reallocation-proposal summary. PURE ARTIFACT READER — it
    reads ONLY the immutable proposal index/artifact for the exact
    ``(active_book_id, eligible_market_date)`` the caller supplies; it never loads
    portfolio state, never runs the engine, and never calls a provider / prediction.
    ``available=False`` with a NOT_RUN state when no production artifact exists."""
    empty = {
        "reallocation_proposal_available": False,
        "reallocation_proposal_state": STATE_NOT_RUN,
        "reallocation_proposal_hash": None,
        "reallocation_proposal_id": None,
        "reallocation_action_counts": {a: 0 for a in ACTION_VOCAB},
        "reallocation_score_improvement": None,
        "reallocation_score_improvement_net_of_cost": None,
        "reallocation_one_way_turnover": None,
        "reallocation_estimated_transaction_cost": None,
        "reallocation_proposed_holding_count": None,
        "reallocation_data_gaps": [],
        "reallocation_proposal_stale": False,
        "reallocation_proposal_stale_reason": None,
        "reallocation_corporate_actions_hash": None,
    }
    try:
        art = artifact if artifact is not None else load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            reallocation_dir=reallocation_dir)
    except Exception:  # noqa: BLE001 — a pure artifact read must never crash the caller
        d = dict(empty)
        d["reallocation_proposal_state"] = STATE_UNAVAILABLE
        return d
    if not art:
        return empty
    p = art.get("proposal") or {}
    sig = p.get("signal") or {}
    trn = p.get("turnover") or {}
    # Stage 19.1: staleness against the corporate-action registry is resolved by the ONE
    # owner from the registry file alone — this stays a pure artifact reader (no portfolio
    # state load, no engine run, no provider call).
    try:
        stale_blk = corporate_action_staleness(artifact=art, active_book_id=active_book_id)
    except Exception:  # noqa: BLE001 — never crash a compact summary read
        stale_blk = {"stale": False, "reason": None,
                     "current_corporate_actions_hash": None}
    return {
        "reallocation_proposal_available": True,
        "reallocation_proposal_stale": bool(stale_blk.get("stale")),
        "reallocation_proposal_stale_reason": stale_blk.get("reason"),
        "reallocation_corporate_actions_hash":
            stale_blk.get("current_corporate_actions_hash"),
        "reallocation_proposal_state": (STATE_STALE if stale_blk.get("stale")
                                        else p.get("proposal_state")),
        "reallocation_proposal_hash": p.get("proposal_hash"),
        "reallocation_proposal_id": art.get("proposal_id"),
        "reallocation_action_counts": p.get("action_counts") or {a: 0 for a in ACTION_VOCAB},
        "reallocation_score_improvement": sig.get("score_improvement"),
        "reallocation_score_improvement_net_of_cost": sig.get("score_improvement_net_of_cost"),
        "reallocation_one_way_turnover": trn.get("one_way_turnover"),
        "reallocation_estimated_transaction_cost": trn.get("estimated_transaction_cost"),
        "reallocation_proposed_holding_count": (p.get("portfolio") or {}).get(
            "proposed_holding_count"),
        "reallocation_data_gaps": [g.get("code") for g in (p.get("data_gaps") or [])
                                   if not g.get("by_design")],
    }
