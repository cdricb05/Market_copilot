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

from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import reallocation_proposal as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
OUTCOME_VOCAB = _cr.OUTCOME_VOCAB
OUTCOME_PROPOSAL_READY = _cr.OUTCOME_PROPOSAL_READY
OUTCOME_HOLD_CURRENT_BOOK = _cr.OUTCOME_HOLD_CURRENT_BOOK
OUTCOME_TRUE_BLOCKER = _cr.OUTCOME_TRUE_BLOCKER
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
#: Release 29.3 — a COMPLETE target exists and is reviewable, but a portfolio-level
#: limit that only the complete target can settle (turnover / concentration / sector /
#: risk) is breached. Never approvable, never executable.
STATE_WITHHELD = kernel.STATE_WITHHELD
STATE_NOT_RUN = "NOT_RUN"
STATE_UNAVAILABLE = "UNAVAILABLE"
#: Stage 19.1 — the persisted proposal was produced against a DIFFERENT corporate-action
#: registry state than the current portfolio. Review-only, never approvable, never
#: executable; the Daily Research Cycle must produce a fresh proposal.
STATE_STALE = "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"
#: R54.2.3.2 — a NEWER authoritative governed decision stands and does not request or
#: endorse this proposal (the live 2026-09-02 pattern: a 23:38Z event-cycle proposal
#: outlived the 23:51Z governed CURRENT_NO_CHANGE conclusion whose manifest recorded
#: the proposal step NOT_REQUIRED). History-visible, immutable — and never current,
#: never reviewable as outstanding work, never approvable. The verdict is computed by
#: the ONE calculation in api.portfolio_decision.assess_proposal_supersession; this
#: module renders it and decides nothing.
STATE_SUPERSEDED = "SUPERSEDED_BY_NEWER_DECISION"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_WITHHELD,
                    STATE_NO_ACTIVE_BOOK, STATE_NOT_RUN, STATE_UNAVAILABLE, STATE_STALE,
                    STATE_SUPERSEDED)
#: The ONLY read-layer states in which a proposal may be reviewed / approved.
APPROVABLE_READ_STATES = (STATE_READY, STATE_DEGRADED)

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
                               active_book_id: Optional[str] = None,
                               actions_dir=None) -> dict:
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
    # Stage 22: explicit seam so a HERMETIC caller resolves the registry from ITS OWN
    # root instead of reaching the operator's real one from a synthetic scenario.
    return ca.staleness_vs_registry(bound, book_id=active_book_id,
                                    actions_dir=actions_dir)


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


def _cr_policy_view() -> dict:
    """The constraint policy to DISPLAY when no proposal exists yet.

    The same projection the proposal engine uses, so the inventory an operator
    reads before a proposal exists carries exactly the limits the next proposal
    will be built with. It computes nothing and decides nothing."""
    try:
        return kernel.constraint_policy_projection(resolve_policy())
    except Exception:  # noqa: BLE001 - a read must never crash
        return {}


# --------------------------------------------------------------------------- #
# Input-contract sourcing (every source is injectable for tests)
# --------------------------------------------------------------------------- #
def _positions_from_state(ps: dict) -> list[dict]:
    out = []
    for p in (ps.get("positions") or []):
        row = {
            "ticker": p.get("ticker"),
            "sector": p.get("sector") or "Unknown",
            "quantity": p.get("quantity"),
            # Release 50 - the EXPOSURE weight (notional / NAV); identical to the
            # market-value weight for a cash equity.
            "current_weight": (_f(p.get("exposure_weight"))
                               if p.get("exposure_weight") is not None
                               else _f(p.get("portfolio_weight"))),
            "market_value": _f(p.get("market_value")),
            "price": _f(p.get("price")),
        }
        # Release 50 - the instrument contract travels with the position.
        for k in ("instrument_type", "asset_class", "sleeve_id", "currency", "multiplier",
                  "unit_type", "notional_usd", "collateral_usd", "execution_convention"):
            if p.get(k) is not None:
                row[k] = p.get(k)
        out.append(row)
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
    """Owned point-in-time daily returns, from the canonical price owner.

    Release 30 moved the body to ``api.price_panel.aligned_returns`` so the
    zero-base allocator and this proposal read the SAME series. This wrapper is
    kept because the call shape here is the proposal's, not the panel's.
    """
    from paper_trader.api import price_panel as pp
    return pp.aligned_returns(price_panel=price_panel, tickers=tickers,
                              as_of=eligible, lookback=lookback)


def _bound_hoc_assessment_hash(hoc_binding, hoc_assessment):
    """Release 61 - the dependency hash, spelled by the opportunity-cost owner.

    This proposal is the THIRD consumer of that identity (after the event cycle
    and the reassessment). On a REUSE the caller's re-derived document was never
    written, so publishing its hash here would bind the proposal to an
    assessment the store does not hold - and the governed gate's
    TARGET_BOUND_TO_SAME_HOC check would then refuse a proposal whose evidence
    is perfectly retrievable. One owner, one spelling, three consumers.
    """
    try:
        from paper_trader.api import holding_opportunity_cost as hocm
        return hocm.bound_assessment_hash(binding=hoc_binding,
                                          assessment=hoc_assessment)
    except Exception:  # noqa: BLE001 - a contract build never crashes on a read
        return ((hoc_binding or {}).get("hoc_assessment_hash")
                or (hoc_assessment or {}).get("assessment_hash"))


def build_input_contract(*, portfolio_state: dict, scoring: dict, hoc_assessment: dict,
                         price_panel: Optional[dict] = None,
                         policy: Optional[dict] = None,
                         frontier: Optional[dict] = None,
                         hoc_binding: Optional[dict] = None) -> dict:
    """Assemble the immutable point-in-time reallocation-input contract.

    Everything is sourced as of the portfolio-state eligible market date. No expected
    return is ever synthesised. No future rows; no fabricated volume.

    Release 50 - ``frontier`` is the ONE cross-asset opportunity frontier
    (``api.opportunity_frontier``). Its ELIGIBLE non-equity rows join the universe
    rows in the SAME shape (ticker / sector / rank / percentile / adv_dollar) plus
    their instrument contract, and its non-equity holding reviews join the HOC
    reviews. An absent or empty frontier is exactly the pre-R50 equity contract.
    """
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    sc = scoring or {}
    hoc = hoc_assessment or {}
    fr = frontier or {}
    eligible = ((ps.get("dates") or {}).get("eligible_market_date"))
    active_book = ps.get("active_book") or {}
    active_book_id = active_book.get("book_id")

    positions = _positions_from_state(ps)
    held = {p["ticker"] for p in positions if p.get("ticker")}
    universe_rows = list(sc.get("rankings") or [])
    universe_tickers = {r.get("ticker") for r in universe_rows}
    frontier_rows = [r for r in (fr.get("candidate_rows_for_proposal") or [])
                     if r.get("ticker") and r.get("ticker") not in universe_tickers]
    universe_rows = universe_rows + frontier_rows
    hoc_reviews = _hoc_reviews_from_assessment(hoc)
    reviewed = {r.get("ticker") for r in hoc_reviews}
    frontier_reviews = [r for r in (fr.get("non_equity_reviews") or [])
                        if r.get("ticker") and r.get("ticker") not in reviewed]
    hoc_reviews = hoc_reviews + frontier_reviews
    hoc_state = hoc.get("assessment_state")
    hoc_available = bool(hoc) and hoc_state in ("READY", "DEGRADED")
    hoc_gaps = ((hoc.get("data_quality") or {}).get("data_gaps")) or []

    aligned = {"dates": [], "series": {}}
    non_equity = sorted({r["ticker"] for r in frontier_rows}
                        | {p["ticker"] for p in positions
                           if p.get("instrument_type") not in (None, "CASH_EQUITY")})
    if price_panel or non_equity:
        cand = _candidate_tickers(scoring=sc, held=held, policy=pol)
        union = sorted(held | set(cand) | set(non_equity))
        if non_equity:
            # ONE aligned panel over equities AND owned non-equity settlements.
            from paper_trader.api import cross_asset_risk as _car
            pos_rows = [{"instrument_id": t, "instrument_type": ("CASH_EQUITY" if t not in non_equity
                                                                   else "FUTURE")}
                        for t in union]
            aligned = _car.build_aligned_returns(
                positions=pos_rows, price_panel=price_panel, as_of=eligible,
                lookback=pol["covariance_lookback"])
        else:
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
        # Release 50 - the frontier identity this proposal was built from.
        "frontier_hash": fr.get("frontier_hash"),
        "frontier_eligible_non_equity_count": fr.get("eligible_non_equity_count") or 0,
        "frontier_rows_admitted": [r["ticker"] for r in frontier_rows],
        "registry_capital_eligible_sleeve_ids": list(
            (fr.get("registry_identity") or {}).get("capital_eligible_sleeve_ids") or []),
        # MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1 - WHY the count is what it is,
        # carried into the proposal so a zero is explained where it is read.
        "frontier_non_equity_admission": [
            {k: l.get(k) for k in ("sleeve_id", "asset_class", "capital_eligible", "blocker",
                                   "gate_state", "gate_remaining", "instruments_listed",
                                   "instruments_admitted")}
            for l in (fr.get("non_equity_admission_ledger") or [])],
        "frontier_non_equity_count_explanation": fr.get("eligible_non_equity_count_explanation"),
        # Stage 19.1: the corporate-action registry the CURRENT holdings/NAV in this
        # contract were projected through. Bound into the proposal identity so a later
        # registration provably invalidates this proposal.
        "corporate_actions_hash": _corporate_actions_hash(ps),
        "universe_scoring_hash": sc.get("output_hash"),
        "universe_input_contract_hash": sc.get("input_contract_hash"),
        "hoc_assessment_hash": _bound_hoc_assessment_hash(hoc_binding, hoc),
        "hoc_recomputed_assessment_hash": (hoc_binding or {}).get(
            "hoc_recomputed_assessment_hash"),
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
    # Stage 22.1 — the OPERATIONAL panel, the same composition the Slice-6 assessment
    # reads, so the proposal's covariance lookback covers every real holding rather than
    # only the names the frozen research artifact happened to include.
    from paper_trader.api import price_panel as pp
    return pp.load_operational_price_panel()


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
def _ic_labels() -> dict:
    """Asset-class display labels from the ONE instrument-contract owner (so a
    surface never invents words for an asset class)."""
    from paper_trader.engine import instrument_contract as _ic
    return dict(_ic.ASSET_CLASS_LABELS)


def _default_frontier_loader(*, portfolio_state, scoring) -> dict:
    """Release 50 - the ONE cross-asset opportunity frontier, composed from the SAME
    portfolio state and scoring this proposal uses (no second read)."""
    from paper_trader.api import opportunity_frontier as of
    return of.load_opportunity_frontier(portfolio_state=portfolio_state, scoring=scoring)


def run_proposal(*, input_contract: Optional[dict] = None,
                 portfolio_state: Optional[dict] = None,
                 scoring: Optional[dict] = None,
                 hoc_assessment: Optional[dict] = None,
                 hoc_binding: Optional[dict] = None,
                 price_panel: Optional[dict] = None,
                 policy: Optional[dict] = None,
                 hoc_dir=None,
                 frontier: Optional[dict] = None,
                 portfolio_state_loader: Optional[Callable] = None,
                 scoring_loader: Optional[Callable] = None,
                 price_panel_loader: Optional[Callable] = None,
                 hoc_assessment_loader: Optional[Callable] = None,
                 frontier_loader: Optional[Callable] = None) -> dict:
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
        fr = frontier
        if fr is None:
            try:
                fr = (frontier_loader or _default_frontier_loader)(portfolio_state=ps, scoring=sc)
            except Exception:  # noqa: BLE001 - an unreadable frontier is the equity contract
                fr = None
        input_contract = build_input_contract(
            portfolio_state=ps, scoring=sc, hoc_assessment=hoc or {},
            price_panel=pp_obj, policy=pol, frontier=fr, hoc_binding=hoc_binding)
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
        # Release 50 - the frontier the target was built from.
        "frontier_hash": input_contract.get("frontier_hash"),
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
        "frontier_hash": ic.get("frontier_hash"),
        "frontier_eligible_non_equity_count": ic.get("frontier_eligible_non_equity_count"),
        "frontier_rows_admitted": list(ic.get("frontier_rows_admitted") or []),
        "registry_capital_eligible_sleeve_ids": list(
            ic.get("registry_capital_eligible_sleeve_ids") or []),
        "frontier_non_equity_admission": list(ic.get("frontier_non_equity_admission") or []),
        "frontier_non_equity_count_explanation": ic.get("frontier_non_equity_count_explanation"),
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


# --------------------------------------------------------------------------- #
# R83 — THE TERMINAL WITHHELD OUTCOME, RESOLVED FROM THE GOVERNED RUN MANIFEST.
#
# WHY THIS EXISTS.
#
# This module declares STATE_WITHHELD in READ_STATE_VOCAB. api.workflow_state maps
# it to REALLOCATION_PROPOSAL_WITHHELD / "WITHHELD — PORTFOLIO LIMITS"; the canonical
# decision lane maps it to CHANGE_CANDIDATE_WITHHELD / "NO PROPOSAL — PORTFOLIO CHANGE
# WITHHELD"; invariant I5 (WITHHELD_PROPOSAL_EXPOSED_AS_APPROVABLE) polices it. The
# whole downstream vocabulary was in place and STATE_WITHHELD WAS STRUCTURALLY
# UNREACHABLE ON EVERY READ PATH, because both read entrypoints resolved their state
# from a PERSISTED artifact and ``persist_proposal`` refuses by contract to persist
# any state but READY / DEGRADED. A declared state that no store can ever produce is
# not a state; it is a comment.
#
# On 2026-09-28 that cost a whole governed session. The Portfolio Cycle ran, the
# economic gate CLEARED (reassessment PROPOSAL_READY), this owner built a complete
# target over 20 holdings, re-optimised it under the breached limit (R47) and STILL
# measured AMD at 0.1503 and SNDK at 0.1545 against the governed per-name
# risk-contribution cap 0.15 — so it withheld, fail-closed, correctly, and persisted
# nothing. Every read surface then reported NOT_RUN: Today said "PORTFOLIO PROPOSAL
# REQUESTED — NOT YET PRODUCED", the reallocation card said "the first proposal will
# be generated by the next completed Daily Research Cycle" with outstanding action
# RUN_DAILY_RESEARCH_CYCLE, and the decision review said "no proposal to review (read
# state NOT_RUN) — REFRESH THE REVIEW". Four surfaces, four futile instructions, and
# the real answer — a named, measured, settled portfolio-limit breach — was nowhere.
#
# The verdict was never lost. The governed run manifest records it in named fields.
# This function reads it back through the ONE canonical manifest reader
# (api.daily_research_cycle.load_governed_manifest_reference) and hands it to the read
# payload VERBATIM. It computes no weight, no limit, no excess and no economics, it
# never persists anything, and it never makes a withheld target approvable.
#
# It is NOT a fallback for a missing proposal. It fires on exactly one condition: the
# governed cycle for THIS book and THIS session reached a terminal state and recorded
# a COMPLETE reallocation outcome whose state is WITHHELD. Anything else — no
# manifest, a non-terminal run, another book, another session, a non-withheld state —
# leaves the read exactly as it was, at NOT_RUN.
# --------------------------------------------------------------------------- #
def load_governed_withheld_outcome(*, active_book_id: Optional[str],
                                   eligible_market_date: Optional[str],
                                   drc_dir=None) -> Optional[dict]:
    """The governed cycle's TERMINAL WITHHELD reallocation verdict, or ``None``.

    Read-only, degrade-safe and bounded (one index entry + one run manifest, through
    the canonical DRC reference reader). Returns ``None`` unless the governed run for
    this exact book + eligible session is terminal AND recorded a complete WITHHELD
    reallocation outcome. Derives nothing: every field is the manifest's own.
    """
    if not active_book_id or not eligible_market_date:
        return None
    try:
        from paper_trader.api import daily_research_cycle as _drc
        ref = _drc.load_governed_manifest_reference(
            eligible_market_date=eligible_market_date, drc_dir=drc_dir)
    except Exception:  # noqa: BLE001 — a read must never crash its caller
        return None
    if not ref or not ref.get("governed"):
        return None
    # The reference must describe THIS book. A manifest that predates the field
    # cannot prove it, and an unprovable binding is refused rather than assumed.
    if str(ref.get("active_book_id") or "") != str(active_book_id):
        return None
    if str(ref.get("reallocation_proposal_state") or "") != STATE_WITHHELD:
        return None
    # A withheld verdict is only terminal if the owner COMPLETED its governed
    # outcome. An incomplete one is not a ruling and is never published as one.
    if ref.get("reallocation_governed_outcome_complete") is not True:
        return None
    return {
        "state": STATE_WITHHELD,
        "withheld": True,
        "approvable": False,
        "executable": False,
        "governed_outcome_complete": True,
        "governed_run_id": ref.get("run_id"),
        "governed_run_state": ref.get("state"),
        "eligible_market_date": ref.get("eligible_market_date"),
        "active_book_id": ref.get("active_book_id"),
        "completed_at": ref.get("completed_at"),
        # A withheld target computes a hash (the target it judged and rejected) and
        # NEVER a proposal_id, because no artifact exists. Publishing the hash without
        # the id is what keeps an acceptance check from demanding a read-back of an
        # artifact the contract forbids. See the R74 withheld-terminal contract.
        "proposal_hash": ref.get("reallocation_proposal_hash"),
        "proposal_id": None,
        "persisted": False,
        "withheld_codes": list(ref.get("reallocation_withheld_codes") or []),
        "withheld_reasons": list(ref.get("reallocation_withheld_reasons") or []),
        "withheld_reason_detail": ref.get("reallocation_withheld_reason_detail"),
        "withheld_breaching_tickers": list(
            ref.get("reallocation_withheld_breaching_tickers") or []),
        "withheld_risk_contribution_breaches": list(
            ref.get("reallocation_withheld_risk_contribution_breaches") or []),
        "outstanding_governance_requirement": ref.get(
            "reallocation_outstanding_governance_requirement"),
        "proposed_holding_count": ref.get("reallocation_proposed_holding_count"),
        "action_counts": dict(ref.get("reallocation_action_counts") or {}),
        "one_way_turnover": ref.get("reallocation_one_way_turnover"),
        "estimated_transaction_cost": ref.get(
            "reallocation_estimated_transaction_cost"),
        "score_improvement": ref.get("reallocation_score_improvement"),
        "score_improvement_net_of_cost": ref.get(
            "reallocation_score_improvement_net_of_cost"),
        "verdict_owner": COMPOSITION_OWNER,
        "verdict_source": "api.daily_research_cycle governed run manifest",
        "running_the_cycle_again_would_change_nothing": True,
        # THE NEXT GOVERNED ACT, named by the backend. Every surface reads this one
        # field. The two actions a withheld verdict must never name are the two the
        # surfaces used to name: RUN_DAILY_RESEARCH_CYCLE and "refresh the review" —
        # the verdict is deterministic and settled, so both are loops.
        "next_required_action": "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT",
        "next_required_action_label": "REVIEW THE PORTFOLIO LIMIT THAT WITHHELD THE CHANGE",
        "next_required_action_owner": COMPOSITION_OWNER,
        "approval_withheld_because": "COMPLETE_TARGET_WITHHELD_ON_PORTFOLIO_LIMITS",
        "terminal": True,
        "selectable_targets": 0,
    }


def _withheld_read_message(outcome: dict) -> str:
    """The one operator sentence for a terminal withheld reallocation. Composed from
    the verdict's own fields; it states no fact the owner did not measure."""
    o = outcome or {}
    codes = ", ".join(o.get("withheld_codes") or []) or "a governed portfolio limit"
    names = ", ".join(o.get("withheld_breaching_tickers") or [])
    return (
        "The governed cycle for this session BUILT a complete alternative portfolio "
        "over %s holdings, re-optimised it under the breached limit, and WITHHELD it: "
        "%s%s. This is a completed governed decision, not a missing proposal — no "
        "proposal artifact is persisted, nothing is approvable, and running the cycle "
        "again would reach the same verdict. What is outstanding is the portfolio "
        "limit itself, which requires manual review. Nothing is fabricated."
        % (o.get("proposed_holding_count") if o.get("proposed_holding_count") is not None
           else "the eligible", codes,
           (" (%s)" % names) if names else ""))


def _should_consult_governed_manifest(*, reallocation_dir, drc_dir) -> bool:
    """Whether this read may consult the governed DRC manifest.

    The SAME rule the supersession verdict already uses, and for the same reason: a
    hermetic caller that redirected ONLY the proposal store must keep its constructed
    world untouched and must never reach the live research store. The manifest is
    consulted on the production-default read, or when the caller explicitly supplies
    the DRC root.
    """
    return bool(reallocation_dir is None or drc_dir is not None)


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None,
                    hoc_assessment: Optional[dict] = None,
                    hoc_binding: Optional[dict] = None,
                    price_panel: Optional[dict] = None, policy: Optional[dict] = None,
                    reallocation_dir=None, hoc_dir=None, now: Optional[datetime] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None,
                    price_panel_loader: Optional[Callable] = None,
                    hoc_assessment_loader: Optional[Callable] = None) -> dict:
    """The Daily Research Cycle entry: build -> kernel -> persist (idempotent)."""
    run = run_proposal(
        portfolio_state=portfolio_state, scoring=scoring, hoc_assessment=hoc_assessment,
        hoc_binding=hoc_binding,
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
                  staleness: Optional[dict] = None,
                  supersession: Optional[dict] = None,
                  withheld_outcome: Optional[dict] = None) -> dict:
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
    superseded = bool((supersession or {}).get("superseded"))
    # --- R63 live integration: the mandatory-repair contract, RENDERED --------- #
    # The kernel decided this at construction time; this seam only carries the
    # verdict. Before it did, the read contract dropped ``mandatory_repair`` and
    # ``full_target_reviewable`` entirely and defaulted ``approvable`` to True,
    # so a target that left an owner-ruled obligation open was republished as
    # approvable AND executable while the review screen beside it refused the
    # very same target. One proposal cannot have two answers.
    repair = kernel.mandatory_repair_read_verdict(p)
    return {
        # Stage 19.1 — the explicit approvability contract every surface renders.
        "stale": stale,
        "staleness": staleness,
        # R54.2.3.2 — the decision-supersession verdict, rendered verbatim from the
        # ONE calculation in api.portfolio_decision. A superseded proposal is
        # history: state SUPERSEDED_BY_NEWER_DECISION keeps it out of
        # APPROVABLE_READ_STATES, so approvable/executable below go False.
        "superseded": superseded,
        "supersession": supersession,
        "superseded_by": (supersession or {}).get("superseded_by"),
        # Release 47 narrows both: a proposal is offered for approval only when the
        # kernel's own outcome is PROPOSAL_READY. HOLD_CURRENT_BOOK is a decision the
        # system has already taken, not outstanding operator work.
        # R63 adds the obligation term, and it fails CLOSED: an artifact that
        # carries no verdict is refused rather than assumed clean.
        "approvable": ((not stale) and (not superseded)
                       and state in APPROVABLE_READ_STATES
                       and bool(p.get("approvable", True))
                       and repair["reviewable"]),
        "executable": ((not stale) and (not superseded)
                       and state in APPROVABLE_READ_STATES
                       and bool(p.get("approvable", True))
                       and repair["reviewable"]),
        # The contract itself, so no surface has to infer it from a boolean.
        "mandatory_repair": p.get("mandatory_repair") or {},
        "mandatory_repair_verdict": repair,
        "full_target_reviewable": repair["reviewable"],
        # Release 29.3 — the complete-target limit verdict, rendered verbatim.
        "complete_target_limits": p.get("complete_target_limits") or {},
        "withheld": state == STATE_WITHHELD,
        # ``withheld_reasons`` is a list of CODE STRINGS — api.portfolio_decision joins
        # it into prose, api.workflow_state and api.daily_action_gate forward it.
        # Emitting anything else breaks every consumer silently (R74). A terminal
        # verdict resolved from the governed manifest carries the same shape.
        "withheld_reasons": (p.get("withheld_reasons")
                             or (withheld_outcome or {}).get("withheld_reasons") or []),
        # R83 — the TERMINAL governed withheld verdict, published as ONE named block so
        # no surface has to re-derive "was a proposal actually produced?" from the
        # absence of an artifact. Present only when the governed cycle ruled; ``None``
        # is "no information", never "not withheld".
        "governed_withheld_outcome": withheld_outcome or None,
        # --- Release 47 — constraint-respecting active reallocation ----------- #
        "outcome": p.get("outcome"),
        "outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "reallocation_outcome": p.get("reallocation_outcome") or {},
        # The constraint inventory is a property of the SYSTEM, not of a proposal:
        # an operator is entitled to see which limits exist and what each one does
        # even before a proposal has been produced. The proposal's own copy wins
        # when it exists (it carries the live policy the proposal was built with).
        "constraint_inventory": (p.get("constraint_inventory")
                                 or _cr.constraint_inventory(_cr_policy_view())),
        "constraint_reoptimization": p.get("constraint_reoptimization") or {},
        "switching_economics": p.get("switching_economics") or {},
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
                               reassessment_dir=None, drc_dir=None, decision_dir=None,
                               supersession: Optional[dict] = None,
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
        # R83 — NO ARTIFACT IS TWO DIFFERENT FACTS, and until R83 they shared one
        # sentence. Either the governed cycle has not produced a proposal for this
        # session, or it produced a COMPLETE governed verdict and WITHHELD it — which
        # this owner's persistence contract deliberately never writes. Ask the governed
        # manifest which one happened before answering.
        wh = None
        if _should_consult_governed_manifest(reallocation_dir=reallocation_dir,
                                             drc_dir=drc_dir):
            wh = load_governed_withheld_outcome(
                active_book_id=book_id, eligible_market_date=eligible, drc_dir=drc_dir)
        if wh:
            return _read_payload(
                state=STATE_WITHHELD, generated_at=generated_at, eligible=eligible,
                active_book=active_book, artifact=None,
                message=_withheld_read_message(wh),
                policy=resolve_policy(), proposal=None, input_contract=None,
                withheld_outcome=wh)
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
    # R54.2.3.2 — a NEWER authoritative governed decision supersedes a standing
    # proposal. Resolved by the ONE calculation in the canonical decision owner
    # (lazy import: that owner imports this module at module scope). The default
    # resolution runs on the PRODUCTION-DEFAULT read (the live route) or when the
    # caller explicitly supplies the sibling store roots; a hermetic caller that
    # redirected ONLY the proposal store keeps its constructed world untouched
    # (it injects ``supersession`` or the sibling dirs to exercise the verdict).
    # Degrade-safe: an unresolvable verdict leaves the read exactly as it was.
    sup = supersession
    resolve_default = (reallocation_dir is None or reassessment_dir is not None
                       or drc_dir is not None or decision_dir is not None)
    if sup is None and resolve_default:
        try:
            from paper_trader.api import portfolio_decision as _pdec
            sup = _pdec.load_decision_supersession(
                active_book_id=book_id,
                proposal_summary={
                    "reallocation_proposal_available": True,
                    "reallocation_proposal_id": art.get("proposal_id"),
                    "reallocation_proposal_hash": (
                        (art.get("identity") or {}).get("proposal_hash")
                        or proposal.get("proposal_hash")),
                    "reallocation_bound_eligible_market_date": (
                        (art.get("identity") or {}).get("eligible_market_date")
                        or eligible),
                    "reallocation_bound_hoc_assessment_hash": (
                        (art.get("identity") or {}).get("hoc_assessment_hash")),
                    "reallocation_proposal_generated_at": art.get("generated_at"),
                },
                reassessment_dir=reassessment_dir, drc_dir=drc_dir,
                decision_dir=decision_dir)
        except Exception:  # noqa: BLE001 - a read must never crash
            sup = None
    if sup and sup.get("superseded"):
        by = sup.get("superseded_by") or {}
        return _read_payload(
            state=STATE_SUPERSEDED, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=art,
            message=("This reallocation proposal was SUPERSEDED by a newer "
                     "authoritative decision (%s for session %s). It remains "
                     "visible as immutable history and cannot be reviewed as "
                     "current or approved. There is no outstanding reallocation "
                     "to act on."
                     % (by.get("decision") or "governed decision",
                        by.get("session") or "?")),
            policy=proposal.get("policy") or resolve_policy(), proposal=proposal,
            input_contract=art.get("input_contract"), staleness=staleness,
            supersession=sup)
    return _read_payload(
        state=proposal.get("proposal_state") or STATE_READY, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art,
        message="Current reallocation proposal for the active book / eligible session. "
                "Manual review required — review only, no orders.",
        policy=proposal.get("policy") or resolve_policy(), proposal=proposal,
        input_contract=art.get("input_contract"), staleness=staleness,
        supersession=sup)


# --------------------------------------------------------------------------- #
# Compact summary the Daily Action Gate / workflow state read
# --------------------------------------------------------------------------- #
def load_constrained_reallocation(*, portfolio_state: Optional[dict] = None,
                                  artifact: Optional[dict] = None,
                                  reallocation_dir=None, decision_dir=None,
                                  desk_dir=None, actions_dir=None,
                                  reassessment_dir=None, drc_dir=None,
                                  supersession: Optional[dict] = None,
                                  now: Optional[datetime] = None,
                                  portfolio_state_loader: Optional[Callable] = None,
                                  include_execution: bool = True,
                                  decision_lane: Optional[dict] = None,
                                  rebalance: Optional[dict] = None) -> dict:
    """Release 47 read contract: the whole constraint-respecting reallocation story,
    in the order an operator has to read it.

        CURRENT PAPER BOOK -> IDEAL TARGET -> CONSTRAINT ADJUSTMENTS ->
        BEST FEASIBLE TARGET -> SWITCHING ECONOMICS -> GOVERNED PROPOSAL ->
        APPROVAL STATE -> EXECUTION STATE

    READ-ONLY and degrade-safe. It runs no engine and writes nothing: every block is
    read from the owner that already produced it, so this surface can never disagree
    with the decision the backend actually took. In particular it NEVER says the
    portfolio is blocked while a feasible constrained target exists - the outcome it
    renders is the kernel's own, verbatim.
    """
    payload = load_reallocation_proposal(
        portfolio_state=portfolio_state, artifact=artifact,
        reallocation_dir=reallocation_dir, reassessment_dir=reassessment_dir,
        drc_dir=drc_dir, decision_dir=decision_dir, supersession=supersession,
        now=now, portfolio_state_loader=portfolio_state_loader)

    # Local imports: these owners import THIS module, so importing them at module
    # scope would be circular. They are composed, never forked.
    from paper_trader.api import portfolio_decision as _pdec
    # Release 50 - the decision snapshot passes the lane and the rebalance state it
    # already composed (ONE heavy read per snapshot identity); a direct call still
    # composes them itself, exactly as before.
    # R54.2.3.2 - the read payload above already resolved the supersession verdict;
    # it is handed to the lane so the two can never disagree on one composition.
    lane = decision_lane if decision_lane is not None else _pdec.load_portfolio_decision(
        portfolio_state=portfolio_state, artifact=artifact,
        decision_dir=decision_dir, reallocation_dir=reallocation_dir,
        reassessment_dir=reassessment_dir, drc_dir=drc_dir,
        supersession=(payload.get("supersession")
                      if supersession is None else supersession),
        now=now, portfolio_state_loader=portfolio_state_loader)

    execution = None
    if include_execution:
        try:
            from paper_trader.api import rebalance_execution as _rex
            rex = rebalance if rebalance is not None else _rex.load_rebalance_state(
                decision_dir=decision_dir, reallocation_dir=reallocation_dir,
                desk_dir=desk_dir, actions_dir=actions_dir,
                portfolio_state=portfolio_state,
                portfolio_state_loader=portfolio_state_loader)
            execution = {
                "rebalance_state": rex.get("rebalance_state") or rex.get("state"),
                "state_vocabulary": rex.get("state_vocabulary"),
                "order_plan_buildable": rex.get("order_plan_buildable"),
                "order_plan_id": rex.get("order_plan_id"),
                "n_orders": rex.get("n_orders"),
                "blocked_reasons": rex.get("blocked_reasons") or [],
                "next_action": rex.get("next_action"),
                "confirm_required_token": _rex.CONFIRM_TOKEN,
                "message": rex.get("message"),
            }
        except Exception as exc:  # noqa: BLE001 - a read must never crash
            execution = {"rebalance_state": "REBALANCE_UNAVAILABLE",
                         "message": str(exc)[:160]}

    reopt = payload.get("constraint_reoptimization") or {}
    verdict = payload.get("reallocation_outcome") or {}
    alloc = payload.get("allocations") or []
    # When no proposal exists yet there is no kernel verdict to render, and the card
    # must not print an empty banner. This owner already owns the READ STATE, so it
    # states that - and only that. It never invents an outcome: `outcome` stays None
    # and `outcome_state` names why, so nothing downstream can mistake "not run" for
    # a decision.
    # R54.2.3.2 — a SUPERSEDED payload state must not render the kernel's own
    # PROPOSAL_READY headline: the newer authoritative decision is the story.
    if payload.get("state") == STATE_SUPERSEDED:
        headline = "PROPOSAL SUPERSEDED - NEWER DECISION STANDS (HISTORY ONLY)"
    else:
        headline = verdict.get("headline") or {
            STATE_NOT_RUN: "NO PROPOSAL YET - RUN THE DAILY RESEARCH CYCLE",
            STATE_NO_ACTIVE_BOOK: "NO ACTIVE PAPER BOOK",
            STATE_UNAVAILABLE: "CONSTRAINED REALLOCATION UNAVAILABLE",
            STATE_STALE: "PROPOSAL SUPERSEDED - FRESH REVIEW REQUIRED",
            STATE_BLOCKED: "PORTFOLIO DECISION BLOCKED",
            # R83 — a withheld complete target is not "no proposal yet", and the one
            # action the NOT_RUN headline names is the one that cannot change it.
            STATE_WITHHELD: "COMPLETE TARGET WITHHELD - PORTFOLIO LIMIT REVIEW REQUIRED",
        }.get(payload.get("state"), "CONSTRAINT-RESPECTING REALLOCATION")
    return {
        "phase": "R47",
        "owner": COMPOSITION_OWNER,
        "calculation_owner": _cr.CALCULATION_OWNER,
        "generated_at": payload.get("generated_at"),
        "state": payload.get("state"),
        "eligible_market_date": payload.get("eligible_market_date"),
        "active_book": payload.get("active_book") or {},
        # 1. the current paper book
        "current_paper_book": {
            "weights": {a["ticker"]: a.get("current_weight") for a in alloc
                        if (a.get("current_weight") or 0) > 0},
            "position_count": (payload.get("portfolio") or {}).get(
                "current_holding_count"),
            "cash_weight": (payload.get("portfolio") or {}).get(
                "current_cash_weight"),
            "nav": (payload.get("portfolio") or {}).get("nav"),
            "owner": "api.portfolio_state",
        },
        # 2. the ideal target this pipeline wanted, before any constraint bound
        "ideal_target": {
            "weights": reopt.get("ideal_target") or {},
            "was_feasible": reopt.get("ideal_target_was_feasible"),
            "breached_limits": reopt.get("breached_limits") or [],
            "zero_base_owner": "api.zero_base_target",
            "zero_base_route": "/v1/portfolio/zero-base-target",
            "doc": ("The complete target built from the opportunity-cost "
                    "assessment and the eligible universe, before the mandatory "
                    "portfolio limits were applied to it."),
        },
        # 3. what the constraints changed
        "constraint_adjustments": reopt.get("constraint_adjustments") or [],
        "constraints_that_reshaped": reopt.get("constraints_that_reshaped") or [],
        "constraint_reoptimization_applied": bool(reopt.get("applied")),
        "constraint_inventory": payload.get("constraint_inventory") or {},
        # 4. the best feasible target actually on the table
        "best_feasible_target": {
            "weights": {a["ticker"]: a.get("proposed_weight") for a in alloc
                        if (a.get("proposed_weight") or 0) > 0},
            "position_count": (payload.get("portfolio") or {}).get(
                "proposed_holding_count"),
            "cash_weight": (payload.get("portfolio") or {}).get(
                "proposed_cash_weight"),
            "allocations": alloc,
            "constraints": payload.get("constraints") or {},
            "complete_target_limits": payload.get("complete_target_limits") or {},
        },
        # R83 — the TERMINAL governed withheld verdict, forwarded from the read payload
        # so the Portfolio Overview reads the SAME measured limit breach that the
        # reallocation card, Today and the decision review read. Without it this
        # surface could only say "no feasible target was priced for this session",
        # which on 2026-09-28 was false twice over: a complete target WAS priced, and
        # it cleared its economics before a risk limit refused it.
        "governed_withheld_outcome": payload.get("governed_withheld_outcome"),
        "complete_target_withheld_on_portfolio_limits": bool(
            (payload.get("governed_withheld_outcome") or {}).get("withheld")),
        # 5. what switching would cost and buy
        "switching_economics": payload.get("switching_economics") or {},
        "turnover": payload.get("turnover") or {},
        "risk": payload.get("risk") or {},
        "signal": payload.get("signal") or {},
        # Release 50 - the same two portfolios by asset class / sleeve, verbatim from
        # the proposal owner (only groups that carry weight; cash is the residual).
        "multi_asset": {
            "current_allocation_by_asset_class": (
                (payload.get("portfolio") or {}).get("current_allocation_by_asset_class")
                or (((portfolio_state or {}).get("capital_pool") or {}).get("allocation")) or {}),
            "target_allocation_by_asset_class": (
                (payload.get("portfolio") or {}).get("proposed_allocation_by_asset_class") or {}),
            "current_allocation_by_sleeve": (
                (payload.get("portfolio") or {}).get("current_allocation_by_sleeve") or {}),
            "target_allocation_by_sleeve": (
                (payload.get("portfolio") or {}).get("proposed_allocation_by_sleeve") or {}),
            "asset_classes_in_target": (
                (payload.get("portfolio") or {}).get("asset_classes_in_target") or []),
            "non_equity_position_count_in_target": (
                (payload.get("portfolio") or {}).get("non_equity_position_count_in_target")),
            "proposed_gross_exposure": (payload.get("portfolio") or {}).get("proposed_gross_exposure"),
            "proposed_collateral_weight": (payload.get("portfolio") or {}).get("proposed_collateral_weight"),
            "frontier_hash": ((payload.get("input_contract") or {}).get("frontier_hash")),
            "frontier_eligible_non_equity_count": (
                (payload.get("input_contract") or {}).get("frontier_eligible_non_equity_count")),
            "frontier_rows_admitted": (
                (payload.get("input_contract") or {}).get("frontier_rows_admitted") or []),
            "capital_eligible_sleeve_ids": (
                (payload.get("input_contract") or {}).get("registry_capital_eligible_sleeve_ids") or []),
            "forced_diversification": False,
            "current_holdings_privileged": False,
            "labels": dict(_ic_labels()),
            "owner": "engine.reallocation_proposal via api.reallocation_proposal",
            "frontier_owner": "api.opportunity_frontier",
            "registry_owner": "api.investability_registry",
        },
        # 6. the one authoritative outcome
        "outcome": payload.get("outcome"),
        "outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "outcome_state": (payload.get("state") if not payload.get("outcome")
                          else None),
        "reallocation_outcome": verdict,
        "headline": headline,
        # R54.2.3.2 — the decision-supersession verdict, verbatim.
        "superseded": bool(payload.get("superseded")),
        "supersession": payload.get("supersession"),
        "superseded_by": payload.get("superseded_by"),
        # R54.2.4 — the immutable artifact meta (id, generated_at), republished
        # verbatim so the proposal-history presentation can state WHEN the
        # superseded proposal was created without a second artifact read.
        "artifact": payload.get("artifact"),
        "feasible_target_exists": verdict.get("feasible_target_exists"),
        # 7. approval, 8. execution
        "approval": {
            "portfolio_decision_state": lane.get("portfolio_decision_state"),
            "label": lane.get("label"),
            "approvable": lane.get("approvable"),
            "requires_manual_review": lane.get("requires_manual_review"),
            "decision": lane.get("decision"),
            "decision_recorded_at": lane.get("decision_recorded_at"),
            "confirm_required_token": lane.get("confirm_required_token"),
            "decision_path": lane.get("sole_decision_path"),
            "manual_approval_required": True,
            # R82.2 — WHY approval is not available, in the decision owner's own
            # words. Without it this card said "nothing awaits approval" for a book
            # that was awaiting a risk-policy ruling, which is a different thing.
            "approval_gate": lane.get("approval_gate"),
            "next_required_action": lane.get("next_required_action"),
        },
        "execution": execution,
        "review_only": True,
        "safety": payload.get("safety") or {},
    }


def load_proposal_summary(*, active_book_id: Optional[str] = None,
                          eligible_market_date: Optional[str] = None,
                          artifact: Optional[dict] = None, reallocation_dir=None,
                          actions_dir=None, drc_dir=None) -> dict:
    """A compact, read-only reallocation-proposal summary. PURE IMMUTABLE-STORE READER
    — it reads the immutable proposal index/artifact for the exact
    ``(active_book_id, eligible_market_date)`` the caller supplies, and, when no
    artifact exists, the governed run manifest for that same book + session so a
    TERMINAL WITHHELD verdict is not reported as a cycle that never ran (R83). It never
    loads portfolio state, never runs the engine, and never calls a provider /
    prediction. ``available=False`` with a NOT_RUN state when no artifact exists and no
    governed verdict was recorded."""
    empty = {
        "reallocation_proposal_available": False,
        "reallocation_proposal_state": STATE_NOT_RUN,
        "reallocation_proposal_hash": None,
        "reallocation_proposal_id": None,
        "reallocation_proposal_generated_at": None,
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
        "reallocation_proposal_withheld": False,
        "reallocation_withheld_reasons": [],
        "reallocation_proposal_approvable": False,
        # R83 — the terminal governed withheld verdict, or None for "no information".
        "reallocation_governed_withheld_outcome": None,
        # --- R63 mandatory-repair contract ----------------------------------- #
        "reallocation_full_target_reviewable": False,
        "reallocation_mandatory_repair_code": None,
        "reallocation_mandatory_obligations_open": [],
        "reallocation_mandatory_repair_detail": None,
        # --- Release 47 ------------------------------------------------------ #
        "reallocation_outcome": None,
        "reallocation_outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "reallocation_outcome_headline": None,
        "reallocation_outcome_reason_codes": [],
        "reallocation_constraints_reshaped": [],
        "reallocation_constraint_reoptimized": False,
        "reallocation_feasible_target_exists": False,
        "reallocation_switching_hurdle": None,
        "reallocation_clears_switching_hurdle": None,
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
        # R83 — the summary is the path Today / Overview / Audit & Details read through
        # (api.daily_action_gate -> api.workflow_state). If it says NOT_RUN for a
        # session the governed cycle already ruled on, every one of those surfaces
        # inherits the false claim and publishes a futile "run the cycle" action.
        wh = None
        if _should_consult_governed_manifest(reallocation_dir=reallocation_dir,
                                             drc_dir=drc_dir):
            wh = load_governed_withheld_outcome(
                active_book_id=active_book_id,
                eligible_market_date=eligible_market_date, drc_dir=drc_dir)
        if not wh:
            return empty
        d = dict(empty)
        # A withheld target is visible, explainable and NEVER approvable or available:
        # ``available`` keeps its one established meaning — a PERSISTED proposal exists.
        d["reallocation_proposal_state"] = STATE_WITHHELD
        d["reallocation_proposal_withheld"] = True
        d["reallocation_proposal_approvable"] = False
        d["reallocation_proposal_hash"] = wh.get("proposal_hash")
        d["reallocation_withheld_reasons"] = list(wh.get("withheld_reasons") or [])
        d["reallocation_proposed_holding_count"] = wh.get("proposed_holding_count")
        d["reallocation_action_counts"] = dict(wh.get("action_counts")
                                               or {a: 0 for a in ACTION_VOCAB})
        d["reallocation_one_way_turnover"] = wh.get("one_way_turnover")
        d["reallocation_estimated_transaction_cost"] = wh.get(
            "estimated_transaction_cost")
        d["reallocation_score_improvement"] = wh.get("score_improvement")
        d["reallocation_score_improvement_net_of_cost"] = wh.get(
            "score_improvement_net_of_cost")
        # A COMPLETE governed verdict DID construct a feasible-set answer: it proved
        # the feasible set EMPTY. That is the opposite of "no target was computed", and
        # the R47 fields say so without claiming an approvable target exists.
        d["reallocation_feasible_target_exists"] = False
        d["reallocation_governed_withheld_outcome"] = wh
        return d
    p = art.get("proposal") or {}
    sig = p.get("signal") or {}
    trn = p.get("turnover") or {}
    # Stage 19.1: staleness against the corporate-action registry is resolved by the ONE
    # owner from the registry file alone — this stays a pure artifact reader (no portfolio
    # state load, no engine run, no provider call).
    try:
        stale_blk = corporate_action_staleness(artifact=art, active_book_id=active_book_id,
                                               actions_dir=actions_dir)
    except Exception:  # noqa: BLE001 — never crash a compact summary read
        stale_blk = {"stale": False, "reason": None,
                     "current_corporate_actions_hash": None}
    _repair = kernel.mandatory_repair_read_verdict(p)
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
        # R54.2.3.2 — when the artifact was produced, so the decision-supersession
        # direction proof never has to re-open the artifact file.
        "reallocation_proposal_generated_at": art.get("generated_at"),
        # Release 29.3 — a withheld complete target is visible but never approvable.
        "reallocation_proposal_withheld": bool(
            p.get("proposal_state") == STATE_WITHHELD),
        "reallocation_withheld_reasons": [
            b.get("code") for b in (p.get("withheld_reasons") or []) if b.get("code")],
        # Release 47: approvability now also requires the kernel's own outcome to be
        # PROPOSAL_READY. A feasible-but-not-worth-it target is HOLD_CURRENT_BOOK, and
        # HOLD is a decision, not a queue item.
        # R63: and the target must resolve every obligation the opportunity-cost
        # owner ruled. Read from the kernel's own published verdict, failing
        # closed when the artifact predates the contract and carries none.
        "reallocation_proposal_approvable": bool(
            not stale_blk.get("stale")
            and p.get("proposal_state") in APPROVABLE_READ_STATES
            and p.get("approvable", True)
            and _repair["reviewable"]),
        "reallocation_full_target_reviewable": _repair["reviewable"],
        "reallocation_mandatory_repair_code": _repair["code"],
        "reallocation_mandatory_obligations_open": list(_repair["instruments"]),
        "reallocation_mandatory_repair_detail": _repair["detail"],
        "reallocation_outcome": p.get("outcome"),
        "reallocation_outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "reallocation_outcome_headline": (p.get("reallocation_outcome") or {}).get(
            "headline"),
        "reallocation_outcome_reason_codes": list(
            (p.get("reallocation_outcome") or {}).get("reason_codes") or []),
        "reallocation_constraints_reshaped": list(
            (p.get("constraint_reoptimization") or {}).get(
                "constraints_that_reshaped") or []),
        "reallocation_constraint_reoptimized": bool(
            (p.get("constraint_reoptimization") or {}).get("applied")),
        "reallocation_feasible_target_exists": bool(
            (p.get("reallocation_outcome") or {}).get("feasible_target_exists")),
        "reallocation_switching_hurdle": (p.get("switching_economics") or {}).get(
            "switching_hurdle"),
        "reallocation_clears_switching_hurdle": (
            p.get("switching_economics") or {}).get("clears_switching_hurdle"),
        "reallocation_action_counts": p.get("action_counts") or {a: 0 for a in ACTION_VOCAB},
        "reallocation_score_improvement": sig.get("score_improvement"),
        "reallocation_score_improvement_net_of_cost": sig.get("score_improvement_net_of_cost"),
        "reallocation_one_way_turnover": trn.get("one_way_turnover"),
        "reallocation_estimated_transaction_cost": trn.get("estimated_transaction_cost"),
        "reallocation_proposed_holding_count": (p.get("portfolio") or {}).get(
            "proposed_holding_count"),
        "reallocation_data_gaps": [g.get("code") for g in (p.get("data_gaps") or [])
                                   if not g.get("by_design")],
        # Stage 22 (Workstream E) — the EXACT assessment / session / portfolio this
        # proposal is bound to. A consumer proves the binding instead of assuming it:
        # a proposal that does not bind to the CURRENT fresh assessment must never be
        # presented as reviewable.
        "reallocation_bound_hoc_assessment_hash": (
            (art.get("identity") or {}).get("hoc_assessment_hash")),
        "reallocation_bound_eligible_market_date": (
            (art.get("identity") or {}).get("eligible_market_date")
            or p.get("eligible_market_date")),
        "reallocation_bound_active_book_id": (
            (art.get("identity") or {}).get("active_book_id")),
    }
