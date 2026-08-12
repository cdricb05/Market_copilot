r"""Phase 29G Slice 6 — Holding Opportunity-Cost composition, persistence & read owner.

This is the ONE canonical orchestration / validation / immutable-artifact /
read-contract owner for the Holding Opportunity-Cost Engine (Consolidation
Roadmap Slice 6 / Charter Milestone 2). It performs NO calculation of its own —
the single canonical calculation lives in ``engine.holding_opportunity_cost``.
This module only:

  1. SOURCES an immutable point-in-time assessment-input contract from the
     authoritative owners (``api.portfolio_state`` for holdings / weights / NAV /
     cash / sectors; ``api.universe_scoring`` for rank / score / eligibility /
     adv_dollar; ``api.price_panel`` for owned trailing close + dollar volume;
     ``engine.market_session`` for the previous eligible session; the previous
     eligible date's persisted artifact for prior-session rank), reusing the
     canonical construction constants from ``api.multi_horizon_engine`` and the
     transaction-cost constant from ``api.paper_trading_desk`` (never forked).
  2. RUNS the pure kernel.
  3. PERSISTS a completed production assessment as an immutable artifact under a
     dedicated research / decision-evidence root (atomic write, index/manifest,
     idempotent identical rerun, conflicting artifact rejected, interrupted write
     recoverable). It NEVER writes an operational ledger, PostgreSQL, order, fill,
     holding, cash or NAV.
  4. Exposes ONE read contract for ``GET /v1/operations/holding-opportunity-cost``
     (read-only) plus a lightweight summary for the Daily Action Gate.

The sole NORMAL execution path is the Daily Research Cycle
(``POST /v1/operations/daily-research-cycle/run``); there is deliberately NO
separate manual opportunity-cost execution endpoint. This module is preview-first,
paper-only, review-only: no target is confirmed, no order is created, no automation
is enabled.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import holding_opportunity_cost as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
DECISION_POLICY_VERSION = kernel.DECISION_POLICY_VERSION
COST_POLICY_VERSION = kernel.COST_POLICY_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
RECOMMENDATION_VOCAB = kernel.RECOMMENDATION_VOCAB
COMPOSITION_OWNER = "api.holding_opportunity_cost"
PHASE = "29G-Slice6"

# Read-layer states extend the kernel's assessment states.
STATE_READY = kernel.STATE_READY
STATE_DEGRADED = kernel.STATE_DEGRADED
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NO_ACTIVE_BOOK = kernel.STATE_NO_ACTIVE_BOOK
STATE_NOT_RUN = "NOT_RUN"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK,
                    STATE_NOT_RUN, STATE_UNAVAILABLE)

# --- immutable artifact root (configurable; a research / decision-evidence root, --- #
# NEVER the operational ledger root). -------------------------------------------- #
HOC_DIR_ENV = "PAPER_TRADER_HOC_DIR"
_DEFAULT_HOC_DIR = Path(r"D:\Stock_Prediction_app_data\holding_opportunity_cost")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"

# Trailing bars pulled from the price panel (comfortably covers the 60-close
# return / volatility / drawdown windows and the covariance lookback).
_TRAILING_BARS = 140


# --------------------------------------------------------------------------- #
# Time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _hoc_dir(hoc_dir=None) -> Path:
    if hoc_dir is not None:
        return Path(hoc_dir)
    env = os.environ.get(HOC_DIR_ENV)
    return Path(env) if env else _DEFAULT_HOC_DIR


def _artifacts_dir(hoc_dir=None) -> Path:
    return _hoc_dir(hoc_dir) / _ARTIFACTS_SUBDIR


def _index_path(hoc_dir=None) -> Path:
    return _hoc_dir(hoc_dir) / _INDEX_FILE


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
# Artifact identity (Workstream H)
# --------------------------------------------------------------------------- #
def artifact_identity(*, input_contract: dict, result: dict) -> dict:
    return {
        "eligible_market_date": input_contract.get("eligible_market_date"),
        "active_book_id": input_contract.get("active_book_id"),
        "portfolio_state_hash": input_contract.get("portfolio_state_hash"),
        # Stage 19.1 — the corporate-action registry the holdings/NAV this assessment
        # consumed were projected through.
        "corporate_actions_hash": input_contract.get("corporate_actions_hash"),
        "universe_scoring_hash": input_contract.get("universe_scoring_hash"),
        "decision_policy_version": DECISION_POLICY_VERSION,
        "assessment_hash": result.get("assessment_hash"),
    }


def artifact_id_for(identity: dict) -> str:
    book = (identity.get("active_book_id") or "book")
    date = (identity.get("eligible_market_date") or "nodate")
    h = (identity.get("assessment_hash") or "")[:12]
    return "hoc_%s_%s_%s" % (date, book, h)


def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


def _compact_input_contract(ic: dict) -> dict:
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "active_book_label": ic.get("active_book_label"),
        "valuation_date": ic.get("valuation_date"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "scoring_input_fingerprints": ic.get("scoring_input_fingerprints"),
        "cost_policy_version": COST_POLICY_VERSION,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "positions_count": len(ic.get("positions") or []),
        "universe_rows_count": len(ic.get("universe_rows") or []),
        "previous_ranking_state": ic.get("previous_ranking_state"),
        "previous_ranking_reason": ic.get("previous_ranking_reason"),
        "previous_ranking_source_date": ic.get("previous_ranking_source_date"),
        "inputs_as_of_eligible_date": ic.get("inputs_as_of_eligible_date"),
    }


# --------------------------------------------------------------------------- #
# Input-contract sourcing (Workstream B). Every source is injectable for tests.
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    """Reuse the canonical construction + cost constants (never forked)."""
    ov: dict[str, Any] = {}
    try:
        import math as _m
        from paper_trader.api import multi_horizon_engine as eng
        n = eng.BOOK_SIZES[0]
        ov.update({
            "entry_rank": n,
            "exit_buffer_fraction": eng.EXIT_BUFFER_FRACTION,
            "exit_buffer_rank": _m.ceil(n * (1.0 + eng.EXIT_BUFFER_FRACTION)),
            "sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
            "max_name_weight": eng.MAX_INDIVIDUAL_WEIGHT,
            "min_adv_dollar": eng.MIN_ADV_DOLLAR,
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
            "target_weight": _f(p.get("target_weight")),
        })
    return out


def _universe_rows_from_scoring(scoring: dict) -> list[dict]:
    return list(scoring.get("rankings") or [])


def _holding_eligibility(*, held: set, scoring: dict) -> dict:
    """Eligibility for HELD names, reusing the canonical universe eligibility.

    In the eligible ``rankings`` -> eligible. Otherwise map the scored-but-excluded
    reason (``exclusions``) or flag NOT_IN_ELIGIBLE_UNIVERSE (a hard exit reason).
    """
    ranked = {r.get("ticker") for r in (scoring.get("rankings") or [])}
    exclusions = scoring.get("exclusions") or {}
    out: dict[str, dict] = {}
    for tk in held:
        if tk in ranked:
            out[tk] = {"eligible": True, "hard_codes": [], "exclusion_reason": None}
        else:
            reason = exclusions.get(tk)
            codes = [reason] if reason else ["NOT_IN_ELIGIBLE_UNIVERSE"]
            out[tk] = {"eligible": False, "hard_codes": codes, "exclusion_reason": reason}
    return out


def _trailing_prices(*, price_panel: dict, tickers: list, eligible: str) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    out: dict[str, dict] = {}
    for tk in tickers:
        s = series.get(tk)
        if not s:
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        if j < 0:
            continue
        lo = max(0, j - _TRAILING_BARS + 1)
        out[tk] = {
            "dates": list(s["dates"][lo:j + 1]),
            "adj": list(s["adj"][lo:j + 1]),
            "ret": list(s["ret"][lo:j + 1]),
        }
    return out


def _median_dollar_volumes(*, price_panel: dict, tickers: list, eligible: str,
                           window: int = 20) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    out: dict[str, Optional[float]] = {}
    for tk in tickers:
        s = series.get(tk)
        if not s:
            out[tk] = None
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        out[tk] = pp.trailing_median_dollar_volume(s, j, window) if j >= 0 else None
    return out


def _aligned_returns(*, price_panel: dict, tickers: list, eligible: str,
                     lookback: int) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    per: dict[str, dict] = {}
    date_sets: list[set] = []
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
            date_sets.append(set(dmap.keys()))
    if not date_sets:
        return {"dates": [], "series": {}}
    common = set.intersection(*date_sets) if len(date_sets) > 1 else date_sets[0]
    common_sorted = sorted(common)[-lookback:]
    out = {tk: [per[tk].get(d) for d in common_sorted] for tk in per}
    return {"dates": common_sorted, "series": out}


def build_input_contract(*, portfolio_state: dict, scoring: dict,
                         price_panel: Optional[dict] = None,
                         previous_ranking: Optional[dict] = None,
                         previous_ranking_state: Optional[str] = None,
                         previous_ranking_reason: Optional[str] = None,
                         previous_ranking_source_date: Optional[str] = None,
                         prior_signal: Optional[dict] = None,
                         policy: Optional[dict] = None) -> dict:
    """Assemble the immutable point-in-time assessment-input contract (Workstream B).

    All inputs are sourced as of the portfolio-state eligible market date. Prior
    point-in-time rank is honestly reported UNAVAILABLE when no prior snapshot
    exists. No future rows; no current-snapshot-as-prior; no fabricated volume.
    """
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    sc = scoring or {}
    eligible = ((ps.get("dates") or {}).get("eligible_market_date"))
    active_book = ps.get("active_book") or {}
    active_book_id = active_book.get("book_id")

    positions = _positions_from_state(ps)
    held = {p["ticker"] for p in positions if p.get("ticker")}
    universe_rows = _universe_rows_from_scoring(sc)
    holding_elig = _holding_eligibility(held=held, scoring=sc)

    trailing = {}
    mdv = {}
    aligned = {"dates": [], "series": {}}
    if price_panel:
        held_list = sorted(held)
        trailing = _trailing_prices(price_panel=price_panel, tickers=held_list, eligible=eligible)
        mdv = _median_dollar_volumes(price_panel=price_panel, tickers=held_list, eligible=eligible)
        aligned = _aligned_returns(price_panel=price_panel, tickers=held_list,
                                   eligible=eligible, lookback=pol["covariance_lookback"])

    prev_state = previous_ranking_state or ("AVAILABLE" if previous_ranking else "UNAVAILABLE")
    prev_reason = previous_ranking_reason or (
        "" if previous_ranking else
        "No prior eligible-session ranking artifact is available; previous rank and "
        "rank change are reported as unavailable.")

    scoring_ranking_date = sc.get("ranking_date")
    inputs_as_of = bool(eligible and (not scoring_ranking_date
                                      or scoring_ranking_date == eligible))

    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": active_book_id,
        "active_book_label": active_book.get("book_label"),
        "valuation_date": (ps.get("dates") or {}).get("valuation_date"),
        "portfolio_state_hash": ps.get("state_hash"),
        # Stage 19.1: the corporate-action registry state the CURRENT holdings / NAV
        # below were projected through (owned by api.corporate_actions, surfaced by
        # api.portfolio_state). Bound into the assessment identity.
        "corporate_actions_hash": ((ps.get("corporate_actions") or {})
                                   .get("registry_fingerprint")),
        "universe_scoring_hash": sc.get("output_hash"),
        "universe_input_contract_hash": sc.get("input_contract_hash"),
        "scoring_input_fingerprints": sc.get("input_fingerprints"),
        "scoring_ranking_date": scoring_ranking_date,
        "inputs_as_of_eligible_date": inputs_as_of,
        "cost_policy_version": COST_POLICY_VERSION,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "nav": (ps.get("capital") or {}).get("nav"),
        "cash": (ps.get("capital") or {}).get("cash"),
        "positions": positions,
        "universe_rows": universe_rows,
        "holding_eligibility": holding_elig,
        "previous_ranking": previous_ranking,
        "previous_ranking_state": prev_state,
        "previous_ranking_reason": prev_reason,
        "previous_ranking_source_date": previous_ranking_source_date,
        "prior_signal": prior_signal or {},
        "trailing_prices": trailing,
        "median_dollar_volume": mdv,
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


def _prior_ranking_from_artifact(*, active_book_id, eligible_market_date, hoc_dir=None):
    """Prior eligible-session per-ticker rank from the previous eligible date's
    persisted artifact (PIT-honest: a real prior snapshot, never today's snapshot).

    Returns ``(ranking|None, state, reason, source_date)``.
    """
    from paper_trader.engine import market_session as ms
    from datetime import date as _date
    if not eligible_market_date:
        return None, "UNAVAILABLE", "No eligible market date to anchor a prior lookup.", None
    try:
        d = _date.fromisoformat(eligible_market_date)
        prior_date = ms.previous_trading_day(d).isoformat()
    except (ValueError, TypeError):
        return None, "UNAVAILABLE", "Eligible market date is not a valid ISO date.", None
    art = load_latest_artifact(active_book_id=active_book_id,
                               eligible_market_date=prior_date, hoc_dir=hoc_dir)
    if not art:
        return (None, "UNAVAILABLE",
                "No persisted opportunity-cost artifact exists for the previous eligible "
                "session (%s); previous rank is unavailable." % prior_date, prior_date)
    snapshot = ((art.get("assessment") or {}).get("diagnostics") or {}).get("rank_snapshot")
    if not snapshot:
        return (None, "UNAVAILABLE",
                "The previous session artifact (%s) carries no rank snapshot." % prior_date,
                prior_date)
    return dict(snapshot), "AVAILABLE", "", prior_date


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_assessment(*, input_contract: Optional[dict] = None,
                   portfolio_state: Optional[dict] = None,
                   scoring: Optional[dict] = None,
                   price_panel: Optional[dict] = None,
                   previous_ranking: Optional[dict] = None,
                   previous_ranking_state: Optional[str] = None,
                   previous_ranking_reason: Optional[str] = None,
                   prior_signal: Optional[dict] = None,
                   policy: Optional[dict] = None,
                   hoc_dir=None,
                   portfolio_state_loader: Optional[Callable] = None,
                   scoring_loader: Optional[Callable] = None,
                   price_panel_loader: Optional[Callable] = None) -> dict:
    """Build the input contract (unless supplied) and run the pure kernel.

    Returns ``{"input_contract": ..., "assessment": <kernel result>}``. Read-only:
    persists nothing.
    """
    pol = policy or resolve_policy()
    if input_contract is None:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
        sc = scoring if scoring is not None else (
            (scoring_loader or _default_scoring_loader)())
        pp_obj = price_panel
        if pp_obj is None and price_panel is None:
            try:
                pp_obj = (price_panel_loader or _default_price_panel_loader)()
            except Exception:  # noqa: BLE001
                pp_obj = None
        if previous_ranking is None and previous_ranking_state is None:
            ab = (ps.get("active_book") or {}).get("book_id")
            eligible = (ps.get("dates") or {}).get("eligible_market_date")
            previous_ranking, previous_ranking_state, previous_ranking_reason, prior_src = (
                _prior_ranking_from_artifact(active_book_id=ab,
                                             eligible_market_date=eligible, hoc_dir=hoc_dir))
        else:
            prior_src = None
        input_contract = build_input_contract(
            portfolio_state=ps, scoring=sc, price_panel=pp_obj,
            previous_ranking=previous_ranking, previous_ranking_state=previous_ranking_state,
            previous_ranking_reason=previous_ranking_reason,
            previous_ranking_source_date=prior_src, prior_signal=prior_signal, policy=pol)
    result = kernel.build_assessment(input_contract=input_contract, policy=pol)
    return {"input_contract": input_contract, "assessment": result}


# --------------------------------------------------------------------------- #
# Persist (immutable artifact) — Workstream H
# --------------------------------------------------------------------------- #
def persist_assessment(*, result: dict, input_contract: dict, hoc_dir=None,
                       now: Optional[datetime] = None) -> dict:
    """Persist a completed production assessment as an immutable artifact.

    Idempotent: an identical re-run (same identity incl. assessment_hash) reuses the
    existing artifact. A CONFLICTING artifact (same book+date but different
    portfolio/universe/assessment hash) is REJECTED — the immutable artifact is never
    overwritten. Only production READY / DEGRADED assessments are persisted; BLOCKED /
    NO_ACTIVE_BOOK are not (nothing durable to record).
    """
    state = result.get("assessment_state")
    if state not in (STATE_READY, STATE_DEGRADED):
        return {"status": "NOT_PERSISTED", "reason": "STATE_%s_NOT_PERSISTABLE" % state,
                "artifact_id": None, "persisted": False, "reused": False, "conflict": False}

    identity = artifact_identity(input_contract=input_contract, result=result)
    aid = artifact_id_for(identity)
    key = _index_key(identity["active_book_id"], identity["eligible_market_date"])
    index = _load_json(_index_path(hoc_dir)) or {}
    existing = index.get(key)

    if existing:
        if existing.get("assessment_hash") == identity["assessment_hash"]:
            return {"status": "REUSED_EXISTING", "artifact_id": existing.get("artifact_id"),
                    "path": existing.get("path"), "persisted": True, "reused": True,
                    "conflict": False, "identity": identity}
        # Same book+date, different content -> a conflicting contract. Never overwrite.
        return {"status": "CONFLICT_REJECTED", "artifact_id": aid,
                "existing_artifact_id": existing.get("artifact_id"),
                "existing_assessment_hash": existing.get("assessment_hash"),
                "persisted": False, "reused": False, "conflict": True,
                "reason": "An immutable artifact already exists for this book+eligible date "
                          "with a different input/assessment hash; not overwritten.",
                "identity": identity}

    payload = {
        "artifact_id": aid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "identity": identity,
        "input_contract": _compact_input_contract(input_contract),
        "assessment": result,
    }
    path = _artifacts_dir(hoc_dir) / ("%s.json" % aid)
    _atomic_write_json(path, payload)
    # Update the index AFTER the artifact write (interrupted-write recoverable: an
    # unindexed artifact is simply re-created on the next identical run).
    index[key] = {"artifact_id": aid, "path": str(path),
                  "assessment_hash": identity["assessment_hash"],
                  "portfolio_state_hash": identity["portfolio_state_hash"],
                  "universe_scoring_hash": identity["universe_scoring_hash"],
                  "decision_policy_version": identity["decision_policy_version"],
                  "eligible_market_date": identity["eligible_market_date"],
                  "active_book_id": identity["active_book_id"],
                  "generated_at": payload["generated_at"]}
    _atomic_write_json(_index_path(hoc_dir), index)
    return {"status": "CREATED", "artifact_id": aid, "path": str(path),
            "persisted": True, "reused": False, "conflict": False, "identity": identity}


def load_latest_artifact(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str], hoc_dir=None) -> Optional[dict]:
    """Load the persisted artifact for an exact (active book, eligible date), or None."""
    index = _load_json(_index_path(hoc_dir)) or {}
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None:
        art = _load_json(_artifacts_dir(hoc_dir) / ("%s.json" % entry.get("artifact_id")))
    return art


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None, price_panel: Optional[dict] = None,
                    previous_ranking: Optional[dict] = None,
                    previous_ranking_state: Optional[str] = None,
                    prior_signal: Optional[dict] = None, policy: Optional[dict] = None,
                    hoc_dir=None, now: Optional[datetime] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None,
                    price_panel_loader: Optional[Callable] = None) -> dict:
    """The Daily Research Cycle entry: build -> kernel -> persist (idempotent)."""
    run = run_assessment(
        portfolio_state=portfolio_state, scoring=scoring, price_panel=price_panel,
        previous_ranking=previous_ranking, previous_ranking_state=previous_ranking_state,
        prior_signal=prior_signal, policy=policy, hoc_dir=hoc_dir,
        portfolio_state_loader=portfolio_state_loader, scoring_loader=scoring_loader,
        price_panel_loader=price_panel_loader)
    persist = persist_assessment(result=run["assessment"],
                                 input_contract=run["input_contract"], hoc_dir=hoc_dir, now=now)
    return {"input_contract": run["input_contract"], "assessment": run["assessment"],
            "persistence": persist}


# --------------------------------------------------------------------------- #
# Read contract (Workstream J) — GET /v1/operations/holding-opportunity-cost
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


def _corporate_action_staleness(*, artifact: Optional[dict],
                                book_id: Optional[str],
                                portfolio_state: Optional[dict] = None) -> dict:
    """Stage 19.1 — is a persisted assessment still valid against the CURRENT
    corporate-action registry? Pure delegation to ``api.corporate_actions``; this module
    owns no split arithmetic and no registry logic."""
    from paper_trader.api import corporate_actions as ca
    art = artifact or {}
    bound = ((art.get("identity") or {}).get("corporate_actions_hash")
             or (art.get("input_contract") or {}).get("corporate_actions_hash"))
    current = ((portfolio_state or {}).get("corporate_actions") or {}).get(
        "registry_fingerprint")
    try:
        if current is not None:
            actions = ((portfolio_state or {}).get("corporate_actions") or {}).get(
                "actions") or []
            return ca.staleness_vs_registry(
                bound, current={"fingerprint": current, "n_registered": len(actions),
                                "actions": actions})
        return ca.staleness_vs_registry(bound, book_id=book_id)
    except Exception:  # noqa: BLE001 - a read contract must never crash
        return {"stale": False, "reason": None}


def _read_payload(*, state: str, generated_at: str, eligible: Optional[str],
                  active_book: dict, artifact: Optional[dict], message: str,
                  policy: dict, assessment: Optional[dict],
                  input_contract: Optional[dict],
                  staleness: Optional[dict] = None) -> dict:
    a = assessment or {}
    art_meta = None
    if artifact:
        art_meta = {"artifact_id": artifact.get("artifact_id"),
                    "generated_at": artifact.get("generated_at"),
                    "identity": artifact.get("identity"),
                    "immutable": True,
                    "root_env": HOC_DIR_ENV}
    return {
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
        "policy": a.get("policy") or policy,
        "portfolio_summary": a.get("portfolio_summary") or {},
        "recommendation_counts": a.get("recommendation_counts")
        or {k: 0 for k in RECOMMENDATION_VOCAB},
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "holding_reviews": a.get("holding_reviews") or [],
        "addition_candidates": a.get("addition_candidates") or [],
        "diagnostics": a.get("diagnostics") or {},
        "data_quality": a.get("data_quality") or {},
        "artifact": art_meta,
        "safety": a.get("safety") or kernel._safety(),
        "provenance": a.get("provenance")
        or {"composition_owner": COMPOSITION_OWNER, "calculation_owner": kernel.CALCULATION_OWNER},
        "assessment_hash": a.get("assessment_hash"),
        "assessment_state": a.get("assessment_state"),
        # Stage 19.1 — an assessment produced BEFORE a registered corporate action was
        # computed against holdings that no longer exist economically. The immutable
        # artifact is preserved and still readable, but it is explicitly NOT current.
        "stale": bool((staleness or {}).get("stale")),
        "staleness": staleness,
        "describes_current_holdings": not bool((staleness or {}).get("stale")),
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
    }


def load_holding_opportunity_cost(*, portfolio_state: Optional[dict] = None,
                                  artifact: Optional[dict] = None, hoc_dir=None,
                                  now: Optional[datetime] = None,
                                  portfolio_state_loader: Optional[Callable] = None) -> dict:
    """The read contract for the endpoint. READ-ONLY: it NEVER runs the engine —
    it returns the latest persisted assessment for the current active book + eligible
    date. When no production artifact exists it returns a readable ``NOT_RUN`` payload
    (the sole execution path is the Daily Research Cycle). Always degrade-safe."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return _read_payload(state=STATE_UNAVAILABLE, generated_at=generated_at, eligible=None,
                             active_book={}, artifact=None,
                             message="Portfolio state is unavailable: %s" % str(exc)[:160],
                             policy=resolve_policy(), assessment=None, input_contract=None)

    active_book = _active_book_block(ps)
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    book_id = active_book.get("book_id")

    if not book_id:
        return _read_payload(state=STATE_NO_ACTIVE_BOOK, generated_at=generated_at,
                             eligible=eligible, active_book=active_book, artifact=None,
                             message="No active operational book; no assessment.",
                             policy=resolve_policy(), assessment=None, input_contract=None)

    art = artifact if artifact is not None else load_latest_artifact(
        active_book_id=book_id, eligible_market_date=eligible, hoc_dir=hoc_dir)
    if not art:
        return _read_payload(
            state=STATE_NOT_RUN, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=None,
            message=("No Holding Opportunity-Cost assessment has been produced for the "
                     "current active book and eligible session yet. Run the Daily Research "
                     "Cycle (POST /v1/operations/daily-research-cycle/run) to produce one."),
            policy=resolve_policy(), assessment=None, input_contract=None)

    assessment = art.get("assessment") or {}
    staleness = _corporate_action_staleness(artifact=art, book_id=book_id,
                                            portfolio_state=ps)
    msg = ("Latest Holding Opportunity-Cost assessment for the active book / eligible "
           "session.")
    if staleness.get("stale"):
        msg = ("This Holding Opportunity-Cost assessment was produced BEFORE a corporate "
               "action was registered, so it was computed against holdings that no longer "
               "describe the current portfolio. Run the Daily Research Cycle to produce a "
               "fresh assessment against the corrected portfolio state.")
    return _read_payload(
        state=assessment.get("assessment_state") or STATE_READY, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art, message=msg,
        policy=assessment.get("policy") or resolve_policy(), assessment=assessment,
        input_contract=art.get("input_contract"), staleness=staleness)


# --------------------------------------------------------------------------- #
# Lightweight summary for the Daily Action Gate (Workstream K)
# --------------------------------------------------------------------------- #
def load_assessment_summary(*, active_book_id: Optional[str] = None,
                            eligible_market_date: Optional[str] = None,
                            artifact: Optional[dict] = None, hoc_dir=None) -> dict:
    """A compact, read-only opportunity-cost summary the Daily Action Gate delegates to.

    Slice 6 (Phase 29G) performance repair — this loader is a PURE ARTIFACT READER.
    It reads ONLY the immutable opportunity-cost artifact index/artifact for the exact
    ``(active_book_id, eligible_market_date)`` context that the Daily Action Gate supplies
    from the authoritative operational state it already owns. It NEVER calls
    ``api.portfolio_state.load_portfolio_state``: that edge closed a circular
    recomposition (``portfolio_state`` composes the gate → the gate delegated to this
    summary → this summary loaded ``portfolio_state`` again → …) that recomputed the
    whole owned-data engine dozens of times per request. The read is bounded: one
    artifact-index lookup and at most one artifact read; no engine run, no live
    assessment, no provider / prediction call, no write. ``available=False`` with zeroed
    counts when no production artifact exists (the sole execution path stays the Daily
    Research Cycle)."""
    zero = {k: 0 for k in RECOMMENDATION_VOCAB}
    try:
        art = artifact if artifact is not None else load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            hoc_dir=hoc_dir)
    except Exception:  # noqa: BLE001 — a pure artifact read must never crash the gate
        return {"opportunity_cost_available": False, "opportunity_cost_assessment_hash": None,
                "opportunity_cost_recommendation_counts": zero,
                "opportunity_cost_replacement_count": 0, "opportunity_cost_exit_count": 0,
                "opportunity_cost_reduce_count": 0, "opportunity_cost_hold_count": 0,
                "opportunity_cost_add_count": 0, "opportunity_cost_data_gaps": [],
                "opportunity_cost_state": STATE_UNAVAILABLE}
    if not art:
        return {"opportunity_cost_available": False, "opportunity_cost_assessment_hash": None,
                "opportunity_cost_recommendation_counts": zero,
                "opportunity_cost_replacement_count": 0, "opportunity_cost_exit_count": 0,
                "opportunity_cost_reduce_count": 0, "opportunity_cost_hold_count": 0,
                "opportunity_cost_add_count": 0, "opportunity_cost_data_gaps": [],
                "opportunity_cost_state": STATE_NOT_RUN}
    a = art.get("assessment") or {}
    counts = a.get("recommendation_counts") or zero
    gaps = (a.get("data_quality") or {}).get("data_gaps") or []
    # Stage 19.1: resolved from the corporate-action registry file alone by the ONE owner,
    # so this stays a PURE artifact reader (no engine run, no provider call, and no edge
    # back into the canonical state composer — the acyclic read contract is preserved).
    _stale = _corporate_action_staleness(artifact=art, book_id=active_book_id)
    return {
        "opportunity_cost_available": True,
        "opportunity_cost_stale": bool(_stale.get("stale")),
        "opportunity_cost_stale_reason": _stale.get("reason"),
        "opportunity_cost_assessment_hash": a.get("assessment_hash"),
        "opportunity_cost_state": a.get("assessment_state"),
        "opportunity_cost_recommendation_counts": counts,
        "opportunity_cost_replacement_count": counts.get("REPLACE", 0),
        "opportunity_cost_exit_count": counts.get("EXIT", 0),
        "opportunity_cost_reduce_count": counts.get("REDUCE", 0),
        "opportunity_cost_hold_count": counts.get("HOLD", 0),
        "opportunity_cost_add_count": counts.get("ADD", 0),
        "opportunity_cost_data_gaps": list(gaps),
        "opportunity_cost_artifact_id": art.get("artifact_id"),
        "opportunity_cost_holding_count": len(a.get("holding_reviews") or []),
    }
