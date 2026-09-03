r"""Stage 20 — Portfolio Reassessment composition, persistence, history & read owner.

This is the ONE canonical orchestration / validation / immutable-artifact /
read-contract owner for the Continuous Active Portfolio Reassessment cycle. It
performs NO calculation of its own — the single canonical portfolio-level
calculation lives in ``engine.portfolio_reassessment``, and every per-holding number
it consumes was calculated exactly once by ``engine.holding_opportunity_cost``
(Slice 6). This module only:

  1. SOURCES an immutable point-in-time reassessment-input contract from the
     authoritative owners:
       * ``api.portfolio_state``            — holdings / weights / NAV / cash / sectors
                                              / corporate-action registry fingerprint;
       * ``api.holding_opportunity_cost``   — the Slice-6 per-holding assessment
                                              (recommendations, replacements, switching
                                              costs, risk contributions, liquidity);
       * ``api.universe_scoring``           — the full-universe ranking snapshot identity
                                              and the frozen champion/model identity;
       * ``api.data_freshness``             — the canonical per-source freshness
                                              semantics (it already declares
                                              ``required_for_portfolio_reassessment``);
       * this module's own immutable history — the recent-change rows the churn /
                                              whipsaw controls consult.
     It reuses the canonical construction constants from ``api.multi_horizon_engine``
     and the transaction-cost constant from ``api.paper_trading_desk`` (never forked).
  2. RUNS the pure kernel.
  3. PERSISTS a completed reassessment as an immutable artifact under a dedicated
     research / decision-evidence root (atomic write, index/manifest, idempotent
     identical rerun, conflicting artifact rejected, interrupted write recoverable). It
     NEVER writes an operational ledger, PostgreSQL, an operational or alpha target, an
     order, a fill, a holding, cash or NAV.
  4. Decides — and ONLY decides — whether the canonical Slice-7 proposal owner should be
     invoked (:func:`should_build_proposal`). It never builds a target itself and never
     approves anything.
  5. Exposes ONE read contract for ``GET /v1/operations/portfolio-reassessment``, a
     compact summary for the workflow-state read, an append-only recommendation HISTORY
     read (Workstream L) and a read-only forward ATTRIBUTION read (Workstream M).

The sole NORMAL execution path is the Daily Research Cycle
(``POST /v1/operations/daily-research-cycle/run``), whose ``REASSESS_PORTFOLIO`` step
runs after a successful signal refresh + Slice-6 assessment. There is deliberately NO
separate manual reassessment execution endpoint and NO scheduler: the reassessment is
SYSTEM ORCHESTRATION, never automatic execution.

Safety boundary (unchanged and enforced here): a reassessment may automatically produce
a REVIEWABLE proposal. It may NOT approve one, confirm an order plan, create an order or
create a fill. The Stage-18 manual approval and the Stage-19 order-plan confirmation
remain mandatory and untouched. While a Stage-19 execution is still pending, the
execution lifecycle keeps operator precedence — a fresh reassessment never overwrites,
obscures or conflicts with it (:func:`execution_precedence`).
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import portfolio_reassessment as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
REASSESSMENT_POLICY_VERSION = kernel.REASSESSMENT_POLICY_VERSION
CHURN_POLICY_VERSION = kernel.CHURN_POLICY_VERSION
REASSESSMENT_STATE_VOCAB = kernel.REASSESSMENT_STATE_VOCAB
COMPOSITION_OWNER = "api.portfolio_reassessment"
CALCULATION_OWNER = kernel.CALCULATION_OWNER
PHASE = "STAGE20"

# Kernel decision states (re-exported).
STATE_NOT_READY = kernel.STATE_NOT_READY
STATE_NO_CHANGE = kernel.STATE_NO_CHANGE
STATE_CHANGE_CANDIDATE = kernel.STATE_CHANGE_CANDIDATE
STATE_PROPOSAL_READY = kernel.STATE_PROPOSAL_READY
STATE_BLOCKED_DATA = kernel.STATE_BLOCKED_DATA
STATE_BLOCKED_EVIDENCE = kernel.STATE_BLOCKED_EVIDENCE
STATE_MANUAL_REVIEW = kernel.STATE_MANUAL_REVIEW

# Read-layer states extend the kernel's decision states.
STATE_NOT_RUN = "NOT_RUN"
STATE_UNAVAILABLE = "UNAVAILABLE"
#: Stage 19.1 semantics carried forward — the persisted reassessment was produced
#: against a DIFFERENT corporate-action registry state than the current portfolio.
STATE_STALE = "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"
READ_STATE_VOCAB = tuple(list(REASSESSMENT_STATE_VOCAB)
                         + [STATE_NOT_RUN, STATE_UNAVAILABLE, STATE_STALE])

# --- immutable artifact root (configurable; a research / decision-evidence root, --- #
# NEVER the operational ledger root). -------------------------------------------- #
REASSESSMENT_DIR_ENV = "PAPER_TRADER_REASSESSMENT_DIR"
_DEFAULT_REASSESSMENT_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_reassessments")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"
_HISTORY_FILE = "recommendation_history.json"

#: Manual policy-override seam. A JSON object in this env var overrides declared
#: thresholds so every Stage-20 threshold stays manually configurable without a code
#: change. Unknown keys are ignored; malformed JSON degrades to the declared policy.
POLICY_OVERRIDE_ENV = "PAPER_TRADER_REASSESSMENT_POLICY"

#: How many prior eligible sessions of change history the churn controls consult. It is
#: derived from the policy (never a second constant): the wider of the cooldown and the
#: reversal lookback.
def _history_window(policy: dict) -> int:
    return max(int(policy["churn_cooldown_trading_days"]),
               int(policy["reversal_lookback_reassessments"])) + 1


# --------------------------------------------------------------------------- #
# Time / IO primitives (mirroring the Slice-6 / Slice-7 owners exactly)
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reassessment_dir(reassessment_dir=None) -> Path:
    if reassessment_dir is not None:
        return Path(reassessment_dir)
    env = os.environ.get(REASSESSMENT_DIR_ENV)
    return Path(env) if env else _DEFAULT_REASSESSMENT_DIR


def _artifacts_dir(reassessment_dir=None) -> Path:
    return _reassessment_dir(reassessment_dir) / _ARTIFACTS_SUBDIR


def _index_path(reassessment_dir=None) -> Path:
    return _reassessment_dir(reassessment_dir) / _INDEX_FILE


def _history_path(reassessment_dir=None) -> Path:
    return _reassessment_dir(reassessment_dir) / _HISTORY_FILE


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True, default=str)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _load_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _f(x: Any) -> Optional[float]:
    return kernel._f(x)   # noqa: SLF001 - the ONE numeric coercion


# --------------------------------------------------------------------------- #
# Policy resolution (reused canonical constants + manual override seam)
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    """Reuse the canonical construction + cost constants and the Slice-6 / Slice-7
    decision thresholds (never forked)."""
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
    try:
        # The PER-NAME hurdles must be the Slice-6 ones, never a Stage-20 copy.
        from paper_trader.engine import holding_opportunity_cost as hoc_kernel
        h = hoc_kernel.default_policy()
        for k in ("min_gross_score_improvement", "min_net_improvement",
                  "score_points_per_cost_bp", "risk_penalty_weight",
                  "deterioration_rank_worsen_threshold"):
            ov[k] = h[k]
    except Exception:  # noqa: BLE001
        pass
    try:
        from paper_trader.engine import reallocation_proposal as rp_kernel
        r = rp_kernel.default_policy()
        for k in ("reduce_fraction", "material_weight_delta"):
            ov[k] = r[k]
    except Exception:  # noqa: BLE001
        pass
    return ov


def _env_policy_overrides() -> dict:
    raw = os.environ.get(POLICY_OVERRIDE_ENV)
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(obj, dict):
        return {}
    allowed = set(kernel.default_policy().keys())
    return {k: v for k, v in obj.items() if k in allowed}


def resolve_policy(policy_overrides: Optional[dict] = None) -> dict:
    pol = dict(kernel.default_policy())
    pol.update(_live_policy_overrides())
    pol.update(_env_policy_overrides())
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


# --------------------------------------------------------------------------- #
# Point-in-time input classification (Workstream F) — built on the CANONICAL
# freshness owner's own per-source verdict; this module invents no cadence rule.
# --------------------------------------------------------------------------- #
#: data_freshness status -> (Stage-20 classification, usage).
_FRESHNESS_MAP = {
    "FRESH": (kernel.FRESH, kernel.USAGE_REFRESHED),
    "NOT_DUE": (kernel.FRESH, kernel.USAGE_REUSED),
    "NOT_APPLICABLE": (kernel.FRESH, kernel.USAGE_REUSED),
    "STALE": (kernel.STALE_BUT_VALID, kernel.USAGE_STALE),
    "MISSING": (kernel.UNAVAILABLE, kernel.USAGE_MISSING),
    "FUTURE_DATED": (kernel.POINT_IN_TIME_GAP, kernel.USAGE_BLOCKED),
    "INCONSISTENT": (kernel.POINT_IN_TIME_GAP, kernel.USAGE_BLOCKED),
    "UNKNOWN": (kernel.UNAVAILABLE, kernel.USAGE_MISSING),
}


def declare_inputs(*, freshness: Optional[dict], eligible: Optional[str]) -> list[dict]:
    """Translate the canonical per-source freshness verdict into the Stage-20
    input-classification rows.

    ``required`` comes from the freshness owner's OWN
    ``required_for_portfolio_reassessment`` flag — Stage 20 never re-decides which
    inputs a reassessment needs. A source whose date equals the eligible session was
    REFRESHED this run; an equally-fresh slower-cadence source is REUSED. Nothing is
    substituted and no current snapshot is back-dated.
    """
    rows: list[dict] = []
    for r in ((freshness or {}).get("source_freshness") or []):
        status = r.get("status") or "UNKNOWN"
        state, usage = _FRESHNESS_MAP.get(status, (kernel.UNAVAILABLE, kernel.USAGE_MISSING))
        as_of = r.get("as_of_date")
        if state == kernel.FRESH and usage == kernel.USAGE_REFRESHED and eligible \
                and as_of != eligible:
            usage = kernel.USAGE_REUSED
        rows.append({
            "source_id": r.get("source_id"),
            "owner": r.get("authoritative_owner"),
            "required": bool(r.get("required_for_portfolio_reassessment")),
            "state": state,
            "usage": usage,
            "as_of_date": as_of,
            "expected_date": r.get("expected_through_date"),
            "cadence": r.get("cadence"),
            "detail": r.get("reason"),
            "source_status": status,
        })
    return rows


# --------------------------------------------------------------------------- #
# Recent-change history (Workstream E churn input + Workstream L evidence)
# --------------------------------------------------------------------------- #
def load_history(*, reassessment_dir=None, active_book_id: Optional[str] = None,
                 limit: Optional[int] = None) -> list[dict]:
    """The append-only recommendation history, oldest-first.

    Each row records what the system RECOMMENDED at one eligible session. It is never
    back-filled and never rewritten: a row is appended exactly once, when the
    reassessment artifact for that (book, session) is first created.
    """
    rows = _load_json(_history_path(reassessment_dir))
    if not isinstance(rows, list):
        return []
    out = [r for r in rows if isinstance(r, dict)
           and (active_book_id is None or r.get("active_book_id") == active_book_id)]
    out.sort(key=lambda r: (r.get("eligible_market_date") or "", r.get("recorded_at") or ""))
    return out[-limit:] if limit else out


def _append_history(row: dict, *, reassessment_dir=None) -> bool:
    """Append ONE immutable history row. Idempotent: a row with the same
    ``reassessment_id`` is never duplicated, and an existing row is never modified."""
    path = _history_path(reassessment_dir)
    rows = _load_json(path)
    if not isinstance(rows, list):
        rows = []
    rid = row.get("reassessment_id")
    if any(isinstance(r, dict) and r.get("reassessment_id") == rid for r in rows):
        return False
    rows.append(row)
    _atomic_write_json(path, rows)
    return True


def _history_row(*, artifact: dict) -> dict:
    """Project the immutable artifact into ONE compact, append-only history row."""
    res = artifact.get("reassessment") or {}
    dec = res.get("decision") or {}
    recs = []
    for a in (res.get("holding_assessments") or []):
        recs.append({
            "ticker": a.get("ticker"),
            "recommendation": a.get("recommendation"),
            "source_recommendation": a.get("source_recommendation"),
            "current_rank": a.get("current_rank"),
            "rank_change": a.get("rank_change"),
            "current_weight": a.get("current_weight"),
            "released_weight": a.get("released_weight"),
            "strongest_replacement_ticker": a.get("strongest_replacement_ticker"),
            "replacement_rank": a.get("replacement_rank"),
            "expected_net_improvement": a.get("expected_net_improvement"),
            "action_withheld": a.get("action_withheld"),
            "withheld_reason_codes": a.get("withheld_reason_codes") or [],
        })
    return {
        "reassessment_id": artifact.get("reassessment_id"),
        "reassessment_hash": res.get("reassessment_hash"),
        "active_book_id": res.get("active_book_id"),
        "eligible_market_date": res.get("eligible_market_date"),
        "recorded_at": artifact.get("generated_at"),
        "decision": res.get("reassessment_state"),
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "actionable_holding_count": dec.get("actionable_holding_count"),
        "blockers": dec.get("blockers") or [],
        "reason_codes": dec.get("reason_codes") or [],
        # Release 47 — held names breaching their own cap. They are the REASON a
        # target is asked for, not a blocker, and they travel with the summary so a
        # downstream surface can name them without re-deriving anything.
        "held_name_constraint_breaches": dec.get(
            "held_name_constraint_breaches") or [],
        "held_name_constraint_breach_effect": dec.get(
            "held_name_constraint_breach_effect"),
        "policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "recommendations": recs,
        "immutable": True,
        "backfilled": False,
    }


def authoritative_history_rows(rows: Optional[list]) -> list[dict]:
    """Collapse the append-only history to ONE authoritative row per (book, session).

    Release 54.2 — a session may now record several immutable assessments. Only the
    LAST one is what the system concluded about that session; the earlier versions
    remain on disk as evidence of how it got there, but they never vote twice. Every
    reader that answers "what did we recommend at session X" must use this, or a
    session that was reassessed three times would count three times.
    """
    latest: dict[tuple, dict] = {}
    for r in (rows or []):
        if not isinstance(r, dict):
            continue
        key = (r.get("active_book_id"), r.get("eligible_market_date"))
        prior = latest.get(key)
        if prior is None or (str(r.get("recorded_at") or ""),
                             str(r.get("reassessment_id") or "")) >= (
                str(prior.get("recorded_at") or ""),
                str(prior.get("reassessment_id") or "")):
            latest[key] = r
    return sorted(latest.values(),
                  key=lambda r: (str(r.get("eligible_market_date") or ""),
                                 str(r.get("recorded_at") or "")))


def recent_change_rows(*, reassessment_dir=None, active_book_id: Optional[str],
                       policy: dict,
                       exclude_eligible_market_date: Optional[str] = None) -> list[dict]:
    """The churn-control input: which names actually changed, and in which direction,
    over the recent eligible sessions. Derived ONLY from the immutable history (never
    from a live desk read), so the churn verdict is reproducible from evidence.

    Release 54.2 — two rules keep this correct once a session can hold several
    assessments:
      * only the AUTHORITATIVE row of each session votes, so a recommendation that
        a later version of the same session superseded cannot protect a name; and
      * the session BEING assessed is excluded. A reassessment has never seen its
        own recommendation (its row is written afterwards), and letting version 2
        read version 1's row would make the cooldown self-blocking — the system
        would be structurally unable to repeat at 11:10 what it concluded at 09:45.
    """
    window = _history_window(policy)
    hist = authoritative_history_rows(
        load_history(reassessment_dir=reassessment_dir, active_book_id=active_book_id))
    if exclude_eligible_market_date:
        hist = [r for r in hist
                if r.get("eligible_market_date") != exclude_eligible_market_date]
    sessions = sorted({r.get("eligible_market_date") for r in hist
                       if r.get("eligible_market_date")})[-window:]
    keep = set(sessions)
    out: list[dict] = []
    for r in hist:
        d = r.get("eligible_market_date")
        if d not in keep:
            continue
        for rec in (r.get("recommendations") or []):
            action = rec.get("recommendation")
            if action in kernel.ACTIONABLE_RECOMMENDATIONS:
                out.append({"eligible_market_date": d, "ticker": rec.get("ticker"),
                            "direction": "OUT", "source": "reassessment",
                            "recommendation": action})
            elif action == kernel.REC_ADD:
                out.append({"eligible_market_date": d, "ticker": rec.get("ticker"),
                            "direction": "IN", "source": "reassessment",
                            "recommendation": action})
    return out


# --------------------------------------------------------------------------- #
# Input-contract sourcing (Workstream B). Every source is injectable for tests.
# --------------------------------------------------------------------------- #
def _corporate_actions_hash(ps: Optional[dict]) -> Optional[str]:
    return ((ps or {}).get("corporate_actions") or {}).get("registry_fingerprint")


def _model_identity(sc: dict) -> dict:
    """The champion / model identity bound into the reassessment. Sourced from the ONE
    scoring owner; automatic promotion is impossible from here."""
    return {
        "strategy_id": sc.get("strategy_id"),
        "strategy_version": sc.get("strategy_version"),
        "primary_model_id": sc.get("primary_model_id"),
        "champion_model_id": sc.get("champion_model_id"),
        "model_registry_version": sc.get("model_registry_version"),
        "universe_id": sc.get("universe_id"),
        "automatic_promotion_allowed": False,
        "owner": "api.universe_scoring",
    }


def _current_portfolio_score(reviews: list) -> Optional[float]:
    """The capital-weighted current signal score of the book. It is the SAME weighted
    percentile the Slice-7 signal block computes for the 'before' portfolio, so the two
    artifacts agree by construction."""
    tot = 0.0
    acc = 0.0
    any_score = False
    for r in reviews:
        w = _f(r.get("current_weight")) or 0.0
        if w <= 0:
            continue
        tot += w
        s = _f(r.get("current_score"))
        if s is not None:
            any_score = True
            acc += w * s
    if tot <= 0 or not any_score:
        return None
    return round(acc / tot, 6)


def build_input_contract(*, portfolio_state: dict, scoring: dict, hoc_assessment: dict,
                         freshness: Optional[dict] = None,
                         recent_change_history: Optional[list] = None,
                         corporate_action_stale: Optional[dict] = None,
                         hoc_binding: Optional[dict] = None,
                         policy: Optional[dict] = None) -> dict:
    """Assemble the immutable point-in-time reassessment-input contract.

    Everything is sourced as of the portfolio-state eligible market date. No expected
    return is ever synthesised, no rank is recomputed and no switching cost is re-derived
    — the per-holding analytics come verbatim from the Slice-6 assessment.

    Release 54.3 — ``hoc_binding`` is the opportunity-cost owner's OWN statement of
    which immutable artifact this assessment became (see
    ``api.holding_opportunity_cost.artifact_binding``). It is recorded verbatim and
    never inferred: when the caller supplies none, the binding fields resolve to
    ``None`` / ``False`` and the reassessment honestly records that it cannot prove
    its dependency was persisted.
    """
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    sc = scoring or {}
    hoc = hoc_assessment or {}
    hoc_binding = dict(hoc_binding or {})
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    active_book = ps.get("active_book") or {}
    reviews = list(hoc.get("holding_reviews") or [])
    stale = corporate_action_stale or {}

    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": active_book.get("book_id"),
        "active_book_label": active_book.get("book_label"),
        "valuation_date": (ps.get("dates") or {}).get("valuation_date"),
        "nav": (ps.get("capital") or {}).get("nav"),
        "cash": (ps.get("capital") or {}).get("cash"),
        # --- bound identities ------------------------------------------------ #
        "portfolio_state_hash": ps.get("state_hash"),
        # Stage 21 (Workstream 0E): the ECONOMIC fingerprints the currency check binds
        # to. ``portfolio_state_hash`` is retained for continuity/audit only — it embeds
        # the HOC assessment's own output (via api.daily_action_gate), so comparing it
        # against the HOC's recorded value invalidated every FRESH assessment.
        "economic_state_hash": ps.get("economic_state_hash"),
        "economic_identity_version": ps.get("economic_identity_version"),
        "hoc_economic_state_hash": ((hoc.get("provenance") or {})
                                    .get("economic_state_hash")),
        "corporate_actions_hash": _corporate_actions_hash(ps),
        "corporate_action_stale": bool(stale.get("stale")),
        "corporate_action_stale_reason": stale.get("reason"),
        "corporate_action_staleness_verifiable": bool(stale.get("verifiable", True)),
        "universe_scoring_hash": sc.get("output_hash"),
        "universe_input_contract_hash": sc.get("input_contract_hash"),
        "model_identity": _model_identity(sc),
        "hoc_assessment_hash": hoc.get("assessment_hash"),
        "hoc_assessment_state": hoc.get("assessment_state"),
        "hoc_eligible_market_date": hoc.get("eligible_market_date"),
        "hoc_portfolio_state_hash": ((hoc.get("provenance") or {}).get("portfolio_state_hash")),
        "hoc_decision_policy_version": (hoc.get("policy") or {}).get("policy_version"),
        # Release 54.3 — WHICH IMMUTABLE HOC ARTIFACT this reassessment depends on,
        # copied verbatim from the opportunity-cost owner's own binding block. Before
        # R54.3 only the hash above was recorded, and a same-session assessment whose
        # write had been refused left that hash naming nothing retrievable: the
        # reassessment persisted a dependency that could never be produced as
        # evidence. ``hoc_persisted`` is the load-bearing field — a refused write
        # stays visible AS a refused write and is never repaired here.
        "hoc_artifact_id": hoc_binding.get("hoc_artifact_id"),
        "hoc_assessment_evidence_hash": hoc_binding.get(
            "hoc_assessment_evidence_hash"),
        "hoc_decision_fingerprint": hoc_binding.get("hoc_decision_fingerprint"),
        "hoc_persistence_status": hoc_binding.get("hoc_persistence_status"),
        "hoc_persisted": hoc_binding.get("hoc_persisted"),
        "hoc_binding_owner": hoc_binding.get("hoc_owner"),
        "hoc_data_gaps": ((hoc.get("data_quality") or {}).get("data_gaps")) or [],
        "hoc_recommendation_counts": hoc.get("recommendation_counts") or {},
        "allocation_policy_version": _allocation_policy_version(),
        "reassessment_policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "holdings_snapshot_hash": kernel.holdings_snapshot_hash(reviews),
        # --- the analytics (never recomputed) -------------------------------- #
        "holding_reviews": reviews,
        "addition_candidates": list(hoc.get("addition_candidates") or []),
        "portfolio_summary": hoc.get("portfolio_summary") or {},
        "current_portfolio_score": _current_portfolio_score(reviews),
        "drawdown_context": ((ps.get("capital") or {}).get("max_drawdown")
                             if isinstance(ps.get("capital"), dict) else None),
        "eligible_universe_size": ((hoc.get("diagnostics") or {}).get("eligible_universe_size")),
        # --- point-in-time / churn inputs ------------------------------------ #
        "inputs": declare_inputs(freshness=freshness, eligible=eligible),
        "recent_change_history": list(recent_change_history or []),
        "inputs_as_of_eligible_date": eligible,
    }


def _allocation_policy_version() -> Optional[str]:
    try:
        from paper_trader.engine import reallocation_proposal as rp_kernel
        return rp_kernel.ALLOCATION_POLICY_VERSION
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Default source loaders (injectable seams)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader() -> dict:
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_freshness_loader() -> Optional[dict]:
    from paper_trader.api import data_freshness as df
    return df.load_data_freshness()


def _default_hoc_assessment_loader(*, active_book_id, eligible_market_date,
                                   hoc_dir=None) -> dict:
    """The latest persisted Slice-6 assessment for the active book + eligible date."""
    from paper_trader.api import holding_opportunity_cost as hoc
    art = hoc.load_latest_artifact(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date,
                                   hoc_dir=hoc_dir)
    return (art or {}).get("assessment") or {}


def resolve_hoc_binding(*, hoc_assessment: Optional[dict],
                        active_book_id: Optional[str] = None,
                        eligible_market_date: Optional[str] = None,
                        hoc_dir=None) -> dict:
    """Release 54.3 — which IMMUTABLE opportunity-cost artifact is this assessment?

    Delegates wholly to the opportunity-cost owner: this module reads the store
    through that owner's own by-id lookup and composes nothing of its own. The
    binding is accepted ONLY when the retrieved artifact's ``assessment_hash``
    equals the hash of the assessment actually consumed — which is precisely the
    R54.3 failure mode. Before R54.3 a same-session assessment whose write had
    been refused still reached the kernel, and the reassessment recorded its
    transient hash as though it named durable evidence; now the mismatch is
    reported as ``hoc_persisted: False`` with the artifact that WAS found named,
    so the governance gate can fail closed on a fact instead of an assumption.
    """
    used_hash = (hoc_assessment or {}).get("assessment_hash")
    try:
        from paper_trader.api import holding_opportunity_cost as hocm
        art = hocm.load_latest_artifact(active_book_id=active_book_id,
                                        eligible_market_date=eligible_market_date,
                                        hoc_dir=hoc_dir)
        stored = ((art or {}).get("identity") or {}).get("assessment_hash")
        if art is not None and used_hash and stored and str(used_hash) == str(stored):
            return hocm.resolve_binding(binding=hocm.artifact_binding(artifact=art),
                                        active_book_id=active_book_id,
                                        eligible_market_date=eligible_market_date,
                                        hoc_dir=hoc_dir)
        return {
            "schema_version": hocm.BINDING_SCHEMA_VERSION,
            "hoc_owner": hocm.COMPOSITION_OWNER,
            "hoc_artifact_id": None,
            "hoc_assessment_hash": used_hash,
            "hoc_assessment_evidence_hash": None,
            "hoc_decision_fingerprint": None,
            "hoc_eligible_market_date": eligible_market_date,
            "hoc_active_book_id": active_book_id,
            "hoc_persistence_status": None,
            "hoc_persisted": False,
            "hoc_artifact_retrievable": False,
            "hoc_artifact_identity_matches": False,
            "hoc_latest_stored_assessment_hash": stored,
            "hoc_binding_detail": (
                "no opportunity-cost artifact is persisted for this book and session"
                if art is None else
                "the assessment consumed (%s) is NOT the persisted artifact (%s); its "
                "evidence exists only transiently" % (str(used_hash)[:16],
                                                      str(stored)[:16])),
        }
    except Exception as exc:  # noqa: BLE001 — a binding read never crashes a run
        return {"hoc_artifact_id": None, "hoc_assessment_hash": used_hash,
                "hoc_persisted": False, "hoc_artifact_retrievable": False,
                "hoc_artifact_identity_matches": False,
                "hoc_binding_detail": "binding unresolvable: %s" % str(exc)[:120]}


def hoc_corporate_actions_hash(hoc_assessment: Optional[dict]) -> Optional[str]:
    """The ONE canonical way to resolve the corporate-action registry fingerprint a
    Slice-6 assessment was computed against (Stage 21, Workstream 0E).

    Before Stage 21 the kernel's ``provenance`` block recorded no corporate-action
    fingerprint at all, so every consumer resolved ``None``. ``staleness_vs_registry``
    then treats ``None`` as "bound to the EMPTY registry", which — with the MNST split
    registered — made EVERY reassessment permanently STALE_CORPORATE_ACTION_EVIDENCE,
    no matter how fresh. That is a structural false positive, not a staleness signal.

    Resolution order (first hit wins), so a legacy artifact still resolves whatever it
    genuinely recorded rather than silently defaulting to the empty registry:
      1. ``provenance.corporate_actions_hash``  (Stage 21 kernel)
      2. ``identity.corporate_actions_hash``    (artifact identity, Stage 19.1)
      3. ``input_contract.corporate_actions_hash``
    Returns ``None`` only when the assessment genuinely recorded nothing.
    """
    a = hoc_assessment or {}
    for block in ("provenance", "identity", "input_contract"):
        val = (a.get(block) or {}).get("corporate_actions_hash")
        if val:
            return val
    return None


def _default_corporate_action_staleness(*, hoc_assessment: dict, portfolio_state: dict,
                                        active_book_id) -> dict:
    """Pure delegation to ``api.corporate_actions`` — this module owns no split
    arithmetic and no registry logic.

    Stage 21 (Workstream 0E): when the assessment recorded NO fingerprint the answer is
    UNVERIFIABLE, never STALE. Claiming staleness from missing evidence is fabrication:
    it blocked fresh assessments permanently while telling the operator a corporate
    action had been registered "since" an assessment that in fact post-dated it.
    """
    try:
        from paper_trader.api import corporate_actions as ca
        bound = hoc_corporate_actions_hash(hoc_assessment)
        cur = ((portfolio_state or {}).get("corporate_actions") or {})
        current_fp = cur.get("registry_fingerprint")
        if current_fp is not None:
            actions = cur.get("actions") or []
            current = {"fingerprint": current_fp, "n_registered": len(actions),
                       "actions": actions}
        else:
            current = None
        if not bound:
            # No recorded binding -> we cannot prove staleness OR currency. Report it
            # honestly and let the ECONOMIC fingerprint (which contains the registry
            # fingerprint) carry the currency decision.
            fp = current or ca.registry_fingerprint(book_id=active_book_id)
            return {"stale": False, "verifiable": False,
                    "reason": None,
                    "bound_corporate_actions_hash": None,
                    "current_corporate_actions_hash": fp.get("fingerprint"),
                    "n_registered_now": fp.get("n_registered"),
                    "unverifiable_reason": "ASSESSMENT_RECORDED_NO_CORPORATE_ACTION_FINGERPRINT",
                    "owner": ca.OWNER,
                    "message": ("This assessment recorded no corporate-action registry "
                                "fingerprint, so corporate-action staleness cannot be "
                                "proven either way. Currency is decided by the economic "
                                "portfolio fingerprint, which contains the registry "
                                "state. Nothing is inferred from the missing value.")}
        out = (ca.staleness_vs_registry(bound, current=current) if current is not None
               else ca.staleness_vs_registry(bound, book_id=active_book_id))
        return {**out, "verifiable": True}
    except Exception:  # noqa: BLE001 - never crash a read
        return {"stale": False, "verifiable": False, "reason": None}


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_reassessment(*, input_contract: Optional[dict] = None,
                     portfolio_state: Optional[dict] = None,
                     scoring: Optional[dict] = None,
                     hoc_assessment: Optional[dict] = None,
                     freshness: Optional[dict] = None,
                     recent_change_history: Optional[list] = None,
                     policy: Optional[dict] = None,
                     hoc_dir=None, reassessment_dir=None,
                     hoc_binding: Optional[dict] = None,
                     portfolio_state_loader: Optional[Callable] = None,
                     scoring_loader: Optional[Callable] = None,
                     freshness_loader: Optional[Callable] = None,
                     hoc_assessment_loader: Optional[Callable] = None) -> dict:
    """Build the immutable input contract and run the pure kernel. Writes nothing."""
    pol = resolve_policy(policy)
    if input_contract is None:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
        sc = scoring if scoring is not None else (
            (scoring_loader or _default_scoring_loader)())
        fr = freshness if freshness is not None else _safe_call(
            freshness_loader or _default_freshness_loader)
        book_id = ((ps or {}).get("active_book") or {}).get("book_id")
        eligible = ((ps or {}).get("dates") or {}).get("eligible_market_date")
        hoc = hoc_assessment if hoc_assessment is not None else (
            (hoc_assessment_loader or _default_hoc_assessment_loader)(
                active_book_id=book_id, eligible_market_date=eligible, hoc_dir=hoc_dir))
        stale = _default_corporate_action_staleness(
            hoc_assessment=hoc or {}, portfolio_state=ps or {}, active_book_id=book_id)
        hist = recent_change_history if recent_change_history is not None else \
            recent_change_rows(reassessment_dir=reassessment_dir,
                               active_book_id=book_id, policy=pol,
                               exclude_eligible_market_date=eligible)
        # R54.3 — the EXACT immutable opportunity-cost artifact this reassessment
        # depends on. Supplied by the caller that just persisted it (the event
        # cycle / the governed daily cycle), else resolved from the store through
        # the opportunity-cost owner's own by-id lookup.
        if hoc_binding is None:
            hoc_binding = resolve_hoc_binding(
                hoc_assessment=hoc or {}, active_book_id=book_id,
                eligible_market_date=eligible, hoc_dir=hoc_dir)
        input_contract = build_input_contract(
            portfolio_state=ps or {}, scoring=sc or {}, hoc_assessment=hoc or {},
            freshness=fr, recent_change_history=hist, corporate_action_stale=stale,
            hoc_binding=hoc_binding, policy=pol)
    result = kernel.build_reassessment(input_contract=input_contract, policy=pol)
    return {"input_contract": input_contract, "reassessment": result}


def _safe_call(fn):
    try:
        return fn()
    except Exception:  # noqa: BLE001 - a missing optional source degrades, never crashes
        return None


# --------------------------------------------------------------------------- #
# Release 54.2 — ASSESSMENT EVIDENCE IDENTITY
#
# Stage 20 asked ONE identity question — "is this the same reassessment?" — and
# answered it with ``reassessment_hash``. Stage 21 split off the ECONOMIC question
# — "does the prior artifact still describe the portfolio?" — and answered it with
# ``economic_state_hash`` after ``portfolio_state_hash`` proved unusable for it (the
# document-wide hash embeds this owner's own output, so every downstream research
# write invalidated its own input).
#
# R54.2 adds the third question, which only continuous intraday management makes
# load-bearing: "is this the same ASSESSMENT of that portfolio?". The book can be
# economically identical all session — same holdings, same cash, same NAV — while
# the evidence behind the investment conclusion moves: a new ranking, a new
# opportunity-cost assessment, a changed input-freshness picture. That is a NEW
# point-in-time conclusion about an unchanged portfolio, and it needs its own
# immutable artifact rather than being refused as a conflict.
#
# The identity binds CANONICAL BOUND EVIDENCE ONLY. It deliberately EXCLUDES:
#   * ``portfolio_state_hash`` — the Stage-21 trap named above;
#   * ``economic_state_hash``  — that is the OTHER axis, compared separately;
#   * ``reassessment_hash``    — the CONCLUSION, not the evidence that produced it;
#   * wall clock, run id, materiality trigger fingerprint — provenance, never
#     identity. Two triggers reaching the same conclusion from the same evidence
#     are ONE assessment; versioning them twice would be evidence noise, which is
#     exactly what a poll-driven cycle would generate.
# --------------------------------------------------------------------------- #
ASSESSMENT_EVIDENCE_IDENTITY_VERSION = "reassessment_assessment_evidence_identity.v1"

#: The bound-evidence components that make one assessment of an unchanged portfolio
#: materially different from another. Every one of them is already published by the
#: Stage-20 input contract — R54.2 introduces no new evidence source.
ASSESSMENT_EVIDENCE_COMPONENTS = (
    "universe_scoring_hash",
    "universe_input_contract_hash",
    "hoc_assessment_hash",
    "hoc_decision_policy_version",
    "corporate_actions_hash",
    "holdings_snapshot_hash",
    "model_identity",
    "allocation_policy_version",
    "reassessment_policy_version",
    "churn_policy_version",
    "declared_inputs_fingerprint",
)


def declared_inputs_fingerprint(input_contract: Optional[dict]) -> Optional[str]:
    """The freshness picture this assessment actually saw, as ONE fingerprint.

    A required source moving FRESH -> STALE changes what the assessment is entitled
    to conclude, so it is evidence. The declaration rows are already point-in-time
    (:func:`declare_inputs`); nothing here re-decides freshness.
    """
    rows = (input_contract or {}).get("inputs")
    if not rows:
        return None
    return kernel.stable_hash([
        {"source_id": r.get("source_id"), "state": r.get("state"),
         "usage": r.get("usage"), "as_of_date": r.get("as_of_date"),
         "required": bool(r.get("required"))}
        for r in sorted(rows, key=lambda r: str(r.get("source_id") or ""))])


def assessment_evidence_identity(*, input_contract: Optional[dict]) -> dict:
    """The evidence identity of ONE reassessment. Accepts a full OR a compacted
    input contract — both publish the same component names, so a historical
    artifact stays comparable without being rewritten."""
    ic = input_contract or {}
    out: dict[str, Any] = {}
    for k in ASSESSMENT_EVIDENCE_COMPONENTS:
        if k == "declared_inputs_fingerprint":
            out[k] = declared_inputs_fingerprint(ic)
        elif k == "reassessment_policy_version":
            out[k] = REASSESSMENT_POLICY_VERSION
        elif k == "churn_policy_version":
            out[k] = CHURN_POLICY_VERSION
        else:
            out[k] = ic.get(k)
    return out


def assessment_evidence_hash(evidence: Optional[dict]) -> str:
    ev = evidence or {}
    return kernel.stable_hash({"schema": ASSESSMENT_EVIDENCE_IDENTITY_VERSION,
                               **{k: ev.get(k) for k in ASSESSMENT_EVIDENCE_COMPONENTS}})


def decision_fingerprint(result: Optional[dict]) -> Optional[str]:
    """The CONCLUSION alone, stripped of provenance.

    ``reassessment_hash`` covers the whole result INCLUDING ``provenance``, which
    carries ``portfolio_state_hash`` — so two runs can differ in that hash while
    having reached an identical conclusion from identical evidence. Comparing the
    conclusion directly is what separates "the same assessment, re-run" from
    "identical evidence produced a DIFFERENT answer", and only the second is a
    genuine conflict.
    """
    res = result or {}
    if not res:
        return None
    return kernel.stable_hash({k: v for k, v in res.items()
                               if k not in ("provenance", "reassessment_hash")})


# --------------------------------------------------------------------------- #
# Identity + persistence (Workstream B / G)
# --------------------------------------------------------------------------- #
def artifact_identity(*, input_contract: dict, result: dict) -> dict:
    """The deterministic identity of ONE reassessment run.

    A repeated evaluation of the EXACT same state produces the exact same identity (so
    nothing duplicates); a change in ANY of the bound components — active book, eligible
    date, ranking snapshot, holdings snapshot, model/champion identity, corporate-action
    registry, opportunity-cost policy version, reassessment/churn policy version or the
    proposal allocation policy version — produces a NEW identity.
    """
    return {
        "active_book_id": input_contract.get("active_book_id"),
        "eligible_market_date": input_contract.get("eligible_market_date"),
        "universe_scoring_hash": input_contract.get("universe_scoring_hash"),
        "universe_input_contract_hash": input_contract.get("universe_input_contract_hash"),
        "portfolio_state_hash": input_contract.get("portfolio_state_hash"),
        # Stage 21 (Workstream 0E) — the ECONOMIC identity this reassessment describes.
        # Stage 21 outcome evidence binds to THIS, never to the document-wide hash.
        "economic_state_hash": input_contract.get("economic_state_hash"),
        "corporate_actions_hash": input_contract.get("corporate_actions_hash"),
        "holdings_snapshot_hash": input_contract.get("holdings_snapshot_hash"),
        "hoc_assessment_hash": input_contract.get("hoc_assessment_hash"),
        # Release 54.3 — the EXACT persisted opportunity-cost version this
        # reassessment binds. It is recorded in the identity (so an auditor can
        # retrieve the dependency) but deliberately NOT in
        # ``ASSESSMENT_EVIDENCE_COMPONENTS``: the artifact id embeds
        # ``hoc_assessment_hash``, which is already an evidence component, so
        # binding it twice would version the same fact twice.
        "hoc_artifact_id": input_contract.get("hoc_artifact_id"),
        "hoc_assessment_evidence_hash": input_contract.get(
            "hoc_assessment_evidence_hash"),
        "hoc_persisted": input_contract.get("hoc_persisted"),
        "model_identity": input_contract.get("model_identity") or {},
        "hoc_decision_policy_version": input_contract.get("hoc_decision_policy_version"),
        "allocation_policy_version": input_contract.get("allocation_policy_version"),
        "reassessment_policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "reassessment_hash": result.get("reassessment_hash"),
        # Release 54.2 — the two identities that decide same-session versioning.
        # ``assessment_evidence_hash`` answers "is this the same ASSESSMENT?" and
        # ``decision_fingerprint`` answers "is this the same CONCLUSION?"; the
        # economic axis stays ``economic_state_hash`` above.
        "assessment_evidence_identity_version": ASSESSMENT_EVIDENCE_IDENTITY_VERSION,
        "assessment_evidence_hash": assessment_evidence_hash(
            assessment_evidence_identity(input_contract=input_contract)),
        "decision_fingerprint": decision_fingerprint(result),
    }


def artifact_id_for(identity: dict) -> str:
    book = identity.get("active_book_id") or "book"
    date = identity.get("eligible_market_date") or "nodate"
    h = (identity.get("reassessment_hash") or "")[:12]
    return "prs_%s_%s_%s" % (date, book, h)


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
        # Stage 21 (Workstream 0E) — persist BOTH sides of the currency comparison so a
        # later audit can prove exactly why an assessment was (or was not) blocked.
        "economic_state_hash": ic.get("economic_state_hash"),
        "economic_identity_version": ic.get("economic_identity_version"),
        "hoc_economic_state_hash": ic.get("hoc_economic_state_hash"),
        "hoc_portfolio_state_hash": ic.get("hoc_portfolio_state_hash"),
        "hoc_eligible_market_date": ic.get("hoc_eligible_market_date"),
        "corporate_action_stale": ic.get("corporate_action_stale"),
        "corporate_action_stale_reason": ic.get("corporate_action_stale_reason"),
        "corporate_action_staleness_verifiable":
            ic.get("corporate_action_staleness_verifiable"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "holdings_snapshot_hash": ic.get("holdings_snapshot_hash"),
        "hoc_assessment_hash": ic.get("hoc_assessment_hash"),
        "hoc_assessment_state": ic.get("hoc_assessment_state"),
        "hoc_decision_policy_version": ic.get("hoc_decision_policy_version"),
        # Release 54.3 — the EXACT immutable opportunity-cost artifact this
        # reassessment depends on, persisted so the dependency stays provable long
        # after the run that produced it.
        "hoc_artifact_id": ic.get("hoc_artifact_id"),
        "hoc_assessment_evidence_hash": ic.get("hoc_assessment_evidence_hash"),
        "hoc_decision_fingerprint": ic.get("hoc_decision_fingerprint"),
        "hoc_persistence_status": ic.get("hoc_persistence_status"),
        "hoc_persisted": ic.get("hoc_persisted"),
        "allocation_policy_version": ic.get("allocation_policy_version"),
        "reassessment_policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "model_identity": ic.get("model_identity") or {},
        "holdings_count": len(ic.get("holding_reviews") or []),
        "addition_candidate_count": len(ic.get("addition_candidates") or []),
        "recent_change_history_rows": len(ic.get("recent_change_history") or []),
        "inputs": ic.get("inputs") or [],
        "inputs_as_of_eligible_date": ic.get("inputs_as_of_eligible_date"),
    }


#: Release 54.2 — the persistence outcomes, named once. ``CREATED_ASSESSMENT_VERSION``
#: is the new one: the SAME economic portfolio, assessed again from materially
#: different evidence.
PERSIST_CREATED = "CREATED"
PERSIST_REUSED = "REUSED_EXISTING"
PERSIST_ECONOMIC_VERSION = "CREATED_NEW_VERSION"
PERSIST_ASSESSMENT_VERSION = "CREATED_ASSESSMENT_VERSION"
PERSIST_CONFLICT = "CONFLICT_REJECTED"
PERSIST_INCONSISTENT = "REJECTED_INCONSISTENT_IDENTITY"


def _existing_assessment_identity(existing: Optional[dict],
                                  reassessment_dir=None) -> tuple:
    """``(assessment_evidence_hash, decision_fingerprint)`` of the indexed artifact.

    An index entry written before R54.2 carries neither. Rather than reinterpreting
    such an artifact — or rewriting it — both values are RECOMPUTED from what it
    already persisted: its own identity, its own compacted input contract and its
    own result. A historical artifact therefore becomes comparable without a single
    byte of it changing.
    """
    if not existing:
        return None, None
    ev_hash = existing.get("assessment_evidence_hash")
    fingerprint = existing.get("decision_fingerprint")
    if ev_hash and fingerprint:
        return ev_hash, fingerprint
    art = _read_indexed_artifact(existing, reassessment_dir) or {}
    ident = art.get("identity") or {}
    if not ev_hash:
        ev_hash = ident.get("assessment_evidence_hash")
    if not ev_hash:
        ic = art.get("input_contract") or {}
        if ic or ident:
            merged = {**ident, **{k: v for k, v in ic.items() if v is not None}}
            ev_hash = assessment_evidence_hash(
                assessment_evidence_identity(input_contract=merged))
    if not fingerprint:
        fingerprint = (ident.get("decision_fingerprint")
                       or decision_fingerprint(art.get("reassessment")))
    return ev_hash, fingerprint


def _session_identity_conflicts(*, identity: dict, input_contract: dict,
                                result: dict) -> list[str]:
    """Point-in-time self-consistency (R54.2 Phase J). An artifact whose own parts
    disagree about WHICH session or WHICH book it describes is impossible evidence
    and is never written — versioning does not relax a single point-in-time rule."""
    ic = input_contract or {}
    res = result or {}
    elig = identity.get("eligible_market_date")
    book = identity.get("active_book_id")
    out: list[str] = []

    def _clash(label, a, b):
        if a is not None and b is not None and a != b:
            out.append("%s (%s != %s)" % (label, a, b))

    _clash("IDENTITY_VS_RESULT_SESSION", elig, res.get("eligible_market_date"))
    _clash("IDENTITY_VS_CONTRACT_SESSION", elig, ic.get("eligible_market_date"))
    _clash("IDENTITY_VS_RESULT_BOOK", book, res.get("active_book_id"))
    _clash("IDENTITY_VS_CONTRACT_BOOK", book, ic.get("active_book_id"))
    _clash("ASSESSMENT_SESSION", elig, ic.get("hoc_eligible_market_date"))
    _clash("INPUT_AS_OF_SESSION", elig, ic.get("inputs_as_of_eligible_date"))
    return out


def _unique_artifact_id(aid: str, identity: dict, reassessment_dir=None) -> str:
    """Never let a new VERSION land on an existing artifact's path.

    ``artifact_id_for`` embeds ``reassessment_hash``, so a collision means the two
    versions reached an identical conclusion — but if their identities differ the
    older file must still not be rewritten. Immutability is enforced here rather
    than assumed from the id scheme.
    """
    path = _artifacts_dir(reassessment_dir) / ("%s.json" % aid)
    if not path.exists():
        return aid
    prior = _load_json(path) or {}
    if (prior.get("identity") or {}) == identity:
        return aid
    return "%s_%s" % (aid, (identity.get("assessment_evidence_hash") or "v")[:8])


def persist_reassessment(*, result: dict, input_contract: dict, reassessment_dir=None,
                         now: Optional[datetime] = None) -> dict:
    """Persist a completed reassessment as an immutable artifact + ONE history row.

    Release 54.2 — FOUR outcomes, decided on two independent axes (the ECONOMIC
    portfolio, and the ASSESSMENT EVIDENCE about it):

      1. same economic state + same evidence + same conclusion
         -> ``REUSED_EXISTING``. Idempotent: no second artifact, no second history
            row. Re-running a cycle from unchanged evidence is not a new decision.
      2. same economic state + materially DIFFERENT assessment evidence
         -> ``CREATED_ASSESSMENT_VERSION``. A NEW immutable version is APPENDED.
            The portfolio being economically unchanged does not mean the investment
            assessment is unchanged, and refusing this is what stranded continuous
            intraday management on the first artifact of the session.
      3. the ECONOMIC state itself changed (holdings / cash / NAV / corporate
         actions) -> ``CREATED_NEW_VERSION``. Stage 21 behaviour, preserved exactly.
      4. same economic state + same evidence + a DIFFERENT conclusion
         -> ``CONFLICT_REJECTED``. Identical evidence that yields a different answer
            is not a new assessment, it is an inconsistency; the immutable artifact
            is never overwritten and the caller must resolve it.

    An artifact whose own identity disagrees about the session or the book is
    refused outright (``REJECTED_INCONSISTENT_IDENTITY``). NOT_READY runs are not
    persisted (there is nothing durable to record). No artifact is EVER rewritten,
    in any outcome.
    """
    state = result.get("reassessment_state")
    if state not in kernel.PERSISTABLE_STATES:
        return {"status": "NOT_PERSISTED", "reason": "STATE_%s_NOT_PERSISTABLE" % state,
                "artifact_id": None, "persisted": False, "reused": False,
                "conflict": False, "history_appended": False}

    identity = artifact_identity(input_contract=input_contract, result=result)
    conflicts = _session_identity_conflicts(identity=identity,
                                            input_contract=input_contract,
                                            result=result)
    if conflicts:
        return {"status": PERSIST_INCONSISTENT, "artifact_id": None,
                "persisted": False, "reused": False, "conflict": True,
                "history_appended": False, "economic_state_changed": False,
                "assessment_evidence_changed": False,
                "identity_conflicts": conflicts,
                "reason": "The reassessment's own parts disagree about the session "
                          "or the book it describes: %s. Impossible evidence is "
                          "never persisted." % "; ".join(conflicts),
                "identity": identity}
    aid = artifact_id_for(identity)
    key = _index_key(identity["active_book_id"], identity["eligible_market_date"])
    index = _load_json(_index_path(reassessment_dir)) or {}
    if not isinstance(index, dict):
        index = {}
    existing = index.get(key)

    # Stage 21 (Workstream 0E, requirement 4). Two DIFFERENT situations were previously
    # collapsed into one CONFLICT_REJECTED, which left the index pointing forever at the
    # first artifact of the session:
    #
    #   (a) SAME economic state, different research inputs -> the prior artifact still
    #       describes the portfolio. Immutability wins: reject, never overwrite. This is
    #       the protection Stage 20 shipped and it is preserved exactly.
    #   (b) The economic state itself CHANGED (holdings / cash / NAV / corporate actions)
    #       -> the prior artifact provably no longer describes the portfolio. Rejecting
    #       here strands the operator on stale evidence for the rest of the session with
    #       no way to reach the current-state assessment. A NEW VERSION is appended; the
    #       prior artifact file is still never rewritten, so nothing is lost.
    #     (c) Release 54.2 — the economic state is unchanged but the ASSESSMENT
    #         EVIDENCE moved (new ranking, new opportunity cost, changed input
    #         freshness). The prior artifact still describes the PORTFOLIO and is
    #         still true about the evidence IT saw, but it is no longer the current
    #         investment conclusion. A NEW VERSION is appended; the prior artifact
    #         file is never rewritten, so both point-in-time conclusions survive.
    new_econ = identity.get("economic_state_hash")
    prior_econ = existing.get("economic_state_hash") if existing else None
    economic_state_changed = bool(existing and new_econ and prior_econ
                                  and new_econ != prior_econ)

    prior_evidence_hash, prior_fingerprint = _existing_assessment_identity(
        existing, reassessment_dir)
    new_evidence_hash = identity.get("assessment_evidence_hash")
    new_fingerprint = identity.get("decision_fingerprint")
    assessment_evidence_changed = bool(
        existing and not economic_state_changed
        and new_evidence_hash and prior_evidence_hash
        and new_evidence_hash != prior_evidence_hash)

    if existing and not economic_state_changed and not assessment_evidence_changed:
        # Same portfolio, same evidence. Either it is the same conclusion (a re-run,
        # idempotent) or identical evidence produced a different answer, which is an
        # inconsistency and never a version.
        same_conclusion = bool(
            (new_fingerprint and prior_fingerprint
             and new_fingerprint == prior_fingerprint)
            or existing.get("reassessment_hash") == identity["reassessment_hash"])
        if same_conclusion:
            return {"status": PERSIST_REUSED,
                    "artifact_id": existing.get("artifact_id"),
                    "path": existing.get("path"), "persisted": True, "reused": True,
                    "conflict": False, "history_appended": False,
                    "economic_state_changed": False,
                    "assessment_evidence_changed": False, "identity": identity}
        return {"status": PERSIST_CONFLICT, "artifact_id": aid,
                "existing_artifact_id": existing.get("artifact_id"),
                "existing_reassessment_hash": existing.get("reassessment_hash"),
                "persisted": False, "reused": False, "conflict": True,
                "history_appended": False, "economic_state_changed": False,
                "assessment_evidence_changed": False,
                "reason": "An immutable reassessment artifact already exists for this "
                          "book + eligible date, bound to the SAME economic state and "
                          "the SAME assessment evidence, but recording a different "
                          "conclusion; it was not overwritten.",
                "identity": identity}

    if existing:
        aid = _unique_artifact_id(aid, identity, reassessment_dir)
    payload = {
        "reassessment_id": aid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _now_iso(now),
        "identity": identity,
        "input_contract": _compact_input_contract(input_contract),
        "reassessment": result,
    }
    path = _artifacts_dir(reassessment_dir) / ("%s.json" % aid)
    _atomic_write_json(path, payload)
    entry = {"artifact_id": aid, "path": str(path),
             "reassessment_hash": identity["reassessment_hash"],
             "portfolio_state_hash": identity["portfolio_state_hash"],
             "economic_state_hash": identity.get("economic_state_hash"),
             # Release 54.2 — indexed so the NEXT persist decides the version
             # question without reopening every artifact of the session.
             "assessment_evidence_hash": identity.get("assessment_evidence_hash"),
             "decision_fingerprint": identity.get("decision_fingerprint"),
             "universe_scoring_hash": identity["universe_scoring_hash"],
             "holdings_snapshot_hash": identity["holdings_snapshot_hash"],
             "hoc_assessment_hash": identity["hoc_assessment_hash"],
             "reassessment_policy_version": identity["reassessment_policy_version"],
             "decision": result.get("reassessment_state"),
             "eligible_market_date": identity["eligible_market_date"],
             "active_book_id": identity["active_book_id"],
             "supersedes_artifact_id": ((existing or {}).get("artifact_id")
                                        if existing else None),
             "generated_at": payload["generated_at"]}
    # Index AFTER the artifact write (interrupted-write recoverable). The newest version
    # sits at the top level (backward compatible for every existing reader) and the full
    # append-only version chain is preserved under ``versions`` so the superseded
    # artifact stays discoverable and is never rewritten.
    prior_versions = list((existing or {}).get("versions") or [])
    if existing and not prior_versions:
        prior_versions = [{k: v for k, v in existing.items() if k != "versions"}]
    index[key] = {**entry, "versions": prior_versions + [entry]}
    _atomic_write_json(_index_path(reassessment_dir), index)
    appended = _append_history(_history_row(artifact=payload),
                               reassessment_dir=reassessment_dir)
    if economic_state_changed:
        status = PERSIST_ECONOMIC_VERSION
    elif assessment_evidence_changed:
        status = PERSIST_ASSESSMENT_VERSION
    else:
        status = PERSIST_CREATED
    return {"status": status,
            "artifact_id": aid, "path": str(path),
            "persisted": True, "reused": False, "conflict": False,
            "economic_state_changed": economic_state_changed,
            "assessment_evidence_changed": assessment_evidence_changed,
            "version_index": len(index[key]["versions"]),
            "prior_assessment_evidence_hash": prior_evidence_hash,
            "superseded_artifact_id": ((existing or {}).get("artifact_id")
                                       if existing else None),
            "history_appended": bool(appended), "identity": identity}


def _read_indexed_artifact(entry: Optional[dict], reassessment_dir=None) -> Optional[dict]:
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None and entry.get("artifact_id"):
        art = _load_json(_artifacts_dir(reassessment_dir)
                         / ("%s.json" % entry.get("artifact_id")))
    return art if isinstance(art, dict) else None


def load_latest_artifact(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str],
                         reassessment_dir=None,
                         economic_state_hash: Optional[str] = None) -> Optional[dict]:
    """Load the persisted artifact for an exact (active book, eligible date), or None.

    Stage 21 (Workstream 0E, requirement 4): when ``economic_state_hash`` is supplied
    the lookup resolves the NEWEST version bound to exactly that economic state, so a
    session that produced more than one version (because the portfolio genuinely changed
    mid-session — e.g. the Aug-13 settlement) resolves the CURRENT-state assessment
    rather than the first one written that day. With no hint, the newest version wins.
    Superseded versions stay on disk and stay readable; nothing is rewritten.
    """
    index = _load_json(_index_path(reassessment_dir)) or {}
    if not isinstance(index, dict):
        return None
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    if economic_state_hash:
        versions = list(entry.get("versions") or [entry])
        for v in reversed(versions):
            if v.get("economic_state_hash") == economic_state_hash:
                art = _read_indexed_artifact(v, reassessment_dir)
                if art is not None:
                    return art
    return _read_indexed_artifact(entry, reassessment_dir)


def load_artifact_versions(*, active_book_id: Optional[str],
                           eligible_market_date: Optional[str],
                           reassessment_dir=None) -> list[dict]:
    """The append-only version chain for ONE (book, session), oldest first.

    Release 54.2 — a session may now hold more than one immutable assessment. The
    chain is the audit surface: every version that was ever authoritative, in the
    order it became so. The LAST entry is the current one.
    """
    index = _load_json(_index_path(reassessment_dir)) or {}
    if not isinstance(index, dict):
        return []
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return []
    versions = list(entry.get("versions") or [])
    return versions or [{k: v for k, v in entry.items() if k != "versions"}]


def load_artifact_by_id(*, reassessment_id: str, active_book_id: Optional[str] = None,
                        eligible_market_date: Optional[str] = None,
                        reassessment_dir=None) -> Optional[dict]:
    """Load ONE historical artifact by its exact id.

    Release 54.2 — an explicit-id read is immutable by construction: it resolves the
    artifact file itself and never the index pointer, so a caller holding an older
    ``reassessment_id`` keeps receiving exactly the assessment it referenced, however
    many later versions the session acquired.
    """
    if not reassessment_id:
        return None
    art = _load_json(_artifacts_dir(reassessment_dir) / ("%s.json" % reassessment_id))
    if isinstance(art, dict):
        return art
    for v in load_artifact_versions(active_book_id=active_book_id,
                                    eligible_market_date=eligible_market_date,
                                    reassessment_dir=reassessment_dir):
        if v.get("artifact_id") == reassessment_id:
            return _read_indexed_artifact(v, reassessment_dir)
    return None


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None,
                    hoc_assessment: Optional[dict] = None,
                    freshness: Optional[dict] = None,
                    recent_change_history: Optional[list] = None,
                    policy: Optional[dict] = None, hoc_dir=None,
                    reassessment_dir=None, now: Optional[datetime] = None,
                    hoc_binding: Optional[dict] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None,
                    freshness_loader: Optional[Callable] = None,
                    hoc_assessment_loader: Optional[Callable] = None) -> dict:
    """The Daily Research Cycle entry: build -> kernel -> persist (idempotent)."""
    run = run_reassessment(
        portfolio_state=portfolio_state, scoring=scoring, hoc_assessment=hoc_assessment,
        freshness=freshness, recent_change_history=recent_change_history, policy=policy,
        hoc_dir=hoc_dir, reassessment_dir=reassessment_dir, hoc_binding=hoc_binding,
        portfolio_state_loader=portfolio_state_loader, scoring_loader=scoring_loader,
        freshness_loader=freshness_loader, hoc_assessment_loader=hoc_assessment_loader)
    persist = persist_reassessment(result=run["reassessment"],
                                   input_contract=run["input_contract"],
                                   reassessment_dir=reassessment_dir, now=now)
    return {"input_contract": run["input_contract"], "reassessment": run["reassessment"],
            "persistence": persist}


# --------------------------------------------------------------------------- #
# Proposal boundary (Workstream H) — the ONLY thing this owner decides about the
# Slice-7 engine is WHETHER it should run. It never builds a target.
# --------------------------------------------------------------------------- #
def should_build_proposal(reassessment: Optional[dict]) -> dict:
    """Deterministic verdict: may the canonical Slice-7 proposal owner be invoked?

    Only ``PROPOSAL_READY`` authorises the cycle to build (or reuse) a reviewable
    proposal. This is NOT an approval: the proposal that results is review-only and
    still requires the Stage-18 manual decision and the Stage-19 order-plan
    confirmation before a single paper order can exist.
    """
    res = reassessment or {}
    state = res.get("reassessment_state")
    build = bool(state in kernel.PROPOSAL_ELIGIBLE_STATES)
    return {
        "build_proposal": build,
        "reassessment_state": state,
        "reassessment_hash": res.get("reassessment_hash"),
        "reason": ("The portfolio-level economic gate cleared; the canonical "
                   "reallocation proposal owner must produce the reviewable proposal."
                   if build else
                   "The portfolio-level economic gate did not clear (%s); no proposal is "
                   "built and no capital is redeployed." % (state or "NOT_READY")),
        "proposal_owner": "api.reallocation_proposal",
        "target_engine_owner": kernel.TARGET_ENGINE_OWNER,
        "creates_orders": False,
        "approves_proposal": False,
        "manual_review_required": True,
    }


def proposal_binding(*, reassessment: Optional[dict], artifact: Optional[dict] = None,
                     input_contract: Optional[dict] = None) -> dict:
    """The provenance a proposal generated by this reassessment MUST carry.

    Every field is an identity the proposal can be verified against later; if any of
    them changes the proposal is provably describing a different portfolio.
    """
    res = reassessment or {}
    ic = input_contract or {}
    prov = res.get("provenance") or {}
    return {
        "reassessment_id": (artifact or {}).get("reassessment_id"),
        "reassessment_hash": res.get("reassessment_hash"),
        "reassessment_state": res.get("reassessment_state"),
        "reassessment_policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "hoc_assessment_hash": prov.get("hoc_assessment_hash") or ic.get("hoc_assessment_hash"),
        "hoc_decision_policy_version": prov.get("hoc_decision_policy_version")
        or ic.get("hoc_decision_policy_version"),
        # Release 54.3 — the proposal inherits the reassessment's EXACT
        # opportunity-cost lineage, so a governed decision built on the proposal
        # can still name (and retrieve) the immutable assessment underneath it.
        "hoc_artifact_id": ((artifact or {}).get("identity") or {}).get(
            "hoc_artifact_id") or ic.get("hoc_artifact_id"),
        "hoc_assessment_evidence_hash": (
            ((artifact or {}).get("identity") or {}).get(
                "hoc_assessment_evidence_hash")
            or ic.get("hoc_assessment_evidence_hash")),
        "hoc_persisted": (((artifact or {}).get("identity") or {}).get("hoc_persisted")
                          if ((artifact or {}).get("identity") or {}).get(
                              "hoc_persisted") is not None
                          else ic.get("hoc_persisted")),
        "universe_scoring_hash": prov.get("universe_scoring_hash")
        or ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": prov.get("universe_input_contract_hash")
        or ic.get("universe_input_contract_hash"),
        "portfolio_state_hash": prov.get("portfolio_state_hash")
        or ic.get("portfolio_state_hash"),
        "corporate_actions_hash": prov.get("corporate_actions_hash")
        or ic.get("corporate_actions_hash"),
        "holdings_snapshot_hash": prov.get("holdings_snapshot_hash")
        or ic.get("holdings_snapshot_hash"),
        "eligible_market_date": res.get("eligible_market_date"),
        "active_book_id": res.get("active_book_id"),
        "model_identity": prov.get("model_identity") or ic.get("model_identity") or {},
        "review_only": True,
        "creates_orders": False,
    }


def proposal_is_current_for(*, reassessment: dict, proposal_artifact: Optional[dict]) -> dict:
    """Does an existing proposal artifact already implement THIS reassessment?

    Duplicate proposals are prevented by identity, not by timing: a proposal whose bound
    HOC assessment hash, portfolio-state hash, universe-scoring hash, corporate-action
    hash and eligible date all match the reassessment already satisfies it and is REUSED.
    """
    res = reassessment or {}
    prov = res.get("provenance") or {}
    art = proposal_artifact or {}
    if not art:
        return {"reusable": False, "reason": "NO_EXISTING_PROPOSAL"}
    ident = art.get("identity") or {}
    ic = art.get("input_contract") or {}
    checks = {
        "eligible_market_date": (ident.get("eligible_market_date") or
                                 ic.get("eligible_market_date"),
                                 res.get("eligible_market_date")),
        "active_book_id": (ident.get("active_book_id") or ic.get("active_book_id"),
                           res.get("active_book_id")),
        "hoc_assessment_hash": (ident.get("hoc_assessment_hash")
                                or ic.get("hoc_assessment_hash"),
                                prov.get("hoc_assessment_hash")),
        "portfolio_state_hash": (ident.get("portfolio_state_hash")
                                 or ic.get("portfolio_state_hash"),
                                 prov.get("portfolio_state_hash")),
        "universe_scoring_hash": (ident.get("universe_scoring_hash")
                                  or ic.get("universe_scoring_hash"),
                                  prov.get("universe_scoring_hash")),
        "corporate_actions_hash": (ident.get("corporate_actions_hash")
                                   or ic.get("corporate_actions_hash"),
                                   prov.get("corporate_actions_hash")),
    }
    mismatches = [k for k, (a, b) in checks.items() if a is not None and b is not None
                  and a != b]
    if mismatches:
        return {"reusable": False, "reason": "BOUND_STATE_CHANGED",
                "mismatched_fields": sorted(mismatches)}
    return {"reusable": True, "reason": "IDENTICAL_BOUND_STATE",
            "proposal_id": art.get("proposal_id"),
            "proposal_hash": (art.get("proposal") or {}).get("proposal_hash")}


# --------------------------------------------------------------------------- #
# Stage-19 execution precedence (Workstream I) — while a controlled paper
# rebalance is still executing, THAT lifecycle owns the operator's attention.
# --------------------------------------------------------------------------- #
#: The Stage-19 lifecycle states in which an execution is genuinely in flight.
EXECUTION_ACTIVE_STATES = frozenset({
    "PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED",
    "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING",
})


def execution_precedence(*, rebalance_state: Optional[str],
                         pending_orders: Optional[int] = None) -> dict:
    """Does an in-flight Stage-19 execution outrank a fresh reassessment result?

    A reassessment is evidence; a submitted order plan is a commitment. While paper
    orders from a confirmed plan are still awaiting their NEXT_CLOSE settlement, a newly
    produced proposal must NOT overwrite, obscure or compete with the execution
    lifecycle: the reassessment stays readable as evidence, but the operator's single
    primary action remains the pending execution.
    """
    active = bool(rebalance_state in EXECUTION_ACTIVE_STATES
                  or (pending_orders or 0) > 0)
    return {
        "execution_active": active,
        "rebalance_state": rebalance_state,
        "pending_orders": pending_orders,
        "reassessment_outranked": active,
        "owner": "api.rebalance_execution",
        "reason": ("A controlled paper rebalance is still executing; its lifecycle keeps "
                   "operator precedence and a new reassessment is presented as evidence "
                   "only, never as a competing action."
                   if active else
                   "No controlled paper rebalance is in flight; the reassessment result "
                   "may drive the operator's primary action."),
        "new_proposal_may_supersede_execution": False,
    }


# --------------------------------------------------------------------------- #
# Read contract (Workstream G / J) — GET /v1/operations/portfolio-reassessment
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


#: Operator-facing headline per decision (Workstream J). ONE primary action maximum;
#: the passive states deliberately have none.
#: Track B (decision consistency) — canonical decision-lane states that SETTLE the
#: portfolio question with no review outstanding, with the operator sentence each
#: one earns. Mirrored from api.portfolio_decision as literals (import-pure).
_DECISION_SETTLED_LANE_STATES = {
    "HOLD_CURRENT_BOOK": ("the feasible alternative does not clear the switching "
                          "hurdle after cost; holding the current book IS the "
                          "decision"),
    "CHANGE_CANDIDATE_WITHHELD": ("the complete candidate target did not clear the "
                                  "portfolio-level limits; the change is withheld"),
    "NO_MATERIAL_CHANGE": ("the produced proposal contains no material "
                           "capital-allocation change; there is nothing to decide"),
}

_OPERATOR_PRESENTATION = {
    STATE_NO_CHANGE: {
        "title": "PORTFOLIO CURRENT",
        "operator_state": "PORTFOLIO_CURRENT",
        "task": "No portfolio change is economically justified",
        "next_action": "No action required",
        "primary_action": None,
        "severity": "SUCCESS",
    },
    STATE_CHANGE_CANDIDATE: {
        "title": "PORTFOLIO CHANGE CANDIDATE",
        "operator_state": "PORTFOLIO_CHANGE_CANDIDATE",
        "task": "Holdings are deteriorating but a change is not yet economically "
                "justified",
        "next_action": "Monitor — no action required",
        "primary_action": None,
        "severity": "ATTENTION",
    },
    STATE_PROPOSAL_READY: {
        "title": "PORTFOLIO PROPOSAL READY",
        "operator_state": "MANUAL_REVIEW_REQUIRED",
        "task": "Review the proposed portfolio change",
        "next_action": "REVIEW PORTFOLIO PROPOSAL",
        "primary_action": "REVIEW_PORTFOLIO_PROPOSAL",
        "severity": "ATTENTION",
    },
    STATE_BLOCKED_DATA: {
        "title": "REASSESSMENT BLOCKED — DATA",
        "operator_state": "PORTFOLIO_REASSESSMENT_BLOCKED_DATA",
        "task": "Required point-in-time evidence is missing or incomplete",
        "next_action": "Resolve the named input, then run the Daily Research Cycle",
        "primary_action": None,
        "severity": "BLOCKED",
    },
    STATE_BLOCKED_EVIDENCE: {
        "title": "REASSESSMENT BLOCKED — EVIDENCE",
        "operator_state": "PORTFOLIO_REASSESSMENT_BLOCKED_EVIDENCE",
        "task": "The bound evidence no longer describes the current portfolio",
        "next_action": "Run the Daily Research Cycle to reassess against current state",
        "primary_action": None,
        "severity": "BLOCKED",
    },
    STATE_MANUAL_REVIEW: {
        "title": "PORTFOLIO CONSTRAINT BREACH",
        "operator_state": "MANUAL_REVIEW_REQUIRED",
        "task": "A holding breaches a hard portfolio constraint",
        "next_action": "REVIEW THE CONSTRAINT BREACH",
        "primary_action": "REVIEW_PORTFOLIO_CONSTRAINT_BREACH",
        "severity": "ATTENTION",
    },
    STATE_NOT_READY: {
        "title": "REASSESSMENT NOT READY",
        "operator_state": "PORTFOLIO_REASSESSMENT_NOT_READY",
        "task": "Waiting for an eligible signal refresh",
        "next_action": "Run the Daily Research Cycle",
        "primary_action": None,
        "severity": "INFO",
    },
    STATE_NOT_RUN: {
        "title": "REASSESSMENT NOT RUN",
        "operator_state": "PORTFOLIO_REASSESSMENT_NOT_RUN",
        "task": "No reassessment exists for the current eligible session",
        "next_action": "Run the Daily Research Cycle",
        "primary_action": None,
        "severity": "INFO",
    },
    STATE_STALE: {
        "title": "REASSESSMENT STALE",
        "operator_state": "PORTFOLIO_REASSESSMENT_STALE",
        "task": "A corporate action was registered after this reassessment was produced",
        "next_action": "Run the Daily Research Cycle to reassess against the corrected "
                       "portfolio",
        "primary_action": None,
        "severity": "BLOCKED",
    },
    STATE_UNAVAILABLE: {
        "title": "REASSESSMENT UNAVAILABLE",
        "operator_state": "PORTFOLIO_REASSESSMENT_UNAVAILABLE",
        "task": "The reassessment read model could not be composed",
        "next_action": "Inspect the named source error",
        "primary_action": None,
        "severity": "ERROR",
    },
}


#: Stage 21 (Workstream 0C) — the reconciliation the operator was previously left to
#: infer. On 2026-08-13 the Holding Review said "13 HOLDINGS NEED ATTENTION" while the
#: global operator state said "DAILY CYCLE COMPLETE / MONITOR THE PORTFOLIO / NO ACTION
#: REQUIRED". Both were correct — they answer DIFFERENT questions — but nothing on the
#: page said so, which reads as a contradiction and invites the operator to go looking
#: for an action that does not exist.
#:
#: PER-HOLDING ATTENTION is a review signal about ONE name.
#: PORTFOLIO-LEVEL DECISION is an economic verdict about the WHOLE book, net of
#: switching costs, turnover, risk, concentration and churn controls.
#:
#: A name can be worth replacing on its own merits while replacing it is not worth
#: paying for. HOC never becomes an execution action; it stays REVIEW ONLY.
_DECISION_SCOPE_EXPLANATION = {
    STATE_NO_CHANGE: (
        "%(n)d holding(s) have individual concerns, but the portfolio-level economic "
        "gate does not justify another rebalance after switching costs, turnover, risk, "
        "concentration and churn controls. Monitor only — these are review signals, not "
        "approved portfolio changes."),
    STATE_CHANGE_CANDIDATE: (
        "%(n)d holding(s) have individual concerns and the portfolio is deteriorating, "
        "but the expected improvement does not yet clear the portfolio-level hurdle net "
        "of switching costs and turnover. No change is proposed — these remain review "
        "signals, not approved portfolio changes."),
    STATE_PROPOSAL_READY: (
        "%(n)d holding(s) have individual concerns AND the portfolio-level hurdle "
        "cleared: the expected improvement exceeds the switching cost, turnover and "
        "churn controls, so a reviewable proposal exists. Nothing is approved until you "
        "review it."),
    STATE_MANUAL_REVIEW: (
        "%(n)d holding(s) have individual concerns and at least one breaches a hard "
        "portfolio constraint that a human must adjudicate. The per-holding signals are "
        "review only; the constraint breach is what requires your decision."),
    STATE_BLOCKED_DATA: (
        "%(n)d holding(s) have individual concerns, but a required point-in-time input "
        "is missing or incomplete, so NO portfolio-level verdict was reached. The "
        "per-holding signals are review only and no change is proposed."),
    STATE_BLOCKED_EVIDENCE: (
        "%(n)d holding(s) have individual concerns, but the bound evidence no longer "
        "describes the current portfolio, so NO portfolio-level verdict was reached. "
        "The per-holding signals are review only and no change is proposed."),
}


def build_decision_scope(*, state: str, reassessment: Optional[dict]) -> dict:
    """Stage 21 (Workstream 0C) — reconcile PER-HOLDING attention with the
    PORTFOLIO-LEVEL decision, explicitly, in the backend that owns the verdict.

    This is presentation-grade text derived from the numbers the kernel already
    computed. It creates no action, no CTA and no new state: it exists so the operator
    never has to infer why 13 flagged holdings can coexist with "no action required".
    """
    res = reassessment or {}
    dec = res.get("decision") or {}
    n = (res.get("attention") or {}).get("count", 0) or 0
    template = _DECISION_SCOPE_EXPLANATION.get(state)
    return {
        "per_holding_attention_count": n,
        "per_holding_scope": "INDIVIDUAL_HOLDING_REVIEW_SIGNAL",
        "portfolio_decision_state": state,
        "portfolio_decision_scope": "WHOLE_PORTFOLIO_ECONOMIC_VERDICT",
        "scopes_are_different_questions": True,
        "explanation": (template % {"n": n}) if template else None,
        "portfolio_gate_reason_codes": dec.get("reason_codes") or [],
        "portfolio_gate_blockers": dec.get("blockers") or [],
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        # The invariants the UI must honour, asserted by the backend that owns them.
        "holding_recommendations_are_review_only": True,
        "holding_recommendations_are_approved_changes": False,
        "holding_review_offers_execution_action": False,
        "holding_review_label": "REVIEW ONLY — NOT AN APPROVED PORTFOLIO CHANGE",
        "safety_badges": ["REVIEW ONLY", "NO ORDERS", "MANUAL REVIEW", "AUTOMATION OFF"],
    }


def build_presentation(*, state: str, reassessment: Optional[dict],
                       execution: Optional[dict] = None,
                       decision_lane: Optional[dict] = None) -> dict:
    """The ONE operator presentation for a reassessment state.

    When a Stage-19 execution is in flight the reassessment's own primary action is
    SUPPRESSED (never duplicated alongside the execution CTA) — the execution lifecycle
    keeps the operator's single primary action.

    ``decision_lane`` (Track B, decision consistency) is the canonical
    portfolio-decision lane (api.portfolio_decision), which consumes the constrained
    owner's authoritative outcome. A reassessment honestly reports PROPOSAL_READY —
    it ASKED for a target — but when the decision owner has already settled the
    question (HOLD_CURRENT_BOOK / CHANGE_CANDIDATE_WITHHELD / NO_MATERIAL_CHANGE)
    this card must not raise its own "review the proposed portfolio change" action
    over a decision that has been taken. Suppression only; the reassessment's own
    state, economics and artifact identity are untouched. ``None`` (older callers)
    keeps the pre-existing behaviour exactly.
    """
    base = dict(_OPERATOR_PRESENTATION.get(state) or _OPERATOR_PRESENTATION[STATE_NOT_RUN])
    res = reassessment or {}
    dec = res.get("decision") or {}
    exe = execution or {}
    lane_state = str((decision_lane or {}).get("portfolio_decision_state") or "")
    decision_settled = (state == STATE_PROPOSAL_READY
                        and lane_state in _DECISION_SETTLED_LANE_STATES)
    if exe.get("execution_active"):
        base["primary_action"] = None
        base["next_action"] = ("Complete the pending controlled paper rebalance first")
        base["execution_precedence"] = True
        base["execution_precedence_reason"] = exe.get("reason")
    else:
        base["execution_precedence"] = False
        base["execution_precedence_reason"] = None
    if decision_settled:
        settled_label = _DECISION_SETTLED_LANE_STATES[lane_state]
        base["primary_action"] = None
        base["operator_state"] = "PORTFOLIO_DECISION_SETTLED"
        base["task"] = ("The requested change was priced by the governed decision "
                        "owner: %s" % settled_label)
        base["next_action"] = "No action required — the governed decision stands"
        base["severity"] = "SUCCESS"
        base["decision_settled"] = True
        base["settled_decision_state"] = lane_state
        base["settled_decision_owner"] = "api.portfolio_decision"
    else:
        base["decision_settled"] = False
        base["settled_decision_state"] = None
    base.update({
        "state": state,
        "explanation": res.get("explanation"),
        "attention_count": (res.get("attention") or {}).get("count", 0),
        "holdings_evaluated": dec.get("holdings_evaluated"),
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "improvement_basis": kernel.IMPROVEMENT_BASIS,
        "strongest_opportunity": dec.get("strongest_evidence"),
        "blockers": dec.get("blockers") or [],
        # Stage 21 (Workstream 0C) — why per-holding attention and the portfolio-level
        # decision can disagree without either being wrong.
        "decision_scope": build_decision_scope(state=state, reassessment=res),
        "safety_badges": ["PREVIEW ONLY", "MANUAL REVIEW", "NO LIVE ORDERS",
                          "AUTOMATION OFF"],
    })
    return base


def _read_payload(*, state: str, generated_at: str, eligible: Optional[str],
                  active_book: dict, artifact: Optional[dict], message: str,
                  policy: dict, reassessment: Optional[dict],
                  input_contract: Optional[dict],
                  staleness: Optional[dict] = None,
                  execution: Optional[dict] = None) -> dict:
    r = reassessment or {}
    art_meta = None
    if artifact:
        art_meta = {"reassessment_id": artifact.get("reassessment_id"),
                    "generated_at": artifact.get("generated_at"),
                    "identity": artifact.get("identity"),
                    "immutable": True,
                    "root_env": REASSESSMENT_DIR_ENV}
    stale = bool((staleness or {}).get("stale"))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": state,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "message": message,
        "stale": stale,
        "staleness": staleness,
        "eligible_market_date": eligible,
        "active_book": active_book,
        "reassessment_id": (art_meta or {}).get("reassessment_id"),
        "reassessment_hash": r.get("reassessment_hash"),
        "policy": r.get("policy") or policy,
        "policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,
        "portfolio_summary": r.get("portfolio_summary") or {},
        "decision": r.get("decision") or {},
        "holding_assessments": r.get("holding_assessments") or [],
        "attention": r.get("attention") or {"exit": [], "replace": [], "reduce": [],
                                            "count": 0},
        "strongest_alternatives": r.get("strongest_alternatives") or [],
        "recommendation_counts": r.get("recommendation_counts") or {},
        "churn_control": r.get("churn_control") or {},
        "concentration": r.get("concentration") or {},
        "input_quality": r.get("input_quality") or {},
        "blockers": r.get("blockers") or [],
        "data_gaps": r.get("data_gaps") or [],
        "explanation": r.get("explanation"),
        "presentation": build_presentation(state=state, reassessment=r,
                                           execution=execution),
        "execution_precedence": execution or execution_precedence(rebalance_state=None),
        "proposal_boundary": should_build_proposal(r),
        "proposal_binding": proposal_binding(reassessment=r, artifact=artifact,
                                             input_contract=input_contract),
        "artifact": art_meta,
        "input_contract": input_contract,
        "safety": r.get("safety") or kernel._safety(),   # noqa: SLF001 - ONE safety block
        "provenance": r.get("provenance") or {"composition_owner": COMPOSITION_OWNER,
                                              "calculation_owner": CALCULATION_OWNER},
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
        "review_only": True,
    }


def _default_rebalance_state_loader() -> dict:
    from paper_trader.api import rebalance_execution as rb
    return rb.load_rebalance_state()


def load_portfolio_reassessment(*, portfolio_state: Optional[dict] = None,
                                artifact: Optional[dict] = None, reassessment_dir=None,
                                now: Optional[datetime] = None,
                                portfolio_state_loader: Optional[Callable] = None,
                                rebalance_state_loader: Optional[Callable] = None,
                                rebalance_state: Optional[dict] = None) -> dict:
    """The read contract for the endpoint. READ-ONLY: it NEVER runs the engine — it
    returns the current persisted reassessment for the active book + eligible session.
    When no artifact exists it returns a readable ``NOT_RUN`` payload (the sole execution
    path is the Daily Research Cycle). Always degrade-safe: it never raises."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return _read_payload(state=STATE_UNAVAILABLE, generated_at=generated_at,
                             eligible=None, active_book={}, artifact=None,
                             message="Portfolio state is unavailable: %s" % str(exc)[:160],
                             policy=resolve_policy(), reassessment=None,
                             input_contract=None)

    active_book = _active_book_block(ps)
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    book_id = active_book.get("book_id")

    rb = rebalance_state
    if rb is None:
        rb = _safe_call(rebalance_state_loader or _default_rebalance_state_loader) or {}
    execution = execution_precedence(
        rebalance_state=(rb or {}).get("rebalance_state"),
        pending_orders=((rb or {}).get("execution_summary") or {}).get("submitted_count"))

    if not book_id:
        return _read_payload(state=STATE_NOT_READY, generated_at=generated_at,
                             eligible=eligible, active_book=active_book, artifact=None,
                             message="No active operational book; no reassessment.",
                             policy=resolve_policy(), reassessment=None,
                             input_contract=None, execution=execution)

    # Stage 21 (Workstream 0E, requirement 4): resolve the version bound to the CURRENT
    # economic state when one exists, so a session with more than one version never
    # strands the operator on a superseded pre-settlement assessment.
    art = artifact if artifact is not None else load_latest_artifact(
        active_book_id=book_id, eligible_market_date=eligible,
        reassessment_dir=reassessment_dir,
        economic_state_hash=ps.get("economic_state_hash"))
    if not art:
        return _read_payload(
            state=STATE_NOT_RUN, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=None,
            message=("No portfolio reassessment has been produced for the current active "
                     "book and eligible session yet. Run the Daily Research Cycle "
                     "(POST /v1/operations/daily-research-cycle/run) to produce one."),
            policy=resolve_policy(), reassessment=None, input_contract=None,
            execution=execution)

    res = art.get("reassessment") or {}
    staleness = _corporate_action_staleness(artifact=art, portfolio_state=ps,
                                            active_book_id=book_id)
    currency = economic_currency(artifact=art, portfolio_state=ps)
    staleness = {**staleness, "economic_currency": currency}
    if staleness.get("stale"):
        return _read_payload(
            state=STATE_STALE, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=art,
            message=("This reassessment was produced BEFORE a corporate action was "
                     "registered, so it evaluated holdings that no longer describe the "
                     "current portfolio. Run the Daily Research Cycle to reassess."),
            policy=res.get("policy") or resolve_policy(), reassessment=res,
            input_contract=art.get("input_contract"), staleness=staleness,
            execution=execution)
    # Stage 21 (Workstream 0E, requirement 5): a PROVEN economic change still fails
    # closed. Only a proven change does — an unverifiable binding never blocks.
    if currency.get("state") == "SUPERSEDED":
        return _read_payload(
            state=STATE_STALE, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=art,
            message=currency.get("message"),
            policy=res.get("policy") or resolve_policy(), reassessment=res,
            input_contract=art.get("input_contract"), staleness=staleness,
            execution=execution)
    return _read_payload(
        state=res.get("reassessment_state") or STATE_NOT_RUN, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art,
        message="Current portfolio reassessment for the active book / eligible session. "
                "Review only — no orders, no automation.",
        policy=res.get("policy") or resolve_policy(), reassessment=res,
        input_contract=art.get("input_contract"), staleness=staleness,
        execution=execution)


def economic_currency(*, artifact: Optional[dict],
                      portfolio_state: Optional[dict]) -> dict:
    """Does a persisted reassessment still describe the CURRENT economic portfolio?

    Stage 21 (Workstream 0E). Binds to the ONE canonical economic fingerprint owned by
    ``api.portfolio_state`` — holdings, cash, NAV, order/fill counts and the
    corporate-action registry. Research outputs are structurally excluded, so a
    downstream write can never invalidate its own input.

    Returns ``CURRENT`` / ``SUPERSEDED`` / ``UNVERIFIABLE``. UNVERIFIABLE is NOT
    staleness: an artifact written before this contract existed simply recorded no
    economic fingerprint, and inferring "stale" from a missing value is fabrication.
    """
    art = artifact or {}
    bound = ((art.get("identity") or {}).get("economic_state_hash")
             or (art.get("input_contract") or {}).get("economic_state_hash"))
    current = (portfolio_state or {}).get("economic_state_hash")
    if not bound or not current:
        return {"state": "UNVERIFIABLE", "current": None,
                "bound_economic_state_hash": bound,
                "current_economic_state_hash": current,
                "reason": "ARTIFACT_RECORDED_NO_ECONOMIC_STATE_HASH" if not bound
                          else "PORTFOLIO_STATE_RECORDED_NO_ECONOMIC_STATE_HASH",
                "message": ("This assessment predates the economic-identity contract, so "
                            "whether it still describes the portfolio cannot be proven "
                            "either way. Nothing is inferred from the missing value.")}
    same = bound == current
    return {"state": "CURRENT" if same else "SUPERSEDED", "current": same,
            "bound_economic_state_hash": bound,
            "current_economic_state_hash": current,
            "reason": None if same else "ECONOMIC_PORTFOLIO_CHANGED_SINCE_ASSESSMENT",
            "message": None if same else
            ("The economic portfolio (holdings / cash / NAV / corporate actions) changed "
             "after this assessment was produced, so it no longer describes the current "
             "portfolio. Run the Daily Research Cycle to reassess.")}


def _corporate_action_staleness(*, artifact: Optional[dict], portfolio_state: Optional[dict],
                                active_book_id: Optional[str]) -> dict:
    """Stage 19.1 semantics — is a persisted reassessment still valid against the
    CURRENT corporate-action registry? Pure delegation to ``api.corporate_actions``.

    Stage 21 (Workstream 0E): the bound fingerprint is resolved through the ONE
    canonical resolver (:func:`hoc_corporate_actions_hash`), and an artifact that
    recorded nothing is UNVERIFIABLE rather than silently "bound to the empty
    registry" — the substitution that made every fresh assessment permanently stale.
    """
    try:
        from paper_trader.api import corporate_actions as ca
        art = artifact or {}
        bound = hoc_corporate_actions_hash(art)
        cur = ((portfolio_state or {}).get("corporate_actions") or {})
        current_fp = cur.get("registry_fingerprint")
        current = ({"fingerprint": current_fp, "n_registered": len(cur.get("actions") or []),
                    "actions": cur.get("actions") or []}
                   if current_fp is not None else None)
        if not bound:
            fp = current or ca.registry_fingerprint(book_id=active_book_id)
            return {"stale": False, "verifiable": False, "reason": None,
                    "bound_corporate_actions_hash": None,
                    "current_corporate_actions_hash": fp.get("fingerprint"),
                    "n_registered_now": fp.get("n_registered"),
                    "unverifiable_reason":
                        "ARTIFACT_RECORDED_NO_CORPORATE_ACTION_FINGERPRINT",
                    "owner": ca.OWNER, "message": None}
        out = (ca.staleness_vs_registry(bound, current=current) if current is not None
               else ca.staleness_vs_registry(bound, book_id=active_book_id))
        return {**out, "verifiable": True}
    except Exception:  # noqa: BLE001
        return {"stale": False, "verifiable": False, "reason": None}


# --------------------------------------------------------------------------- #
# Compact summary the workflow-state read delegates to (PURE ARTIFACT READER)
# --------------------------------------------------------------------------- #
def load_reassessment_summary(*, active_book_id: Optional[str] = None,
                              eligible_market_date: Optional[str] = None,
                              artifact: Optional[dict] = None,
                              reassessment_dir=None) -> dict:
    """A compact, read-only reassessment summary.

    PURE ARTIFACT READER — it reads ONLY the immutable artifact index/artifact for the
    exact ``(active_book_id, eligible_market_date)`` the caller supplies. It never loads
    portfolio state, never runs the engine, never calls a provider / prediction and never
    writes. This is what keeps ``api.workflow_state`` acyclic.
    """
    empty = {
        "reassessment_available": False,
        "reassessment_state": STATE_NOT_RUN,
        "reassessment_id": None,
        "reassessment_hash": None,
        "reassessment_date": None,
        "decision": STATE_NOT_RUN,
        "proposal_required": False,
        "attention_count": 0,
        "holdings_evaluated": 0,
        "expected_net_improvement": None,
        "expected_one_way_turnover": None,
        "expected_transaction_cost_usd": None,
        "blockers": [],
        "reason_codes": [],
        "explanation": None,
        "hoc_assessment_hash": None,
        # R54.2.3.2 — the assessment's OWN evidence identity (see below).
        "assessment_hoc_assessment_hash": None,
        "assessment_generated_at": None,
        "assessment_artifact_id": None,
        "mandatory_exit_tickers": [],
        "mandatory_exit_policy": {},
        "constraint_ownership": {},
        "turnover_budget_binding_here": False,
        "expected_turnover_basis": None,
        "concentration_basis": None,
        "complete_target_constraint_owner": None,
        "policy_version": REASSESSMENT_POLICY_VERSION,
        "owner": COMPOSITION_OWNER,
    }
    try:
        art = artifact if artifact is not None else load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            reassessment_dir=reassessment_dir)
    except Exception:  # noqa: BLE001 - a pure artifact read must never crash a caller
        return {**empty, "reassessment_state": STATE_UNAVAILABLE,
                "decision": STATE_UNAVAILABLE}
    if not art:
        return empty
    res = art.get("reassessment") or {}
    dec = res.get("decision") or {}
    return {
        "reassessment_available": True,
        "reassessment_state": res.get("reassessment_state"),
        "reassessment_id": art.get("reassessment_id"),
        "reassessment_hash": res.get("reassessment_hash"),
        "reassessment_date": res.get("eligible_market_date"),
        "decision": res.get("reassessment_state"),
        "proposal_required": bool(dec.get("proposal_required")),
        "attention_count": (res.get("attention") or {}).get("count", 0),
        "holdings_evaluated": dec.get("holdings_evaluated") or 0,
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "blockers": dec.get("blockers") or [],
        "reason_codes": dec.get("reason_codes") or [],
        # Release 47 — held names breaching their own name / sector / risk cap. They
        # are the REASON a target is asked for, not a blocker, so they travel with
        # the summary and a downstream surface can name them without re-deriving
        # anything (before Release 47 they arrived through ``blockers``, which is
        # what let a cap breach freeze the portfolio).
        "held_name_constraint_breaches": dec.get(
            "held_name_constraint_breaches") or [],
        "held_name_constraint_breach_effect": dec.get(
            "held_name_constraint_breach_effect"),
        "explanation": res.get("explanation"),
        # Release 29.3 — the explicit mandatory eligibility-exit contract and the
        # constraint-ownership statement travel with the summary so no consumer has to
        # infer whether "mandatory" means "sell it now" (it never does).
        # Release 29.3 — the evidence a proposal produced by THIS reassessment must
        # carry. A proposal bound to a different HOC assessment is not bound to the
        # current eligible-session reassessment, and the semantic invariants say so.
        "hoc_assessment_hash": (art.get("reassessment") or {}).get(
            "proposal_binding", {}).get("hoc_assessment_hash") or (
                (art.get("proposal_binding") or {}).get("hoc_assessment_hash")),
        # R54.2.3.2 — the assessment's OWN evidence identity, published regardless of
        # whether a proposal was requested. ``hoc_assessment_hash`` above is the
        # PROPOSAL BINDING (absent by design on a CURRENT_NO_CHANGE assessment because
        # no proposal is requested), which is exactly why the decision-supersession
        # comparison must never rely on it: the live 2026-09-02 payload published
        # ``hoc_assessment_hash: null`` for the authoritative NO-CHANGE assessment and
        # the proposal-binding invariant could not fire. These three fields carry the
        # artifact's own identity so a standing proposal can be compared against the
        # assessment that stands, whatever that assessment decided.
        "assessment_hoc_assessment_hash": (art.get("identity") or {}).get(
            "hoc_assessment_hash"),
        "assessment_generated_at": art.get("generated_at"),
        "assessment_artifact_id": art.get("reassessment_id"),
        "mandatory_exit_tickers": dec.get("mandatory_exit_tickers") or [],
        "mandatory_exit_policy": dec.get("mandatory_exit_policy") or {},
        "constraint_ownership": dec.get("constraint_ownership") or {},
        "turnover_budget_binding_here": bool(dec.get("turnover_budget_binding_here")),
        "expected_turnover_basis": dec.get("expected_turnover_basis"),
        "concentration_basis": dec.get("concentration_basis"),
        "complete_target_constraint_owner": dec.get("complete_target_constraint_owner"),
        "policy_version": REASSESSMENT_POLICY_VERSION,
        "owner": COMPOSITION_OWNER,
    }


def load_latest_assessment_pointer(*, active_book_id: Optional[str],
                                   reassessment_dir=None) -> Optional[dict]:
    """R54.2.3.2 — the store's newest assessment pointer for a book, ANY session.

    PURE INDEX READ (no artifact file, no engine, no provider): scans the index keys
    ``"<book>|<date>"`` for the book, takes the LATEST eligible session, and returns a
    copy of that head entry (the R54.2 version-chain head — the authoritative
    conclusion of its session) with ``eligible_market_date`` guaranteed present.
    ``None`` when the book has no assessment or the index cannot be read. This is the
    seam the decision-supersession loader uses to ask "what is the newest assessment
    of record for this book?" — it decides nothing itself.
    """
    if not active_book_id:
        return None
    try:
        index = _load_json(_index_path(reassessment_dir)) or {}
        best_date, best_entry = None, None
        prefix = "%s|" % active_book_id
        for key, entry in index.items():
            if not (isinstance(key, str) and key.startswith(prefix)):
                continue
            date = key[len(prefix):]
            if best_date is None or date > best_date:
                best_date, best_entry = date, entry
        if best_entry is None:
            return None
        out = dict(best_entry)
        out.setdefault("eligible_market_date", best_date)
        out.setdefault("active_book_id", active_book_id)
        out.pop("versions", None)  # the head is the pointer; the chain stays on disk
        return out
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


# --------------------------------------------------------------------------- #
# History read (Workstream L) — GET /v1/operations/portfolio-reassessment/history
# --------------------------------------------------------------------------- #
def load_reassessment_history(*, active_book_id: Optional[str] = None,
                              limit: int = 60, reassessment_dir=None,
                              now: Optional[datetime] = None) -> dict:
    """Read-only, append-only reassessment history.

    Answers: what did we recommend at each eligible session, which holdings were
    flagged, which replacements were preferred, and what the churn controls withheld.
    NOTHING is back-filled: sessions before Stage 20 landed simply have no rows, and
    that gap is reported honestly rather than reconstructed with hindsight.
    """
    rows = load_history(reassessment_dir=reassessment_dir,
                        active_book_id=active_book_id, limit=limit)
    authoritative = authoritative_history_rows(rows)
    authoritative_ids = {r.get("reassessment_id") for r in authoritative}
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "active_book_id": active_book_id,
        "rows": rows,
        "row_count": len(rows),
        # Release 54.2 — the history stays the FULL append-only record (hiding a
        # superseded assessment would be rewriting evidence); these say which rows
        # are the authoritative conclusion of their session and which were
        # superseded within it.
        "authoritative_reassessment_ids": sorted(
            i for i in authoritative_ids if i),
        "authoritative_row_count": len(authoritative),
        "superseded_row_count": len(rows) - len(authoritative),
        "first_eligible_market_date": (rows[0].get("eligible_market_date")
                                       if rows else None),
        "last_eligible_market_date": (rows[-1].get("eligible_market_date")
                                      if rows else None),
        "append_only": True,
        "backfilled": False,
        "historical_gap_note": (
            "Reassessment history begins when Stage 20 first ran. Earlier eligible "
            "sessions have NO reassessment row and are NOT reconstructed: a hindsight "
            "backfill would be fabricated evidence. The gap is a documented limitation."),
        "read_only": True,
    }


# --------------------------------------------------------------------------- #
# Forward attribution (Workstream M) — read-only decision -> outcome evidence.
# --------------------------------------------------------------------------- #
def _forward_return(series: dict, from_date: str, to_date: Optional[str]) -> Optional[float]:
    """Total return between two owned eligible closes, or None. Never extrapolated."""
    dates = series.get("dates") or []
    adj = series.get("adjusted_close") or []
    try:
        i = dates.index(from_date)
    except ValueError:
        return None
    if to_date is None:
        j = len(dates) - 1
    else:
        try:
            j = dates.index(to_date)
        except ValueError:
            j = len(dates) - 1
    if j <= i:
        return None
    a, b = adj[i], adj[j]
    if a in (None, 0) or b is None:
        return None
    try:
        return round(float(b) / float(a) - 1.0, 6)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def build_attribution(*, history: Optional[list] = None,
                      price_panel: Optional[dict] = None,
                      as_of: Optional[str] = None,
                      active_book_id: Optional[str] = None,
                      reassessment_dir=None,
                      now: Optional[datetime] = None) -> dict:
    """Link prior reassessment recommendations to their subsequent realized outcomes.

    STRICTLY read-only evidence for a LATER, human-gated policy/model review. It changes
    NO model, NO threshold, NO champion and NO portfolio. A row is produced ONLY where
    genuine forward closes exist after the recommendation date — a missing outcome stays
    ``PENDING``, never zero and never estimated.
    """
    # Release 54.2 — attribution measures what the system CONCLUDED at each session,
    # so a session that was reassessed several times contributes its authoritative
    # assessment once. Counting every version would inflate the forward evidence.
    rows = authoritative_history_rows(
        history if history is not None else load_history(
            reassessment_dir=reassessment_dir, active_book_id=active_book_id))
    panel = (price_panel or {}).get("series") or {}
    out: list[dict] = []
    measured = 0
    pending = 0
    for h in rows:
        d = h.get("eligible_market_date")
        if not d:
            continue
        for rec in (h.get("recommendations") or []):
            action = rec.get("recommendation")
            if action not in (kernel.REC_EXIT, kernel.REC_REPLACE, kernel.REC_REDUCE,
                              kernel.REC_HOLD):
                continue
            inc = rec.get("ticker")
            rep = rec.get("strongest_replacement_ticker")
            inc_ret = _forward_return(panel.get(inc) or {}, d, as_of) if inc else None
            rep_ret = _forward_return(panel.get(rep) or {}, d, as_of) if rep else None
            spread = ((rep_ret - inc_ret) if (rep_ret is not None and inc_ret is not None)
                      else None)
            acted = bool(action in kernel.ACTIONABLE_RECOMMENDATIONS
                         and not rec.get("action_withheld"))
            state = "MEASURED" if (inc_ret is not None) else "PENDING"
            if state == "MEASURED":
                measured += 1
            else:
                pending += 1
            out.append({
                "eligible_market_date": d,
                "reassessment_id": h.get("reassessment_id"),
                "portfolio_decision": h.get("decision"),
                "ticker": inc,
                "recommendation": action,
                "source_recommendation": rec.get("source_recommendation"),
                "action_taken": acted,
                "action_withheld": bool(rec.get("action_withheld")),
                "withheld_reason_codes": rec.get("withheld_reason_codes") or [],
                "replacement_ticker": rep,
                "incumbent_forward_return": inc_ret,
                "replacement_forward_return": rep_ret,
                "realized_spread": (round(spread, 6) if spread is not None else None),
                "portfolio_weight_at_decision": rec.get("current_weight"),
                "portfolio_impact": (round((rec.get("current_weight") or 0.0) * spread, 6)
                                     if spread is not None else None),
                "expected_net_improvement_at_decision": rec.get("expected_net_improvement"),
                "outcome_state": state,
                "measured_through": as_of,
            })
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "active_book_id": active_book_id,
        "as_of": as_of,
        "rows": out,
        "row_count": len(out),
        "measured_count": measured,
        "pending_count": pending,
        "read_only": True,
        "changes_model": False,
        "changes_thresholds": False,
        "changes_champion": False,
        "changes_portfolio": False,
        "note": ("Forward outcomes are measured ONLY where genuine owned closes exist "
                 "after the recommendation date. A missing outcome remains PENDING; it "
                 "is never zero-filled, estimated or back-dated. This evidence informs a "
                 "later human-gated recalibration review and changes nothing by itself."),
    }


__all__ = [
    "PHASE", "SCHEMA_VERSION", "INPUT_SCHEMA_VERSION", "REASSESSMENT_POLICY_VERSION",
    "CHURN_POLICY_VERSION", "COMPOSITION_OWNER", "CALCULATION_OWNER",
    "REASSESSMENT_DIR_ENV", "POLICY_OVERRIDE_ENV", "READ_STATE_VOCAB",
    "STATE_NOT_READY", "STATE_NO_CHANGE", "STATE_CHANGE_CANDIDATE", "STATE_PROPOSAL_READY",
    "STATE_BLOCKED_DATA", "STATE_BLOCKED_EVIDENCE", "STATE_MANUAL_REVIEW",
    "STATE_NOT_RUN", "STATE_UNAVAILABLE", "STATE_STALE",
    "EXECUTION_ACTIVE_STATES",
    "resolve_policy", "declare_inputs", "build_input_contract", "run_reassessment",
    "artifact_identity", "artifact_id_for", "persist_reassessment", "load_latest_artifact",
    "run_and_persist", "should_build_proposal", "proposal_binding", "proposal_is_current_for",
    "execution_precedence", "build_presentation", "load_portfolio_reassessment",
    "load_reassessment_summary", "load_reassessment_history", "load_history",
    "load_latest_assessment_pointer",
    "recent_change_rows", "build_attribution", "build_decision_scope",
    "economic_currency", "hoc_corporate_actions_hash",
    # Release 54.2 — same-session assessment versioning.
    "ASSESSMENT_EVIDENCE_IDENTITY_VERSION", "ASSESSMENT_EVIDENCE_COMPONENTS",
    "PERSIST_CREATED", "PERSIST_REUSED", "PERSIST_ECONOMIC_VERSION",
    "PERSIST_ASSESSMENT_VERSION", "PERSIST_CONFLICT", "PERSIST_INCONSISTENT",
    "declared_inputs_fingerprint", "assessment_evidence_identity",
    "assessment_evidence_hash", "decision_fingerprint",
    "authoritative_history_rows", "load_artifact_versions", "load_artifact_by_id",
]
