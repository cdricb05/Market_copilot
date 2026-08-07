r"""Phase 29I Slice 8 — Persistent Alpha Research Agent composition, persistence & read owner.

This is the ONE canonical orchestration / validation / immutable-artifact / read-contract
owner for the Persistent Alpha Research Agent (Consolidation Roadmap Slice 8 / Charter
Milestone 4). It performs NO research-state evaluation of its own — the single canonical
evaluation lives in ``engine.research_agent``. This module only:

  1. SOURCES an immutable point-in-time research-evidence contract from the authoritative
     research/model evidence owners (``api.universe_scoring`` for champion / challenger
     identity + cross-sectional ranking coverage; ``api.forward_prediction_skill`` for
     matured TRUE_FORWARD rank IC / decile spread / observation counts;
     ``api.paper_trading_desk`` + ``api.forward_evidence`` for realized return / benchmark
     excess / drawdown / turnover / cost; ``api.current_alpha_tournament`` for challenger
     evidence; ``api.current_alpha_decision_gate`` for the authoritative minimum
     forward-observation threshold; the Slice 6 ``api.holding_opportunity_cost`` and
     Slice 7 ``api.reallocation_proposal`` immutable artifact histories; and
     ``api.portfolio_state`` for the active book / eligible session / sector state). Every
     metric is READ from its existing owner — none is re-derived here.
  2. RUNS the pure kernel.
  3. PERSISTS a completed assessment as an immutable artifact under a dedicated research
     root (atomic write, index/manifest, idempotent identical rerun; an assessment from a
     DIFFERENT evidence hash for the same book+date SUPERSEDES — never silently reuses —
     the stale assessment, keeping every artifact immutable on disk). It NEVER writes an
     operational ledger, PostgreSQL, a champion pointer, an operational / alpha target, an
     order, a fill, a holding, cash or NAV; it promotes / recalibrates / retrains no model.
  4. Exposes ONE read contract for ``GET /v1/research/research-agent`` (read-only) plus a
     compact summary the Daily Research Cycle manifest / workflow state read.

The sole NORMAL execution path is the Daily Research Cycle
(``POST /v1/operations/daily-research-cycle/run``) via the ``RUN_RESEARCH_AGENT`` step;
there is deliberately NO create / promote / recalibrate / retrain / apply endpoint. This
module is research-only, preview-first, paper-only, manual-review: it recommends bounded
research actions but takes none, and enables no automation.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import research_agent as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
POLICY_VERSION = kernel.POLICY_VERSION
RESEARCH_STATE_VOCAB = kernel.RESEARCH_STATE_VOCAB
COMPOSITION_OWNER = "api.research_agent"
PHASE = "29I-Slice8"

# Read-layer states extend the kernel's research states.
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NOT_RUN = "NOT_RUN"
STATE_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = tuple(list(RESEARCH_STATE_VOCAB) + [STATE_NOT_RUN, STATE_NO_ACTIVE_BOOK,
                                                       STATE_UNAVAILABLE])

# --- immutable artifact root (configurable; a research / decision-evidence root, ------ #
# NEVER the operational ledger root). ----------------------------------------------- #
RESEARCH_AGENT_DIR_ENV = "PAPER_TRADER_RESEARCH_AGENT_DIR"
# NOTE: a distinct root from the alpha_agent research subsystem's existing
# ``D:\Stock_Prediction_app_data\research_agent`` directory — the Slice 8 assessment
# artifacts must not pollute (or be confused with) that subsystem's runs.
_DEFAULT_RESEARCH_AGENT_DIR = Path(r"D:\Stock_Prediction_app_data\research_agent_assessments")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"

# Fallback research-evidence roots for the Slice 6 / Slice 7 immutable histories. The DRC
# and the test suite always pass EXPLICIT dirs; these mirror the canonical Slice 6/7
# defaults and are used only when no explicit dir is supplied (a pure evidence read).
_HOC_DIR_ENV = "PAPER_TRADER_HOC_DIR"
_DEFAULT_HOC_DIR = Path(r"D:\Stock_Prediction_app_data\holding_opportunity_cost")
_REALLOC_DIR_ENV = "PAPER_TRADER_REALLOC_DIR"
_DEFAULT_REALLOC_DIR = Path(r"D:\Stock_Prediction_app_data\reallocation_proposals")


# --------------------------------------------------------------------------- #
# Time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _agent_dir(research_agent_dir=None) -> Path:
    if research_agent_dir is not None:
        return Path(research_agent_dir)
    env = os.environ.get(RESEARCH_AGENT_DIR_ENV)
    return Path(env) if env else _DEFAULT_RESEARCH_AGENT_DIR


def _artifacts_dir(research_agent_dir=None) -> Path:
    return _agent_dir(research_agent_dir) / _ARTIFACTS_SUBDIR


def _index_path(research_agent_dir=None) -> Path:
    return _agent_dir(research_agent_dir) / _INDEX_FILE


def _resolve_evidence_dir(explicit, env_name: str, default: Path) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get(env_name)
    return Path(env) if env else default


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic write: a temp file in the same dir then ``os.replace`` (interrupted writes
    leave the prior file intact and only an orphan .tmp, never a partial)."""
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
# Policy (reuse authoritative governance thresholds — never forked)
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    """Override the genuinely-new policy defaults with any authoritative governance
    threshold that already has an owner, so no governance number is silently forked."""
    ov: dict[str, Any] = {}
    try:
        from paper_trader.api import current_alpha_decision_gate as dg
        mfo = getattr(dg, "MIN_FORWARD_OBS", None)
        if isinstance(mfo, (int, float)) and mfo > 0:
            ov.update({"min_forward_observations": int(mfo),
                       "recalibration_min_forward_obs": int(mfo),
                       "challenger_min_obs": int(mfo)})
    except Exception:  # noqa: BLE001 — degrade to kernel defaults
        pass
    return ov


def resolve_policy(policy_overrides: Optional[dict] = None) -> dict:
    pol = dict(kernel.default_policy())
    pol.update(_live_policy_overrides())
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


# --------------------------------------------------------------------------- #
# Evidence extractors (defensive — every field is read from an existing owner and
# degrades to a documented gap; never re-derives a metric).
# --------------------------------------------------------------------------- #
def _champion_from_scoring(scoring: dict) -> dict:
    sc = scoring or {}
    return {
        "model_id": sc.get("champion_model_id"),
        "primary_model_id": sc.get("primary_model_id"),
        "strategy_version": sc.get("strategy_version"),
        "model_registry_version": sc.get("model_registry_version"),
        "automatic_promotion_allowed": bool(sc.get("automatic_promotion_allowed", False)),
    }


def _ranking_quality_from_cells(prediction_cells: Optional[list],
                                active_model_id: Optional[str],
                                prediction_summary: Optional[dict]) -> tuple[dict, int, str]:
    """Aggregate the active model's matured forward rank-IC / decile-spread cells.

    Returns ``(ranking_quality, forward_observation_count, evidence_state)``. All values
    are read verbatim from ``api.forward_prediction_skill`` cells — nothing re-derived.
    """
    cells = [c for c in (prediction_cells or [])
             if not active_model_id or c.get("model_id") == active_model_id]
    ic_vals = [_f(c.get("ic_mean")) for c in cells if _f(c.get("ic_mean")) is not None]
    pos_vals = [_f(c.get("ic_positive_rate_pct")) for c in cells
                if _f(c.get("ic_positive_rate_pct")) is not None]
    spread_vals = [_f(c.get("top_minus_bottom_pp")) for c in cells
                   if _f(c.get("top_minus_bottom_pp")) is not None]
    ic_obs = max([kernel._i(c.get("ic_observation_count")) for c in cells] or [0])
    spread_obs = max([kernel._i(c.get("matured_observation_count")) for c in cells] or [0])
    by_horizon = {str(c.get("horizon_eligible_closes")): _f(c.get("ic_mean")) for c in cells}
    rq = {
        "rank_ic_mean": kernel._mean(ic_vals),
        "rank_ic_median": None,
        "rank_ic_positive_rate_pct": kernel._mean(pos_vals),
        "rank_ic_obs": ic_obs,
        "rank_ic_recent_mean": None,   # split-window trend unavailable from cells (honest gap)
        "rank_ic_prior_mean": None,
        "decile_spread_pp": kernel._mean(spread_vals),
        "decile_spread_obs": spread_obs,
        "rank_ic_by_horizon": by_horizon,
        "evidence_state": (prediction_summary or {}).get("evidence_state"),
    }
    fwd_obs = kernel._i((prediction_summary or {}).get("matured_outcome_count"))
    if not fwd_obs:
        fwd_obs = spread_obs
    return rq, fwd_obs, (prediction_summary or {}).get("evidence_state")


def _performance_from_owners(performance: Optional[dict], rolling: Optional[dict],
                             decision_gate: Optional[dict]) -> tuple[dict, int]:
    """Realized performance read verbatim from ``api.paper_trading_desk.load_performance``
    (+ ``api.forward_evidence`` rolling turnover and ``api.current_alpha_decision_gate``
    current drawdown). Returns ``(performance_block, eligible_session_count)``."""
    perf = performance or {}
    summary = perf.get("summary") if isinstance(perf.get("summary"), dict) else perf
    rows = perf.get("rows") if isinstance(perf.get("rows"), list) else None
    sess = perf.get("n_rows")
    if sess is None and rows is not None:
        sess = len(rows)
    sess = kernel._i(sess)

    roll = rolling or {}
    windows = roll.get("windows") if isinstance(roll.get("windows"), list) else []
    inception = roll.get("inception") if isinstance(roll.get("inception"), dict) else {}
    avg_turnover = _f(inception.get("avg_daily_turnover"))
    if avg_turnover is None:
        turns = [_f((w or {}).get("avg_daily_turnover")) for w in windows]
        avg_turnover = next((t for t in turns if t is not None), None)
    rolling_out = [{"window": (w or {}).get("window") or (w or {}).get("label"),
                    "excess_return_pct": _f((w or {}).get("excess_return_pct")
                                            or (w or {}).get("cumulative_excess_return_pct")),
                    "max_drawdown_pct": _f((w or {}).get("max_drawdown_pct")),
                    "avg_daily_turnover": _f((w or {}).get("avg_daily_turnover"))}
                   for w in windows]

    dg = decision_gate or {}
    current_dd = _f(dg.get("current_drawdown_pct"))
    if current_dd is None:
        for b in (dg.get("books") or []):
            if _f((b or {}).get("current_drawdown_pct")) is not None:
                current_dd = _f(b.get("current_drawdown_pct"))
                break

    block = {
        "cumulative_return_pct": _f(summary.get("cumulative_return_pct")),
        "benchmark_ticker": summary.get("benchmark_ticker") or "SPY",
        "benchmark_cumulative_return_pct": _f(summary.get("benchmark_cumulative_return_pct")),
        "excess_pct_points": _f(summary.get("excess_vs_benchmark_pct_points")
                                if summary.get("excess_vs_benchmark_pct_points") is not None
                                else summary.get("excess_pct_points")),
        "max_drawdown_pct": _f(summary.get("max_drawdown_pct")),
        "current_drawdown_pct": current_dd,
        "avg_daily_turnover": avg_turnover,
        "total_transaction_cost": _f(summary.get("total_transaction_cost")),
        "eligible_session_count": sess,
        "rolling": rolling_out,
    }
    return block, sess


def _challenger_from_tournament(tournament: Optional[dict], champion: dict) -> dict:
    t = tournament or {}
    if not t:
        return {"available": False}
    books = t.get("book_summaries") or {}

    def _ret(name):
        b = books.get(name) if isinstance(books, dict) else None
        if not isinstance(b, dict):
            return None, 0
        r = _f(b.get("cumulative_return_pct"))
        n = kernel._i(b.get("observation_count") or b.get("n_days") or b.get("n_marks")
                      or b.get("mark_count"))
        return r, n

    comparisons = []
    for tag, champ_book, chal_book in (("top25", "champion_top25", "challenger_top25"),
                                       ("top50", "champion_top50", "challenger_top50")):
        cr, cn = _ret(champ_book)
        hr, hn = _ret(chal_book)
        if cr is None or hr is None:
            continue
        comparisons.append({"book": tag, "champion_return_pct": cr,
                            "challenger_return_pct": hr,
                            "excess_pp": round(hr - cr, 4),
                            "observation_count": min(cn, hn) if (cn and hn) else max(cn, hn)})
    scr = t.get("sector_repaired_paper_challenger") or {}
    return {
        "available": bool(comparisons),
        "signal": scr.get("signal") or t.get("challenger_signal"),
        "champion_signal": champion.get("model_id"),
        "book_comparisons": comparisons,
        "decision": (t.get("decision") or {}).get("decision") if isinstance(t.get("decision"), dict)
        else t.get("decision"),
    }


def _sector_from_state(portfolio_state: Optional[dict], reallocation: Optional[dict],
                       reallocation_history: list) -> dict:
    ps = portfolio_state or {}
    positions = ps.get("positions") or []
    sector_w: dict[str, float] = {}
    for p in positions:
        sec = p.get("sector") or "Unknown"
        w = _f(p.get("portfolio_weight"))
        if w is not None:
            sector_w[sec] = sector_w.get(sec, 0.0) + w
    max_sec = max(sector_w.items(), key=lambda kv: kv[1]) if sector_w else (None, None)
    # A concentration series from the reallocation-proposal risk history (owned metric).
    series = []
    for r in reallocation_history:
        v = _f((r or {}).get("sector_concentration"))
        if v is not None:
            series.append(v)
    if reallocation:
        v = _f(((reallocation.get("risk") or {}) if isinstance(reallocation, dict) else {})
               .get("sector_concentration_after"))
        if v is not None:
            series.append(v)
    return {
        "max_sector_weight": max_sec[1],
        "max_sector": max_sec[0],
        "sector_weights": {k: round(v, 6) for k, v in sorted(sector_w.items())},
        "sector_concentration_series": series,
    }


# --------------------------------------------------------------------------- #
# Slice 6 / Slice 7 immutable-history enumeration (pure evidence reads).
# --------------------------------------------------------------------------- #
def _iter_index_history(index_dir: Path, active_book_id: Optional[str]) -> list[dict]:
    idx = _load_json(index_dir / _INDEX_FILE) or {}
    rows = []
    for _key, entry in idx.items():
        if not isinstance(entry, dict):
            continue
        if active_book_id and entry.get("active_book_id") not in (None, active_book_id):
            continue
        path = entry.get("path")
        art = _load_json(Path(path)) if path else None
        if art is None:
            continue
        rows.append((entry.get("eligible_market_date"), entry, art))
    rows.sort(key=lambda t: t[0] or "")
    return rows


def iter_hoc_history(*, active_book_id: Optional[str], hoc_dir=None) -> list[dict]:
    """Read the Slice 6 immutable HOC assessment history (a pure evidence read of the
    owner's immutable artifacts; enumerates the index, never runs the HOC engine)."""
    root = _resolve_evidence_dir(hoc_dir, _HOC_DIR_ENV, _DEFAULT_HOC_DIR)
    out = []
    for date, _entry, art in _iter_index_history(root, active_book_id):
        assess = art.get("assessment") or {}
        out.append({
            "date": date,
            "state": assess.get("assessment_state") or assess.get("state"),
            "recommendation_counts": assess.get("recommendation_counts") or {},
            "hash": (art.get("identity") or {}).get("assessment_hash")
            or assess.get("assessment_hash"),
        })
    return out


def iter_reallocation_history(*, active_book_id: Optional[str], reallocation_dir=None) -> list[dict]:
    """Read the Slice 7 immutable Reallocation Proposal history (a pure evidence read of
    the owner's immutable artifacts; enumerates the index, never runs the engine)."""
    root = _resolve_evidence_dir(reallocation_dir, _REALLOC_DIR_ENV, _DEFAULT_REALLOC_DIR)
    out = []
    for date, _entry, art in _iter_index_history(root, active_book_id):
        prop = art.get("proposal") or {}
        sig = prop.get("signal") or {}
        trn = prop.get("turnover") or {}
        risk = prop.get("risk") or {}
        out.append({
            "date": date,
            "state": prop.get("proposal_state"),
            "action_counts": prop.get("action_counts") or {},
            "one_way_turnover": trn.get("one_way_turnover"),
            "score_improvement": sig.get("score_improvement"),
            "score_improvement_net_of_cost": sig.get("score_improvement_net_of_cost"),
            "sector_concentration": risk.get("sector_concentration_after"),
            "proposed_holding_count": (prop.get("portfolio") or {}).get("proposed_holding_count"),
            "hash": prop.get("proposal_hash"),
        })
    return out


# --------------------------------------------------------------------------- #
# Input-contract sourcing
# --------------------------------------------------------------------------- #
def build_input_contract(*, portfolio_state: dict, scoring: dict,
                         prediction_cells: Optional[list] = None,
                         prediction_summary: Optional[dict] = None,
                         performance: Optional[dict] = None,
                         rolling_evidence: Optional[dict] = None,
                         tournament: Optional[dict] = None,
                         decision_gate: Optional[dict] = None,
                         hoc_history: Optional[list] = None,
                         reallocation_history: Optional[list] = None,
                         reallocation: Optional[dict] = None,
                         regime: Optional[dict] = None,
                         prior_artifact: Optional[dict] = None,
                         source_gaps: Optional[list] = None) -> dict:
    """Assemble the immutable point-in-time research-evidence contract.

    Every metric is read from its authoritative owner (or explicitly injected for tests);
    nothing is re-derived, backfilled from current snapshots, or reconstructed with
    hindsight. Missing evidence is carried through as a documented gap.
    """
    ps = portfolio_state or {}
    sc = scoring or {}
    eligible = ((ps.get("dates") or {}).get("eligible_market_date"))
    active_book = ps.get("active_book") or {}
    active_book_id = active_book.get("book_id")

    champion = _champion_from_scoring(sc)
    active_model = champion.get("primary_model_id") or (prediction_summary or {}).get(
        "active_model_id") or champion.get("model_id")

    rq, fwd_obs, ev_state = _ranking_quality_from_cells(
        prediction_cells, active_model, prediction_summary)
    perf, sess = _performance_from_owners(performance, rolling_evidence, decision_gate)
    hoc_hist = list(hoc_history or [])
    realloc_hist = list(reallocation_history or [])
    sector = _sector_from_state(ps, reallocation, realloc_hist)
    challenger = _challenger_from_tournament(tournament, champion)

    # Cross-sectional ranking coverage (from the canonical scoring contract).
    rankings = sc.get("rankings") or []
    ranking_obs = len(rankings)
    coverage = _f(sc.get("coverage_ratio"))

    reg = dict(regime or {})
    if "available" not in reg:
        reg["available"] = bool(reg)

    # Checkpoint marker carried from the prior immutable assessment (deterministic).
    last_ckpt = _checkpoint_marker(prior_artifact)

    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": active_book_id,
        "active_book_label": active_book.get("book_label"),
        "champion": champion,
        "champion_identity_hash": sc.get("output_hash") or sc.get("input_contract_hash"),
        "universe_scoring_hash": sc.get("output_hash"),
        "portfolio_state_hash": ps.get("state_hash"),
        "forward_evidence_hash": (prediction_summary or {}).get("latest_snapshot_date"),
        "evidence": {
            "forward_observation_count": fwd_obs,
            "eligible_session_count": sess,
            "ranking_observation_count": ranking_obs,
            "universe_coverage_ratio": coverage,
            "reallocation_observation_count": len(realloc_hist),
            "hoc_observation_count": len(hoc_hist),
            "regime_observation_count": kernel._i(reg.get("observation_count")),
            "distinct_regime_count": kernel._i(reg.get("distinct_count")),
            "evidence_start": _first_date(hoc_hist, realloc_hist),
            "evidence_end": eligible,
        },
        "ranking_quality": rq,
        "performance": perf,
        "sector": sector,
        "regime": reg,
        "hoc_history": hoc_hist,
        "reallocation_history": realloc_hist,
        "challenger": challenger,
        "last_checkpoint_forward_observation_count": last_ckpt,
        "source_gaps": list(source_gaps or []),
    }


def _first_date(hoc_hist: list, realloc_hist: list) -> Optional[str]:
    dates = [h.get("date") for h in hoc_hist if h.get("date")] + \
            [r.get("date") for r in realloc_hist if r.get("date")]
    return min(dates) if dates else None


def _checkpoint_marker(prior_artifact: Optional[dict]) -> int:
    if not prior_artifact:
        return 0
    prior = (prior_artifact.get("assessment") or {})
    recal = prior.get("recalibration") or {}
    prior_state = recal.get("state")
    prior_fwd = kernel._i(recal.get("forward_observation_count"))
    prior_marker = kernel._i(recal.get("last_checkpoint_forward_observation_count"))
    # The checkpoint marker advances to the forward-obs count only when a checkpoint /
    # recalibration actually fired; otherwise it carries the prior marker forward.
    if prior_state in (kernel.RECAL_RECOMMENDED, kernel.RECAL_CHECKPOINT_DUE):
        return prior_fwd
    return prior_marker


# --------------------------------------------------------------------------- #
# Default source loaders (injectable seams; all READ-ONLY existing owners)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader() -> dict:
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_prediction_cells_loader(desk_dir=None) -> Optional[list]:
    from paper_trader.api import forward_prediction_skill as fps
    return (fps.build_prediction_skill(desk_dir=desk_dir) or {}).get("cells")


def _default_prediction_summary_loader(desk_dir=None) -> Optional[dict]:
    from paper_trader.api import forward_prediction_skill as fps
    return fps.prediction_skill_summary(desk_dir=desk_dir)


def _default_performance_loader(desk_dir=None) -> Optional[dict]:
    from paper_trader.api import paper_trading_desk as desk
    return desk.load_performance(desk_dir=desk_dir)


def _default_rolling_evidence_loader(desk_dir=None) -> Optional[dict]:
    from paper_trader.api import forward_evidence as fe
    return fe.build_rolling_evidence(desk_dir=desk_dir)


def _default_tournament_loader() -> Optional[dict]:
    from paper_trader.api import current_alpha_tournament as tour
    return tour.load_current_alpha_tournament()


def _default_decision_gate_loader() -> Optional[dict]:
    from paper_trader.api import current_alpha_decision_gate as dg
    return dg.load_current_alpha_decision_gate()


def _active_model_id() -> Optional[str]:
    try:
        from paper_trader.api import forward_prediction_skill as fps
        return getattr(fps, "ACTIVE_MODEL_ID", None)
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_assessment(*, input_contract: Optional[dict] = None,
                   portfolio_state: Optional[dict] = None,
                   scoring: Optional[dict] = None,
                   prediction_cells: Optional[list] = None,
                   prediction_summary: Optional[dict] = None,
                   performance: Optional[dict] = None,
                   rolling_evidence: Optional[dict] = None,
                   tournament: Optional[dict] = None,
                   decision_gate: Optional[dict] = None,
                   hoc_history: Optional[list] = None,
                   reallocation_history: Optional[list] = None,
                   reallocation: Optional[dict] = None,
                   regime: Optional[dict] = None,
                   prior_artifact: Optional[dict] = None,
                   policy: Optional[dict] = None,
                   hoc_dir=None, reallocation_dir=None, desk_dir=None,
                   research_agent_dir=None,
                   portfolio_state_loader: Optional[Callable] = None,
                   scoring_loader: Optional[Callable] = None) -> dict:
    """Build the research-evidence contract (unless supplied) and run the pure kernel.

    Returns ``{"input_contract": ..., "assessment": <kernel result>}``. Read-only:
    persists nothing. Every source is injectable; only the fields not injected are loaded
    from their existing READ owner.
    """
    pol = policy or resolve_policy()
    if input_contract is None:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
        sc = scoring if scoring is not None else (
            (scoring_loader or _default_scoring_loader)())
        active_book_id = (ps.get("active_book") or {}).get("book_id")

        def _safe(fn, default=None):
            try:
                return fn()
            except Exception:  # noqa: BLE001 — a missing evidence owner is a gap, not a crash
                return default

        cells = prediction_cells if prediction_cells is not None else _safe(
            lambda: _default_prediction_cells_loader(desk_dir=desk_dir))
        summ = prediction_summary if prediction_summary is not None else _safe(
            lambda: _default_prediction_summary_loader(desk_dir=desk_dir))
        perf = performance if performance is not None else _safe(
            lambda: _default_performance_loader(desk_dir=desk_dir))
        roll = rolling_evidence if rolling_evidence is not None else _safe(
            lambda: _default_rolling_evidence_loader(desk_dir=desk_dir))
        tour = tournament if tournament is not None else _safe(_default_tournament_loader)
        gate = decision_gate if decision_gate is not None else _safe(_default_decision_gate_loader)
        hoc_hist = hoc_history if hoc_history is not None else _safe(
            lambda: iter_hoc_history(active_book_id=active_book_id, hoc_dir=hoc_dir), [])
        realloc_hist = reallocation_history if reallocation_history is not None else _safe(
            lambda: iter_reallocation_history(active_book_id=active_book_id,
                                              reallocation_dir=reallocation_dir), [])

        input_contract = build_input_contract(
            portfolio_state=ps, scoring=sc, prediction_cells=cells, prediction_summary=summ,
            performance=perf, rolling_evidence=roll, tournament=tour, decision_gate=gate,
            hoc_history=hoc_hist, reallocation_history=realloc_hist,
            reallocation=reallocation, regime=regime, prior_artifact=prior_artifact)
    assessment = kernel.evaluate(input_contract=input_contract, policy=pol)
    return {"input_contract": input_contract, "assessment": assessment}


# --------------------------------------------------------------------------- #
# Persist (immutable artifact)
# --------------------------------------------------------------------------- #
def assessment_identity(*, input_contract: dict, result: dict) -> dict:
    return {
        "eligible_market_date": input_contract.get("eligible_market_date"),
        "active_book_id": input_contract.get("active_book_id"),
        "champion_model_id": (input_contract.get("champion") or {}).get("model_id"),
        "universe_scoring_hash": input_contract.get("universe_scoring_hash"),
        "portfolio_state_hash": input_contract.get("portfolio_state_hash"),
        "forward_evidence_hash": input_contract.get("forward_evidence_hash"),
        "policy_version": POLICY_VERSION,
        "assessment_hash": result.get("assessment_hash"),
    }


def assessment_id_for(identity: dict) -> str:
    book = (identity.get("active_book_id") or "book")
    date = (identity.get("eligible_market_date") or "nodate")
    h = (identity.get("assessment_hash") or "")[:12]
    return "raga_%s_%s_%s" % (date, book, h)


def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


def _compact_input_contract(ic: dict) -> dict:
    ev = ic.get("evidence") or {}
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "active_book_label": ic.get("active_book_label"),
        "champion": ic.get("champion"),
        "champion_identity_hash": ic.get("champion_identity_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "forward_evidence_hash": ic.get("forward_evidence_hash"),
        "evidence": ev,
        "hoc_history_count": len(ic.get("hoc_history") or []),
        "reallocation_history_count": len(ic.get("reallocation_history") or []),
        "policy_version": POLICY_VERSION,
    }


def persist_assessment(*, result: dict, input_contract: dict, research_agent_dir=None,
                       now: Optional[datetime] = None) -> dict:
    """Persist a completed assessment as an immutable artifact.

    Idempotency contract:
      * identical inputs (same ``assessment_hash``) reuse the existing artifact;
      * a DIFFERENT evidence hash for the same book+date SUPERSEDES — the fresh assessment
        is written to its own immutable artifact and the index pointer advanced; the stale
        assessment is NEVER silently reused. Every artifact on disk is immutable (the
        artifact_id embeds the assessment hash).

    Every non-BLOCKED research state is persistable (INSUFFICIENT_EVIDENCE is a meaningful,
    durable result). BLOCKED is not persisted (missing core inputs; nothing durable).
    """
    state = result.get("research_state")
    if state == STATE_BLOCKED or not state:
        return {"status": "NOT_PERSISTED", "reason": "STATE_%s_NOT_PERSISTABLE" % state,
                "assessment_id": None, "persisted": False, "reused": False,
                "superseded": False}

    identity = assessment_identity(input_contract=input_contract, result=result)
    aid = assessment_id_for(identity)
    key = _index_key(identity["active_book_id"], identity["eligible_market_date"])
    index = _load_json(_index_path(research_agent_dir)) or {}
    existing = index.get(key)

    if existing and existing.get("assessment_hash") == identity["assessment_hash"]:
        return {"status": "REUSED_EXISTING", "assessment_id": existing.get("assessment_id"),
                "path": existing.get("path"), "persisted": True, "reused": True,
                "superseded": False, "identity": identity}

    superseded_id = existing.get("assessment_id") if existing else None

    payload = {
        "assessment_id": aid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "identity": identity,
        "supersedes_assessment_id": superseded_id,
        "input_contract": _compact_input_contract(input_contract),
        "assessment": result,
    }
    path = _artifacts_dir(research_agent_dir) / ("%s.json" % aid)
    _atomic_write_json(path, payload)
    index[key] = {"assessment_id": aid, "path": str(path),
                  "assessment_hash": identity["assessment_hash"],
                  "research_state": state,
                  "eligible_market_date": identity["eligible_market_date"],
                  "active_book_id": identity["active_book_id"],
                  "generated_at": payload["generated_at"]}
    _atomic_write_json(_index_path(research_agent_dir), index)
    return {"status": ("SUPERSEDED_PRIOR" if superseded_id else "CREATED"),
            "assessment_id": aid, "path": str(path), "persisted": True, "reused": False,
            "superseded": bool(superseded_id), "superseded_assessment_id": superseded_id,
            "identity": identity}


def load_latest_artifact(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str],
                         research_agent_dir=None) -> Optional[dict]:
    """Load the current persisted assessment for an exact (active book, eligible date)."""
    index = _load_json(_index_path(research_agent_dir)) or {}
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None:
        art = _load_json(_artifacts_dir(research_agent_dir)
                         / ("%s.json" % entry.get("assessment_id")))
    return art


def latest_prior_artifact(*, active_book_id: Optional[str],
                          eligible_market_date: Optional[str],
                          research_agent_dir=None) -> Optional[dict]:
    """The most recent persisted assessment for the book with an eligible date STRICTLY
    BEFORE ``eligible_market_date``. Used only for the deterministic checkpoint marker so
    that re-running the same session is idempotent (it never reads the same-date artifact,
    which would change the assessment on the second run)."""
    index = _load_json(_index_path(research_agent_dir)) or {}
    prior_key = None
    prior_date = None
    for key, entry in index.items():
        if not isinstance(entry, dict):
            continue
        if active_book_id and entry.get("active_book_id") not in (None, active_book_id):
            continue
        d = entry.get("eligible_market_date")
        if not d or (eligible_market_date and d >= eligible_market_date):
            continue
        if prior_date is None or d > prior_date:
            prior_date, prior_key = d, key
    if prior_key is None:
        return None
    entry = index.get(prior_key) or {}
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None:
        art = _load_json(_artifacts_dir(research_agent_dir)
                         / ("%s.json" % entry.get("assessment_id")))
    return art


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None,
                    prediction_cells: Optional[list] = None,
                    prediction_summary: Optional[dict] = None,
                    performance: Optional[dict] = None,
                    rolling_evidence: Optional[dict] = None,
                    tournament: Optional[dict] = None,
                    decision_gate: Optional[dict] = None,
                    hoc_history: Optional[list] = None,
                    reallocation_history: Optional[list] = None,
                    reallocation: Optional[dict] = None,
                    regime: Optional[dict] = None,
                    policy: Optional[dict] = None,
                    research_agent_dir=None, hoc_dir=None, reallocation_dir=None,
                    desk_dir=None, now: Optional[datetime] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None) -> dict:
    """The Daily Research Cycle entry: build -> kernel -> persist (idempotent).

    Reads the prior immutable assessment (for the deterministic checkpoint marker) before
    running, then persists the fresh assessment.
    """
    pol = policy or resolve_policy()
    # Resolve (book, eligible) first so we can read the prior artifact + histories.
    ps = portfolio_state if portfolio_state is not None else (
        (portfolio_state_loader or _default_portfolio_state_loader)())
    active_book_id = (ps.get("active_book") or {}).get("book_id")
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    # The checkpoint marker is read from a STRICTLY EARLIER session so that re-running the
    # same eligible session stays idempotent (identical evidence -> identical assessment).
    prior = latest_prior_artifact(active_book_id=active_book_id, eligible_market_date=eligible,
                                  research_agent_dir=research_agent_dir)

    run = run_assessment(
        portfolio_state=ps, scoring=scoring, prediction_cells=prediction_cells,
        prediction_summary=prediction_summary, performance=performance,
        rolling_evidence=rolling_evidence, tournament=tournament, decision_gate=decision_gate,
        hoc_history=hoc_history, reallocation_history=reallocation_history,
        reallocation=reallocation, regime=regime, prior_artifact=prior, policy=pol,
        hoc_dir=hoc_dir, reallocation_dir=reallocation_dir, desk_dir=desk_dir,
        research_agent_dir=research_agent_dir, scoring_loader=scoring_loader)
    persist = persist_assessment(result=run["assessment"], input_contract=run["input_contract"],
                                 research_agent_dir=research_agent_dir, now=now)
    return {"input_contract": run["input_contract"], "assessment": run["assessment"],
            "persistence": persist}


# --------------------------------------------------------------------------- #
# Read contract — GET /v1/research/research-agent
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
                  policy: dict, assessment: Optional[dict]) -> dict:
    a = assessment or {}
    art_meta = None
    if artifact:
        art_meta = {"assessment_id": artifact.get("assessment_id"),
                    "generated_at": artifact.get("generated_at"),
                    "identity": artifact.get("identity"),
                    "supersedes_assessment_id": artifact.get("supersedes_assessment_id"),
                    "immutable": True, "root_env": RESEARCH_AGENT_DIR_ENV}
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": state,
        "research_state": a.get("research_state"),
        "state_vocabulary": list(READ_STATE_VOCAB),
        "message": message,
        "eligible_market_date": eligible,
        "active_book": active_book,
        "policy": a.get("policy") or policy,
        "policy_version": a.get("policy_version") or POLICY_VERSION,
        "state_reason_codes": a.get("state_reason_codes") or [],
        "champion": a.get("champion") or {},
        "evidence_window": a.get("evidence_window") or {},
        "evidence_quality": a.get("evidence_quality") or {},
        "champion_health": a.get("champion_health") or {},
        "degradation": a.get("degradation") or {},
        "forward_performance": a.get("forward_performance") or {},
        "ranking_quality": a.get("ranking_quality") or {},
        "benchmark_relative": a.get("benchmark_relative") or {},
        "turnover_cost": a.get("turnover_cost") or {},
        "drawdown": a.get("drawdown") or {},
        "regime": a.get("regime") or {},
        "sector": a.get("sector") or {},
        "hoc_diagnostics": a.get("hoc_diagnostics") or {},
        "reallocation_diagnostics": a.get("reallocation_diagnostics") or {},
        "challenger": a.get("challenger") or {},
        "recalibration": a.get("recalibration") or {},
        "research_opportunities": a.get("research_opportunities") or [],
        "manual_review": a.get("manual_review") or {},
        "data_gaps": a.get("data_gaps") or [],
        "artifact": art_meta,
        "assessment_hash": a.get("assessment_hash"),
        "provenance": a.get("provenance")
        or {"composition_owner": COMPOSITION_OWNER, "calculation_owner": kernel.CALCULATION_OWNER},
        "safety": a.get("safety") or kernel._safety(),
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
        "research_only": True,
    }


def load_research_agent(*, portfolio_state: Optional[dict] = None,
                        artifact: Optional[dict] = None, research_agent_dir=None,
                        now: Optional[datetime] = None,
                        portfolio_state_loader: Optional[Callable] = None) -> dict:
    """The read contract for the endpoint. READ-ONLY: it NEVER runs the engine — it returns
    the current persisted assessment for the active book + eligible date. When no
    production artifact exists it returns a readable ``NOT_RUN`` payload (the sole execution
    path is the Daily Research Cycle). Always degrade-safe."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return _read_payload(state=STATE_UNAVAILABLE, generated_at=generated_at, eligible=None,
                             active_book={}, artifact=None,
                             message="Portfolio state is unavailable: %s" % str(exc)[:160],
                             policy=resolve_policy(), assessment=None)

    active_book = _active_book_block(ps)
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    book_id = active_book.get("book_id")

    if not book_id:
        return _read_payload(state=STATE_NO_ACTIVE_BOOK, generated_at=generated_at,
                             eligible=eligible, active_book=active_book, artifact=None,
                             message="No active operational book; no research assessment.",
                             policy=resolve_policy(), assessment=None)

    art = artifact if artifact is not None else load_latest_artifact(
        active_book_id=book_id, eligible_market_date=eligible,
        research_agent_dir=research_agent_dir)
    if not art:
        return _read_payload(
            state=STATE_NOT_RUN, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=None,
            message=("No research-agent assessment has been produced for the current active "
                     "book and eligible session yet. Run the Daily Research Cycle "
                     "(POST /v1/operations/daily-research-cycle/run) to produce one."),
            policy=resolve_policy(), assessment=None)

    assessment = art.get("assessment") or {}
    return _read_payload(
        state=assessment.get("research_state") or STATE_UNAVAILABLE, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art,
        message="Current research-agent assessment for the active book / eligible session. "
                "Research governance only — manual review required; no model promotion, no "
                "portfolio mutation, no orders, automation off.",
        policy=assessment.get("policy") or resolve_policy(), assessment=assessment)


# --------------------------------------------------------------------------- #
# Compact summary the Daily Research Cycle manifest / workflow state read
# --------------------------------------------------------------------------- #
def load_research_agent_summary(*, active_book_id: Optional[str] = None,
                                eligible_market_date: Optional[str] = None,
                                artifact: Optional[dict] = None,
                                research_agent_dir=None) -> dict:
    """A compact, read-only research-agent summary. PURE ARTIFACT READER — it reads ONLY the
    immutable assessment index/artifact for the exact ``(active_book_id, eligible_market_date)``
    the caller supplies; it never loads portfolio state, never runs the engine, and never
    calls a provider / prediction. ``available=False`` with a NOT_RUN state when no artifact
    exists."""
    empty = {
        "research_agent_available": False,
        "research_agent_state": STATE_NOT_RUN,
        "research_agent_hash": None,
        "research_agent_id": None,
        "research_agent_evidence_quality": None,
        "research_agent_recalibration_state": None,
        "research_agent_challenger_state": None,
        "research_agent_degradation_categories": [],
        "research_agent_top_opportunity": None,
        "research_agent_data_gaps": [],
    }
    try:
        art = artifact if artifact is not None else load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            research_agent_dir=research_agent_dir)
    except Exception:  # noqa: BLE001 — a pure artifact read must never crash the caller
        d = dict(empty)
        d["research_agent_state"] = STATE_UNAVAILABLE
        return d
    if not art:
        return empty
    a = art.get("assessment") or {}
    opps = a.get("research_opportunities") or []
    return {
        "research_agent_available": True,
        "research_agent_state": a.get("research_state"),
        "research_agent_hash": a.get("assessment_hash"),
        "research_agent_id": art.get("assessment_id"),
        "research_agent_evidence_quality": (a.get("evidence_quality") or {}).get("state"),
        "research_agent_recalibration_state": (a.get("recalibration") or {}).get("state"),
        "research_agent_challenger_state": (a.get("challenger") or {}).get("state"),
        "research_agent_degradation_categories": (a.get("degradation") or {}).get("categories") or [],
        "research_agent_top_opportunity": (opps[0].get("opportunity_id") if opps else None),
        "research_agent_data_gaps": [g.get("code") for g in (a.get("data_gaps") or [])
                                     if not g.get("by_design")],
    }
