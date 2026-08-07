r"""Phase 29I Slice 8 — Persistent Alpha Research Agent (pure evaluation kernel).

This module is the ONE canonical research-STATE evaluation calculation (Consolidation
Roadmap Slice 8 / Charter Milestone 4). It is a **pure, deterministic kernel**: it
performs NO I/O — no file, database, network, provider or prediction access — and never
mutates its inputs. Everything it needs arrives as one immutable research-evidence input
contract (assembled by ``api.research_agent`` from the authoritative research/model
evidence owners), and it returns one immutable research-assessment result.

From the already-produced daily research evidence it continuously answers *whether the
current research / model stack remains trustworthy and whether bounded research
experiments should be run*. It evaluates, over an explicit point-in-time evidence window:

  * evidence sufficiency (forward TRUE_FORWARD observations, eligible sessions,
    cross-sectional ranking observations, universe coverage, reallocation / HOC
    observations, regime coverage) — a short negative P&L run yields
    INSUFFICIENT_EVIDENCE / WATCH, never a premature RECALIBRATION_DUE;
  * champion health as explained component assessments (forward rank IC + trend, decile
    spread, benchmark-relative excess, drawdown, turnover / cost efficiency, sector
    stability, regime stability, HOC deterioration frequency, reallocation churn, data /
    forward-evidence completeness) with reason codes — never one opaque magic score;
  * model-degradation categories (PERFORMANCE_WEAKNESS / SIGNAL_DEGRADATION /
    RANKING_DEGRADATION / REGIME_DRIFT / SECTOR_INSTABILITY / TURNOVER_INEFFICIENCY /
    PORTFOLIO_STALENESS / DATA_QUALITY_DEGRADATION / INSUFFICIENT_EVIDENCE);
  * HOC (Slice 6) and Reallocation (Slice 7) history as diagnostic feedback that
    distinguishes portfolio staleness / governance latency from model weakness;
  * challenger classification (NOT_EVALUATED / INSUFFICIENT_EVIDENCE / UNDERPERFORMING /
    COMPETITIVE / PROMISING / SUPERIOR_CANDIDATE — never PROMOTED);
  * a deterministic ranked queue of bounded research opportunities, each with a hypothesis,
    supporting evidence, gaps, priority and a fully-specified bounded experiment;
  * a controlled recalibration recommendation gated on evidence.

Everything the agent produces is RESEARCH GOVERNANCE ONLY. The result NEVER promotes a
model, recalibrates or replaces the champion, alters the operational portfolio, changes a
target weight, creates an order or a fill, changes holdings / cash / NAV, enables cadence
or performs broker execution. Manual approval remains mandatory for every recommended
action. The genuinely-new Slice-8 evidence and health thresholds are declared once in
:func:`default_policy`, returned in the payload and folded into the deterministic
``assessment_hash``; where an authoritative governance threshold already exists (e.g. the
decision-gate minimum forward-observation count) the API owner injects it so no threshold
is silently forked.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

SCHEMA_VERSION = "research_agent.v1"
INPUT_SCHEMA_VERSION = "research_agent.input.v1"
POLICY_VERSION = "research_agent_policy.v1"

CALCULATION_OWNER = "engine.research_agent"

# --- Frozen research-state vocabulary ---------------------------------------- #
STATE_HEALTHY = "HEALTHY"
STATE_WATCH = "WATCH"
STATE_INVESTIGATE = "INVESTIGATE"
STATE_RECALIBRATION_DUE = "RECALIBRATION_DUE"
STATE_CHALLENGER_PROMISING = "CHALLENGER_PROMISING"
STATE_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
STATE_BLOCKED = "BLOCKED"
RESEARCH_STATE_VOCAB = (STATE_HEALTHY, STATE_WATCH, STATE_INVESTIGATE,
                        STATE_RECALIBRATION_DUE, STATE_CHALLENGER_PROMISING,
                        STATE_INSUFFICIENT_EVIDENCE, STATE_BLOCKED)

# --- Evidence-quality vocabulary --------------------------------------------- #
EV_SUFFICIENT = "SUFFICIENT"
EV_PRELIMINARY = "PRELIMINARY"
EV_INSUFFICIENT = "INSUFFICIENT"
EVIDENCE_QUALITY_VOCAB = (EV_SUFFICIENT, EV_PRELIMINARY, EV_INSUFFICIENT)

# --- Component-health vocabulary --------------------------------------------- #
H_HEALTHY = "HEALTHY"
H_WATCH = "WATCH"
H_WEAK = "WEAK"
H_UNKNOWN = "UNKNOWN"
COMPONENT_HEALTH_VOCAB = (H_HEALTHY, H_WATCH, H_WEAK, H_UNKNOWN)

# --- Degradation category vocabulary ----------------------------------------- #
DEG_PERFORMANCE = "PERFORMANCE_WEAKNESS"
DEG_SIGNAL = "SIGNAL_DEGRADATION"
DEG_RANKING = "RANKING_DEGRADATION"
DEG_REGIME = "REGIME_DRIFT"
DEG_SECTOR = "SECTOR_INSTABILITY"
DEG_TURNOVER = "TURNOVER_INEFFICIENCY"
DEG_STALENESS = "PORTFOLIO_STALENESS"
DEG_DATA_QUALITY = "DATA_QUALITY_DEGRADATION"
DEG_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
DEGRADATION_VOCAB = (DEG_PERFORMANCE, DEG_SIGNAL, DEG_RANKING, DEG_REGIME, DEG_SECTOR,
                     DEG_TURNOVER, DEG_STALENESS, DEG_DATA_QUALITY, DEG_INSUFFICIENT)

# --- Challenger classification vocabulary ------------------------------------ #
CH_NOT_EVALUATED = "NOT_EVALUATED"
CH_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
CH_UNDERPERFORMING = "UNDERPERFORMING"
CH_COMPETITIVE = "COMPETITIVE"
CH_PROMISING = "PROMISING"
CH_SUPERIOR = "SUPERIOR_CANDIDATE"
CHALLENGER_VOCAB = (CH_NOT_EVALUATED, CH_INSUFFICIENT, CH_UNDERPERFORMING, CH_COMPETITIVE,
                    CH_PROMISING, CH_SUPERIOR)

# --- Recalibration-recommendation vocabulary --------------------------------- #
RECAL_NOT_DUE = "NOT_DUE"
RECAL_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
RECAL_MONITORING = "MONITORING"
RECAL_CHECKPOINT_DUE = "CHECKPOINT_DUE"
RECAL_RECOMMENDED = "RECOMMENDED"
RECAL_VOCAB = (RECAL_NOT_DUE, RECAL_INSUFFICIENT, RECAL_MONITORING, RECAL_CHECKPOINT_DUE,
               RECAL_RECOMMENDED)

# The Research Agent NEVER promotes / retrains / mutates; every recommended action needs
# explicit manual governance approval. This list is embedded in every opportunity.
FORBIDDEN_ACTIONS = (
    "AUTO_PROMOTE_MODEL", "AUTO_RECALIBRATE_CHAMPION", "AUTO_REPLACE_CHAMPION",
    "AUTO_RETRAIN_MODEL", "MUTATE_OPERATIONAL_PORTFOLIO", "CHANGE_TARGET_WEIGHTS",
    "CREATE_ORDERS", "CREATE_FILLS", "MUTATE_HOLDINGS_CASH_NAV", "ENABLE_CADENCE",
    "BROKER_EXECUTION",
)

MANUAL_APPROVAL_PENDING = "PENDING_MANUAL_APPROVAL"


# --------------------------------------------------------------------------- #
# Policy — one explicit, versioned set of evidence + health thresholds.
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned research-governance policy.

    Thresholds marked ``authoritative`` MIRROR an existing governance owner and are
    OVERRIDDEN by the API owner with the live value (e.g. the decision-gate minimum
    forward-observation count / the research experiment gates) so no governance threshold
    is silently forked. Thresholds marked ``new`` are genuinely-new Slice-8 policy,
    justified in docs/ARCHITECTURE_DECISIONS.md; they are declared here once, returned in
    the payload and folded into the assessment hash, and exercised at their boundaries by
    the test suite.
    """
    return {
        "policy_version": POLICY_VERSION,
        # --- evidence-sufficiency gates -------------------------------------- #
        "min_forward_observations": 20,        # authoritative: decision_gate.MIN_FORWARD_OBS
        "preliminary_forward_observations": 5,  # new
        "min_eligible_sessions": 20,           # new
        "min_ranking_observations": 50,        # new (>= 2 * primary book size)
        "min_universe_coverage": 0.60,         # new (mirrors research adequacy floor)
        "min_reallocation_observations": 3,    # new
        "min_hoc_observations": 3,             # new
        "min_regime_observations": 20,         # new
        "min_distinct_regimes": 2,             # new
        # --- champion health: forward rank IC -------------------------------- #
        "rank_ic_watch": 0.0,                  # new: ic_mean below -> WATCH
        "rank_ic_weak": -0.01,                 # new: ic_mean below -> WEAK
        "rank_ic_positive_rate_watch_pct": 45.0,   # new
        "rank_ic_positive_rate_weak_pct": 40.0,    # new
        "rank_ic_trend_drop": 0.02,            # new: (recent - prior) below -drop -> weak trend
        "degradation_min_obs": 10,             # new: obs needed to call rank-IC deg "material"
        # --- champion health: decile spread ---------------------------------- #
        "decile_spread_watch_pp": 0.0,         # new: top-minus-bottom (pct points) below -> WATCH
        "decile_spread_weak_pp": -0.5,         # new
        # --- champion health: realized benchmark-relative -------------------- #
        "excess_watch_pp": -1.0,               # new: excess-vs-benchmark (pct points)
        "excess_weak_pp": -5.0,                # new
        # --- champion health: drawdown --------------------------------------- #
        "drawdown_watch_pct": -8.0,            # new (negative = loss from peak)
        "drawdown_weak_pct": -15.0,            # new
        # --- champion health: turnover --------------------------------------- #
        "turnover_watch": 0.35,                # new: avg daily turnover fraction
        "turnover_weak": 0.60,                 # new
        # --- champion health: HOC / reallocation feedback -------------------- #
        "hoc_exit_replace_watch_rate": 0.25,   # new
        "hoc_exit_replace_weak_rate": 0.50,    # new
        "reallocation_churn_watch": 0.40,      # new
        "reallocation_churn_weak": 0.70,       # new
        "reallocation_strong_improvement": 0.05,  # new: strong net score improvement
        # --- champion health: sector ----------------------------------------- #
        "sector_concentration_watch": 0.30,    # new
        "sector_concentration_weak": 0.40,     # new
        "sector_instability_watch": 0.05,      # new: stdev of max-sector-weight over history
        "sector_instability_weak": 0.10,       # new
        # --- challenger ------------------------------------------------------ #
        "challenger_min_obs": 20,              # authoritative: decision_gate.MIN_FORWARD_OBS
        "challenger_competitive_excess_pp": 0.0,   # new
        "challenger_promising_excess_pp": 2.0,     # new
        "challenger_superior_excess_pp": 5.0,      # new
        # --- recalibration / checkpoint -------------------------------------- #
        "recalibration_min_forward_obs": 20,   # authoritative: decision_gate.MIN_FORWARD_OBS
        "checkpoint_forward_obs_interval": 20,  # new: scheduled research checkpoint cadence
    }


# --------------------------------------------------------------------------- #
# Small numeric helpers (stdlib only, deterministic)
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _i(x: Any) -> int:
    v = _f(x)
    return int(v) if v is not None else 0


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _mean(xs: list) -> Optional[float]:
    vals = [v for v in (_f(x) for x in xs) if v is not None]
    return sum(vals) / len(vals) if vals else None


def _stdev(xs: list) -> Optional[float]:
    vals = [v for v in (_f(x) for x in xs) if v is not None]
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))


# --------------------------------------------------------------------------- #
# Stable hashing (assessment_hash excludes generated_at / volatile keys)
# --------------------------------------------------------------------------- #
_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "loaded_at", "built_at",
                            "assessment_hash", "artifact"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    payload = json.dumps(_strip_volatile(obj), sort_keys=True, default=str,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# A component-health record: one explained sub-assessment (never opaque).
# --------------------------------------------------------------------------- #
def _component(name: str, state: str, *, value=None, threshold=None, obs=None,
               reason_codes=None, detail=None) -> dict:
    return {
        "component": name,
        "state": state,
        "value": value,
        "threshold": threshold,
        "observation_count": obs,
        "reason_codes": sorted(set(reason_codes or [])),
        "detail": detail,
    }


def _worst(states: list) -> str:
    """The worst (most concerning) NON-unknown component state, else UNKNOWN."""
    order = {H_WEAK: 3, H_WATCH: 2, H_HEALTHY: 1, H_UNKNOWN: 0}
    known = [s for s in states if s != H_UNKNOWN]
    if not known:
        return H_UNKNOWN
    return max(known, key=lambda s: order.get(s, 0))


# --------------------------------------------------------------------------- #
# Core entry point
# --------------------------------------------------------------------------- #
def evaluate(*, input_contract: dict, policy: Optional[dict] = None) -> dict:
    """Evaluate the canonical research-state assessment (pure, deterministic).

    ``input_contract`` is the immutable point-in-time research-evidence contract. Returns
    the frozen research-assessment contract. Never raises on incomplete data — it degrades
    to INSUFFICIENT_EVIDENCE / WATCH / BLOCKED with explicit reason codes and data gaps.
    Never promotes, recalibrates, retrains or mutates anything.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    ic = input_contract or {}
    eligible_date = ic.get("eligible_market_date")
    active_book_id = ic.get("active_book_id")
    champion = dict(ic.get("champion") or {})
    ev = dict(ic.get("evidence") or {})
    rq = dict(ic.get("ranking_quality") or {})
    perf = dict(ic.get("performance") or {})
    sector = dict(ic.get("sector") or {})
    regime = dict(ic.get("regime") or {})
    hoc_hist = list(ic.get("hoc_history") or [])
    realloc_hist = list(ic.get("reallocation_history") or [])
    challenger = dict(ic.get("challenger") or {})
    source_gaps = list(ic.get("source_gaps") or [])

    # --- core-input validation -> BLOCKED ------------------------------------ #
    blockers: list[dict] = []
    if not active_book_id:
        blockers.append({"code": "NO_ACTIVE_BOOK",
                         "detail": "No active operational book was supplied."})
    if not eligible_date:
        blockers.append({"code": "MISSING_ELIGIBLE_MARKET_DATE"})
    if not champion.get("model_id"):
        blockers.append({"code": "MISSING_CHAMPION_IDENTITY"})
    if blockers:
        return _blocked_result(pol, ic, blockers)

    # --- evidence window + counts -------------------------------------------- #
    fwd_obs = _i(ev.get("forward_observation_count"))
    sess_obs = _i(ev.get("eligible_session_count"))
    rank_obs = _i(ev.get("ranking_observation_count"))
    coverage = _f(ev.get("universe_coverage_ratio"))
    realloc_obs = _i(ev.get("reallocation_observation_count"))
    hoc_obs = _i(ev.get("hoc_observation_count"))
    regime_obs = _i(ev.get("regime_observation_count"))
    distinct_regimes = _i(ev.get("distinct_regime_count"))

    window = {
        "evidence_start": ev.get("evidence_start"),
        "evidence_end": ev.get("evidence_end") or eligible_date,
        "forward_observation_count": fwd_obs,
        "eligible_session_count": sess_obs,
        "ranking_observation_count": rank_obs,
        "universe_coverage_ratio": _r(coverage, 4),
        "reallocation_observation_count": realloc_obs,
        "hoc_observation_count": hoc_obs,
        "regime_observation_count": regime_obs,
        "distinct_regime_count": distinct_regimes,
    }

    # --- evidence sufficiency ------------------------------------------------ #
    evidence = _evidence_quality(window=window, pol=pol)

    # --- data gaps (structural absences that matter) ------------------------- #
    data_gaps = _collect_data_gaps(ic=ic, rq=rq, perf=perf, regime=regime,
                                   hoc_hist=hoc_hist, realloc_hist=realloc_hist,
                                   source_gaps=source_gaps, pol=pol)

    # --- champion-health components ------------------------------------------ #
    components = _champion_health_components(
        rq=rq, perf=perf, sector=sector, regime=regime, hoc_hist=hoc_hist,
        realloc_hist=realloc_hist, evidence=evidence, pol=pol)

    # --- degradation categories ---------------------------------------------- #
    degradation = _degradation_categories(components=components, evidence=evidence,
                                          hoc_hist=hoc_hist, realloc_hist=realloc_hist,
                                          regime=regime, pol=pol)

    # --- HOC / reallocation diagnostic feedback ------------------------------ #
    hoc_diag = _hoc_diagnostics(hoc_hist=hoc_hist, pol=pol)
    realloc_diag = _reallocation_diagnostics(realloc_hist=realloc_hist, pol=pol)

    # --- challenger classification ------------------------------------------- #
    challenger_summary = _challenger_summary(challenger=challenger, champion=champion,
                                             pol=pol)

    # --- recalibration recommendation ---------------------------------------- #
    recalibration = _recalibration_recommendation(
        evidence=evidence, degradation=degradation, components=components,
        challenger=challenger_summary, window=window, ic=ic, pol=pol)

    # --- overall research state ---------------------------------------------- #
    research_state, state_reasons = _resolve_state(
        evidence=evidence, components=components, degradation=degradation,
        challenger=challenger_summary, recalibration=recalibration)

    # --- research opportunity queue ------------------------------------------ #
    opportunities = _research_opportunities(
        components=components, degradation=degradation, challenger=challenger_summary,
        evidence=evidence, hoc_diag=hoc_diag, realloc_diag=realloc_diag,
        regime=regime, data_gaps=data_gaps, window=window, champion=champion, pol=pol)

    # --- champion-health headline (explained, not opaque) -------------------- #
    component_states = [c["state"] for c in components]
    champion_health = {
        "headline_state": _headline_health(research_state, component_states, evidence),
        "component_summary": {c["component"]: c["state"] for c in components},
        "weak_components": sorted(c["component"] for c in components if c["state"] == H_WEAK),
        "watch_components": sorted(c["component"] for c in components if c["state"] == H_WATCH),
        "unknown_components": sorted(c["component"] for c in components
                                     if c["state"] == H_UNKNOWN),
        "components": components,
    }

    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": POLICY_VERSION,
        "eligible_market_date": eligible_date,
        "active_book_id": active_book_id,
        "research_state": research_state,
        "state_reason_codes": sorted(set(state_reasons)),
        "state_vocabulary": list(RESEARCH_STATE_VOCAB),
        "champion": _champion_block(champion),
        "evidence_window": window,
        "evidence_quality": evidence,
        "champion_health": champion_health,
        "degradation": degradation,
        "forward_performance": _forward_performance_summary(perf),
        "ranking_quality": _ranking_quality_summary(rq),
        "benchmark_relative": _benchmark_summary(perf),
        "turnover_cost": _turnover_cost_summary(perf),
        "drawdown": _drawdown_summary(perf),
        "regime": _regime_summary(regime),
        "sector": _sector_summary(sector, hoc_hist, realloc_hist),
        "hoc_diagnostics": hoc_diag,
        "reallocation_diagnostics": realloc_diag,
        "challenger": challenger_summary,
        "recalibration": recalibration,
        "research_opportunities": opportunities,
        "manual_review": _manual_review(research_state, recalibration, challenger_summary,
                                        opportunities),
        "data_gaps": data_gaps,
        "policy": pol,
        "provenance": _provenance(ic),
        "safety": _safety(),
    }
    result["assessment_hash"] = stable_hash(result)
    return result


# --------------------------------------------------------------------------- #
# Evidence sufficiency
# --------------------------------------------------------------------------- #
def _evidence_quality(*, window: dict, pol: dict) -> dict:
    fwd = window["forward_observation_count"]
    checks = []

    def _chk(name, value, floor, prelim=None):
        state = EV_SUFFICIENT
        if value < floor:
            state = EV_PRELIMINARY if (prelim is not None and value >= prelim) else EV_INSUFFICIENT
        checks.append({"gate": name, "value": value, "required": floor,
                       "preliminary_floor": prelim, "state": state})
        return state

    _chk("forward_observations", fwd, pol["min_forward_observations"],
         pol["preliminary_forward_observations"])
    _chk("eligible_sessions", window["eligible_session_count"], pol["min_eligible_sessions"])
    _chk("ranking_observations", window["ranking_observation_count"],
         pol["min_ranking_observations"])
    cov = window["universe_coverage_ratio"]
    checks.append({"gate": "universe_coverage", "value": cov,
                   "required": pol["min_universe_coverage"], "preliminary_floor": None,
                   "state": (EV_SUFFICIENT if (cov is not None and cov >= pol["min_universe_coverage"])
                             else (EV_INSUFFICIENT if cov is not None else EV_INSUFFICIENT))})

    # Forward-observation sufficiency is the DOMINANT gate for model-quality claims: a
    # short live P&L run must NOT be read as model degradation. The overall quality is the
    # worst of the forward-observation gate and the ranking/coverage gates.
    order = {EV_SUFFICIENT: 2, EV_PRELIMINARY: 1, EV_INSUFFICIENT: 0}
    fwd_state = next(c["state"] for c in checks if c["gate"] == "forward_observations")
    rank_state = next(c["state"] for c in checks if c["gate"] == "ranking_observations")
    cov_state = next(c["state"] for c in checks if c["gate"] == "universe_coverage")
    overall = min([fwd_state, rank_state, cov_state], key=lambda s: order[s])

    return {
        "state": overall,
        "state_vocabulary": list(EVIDENCE_QUALITY_VOCAB),
        "forward_observation_state": fwd_state,
        "dominant_gate": "forward_observations",
        "gates": checks,
        "sufficient": overall == EV_SUFFICIENT,
        "insufficient_forward_evidence": fwd_state != EV_SUFFICIENT,
        "detail": ("Forward TRUE_FORWARD evidence is the dominant gate; %d/%d matured "
                   "observations." % (fwd, pol["min_forward_observations"])),
    }


# --------------------------------------------------------------------------- #
# Champion-health components (each explained, never one opaque score)
# --------------------------------------------------------------------------- #
def _champion_health_components(*, rq: dict, perf: dict, sector: dict, regime: dict,
                                hoc_hist: list, realloc_hist: list, evidence: dict,
                                pol: dict) -> list[dict]:
    comps: list[dict] = []
    ev_ok = evidence["state"] != EV_INSUFFICIENT

    # --- forward rank IC ----------------------------------------------------- #
    ic_mean = _f(rq.get("rank_ic_mean"))
    ic_obs = _i(rq.get("rank_ic_obs"))
    pos_rate = _f(rq.get("rank_ic_positive_rate_pct"))
    if ic_mean is None or ic_obs <= 0:
        comps.append(_component("forward_rank_ic", H_UNKNOWN, value=ic_mean, obs=ic_obs,
                                reason_codes=["RANK_IC_UNAVAILABLE"],
                                detail="No matured forward rank-IC observations."))
    else:
        rc, st = [], H_HEALTHY
        if ic_mean < pol["rank_ic_weak"]:
            st, rc = H_WEAK, ["RANK_IC_BELOW_WEAK_FLOOR"]
        elif ic_mean < pol["rank_ic_watch"]:
            st, rc = H_WATCH, ["RANK_IC_BELOW_WATCH_FLOOR"]
        if pos_rate is not None and pos_rate < pol["rank_ic_positive_rate_weak_pct"]:
            st, rc = H_WEAK, rc + ["RANK_IC_POSITIVE_RATE_BELOW_WEAK"]
        elif pos_rate is not None and pos_rate < pol["rank_ic_positive_rate_watch_pct"] \
                and st == H_HEALTHY:
            st, rc = H_WATCH, rc + ["RANK_IC_POSITIVE_RATE_BELOW_WATCH"]
        # A WEAK verdict requires a material number of observations; otherwise cap at WATCH.
        if st == H_WEAK and ic_obs < pol["degradation_min_obs"]:
            st = H_WATCH
            rc = rc + ["INSUFFICIENT_OBS_FOR_WEAK_VERDICT_CAPPED_AT_WATCH"]
        comps.append(_component("forward_rank_ic", st, value=_r(ic_mean, 4),
                                threshold=pol["rank_ic_weak"], obs=ic_obs, reason_codes=rc,
                                detail="Forward Spearman rank IC of the active model."))

    # --- rank-IC trend ------------------------------------------------------- #
    recent = _f(rq.get("rank_ic_recent_mean"))
    prior = _f(rq.get("rank_ic_prior_mean"))
    if recent is None or prior is None:
        comps.append(_component("rank_ic_trend", H_UNKNOWN,
                                reason_codes=["RANK_IC_TREND_UNAVAILABLE"],
                                detail="Insufficient split-window rank-IC history for a trend."))
    else:
        drop = recent - prior
        st, rc = H_HEALTHY, []
        if drop < -pol["rank_ic_trend_drop"]:
            st, rc = (H_WEAK if ic_obs >= pol["degradation_min_obs"] else H_WATCH), \
                     ["RANK_IC_DETERIORATING_TREND"]
        comps.append(_component("rank_ic_trend", st, value=_r(drop, 4),
                                threshold=-pol["rank_ic_trend_drop"], obs=ic_obs,
                                reason_codes=rc,
                                detail="Recent-minus-prior forward rank-IC change."))

    # --- decile spread ------------------------------------------------------- #
    spread = _f(rq.get("decile_spread_pp"))
    spread_obs = _i(rq.get("decile_spread_obs"))
    if spread is None or spread_obs <= 0:
        comps.append(_component("decile_spread", H_UNKNOWN, value=spread, obs=spread_obs,
                                reason_codes=["DECILE_SPREAD_UNAVAILABLE"],
                                detail="No matured top-minus-bottom decile spread."))
    else:
        st, rc = H_HEALTHY, []
        if spread < pol["decile_spread_weak_pp"]:
            st, rc = (H_WEAK if spread_obs >= pol["degradation_min_obs"] else H_WATCH), \
                     ["DECILE_SPREAD_BELOW_WEAK_FLOOR"]
        elif spread < pol["decile_spread_watch_pp"]:
            st, rc = H_WATCH, ["DECILE_SPREAD_BELOW_WATCH_FLOOR"]
        comps.append(_component("decile_spread", st, value=_r(spread, 4),
                                threshold=pol["decile_spread_weak_pp"], obs=spread_obs,
                                reason_codes=rc,
                                detail="Forward top-minus-bottom decile spread (pct points)."))

    # --- benchmark-relative excess ------------------------------------------- #
    excess = _f(perf.get("excess_pct_points"))
    if excess is None:
        comps.append(_component("benchmark_relative", H_UNKNOWN,
                                reason_codes=["EXCESS_RETURN_UNAVAILABLE"],
                                detail="No realized benchmark-relative excess available."))
    else:
        st, rc = H_HEALTHY, []
        if excess < pol["excess_weak_pp"]:
            st, rc = H_WEAK, ["EXCESS_RETURN_BELOW_WEAK_FLOOR"]
        elif excess < pol["excess_watch_pp"]:
            st, rc = H_WATCH, ["EXCESS_RETURN_BELOW_WATCH_FLOOR"]
        # Realized-performance weakness on INSUFFICIENT evidence is capped at WATCH — it is
        # a symptom, not proof of model degradation (Charter data-integrity rule).
        if st == H_WEAK and not ev_ok:
            st = H_WATCH
            rc = rc + ["PERFORMANCE_WEAK_BUT_EVIDENCE_INSUFFICIENT_CAPPED_AT_WATCH"]
        comps.append(_component("benchmark_relative", st, value=_r(excess, 4),
                                threshold=pol["excess_weak_pp"],
                                obs=window_sessions(perf), reason_codes=rc,
                                detail="Realized cumulative excess vs benchmark (pct points)."))

    # --- drawdown ------------------------------------------------------------ #
    dd = _f(perf.get("max_drawdown_pct"))
    if dd is None:
        comps.append(_component("drawdown", H_UNKNOWN,
                                reason_codes=["DRAWDOWN_UNAVAILABLE"],
                                detail="No realized drawdown available."))
    else:
        st, rc = H_HEALTHY, []
        if dd < pol["drawdown_weak_pct"]:
            st, rc = (H_WEAK if ev_ok else H_WATCH), ["DRAWDOWN_BELOW_WEAK_FLOOR"]
        elif dd < pol["drawdown_watch_pct"]:
            st, rc = H_WATCH, ["DRAWDOWN_BELOW_WATCH_FLOOR"]
        comps.append(_component("drawdown", st, value=_r(dd, 4),
                                threshold=pol["drawdown_weak_pct"], reason_codes=rc,
                                detail="Realized max drawdown (pct, negative = loss)."))

    # --- turnover / cost efficiency ------------------------------------------ #
    turn = _f(perf.get("avg_daily_turnover"))
    if turn is None:
        comps.append(_component("turnover_efficiency", H_UNKNOWN,
                                reason_codes=["TURNOVER_UNAVAILABLE"],
                                detail="No realized turnover available."))
    else:
        st, rc = H_HEALTHY, []
        if turn > pol["turnover_weak"]:
            st, rc = H_WEAK, ["TURNOVER_ABOVE_WEAK_CEILING"]
        elif turn > pol["turnover_watch"]:
            st, rc = H_WATCH, ["TURNOVER_ABOVE_WATCH_CEILING"]
        comps.append(_component("turnover_efficiency", st, value=_r(turn, 4),
                                threshold=pol["turnover_weak"], reason_codes=rc,
                                detail="Average daily turnover fraction."))

    # --- sector stability ---------------------------------------------------- #
    max_sec = _f(sector.get("max_sector_weight"))
    sec_series = [_f(v) for v in (sector.get("sector_concentration_series") or [])]
    sec_series = [v for v in sec_series if v is not None]
    instability = _stdev(sec_series)
    if max_sec is None and instability is None:
        comps.append(_component("sector_stability", H_UNKNOWN,
                                reason_codes=["SECTOR_STATE_UNAVAILABLE"],
                                detail="No sector-concentration evidence available."))
    else:
        st, rc = H_HEALTHY, []
        if max_sec is not None and max_sec > pol["sector_concentration_weak"]:
            st, rc = H_WEAK, ["SECTOR_CONCENTRATION_ABOVE_WEAK_CEILING"]
        elif max_sec is not None and max_sec > pol["sector_concentration_watch"]:
            st, rc = H_WATCH, ["SECTOR_CONCENTRATION_ABOVE_WATCH_CEILING"]
        if instability is not None and instability > pol["sector_instability_weak"]:
            st = H_WEAK
            rc = rc + ["SECTOR_WEIGHT_INSTABILITY_ABOVE_WEAK"]
        elif instability is not None and instability > pol["sector_instability_watch"] \
                and st == H_HEALTHY:
            st, rc = H_WATCH, rc + ["SECTOR_WEIGHT_INSTABILITY_ABOVE_WATCH"]
        comps.append(_component("sector_stability", st, value=_r(max_sec, 4),
                                threshold=pol["sector_concentration_weak"],
                                obs=len(sec_series), reason_codes=rc,
                                detail="Max sector weight and its instability over history."))

    # --- regime stability ---------------------------------------------------- #
    if not regime.get("available"):
        comps.append(_component("regime_stability", H_UNKNOWN,
                                reason_codes=["REGIME_EVIDENCE_UNAVAILABLE"],
                                detail="No point-in-time regime classification available."))
    else:
        st, rc = H_HEALTHY, []
        if regime.get("drift_flag"):
            st, rc = H_WATCH, ["REGIME_DRIFT_FLAGGED"]
        comps.append(_component("regime_stability", st,
                                value=regime.get("current_label"),
                                obs=_i(regime.get("observation_count")), reason_codes=rc,
                                detail="Point-in-time regime label + drift flag."))

    # --- HOC deterioration frequency ----------------------------------------- #
    hoc_rate = _hoc_exit_replace_rate(hoc_hist)
    if hoc_rate is None:
        comps.append(_component("hoc_deterioration", H_UNKNOWN,
                                reason_codes=["HOC_HISTORY_UNAVAILABLE"],
                                detail="No Holding Opportunity-Cost history available."))
    else:
        st, rc = H_HEALTHY, []
        if hoc_rate > pol["hoc_exit_replace_weak_rate"]:
            st, rc = H_WEAK, ["HOC_EXIT_REPLACE_RATE_ABOVE_WEAK"]
        elif hoc_rate > pol["hoc_exit_replace_watch_rate"]:
            st, rc = H_WATCH, ["HOC_EXIT_REPLACE_RATE_ABOVE_WATCH"]
        comps.append(_component("hoc_deterioration", st, value=_r(hoc_rate, 4),
                                threshold=pol["hoc_exit_replace_weak_rate"],
                                obs=len(hoc_hist), reason_codes=rc,
                                detail="Fraction of HOC reviews recommending EXIT/REPLACE."))

    # --- reallocation churn -------------------------------------------------- #
    churn = _reallocation_churn(realloc_hist)
    if churn is None:
        comps.append(_component("reallocation_churn", H_UNKNOWN,
                                reason_codes=["REALLOCATION_HISTORY_UNAVAILABLE"],
                                detail="No Reallocation Proposal history available."))
    else:
        st, rc = H_HEALTHY, []
        if churn > pol["reallocation_churn_weak"]:
            st, rc = H_WEAK, ["REALLOCATION_CHURN_ABOVE_WEAK"]
        elif churn > pol["reallocation_churn_watch"]:
            st, rc = H_WATCH, ["REALLOCATION_CHURN_ABOVE_WATCH"]
        comps.append(_component("reallocation_churn", st, value=_r(churn, 4),
                                threshold=pol["reallocation_churn_weak"],
                                obs=len(realloc_hist), reason_codes=rc,
                                detail="Average proposed one-way turnover across proposals."))

    # --- data / forward-evidence completeness -------------------------------- #
    fwd_state = evidence["forward_observation_state"]
    st = {EV_SUFFICIENT: H_HEALTHY, EV_PRELIMINARY: H_WATCH,
          EV_INSUFFICIENT: H_WATCH}[fwd_state]
    rc = [] if fwd_state == EV_SUFFICIENT else ["FORWARD_EVIDENCE_INCOMPLETE"]
    comps.append(_component("data_completeness", st, value=fwd_state,
                            obs=window_forward(evidence), reason_codes=rc,
                            detail="Forward-evidence completeness (dominant sufficiency gate)."))

    return comps


def window_sessions(perf: dict) -> Optional[int]:
    v = _f(perf.get("eligible_session_count"))
    return int(v) if v is not None else None


def window_forward(evidence: dict) -> Optional[int]:
    for g in evidence.get("gates") or []:
        if g.get("gate") == "forward_observations":
            return g.get("value")
    return None


# --------------------------------------------------------------------------- #
# HOC / reallocation feedback primitives
# --------------------------------------------------------------------------- #
def _hoc_exit_replace_rate(hoc_hist: list) -> Optional[float]:
    """Average per-review fraction of holdings flagged EXIT/REPLACE across HOC history."""
    rates = []
    for h in hoc_hist:
        counts = dict((h or {}).get("recommendation_counts") or {})
        total = sum(_i(v) for v in counts.values())
        if total <= 0:
            continue
        er = _i(counts.get("EXIT")) + _i(counts.get("REPLACE"))
        rates.append(er / total)
    return _mean(rates)


def _reallocation_churn(realloc_hist: list) -> Optional[float]:
    vals = [_f((r or {}).get("one_way_turnover")) for r in realloc_hist]
    return _mean([v for v in vals if v is not None])


def _hoc_diagnostics(*, hoc_hist: list, pol: dict) -> dict:
    rate = _hoc_exit_replace_rate(hoc_hist)
    stable = (rate is not None and rate <= pol["hoc_exit_replace_watch_rate"])
    interp = "HOC_HISTORY_UNAVAILABLE"
    if rate is not None:
        if rate > pol["hoc_exit_replace_weak_rate"]:
            interp = "PERSISTENT_HIGH_EXIT_REPLACE_MAY_INDICATE_STALE_OR_UNSTABLE_RANKINGS"
        elif rate > pol["hoc_exit_replace_watch_rate"]:
            interp = "ELEVATED_EXIT_REPLACE_WATCH"
        else:
            interp = "STABLE_HOC_HISTORY"
    return {
        "observation_count": len(hoc_hist),
        "exit_replace_rate": _r(rate, 4),
        "stable": bool(stable),
        "interpretation": interp,
        "latest_recommendation_counts": (hoc_hist[-1].get("recommendation_counts")
                                         if hoc_hist else None),
        "watch_threshold": pol["hoc_exit_replace_watch_rate"],
        "weak_threshold": pol["hoc_exit_replace_weak_rate"],
    }


def _reallocation_diagnostics(*, realloc_hist: list, pol: dict) -> dict:
    churn = _reallocation_churn(realloc_hist)
    improvements = [_f((r or {}).get("score_improvement_net_of_cost")) for r in realloc_hist]
    improvements = [v for v in improvements if v is not None]
    avg_improve = _mean(improvements)
    strong = [v for v in improvements if v >= pol["reallocation_strong_improvement"]]
    weak_improve = (avg_improve is not None
                    and avg_improve < pol["reallocation_strong_improvement"])
    # Distinguish: strong repeated improvement that is NOT reflected in the operational
    # book -> portfolio lag / governance latency (NOT model failure); weak improvement
    # despite poor P&L -> limited cross-sectional alpha / broader degradation.
    interp = "REALLOCATION_HISTORY_UNAVAILABLE"
    if realloc_hist:
        if strong and len(strong) >= max(1, len(improvements) // 2):
            interp = "STRONG_REPEATED_PROPOSAL_IMPROVEMENT_MAY_INDICATE_PORTFOLIO_LAG_NOT_MODEL_FAILURE"
        elif weak_improve:
            interp = "WEAK_PROPOSAL_IMPROVEMENT_MAY_INDICATE_LIMITED_CROSS_SECTIONAL_ALPHA"
        else:
            interp = "MODERATE_PROPOSAL_IMPROVEMENT"
    return {
        "observation_count": len(realloc_hist),
        "avg_one_way_turnover": _r(churn, 4),
        "avg_score_improvement_net_of_cost": _r(avg_improve, 4),
        "strong_improvement_count": len(strong),
        "high_churn": bool(churn is not None and churn > pol["reallocation_churn_watch"]),
        "interpretation": interp,
        "latest_action_counts": (realloc_hist[-1].get("action_counts")
                                 if realloc_hist else None),
        "strong_improvement_threshold": pol["reallocation_strong_improvement"],
    }


# --------------------------------------------------------------------------- #
# Degradation categorization
# --------------------------------------------------------------------------- #
def _degradation_categories(*, components: list, evidence: dict, hoc_hist: list,
                            realloc_hist: list, regime: dict, pol: dict) -> dict:
    by_comp = {c["component"]: c for c in components}
    cats: list[dict] = []

    def _add(code, comp_names, detail):
        rc = sorted({r for n in comp_names for r in (by_comp.get(n) or {}).get("reason_codes", [])})
        cats.append({"category": code, "components": sorted(comp_names),
                     "reason_codes": rc, "detail": detail})

    ev_sufficient = evidence["state"] == EV_SUFFICIENT

    # RANKING / SIGNAL degradation require SUFFICIENT evidence AND a WEAK ranking component.
    ric = by_comp.get("forward_rank_ic") or {}
    trend = by_comp.get("rank_ic_trend") or {}
    spread = by_comp.get("decile_spread") or {}
    if ev_sufficient and ric.get("state") == H_WEAK:
        _add(DEG_RANKING, ["forward_rank_ic"],
             "Forward rank IC materially below floor across sufficient observations.")
        _add(DEG_SIGNAL, ["forward_rank_ic", "rank_ic_trend"],
             "Active-model signal quality has degraded on matured forward evidence.")
    elif ev_sufficient and (trend.get("state") == H_WEAK or spread.get("state") == H_WEAK):
        _add(DEG_RANKING, [n for n in ("rank_ic_trend", "decile_spread")
                           if by_comp.get(n, {}).get("state") == H_WEAK],
             "Ranking quality (trend / decile spread) has deteriorated.")

    # PERFORMANCE_WEAKNESS: realized excess / drawdown weak (symptom; not evidence-gated but
    # explicitly separated from signal/ranking degradation).
    perf_weak = [n for n in ("benchmark_relative", "drawdown")
                 if by_comp.get(n, {}).get("state") == H_WEAK]
    if perf_weak:
        _add(DEG_PERFORMANCE, perf_weak,
             "Realized benchmark-relative / drawdown weakness (portfolio symptom).")

    if by_comp.get("turnover_efficiency", {}).get("state") == H_WEAK:
        _add(DEG_TURNOVER, ["turnover_efficiency"], "Turnover above the weak ceiling.")
    if by_comp.get("sector_stability", {}).get("state") == H_WEAK:
        _add(DEG_SECTOR, ["sector_stability"],
             "Sector concentration / instability above the weak ceiling.")
    if regime.get("available") and regime.get("drift_flag"):
        _add(DEG_REGIME, ["regime_stability"], "Point-in-time regime drift flagged.")

    # PORTFOLIO_STALENESS: high HOC exit/replace + high reallocation churn together suggest
    # the operational book has drifted from the model's current preference (staleness), a
    # distinct diagnosis from model degradation.
    if by_comp.get("hoc_deterioration", {}).get("state") in (H_WEAK, H_WATCH) \
            and by_comp.get("reallocation_churn", {}).get("state") in (H_WEAK, H_WATCH):
        _add(DEG_STALENESS, ["hoc_deterioration", "reallocation_churn"],
             "Elevated HOC exit/replace with elevated reallocation churn suggests portfolio "
             "staleness rather than (or in addition to) model degradation.")

    # DATA_QUALITY_DEGRADATION: multiple UNKNOWN components / missing forward evidence.
    unknown = [c["component"] for c in components if c["state"] == H_UNKNOWN]
    if len(unknown) >= 3:
        _add(DEG_DATA_QUALITY, unknown, "Multiple health components lack evidence.")

    # INSUFFICIENT_EVIDENCE is itself a documented degradation-diagnosis blocker.
    if evidence["state"] == EV_INSUFFICIENT:
        cats.append({"category": DEG_INSUFFICIENT, "components": [],
                     "reason_codes": ["EVIDENCE_INSUFFICIENT_FOR_DEGRADATION_DIAGNOSIS"],
                     "detail": "Evidence is insufficient to diagnose model degradation; "
                               "any weakness observed is a symptom, not proof."})

    return {
        "categories": sorted({c["category"] for c in cats}),
        "vocabulary": list(DEGRADATION_VOCAB),
        "details": sorted(cats, key=lambda c: c["category"]),
        "model_degradation_diagnosable": ev_sufficient,
        "has_signal_or_ranking_degradation": any(
            c["category"] in (DEG_RANKING, DEG_SIGNAL) for c in cats),
    }


# --------------------------------------------------------------------------- #
# Challenger classification (never PROMOTED)
# --------------------------------------------------------------------------- #
def _challenger_summary(*, challenger: dict, champion: dict, pol: dict) -> dict:
    base = {
        "state": CH_NOT_EVALUATED,
        "state_vocabulary": list(CHALLENGER_VOCAB),
        "champion_signal": champion.get("model_id"),
        "challenger_signal": challenger.get("signal"),
        "best_excess_pp": None,
        "observation_count": 0,
        "manual_model_review_required": False,
        "auto_promotion_allowed": False,
        "detail": "No challenger evidence supplied.",
    }
    if not challenger or not challenger.get("available"):
        return base

    comps = list(challenger.get("book_comparisons") or [])
    best = None
    best_obs = 0
    for c in comps:
        exc = _f(c.get("excess_pp"))
        obs = _i(c.get("observation_count"))
        if exc is None:
            continue
        if best is None or exc > best:
            best, best_obs = exc, obs
    obs_total = max([_i(c.get("observation_count")) for c in comps] or [0])

    if best is None:
        state = CH_INSUFFICIENT
        detail = "Challenger present but no comparable excess observations."
    elif obs_total < pol["challenger_min_obs"]:
        state = CH_INSUFFICIENT
        detail = ("Challenger excess %.2fpp but only %d/%d observations — insufficient."
                  % (best, obs_total, pol["challenger_min_obs"]))
    elif best >= pol["challenger_superior_excess_pp"]:
        state = CH_SUPERIOR
        detail = "Challenger materially outperforms on sufficient evidence."
    elif best >= pol["challenger_promising_excess_pp"]:
        state = CH_PROMISING
        detail = "Challenger promising on sufficient evidence."
    elif best >= pol["challenger_competitive_excess_pp"]:
        state = CH_COMPETITIVE
        detail = "Challenger competitive but not decisively ahead."
    else:
        state = CH_UNDERPERFORMING
        detail = "Challenger underperforms the champion."

    manual_review = state in (CH_PROMISING, CH_SUPERIOR)
    return {
        **base,
        "state": state,
        "best_excess_pp": _r(best, 4),
        "observation_count": obs_total,
        "book_comparisons": comps,
        "decision": challenger.get("decision"),
        "manual_model_review_required": manual_review,
        "detail": detail + (" MANUAL MODEL REVIEW REQUIRED — no automatic promotion."
                            if manual_review else ""),
    }


# --------------------------------------------------------------------------- #
# Recalibration recommendation (evidence-gated)
# --------------------------------------------------------------------------- #
def _recalibration_recommendation(*, evidence: dict, degradation: dict, components: list,
                                  challenger: dict, window: dict, ic: dict,
                                  pol: dict) -> dict:
    fwd = window["forward_observation_count"]
    last_ckpt = _i(ic.get("last_checkpoint_forward_observation_count"))
    interval = pol["checkpoint_forward_obs_interval"]
    checkpoint_due = (fwd - last_ckpt) >= interval and fwd >= pol["recalibration_min_forward_obs"]

    reasons: list[str] = []
    gates = {
        "evidence_sufficient": evidence["state"] == EV_SUFFICIENT,
        "forward_observations_met": fwd >= pol["recalibration_min_forward_obs"],
        "signal_or_ranking_degradation": degradation["has_signal_or_ranking_degradation"],
        "checkpoint_due": bool(checkpoint_due),
        "challenger_promising": challenger["state"] in (CH_PROMISING, CH_SUPERIOR),
    }

    if evidence["state"] == EV_INSUFFICIENT:
        state = RECAL_INSUFFICIENT
        reasons.append("EVIDENCE_INSUFFICIENT_NO_RECALIBRATION")
    elif gates["evidence_sufficient"] and gates["signal_or_ranking_degradation"]:
        state = RECAL_RECOMMENDED
        reasons.append("SUFFICIENT_RANK_IC_DEGRADATION_JUSTIFIES_CONTROLLED_STUDY")
    elif checkpoint_due:
        state = RECAL_CHECKPOINT_DUE
        reasons.append("SCHEDULED_RESEARCH_CHECKPOINT_DUE")
    elif gates["challenger_promising"]:
        state = RECAL_MONITORING
        reasons.append("CHALLENGER_EVIDENCE_WARRANTS_CONTROLLED_COMPARISON")
    else:
        state = RECAL_NOT_DUE
        reasons.append("NO_RECALIBRATION_TRIGGER")

    next_ckpt_at = last_ckpt + interval
    return {
        "state": state,
        "state_vocabulary": list(RECAL_VOCAB),
        "recommended": state == RECAL_RECOMMENDED,
        "gates": gates,
        "reason_codes": sorted(set(reasons)),
        "forward_observation_count": fwd,
        "last_checkpoint_forward_observation_count": last_ckpt,
        "next_checkpoint_forward_observation_count": next_ckpt_at,
        "checkpoint_interval": interval,
        "manual_approval_required": True,
        "automatic_retraining_allowed": False,
        "automatic_promotion_allowed": False,
        "detail": ("Recalibration is a MANUALLY-APPROVED controlled study; the agent never "
                   "retrains or promotes automatically."),
    }


# --------------------------------------------------------------------------- #
# Overall research-state resolution
# --------------------------------------------------------------------------- #
def _resolve_state(*, evidence: dict, components: list, degradation: dict, challenger: dict,
                   recalibration: dict) -> tuple[str, list]:
    reasons: list[str] = []
    comp_states = [c["state"] for c in components]
    any_weak = H_WEAK in comp_states
    any_watch = H_WATCH in comp_states

    if evidence["state"] == EV_INSUFFICIENT:
        # A short/thin evidence window never escalates past WATCH regardless of P&L.
        if any_weak or any_watch:
            reasons.append("INSUFFICIENT_FORWARD_EVIDENCE")
            reasons.append("PERFORMANCE_SYMPTOM_PRESENT_BUT_UNCONFIRMED")
            return STATE_WATCH, reasons
        reasons.append("INSUFFICIENT_FORWARD_EVIDENCE")
        return STATE_INSUFFICIENT_EVIDENCE, reasons

    # Evidence is SUFFICIENT or PRELIMINARY beyond this point.
    if recalibration["state"] == RECAL_RECOMMENDED:
        reasons.append("RECALIBRATION_RECOMMENDED_ON_SUFFICIENT_DEGRADATION")
        return STATE_RECALIBRATION_DUE, reasons
    if challenger["state"] in (CH_PROMISING, CH_SUPERIOR):
        reasons.append("CHALLENGER_%s_MANUAL_MODEL_REVIEW_REQUIRED" % challenger["state"])
        return STATE_CHALLENGER_PROMISING, reasons
    if recalibration["state"] == RECAL_CHECKPOINT_DUE:
        reasons.append("SCHEDULED_CHECKPOINT_DUE")
        return STATE_INVESTIGATE, reasons
    if any_weak:
        reasons.append("WEAK_HEALTH_COMPONENT_PRESENT")
        return STATE_INVESTIGATE, reasons
    if any_watch:
        reasons.append("WATCH_HEALTH_COMPONENT_PRESENT")
        if evidence["state"] == EV_PRELIMINARY:
            reasons.append("EVIDENCE_ONLY_PRELIMINARY")
        return STATE_WATCH, reasons
    if evidence["state"] == EV_PRELIMINARY:
        reasons.append("HEALTHY_ON_PRELIMINARY_EVIDENCE")
        return STATE_WATCH, reasons
    reasons.append("ALL_COMPONENTS_HEALTHY_ON_SUFFICIENT_EVIDENCE")
    return STATE_HEALTHY, reasons


def _headline_health(research_state: str, comp_states: list, evidence: dict) -> str:
    if research_state == STATE_BLOCKED:
        return "BLOCKED"
    if evidence["state"] == EV_INSUFFICIENT:
        return "UNDETERMINED_INSUFFICIENT_EVIDENCE"
    worst = _worst(comp_states)
    return {H_WEAK: "WEAK", H_WATCH: "WATCH", H_HEALTHY: "HEALTHY",
            H_UNKNOWN: "UNDETERMINED_INSUFFICIENT_EVIDENCE"}[worst]


# --------------------------------------------------------------------------- #
# Research opportunity queue (deterministic)
# --------------------------------------------------------------------------- #
def _opportunity(opportunity_id: str, category: str, *, hypothesis: str, reason: str,
                 priority: int, expected_research_value: str,
                 supporting_evidence: list, evidence_gaps: list, permitted_data: list,
                 experiment: dict) -> dict:
    return {
        "opportunity_id": opportunity_id,
        "category": category,
        "hypothesis": hypothesis,
        "reason": reason,
        "supporting_evidence": supporting_evidence,
        "evidence_gaps": sorted(set(evidence_gaps)),
        "priority": int(priority),
        "expected_research_value": expected_research_value,
        "estimated_computational_scope": experiment.get("estimated_computational_scope"),
        "permitted_data": sorted(set(permitted_data)),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "proposed_experiment": experiment,
        "required_manual_approval": True,
        "manual_approval_state": MANUAL_APPROVAL_PENDING,
    }


def _research_opportunities(*, components: list, degradation: dict, challenger: dict,
                            evidence: dict, hoc_diag: dict, realloc_diag: dict,
                            regime: dict, data_gaps: list, window: dict, champion: dict,
                            pol: dict) -> list[dict]:
    by_comp = {c["component"]: c for c in components}
    opps: list[dict] = []
    permitted_owned = ["OWNED_PRICE_PANEL", "OWNED_UNIVERSE_SCORING", "OWNED_FORWARD_EVIDENCE",
                       "OWNED_HOC_HISTORY", "OWNED_REALLOCATION_HISTORY"]

    def _bounded(exp_type, desc, scope="BOUNDED_SINGLE_STUDY_NO_LIVE_DATA"):
        return {"experiment_type": exp_type, "description": desc,
                "runner": "SPECIFICATION_ONLY_NO_AUTOMATIC_EXECUTION",
                "estimated_computational_scope": scope,
                "gates_reference": "alpha_agent.experiment_contracts.DEFAULT_GATES",
                "shadow_only": True, "changes_champion": False}

    # 1) Forward-evidence maturation (baseline — always emitted).
    if evidence["forward_observation_state"] != EV_SUFFICIENT:
        opps.append(_opportunity(
            "accumulate_forward_evidence", "EVIDENCE",
            hypothesis="Accumulating additional matured TRUE_FORWARD observations will "
                       "raise evidence sufficiency enough to diagnose model health.",
            reason="Forward evidence is the dominant sufficiency gate and is not yet met.",
            priority=90, expected_research_value="HIGH",
            supporting_evidence=[{"metric": "forward_observation_count",
                                  "value": window["forward_observation_count"],
                                  "source": "api.forward_prediction_skill"}],
            evidence_gaps=["FORWARD_OBSERVATIONS_BELOW_MINIMUM"],
            permitted_data=["OWNED_FORWARD_EVIDENCE"],
            experiment=_bounded("FORWARD_EVIDENCE_MATURATION",
                                "Continue point-in-time forward capture + maturation; no "
                                "new model, no retraining — evidence accrual only.",
                                scope="ZERO_COMPUTE_OBSERVATIONAL")))

    # 2) Rank-IC degradation investigation.
    ric = by_comp.get("forward_rank_ic") or {}
    if ric.get("state") in (H_WEAK, H_WATCH):
        opps.append(_opportunity(
            "investigate_rank_ic_degradation", "SIGNAL",
            hypothesis="The active model's forward rank IC has deteriorated; a controlled "
                       "walk-forward study will confirm whether the relationship has broken.",
            reason="Forward rank IC is %s." % ric.get("state"),
            priority=85 if ric.get("state") == H_WEAK else 60,
            expected_research_value="HIGH" if ric.get("state") == H_WEAK else "MEDIUM",
            supporting_evidence=[{"metric": "rank_ic_mean", "value": ric.get("value"),
                                  "source": "api.forward_prediction_skill"}],
            evidence_gaps=(["RANK_IC_OBS_BELOW_DEGRADATION_MINIMUM"]
                           if (ric.get("observation_count") or 0) < pol["degradation_min_obs"]
                           else []),
            permitted_data=["OWNED_FORWARD_EVIDENCE", "OWNED_UNIVERSE_SCORING"],
            experiment=_bounded("RANK_IC_WALK_FORWARD",
                                "Bounded walk-forward rank-IC re-estimation of the active "
                                "model against owned cross-sections; SHADOW only.")))
        opps.append(_opportunity(
            "investigate_factor_crowding", "SIGNAL",
            hypothesis="Rank-IC weakness may reflect factor crowding; test orthogonality "
                       "of the active factor vs owned alternatives.",
            reason="Rank-IC weakness can indicate crowding rather than a broken signal.",
            priority=55, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "rank_ic_mean", "value": ric.get("value"),
                                  "source": "api.forward_prediction_skill"}],
            evidence_gaps=[], permitted_data=["OWNED_UNIVERSE_SCORING", "OWNED_FORWARD_EVIDENCE"],
            experiment=_bounded("FACTOR_CROWDING_ORTHOGONALITY",
                                "Bounded orthogonality / correlation study across owned "
                                "factor families; SHADOW only.")))

    # 3) Challenger validation.
    if challenger.get("state") != CH_NOT_EVALUATED:
        pr = {CH_SUPERIOR: 88, CH_PROMISING: 80, CH_COMPETITIVE: 50,
              CH_UNDERPERFORMING: 35, CH_INSUFFICIENT: 45}.get(challenger["state"], 40)
        opps.append(_opportunity(
            "validate_frozen_challenger", "CHALLENGER",
            hypothesis="The frozen sector-repaired challenger may match or beat the champion "
                       "on sufficient forward evidence.",
            reason="Challenger classified %s." % challenger["state"],
            priority=pr,
            expected_research_value=("HIGH" if challenger["state"] in (CH_PROMISING, CH_SUPERIOR)
                                     else "MEDIUM"),
            supporting_evidence=[{"metric": "challenger_best_excess_pp",
                                  "value": challenger.get("best_excess_pp"),
                                  "source": "api.current_alpha_tournament"}],
            evidence_gaps=(["CHALLENGER_OBS_BELOW_MINIMUM"]
                           if challenger["state"] == CH_INSUFFICIENT else []),
            permitted_data=["OWNED_FORWARD_EVIDENCE", "OWNED_UNIVERSE_SCORING"],
            experiment=_bounded("CHAMPION_VS_CHALLENGER_FORWARD",
                                "Bounded champion-vs-frozen-challenger forward comparison; "
                                "SHADOW only; no promotion.")))

    # 4) Turnover penalty.
    if by_comp.get("turnover_efficiency", {}).get("state") in (H_WEAK, H_WATCH):
        opps.append(_opportunity(
            "test_turnover_penalty", "PORTFOLIO",
            hypothesis="Adding / raising a turnover penalty improves net-of-cost forward "
                       "performance without material rank-IC loss.",
            reason="Turnover is %s." % by_comp["turnover_efficiency"]["state"],
            priority=50, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "avg_daily_turnover",
                                  "value": by_comp["turnover_efficiency"].get("value"),
                                  "source": "api.forward_evidence"}],
            evidence_gaps=[], permitted_data=permitted_owned,
            experiment=_bounded("TURNOVER_PENALTY_SWEEP",
                                "Bounded turnover-penalty sweep on owned panel; SHADOW only.")))

    # 5) Holding buffer (from HOC churn).
    if by_comp.get("hoc_deterioration", {}).get("state") in (H_WEAK, H_WATCH):
        opps.append(_opportunity(
            "test_holding_buffer", "PORTFOLIO",
            hypothesis="A wider holding buffer reduces churn-driven exit/replace without "
                       "harming forward performance.",
            reason="HOC exit/replace rate is %s." % by_comp["hoc_deterioration"]["state"],
            priority=48, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "hoc_exit_replace_rate",
                                  "value": by_comp["hoc_deterioration"].get("value"),
                                  "source": "api.holding_opportunity_cost"}],
            evidence_gaps=[], permitted_data=["OWNED_HOC_HISTORY", "OWNED_UNIVERSE_SCORING"],
            experiment=_bounded("HOLDING_BUFFER_SWEEP",
                                "Bounded exit-buffer sweep on owned rankings; SHADOW only.")))

    # 6) Regime-conditioned weighting / regime data.
    if not regime.get("available"):
        opps.append(_opportunity(
            "inspect_regime_evidence", "DATA",
            hypothesis="Adding point-in-time regime classification will let the agent test "
                       "regime-conditioned model stability.",
            reason="No point-in-time regime classification is currently available.",
            priority=40, expected_research_value="LOW",
            supporting_evidence=[], evidence_gaps=["REGIME_EVIDENCE_UNAVAILABLE"],
            permitted_data=["OWNED_PRICE_PANEL"],
            experiment=_bounded("REGIME_EVIDENCE_INSPECTION",
                                "Bounded point-in-time regime classification build from owned "
                                "market features; SHADOW only.",
                                scope="BOUNDED_OBSERVATIONAL")))
    elif regime.get("drift_flag"):
        opps.append(_opportunity(
            "test_regime_conditioned_weighting", "SIGNAL",
            hypothesis="Regime-conditioned weighting may preserve rank IC through the drift.",
            reason="Regime drift flagged.",
            priority=52, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "regime_label", "value": regime.get("current_label"),
                                  "source": "alpha_agent.regimes"}],
            evidence_gaps=[], permitted_data=["OWNED_PRICE_PANEL", "OWNED_UNIVERSE_SCORING"],
            experiment=_bounded("REGIME_CONDITIONED_WEIGHTING",
                                "Bounded regime-conditioned weighting study; SHADOW only.")))

    # 7) Sector concentration investigation.
    if by_comp.get("sector_stability", {}).get("state") in (H_WEAK, H_WATCH):
        opps.append(_opportunity(
            "investigate_sector_concentration", "PORTFOLIO",
            hypothesis="Persistent sector concentration/instability may be tightened without "
                       "loss of forward performance.",
            reason="Sector stability is %s." % by_comp["sector_stability"]["state"],
            priority=45, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "max_sector_weight",
                                  "value": by_comp["sector_stability"].get("value"),
                                  "source": "api.portfolio_state"}],
            evidence_gaps=[], permitted_data=permitted_owned,
            experiment=_bounded("SECTOR_CAP_SWEEP",
                                "Bounded sector-cap sweep on owned rankings; SHADOW only.")))

    # 8) Data-quality anomaly.
    if DEG_DATA_QUALITY in degradation["categories"]:
        opps.append(_opportunity(
            "inspect_data_quality_anomaly", "DATA",
            hypothesis="Missing/incomplete evidence components indicate a data-quality "
                       "anomaly that should be resolved before any model claim.",
            reason="Multiple health components lack evidence.",
            priority=58, expected_research_value="MEDIUM",
            supporting_evidence=[{"metric": "unknown_component_count",
                                  "value": len([c for c in components if c["state"] == H_UNKNOWN]),
                                  "source": "engine.research_agent"}],
            evidence_gaps=[g.get("code") for g in data_gaps if not g.get("by_design")],
            permitted_data=permitted_owned,
            experiment=_bounded("DATA_QUALITY_INSPECTION",
                                "Bounded owned-evidence completeness inspection; SHADOW only.",
                                scope="BOUNDED_OBSERVATIONAL")))

    # Deterministic ordering: priority desc, then opportunity_id asc.
    opps.sort(key=lambda o: (-o["priority"], o["opportunity_id"]))
    return opps


# --------------------------------------------------------------------------- #
# Read-model summaries (verbatim reflections of owned evidence)
# --------------------------------------------------------------------------- #
def _champion_block(champion: dict) -> dict:
    return {
        "model_id": champion.get("model_id"),
        "primary_model_id": champion.get("primary_model_id"),
        "strategy_version": champion.get("strategy_version"),
        "model_registry_version": champion.get("model_registry_version"),
        "champion_identity_source": "api.universe_scoring",
        "automatic_promotion_allowed": bool(champion.get("automatic_promotion_allowed", False)),
    }


def _forward_performance_summary(perf: dict) -> dict:
    return {
        "cumulative_return_pct": _f(perf.get("cumulative_return_pct")),
        "eligible_session_count": window_sessions(perf),
        "source": "api.paper_trading_desk.load_performance (via api.forward_evidence)",
    }


def _ranking_quality_summary(rq: dict) -> dict:
    return {
        "rank_ic_mean": _f(rq.get("rank_ic_mean")),
        "rank_ic_median": _f(rq.get("rank_ic_median")),
        "rank_ic_positive_rate_pct": _f(rq.get("rank_ic_positive_rate_pct")),
        "rank_ic_obs": _i(rq.get("rank_ic_obs")),
        "rank_ic_recent_mean": _f(rq.get("rank_ic_recent_mean")),
        "rank_ic_prior_mean": _f(rq.get("rank_ic_prior_mean")),
        "decile_spread_pp": _f(rq.get("decile_spread_pp")),
        "decile_spread_obs": _i(rq.get("decile_spread_obs")),
        "rank_ic_by_horizon": rq.get("rank_ic_by_horizon") or {},
        "evidence_state": rq.get("evidence_state"),
        "source": "api.forward_prediction_skill",
    }


def _benchmark_summary(perf: dict) -> dict:
    return {
        "benchmark_ticker": perf.get("benchmark_ticker") or "SPY",
        "benchmark_cumulative_return_pct": _f(perf.get("benchmark_cumulative_return_pct")),
        "excess_pct_points": _f(perf.get("excess_pct_points")),
        "rolling": perf.get("rolling") or [],
        "source": "api.paper_trading_desk / api.forward_evidence",
    }


def _turnover_cost_summary(perf: dict) -> dict:
    return {
        "avg_daily_turnover": _f(perf.get("avg_daily_turnover")),
        "total_transaction_cost": _f(perf.get("total_transaction_cost")),
        "source": "api.paper_trading_desk / api.forward_evidence",
    }


def _drawdown_summary(perf: dict) -> dict:
    return {
        "max_drawdown_pct": _f(perf.get("max_drawdown_pct")),
        "current_drawdown_pct": _f(perf.get("current_drawdown_pct")),
        "source": "api.paper_trading_desk / api.current_alpha_decision_gate",
    }


def _regime_summary(regime: dict) -> dict:
    return {
        "available": bool(regime.get("available")),
        "current_label": regime.get("current_label"),
        "distinct_count": _i(regime.get("distinct_count")),
        "drift_flag": bool(regime.get("drift_flag")),
        "observation_count": _i(regime.get("observation_count")),
        "source": regime.get("source") or "alpha_agent.regimes (research subsystem)",
    }


def _sector_summary(sector: dict, hoc_hist: list, realloc_hist: list) -> dict:
    series = [_f(v) for v in (sector.get("sector_concentration_series") or [])]
    series = [v for v in series if v is not None]
    return {
        "max_sector_weight": _f(sector.get("max_sector_weight")),
        "max_sector": sector.get("max_sector"),
        "sector_weights": sector.get("sector_weights") or {},
        "concentration_series_length": len(series),
        "concentration_instability": _r(_stdev(series), 4),
        "source": "api.portfolio_state / engine.reallocation_proposal risk block",
    }


# --------------------------------------------------------------------------- #
# Data gaps
# --------------------------------------------------------------------------- #
def _collect_data_gaps(*, ic: dict, rq: dict, perf: dict, regime: dict, hoc_hist: list,
                       realloc_hist: list, source_gaps: list, pol: dict) -> list[dict]:
    gaps: list[dict] = []

    def _gap(code, affects, by_design, detail):
        gaps.append({"code": code, "affects": affects, "by_design": by_design,
                     "detail": detail})

    if _f(rq.get("rank_ic_mean")) is None or _i(rq.get("rank_ic_obs")) <= 0:
        _gap("FORWARD_RANK_IC_UNAVAILABLE", "RANKING_QUALITY", False,
             "No matured forward rank-IC observations for the active model.")
    if _f(rq.get("decile_spread_pp")) is None:
        _gap("DECILE_SPREAD_UNAVAILABLE", "RANKING_QUALITY", False,
             "No matured decile-spread observation.")
    if _f(perf.get("excess_pct_points")) is None:
        _gap("BENCHMARK_RELATIVE_UNAVAILABLE", "PERFORMANCE", False,
             "No realized benchmark-relative excess available.")
    if not regime.get("available"):
        _gap("REGIME_CLASSIFICATION_UNAVAILABLE", "REGIME", False,
             "No operational-stack regime classifier exists; regime evidence is a "
             "documented gap unless sourced from the research subsystem.")
    if not hoc_hist:
        _gap("HOC_HISTORY_UNAVAILABLE", "HOC_FEEDBACK", False,
             "No Holding Opportunity-Cost history persisted yet.")
    if not realloc_hist:
        _gap("REALLOCATION_HISTORY_UNAVAILABLE", "REALLOCATION_FEEDBACK", False,
             "No Reallocation Proposal history persisted yet.")
    # Regime being unavailable is genuinely by-design in the current owned-data stack.
    for g in sorted(set(source_gaps)):
        _gap("CARRIED_%s" % g, "UPSTREAM", False,
             "Carried forward from an upstream evidence owner.")
    return gaps


# --------------------------------------------------------------------------- #
# Manual review requirements / safety / provenance / blocked result
# --------------------------------------------------------------------------- #
def _manual_review(research_state: str, recalibration: dict, challenger: dict,
                   opportunities: list) -> dict:
    return {
        "manual_review_required": True,
        "recalibration_manual_approval_required": True,
        "challenger_manual_model_review_required": bool(
            challenger.get("manual_model_review_required")),
        "any_opportunity_requires_manual_approval": bool(opportunities),
        "automatic_model_promotion_allowed": False,
        "automatic_recalibration_allowed": False,
        "automatic_experiment_execution_allowed": False,
        "detail": ("All Research Agent findings are research-governance recommendations; no "
                   "action is taken without explicit manual approval."),
    }


def _safety() -> dict:
    return {
        "read_only": True,
        "research_only": True,
        "paper_only": True,
        "manual_review": True,
        "promoted_model": False,
        "recalibrated_model": False,
        "replaced_champion": False,
        "retrained_model": False,
        "wrote_champion_pointer": False,
        "changed_target_weights": False,
        "created_operational_target": False,
        "confirmed_alpha_target": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "performed_broker_execution": False,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "executed_experiment": False,
        "automatic_promotion_allowed": False,
        "automatic_recalibration_allowed": False,
        "automatic_experiment_execution_allowed": False,
        "automation_off": True,
        "safety_badges": ["RESEARCH ONLY", "MANUAL REVIEW", "NO MODEL PROMOTION",
                          "NO PORTFOLIO MUTATION", "NO ORDERS", "AUTOMATION OFF"],
    }


def _provenance(ic: dict) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "champion_identity_source": "api.universe_scoring",
        "challenger_source": "api.current_alpha_tournament",
        "forward_evidence_source": "api.forward_prediction_skill / api.forward_evidence",
        "realized_performance_source": "api.paper_trading_desk.load_performance",
        "hoc_history_source": "api.holding_opportunity_cost (immutable artifacts)",
        "reallocation_history_source": "api.reallocation_proposal (immutable artifacts)",
        "decision_gate_source": "api.current_alpha_decision_gate",
        "regime_source": "alpha_agent.regimes (research subsystem; gap if absent)",
        "validation_gate_reference": "alpha_agent.experiment_contracts.DEFAULT_GATES",
        "eligible_market_date": ic.get("eligible_market_date"),
        "champion_identity_hash": ic.get("champion_identity_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "forward_evidence_hash": ic.get("forward_evidence_hash"),
        "policy_version": POLICY_VERSION,
    }


def _blocked_result(pol: dict, ic: dict, blockers: list) -> dict:
    """A readable BLOCKED result (never raises; degrade-safe)."""
    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": POLICY_VERSION,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "research_state": STATE_BLOCKED,
        "state_reason_codes": sorted({b["code"] for b in blockers}),
        "state_vocabulary": list(RESEARCH_STATE_VOCAB),
        "champion": _champion_block(dict(ic.get("champion") or {})),
        "evidence_window": {},
        "evidence_quality": {"state": EV_INSUFFICIENT,
                             "state_vocabulary": list(EVIDENCE_QUALITY_VOCAB),
                             "sufficient": False, "gates": []},
        "champion_health": {"headline_state": "BLOCKED", "components": []},
        "degradation": {"categories": [], "vocabulary": list(DEGRADATION_VOCAB),
                        "details": [], "model_degradation_diagnosable": False,
                        "has_signal_or_ranking_degradation": False},
        "forward_performance": {}, "ranking_quality": {}, "benchmark_relative": {},
        "turnover_cost": {}, "drawdown": {}, "regime": {"available": False},
        "sector": {}, "hoc_diagnostics": {}, "reallocation_diagnostics": {},
        "challenger": {"state": CH_NOT_EVALUATED,
                       "state_vocabulary": list(CHALLENGER_VOCAB),
                       "auto_promotion_allowed": False},
        "recalibration": {"state": RECAL_INSUFFICIENT,
                          "state_vocabulary": list(RECAL_VOCAB), "recommended": False,
                          "manual_approval_required": True,
                          "automatic_retraining_allowed": False},
        "research_opportunities": [],
        "manual_review": {"manual_review_required": True,
                          "automatic_model_promotion_allowed": False},
        "data_gaps": [{"code": b["code"], "affects": "CORE_INPUT", "by_design": False,
                       "detail": b.get("detail")} for b in blockers],
        "blockers": blockers,
        "policy": pol,
        "provenance": _provenance(ic),
        "safety": _safety(),
    }
    result["assessment_hash"] = stable_hash(result)
    return result
