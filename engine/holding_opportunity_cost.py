r"""Phase 29G Slice 6 — Holding Opportunity-Cost Engine (pure calculation kernel).

This module is the ONE canonical holding comparison and decision calculation
(Consolidation Roadmap Slice 6 / Charter Milestone 2). It is a **pure,
deterministic kernel**: it performs NO I/O — no file, database, network, provider
or prediction access — and never mutates its inputs. Everything it needs arrives
as one immutable assessment-input contract (built by ``api.holding_opportunity_cost``
from the authoritative owners), and it returns one immutable assessment result.

For every current holding it measures, using only point-in-time inputs supplied
in the contract:

  * current rank, previous rank and rank change (previous rank is honestly
    reported UNAVAILABLE when no prior point-in-time snapshot exists);
  * signal strength and deterioration;
  * trailing recent performance (5 / 20 / 60 eligible-close total returns);
  * realized volatility (20 / 60 close, annualized);
  * maximum drawdown (60 eligible closes);
  * covariance risk contribution (w_i (Σw)_i / portfolio variance from date-aligned
    daily returns);
  * concentration contribution;
  * liquidity (trailing median dollar volume -> estimated days to liquidate);
  * the strongest eligible replacement candidate (a NON-ALLOCATED comparison
    candidate — Slice 7 does not exist, so it is never an approved target);
  * switching cost (reusing the existing paper trading desk transaction-cost model);
  * gross / risk-adjusted / net expected improvement (a SCORE comparison — never a
    forecasted dollar return, because no validated expected-return model exists);
  * a recommendation from the frozen vocabulary HOLD / REDUCE / EXIT / REPLACE / ADD.

The result NEVER modifies a target, holding, cash, NAV or order. It is review-only.

Decision policy is explicit, versioned and returned in the payload; the reused
scoring / construction / cost constants come from ``api.multi_horizon_engine`` and
``api.paper_trading_desk`` (the API owner injects the live values), and the
genuinely-new thresholds are declared once in :func:`default_policy`.
"""
from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

SCHEMA_VERSION = "holding_opportunity_cost.v1"
INPUT_SCHEMA_VERSION = "holding_opportunity_cost.input.v1"
DECISION_POLICY_VERSION = "hoc_decision_policy.v1"
COST_POLICY_VERSION = "hoc_cost_policy.v1"

CALCULATION_OWNER = "engine.holding_opportunity_cost"

# --- Frozen recommendation vocabulary ---------------------------------------- #
REC_HOLD = "HOLD"
REC_REDUCE = "REDUCE"
REC_EXIT = "EXIT"
REC_REPLACE = "REPLACE"
REC_ADD = "ADD"
RECOMMENDATION_VOCAB = (REC_HOLD, REC_REDUCE, REC_EXIT, REC_REPLACE, REC_ADD)

#: Release 46.6 - the two DISTINCT meanings a ``recommendation_counts`` block
#: can carry, named so they can never be silently interchanged. A read model
#: that publishes counts must declare which of these it is.
SEMANTIC_SIGNAL_LEVEL = "SIGNAL_LEVEL_PER_HOLDING_HOC_RECOMMENDATIONS"
SEMANTIC_POST_PORTFOLIO_GATE = "POST_PORTFOLIO_GATE_ACTIONABLE_RECOMMENDATIONS"
RECOMMENDATION_COUNT_SEMANTICS = (SEMANTIC_SIGNAL_LEVEL,
                                  SEMANTIC_POST_PORTFOLIO_GATE)

# --- Frozen assessment-state vocabulary -------------------------------------- #
STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_BLOCKED = "BLOCKED"
STATE_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
ASSESSMENT_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK)

# --- Deterioration vocabulary ------------------------------------------------ #
DET_STABLE = "STABLE"
DET_IMPROVING = "IMPROVING"
DET_DETERIORATING = "DETERIORATING"
DET_BROKEN = "BROKEN"
DET_UNKNOWN = "UNKNOWN"
DETERIORATION_VOCAB = (DET_STABLE, DET_IMPROVING, DET_DETERIORATING, DET_BROKEN, DET_UNKNOWN)

# --- Liquidity vocabulary ---------------------------------------------------- #
LIQ_LIQUID = "LIQUID"
LIQ_MODERATE = "MODERATE"
LIQ_ILLIQUID = "ILLIQUID"
LIQ_UNAVAILABLE = "UNAVAILABLE"
LIQUIDITY_VOCAB = (LIQ_LIQUID, LIQ_MODERATE, LIQ_ILLIQUID, LIQ_UNAVAILABLE)

# --- Confidence vocabulary --------------------------------------------------- #
CONF_HIGH = "HIGH"
CONF_MEDIUM = "MEDIUM"
CONF_LOW = "LOW"

# The improvement basis is always a score comparison; we NEVER claim a dollar
# expected return (no validated forecast model exists).
IMPROVEMENT_BASIS = "COMBINED_PERCENTILE_UPLIFT_NET_OF_MODELED_SWITCHING_COST"
EXPECTED_RETURN_STATE_UNAVAILABLE = "UNAVAILABLE"

NON_ALLOCATED_LABEL = "NON_ALLOCATED_COMPARISON_CANDIDATE"

# --- Track B — sector classification vocabulary ------------------------------ #
#: The display token for a holding whose sector could not be established from any
#: owned source. It is a DATA-QUALITY STATE, never an economic sector: names with
#: no classification share no evidenced common industry factor, so aggregating
#: them into one capped "Unknown" bucket fabricates a concentration claim (on
#: 2026-08-28 it manufactured a fictitious 31.7% "sector" that breached the 25%
#: cap and froze the workflow). Unclassified weight is therefore reported
#: explicitly (and flagged as a data gap) but never capped as an industry; each
#: unclassified name still faces the name-weight and risk-contribution caps.
UNCLASSIFIED_SECTOR = "Unknown"
#: The data-gap code raised when any held name has no resolvable sector.
GAP_SECTOR_CLASSIFICATION_MISSING = "SECTOR_CLASSIFICATION_MISSING"


def known_sector(*candidates) -> Optional[str]:
    """The first KNOWN sector among ``candidates`` (None when there is none).

    The string ``"Unknown"`` is treated as MISSING, not as a sector — before
    Track B a position row carrying the literal ``"Unknown"`` was truthy and
    silently shadowed the canonical universe-row sector that was available for
    every affected holding.
    """
    for c in candidates:
        if c and c != UNCLASSIFIED_SECTOR:
            return c
    return None


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned decision policy.

    Values marked ``reused`` mirror the canonical ``api.multi_horizon_engine`` /
    ``api.paper_trading_desk`` constants and are OVERRIDDEN by the API owner with
    the live values so no threshold is silently forked. Values marked ``new`` are
    genuinely-new Slice-6 thresholds justified in docs/ARCHITECTURE_DECISIONS.md;
    they are declared here once, returned in the payload and folded into the
    assessment hash, and are exercised at their boundaries by the test suite.
    """
    return {
        "policy_version": DECISION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
        # --- reused canonical scoring / construction / cost constants ---------- #
        "entry_rank": 25,                 # reused: primary top-25 book size N
        "exit_buffer_fraction": 0.20,     # reused: eng.EXIT_BUFFER_FRACTION
        "exit_buffer_rank": 30,           # reused: ceil(N*(1+exit_buffer_fraction))
        "sector_cap_fraction": 0.25,      # reused: eng.SECTOR_CAP_FRACTION
        "max_name_weight": 0.10,          # reused: eng.MAX_INDIVIDUAL_WEIGHT
        "min_adv_dollar": 1.0e7,          # reused: eng.MIN_ADV_DOLLAR liquidity floor
        "cost_bps_per_side": 12.5,        # reused: desk.COST_BPS_PER_SIDE
        "round_trip_cost_bps": 25.0,      # reused: 2 * cost_bps_per_side
        "cost_rate_per_side": 0.00125,    # reused: desk.COST_RATE_PER_SIDE
        # --- performance / risk / liquidity measurement windows --------------- #
        "return_windows": {"5d": 5, "20d": 20, "60d": 60},
        "min_return_obs": {"5d": 5, "20d": 20, "60d": 60},          # need the full window
        "volatility_windows": {"20d": 20, "60d": 60},
        "min_volatility_obs": {"20d": 15, "60d": 40},               # new: min sample
        "drawdown_window": 60,
        "min_drawdown_obs": 30,                                     # new: min sample
        "covariance_lookback": 60,                                  # new: aligned-return lookback
        "min_covariance_obs": 40,                                   # new: min aligned observations
        "covariance_variance_floor": 1.0e-12,                       # new: numerical tolerance
        # --- concentration / risk gates --------------------------------------- #
        # A name over-contributes to risk when its covariance variance share exceeds
        # this multiple of the equal-weight share (1 / n_covariance_names). This is
        # portfolio-size robust: at N=25 the effective trip is ~0.12 (3 / 25); it does
        # not spuriously fire in a tiny book where a large fair share is expected.
        "risk_contribution_excess_multiple": 3.0,                   # new
        # --- liquidity policy ------------------------------------------------- #
        "participation_rate": 0.10,                                 # new: max fraction of median $vol/day
        "liquidity_days_liquid_max": 1.0,                           # new: <=1 day -> LIQUID
        "liquidity_days_moderate_max": 3.0,                         # new: <=3 days -> MODERATE else ILLIQUID
        # --- improvement / replacement thresholds ----------------------------- #
        "min_gross_score_improvement": 0.02,                        # new: candidate must beat incumbent
        "min_net_improvement": 0.05,                                # new: REPLACE hurdle after costs
        "score_points_per_cost_bp": 0.001,                          # new: maps round-trip bps -> percentile hurdle
        "risk_penalty_weight": 0.5,                                 # new: risk-adjust gross by excess risk
        # --- deterioration ---------------------------------------------------- #
        "deterioration_rank_worsen_threshold": 5,                  # new: rank worsened by >= this
        "improvement_rank_threshold": 5,                          # new: rank improved by >= this
        # --- addition-candidate policy ---------------------------------------- #
        "addition_candidate_max": 10,                              # new: cap ADD candidates surfaced
    }


# --------------------------------------------------------------------------- #
# Small numeric / money helpers (stdlib only, deterministic)
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


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _round_money(x: Optional[float]) -> Optional[float]:
    """Decimal-safe money quantization to cents (Workstream D)."""
    if x is None:
        return None
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _mean(xs: list[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


def _std(xs: list[float], ddof: int = 1) -> Optional[float]:
    n = len(xs)
    if n - ddof <= 0:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - ddof))


def _median(xs: list[float]) -> Optional[float]:
    n = len(xs)
    if n == 0:
        return None
    s = sorted(xs)
    mid = n // 2
    return s[mid] if n % 2 == 1 else 0.5 * (s[mid - 1] + s[mid])


_TRADING_DAYS_YEAR = 252.0


# --------------------------------------------------------------------------- #
# Performance / risk / liquidity primitives (Workstream D). Each is PIT: it uses
# only the trailing arrays supplied in the contract (ascending, ending at the
# eligible bar) and never looks ahead.
# --------------------------------------------------------------------------- #
def trailing_return(adj: list[float], k: int, min_obs: int) -> Optional[float]:
    """Trailing simple total return over ``k`` closes ending at the last bar."""
    vals = [v for v in (adj or []) if _f(v) is not None]
    if len(vals) < min_obs + 1 or len(vals) <= k:
        return None
    base = vals[-1 - k]
    last = vals[-1]
    return (last / base - 1.0) if base > 0 else None


def realized_volatility(ret: list[Optional[float]], k: int, min_obs: int) -> Optional[float]:
    """Annualized realized volatility from the last ``k`` daily returns."""
    seg = [r for r in (ret or [])[-k:] if _f(r) is not None]
    if len(seg) < min_obs:
        return None
    s = _std(seg, 1)
    return (s * math.sqrt(_TRADING_DAYS_YEAR)) if s is not None else None


def max_drawdown(adj: list[float], k: int, min_obs: int) -> Optional[float]:
    """Maximum peak-to-trough drawdown (<= 0) over the last ``k`` closes."""
    seg = [v for v in (adj or [])[-(k + 1):] if _f(v) is not None]
    if len(seg) < min_obs:
        return None
    peak = seg[0]
    mdd = 0.0
    for v in seg:
        if v > peak:
            peak = v
        if peak > 0:
            mdd = min(mdd, v / peak - 1.0)
    return mdd


def days_to_liquidate(market_value: Optional[float], median_dollar_volume: Optional[float],
                      participation_rate: float) -> Optional[float]:
    """Estimated trading days to liquidate = market value / allowed daily participation.

    Returns None (liquidity UNAVAILABLE) when owned median dollar volume is absent —
    volume is never invented.
    """
    mv = _f(market_value)
    mdv = _f(median_dollar_volume)
    if mv is None or mdv is None or mdv <= 0 or participation_rate <= 0:
        return None
    allowed = participation_rate * mdv
    return mv / allowed if allowed > 0 else None


def liquidity_state(days: Optional[float], policy: dict) -> str:
    if days is None:
        return LIQ_UNAVAILABLE
    if days <= policy["liquidity_days_liquid_max"]:
        return LIQ_LIQUID
    if days <= policy["liquidity_days_moderate_max"]:
        return LIQ_MODERATE
    return LIQ_ILLIQUID


# --------------------------------------------------------------------------- #
# Covariance risk contribution (Workstream D.4)
# --------------------------------------------------------------------------- #
def _sample_covariance(a: list[float], b: list[float]) -> Optional[float]:
    n = len(a)
    if n != len(b) or n < 2:
        return None
    ma = sum(a) / n
    mb = sum(b) / n
    return sum((a[i] - ma) * (b[i] - mb) for i in range(n)) / (n - 1)


def build_covariance(*, tickers, aligned_returns: dict, policy: dict) -> dict:
    """The ONE daily-return covariance builder.

    ``aligned_returns`` = ``{"dates": [...], "series": {ticker: [ret|None ...]}}``.
    A ticker is included only when at least ``min_covariance_obs`` observations
    survive over the dates where EVERY included ticker has a value; the rest are
    excluded BY NAME with a reason rather than silently zero-filled.

    Extracted in Release 30 so the zero-base allocator can optimise against the
    same matrix the risk contributions are read from. A second covariance builder
    would be a second risk owner, and the two would disagree the first time a
    lookback or an alignment rule changed.
    """
    series = (aligned_returns or {}).get("series") or {}
    lookback = int(policy["covariance_lookback"])
    min_obs = int(policy["min_covariance_obs"])
    cand = sorted(tk for tk in tickers if tk in series)

    trimmed = {tk: list(series[tk])[-lookback:] for tk in cand}
    length = min((len(v) for v in trimmed.values()), default=0)
    included: list[str] = []
    excluded: dict[str, str] = {}
    aligned: dict[str, list] = {}
    if length >= min_obs and cand:
        # Align by trailing position; keep indices where every candidate is non-None.
        for tk in cand:
            trimmed[tk] = trimmed[tk][-length:]
        common_idx = [i for i in range(length)
                      if all(_f(trimmed[tk][i]) is not None for tk in cand)]
        if len(common_idx) >= min_obs:
            included = list(cand)
            aligned = {tk: [float(trimmed[tk][i]) for i in common_idx] for tk in cand}
        else:
            for tk in cand:
                excluded[tk] = "INSUFFICIENT_ALIGNED_OBS"
    else:
        for tk in cand:
            excluded[tk] = "INSUFFICIENT_ALIGNED_OBS"
    for tk in tickers:
        if tk not in cand:
            excluded.setdefault(tk, "NO_RETURN_SERIES")

    cov: dict[str, dict[str, float]] = {}
    if included:
        cov = {i: {} for i in included}
        for i in included:
            for j in included:
                if j in cov[i]:
                    continue
                c = _sample_covariance(aligned[i], aligned[j])
                cov[i][j] = c if c is not None else 0.0
                cov[j][i] = cov[i][j]
    return {
        "included_tickers": sorted(included),
        "excluded_tickers": dict(sorted(excluded.items())),
        "covariance": cov,
        "observations_used": (len(next(iter(aligned.values()))) if included else 0),
        "lookback": lookback,
        "min_observations": min_obs,
        "basis": "DAILY_SAMPLE_COVARIANCE_OVER_COMMON_ALIGNED_DATES",
    }


def compute_risk_contributions(*, weights: dict[str, float],
                               aligned_returns: dict, policy: dict) -> dict:
    """Standard covariance risk contribution w_i (Σw)_i / (wᵀΣw).

    Uses ``build_covariance`` for the matrix itself; only tickers with a positive
    weight are candidates. Portfolio variance below ``covariance_variance_floor``
    yields UNAVAILABLE for all (never a forced result). Returns a dict with a
    ``contributions`` map ({ticker: pct|None}) and diagnostic metadata.
    """
    built = build_covariance(
        tickers=sorted(tk for tk in weights if (weights.get(tk) or 0.0) > 0),
        aligned_returns=aligned_returns, policy=policy)
    included = list(built["included_tickers"])
    excluded = dict(built["excluded_tickers"])
    cov = built["covariance"]
    for tk in weights:
        if tk not in included:
            excluded.setdefault(tk, "NO_RETURN_SERIES")

    contributions: dict[str, Optional[float]] = {tk: None for tk in weights}
    portfolio_variance: Optional[float] = None
    obs_used = 0
    if included:
        obs_used = int(built["observations_used"])
        # Renormalize weights over the covariance universe (documented policy).
        wsum = sum(max(0.0, weights[tk]) for tk in included)
        if wsum > 0:
            w = {tk: max(0.0, weights[tk]) / wsum for tk in included}
            sigma_w = {i: sum(cov[i][j] * w[j] for j in included) for i in included}
            pvar = sum(w[i] * sigma_w[i] for i in included)
            portfolio_variance = pvar
            if pvar > policy["covariance_variance_floor"]:
                for tk in included:
                    contributions[tk] = (w[tk] * sigma_w[tk]) / pvar
            else:
                for tk in included:
                    excluded[tk] = "PORTFOLIO_VARIANCE_BELOW_FLOOR"
                included = []
    state = "AVAILABLE" if included else "UNAVAILABLE"
    return {
        "state": state,
        "contributions": contributions,
        "included_tickers": sorted(included),
        "excluded_tickers": dict(sorted(excluded.items())),
        "observations_used": obs_used,
        "portfolio_variance_daily": portfolio_variance,
        "lookback": int(built["lookback"]),
        "min_observations": int(built["min_observations"]),
        "weight_basis": "renormalized over the covariance-eligible held names",
    }


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
# Deterioration
# --------------------------------------------------------------------------- #
def _deterioration(*, eligible: bool, hard_codes: list, current_rank: Optional[int],
                   previous_rank: Optional[int], policy: dict) -> tuple[str, list]:
    codes: list[str] = []
    exit_buffer = policy["exit_buffer_rank"]
    entry = policy["entry_rank"]
    if not eligible or hard_codes:
        for c in hard_codes:
            codes.append("INELIGIBLE_%s" % c)
        if not eligible and not hard_codes:
            codes.append("NOT_ELIGIBLE")
        return DET_BROKEN, sorted(set(codes))
    if current_rank is not None and current_rank > exit_buffer:
        codes.append("FELL_BELOW_EXIT_BUFFER")
        return DET_BROKEN, codes
    if previous_rank is None:
        codes.append("PRIOR_RANK_UNAVAILABLE")
        if current_rank is not None and current_rank > entry:
            codes.append("WITHIN_EXIT_BUFFER")
            return DET_DETERIORATING, codes
        return DET_UNKNOWN, codes
    if current_rank is None:
        codes.append("CURRENT_RANK_UNAVAILABLE")
        return DET_UNKNOWN, codes
    delta = current_rank - previous_rank  # positive => worsened (rank number grew)
    if delta >= policy["deterioration_rank_worsen_threshold"]:
        codes.append("RANK_WORSENED")
        if current_rank > entry:
            codes.append("WITHIN_EXIT_BUFFER")
        return DET_DETERIORATING, codes
    if -delta >= policy["improvement_rank_threshold"]:
        codes.append("RANK_IMPROVED")
        return DET_IMPROVING, codes
    if current_rank > entry:
        codes.append("WITHIN_EXIT_BUFFER")
        return DET_DETERIORATING, codes
    codes.append("RANK_STABLE")
    return DET_STABLE, codes


# --------------------------------------------------------------------------- #
# Replacement eligibility (Workstream F)
# --------------------------------------------------------------------------- #
def _strongest_replacement(*, held: set, universe_by_rank: list, sector_weight: dict,
                           policy: dict) -> tuple:
    """The single strongest eligible non-held candidate (best rank), applying canonical
    universe eligibility, the no-duplicate-holding rule, the liquidity floor and the
    sector-concentration constraint. Returns ``(chosen|None, rejected)`` where each
    rejected entry is ``{ticker, rank, reason}`` (surfaced in diagnostics — Workstream F).
    The chosen row is a NON-ALLOCATED comparison candidate; the same candidate may be
    surfaced for multiple holdings.
    """
    rejected: list[dict] = []
    chosen: Optional[dict] = None
    sector_cap = policy["sector_cap_fraction"]
    for row in universe_by_rank:
        tk = row.get("ticker")
        if tk in held:
            rejected.append({"ticker": tk, "rank": row.get("rank"),
                             "reason": "ALREADY_HELD"})
            continue
        if not row.get("eligible", True):
            rejected.append({"ticker": tk, "rank": row.get("rank"),
                             "reason": "NOT_ELIGIBLE"})
            continue
        adv = _f(row.get("adv_dollar"))
        if adv is not None and adv < policy["min_adv_dollar"]:
            rejected.append({"ticker": tk, "rank": row.get("rank"),
                             "reason": "LIQUIDITY_FILTER_FAILED"})
            continue
        sec = row.get("sector") or "Unknown"
        if sec != "Unknown" and sector_weight.get(sec, 0.0) >= sector_cap - 1e-12:
            rejected.append({"ticker": tk, "rank": row.get("rank"),
                             "reason": "SECTOR_CONCENTRATION_CONSTRAINED"})
            continue
        if chosen is None:
            chosen = row
        # keep scanning only to record a bounded rejected list.
        if len(rejected) >= 25:
            break
    return chosen, rejected


# --------------------------------------------------------------------------- #
# Core entry point
# --------------------------------------------------------------------------- #
def build_assessment(*, input_contract: dict, policy: Optional[dict] = None) -> dict:
    """Compute the canonical holding opportunity-cost assessment (pure, deterministic).

    ``input_contract`` is the immutable assessment-input contract (Workstream B).
    Returns the frozen result contract (Workstream C). Never raises on incomplete
    data — it degrades to DEGRADED / BLOCKED with explicit reason codes.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    ic = input_contract or {}
    eligible_date = ic.get("eligible_market_date")
    positions = list(ic.get("positions") or [])
    universe_rows = list(ic.get("universe_rows") or [])
    active_book_id = ic.get("active_book_id")

    # --- core-input validation -> BLOCKED / NO_ACTIVE_BOOK ------------------- #
    blockers: list[dict] = []
    if not active_book_id:
        return _empty_result(pol, ic, STATE_NO_ACTIVE_BOOK,
                             [{"code": "NO_ACTIVE_BOOK",
                               "detail": "No active operational book was supplied."}])
    if not eligible_date:
        blockers.append({"code": "MISSING_ELIGIBLE_MARKET_DATE"})
    if not ic.get("portfolio_state_hash"):
        blockers.append({"code": "MISSING_PORTFOLIO_STATE_HASH"})
    if not ic.get("universe_scoring_hash"):
        blockers.append({"code": "MISSING_UNIVERSE_SCORING_HASH"})
    if not universe_rows:
        blockers.append({"code": "MISSING_UNIVERSE_ROWS"})
    if not positions:
        blockers.append({"code": "NO_HOLDINGS"})
    if blockers:
        return _empty_result(pol, ic, STATE_BLOCKED, blockers)

    # --- index the universe -------------------------------------------------- #
    urows = {r.get("ticker"): r for r in universe_rows if r.get("ticker")}
    universe_by_rank = sorted(
        (r for r in universe_rows if r.get("ticker") and r.get("rank") is not None),
        key=lambda r: (r.get("rank"), r.get("ticker")))
    held = {p.get("ticker") for p in positions if p.get("ticker")}

    prev_ranking = ic.get("previous_ranking")
    prev_state = ic.get("previous_ranking_state") or (
        "AVAILABLE" if prev_ranking else "UNAVAILABLE")
    prev_reason = ic.get("previous_ranking_reason") or (
        "" if prev_ranking else "No prior point-in-time ranking snapshot is available.")
    prior_signal = ic.get("prior_signal") or {}
    holding_elig = ic.get("holding_eligibility") or {}
    trailing = ic.get("trailing_prices") or {}
    mdv_map = ic.get("median_dollar_volume") or {}
    aligned_returns = ic.get("aligned_returns") or {}

    nav = _f(ic.get("nav"))
    cash = _f(ic.get("cash"))

    # --- covariance risk contributions (once, over held names) --------------- #
    weights = {p.get("ticker"): (_f(p.get("current_weight")) or 0.0) for p in positions
               if p.get("ticker")}
    risk = compute_risk_contributions(weights=weights, aligned_returns=aligned_returns,
                                      policy=pol)
    risk_contrib = risk["contributions"]
    # Effective single-name risk-contribution trip: the smaller of the absolute ceiling
    # and (excess multiple / equal-weight share). Portfolio-size robust (Workstream D).
    _n_cov = len(risk.get("included_tickers") or [])
    eff_risk_threshold = (pol["risk_contribution_excess_multiple"] / _n_cov
                          if _n_cov > 0 else None)

    # --- concentration inputs (sector weights, HHI share) -------------------- #
    # Track B — sector is resolved ONCE per holding, before any aggregation:
    # the position row first, then the canonical universe row (which composes the
    # owned GICS map). The literal "Unknown" is a missing classification, never a
    # sector, so it can neither shadow a knowable sector nor aggregate into a
    # fabricated capped industry. Unclassified weight is reported explicitly.
    resolved_sector: dict[str, Optional[str]] = {}
    for p in positions:
        tk = p.get("ticker")
        if tk:
            resolved_sector[tk] = known_sector(p.get("sector"),
                                               (urows.get(tk) or {}).get("sector"))
    sector_weight: dict[str, float] = {}
    unclassified_sector_weight = 0.0
    unclassified_sector_tickers: list[str] = []
    for p in positions:
        w = _f(p.get("current_weight")) or 0.0
        sec = resolved_sector.get(p.get("ticker"))
        if sec is None:
            unclassified_sector_weight += w
            if p.get("ticker"):
                unclassified_sector_tickers.append(p["ticker"])
        else:
            sector_weight[sec] = sector_weight.get(sec, 0.0) + w
    unclassified_sector_tickers.sort()
    hhi = sum((_f(p.get("current_weight")) or 0.0) ** 2 for p in positions)

    # --- the single strongest eligible non-held comparison candidate --------- #
    replacement, replacement_rejections = _strongest_replacement(
        held=held, universe_by_rank=universe_by_rank, sector_weight=sector_weight,
        policy=pol)

    round_trip_rate = 2.0 * pol["cost_rate_per_side"]
    switching_cost_bps = pol["round_trip_cost_bps"]

    reviews: list[dict] = []
    rejected_candidates: list[dict] = []
    data_gaps: set[str] = set()
    if prev_state != "AVAILABLE":
        data_gaps.add("PRIOR_RANK_UNAVAILABLE")
    if risk["state"] != "AVAILABLE":
        data_gaps.add("RISK_CONTRIBUTION_UNAVAILABLE")

    for p in sorted(positions, key=lambda x: (-(_f(x.get("current_weight")) or 0.0),
                                              x.get("ticker") or "")):
        tk = p.get("ticker")
        urow = urows.get(tk) or {}
        cw = _f(p.get("current_weight"))
        mv = _f(p.get("market_value"))
        sec = resolved_sector.get(tk) or UNCLASSIFIED_SECTOR

        current_rank = urow.get("rank")
        elig_info = holding_elig.get(tk) or {}
        eligible = bool(urow) and bool(urow.get("eligible", elig_info.get("eligible", True)))
        hard_codes = list(elig_info.get("hard_codes") or [])
        if not urow:
            eligible = False
            hard_codes = sorted(set(hard_codes + ["NOT_IN_ELIGIBLE_UNIVERSE"]))

        previous_rank = None
        if isinstance(prev_ranking, dict):
            previous_rank = prev_ranking.get(tk)
        rank_change = ((previous_rank - current_rank)
                       if (previous_rank is not None and current_rank is not None) else None)

        current_score = _f(urow.get("percentile"))
        combined_score = _f(urow.get("combined_score"))
        score_components = {
            "combined_score": combined_score,
            "combined_percentile": current_score,
            "fundamental_score": _f(urow.get("fundamental_score")),
            "fundamental_percentile": _f(urow.get("fundamental_percentile")),
            "momentum_score": _f(urow.get("momentum_score")),
            "momentum_percentile": _f(urow.get("momentum_percentile")),
        }
        signal_strength = current_score  # blended combined percentile in [0,1]

        det_state, det_codes = _deterioration(
            eligible=eligible, hard_codes=hard_codes, current_rank=current_rank,
            previous_rank=previous_rank, policy=pol)
        # signal decay contributes a deterioration code (never overrides BROKEN).
        prior_sig = _f(prior_signal.get(tk))
        if (prior_sig is not None and signal_strength is not None
                and (prior_sig - signal_strength) >= 0.10 and det_state != DET_BROKEN):
            det_codes = sorted(set(det_codes + ["SIGNAL_DECAY"]))
            if det_state == DET_STABLE:
                det_state = DET_DETERIORATING

        # --- performance / risk / liquidity ---------------------------------- #
        tp = trailing.get(tk) or {}
        adj = [x for x in (tp.get("adj") or [])]
        ret = [x for x in (tp.get("ret") or [])]
        rw = pol["return_windows"]
        rmo = pol["min_return_obs"]
        return_5d = trailing_return(adj, rw["5d"], rmo["5d"])
        return_20d = trailing_return(adj, rw["20d"], rmo["20d"])
        return_60d = trailing_return(adj, rw["60d"], rmo["60d"])
        vw = pol["volatility_windows"]
        vmo = pol["min_volatility_obs"]
        vol_20d = realized_volatility(ret, vw["20d"], vmo["20d"])
        vol_60d = realized_volatility(ret, vw["60d"], vmo["60d"])
        drawdown_60d = max_drawdown(adj, pol["drawdown_window"], pol["min_drawdown_obs"])

        mdv = _f(mdv_map.get(tk))
        dtl = days_to_liquidate(mv, mdv, pol["participation_rate"])
        liq_state = liquidity_state(dtl, pol)
        if liq_state == LIQ_UNAVAILABLE:
            data_gaps.add("LIQUIDITY_UNAVAILABLE")

        rc = risk_contrib.get(tk)
        concentration_contribution = ((cw or 0.0) ** 2 / hhi) if (hhi and hhi > 0) else None

        # --- replacement comparison (non-allocated) -------------------------- #
        (strongest_ticker, replacement_rank, replacement_score, replacement_sector,
         gross_improvement, risk_adj_improvement, switching_cost_usd, net_improvement,
         replacement_eligible) = (None, None, None, None, None, None, None, None, False)
        if replacement is not None and replacement.get("ticker") != tk:
            strongest_ticker = replacement.get("ticker")
            replacement_rank = replacement.get("rank")
            replacement_score = _f(replacement.get("percentile"))
            replacement_sector = replacement.get("sector")
            if replacement_score is not None and current_score is not None:
                gross_improvement = replacement_score - current_score
                # risk adjustment: penalize keeping a name whose risk contribution is
                # excessive (documented — the incumbent's excess variance share).
                excess_risk = 0.0
                if (rc is not None and eff_risk_threshold is not None
                        and rc > eff_risk_threshold):
                    excess_risk = rc - eff_risk_threshold
                risk_adj_improvement = gross_improvement + pol["risk_penalty_weight"] * excess_risk
                switching_cost_usd = _round_money((mv or 0.0) * round_trip_rate)
                cost_hurdle = switching_cost_bps * pol["score_points_per_cost_bp"]
                net_improvement = risk_adj_improvement - cost_hurdle
                replacement_eligible = True

        # --- recommendation policy (Workstream G) ---------------------------- #
        breach_codes: list[str] = []
        if cw is not None and cw > pol["max_name_weight"] + 1e-12:
            breach_codes.append("NAME_WEIGHT_BREACH")
        # Track B — only a KNOWN sector can breach the sector cap. An unclassified
        # holding is a data-quality state (reported in the portfolio summary and as
        # a data gap), never a member of a fabricated capped industry.
        if (sec != UNCLASSIFIED_SECTOR
                and sector_weight.get(sec, 0.0) > pol["sector_cap_fraction"] + 1e-12):
            breach_codes.append("SECTOR_WEIGHT_BREACH")
        if (rc is not None and eff_risk_threshold is not None
                and rc > eff_risk_threshold + 1e-12):
            breach_codes.append("RISK_CONTRIBUTION_BREACH")

        required_data_complete = (current_rank is not None and current_score is not None
                                  and return_20d is not None and vol_60d is not None)

        recommendation, rec_codes, explanation, confidence = _recommend(
            det_state=det_state, det_codes=det_codes, breach_codes=breach_codes,
            eligible=eligible, hard_codes=hard_codes, current_rank=current_rank,
            replacement_eligible=replacement_eligible, gross_improvement=gross_improvement,
            net_improvement=net_improvement, replacement_ticker=strongest_ticker,
            required_data_complete=required_data_complete, ticker=tk, policy=pol)

        if (replacement_eligible and recommendation != REC_REPLACE
                and gross_improvement is not None):
            reason = ("BELOW_GROSS_THRESHOLD"
                      if gross_improvement < pol["min_gross_score_improvement"]
                      else ("BELOW_NET_THRESHOLD"
                            if (net_improvement is None
                                or net_improvement < pol["min_net_improvement"])
                            else "INCUMBENT_NOT_BROKEN_OR_BREACHED"))
            rejected_candidates.append({
                "holding": tk, "candidate": strongest_ticker,
                "gross_score_improvement": _r(gross_improvement, 6),
                "net_improvement": _r(net_improvement, 6), "reason": reason})

        reviews.append({
            "ticker": tk,
            "sector": sec,
            "current_quantity": p.get("quantity"),
            "current_weight": _r(cw, 6),
            "market_value": _round_money(mv),
            "current_rank": current_rank,
            "previous_rank": previous_rank,
            "rank_change": rank_change,
            "current_score": _r(combined_score, 6),
            "score_components": {k: _r(v, 6) for k, v in score_components.items()},
            "signal_strength": _r(signal_strength, 6),
            "deterioration_state": det_state,
            "deterioration_reason_codes": det_codes,
            "return_5d": _r(return_5d, 6),
            "return_20d": _r(return_20d, 6),
            "return_60d": _r(return_60d, 6),
            "volatility_20d": _r(vol_20d, 6),
            "volatility_60d": _r(vol_60d, 6),
            "drawdown_60d": _r(drawdown_60d, 6),
            "risk_contribution_pct": _r(rc, 6),
            "concentration_contribution": _r(concentration_contribution, 6),
            "median_dollar_volume_20d": _round_money(mdv),
            "estimated_days_to_liquidate": _r(dtl, 4),
            "liquidity_state": liq_state,
            "strongest_replacement_ticker": strongest_ticker,
            "replacement_rank": replacement_rank,
            "replacement_score": _r(replacement_score, 6),
            "replacement_sector": replacement_sector,
            "replacement_label": (NON_ALLOCATED_LABEL if strongest_ticker else None),
            "gross_score_improvement": _r(gross_improvement, 6),
            "risk_adjusted_improvement": _r(risk_adj_improvement, 6),
            "switching_cost_bps": _r(switching_cost_bps, 4),
            "switching_cost_usd": switching_cost_usd,
            "net_improvement": _r(net_improvement, 6),
            "improvement_basis": IMPROVEMENT_BASIS,
            "expected_return_delta": None,
            "expected_return_delta_state": EXPECTED_RETURN_STATE_UNAVAILABLE,
            "recommendation": recommendation,
            "recommendation_confidence": confidence,
            "reason_codes": rec_codes,
            "explanation": explanation,
            "required_data_complete": bool(required_data_complete),
        })

    # --- addition candidates (Workstream G ADD) ------------------------------ #
    addition_candidates = _addition_candidates(
        universe_by_rank=universe_by_rank, held=held, policy=pol)

    # --- portfolio summary --------------------------------------------------- #
    if unclassified_sector_tickers:
        data_gaps.add(GAP_SECTOR_CLASSIFICATION_MISSING)
    portfolio_summary = _portfolio_summary(
        positions=positions, nav=nav, cash=cash, sector_weight=sector_weight,
        hhi=hhi, risk=risk, policy=pol,
        unclassified_sector_weight=unclassified_sector_weight,
        unclassified_sector_tickers=unclassified_sector_tickers)

    # --- recommendation counts ---------------------------------------------- #
    rec_counts = {k: 0 for k in RECOMMENDATION_VOCAB}
    for r in reviews:
        rec_counts[r["recommendation"]] = rec_counts.get(r["recommendation"], 0) + 1
    rec_counts[REC_ADD] = len(addition_candidates)

    # --- data quality / assessment state ------------------------------------ #
    complete_count = sum(1 for r in reviews if r["required_data_complete"])
    data_quality = {
        "holdings_evaluated": len(reviews),
        "holdings_data_complete": complete_count,
        "previous_ranking_state": prev_state,
        "previous_ranking_reason": prev_reason,
        "risk_contribution_state": risk["state"],
        "liquidity_names_unavailable": sum(1 for r in reviews
                                           if r["liquidity_state"] == LIQ_UNAVAILABLE),
        "data_gaps": sorted(data_gaps),
    }
    assessment_state = STATE_DEGRADED if data_gaps else STATE_READY

    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": eligible_date,
        "active_book_id": active_book_id,
        "assessment_state": assessment_state,
        "state_vocabulary": list(ASSESSMENT_STATE_VOCAB),
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "policy": pol,
        "portfolio_summary": portfolio_summary,
        "recommendation_counts": rec_counts,
        # Release 46.6 - ONE business concept, ONE authoritative name.
        #
        # Two different read models published a key called
        # ``recommendation_counts`` carrying two different concepts: these
        # SIGNAL-LEVEL per-holding review recommendations, and the
        # POST-PORTFOLIO-GATE actionable counts that
        # engine.portfolio_reassessment produces after churn control,
        # concentration and turnover budget have had their say. The numbers
        # differ and both were correct; the NAME was not. The explicit key and
        # the declared semantic below are what a consumer should bind to, and
        # ``recommendation_counts`` is retained unchanged for compatibility.
        "signal_level_holding_recommendation_counts": dict(rec_counts),
        "recommendation_counts_semantic": SEMANTIC_SIGNAL_LEVEL,
        "recommendation_counts_scope": (
            "raw per-holding opportunity-cost review signals, before any "
            "portfolio-level gate has been applied"),
        "holding_reviews": reviews,
        "addition_candidates": addition_candidates,
        "diagnostics": {
            "rejected_replacement_candidates": rejected_candidates,
            "rejected_candidate_scan": replacement_rejections,
            "non_allocated_comparison_candidate": (
                replacement.get("ticker") if replacement else None),
            "non_allocated_note": (
                "A single strongest eligible non-held candidate may be surfaced as the "
                "comparison for multiple holdings. It is labelled %s and is NEVER an "
                "approved target — the Reallocation Proposal engine (Slice 7) does not "
                "exist yet." % NON_ALLOCATED_LABEL),
            "risk_contribution": {k: v for k, v in risk.items() if k != "contributions"},
            "rank_snapshot": {r.get("ticker"): r.get("rank")
                              for r in universe_by_rank if r.get("ticker")},
            "held_count": len(held),
            "eligible_universe_size": len(universe_by_rank),
        },
        "data_quality": data_quality,
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    result["assessment_hash"] = stable_hash(result)
    return result


def _recommend(*, det_state, det_codes, breach_codes, eligible, hard_codes, current_rank,
               replacement_eligible, gross_improvement, net_improvement, replacement_ticker,
               required_data_complete, ticker, policy) -> tuple[str, list, str, str]:
    """Deterministic HOLD/REDUCE/EXIT/REPLACE decision for a HELD name.

    Precedence: EXIT (broken) > REDUCE (concentration/risk breach) > REPLACE
    (qualified better alternative net of cost) > HOLD.
    """
    codes: list[str] = []
    # 1. EXIT — ineligible / materially broken (no replacement required).
    if det_state == DET_BROKEN:
        codes = sorted(set(det_codes))
        expl = ("%s is no longer an eligible / retained holding (%s); EXIT is "
                "recommended for manual review. No replacement is required to establish "
                "the exit." % (ticker, ", ".join(codes) or "broken"))
        return REC_EXIT, codes, expl, (CONF_HIGH if hard_codes or current_rank else CONF_MEDIUM)
    # 2. REDUCE — concentration / risk breach; complete exit not otherwise supported.
    if breach_codes:
        codes = sorted(set(breach_codes))
        expl = ("%s remains eligible but breaches a concentration / risk limit (%s); "
                "REDUCE toward policy is recommended for manual review." %
                (ticker, ", ".join(codes)))
        return REC_REDUCE, codes, expl, CONF_MEDIUM
    # 3. REPLACE — a specific eligible alternative clears the net threshold after costs.
    if (replacement_eligible and gross_improvement is not None
            and gross_improvement >= policy["min_gross_score_improvement"] - 1e-9
            and net_improvement is not None
            and net_improvement >= policy["min_net_improvement"] - 1e-9
            and required_data_complete):
        codes = ["QUALIFIED_REPLACEMENT_CLEARS_NET_THRESHOLD"]
        if det_state == DET_DETERIORATING:
            codes.append("INCUMBENT_DETERIORATING")
        expl = ("A stronger eligible alternative (%s) clears the net-improvement "
                "threshold after switching cost; REPLACE is recommended for manual "
                "review. This is a NON-ALLOCATED comparison — no target is created." %
                replacement_ticker)
        return REC_REPLACE, sorted(set(codes)), expl, (
            CONF_HIGH if net_improvement >= 2 * policy["min_net_improvement"] else CONF_MEDIUM)
    # 4. HOLD.
    codes = sorted(set(det_codes)) or ["REMAINS_ELIGIBLE_NO_QUALIFIED_ALTERNATIVE"]
    expl = ("%s remains eligible with no severe deterioration, no material risk / "
            "concentration breach, and no replacement clearing the net-improvement "
            "threshold; HOLD." % ticker)
    conf = CONF_HIGH if required_data_complete else CONF_LOW
    return REC_HOLD, codes, expl, conf


def _addition_candidates(*, universe_by_rank, held, policy) -> list[dict]:
    """Eligible non-held names inside the entry rank surfaced as ADD candidates.

    Report-only (Slice 7 does not exist): no weight is assigned and no order is
    created. Each is a candidate for the future Reallocation Proposal engine.
    """
    out: list[dict] = []
    entry = policy["entry_rank"]
    for row in universe_by_rank:
        tk = row.get("ticker")
        rank = row.get("rank")
        if tk in held or rank is None or rank > entry:
            continue
        if not row.get("eligible", True):
            continue
        adv = _f(row.get("adv_dollar"))
        if adv is not None and adv < policy["min_adv_dollar"]:
            continue
        out.append({
            "ticker": tk,
            "rank": rank,
            "score": _r(_f(row.get("percentile")), 6),
            "combined_score": _r(_f(row.get("combined_score")), 6),
            "sector": row.get("sector"),
            "recommendation": REC_ADD,
            "reason_codes": ["ELIGIBLE_TOP_%d_NOT_HELD" % entry],
            "explanation": ("%s is an eligible top-%d name not currently held; ADD "
                            "candidate for the future Reallocation Proposal engine "
                            "(Slice 7). No weight is assigned and no order is created." %
                            (tk, entry)),
            "allocation": None,
        })
        if len(out) >= policy["addition_candidate_max"]:
            break
    return out


def _portfolio_summary(*, positions, nav, cash, sector_weight, hhi, risk, policy,
                       unclassified_sector_weight: float = 0.0,
                       unclassified_sector_tickers: Optional[list] = None) -> dict:
    invested = sum(_f(p.get("market_value")) or 0.0 for p in positions)
    max_name = None
    max_name_tk = None
    for p in positions:
        w = _f(p.get("current_weight"))
        if w is not None and (max_name is None or w > max_name):
            max_name = w
            max_name_tk = p.get("ticker")
    # Track B — max sector is taken over KNOWN sectors only; ``sector_weight`` no
    # longer aggregates unclassified names, which are reported explicitly below.
    max_sector = None
    max_sector_name = None
    for sec, w in sector_weight.items():
        if sec == UNCLASSIFIED_SECTOR:
            continue
        if max_sector is None or w > max_sector:
            max_sector = w
            max_sector_name = sec
    return {
        "nav": _round_money(nav),
        "cash": _round_money(cash),
        "invested_value": _round_money(invested),
        "holdings_count": len(positions),
        "max_name_weight": _r(max_name, 6),
        "max_name_ticker": max_name_tk,
        "max_name_weight_cap": policy["max_name_weight"],
        "max_sector_weight": _r(max_sector, 6),
        "max_sector": max_sector_name,
        "sector_cap_fraction": policy["sector_cap_fraction"],
        "herfindahl_index": _r(hhi, 6),
        "sector_weights": {k: _r(v, 6) for k, v in sorted(sector_weight.items())
                           if k != UNCLASSIFIED_SECTOR},
        # Track B — the DATA-QUALITY surface for missing sector classification.
        # Visible, never hidden; never capped as though it were one industry.
        "unclassified_sector_weight": _r(unclassified_sector_weight, 6),
        "unclassified_sector_tickers": list(unclassified_sector_tickers or []),
        "unclassified_sector_policy": (
            "UNCLASSIFIED weight is a data-quality state, not an economic sector: "
            "it is reported and gap-flagged, while each unclassified name still "
            "faces the name-weight and risk-contribution caps individually."),
        "portfolio_variance_daily": _r(risk.get("portfolio_variance_daily"), 10),
        "risk_contribution_state": risk.get("state"),
    }


def _safety() -> dict:
    return {
        "read_only": True,
        "paper_only": True,
        "manual_review": True,
        "preview_only": True,
        "created_target": False,
        "created_target_weights": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "promoted_model": False,
        "automatic_promotion_allowed": False,
        "automation_off": True,
        "safety_badges": ["PREVIEW ONLY", "REVIEW ONLY", "NO ORDERS", "NO LIVE ORDERS",
                          "AUTOMATION OFF", "MANUAL REVIEW", "NON-ALLOCATED COMPARISON"],
    }


def _provenance(ic: dict) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "portfolio_source": "api.portfolio_state (positions / weights / NAV / cash / sectors)",
        "scoring_source": "api.universe_scoring (rank / score / eligibility / adv_dollar)",
        "price_volume_source": "api.price_panel (owned PIT trailing close + dollar volume)",
        "transaction_cost_source": "api.paper_trading_desk.COST_RATE_PER_SIDE (reused)",
        "construction_policy_source": "api.multi_horizon_engine (entry/exit/sector/name/liquidity)",
        "eligible_market_date": ic.get("eligible_market_date"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        # Stage 21 (Workstream 0E) — the two fingerprints a downstream consumer must
        # bind to when it asks whether this assessment still describes the portfolio.
        # Recording them here is what makes that question answerable at all: before
        # Stage 21 the provenance carried NO corporate-action fingerprint, so every
        # consumer resolved `None` -> "bound to the EMPTY registry" -> permanently
        # STALE for as long as any corporate action stayed registered.
        "economic_state_hash": ic.get("economic_state_hash"),
        "economic_identity_version": ic.get("economic_identity_version"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "decision_policy_version": DECISION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
    }


def _empty_result(pol: dict, ic: dict, state: str, blockers: list) -> dict:
    """A readable BLOCKED / NO_ACTIVE_BOOK result (never raises; degrade-safe)."""
    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "assessment_state": state,
        "state_vocabulary": list(ASSESSMENT_STATE_VOCAB),
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "policy": pol,
        "portfolio_summary": {},
        "recommendation_counts": {k: 0 for k in RECOMMENDATION_VOCAB},
        "holding_reviews": [],
        "addition_candidates": [],
        "diagnostics": {"blockers": blockers},
        "data_quality": {"holdings_evaluated": 0, "data_gaps": sorted(
            {b["code"] for b in blockers})},
        "blockers": blockers,
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    result["assessment_hash"] = stable_hash(result)
    return result
