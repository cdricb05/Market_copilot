"""Stage 12 Workstream A -- Stage 11 failure autopsy (read-only).

This module reads the immutable Stage 11 research artifacts (never mutates them,
never touches an operational ledger) and produces a defensible, evidence-based
explanation of *why* Stage 11 produced zero qualified candidates.

The core question it answers is the one the standing research strategy demands:
was the Stage 11 negative a *genuine null*, or was the tournament *statistically
underpowered / over-penalised* because a 300-member family was dominated by
near-duplicate variations of the same conventional factors?

Everything here is a pure function over already-loaded artifact dicts, so it is
deterministic and unit-testable without any store or network. `run_autopsy`
is a thin convenience wrapper that loads the Stage 11 state directory.

Design notes
------------
* Two taxonomy views are produced, and each is labelled with its authority:
    - ``deep_taxonomy`` : the 24 candidates that received the FULL unchanged gate
      battery (``tournament.classify_evidence`` in Stage 11). These verdicts are
      AUTHORITATIVE.
    - ``screen_taxonomy`` : all supported candidates, classified from the SCREEN
      metrics only (a subset of the full gate battery). Clearly marked
      ``authority="screen_metrics"`` because the loose broad-screen never applied
      the full 2.0-t / cost / regime / drawdown gates.
* The weakest-gate vocabulary is exactly the enumerated Stage 12 set.
* The effective-hypothesis-count uses the participation ratio of the survivor
  long/short return streams -- a standard, defensible measure of how many
  economically independent tests the 300-member family actually represents.
* The BH re-computation at the effective family size is emitted as an explicit
  DIAGNOSTIC (never a qualification): it quantifies how much of the Stage 11
  negative was multiple-testing burden inflated by hypothesis redundancy.
"""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Weakest-gate vocabulary (exactly the enumerated Stage 12 set).
# ---------------------------------------------------------------------------
WEAK_IC = "WEAK_IC"
UNSTABLE_SIGN = "UNSTABLE_SIGN"
INSUFFICIENT_EFFECTIVE_SAMPLE = "INSUFFICIENT_EFFECTIVE_SAMPLE"
INSUFFICIENT_COVERAGE = "INSUFFICIENT_COVERAGE"
EXCESSIVE_TURNOVER = "EXCESSIVE_TURNOVER"
COST_DESTROYED = "COST_DESTROYED"
DRAWDOWN = "DRAWDOWN"
MARKET_EXPOSURE = "MARKET_EXPOSURE"
INDUSTRY_CONCENTRATION = "INDUSTRY_CONCENTRATION"
SUBPERIOD_CONCENTRATION = "SUBPERIOD_CONCENTRATION"
HOLDOUT_FAILURE = "HOLDOUT_FAILURE"
MULTIPLE_TESTING_FAILURE = "MULTIPLE_TESTING_FAILURE"
PARAMETER_INSTABILITY = "PARAMETER_INSTABILITY"
DUPLICATE_HYPOTHESIS = "DUPLICATE_HYPOTHESIS"
OTHER_EXPLICIT_REASON = "OTHER_EXPLICIT_REASON"

WEAKEST_GATE_REASONS: Tuple[str, ...] = (
    WEAK_IC, UNSTABLE_SIGN, INSUFFICIENT_EFFECTIVE_SAMPLE, INSUFFICIENT_COVERAGE,
    EXCESSIVE_TURNOVER, COST_DESTROYED, DRAWDOWN, MARKET_EXPOSURE,
    INDUSTRY_CONCENTRATION, SUBPERIOD_CONCENTRATION, HOLDOUT_FAILURE,
    MULTIPLE_TESTING_FAILURE, PARAMETER_INSTABILITY, DUPLICATE_HYPOTHESIS,
    OTHER_EXPLICIT_REASON,
)

# Map the unchanged-gate reason strings (tournament.classify_evidence blockers)
# onto the Stage 12 weakest-gate vocabulary.
_BLOCKER_TO_REASON: Dict[str, str] = {
    "REJECT_WEAK_RANK_IC": WEAK_IC,
    "REJECT_STATISTICALLY_WEAK_RANK_IC_T": WEAK_IC,
    "REJECT_WEAK_SPREAD_T": WEAK_IC,
    "REJECT_LOW_POSITIVE_IC_HIT_RATE": UNSTABLE_SIGN,
    "REJECT_NOT_COST_ROBUST_NET25_NONPOSITIVE": COST_DESTROYED,
    "REJECT_UNSTABLE_SUBPERIOD": SUBPERIOD_CONCENTRATION,
    "REJECT_UNSTABLE_REGIME": SUBPERIOD_CONCENTRATION,
    "REJECT_SEVERE_DRAWDOWN": DRAWDOWN,
    "REJECT_EXCESSIVE_TURNOVER": EXCESSIVE_TURNOVER,
    "REJECT_FATAL_SECTOR_CONCENTRATION": INDUSTRY_CONCENTRATION,
    "DATA_HOLD_INSUFFICIENT_COVERAGE": INSUFFICIENT_COVERAGE,
    "DATA_HOLD_INSUFFICIENT_OBSERVATIONS": INSUFFICIENT_EFFECTIVE_SAMPLE,
    "DATA_HOLD_INSUFFICIENT_UNIVERSE": INSUFFICIENT_COVERAGE,
    "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE": OTHER_EXPLICIT_REASON,
    "DATA_HOLD_LOOKAHEAD_CONTAMINATION": OTHER_EXPLICIT_REASON,
}

# Unchanged-gate thresholds (from configs/alpha_agent/stage9_tournament.json) --
# used only to *classify* screen-level candidates against the real gate the loose
# broad-screen never applied. Not a re-evaluation; a labelling aid.
GATE_MIN_RANK_IC = 0.010
GATE_MIN_RANK_IC_T = 2.0
GATE_MIN_SPREAD_T = 2.0
GATE_MIN_NET25 = 0.0
GATE_MAX_TURNOVER = 2.0
GATE_MIN_SUBPERIOD = 0.60


# ---------------------------------------------------------------------------
# Effective number of independent hypotheses (participation ratio).
# ---------------------------------------------------------------------------
def _aligned_return_matrix(series_by_spec: Mapping[str, Mapping[str, Any]]
                           ) -> Tuple[List[str], List[str], List[List[float]]]:
    """Align the survivor long/short streams onto their common rebalance dates.

    Returns (spec_ids, common_dates, matrix) where matrix[i] is stream i sampled
    on common_dates. Only streams that cover every common date are retained so
    the correlation matrix is computed on complete cases (no imputation).
    """
    per_spec_map: Dict[str, Dict[str, float]] = {}
    date_sets: List[set] = []
    for sid, s in series_by_spec.items():
        if not isinstance(s, Mapping):
            continue
        dates = s.get("dates")
        ls = s.get("ls")
        if not isinstance(dates, Sequence) or not isinstance(ls, Sequence):
            continue
        m: Dict[str, float] = {}
        for d, v in zip(dates, ls):
            if v is None:
                continue
            try:
                m[str(d)] = float(v)
            except (TypeError, ValueError):
                continue
        if m:
            per_spec_map[sid] = m
            date_sets.append(set(m.keys()))
    if not per_spec_map:
        return [], [], []
    common = set.intersection(*date_sets) if date_sets else set()
    common_dates = sorted(common)
    if len(common_dates) < 3:
        # Fall back to the union's densest subset: keep dates present in >=90%.
        counts: Counter = Counter()
        for m in per_spec_map.values():
            counts.update(m.keys())
        thresh = max(3, int(0.9 * len(per_spec_map)))
        common_dates = sorted(d for d, c in counts.items() if c >= thresh)
    spec_ids: List[str] = []
    matrix: List[List[float]] = []
    for sid, m in per_spec_map.items():
        if all(d in m for d in common_dates) and common_dates:
            spec_ids.append(sid)
            matrix.append([m[d] for d in common_dates])
    return spec_ids, common_dates, matrix


def effective_hypothesis_count(series_by_spec: Mapping[str, Mapping[str, Any]],
                               *, nominal: Optional[int] = None) -> Dict[str, Any]:
    """Estimate how many *economically independent* tests the family represents.

    Uses the participation ratio of the correlation-matrix eigenvalues,
    ``M_eff = (sum(lambda))^2 / sum(lambda^2)``. For a correlation matrix the
    trace equals M, so this reduces to ``M^2 / sum(lambda^2)`` and lies in
    [1, M]: M when the tests are uncorrelated, 1 when they are collinear.
    """
    spec_ids, common_dates, matrix = _aligned_return_matrix(series_by_spec)
    n_streams = len(spec_ids)
    result: Dict[str, Any] = {
        "nominal_family_size": int(nominal) if nominal is not None else n_streams,
        "n_streams_used": n_streams,
        "n_common_dates": len(common_dates),
        "method": "participation_ratio_of_correlation_eigenvalues",
        "effective_family_size": None,
        "mean_abs_offdiag_corr": None,
        "redundancy_ratio": None,
    }
    if n_streams < 2 or len(common_dates) < 3:
        result["effective_family_size"] = float(max(1, n_streams))
        return result
    try:
        import numpy as np
    except Exception:  # pragma: no cover - numpy is present in this env
        result["method"] = "unavailable_numpy_missing"
        return result
    x = np.asarray(matrix, dtype=float)  # shape (n_streams, n_dates)
    # Correlation across streams (rows). Guard zero-variance rows.
    stds = x.std(axis=1)
    keep = stds > 1e-12
    x = x[keep]
    kept_ids = [sid for sid, k in zip(spec_ids, keep.tolist()) if k]
    m = x.shape[0]
    if m < 2:
        result["effective_family_size"] = float(max(1, m))
        result["n_streams_used"] = m
        return result
    corr = np.corrcoef(x)
    corr = np.nan_to_num(corr, nan=0.0)
    # Mean absolute off-diagonal correlation.
    off = corr[~np.eye(m, dtype=bool)]
    mean_abs = float(np.abs(off).mean()) if off.size else 0.0
    eig = np.linalg.eigvalsh(corr)
    eig = np.clip(eig, 0.0, None)
    s1 = float(eig.sum())
    s2 = float((eig ** 2).sum())
    meff = (s1 * s1 / s2) if s2 > 0 else float(m)
    meff = max(1.0, min(float(m), meff))
    result.update({
        "n_streams_used": m,
        "effective_family_size": round(meff, 3),
        "mean_abs_offdiag_corr": round(mean_abs, 4),
        "redundancy_ratio": round(meff / m, 4) if m else None,
    })
    return result


# ---------------------------------------------------------------------------
# Benjamini-Hochberg minimum q-value at an arbitrary family size (diagnostic).
# ---------------------------------------------------------------------------
def bh_min_qvalue(pvalues: Iterable[float], family_size: int) -> Optional[float]:
    """Minimum BH q-value if the family had ``family_size`` tests.

    q_(k) = min over j>=k of ( p_(j) * family_size / j ). Returns the smallest
    q across ranks. Used purely to show how the multiple-testing penalty scales
    with the (nominal vs effective) family size.
    """
    ps = sorted(float(p) for p in pvalues if p is not None and 0.0 <= float(p) <= 1.0)
    n = len(ps)
    if n == 0 or family_size <= 0:
        return None
    q_min = math.inf
    running = math.inf
    for k in range(n, 0, -1):
        raw_q = ps[k - 1] * family_size / k
        running = min(running, raw_q)
        q_min = min(q_min, running)
    return min(1.0, q_min) if q_min != math.inf else None


# ---------------------------------------------------------------------------
# Per-candidate classification.
# ---------------------------------------------------------------------------
def classify_deep(deep_result: Mapping[str, Any]) -> str:
    """AUTHORITATIVE weakest-gate for a deep-evaluated candidate."""
    blocker = deep_result.get("gate_blocker")
    target = deep_result.get("gate_target_state")
    if blocker:
        return _BLOCKER_TO_REASON.get(str(blocker), OTHER_EXPLICIT_REASON)
    # No strength blocker -> it reached KEEP_FOR_RESEARCH; find the *next* gate.
    if target == "KEEP_FOR_RESEARCH" or blocker is None:
        if not deep_result.get("multiple_testing_survived", False):
            return MULTIPLE_TESTING_FAILURE
        if not deep_result.get("sign_stable", True):
            return PARAMETER_INSTABILITY
        if not deep_result.get("holdout_confirms", True):
            return HOLDOUT_FAILURE
    return OTHER_EXPLICIT_REASON


def classify_screen(record: Mapping[str, Any]) -> str:
    """Screen-metrics weakest-gate (authority='screen_metrics').

    Applies the real (unchanged) gate thresholds to the metrics the broad screen
    recorded, in the tournament's evaluation order, and labels the first failure.
    A candidate that clears every metric it has is attributed to the family-level
    multiple-testing wall (every tested Stage 11 candidate failed FDR).
    """
    periods = record.get("periods") or 0
    if periods == 0:
        return INSUFFICIENT_COVERAGE
    if periods < 12:
        return INSUFFICIENT_EFFECTIVE_SAMPLE
    ric = record.get("rank_ic_mean")
    rict = record.get("rank_ic_t")
    st = record.get("spread_t")
    net25 = record.get("net25")
    turnover = record.get("turnover")
    if ric is not None and abs(float(ric)) < GATE_MIN_RANK_IC:
        return WEAK_IC
    if rict is not None and abs(float(rict)) < GATE_MIN_RANK_IC_T:
        return WEAK_IC
    if st is not None and abs(float(st)) < GATE_MIN_SPREAD_T:
        return WEAK_IC
    if net25 is not None and float(net25) <= GATE_MIN_NET25:
        return COST_DESTROYED
    if turnover is not None and float(turnover) > GATE_MAX_TURNOVER:
        return EXCESSIVE_TURNOVER
    return MULTIPLE_TESTING_FAILURE


# ---------------------------------------------------------------------------
# Full autopsy.
# ---------------------------------------------------------------------------
def _num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def build_autopsy(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Assemble the Stage 11 failure autopsy from loaded artifacts.

    ``state`` keys (each optional; missing -> that view is empty):
        catalogue, screen, deep_eval, multiple_testing, command_center,
        inventory, shadow.
    """
    catalogue = state.get("catalogue") or {}
    screen = state.get("screen") or {}
    deep = state.get("deep_eval") or {}
    mt = state.get("multiple_testing") or {}
    cc = state.get("command_center") or {}
    inventory = state.get("inventory") or {}
    shadow = state.get("shadow") or {}

    screen_records: Dict[str, Any] = dict(screen.get("records") or {})
    deep_results: Dict[str, Any] = dict(deep.get("results") or {})
    series_by_spec: Dict[str, Any] = dict(screen.get("series") or {})
    catalogue_specs = catalogue.get("specs") or []
    spec_family = {s.get("spec_id"): s.get("family") for s in catalogue_specs}
    spec_name = {s.get("spec_id"): s.get("name") for s in catalogue_specs}

    # ---- candidate-level taxonomy -------------------------------------------
    candidate_taxonomy: List[Dict[str, Any]] = []
    screen_dist: Counter = Counter()
    deep_dist: Counter = Counter()
    family_dist: Dict[str, Counter] = defaultdict(Counter)

    for sid, rec in screen_records.items():
        fam = rec.get("family") or spec_family.get(sid)
        name = rec.get("name") or spec_name.get(sid)
        deep_rec = deep_results.get(sid)
        if deep_rec is not None:
            reason = classify_deep(deep_rec)
            authority = "full_gate_battery"
            deep_dist[reason] += 1
        else:
            reason = classify_screen(rec)
            authority = "screen_metrics"
        screen_dist[reason] += 1
        family_dist[fam][reason] += 1
        candidate_taxonomy.append({
            "spec_id": sid,
            "name": name,
            "family": fam,
            "authority": authority,
            "weakest_gate": reason,
            "rank_ic_t": _num(rec.get("rank_ic_t")),
            "spread_t": _num(rec.get("spread_t")),
            "net25": _num(rec.get("net25")),
            "turnover": _num(rec.get("turnover")),
            "periods": rec.get("periods"),
            "deep_evaluated": deep_rec is not None,
            "multiple_testing_survived": (deep_rec or {}).get("multiple_testing_survived"),
            "holdout_confirms": (deep_rec or {}).get("holdout_confirms"),
            "sign_stable": (deep_rec or {}).get("sign_stable"),
            "gate_target_state": (deep_rec or {}).get("gate_target_state"),
        })

    # ---- family-level rollup -------------------------------------------------
    family_autopsy: List[Dict[str, Any]] = []
    for fam, counts in sorted(family_dist.items(), key=lambda kv: -sum(kv[1].values())):
        fam_rows = [c for c in candidate_taxonomy if c["family"] == fam]
        best = max(fam_rows, key=lambda c: (c["rank_ic_t"] or -9), default=None)
        family_autopsy.append({
            "family": fam,
            "n_candidates": len(fam_rows),
            "weakest_gate_counts": dict(counts),
            "dominant_weakest_gate": counts.most_common(1)[0][0] if counts else None,
            "best_rank_ic_t": best["rank_ic_t"] if best else None,
            "best_name": best["name"] if best else None,
        })

    # ---- effective hypothesis count -----------------------------------------
    nominal_family = mt.get("family_size") or screen.get("family_size") or len(screen_records)
    eff = effective_hypothesis_count(series_by_spec, nominal=nominal_family)

    # ---- multiple-testing burden diagnostic ---------------------------------
    mt_rows = mt.get("rows") or []
    raw_pvals = [r.get("raw_pvalue") for r in mt_rows if r.get("raw_pvalue") is not None]
    tested = mt.get("tested") or len(raw_pvals)
    eff_m = eff.get("effective_family_size")
    diag = {
        "tested": tested,
        "nominal_family_size": nominal_family,
        "effective_family_size": eff_m,
        "min_bh_qvalue_at_tested": bh_min_qvalue(raw_pvals, tested) if tested else None,
        "min_bh_qvalue_at_effective": (
            bh_min_qvalue(raw_pvals, int(round(eff_m))) if eff_m else None),
        "fdr_alpha": mt.get("alpha", 0.05),
        "n_raw_p_below_0p05": sum(1 for p in raw_pvals if p < 0.05),
        "n_raw_p_below_0p01": sum(1 for p in raw_pvals if p < 0.01),
        "note": (
            "DIAGNOSTIC ONLY. The min BH q-value scales with the family size. "
            "The gap between the nominal-size and effective-size q-values "
            "quantifies how much of the Stage 11 negative was multiple-testing "
            "burden inflated by hypothesis redundancy -- NOT evidence of alpha, "
            "and never a basis to relax the gate."
        ),
    }

    # ---- graveyard (permanent do-not-retry on owned data) -------------------
    graveyard: List[Dict[str, Any]] = []
    for s in catalogue.get("data_hold_specs") or []:
        reason = s.get("reason", "")
        if "NO_MARKET_CAP_NO_SHARES_OUTSTANDING" in reason:
            graveyard.append({
                "spec_id": s.get("spec_id"), "name": s.get("name"),
                "family": s.get("family"), "reason": reason,
                "class": "PERMANENTLY_UNSUPPORTED_OWNED_DATA",
            })
    for c in candidate_taxonomy:
        if c["deep_evaluated"] and c["weakest_gate"] in (DRAWDOWN,):
            graveyard.append({
                "spec_id": c["spec_id"], "name": c["name"], "family": c["family"],
                "reason": "REJECT_SEVERE_DRAWDOWN (full gate battery)",
                "class": "ECONOMICALLY_REJECTED_ROBUST",
            })

    # ---- underpowered-but-economically-interesting shortlist ----------------
    shortlist: List[Dict[str, Any]] = []
    for c in candidate_taxonomy:
        strong_deep = (c["deep_evaluated"]
                       and c["gate_target_state"] == "KEEP_FOR_RESEARCH"
                       and c["weakest_gate"] == MULTIPLE_TESTING_FAILURE)
        strong_screen = (not c["deep_evaluated"]
                         and (c["rank_ic_t"] or 0) >= GATE_MIN_RANK_IC_T
                         and (c["spread_t"] or 0) >= GATE_MIN_SPREAD_T
                         and (c["net25"] or -1) > GATE_MIN_NET25)
        if strong_deep or strong_screen:
            shortlist.append({
                "spec_id": c["spec_id"], "name": c["name"], "family": c["family"],
                "rank_ic_t": c["rank_ic_t"], "spread_t": c["spread_t"],
                "net25": c["net25"], "turnover": c["turnover"],
                "authority": c["authority"],
                "reached_keep_for_research": c["gate_target_state"] == "KEEP_FOR_RESEARCH",
            })
    shortlist.sort(key=lambda r: -(r["rank_ic_t"] or 0))

    # ---- evidence-based Stage 12 design recommendation ----------------------
    momentum_reversal = sum(1 for s in catalogue_specs
                            if s.get("family") in ("price_momentum", "reversal_path"))
    total_specs = len(catalogue_specs)
    recommendation = {
        "verdict": _verdict(diag, shortlist),
        "family_redundancy": {
            "nominal_specs": total_specs,
            "momentum_reversal_specs": momentum_reversal,
            "momentum_reversal_share": (round(momentum_reversal / total_specs, 3)
                                        if total_specs else None),
            "effective_family_size": eff_m,
            "redundancy_ratio": eff.get("redundancy_ratio"),
        },
        "directives": [
            "Pre-register a SMALL set of economically-distinct hypotheses (>=5 "
            "genuinely different economic families); do NOT regenerate hundreds "
            "of near-duplicate momentum/reversal variations -- that inflates the "
            "multiple-testing family and erodes power.",
            "Prioritise event-driven information arrival (PIT fundamental-change "
            "events dated by SEC filing) and market/industry-residual price "
            "reaction -- signals orthogonal to the exhausted price factors.",
            "Increase statistical power via broader survivorship-safe breadth and "
            "non-overlapping / event-timed observations; the Stage 11 panel used "
            "only 40 quarterly rebalances over a ~535-name survivor-biased subset.",
            "Preserve every unchanged gate: strength, cost, turnover, drawdown, "
            "holdout, family-aware multiple-testing, deflated statistics. A "
            "smaller honest family is the remedy, never a relaxed threshold.",
        ],
    }

    return {
        "stage": "12",
        "workstream": "A_stage11_failure_autopsy",
        "read_only": True,
        "source_status": cc.get("status"),
        "shadow_status": shadow.get("status") or (cc.get("shadow") or {}).get("status"),
        "candidate_funnel": cc.get("candidate_funnel"),
        "best_current_evidence": cc.get("best_current_evidence"),
        "n_candidates_classified": len(candidate_taxonomy),
        "weakest_gate_distribution_all": dict(screen_dist.most_common()),
        "weakest_gate_distribution_deep_authoritative": dict(deep_dist.most_common()),
        "family_autopsy": family_autopsy,
        "effective_hypothesis_count": eff,
        "multiple_testing_burden_diagnostic": diag,
        "graveyard": graveyard,
        "graveyard_count": len(graveyard),
        "underpowered_shortlist": shortlist,
        "underpowered_shortlist_count": len(shortlist),
        "design_recommendation": recommendation,
        "candidate_taxonomy": candidate_taxonomy,
    }


def _verdict(diag: Mapping[str, Any], shortlist: Sequence[Any]) -> str:
    """One-line classification of the Stage 11 negative."""
    q_tested = diag.get("min_bh_qvalue_at_tested")
    q_eff = diag.get("min_bh_qvalue_at_effective")
    alpha = diag.get("fdr_alpha", 0.05)
    has_strong = bool(shortlist)
    if has_strong and q_tested is not None and q_eff is not None:
        if q_tested > alpha >= q_eff:
            return ("MULTIPLE_TESTING_LIMITED_NOT_CLEAN_NULL: the strongest "
                    "candidates carry real raw signal and clear the strength, "
                    "sign and holdout gates; they fail only the family-size "
                    "multiple-testing correction, which is inflated by "
                    "hypothesis redundancy.")
    if has_strong:
        return ("MULTIPLE_TESTING_LIMITED: strong individual candidates exist "
                "but none survive the family-aware correction.")
    return "GENUINE_NULL_ON_OWNED_DATA: no candidate carries defensible signal."


# ---------------------------------------------------------------------------
# Thin loaders (I/O boundary; the analysis above stays pure).
# ---------------------------------------------------------------------------
_STATE_FILES = {
    "catalogue": "catalogue.json",
    "screen": "screen.json",
    "deep_eval": "deep_eval.json",
    "multiple_testing": "multiple_testing.json",
    "command_center": "command_center.json",
    "inventory": "inventory.json",
}


def load_stage11_state(state_dir: str | Path,
                       shadow_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load the Stage 11 artifact dicts from a state directory (read-only)."""
    state_dir = Path(state_dir)
    out: Dict[str, Any] = {}
    for key, fname in _STATE_FILES.items():
        p = state_dir / fname
        if p.exists():
            try:
                out[key] = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                out[key] = {}
    if shadow_path is None:
        shadow_path = state_dir.parent / "shadow" / "shadow_state.json"
    shadow_path = Path(shadow_path)
    if shadow_path.exists():
        try:
            out["shadow"] = json.loads(shadow_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            out["shadow"] = {}
    return out


def run_autopsy(state_dir: str | Path,
                shadow_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load Stage 11 artifacts and build the autopsy (read-only end to end)."""
    return build_autopsy(load_stage11_state(state_dir, shadow_path))
