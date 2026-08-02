"""
alpha_agent.stage9_4 - Stage 9.4 evaluation ADAPTERS (reusable capabilities).

This module is a library of pure, deterministic evaluation adapters that the
EXISTING AlphaAgent invokes from its canonical collect loop (the tournament tick
inside ``run_alpha_agent.py --mode collect``). It is NOT a parallel research
orchestrator and must never be driven manually as the acceptance proof.

It composes the Stage 9.4 reusable capabilities - the pre-registered price-factor
hypothesis catalogue (``price_factor_catalogue``), the deterministic feature
calculators (``price_factors``), orthogonality (``orthogonality``), regime
conditioning (``regimes``) and selection-bias controls (``selection_controls``) -
into the two things the agent's tick needs:

  * ``enrich_price_factor_row`` - attach orthogonality / regime / walk-forward /
    bootstrap / selection-bias / deflated-Sharpe evidence (and the contract keys
    the scorer reads) to a Stage 5 result row BEFORE the canonical tournament
    classifies it. The evidence rides ALONGSIDE ``classify_evidence`` and never
    lowers a gate.
  * ``build_decision_record`` - the Track H lifecycle evidence artifact.

Plus read-only reporting helpers (Track J). Pure stdlib; no state writes here -
the canonical tournament registry persists everything.
"""
from __future__ import annotations

import math
from typing import Optional

from . import experiment_contracts as ec
from . import orthogonality as orth
from . import regimes as rg
from . import selection_controls as sctl

STAGE9_4_VERSION = "stage9_4-eval-adapters-1.0.0"


def _two_sided_p_from_t(t: Optional[float]) -> Optional[float]:
    """Two-sided normal-approx p-value from a t-like statistic."""
    if t is None:
        return None
    return 2.0 * (1.0 - sctl._norm_cdf(abs(float(t))))


def enrich_price_factor_row(row: dict, *, family_size: int = 1,
                            champion_returns: Optional[list] = None,
                            cfg: Optional[dict] = None) -> dict:
    """Attach Stage 9.4 evidence to one Stage 5 price-factor result row using its
    per-period series. Additive: writes NEW keys only (the contract keys
    ``partial_rank_ic`` / ``incremental_net_return`` the scorer reads, plus nested
    ``stage9_4_*`` evidence dicts). Never mutates the gate inputs. Deterministic.
    """
    cfg = cfg or {}
    series = row.get("_periods_series") or {}
    ic_series = [v for v in series.get("rank_ic", [])]
    spread_series = series.get("spread", [])
    port = series.get("portfolio_returns", [])
    turns = series.get("turnovers", [])
    mfeat = series.get("market_features", [])
    periods = int(row.get("periods") or len([v for v in ic_series if v is not None]))

    # ---- Track C: regime-conditioned evaluation (fixed, PIT-safe) -----------
    regime_cfg = cfg.get("regimes", {})
    labels = rg.classify_regimes(mfeat, cfg=regime_cfg) if mfeat else []
    conditioned = (rg.regime_conditioned_metrics(
        labels, ic_series=ic_series, spread_series=spread_series,
        turnover_series=turns, portfolio_returns=port, cfg=regime_cfg)
        if labels else {})
    stability = (rg.classify_alpha_stability(conditioned, cfg=regime_cfg)
                 if conditioned else {"overall": rg.INSUFFICIENT_EVIDENCE})

    # ---- Track D: block bootstrap CI of the per-period IC -------------------
    boot = sctl.block_bootstrap_ci(
        [v for v in ic_series if v is not None],
        block_size=int(cfg.get("bootstrap_block_size", 4)),
        n_resamples=int(cfg.get("bootstrap_resamples", 500)),
        seed=int(cfg.get("bootstrap_seed", 12345)))

    # ---- Track D: genuine forward-only, purged+embargoed walk-forward OOS IC -
    embargo = int(cfg.get("embargo_periods", 1))
    purge = int(cfg.get("purge_periods", 1))
    label_h = int(cfg.get("label_horizon", 1))
    folds = sctl.purged_walk_forward_folds(
        periods, n_folds=int(cfg.get("walk_forward_folds", 4)),
        embargo=embargo, purge=purge, label_horizon=label_h)
    # Candidate evidence is drawn ONLY from legitimate forward folds (train
    # strictly before test); the earliest periods that have no eligible prior
    # history contribute no fold and therefore no OOS evidence.
    fold_test_ic = []
    for f in folds:
        vals = [ic_series[i] for i in f["test"]
                if i < len(ic_series) and ic_series[i] is not None]
        fold_test_ic.append(ec.mean(vals))
    wf = {"folds": len(folds), "embargo": embargo, "purge": purge,
          "label_horizon": label_h, "forward_only": True,
          "no_leakage": sctl.verify_no_leakage(folds, embargo=embargo,
                                               purge=purge,
                                               label_horizon=label_h),
          "oos_ic_by_fold": fold_test_ic,
          "oos_ic_mean": ec.mean([v for v in fold_test_ic if v is not None])}

    # ---- Track D: deflated Sharpe (discount for the search family size) -----
    sharpe = row.get("sharpe")
    dsr = (sctl.deflated_sharpe_ratio(sharpe, n_obs=periods,
                                      n_trials=max(1, int(family_size)))
           if sharpe is not None and periods >= 2 else None)

    # ---- Track D: selection-adjusted significance ---------------------------
    raw_t = row.get("rank_ic_t")
    raw_p = _two_sided_p_from_t(raw_t)
    adj_p = (sctl.selection_adjusted_pvalue(raw_p, family_size=max(1, int(family_size)))
             if raw_p is not None else None)
    selection = {"search_family_size": max(1, int(family_size)),
                 "raw_rank_ic_t": raw_t, "raw_pvalue": raw_p,
                 "selection_adjusted_pvalue": adj_p,
                 "search_size_penalty": sctl.selection_penalty(
                     max(1, int(family_size)))}

    # ---- Track B: orthogonality / independent information -------------------
    champ_comp = row.get("champion_complementarity")
    corr_champ = None if champ_comp is None else round(1.0 - float(champ_comp), 6)
    incr_net = row.get("benchmark_excess_annualized")
    indep = orth.independent_information_score(
        corr_champion=corr_champ, corr_retained=None, partial_rank_ic=None,
        incremental_net_return=incr_net,
        cfg=cfg.get("independent_information", {}))

    # Contract-visible keys the scorer reads (additive; None-safe).
    row["partial_rank_ic"] = None  # panel-level partial IC needs a control factor
    row["incremental_net_return"] = incr_net
    row["corr_vs_champion"] = corr_champ
    row["independent_information_score"] = indep["independent_information_score"]
    # Nested Stage 9.4 evidence (surfaced read-only; not gate inputs).
    row["stage9_4_regime"] = {"stability": stability, "by_axis": conditioned}
    row["stage9_4_bootstrap"] = boot
    row["stage9_4_walk_forward"] = wf
    row["stage9_4_deflated_sharpe"] = dsr
    row["stage9_4_selection"] = selection
    row["stage9_4_independent_information"] = indep
    return row


def build_decision_record(candidate: dict, metrics: dict, scores: dict,
                          gate: dict, *, hypothesis: str = "", family: str = "",
                          search_family_size: int = 1) -> dict:
    """Track H lifecycle evidence artifact - the full auditable record attached
    to every candidate decision: hypothesis, family, search-family size, raw and
    selection-adjusted statistics, strongest/weakest evidence, orthogonality
    evidence, regime evidence, failed gates and the next required evidence."""
    subs = {k: scores.get(k) for k in (
        "historical_evidence_score", "robustness_score", "cost_score",
        "stability_score", "diversification_score", "forward_evidence_score",
        "independent_information_score")}
    present = {k: v for k, v in subs.items() if v is not None}
    strongest = (max(present, key=present.get) if present else None)
    weakest = (min(present, key=present.get) if present else None)
    return {
        "stage9_4_version": STAGE9_4_VERSION,
        "candidate_id": candidate.get("candidate_id"),
        "name": candidate.get("name"),
        "hypothesis": hypothesis or (candidate.get("spec") or {}).get("formula"),
        "family": family or (candidate.get("spec") or {}).get("factor_family"),
        "search_family_size": int(search_family_size),
        "raw_statistics": {
            "rank_ic": metrics.get("rank_ic"), "rank_ic_t": metrics.get("rank_ic_t"),
            "spread_t": metrics.get("spread_t"),
            "net25_spread": metrics.get("net25_spread")},
        "selection_adjusted_statistics": metrics.get("stage9_4_selection"),
        "orthogonality_evidence": metrics.get("stage9_4_independent_information"),
        "regime_evidence": metrics.get("stage9_4_regime"),
        "bootstrap_evidence": metrics.get("stage9_4_bootstrap"),
        "walk_forward_evidence": metrics.get("stage9_4_walk_forward"),
        "deflated_sharpe_evidence": metrics.get("stage9_4_deflated_sharpe"),
        "target_state": gate.get("target_state"),
        "evidence_status": gate.get("evidence_status"),
        "failed_gates": gate.get("failed_gates", []),
        "strongest_evidence": strongest, "weakest_evidence": weakest,
        "blocker": gate.get("blocker"),
        "next_required_evidence": _next_required(gate),
    }


def _next_required(gate: dict) -> str:
    ts = gate.get("target_state")
    if ts == "DATA_HOLD":
        return "acquire/materialize: %s" % (gate.get("blocker") or "data")
    if ts == "REJECTED":
        return "rejected on complete evidence: %s" % (
            ",".join(gate.get("failed_gates", []) or []) or "weak")
    if ts == "KEEP_FOR_RESEARCH":
        return "activate research shadow book; accrue true-forward observations"
    return "run canonical evidence contract"


# --------------------------------------------------------------------------- #
# Read-only reporting (Track J).
# --------------------------------------------------------------------------- #
def catalogue_report() -> dict:
    """Read-only Stage 9.4 catalogue + combination + regime summary (no state)."""
    from . import price_factor_catalogue as cat
    from . import factor_combinations as fc
    return {
        "stage9_4_version": STAGE9_4_VERSION,
        "price_factor_catalogue": cat.catalogue_summary(),
        "combinations": fc.combinations_summary(),
        "regime_axes": list(rg.REGIME_AXIS_KEYS),
        "regime_version_hash": rg.regime_version_hash(),
    }


__all__ = ["STAGE9_4_VERSION", "enrich_price_factor_row", "build_decision_record",
           "catalogue_report"]
