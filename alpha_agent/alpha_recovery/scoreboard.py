"""alpha_agent.alpha_recovery.scoreboard - the permanent project scoreboard.

Contract section 4: ``alpha_recovery_scoreboard.json`` is THE primary progress
measure of the project. It is built from the campaign artifacts (never typed
in), deterministic (the artifact hash excludes ``generated_at``), and rendered
to a concise human page. It shows the incumbent and the best challenger on
the same fields, the advantage, exactly one STATUS, the session clock against
the frozen checkpoint, and the campaign counters the contract demands.

Historical OOS and TRUE_FORWARD are separate columns; they are never pooled.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from . import (EQ_TOP_N_OPERATIONAL, INCUMBENT_MODEL_ID, SCOREBOARD_JSON_NAME, SCOREBOARD_MD_NAME,
               ST_COMPETING, ST_EXHAUSTED, ST_HISTORICAL_SURVIVOR, ST_READY, ST_REJECTED, ST_RESEARCHING,
               STATUSES, read_artifact, read_json, repo_dir, write_repo_artifact)
from . import checkpoint as CK

CALCULATION_OWNER = "alpha_agent.alpha_recovery.scoreboard"
SCHEMA = "alpha_recovery_scoreboard/1"

REGISTRY_DIR_ENV = "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR"
DEFAULT_REGISTRY_DIR = Path(r"D:\Stock_Prediction_app_data\forward_challenger_registry")


def registry_dir() -> Path:
    return Path(os.environ.get(REGISTRY_DIR_ENV) or DEFAULT_REGISTRY_DIR)


def forward_registrations() -> list:
    """Registrations in the canonical registry, READ by path (the owner's own
    env var and layout); never written here."""
    d = registry_dir() / "registrations"
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        j = read_json(p)
        if isinstance(j, dict):
            out.append({"challenger_id": j.get("challenger_id") or (j.get("identity") or {}).get("challenger_id"),
                        "asset_class": j.get("asset_class") or (j.get("identity") or {}).get("asset_class"),
                        "registered_at": j.get("registered_at") or j.get("evaluated_at"),
                        "file": p.name})
    return out


# --------------------------------------------------------------------------- #
# Blocks
# --------------------------------------------------------------------------- #
def _incumbent_block(inc: dict | None) -> dict:
    if not inc:
        return {"state": "NOT_MEASURED"}
    h = ((inc.get("historical_oos") or {}).get("horizons") or {}).get("21") or {}
    op = h.get("top%d" % EQ_TOP_N_OPERATIONAL) or {}
    a, lock = op.get("all") or {}, op.get("lockbox") or {}
    cal = ((inc.get("historical_oos") or {}).get("calibration") or {}).get("21") or {}
    fwd = inc.get("true_forward") or {}
    rb = fwd.get("realised_book") or {}
    o1 = (fwd.get("outcomes_by_model_horizon") or {}).get("%s_h1" % INCUMBENT_MODEL_ID) or {}
    o20 = (fwd.get("outcomes_by_model_horizon") or {}).get("%s_h20" % INCUMBENT_MODEL_ID) or {}
    return {
        "model_id": INCUMBENT_MODEL_ID, "role": "BENCHMARK",
        "verdict": (inc.get("verdict") or {}).get("verdict"),
        "historical_oos": {
            "horizon_sessions": 21, "book": "top-25 equal-weight vs EW scored universe, 12.5bp/side",
            "ann_net_excess": a.get("ann_net_excess"), "t_net_excess": a.get("t_net_excess"),
            "ann_strat_net": a.get("ann_strat_net"), "sharpe_excess": a.get("sharpe_excess"),
            "max_drawdown": a.get("strat_max_dd"), "benchmark_max_drawdown": a.get("bench_max_dd"),
            "mean_oneway_turnover_per_period": a.get("mean_oneway_turnover_per_period"),
            "mean_rank_ic": a.get("mean_rank_ic"), "t_rank_ic": a.get("t_rank_ic"),
            "hit_rate": a.get("hit_rate"), "periods": a.get("periods"),
            "effective_periods": a.get("effective_periods"),
            "lockbox_ann_net_excess": lock.get("ann_net_excess"), "lockbox_t": lock.get("t_net_excess"),
            "evidence_maturity": "HISTORICAL_OOS %s..%s" % (a.get("first"), a.get("last"))},
        "true_forward": {
            "book_id": rb.get("book_id"), "sessions": rb.get("sessions"),
            "cumulative_return": rb.get("cumulative_return"),
            "benchmark_cumulative_return": rb.get("benchmark_cumulative_return"),
            "excess_cumulative": rb.get("excess_cumulative"), "sharpe_annualised": rb.get("sharpe_annualised"),
            "max_drawdown": rb.get("max_drawdown"), "total_turnover_oneway": rb.get("total_turnover_oneway"),
            "h1_mean_rank_ic": o1.get("mean_rank_ic"), "h1_t_rank_ic": o1.get("t_rank_ic"),
            "h1_matured_sessions": o1.get("n_matured_sessions"),
            "h20_mean_rank_ic": o20.get("mean_rank_ic"), "h20_matured_sessions": o20.get("n_matured_sessions"),
            "evidence_maturity": rb.get("evidence_maturity")},
        "expected_return_calibration": {"state": cal.get("state"), "calibrated": cal.get("calibrated"),
                                        "reason": cal.get("reason")},
        "directional_calibration": "NOT_APPLICABLE (a cross-sectional rank model emits no direction)",
    }


def _challenger_candidates(tour: dict | None, cad: dict | None, direction: dict | None,
                           eqch: dict | None = None, residual: dict | None = None) -> list:
    out = []
    for b in (eqch or {}).get("brief") or []:
        if b.get("ann_advantage") is None:
            continue
        out.append({"kind": "SAME_DOMAIN_CONSTRUCTION", "cell_id": b["cell_id"], "family": b.get("family"),
                    "horizon": 21, "information": "the incumbent's own (%s, rebalanced every %s sessions)"
                                                  % (b.get("leg"), b.get("trade_every")),
                    "verdict": b.get("verdict"),
                    "historical_oos_net_advantage": b.get("ann_advantage"),
                    "t_advantage": b.get("t_advantage"), "sharpe_delta": b.get("sharpe_advantage"),
                    "drawdown_delta": ((b.get("max_dd") or 0) - (b.get("reference_max_dd") or 0)
                                       if b.get("max_dd") is not None and b.get("reference_max_dd") is not None
                                       else None),
                    "turnover_delta": ((b.get("mean_oneway_turnover_per_21s") or 0)
                                       - (b.get("reference_oneway_turnover_per_21s") or 0)
                                       if b.get("mean_oneway_turnover_per_21s") is not None
                                       and b.get("reference_oneway_turnover_per_21s") is not None else None),
                    "challenger_turnover": b.get("mean_oneway_turnover_per_21s"),
                    "lockbox_net_advantage": b.get("lockbox_ann_advantage"),
                    "challenger_ann_net_excess": b.get("ann_net_excess"),
                    "challenger_sharpe_excess": b.get("sharpe_excess"),
                    "fdr_pass": b.get("fdr_pass_paired"), "holm_pass": b.get("holm_pass_family"),
                    "failed_gates": b.get("failed_gates"), "effective_periods": b.get("periods"),
                    "evidence_label": b.get("evidence_label"),
                    "capital_applicability": "US_EQUITY long-only top-25 book: the SAME universe, score and "
                                             "cost as the incumbent, rebalanced every %s sessions"
                                             % b.get("trade_every")})
    for b in (residual or {}).get("brief") or []:
        if b.get("ann_net_increment") is None:
            continue
        out.append({"kind": "FRONTIER_NEED", "cell_id": b["cell_id"], "family": (residual or {}).get("family"),
                    "horizon": None, "information": b.get("cell_key"), "verdict": b.get("r64_verdict"),
                    "historical_oos_net_advantage": b.get("ann_net_increment"),
                    "t_advantage": b.get("t_increment"), "sharpe_delta": b.get("sharpe_increment"),
                    "drawdown_delta": None, "turnover_delta": None,
                    "augmented_ann_net": b.get("augmented_ann_net"),
                    "augmented_max_dd": b.get("augmented_max_dd"),
                    "conditional_t": b.get("conditional_t"), "residual_share": b.get("residual_share"),
                    "fdr_pass": b.get("fdr_pass_economic"), "holm_pass": b.get("holm_pass_family"),
                    "frontier_rank": b.get("frontier_rank"),
                    "capital_applicability": "the frontier's own top-ranked OWNED need, priced under the "
                                             "R64 risk-controlled book"})
    for b in (tour or {}).get("brief") or []:
        if b.get("ann_advantage_top25") is None:
            continue
        out.append({"kind": "SAME_DOMAIN", "cell_id": b["cell_id"], "family": b.get("family"),
                    "horizon": b.get("horizon"), "information": b.get("dimension"),
                    "verdict": b.get("tournament_verdict"),
                    "historical_oos_net_advantage": b.get("ann_advantage_top25"),
                    "t_advantage": b.get("t_advantage_top25"), "sharpe_delta": b.get("sharpe_advantage_top25"),
                    "drawdown_delta": ((b.get("challenger_max_dd") or 0) - (b.get("incumbent_max_dd") or 0)
                                       if b.get("challenger_max_dd") is not None and b.get("incumbent_max_dd") is not None else None),
                    "turnover_delta": ((b.get("challenger_turnover") or 0) - (b.get("incumbent_turnover") or 0)
                                       if b.get("challenger_turnover") is not None and b.get("incumbent_turnover") is not None else None),
                    "challenger_turnover": b.get("challenger_turnover"),
                    "lockbox_net_advantage": b.get("lockbox_ann_advantage_top25"),
                    "conditional_t": b.get("conditional_t"), "residual_share": b.get("residual_share"),
                    "fdr_pass": b.get("fdr_pass_paired"), "holm_pass": b.get("holm_pass_family"),
                    "failed_gates": b.get("failed_gates"), "effective_periods": b.get("effective_periods"),
                    "capital_applicability": "US_EQUITY long-only top-25 book (same domain)"})
    for b in (cad or {}).get("brief") or []:
        if b.get("ann_net_increment") is None:
            continue
        out.append({"kind": "CROSS_DOMAIN_SLEEVE", "cell_id": b["cell_id"], "family": b.get("family"),
                    "horizon": 1, "information": b["cell_id"].split("|")[3] if "|" in b["cell_id"] else None,
                    "verdict": b.get("cadence_verdict"),
                    "historical_oos_net_advantage": b.get("ann_net_increment"),
                    "t_advantage": b.get("t_increment"), "sharpe_delta": b.get("sharpe_increment"),
                    "drawdown_delta": None, "turnover_delta": None, "challenger_turnover": b.get("augmented_turnover"),
                    "augmented_ann_net": b.get("augmented_ann_net"), "augmented_sharpe": b.get("augmented_sharpe"),
                    "augmented_max_dd": b.get("augmented_max_dd"),
                    "lockbox_net_advantage": b.get("lockbox_increment_ann"),
                    "conditional_t": b.get("conditional_t"), "fdr_pass": b.get("fdr_pass_economic"),
                    "holm_pass": b.get("holm_pass_family"), "evidence_label": b.get("evidence_label"),
                    "effective_periods": b.get("effective_periods"),
                    "capital_applicability": "futures sleeve; incremental to the incumbent under equal risk"})
    for h, r in ((direction or {}).get("horizons") or {}).items():
        if not isinstance(r, dict) or r.get("state") != "OK":
            continue
        eb = (r.get("economics") or {}).get("overlay_B") or {}
        out.append({"kind": "MARKET_DIRECTION", "cell_id": "SPY|%s" % h, "family": "MARKET_DIRECTION_SPY",
                    "horizon": int(h), "information": "conditioners + price state",
                    "verdict": (r.get("verdicts") or {}).get("verdict"),
                    "historical_oos_net_advantage": eb.get("ann_increment"), "t_advantage": eb.get("t_increment"),
                    "sharpe_delta": ((eb.get("overlay_sharpe") or 0) - (eb.get("buy_hold_sharpe") or 0)
                                     if eb.get("overlay_sharpe") is not None and eb.get("buy_hold_sharpe") is not None else None),
                    "drawdown_delta": ((eb.get("overlay_max_dd") or 0) - (eb.get("buy_hold_max_dd") or 0)
                                       if eb.get("overlay_max_dd") is not None and eb.get("buy_hold_max_dd") is not None else None),
                    "brier_skill_score": r.get("brier_skill_score"),
                    "expected_calibration_error": (r.get("calibration") or {}).get("expected_calibration_error"),
                    "hit_rate": r.get("hit_rate_model"), "hit_rate_always_up": r.get("hit_rate_always_up"),
                    "capital_applicability": "SPY exposure overlay (timing), not a stock ranking"})
    return out


def _best(cands: list) -> dict | None:
    rank = {"MATERIALLY_BEATS_INCUMBENT": 5, "ECONOMIC_UNDER_CONTROLS": 5, "CALIBRATED_DIRECTIONAL_SKILL": 5,
            "BEATS_INCUMBENT_NOT_QUALIFIED": 4, "ECONOMIC_UNDER_CONTROLS_NOT_FDR": 4,
            "ECONOMIC_UNDER_CONTROLS_UNSTABLE": 3, "PROFITABLE_NOT_CALIBRATED": 3,
            "CALIBRATED_NOT_PROFITABLE": 2, "NO_ADVANTAGE": 1, "NOT_ECONOMIC_UNDER_CONTROLS": 1,
            "NO_CONDITIONAL_VALUE": 0, "NO_DIRECTIONAL_SKILL": 0, "WORSE_THAN_INCUMBENT": -1}
    scored = [c for c in cands if c.get("historical_oos_net_advantage") is not None]
    if not scored:
        return None
    return max(scored, key=lambda c: (rank.get(c.get("verdict"), 0), c.get("historical_oos_net_advantage") or -9))


def status_from(best: dict | None, *, registrations: list, purchase: dict | None,
                any_measured: bool, cross_domain_positive: bool | None = None) -> str:
    """Exactly one status. A cross-domain sleeve counts as READY or as a
    HISTORICAL_SURVIVOR only when it ADDS utility to the incumbent-only book
    at equal risk (the contract's cross-domain rule); a same-domain candidate
    needs the head-to-head verdict."""
    if best is not None and registrations and any(
            str(r.get("challenger_id") or "").startswith("ALPHA_RECOVERY") for r in registrations):
        return ST_COMPETING
    v = (best or {}).get("verdict")
    sleeve = (best or {}).get("kind") == "CROSS_DOMAIN_SLEEVE"
    utility_ok = (cross_domain_positive is True) if sleeve else True
    if v in ("MATERIALLY_BEATS_INCUMBENT", "ECONOMIC_UNDER_CONTROLS", "CALIBRATED_DIRECTIONAL_SKILL") and utility_ok:
        return ST_READY
    if v in ("BEATS_INCUMBENT_NOT_QUALIFIED", "ECONOMIC_UNDER_CONTROLS_NOT_FDR") and utility_ok:
        return ST_HISTORICAL_SURVIVOR
    if purchase and purchase.get("owned_free_information_exhausted"):
        return ST_EXHAUSTED
    if any_measured and best is not None and (
            v in ("NO_ADVANTAGE", "WORSE_THAN_INCUMBENT", "NOT_ECONOMIC_UNDER_CONTROLS",
                  "NO_CONDITIONAL_VALUE", "NO_DIRECTIONAL_SKILL") or (sleeve and not utility_ok)):
        return ST_REJECTED
    return ST_RESEARCHING


def build(*, as_of=None, write: bool = True) -> dict:
    cp = CK.load()
    inc = read_artifact("incumbent_baseline.json")
    tour = read_artifact("tournament.json")
    cad = read_artifact("cadence_grid.json")
    direction = read_artifact("market_direction.json")
    program = read_artifact("research_program.json")
    purchase = read_artifact("purchase_case.json")
    cross = read_artifact("cross_domain.json")
    mandates = read_artifact("frontier_mandates.json")
    eqch = read_artifact("equity_challengers.json")
    residual = read_artifact("frontier_residual.json")
    products = read_artifact("forecast_products.json")
    cands = _challenger_candidates(tour, cad, direction, eqch=eqch, residual=residual)
    for b in (mandates or {}).get("brief") or []:
        if b.get("ann_net_increment") is None:
            continue
        cands.append({"kind": "CROSS_DOMAIN_SLEEVE", "cell_id": b["cell_id"], "family": (mandates or {}).get("family"),
                      "horizon": 21, "information": b.get("cell_key"), "verdict": b.get("r64_verdict"),
                      "historical_oos_net_advantage": b.get("ann_net_increment"), "t_advantage": b.get("t_increment"),
                      "sharpe_delta": b.get("sharpe_increment"), "drawdown_delta": None, "turnover_delta": None,
                      "conditional_t": b.get("conditional_t"), "fdr_pass": b.get("fdr_pass_economic"),
                      "holm_pass": b.get("holm_pass_family"), "augmented_max_dd": b.get("augmented_max_dd"),
                      "capital_applicability": "futures sleeve; governor-mandated need priced under the R64 book"})
    best = _best(cands)
    cr = (cross or {}).get("result") or {}
    chosen = ((cross or {}).get("chosen_sleeve") or {}).get("cell_id")
    cross_positive = cr.get("positive_incremental_utility_after_costs") if cr.get("state") == "OK" else None
    for c in cands:
        if c.get("kind") == "CROSS_DOMAIN_SLEEVE" and c.get("cell_id") == chosen and cr.get("state") == "OK":
            c["drawdown_delta"] = cr.get("drawdown_delta")
            c["equal_risk_incremental_ann_net"] = cr.get("incremental_ann_net_return")
            c["equal_risk_sharpe_delta"] = cr.get("sharpe_delta")
            c["equal_risk_t"] = cr.get("t_incremental")
            c["positive_incremental_utility_after_costs"] = cross_positive
    regs = forward_registrations()
    any_measured = bool(cands)
    best_is_chosen_sleeve = bool(best and best.get("kind") == "CROSS_DOMAIN_SLEEVE")
    status = status_from(best, registrations=regs, purchase=purchase, any_measured=any_measured,
                         cross_domain_positive=(cross_positive if best_is_chosen_sleeve and best.get("cell_id") == chosen
                                                else (False if best_is_chosen_sleeve else None)))
    if status not in STATUSES:
        status = ST_RESEARCHING
    clock = CK.session_clock(cp, as_of=as_of) if cp else {"state": "NO_CHECKPOINT"}
    outcome = CK.deadline_outcome(cp, scoreboard_status=status, as_of=as_of) if cp else None
    fam_tested = sorted({c.get("family") for c in cands if c.get("family")})
    alive = [c for c in cands if c.get("verdict") in ("MATERIALLY_BEATS_INCUMBENT", "BEATS_INCUMBENT_NOT_QUALIFIED",
                                                       "ECONOMIC_UNDER_CONTROLS", "ECONOMIC_UNDER_CONTROLS_NOT_FDR",
                                                       "ECONOMIC_UNDER_CONTROLS_UNSTABLE", "CALIBRATED_DIRECTIONAL_SKILL",
                                                       "PROFITABLE_NOT_CALIBRATED")]
    np_rule = (program or {}).get("non_price_rule") or {}
    frontier_rows = ((program or {}).get("frontier") or {}).get("rows") or []
    # a need this campaign has MEASURED is no longer an unresolved gap, whatever
    # the persisted frontier still says about it
    measured_keys = {b.get("cell_key") for b in ((residual or {}).get("brief") or [])}
    measured_keys |= {b.get("cell_key") for b in ((mandates or {}).get("brief") or [])}
    measured_keys |= {c.get("information") for c in cands if c.get("kind") == "FRONTIER_NEED"}
    measured_keys.discard(None)
    gap = None
    for r in frontier_rows:
        if r.get("cell_key") in measured_keys:
            continue
        if r.get("cell_key") and r.get("observation_state") != "WELL_OBSERVED" or (r.get("best_verdict") in (None, "")):
            gap = r
            break
    if gap is None:
        gap = next((r for r in frontier_rows if r.get("cell_key") not in measured_keys), None)
    if gap is not None:
        gap = {**gap, "measured_needs_excluded": sorted(measured_keys)}
    advantage = None
    if best is not None:
        advantage = {"incremental_net_return": best.get("historical_oos_net_advantage"),
                     "sharpe_delta": best.get("sharpe_delta"), "drawdown_delta": best.get("drawdown_delta"),
                     "turnover_delta": best.get("turnover_delta"),
                     "forecast_calibration_improvement": (
                         {"brier_skill_score": best.get("brier_skill_score"),
                          "expected_calibration_error": best.get("expected_calibration_error")}
                         if best.get("kind") == "MARKET_DIRECTION" else "NOT_APPLICABLE (rank model)"),
                     "statistical_status": {"t": best.get("t_advantage"), "conditional_t": best.get("conditional_t"),
                                            "fdr_pass": best.get("fdr_pass"), "holm_pass": best.get("holm_pass"),
                                            "failed_gates": best.get("failed_gates")},
                     "economic_materiality": bool((best.get("historical_oos_net_advantage") or 0) >= 0.015),
                     "cross_domain_equal_risk": ({"incremental_ann_net": cr.get("incremental_ann_net_return"),
                                                  "sharpe_delta": cr.get("sharpe_delta"), "t": cr.get("t_incremental"),
                                                  "positive_after_costs": cross_positive}
                                                 if best.get("kind") == "CROSS_DOMAIN_SLEEVE" and cr.get("state") == "OK"
                                                 else "NOT_APPLICABLE"),
                     "forward_evidence": "NONE (no ALPHA_RECOVERY challenger is registered)"
                     if not any(str(r.get("challenger_id") or "").startswith("ALPHA_RECOVERY") for r in regs)
                     else "REGISTERED"}
    same_domain = [c for c in cands if c.get("kind") in ("SAME_DOMAIN", "SAME_DOMAIN_CONSTRUCTION")]
    best_same_domain = _best(same_domain)
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "status": status, "status_vocabulary": list(STATUSES),
            "incumbent": _incumbent_block(inc),
            "best_challenger": best, "advantage": advantage,
            "best_same_domain_challenger": best_same_domain,
            "best_same_domain_note": ("the candidate that would use the SAME capital as the incumbent; "
                                      "reported alongside the ranked best because a cross-domain sleeve "
                                      "can win the ranking without being applicable to this portfolio"),
            "forecast_products": ({"n_licensed_families": products.get("n_licensed_families"),
                                   "n_families": products.get("n_families"),
                                   "licensed_families": products.get("licensed_families"),
                                   "answer": products.get("answer")} if products else None),
            "campaign": {"clock": clock, "deadline": outcome,
                         "share_new_research_effort_non_price": np_rule.get("share_non_price"),
                         "non_price_rule_met": np_rule.get("rule_met"),
                         "share_non_price_inclusive": ((program or {}).get("non_price_rule_inclusive")
                                                       or {}).get("share_non_price"),
                         "non_price_rule_met_inclusive": ((program or {}).get("non_price_rule_inclusive")
                                                          or {}).get("rule_met"),
                         "n_specifications_executed": np_rule.get("executed"),
                         "n_information_families_tested": len(fam_tested), "families_tested": fam_tested,
                         "n_candidate_specifications_alive": len(alive),
                         "n_in_true_forward_competition": sum(
                             1 for r in regs if str(r.get("challenger_id") or "").startswith("ALPHA_RECOVERY")),
                         "highest_value_unresolved_information_gap": gap,
                         "top_missing_information_need": (purchase or {}).get("top_missing_information_need")},
            "candidates": cands, "forward_registrations_read_only": regs,
            "evidence_separation": "historical_oos and true_forward are never pooled",
            "not_measures_of_success": ["release numbers", "lines changed", "tests passed", "architecture work"]}
    if write:
        write_repo_artifact(SCOREBOARD_JSON_NAME, body)
        (repo_dir() / SCOREBOARD_MD_NAME).write_text(render(body), encoding="utf-8")
    return body


def _f(x, nd=4):
    if x is None:
        return "n/a"
    try:
        return ("%%.%df" % nd) % float(x)
    except (TypeError, ValueError):
        return str(x)


def render(sb: dict) -> str:
    inc = sb.get("incumbent") or {}
    ho, tf = inc.get("historical_oos") or {}, inc.get("true_forward") or {}
    b = sb.get("best_challenger") or {}
    adv = sb.get("advantage") or {}
    camp = sb.get("campaign") or {}
    clock = camp.get("clock") or {}
    dl = camp.get("deadline") or {}
    L = ["# ALPHA RECOVERY SCOREBOARD", "",
         "**STATUS: %s**" % sb.get("status"), "",
         "Stop-loss clock: %s" % clock.get("rendered", "no checkpoint"),
         "Deadline outcome: %s%s" % (dl.get("outcome"), " (PROJECT FAILURE STATE)" if dl.get("project_failure_state") else ""), "",
         "| field | INCUMBENT (historical OOS, 21s, top-25) | INCUMBENT (TRUE_FORWARD) | BEST CHALLENGER (historical OOS) |",
         "|---|---|---|---|",
         "| identity | %s | %s | %s |" % (inc.get("model_id"), tf.get("book_id"), b.get("cell_id")),
         "| verdict | %s | separate gate | %s |" % (inc.get("verdict"), b.get("verdict")),
         "| net return / excess | %s /yr net excess | %s cum (SPY %s) | advantage %s /yr |" % (
             _f(ho.get("ann_net_excess")), _f(tf.get("cumulative_return")), _f(tf.get("benchmark_cumulative_return")),
             _f(b.get("historical_oos_net_advantage"))),
         "| Sharpe | %s (excess) | %s | delta %s |" % (_f(ho.get("sharpe_excess"), 2), _f(tf.get("sharpe_annualised"), 2), _f(b.get("sharpe_delta"), 2)),
         "| drawdown | %s | %s | delta %s |" % (_f(ho.get("max_drawdown"), 3), _f(tf.get("max_drawdown"), 3), _f(b.get("drawdown_delta"), 3)),
         "| turnover (one-way / period) | %s | %s total | %s |" % (_f(ho.get("mean_oneway_turnover_per_period"), 3), _f(tf.get("total_turnover_oneway"), 3), _f(b.get("challenger_turnover"), 3)),
         "| rank IC (t) | %s (%s) | h1 %s (%s) | conditional t %s |" % (_f(ho.get("mean_rank_ic")), _f(ho.get("t_rank_ic"), 2), _f(tf.get("h1_mean_rank_ic")), _f(tf.get("h1_t_rank_ic"), 2), _f(b.get("conditional_t"), 2)),
         "| calibration | %s | n/a | %s |" % ((inc.get("expected_return_calibration") or {}).get("state"),
                                            adv.get("forecast_calibration_improvement") if isinstance(adv.get("forecast_calibration_improvement"), str) else _f(b.get("brier_skill_score"))),
         "| multiplicity | benchmark (exempt) | n/a | BH %s / Holm %s |" % (b.get("fdr_pass"), b.get("holm_pass")),
         "| evidence sample | %s periods (%s effective) | %s sessions | %s effective |" % (
             ho.get("periods"), ho.get("effective_periods"), tf.get("sessions"), b.get("effective_periods")),
         "| evidence maturity | %s | %s | HISTORICAL_OOS_ONLY |" % (ho.get("evidence_maturity"), tf.get("evidence_maturity")),
         "| forward evidence | n/a | this IS the forward evidence | %s |" % adv.get("forward_evidence"),
         ""]
    sd = sb.get("best_same_domain_challenger") or {}
    if sd:
        L += ["## Best challenger on the SAME capital as the incumbent", "",
              "`%s` - %s" % (sd.get("cell_id"), sd.get("verdict")),
              "",
              "| net advantage /yr | t | Sharpe delta | drawdown delta | turnover delta | lockbox advantage | failed gates |",
              "|---|---|---|---|---|---|---|",
              "| %s | %s | %s | %s | %s | %s | %s |" % (
                  _f(sd.get("historical_oos_net_advantage")), _f(sd.get("t_advantage"), 2),
                  _f(sd.get("sharpe_delta"), 3), _f(sd.get("drawdown_delta"), 3),
                  _f(sd.get("turnover_delta"), 3), _f(sd.get("lockbox_net_advantage")),
                  ",".join(sd.get("failed_gates") or []) or "none"),
              "", sd.get("capital_applicability") or "", ""]
    fp = sb.get("forecast_products") or {}
    if fp:
        L += ["## Forecast products", "",
              "- %s" % fp.get("answer"), ""]
    L += ["## Campaign counters", "",
         "- eligible sessions elapsed / 10: %s / %s (remaining %s)" % (clock.get("sessions_elapsed"), clock.get("stop_loss_sessions"), clock.get("sessions_remaining")),
         "- share of new research effort on non-price information: %s (rule >= 0.75: %s); inclusive of "
         "construction-only specifications %s (%s)" % (
             _f(camp.get("share_new_research_effort_non_price"), 3), camp.get("non_price_rule_met"),
             _f(camp.get("share_non_price_inclusive"), 3), camp.get("non_price_rule_met_inclusive")),
         "- economically distinct information families tested: %s (%s)" % (camp.get("n_information_families_tested"), ", ".join(camp.get("families_tested") or [])),
         "- candidate specifications alive: %s" % camp.get("n_candidate_specifications_alive"),
         "- in TRUE_FORWARD competition: %s" % camp.get("n_in_true_forward_competition"),
         "- highest-value unresolved information gap: %s" % ((camp.get("highest_value_unresolved_information_gap") or {}).get("cell_key")),
         "- top missing information need (purchase case): %s" % camp.get("top_missing_information_need"),
         "",
         "## Every candidate", "",
         "| cell | kind | verdict | net advantage /yr | t | Sharpe delta | lockbox advantage | BH | Holm | failed gates |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for c in sb.get("candidates") or []:
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            c.get("cell_id"), c.get("kind"), c.get("verdict"), _f(c.get("historical_oos_net_advantage")),
            _f(c.get("t_advantage"), 2), _f(c.get("sharpe_delta"), 3), _f(c.get("lockbox_net_advantage")),
            c.get("fdr_pass"), c.get("holm_pass"), ",".join(c.get("failed_gates") or []) if c.get("failed_gates") else ""))
    L += ["", "Historical OOS and TRUE_FORWARD are separate columns and are never pooled. "
              "Release numbers, lines changed, tests passed and architecture work are not on this page.", ""]
    return "\n".join(L)
