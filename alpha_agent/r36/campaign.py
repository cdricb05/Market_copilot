"""alpha_agent.r36.campaign - orchestration, multiple testing and the verdict.

The order of operations here is the release's argument, so it is worth stating.
The contract is frozen first, before a single price is read. Entitlement is
MEASURED second, because what this estate can research is a fact about endpoints
and not about intentions. Acquisition third, and it downloads only what is not
already on disk. Then the native instruments are built, then every frozen
configuration is executed - all of them, including the ones that will fail -
then multiple testing is applied to the complete set, and only then is a verdict
computed from gates that were written down before any of it ran.

Two things this module refuses to do.

**It does not choose a winner and then test it.** Benjamini-Hochberg runs over
every executed configuration, and the direction is split: a configuration that
LOSES to its control significantly is a rejection too, and counting it as a
success would be reading the sign after the fact.

**It does not call anything alpha.** A configuration that passes every
historical gate becomes ``RESEARCH_CANDIDATE_RESULT = PASS``. ``ALPHA_RESULT``
requires genuinely independent evidence, the contract records that none exists,
and the gate reads the contract rather than the results.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

import numpy as np
import pandas as pd

from .. import r36
from ..r31 import multiple_testing as _mt
from ..r34 import economics as _economics
from ..r35 import information as _r35_information
from . import acquisition as _acquisition
from . import contract as _contract
from . import coverage as _coverage
from . import entitlements as _entitlements
from . import experiments as _experiments
from . import native_markets as _native
from . import strategies as _strategies

CALCULATION_OWNER = "alpha_agent.r36.campaign"
VERDICT_SCHEMA = "r36_global_multi_asset_verdict/1"
VERDICT_ARTIFACT = "final_verdict.json"
MT_SCHEMA = "r36_multiple_testing_results/1"
MT_ARTIFACT = "multiple_testing_results.json"
FORWARD_SCHEMA = "r36_forward_evidence_handoff/1"
FORWARD_ARTIFACT = "forward_evidence_handoff.json"


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Information
# --------------------------------------------------------------------------- #
def load_information(results: dict) -> dict:
    """Every raw payload turned into the point-in-time objects lanes read."""
    fred_files = (results.get(_acquisition.SRC_FRED) or {}).get("files") or {}
    fred = _native.read_fred(fred_files)

    cboe_files = (results.get(_acquisition.SRC_R35_CBOE) or {}).get(
        "files") or {}
    cboe = _r35_information.load_cboe(cboe_files) if cboe_files else {
        "ok": False, "series": {}}

    cftc_files = (results.get(_acquisition.SRC_R35_CFTC) or {}).get(
        "files") or {}
    codes = sorted({c for group in _contract.CFTC_CODES.values()
                    for c in group})
    cot = (_r35_information.load_cot(cftc_files, codes=codes)
           if cftc_files else {"ok": False, "frame": None})

    petroleum = ((results.get(_acquisition.SRC_R35_EIA_PET) or {}).get("files")
                 or {}).get("PET")
    natural_gas = ((results.get(_acquisition.SRC_EIA_NG) or {}).get("files")
                   or {}).get("NG")
    curves = _native.read_commodity_curves(petroleum, natural_gas)

    return {"fred": fred, "cboe": cboe, "cot": cot, "curves": curves}


def build_panels(information: dict) -> dict:
    """Every lane's instrument panel, built once and shared by its strategies."""
    fred = information["fred"]
    cot_frame = (information["cot"] or {}).get("frame")
    panels = {}
    panels[_contract.LANE_FX] = _native.build_fx(fred, cot_frame=cot_frame)
    panels[_contract.LANE_COMMODITY] = _native.build_commodity(
        information["curves"], fred, cot_frame=cot_frame)
    panels[_contract.LANE_RATES] = _native.build_rates(fred)
    panels[_contract.LANE_CREDIT] = _native.build_credit(fred)
    panels[_contract.LANE_VOL] = _native.build_volatility(
        information["cboe"], fred)
    panels[_contract.LANE_CRYPTO] = _native.build_crypto(fred)
    panels[_contract.LANE_CROSS_ASSET] = _native.build_cross_asset(
        fred, **_cross_asset_inputs(panels.get(_contract.LANE_FX), fred))
    # Every lane is trimmed to the window where its own control is observable,
    # uniformly and before any strategy runs, so no configuration can be
    # credited or blamed for decisions whose benchmark did not exist.
    return {lane: (_native.trim_to_control(panel) if panel.get("ok") else panel)
            for lane, panel in panels.items()}


def _cross_asset_inputs(fx_panel: Optional[dict], fred: dict) -> dict:
    """The currency carry book, offered to the cross-asset lane as a column.

    Built from the FROZEN ``FX_CARRY`` rule, never from a selected winner: the
    rule was written down before any result existed, so using its book as an
    input to a cross-asset allocation is not a second look at the data.
    """
    if not fx_panel or not fx_panel.get("ok"):
        return {}
    try:
        built = _strategies.build_weights("FX_CARRY", fx_panel)
    except Exception:  # noqa: BLE001 - a missing book is not a failed release
        return {}
    weights = built["weights"]
    excess = fx_panel["excess"]
    index = weights.index.intersection(excess.index)
    meta = {c: dict((fx_panel.get("meta") or {}).get(c) or {})
            for c in weights.columns}
    path = _economics.evaluate_book(
        weights.reindex(index), excess,
        pd.Series(0.0, index=index), meta=meta,
        horizon=int(fx_panel.get("cadence") or 21))
    if path.get("state") != "OK":
        return {}
    book = pd.Series(path["net"], index=path["dates"], dtype=float)
    return {"align_dates": book.index,
            "extra_excess": {"FX_CARRY_BOOK": book},
            "extra_meta": {"FX_CARRY_BOOK": {
                "asset_class": "FX", "economic_group": "CURRENCY_CARRY_BOOK",
                "cost_tier": "FX_G10",
                "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["FX_G10"],
                "note": "the frozen FX_CARRY book, already charged its own "
                        "costs inside the currency lane"}}}


def _reindex_book(book: pd.Series, dates) -> pd.Series:
    return book.reindex(pd.DatetimeIndex(dates)).astype(float)


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
def run_experiments(panels: dict, *, verbose: bool = False) -> list:
    """Every frozen configuration, executed. Failures stay in the record."""
    results = []
    for name in sorted(_contract.STRATEGIES):
        lane = _contract.STRATEGIES[name][0]
        panel = panels.get(lane)
        if not panel or not panel.get("ok"):
            results.append({"name": name, "lane": lane,
                            "state": _experiments.NOT_EXECUTED,
                            "families": list(_contract.STRATEGIES[name][1]),
                            "implementation_level":
                                _contract.STRATEGIES[name][2],
                            "reason": "LANE_UNAVAILABLE:%s"
                                      % ((panel or {}).get("reason")
                                         or "NOT_BUILT")})
            if verbose:
                print("  %-32s SKIPPED (%s)" % (name, lane))
            continue
        try:
            row = _experiments.run_configuration(name, panel)
        except Exception as exc:  # noqa: BLE001 - a failure is a record
            row = {"name": name, "lane": lane,
                   "state": _experiments.NOT_EXECUTED,
                   "families": list(_contract.STRATEGIES[name][1]),
                   "implementation_level": _contract.STRATEGIES[name][2],
                   "reason": "EXECUTION_ERROR:%s:%s"
                             % (type(exc).__name__, str(exc)[:160])}
        results.append(row)
        if verbose:
            if row.get("state") == _experiments.EXECUTED:
                print("  %-32s excess %+.4f  t %+.2f  periods %d"
                      % (name, row.get("after_cost_excess_annualised") or 0.0,
                         row.get("after_cost_excess_t_stat") or 0.0,
                         row["economics"]["periods"]))
            else:
                print("  %-32s NOT EXECUTED (%s)" % (name, row.get("reason")))
    return results


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def run_multiple_testing(results: list) -> dict:
    """Benjamini-Hochberg over EVERY executed configuration, direction split."""
    executed = [r for r in results if r.get("state") == _experiments.EXECUTED]
    per_config, p_values = [], []
    for row in executed:
        t_stat = row.get("after_cost_excess_t_stat")
        p_value = (_mt.two_sided_p(t_stat)
                   if t_stat is not None and np.isfinite(t_stat) else None)
        per_config.append({
            "name": row["name"], "lane": row["lane"],
            "implementation_level": row.get("implementation_level"),
            "after_cost_excess_annualised":
                row.get("after_cost_excess_annualised"),
            "t_stat": t_stat, "p_value": p_value,
            "periods": row["economics"]["periods"]})
        p_values.append(p_value)

    bh = _mt.benjamini_hochberg(p_values, q=_contract.FDR_Q)
    beating, losing = [], []
    for position in bh["rejected"]:
        row = per_config[position]
        target = beating if (row["after_cost_excess_annualised"] or 0.0) > 0 \
            else losing
        target.append(row["name"])

    spa_by_lane = {}
    for lane in sorted({r["lane"] for r in executed}):
        series = {}
        for row in executed:
            if row["lane"] != lane:
                continue
            diff = row.get("_diff")
            if diff is None or len(diff) < 8:
                continue
            series[row["name"]] = np.asarray(diff, dtype=np.float64)
        if len(series) < 2:
            continue
        lengths = {len(v) for v in series.values()}
        if len(lengths) > 1:
            shortest = min(lengths)
            series = {k: v[-shortest:] for k, v in series.items()}
        spa_by_lane[lane] = _mt.superior_predictive_ability(
            series, resamples=_contract.BOOTSTRAP_RESAMPLES,
            block_mean=_contract.BOOTSTRAP_BLOCK_MEAN,
            seed=_contract.BOOTSTRAP_SEED)

    best = _best_configuration(executed)
    paired = {"state": "NO_CANDIDATE"}
    if best is not None and best.get("_diff") is not None:
        paired = _mt.paired_block_bootstrap(
            np.asarray(best["_diff"], dtype=np.float64),
            resamples=_contract.BOOTSTRAP_RESAMPLES,
            block_mean=_contract.BOOTSTRAP_BLOCK_MEAN,
            seed=_contract.BOOTSTRAP_SEED)
        paired["candidate"] = best["name"]

    payload = {
        "calculation_owner": _mt.CALCULATION_OWNER,
        "policy": "BENJAMINI_HOCHBERG_OVER_EVERY_EXECUTED_CONFIGURATION",
        "denominator_executed_configurations": len(executed),
        "denominator_counts_all_executed":
            _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
        "controls_enter_denominator": _contract.CONTROLS_ENTER_DENOMINATOR,
        "fdr_q": _contract.FDR_Q,
        "benjamini_hochberg": bh,
        "rejected_beating_the_control": sorted(beating),
        "rejected_losing_to_the_control": sorted(losing),
        "only_positive_rejections_may_qualify":
            _contract.ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY,
        "superior_predictive_ability_by_lane": spa_by_lane,
        "paired_block_bootstrap_best": paired,
        "per_configuration": per_config,
        "seeds": {"bootstrap": _contract.BOOTSTRAP_SEED},
    }
    return r36.artifact_body(MT_SCHEMA, payload)


def _best_configuration(executed: list) -> Optional[dict]:
    """The largest after-cost excess t-statistic among configurations that
    actually took positions."""
    live = [r for r in executed
            if (r.get("active_periods") or 0) > 0
            and r.get("after_cost_excess_t_stat") is not None
            and np.isfinite(r["after_cost_excess_t_stat"])]
    if not live:
        return None
    return max(live, key=lambda r: r["after_cost_excess_t_stat"])


def qualify(results: list, multiple_testing: dict) -> list:
    """Apply the frozen gate to every executed configuration."""
    survivors = set(multiple_testing["rejected_beating_the_control"])
    for row in results:
        if row.get("state") != _experiments.EXECUTED:
            row["qualified"] = False
            continue
        gates = dict(row["gates"])
        gates["survives_multiple_testing_procedure"] = row["name"] in survivors
        row["gates"] = gates
        row["qualified"] = all(gates.values())
        row["gates_failed"] = sorted(k for k, v in gates.items() if not v)
    return results


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def build_verdict(*, results: list, multiple_testing: dict, cells: list,
                  entitlement: dict, panels: dict) -> dict:
    executed = [r for r in results if r.get("state") == _experiments.EXECUTED]
    qualified = [r for r in executed if r.get("qualified")]
    summary = _coverage.summarise(cells)

    lanes_built = sorted(l for l, p in panels.items() if p.get("ok"))
    lanes_unavailable = sorted(l for l, p in panels.items()
                               if not p.get("ok"))
    native_executed = [r for r in executed
                       if r.get("implementation_level")
                       == _contract.LEVEL_NATIVE]
    integrity = data_integrity(panels, results)

    if not integrity["passes"]:
        verdict = _contract.VERDICT_INTEGRITY
    elif qualified:
        verdict = _contract.VERDICT_EDGE_FOUND
    elif not native_executed and entitlement.get("sources_blocked"):
        verdict = _contract.VERDICT_BLOCKED_ENTITLEMENT
    elif not executed:
        verdict = _contract.VERDICT_BLOCKED_DATA
    elif summary["still_untested_but_executable"] > 0:
        verdict = _contract.VERDICT_PARTIAL
    else:
        verdict = _contract.VERDICT_NO_EDGE

    system_result = (_contract.RESULT_PASS
                     if (executed and integrity["passes"]
                         and summary["every_cell_is_terminal"])
                     else _contract.RESULT_FAIL)
    research_result = (_contract.RESULT_PASS
                       if verdict == _contract.RESEARCH_CANDIDATE_PASS_REQUIRES
                       else _contract.RESULT_FAIL)
    alpha_result = (
        _contract.RESULT_PASS
        if (verdict == _contract.ALPHA_PASS_REQUIRES
            and _contract.genuinely_independent_evidence_exists())
        else _contract.RESULT_FAIL)

    best = _best_configuration(executed)
    payload = {
        "campaign_id": _contract.CAMPAIGN_ID,
        "created_at": _now(),
        "calculation_owner": CALCULATION_OWNER,
        "verdict": verdict,
        "verdict_reading": VERDICT_READING.get(verdict, ""),
        "verdict_options": list(_contract.PRIMARY_VERDICTS),
        "SYSTEM_RESULT": system_result,
        "RESEARCH_CANDIDATE_RESULT": research_result,
        "ALPHA_RESULT": alpha_result,
        "result_names": list(_contract.RESULT_NAMES),
        "alpha_pass_requires": _contract.ALPHA_PASS_REQUIRES,
        "alpha_pass_also_requires_independent_evidence":
            _contract.ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE,
        "genuinely_independent_evidence_exists":
            _contract.genuinely_independent_evidence_exists(),
        "fresh_unseen_evidence_reason": _contract.FRESH_UNSEEN_EVIDENCE_REASON,
        "executed_configurations": len(executed),
        "planned_configurations": _contract.PLANNED_CONFIG_TOTAL,
        "ceiling": _contract.MAX_PRIMARY_CONFIGS,
        "native_configurations_executed": len(native_executed),
        "qualified_configurations": sorted(r["name"] for r in qualified),
        "lanes_built": lanes_built,
        "lanes_unavailable": lanes_unavailable,
        "lane_unavailable_reasons": {l: panels[l].get("reason")
                                     for l in lanes_unavailable},
        "coverage_summary": summary,
        "findings": findings(results, multiple_testing),
        "data_integrity": integrity,
        "best_configuration": ({
            "name": best["name"], "lane": best["lane"],
            "implementation_level": best.get("implementation_level"),
            "after_cost_excess_annualised":
                best.get("after_cost_excess_annualised"),
            "after_cost_excess_t_stat": best.get("after_cost_excess_t_stat"),
            "minimum_detectable_excess": best.get("minimum_detectable_excess"),
            "gates_failed": best.get("gates_failed"),
        } if best else None),
        "multiple_testing": {
            "denominator": multiple_testing["denominator_executed_"
                                            "configurations"],
            "rejected_beating_the_control":
                multiple_testing["rejected_beating_the_control"],
            "rejected_losing_to_the_control":
                multiple_testing["rejected_losing_to_the_control"],
        },
        "money_spent": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
    }
    return r36.artifact_body(VERDICT_SCHEMA, payload)


VERDICT_READING = {
    _contract.VERDICT_EDGE_FOUND:
        "at least one native configuration beat a lane-appropriate control "
        "after cost and survived multiple testing; it is a research candidate "
        "and not alpha, because the evidence is historical",
    _contract.VERDICT_NO_EDGE:
        "every executable cell was executed and nothing beat its control; the "
        "frontier is closed on the data this estate can reach for free",
    _contract.VERDICT_PARTIALLY_CLOSED_READING:
        "nothing beat its control, and a named minority of executable cells "
        "was not executed because the configuration grid was frozen before "
        "any result was seen and may not be widened afterwards",
    _contract.VERDICT_BLOCKED_DATA:
        "no native lane could be built from owned or free point-in-time data",
    _contract.VERDICT_BLOCKED_ENTITLEMENT:
        "every native lane needs a vendor entitlement this release was not "
        "authorised to buy",
    _contract.VERDICT_INTEGRITY:
        "a point-in-time or construction check failed, so no economic number "
        "in this release may be read",
}


def findings(results: list, multiple_testing: dict) -> dict:
    """The two questions a reader actually has: what predicted, and what paid."""
    executed = [r for r in results if r.get("state") == _experiments.EXECUTED]
    predictive = []
    for row in executed:
        diagnostic = row.get("predictive_diagnostic") or {}
        if diagnostic.get("state") != "OK":
            continue
        t_stat = diagnostic.get("t_stat")
        if t_stat is None or not np.isfinite(t_stat):
            continue
        predictive.append((abs(float(t_stat)), row, diagnostic))
    predictive.sort(key=lambda item: -item[0])
    economic = sorted(
        (r for r in executed
         if r.get("after_cost_excess_t_stat") is not None
         and np.isfinite(r["after_cost_excess_t_stat"])),
        key=lambda r: -r["after_cost_excess_t_stat"])
    return {
        "strongest_predictive": [{
            "name": row["name"], "lane": row["lane"],
            "statistic": diagnostic.get("statistic"),
            "value": diagnostic.get("mean_rank_ic",
                                    diagnostic.get("hit_rate")),
            "t_stat": diagnostic.get("t_stat"),
            "scored_dates": diagnostic.get("scored_dates",
                                           diagnostic.get("active_periods")),
            "after_cost_excess_annualised":
                row.get("after_cost_excess_annualised"),
            "after_cost_excess_t_stat": row.get("after_cost_excess_t_stat"),
        } for _t, row, diagnostic in predictive[:5]],
        "strongest_economic": [{
            "name": row["name"], "lane": row["lane"],
            "implementation_level": row.get("implementation_level"),
            "control": row.get("control"),
            "after_cost_excess_annualised":
                row.get("after_cost_excess_annualised"),
            "after_cost_excess_t_stat": row.get("after_cost_excess_t_stat"),
            "minimum_detectable_excess": row.get("minimum_detectable_excess"),
            "gates_failed": row.get("gates_failed"),
        } for row in economic[:5]],
        "significantly_worse_than_control":
            multiple_testing["rejected_losing_to_the_control"],
        "significantly_better_than_control":
            multiple_testing["rejected_beating_the_control"],
        "reading": (
            "a significant NEGATIVE rejection is information: it says the rule "
            "is reliably the wrong way round, not that the market is "
            "unpredictable"),
    }


def data_integrity(panels: dict, results: list) -> dict:
    """The checks that would make every number above meaningless if they failed."""
    checks = {
        "at_least_one_lane_built": any(p.get("ok") for p in panels.values()),
        "every_lane_control_is_observable_throughout": all(
            bool(pd.Series(p["control_excess"]).reindex(
                p["excess"].index).notna().all())
            for p in panels.values() if p.get("ok")),
        "no_lane_reports_overlapping_decisions": all(
            bool(pd.Index(p["excess"].index).is_monotonic_increasing
                 and pd.Index(p["excess"].index).is_unique)
            for p in panels.values() if p.get("ok")),
        "every_executed_configuration_has_a_control": all(
            r.get("control") for r in results
            if r.get("state") == _experiments.EXECUTED),
        "no_configuration_used_the_universal_equity_control": all(
            r.get("control") != "SPY_PLUS_CASH" for r in results
            if r.get("state") == _experiments.EXECUTED),
        "cost_is_charged_on_traded_notional":
            _contract.COST_BASE == "TRADED_NOTIONAL",
        "normalisation_is_trailing_only":
            _contract.NORMALISATION_IS_TRAILING_ONLY
            and not _contract.FULL_SAMPLE_STATISTICS_ALLOWED,
    }
    return {"checks": checks, "passes": all(checks.values())}


def forward_handoff(results: list) -> dict:
    """What a survivor would need next - and the fact that nothing was done."""
    qualified = [r for r in results if r.get("qualified")]
    payload = {
        "campaign_id": _contract.CAMPAIGN_ID,
        "created_at": _now(),
        "calculation_owner": CALCULATION_OWNER,
        "qualified_configurations": sorted(r["name"] for r in qualified),
        "registered_anything": False,
        "created_a_second_true_forward_store":
            _contract.MAY_CREATE_SECOND_TRUE_FORWARD_STORE,
        "promoted_a_model": _contract.MAY_PROMOTE_MODEL,
        "activated_a_sleeve": _contract.MAY_ACTIVATE_SLEEVE,
        "canonical_forward_evidence_owner": _contract.FORWARD_EVIDENCE_OWNER,
        "canonical_forward_prediction_owner":
            _contract.FORWARD_PREDICTION_OWNER,
        "operator_action_required": (
            "none - no configuration qualified, so there is nothing to "
            "register" if not qualified else
            "an operator may choose to register the qualified rule with the "
            "canonical forward-evidence owner named above; this release did "
            "not, because registration is a governed operational act and "
            "historical evidence cannot promote anything"),
        "historical_evidence_is_not_prospective_evidence": True,
    }
    return r36.artifact_body(FORWARD_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(*, campaign_id: str = _contract.CAMPAIGN_ID, acquire: bool = True,
        verbose: bool = True) -> dict:
    created_at = _now()
    artifacts = {}

    contract_body = _contract.build(campaign_id=campaign_id,
                                    created_at=created_at)
    artifacts["contract"] = str(_contract.freeze(contract_body))
    if verbose:
        print("contract frozen: %s" % contract_body["contract_hash"][:16])

    measured = _entitlements.measure_all()
    entitlement_body = _entitlements.artifact(measured, campaign_id=campaign_id,
                                              created_at=created_at)
    artifacts["entitlements"] = str(_entitlements.freeze(entitlement_body))
    if verbose:
        print("entitlement measured: native futures supported = %s; "
              "VIX futures entitled = %s"
              % (entitlement_body["native_futures_supported"],
                 entitlement_body["vix_futures_entitled"]))

    results_raw = (_acquisition.acquire_all() if acquire
                   else _acquisition.cached_results())
    manifest = _acquisition.manifest_artifact(
        results_raw, campaign_id=campaign_id, created_at=created_at)
    artifacts["acquisition"] = str(_acquisition.freeze(manifest))
    if verbose:
        print("acquisition: %d payloads, %.1f MB, downloaded %s, reused %s"
              % (manifest["payload_count"],
                 manifest["total_bytes"] / 1e6,
                 manifest["sources_downloaded"],
                 manifest["sources_located_not_downloaded"]))

    information = load_information(results_raw)
    panels = build_panels(information)
    registry = _native.registry_artifact(panels, campaign_id=campaign_id,
                                         created_at=created_at)
    artifacts["native_markets"] = str(_native.freeze(registry))
    if verbose:
        for lane, row in sorted(registry["lanes"].items()):
            if row["ok"]:
                cov = row["coverage"] or {}
                print("  %-28s %2d instruments, %4d decisions, %s -> %s"
                      % (lane, cov.get("instruments") or 0,
                         cov.get("decisions") or 0, cov.get("first"),
                         cov.get("last")))
            else:
                print("  %-28s UNAVAILABLE (%s)" % (lane, row["reason"]))

    if verbose:
        print("executing %d frozen configurations"
              % _contract.PLANNED_CONFIG_TOTAL)
    results = run_experiments(panels, verbose=verbose)

    multiple_testing = run_multiple_testing(results)
    results = qualify(results, multiple_testing)
    artifacts["multiple_testing"] = str(
        r36.write_json(r36.campaign_dir(campaign_id) / MT_ARTIFACT,
                       dict(multiple_testing, campaign_id=campaign_id)))

    experiment_registry = _experiments.registry_artifact(
        results, campaign_id=campaign_id, created_at=created_at)
    artifacts["experiments"] = str(
        r36.write_json(r36.campaign_dir(campaign_id)
                       / "experiment_registry.json", experiment_registry))

    cells = _coverage.build([r for r in results
                             if r.get("state") == _experiments.EXECUTED])
    coverage_body = _coverage.artifact(cells, campaign_id=campaign_id,
                                       created_at=created_at,
                                       entitlements=entitlement_body)
    artifacts["coverage"] = str(_coverage.freeze(coverage_body))

    verdict = build_verdict(results=results, multiple_testing=multiple_testing,
                            cells=cells, entitlement=entitlement_body,
                            panels=panels)
    artifacts["verdict"] = str(
        r36.write_json(r36.campaign_dir(campaign_id) / VERDICT_ARTIFACT,
                       verdict))

    handoff = forward_handoff(results)
    artifacts["forward_handoff"] = str(
        r36.write_json(r36.campaign_dir(campaign_id) / FORWARD_ARTIFACT,
                       dict(handoff, campaign_id=campaign_id)))

    return {"campaign_id": campaign_id, "created_at": created_at,
            "artifacts": artifacts, "verdict": verdict,
            "coverage_summary": coverage_body["summary"],
            "multiple_testing": multiple_testing,
            "results": results, "panels": panels}


__all__ = ["CALCULATION_OWNER", "load_information", "build_panels",
           "run_experiments", "run_multiple_testing", "qualify",
           "build_verdict", "data_integrity", "forward_handoff", "run"]
