"""alpha_agent.r39.continuation_campaign - Tracks E/F/G/J/K orchestration.

Executes, in order and with every Zone-B evaluation added to the CUMULATIVE
reuse ledger:

* the 12 DATA_AVAILABLE_BUT_NOT_TESTED cells (including the
  international-rates extension built through the canonical R38 layer
  builder);
* the four owned-but-unconsumed information families under the paired
  increment framework (only the increment counts);
* the Track-F expression frontier extras;
* the Track-G $0 model-frontier completion (neural tabular, calibrated
  probability, distributional blend, from-scratch TCN/GRU sequence nets,
  self-supervised-lite embeddings);
* the declared Zone-C pre-gate decision, the cumulative search ledger and
  the continuation verdict (Track-K qualification: v1 conditions PLUS
  residual alpha).
"""
from __future__ import annotations

import time
import traceback

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C
from . import continuation as CONT
from . import info_expansion as IE
from . import models_ext as MX
from . import trade_space_ext as TX
from .continuation import CONTINUATION_CAMPAIGN_ID
from .continuation_director import (
    Director2,
    build_intl_rates_extension,
    new_cand,
    register_bundles,
    register_sequence_bundle,
)
from .discovery_director import _strip
from .wide_prosecution import _paired_increment

CALCULATION_OWNER = "alpha_agent.r39.continuation_campaign"

CELLS_NAME = "integrity_cell_execution.json"
INFO_NAME = "new_information_increments.json"
EXPR_NAME = "trade_space_expansion.json"
MODELS_NAME = "model_frontier_completion.json"
LEDGER_NAME = "cumulative_search_ledger.json"
PREGATE_NAME = "zone_c_pregate_decision.json"
VERDICT_NAME = "continuation_verdict.json"
REGISTRY_NAME = "continuation_candidate_registry.json"


def log(msg: str) -> None:
    print("[r39c %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def _summ(rep: dict) -> dict:
    keep = ("state", "after_cost_excess_annualised",
            "after_cost_excess_t_stat", "sharpe", "annualised_turnover",
            "annualised_cost", "periods", "control", "n_fit_rows",
            "protocol")
    out = {k: rep.get(k) for k in keep if k in rep}
    ic = rep.get("ic") or {}
    out["mean_ic"] = ic.get("mean_ic")
    return out


class Continuation:
    def __init__(self, director: Director2):
        """Wraps an ALREADY-PREPARED Director2 (representations are
        generated exactly once per process; regenerating seeded symbolic
        features over a mutated panel would corrupt the bundles)."""
        self.campaign_id = director.campaign_id
        self.d2 = director
        self.records: list = []       # every evaluated candidate + summary
        self.provenance: dict = {}

    def _run(self, cand: dict, *, stage: str, protocol: str = "ZONE_B",
             note: str = "") -> dict:
        if protocol == "ZONE_B":
            rep = self.d2.eval_zone_b(cand, stage=stage)
        elif protocol == "SUBSPLIT":
            rep = self.d2.eval_subsplit(cand, stage=stage)
        else:
            raise ValueError(protocol)
        row = {"candidate_id": cand["candidate_id"], "stage": stage,
               "protocol": protocol, "note": note,
               **{k: cand[k] for k in ("lane", "scope", "bundle", "family",
                                       "model", "expression", "hyper")},
               "zone_b": _summ(rep)}
        self.records.append(row)
        self._reps = getattr(self, "_reps", {})
        self._reps[cand["candidate_id"]] = rep
        return rep

    # ------------------------------------------------------------------ #
    # Phase E1 - the 12 untested cells
    # ------------------------------------------------------------------ #
    def run_cells(self, intl_ext: dict) -> dict:
        d2 = self.d2
        cell_rows = []

        def cell(cell_id, cands, coverage_note=""):
            execd = []
            for cd, note in cands:
                rep = self._run(cd, stage="CONT_CELLS", note=cell_id)
                execd.append({"candidate_id": cd["candidate_id"],
                              "model": cd["model"],
                              "expression": cd["expression"],
                              "scope": cd["scope"],
                              "zone_b": _summ(rep), "design_note": note})
            ok = [e for e in execd if e["zone_b"].get("state") == "OK"]
            cell_rows.append({
                "r36_cell": cell_id,
                "execution_state": "EXECUTED" if ok else
                "EXECUTION_FAILED",
                "coverage_note": coverage_note,
                "candidates": execd,
                "best_zone_b_t": max(
                    (e["zone_b"].get("after_cost_excess_t_stat") or -9.9
                     for e in ok), default=None)})

        cmdty_cells = {
            "CMDTY_GRAINS::RELATIVE_VALUE": "GRAINS_AND_OILSEEDS",
            "CMDTY_LIVESTOCK::RELATIVE_VALUE": "LIVESTOCK",
            "CMDTY_PRECIOUS::RELATIVE_VALUE": "PRECIOUS_METALS",
            "CMDTY_SOFTS::RELATIVE_VALUE": "SOFTS",
        }
        for cid, grp in cmdty_cells.items():
            scope = "GROUP:" + grp
            cell(cid, [
                (new_cand("FUT", scope, "FUT_CLASSICAL", "FUT:CELL_RV",
                          "rule:carry_slope_ann", "GROUP_RV"),
                 "carry-ranked within-group RV, vol-scaled"),
                (new_cand("FUT", scope, "FUT_REV", "FUT:CELL_RV",
                          "rule:rev_1m", "GROUP_RV"),
                 "1m-reversal within-group RV"),
                (new_cand("FUT", scope, "FUT_CLASSICAL", "FUT:CELL_RV",
                          "ridge", "GROUP_RV"),
                 "learned within-group RV"),
            ])
        dm = "GROUP:INTL_INDEX_FUTURES"
        em = "GROUP:INTL_INDEX_FUTURES_EMERGING"
        cell("INTL_EQUITY_DEVELOPED::MEAN_REVERSION", [
            (new_cand("FUT", dm, "FUT_REV", "FUT:CELL_MR", "rule:rev_1m",
                      "TS_OUTRIGHT"), "1m reversal, outright"),
            (new_cand("FUT", dm, "FUT_REV", "FUT:CELL_MR", "rule:rev_1m",
                      "XS_LONG_SHORT"), "1m reversal, cross-sectional"),
            (new_cand("FUT", dm, "FUT_REV", "FUT:CELL_MR", "ridge",
                      "XS_LONG_SHORT"), "learned reversal blend"),
        ])
        cell("INTL_EQUITY_DEVELOPED::RELATIVE_VALUE", [
            (new_cand("FUT", dm, "FUT_REV", "FUT:CELL_RV", "rule:rev_1m",
                      "GROUP_RV"), "reversal RV across DM indices"),
            (new_cand("FUT", dm, "FUT_CLASSICAL", "FUT:CELL_RV", "ridge",
                      "GROUP_RV"), "learned RV across DM indices"),
        ])
        cell("INTL_EQUITY_DEVELOPED::MACRO_CONDITIONAL", [
            (new_cand("FUT", dm, "FUT_CLASSICAL_MACRO", "FUT:CELL_MC",
                      "ridge", "XS_LS_GATED", hyper="gate:vix:below_median"),
             "cross-section gated to calm-vol regimes"),
            (new_cand("FUT", dm, "FUT_CLASSICAL_MACRO", "FUT:CELL_MC",
                      "ridge", "XS_LS_GATED", hyper="gate:vix:above_median"),
             "cross-section gated to stress-vol regimes"),
            (new_cand("FUT", dm, "FUT_CLASSICAL", "FUT:CELL_MC",
                      "rule:mom_12_1", "TS_GATED",
                      hyper="gate:credit_baa10y:below_median"),
             "trend gated to tight-credit regimes"),
        ])
        em_note = ("the frozen panel carries TWO emerging-index futures "
                   "(HTW, SCN); executed, sample breadth recorded")
        cell("INTL_EQUITY_EMERGING::MEAN_REVERSION", [
            (new_cand("FUT", em, "FUT_REV", "FUT:CELL_MR", "rule:rev_1m",
                      "TS_OUTRIGHT"), "1m reversal, outright")],
             coverage_note=em_note)
        cell("INTL_EQUITY_EMERGING::RELATIVE_VALUE", [
            (new_cand("FUT", em, "FUT_REV", "FUT:CELL_RV", "rule:rev_1m",
                      "GROUP_RV"), "two-market reversal RV")],
             coverage_note=em_note)
        cell("INTL_EQUITY_EMERGING::MACRO_CONDITIONAL", [
            (new_cand("FUT", em, "FUT_CLASSICAL", "FUT:CELL_MC",
                      "rule:mom_12_1", "TS_GATED",
                      hyper="gate:vix:below_median"),
             "trend gated to calm regimes")], coverage_note=em_note)
        cell("RATES_TREASURY_FUTURES::RELATIVE_VALUE", [
            (new_cand("FUT", "RATES", "FUT_CLASSICAL", "FUT:CELL_RV",
                      "rule:carry_slope_ann", "GROUP_RV"),
             "vol-scaled curve RV across the six Treasury futures"),
            (new_cand("FUT", "RATES", "FUT_CLASSICAL", "FUT:CELL_RV",
                      "ridge", "GROUP_RV"), "learned curve RV"),
        ])
        if intl_ext.get("state") in ("BUILT", "CACHE_HIT"):
            cell("RATES_INTERNATIONAL::RELATIVE_VALUE", [
                (new_cand("FUT", "INTL_RATES", "FUT_CLASSICAL",
                          "FUT:CELL_RV", "rule:carry_slope_ann",
                          "GROUP_RV"),
                 "carry RV across 11 international bond futures"),
                (new_cand("FUT", "INTL_RATES", "FUT_CLASSICAL",
                          "FUT:CELL_RV", "ridge", "GROUP_RV"),
                 "learned RV across international bond futures"),
                (new_cand("FUT", "RATES_XCOUNTRY", "FUT_CLASSICAL",
                          "FUT:CELL_RV", "rule:carry_slope_ann",
                          "GROUP_RV", hyper="one_group"),
                 "cross-country RV, US + international, one group"),
            ], coverage_note="11 markets built through the canonical R38 "
                             "layer builder at $0; frozen layers untouched")
        else:
            cell_rows.append({
                "r36_cell": "RATES_INTERNATIONAL::RELATIVE_VALUE",
                "execution_state": "BLOCKED",
                "blocker": intl_ext,
                "candidates": []})

        body = r39.artifact_body("r39_integrity_cell_execution/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "cells": cell_rows, "n_cells": len(cell_rows),
            "intl_rates_extension": {k: v for k, v in intl_ext.items()
                                     if k != "markets"} | {
                "per_market": intl_ext.get("markets")},
            "evidence": "fit ZONE_A, judged on ZONE_B; ledger-counted; "
                        "Zone C untouched",
        })
        body["cell_execution_hash"] = r39.sha(body)
        r39.write_json(r39.campaign_dir(self.campaign_id) / CELLS_NAME,
                       body, immutable=False)
        return body

    # ------------------------------------------------------------------ #
    # Phase E2 - owned information families (paired increments)
    # ------------------------------------------------------------------ #
    def _pair(self, base_cand, var_cand, *, stage, protocol) -> dict:
        base = self._run(base_cand, stage=stage, protocol=protocol)
        var = self._run(var_cand, stage=stage, protocol=protocol)
        return {"base": {"candidate_id": base_cand["candidate_id"],
                         **_summ(base)},
                "variant": {"candidate_id": var_cand["candidate_id"],
                            **_summ(var)},
                "paired_increment": _paired_increment(base, var)}

    def run_info(self) -> dict:
        d2 = self.d2
        fut = d2.state["fut"]
        families = {}

        # ---- EIA (full-zone coverage) -------------------------------- #
        prov = self.provenance.get("eia") or {}
        eia_ok = "eia_crude_z" in fut.columns and \
            fut["eia_crude_z"].notna().any()
        if eia_ok:
            scope = "GROUP:ENERGY"
            pairs = {}
            for model in ("ridge", "lightgbm"):
                pairs["xs_" + model] = self._pair(
                    new_cand("FUT", scope, "CLS", "FUT:INFO_EIA", model,
                             "XS_LONG_SHORT"),
                    new_cand("FUT", scope, "CLS_EIA", "FUT:INFO_EIA",
                             model, "XS_LONG_SHORT"),
                    stage="CONT_INFO_EIA", protocol="ZONE_B")
            pairs["ts_ridge"] = self._pair(
                new_cand("FUT", scope, "CLS", "FUT:INFO_EIA", "ridge",
                         "TS_OUTRIGHT"),
                new_cand("FUT", scope, "CLS_EIA", "FUT:INFO_EIA", "ridge",
                         "TS_OUTRIGHT"),
                stage="CONT_INFO_EIA", protocol="ZONE_B")
            rule = self._run(
                new_cand("FUT", scope, "CLS_EIA", "FUT:INFO_EIA",
                         "rule:neg_eia_crude_z", "TS_OUTRIGHT"),
                stage="CONT_INFO_EIA",
                note="high inventories bearish, standalone diagnostic")
            families["EIA_PHYSICAL_SUPPLY_DEMAND"] = {
                "provenance": prov, "protocol": "FULL_ZONE_PAIRED",
                "pairs": pairs,
                "standalone_rule_diagnostic": _summ(rule)}
        else:
            families["EIA_PHYSICAL_SUPPLY_DEMAND"] = {
                "provenance": prov, "protocol": "FAILED_TO_JOIN"}

        # ---- NYFed (coverage-adaptive) -------------------------------- #
        prov = self.provenance.get("nyfed") or {}
        ny_col = "nyfed_net_ust_z"
        cov = fut.loc[fut[ny_col].notna(), "decision_date"] \
            if ny_col in fut.columns else pd.Series(dtype="datetime64[ns]")
        rates_fit_a = fut[(fut["zone"] == "ZONE_A")
                          & (fut["asset_class"] == "RATES")
                          & fut[ny_col].notna()] if len(cov) else \
            fut.iloc[0:0]
        cut = pd.Timestamp(IE.SUBSPLIT_FIT_END)
        fit_b_early = fut[(fut["zone"] == "ZONE_B")
                          & (fut["asset_class"] == "RATES")
                          & (fut["decision_date"] <= cut)
                          & fut[ny_col].notna()] if len(cov) else \
            fut.iloc[0:0]
        if len(rates_fit_a) >= 200:
            protocol = "ZONE_B"
        elif len(fit_b_early) >= 200:
            protocol = "SUBSPLIT"
        else:
            protocol = "RULE_ONLY_DATA_COVERAGE_LIMITED"
        if len(cov):
            pairs = {}
            if protocol != "RULE_ONLY_DATA_COVERAGE_LIMITED":
                for expr in ("XS_LONG_SHORT", "TS_OUTRIGHT"):
                    pairs[expr.lower()] = self._pair(
                        new_cand("FUT", "RATES", "CLS", "FUT:INFO_NYFED",
                                 "ridge", expr),
                        new_cand("FUT", "RATES", "CLS_NYFED",
                                 "FUT:INFO_NYFED", "ridge", expr),
                        stage="CONT_INFO_NYFED", protocol=protocol)
            rule = self._run(
                new_cand("FUT", "RATES", "CLS_NYFED", "FUT:INFO_NYFED",
                         "rule:nyfed_net_ust_z", "TS_OUTRIGHT"),
                stage="CONT_INFO_NYFED", note="dealer net-long rule")
            families["NYFED_PRIMARY_DEALER_POSITIONING"] = {
                "provenance": prov,
                "protocol": protocol,
                "zone_a_covered_rate_rows": int(len(rates_fit_a)),
                "zone_b_early_covered_rate_rows": int(len(fit_b_early)),
                "fitted_pair_blocker": None
                if protocol != "RULE_ONLY_DATA_COVERAGE_LIMITED" else
                "the summed current-format PDPOSGS codes begin 2013-04, "
                "AFTER the declared sub-split fit window; a fitted "
                "point-in-time model cannot learn this family's weights "
                "in any in-zone window. $0 unlock: decode the legacy "
                "1998-2013 mnemonics in the same owned CSV.",
                "pairs": pairs,
                "standalone_rule_diagnostic": _summ(rule)}
        else:
            families["NYFED_PRIMARY_DEALER_POSITIONING"] = {
                "provenance": prov, "protocol": "FAILED_TO_JOIN"}

        # ---- SEC insider ---------------------------------------------- #
        prov = self.provenance.get("insider") or {}
        etf = d2.state.get("etf")
        spy_rule = {"state": "FEATURE_JOIN_FAILED"}
        if etf is not None and not etf.empty and \
                "insider_net_breadth_z" in etf.columns:
            spy_rule = self._run(
                new_cand("ETF", "SPY_ONLY", "ETF_INSIDER_RULE",
                         "ETF:INFO_INSIDER", "rule:insider_net_breadth_z",
                         "TS_OUTRIGHT"),
                stage="CONT_INFO_INSIDER",
                note="market-wide net-buy breadth timing SPY vs holding "
                     "SPY")
        eq_pairs = {}
        identity = self.provenance.get("insider_identity") or {}
        identity_ok = (identity.get("eq_panel_coverage_rate") or 0) > 0.02
        if identity_ok and \
                IE.INSIDER_NAME_FEATURE in d2.state["eq"].columns:
            for model in ("ridge", "lightgbm"):
                eq_pairs[model] = self._pair(
                    new_cand("EQ", "US_SINGLE_NAME", "EQ_PRICE14",
                             "EQ:INFO_INSIDER", model, "XS_LONG_SHORT"),
                    new_cand("EQ", "US_SINGLE_NAME", "EQ_PRICE14_INSIDER",
                             "EQ:INFO_INSIDER", model, "XS_LONG_SHORT"),
                    stage="CONT_INFO_INSIDER", protocol="SUBSPLIT")
        families["SEC_INSIDER_TRANSACTIONS"] = {
            "provenance": prov,
            "identity": identity,
            "identity_state": "OK" if identity_ok else "IDENTITY_FAILURE",
            "market_timing_rule_spy": _summ(spy_rule),
            "per_name_pairs_subsplit": eq_pairs,
            "protocol": "SUBSPLIT (filings begin 2008; no Zone-A "
                        "coverage exists in the archives)"}

        # ---- Direct PIT fundamentals ----------------------------------- #
        prov = self.provenance.get("fundamentals") or {}
        eqf = d2.state.get("eqf")
        if eqf is not None and not eqf.empty:
            fit_a = eqf[eqf["zone"] == "ZONE_A"]
            protocol = "ZONE_B" if len(fit_a) >= 2000 else "SUBSPLIT"
            pairs = {}
            for model in ("ridge", "lightgbm"):
                pairs[model] = self._pair(
                    new_cand("EQF", "US_FUND_MATCHED", "EQF_PRICE14",
                             "EQ:INFO_FUND", model, "XS_LONG_SHORT"),
                    new_cand("EQF", "US_FUND_MATCHED", "EQF_ALL21",
                             "EQ:INFO_FUND", model, "XS_LONG_SHORT"),
                    stage="CONT_INFO_FUND", protocol=protocol)
            families["SEC_COMPANYFACTS_FUNDAMENTALS_DIRECT"] = {
                "provenance": prov, "protocol": protocol,
                "zone_a_rows": int(len(fit_a)),
                "coverage_caveat": "Release-30 measured 2.74x size skew in "
                                   "the fundamental-matched subset; judged "
                                   "only against its own control",
                "pairs": pairs}
        else:
            families["SEC_COMPANYFACTS_FUNDAMENTALS_DIRECT"] = {
                "provenance": prov, "protocol": "PANEL_UNAVAILABLE"}

        body = r39.artifact_body("r39_new_information_increments/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "standalone_significance_does_not_count": True,
            "only_the_increment_counts": True,
            "families": families,
            "structural_finding": "free information families begin in "
                                  "1982/1998/2008/2009; families without "
                                  "Zone-A coverage cannot train "
                                  "point-in-time models in the discovery "
                                  "zone and are tested on the declared "
                                  "Zone-B sub-split instead",
        })
        body["information_increments_hash"] = r39.sha(body)
        r39.write_json(r39.campaign_dir(self.campaign_id) / INFO_NAME,
                       body, immutable=False)
        return body

    # ------------------------------------------------------------------ #
    # Phase F - expression frontier extras
    # ------------------------------------------------------------------ #
    def run_expressions(self) -> dict:
        rows = {}
        rows["leadlag_xs"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_GRAPH", "FUT:EXPR_EXT",
                     "rule:graph_leadlag", "XS_LONG_SHORT"),
            stage="CONT_EXPR", note="lead-lag graph rule, cross-section"))
        rows["leadlag_ts"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_GRAPH", "FUT:EXPR_EXT",
                     "rule:graph_leadlag", "TS_OUTRIGHT"),
            stage="CONT_EXPR", note="lead-lag graph rule, outright"))
        rows["abstain_classical"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_CLASSICAL", "FUT:EXPR_EXT",
                     "ridge", "XS_LS_ABSTAIN"),
            stage="CONT_EXPR", note="abstention overlay |z|>=0.5"))
        rows["abstain_wide"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_WIDE", "FUT:EXPR_EXT",
                     "ridge", "XS_LS_ABSTAIN"),
            stage="CONT_EXPR",
            note="abstention overlay on the WIDE bundle - a NEW candidate "
                 "generated from discovery information, not a redesign of "
                 "the frozen finalist"))
        rows["cmdty_group_spread_carry"] = _summ(self._run(
            new_cand("FUT", "COMMODITY", "FUT_CLASSICAL", "FUT:EXPR_EXT",
                     "rule:carry_slope_ann", "GROUP_SPREAD"),
            stage="CONT_EXPR", note="best-vs-worst carry inside every "
                                    "commodity group"))
        rows["cmdty_group_spread_ridge"] = _summ(self._run(
            new_cand("FUT", "COMMODITY", "FUT_CLASSICAL", "FUT:EXPR_EXT",
                     "ridge", "GROUP_SPREAD"),
            stage="CONT_EXPR", note="learned best-vs-worst inside groups"))
        if "sector" in self.d2.state["eq"].columns:
            for model in ("ridge", "lightgbm"):
                rows["eq_sector_neutral_" + model] = _summ(self._run(
                    new_cand("EQ", "US_SINGLE_NAME", "EQ_PRICE14",
                             "EQ:EXPR_EXT", model,
                             "XS_LS_SECTOR_NEUTRAL"),
                    stage="CONT_EXPR",
                    note="sector-demeaned cross-section; unblocked by "
                         "the phase-24 sectors axis"))
        body = r39.artifact_body("r39_trade_space_expansion/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "new_expressions": {k: {"control": v}
                                for k, v in
                                TX.EXPRESSION_CONTROLS_EXT.items()},
            "results": rows,
            "blocked_structures": TX.BLOCKED_STRUCTURES,
            "sector_neutral_unblocked_by": TX.SECTOR_NEUTRAL_UNBLOCKED_BY,
            "uncertainty_weighting": "carried in this pass by the binary "
                                     "abstention overlay; a continuous "
                                     "conviction weight is future work, "
                                     "declared not silent",
        })
        body["trade_space_expansion_hash"] = r39.sha(body)
        r39.write_json(r39.campaign_dir(self.campaign_id) / EXPR_NAME,
                       body, immutable=False)
        return body

    # ------------------------------------------------------------------ #
    # Phase X2 - representation repair (a continuation DISCOVERY)
    # ------------------------------------------------------------------ #
    def run_representation_repair(self) -> dict:
        from .continuation_director import add_latent_graph_repaired
        d2 = self.d2
        fut = d2.state["fut"]
        dead = {}
        for col in ("latent_load_pc1", "latent_load_pc2",
                    "latent_load_pc3", "latent_resid_mom",
                    "graph_leadlag"):
            if col in fut.columns:
                per_zone = {}
                for z in ("ZONE_A", "ZONE_B", "ZONE_C"):
                    zz = fut[fut["zone"] == z]
                    per_zone[z] = round(float(np.isfinite(
                        zz[col].to_numpy(dtype=float)).mean()), 4)
                dead[col] = {"overall": round(float(np.isfinite(
                    fut[col].to_numpy(dtype=float)).mean()), 4),
                    **per_zone}
        fut, names, coverage = add_latent_graph_repaired(fut)
        d2.state["fut"] = fut
        d2.bundles["FUT_LATENT2"] = [n for n in names
                                     if n.startswith("latent2")]
        d2.bundles["FUT_GRAPH2"] = ["graph2_leadlag", "ret_1m", "vol_63"]
        live_wide = [c for c in d2.bundles["FUT_WIDE"]
                     if c in fut.columns
                     and np.isfinite(fut[c].to_numpy(dtype=float)).any()]
        d2.bundles["FUT_WIDE2"] = live_wide + names
        results = {}
        results["latent2_ridge_xs"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_LATENT2", "FUT:REPRESENT2",
                     "ridge", "XS_LONG_SHORT"),
            stage="CONT_REPR2", note="repaired latent factors"))
        results["graph2_rule_xs"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_GRAPH2", "FUT:REPRESENT2",
                     "rule:graph2_leadlag", "XS_LONG_SHORT"),
            stage="CONT_REPR2", note="repaired lead-lag rule"))
        results["graph2_rule_ts"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_GRAPH2", "FUT:REPRESENT2",
                     "rule:graph2_leadlag", "TS_OUTRIGHT"),
            stage="CONT_REPR2", note="repaired lead-lag rule, outright"))
        results["wide2_ridge_xs"] = _summ(self._run(
            new_cand("FUT", "ALL_FUT", "FUT_WIDE2", "FUT:REPRESENT2",
                     "ridge", "XS_LONG_SHORT"),
            stage="CONT_REPR2",
            note="live WIDE columns plus REPAIRED latent/graph - a new "
                 "candidate from discovery information, never a redesign "
                 "of the frozen finalist"))
        body = r39.artifact_body("r39_representation_repair/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "defect_found": {
                "what": "the v1 LATENT_PCA and GRAPH_LEAD_LAG features "
                        "exist only ERA-PATCHILY: the generator pivots on "
                        "exact per-market decision dates, so per-window "
                        "column coverage sits at the knife edge of the "
                        "80% trailing-coverage rule - measured EMPTY "
                        "across the entire SELECTION zone (Zone B), "
                        "partially live in Zones A and C",
                "v1_finite_share_by_column_and_zone": dead,
                "consequences": [
                    "the v1 FUT:LATENT and FUT:GRAPH family verdicts on "
                    "Zone B judged filler features (ret_1m, vol_63), not "
                    "the representations",
                    "the confirmed WIDE book carried LIVE latent "
                    "loadings into Zone C (the fixed-coefficient "
                    "decomposition attributes ~53% of its Zone-C "
                    "prediction variance to them) while its Zone-B "
                    "selection evidence contained none - same frozen "
                    "spec, zone-dependent effective information",
                    "the Track-A reconstruction is UNAFFECTED - v1 "
                    "computed the same values, and the frozen generator "
                    "stays byte-identical"],
            },
            "repair": {"alignment": "calendar-month period grid",
                       "coverage_after_repair": coverage,
                       "new_feature_names": names},
            "results": results,
        })
        body["representation_repair_hash"] = r39.sha(body)
        r39.write_json(r39.campaign_dir(self.campaign_id)
                       / "representation_repair.json", body,
                       immutable=False)
        return body

    # ------------------------------------------------------------------ #
    # Phase G - $0 model frontier
    # ------------------------------------------------------------------ #
    def run_models(self) -> dict:
        torch_state = {}
        try:
            MX._torch()
            import torch as _t  # noqa: F401
            torch_state = {"available": True,
                           "version": __import__("torch").__version__,
                           "licence": "BSD-3-Clause",
                           "install": "CPU-only wheel on the research "
                                      "drive; trained from random init; "
                                      "no pretrained weights downloaded"}
        except MX.TorchUnavailable as e:
            torch_state = {"available": False, "reason": str(e)}

        register_sequence_bundle(self.d2)
        results = {}

        def go(key, cand, note=""):
            rep = self._run(cand, stage="CONT_MODELS", note=note)
            results[key] = {"candidate_id": cand["candidate_id"],
                            **_summ(rep)}

        # ridge/lightgbm baselines on the SAME bundles, under the exact v1
        # families so the candidate ids ARE the v1 candidates - re-scoring
        # adds reuse counts, never new distinct trials
        v1_family = {"FUT_CLASSICAL": "FUT:CLASSICAL",
                     "FUT_WIDE": "FUT:WIDE"}
        for bundle in ("FUT_CLASSICAL", "FUT_WIDE"):
            for model in ("ridge", "lightgbm"):
                go("baseline_%s_%s" % (bundle, model),
                   new_cand("FUT", "ALL_FUT", bundle, v1_family[bundle],
                            model, "XS_LONG_SHORT"))
        for bundle in ("FUT_CLASSICAL", "FUT_WIDE"):
            for model in ("mlp", "calibrated_sign", "quantile_blend",
                          "ssl_embed_ridge"):
                if model == "ssl_embed_ridge" and \
                        not torch_state.get("available"):
                    results["%s_%s" % (bundle, model)] = {
                        "state": "TORCH_UNAVAILABLE"}
                    continue
                go("%s_%s" % (bundle, model),
                   new_cand("FUT", "ALL_FUT", bundle, "FUT:MODEL_EXT",
                            model, "XS_LONG_SHORT"))
        go("FUT_CLASSICAL_mlp_ts",
           new_cand("FUT", "ALL_FUT", "FUT_CLASSICAL", "FUT:MODEL_EXT",
                    "mlp", "TS_OUTRIGHT"))
        go("FUT_CLASSICAL_qblend_ts",
           new_cand("FUT", "ALL_FUT", "FUT_CLASSICAL", "FUT:MODEL_EXT",
                    "quantile_blend", "TS_OUTRIGHT"))
        if torch_state.get("available"):
            for model in ("tcn_seq", "gru_seq"):
                for expr in ("XS_LONG_SHORT", "TS_OUTRIGHT"):
                    go("SEQ_%s_%s" % (model, expr.lower()),
                       new_cand("FUT", "ALL_FUT", "SEQ_CLS",
                                "FUT:MODEL_DEEP", model, expr),
                       note="from-scratch sequence net over trailing "
                            "12-decision windows")
        for model in ("mlp",) + (("ssl_embed_ridge",)
                                 if torch_state.get("available") else ()):
            go("EQ_%s" % model,
               new_cand("EQ", "US_SINGLE_NAME", "EQ_PRICE14",
                        "EQ:MODEL_EXT", model, "XS_LONG_SHORT"))

        body = r39.artifact_body("r39_model_frontier_completion/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "torch": torch_state,
            "new_families_executed": dict(MX.EXT_MODEL_FAMILIES),
            "results": results,
            "foundation_model_blockers": CONT.FOUNDATION_MODEL_BLOCKERS,
            "no_third_boosting_implementation": True,
            "packages_installed": [
                {"package": p, "licence": lic} for p, lic in
                CONT.PACKAGES_INSTALLED_FOR_CONTINUATION],
            "may_download_model_weights_still_false":
                CONT.MAY_DOWNLOAD_MODEL_WEIGHTS_STILL_FALSE,
        })
        body["model_frontier_hash"] = r39.sha(body)
        r39.write_json(r39.campaign_dir(self.campaign_id) / MODELS_NAME,
                       body, immutable=False)
        return body

    # ------------------------------------------------------------------ #
    # Registry + pre-gate + ledger + verdict
    # ------------------------------------------------------------------ #
    def finalise(self, *, wide_artifacts: dict) -> dict:
        camp = r39.campaign_dir(self.campaign_id)
        r39.write_json(camp / REGISTRY_NAME, r39.artifact_body(
            "r39_continuation_candidate_registry/1", {
                "campaign_id": self.campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                "rows": self.records, "n_rows": len(self.records),
                "no_silent_deletion_of_failures": True}),
            immutable=False)

        burden = CONT.cumulative_burden(self.campaign_id)
        r39.write_json(camp / LEDGER_NAME, r39.artifact_body(
            "r39_cumulative_search_ledger/1", {
                "campaign_id": self.campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                **burden,
                "no_campaign_id_laundering": CONT.NO_CAMPAIGN_ID_LAUNDERING,
                "dsr_denominator_rule": "the CUMULATIVE distinct count is "
                                        "the deflated-Sharpe trial "
                                        "denominator for every future "
                                        "qualification claim in this "
                                        "release family"}),
            immutable=False)

        ok = [r for r in self.records
              if (r["zone_b"] or {}).get("state") == "OK"
              and r["protocol"] == "ZONE_B"]
        best = max(ok, key=lambda r:
                   (r["zone_b"].get("after_cost_excess_t_stat") or -9.9),
                   default=None)
        best_t = (best["zone_b"].get("after_cost_excess_t_stat")
                  if best else None)
        clears = [r for r in ok
                  if (r["zone_b"].get("after_cost_excess_t_stat") or -9.9)
                  >= CONT.ZONE_C_PREGATE_MIN_ZONE_B_T]
        pregate = r39.artifact_body("r39_zone_c_pregate_decision/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "pregate_min_zone_b_t": CONT.ZONE_C_PREGATE_MIN_ZONE_B_T,
            "declared_before_results": True,
            "n_continuation_zone_b_ok": len(ok),
            "best_zone_b": (best if best is None else {
                "candidate_id": best["candidate_id"],
                "family": best["family"], "model": best["model"],
                "expression": best["expression"],
                "t": best_t}),
            "candidates_clearing_pregate":
                [r["candidate_id"] for r in clears],
            "zone_c_accesses_by_continuation": 0 if not clears else None,
            "decision": ("NO_ZONE_C_ACCESS - no continuation candidate "
                         "clears the declared pre-gate; the v2 lockbox "
                         "budget is preserved and no confirmation is "
                         "burned on arithmetic-certain failures")
            if not clears else "FREEZE_AND_CONFIRM_REQUIRED",
        })
        pregate["pregate_hash"] = r39.sha(pregate)
        r39.write_json(camp / PREGATE_NAME, pregate, immutable=False)

        fac = wide_artifacts.get("factor") or {}
        full = fac.get("full_model") or {}
        wide_res_t = full.get("residual_alpha_t")
        verdict = r39.artifact_body("r39_continuation_verdict/1", {
            "campaign_id": self.campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "verdict": "R39_CONTINUATION_NO_NEW_QUALIFIED_ALPHA"
            if not clears else "R39_CONTINUATION_PREGATE_CLEARED",
            "verdict_is_forced": False,
            "wide_prosecution": {
                "reconstruction_exact": (wide_artifacts.get("recon") or {}
                                         ).get("proofs"),
                "control": "RISK_MATCHED_CASH (narrative corrected; "
                           "computation always correct)",
                "residual_alpha_annualised":
                    full.get("residual_alpha_annualised"),
                "residual_alpha_t": wide_res_t,
                "factor_r_squared": full.get("r_squared"),
                "group_kill_sign_flips":
                    (wide_artifacts.get("kills") or {}).get("n_sign_flips"),
            },
            "track_k_qualification": {
                "rule": "all v1 conditions at the CUMULATIVE trial count "
                        "PLUS positive residual alpha (|t| >= %s) after "
                        "the known-premia factor model"
                        % CONT.RESIDUAL_ALPHA_T_MIN,
                "wide_status": "residual alpha positive and t=%.2f, but "
                               "the DSR gate at the cumulative trial "
                               "count remains failed from v1; the "
                               "candidate stays a NEAR MISS, its status "
                               "is UNCHANGED by diagnostics, and only "
                               "prospective evidence can move it"
                               % (wide_res_t or float("nan")),
                "historical_alpha_candidates": [],
            },
            "cumulative_burden": burden,
            "pregate": {"decision": pregate["decision"],
                        "best_zone_b_t": best_t},
            "safety_tail": {
                "MONEY_SPENT": 0.0, "CLOUD_COMPUTE_SPEND": 0.0,
                "NEW_SUBSCRIPTIONS": 0, "SUBSCRIPTION_CHANGES": 0,
                "TRIALS_STARTED": 0, "OPERATIONAL_WRITES": 0,
                "PORTFOLIO_MUTATIONS": 0, "MODEL_PROMOTIONS": 0,
                "PRODUCTION_RESTARTS": 0},
            "shell_policy": CONT.SHELL_POLICY_AUDIT,
        })
        verdict["continuation_verdict_hash"] = r39.sha(verdict)
        r39.write_json(camp / VERDICT_NAME, verdict, immutable=False)
        return verdict


# --------------------------------------------------------------------------- #
# Feature attachment (Track E loaders onto the frozen state)
# --------------------------------------------------------------------------- #
def attach_information(state: dict, cont: "Continuation",
                       campaign_id: str = CONTINUATION_CAMPAIGN_ID) -> None:
    fut = state["fut"]
    try:
        ny, prov = IE.load_nyfed()
        fut = IE.join_weekly_to_fut(fut, ny, IE.NYFED_FEATURES)
        cont.provenance["nyfed"] = prov
    except Exception:
        cont.provenance["nyfed"] = {"state": "LOAD_FAILED",
                                    "trace": traceback.format_exc()[-600:]}
    try:
        eia, prov = IE.load_eia()
        fut = IE.join_weekly_to_fut(fut, eia, IE.EIA_FEATURES)
        fut["neg_eia_crude_z"] = -fut["eia_crude_z"]
        cont.provenance["eia"] = prov
    except Exception:
        cont.provenance["eia"] = {"state": "LOAD_FAILED",
                                  "trace": traceback.format_exc()[-600:]}
    try:
        agg, prov = IE.load_insider(campaign_id)
        breadth = IE.insider_market_series(agg)
        fut = IE.join_monthly_to_fut(fut, breadth)
        etf = state["etf"]
        if etf is not None and not etf.empty:
            state["etf"] = IE.join_monthly_to_fut(etf, breadth)
        cont.provenance["insider"] = prov
        eq2, stats = IE.insider_name_feature(agg, state["eq"])
        state["eq"] = IE.attach_eq_sector(eq2)
        cont.provenance["insider_identity"] = stats
    except Exception:
        cont.provenance["insider"] = {"state": "LOAD_FAILED",
                                      "trace":
                                          traceback.format_exc()[-600:]}
    try:
        state["eqf"] = IE.attach_eq_sector(IE.load_fundamental_panel())
        cont.provenance["fundamentals"] = {
            "rows": int(len(state["eqf"])),
            "dates": [str(state["eqf"]["decision_date"].min().date()),
                      str(state["eqf"]["decision_date"].max().date())]}
    except Exception:
        state["eqf"] = pd.DataFrame()
        cont.provenance["fundamentals"] = {
            "state": "LOAD_FAILED",
            "trace": traceback.format_exc()[-600:]}
    state["fut"] = fut
    cont.d2.state = state
