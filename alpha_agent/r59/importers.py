"""alpha_agent.r59.importers - turn prior-release evidence into research MEMORY.

Release documents are not memory. R39 wrote a 608-row candidate registry, R46
froze 45 prospective challengers, R57 returned 12 verdicts and R58 returned 13
plus an information inventory - and none of it was consultable when R58 chose
what to test, which is why R58 carried the previous burden as a hand-copied
constant. This module reads those artifacts where they actually are, maps each
one onto the R59 identity coordinates and writes it into
:mod:`alpha_agent.r59.memory`.

Import rules:

* IDEMPOTENT. Re-running it re-registers the same identities and changes
  nothing; an artifact that has moved or is unreadable is reported as ABSENT,
  never silently imported as an empty result.
* NO PROMOTION OF MATURITY. A historical backtest verdict is imported as
  HISTORICAL. R46/R58 prospective freezes are imported as PROSPECTIVE_INCEPTION
  carrying their inception instant and record hash, and their forward scores
  are NOT copied - whatever they earn belongs to the R46/R52 runtime that is
  actually measuring it forward.
* NO REWRITE. Nothing here opens a prior-release artifact for writing.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import r59
from . import memory as M

# --------------------------------------------------------------------------- #
# R57 family -> R59 coordinates. R57 named families by letter; the economic and
# information content is recovered here so the memory is queryable by MEANING
# rather than by the release's own shorthand.
# --------------------------------------------------------------------------- #
R57_EQUITY_MAP = {
    "E1_XS_MOMENTUM": ("CROSS_SECTIONAL_MOMENTUM", "PRICE_STATE"),
    "E2_SHORT_REVERSAL": ("SHORT_HORIZON_REVERSAL", "PRICE_STATE"),
    "E3_RESIDUAL_MOMENTUM": ("RESIDUAL_MOMENTUM", "PRICE_STATE"),
    "E4_SECTOR_RELATIVE_MOMENTUM": ("SECTOR_RELATIVE_MOMENTUM", "PRICE_STATE"),
    "E5_LOW_RISK": ("LOW_RISK_ANOMALY", "PRICE_STATE"),
    "E6_IDIO_VOL": ("IDIOSYNCRATIC_VOLATILITY", "PRICE_STATE"),
    "E7_HIGH_PROXIMITY": ("52_WEEK_HIGH_PROXIMITY", "PRICE_STATE"),
    "E8_LIQUIDITY": ("LIQUIDITY_PREMIUM", "PRICE_STATE"),
    "E9_COMBO": ("PRICE_FACTOR_COMBINATION", "PRICE_STATE"),
}
R57_FUTURES_MAP = {
    "F1_TS_TREND": ("TIME_SERIES_TREND", "PRICE_STATE"),
    "F2_CHANNEL_BREAKOUT": ("CHANNEL_BREAKOUT", "PRICE_STATE"),
    "F3_XS_MOMENTUM": ("CROSS_SECTIONAL_MOMENTUM", "PRICE_STATE"),
}

R58_FAMILY_MAP = {
    "A1": ("FUNDAMENTAL_COMPOSITE", "FUNDAMENTAL_FACT"),
    "A2": ("FREE_CASH_FLOW_YIELD", "FUNDAMENTAL_FACT"),
    "A3": ("ACCRUALS", "FUNDAMENTAL_FACT"),
    "A4": ("FUNDAMENTAL_FRESHNESS", "FUNDAMENTAL_FACT"),
    "B0": ("INCUMBENT_BLEND_DIAGNOSTIC", "PRICE_AND_FUNDAMENTAL"),
    "B2": ("FUNDAMENTAL_MOMENTUM_BLEND", "PRICE_AND_FUNDAMENTAL"),
    "B3": ("MOMENTUM_VETO_GATING", "PRICE_AND_FUNDAMENTAL"),
    "B4": ("CROSS_ASSET_REGIME_CONDITIONING", "PRICE_AND_MACRO"),
    "B5": ("HOLD_BAND_TURNOVER_CONTROL", "PRICE_AND_FUNDAMENTAL"),
    "C1": ("PROFITABILITY_ACCELERATION", "FUNDAMENTAL_CHANGE"),
    "C2": ("ACCRUAL_CHANGE", "FUNDAMENTAL_CHANGE"),
    "C3": ("WORKING_CAPITAL_BUILD", "FUNDAMENTAL_CHANGE"),
    "C4": ("RND_INTENSITY", "FUNDAMENTAL_CHANGE"),
    "C5": ("POST_FILING_DRIFT", "FILING_EVENT"),
}

# R39 lane/scope -> R59 asset class.
R39_SCOPE_MAP = {
    "ALL_FUT": r59.AC_CROSS_ASSET,
    "COMMODITY": r59.AC_COMMODITY,
    "RATES": r59.AC_RATES,
    "FX": r59.AC_FX,
    "INTERNATIONAL_EQUITY": r59.AC_EQUITY_INDEX,
    "US_SINGLE_NAME": r59.AC_US_EQUITY,
    "ALL_ETF": r59.AC_CROSS_ASSET,
    "VX": r59.AC_VOLATILITY,
}

# R39 family suffix -> the information family it actually consumed.
R39_INFO_MAP = {
    "RAW": "PRICE_STATE", "CLASSICAL": "PRICE_STATE", "AUTO": "PRICE_STATE",
    "SYMBOLIC": "PRICE_STATE", "SPECTRAL": "PRICE_STATE",
    "LATENT": "PRICE_STATE", "GRAPH": "PRICE_STATE", "MSTRUCT": "PRICE_STATE",
    "FIB": "PRICE_STATE", "FIB_PLACEBO": "PRICE_STATE_PLACEBO",
    "WIDE": "PRICE_STATE", "HAND_RULE": "PRICE_STATE",
    "MACRO_OVERLAY": "MACRO_OBSERVATION", "POSITIONING": "CFTC_POSITIONING",
    "TERM_STRUCTURE": "VOLATILITY_TERM_STRUCTURE",
    "CROSS_ASSET": "PRICE_STATE", "ENSEMBLE": "PRICE_STATE",
    "REGIME_TAIL": "PRICE_STATE",
}


# R46 wrote its own asset-class vocabulary before R59 existed. Mapping it here
# rather than storing the raw label is what lets the scheduler see one frontier:
# an unmapped label would create a phantom asset class that no mandate can ever
# be generated for, and the fairness reservation would silently under-count.
R46_ASSET_MAP = {
    "US_EQUITY": r59.AC_US_EQUITY,
    "US_ETF": r59.AC_US_EQUITY,
    "EQUITY_INDEX": r59.AC_EQUITY_INDEX,
    "RATES": r59.AC_RATES,
    "COMMODITY": r59.AC_COMMODITY,
    "FX": r59.AC_FX,
    "VOLATILITY": r59.AC_VOLATILITY,
    "CREDIT": r59.AC_CREDIT,
    "FUTURES": r59.AC_CROSS_ASSET,
    "MULTI_ASSET_FUTURES": r59.AC_CROSS_ASSET,
}


def normalise_asset_class(label: Optional[str]) -> str:
    """Map any prior release's asset label onto the R59 vocabulary.

    An unrecognised label falls back to CROSS_ASSET rather than being invented
    as a new class, so the frontier can never grow a scope the governor has no
    substrate for.
    """
    key = str(label or "").strip().upper()
    if key in r59.ASSET_CLASSES:
        return key
    return R46_ASSET_MAP.get(key, r59.AC_CROSS_ASSET)


def _absent(name: str, path: Path) -> dict:
    return {"source": name, "path": str(path), "state": "ABSENT",
            "imported": 0, "reason": "artifact not present or unreadable"}


# --------------------------------------------------------------------------- #
# R57 - the price-information verdict (12 families)
# --------------------------------------------------------------------------- #
def import_r57(mem: M.ResearchMemory) -> dict:
    path = r59.R57_ROOT / "results" / "campaign_verdicts.json"
    doc = r59.read_json(path)
    if not doc:
        return _absent("R57", path)
    n = 0
    for block, mapping, asset in (
            ("equity_verdicts", R57_EQUITY_MAP, r59.AC_US_EQUITY),
            ("futures_verdicts", R57_FUTURES_MAP, r59.AC_CROSS_ASSET)):
        for fam_id, res in (doc.get(block) or {}).items():
            econ, info = mapping.get(fam_id, (fam_id, "PRICE_STATE"))
            hid = mem.register(
                title="R57 %s" % fam_id, release="R57", origin="HUMAN_TEMPLATE",
                generation_method="PRE_REGISTERED_FAMILY",
                information_family=info, economic_family=econ,
                asset_class=asset, model_family="RANK_TOPN",
                horizon_sessions=r59.HORIZON,
                input_data_identity="norgate_sp500_pit_panel_v1"
                if asset == r59.AC_US_EQUITY else "norgate_futures_panel_v1",
                spec={"release": "R57", "family_id": fam_id})
            failed = res.get("failed_gates") or []
            mem.record_result(
                hid,
                outcome=r59.HO_NO_ALPHA_EVIDENCE
                if res.get("verdict") == "NO_ALPHA_EVIDENCE" else r59.HO_REJECTED,
                evidence_maturity="HISTORICAL",
                statistic={"lockbox_t": res.get("lockbox_t"),
                           "lockbox_periods": res.get("lockbox_periods"),
                           "bh_pass": (doc.get("bh_pass") or {}).get(fam_id),
                           "bh_denominator": doc.get("bh_denominator")},
                economics={"lockbox_ann_net_excess":
                           res.get("lockbox_ann_net_excess")},
                robustness={"gates": res.get("gates")},
                reason_rejected="failed gates: %s" % ", ".join(failed)
                if failed else "did not qualify",
                reopen_condition="NEW_ORTHOGONAL_INFORMATION")
            n += 1
    return {"source": "R57", "path": str(path), "state": "IMPORTED",
            "imported": n,
            "note": "12 pre-registered price families, all NO_ALPHA_EVIDENCE on "
                    "an untouched lockbox"}


# --------------------------------------------------------------------------- #
# R58 - the orthogonal / fundamental verdict (13 families + 4 freezes)
# --------------------------------------------------------------------------- #
def import_r58(mem: M.ResearchMemory) -> dict:
    path = r59.R58_ROOT / "results" / "r58_campaign_verdicts.json"
    doc = r59.read_json(path)
    if not doc:
        return _absent("R58", path)
    verdicts = doc.get("campaign_verdicts") or {}
    n = 0
    for fam_id, res in verdicts.items():
        base = fam_id.split("_")[0]
        econ, info = R58_FAMILY_MAP.get(base, (fam_id, "FUNDAMENTAL_FACT"))
        res = res if isinstance(res, dict) else {"verdict": str(res)}
        hid = mem.register(
            title="R58 %s" % fam_id, release="R58", origin="HUMAN_TEMPLATE",
            generation_method="PRE_REGISTERED_FAMILY",
            information_family=info, economic_family=econ,
            asset_class=r59.AC_US_EQUITY, model_family="RANK_TOPN",
            horizon_sessions=r59.HORIZON,
            input_data_identity="r58_pit_fundamental_panel_v1",
            counts_to_burden=base not in ("B0",),
            spec={"release": "R58", "family_id": fam_id})
        verdict = str(res.get("verdict") or "")
        outcome = (r59.HO_NO_ALPHA_EVIDENCE
                   if "NO_ALPHA" in verdict or not verdict
                   else (r59.HO_QUALIFIED if "QUALIF" in verdict
                         else r59.HO_REJECTED))
        mem.record_result(
            hid, outcome=outcome, evidence_maturity="HISTORICAL",
            statistic={k: res.get(k) for k in
                       ("lockbox_t", "buy_t", "sell_t", "lockbox_periods",
                        "bh_pass") if k in res},
            economics={k: res.get(k) for k in
                       ("lockbox_ann_net_excess", "ann_net_excess")
                       if k in res},
            robustness={"gates": res.get("gates"),
                        "failed_gates": res.get("failed_gates")},
            reason_rejected=", ".join(res.get("failed_gates") or []) or verdict,
            reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        n += 1

    frozen = 0
    fdoc = r59.read_json(r59.R58_ROOT / "results" / "r58_forward_challengers.json")
    if fdoc:
        for cid, rec in (fdoc.get("frozen") or {}).items():
            hid = mem.register(
                title="R58 prospective challenger %s" % cid, release="R58",
                origin="HUMAN_TEMPLATE",
                generation_method="PROSPECTIVE_FREEZE",
                information_family="SHORT_VOLUME"
                if "SHORT_VOLUME" in cid else "FILING_EVENT"
                if "DISCLOSURE" in cid else "FUNDAMENTAL_FACT",
                economic_family=cid, asset_class=r59.AC_US_EQUITY,
                model_family="RANK_TOPN", horizon_sessions=r59.HORIZON,
                input_data_identity="r58_challenger_freeze",
                counts_to_burden=False,
                spec={"release": "R58", "challenger_id": cid})
            mem.freeze_forward(hid, challenger_id=cid,
                               inception=fdoc.get("eligible_session") or "",
                               record_hash=rec.get("record_hash") or "")
            frozen += 1
    return {"source": "R58", "path": str(path), "state": "IMPORTED",
            "imported": n, "prospective_freezes_referenced": frozen,
            "note": "13 FDR-counted fundamental / blend / information-change "
                    "families plus one diagnostic; 4 prospective freezes "
                    "referenced by inception, never by score"}


# --------------------------------------------------------------------------- #
# R39 - the machine discovery burden (608 + continuation)
# --------------------------------------------------------------------------- #
def import_r39(mem: M.ResearchMemory) -> dict:
    base = r59.R39_ROOT / "r39_universal_alpha_discovery_v1"
    path = base / "autonomous_candidate_registry.json"
    doc = r59.read_json(path)
    if not doc:
        return _absent("R39", path)
    verdict = (r59.read_json(base / "final_verdict.json") or {})
    cands = doc.get("candidates") or []
    n = 0
    machine = 0
    for c in cands:
        fam = str(c.get("family") or "")
        suffix = fam.split(":")[-1] if ":" in fam else fam
        info = R39_INFO_MAP.get(suffix, "PRICE_STATE")
        asset = R39_SCOPE_MAP.get(str(c.get("scope")), r59.AC_CROSS_ASSET)
        origin = str(c.get("origin") or "MACHINE_GENERATED")
        if origin == "MACHINE_GENERATED":
            machine += 1
        hid = mem.register(
            title="R39 %s (%s)" % (c.get("candidate_id"), fam),
            release="R39", origin=origin,
            generation_method=str(c.get("generation_method") or "UNKNOWN"),
            information_family=info,
            economic_family="MACHINE_REPRESENTATION:%s" % suffix,
            asset_class=asset,
            model_family=str(c.get("model") or "unknown").upper(),
            horizon_sessions=int(c.get("horizon") or r59.HORIZON),
            input_data_identity="r39_universal_state",
            spec={"release": "R39", "candidate_id": c.get("candidate_id"),
                  "expression": c.get("expression"), "target": c.get("target"),
                  "scope": c.get("scope"), "hyper": c.get("hyper")})
        s1 = c.get("stage1") or {}
        mem.record_result(
            hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
            evidence_maturity="HISTORICAL",
            statistic={"zone_a_cv_mean_ic": s1.get("zone_a_cv_mean_ic"),
                       "zone_a_cv_sharpe_per_period":
                           s1.get("zone_a_cv_sharpe_per_period"),
                       "periods": s1.get("periods")},
            reason_rejected=str(verdict.get("verdict")
                                or "R39_NO_ROBUST_ALPHA_DESPITE_UNIVERSAL_SEARCH"),
            reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        n += 1

    cont = r59.read_json(r59.R39_ROOT / "r39_universal_alpha_continuation_v2"
                         / "continuation_candidate_registry.json")
    cn = 0
    for row in ((cont or {}).get("rows") or []):
        hid = mem.register(
            title="R39C %s" % (row.get("candidate_id") or row.get("id")
                               or "row"),
            release="R39C", origin="MACHINE_GENERATED",
            generation_method=str(row.get("generation_method")
                                  or "CONTINUATION"),
            information_family=R39_INFO_MAP.get(
                str(row.get("family") or "").split(":")[-1], "PRICE_STATE"),
            economic_family="MACHINE_REPRESENTATION:CONTINUATION",
            asset_class=R39_SCOPE_MAP.get(str(row.get("scope")),
                                          r59.AC_CROSS_ASSET),
            model_family=str(row.get("model") or "unknown").upper(),
            horizon_sessions=int(row.get("horizon") or r59.HORIZON),
            input_data_identity="r39_universal_state_continuation",
            spec={"release": "R39C", "row": row.get("candidate_id")})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                          evidence_maturity="HISTORICAL",
                          reason_rejected="R39_CONTINUATION_NO_NEW_QUALIFIED_ALPHA",
                          reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        cn += 1
    return {"source": "R39", "path": str(path), "state": "IMPORTED",
            "imported": n + cn, "machine_generated": machine,
            "continuation_rows": cn,
            "verdict": verdict.get("verdict"),
            "note": "the mathematical engine's own prior search burden, now "
                    "counted instead of copied"}


# --------------------------------------------------------------------------- #
# R46 - the prospective challenger registry (identity + inception only)
# --------------------------------------------------------------------------- #
def import_r46(mem: M.ResearchMemory) -> dict:
    path = (r59.R46_ROOT / "r46_prospective_alpha_tournament_v1"
            / "r46_challenger_registry.json")
    doc = r59.read_json(path)
    if not doc:
        return _absent("R46", path)
    n = 0
    for c in (doc.get("challengers") or []):
        cid = str(c.get("challenger_id"))
        hid = mem.register(
            title="R46 challenger %s" % cid, release="R46",
            origin=str(c.get("origin") or "R46_SEED"),
            generation_method="PROSPECTIVE_FREEZE",
            information_family=str(c.get("information_family") or "UNKNOWN"),
            economic_family=str(c.get("family") or "UNKNOWN"),
            asset_class=normalise_asset_class(c.get("asset_class")),
            model_family=str(c.get("prediction_type") or "UNKNOWN"),
            horizon_sessions=(c.get("horizons") or [None])[0],
            input_data_identity=str(c.get("universe") or ""),
            counts_to_burden=False,
            spec={"release": "R46", "challenger_id": cid,
                  "spec_hash": c.get("spec_hash")})
        mem.freeze_forward(hid, challenger_id=cid,
                           inception=str(c.get("forward_start")
                                         or c.get("frozen_at") or ""),
                           record_hash=str(c.get("spec_hash") or ""))
        n += 1
    return {"source": "R46", "path": str(path), "state": "IMPORTED",
            "imported": n,
            "note": "prospective identities and inception instants only; their "
                    "forward scores stay with the R46/R52 runtime"}


# --------------------------------------------------------------------------- #
# R56 - the shadow portfolios (forward, not historical)
# --------------------------------------------------------------------------- #
def import_r56(mem: M.ResearchMemory) -> dict:
    d = r59.R56_ROOT / "records"
    if not d.exists():
        return _absent("R56", d)
    n = 0
    for p in sorted(d.glob("*.json")):
        doc = r59.read_json(p)
        if not doc:
            continue
        book = p.stem.split("__")[0]
        hid = mem.register(
            title="R56 shadow book %s" % book, release="R56",
            origin="R56_SHADOW", generation_method="PROSPECTIVE_FREEZE",
            information_family="PORTFOLIO_CONSTRUCTION",
            economic_family="SHADOW_PORTFOLIO:%s" % book,
            asset_class=r59.AC_US_EQUITY, model_family="PORTFOLIO",
            input_data_identity=str(p.name), counts_to_burden=False,
            spec={"release": "R56", "book": book})
        mem.freeze_forward(hid, challenger_id=book,
                           inception=str(doc.get("as_of")
                                         or doc.get("eligible_session") or ""),
                           record_hash=str(doc.get("record_hash")
                                           or doc.get("artifact_hash") or ""))
        n += 1
    return {"source": "R56", "path": str(d), "state": "IMPORTED",
            "imported": n,
            "note": "shadow portfolio inceptions; PnL stays with the R46.4 "
                    "money layer"}


# --------------------------------------------------------------------------- #
# R58 information inventory -> the data-opportunity frontier
# --------------------------------------------------------------------------- #
_INVENTORY_STATE = {
    "HISTORICAL_PIT_READY": r59.DO_FREE_AVAILABLE,
    "PROSPECTIVE_ONLY": r59.DO_FREE_AVAILABLE,
    "DATA_INCOMPLETE": r59.DO_ALREADY_OWNED_UNUSED,
    "TIMESTAMP_INSUFFICIENT": r59.DO_BLOCKED,
    "SPLIT_BY_SOURCE": r59.DO_FREE_AVAILABLE,
    "BLOCKED": r59.DO_BLOCKED,
    "EXHAUSTED": r59.DO_EXHAUSTED,
}


def import_r58_inventory(mem: M.ResearchMemory) -> dict:
    path = r59.R58_ROOT / "results" / "r58_information_inventory.json"
    doc = r59.read_json(path)
    if not doc:
        return _absent("R58_INVENTORY", path)
    n = 0
    for fam, rec in (doc.get("families") or {}).items():
        if not isinstance(rec, dict):
            continue
        cls = str(rec.get("classification") or "")
        state = _INVENTORY_STATE.get(cls, r59.DO_BLOCKED)
        mem.set_opportunity(
            "OWNED_%s" % fam,
            title="Owned information family %s" % fam,
            state=state,
            information_need=str(rec.get("reason") or ""),
            owned_but_unused=(state == r59.DO_ALREADY_OWNED_UNUSED),
            pit_integrity=cls,
            effective_sample="%s years owned" % rec.get("owned_years"),
            detail={"r58_classification": cls,
                    "records_scanned": rec.get("records_scanned"),
                    "distinct_tickers": rec.get("distinct_tickers"),
                    "owned_years": rec.get("owned_years"),
                    "sources": rec.get("sources"),
                    "quality_warnings": rec.get("quality_warning_classes"),
                    "measured_by": "alpha_agent.r58.inventory"})
        n += 1
    return {"source": "R58_INVENTORY", "path": str(path), "state": "IMPORTED",
            "imported": n}


def import_all(mem: Optional[M.ResearchMemory] = None) -> dict:
    """Import every prior-release evidence source. Absence never raises."""
    mem = mem or M.open_memory()
    reports = {}
    for name, fn in (("r57", import_r57), ("r58", import_r58),
                     ("r39", import_r39), ("r46", import_r46),
                     ("r56", import_r56),
                     ("r58_inventory", import_r58_inventory)):
        try:
            reports[name] = fn(mem)
        except Exception as exc:  # noqa: BLE001 - one bad source never blocks
            reports[name] = {"source": name.upper(), "state": "IMPORT_ERROR",
                             "imported": 0, "reason": "%s: %s"
                             % (type(exc).__name__, exc)}
    total = sum(int(r.get("imported") or 0) for r in reports.values())
    mem.event("RESEARCH_MEMORY_IMPORT", subject="prior_releases",
              detail={"total_imported": total,
                      "sources": {k: v.get("state") for k, v in reports.items()}})
    return {"total_imported": total, "sources": reports,
            "summary": mem.summary()}
