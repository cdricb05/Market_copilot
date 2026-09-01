r"""alpha_agent.r53_1.status - the two composite Release 53.1 status
artifacts, composed from the canonical per-track artifacts (never
recomputed):

* ``R53_1_INTRADAY_PREDICTION_STATUS.json`` - the live prospective intraday
  evidence state: what was emitted, at which slots, what matured, what is
  pending, and what the next legal slot is;
* ``R53_1_ALPHA_TO_CAPITAL_STATUS.json`` - the cross-market standings: for
  every serious sleeve, its evidence position, its unit-size position at the
  actual NAV, its risk-budget shadow verdicts, and exactly what still stands
  between it and capital.
"""
from __future__ import annotations

import datetime as _dt

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53_1.status"
ARTIFACT_PREDICTIONS = "R53_1_INTRADAY_PREDICTION_STATUS.json"
ARTIFACT_ALPHA_CAPITAL = "R53_1_ALPHA_TO_CAPITAL_STATUS.json"


def intraday_prediction_status() -> dict:
    from ..r53 import intraday_factory as IF
    preds = IF.predictions()
    outs = IF.outcomes()
    forf = IF.forfeitures()
    by_ch: dict = {}
    for p in preds:
        d = by_ch.setdefault(p["challenger_id"], {"n": 0, "slots": set(),
                                                  "instruments": set()})
        d["n"] += 1
        d["slots"].add(p["slot_utc"])
        d["instruments"].add(p["instrument"])
    now = _dt.datetime.now(_dt.timezone.utc)
    body = artifact_body(
        "r53_1_intraday_prediction_status/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        as_of_utc=now.isoformat().replace("+00:00", "Z"),
        predictions_total=len(preds),
        outcomes_matured=len(outs),
        forfeitures=len(forf),
        pending=len(preds) - len({(o["prediction_id"], str(o["horizon"]))
                                  for o in outs}),
        first_emission_utc=(min(p["emitted_at_utc"] for p in preds)
                           if preds else None),
        by_challenger={k: {"predictions": v["n"],
                           "slots": sorted(v["slots"]),
                           "instruments": sorted(v["instruments"])}
                       for k, v in sorted(by_ch.items())},
        specs_that_refused_this_slot=[
            s["challenger_id"] for s in IF.INTRADAY_SPECS
            if s["challenger_id"] not in by_ch],
        refusal_is_honest=("a spec whose frozen preconditions were unmet "
                           "emits nothing - absence, never a forced row"),
        emission_slots_et=list(IF.EMISSION_SLOTS_ET),
        ledger_integrity=IF.verify()["all_intact"],
        evidence_class="TRUE_FORWARD only",
        **safety_block())
    write_json(research_dir() / ARTIFACT_PREDICTIONS, body)
    return body


def alpha_to_capital_status() -> dict:
    from ..r53 import research_dir as r53_dir
    comp = read_json(r53_dir() / "R53_SHADOW_CAPITAL_COMPETITION.json",
                     default={}) or {}
    micro = read_json(research_dir() / "R53_1_MICRO_CONTRACT_FEASIBILITY.json",
                      default={}) or {}
    rbudget = read_json(research_dir() / "R53_1_RISK_BUDGET_SHADOW.json",
                        default={}) or {}
    body = artifact_body(
        "r53_1_alpha_to_capital_status/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        question="for every serious sleeve: what still stands between its "
                 "alpha and actual capital?",
        evidence_gate="R46 promotion floors (unchanged; PROMOTION_READY=0; "
                      "nearest sleeves are weeks from the forward-evidence "
                      "floor)",
        unit_size_at_actual_nav={
            "nav": micro.get("nav_used"),
            "summary": micro.get("summary"),
            "headline": "&MBT and &MET are executable under the PRODUCTION "
                        "10% cap TODAY; &M2K under a 15% shadow cap; &VX "
                        "under 20%; FX / metals / energy / rates sleeves "
                        "have no owned smaller contract and stay locked at "
                        "this NAV"},
        risk_budget_shadow={
            "policy_summary": rbudget.get("policy_summary"),
            "cases_present": rbudget.get("cases_present"),
            "headline": "&VX correlates -0.71 to the book (best measured "
                        "diversifier, short-only signal today); &MBT at 8% "
                        "weight adds 2.8bp/day portfolio vol with 8.8bp "
                        "absorbed by diversification"},
        score_only_competition_r53={
            "headline": "at actual NAV the R53 competition allocated ZERO "
                        "non-equity capital (unit granularity); at $1M only "
                        "the FX carry long leg entered; and even fully "
                        "eligible, net improvement 0.027 < the 0.05 hurdle "
                        "- diversification is invisible to the hurdle",
            "artifact": "R53_SHADOW_CAPITAL_COMPETITION.json",
            "scenarios": len((comp.get("scenarios") or [])) or None},
        blocking_ladder_per_sleeve={
            "sleeve_equity_index_futures": [
                "evidence floor (~4.8 weeks)",
                "unit size (solved by &M2K under 15% shadow cap)"],
            "sleeve_volatility_futures": [
                "evidence floor (~7.6 weeks)", "short-only signal (long-only "
                "mandate)", "unit size (fits under 20% cap)"],
            "sleeve_crypto_futures": [
                "no approved challenger book on the frontier (R41 BTC "
                "funding-carry accrues in shadow)",
                "unit size SOLVED at production cap (&MBT/&MET)"],
            "sleeve_commodity_futures": [
                "evidence floor", "unit size UNSOLVED (no owned micro; "
                "GLD/SLV proxies are SAME_THESIS_SAME_MARKET)"],
            "sleeve_fx_futures": [
                "evidence floor", "unit size UNSOLVED (no owned micro FX; "
                "FXE/FXY proxies are SAME_THESIS_SAME_MARKET)"],
            "sleeve_rates_futures": [
                "evidence floor", "short-only signal", "unit size UNSOLVED "
                "(IEF/TLT proxies carry modellable basis)"],
        },
        promotion_gates_untouched=True,
        **safety_block())
    write_json(research_dir() / ARTIFACT_ALPHA_CAPITAL, body)
    return body


def write_all() -> dict:
    return {"predictions": intraday_prediction_status(),
            "alpha_capital": alpha_to_capital_status()}
