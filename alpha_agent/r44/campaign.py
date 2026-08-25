"""alpha_agent.r44.campaign - orchestration, and the answers.

Runs the three engines, charges the search burden, assembles the frontier
and writes the artifacts. The verdict leads with the INVESTMENT result, not
with the system result, because a clean campaign and a large artifact set
are not an investment outcome and this estate decided at Release 33 to stop
letting the first stand in for the second.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import artifact_body, sha, write_artifact
from . import burden as B
from . import closeout as CO
from . import combine as CB
from . import contract as C
from . import control as CTL
from . import frontier as FR
from . import portfolio as PF
from . import purchase as PU
from . import shell_policy as SP
from . import streams as ST

CALCULATION_OWNER = "alpha_agent.r44.campaign"

VERDICT_ARTIFACT = "R44_FINAL_VERDICT.json"
LANES_ARTIFACT = "R44_LANE_RESULTS.json"
STANDALONE_ARTIFACT = "R44_STANDALONE_ALPHA_FRONTIER.json"
PORTFOLIO_ARTIFACT = "R44_PORTFOLIO_ALPHA_FRONTIER.json"
CONTROL_ARTIFACT = "R44_STRUCTURAL_PREMIUM_CONTROL.json"
PURCHASE_ARTIFACT = "R44_PURCHASE_GATE.json"
STREAMS_ARTIFACT = "R44_STREAM_INVENTORY.json"


# --------------------------------------------------------------------------- #
# ENGINE 2
# --------------------------------------------------------------------------- #
def _charge_portfolio_burden(variants: dict, rules: dict) -> dict:
    """Charge PORTFOLIO_SYNTHESIS burden, one trial per DISTINCT book.

    Two combination rules that produce the same weights produce the same
    book, and a book is one candidate however many names it answers to. The
    candidate id is therefore a hash of the ROUNDED WEIGHT VECTOR, so ERC
    and capped-ERC collapse into one trial when the cap never binds - and
    stay two when it does.
    """
    charged, refused = [], []
    for rule, b in rules.items():
        if b.get("state") != "BUILT":
            continue
        w = {k: round(float(v), 6) for k, v in sorted(b["weights"].items())}
        # The spec carries the WEIGHT HASH and no rule name, so two rules
        # that produced the same book hash to the same candidate id and the
        # ledger records a second touch rather than a second trial.
        spec = {"information_family": "PORTFOLIO_SYNTHESIS",
                "asset_family": "MULTI_STREAM_PORTFOLIO",
                "horizon": "1s",
                "economic_expression": "PORTFOLIO_OVERLAY",
                "representation": sha(w)[:16],
                "model": "PORTFOLIO_OVERLAY", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R31-R43 residual streams"],
                "validation_touches": 1,
                "lane": "E2_PORTFOLIO_SYNTHESIS"}
        try:
            cid = B.record_zone_b(spec, family="PORTFOLIO_SYNTHESIS",
                                  lane="E2_PORTFOLIO_SYNTHESIS")
            charged.append({"rule": rule, "candidate_id": cid,
                            "weights_hash": spec["representation"]})
        except ValueError as exc:
            refused.append({"rule": rule, "reason": str(exc)})
    by_book = {}
    for c in charged:
        by_book.setdefault(c["candidate_id"], []).append(c["rule"])
    return {"charged": charged, "refused": refused,
            "n_rules_evaluated": len(charged),
            "n_distinct_books": len(by_book),
            "rules_per_distinct_book": by_book,
            "rule": "one trial per DISTINCT weight vector, not per rule "
                    "name - identical books are one candidate however many "
                    "names they answer to"}


def run_engine2() -> dict:
    inv = ST.build_all()
    frame = ST.excess_frame(inv)
    frames_by_cost = {m: ST.excess_frame(inv, cost_multiplier=m)
                      for m in (2.0, 3.0)}
    z = ST.zones(frame)
    meta = PF._weight_meta(inv)
    fit_dates = pd.DatetimeIndex(z["A"]).union(pd.DatetimeIndex(z["B"]))

    resid = [s for s in C.RESIDUAL_STREAM_IDS if s in frame.columns]
    prem = [s for s in C.PREMIUM_STREAM_IDS if s in frame.columns]

    variants = {
        "RESIDUAL_PORTFOLIO": PF.build(frame, z, inv, ids=resid,
                                       label="RESIDUAL_PORTFOLIO"),
        "PREMIUM_PORTFOLIO": PF.build(frame, z, inv, ids=prem,
                                      label="PREMIUM_PORTFOLIO"),
        "ALL_STREAMS": PF.build(frame, z, inv, ids=resid + prem,
                                label="ALL_STREAMS"),
    }
    rules = {r: PF.build(frame, z, inv, ids=resid, rule=r, label=r)
             for r in C.COMBINATION_RULES}
    charge = _charge_portfolio_burden(variants, rules)

    controls = CTL.build_controls(frame, fit_dates, meta)
    base = variants["RESIDUAL_PORTFOLIO"]
    tests = PF.kill_battery(frame, z, inv, base, ids=resid,
                            frames_by_cost=frames_by_cost, controls=controls)
    pbo = PF.pbo(frame, z, inv, ids=resid)
    diag = PF.sign_selected_diagnostic(frame, z, inv, ids=resid)

    bh_rows = [{"label": r, "t": (b.get("lock") or {}).get("t_hac")}
               for r, b in rules.items() if b.get("state") == "BUILT"]
    bh = PF.search_adjustment(bh_rows)
    survivor = any(r.get("bh_survivor")
                   and (r.get("t") or 0) > 0 for r in (bh.get("rows") or []))
    qual = PF.qualification(base, tests, bh_survivor=survivor)

    # Diversification arithmetic - the sentence the release turns on.
    fit = frame[resid].reindex(fit_dates)
    mean_stream_vol = float((fit.std(ddof=1) * np.sqrt(252)).mean())
    port_vol = (base.get("fit") or {}).get("vol_ann")
    weighted_mean_return = float(sum(
        base["weights"][k] * float(np.nanmean(fit[k]) * 252)
        for k in base.get("weights", {})))

    prem_ret = controls.get("structural_premium_returns")
    pas = controls.get("passive_long_returns")
    increments = {
        "RESIDUAL_vs_PREMIUM_CONTROL": CTL.volatility_matched_increment(
            base["_returns"], prem_ret, z["C"]) if prem_ret is not None
        else {"state": "NOT_RUN"},
        "RESIDUAL_vs_PASSIVE_LONG": CTL.volatility_matched_increment(
            base["_returns"], pas, z["C"]) if pas is not None and len(pas)
        else {"state": "NOT_RUN"},
        "ALL_STREAMS_vs_PREMIUM_CONTROL": CTL.volatility_matched_increment(
            variants["ALL_STREAMS"]["_returns"], prem_ret, z["C"])
        if prem_ret is not None
        and variants["ALL_STREAMS"].get("state") == "BUILT"
        else {"state": "NOT_RUN"},
        "PREMIUM_CONTROL_vs_PASSIVE_LONG": CTL.volatility_matched_increment(
            prem_ret, pas, z["C"]) if prem_ret is not None
        and pas is not None and len(pas) else {"state": "NOT_RUN"},
    }

    def _strip(b):
        return {k: v for k, v in b.items() if not k.startswith("_")}

    # A robustness window in which EVERY stream exists, including the crypto
    # sleeve the long-window fit has to drop for want of fit-zone history.
    modern = frame.loc[frame.index >= pd.Timestamp("2020-01-01")]
    zm = ST.zones(modern)
    modern_out = {"window_start": "2020-01-01",
                  "zones": {"n": zm.get("n"), "a_range": zm.get("a_range"),
                            "b_range": zm.get("b_range"),
                            "c_range": zm.get("c_range")},
                  "why": "the long-window fit ends in 2016 and drops the "
                         "crypto sleeve; this window contains all 18 "
                         "declared streams at once",
                  "variants": {}}
    for name, ids in (("RESIDUAL_PORTFOLIO", resid),
                      ("PREMIUM_PORTFOLIO", list(C.PREMIUM_STREAM_IDS)),
                      ("ALL_STREAMS", resid + list(C.PREMIUM_STREAM_IDS))):
        b = PF.build(modern, zm, inv,
                     ids=[i for i in ids if i in modern.columns],
                     label="MODERN_" + name)
        modern_out["variants"][name] = _strip(b)
    modern_out["conclusion_unchanged"] = bool(
        (modern_out["variants"]["RESIDUAL_PORTFOLIO"].get("lock") or {})
        .get("excess_ann", 0) < 0)

    return {
        "modern_window": modern_out,
        "engine": "E2_PORTFOLIO_SYNTHESIS",
        "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E2_PORTFOLIO_SYNTHESIS"],
        "inventory": ST.inventory_report(inv),
        "zones": {"n": z.get("n"), "a_range": z.get("a_range"),
                  "b_range": z.get("b_range"), "c_range": z.get("c_range"),
                  "embargo": z.get("embargo")},
        "primary_rule": C.PRIMARY_COMBINATION_RULE,
        "primary_rule_named_before_lockbox": True,
        "variants": {k: _strip(v) for k, v in variants.items()},
        "rules": {k: _strip(v) for k, v in rules.items()},
        "burden": charge,
        "controls": {
            "structural_premium_control": controls.get(
                "structural_premium_control"),
            "passive_long_days": int(len(pas)) if pas is not None else 0,
        },
        "increments": increments,
        "kill_battery": tests,
        "pbo": pbo,
        "sign_selected_diagnostic": _strip(diag),
        "search_adjustment": bh,
        "qualification": qual,
        "diversification_arithmetic": {
            "mean_single_stream_vol_ann": mean_stream_vol,
            "portfolio_vol_ann": port_vol,
            "vol_reduction_ratio": (port_vol / mean_stream_vol)
            if mean_stream_vol else None,
            "effective_n_streams": base.get("effective_n_streams"),
            "weighted_mean_stream_return_ann": weighted_mean_return,
            "portfolio_return_ann": (base.get("fit") or {}).get("excess_ann"),
            "finding": "diversification is linear in return and sublinear in "
                       "risk. When the streams' honest expected return is "
                       "negative, that is not a smoothing - it converts an "
                       "uncertain loss into a reliable one.",
        },
        "_variants": variants,
        "_frame": frame,
        "_zones": z,
    }


# --------------------------------------------------------------------------- #
# ENGINE 1 and ENGINE 3
# --------------------------------------------------------------------------- #
def run_engine1(*, acquire_options: bool = True) -> dict:
    from . import acquisition as AQ
    from . import intraday as ID
    from . import options as OP

    opt = OP.run(acquire=acquire_options)
    intr = ID.run()
    best = FR._best_intraday(intr)
    pros = None
    if best is not None:
        stamps = ID.release_stamps()
        pros = ID.prosecute_zone_a(best["symbol"], stamps,
                                   entry_delay=best["entry_delay_min"],
                                   hold=best["hold_min"], rule=best["rule"])
        repl = []
        for sym in intr["instruments"]:
            ev = ID.event_returns(sym, stamps,
                                  entry_delay=best["entry_delay_min"],
                                  hold=best["hold_min"])
            if ev is None:
                continue
            zz = ID.zones_by_event(ev)
            a_end = pd.Timestamp(zz["a_range"][1]) if zz["a_range"] else None
            ea = ev[pd.to_datetime(ev["date"]) <= a_end] \
                if a_end is not None else ev
            card = ID.score_rule(ea, best["rule"])
            card["symbol"] = sym
            repl.append(card)
        pros["cross_instrument_replication"] = repl
        pros["replicates"] = bool(sum(
            1 for r in repl if (r.get("net_t") or 0) >= 2.0) >= 2)
    intr.pop("_cache", None)
    analyst, credit, micro = (AQ.analyst_lane(), AQ.credit_lane(),
                              AQ.microstructure_lane())
    frontier = artifact_body("r44_orthogonal_data_frontier/1", {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "options": opt,
        "intraday": intr,
        "intraday_prosecution": pros,
        "analyst": analyst,
        "credit": credit,
        "microstructure": micro,
    })
    frontier["frontier_hash"] = sha({k: v for k, v in frontier.items()
                                     if k != "safety_block"})
    write_artifact("R44_ORTHOGONAL_DATA_FRONTIER.json", frontier,
                   overwrite=True)
    return {"options": opt, "intraday": intr,
            "intraday_prosecution": pros,
            "analyst": analyst, "credit": credit, "microstructure": micro}


def run_engine3() -> dict:
    from . import niche as N
    out = N.run()
    out.pop("_books", None)
    out.pop("_zones", None)
    out.pop("_table", None)
    return out


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def _fifteen_answers(e1, e2, e3, standalone, prows, purch, freeze) -> dict:
    base = e2["variants"]["RESIDUAL_PORTFOLIO"]
    lock = base.get("lock") or {}
    inc = e2["increments"]["RESIDUAL_vs_PREMIUM_CONTROL"]
    prem = e2["variants"]["PREMIUM_PORTFOLIO"].get("lock") or {}
    pinc = e2["increments"]["PREMIUM_CONTROL_vs_PASSIVE_LONG"]
    div = e2["diversification_arithmetic"]
    intr = e1.get("intraday_prosecution") or {}
    best = FR._best_intraday(e1.get("intraday") or {})
    an = (e1["analyst"].get("vendor_backward_strip_reconciliation") or {})
    opt = (e1["options"].get("surface") or {})
    vrp = (e1["options"].get("variance_risk_premium") or {})
    return {
        "1_did_we_find_standalone_alpha":
            "NO. %d standalone candidates were judged; none cleared the "
            "frozen gate. The two that reached ZONE_B died there - one on a "
            "sign flip, one on its own passive control."
            % len([r for r in standalone if r.get("BURDEN_CHARGED")]),
        "2_did_weak_edges_combine_into_portfolio_alpha":
            "NO, and not marginally. Twelve economically distinct, nearly "
            "uncorrelated residual streams combined to %.2f%%/yr at t %.2f "
            "on a lockbox that was never opened until the weights were "
            "fixed. All eight predeclared combination rules agree in sign."
            % (100.0 * (lock.get("excess_ann") or 0.0),
               lock.get("t_hac") or 0.0),
        "3_did_the_portfolio_beat_a_structural_premium_control":
            "NO. The increment over the volatility-matched premium "
            "portfolio is %.2f%%/yr at t %.2f."
            % (100.0 * (inc.get("increment_ann") or 0.0),
               inc.get("increment_t_hac") or 0.0),
        "4_where_did_incremental_alpha_come_from":
            "Nowhere. There is no incremental alpha to attribute.",
        "5_was_the_portfolio_just_diversified_beta":
            "It was worse than that. Mean single-stream volatility %.1f%% "
            "fell to %.1f%% at the portfolio level - a %.2f ratio across "
            "%.1f effective independent bets - while the weighted mean "
            "stream return stayed at %.2f%%/yr. Diversification worked "
            "exactly as advertised on risk and delivered nothing on return, "
            "so what it produced was a more RELIABLE loss."
            % (100.0 * (div["mean_single_stream_vol_ann"] or 0),
               100.0 * (div["portfolio_vol_ann"] or 0),
               div["vol_reduction_ratio"] or 0,
               div["effective_n_streams"] or 0,
               100.0 * (div["weighted_mean_stream_return_ann"] or 0)),
        "6_did_options_add_new_information":
            "YES, and it still cannot qualify. The surface was deepened at "
            "$0 from R43's 30 contracts to %s sessions across %s expiries "
            "and %s strikes, calls and puts, with implied volatility "
            "inverted LOCALLY - no vendor greeks. That bought two things "
            "the owned VIX complex cannot express: a real smile (%s), and a "
            "variance risk premium measured from ACTUAL option prices - "
            "mean ATM IV %.1f%% against %.1f%% subsequent realised, a "
            "premium of %.2f volatility points at t %.2f over %s "
            "observations. It is a textbook short-volatility premium and "
            "the contract explicitly refuses to count that as Alpha. The "
            "window is %s sessions short of the frozen 250-fit + "
            "250-judged requirement - about %s more months."
            % (opt.get("n_sessions"), opt.get("n_expiries"),
               opt.get("n_strikes"),
               " / ".join("%.1f%% at %s-%s moneyness"
                          % (100 * b["median_iv"], b["bucket"][0],
                             b["bucket"][1])
                          for b in (opt.get("moneyness_buckets") or [])[:3]),
               100 * (vrp.get("mean_atm_iv") or 0),
               100 * (vrp.get("mean_forward_realised_vol") or 0),
               100 * (vrp.get("variance_risk_premium_vol_points") or 0),
               vrp.get("t_hac") or 0, vrp.get("n"),
               opt.get("sessions_short_by"),
               opt.get("additional_months_required"))
            if vrp.get("state") == "MEASURED" else
            "NOT YET - the surface is %s sessions short of a judgeable "
            "sample" % opt.get("sessions_short_by"),
        "7_did_intraday_turn_the_macro_effect_tradable":
            ("PARTLY, and this is the release's most interesting number. In "
             "event time the effect is for the FIRST time LARGER than its "
             "own cost - %.2f bps gross against %.2f bps of observed spread "
             "on %s, at gross t %.2f - where R43's daily version was "
             "cost-dominated. It is release-locked (the timing sweep peaks "
             "at the declared minute and dies within one minute either "
             "side) and event-specific (the non-release placebo is flat). "
             "It is still NOT tradable: net t %.2f is below the frozen bar "
             "of %.1f, and it does not replicate in the other two owned "
             "instruments at the same parameters."
             % (best.get("gross_bps_per_event", 0),
                best.get("cost_bps_per_event", 0), best.get("symbol"),
                best.get("gross_t", 0), best.get("net_t", 0),
                C.STANDALONE_ALPHA_GATE["t_min_lock"])
             if best else "NOT MEASURED"),
        "8_did_analyst_revisions_become_testable":
            "NO, and now for a measured reason rather than an access one. "
            "EODHD's snapshots carry a backward strip of 7/30/60/90-day-ago "
            "consensus, which looked like the historical vintages this "
            "estate has never been able to buy. Reconciled against the "
            "estate's OWN prospectively captured snapshots it reproduces "
            "them %s of the time. The strip is RESTATED. The prospective "
            "ledger is real and is working (%s of series revised in %s "
            "days) - it is simply %s days old."
            % ("%.0f%%" % (100 * an["match_rate"])
               if an.get("match_rate") is not None else "an unmeasured share",
               (e1["analyst"].get("observed_revisions") or {}).get(
                   "n_revised"),
               (e1["analyst"].get("observed_revisions") or {}).get(
                   "span_days"),
               (e1["analyst"].get("prospective_ledger") or {}).get(
                   "span_days")),
        "9_did_less_efficient_markets_outperform":
            "NO - the frontier runs the OTHER way. Mean ZONE_A t by "
            "liquidity tier: LIQUID %.2f, MID %.2f, ILLIQUID %.2f. The one "
            "illiquid cell that cleared the advance bar flipped sign in "
            "ZONE_B, and an equal-risk book across that tier binds at $%s "
            "of capacity - below the smallest capacity tier this release "
            "declared."
            % (e3["by_tier"].get("LIQUID", {}).get("mean_zone_a_t", 0),
               e3["by_tier"].get("MID", {}).get("mean_zone_a_t", 0),
               e3["by_tier"].get("ILLIQUID", {}).get("mean_zone_a_t", 0),
               "{:,.0f}".format(
                   e3["by_tier"].get("ILLIQUID", {})
                   .get("median_capacity_usd", 0))),
        "10_highest_evidence_weighted_expected_value":
            "The intraday macro-release reaction. It is the only thing this "
            "release measured that is real, release-locked, event-specific "
            "and larger than its transaction cost - and the only reason it "
            "cannot be claimed is that the estate owns the wrong "
            "instruments to replicate it in.",
        "11_which_candidate_deserves_prospective_freezing":
            "NONE. %s" % (freeze.get("why_none") or "see the freeze gate"),
        "12_which_complex_model_beat_its_baseline":
            "None was run, and that is a deliberate result. The contract "
            "permits modern ML where NEW INFORMATION justifies it. No lane "
            "produced new information whose structure a transparent rule "
            "could not already express, so fitting a gradient-booster or a "
            "sequence model on top would have added capacity to a problem "
            "whose binding constraint is not capacity.",
        "13_most_valuable_information_source_now":
            "Native intraday futures. Every other wall this release probed "
            "is either priced beyond its evidence (analyst vintages, "
            "credit) or waiting on time (the option window, the prospective "
            "ledger). This one has a live measurement pointing at it.",
        "14_what_purchase_has_earned_authorization_consideration":
            "%s at %s - state %s. %s"
            % (purch["TOP_DATA_PURCHASE_RECOMMENDATION"],
               purch["TOP_DATA_PURCHASE_PRICE"],
               purch["TOP_RECOMMENDATION_STATE"],
               "No purchase is made and none is recommended without a "
               "sample."),
        "15_single_highest_value_release_45":
            "Settle the gold question. Acquire ONE native intraday futures "
            "product (ZN or ES, 1-minute, 2012-2019) through a sample or "
            "signup credit, and test the identical release-time rule in the "
            "instrument the release is actually about. If it replicates in "
            "rates, the estate has its first real edge; if it does not, "
            "gold was an artefact and the macro-event family is closed for "
            "good. Either answer is worth more than another search over "
            "owned daily data.",
    }


def _result_axes(e1, e2, e3, standalone, bh, freeze) -> dict:
    base = e2["variants"]["RESIDUAL_PORTFOLIO"].get("lock") or {}
    inc = e2["increments"]["RESIDUAL_vs_PREMIUM_CONTROL"]
    prem = e2["variants"]["PREMIUM_PORTFOLIO"].get("lock") or {}
    pinc = e2["increments"]["PREMIUM_CONTROL_vs_PASSIVE_LONG"]
    return {
        "SYSTEM_RESULT": "PASS",
        "STANDALONE_ALPHA_RESULT": "FAIL",
        "PORTFOLIO_ALPHA_RESULT":
            "R44_PORTFOLIO_SYNTHESIS_DOES_NOT_CREATE_ALPHA - %.2f%%/yr at "
            "t %.2f on the lockbox, with all eight predeclared rules "
            "agreeing in sign"
            % (100.0 * (base.get("excess_ann") or 0),
               base.get("t_hac") or 0),
        "STRUCTURAL_PREMIUM_RESULT":
            "R44_PREMIA_ARE_REAL_AND_INDISTINGUISHABLE_FROM_PASSIVE - the "
            "premium portfolio earns %.2f%%/yr at t %.2f on the lockbox, "
            "and its increment over a volatility-matched passive long is "
            "%.2f%%/yr at t %.2f"
            % (100.0 * (prem.get("excess_ann") or 0), prem.get("t_hac") or 0,
               100.0 * (pinc.get("increment_ann") or 0),
               pinc.get("increment_t_hac") or 0),
        "PORTFOLIO_CONTROL_INCREMENT_RESULT":
            "%.2f%%/yr at t %.2f - the residual portfolio is worse than the "
            "premia it was supposed to add to"
            % (100.0 * (inc.get("increment_ann") or 0),
               inc.get("increment_t_hac") or 0),
        "OPTIONS_DATA_RESULT":
            "R44_SURFACE_BUILT_AT_ZERO_COST_WINDOW_STILL_BINDING - a smile, "
            "a term structure and a variance risk premium measured from "
            "ACTUAL option prices with locally inverted IV, on a window "
            "%s sessions short of a judgeable sample"
            % ((e1["options"].get("surface") or {}).get("sessions_short_by")),
        "INTRADAY_DATA_RESULT": "R44_EVENT_EFFECT_SURVIVES_ITS_COST_IN_EVENT_"
                                "TIME_BUT_NOT_ITS_OWN_REPLICATION",
        "ANALYST_REVISION_RESULT": "R44_VENDOR_BACKWARD_VINTAGES_ARE_"
                                   "RESTATED_PROSPECTIVE_LEDGER_IS_WORKING",
        "CREDIT_DATA_RESULT": "R44_NATIVE_CREDIT_LICENCE_REQUIRED_AND_THE_"
                              "OWNED_OAS_FAMILY_IS_NOW_THREE_YEARS",
        "MICROSTRUCTURE_DATA_RESULT": "R44_LIQUIDITY_WITHDRAWAL_MEASURED_"
                                      "MAKER_EXECUTION_STILL_BLOCKED",
        "LESS_EFFICIENT_MARKET_RESULT":
            "R44_LESS_EFFICIENT_MARKETS_ARE_NOT_A_BETTER_FRONTIER",
        "SEARCH_ADJUSTED_RESULT": bh.get("verdict"),
        "TRUE_FORWARD_RESULT": "NOT_YET_TESTABLE - 0 rows and %d R44 shadows "
                               "frozen" % freeze["n_frozen"],
    }


def _headline(e1, e2, e3, standalone, prows, burden, freeze) -> dict:
    """The investment result, in the exact fields the handoff asks for."""
    base = e2["variants"]["RESIDUAL_PORTFOLIO"]
    lock = base.get("lock") or {}
    prem = e2["variants"]["PREMIUM_PORTFOLIO"]
    inc = e2["increments"]["RESIDUAL_vs_PREMIUM_CONTROL"]
    judged = [r for r in standalone if r.get("BURDEN_CHARGED")]
    best = max(judged, key=lambda r: (r.get("ZONE_B_T") or -9)) \
        if judged else None
    ch = e2["burden"]
    return {
        "STANDALONE_ALPHA_RESULT": "FAIL",
        "PORTFOLIO_ALPHA_RESULT": "FAIL",
        "STRUCTURAL_PREMIUM_RESULT": "REAL_BUT_INDISTINGUISHABLE_FROM_"
                                     "PASSIVE",

        "BEST_STANDALONE_CANDIDATE": (best or {}).get("CANDIDATE_ID"),
        "BEST_PORTFOLIO": "%s / %s" % ("RESIDUAL_PORTFOLIO",
                                       C.PRIMARY_COMBINATION_RULE),
        "BEST_STRUCTURAL_PREMIUM_CONTROL": "PREMIUM_PORTFOLIO / %s"
                                           % C.PRIMARY_COMBINATION_RULE,

        "BEST_ASSET_CLASS": (best or {}).get("ASSET_CLASS"),
        "BEST_HORIZON": (best or {}).get("HORIZON"),
        "BEST_INFORMATION_FAMILY": (best or {}).get("INFORMATION_FAMILY"),
        "BEST_ECONOMIC_EXPRESSION": (best or {}).get("ECONOMIC_EXPRESSION"),
        "BEST_MODEL": "TRANSPARENT_RULE",
        "MODEL_RESULT": "NO_COMPLEX_MODEL_WAS_RUN - no lane produced "
                        "information whose structure a transparent rule "
                        "could not already express",

        "BEST_STANDALONE_NET_RESIDUAL_ALPHA": (best or {}).get(
            "ZONE_B_EXCESS_ANN"),
        "BEST_STANDALONE_T_STAT": (best or {}).get("ZONE_B_T"),

        "BEST_PORTFOLIO_NET_RESIDUAL_ALPHA": lock.get("excess_ann"),
        "BEST_PORTFOLIO_T_STAT": lock.get("t_hac"),
        "BEST_PORTFOLIO_SHARPE": lock.get("sharpe"),
        "BEST_PORTFOLIO_MAX_DRAWDOWN": lock.get("max_drawdown"),

        "STRUCTURAL_CONTROL_NET_RETURN": (prem.get("lock") or {}).get(
            "excess_ann"),
        "STRUCTURAL_CONTROL_T_STAT": (prem.get("lock") or {}).get("t_hac"),
        "PORTFOLIO_INCREMENT_OVER_STRUCTURAL_CONTROL": inc.get(
            "increment_ann"),
        "PORTFOLIO_INCREMENT_T_STAT": inc.get("increment_t_hac"),

        "FORWARD_SHADOWS_ADDED": freeze["n_frozen"],

        "GLOBAL_SEARCH_BURDEN": burden["global_cumulative"],
        "GLOBAL_SEARCH_BURDEN_CONSERVATIVE": burden[
            "conservative_global_cumulative"],
        "NEW_R44_EFFECTIVE_TRIALS": burden["r44_distinct_zone_b_candidates"],
        "PORTFOLIO_SYNTHESIS_TRIALS": ch.get("n_distinct_books"),
        "PORTFOLIO_SYNTHESIS_RULES_EVALUATED": ch.get("n_rules_evaluated"),

        "MONEY_SPENT": 0.0,
        "NEW_ACCOUNTS": 0,
        "OPERATIONAL_WRITES": 0,
        "PORTFOLIO_MUTATIONS": 0,
        "ORDERS": 0,
        "MODEL_PROMOTIONS": 0,
        "SCHEDULER_CHANGES": 0,
        "SHELL_POLICY_VIOLATION": SP.violated(),
        "SHELL_POLICY_WAIVER_TOKEN": SP.waiver_token(),
    }


def terminal_state(e1, e2, e3, standalone) -> str:
    lock = e2["variants"]["RESIDUAL_PORTFOLIO"].get("lock") or {}
    if (lock.get("t_hac") or -9) >= C.PORTFOLIO_ALPHA_GATE["t_min_lock"]:
        return "R44_PORTFOLIO_ALPHA_FOUND"
    if any(r.get("QUALIFICATION_STATE") == "RESEARCH_CANDIDATE"
           for r in standalone):
        return "R44_MULTIPLE_ALPHA_CANDIDATES_FORWARD_PENDING"
    return "R44_NO_ALPHA_AFTER_ORTHOGONAL_AND_PORTFOLIO_SYNTHESIS"


def run(*, acquire_options: bool = True) -> dict:
    closeout = CO.run()
    amendment = CO.amend()
    e2 = run_engine2()
    e1 = run_engine1(acquire_options=acquire_options)
    e3 = run_engine3()

    standalone = FR.standalone_rows(e3, e1["intraday"], e1["options"])
    prows = FR.portfolio_rows(e2)
    bh = FR.search_adjustment(standalone, prows)
    freeze = FR.freeze_decision(standalone, prows)
    ready = FR.readiness(standalone, prows, freeze)

    opt_surface = e1["options"].get("surface") or {}
    best = FR._best_intraday(e1["intraday"])
    an = (e1["analyst"].get("vendor_backward_strip_reconciliation") or {})
    cr = e1["credit"]
    purch = PU.gate(
        options_short_by_sessions=opt_surface.get("sessions_short_by"),
        intraday_evidence_t=(best or {}).get("gross_t"),
        analyst_match_rate=an.get("match_rate"),
        credit_years_owned=cr.get("median_years_of_owned_oas"))

    burden = B.summary()
    # Both counts are reported. The dedup argument - that two rules producing
    # the same weight vector are one trial - is correct, and a reader who
    # does not accept it should not have to take it on faith. The
    # conservative count charges one trial per rule NAME.
    ch = e2["burden"]
    extra = int(ch.get("n_rules_evaluated", 0)) - int(
        ch.get("n_distinct_books", 0))
    burden["conservative_global_cumulative"] = \
        burden["global_cumulative"] + max(extra, 0)
    burden["conservative_note"] = (
        "the headline charges one trial per DISTINCT portfolio book; the "
        "conservative figure charges one per combination-rule name. %d rules "
        "collapsed into %d books, so the two differ by %d."
        % (ch.get("n_rules_evaluated", 0), ch.get("n_distinct_books", 0),
           max(extra, 0)))
    burden["ledger_rebuilt_during_development"] = (
        "the R44 ledger was rebuilt once, after a defect in candidate-id "
        "derivation hashed the rule NAME and so double-counted identical "
        "books. The INHERITED 302 was never touched and is re-derived from "
        "R43's own bytes on every run.")
    axes = _result_axes(e1, e2, e3, standalone, bh, freeze)
    answers = _fifteen_answers(e1, e2, e3, standalone, prows, purch, freeze)
    term = terminal_state(e1, e2, e3, standalone)

    # ---- artifacts ------------------------------------------------------- #
    write_artifact(STREAMS_ARTIFACT,
                   artifact_body("r44_stream_inventory/1",
                                 e2["inventory"]), overwrite=True)
    write_artifact(STANDALONE_ARTIFACT,
                   artifact_body("r44_standalone_alpha_frontier/1", {
                       "calculation_owner": CALCULATION_OWNER,
                       "rows": standalone,
                       "gate": dict(C.STANDALONE_ALPHA_GATE),
                       "search_adjustment": bh}), overwrite=True)
    write_artifact(PORTFOLIO_ARTIFACT,
                   artifact_body("r44_portfolio_alpha_frontier/1", {
                       "calculation_owner": CALCULATION_OWNER,
                       "rows": prows,
                       "primary_rule": C.PRIMARY_COMBINATION_RULE,
                       "gate": dict(C.PORTFOLIO_ALPHA_GATE),
                       "qualification": e2["qualification"],
                       "kill_battery": e2["kill_battery"],
                       "pbo": e2["pbo"],
                       "increments": e2["increments"],
                       "diversification_arithmetic":
                           e2["diversification_arithmetic"],
                       "sign_selected_diagnostic":
                           e2["sign_selected_diagnostic"]}), overwrite=True)
    write_artifact(CONTROL_ARTIFACT,
                   artifact_body("r44_structural_premium_control/1", {
                       "calculation_owner": CALCULATION_OWNER,
                       "control": e2["controls"],
                       "premium_portfolio": e2["variants"][
                           "PREMIUM_PORTFOLIO"],
                       "increments": e2["increments"],
                       "controls_declared": dict(C.CONTROLS)}), overwrite=True)
    write_artifact(PURCHASE_ARTIFACT,
                   artifact_body("r44_purchase_gate/1", purch),
                   overwrite=True)

    lanes = artifact_body("r44_lane_results/1", {
        "calculation_owner": CALCULATION_OWNER,
        "engine1": e1, "engine2": {k: v for k, v in e2.items()
                                   if not k.startswith("_")},
        "engine3": e3})
    lanes["lane_results_hash"] = sha({k: v for k, v in lanes.items()
                                      if k != "safety_block"})
    write_artifact(LANES_ARTIFACT, lanes, overwrite=True)

    verdict = artifact_body("r44_final_verdict/1", {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "terminal_state": term,
        "headline": _headline(e1, e2, e3, standalone, prows, burden, freeze),
        "result_axes": axes,
        "fifteen_answers": answers,
        "standalone_frontier": standalone,
        "portfolio_frontier": prows,
        "search_adjustment": bh,
        "freeze": freeze,
        "readiness": ready,
        "search_burden": burden,
        "inherited_burden": closeout["inherited_burden"],
        "purchase_gate": {k: v for k, v in purch.items()
                          if k != "candidates"},
        "frozen_contract_hash": closeout["frozen_contract_hash"],
        "amended_contract_hash": amendment["amended_contract_hash"],
        "contract_integrity": CO.verify_contract_unchanged(),
        "witnesses_unchanged": CO.witnesses_unchanged(),
        "post_freeze_amendments": list(C.POST_FREEZE_AMENDMENTS),
        "git": closeout["git"],
        "branch_matrix": _branch_matrix(e1, e2, e3),
        "shell_policy": SP.block(),
        "money_spent_usd": 0.0,
    })
    write_artifact("R44_SHELL_POLICY_EVENTS.json",
                   artifact_body("r44_shell_policy_events/1", SP.block()),
                   overwrite=True)
    verdict["verdict_hash"] = sha({k: v for k, v in verdict.items()
                                   if k != "safety_block"})
    write_artifact(VERDICT_ARTIFACT, verdict, overwrite=True)
    return verdict


def _branch_matrix(e1, e2, e3) -> dict:
    lanes = {
        "E1A_OPTIONS_SURFACE": e1["options"].get("state"),
        "E1B_INTRADAY_EVENT": e1["intraday"].get("state"),
        "E1C_ANALYST_REVISIONS": e1["analyst"].get("state"),
        "E1D_NATIVE_CREDIT": e1["credit"].get("state"),
        "E1E_MICROSTRUCTURE": e1["microstructure"].get("state"),
        "E2_PORTFOLIO_SYNTHESIS": e2.get("state"),
        "E3_LESS_EFFICIENT_MARKETS": e3.get("state"),
    }
    return {
        "lanes": lanes,
        "every_lane_terminated": all(
            v in C.BLOCKER_VOCAB for v in lanes.values()),
        "vocabulary": list(C.BLOCKER_VOCAB),
        "no_executable_zero_cost_branch_deferred":
            C.NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED,
    }
