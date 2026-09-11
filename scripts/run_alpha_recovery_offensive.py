"""scripts/run_alpha_recovery_offensive.py - the ONE runner of the Alpha Recovery Offensive.

WHAT THIS IS
    A research-only entrypoint that runs the campaign stages against the OWNED
    substrates and the persisted R63/R64 artifacts (read only), writing every
    artifact under the campaign research root and the committed campaign
    directory (research/alpha_recovery):

        checkpoint   freeze (write-once) and verify the 10-session stop-loss
        program      the frontier-directed research program and the isolated
                     governor proof
        incumbent    the real baseline of fundamental_momentum_50_50_v1
                     (historical OOS and TRUE_FORWARD, separately)
        tournament   the head-to-head grid on identical samples
        news         the bounded news sample acquisition (needs EODHD_API_KEY)
        cadence      the signal-frequency != trading-frequency grid
        direction    calibrated broad-market directional forecasts (SPY)
        equity       the incumbent's own information under a different
                     construction: the rebalance-cadence ladder and its legs
        residual     the OWNED frontier needs the first pass left unmeasured
        products     what this estate can predict today, in economic units
        compete      cross-domain equal-risk incremental utility of the chosen
                     sleeve on top of the incumbent-only book
        package      immutable candidate records and the human-gated command
        purchase     owned / free exhaustion and the paid-information case
        scoreboard   alpha_recovery_scoreboard.json + the human rendering
        report       ALPHA_RECOVERY_REPORT.md (scoreboard first)
        all          every stage in that order

WHAT THIS IS NOT
    It does not restart, deploy, register, adopt, promote, approve, order,
    fill, purchase, subscribe or write any live store. The canonical checkout,
    the live desk ledgers and every earlier research root are read only. There
    is no execute flag because there is nothing operational to execute.

    The one flag that causes an outward call is ``--spend-free-credits``, and
    it is a DATA-ACQUISITION flag: it consumes pre-existing free provider
    credits against a plan whose cost was already estimated, spends no money,
    starts no subscription, and touches nothing operational.

TERMINAL TOKENS (exactly one on the last line)
    ALPHA_RECOVERY_STAGE_OK / ALPHA_RECOVERY_STAGE_FAILED - <reason>
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

import warnings  # noqa: E402

warnings.filterwarnings("ignore")

from alpha_agent import alpha_recovery as AR  # noqa: E402

ENTRYPOINT = "scripts/run_alpha_recovery_offensive.py"
OK = "ALPHA_RECOVERY_STAGE_OK"
FAILED = "ALPHA_RECOVERY_STAGE_FAILED"
STAGES = ("checkpoint", "program", "incumbent", "tournament", "news", "cadence", "direction",
          "equity", "residual", "intraday", "options", "reversed_skew", "databento", "futures",
          "futures_alpha", "microstructure", "compete", "products", "package", "purchase",
          "scoreboard", "report", "all")
#: ``databento`` is deliberately OUTSIDE ``all``: it is the only stage that can
#: consume a credit balance, so it is never swept up by a full-campaign run.
STAGES_EXCLUDED_FROM_ALL = ("databento",)


def _stage_checkpoint() -> dict:
    from alpha_agent.alpha_recovery import checkpoint as CK
    cp = CK.freeze()
    v = CK.verify(CK.load())
    if not v["valid"]:
        raise RuntimeError("checkpoint invalid: %s" % v["failures"])
    return {"freeze_action": cp.get("freeze_action"), "clock": CK.session_clock(CK.load())["rendered"],
            "verify": v}


def _stage_program() -> dict:
    from alpha_agent.alpha_recovery import program as PR
    body = PR.build()
    return {"allocation_follows_frontier": body["allocation_follows_frontier"],
            "non_price_rule": body["non_price_rule"],
            "n_families_mandated_by_governor": body["n_families_mandated_by_governor"]}


def _stage_incumbent() -> dict:
    from alpha_agent.alpha_recovery import incumbent as INC
    body = INC.run(verbose=True)
    return {"verdict": body["verdict"]["verdict"]}


def _stage_tournament() -> dict:
    from alpha_agent.alpha_recovery import tournament as T
    cells = T.run_grid(verbose=True)
    body = T.merge(cells=cells)
    return {"counts": body["counts"]}


NEWS_MIN_NAMES_FOR_CELLS = 30


def _stage_news() -> dict:
    from alpha_agent.alpha_recovery import incumbent as INC, news as NW, tournament as T
    E, elig = INC.equity_substrate()
    plan = NW.plan(E, elig)
    man = NW.manifest() or {}
    n_done = len(man.get("complete") or {})
    # a bounded acquisition that already holds a usable sample is not re-fetched
    # by the stage (a single writer, ever); a thin one is continued (resumable)
    if n_done < NEWS_MIN_NAMES_FOR_CELLS:
        man = NW.acquire(plan["names"], end=plan["end"], verbose=True)
    else:
        man = {**man, "n_complete": n_done, "state": man.get("state") or "SAMPLE_IN_USE"}
    out = {"acquisition": {k: man.get(k) for k in ("state", "n_complete", "requests", "items")}}
    if (man.get("n_complete") or 0) >= NEWS_MIN_NAMES_FOR_CELLS:
        # the family's cells (protocol: 1, 5, 21 on the block; count-only and sentiment-only at 5)
        inc = (INC.BLOCK_SCORE,)
        grid = [T.spec(h, NW.BLOCK, baseline=inc, family="NEWS_INTENSITY", tag="PRIMARY") for h in (1, 5, 21)]
        grid.append(T.spec(5, NW.BLOCK_COUNT_ONLY, baseline=inc, family="NEWS_INTENSITY", tag="PRIMARY"))
        grid.append(T.spec(5, NW.BLOCK_SENT_ONLY, baseline=inc, family="NEWS_INTENSITY", tag="PRIMARY"))
        T._DS.clear()
        for h in (1, 5, 21):
            T.equity_dataset(h, extra_builders=(NW.build_blocks,))
        cells = T.run_grid(grid, verbose=True)
        body = T.merge()
        out["counts"] = body["counts"]
    return out


def _stage_cadence() -> dict:
    from alpha_agent.alpha_recovery import cadence as CD
    cells = CD.run_grid(verbose=True)
    body = CD.merge(cells=cells)
    return {"counts": body["counts"], "chosen": body["chosen_by_family"]}


def _stage_direction() -> dict:
    from alpha_agent.alpha_recovery import market_direction as MD
    body = MD.run(verbose=True)
    return {h: (r.get("verdicts") or {}).get("verdict") or r.get("state") for h, r in body["horizons"].items()}


def _stage_equity() -> dict:
    """The incumbent's OWN information under a different construction: the
    rebalance-cadence ladder and the two legs."""
    from alpha_agent.alpha_recovery import equity_challengers as EC
    cells = EC.run_grid(verbose=True)
    body = EC.merge(cells=cells)
    return {"counts": body["counts"], "best": (body.get("best_by_advantage") or {}).get("cell_id"),
            "reconciliation": body["reconciliation_with_incumbent_baseline"]}


def _stage_residual() -> dict:
    """The OWNED frontier needs the first pass left unmeasured."""
    from alpha_agent.alpha_recovery import frontier_residual as FR
    cells = FR.run_grid(verbose=True)
    body = FR.merge(cells=cells)
    return {"counts": body["counts"],
            "cells": [(b["cell_id"], b["r64_verdict"], b["conditional_t"]) for b in body["brief"]]}


def _stage_intraday() -> dict:
    """Information axis: NATIVE INTRADAY CROSS-ASSET. Establish the real data
    state first, then fire the bounded families at it."""
    from alpha_agent.alpha_recovery import intraday_alpha as IA, intraday_data as ID
    state = ID.build()
    print("intraday panel: %d sessions %s -> %s, tradable %s"
          % (state["sessions"], state["first_session"], state["last_session"], state["tradable"]),
          flush=True)
    cells = IA.run_grid(verbose=True)
    body = IA.merge(cells=cells)
    d = body["cost_vs_information_diagnosis"]
    return {"counts": body["counts"], "diagnosis": d["verdict"],
            "max_gross_t_primary": d["max_gross_t_primary"],
            "max_gross_t_rescue": d["max_gross_t_rescue"],
            "best": (body.get("best_by_ann_net") or {}).get("cell_id")}


def _stage_options() -> dict:
    """Information axis A: the SPY option / implied-volatility surface.

    Builds the MONEYNESS-ANCHORED surface from the acquired OPRA bands when they
    are on disk, then runs the pre-registered grid. The build is idempotent and
    costs nothing - the bands are already paid for - and without it the module
    falls back to the R45 fixed strike band, which is the configuration this
    axis was blocked on.
    """
    from alpha_agent.alpha_recovery import options_acquisition as OA
    from alpha_agent.alpha_recovery import options_surface as OS
    built = None
    if any(OA.data_root().glob("SPY_*_%s_*.csv" % OA.SCHEMA)):
        built = OA.build_surface()
        OS._CACHE.clear()
    cells = OS.run_grid(verbose=True)
    body = OS.merge(cells=cells)
    return {"usability": body["usability"]["state"], "counts": body["counts"],
            "surface_is_moneyness_anchored": body["surface"].get("is_moneyness_anchored"),
            "surface_dates": (built or {}).get("dates"),
            "dates_bracketing_the_money": body["usability"]["dates_whose_strikes_bracket_the_money"]}


def _stage_reversed_skew() -> dict:
    """The ONE frozen challenger born from a contradicted pre-registered sign.

    Runs the independent historical confirmation on a window that PRECEDES the
    discovery sample and reports what the canonical forward registrar holds.
    This stage registers nothing: registration is an operator act and lives in
    ``scripts/register_reversed_skew_challenger.py``, because a research stage
    that could start its own forward clock is one loop away from one that
    promotes itself.
    """
    from alpha_agent.alpha_recovery import options_acquisition as OA
    from alpha_agent.alpha_recovery import options_surface as OS
    from alpha_agent.alpha_recovery import reversed_skew as RS
    built = None
    # ALWAYS rebuild when bands are present, never "only if the file is absent".
    # A partially downloaded window writes a perfectly well-formed surface that
    # covers a fraction of the dates, and a build guarded on existence would
    # then keep serving it forever - the confirmation would silently be run on
    # whatever had finished downloading first.
    if any(OA.data_root(OA.CONFIRMATION_TAG).glob("SPY_*_%s_*.csv" % OA.SCHEMA)):
        built = OA.build_surface(window=RS.CONFIRMATION_WINDOW, tag=OA.CONFIRMATION_TAG,
                                 spot_tag=OA.CONFIRMATION_TAG,
                                 spot_dataset=OA.SPOT_DATASET_PRE_2024,
                                 spot_schema=OA.SPOT_SCHEMA_INTRADAY)
        OS._CACHE.clear()
    body = RS.build()
    conf = body["independent_historical_confirmation"]
    fwd = body["true_forward"]
    return {"challenger_id": body["challenger_id"],
            "historical_confirmation": conf.get("classification"),
            "independent_periods": conf.get("independent_periods"),
            "confirmation_surface_dates": (built or {}).get("dates"),
            "confirmation_surface_expiries": (built or {}).get("expiries"),
            "true_forward": fwd.get("state"),
            "evidence_status": fwd.get("evidence_status"),
            "capital_eligible_now": body["capital_eligible_now"]}


def _stage_databento() -> dict:
    """Acquire native CME futures 1-minute history within the FREE credit only.

    Cost is estimated through the provider's own metadata API before anything
    is downloaded, and the download step refuses any request the plan did not
    price. ``--spend-free-credits`` is required to consume a single credit;
    without it the stage prices the panel and stops.

    The flag is deliberately NOT named for operational execution: this runner
    guarantees it has no such flag, and that guarantee is a pinned invariant.
    """
    from alpha_agent.alpha_recovery import databento_acquisition as DBN
    body = DBN.build(execute=_SPEND_FREE_CREDITS, years=_ACQUISITION_YEARS)
    out = {"state": body["state"]}
    if body.get("blocker"):
        out["blocker"] = body["blocker"]["kind"]
        out["remediation"] = body["blocker"]["remediation"]
        print("databento BLOCKED: %s\n  %s" % (body["blocker"]["kind"],
                                               body["blocker"]["remediation"]), flush=True)
        return out
    sel = (body.get("plan") or {}).get("selection") or {}
    out.update({"chosen": sel.get("chosen"), "estimated_spend_usd": sel.get("estimated_spend_usd"),
                "effective_cap_usd": sel.get("effective_cap_usd"),
                "buckets": sel.get("buckets_covered"),
                "full_panel_cost_usd": (body.get("plan") or {}).get("full_panel_cost_usd"),
                "downloaded": (body.get("download") or {}).get("state")})
    print("databento plan: %s for $%s of a $%s cap (full panel $%s)"
          % (sel.get("chosen"), sel.get("estimated_spend_usd"), sel.get("effective_cap_usd"),
             (body.get("plan") or {}).get("full_panel_cost_usd")), flush=True)
    return out


def _stage_futures() -> dict:
    """The native CME futures panel's PRE-REGISTRATION.

    Costs nothing and touches no provider. It exists to be written and
    committed BEFORE the panel is downloaded, so the session definition, the
    cost ladder and the family list cannot have been chosen after seeing a bar.
    """
    from alpha_agent.alpha_recovery import futures_intraday as FI
    body = FI.build()
    out = {"state": body["state"], "roots_on_disk": body["roots_on_disk"],
           "families": list(body["families"]), "mark_to": body["session"]["mark_to"]}
    if body.get("blocker"):
        out["blocker"] = body["blocker"]["kind"]
    return out


def _stage_futures_alpha() -> dict:
    """The native CME futures intraday ALPHA campaign.

    Runs the pre-registered grid on the acquired panel, spends the rescue
    budget only where the named binding failure is actually measured, and
    applies BH / family Holm over every executed specification. It scores
    nothing if the panel is absent - an absent panel is a blocker, never a
    silent "no advantage".
    """
    from alpha_agent.alpha_recovery import futures_alpha as FA
    from alpha_agent.alpha_recovery import futures_intraday as FI
    if not FI.available_roots():
        return {"state": "BLOCKED", "blocker": "PANEL_NOT_ACQUIRED",
                "remediation": "run the databento stage with --spend-free-credits first"}
    primaries = FA.run_grid(FA.default_grid(), verbose=False)
    rescues = FA.run_grid(FA.rescue_grid(primaries), verbose=False)
    body = FA.merge(cells=primaries + rescues)
    return {"n_cells": body["n_cells"], "counts": body["counts"],
            "bh_rejected": body["multiple_testing"]["benjamini_hochberg"]["n_rejected"],
            "qualified": len(body["qualified_for_true_forward"]),
            "true_forward_ready": body["true_forward_ready"],
            "best_gross_t": (body.get("best_by_gross_t") or {}).get("t_gross")}


def _stage_microstructure() -> dict:
    """The native CME ORDER-FLOW campaign.

    The first axis in this campaign whose information is not price state at all:
    resting depth, queue asymmetry, order counts and trade aggressor side cannot
    be computed from an OHLCV bar at any lag. Runs the pre-registered grid on
    the acquired top-of-book panel and applies BH / family Holm over every
    executed specification. An absent panel is a blocker, never a silent
    "no advantage".
    """
    from alpha_agent.alpha_recovery import microstructure as MS
    from alpha_agent.alpha_recovery import microstructure_alpha as MA
    if not any(MS.data_root().glob("*_%s_*.csv" % MS.SCHEMA)):
        return {"state": "BLOCKED", "blocker": "ORDER_FLOW_PANEL_NOT_ACQUIRED",
                "remediation": "run the microstructure acquisition with an explicit budget first"}
    primaries = MA.run_grid(MA.default_grid(), verbose=False)
    rescues = MA.run_grid(MA.rescue_grid(primaries), verbose=False)
    body = MA.merge(cells=primaries + rescues)
    return {"n_cells": body["n_cells"], "counts": body["counts"],
            "bh_rejected": body["multiple_testing"]["benjamini_hochberg"]["n_rejected"],
            "qualified": len(body["qualified_for_true_forward"]),
            "true_forward_ready": body["true_forward_ready"],
            "best_gross_t": (body.get("best_by_gross_t") or {}).get("t_gross")}


def _stage_products() -> dict:
    """What this estate can predict today, in economic units."""
    from alpha_agent.alpha_recovery import forecast_products as FP
    body = FP.build()
    return {"licensed_families": body["licensed_families"], "answer": body["answer"]}


def _stage_compete() -> dict:
    from alpha_agent.alpha_recovery import cadence as CD, incumbent as INC, tournament as T
    import numpy as np
    from alpha_agent.r63 import features as FE
    cad = AR.read_artifact(CD.ARTIFACT_NAME) or {}
    chosen = (cad.get("chosen_by_family") or {}).get("FX_CARRY_CADENCE") or {}
    if not chosen.get("cell_id"):
        body = {"state": "DATA_HOLD", "why": "no chosen FX carry cadence cell"}
        AR.write_artifact("cross_domain.json", body)
        return body
    E, elig = INC.equity_substrate()
    blk = INC.incumbent_blocks(E, elig)
    spy = E["price"]["spy_tr"]
    spy_r = np.full(len(E["dates"]), np.nan)
    spy_r[1:] = spy[1:] / spy[:-1] - 1.0
    trend = FE._ret_over(spy_r[None, :], 252, 21)[0]
    inc_rows = INC.run_score(blk[INC.BLOCK_SCORE][..., 0], E=E, elig=elig, ret=blk["_ret"], h=21,
                             top_n=AR.EQ_TOP_N_OPERATIONAL, label="incumbent_top25", spy_trend=trend)["_rows"]
    sleeve = CD.sleeve_series(chosen["cell_id"])
    res = T.cross_domain(inc_rows, sleeve, horizon=21, label=chosen["cell_id"])
    body = {"schema": "alpha_recovery_cross_domain/1", "chosen_sleeve": chosen, "result": res,
            "evidence_label": cad.get("evidence_label")}
    AR.write_artifact("cross_domain.json", body)
    return {"state": res.get("state"), "positive": res.get("positive_incremental_utility_after_costs"),
            "incremental_ann_net_return": res.get("incremental_ann_net_return"), "t": res.get("t_incremental")}


def _stage_package() -> dict:
    from alpha_agent.alpha_recovery import forward_package as FP
    body = FP.build(tournament=AR.read_artifact("tournament.json"), cadence=AR.read_artifact("cadence_grid.json"))
    return {"n_ready": body["n_ready"], "n_survivors_not_qualified": body["n_survivors_not_qualified"]}


def _stage_purchase() -> dict:
    from alpha_agent.alpha_recovery import news as NW, purchase_case as PC
    body = PC.build(news_manifest=NW.manifest())
    return {"exhausted": body["owned_free_information_exhausted"],
            "top_missing_information_need": body["top_missing_information_need"],
            "verdicts": [(c["need"], c["purchase_experiment_verdict"]) for c in body["candidates_ranked"]]}


def _stage_scoreboard() -> dict:
    from alpha_agent.alpha_recovery import scoreboard as SB
    body = SB.build()
    return {"status": body["status"], "clock": (body["campaign"].get("clock") or {}).get("rendered")}


def _stage_report() -> dict:
    from alpha_agent.alpha_recovery import report as RP
    p = RP.write()
    return {"report": str(p)}


STAGE_FN = {"checkpoint": _stage_checkpoint, "program": _stage_program, "incumbent": _stage_incumbent,
            "tournament": _stage_tournament, "news": _stage_news, "cadence": _stage_cadence,
            "direction": _stage_direction, "equity": _stage_equity, "residual": _stage_residual,
            "intraday": _stage_intraday, "options": _stage_options,
            "reversed_skew": _stage_reversed_skew, "databento": _stage_databento,
            "futures": _stage_futures, "futures_alpha": _stage_futures_alpha,
            "microstructure": _stage_microstructure,
            "compete": _stage_compete, "products": _stage_products, "package": _stage_package,
            "purchase": _stage_purchase, "scoreboard": _stage_scoreboard, "report": _stage_report}

#: Every declared stage must be runnable. ``intraday`` and ``options`` shipped
#: in STAGES without a STAGE_FN entry and raised KeyError on invocation, which
#: went unnoticed because those axes were driven by importing their modules
#: directly. The test suite now pins this.
_UNREGISTERED = [s for s in STAGES if s != "all" and s not in STAGE_FN]
assert not _UNREGISTERED, "stages declared but not registered: %s" % _UNREGISTERED

_SPEND_FREE_CREDITS = False
#: History depth for the databento stage. Deeper history is strictly more
#: statistical power and cannot bias a result - it is chosen before any bar
#: exists, let alone any strategy outcome.
_ACQUISITION_YEARS = 2.0


def main(argv=None) -> int:
    global _SPEND_FREE_CREDITS, _ACQUISITION_YEARS
    ap = argparse.ArgumentParser(description="Alpha Recovery Offensive research runner (research only).")
    ap.add_argument("stage", choices=STAGES)
    ap.add_argument("--json", action="store_true", help="print the stage result as JSON")
    ap.add_argument("--spend-free-credits", action="store_true",
                    help="databento stage only: consume FREE credits on the already-priced plan. "
                         "Without it the plan is estimated and nothing is downloaded. This is a "
                         "data-acquisition flag; nothing operational is ever executed here.")
    ap.add_argument("--years", type=float, default=_ACQUISITION_YEARS,
                    help="databento stage only: years of dated-contract history to price and, "
                         "with --spend-free-credits, acquire.")
    args = ap.parse_args(argv)
    _SPEND_FREE_CREDITS = bool(args.spend_free_credits)
    _ACQUISITION_YEARS = float(args.years)
    try:
        AR.assert_worktree_import()
        AR.assert_research_root_is_not_live()
        stages = ([s for s in STAGES if s != "all" and s not in STAGES_EXCLUDED_FROM_ALL]
                  if args.stage == "all" else [args.stage])
        out = {}
        t0 = time.time()
        for s in stages:
            print("== %s ==" % s, flush=True)
            out[s] = STAGE_FN[s]()
            print("   %s" % json.dumps(out[s], default=str)[:600], flush=True)
        out["seconds"] = round(time.time() - t0, 1)
        if args.json:
            print(json.dumps(out, indent=1, default=str))
    except Exception as exc:                                  # noqa: BLE001
        print("%s - %s: %s" % (FAILED, type(exc).__name__, exc))
        return 1
    print(OK)
    return 0


if __name__ == "__main__":
    sys.exit(main())
