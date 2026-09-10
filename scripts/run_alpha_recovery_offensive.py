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
          "compete", "package", "purchase", "scoreboard", "report", "all")


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
            "direction": _stage_direction, "compete": _stage_compete, "package": _stage_package,
            "purchase": _stage_purchase, "scoreboard": _stage_scoreboard, "report": _stage_report}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Alpha Recovery Offensive research runner (research only).")
    ap.add_argument("stage", choices=STAGES)
    ap.add_argument("--json", action="store_true", help="print the stage result as JSON")
    args = ap.parse_args(argv)
    try:
        AR.assert_worktree_import()
        AR.assert_research_root_is_not_live()
        stages = [s for s in STAGES if s != "all"] if args.stage == "all" else [args.stage]
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
