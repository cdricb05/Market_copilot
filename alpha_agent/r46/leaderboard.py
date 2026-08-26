"""alpha_agent.r46.leaderboard - THE board. One place, every competitor.

Before this module existed an operator asking "how much prospective evidence
does this estate actually have?" would have had to open five campaign roots,
four registry schemas and four ledger conventions, and would still have had to
notice that every row count was zero. The number was never hidden. It was just
never anywhere.

Ranking rule, and the reason for it: a challenger is ranked FIRST by evidence
maturity band and only THEN by measured edge. Two forward observations cannot
outrank five hundred however good they look, because the thing that makes the
first number look good is usually that there are two of them. Release 45
priced that effect exactly - the best of sixty noisy cells always looks
locally peaked - and this is the display consequence.

No row in this board may read "proven". ``FORWARD_CONFIRMED`` is the strongest
state available, it requires the full declared gate, and it still confers no
capital, no promotion and no order.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, write_json
from . import campaign_dir
from . import clock as CK
from . import contract as C
from . import evidence as EV
from . import ledger as LG
from . import registry as RG

CALCULATION_OWNER = "alpha_agent.r46.leaderboard"

ARTIFACT = "R46_LEADERBOARD.json"

#: Evidence bands, most mature first. Rank inside a band by measured edge.
_BAND = {
    C.FORWARD_CONFIRMED: 0,
    C.FORWARD_CANDIDATE: 1,
    C.EARLY_FORWARD_EVIDENCE: 2,
    C.FORWARD_PENDING: 3,
    C.HISTORICAL_ONLY: 4,
    C.FORWARD_REJECTED: 5,
    C.DATA_BLOCKED: 6,
}


def _rank_key(row: dict):
    band = _BAND.get(row.get("state"), 7)
    eff = -int(row.get("effective_independent") or 0)
    edge = row.get("net_alpha_bps")
    edge = -float(edge) if edge is not None else 0.0
    return (band, eff, edge)


def build(campaign_id: str = CAMPAIGN_ID, registry: dict = None) -> dict:
    reg = registry if registry is not None else RG.load(campaign_id)
    preds = LG.predictions(campaign_id)
    outs = LG.outcomes(campaign_id)

    outs_by_cid: dict = {}
    for o in outs:
        outs_by_cid.setdefault(o.get("challenger_id"), []).append(o)
    preds_by_cid: dict = {}
    for p in preds:
        preds_by_cid.setdefault(p.get("challenger_id"), []).append(p)

    rows = []
    retuned = {r["challenger_id"]
               for r in (reg.get("retunes_detected") or ())}

    for c in (reg.get("challengers") or ()):
        cid = c["challenger_id"]
        my_preds = preds_by_cid.get(cid, [])
        my_outs = outs_by_cid.get(cid, [])
        pit_ok = c.get("point_in_time_status") == C.PIT_OK
        retune_free = cid not in retuned
        for h in c.get("horizons", ()):
            summary = EV.summarise(my_outs, h)
            emitted = sum(1 for p in my_preds if int(p.get("horizon")) == h)
            if c.get("state") == C.DATA_BLOCKED:
                verdict = {"state": C.DATA_BLOCKED,
                           "reject_reason": c.get("blocked_reason"),
                           "checks": {}, "all_checks_passed": False,
                           "rejected": False}
            else:
                verdict = EV.gate(summary, pit_ok, retune_free)
            rows.append(_row(c, h, summary, verdict, emitted,
                             origin="R46_SEED"))

    for a in ((reg.get("adoption") or {}).get("adopted") or ()):
        rows.append({
            "challenger_id": a["challenger_id"],
            "challenger_version": a.get("challenger_version", "adopted"),
            "origin": "ADOPTED_PRIOR_RELEASE",
            "source_release": a.get("source_release"),
            "family": a.get("family"),
            "asset_class": a.get("asset_class"),
            "horizon": None,
            "historical_qualification_state": C.HISTORICAL_ONLY,
            "forward_start": a.get("frozen_at"),
            "forward_predictions_emitted": 0,
            "forward_predictions_matured": 0,
            "raw_matured": 0,
            "effective_independent": 0,
            "hit_rate": None, "rank_ic": None, "net_return": None,
            "net_alpha_bps": None, "sharpe": None, "max_drawdown": None,
            "turnover": None, "ci95_low": None, "ci95_high": None,
            "forward_evidence_score": 0.0,
            "data_quality": a.get("stream_state", {}).get("state"),
            "state": a.get("state"),
            "blocked_reason": a.get("stream_state", {}).get("reason"),
            "next_decision_date": None,
            "next_evidence_gate": "stream cannot accrue - see blocked_reason",
            "forward_rows_owned_by": a.get("forward_rows_owned_by"),
            "r46_writes_forward_rows_for_it": False,
            "promotion_allowed": False,
        })

    rows.sort(key=_rank_key)
    for i, r in enumerate(rows, start=1):
        r["rank"] = i

    active = [r for r in rows if r["origin"] == "R46_SEED"
              and r["state"] != C.DATA_BLOCKED]
    body = artifact_body(
        "r46_leaderboard/1", CALCULATION_OWNER,
        built_at_utc=CK.iso(CK.now_utc()),
        n_rows=len(rows),
        n_competing=len({r["challenger_id"] for r in rows}),
        n_r46_cells=len([r for r in rows if r["origin"] == "R46_SEED"]),
        n_adopted=len([r for r in rows
                       if r["origin"] == "ADOPTED_PRIOR_RELEASE"]),
        n_forward_pending=len([r for r in rows
                               if r["state"] == C.FORWARD_PENDING]),
        n_early=len([r for r in rows
                     if r["state"] == C.EARLY_FORWARD_EVIDENCE]),
        n_candidate=len([r for r in rows
                         if r["state"] == C.FORWARD_CANDIDATE]),
        n_confirmed=len([r for r in rows
                         if r["state"] == C.FORWARD_CONFIRMED]),
        n_rejected=len([r for r in rows if r["state"] == C.FORWARD_REJECTED]),
        n_data_blocked=len([r for r in rows if r["state"] == C.DATA_BLOCKED]),
        total_forward_predictions_emitted=len(preds),
        total_forward_predictions_matured=len(outs),
        best_net_alpha_bps=max(
            (r["net_alpha_bps"] for r in active
             if r.get("net_alpha_bps") is not None), default=None),
        multiple_testing=EV.benjamini_hochberg(
            [r.get("p_value") for r in active], fdr=0.10),
        ranking_rule="evidence maturity band first, measured edge second - "
                     "two observations never outrank five hundred",
        no_row_may_read_proven=True,
        proven_alpha_is_not_a_state=True,
        rows=rows,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


def _row(c: dict, horizon: int, summary: dict, verdict: dict,
         emitted: int, origin: str) -> dict:
    return {
        "challenger_id": c["challenger_id"],
        "challenger_version": c["challenger_version"],
        "origin": origin,
        "source_release": "R46",
        "family": c["family"],
        "asset_class": c["asset_class"],
        "horizon": int(horizon),
        "horizon_unit": "ELIGIBLE_SESSIONS",

        "historical_qualification_state": c.get(
            "historical_qualification_state", C.HISTORICAL_ONLY),
        "historical_qualification_summary": c.get(
            "historical_qualification_summary"),

        "forward_start": c.get("forward_start"),
        "forward_predictions_emitted": emitted,
        "forward_predictions_matured": summary["raw_matured"],
        "raw_matured": summary["raw_matured"],
        "effective_independent": summary["effective_independent"],
        "n_distinct_decision_dates": summary["n_distinct_decision_dates"],
        "first_decision": summary["first_decision"],
        "last_decision": summary["last_decision"],

        "hit_rate": summary["hit_rate"],
        "rank_ic": summary["mean_rank_ic"],
        "net_return": summary["mean_net_return"],
        "net_alpha_bps": summary["mean_net_alpha_bps"],
        "net_alpha_at_2x_costs": summary["mean_net_alpha_at_2x_costs"],
        "t_stat": summary["t_stat_net_vs_control"],
        "sharpe": summary["sharpe_annualised"],
        "max_drawdown": summary["max_drawdown"],
        "turnover": summary["turnover_per_decision"],
        "payoff_ratio": summary["payoff_ratio"],
        "ci95_low": summary["ci95_low"],
        "ci95_high": summary["ci95_high"],
        "single_day_share_of_pnl": summary["single_day_share_of_pnl"],

        "control": c.get("control"),
        "benchmark": c.get("benchmark"),
        "forward_evidence_score": _score(summary, verdict),
        "data_quality": (c.get("feasibility") or {}).get("state"),
        "state": verdict["state"],
        "gate_checks": verdict.get("checks"),
        "rejected": verdict.get("rejected"),
        "blocked_reason": verdict.get("reject_reason"),
        "next_evidence_gate": _next_gate(verdict),
        "next_decision_date": None,
        "promotion_allowed": False,
        "economic_overlap_with": c.get("economic_overlap_with") or [],
        "overlap_note": c.get("overlap_note"),
    }


def _score(summary: dict, verdict: dict) -> float:
    """0-1 maturity score. Deliberately dominated by EVIDENCE, not by edge."""
    need = max(1, int(verdict.get("required_effective_independent") or 1))
    eff = int(summary.get("effective_independent") or 0)
    return round(min(1.0, float(eff) / float(need)), 4)


def _next_gate(verdict: dict) -> str:
    if verdict.get("state") == C.FORWARD_REJECTED:
        return "killed at this version - a fix requires a NEW version with a " \
               "new forward clock"
    if verdict.get("state") == C.FORWARD_CONFIRMED:
        return "gate passed; the portfolio manager still decides, manually"
    se = verdict.get("shortfall_effective")
    sr = verdict.get("shortfall_raw")
    if se is None:
        return "blocked before any evidence can accrue"
    return ("needs %s more effective independent observations (%s more "
            "matured rows)" % (se, sr))
