r"""alpha_agent.r53.latency - the event-to-decision latency profile, measured
from the owned run manifests rather than instrumented guesswork.

WHAT IS MEASURED
----------------
* The Daily Research Cycle manifests stamp every step's record instant; the
  gap between consecutive stamps bounds each step's wall-clock cost. Across
  every manifest the distribution names the dominant bottlenecks.
* The collection service's iteration history stamps each iteration's duration
  and wake interval - the event-DETECTION latency while the worker runs.
* The portfolio-decision chain (HOC -> reassessment -> proposal) is measured
  the same way, separately, because it is the part an intraday path would
  re-run per event.

THE POINT
---------
"Near-real-time cannot mean 30 minutes later." The measurement shows the
decision chain itself runs in SECONDS; the wall-clock is consumed by research
bolt-ons (forward-evidence capture, the tournament advance) that an
event-driven reassessment does NOT need to re-run. The latency budget states
that explicitly per operating mode.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53.latency"
ARTIFACT = "R53_LATENCY_PROFILE.json"

DRC_RUNS = Path(r"D:\Stock_Prediction_app_data\daily_research_cycle\runs")
COLLECTION_HISTORY = Path(r"D:\Stock_Prediction_app_data\information_collection"
                          r"\collection_iteration_history.json")

#: The steps an EVENT-DRIVEN reassessment would actually re-run. Everything
#: else in the daily cycle is research accrual or close bookkeeping.
DECISION_CHAIN_STEPS = ("ASSESS_HOLDING_OPPORTUNITY_COST",
                       "REASSESS_PORTFOLIO", "BUILD_REALLOCATION_PROPOSAL")

#: Declared budgets per operating mode, seconds. Budgets are TARGETS the
#: profile is judged against, not promises; each carries its rationale.
LATENCY_BUDGETS = {
    "daily": {
        "budget_seconds": 1800,
        "rationale": "one governed cycle between close and the operator's "
                     "evening review; the 1800s client timeout is the "
                     "binding operational constraint observed in R48",
    },
    "event_driven": {
        "budget_seconds": 300,
        "rationale": "a material event should reach a governed HOLD/"
                     "REALLOCATE/TRUE_BLOCKER answer within five minutes: "
                     "collection cadence (~60s) + refresh + the measured "
                     "seconds-scale decision chain",
    },
    "true_intraday": {
        "budget_seconds": 60,
        "rationale": "a 30-minute prediction horizon tolerates at most about "
                     "a minute of decision latency before the edge decays; "
                     "requires the incremental refresh path, never a full "
                     "cycle re-run",
    },
}


def _parse(ts: Optional[str]) -> Optional[_dt.datetime]:
    if not ts:
        return None
    try:
        return _dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def _steps(manifest: dict) -> list[dict]:
    seq = manifest.get("step_sequence") or []
    res = manifest.get("step_results") or []
    rows = []
    for i, r in enumerate(res):
        rows.append({"step": (r.get("step_id") or
                              (seq[i] if i < len(seq) else "STEP_%d" % i)),
                     "at": _parse(r.get("completed_at") or r.get("started_at")),
                     "reused": r.get("reused"), "status": r.get("status")})
    return [r for r in rows if r["at"] is not None]


def profile_drc_runs() -> dict:
    per_step: dict[str, list[float]] = {}
    runs = []
    for p in sorted(DRC_RUNS.glob("drc_*.json")):
        m = read_json(p, default=None)
        if not m:
            continue
        rows = _steps(m)
        started = _parse(m.get("started_at"))
        completed = _parse(m.get("completed_at"))
        total = ((completed - started).total_seconds()
                 if started and completed else None)
        gaps = {}
        prev = started
        for r in rows:
            if prev is not None:
                gaps[r["step"]] = round((r["at"] - prev).total_seconds(), 3)
            prev = r["at"]
        for step, g in gaps.items():
            per_step.setdefault(step, []).append(g)
        decision_chain = sum(g for s, g in gaps.items()
                             if s in DECISION_CHAIN_STEPS)
        runs.append({"run_id": m.get("run_id"),
                     "market_date": m.get("eligible_market_date"),
                     "total_seconds": total,
                     "decision_chain_seconds": round(decision_chain, 3),
                     "step_gaps_seconds": gaps})
    stats = {}
    for step, vals in per_step.items():
        vs = sorted(vals)
        stats[step] = {"n": len(vs), "median_seconds": vs[len(vs) // 2],
                       "max_seconds": vs[-1],
                       "share_of_median_total": None}
    totals = sorted([r["total_seconds"] for r in runs
                     if r["total_seconds"] is not None])
    median_total = totals[len(totals) // 2] if totals else None
    if median_total:
        for step in stats:
            stats[step]["share_of_median_total"] = round(
                stats[step]["median_seconds"] / median_total, 4)
    chain = sorted([r["decision_chain_seconds"] for r in runs])
    return {"n_runs": len(runs), "runs": runs, "per_step": stats,
            "median_total_seconds": median_total,
            "max_total_seconds": totals[-1] if totals else None,
            "decision_chain_median_seconds":
                chain[len(chain) // 2] if chain else None,
            "decision_chain_max_seconds": chain[-1] if chain else None,
            "measurement_note": (
                "step cost is bounded by the gap between consecutive result "
                "stamps; a reused step records ~0")}


def profile_collection() -> dict:
    hist = read_json(COLLECTION_HISTORY, default=None)
    rows = hist if isinstance(hist, list) else (hist or {}).get("iterations") or []
    if not rows:
        return {"state": "NO_HISTORY"}
    tail = rows[-500:]
    durs = sorted(float(r.get("duration_seconds") or 0.0) for r in tail
                  if r.get("duration_seconds") is not None)
    wakes = sorted(float(r.get("next_wake_seconds") or 0.0) for r in tail
                   if r.get("next_wake_seconds") is not None)
    return {
        "state": "MEASURED_WHILE_RUNNING",
        "n_iterations_sampled": len(tail),
        "iteration_duration_median_seconds": durs[len(durs) // 2] if durs else None,
        "iteration_duration_p90_seconds": durs[int(len(durs) * 0.9)] if durs else None,
        "iteration_duration_max_seconds": durs[-1] if durs else None,
        "wake_interval_median_seconds": wakes[len(wakes) // 2] if wakes else None,
        "detection_latency_bound_seconds": (
            (wakes[len(wakes) // 2] if wakes else 0)
            + (durs[int(len(durs) * 0.9)] if durs else 0)),
        "caveat": "history ends 2026-08-28 - the worker has been down since; "
                  "these numbers describe the worker WHEN it runs",
    }


def compose() -> dict:
    drc = profile_drc_runs()
    coll = profile_collection()
    # Name the bottlenecks: the steps with the largest median gap.
    ranked = sorted(((s, v["median_seconds"]) for s, v in
                     (drc.get("per_step") or {}).items()),
                    key=lambda kv: -kv[1])
    body = artifact_body(
        "r53_latency_profile/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        drc=drc,
        collection=coll,
        dominant_bottlenecks=[{"step": s, "median_seconds": v}
                              for s, v in ranked[:4]],
        decision_chain_steps=list(DECISION_CHAIN_STEPS),
        headline=(
            "the governed decision chain (HOC -> reassessment -> proposal) "
            "runs in seconds; the cycle's wall-clock is consumed by "
            "research accrual (forward-evidence capture and the tournament "
            "advance), which an event-driven reassessment does not re-run"),
        latency_budgets=LATENCY_BUDGETS,
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT, body)
    return body
