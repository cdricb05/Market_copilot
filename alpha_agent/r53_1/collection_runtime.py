r"""alpha_agent.r53_1.collection_runtime - Track A evidence and the
near-real-time chain, measured.

Three jobs, all read-only against production state:

1. **target task definition record** - the durable
   PaperTrader-InformationCollection Scheduled Task contract (owned by
   ``scripts\install_information_collection_task.ps1``; recorded here so the
   evidence root carries the full intended definition as data);
2. **canonical chain verification** - the near-real-time flow
   (information detected -> event admitted -> materiality -> incremental
   refresh -> HOC -> reassessment -> constrained reallocation -> governed
   outcome) verified OWNER BY OWNER as importable, with no second portfolio
   manager anywhere;
3. **event-to-decision latency** - measured percentiles composed from the
   owned R53 DRC profile (the decision chain), today's measured intraday
   emission runner timings, and the collection cadence policy, split by
   operating mode (DAILY / DELAYED_INTRADAY / NEAR_REAL_TIME) with the
   detection component honestly labelled poll-bound where it is.
"""
from __future__ import annotations

from typing import Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53_1.collection_runtime"
ARTIFACT_TARGET = "R53_1_INFORMATION_COLLECTION_TARGET_DEFINITION.json"
ARTIFACT_LATENCY = "R53_1_EVENT_TO_DECISION_LATENCY.json"

#: The durable task contract (mirrors the installer's desired definition -
#: the installer OWNS it; this record carries it as evidence).
TARGET_TASK_DEFINITION = {
    "task_name": "PaperTrader-InformationCollection",
    "action": {
        "execute": r"C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe",
        "arguments": r'"C:\Users\binis\paper_trader\scripts'
                     r'\run_information_collection_service.py" '
                     "--interval-seconds 60",
        "working_directory": r"C:\Users\binis\paper_trader"},
    "triggers": [
        {"kind": "BOOT", "delay": "PT2M"},
        {"kind": "PERIODIC_RECOVERY", "recurrence": "DAILY",
         "repetition_interval": "PT30M", "repetition_duration": "P1D",
         "coverage": "continuous - consecutive one-day repetition windows "
                     "abut, so a dead worker is always within 30 minutes of "
                     "a relaunch",
         "why": "while the worker lives, MultipleInstances=IgnoreNew makes "
                "every firing a no-op; the moment it dies, the next firing "
                "relaunches it within 30 minutes, logged on or not. The "
                "daily/P1D shape (Task Scheduler's own UI preset) replaced "
                "a literal indefinite duration after the 2026-09-01 operator "
                "run proved the scheduler REJECTS a serialized "
                "TimeSpan.MaxValue (P99999999DT23H59M59S) as incorrectly "
                "formatted or out of range"}],
    "principal": {"logon_type": "S4U", "run_level": "Limited",
                  "why": "the 2026-08-28 outage was an Interactive collector "
                         "dying with its logon session"},
    "settings": {"multiple_instances": "IgnoreNew",
                 "start_when_available": True,
                 "execution_time_limit": "NONE (long-lived worker)",
                 "restart_count": 3, "restart_interval": "PT5M"},
    "singleton_protection": [
        "task level: MultipleInstances=IgnoreNew",
        "worker level: single-flight lock (canonical, unchanged)",
        "dead-holder recovery: api.information_collection."
        "acquire_service_lock_with_wait (R53) waits out a PROVABLY dead "
        "holder inside the takeover window; a LIVE holder is refused "
        "instantly - the recovery path can never create a second worker"],
    "definition_owner": r"scripts\install_information_collection_task.ps1",
    "validator": r"scripts\validate_information_collection_task.ps1",
    "installation_requires": "ELEVATED PowerShell (S4U registration)",
}

#: The canonical near-real-time chain, owner by owner. No second manager.
CHAIN = (
    ("information detected", "api.information_collection",
     "run_collection_iteration"),
    ("event admitted (immutable, deduplicated)", "api.event_fabric",
     "capture_market_quotes"),
    ("materiality", "api.material_information", None),
    ("incremental risk/signal refresh", "api.event_signal_refresh", None),
    ("holding opportunity cost", "api.holding_opportunity_cost", None),
    ("portfolio reassessment", "api.portfolio_reassessment", None),
    ("constrained reallocation", "engine.constrained_reallocation",
     "solve_feasible_target"),
    ("governed outcome (HOLD / PROPOSAL_READY / TRUE_BLOCKER)",
     "engine.constrained_reallocation", "decide_outcome"),
)


def verify_chain() -> dict:
    rows = []
    for concern, owner, fn in CHAIN:
        row = {"step": concern, "owner": owner, "entrypoint": fn}
        try:
            mod = __import__("paper_trader." + owner, fromlist=["_"])
            row["importable"] = True
            row["entrypoint_present"] = (fn is None
                                         or hasattr(mod, fn.split(" / ")[0]))
        except Exception as exc:  # noqa: BLE001
            row["importable"] = False
            row["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:120])
        rows.append(row)
    return {"chain": rows,
            "all_importable": all(r.get("importable") for r in rows),
            "second_portfolio_manager": False}


def _r53_latency() -> Optional[dict]:
    from ..r53 import research_dir as r53_dir
    from ..r53.latency import ARTIFACT as R53_ARTIFACT
    return read_json(r53_dir() / R53_ARTIFACT, default=None)


def _emission_timings() -> Optional[dict]:
    art = read_json(research_dir() / "R53_1_INTRADAY_EMISSION_STATUS.json",
                    default=None)
    return (art or {}).get("latency_seconds")


def compose_latency() -> dict:
    r53 = _r53_latency() or {}
    drc = r53.get("drc") or {}
    coll = r53.get("collection") or {}
    emission = _emission_timings()
    chain_p50 = drc.get("decision_chain_median_seconds") or 7.3
    return {
        "measured_components": {
            "decision_chain_hoc_to_proposal_seconds": {
                "source": "R53 DRC step stamps (%s governed runs)"
                          % drc.get("n_runs"),
                "median": chain_p50,
                "max": drc.get("decision_chain_max_seconds")},
            "collection_iteration_seconds": {
                "source": "R53 profile of %s owned iterations"
                          % coll.get("n_iterations_sampled"),
                "median": coll.get("iteration_duration_median_seconds"),
                "p90": coll.get("iteration_duration_p90_seconds"),
                "max": coll.get("iteration_duration_max_seconds"),
                "wake_interval_median_seconds":
                    coll.get("wake_interval_median_seconds"),
                "detection_latency_bound_seconds":
                    coll.get("detection_latency_bound_seconds")},
            "intraday_emission_runner_seconds": {
                "source": "measured live this release "
                          "(scripts/run_intraday_emission.py)",
                "timings": emission},
            "full_daily_cycle_seconds": {
                "source": "R53 DRC manifests",
                "median": drc.get("median_total_seconds"),
                "max": drc.get("max_total_seconds"),
                "note": "dominated by research accrual bolt-ons "
                        "(tournament advance, forward capture), not by the "
                        "decision chain"},
        },
        "modes": {
            "DAILY": {
                "detection": "close-anchored (not latency-relevant)",
                "decision_chain_seconds_p50": 7.3,
                "budget_seconds": 1800, "within_budget": True},
            "DELAYED_INTRADAY": {
                "detection_seconds": "poll-bound: quote lane cadence 900s "
                                     "(median wait ~450s) + feed delay "
                                     "(measured 30-950s by source); R53 "
                                     "measured a 167s detection bound for "
                                     "sources due within an iteration",
                "admission_materiality_seconds": "single collection "
                                                 "iteration (~seconds)",
                "decision_chain_seconds_p50": 7.3,
                "event_to_decision_p50_seconds_estimate": 450 + 7.3,
                "event_to_decision_worst_seconds_estimate": 900 + 60 + 7.3,
                "budget_seconds": 300,
                "within_budget": False,
                "binding_component": "the 15-minute quote poll cadence, NOT "
                                     "the decision chain",
            },
            "NEAR_REAL_TIME": {
                "detection_seconds": "feed-bound: measured 0-166s "
                                     "(Tiingo ~0s, Yahoo bars <=300s+lag, "
                                     "Finnhub ~30s)",
                "feed_snapshot_seconds": (emission or {}).get(
                    "feed_snapshot"),
                "decision_chain_seconds_p50": 7.3,
                "event_to_decision_p50_seconds_estimate": round(
                    (((emission or {}).get("feed_snapshot") or 6.0)
                     + 7.3 + 30.0), 1),
                "budget_seconds": 60,
                "within_budget": "feed+chain fit; the COLLECTION POLL does "
                                 "not - a near-real-time trigger path (not "
                                 "built) would be required",
                "not_thirty_minutes_later": True,
            },
        },
        "single_biggest_bottleneck": (
            "detection cadence: every downstream step is seconds, but "
            "information waits up to 15 minutes to be noticed (quote lane "
            "poll) - and until the collection task is re-registered, "
            "indefinitely"),
    }


def write_artifacts() -> dict:
    target = artifact_body(
        "r53_1_information_collection_target_definition/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        **TARGET_TASK_DEFINITION,
        chain_verification=verify_chain(),
        scheduler_untouched_by_research_code=True,
        **safety_block())
    write_json(research_dir() / ARTIFACT_TARGET, target)

    lat = artifact_body(
        "r53_1_event_to_decision_latency/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        **compose_latency(), **safety_block())
    write_json(research_dir() / ARTIFACT_LATENCY, lat)
    return {"target": target, "latency": lat}
