r"""alpha_agent.r53.runtime_status - Track B: what is ACTUALLY running?

The release was told: "we believe PaperTrader-InformationCollection was not
operating correctly - do not assume, inspect." This module composes the
read-only evidence into ONE runtime-status artifact:

* the collection service's own state files (heartbeat, lock, log tail) -
  read from the canonical collection root, never modified;
* the read-only Windows Scheduled Task snapshot the release captured into the
  evidence root (PowerShell ``Get-ScheduledTask`` - this module never touches
  the scheduler);
* the ownership map of the event-driven path, each owner verified importable
  so the map cannot rot silently.

THE R53 DIAGNOSIS (recorded here so the artifact carries it verbatim)
---------------------------------------------------------------------
The collection worker ran CONTINUOUSLY from 2026-08-21 15:15Z to 2026-08-28
17:51Z (loop_count 2512, 7/7 sources healthy). At the 2026-08-28 logon the
Scheduled Task relaunched it; the new instance found the dead worker's lock
with a heartbeat only ~100 seconds old, and the acquire rule - reclaim ONLY
when the heartbeat is silent past the 900s takeover window AND the pid is
gone - refused the slot (exit 3, ``SINGLE_FLIGHT_LOCK_HELD``). Because the
task's ONLY trigger is a LogonTrigger, a refused start is terminal: nothing
retries. Collection has been DOWN since, silently.

Two defects, two remediations:
1. CODE (fixed in R53): the worker start is now a bounded WAIT when the
   holder's pid is provably dead - single-flight is preserved, a logon race
   self-heals (``api.information_collection.acquire_service_lock_with_wait``).
2. SCHEDULER (operator action, elevated shell; R53 changes no task): the task
   needs restart-on-failure / periodic triggers and the S4U principal, i.e.
   the same lifecycle treatment R52 gave PaperTrader-ResearchRuntime.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from . import (CAMPAIGN_ID, EVIDENCE_ROOT, RELEASE, artifact_body, read_json,
               research_dir, safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53.runtime_status"
ARTIFACT = "R53_INTRADAY_RUNTIME_STATUS.json"

COLLECTION_ROOT = Path(r"D:\Stock_Prediction_app_data\information_collection")
SCHEDULER_SNAPSHOT = EVIDENCE_ROOT / "scheduler_snapshot_readonly.json"

#: concern -> canonical owner module. Verified importable at compose time.
OWNERSHIP_MAP = {
    "event fabric": "paper_trader.engine.event_fabric",
    "event fabric composition/routes": "paper_trader.api.event_fabric",
    "material information": "paper_trader.api.material_information",
    "event materiality policy": "paper_trader.engine.event_materiality",
    "information collection (composition)":
        "paper_trader.api.information_collection",
    "collection cadence/backoff": "paper_trader.engine.collection_cadence",
    "continuous collection worker (process)":
        "scripts/run_information_collection_service.py",
    "intraday/delayed quotes + news + SEC + halts (collectors)":
        "paper_trader.api.information_collection (composed collectors)",
    "market context": "paper_trader.api.market_reference_data",
    "incremental signal refresh": "paper_trader.api.event_signal_refresh",
    "holding opportunity cost": "paper_trader.engine.holding_opportunity_cost",
    "portfolio reassessment": "paper_trader.engine.portfolio_reassessment",
    "reallocation proposal": "paper_trader.engine.reallocation_proposal",
    "constraint + switching owner":
        "paper_trader.engine.constrained_reallocation",
    "workflow next-action": "paper_trader.api.workflow_state",
    "portfolio cycle orchestration": "paper_trader.api.portfolio_cycle",
    "prospective daily evidence": "paper_trader.alpha_agent.r46 (ledger/emit)",
    "persistent research runtime": "paper_trader.alpha_agent.r52.runtime",
    "prospective intraday evidence (R53)":
        "paper_trader.alpha_agent.r53.intraday_factory",
}

#: The current authority boundary, restated so no reader can mistake shadow
#: research for production authority.
AUTHORITY_MODEL = {
    "intraday/delayed price movement": "RISK authority (may trigger "
                                       "reassessment; never a return forecast)",
    "news / filings / earnings / halts": "TRIGGER authority (materiality -> "
                                         "reassess through canonical owners)",
    "expected_return authority": "the approved operational model ONLY; no "
                                 "R53 lane carries it; promotion gates are "
                                 "the sole path and they are manual",
}


def _verify_owners() -> dict:
    import importlib
    out = {}
    for concern, owner in OWNERSHIP_MAP.items():
        mod = owner.split(" ")[0]
        if mod.startswith("scripts/"):
            out[concern] = {"owner": owner,
                            "verified": (Path(r"C:\Users\binis\paper_trader")
                                         / mod).exists()}
            continue
        try:
            importlib.import_module(mod)
            out[concern] = {"owner": owner, "verified": True}
        except Exception as exc:  # noqa: BLE001
            out[concern] = {"owner": owner, "verified": False,
                            "error": str(exc)[:160]}
    return out


def _collector_evidence() -> dict:
    state = read_json(COLLECTION_ROOT / "collection_service_state.json",
                      default={}) or {}
    lock = read_json(COLLECTION_ROOT / "collection_service.lock",
                     default={}) or {}
    log_tail = []
    log = COLLECTION_ROOT / "logs" / "collection_service.log"
    try:
        lines = log.read_text(encoding="utf-8").splitlines()
        log_tail = lines[-3:]
    except OSError:
        pass
    return {
        "service_state": {k: state.get(k) for k in
                          ("heartbeat_at", "loop_count", "restart_count",
                           "last_iteration_finished_at",
                           "last_collection_success_at",
                           "last_event_cycle_state", "last_reassessment_at",
                           "collection_automation_enabled", "pid",
                           "started_at", "stopped_at")},
        "lock": {k: lock.get(k) for k in
                 ("acquired_at", "heartbeat_at", "pid", "instance_id")},
        "log_tail": log_tail,
    }


def _read_snapshot() -> Optional[dict]:
    # PowerShell 5.1 Out-File writes a UTF-8 BOM; tolerate it.
    import json
    try:
        return json.loads(SCHEDULER_SNAPSHOT.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def compose(*, scheduler_snapshot: Optional[dict] = None,
            collector_process_running: Optional[bool] = None) -> dict:
    snap = scheduler_snapshot or _read_snapshot()
    tasks = {t.get("task_name"): t for t in (snap or {}).get("tasks") or []}
    ic_task = tasks.get("PaperTrader-InformationCollection") or {}
    rr_task = tasks.get("PaperTrader-ResearchRuntime") or {}
    running = (collector_process_running
               if collector_process_running is not None
               else (snap or {}).get("collector_process_running"))
    ev = _collector_evidence()
    hb = str(ev["service_state"].get("heartbeat_at") or "")

    body = artifact_body(
        "r53_intraday_runtime_status/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        continuous_collection_actually_running=bool(running),
        collection_worker={
            "scheduled_task": {
                "state": ic_task.get("state"),
                "last_run": ic_task.get("last_run"),
                "last_result_code": ic_task.get("last_result"),
                "next_run": ic_task.get("next_run") or "NONE - LogonTrigger only",
                "principal_logon_type": ic_task.get("principal_logon"),
                "triggers": ic_task.get("triggers"),
                "action": ic_task.get("action"),
            },
            "evidence": ev,
            "last_heartbeat_at": hb,
            "diagnosis": (
                "DOWN since 2026-08-28T17:53Z: the logon relaunch was "
                "refused (SINGLE_FLIGHT_LOCK_HELD, exit 3) because the dead "
                "worker's heartbeat was younger than the 900s takeover "
                "window, and the LogonTrigger-only task never retries. The "
                "worker itself was healthy until the logoff killed it."),
            "stale": True,
            "superseded": False,
            "canonical_owner": "api.information_collection + "
                               "scripts/run_information_collection_service.py",
        },
        research_runtime={
            "scheduled_task": {
                "state": rr_task.get("state"),
                "last_run": rr_task.get("last_run"),
                "last_result_code": rr_task.get("last_result"),
                "next_run": rr_task.get("next_run"),
                "principal_logon_type": rr_task.get("principal_logon"),
            },
            "assessment": "OPERATING (R52); do not break it",
        },
        remediation={
            "code_fixed_in_r53": (
                "api.information_collection.acquire_service_lock_with_wait - "
                "bounded wait + dead-pid takeover keeps single-flight and "
                "makes a logon race self-healing; the worker script now "
                "starts through it"),
            "operator_actions_required": [
                "re-register PaperTrader-InformationCollection with "
                "restart-on-failure and periodic triggers (mirror the R52 "
                "installer pattern; elevated PowerShell)",
                "migrate the task principal to S4U so a logged-out machine "
                "still collects (same blocker R52 recorded for the research "
                "runtime task)",
                "until then: after any reboot/logon, confirm the collector "
                "process exists or start it manually",
            ],
            "scheduler_untouched_by_r53": True,
        },
        ownership_map=_verify_owners(),
        authority_model=dict(AUTHORITY_MODEL),
        scheduler_snapshot_source=str(SCHEDULER_SNAPSHOT),
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT, body)
    return body
