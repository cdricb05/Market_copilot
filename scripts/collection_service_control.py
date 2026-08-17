"""
scripts/collection_service_control.py — control helper for the collection service.

The single JSON contract between ``scripts/manage_information_collection.ps1``
and ``api.information_collection``. It exists so the PowerShell owner never has
to embed inline Python or a here-string: every state read and every state flip
goes through one file with one argument contract.

Actions
    status      emit the service/source/credential state as ONE JSON object
    preflight   prove the worker's owners import and the cadence policy resolves,
                WITHOUT calling any provider; emit a human-readable report
    enable      arm INFORMATION COLLECTION automation (execution stays OFF)
    disable     disarm information collection automation
    mark-stopped record a clean shutdown after the manager stopped the worker

Never prints a credential value. Never creates an order. Never starts a worker.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.api import information_collection as ic  # noqa: E402
from paper_trader.engine import collection_cadence as cad  # noqa: E402


def _tail(path: Path, lines: int = 12) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    return [ln[:300] for ln in text.splitlines()[-int(lines):]]


def _credential_summary(creds: dict) -> str:
    required = [(sid, row) for sid, row in (creds.get("sources") or {}).items()
                if row.get("required")]
    present = [sid for sid, row in required if row.get("available")]
    missing = [sid for sid, row in required if not row.get("available")]
    parts = []
    if present:
        parts.append("visible: " + ", ".join(sorted(present)))
    if missing:
        parts.append("NOT visible: " + ", ".join(sorted(missing)))
    return "; ".join(parts) or "no source requires a credential"


def action_status(root=None) -> int:
    now = ic.now_utc()
    service = ic.load_service_state(root=root)
    lock = ic.read_service_lock(root=root)
    lifecycle = ic.resolve_service_lifecycle(service, lock, now)
    creds = ic.credential_availability()
    health = ic.build_source_runtime_health(root=root, now=now, credentials=creds)
    summary = health["summary"]
    logs = ic.logs_root(root)
    stdout_path = _REPO / "paper_trader_collection.stdout.log"
    stderr_path = _REPO / "paper_trader_collection.stderr.log"
    payload = {
        "service_state": lifecycle["service_state"],
        "reason": lifecycle["reason"],
        "collection_automation_enabled": bool(
            service.get("collection_automation_enabled")),
        "execution_automation_enabled": False,
        "broker_execution_enabled": False,
        "instance_id": service.get("instance_id"),
        "worker_pid": lifecycle["worker_pid"],
        "worker_alive": lifecycle["worker_alive"],
        "lock_held": bool(lock),
        "lock_pid": (lock or {}).get("pid"),
        "lock_instance_id": (lock or {}).get("instance_id"),
        "heartbeat_at": service.get("heartbeat_at"),
        "heartbeat_age_seconds": lifecycle["heartbeat_age_seconds"],
        "loop_count": service.get("loop_count"),
        "restart_count": service.get("restart_count"),
        "last_iteration_finished_at": service.get("last_iteration_finished_at"),
        "last_collection_success_at": service.get("last_collection_success_at"),
        "last_material_information_at": service.get("last_material_information_at"),
        "next_wake_at": service.get("next_wake_at"),
        "due_now": summary["due_now"],
        "healthy_due": summary["healthy_due"],
        "not_due": summary["not_due"],
        "backoff": summary["backoff"],
        "degraded": summary["degraded"],
        "failed": summary["failed"],
        "blocked": summary["blocked"],
        "disabled": summary["disabled"],
        "research_only": summary["research_only"],
        "market_session_phase": health["market_session"]["phase"],
        "store_root": str(ic.collection_root(root)),
        "cadence_policy_id": cad.CADENCE_POLICY_VERSION,
        "credential_summary": _credential_summary(creds),
        "source_state_summary": "; ".join(
            "%s=%s" % (r["source_id"], r["runtime_state"])
            for r in health["sources"]),
        "log_tail": _tail(logs / "collection_service.log"),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "stdout_tail": _tail(stdout_path),
        "stderr_tail": _tail(stderr_path),
    }
    print(json.dumps(payload, indent=1, default=str))
    return 0


def action_preflight(root=None) -> int:
    """Prove the worker can run WITHOUT calling any provider."""
    problems: list[str] = []
    print("[preflight] cadence policy version : %s" % cad.CADENCE_POLICY_VERSION)
    print("[preflight] sources with a policy  : %d" % len(cad.CADENCE_POLICY_TABLE))
    print("[preflight] scheduled sources      : %d (%s)"
          % (len(cad.SCHEDULED_SOURCE_IDS), ", ".join(cad.SCHEDULED_SOURCE_IDS)))

    # Every source in the capability registry must have a cadence policy.
    from paper_trader.api import source_capability as scap
    missing = [s["source_id"] for s in scap.SOURCE_REGISTRY
               if cad.policy_for(s["source_id"]) is None]
    if missing:
        problems.append("sources without a cadence policy: %s" % ", ".join(missing))
    print("[preflight] sources without policy : %d" % len(missing))

    # Every scheduled source must resolve to a collector lane.
    orphan = [s for s in cad.SCHEDULED_SOURCE_IDS
              if s not in ic.COLLECTOR_LANE]
    if orphan:
        problems.append("due sources without a collector: %s" % ", ".join(orphan))
    print("[preflight] due without collector  : %d" % len(orphan))

    # The Release-28 orchestrator must be importable and unforked.
    from paper_trader.api import event_signal_refresh as esr
    print("[preflight] event orchestrator     : %s" % esr.COMPOSITION_OWNER)

    now = ic.now_utc()
    health = ic.build_source_runtime_health(root=root, now=now)
    summary = health["summary"]
    print("[preflight] market session         : %s" % health["market_session"]["phase"])
    print("[preflight] source health          : %s" % summary["headline"])
    creds = ic.credential_availability()
    print("[preflight] credentials            : %s" % _credential_summary(creds))
    print("[preflight] store root             : %s" % ic.collection_root(root))

    safety = ic.collection_safety_contract()
    for key in ("execution_automation_enabled", "broker_execution_enabled",
                "creates_orders", "promotes_models", "runs_daily_close"):
        if safety.get(key):
            problems.append("SAFETY VIOLATION: %s is true" % key)
    print("[preflight] execution automation   : %s"
          % safety["execution_automation_enabled"])
    print("[preflight] broker execution       : %s"
          % safety["broker_execution_enabled"])

    try:
        ic.collection_root(root).mkdir(parents=True, exist_ok=True)
        print("[preflight] state root writable    : True")
    except OSError as exc:
        problems.append("state root not writable: %s" % exc)

    if problems:
        for p in problems:
            print("[preflight] BLOCKER: %s" % p)
        return 1
    print("[preflight] OK")
    return 0


def action_enable(root=None) -> int:
    res = ic.set_collection_automation(
        enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
        requested_by="manage_information_collection.ps1", root=root)
    print(json.dumps({"collection_automation_enabled":
                      res.get("collection_automation_enabled"),
                      "enabled_at": res.get("enabled_at"),
                      "execution_automation_enabled": False}, indent=1))
    return 0 if res.get("ok") else 1


def action_disable(root=None) -> int:
    res = ic.set_collection_automation(enabled=False, root=root)
    print(json.dumps({"collection_automation_enabled":
                      res.get("collection_automation_enabled")}, indent=1))
    return 0 if res.get("ok") else 1


def action_mark_stopped(root=None) -> int:
    state = ic.load_service_state(root=root)
    state["stopped_at"] = ic.utc_iso()
    state["graceful_shutdown"] = True
    state["current_source"] = None
    ic.save_service_state(state, root=root)
    lock_path = ic.collection_root(root) / "collection_service.lock"
    try:
        if lock_path.exists():
            lock_path.unlink()
    except OSError:
        pass
    print(json.dumps({"stopped_at": state["stopped_at"],
                      "graceful_shutdown": True}, indent=1))
    return 0


_ACTIONS = {
    "status": action_status,
    "preflight": action_preflight,
    "enable": action_enable,
    "disable": action_disable,
    "mark-stopped": action_mark_stopped,
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Collection-service control helper (read/flip state only).")
    ap.add_argument("--action", required=True, choices=sorted(_ACTIONS))
    ap.add_argument("--root", default=None)
    args = ap.parse_args(argv)
    try:
        return _ACTIONS[args.action](root=args.root)
    except Exception as exc:  # noqa: BLE001 - one clean line for the PS owner
        print("CONTROL_ERROR %s: %s" % (type(exc).__name__, str(exc)[:300]))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
