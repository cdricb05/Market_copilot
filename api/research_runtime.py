"""api.research_runtime - Release 52 runtime health read model (read-only).

ONE authoritative view of the persistent prospective research runtime: when
it last ran, what it emitted, what it scored, what was forfeited, whether the
forward chains are intact, when the next invocation is expected, and where
the promotion frontier stands. Served by ``GET /v1/research/runtime-health``.

Read-only composition over the artifacts the R52 runtime owners already
wrote. It computes nothing, schedules nothing, emits nothing and can mutate
nothing - operational or research.
"""
from __future__ import annotations

OWNER = "api.research_runtime"
ROUTE = "/v1/research/runtime-health"


def load_runtime_health() -> dict:
    from paper_trader.alpha_agent.r52 import forfeiture as FF
    from paper_trader.alpha_agent.r52 import runtime as RT
    from paper_trader.alpha_agent.r52 import timing_contract as TC

    health = RT.load_health()
    runs = RT.load_runs()
    forf = FF.load()
    contract = TC.load()

    if not health:
        return {
            "owner": OWNER,
            "route": ROUTE,
            "state": "RUNTIME_NEVER_RAN",
            "statement": "no R52 research runtime cycle has completed on "
                         "this machine yet; install and validate the "
                         "PaperTrader-ResearchRuntime scheduled task "
                         "(scripts/install_research_runtime_task.ps1)",
            "runtime_health": None,
            "research_only": True,
        }
    return {
        "owner": OWNER,
        "route": ROUTE,
        "state": health.get("runtime_state"),
        "runtime_health": health,
        "recent_runs": (runs.get("runs") or [])[-10:],
        "n_runs_total": runs.get("n_runs_total"),
        "forfeitures": {
            "n_total": forf.get("n_total_forfeitures"),
            "n_cells_lost_total": forf.get("n_cells_lost_total"),
            "by_lane": forf.get("by_lane"),
            "backfill_refused_on_every_row": forf.get(
                "backfill_refused_on_every_row"),
        },
        "timing_contract": {
            "derived_at_utc": contract.get("derived_at_utc"),
            "n_lanes": contract.get("n_lanes"),
            "invocation_plan": contract.get("invocation_plan"),
            "emission_policy_now": contract.get("emission_policy_now"),
        },
        "safety": {
            "calls_portfolio_cycle": False,
            "runs_daily_close": False,
            "promotes_models": False,
            "backfills_forward_rows": False,
            "manual_review_remains_mandatory": True,
        },
        "research_only": True,
    }


__all__ = ["OWNER", "ROUTE", "load_runtime_health"]
