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


#: Release 53.1 emission-status artifact name (written by
#: scripts/run_intraday_emission.py, the PaperTrader-IntradayEmission task).
INTRADAY_EMISSION_ARTIFACT = "R53_1_INTRADAY_EMISSION_STATUS.json"


def load_intraday_emission_status() -> dict:
    """Release 53.1 intraday prospective-emission lane — READ-ONLY status.

    R54 finalization: the ONE API read surface for the intraday prospective
    evidence lane. These are RESEARCH ledger rows (``forward_evidence_type``
    TRUE_FORWARD with ``evidence_class`` PROSPECTIVE_INTRADAY, chain-hashed by
    the frozen R53 factory) and are NEVER the daily governed TRUE_FORWARD
    bundle owned by ``api.forward_prediction_skill`` — the two evidence
    identities are distinct and must never be summed or interchanged.

    Reads only what the R53.1 owners already wrote (the emission-status
    artifact and the factory's ledgers). Creates no directory, writes nothing,
    emits nothing, scores nothing, and never touches the scheduler.
    """
    import json as _json

    from paper_trader.alpha_agent import r53_1 as R1
    from paper_trader.alpha_agent.r53 import intraday_factory as factory

    base = {
        "owner": OWNER,
        "lane_owner": "alpha_agent.r53.intraday_factory (frozen R53 specs)",
        "entrypoint": ("scripts/run_intraday_emission.py "
                       "(PaperTrader-IntradayEmission scheduled task)"),
        "evidence_class": "PROSPECTIVE_INTRADAY",
        "forward_evidence_type": "TRUE_FORWARD",
        "distinct_from_daily_governed_true_forward": True,
        "daily_governed_bundle_owner": "api.forward_prediction_skill",
        "research_only": True,
    }
    # Path composed WITHOUT the owners' mkdir helpers: a read surface must not
    # create research directories as a side effect of a GET.
    art = R1.RESEARCH_ROOT / R1.CAMPAIGN_ID / INTRADAY_EMISSION_ARTIFACT
    if not art.exists():
        return dict(base, state="NO_EMISSION_ATTEMPT_RECORDED",
                    statement=("no R53.1 intraday emission attempt has been "
                               "recorded on this machine yet"))
    try:
        body = _json.loads(art.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return dict(base, state="STATUS_ARTIFACT_UNREADABLE",
                    statement=str(exc)[:200])
    emission = body.get("emission") or {}
    last_emission = None
    try:
        for row in reversed(factory.predictions()):
            if row.get("emitted_at_utc"):
                last_emission = {
                    "emitted_at_utc": row.get("emitted_at_utc"),
                    "slot_utc": row.get("slot_utc"),
                    "forward_evidence_type": row.get("forward_evidence_type"),
                    "evidence_class": row.get("evidence_class"),
                }
                break
    except Exception as exc:  # noqa: BLE001 - a read surface never crashes
        last_emission = {"error": str(exc)[:160]}
    return dict(
        base,
        state=emission.get("state") or "UNKNOWN",
        last_attempt_at_utc=body.get("attempted_at_utc"),
        last_attempt_appended=emission.get("n_appended"),
        lane_state=body.get("lane_state"),
        last_emission=last_emission,
        ledger_totals=body.get("ledger_totals"),
        scoring=body.get("scoring"),
    )


__all__ = ["OWNER", "ROUTE", "INTRADAY_EMISSION_ARTIFACT",
           "load_runtime_health", "load_intraday_emission_status"]
