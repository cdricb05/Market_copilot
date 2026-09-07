r"""api/alphaagent_outcomes.py - Release 60: the ONE AlphaAgent RESEARCH-OUTCOMES
read model (strictly read-only).

    load_alphaagent_outcomes()  ->  GET /v1/research/alphaagent-outcomes

WHY IT EXISTS
-------------
Release 59 made the researcher persistent. It did not make the research
LEGIBLE. The only thing the estate could say about a running AlphaAgent was a
process line::

    worker=RESEARCHING  queue=141  hypotheses=4653

which answers none of the questions an operator actually has. A healthy
process is not successful research, and the two were the same green word.
Worse, there was no API surface over the R59 persistent memory at all: the
research record existed only in SQLite on disk, and every question about it
had to be answered by a person opening a database.

This module is that surface, and nothing more. It answers, from persisted
state only:

    what has AlphaAgent tested, and what did it reject?
    what is statistically interesting but not qualified, and which gate refused it?
    what has been frozen prospectively, and is anything accruing TRUE_FORWARD evidence?
    has anything reached governed-review quality?
    what data opportunities exist, and is a purchase actually recommended?
    what is it researching right now, why, and what will it try next?
    is research BLOCKED, or merely waiting for new information?
    and what changed since yesterday?

WHAT IT OWNS
------------
Presentation. One projection, one vocabulary, one place. It owns NO research
state: it opens the canonical R59 memory and the canonical Stage-8 queue
through their READ-ONLY handles (``r59.memory.open_memory_readonly`` /
``r59.loop.open_queue(read_only=True)``), which refuse every mutation and
create no directory, schema row or artifact.

WHAT IT MUST NEVER DO, and the reason each one matters
------------------------------------------------------
* It never calls ``r59.governor.generate_mandates``, ``r59.frontier.measure``
  or ``r59.report.build``. Those are the RIGHT owners of what to research
  next - and every one of them WRITES (a frontier row, a capacity fingerprint,
  an event, an artifact). A GET that generates mandates would make reading the
  dashboard change the research plan.
* It computes no statistic, no gate, no verdict and no economics. Every
  t-statistic, p-value, materiality, robustness check and refusing gate is
  read back exactly as ``alpha_agent.r57.engine`` recorded it through
  ``alpha_agent.r59.handlers``.
* It never mints forward evidence. TRUE_FORWARD maturation belongs to
  ``alpha_agent.r46`` / ``alpha_agent.r52`` and is read through the canonical
  api owners (``api.research_runtime``, ``api.prospective_tournament``). A
  historical result is never displayed as a forward one.
* It promotes nothing, approves nothing, allocates nothing and orders nothing.
  ``CHALLENGER_WARRANTS_GOVERNED_REVIEW`` is a sentence addressed to a human.

HONESTY RULES ENFORCED HERE RATHER THAN IN THE BROWSER
------------------------------------------------------
* PROCESS HEALTH IS NOT RESEARCH SUCCESS. ``runtime`` and ``governance`` are
  separate blocks with separate vocabularies, and the operator-facing headline
  is the EVIDENCE state, never the worker state.
* A window count says WHAT IT COUNTED. ``settled_at`` is the instant this
  memory recorded a verdict, which for an imported prior-release result is the
  import instant - so every window splits ``measured_here`` from
  ``imported_from_prior_release``. A backfill is not a day's research.
* A PROSPECTIVE FREEZE IS NOT FORWARD EVIDENCE. A freeze that no forward
  owner has adopted accrues nothing, and says so
  (``NOT_REGISTERED_WITH_FORWARD_EVIDENCE_OWNER``) rather than showing a
  hopeful zero.
* ANYTHING NOT DERIVABLE IS ``NOT_AVAILABLE`` with a reason. Nothing is
  inferred, defaulted or filled in.
"""
from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Optional

OWNER = "api.alphaagent_outcomes"
ROUTE = "/v1/research/alphaagent-outcomes"
RELEASE = "R60"
SCHEMA_VERSION = "r60_alphaagent_outcomes/1"

#: Nothing is derived, defaulted or guessed. A value this module cannot read
#: from an owner is this token plus a reason.
NOT_AVAILABLE = "NOT_AVAILABLE"

# --------------------------------------------------------------------------- #
# The governance vocabulary. Six terminal words, each meaning exactly one
# thing, in strict precedence order. The operator reads THIS - not a badge
# coloured by whether a process is running.
# --------------------------------------------------------------------------- #
G_MEMORY_ABSENT = "RESEARCH_MEMORY_NOT_PRESENT"
G_CHALLENGER_REVIEW = "CHALLENGER_WARRANTS_GOVERNED_REVIEW"
G_FORWARD_MATURING = "FORWARD_EVIDENCE_MATURING"
G_HISTORICAL_ONLY = "HISTORICAL_CANDIDATE_ONLY"
G_WAITING = "RESEARCH_WAITING_FOR_NEW_INFORMATION"
G_NO_QUALIFIED = "NO_QUALIFIED_ALPHA_YET"

GOVERNANCE_STATES = (G_MEMORY_ABSENT, G_CHALLENGER_REVIEW, G_FORWARD_MATURING,
                     G_HISTORICAL_ONLY, G_WAITING, G_NO_QUALIFIED)

#: A prospective freeze whose challenger id no forward-evidence owner knows
#: about. It has an inception instant and will never accrue an observation
#: until someone registers it, and that is a FACT about the estate, not a
#: rendering choice.
FWD_LINK_ADOPTED = "ACCRUING_THROUGH_FORWARD_EVIDENCE_OWNER"
FWD_LINK_ORPHAN = "NOT_REGISTERED_WITH_FORWARD_EVIDENCE_OWNER"
FWD_LINK_UNKNOWN = "FORWARD_EVIDENCE_OWNER_UNAVAILABLE"

#: Which operator question each recorded gate check answers. The check names
#: are the evaluation kernel's; the captions are this projection's only
#: contribution, and they add no logic.
GATE_DIMENSION = {
    "has_lockbox_observations": "EFFECTIVE_SAMPLE",
    "lockbox_material": "MATERIALITY_LOCKBOX",
    "validation_material": "MATERIALITY_VALIDATION",
    "validation_same_sign": "REGIME_STABILITY",
    "lockbox_t_positive": "DIRECTION",
    "burden_corrected_significant": "MULTIPLICITY",
}

#: Data-opportunity states that mean "someone must decide whether to spend
#: money". Everything else is explicitly NOT a purchase question.
PURCHASE_DECISION_STATES = ("PURCHASE_CANDIDATE",)

SAFETY = {
    "read_only": True,
    "research_only": True,
    "paper_only": True,
    "owns_research_state": False,
    "writes_research_store": False,
    "creates_orders": False,
    "creates_fills": False,
    "promotes_model": False,
    "approves_proposal": False,
    "allocates_capital": False,
    "mutates_operational_store": False,
    "automation_enabled": False,
    "manual_review_remains_mandatory": True,
}


# --------------------------------------------------------------------------- #
# Small helpers. Deliberately trivial: anything that looked like research
# mathematics would belong to an owner, not to a read model.
# --------------------------------------------------------------------------- #
def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)


def _iso(dt: _dt.datetime) -> str:
    return dt.isoformat()


def _parse(ts: Any) -> Optional[_dt.datetime]:
    """Parse a persisted ISO instant, tolerating the trailing-Z form."""
    if not ts:
        return None
    text = str(ts).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        out = _dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return out if out.tzinfo else out.replace(tzinfo=_dt.timezone.utc)


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def _not_available(reason: str, detail: str = "") -> dict:
    row = {"state": NOT_AVAILABLE, "reason": reason}
    if detail:
        row["detail"] = detail[:400]
    return row


# --------------------------------------------------------------------------- #
# Canonical owners. Imported inside the functions that need them so a missing
# research package degrades to NOT_AVAILABLE instead of breaking app import.
# --------------------------------------------------------------------------- #
def _r59():
    from paper_trader.alpha_agent import r59 as _mod
    return _mod


def _memory_owner():
    from paper_trader.alpha_agent.r59 import memory as _mod
    return _mod


def _loop_owner():
    from paper_trader.alpha_agent.r59 import loop as _mod
    return _mod


def _runtime_owner():
    from paper_trader.alpha_agent.r59 import runtime as _mod
    return _mod


# --------------------------------------------------------------------------- #
# 1. RUNTIME - process health, and nothing else
# --------------------------------------------------------------------------- #
def _runtime_block(status: Optional[dict], now: _dt.datetime) -> dict:
    """What the WORKER is doing. Explicitly not what the research has found.

    ``status`` is the composed document from ``r59.runtime.status`` - the
    canonical runtime-status owner - so the lease, the identity and the
    heartbeat are read rather than re-derived here. Queue depth belongs to
    the queue owner and is reported once, under ``research_volume``.
    """
    if not status:
        return dict(_not_available(
            "R59_RUNTIME_STATUS_UNREADABLE",
            "no persisted runtime status artifact could be read"),
            process_health_is_not_research_success=True)

    lease = status.get("lease") or {}
    hb = _parse(lease.get("heartbeat_at_utc") or status.get("last_heartbeat"))
    age = None if hb is None else int((now - hb).total_seconds())
    rt = _runtime_owner()
    if age is None:
        freshness = NOT_AVAILABLE
    elif age <= rt.HEARTBEAT_SECONDS * 3:
        freshness = "FRESH"
    elif age <= rt.LEASE_STALE_SECONDS:
        freshness = "LATE"
    else:
        freshness = "STALE"

    src = status.get("source_identity") or {}
    return {
        "worker_state": status.get("worker_state"),
        "current_lane": status.get("current_lane"),
        "started_at": status.get("started_at"),
        "last_heartbeat": lease.get("heartbeat_at_utc")
        or status.get("last_heartbeat"),
        "heartbeat_age_seconds": age,
        "heartbeat_freshness": freshness,
        "heartbeat_interval_seconds": rt.HEARTBEAT_SECONDS,
        "lease_stale_after_seconds": rt.LEASE_STALE_SECONDS,
        "lease": {
            "held": lease.get("held"),
            "holder": lease.get("holder"),
            "pid": lease.get("pid"),
            # None means "the operating system would not say" - a worker
            # started under a scheduled task's principal cannot be queried by
            # an unprivileged reader, and that is not evidence of death.
            "pid_alive": lease.get("pid_alive"),
            "acquired_at_utc": lease.get("acquired_at_utc"),
            "age_seconds": lease.get("age_seconds"),
        },
        "lease_is_live": status.get("lease_is_live"),
        "release_identity": {
            "release": src.get("release"),
            "commit_short": src.get("commit_short"),
            "branch": src.get("branch"),
            "dirty": src.get("dirty"),
            "resolved_from": src.get("resolved_from"),
            "repo_root": src.get("repo_root"),
        },
        "maturation_policy": status.get("maturation"),
        "sleep_or_stop_reason": status.get("stop_or_sleep_reason"),
        "next_planned_wake": status.get("next_planned_wake"),
        "last_completed_experiment": status.get("last_completed_experiment"),
        "latest_error": status.get("latest_error"),
        "process_health_is_not_research_success": True,
    }


# --------------------------------------------------------------------------- #
# 2. CURRENT RESEARCH INTENT - read from the queue, never regenerated
# --------------------------------------------------------------------------- #
def _mandate_row(job: Any) -> dict:
    """One queued/running mandate, rendered from its persisted payload."""
    d = job.as_dict() if hasattr(job, "as_dict") else dict(job)
    payload = d.get("payload") or {}
    return {
        "job_id": d.get("job_id"),
        "lane": d.get("lane"),
        "category": d.get("category"),
        "state": d.get("state"),
        "priority": d.get("priority"),
        "started_at": d.get("started_at"),
        "created_at": d.get("created_at"),
        "blocked_reason": d.get("blocked_reason"),
        "mandate_id": payload.get("mandate_id"),
        "mandate_kind": payload.get("kind"),
        "asset_class": payload.get("asset_class"),
        "family": payload.get("family"),
        "expected_information_value": _f(
            payload.get("expected_information_value")),
        "why_selected": payload.get("reason"),
        "issued_by": payload.get("issued_by"),
        "issued_at": payload.get("issued_at"),
        "batch_rank": payload.get("batch_rank"),
    }


def _intent_block(queue, status: Optional[dict], frontier: dict) -> dict:
    """What AlphaAgent is working on NOW, and why.

    The queue is the authority: a job is RUNNING because a worker claimed it,
    and its payload is the exact mandate the governor issued. Nothing here
    asks the governor for a fresh answer - that call writes.
    """
    if queue is None:
        return _not_available("R59_QUEUE_NOT_PRESENT",
                            "the persistent research queue does not exist")
    running = [_mandate_row(j) for j in queue.list_jobs(state="RUNNING",
                                                        limit=5)]
    current = running[0] if running else None
    ready = [ac for ac, row in (frontier or {}).items()
             if row.get("state") in ("DATA_READY", "RESEARCH_READY",
                                     "ACTIVE_SEARCH")]
    return {
        "current_lane": (status or {}).get("current_lane"),
        "current_mandate": current,
        "also_running": running[1:],
        "n_running": len(running),
        "mandate_source": "alpha_agent.r59.governor (persisted on the "
                          "canonical Stage-8 queue as the job payload)",
        "what_would_change_the_lane": {
            "rule": "the queue drains strictly by priority, which is the "
                    "governor's own batch rank; the lane changes when this "
                    "job settles and the next-priority job is claimed, or "
                    "when a measured frontier or data-opportunity change "
                    "makes the governor issue a different batch",
            "ready_scopes": sorted(ready),
            "next_jobs_are_listed_under": "next_research.queued",
        },
        "expected_information_value_is_the_governors": True,
    }


# --------------------------------------------------------------------------- #
# 3. RESEARCH VOLUME
# --------------------------------------------------------------------------- #
def _volume_block(summary: dict, burden: dict, queue) -> dict:
    counts = {} if queue is None else queue.counts_by_state()
    return {
        "hypotheses_total": summary.get("hypotheses_total"),
        "hypotheses_settled": summary.get("hypotheses_settled"),
        "hypotheses_open": summary.get("hypotheses_open"),
        "search_burden_total": burden.get("total"),
        "distinct_families": burden.get("distinct_families"),
        "burden_by_asset_class": burden.get("by_asset_class"),
        "by_outcome": summary.get("by_outcome"),
        "by_asset_class": summary.get("by_asset_class"),
        "by_generation_method": summary.get("by_generation_method"),
        "by_release": summary.get("by_release"),
        "queue": {
            "runnable_now": None if queue is None else queue.runnable_depth(),
            "queued": int(counts.get("QUEUED", 0)),
            "running": int(counts.get("RUNNING", 0)),
            "retryable": int(counts.get("RETRYABLE", 0)),
            "blocked_specific": int(counts.get("BLOCKED_SPECIFIC", 0)),
            "completed": int(counts.get("COMPLETED", 0)),
            "rejected": int(counts.get("REJECTED", 0)),
            "failed_permanent": int(counts.get("FAILED_PERMANENT", 0)),
            "last_progress_at": None if queue is None
            else queue.last_progress_at(),
        },
        "burden_is_counted_not_copied": True,
    }


# --------------------------------------------------------------------------- #
# 4. WINDOWED ACTIVITY - what CHANGED, honestly labelled
# --------------------------------------------------------------------------- #
def _window_counts(rows: list) -> dict:
    """Summarise settled rows, keeping measured and imported work apart."""
    r59 = _r59()
    native = [r for r in rows if (r.get("release") or "") == r59.RELEASE]
    imported = [r for r in rows if (r.get("release") or "") != r59.RELEASE]

    def _by_outcome(rs: list) -> dict:
        out: dict = {}
        for r in rs:
            key = r.get("outcome") or "UNSETTLED"
            out[key] = out.get(key, 0) + 1
        return out

    def _by_asset(rs: list) -> dict:
        out: dict = {}
        for r in rs:
            key = r.get("asset_class") or "UNDECLARED"
            out[key] = out.get(key, 0) + 1
        return out

    return {
        "hypotheses_settled": len(rows),
        "measured_here": len(native),
        "imported_from_prior_release": len(imported),
        "by_outcome": _by_outcome(rows),
        "measured_here_by_outcome": _by_outcome(native),
        "measured_here_by_asset_class": _by_asset(native),
        "rejected": sum(1 for r in rows if r.get("outcome")
                        in ("REJECTED", "NO_ALPHA_EVIDENCE")),
        "qualified": sum(1 for r in rows if r.get("outcome") == "QUALIFIED"),
        "prospective_freezes": sum(1 for r in rows
                                   if r.get("outcome") == "FORWARD_FROZEN"),
    }


def _event_counts(events: list, since: Optional[_dt.datetime]) -> dict:
    out: dict = {}
    for ev in events:
        at = _parse(ev.get("recorded_at"))
        if since is not None and (at is None or at < since):
            continue
        kind = ev.get("kind") or "UNKNOWN"
        out[kind] = out.get(kind, 0) + 1
    return out


def _activity_block(mem, events: list, now: _dt.datetime,
                    status: Optional[dict],
                    eligible_session: Optional[str]) -> dict:
    """Deltas over windows that EXIST in persisted state, and no others.

    ONE read of the settled rows serves every window: the shorter windows are
    subsets of the lifetime one, and re-querying per window would make the
    projection's cost grow with the number of questions asked.
    """
    lifetime = mem.settled_between()
    windows: dict = {}

    def _add(name: str, since: Optional[_dt.datetime], basis: str,
             available: bool = True, reason: str = "") -> None:
        if not available:
            windows[name] = _not_available(reason or "WINDOW_NOT_DERIVABLE")
            return
        if since is None:
            rows = lifetime
        else:
            cutoff = _iso(since)
            rows = [r for r in lifetime
                    if (r.get("settled_at") or "") >= cutoff]
        windows[name] = {
            "since": None if since is None else _iso(since),
            "basis": basis,
            **_window_counts(rows),
            "events": _event_counts(events, since),
        }

    _add("last_24h", now - _dt.timedelta(hours=24),
         "rolling 24 hours from this read")

    started = _parse((status or {}).get("started_at"))
    _add("since_worker_start", started,
         "the current worker's start instant, from the runtime status "
         "artifact",
         available=started is not None,
         reason="R59_WORKER_NEVER_STARTED_ON_THIS_MACHINE")

    session_dt = None
    if eligible_session:
        session_dt = _parse("%sT00:00:00+00:00" % str(eligible_session)[:10])
    _add("since_latest_eligible_session", session_dt,
         "midnight UTC of the latest eligible market session reported by the "
         "canonical forward-evidence runtime",
         available=session_dt is not None,
         reason="ELIGIBLE_SESSION_UNAVAILABLE_FROM_FORWARD_EVIDENCE_OWNER")

    _add("lifetime", None, "every verdict this memory has ever recorded")

    return {
        "windows": windows,
        "counted_on": "hypotheses.settled_at - the instant THIS memory "
                      "recorded a verdict",
        "why_split": "settled_at is the IMPORT instant for a result a prior "
                     "release originally measured, so every window reports "
                     "measured_here separately from "
                     "imported_from_prior_release; a backfill is not a day "
                     "of research",
        "latest_events": events[:25],
    }


# --------------------------------------------------------------------------- #
# 5. RECENT OUTCOMES + BEST CURRENT CANDIDATES
# --------------------------------------------------------------------------- #
def _why_not_qualified(row: dict) -> dict:
    """Which gate refused this candidate, in the operator's dimensions.

    Read back from the recorded ``robustness.checks``; the captions are a
    dictionary lookup and nothing is recalculated.
    """
    checks = ((row.get("robustness") or {}).get("checks") or {})
    failed = [GATE_DIMENSION.get(k, k) for k, ok in sorted(checks.items())
              if not ok]
    passed = [GATE_DIMENSION.get(k, k) for k, ok in sorted(checks.items())
              if ok]
    return {
        "failed_dimensions": failed,
        "passed_dimensions": passed,
        "recorded_reason": row.get("reason_rejected"),
        "gate_owner": "alpha_agent.r59.engines.gate",
        "checks_as_recorded": checks,
    }


def _candidate_row(row: dict) -> dict:
    st = row.get("statistic") or {}
    econ = row.get("economics") or {}
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "release": row.get("release"),
        "economic_family": row.get("economic_family"),
        "information_family": row.get("information_family"),
        "asset_class": row.get("asset_class"),
        "horizon_sessions": row.get("horizon_sessions"),
        "model_family": row.get("model_family"),
        "outcome": row.get("outcome"),
        "settled_at": row.get("settled_at"),
        "historical_statistics": {
            "lockbox_t": _f(st.get("lockbox_t")),
            "lockbox_p_one_sided": _f(st.get("lockbox_p_one_sided")),
            "lockbox_observations": st.get("lockbox_observations"),
            "lockbox_raw_periods": st.get("lockbox_raw_periods"),
            "overlap_factor": _f(st.get("overlap_factor")),
        },
        "multiplicity_state": {
            "burden_denominator": st.get("burden_denominator"),
            "burden_corrected_p": _f(st.get("burden_corrected_p")),
            "correction": "the family's own prior search plus the generative "
                          "search that produced the candidate",
        },
        "materiality_state": {
            "lockbox_materiality": _f(econ.get("lockbox_materiality")),
            "validation_materiality": _f(econ.get("validation_materiality")),
        },
        "why_not_yet_qualified": _why_not_qualified(row),
        "reopen_condition": row.get("reopen_condition"),
        "evidence_class": "HISTORICAL",
        "confers_no_capital": True,
    }


def _cost_state() -> dict:
    """The pre-registered cost convention every recorded statistic is net of.

    These are the R59 constants, quoted. Nothing is computed here: a read
    model that recalculated a cost would be a second cost owner.
    """
    r59 = _r59()
    return {
        "statistics_are_net_of_cost": True,
        "equity_cost_rate_per_side": r59.EQ_COST_RATE_PER_SIDE,
        "futures_cost_rate_per_side": r59.FUT_COST_RATE_PER_SIDE,
        "max_turnover_per_decision_one_side": r59.GATE_MAX_TURNOVER,
        "materiality_floor_annualised_net_excess": r59.GATE_MATERIALITY,
        "effective_observation_floor": r59.OBS_FLOOR,
        "false_discovery_rate_q": r59.BH_Q,
        "owner": "alpha_agent.r59 (pre-registered, inherited from R57/R58 so "
                 "an R59 result stays comparable to the families those "
                 "releases already prosecuted)",
    }


def _candidates_block(strongest: list) -> dict:
    return {
        "candidates": [_candidate_row(r) for r in strongest],
        "n_reported": len(strongest),
        "ranked_by": "the persisted lockbox t-statistic, signed - a large "
                     "negative t is the opposite finding, not a near miss",
        "regime_state": "REGIME_STABILITY is reported per candidate as the "
                        "recorded validation/lockbox same-sign check; there "
                        "is no live regime classifier in this estate and none "
                        "is invented here",
        "cost_state": _cost_state(),
        "no_candidate_here_holds_capital": True,
    }


def _recent_outcomes_block(mem, windows: dict, summary: dict,
                           graveyard_total: int) -> dict:
    r59 = _r59()
    day = windows.get("last_24h") or {}
    n_interesting = mem.count_unqualified_above_t(2.0)
    return {
        "last_24h": {
            "hypotheses_settled": day.get("hypotheses_settled"),
            "measured_here": day.get("measured_here"),
            "rejected": day.get("rejected"),
            "qualified": day.get("qualified"),
            "prospective_freezes": day.get("prospective_freezes"),
            "events": day.get("events"),
        } if day.get("state") != NOT_AVAILABLE else day,
        "statistically_interesting_but_unqualified": {
            "count": n_interesting,
            "definition": "a settled, non-invalidated hypothesis whose "
                          "recorded lockbox t is at least 2.0 and which the "
                          "gate still refused",
            "threshold_t": 2.0,
        },
        "qualified_historically": (summary.get("by_outcome") or {}).get(
            r59.HO_QUALIFIED, 0),
        "graveyard_total": graveyard_total,
        "duplicates_rejected_before_evaluation": _not_available(
            "NOT_PERSISTED",
            "novelty de-duplication is decided in flight by "
            "ResearchMemory.is_novel and leaves no counted row; the "
            "generator_yield ledger records duplicates per generative "
            "family and is reported under generative_yield"),
        "generative_yield": mem.generator_yield(),
    }


# --------------------------------------------------------------------------- #
# 6. PROSPECTIVE / TRUE_FORWARD
# --------------------------------------------------------------------------- #
def _forward_owner_view() -> dict:
    """The canonical forward-evidence owners, read - never re-implemented.

    There are two, and they compete DIFFERENT objects: the Release-46 board
    competes SIGNALS forward, and the Release-56 shadow owner competes whole
    PORTFOLIOS forward. A freeze is only orphaned if NEITHER knows it, so
    both are consulted before this projection says anything is accruing
    nothing.
    """
    view: dict = {"runtime": None, "board": None, "shadow_records": None,
                  "warnings": []}
    try:
        from paper_trader.api import research_runtime as _r52
        view["runtime"] = _r52.load_runtime_health()
    except Exception as exc:                                # noqa: BLE001
        view["warnings"].append("forward-evidence runtime unavailable: %s"
                                % str(exc)[:200])
    try:
        from paper_trader.api import prospective_tournament as _r46
        view["board"] = _r46.load_prospective_tournament()
    except Exception as exc:                                # noqa: BLE001
        view["warnings"].append("prospective tournament board unavailable: %s"
                                % str(exc)[:200])
    try:
        from paper_trader.api import shadow_portfolio_evidence as _r56
        view["shadow_records"] = _r56.load_records()
    except Exception as exc:                                # noqa: BLE001
        view["warnings"].append("forward paper-portfolio owner unavailable: "
                                "%s" % str(exc)[:200])
    return view


def _known_forward_ids(forward: dict) -> Optional[dict]:
    """``challenger_id -> the owner that accrues its forward evidence``.

    None when NO forward owner could be read at all - in which case a freeze
    is reported as ``FORWARD_EVIDENCE_OWNER_UNAVAILABLE`` rather than
    accused of being orphaned by a read failure.
    """
    board = forward.get("board")
    shadow = forward.get("shadow_records")
    if board is None and shadow is None:
        return None
    known: dict = {}
    for key in ("which_are_winning", "which_are_losing",
                "which_are_too_early_to_judge", "which_were_killed",
                "which_are_data_blocked"):
        for row in ((board or {}).get(key) or []):
            cid = row.get("challenger_id")
            if cid:
                known[str(cid)] = "api.prospective_tournament (R46 signal " \
                                  "challengers)"
    for rec in (shadow or []):
        cid = rec.get("challenger_id")
        if cid:
            known.setdefault(
                str(cid),
                "api.shadow_portfolio_evidence (R56 forward paper portfolios)")
    return known


def _freeze_row(row: dict, known: Optional[dict]) -> dict:
    """One prospective freeze recorded in research memory."""
    raw = row.get("forward_challenger")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            raw = None
    raw = raw or {}
    cid = raw.get("challenger_id")
    owner = None
    if known is None:
        link = FWD_LINK_UNKNOWN
    elif cid and str(cid) in known:
        link = FWD_LINK_ADOPTED
        owner = known[str(cid)]
    else:
        link = FWD_LINK_ORPHAN
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "challenger_id": cid,
        "release": row.get("release"),
        "asset_class": row.get("asset_class"),
        "economic_family": row.get("economic_family"),
        "horizon_sessions": row.get("horizon_sessions"),
        "inception": raw.get("inception"),
        "record_hash": raw.get("record_hash"),
        "forward_observations_at_freeze": raw.get(
            "forward_observations_at_freeze"),
        "forward_evidence_link": link,
        "accruing_through": owner,
        "scoring_owner": "a canonical forward-evidence owner - never R59, "
                         "which writes only the inception",
    }


def _prospective_block(mem, forward: dict) -> dict:
    r59 = _r59()
    board = forward.get("board")
    health = (forward.get("runtime") or {}).get("runtime_health") or {}
    known = _known_forward_ids(forward)

    frozen = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    rows = [_freeze_row(r, known) for r in frozen]
    orphans = [r for r in rows if r["forward_evidence_link"] == FWD_LINK_ORPHAN]

    if board is None:
        accrual = _not_available(
            "FORWARD_EVIDENCE_BOARD_UNAVAILABLE",
            "; ".join(forward.get("warnings") or []))
    else:
        accrual = {
            "challengers_competing": board.get("how_many_models_are_competing"),
            "challengers_active": board.get("how_many_are_active"),
            "challengers_data_blocked": board.get("how_many_are_blocked"),
            "forward_predictions_made": board.get(
                "how_many_real_forward_predictions_exist"),
            "forward_predictions_matured": board.get("how_many_have_matured"),
            "forward_predictions_pending": board.get(
                "how_many_are_still_pending"),
            "effective_independent_observations": health.get(
                "effective_independent_observations"),
            "best_net_alpha_vs_control_bps": board.get(
                "best_net_alpha_vs_control_bps"),
            "current_top_forward_challenger": board.get(
                "current_top_forward_challenger"),
            "evidence_maturity_state": board.get("evidence_maturity_state"),
            "next_material_maturity": board.get("next_material_maturity"),
            "degraded_or_killed": board.get("which_were_killed"),
            "losing": board.get("which_are_losing"),
            "forward_chain_integrity": health.get("forward_chain_integrity"),
        }

    return {
        "freezes_recorded_in_research_memory": len(rows),
        "freezes": rows,
        "freezes_not_registered_with_a_forward_owner": len(orphans),
        "orphan_freezes": orphans,
        "forward_accrual": accrual,
        "promotion_ready_count": health.get("promotion_ready_count"),
        "forward_paper_portfolios": (
            None if forward.get("shadow_records") is None
            else len(forward.get("shadow_records") or [])),
        "forward_evidence_owners": [
            "api.prospective_tournament (R46 forward SIGNAL challengers, "
            "advanced by alpha_agent.r52.runtime)",
            "api.shadow_portfolio_evidence (R56 forward PAPER PORTFOLIO "
            "challengers)",
            "api.research_runtime (R52 maturation-cycle health)",
        ],
        "r59_writes_only_the_inception": True,
        "historical_result_is_never_forward_evidence": True,
    }


# --------------------------------------------------------------------------- #
# 7. GOVERNANCE - the operator-facing state, derived only from persisted facts
# --------------------------------------------------------------------------- #
def _governance_block(summary: dict, prospective: dict, frontier: dict,
                      queue) -> dict:
    r59 = _r59()
    by_outcome = summary.get("by_outcome") or {}
    qualified = int(by_outcome.get(r59.HO_QUALIFIED, 0) or 0)
    ready_now = 0 if queue is None else queue.runnable_depth()
    promotion_ready = prospective.get("promotion_ready_count")
    accruing = [f for f in (prospective.get("freezes") or [])
                if f.get("forward_evidence_link") == FWD_LINK_ADOPTED]

    open_scopes = [ac for ac, row in (frontier or {}).items()
                   if row.get("state") in ("DATA_READY", "RESEARCH_READY",
                                           "ACTIVE_SEARCH")]
    blocked_scopes = [ac for ac, row in (frontier or {}).items()
                      if row.get("state") == "BLOCKED"]
    exhausted_scopes = [ac for ac, row in (frontier or {}).items()
                        if row.get("state") == "EXHAUSTED"]

    if promotion_ready:
        state = G_CHALLENGER_REVIEW
        because = ("the canonical forward-evidence runtime reports %s "
                   "challenger(s) at promotion-ready evidence; a human "
                   "governance review is required and nothing is promoted "
                   "automatically" % promotion_ready)
    elif accruing:
        state = G_FORWARD_MATURING
        because = ("%d prospective challenger(s) are accruing TRUE_FORWARD "
                   "evidence through the canonical forward owner; none has "
                   "reached the declared evidence gate" % len(accruing))
    elif qualified:
        state = G_HISTORICAL_ONLY
        because = ("%d hypothesis(es) passed the historical gate and none is "
                   "accruing forward evidence; a backtest is not proof and "
                   "confers no capital" % qualified)
    elif not open_scopes and ready_now == 0:
        state = G_WAITING
        because = ("no measured scope remains open and the queue has no "
                   "runnable work; research advances again only when new "
                   "information arrives")
    else:
        state = G_NO_QUALIFIED
        because = ("no hypothesis has passed the qualification gate; %d scope"
                   "(s) remain open and %d job(s) are runnable, so research "
                   "is proceeding" % (len(open_scopes), ready_now))

    return {
        "state": state,
        "vocabulary": list(GOVERNANCE_STATES),
        "because": because,
        "qualified_historically": qualified,
        "forward_challengers_accruing": len(accruing),
        "promotion_ready_count": promotion_ready,
        "open_scopes": sorted(open_scopes),
        "blocked_scopes": sorted(blocked_scopes),
        "exhausted_scopes": sorted(exhausted_scopes),
        "research_is_blocked": bool(blocked_scopes) and not open_scopes,
        "automatic_promotion_allowed": False,
        "portfolio_mutation_allowed": False,
        "capital_allocation_allowed": False,
        "manual_review_required": True,
        "process_health_is_a_separate_question": True,
    }


# --------------------------------------------------------------------------- #
# 8. GRAVEYARD / LEARNING
# --------------------------------------------------------------------------- #
def _graveyard_block(mem, summary: dict) -> dict:
    r59 = _r59()
    by_outcome = summary.get("by_outcome") or {}
    # The TRUE total comes from the memory's own outcome census, never from
    # the length of a sampled page: reporting a page size as a total is how a
    # cap silently becomes a number an operator reasons with.
    total = (int(by_outcome.get(r59.HO_REJECTED, 0) or 0)
             + int(by_outcome.get(r59.HO_NO_ALPHA_EVIDENCE, 0) or 0))
    sample_limit = 4000
    rows = mem.graveyard(limit=sample_limit)
    families: dict = {}
    for row in rows:
        key = "%s|%s" % (row.get("economic_family") or "UNDECLARED",
                         row.get("asset_class") or "UNDECLARED")
        entry = families.setdefault(key, {
            "economic_family": row.get("economic_family"),
            "asset_class": row.get("asset_class"),
            "rejected": 0,
            "reasons": {},
            "reopen_condition": row.get("reopen_condition"),
            "latest_settled_at": row.get("settled_at"),
        })
        entry["rejected"] += 1
        for reason in str(row.get("reason_rejected") or "").split(","):
            reason = reason.strip()
            if not reason:
                continue
            dim = GATE_DIMENSION.get(reason, reason)
            entry["reasons"][dim] = entry["reasons"].get(dim, 0) + 1
        if (row.get("settled_at") or "") > (entry["latest_settled_at"] or ""):
            entry["latest_settled_at"] = row.get("settled_at")

    ranked = sorted(families.values(), key=lambda e: -e["rejected"])
    invalidated = mem.invalidated()
    inv_families: dict = {}
    for row in invalidated:
        key = row.get("economic_family") or "UNDECLARED"
        entry = inv_families.setdefault(
            key, {"economic_family": key, "rows": 0,
                  "reason": row.get("invalidated_reason")})
        entry["rows"] += 1

    return {
        "rejected_total": total,
        "rows_sampled_for_the_family_breakdown": len(rows),
        "family_breakdown_is_complete": len(rows) >= total,
        "sample_limit": sample_limit,
        "major_families_rejected": ranked[:20],
        "n_distinct_rejected_families": len(families),
        "invalidated_rows": len(invalidated),
        "invalidated_families": sorted(inv_families.values(),
                                       key=lambda e: -e["rows"]),
        "nothing_is_deleted": True,
        "reopen_rule": "a graveyard row re-enters research only when a caller "
                       "declares its named reopen condition satisfied; "
                       "nothing reopens by being asked twice",
        "rejections_stay_in_the_denominator": True,
    }


# --------------------------------------------------------------------------- #
# 9. DATA OPPORTUNITIES
# --------------------------------------------------------------------------- #
def _opportunities_block(mem) -> dict:
    rows = mem.opportunities()
    by_state: dict = {}
    for row in rows:
        by_state.setdefault(row.get("state") or "UNDECLARED", []).append({
            "opportunity_id": row.get("opportunity_id"),
            "title": row.get("title"),
            "asset_class": row.get("asset_class"),
            "information_need": row.get("information_need"),
            "unlocks": row.get("unlocks"),
            "owned_but_unused": row.get("owned_but_unused"),
            "free_proxy": row.get("free_proxy"),
            "provider": row.get("provider"),
            "pit_integrity": row.get("pit_integrity"),
            "effective_sample": row.get("effective_sample"),
            "expected_value": row.get("expected_value"),
            "cost_usd_year": row.get("cost_usd_year"),
            "gate_verdict": row.get("gate_verdict"),
            "updated_at": row.get("updated_at"),
        })

    candidates = [o for state, rows_ in by_state.items()
                  if state in PURCHASE_DECISION_STATES for o in rows_]
    # A purchase is RECOMMENDED only when the canonical purchase gate says so.
    # This module owns no gate and invents no verdict: an opportunity sitting
    # in PURCHASE_CANDIDATE with an INSUFFICIENT_EVIDENCE verdict is not a
    # recommendation, and reporting it as one is exactly the mistake the
    # information-purchase gate exists to prevent.
    recommended = [o for o in candidates
                   if "PURCHASE_RECOMMENDED" in str(o.get("gate_verdict") or "")
                   or "INTEGRATION_RECOMMENDED" in str(
                       o.get("gate_verdict") or "")]
    return {
        "total": len(rows),
        "by_state": by_state,
        "state_counts": {k: len(v) for k, v in sorted(by_state.items())},
        "purchase_candidates": candidates,
        "purchase_actually_recommended": bool(recommended),
        "purchase_recommendations": recommended,
        "purchase_verdict_owner": "engine.data_expansion_gate through "
                                  "api.data_expansion (the sixteen-dimension "
                                  "purchase gate); this projection reports "
                                  "the recorded verdict and issues none",
        "nothing_is_purchased_here": True,
        "manual_purchase_approval_required": True,
    }


# --------------------------------------------------------------------------- #
# 9b. FRONTIER + PROVIDER UTILISATION - projected, never re-measured
# --------------------------------------------------------------------------- #
def _frontier_block(frontier: dict) -> dict:
    """The per-scope research frontier, as the frontier owner last MEASURED it.

    Projected to what an operator reads. The owner's full ``detail`` repeats
    every family specification for every scope; carrying that verbatim would
    quadruple the payload without answering a question anyone asks.
    """
    out: dict = {}
    for ac, row in (frontier or {}).items():
        d = row.get("detail") or {}
        out[ac] = {
            "state": row.get("state"),
            "reason": row.get("reason"),
            "updated_at": row.get("updated_at"),
            "substrate": d.get("substrate"),
            "substrate_present": d.get("substrate_present"),
            "instruments": d.get("instruments"),
            "sessions": d.get("sessions"),
            "hypotheses_recorded": d.get("hypotheses_recorded"),
            "search_burden": d.get("search_burden"),
            "forward_frozen": d.get("forward_frozen"),
            "n_executable_families": len(d.get("executable_families") or []),
            "n_prosecuted_families": len(d.get("prosecuted_families") or []),
            "remaining_families": d.get("remaining_families"),
            "generative_families": d.get("generative_families"),
            "measured_by": d.get("measured_by"),
        }
    return {
        "asset_classes": out,
        "measured_by": "alpha_agent.r59.frontier (this projection reads the "
                       "persisted measurement and never re-measures - "
                       "measuring writes)",
        "asset_labels_are_scopes_not_risk_factors": True,
    }


def _provider_block(rows: list) -> list:
    """What each provider actually unlocked, as the R59 owner recorded it."""
    out = []
    for row in rows or []:
        detail = row.get("detail") or {}
        out.append({
            "provider": row.get("provider"),
            "data_class": row.get("data_class"),
            "updated_at": row.get("updated_at"),
            "coverage": row.get("coverage"),
            "classification": detail.get("classification"),
            "current_usage": detail.get("current_usage"),
            "cost_known": detail.get("cost_known"),
            "cost_usd_year": detail.get("cost_usd_year"),
        })
    return out


# --------------------------------------------------------------------------- #
# 10. NEXT RESEARCH
# --------------------------------------------------------------------------- #
def _next_block(queue, frontier: dict, mem) -> dict:
    if queue is None:
        return _not_available("R59_QUEUE_NOT_PRESENT")
    queued = [_mandate_row(j) for j in queue.list_jobs(state="QUEUED",
                                                       limit=10)]
    blocked = [_mandate_row(j) for j in queue.blocked_jobs(limit=10)]
    capacity = mem.get_meta("governor_information_set_fingerprint")
    return {
        "queued": queued,
        "n_queued_shown": len(queued),
        "blocked": blocked,
        "ordering": "strictly by priority, which is the governor's own batch "
                    "rank - so its cross-asset and cross-kind fairness "
                    "decision survives into execution",
        "what_could_change_the_priority": [
            "a settled hypothesis raises the family's search burden and "
            "lowers the expected information value of the next test there",
            "a data-opportunity state change re-fingerprints the information "
            "set and restores generative capacity",
            "a measured frontier change opens or exhausts a scope",
        ],
        "information_set_fingerprint": capacity,
        "frontier_states": {ac: row.get("state")
                            for ac, row in (frontier or {}).items()},
        "generated_by": "alpha_agent.r59.governor; this projection reads the "
                        "queue and never asks the governor for a new batch "
                        "(that call writes)",
    }


# --------------------------------------------------------------------------- #
# 11. HUMAN ACTION
# --------------------------------------------------------------------------- #
def _human_action_block(governance: dict, prospective: dict,
                        runtime: dict, opportunities: dict) -> dict:
    """Is a person required, and for exactly what?"""
    actions: list = []
    if governance.get("state") == G_CHALLENGER_REVIEW:
        actions.append({
            "required": True,
            "action": "GOVERNED_CHALLENGER_REVIEW",
            "detail": "a forward challenger has reached promotion-ready "
                      "evidence; review it manually. Nothing promotes, "
                      "allocates or orders on its own.",
        })
    if prospective.get("freezes_not_registered_with_a_forward_owner"):
        actions.append({
            "required": True,
            "action": "REGISTER_ORPHAN_PROSPECTIVE_FREEZE",
            "detail": "%d prospective freeze(s) recorded in research memory "
                      "are not registered with the canonical forward-evidence "
                      "owner, so they accrue no TRUE_FORWARD observations. "
                      "This is a documented architecture gap, not a research "
                      "result." % prospective.get(
                          "freezes_not_registered_with_a_forward_owner"),
        })
    if opportunities.get("purchase_actually_recommended"):
        actions.append({
            "required": True,
            "action": "REVIEW_DATA_PURCHASE_RECOMMENDATION",
            "detail": "the canonical purchase gate recommends acquiring a "
                      "dataset; approval is manual and nothing is purchased "
                      "by the system.",
        })
    if runtime.get("latest_error"):
        actions.append({
            "required": True,
            "action": "INVESTIGATE_RESEARCH_RUNTIME_ERROR",
            "detail": str(runtime.get("latest_error"))[:300],
        })
    if runtime.get("heartbeat_freshness") == "STALE":
        actions.append({
            "required": True,
            "action": "INVESTIGATE_STALLED_RESEARCH_WORKER",
            "detail": "the worker lease heartbeat is older than the stale "
                      "threshold; the researcher may not be running.",
        })
    return {
        "human_action_required": bool(actions),
        "actions": actions,
        "none_of_these_are_executed_by_the_system": True,
    }


# --------------------------------------------------------------------------- #
# THE READ MODEL
# --------------------------------------------------------------------------- #
def load_alphaagent_outcomes() -> dict:
    """Compose the read-only AlphaAgent research-outcomes projection.

    Every store is opened through its canonical owner's READ-ONLY handle. If
    the persistent memory does not exist, this returns an explicit absence
    rather than an empty research record - a measured zero and an unmeasured
    one are different facts.
    """
    now = _now()
    base = {
        "schema_version": SCHEMA_VERSION,
        "owner": OWNER,
        "route": ROUTE,
        "release": RELEASE,
        "generated_at": _iso(now),
        "reads": [
            "alpha_agent.r59.memory (persistent research memory; read-only "
            "handle)",
            "alpha_agent.autonomous_research.ResearchQueue via "
            "alpha_agent.r59.loop (Stage-8 durable queue; read-only handle)",
            "alpha_agent.r59.runtime (persisted worker status + lease)",
            "api.research_runtime (R52 forward-evidence runtime health)",
            "api.prospective_tournament (R46 forward-evidence board)",
        ],
        "computes": [],
        "computes_no_research_mathematics": True,
        "owns_no_research_state": True,
        "safety": dict(SAFETY),
    }

    M = _memory_owner()
    LP = _loop_owner()
    RT = _runtime_owner()

    if not M.memory_present():
        return dict(
            base,
            availability={
                "research_memory": NOT_AVAILABLE,
                "reason": "R59_RESEARCH_MEMORY_NOT_PRESENT",
                "expected_at": str(M.memory_db_path()),
            },
            governance={
                "state": G_MEMORY_ABSENT,
                "vocabulary": list(GOVERNANCE_STATES),
                "because": "no persistent research memory exists on this "
                           "machine, so nothing is known about what "
                           "AlphaAgent has tested; this is an absence of "
                           "evidence, never a negative result",
                "automatic_promotion_allowed": False,
                "manual_review_required": True,
            },
            runtime=_runtime_block(RT.read_status(), now),
            human_action={"human_action_required": False, "actions": []},
        )

    mem = M.open_memory_readonly()
    queue = None
    queue_warning = None
    try:
        queue = LP.open_queue(read_only=True)
    except (FileNotFoundError, OSError) as exc:
        queue_warning = "research queue unavailable: %s" % str(exc)[:200]

    # The canonical runtime-status owner composes the persisted artifact with
    # the LIVE lease (holder, pid liveness, heartbeat instant). Both handles
    # are read-only, so asking creates nothing.
    try:
        status = RT.status(mem=mem, queue=queue, read_only=True) \
            if queue is not None else RT.read_status()
    except Exception as exc:                                # noqa: BLE001
        queue_warning = ("runtime status composition failed: %s"
                         % str(exc)[:200])
        status = RT.read_status()

    summary = mem.summary()
    burden = mem.burden()
    frontier = mem.get_frontier()
    events = mem.events(limit=200)

    forward = _forward_owner_view()
    health = (forward.get("runtime") or {}).get("runtime_health") or {}
    eligible_session = health.get("latest_eligible_session")

    runtime = _runtime_block(status, now)
    activity = _activity_block(mem, events, now, status, eligible_session)
    prospective = _prospective_block(mem, forward)
    governance = _governance_block(summary, prospective, frontier, queue)
    opportunities = _opportunities_block(mem)
    strongest = mem.strongest_unqualified(limit=8)
    graveyard = _graveyard_block(mem, summary)

    warnings = list(forward.get("warnings") or [])
    if queue_warning:
        warnings.append(queue_warning)

    return dict(
        base,
        availability={
            "research_memory": "PRESENT",
            "research_memory_path": str(M.memory_db_path()),
            "research_queue": "PRESENT" if queue is not None else NOT_AVAILABLE,
            "runtime_status_artifact": "PRESENT" if status else NOT_AVAILABLE,
            "forward_evidence_runtime": "PRESENT"
            if forward.get("runtime") else NOT_AVAILABLE,
            "forward_evidence_board": "PRESENT"
            if forward.get("board") else NOT_AVAILABLE,
        },
        headline=_headline(governance, runtime, summary),
        governance=governance,
        runtime=runtime,
        current_research_intent=_intent_block(queue, status, frontier),
        research_volume=_volume_block(summary, burden, queue),
        recent_activity=activity,
        recent_outcomes=_recent_outcomes_block(
            mem, activity.get("windows") or {}, summary,
            graveyard.get("rejected_total")),
        best_current_candidates=_candidates_block(strongest),
        prospective=prospective,
        graveyard=graveyard,
        data_opportunities=opportunities,
        next_research=_next_block(queue, frontier, mem),
        human_action=_human_action_block(governance, prospective, runtime,
                                         opportunities),
        multi_asset_frontier=_frontier_block(frontier),
        provider_utilisation=_provider_block(mem.provider_usage()),
        warnings=warnings,
    )


def _headline(governance: dict, runtime: dict, summary: dict) -> str:
    """One sentence. The EVIDENCE state leads; the process is a clause."""
    return ("%s - %s hypotheses measured, %s settled; worker %s."
            % (governance.get("state"),
               summary.get("hypotheses_total"),
               summary.get("hypotheses_settled"),
               runtime.get("worker_state") or NOT_AVAILABLE))
