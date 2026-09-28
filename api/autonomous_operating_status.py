"""api.autonomous_operating_status - THE ONE authoritative autonomy status (R79).

WHY THIS EXISTS
---------------
Paper Trader runs three cycles and eight forward registrations across two
processes, and answering the only question an operator actually has - *is this
thing still running, and what is it waiting for?* - required reading six
surfaces and knowing which of them was allowed to be stale.

Worse, the surfaces disagreed in a way that hid a nineteen-day outage. On
2026-09-27 the estate reported, simultaneously:

* ``PaperTrader-ResearchRuntime`` Running, lease live, heartbeat 90 seconds old;
* the research frontier READY on CROSS_ASSET;
* the governor able to "generate 2 independently executable mandates";
* the worker SLEEPING on ``WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE``.

Every one of those was individually true and the conjunction was false: the two
mandates were the estate's ONLY generatable work, both named economic families
the research director had ruled TERMINALLY closed, and no amount of elapsed time
or arriving data could ever have produced a third. The worker had executed no
research since 2026-09-08 and every surface implied it would resume by itself.

WHAT IT IS
----------
A PROJECTION, and nothing else. Every field is copied from the owner that
already computes it, that owner is named in :data:`PROJECTED_FROM` beside the
value, and this module contains no threshold, no calendar, no clock arithmetic
and no business rule of its own. It cannot disagree with its sources because it
has no opinion to disagree with - which is the property the six surfaces lacked.

It answers the eight questions in one read:

    LAST_COMPLETED_CYCLE          what finished, operationally and in research
    CURRENT_ACTIVE_WORK           what is executing right now, if anything
    NEXT_SCHEDULED_WORK           what is due next, and when
    CURRENT_BLOCKERS              what is held, and WHAT WOULD RELEASE IT
    RESEARCH_CANDIDATE_AND_STAGE  the candidate under test and its stage
    FORWARD_EMITTED_PENDING_MATURED   the forward evidence ledger
    PORTFOLIO_PROPOSAL_STATE      the governed proposal and its decision state
    RUNTIME_SOURCE_IDENTITY       which code each process is actually running

WHAT IT IS NOT
--------------
Not a second workflow state: ``api.workflow_state`` remains the canonical
operator interpretation and this module reads it rather than re-deriving it.
Not a scheduler, not a queue, not a writer. It performs no write of any kind,
creates no order, approves nothing and promotes nothing.

THE ONE JUDGEMENT IT MAKES
--------------------------
:func:`_autonomy_verdict` answers whether the loop will advance WITHOUT a human,
and it makes that call from the clearance vocabulary the blocker taxonomy
already owns - never from liveness. A live lease and a fresh heartbeat are
evidence that a process exists, not that research is progressing, and treating
them as the latter is the exact substitution that let the outage above run for
nineteen days.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

CALCULATION_OWNER = "api.autonomous_operating_status"

#: Every field, and the owner it is copied from. Published in the artifact so a
#: reader can go to the owner rather than trust this projection.
PROJECTED_FROM = {
    "last_completed_cycle": ["api.workflow_state", "alpha_agent.r59.runtime"],
    "current_active_work": ["alpha_agent.r59.runtime"],
    "next_scheduled_work": ["alpha_agent.r59.runtime",
                            "api.forward_producer_health"],
    "current_blockers": ["api.workflow_state", "alpha_agent.r59.blockers"],
    "research_candidate_and_stage": ["alpha_agent.r59.runtime",
                                     "alpha_agent.r59.frontier",
                                     "alpha_agent.r59.governor"],
    "forward_emitted_pending_matured": ["api.canonical_forward_accrual"],
    "portfolio_proposal_state": ["api.portfolio_decision (via api.workflow_state)"],
    "runtime_source_identity": ["api.runtime_identity",
                                "alpha_agent.r59.runtime"],
}

#: Will the loop advance on its own?
A_ADVANCING = "ADVANCING_WITHOUT_A_HUMAN"
#: Everything outstanding clears when a market session elapses. No action.
A_WAITING_ON_TIME = "WAITING_ON_TIME_ONLY"
#: Something outstanding needs information the estate does not own. A human must
#: decide whether to acquire it; nothing will arrive by itself.
A_NEEDS_INFORMATION = "STALLED_PENDING_INFORMATION_THE_ESTATE_DOES_NOT_OWN"
#: Something outstanding is closed by a decision already made. Only a human can
#: reopen it, and no elapsed time or arriving data will.
A_NEEDS_DECISION = "STALLED_PENDING_A_HUMAN_GOVERNANCE_DECISION"
#: The research worker is not running at all.
A_WORKER_DOWN = "RESEARCH_WORKER_NOT_RUNNING"
AUTONOMY_STATES = (A_ADVANCING, A_WAITING_ON_TIME, A_NEEDS_INFORMATION,
                   A_NEEDS_DECISION, A_WORKER_DOWN)

SAFETY_BADGES = ("PREVIEW ONLY", "NO ORDERS", "ORDERS DISABLED",
                 "AUTOMATION OFF", "MANUAL REVIEW")

#: The accrual owner's refusal code for a strategy whose declared entry mark its
#: declared valuation path cannot serve. Named here, not re-derived: the string is
#: :data:`api.canonical_forward_accrual.INTEGRITY_ENTRY_MARK_UNPRICEABLE` and is
#: imported rather than retyped so a rename cannot silently stop matching.
def _unpriceable_blocker() -> str:
    try:
        from . import canonical_forward_accrual as _ACC
        return _ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE
    except Exception:                                       # noqa: BLE001
        return "DECLARED_ENTRY_MARK_IS_NOT_SERVED_BY_THE_DECLARED_VALUATION_PATH"


UNPRICEABLE_BLOCKER = _unpriceable_blocker()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _g(doc: Any, *path, default=None):
    """Read a nested key without ever raising. A projection may not crash."""
    cur = doc
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


# --------------------------------------------------------------------------- #
# The eight blocks
# --------------------------------------------------------------------------- #
def _last_completed_cycle(workflow: dict, worker: dict) -> dict:
    """What actually finished, in each of the three cycles."""
    return {
        "operational_session": _g(workflow, "current_session",
                                  "latest_eligible_completed_market_date"),
        "operational_session_status": _g(workflow, "current_session",
                                         "session_status"),
        "operational_stage": _g(workflow, "normal_cycle_stage"),
        "catch_up_required": _g(workflow, "catch_up_required"),
        "research_cycles_completed": worker.get("cycles_completed"),
        "research_last_completed_experiment": worker.get(
            "last_completed_experiment"),
        "research_last_challenger_freeze": worker.get("last_challenger_freeze"),
        "research_worker_started_at": worker.get("started_at"),
        "research_last_heartbeat": worker.get("last_heartbeat"),
    }


def _current_active_work(worker: dict) -> dict:
    """What is executing right now - which is usually nothing, said plainly."""
    state = worker.get("worker_state")
    return {
        "research_worker_state": state,
        "research_current_lane": worker.get("current_lane"),
        "research_lease_held": worker.get("lease_held"),
        "is_executing_research": bool(worker.get("current_lane")),
        "unproductive_streak": worker.get("unproductive_streak"),
        "latest_error": worker.get("latest_error"),
    }


def _next_scheduled_work(worker: dict, coverage: dict) -> dict:
    """What is due next. The research wake and the forward boundaries."""
    rows = coverage.get("registrations") or []
    boundaries = sorted(
        {(str(r.get("next_boundary_from_producer"))[:10], r.get("challenger_id"))
         for r in rows if r.get("next_boundary_from_producer")})
    return {
        "research_next_planned_wake": worker.get("next_planned_wake"),
        "research_wake_condition": worker.get("wake_condition"),
        "research_effective_sleep_seconds": worker.get(
            "effective_sleep_seconds"),
        "next_forward_boundary": boundaries[0][0] if boundaries else None,
        "next_forward_boundaries": [
            {"session": s, "challenger_id": c} for s, c in boundaries],
        "operator_next_required_action": None,
    }


def _current_blockers(workflow: dict, worker: dict) -> dict:
    """What is held, and - the part that was missing - what would release it."""
    mix = worker.get("blocker_clearance_mix") or {}
    return {
        "operational_blockers": list(_g(workflow, "blockers", default=[]) or []),
        "operational_blocking_data_gaps": _g(workflow,
                                             "blocking_data_gap_count"),
        "research_sleep_reason": worker.get("stop_or_sleep_reason"),
        "research_dominant_blocker": worker.get("blocker_reason"),
        "research_blockers_by_reason": worker.get("blocker_reasons") or {},
        "research_blocker_clearance_mix": mix,
        "research_terminal_blockers": int(
            mix.get("terminal_without_a_decision") or 0),
        "research_information_blockers": int(
            mix.get("needs_new_information") or 0),
        "research_time_blockers": int(mix.get("time_will_clear") or 0),
    }


def _research_candidate_and_stage(worker: dict, frontier: dict,
                                  mandates: dict) -> dict:
    """The candidate under test, the stage it is at, and what remains after it."""
    classes = _g(frontier, "asset_classes", default={}) or {}
    withheld = []
    for ac, row in classes.items():
        for w in (_g(row, "detail", "withheld_by_director_ruling",
                     default=[]) or []):
            withheld.append({"asset_class": ac,
                             "economic_family": w.get("economic_family"),
                             "blocker_reason": w.get("blocker_reason"),
                             "verdict": w.get("verdict"),
                             "reopen_condition": w.get("reopen_condition")})
    pending = [{"asset_class": m.get("asset_class"),
                "economic_family": m.get("family"),
                "kind": m.get("kind"),
                "mandate_id": m.get("mandate_id")}
               for m in (mandates.get("mandates") or [])]
    return {
        "current_lane": worker.get("current_lane"),
        "campaign_id": worker.get("campaign_id"),
        "last_completed_experiment": worker.get("last_completed_experiment"),
        "frontier_ready_scopes": list(_g(frontier, "ready", default=[]) or []),
        "frontier_non_equity_ready": list(
            _g(frontier, "non_equity_ready", default=[]) or []),
        "frontier_state_by_asset_class": {
            ac: _g(row, "state") for ac, row in classes.items()},
        "next_mandates": pending,
        "n_next_mandates": len(pending),
        "generator_terminal_reason": mandates.get("terminal"),
        # R79 - the families a durable director ruling has closed, with the
        # condition that reopens each. This is the block that was invisible.
        "withheld_by_director_ruling": withheld,
        "n_withheld_by_director_ruling": len(withheld),
    }


def _forward_ledger(accrual: dict) -> dict:
    """Emitted / pending / matured, per registration and in total."""
    rows, totals = [], {"predictions_emitted": 0, "forfeitures_recorded": 0,
                        "matured_observations": 0, "pending_observations": 0,
                        "effective_independent_observations": 0}
    for _hash, r in (accrual or {}).items():
        if not isinstance(r, dict) or not r.get("challenger_id"):
            continue
        for k in totals:
            try:
                totals[k] += int(r.get(k) or 0)
            except (TypeError, ValueError):
                pass
        rows.append({
            "challenger_id": r.get("challenger_id"),
            "asset_class": r.get("asset_class"),
            # The persisted projection names it ``current_accrual_state``; a live
            # ``assess_registration`` document names it ``state``. Both are read,
            # because reading only the second made every persisted row report a
            # null accrual state.
            "state": (r.get("current_accrual_state") or r.get("state")),
            "cadence_sessions": r.get("cadence_sessions"),
            "horizon_sessions": r.get("horizon_sessions"),
            "predictions_emitted": r.get("predictions_emitted"),
            "pending_observations": r.get("pending_observations"),
            "matured_observations": r.get("matured_observations"),
            "effective_independent_observations": r.get(
                "effective_independent_observations"),
            "last_emission_session": r.get("last_emission_session"),
            "latest_blocker": r.get("latest_blocker"),
            "entry_mark_feasible": _g(r, "entry_mark_feasibility", "feasible"),
            "entry_mark_refusal": (
                _g(r, "entry_mark_feasibility", "reason")
                # The refusal CODE is published as the accrual's own
                # ``latest_blocker`` and the feasibility block beside it. The
                # accrual stage is legitimately skipped while its inputs are
                # unchanged, so a projection persisted before that block existed
                # carries the code and not the block; reading both means the
                # refusal is visible from the moment it is made rather than from
                # the next advance.
                or (r.get("latest_blocker")
                    if r.get("latest_blocker")
                    == UNPRICEABLE_BLOCKER else None)),
        })
    rows.sort(key=lambda x: str(x.get("challenger_id")))
    unpriceable = [r["challenger_id"] for r in rows
                   if r.get("entry_mark_feasible") is False
                   or r.get("entry_mark_refusal") == UNPRICEABLE_BLOCKER]
    return {"registrations": rows, "n_registrations": len(rows),
            "totals": totals,
            "unpriceable_registrations": unpriceable,
            "n_unpriceable": len(unpriceable)}


def _portfolio_proposal_state(workflow: dict) -> dict:
    """The governed proposal, verbatim from the decision owner's projection.

    The workflow read model publishes the decision block under TWO names with
    two field vocabularies: ``canonical_portfolio_decision`` (the canonical
    owner's own projection, which is what the snapshot serves) and
    ``portfolio_decision`` (the operator-facing block). Reading only the second
    left every field null on a live read while an injected test document passed -
    the third instance in this release of one fact travelling under two spellings.
    Both are read, canonical first, and ``projected_under`` says which answered so
    a reader is never guessing which vocabulary they are looking at.

    R80 - THE FOURTH INSTANCE OF THE SAME DEFECT, AND IT IS NOT A SPELLING THIS
    TIME BUT A DEPTH. ``canonical_portfolio_decision`` carries ``proposal_hash``
    at its top level and carries NO ``proposal_id`` key at all: the canonical id
    travels one level down, inside ``proposal_supersession``, which is the block
    ``api.portfolio_decision`` writes and the block ``/v1/operations/
    proposal-decision-review`` serves ``proposal_id`` from. A top-level-only
    ``_pick`` therefore published ``proposal_id: null`` beside a real
    ``proposal_hash`` for a proposal the estate can name, which is worse than
    omitting the field: a reader reconciling this surface against the review
    route sees the review name a proposal and the autonomy surface deny it.

    ``_pick`` now falls back to the NESTED identity blocks after exhausting both
    top levels. The order is the same as everywhere else in this projection -
    canonical first - and no value is ever synthesised: if no block carries the
    name, the answer is still None.
    """
    canon = _g(workflow, "canonical_portfolio_decision", default={}) or {}
    pd = _g(workflow, "portfolio_decision", default={}) or {}
    src = canon or pd

    #: Blocks that carry proposal identity one level below the decision block.
    #: A fact must be readable on the seam its reader uses, and the id's seam is
    #: the supersession block rather than the decision root.
    def _nested() -> list:
        out = []
        for block in (canon, pd):
            for key in ("proposal_supersession", "supersession"):
                nested = block.get(key)
                if isinstance(nested, dict):
                    out.append(nested)
        return out

    def _pick(*names):
        for n in names:
            if src.get(n) is not None:
                return src[n]
        for n in names:
            if pd.get(n) is not None:
                return pd[n]
        for block in _nested():
            for n in names:
                if block.get(n) is not None:
                    return block[n]
        return None

    return {
        "projected_under": ("canonical_portfolio_decision" if canon
                           else "portfolio_decision" if pd else None),
        "portfolio_decision_state": _pick("decision_state",
                                          "portfolio_decision_state", "state"),
        "proposal_id": _pick("proposal_id"),
        "proposal_hash": _pick("proposal_hash"),
        "proposal_state": _pick("proposal_state"),
        "one_way_turnover": _pick("expected_one_way_turnover",
                                  "one_way_turnover"),
        "estimated_transaction_cost": _pick("expected_transaction_cost_usd",
                                           "estimated_transaction_cost"),
        "score_improvement_net_of_cost": _pick(
            "expected_net_improvement", "score_improvement_net_of_cost"),
        "switching_hurdle": _pick("switching_hurdle", "net_improvement_hurdle"),
        "clears_switching_hurdle": _pick("clears_switching_hurdle"),
        "approvable": _pick("approvable"),
        "requires_manual_review": _pick("manual_review_only",
                                        "requires_manual_review"),
        "mandatory_repair_code": _pick("mandatory_repair_code",
                                       "mandatory_exit_obligation"),
        "change_withheld": _pick("change_withheld"),
        "withheld_reasons": _pick("withheld_reasons"),
        "hold_current_book": _pick("hold_current_book"),
        "feasible_target_exists": _pick("feasible_target_exists"),
        "decision": _pick("decision"),
        "decision_is_current": _pick("decision_is_current"),
        "creates_orders": _pick("creates_orders"),
        "automation_off": _pick("automation_off"),
        "reassessment_state": (_pick("reassessment_state")
                              or _g(workflow, "portfolio_reassessment_state")),
        "reallocation_state": (_pick("reallocation_outcome")
                              or _g(workflow, "reallocation_operator_state")),
    }


def _runtime_source_identity(ready: dict, worker: dict) -> dict:
    """WHICH CODE each process is running. Two processes, two answers.

    R80 - AND WHETHER THAT CODE MAY STILL WRITE. ``maturation_gate`` published
    ``worker["maturation"]``, which is the verdict the worker took ONCE, about
    the source as it was at startup. For a worker that runs a cycle and exits
    that is the whole question. This one is persistent - it has held its lease
    since 16:05 and will hold it for days - so the startup answer describes a
    checkout that no longer exists, and ``alpha_agent.r59.runtime`` says so in
    as many words: it re-takes the attestation at the moment of use and records
    it as ``maturation_now``.

    The two disagree right now, and the surface was publishing the wrong one.
    The worker's own artifact carries ``maturation_now`` = allowed FALSE,
    SOURCE_HAS_UNCOMMITTED_CHANGES (loaded 5e98c80, checkout b0f7cb3, dirty)
    while this block published allowed TRUE, COMMITTED_CLEAN_SOURCE - so the
    one surface an operator reads to ask "can the estate still record forward
    evidence?" answered yes on behalf of a worker that answers no, two days
    before two real decision boundaries. ``processes_agree`` compounded it:
    backend and worker do agree, and both are behind the checkout, which the
    field cannot say because it only ever compared the two processes.

    So the at-use verdict is published beside the startup one, never instead of
    it, and the field that states the permission is the AT-USE one. Nothing is
    computed here: both verdicts are the worker's own, read from its artifact,
    and a worker that recorded no at-use verdict yields None rather than an
    inferred yes.
    """
    src = worker.get("source_identity") or {}
    # The worker re-takes this at every prospective write. Absent (an older
    # worker, or one that has not reached a write) it stays None: this
    # projection never derives a permission the owner did not record.
    at_use = worker.get("maturation_now") or None
    checkout = (at_use or {}).get("current_commit")
    # ``api.runtime_identity.loaded_identity`` publishes ``commit``; the
    # ``/v1/ready`` envelope republishes the same value as ``loaded_commit``.
    # Both spellings are accepted so this block reads the owner directly or the
    # envelope, and reports the SAME answer either way - reading only the
    # envelope's spelling made two identical commits compare unequal.
    backend = str(ready.get("commit") or ready.get("loaded_commit") or "")
    research = str(src.get("commit") or src.get("head") or "")
    return {
        "backend_loaded_commit": backend or None,
        "backend_loaded_branch": (ready.get("branch")
                                 or ready.get("loaded_branch")),
        "backend_loaded_pid": ready.get("pid") or ready.get("loaded_pid"),
        "backend_identity_owner": (ready.get("owner")
                                  or ready.get("loaded_identity_owner")),
        "research_worker_source": src,
        "research_worker_commit": research or None,
        "research_worker_dirty": src.get("dirty"),
        # The STARTUP verdict, kept under its established name so no reader
        # loses a field, and labelled so none mistakes it for permission.
        "maturation_gate": worker.get("maturation"),
        "maturation_gate_is_the_startup_verdict": True,
        # The verdict that actually governs the next prospective write.
        "maturation_gate_at_use": at_use,
        "may_write_prospective_evidence_now": (
            None if not at_use else bool(at_use.get("allowed"))),
        "prospective_write_blocker": (
            None if not at_use or at_use.get("allowed")
            else at_use.get("reason")),
        "startup_and_at_use_verdicts_agree": (
            None if not at_use or not worker.get("maturation")
            else bool(at_use.get("allowed")
                      is (worker.get("maturation") or {}).get("allowed"))),
        # R80 - the THIRD identity. The two processes can agree with each other
        # and both be behind the checkout, which is this estate's actual state.
        "checkout_commit": checkout,
        "processes_agree": bool(backend and research
                               and backend[:12] == research[:12]),
        "processes_agree_with_checkout": (
            None if not (backend and research and checkout)
            else bool(backend[:12] == research[:12] == str(checkout)[:12])),
    }


# --------------------------------------------------------------------------- #
# The verdict
# --------------------------------------------------------------------------- #
def _autonomy_verdict(*, active: dict, blockers: dict, candidate: dict) -> dict:
    """Will this loop advance without a human? Derived, never asserted.

    Read the order carefully: a worker that is not running outranks everything,
    then a TERMINAL blocker, then an INFORMATION blocker, then executable work.
    A live lease never appears in this decision, because process liveness was
    exactly the evidence that made a dead research program look healthy.
    """
    state = str(active.get("research_worker_state") or "")
    if not state or state in ("STOPPED", "STOPPED_LEASE_LOST",
                              "REFUSED_ANOTHER_WORKER_HOLDS_THE_LEASE"):
        return {"state": A_WORKER_DOWN,
                "advances_without_a_human": False,
                "reason": "the research worker is not in a running state (%s)"
                          % (state or "no status artifact"),
                "what_would_release_it": "start the canonical research worker"}
    if int(blockers.get("research_terminal_blockers") or 0) > 0:
        return {"state": A_NEEDS_DECISION,
                "advances_without_a_human": False,
                "reason": ("%d outstanding research job(s) are blocked on a "
                           "governance decision already made against them; no "
                           "elapsed session and no arriving data can release "
                           "them"
                           % int(blockers["research_terminal_blockers"])),
                "what_would_release_it": (
                    "a human decision that reopens a ruled family, or a new "
                    "information source that satisfies its reopen condition")}
    if candidate.get("generator_terminal_reason") or (
            int(candidate.get("n_next_mandates") or 0) == 0
            and not active.get("is_executing_research")):
        return {"state": A_NEEDS_INFORMATION,
                "advances_without_a_human": False,
                "reason": ("the research director can generate no executable "
                           "mandate from the information the estate owns (%s)"
                           % (candidate.get("generator_terminal_reason")
                              or "no candidate remains")),
                "what_would_release_it": (
                    "new admissible information; the purchase gate decides "
                    "whether any is worth acquiring")}
    if int(blockers.get("research_information_blockers") or 0) > 0 and not \
            active.get("is_executing_research"):
        return {"state": A_NEEDS_INFORMATION,
                "advances_without_a_human": False,
                "reason": ("%d outstanding job(s) need information the estate "
                           "does not own"
                           % int(blockers["research_information_blockers"])),
                "what_would_release_it": "new admissible information"}
    if active.get("is_executing_research") or int(
            candidate.get("n_next_mandates") or 0) > 0:
        return {"state": A_ADVANCING, "advances_without_a_human": True,
                "reason": "executable research remains and the worker is live",
                "what_would_release_it": None}
    return {"state": A_WAITING_ON_TIME, "advances_without_a_human": True,
            "reason": "everything outstanding clears when a market session "
                      "elapses",
            "what_would_release_it": None}


# --------------------------------------------------------------------------- #
# The one read
# --------------------------------------------------------------------------- #
def build_autonomous_operating_status(
        *, workflow_state: Optional[dict] = None,
        worker_status: Optional[dict] = None,
        accrual: Optional[dict] = None,
        producer_coverage: Optional[dict] = None,
        frontier_view: Optional[dict] = None,
        mandates: Optional[dict] = None,
        ready: Optional[dict] = None) -> dict:
    """Compose the ONE authoritative autonomy status. Strictly read-only.

    Every argument is an injection point for the owner's own document, so a test
    can hand this function a world and the route can hand it the live one. An
    owner that cannot be read contributes its absence, never a guess: the block
    is present with null fields and the owner is listed in ``degraded_owners``,
    because a status surface that omits what it could not read is how a silent
    outage stays silent.
    """
    degraded: list = []

    def _read(name: str, fn, default):
        if fn is None:
            return default
        try:
            return fn()
        except Exception as exc:                            # noqa: BLE001
            degraded.append({"owner": name,
                             "error": "%s: %s" % (type(exc).__name__,
                                                  str(exc)[:160])})
            return default

    if worker_status is None:
        def _worker():
            from alpha_agent.r59 import runtime as RT
            return RT.read_status() or {}
        worker_status = _read("alpha_agent.r59.runtime", _worker, {})
    if workflow_state is None:
        def _wf():
            # Through the ONE composed snapshot the operator surfaces already
            # read (Release 50), never by re-deriving api.workflow_state: a
            # status projection that recomposed the workflow would be a second
            # interpretation of it, which is the thing this module exists not to
            # be.
            from . import decision_snapshot as SNAP
            return SNAP.section("workflow")
        workflow_state = _read("api.workflow_state", _wf, {})
    if accrual is None:
        def _acc():
            from . import canonical_forward_accrual as ACC
            return ACC.load_accrual_projection()
        accrual = _read("api.canonical_forward_accrual", _acc, {})
    if producer_coverage is None:
        def _cov():
            from . import forward_producer_health as PH
            return PH.producer_coverage()
        producer_coverage = _read("api.forward_producer_health", _cov, {})
    if frontier_view is None:
        def _fr():
            from alpha_agent.r59 import frontier as FR, memory as M
            return FR.measure(M.open_memory())
        frontier_view = _read("alpha_agent.r59.frontier", _fr, {})
    if mandates is None:
        def _gov():
            from alpha_agent.r59 import governor as GOV, memory as M
            return GOV.generate_mandates(M.open_memory(), limit=8,
                                         frontier_view=frontier_view or None)
        mandates = _read("alpha_agent.r59.governor", _gov, {})
    if ready is None:
        def _rid():
            from . import runtime_identity as RID
            return RID.loaded_identity()
        ready = _read("api.runtime_identity", _rid, {})

    last = _last_completed_cycle(workflow_state or {}, worker_status or {})
    active = _current_active_work(worker_status or {})
    nxt = _next_scheduled_work(worker_status or {}, producer_coverage or {})
    nxt["operator_next_required_action"] = _g(workflow_state,
                                              "operator_action_code")
    blockers = _current_blockers(workflow_state or {}, worker_status or {})
    candidate = _research_candidate_and_stage(worker_status or {},
                                              frontier_view or {},
                                              mandates or {})
    forward = _forward_ledger(accrual or {})
    portfolio = _portfolio_proposal_state(workflow_state or {})
    identity = _runtime_source_identity(ready or {}, worker_status or {})
    verdict = _autonomy_verdict(active=active, blockers=blockers,
                                candidate=candidate)

    return {
        "status": "OK",
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _now_iso(),
        "projected_from": dict(PROJECTED_FROM),
        "degraded_owners": degraded,
        "n_degraded_owners": len(degraded),
        "LAST_COMPLETED_CYCLE": last,
        "CURRENT_ACTIVE_WORK": active,
        "NEXT_SCHEDULED_WORK": nxt,
        "CURRENT_BLOCKERS": blockers,
        "RESEARCH_CANDIDATE_AND_STAGE": candidate,
        "FORWARD_EMITTED_PENDING_MATURED": forward,
        "PORTFOLIO_PROPOSAL_STATE": portfolio,
        "RUNTIME_SOURCE_IDENTITY": identity,
        "autonomy": verdict,
        "autonomy_state_vocabulary": list(AUTONOMY_STATES),
        "safety_badges": list(SAFETY_BADGES),
        "read_only": True,
        "performed_write": False,
        "creates_orders": False,
        "approves_anything": False,
        "automation_enabled": False,
        "promotes_to_live": False,
    }


__all__ = ["CALCULATION_OWNER", "PROJECTED_FROM", "AUTONOMY_STATES",
           "A_ADVANCING", "A_WAITING_ON_TIME", "A_NEEDS_INFORMATION",
           "A_NEEDS_DECISION", "A_WORKER_DOWN", "SAFETY_BADGES",
           "build_autonomous_operating_status"]
