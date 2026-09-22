r"""alpha_agent.r52.eligibility - may the EXPENSIVE maturation work be skipped?

R64 stopped the autonomous researcher spending a CPU core re-asking a question
it had already been refused 4,825 times. It repaired the *sleep*. It did not
repair what the worker does when it wakes, and the R52 run journal measured
exactly what that costs:

    400 retained runs, 2026-09-20T21:24Z .. 2026-09-22T20:17Z (46.9 h)
    mean 292 s, median 286 s, max 1014 s, TOTAL 116,747 s = 32.4 CPU-hours
    median gap between the end of one run and the start of the next: 8 s
    measured duty cycle: 91-105% of one core, hour after hour
    runs in which ANY substantive stage produced something:  16 / 400  (4%)

Ninety-six per cent of that work re-derived a conclusion from inputs that had
not moved. This module is the correction, and it is deliberately NOT a
scheduler: it holds no cadence, fires nothing, and cannot make the runtime run.
It answers ONE question for the runtime that already owns the cadence -

    has anything changed since the last completed cycle that could make the
    expensive stages reach a different answer?

and it answers it from the CANONICAL owners, never from a clock rule of its
own: :mod:`alpha_agent.r46.clock` for sessions, :mod:`alpha_agent.r52`'s own
derived timing contract for lane dueness and emission mode, the evidence chain
verifiers for row counts, and each forward owner's declared store for its
watermark.

WHAT IS NEVER GATED. The three per-session owners (next-open, FX carry cadence,
futures trend) hold live decision windows and poll external publication state
that no local watermark can see. They are measured at ~8 s for all three and
they run on EVERY invocation, gate or no gate. The R62.3.3 rule that a window
is covered by firing repeatedly inside it is preserved verbatim. What is gated
is the ~283 s of tournament advance, forfeiture sweep, Stage-26 mark, canonical
accrual, velocity rebuild and frontier refresh - and only while every input
they read is byte-identical to the state the last completed cycle left behind.

FAIL-OPEN, ALWAYS. A term that cannot be resolved, a previous cycle that did
not complete, a cheap owner that just froze a decision, or simply too long
since the last real cycle - each of these RUNS the expensive work. The only
path to a skip is: every term resolved, every term unchanged, the last cycle
completed, and the ceiling not yet reached.

RESEARCH ONLY. This module reads. It writes exactly one small bookmark
(``maturation_gate.json``) into the R52 runtime root, which is deliberately
excluded from its own watermark set so the bookmark can never look like news.
"""
from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path

from . import artifact_body, read_json, runtime_dir, sha, write_json
from . import timing_contract as TC
from ..r46 import clock as CK

CALCULATION_OWNER = "alpha_agent.r52.eligibility"

GATE_ARTIFACT = "maturation_gate.json"

#: The ceiling. Even with a byte-identical fingerprint the expensive stages run
#: at least this often, so a term this module failed to imagine can delay
#: evidence by hours and never by days.
#:
#: DERIVED, not invented: :data:`alpha_agent.r52.timing_contract.INVOCATION_PLAN`
#: declares 08:15 / 17:45 / 19:45 / 21:45 Eastern, whose widest gap is 10.5 h
#: (21:45 -> 08:15). Six hours is strictly tighter than the cadence the timing
#: contract itself declares sufficient for every prospective lane, so a gated
#: runtime is never sparser than an ungated scheduled one.
MAX_SKIP_SECONDS = 6 * 3600.0

#: A directory walk is capped so a store that grows without bound can never
#: turn a cheap precondition into an expensive one. Hitting the cap yields an
#: UNRESOLVED term, which fails open and runs the cycle.
WALK_FILE_CAP = 20000

#: Suffixes written and replaced atomically by :func:`alpha_agent.r52.write_json`
#: and its siblings. A transient temp file is not news.
_IGNORED_SUFFIXES = (".tmp",)

# --- verdict vocabulary ----------------------------------------------------- #
RUN_FORCED = "RUN_FORCED_BY_CALLER"
RUN_NO_PRIOR = "RUN_NO_PRIOR_FINGERPRINT"
RUN_UNRESOLVED = "RUN_INPUT_COULD_NOT_BE_RESOLVED"
RUN_LAST_INCOMPLETE = "RUN_LAST_CYCLE_DID_NOT_COMPLETE"
RUN_OWNER_PROGRESSED = "RUN_A_PER_SESSION_OWNER_PROGRESSED_THIS_CYCLE"
RUN_INPUTS_CHANGED = "RUN_INPUTS_CHANGED"
RUN_CEILING = "RUN_MAX_SKIP_INTERVAL_ELAPSED"
SKIP_UNCHANGED = "SKIP_INPUTS_UNCHANGED"

RUN_REASONS = (RUN_FORCED, RUN_NO_PRIOR, RUN_UNRESOLVED, RUN_LAST_INCOMPLETE,
               RUN_OWNER_PROGRESSED, RUN_INPUTS_CHANGED, RUN_CEILING)
GATE_REASONS = RUN_REASONS + (SKIP_UNCHANGED,)

#: The stages this gate may hold back, named so the journal, the health read
#: model and the audit all quote ONE list.
GATED_STAGES = ("tournament_advance", "forfeiture_sweep",
                "stage26_prospective_mark", "canonical_forward_accrual",
                "velocity_operational", "promotion_frontier")

#: The stages that run on every invocation whatever the gate says. Each owns a
#: live decision window and external publication state; all three together were
#: measured at ~8 s, against ~283 s for the gated set.
UNGATED_STAGES = ("next_open_prospective_decision",
                  "fx_carry_cadence_prospective_decision",
                  "futures_trend_prospective_decision")

#: Stage states that mean a per-session owner MOVED - new evidence exists, so
#: the gated set must run in this same cycle rather than wait for the next one.
PROGRESS_STAGE_STATES = ("SUCCESS", "FORFEITED")

_UNRESOLVED = "UNRESOLVED"


# --------------------------------------------------------------------------- #
# Watermarks: what each forward owner's OWN declared store looks like right now
# --------------------------------------------------------------------------- #
def _dir_mark(root) -> str:
    """A cheap, recursive change detector for one store.

    ``<n_files>:<newest mtime, whole seconds>:<total bytes>``. Measured at
    33 ms for the complete watched set. Returns :data:`_UNRESOLVED` on any
    error or on hitting :data:`WALK_FILE_CAP`, which fails open.
    """
    n = 0
    newest = 0
    total = 0
    try:
        for dirpath, _dirnames, filenames in os.walk(str(root)):
            for name in filenames:
                if name.endswith(_IGNORED_SUFFIXES):
                    continue
                n += 1
                if n > WALK_FILE_CAP:
                    return _UNRESOLVED
                try:
                    st = os.stat(os.path.join(dirpath, name))
                except OSError:
                    continue
                if int(st.st_mtime) > newest:
                    newest = int(st.st_mtime)
                total += st.st_size
    except OSError:
        return _UNRESOLVED
    return "%d:%d:%d" % (n, newest, total)


def watched_stores() -> list:
    """``[(term_name, path)]`` - every store the GATED stages read, resolved
    from the module that owns it rather than repeated here.

    Two deliberate exclusions:

    * the R52 runtime root, because every file in it is written BY the cycle
      (the timing contract, the run journal, the health read model, the
      refreshed frontier, this gate's own bookmark). Including it would make
      the runtime permanently look like news to itself. Nothing else writes
      there, and the one input it holds - the forfeiture ledger - is already
      counted row by row in the chain terms.
    * the ``r41`` marks sub-store of each forward owner, because it is a
      subdirectory of that owner's forward directory and already walked.
    """
    out = []

    def _add(name, fn):
        try:
            p = fn()
        except Exception:                                    # noqa: BLE001
            p = None
        out.append((name, Path(p) if p else None))

    from ..r46 import campaign_dir as _campaign_dir
    _add("store.r46_campaign", _campaign_dir)

    def _canonical_accrual():
        from paper_trader.api import canonical_forward_accrual as CFA
        return CFA.store_dir()
    _add("store.canonical_accrual", _canonical_accrual)

    def _stage26():
        from .. import stage26_forward_runtime as S26F
        return S26F.store_dir()
    _add("store.stage26", _stage26)

    def _decisions():
        from ..alpha_recovery import prospective_decision as PD
        return Path(PD.research_root()) / PD.DECISIONS_SUBDIR
    _add("store.prospective_decisions", _decisions)

    def _fx_forward():
        from ..alpha_recovery import fx_carry_cadence_runtime as FXR
        return FXR.forward_dir()
    _add("store.fx_carry_forward", _fx_forward)

    def _futures_forward():
        from ..alpha_recovery import futures_trend_runtime as FTR
        return FTR.forward_dir()
    _add("store.futures_trend_forward", _futures_forward)

    def _frozen_curves():
        from ..alpha_recovery import fx_carry_cadence_runtime as FXR
        return FXR.FROZEN_CURVES_DIR
    _add("store.frozen_curves", _frozen_curves)

    return out


def store_watermarks() -> dict:
    return {name: (_dir_mark(path) if path is not None else _UNRESOLVED)
            for name, path in watched_stores()}


# --------------------------------------------------------------------------- #
# Clock and contract terms, every one of them read from a canonical owner
# --------------------------------------------------------------------------- #
def clock_terms(now: _dt.datetime, *, contract=None) -> dict:
    """Session identity and lane dueness, from the owners that hold them.

    This adds no rule. ``eastern_date`` and ``entry_session_date`` come from
    :mod:`alpha_agent.r46.clock`; ``owned_last_session`` and ``emission_mode``
    come from the derived timing contract, which reads them from
    :mod:`alpha_agent.r46.marketdata` and its own emission policy; lane dueness
    comes from :func:`alpha_agent.r46.lanes.registry` through that contract.

    ``owned_last_session`` is the term that answers "has a NEW ELIGIBLE TRADING
    SESSION arrived": it moves the moment the owned nightly refresh prints, and
    a moved term runs the cycle.
    """
    terms = {}
    try:
        terms["clock.eastern_date"] = str(CK.eastern_date(now))
    except Exception:                                        # noqa: BLE001
        terms["clock.eastern_date"] = _UNRESOLVED
    try:
        terms["clock.entry_session_date"] = str(CK.entry_session_date(now))
    except Exception:                                        # noqa: BLE001
        terms["clock.entry_session_date"] = _UNRESOLVED

    body = contract
    if body is None:
        try:
            body = TC.build(now, write=False)
        except Exception:                                    # noqa: BLE001
            body = None
    policy = (body or {}).get("emission_policy_now") or {}
    terms["clock.owned_last_session"] = str(
        policy.get("owned_last_session") or _UNRESOLVED)
    terms["clock.emission_mode"] = str(policy.get("mode") or _UNRESOLVED)

    lanes = (body or {}).get("lanes")
    if lanes is None:
        terms["lanes.due"] = _UNRESOLVED
        terms["lanes.next_dates"] = _UNRESOLVED
    else:
        terms["lanes.due"] = ",".join(sorted(
            str(r.get("lane_id")) for r in lanes if r.get("due_today")))
        terms["lanes.next_dates"] = ";".join(sorted(
            "%s=%s/%s" % (r.get("lane_id"), r.get("next_call_date"),
                          r.get("next_decision_date")) for r in lanes))
    return terms


def chain_terms(chains=None) -> dict:
    """Row counts of every shared evidence chain.

    The runtime verifies these chains BEFORE anything may be written, so when
    it passes its report this costs nothing. A new prediction, a new outcome, a
    new continuation row or a new forfeiture moves a count, and a moved count
    runs the cycle.

    When no report is supplied this verifies them itself (measured at 0.36 s)
    rather than returning UNRESOLVED. A fingerprint that is permanently
    unresolvable for any caller who forgets an argument is a gate that can
    never close, which would be an expensive and silent way to lose the whole
    point of the release.
    """
    terms = {}
    reports = (chains or {}).get("chains")
    if not reports:
        try:
            from . import runtime as _RT
            reports = (_RT._chains_ok() or {}).get("chains")
        except Exception:                                    # noqa: BLE001
            reports = None
    if not reports:
        return {"chain.rows": _UNRESOLVED}
    for family, rep in sorted(reports.items()):
        ledgers = rep.get("ledgers")
        if not ledgers:
            terms["chain.%s" % family] = _UNRESOLVED
            continue
        terms["chain.%s" % family] = ",".join(
            "%s=%s" % (l.get("ledger"), l.get("n_rows"))
            for l in sorted(ledgers, key=lambda r: str(r.get("ledger"))))
    return terms


def fingerprint(now: _dt.datetime, *, contract=None, chains=None) -> dict:
    """Every cheap term that could change what the gated stages conclude."""
    terms = {}
    terms.update(clock_terms(now, contract=contract))
    terms.update(chain_terms(chains))
    terms.update(store_watermarks())
    unresolved = sorted(k for k, v in terms.items() if v == _UNRESOLVED)
    return {"digest": sha(terms), "terms": terms, "unresolved": unresolved,
             "n_terms": len(terms)}


def changed_terms(previous_terms, current_terms) -> list:
    prev = dict(previous_terms or {})
    cur = dict(current_terms or {})
    names = set(prev) | set(cur)
    return sorted(n for n in names if prev.get(n) != cur.get(n))


# --------------------------------------------------------------------------- #
# The bookmark
# --------------------------------------------------------------------------- #
def gate_path() -> Path:
    return runtime_dir() / GATE_ARTIFACT


def load_gate() -> dict:
    return read_json(gate_path(), default={}) or {}


def _write_gate(body: dict) -> Path:
    return write_json(gate_path(), body)


def record_cycle(*, now: _dt.datetime, run_id: str, run_state: str,
                 print_: dict, verdict: dict) -> dict:
    """Bookmark the state the EXPENSIVE stages have just been brought up to.

    Called AFTER the cycle's own writes, so the fingerprint recorded here is
    the post-run state. The next invocation compares its pre-run fingerprint
    against it: equal means nothing external moved in between.
    """
    body = artifact_body(
        "r52_maturation_gate/1", CALCULATION_OWNER,
        statement="a bookmark, not a schedule: it records the inputs the "
                  "expensive maturation stages were last brought up to, so an "
                  "unchanged world is not re-derived",
        recorded_at_utc=CK.iso(now),
        last_run_id=run_id,
        last_run_state=run_state,
        last_run_finished_utc=CK.iso(now),
        last_run_reason=verdict.get("reason"),
        digest=print_.get("digest"),
        n_terms=print_.get("n_terms"),
        terms=print_.get("terms"),
        max_skip_seconds=MAX_SKIP_SECONDS,
        next_forced_run_utc=CK.iso(
            now + _dt.timedelta(seconds=MAX_SKIP_SECONDS)),
        skips_since_last_run=0,
        gated_stages=list(GATED_STAGES),
        ungated_stages=list(UNGATED_STAGES),
        reason_vocabulary=list(GATE_REASONS))
    _write_gate(body)
    return body


def record_skip(*, now: _dt.datetime, verdict: dict) -> dict:
    """Count a skip without disturbing the recorded fingerprint.

    The digest stays exactly as the last completed cycle left it; only the
    observation counters move. A skip is therefore never able to make a stale
    bookmark look fresh.
    """
    prior = load_gate()
    if not prior:
        return {}
    body = dict(prior)
    body["last_evaluated_utc"] = CK.iso(now)
    body["last_evaluation_reason"] = verdict.get("reason")
    body["skips_since_last_run"] = int(
        prior.get("skips_since_last_run") or 0) + 1
    body["seconds_saved_estimate"] = None
    _write_gate(body)
    return body


# --------------------------------------------------------------------------- #
# The verdict
# --------------------------------------------------------------------------- #
def decide(now: _dt.datetime, *, contract=None, chains=None,
             previous=None, ungated_stages=None, force: bool = False) -> dict:
    """May the expensive stages be skipped at ``now``?

    ``ungated_stages`` is the list of stage rows the per-session owners have
    ALREADY produced in this same cycle. One of them reporting progress is new
    evidence and runs the gated set immediately - the gate never defers work
    the same cycle has just created.
    """
    prev = previous if previous is not None else load_gate()
    print_ = fingerprint(now, contract=contract, chains=chains)

    def _verdict(run, reason, **extra):
        out = {"calculation_owner": CALCULATION_OWNER,
               "run": bool(run),
               "reason": reason,
               "digest": print_["digest"],
               "previous_digest": prev.get("digest"),
               "n_terms": print_["n_terms"],
               "unresolved_terms": print_["unresolved"],
               "gated_stages": list(GATED_STAGES),
               "ungated_stages": list(UNGATED_STAGES),
               "max_skip_seconds": MAX_SKIP_SECONDS,
               "skips_since_last_run": int(
                   prev.get("skips_since_last_run") or 0),
               "last_run_id": prev.get("last_run_id"),
               "last_run_finished_utc": prev.get("last_run_finished_utc"),
               "seconds_since_last_run": _age(now, prev),
               "is_a_scheduler": False}
        out.update(extra)
        out["fingerprint"] = print_
        return out

    if force:
        return _verdict(True, RUN_FORCED,
                        detail="the caller asked for the expensive stages "
                               "explicitly; the gate never overrides that")
    if print_["unresolved"]:
        return _verdict(True, RUN_UNRESOLVED,
                        detail="%d input(s) could not be established (%s); an "
                               "unestablished input is treated as changed"
                               % (len(print_["unresolved"]),
                                  ", ".join(print_["unresolved"][:6])))
    # Only the three per-session owners count. The rows above them in the
    # cycle (lock, timing contract, chain integrity) report SUCCESS on every
    # healthy invocation, and treating those as "an owner moved" would make
    # the gate answer RUN forever while looking like it worked.
    progressed = sorted(
        str(s.get("stage")) for s in (ungated_stages or ())
        if str(s.get("stage")) in UNGATED_STAGES
        and str(s.get("state")) in PROGRESS_STAGE_STATES)
    if progressed:
        return _verdict(True, RUN_OWNER_PROGRESSED,
                        progressed_stages=progressed,
                        detail="a per-session owner moved in this cycle (%s); "
                               "the evidence it just produced is scored now, "
                               "not at the next invocation"
                               % ", ".join(progressed))
    if not prev or not prev.get("digest"):
        return _verdict(True, RUN_NO_PRIOR,
                        detail="no completed cycle has recorded a fingerprint "
                               "yet, so nothing can be known to be unchanged")
    if str(prev.get("last_run_state")) != "RUN_COMPLETED":
        return _verdict(True, RUN_LAST_INCOMPLETE,
                        last_run_state=prev.get("last_run_state"),
                        detail="the previous cycle ended %s; a cycle that did "
                               "not complete is retried, never skipped"
                               % prev.get("last_run_state"))
    age = _age(now, prev)
    if age is None or age >= MAX_SKIP_SECONDS:
        return _verdict(True, RUN_CEILING,
                        detail="%s since the last completed cycle reaches the "
                               "%.0f s ceiling; the expensive stages run even "
                               "with an unchanged world"
                               % ("an unknown interval" if age is None
                                  else "%.0f s" % age, MAX_SKIP_SECONDS))
    moved = changed_terms(prev.get("terms"), print_["terms"])
    if moved:
        return _verdict(True, RUN_INPUTS_CHANGED, changed_terms=moved,
                        detail="%d input(s) moved since the last completed "
                               "cycle: %s" % (len(moved), ", ".join(moved[:8])))
    return _verdict(False, SKIP_UNCHANGED, changed_terms=[],
                    detail="every one of the %d inputs the expensive stages "
                           "read is byte-identical to the state the last "
                           "completed cycle left behind; re-deriving them "
                           "would reach the same answer"
                           % print_["n_terms"])


def _age(now: _dt.datetime, prev: dict):
    stamp = (prev or {}).get("last_run_finished_utc")
    if not stamp:
        return None
    try:
        then = _dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=_dt.timezone.utc)
    ref = now if now.tzinfo is not None else now.replace(
        tzinfo=_dt.timezone.utc)
    return max(0.0, (ref - then).total_seconds())


__all__ = ["CALCULATION_OWNER", "GATE_ARTIFACT", "MAX_SKIP_SECONDS",
           "WALK_FILE_CAP", "GATED_STAGES", "UNGATED_STAGES",
           "PROGRESS_STAGE_STATES", "GATE_REASONS", "RUN_REASONS",
           "RUN_FORCED", "RUN_NO_PRIOR", "RUN_UNRESOLVED",
           "RUN_LAST_INCOMPLETE", "RUN_OWNER_PROGRESSED", "RUN_INPUTS_CHANGED",
           "RUN_CEILING", "SKIP_UNCHANGED",
           "watched_stores", "store_watermarks", "clock_terms", "chain_terms",
           "fingerprint", "changed_terms", "gate_path", "load_gate",
           "record_cycle", "record_skip", "decide"]
