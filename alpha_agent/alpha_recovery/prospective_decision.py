r"""alpha_agent.alpha_recovery.prospective_decision - the RESEARCH owner of the
immutable prospective signal decision.

THE SEPARATION THIS EXISTS TO CREATE
------------------------------------
``api.canonical_forward_accrual`` is the live accrual owner. It must be able to
take a frozen weight book and accrue forward evidence against it, and it must
NEVER be able to produce that book itself - because an accrual owner that can
compute the signal is an accrual owner that can compute it twice and keep the
better answer.

So the two responsibilities are split by construction:

    RESEARCH (this module)         computes nothing itself either, but it is the
                                   only place a decision may be FROZEN. It is
                                   handed a weight book by the campaign's own
                                   scorer and writes it once, immutably.
    LIVE (the accrual owner)       READS the frozen artifact. It cannot download
                                   an option chain, compute an implied
                                   volatility, form a z-score, choose a
                                   position, promote a model or allocate
                                   capital, because none of that code is
                                   reachable from it.

The artifact is the seam, and it is a file rather than a function call, so the
live side depends on DATA and never on the research package's behaviour.

WHAT "IMMUTABLE" MEANS HERE
---------------------------
First write wins, per ``(challenger, eligible session)``. A second freeze of the
IDENTICAL decision is idempotent and returns the record already held - a retry
after a crash resolves to the same artifact rather than a second one. A second
freeze that differs in any economically meaningful field is REFUSED: the stored
record stands, and the conflict is reported rather than resolved. There is no
code path in this module that overwrites, amends or deletes a frozen decision.

THE THREE REFUSALS THAT MAKE A DECISION PROSPECTIVE
---------------------------------------------------
1. TOO EARLY. A decision may not be frozen before its declared information
   cutoff, because the inputs it claims to be formed from do not exist yet.
2. TOO LATE. A decision may not be frozen after its emission window shuts. A
   session whose window has closed is MISSED, permanently: there is no
   retrospective creation here and no backfill, and a missed session simply has
   no artifact for the rest of time.
3. CONTAMINATED. A decision whose inputs were observed after the declared cutoff
   is refused even when it is written inside its window, because when it was
   written and what it was written from are two different questions.

The window arithmetic itself belongs to :mod:`engine.forward_emission_window`,
which is generic and knows nothing about this challenger.

RESEARCH ONLY. Freezing a decision starts a MEASUREMENT. It promotes no model,
activates no sleeve, allocates no capital, changes no holding, creates no order
and no fill, and writes nothing outside the campaign research root.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import forward_emission_window as EW

from . import now_iso, research_root, stable_hash

CALCULATION_OWNER = "alpha_agent.alpha_recovery.prospective_decision"
RECORD_SCHEMA = "alpha_recovery_prospective_decision/1"
POLICY_SCHEMA = "alpha_recovery_prospective_decision_policy/1"

#: Where frozen decisions live, under the campaign research root.
DECISIONS_SUBDIR = "prospective_decisions"
#: The per-challenger boundary declaration. Written ONCE, read by the live
#: resolver even on sessions that carry no decision - which is how the live side
#: can say "the window has not opened yet" instead of "there is nothing here".
POLICY_FILE = "_policy.json"

# --------------------------------------------------------------------------- #
# Outcomes
# --------------------------------------------------------------------------- #
FROZEN = "FROZEN"
ALREADY_FROZEN = "ALREADY_FROZEN"
REFUSED_NO_POLICY = "REFUSED_NO_DECLARED_EMISSION_POLICY"
REFUSED_TOO_EARLY = "REFUSED_BEFORE_THE_DECLARED_INFORMATION_CUTOFF"
REFUSED_TOO_LATE = "REFUSED_AFTER_THE_EMISSION_WINDOW_CLOSED"
REFUSED_CONTAMINATED = "REFUSED_INPUT_OBSERVED_AFTER_THE_DECLARED_CUTOFF"
REFUSED_CONFLICT = "REFUSED_CONFLICTING_DECISION_FOR_THIS_SESSION"
REFUSED_MALFORMED = "REFUSED_INCOMPLETE_DECISION"
OUTCOMES = (FROZEN, ALREADY_FROZEN, REFUSED_NO_POLICY, REFUSED_TOO_EARLY,
            REFUSED_TOO_LATE, REFUSED_CONTAMINATED, REFUSED_CONFLICT,
            REFUSED_MALFORMED)

#: The fields that make one decision the SAME decision as another. The
#: timestamp is deliberately absent: re-running the identical rule a minute
#: later is the same decision, while a different weight book is a different one
#: however similar the clock reading.
_IDENTITY_FIELDS = ("challenger_id", "eligible_session", "weights_hash",
                    "declared_information_cutoff",
                    "rebalance_cadence_sessions", "evaluation_horizon_sessions",
                    "cost_policy", "freeze_record_hash", "model_spec_hash",
                    "identity_hash", "source_data_hash", "feature_state_hash")

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "backfill_allowed": False,
    "records_are_immutable": True,
    "first_write_wins": True,
    "promotes_model": False,
    "activates_sleeve": False,
    "allocates_capital": False,
    "creates_orders": False,
    "creates_fills": False,
    "mutates_operational_store": False,
    "registers_forward_challenger": False,
    "manual_review_required": True,
}


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _atomic_write(path: Path, body: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True, default=str),
                   encoding="utf-8")
    tmp.replace(path)
    return path


def _read(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# The store
# --------------------------------------------------------------------------- #
def decisions_dir(challenger_id: str) -> Path:
    return research_root() / DECISIONS_SUBDIR / str(challenger_id)


def policy_path(challenger_id: str) -> Path:
    return decisions_dir(challenger_id) / POLICY_FILE


def decision_path(challenger_id: str, session: str) -> Path:
    return decisions_dir(challenger_id) / ("%s.json" % str(session)[:10])


def load_policy(challenger_id: str) -> Optional[dict]:
    return _read(policy_path(challenger_id))


def load_decision(challenger_id: str, session: str) -> Optional[dict]:
    return _read(decision_path(challenger_id, session))


def list_decisions(challenger_id: str) -> list:
    d = decisions_dir(challenger_id)
    if not d.exists():
        return []
    rows = [_read(p) for p in sorted(d.glob("*.json")) if p.name != POLICY_FILE]
    rows = [r for r in rows if r]
    rows.sort(key=lambda r: str(r.get("eligible_session") or ""))
    return rows


# --------------------------------------------------------------------------- #
# The declaration - written once, before anything is decided
# --------------------------------------------------------------------------- #
def declare_policy(*, challenger_id: str, information_cutoff_et,
                   entry_mark_et, rebalance_cadence_sessions: int,
                   evaluation_horizon_sessions: int, cost_policy: dict,
                   instrument_scope: list, identity: Optional[dict] = None,
                   emission_rule: Optional[str] = None,
                   now: Optional[str] = None) -> dict:
    """Declare the boundary this challenger's decisions are frozen under.

    Written ONCE and never amended. Declaring the boundary in advance - before
    any decision exists - is what makes the cutoff a constraint rather than a
    description: a policy that could be written alongside the decision could be
    written to fit it.
    """
    existing = load_policy(challenger_id)
    if existing:
        return {"outcome": ALREADY_FROZEN, "idempotent": True,
                "policy": existing, "path": str(policy_path(challenger_id)),
                "detail": "this challenger already declares an emission policy; "
                          "it is immutable and was not rewritten"}
    window = EW.declare_same_session(information_cutoff_et=information_cutoff_et,
                                     entry_mark_et=entry_mark_et)
    if window.get("declaration_state") != EW.DECL_ACCEPTED:
        return {"outcome": REFUSED_MALFORMED, "declaration": window,
                "detail": "the declared boundary was not accepted by %s"
                          % EW.CALCULATION_OWNER}
    body = {
        "schema": POLICY_SCHEMA,
        "calculation_owner": CALCULATION_OWNER,
        "window_owner": EW.CALCULATION_OWNER,
        "challenger_id": str(challenger_id),
        "emission_window": window,
        "declared_information_cutoff_et": list(window["information_cutoff_et"]),
        "entry_mark_et": list(window["entry_mark_et"]),
        "rebalance_cadence_sessions": int(rebalance_cadence_sessions),
        "evaluation_horizon_sessions": int(evaluation_horizon_sessions),
        "cost_policy": dict(cost_policy or {}),
        "instrument_scope": list(instrument_scope or []),
        "identity": dict(identity or {}),
        "emission_rule": emission_rule,
        "declared_at": now or now_iso(),
        "records_are_immutable": True,
        "backfill_allowed": False,
        "safety": dict(SAFETY),
    }
    body["record_hash"] = stable_hash(
        {k: v for k, v in body.items() if k != "declared_at"})
    _atomic_write(policy_path(challenger_id), body)
    return {"outcome": FROZEN, "idempotent": False, "policy": body,
            "path": str(policy_path(challenger_id))}


# --------------------------------------------------------------------------- #
# The freeze
# --------------------------------------------------------------------------- #
def freeze_decision(*, challenger_id: str, eligible_session: str,
                    weights: dict, source_data_hash: str,
                    feature_state_hash: str,
                    feature_observed_at: Optional[str] = None,
                    decision_timestamp: Optional[str] = None,
                    now: Optional[str] = None,
                    policy: Optional[dict] = None) -> dict:
    """Freeze ONE immutable prospective decision, or refuse and say why.

    ``now`` is the instant the freeze is attempted and is the ONLY clock this
    function consults - supplied by the caller so the whole contract is testable
    without waiting for an afternoon.
    """
    cid = str(challenger_id)
    session = str(eligible_session)[:10]
    pol = policy or load_policy(cid)
    ts = now or now_iso()
    decided_at = decision_timestamp or ts
    base = {"challenger_id": cid, "eligible_session": session,
            "attempted_at": ts, "owner": CALCULATION_OWNER,
            "outcome_vocabulary": list(OUTCOMES)}

    if not pol:
        return {**base, "outcome": REFUSED_NO_POLICY, "frozen": False,
                "detail": ("no emission policy is declared for %s; a decision "
                           "cannot be judged prospective without a boundary "
                           "declared before it" % cid)}
    window_decl = pol.get("emission_window")

    # (a) THE WINDOW. Too early and too late are different refusals, because one
    #     is "wait" and the other is "this session is gone forever".
    cls = EW.classify(policy=window_decl, session=session, now=ts)
    base["emission_window"] = {k: cls.get(k) for k in
                               ("state", "opens_at", "closes_at", "reason",
                                "boundary")}
    if cls.get("state") == EW.WINDOW_NOT_OPEN:
        return {**base, "outcome": REFUSED_TOO_EARLY, "frozen": False,
                "detail": cls.get("reason")}
    if cls.get("state") != EW.WINDOW_OPEN:
        return {**base, "outcome": REFUSED_TOO_LATE, "frozen": False,
                "backfill_refused": True,
                "detail": ("%s - a missed session is never created "
                           "retrospectively and is never backfilled"
                           % cls.get("reason"))}

    # (b) THE INFORMATION. Inside the window is not enough; the inputs must
    #     predate the declared cutoff.
    viol = EW.violates_information_cutoff(policy=window_decl, session=session,
                                          observed_at=feature_observed_at)
    base["information_cutoff_check"] = viol
    if viol.get("violates"):
        return {**base, "outcome": REFUSED_CONTAMINATED, "frozen": False,
                "detail": viol.get("reason")}

    # (c) THE BOOK.
    w = {}
    for k, v in (weights or {}).items():
        fv = _f(v)
        if fv is not None and abs(fv) > 1e-9:
            w[str(k)] = round(fv, 8)
    if not w:
        return {**base, "outcome": REFUSED_MALFORMED, "frozen": False,
                "detail": "the decision carries no non-zero weight"}
    scope = set(pol.get("instrument_scope") or [])
    outside = sorted(set(w) - scope) if scope else []
    if outside:
        return {**base, "outcome": REFUSED_MALFORMED, "frozen": False,
                "outside_declared_scope": outside,
                "detail": ("the decision names instruments outside the declared "
                           "scope: %s" % ", ".join(outside))}

    gross = round(sum(abs(v) for v in w.values()), 8)
    net = round(sum(w.values()), 8)
    body = {
        "schema": RECORD_SCHEMA,
        "calculation_owner": CALCULATION_OWNER,
        "window_owner": EW.CALCULATION_OWNER,
        "challenger_id": cid,
        "eligible_session": session,
        "decision_timestamp": decided_at,
        "declared_information_cutoff": cls.get("information_cutoff_at"),
        "emission_window_opens_at": cls.get("opens_at"),
        "emission_window_closes_at": cls.get("closes_at"),
        "emission_boundary": cls.get("boundary"),
        "weights": dict(sorted(w.items())),
        "weights_hash": stable_hash(dict(sorted(w.items()))),
        "gross_exposure": gross,
        "net_exposure": net,
        "position_count": len(w),
        "rebalance_cadence_sessions": int(pol.get("rebalance_cadence_sessions")),
        "evaluation_horizon_sessions": int(
            pol.get("evaluation_horizon_sessions")),
        "cost_policy": dict(pol.get("cost_policy") or {}),
        "instrument_scope": list(pol.get("instrument_scope") or []),
        "freeze_record_hash": (pol.get("identity") or {}).get(
            "freeze_record_hash"),
        "model_spec_hash": (pol.get("identity") or {}).get("model_spec_hash"),
        "identity_hash": (pol.get("identity") or {}).get("identity_hash"),
        "identity": dict(pol.get("identity") or {}),
        "policy_record_hash": pol.get("record_hash"),
        "source_data_hash": str(source_data_hash),
        "feature_state_hash": str(feature_state_hash),
        "feature_observed_at": feature_observed_at,
        "emission_rule": pol.get("emission_rule"),
        "is_true_forward": True,
        "backfilled": False,
        "records_are_immutable": True,
        "first_write_wins": True,
        "safety": dict(SAFETY),
    }
    body["decision_identity_hash"] = stable_hash(
        {k: body.get(k) for k in _IDENTITY_FIELDS})
    body["record_hash"] = stable_hash(
        {k: v for k, v in body.items()
         if k not in ("record_hash", "decision_timestamp", "attempted_at")})

    # (d) FIRST WRITE WINS. Identical is idempotent; different is refused.
    existing = load_decision(cid, session)
    if existing:
        same = (existing.get("decision_identity_hash")
                == body["decision_identity_hash"])
        if same:
            return {**base, "outcome": ALREADY_FROZEN, "frozen": True,
                    "idempotent": True, "decision": existing,
                    "path": str(decision_path(cid, session)),
                    "detail": ("this exact decision is already frozen for %s; "
                               "a second freeze appends nothing" % session)}
        return {**base, "outcome": REFUSED_CONFLICT, "frozen": False,
                "held_decision_identity_hash":
                    existing.get("decision_identity_hash"),
                "offered_decision_identity_hash":
                    body["decision_identity_hash"],
                "held_weights": existing.get("weights"),
                "offered_weights": body["weights"],
                "decision": existing,
                "detail": ("a DIFFERENT decision is already frozen for %s; the "
                           "held record stands and is never overwritten"
                           % session)}

    _atomic_write(decision_path(cid, session), body)
    return {**base, "outcome": FROZEN, "frozen": True, "idempotent": False,
            "decision": body, "path": str(decision_path(cid, session))}


# --------------------------------------------------------------------------- #
# The read model
# --------------------------------------------------------------------------- #
def state(challenger_id: str, *, now: Optional[str] = None,
          session: Optional[str] = None) -> dict:
    """What an operator needs to see: is the boundary reached, and what is held?"""
    cid = str(challenger_id)
    pol = load_policy(cid)
    held = list_decisions(cid)
    ts = now or now_iso()
    out = {
        "calculation_owner": CALCULATION_OWNER,
        "challenger_id": cid,
        "policy_declared": bool(pol),
        "decisions_frozen": len(held),
        "frozen_sessions": [r.get("eligible_session") for r in held],
        "latest": held[-1] if held else None,
        "now": ts,
        "backfilled": False,
        "safety": dict(SAFETY),
    }
    if not pol:
        return {**out, "state": "NO_POLICY_DECLARED"}
    out["policy"] = pol
    if session:
        cls = EW.classify(policy=pol.get("emission_window"), session=session,
                          now=ts)
        out["session"] = session
        out["emission_window"] = cls
        existing = load_decision(cid, session)
        out["decision_for_session"] = existing
        if existing:
            out["state"] = "DECISION_FROZEN"
        elif cls.get("state") == EW.WINDOW_NOT_OPEN:
            out["state"] = "AWAITING_DECISION_BOUNDARY"
        elif cls.get("state") == EW.WINDOW_OPEN:
            out["state"] = "DECISION_DUE_NOW"
        else:
            out["state"] = "WINDOW_CLOSED_WITHOUT_A_DECISION"
    else:
        out["state"] = "POLICY_DECLARED"
    return out


__all__ = [
    "CALCULATION_OWNER", "RECORD_SCHEMA", "POLICY_SCHEMA", "DECISIONS_SUBDIR",
    "POLICY_FILE", "OUTCOMES", "FROZEN", "ALREADY_FROZEN", "REFUSED_NO_POLICY",
    "REFUSED_TOO_EARLY", "REFUSED_TOO_LATE", "REFUSED_CONTAMINATED",
    "REFUSED_CONFLICT", "REFUSED_MALFORMED", "SAFETY",
    "decisions_dir", "policy_path", "decision_path", "load_policy",
    "load_decision", "list_decisions", "declare_policy", "freeze_decision",
    "state",
]
