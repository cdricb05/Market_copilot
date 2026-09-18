r"""api/capital_eligibility_gate.py - MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1: the ONE
declared, evidence-gated door through which a RESEARCH sleeve becomes CAPITAL
ELIGIBLE - automatically once its declared gate is passed, and never before.

THE GAP THIS CLOSES
-------------------
Release 50 made the operational manager asset-agnostic and left every non-equity
sleeve blocked on exactly one thing: ``NO_APPROVED_OPERATIONAL_SIGNAL``. The
registry's ``model_approval_state`` was DATA typed into the record, so there was
no path from "a registered challenger accrued forward evidence that clears the
frozen gates" to "the sleeve competes for capital" other than a human editing a
Python file. That is why ``frontier_eligible_non_equity_count`` has read 0 on
every proposal the estate has ever built, and why a "cross-asset" feature could
be nominal: nothing could ever flow through it.

WHAT THIS OWNER DECLARES
------------------------
For each research sleeve that has an OPERATIONAL-SIGNAL CANDIDATE - a canonical
forward registration whose per-session decisions the accrual owner already
measures - it declares, as data:

    * WHICH registered challenger is the candidate operational signal;
    * the FROZEN forward-evidence thresholds it must clear (mirrored from the
      R46 forward-evidence contract by horizon: effective independent
      observations, raw matured observations, calendar days, net edge and its
      t-statistic against the zero-return cash control);
    * that a CONDITIONAL OPERATIONAL APPROVAL must exist - a human governance
      record, written IN ADVANCE, that binds this exact registration identity to
      these exact thresholds and says "when the evidence clears them, this
      sleeve may compete for capital".

and it EVALUATES the gate from persisted facts alone: the accrual projection the
evidence owner wrote, the registration the registrar wrote, and the approval
record the operator wrote. When every requirement holds the gate is PASSED and
``api.investability_registry`` derives ``MODEL_APPROVED_FOR_OPERATION`` from it -
so the sleeve enters the opportunity frontier, the cross-asset risk state and the
zero-base allocator on the next read WITHOUT a separate operator action. When
any requirement fails the gate is NOT_PASSED and the exact remaining gap is
published, by name and by number.

WHY THIS IS NOT AUTOMATIC PROMOTION
-----------------------------------
The decision is taken by a human, before the evidence exists, in the form of the
conditional approval record; the thresholds are frozen there and here; this
module only checks whether the pre-declared condition has been met. It cannot
create an approval record, cannot lower a threshold, cannot promote a sleeve
whose approval names a different challenger or identity, and writes nothing on a
read. The one write it exposes (:func:`declare_conditional_operational_approval`)
requires an explicit confirmation token, is first-write-wins, and is the
operator's act.

RESEARCH STATISTICS NEVER BECOME EXPECTED RETURNS. A passed gate makes a
sleeve eligible to compete on its rank-normalised operational signal (the
frontier's OPERATIONAL_SLEEVE_NORMALISED_RANK basis); it never supplies an
expected return.

Read-only except the one confirmed operator write. Owns one small governance
store. Never creates an order, a fill, a proposal or an approval of a proposal.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

PHASE = "MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1"
OWNER = "api.capital_eligibility_gate"
SCHEMA_VERSION = "capital_eligibility_gate.v1"

GATE_DIR_ENV = "PAPER_TRADER_CAPITAL_ELIGIBILITY_DIR"
_DEFAULT_GATE_DIR = Path(r"D:\Stock_Prediction_app_data\capital_eligibility")
_APPROVALS_SUBDIR = "conditional_operational_approvals"

#: The explicit confirmation the ONE write requires.
APPROVAL_CONFIRM_TOKEN = "CONFIRM_CONDITIONAL_OPERATIONAL_APPROVAL"

# --------------------------------------------------------------------------- #
# Gate states
# --------------------------------------------------------------------------- #
G_PASSED = "GATE_PASSED"
G_NOT_PASSED = "GATE_NOT_PASSED"
G_NOT_DECLARED = "GATE_NOT_DECLARED"
G_EVIDENCE_UNAVAILABLE = "FORWARD_EVIDENCE_UNAVAILABLE"
GATE_STATES = (G_PASSED, G_NOT_PASSED, G_NOT_DECLARED, G_EVIDENCE_UNAVAILABLE)

# --------------------------------------------------------------------------- #
# Requirement codes (each remaining item names exactly one)
# --------------------------------------------------------------------------- #
REQ_REGISTRATION = "CANONICAL_FORWARD_REGISTRATION"
REQ_ACCRUAL = "ACCRUAL_PROJECTION_PUBLISHED"
REQ_EFFECTIVE = "MIN_EFFECTIVE_INDEPENDENT_OBSERVATIONS"
REQ_RAW = "MIN_RAW_MATURED_OBSERVATIONS"
REQ_CALENDAR = "MIN_CALENDAR_DAYS_OF_FORWARD_EVIDENCE"
REQ_EDGE = "MIN_NET_EDGE_PER_DECISION"
REQ_T_STAT = "MIN_T_STAT_NET_VS_CASH_CONTROL"
REQ_NO_INTEGRITY_BLOCK = "NO_INTEGRITY_BLOCKER_ON_THE_ACCRUAL"
REQ_APPROVAL = "CONDITIONAL_OPERATIONAL_APPROVAL_DECLARED"
REQ_APPROVAL_BOUND = "CONDITIONAL_APPROVAL_BOUND_TO_THIS_REGISTRATION"
REQUIREMENT_CODES = (REQ_REGISTRATION, REQ_ACCRUAL, REQ_EFFECTIVE, REQ_RAW, REQ_CALENDAR,
                     REQ_EDGE, REQ_T_STAT, REQ_NO_INTEGRITY_BLOCK, REQ_APPROVAL,
                     REQ_APPROVAL_BOUND)

# --------------------------------------------------------------------------- #
# The frozen forward-evidence thresholds, by horizon. MIRROR of
# alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES (asserted equal by test); the
# api layer imports no research package. A horizon absent from the table takes
# the row of the largest declared horizon not above it (21 -> the 20 row): the
# longer the hold, the fewer independent decisions a year holds, so a longer
# horizon may never be judged by a SHORTER horizon's stricter count.
# --------------------------------------------------------------------------- #
FORWARD_GATE_MIRROR = {
    "min_effective_independent": {1: 60, 5: 40, 20: 24},
    "min_calendar_days": {1: 90, 5: 180, 20: 365},
    "min_raw_matured": {1: 60, 5: 60, 20: 60},
    "min_net_edge_bps_per_decision": 1.0,
    "min_t_stat_net_vs_control": 2.5,
    "control": "ZERO_RETURN_CASH",
    "source": "alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES (mirrored; asserted equal by test)",
}


def gate_thresholds(horizon_sessions: Any) -> dict:
    """The frozen thresholds ONE evidence horizon is judged by."""
    try:
        h = int(horizon_sessions)
    except (TypeError, ValueError):
        h = 1
    keys = sorted(FORWARD_GATE_MIRROR["min_effective_independent"])
    row = keys[0]
    for k in keys:
        if k <= h:
            row = k
    return {
        "evidence_gate_horizon_sessions": h,
        "gate_row_horizon": row,
        "min_effective_independent": FORWARD_GATE_MIRROR["min_effective_independent"][row],
        "min_raw_matured": FORWARD_GATE_MIRROR["min_raw_matured"][row],
        "min_calendar_days": FORWARD_GATE_MIRROR["min_calendar_days"][row],
        "min_net_edge_bps_per_decision": FORWARD_GATE_MIRROR["min_net_edge_bps_per_decision"],
        "min_t_stat_net_vs_control": FORWARD_GATE_MIRROR["min_t_stat_net_vs_control"],
        "control": FORWARD_GATE_MIRROR["control"],
        "frozen": True,
        "tuned_on_outcomes": False,
        "source": FORWARD_GATE_MIRROR["source"],
    }


# --------------------------------------------------------------------------- #
# The declared candidates: registry sleeve -> canonical forward registration.
# A sleeve absent here has NO operational-signal candidate and its gate is
# NOT_DECLARED (never NOT_PASSED, which would imply something is accruing).
# --------------------------------------------------------------------------- #
#: The FX carry cadence challenger (registered 2026-09-14; identity 436a13011b67...).
FX_CARRY_CADENCE_CHALLENGER_ID = "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"
#: The managed-futures time-series trend challenger (this release). Mirrors
#: ``alpha_agent.alpha_recovery.futures_trend_challenger.CHALLENGER_ID``.
FUTURES_TREND_CHALLENGER_ID = "ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1"

SHORT_LEG_RULE = "SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK"


def declared_candidates() -> list[dict]:
    """The operational-signal candidates, as data. One record per sleeve."""
    return [
        {
            "sleeve_id": "sleeve_fx_futures",
            "asset_class": "FX_FUTURES",
            "challenger_id": FX_CARRY_CADENCE_CHALLENGER_ID,
            "release": "ALPHA_RECOVERY_OFFENSIVE",
            "evidence_gate_horizon_sessions": 5,
            "horizon_note": ("the frozen record's horizon_sessions=1 is the information-label "
                             "horizon; a forward observation is one decision held 5 sessions "
                             "(alpha_agent.alpha_recovery.fx_carry_cadence_challenger."
                             "HORIZON_CONTRACT)"),
            "signal_owner": ("alpha_agent.alpha_recovery.prospective_decision - the latest "
                             "frozen cadence decision of the challenger"),
            "instrument_symbol_rule": "registry symbol = '&' + frozen-decision market root",
            "long_only_expression": SHORT_LEG_RULE,
            "strategy_family": "FX_CARRY_CADENCE",
        },
        {
            "sleeve_id": "sleeve_managed_futures_trend",
            "asset_class": "MULTI_ASSET_FUTURES",
            "challenger_id": FUTURES_TREND_CHALLENGER_ID,
            "release": "ALPHA_RECOVERY_OFFENSIVE",
            "evidence_gate_horizon_sessions": 21,
            "horizon_note": "one decision held 21 sessions (monthly rebalance)",
            "signal_owner": ("alpha_agent.alpha_recovery.prospective_decision - the latest "
                             "frozen trend decision of the challenger"),
            "instrument_symbol_rule": "registry symbol = '&' + frozen-decision market root",
            "long_only_expression": SHORT_LEG_RULE,
            "strategy_family": "MANAGED_FUTURES_TIME_SERIES_TREND",
        },
    ]


def candidate_for_sleeve(sleeve_id: str) -> Optional[dict]:
    for c in declared_candidates():
        if c["sleeve_id"] == sleeve_id:
            return dict(c)
    return None


# --------------------------------------------------------------------------- #
# Governance store (the ONE confirmed operator write)
# --------------------------------------------------------------------------- #
def gate_dir(override=None) -> Path:
    if override is not None:
        return Path(override)
    env = os.environ.get(GATE_DIR_ENV)
    return Path(env) if env else _DEFAULT_GATE_DIR


def _approval_path(sleeve_id: str, override=None) -> Path:
    return gate_dir(override) / _APPROVALS_SUBDIR / ("%s.json" % sleeve_id)


def _now_iso(now=None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _load_json(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def load_conditional_approval(sleeve_id: str, *, gate_dir_override=None) -> Optional[dict]:
    return _load_json(_approval_path(sleeve_id, gate_dir_override))


def declare_conditional_operational_approval(*, sleeve_id: str, challenger_id: str,
                                             registration_identity_hash: str,
                                             declared_by: str, statement: str,
                                             confirm: Optional[str] = None,
                                             gate_dir_override=None,
                                             now=None) -> dict:
    """THE operator's act: pre-authorise a sleeve, conditionally, in advance.

    First-write-wins and immutable. The record binds the sleeve to ONE challenger
    and ONE registration identity and freezes the thresholds it will be judged
    by (copied from :func:`gate_thresholds` at declaration time). It grants
    nothing today: the gate still has to be passed by forward evidence, and this
    module still has to see both facts together before anything becomes eligible.
    """
    if confirm != APPROVAL_CONFIRM_TOKEN:
        return {"owner": OWNER, "outcome": "CONFIRMATION_REQUIRED", "written": False,
                "confirm_required_token": APPROVAL_CONFIRM_TOKEN}
    cand = candidate_for_sleeve(sleeve_id)
    if cand is None:
        return {"owner": OWNER, "outcome": "REFUSED_NO_DECLARED_CANDIDATE", "written": False,
                "detail": "sleeve %s declares no operational-signal candidate" % sleeve_id}
    if cand["challenger_id"] != challenger_id:
        return {"owner": OWNER, "outcome": "REFUSED_CHALLENGER_MISMATCH", "written": False,
                "detail": ("sleeve %s's declared candidate is %s, not %s"
                           % (sleeve_id, cand["challenger_id"], challenger_id))}
    if not registration_identity_hash or not declared_by or not statement:
        return {"owner": OWNER, "outcome": "REFUSED_INCOMPLETE", "written": False,
                "detail": "identity hash, declarer and statement are all required"}
    path = _approval_path(sleeve_id, gate_dir_override)
    existing = _load_json(path)
    if existing:
        return {"owner": OWNER, "outcome": "ALREADY_DECLARED", "written": False,
                "record": existing, "path": str(path), "first_write_wins": True}
    record = {
        "schema_version": SCHEMA_VERSION, "owner": OWNER, "phase": PHASE,
        "record_kind": "CONDITIONAL_OPERATIONAL_APPROVAL",
        "sleeve_id": sleeve_id, "challenger_id": challenger_id,
        "registration_identity_hash": str(registration_identity_hash),
        "declared_by": str(declared_by), "declared_at": _now_iso(now),
        "statement": str(statement),
        "thresholds_frozen_at_declaration": gate_thresholds(
            cand["evidence_gate_horizon_sessions"]),
        "grants_eligibility_today": False,
        "eligibility_becomes_effective": ("automatically, on the first registry read after "
                                          "the frozen forward-evidence thresholds are met"),
        "revocable_by": "deleting or superseding this record is a separate human act",
        "records_are_immutable": True, "first_write_wins": True,
        "safety": {"creates_orders": False, "creates_fills": False,
                   "approves_proposal": False, "promotes_model_today": False,
                   "allocates_capital_today": False},
    }
    _atomic_write_json(path, record)
    return {"owner": OWNER, "outcome": "DECLARED", "written": True, "record": record,
            "path": str(path)}


# --------------------------------------------------------------------------- #
# Evidence sources (each injectable for hermetic tests)
# --------------------------------------------------------------------------- #
def _default_registrations() -> list:
    from paper_trader.api import forward_challenger_registry as FCR
    return FCR.load_registrations()


def _default_accrual_projection() -> dict:
    from paper_trader.api import canonical_forward_accrual as CFA
    return CFA.load_accrual_projection()


def _registration_for(challenger_id: str, registrations: list) -> Optional[dict]:
    rows = [r for r in (registrations or []) if r.get("challenger_id") == challenger_id]
    if not rows:
        return None
    rows.sort(key=lambda r: str(r.get("registered_at") or ""))
    return rows[-1]


def _calendar_days(first: Optional[str], last: Optional[str]) -> Optional[int]:
    try:
        a = datetime.fromisoformat(str(first)[:10])
        b = datetime.fromisoformat(str(last)[:10])
    except (TypeError, ValueError):
        return None
    return max(0, (b - a).days)


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# THE evaluation (pure over persisted facts)
# --------------------------------------------------------------------------- #
def evaluate_gate(candidate: dict, *, registrations: Optional[list] = None,
                  accrual_projection: Optional[dict] = None,
                  approval: Optional[dict] = None,
                  gate_dir_override=None, now: Optional[str] = None) -> dict:
    """Judge ONE declared candidate against its frozen gate. Writes nothing."""
    cand = dict(candidate or {})
    ts = now or _now_iso()
    thresholds = gate_thresholds(cand.get("evidence_gate_horizon_sessions"))
    base = {
        "schema_version": SCHEMA_VERSION, "owner": OWNER, "phase": PHASE,
        "evaluated_at": ts,
        "sleeve_id": cand.get("sleeve_id"), "challenger_id": cand.get("challenger_id"),
        "asset_class": cand.get("asset_class"),
        "thresholds": thresholds,
        "state_vocabulary": list(GATE_STATES),
        "requirement_vocabulary": list(REQUIREMENT_CODES),
        "automatic_promotion": False,
        "human_decision": "the conditional operational approval, declared in advance",
        "evaluation_is_automatic": True,
        "writes_nothing": True,
    }
    if not cand.get("challenger_id"):
        return {**base, "state": G_NOT_DECLARED, "remaining": [],
                "detail": "no operational-signal candidate is declared for this sleeve"}

    remaining: list[dict] = []
    evidence: dict[str, Any] = {}

    # (1) the registration
    try:
        regs = registrations if registrations is not None else _default_registrations()
    except Exception as exc:  # noqa: BLE001
        regs = None
        evidence["registrations_error"] = str(exc)[:160]
    reg = _registration_for(cand["challenger_id"], regs or []) if regs is not None else None
    ident_hash = ((reg or {}).get("identity") or {}).get("identity_hash") if reg else None
    evidence["registration_identity_hash"] = ident_hash
    evidence["registration_session"] = (reg or {}).get("registration_session")
    if reg is None:
        remaining.append({"code": REQ_REGISTRATION, "required": "one canonical forward "
                          "registration for the challenger", "observed": None})

    # (2) the accrual projection
    try:
        proj = accrual_projection if accrual_projection is not None else _default_accrual_projection()
    except Exception as exc:  # noqa: BLE001
        proj = None
        evidence["accrual_error"] = str(exc)[:160]
    row = (proj or {}).get(str(ident_hash)) if (proj is not None and ident_hash) else None
    if row is None:
        remaining.append({"code": REQ_ACCRUAL, "required": "an accrual projection row for the "
                          "registration identity", "observed": None})
        econ = {}
    else:
        econ = row.get("forward_economics") or {}
        evidence["accrual"] = {k: row.get(k) for k in
                               ("current_accrual_state", "predictions_emitted", "forfeitures",
                                "matured_observations", "effective_independent_observations",
                                "latest_blocker", "horizon_sessions", "last_emission_session")}
        evidence["forward_economics"] = econ
        eff = int(row.get("effective_independent_observations") or 0)
        raw = int(row.get("matured_observations") or 0)
        if eff < thresholds["min_effective_independent"]:
            remaining.append({"code": REQ_EFFECTIVE, "required": thresholds["min_effective_independent"],
                              "observed": eff})
        if raw < thresholds["min_raw_matured"]:
            remaining.append({"code": REQ_RAW, "required": thresholds["min_raw_matured"],
                              "observed": raw})
        days = _calendar_days(econ.get("first_decision_session"), econ.get("last_maturity_session"))
        evidence["calendar_days_of_matured_evidence"] = days
        if days is None or days < thresholds["min_calendar_days"]:
            remaining.append({"code": REQ_CALENDAR, "required": thresholds["min_calendar_days"],
                              "observed": days})
        mean = _f(econ.get("mean_net_return_per_decision"))
        edge_bps = None if mean is None else mean * 1e4
        evidence["net_edge_bps_per_decision"] = (round(edge_bps, 4) if edge_bps is not None else None)
        if edge_bps is None or edge_bps < thresholds["min_net_edge_bps_per_decision"]:
            remaining.append({"code": REQ_EDGE, "required": thresholds["min_net_edge_bps_per_decision"],
                              "observed": evidence["net_edge_bps_per_decision"]})
        t = _f(econ.get("t_stat_net_vs_zero"))
        if t is None or t < thresholds["min_t_stat_net_vs_control"]:
            remaining.append({"code": REQ_T_STAT, "required": thresholds["min_t_stat_net_vs_control"],
                              "observed": t})
        if str(row.get("current_accrual_state") or "") == "INTEGRITY_BLOCKED":
            remaining.append({"code": REQ_NO_INTEGRITY_BLOCK, "required": "no integrity blocker",
                              "observed": row.get("latest_blocker")})

    # (3) the human pre-authorisation
    appr = approval if approval is not None else load_conditional_approval(
        str(cand.get("sleeve_id")), gate_dir_override=gate_dir_override)
    evidence["conditional_approval"] = ({k: appr.get(k) for k in
                                         ("declared_by", "declared_at", "challenger_id",
                                          "registration_identity_hash")}
                                        if appr else None)
    if not appr:
        remaining.append({"code": REQ_APPROVAL, "required": "a conditional operational approval "
                          "record declared by the operator in advance", "observed": None})
    else:
        bound = (appr.get("challenger_id") == cand["challenger_id"]
                 and ident_hash is not None
                 and str(appr.get("registration_identity_hash")) == str(ident_hash))
        if not bound:
            remaining.append({"code": REQ_APPROVAL_BOUND,
                              "required": "approval bound to challenger %s / identity %s"
                                          % (cand["challenger_id"], ident_hash),
                              "observed": {"challenger_id": appr.get("challenger_id"),
                                           "registration_identity_hash":
                                               appr.get("registration_identity_hash")}})

    if reg is None or row is None:
        state = G_EVIDENCE_UNAVAILABLE if reg is not None else G_NOT_PASSED
    else:
        state = G_PASSED if not remaining else G_NOT_PASSED
    return {
        **base,
        "state": state,
        "passed": state == G_PASSED,
        "remaining": remaining,
        "remaining_codes": [r["code"] for r in remaining],
        "evidence": evidence,
        "detail": ("every frozen requirement is met and a pre-declared conditional approval "
                   "binds this registration; the sleeve competes for capital on its "
                   "rank-normalised operational signal"
                   if state == G_PASSED else
                   "%d requirement(s) remain: %s" % (len(remaining),
                                                     ", ".join(r["code"] for r in remaining))),
    }


def evaluate_all(*, registrations: Optional[list] = None,
                 accrual_projection: Optional[dict] = None,
                 approvals: Optional[dict] = None,
                 gate_dir_override=None, now: Optional[str] = None) -> dict:
    """``sleeve_id -> gate evaluation`` for every declared candidate."""
    out = {}
    for cand in declared_candidates():
        appr = (approvals or {}).get(cand["sleeve_id"]) if approvals is not None else None
        out[cand["sleeve_id"]] = evaluate_gate(
            cand, registrations=registrations, accrual_projection=accrual_projection,
            approval=appr, gate_dir_override=gate_dir_override, now=now)
    return out


# --------------------------------------------------------------------------- #
# The operational signal of a PASSED sleeve: the latest frozen decision, long
# legs only, rank-normalised within the sleeve (the frontier's SB_SLEEVE_RANK).
# --------------------------------------------------------------------------- #
def operational_signal_scores(candidate: dict, *, decision_loader: Optional[Callable] = None) -> dict:
    """The candidate's latest frozen prospective decision as registry-keyed scores.

    Long legs become rank-normalised scores in (0, 1]; short legs are NOT
    expressible in the long-only operational book and are reported by name under
    ``not_expressible`` rather than silently dropped or flipped.
    """
    cand = candidate or {}
    cid = str(cand.get("challenger_id") or "")
    if decision_loader is None:
        def decision_loader(challenger_id: str) -> list:
            from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
            return PD.list_decisions(challenger_id)
    try:
        decisions = decision_loader(cid) or []
    except Exception as exc:  # noqa: BLE001
        return {"challenger_id": cid, "decision_session": None, "scores": {},
                "not_expressible": [], "state": "DECISION_OWNER_UNREADABLE",
                "detail": str(exc)[:160]}
    if not decisions:
        return {"challenger_id": cid, "decision_session": None, "scores": {},
                "not_expressible": [], "state": "NO_FROZEN_DECISION_YET"}
    latest = decisions[-1]
    weights = dict(latest.get("weights") or {})
    longs = {("&" + str(k)): float(v) for k, v in weights.items() if _f(v) is not None and float(v) > 0}
    shorts = sorted(str(k) for k, v in weights.items() if _f(v) is not None and float(v) < 0)
    from paper_trader.engine import opportunity_frontier as OF
    scores = OF.rank_normalise(longs) if longs else {}
    return {"challenger_id": cid, "decision_session": latest.get("eligible_session"),
            "decision_record_hash": latest.get("record_hash"),
            "scores": {k: round(float(v), 6) for k, v in scores.items()},
            "long_weights": {k: round(v, 8) for k, v in longs.items()},
            "not_expressible": [{"market": m, "reason": SHORT_LEG_RULE} for m in shorts],
            "score_basis": OF.SB_SLEEVE_RANK,
            "state": "OPERATIONAL_SIGNAL_AVAILABLE" if scores else "NO_LONG_LEG_IN_LATEST_DECISION"}


__all__ = [
    "PHASE", "OWNER", "SCHEMA_VERSION", "GATE_DIR_ENV", "APPROVAL_CONFIRM_TOKEN",
    "GATE_STATES", "G_PASSED", "G_NOT_PASSED", "G_NOT_DECLARED", "G_EVIDENCE_UNAVAILABLE",
    "REQUIREMENT_CODES", "FORWARD_GATE_MIRROR", "gate_thresholds",
    "FX_CARRY_CADENCE_CHALLENGER_ID", "FUTURES_TREND_CHALLENGER_ID", "SHORT_LEG_RULE",
    "declared_candidates", "candidate_for_sleeve", "gate_dir",
    "load_conditional_approval", "declare_conditional_operational_approval",
    "evaluate_gate", "evaluate_all", "operational_signal_scores",
]
