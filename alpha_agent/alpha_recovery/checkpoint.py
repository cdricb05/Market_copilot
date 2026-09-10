"""alpha_agent.alpha_recovery.checkpoint - the frozen 10-eligible-session stop-loss.

Contract rule 15: the campaign start session and the deadline session are
computed by the canonical exchange-session owner and frozen in a
machine-readable checkpoint. The deadline never moves because results are
weak. This module:

    * asks ``engine.exchange_calendar`` (the ONE rule-based NYSE calendar
      supplier, ``CALENDAR_ID`` NYSE_RULE_BASED_R60_1) whether a date is a
      session - it never counts weekdays on its own;
    * freezes the campaign start, the ten eligible sessions strictly after it,
      the incumbent identity, the R64 parent hash, the contract hash and the
      success criteria, WRITE-ONCE;
    * answers ``sessions_elapsed`` / ``sessions_remaining`` / the deadline
      state for any as-of date, from the frozen record only;
    * verifies that a checkpoint still reproduces from its own frozen inputs.

It reads no live store and writes only the committed checkpoint file (which a
test redirects). ``api.forward_challenger_registry`` uses the same supplier
for the forward observation clock, so the two clocks can never disagree.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import (CAMPAIGN_ID, CHECKPOINT_NAME, CONTRACT_DOC, CONTRACT_VERSION, DL_BREACH,
               DL_CHALLENGER, DL_EXHAUSTED, DEADLINE_OUTCOMES, INCUMBENT_BLEND,
               INCUMBENT_LEGS, INCUMBENT_MODEL_ID, INCUMBENT_OPERATIONAL_BOOK,
               MATERIALITY_ANN_NET, R64_PARENT_COMMIT, STOP_LOSS_SESSIONS, checkpoint_path,
               contract_hash, git_head, read_repo_artifact, stable_hash, write_repo_artifact)

CALCULATION_OWNER = "alpha_agent.alpha_recovery.checkpoint"
SCHEMA = "alpha_recovery_checkpoint/1"

STATE_BEFORE = "BEFORE_DEADLINE"
STATE_AT = "AT_DEADLINE"
STATE_PAST = "PAST_DEADLINE"

#: The exchange whose sessions count. The incumbent is a US cash-equity book,
#: so the NYSE calendar decides (the same declaration the forward registry
#: makes for US_EQUITY).
ASSET_CLASS = "US_EQUITY"

#: The keys the checkpoint hash binds. Everything a later reader could be
#: tempted to move is in this tuple; the artifact stamp keys are not.
FROZEN_KEYS = ("schema", "campaign_id", "campaign_started_at", "asset_class_calendar", "calendar",
               "start_eligible_market_session", "eligible_sessions_after_start",
               "tenth_eligible_market_session", "stop_loss_sessions", "incumbent",
               "r64_parent_commit", "contract", "frozen_success_criteria", "deadline_outcomes",
               "deadline_never_moves", "write_once")


def frozen_hash(cp: dict) -> str:
    return stable_hash({k: cp.get(k) for k in FROZEN_KEYS})


def _calendar():
    """The ONE calendar supplier, imported from THIS checkout (top-level
    ``engine``), never through the ``paper_trader`` alias that the venv's
    editable finder maps to the live checkout."""
    from engine import exchange_calendar as EC   # noqa: WPS433
    return EC


def calendar_identity() -> dict:
    EC = _calendar()
    return {"owner": "engine/exchange_calendar.py", "calendar_id": EC.CALENDAR_ID,
            "exchange": EC.EXCHANGE,
            "session_rule": "a date is an eligible market session when the supplier's "
                            "is_non_session(date) is False (weekends and full-day closures "
                            "excluded); never weekday arithmetic",
            "forward_clock_consumer": "api/forward_challenger_registry.py (same supplier)"}


def is_eligible_session(d: date) -> bool:
    EC = _calendar()
    if not EC.is_supported(d):
        raise ValueError("date %s lies outside the supported calendar range" % d)
    return not EC.is_non_session(d)


def on_or_after(d: date) -> date:
    cur = d
    for _ in range(30):
        if is_eligible_session(cur):
            return cur
        cur += timedelta(days=1)
    raise RuntimeError("no eligible session within 30 days of %s" % d)


def sessions_strictly_after(d: date, n: int) -> list:
    out = []
    cur = d
    while len(out) < n:
        cur += timedelta(days=1)
        if is_eligible_session(cur):
            out.append(cur.isoformat())
    return out


def _as_date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def success_criteria() -> dict:
    return {
        "A_material_challenger_in_true_forward_competition": {
            "same_domain": ("passes the repository's frozen historical materiality gates against the "
                            "incumbent on the identical sample: net advantage >= %.3f/yr, lockbox sign "
                            "agreement, halves floor, turnover cap, drawdown multiple, Benjamini-Hochberg "
                            "q=0.10 over the executed denominator, family-aware Holm; AND is registered "
                            "prospectively through the human-gated adoption path" % MATERIALITY_ANN_NET),
            "cross_domain": ("positive incremental portfolio utility after costs for incumbent + sleeve "
                             "against incumbent-only under equal capital / risk, same evidence discipline; "
                             "AND registered prospectively through the human-gated adoption path"),
        },
        "B_owned_free_information_exhausted": (
            "every ranked owned / free information family closed under its budget with a named "
            "binding failure, plus the exact missing information with acquisition path, cost, "
            "break-even alpha, confidence haircut and expected portfolio value"),
        "otherwise": DL_BREACH,
        "thresholds_may_not_be_relaxed_after_seeing_results": True,
        "deadline_never_moves": True,
    }


def build(*, now: datetime | None = None, worktree_head: str | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    start_date = _as_date(now)
    start = on_or_after(start_date)
    after = sessions_strictly_after(start, STOP_LOSS_SESSIONS)
    body = {
        "schema": SCHEMA,
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": CAMPAIGN_ID,
        "campaign_started_at": now.replace(microsecond=0).isoformat(),
        "asset_class_calendar": ASSET_CLASS,
        "calendar": calendar_identity(),
        "start_eligible_market_session": start.isoformat(),
        "start_session_counts_as": "session 0 (the campaign's own session); the ten sessions "
                                   "STRICTLY AFTER it are the stop-loss window",
        "eligible_sessions_after_start": after,
        "tenth_eligible_market_session": after[-1],
        "stop_loss_sessions": STOP_LOSS_SESSIONS,
        "incumbent": {"model_id": INCUMBENT_MODEL_ID, "operational_book": INCUMBENT_OPERATIONAL_BOOK,
                      "legs": dict(INCUMBENT_LEGS), "blend": dict(INCUMBENT_BLEND),
                      "role": "BENCHMARK, not the presumed correct model"},
        "r64_parent_commit": R64_PARENT_COMMIT,
        "worktree_head_at_freeze": worktree_head or git_head(),
        "contract": {"path": str(CONTRACT_DOC.relative_to(CONTRACT_DOC.parents[1])).replace("\\", "/"),
                     "version": CONTRACT_VERSION, "sha256_crlf_normalised": contract_hash()},
        "frozen_success_criteria": success_criteria(),
        "deadline_outcomes": list(DEADLINE_OUTCOMES),
        "deadline_never_moves": True,
        "write_once": True,
    }
    body["checkpoint_hash"] = frozen_hash(body)
    return body


def freeze(*, now: datetime | None = None, write: bool = True) -> dict:
    """Create the checkpoint ONCE. An existing checkpoint is returned untouched:
    there is no path that rewrites a frozen deadline."""
    existing = read_repo_artifact(CHECKPOINT_NAME)
    if existing is not None:
        return {**existing, "freeze_action": "ALREADY_FROZEN"}
    body = build(now=now)
    if write:
        write_repo_artifact(CHECKPOINT_NAME, body)
    return {**body, "freeze_action": "FROZEN"}


def load() -> dict | None:
    return read_repo_artifact(CHECKPOINT_NAME)


def session_clock(checkpoint: dict, *, as_of=None) -> dict:
    """Elapsed / remaining eligible sessions from the FROZEN record only."""
    as_of_d = _as_date(as_of) if as_of is not None else datetime.now(timezone.utc).date()
    after = list(checkpoint.get("eligible_sessions_after_start") or [])
    elapsed = sum(1 for s in after if date.fromisoformat(s) <= as_of_d)
    total = int(checkpoint.get("stop_loss_sessions") or STOP_LOSS_SESSIONS)
    remaining = max(0, total - elapsed)
    deadline = checkpoint.get("tenth_eligible_market_session")
    if as_of_d < date.fromisoformat(deadline):
        state = STATE_BEFORE
    elif as_of_d == date.fromisoformat(deadline):
        state = STATE_AT
    else:
        state = STATE_PAST
    next_session = next((s for s in after if date.fromisoformat(s) > as_of_d), None)
    return {"as_of": as_of_d.isoformat(), "sessions_elapsed": elapsed,
            "sessions_remaining": remaining, "stop_loss_sessions": total,
            "start_eligible_market_session": checkpoint.get("start_eligible_market_session"),
            "tenth_eligible_market_session": deadline, "state": state,
            "next_eligible_session": next_session,
            "rendered": "%d / %d eligible sessions elapsed, %d remaining, deadline %s (%s)"
                        % (elapsed, total, remaining, deadline, state)}


def deadline_outcome(checkpoint: dict, *, scoreboard_status: str, as_of=None) -> dict:
    """Exactly one outcome. Before the deadline the outcome is PENDING with the
    would-be verdict; at or after it the verdict is final and a breach is a
    project failure state."""
    clock = session_clock(checkpoint, as_of=as_of)
    from . import ST_COMPETING, ST_EXHAUSTED
    if scoreboard_status == ST_COMPETING:
        would = DL_CHALLENGER
    elif scoreboard_status == ST_EXHAUSTED:
        would = DL_EXHAUSTED
    else:
        would = DL_BREACH
    final = clock["state"] in (STATE_AT, STATE_PAST)
    return {**clock, "outcome": would if final else "PENDING",
            "outcome_if_deadline_were_now": would, "final": final,
            "project_failure_state": bool(final and would == DL_BREACH)}


def verify(checkpoint: dict | None = None) -> dict:
    """Recompute the frozen sessions from the frozen start and compare; compare
    the contract hash; report every disagreement."""
    cp = checkpoint if checkpoint is not None else load()
    if cp is None:
        return {"present": False, "valid": False, "failures": ["checkpoint absent"]}
    failures = []
    try:
        start = date.fromisoformat(cp["start_eligible_market_session"])
        if not is_eligible_session(start):
            failures.append("start session is not an eligible session")
        if on_or_after(_as_date(cp["campaign_started_at"])) != start:
            failures.append("start session does not follow from campaign_started_at")
        after = sessions_strictly_after(start, int(cp.get("stop_loss_sessions") or 0))
        if after != list(cp.get("eligible_sessions_after_start") or []):
            failures.append("eligible sessions after start do not reproduce from the calendar")
        if cp.get("tenth_eligible_market_session") != (after[-1] if after else None):
            failures.append("deadline session does not reproduce")
    except Exception as exc:                                  # noqa: BLE001
        failures.append("calendar recomputation failed: %s" % exc)
    if (cp.get("contract") or {}).get("sha256_crlf_normalised") != contract_hash():
        failures.append("contract hash moved since the freeze")
    if cp.get("r64_parent_commit") != R64_PARENT_COMMIT:
        failures.append("R64 parent commit differs from the package constant")
    if (cp.get("incumbent") or {}).get("model_id") != INCUMBENT_MODEL_ID:
        failures.append("incumbent identity differs")
    if cp.get("stop_loss_sessions") != STOP_LOSS_SESSIONS:
        failures.append("stop-loss length differs")
    if frozen_hash(cp) != cp.get("checkpoint_hash"):
        failures.append("checkpoint hash does not match its content")
    return {"present": True, "valid": not failures, "failures": failures,
            "path": str(checkpoint_path())}
