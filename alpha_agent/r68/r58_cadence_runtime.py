r"""alpha_agent.r68.r58_cadence_runtime - the CADENCE PRODUCER the four R58
forward challengers were registered with and never had.

WHAT WAS BROKEN
---------------
``alpha_agent.r58.challengers.freeze(price, session)`` computes all four
challengers' cross-sections at any session and writes an immutable record. It
was called ONCE, by hand, on 2026-09-04. Nothing has called it since, because
nothing ever called it at all: a repository-wide search for a caller found the
module's own docstring and a MAP ENTRY naming it as an owner.

So each of the four emitted exactly one prospective prediction - for
2026-09-10, against the book adopted at registration - and at every cadence
boundary after that the accrual owner reported, correctly,
``AWAITING_NEW_GOVERNED_FREEZE``. The capital gate needs 60 matured
observations. One decision per registration, forever, reaches 1.

WHAT THIS MODULE IS, AND THE FOUR THINGS IT IS NOT
--------------------------------------------------
It is the missing PRODUCER: at each cadence boundary it re-scores the frozen
specification on information available strictly before that boundary, and hands
the resulting book to the estate's one decision-freezing owner.

  * It is NOT a second decision store. The freeze goes through
    :mod:`alpha_agent.alpha_recovery.prospective_decision`, the same immutable,
    window-bounded, first-write-wins owner every other per-session release uses
    - only under the R58 research root.
  * It is NOT a second signal implementation. Every score comes from
    ``alpha_agent.r58.challengers.build``, the originating owner's own code,
    called with a session. This module contains no z-score, no rank, no
    universe rule and no weight.
  * It is NOT a second scheduler. :mod:`alpha_agent.r52.runtime` calls
    :func:`advance` as one more stage, inside the one runtime lock.
  * It is NOT a second registry or a second accrual owner. It reads
    registrations from :mod:`api.forward_challenger_registry` and writes
    nothing the accrual owner owns.

THE BOUNDARY, AND WHY IT COMES FROM THE CALENDAR
------------------------------------------------
A decision for session ``S`` must be frozen STRICTLY BEFORE ``S`` begins - the
rule these four registrations have been held to since the day they were made,
and the rule their own inception text states ("signal uses information
available through the close of X; the position is effective at the NEXT
close"). A boundary can therefore only be acted on while it is still in the
FUTURE, so it cannot be learned from a realised price panel, which knows a
session only after it has printed. It is counted in eligible sessions from the
registrar's own ``first_eligible_observation_session`` on the authoritative
exchange calendar - the same authority the registrar used, asked through
``api.canonical_forward_accrual.cadence_boundary_after`` so that the producer
and the accrual owner can never disagree about which session is next.

FAIL CLOSED, IN FIVE PLACES
---------------------------
1. No registration, or a registration whose lifecycle is not adoptable: nothing.
2. No declared policy: nothing is frozen (the freeze owner refuses).
3. The forward panel does not reach the session the decision must be formed
   from: ``AWAITING_PUBLISHED_DATA``. A stale panel is never scored - that is
   the R66 defect, where eight sessions were lost to a frozen panel anchoring a
   live decision.
4. The boundary's window has shut with no decision: ``MISSED``, recorded and
   never backfilled.
5. The score produces an empty book: refused as malformed rather than frozen.

RESEARCH ONLY. No order, no fill, no proposal, no approval, no promotion, no
capital, no registration, no purchase, no operational-store write, no backfill.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from . import REPO_ROOT, SAFETY, now_iso, research_root, stable_hash

CALCULATION_OWNER = "alpha_agent.r68.r58_cadence_runtime"
RELEASE = "R58"

#: The four registrations this producer serves, in the order R58 froze them.
CHALLENGER_IDS = (
    "R58_SHORT_VOLUME_PRESSURE_V1",
    "R58_DISCLOSURE_INTENSITY_V1",
    "R58_FUND_MOMENTUM_VETO_V1",
    "R58_FCF_PURE_V1",
)

#: Read from the R58 package so a cadence or horizon change there can never
#: leave this producer decided on a different clock than the specification.
try:                                                        # pragma: no cover
    from ..r58 import CADENCE as R58_CADENCE
    from ..r58 import EQ_COST_RATE_PER_SIDE as R58_COST_RATE
    from ..r58 import HORIZON as R58_HORIZON
except Exception:                                           # noqa: BLE001
    R58_CADENCE, R58_HORIZON, R58_COST_RATE = 21, 21, 0.00125

#: How close a boundary must be before this producer spends a Norgate refresh
#: and a score on it. Two eligible sessions gives the schedule two independent
#: chances to cover one boundary before its window shuts, and keeps the
#: information the decision is formed from as fresh as the rule allows. It is an
#: OPERATING parameter of the producer, not part of the decision rule: it can
#: only make a decision use OLDER information than the rule permits, never
#: newer, so it can never loosen the boundary.
FREEZE_LEAD_SESSIONS = 2

#: The refresh runs in a CHILD process. It opens a vendor connection and walks
#: ~1,900 symbols; doing that inside the long-lived research worker is what the
#: estate forbids, and a child also means a vendor hang cannot wedge the lock.
ENTRYPOINT = "scripts/run_r58_forward_panel_refresh.py"
TIMEOUT_REFRESH = 3600
R57_ROOT_ENV = "PAPER_TRADER_R57_RESEARCH_ROOT"

FORWARD_SUBDIR = "r58_forward"
ACTIVATION_ARTIFACT = "R58_PRODUCER_ACTIVATION_DECISION.json"

# --------------------------------------------------------------------------- #
# States - every one of them a fact, never an estimate
# --------------------------------------------------------------------------- #
ST_NO_REGISTRATION = "R58_NO_ADOPTABLE_REGISTRATION"
ST_AWAITING_POLICY = "R58_AWAITING_POLICY_DECLARATION"
ST_NOT_A_BOUNDARY = "R58_HOLDING_NOT_A_REBALANCE_SESSION"
ST_BEFORE_LEAD = "R58_BOUNDARY_BEYOND_THE_FREEZE_LEAD"
ST_ALREADY_FROZEN = "R58_ALREADY_FROZEN"
ST_AWAITING_DATA = "R58_AWAITING_PUBLISHED_DATA"
ST_FROZEN = "R58_FROZEN"
ST_MISSED = "R58_MISSED"
ST_BLOCKED = "R58_BLOCKED"
STATES = (ST_NO_REGISTRATION, ST_AWAITING_POLICY, ST_NOT_A_BOUNDARY,
          ST_BEFORE_LEAD, ST_ALREADY_FROZEN, ST_AWAITING_DATA, ST_FROZEN,
          ST_MISSED, ST_BLOCKED)
PROGRESS_STATES = (ST_FROZEN,)
DATA_WAIT_STATES = (ST_AWAITING_DATA,)
MISSED_STATES = (ST_MISSED,)
FAILURE_STATES = (ST_BLOCKED,)
IDLE_STATES = (ST_NOT_A_BOUNDARY, ST_BEFORE_LEAD, ST_ALREADY_FROZEN,
               ST_NO_REGISTRATION)


# --------------------------------------------------------------------------- #
# Paths - everything this producer writes lives under ONE directory
# --------------------------------------------------------------------------- #
def forward_dir() -> Path:
    return research_root() / FORWARD_SUBDIR


def panel_root() -> Path:
    """The root the FORWARD panel copy is built under.

    Deliberately not the R57 research root. The R57 panel is the frozen
    substrate every settled R57/R58 result was measured on; a producer that
    rebuilt it in place would silently redefine what those results mean.
    """
    return forward_dir() / "r57_forward"


def runs_dir() -> Path:
    return forward_dir() / "runs"


def decision_root() -> Path:
    """Where the per-session R58 decisions live - the R58 research root.

    The SAME root ``api.canonical_forward_accrual`` resolves for release R58
    through its ``_PER_SESSION_DECISION_ROOTS`` table, so the file this producer
    writes is the file that resolver reads. One path, asked of one owner.
    """
    from paper_trader.api import canonical_forward_accrual as CFA
    root = CFA.per_session_decision_root(RELEASE)
    if root is None:                                        # pragma: no cover
        from ..r58 import research_root as r58_root
        return r58_root()
    return Path(root)


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
# The registrations, and the originating freeze behind each
# --------------------------------------------------------------------------- #
def registrations() -> list:
    """Every adoptable canonical registration whose release is R58."""
    try:
        from paper_trader.api import forward_challenger_registry as FCR
        regs = FCR.load_registrations()
    except Exception:                                       # noqa: BLE001
        return []
    out = []
    for r in regs or []:
        ident = r.get("identity") or {}
        if str(ident.get("release") or "") != RELEASE:
            continue
        life = (r.get("lifecycle_at_registration") or {})
        if life and life.get("adoptable") is False:
            continue
        out.append(r)
    return out


def frozen_record(challenger_id: str) -> Optional[dict]:
    """The ORIGINAL adoption freeze, read from the R58 owner's own artifact.

    Never rewritten and never re-derived. It supplies the identity a per-session
    decision must carry - above all ``record_hash``, which is what the registrar
    bound the registration to and what keeps that binding intact.
    """
    try:
        from paper_trader.alpha_agent import r58 as R58
        return _read(R58.research_root() / "challengers"
                     / ("%s.json" % str(challenger_id)))
    except Exception:                                       # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# The policy - declared ONCE per challenger, before anything is decided
# --------------------------------------------------------------------------- #
def declare_policies(*, now: Optional[str] = None) -> dict:
    """Declare each challenger's emission boundary. Idempotent, first write wins.

    The boundary declared is ``PRIOR_SESSION_ONLY`` - the STRICTEST rule in the
    vocabulary, and byte-for-byte the one an ABSENT declaration resolves to. So
    declaring it changes nothing about how these four registrations are judged;
    it only makes the rule they were always held to a written fact rather than a
    default, and gives the freeze owner the boundary it requires.
    """
    from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
    from paper_trader.engine import forward_emission_window as EW
    from paper_trader.api import canonical_forward_accrual as CFA

    root = decision_root()
    out = {}
    for reg in registrations():
        cid = str(reg.get("challenger_id") or "")
        rec = frozen_record(cid)
        if not rec:
            out[cid] = {"outcome": "REFUSED_NO_FROZEN_RECORD",
                        "detail": "the R58 adoption freeze could not be read; a "
                                  "policy is never declared against a record "
                                  "this producer cannot see"}
            continue
        construction = rec.get("construction") or {}
        ident = reg.get("identity") or {}
        res = PD.declare_policy(
            challenger_id=cid,
            boundary=EW.BOUNDARY_PRIOR_SESSION,
            rebalance_cadence_sessions=int(
                construction.get("rebalance_cadence_sessions") or R58_CADENCE),
            evaluation_horizon_sessions=int(
                construction.get("evaluation_horizon_sessions") or R58_HORIZON),
            cost_policy=dict(rec.get("cost_policy") or {}),
            # DELIBERATELY EMPTY. The R58 universe is the point-in-time S&P 500
            # and its membership changes between boundaries by construction. A
            # scope frozen today would refuse a decision that correctly held a
            # name added to the index next month - turning an index addition
            # into a producer failure. The registration itself carries an empty
            # scope for the same reason.
            instrument_scope=[],
            identity={
                "release": RELEASE,
                "challenger_id": cid,
                "freeze_record_hash": rec.get("record_hash"),
                "model_spec_hash": ident.get("model_spec_hash"),
                "identity_hash": ident.get("identity_hash"),
                "freeze_id": reg.get("freeze_id"),
                "spec_hash": rec.get("spec_hash"),
                "role": (rec.get("spec") or {}).get("role", "CANDIDATE"),
            },
            emission_rule=rec.get("inception_rule"),
            execution_contract={
                "decision_session_is": "the INFORMATION session",
                "execution_boundary": "DECISION_SESSION_CLOSE",
                "holding_sessions": int(
                    construction.get("evaluation_horizon_sessions")
                    or R58_HORIZON),
                # Lets the accrual owner see this producer's next boundary while
                # it is still in the future. Without it every boundary after the
                # first would be learned only once it had printed - after its
                # window shut - and forfeited with the decision already on disk.
                CFA.ARMS_CADENCE_GRID: True,
                "cadence_grid_owner": CALCULATION_OWNER,
                "declared_by": RELEASE,
            },
            now=now, root=root)
        out[cid] = {"outcome": res.get("outcome"), "path": res.get("path")}
    return out


# --------------------------------------------------------------------------- #
# The forward panel - refreshed in a child, never in the worker
# --------------------------------------------------------------------------- #
def forward_panel_meta() -> Optional[dict]:
    from paper_trader.alpha_agent.r57 import panel as R57P
    return _read(panel_root() / "panels" / (R57P.PANEL_NAME + ".meta.json"))


def forward_panel_last_session() -> Optional[str]:
    meta = forward_panel_meta() or {}
    dates = meta.get("dates") or []
    return str(dates[-1]) if dates else None


def refresh_forward_panel(*, end: str,
                          timeout: int = TIMEOUT_REFRESH) -> dict:
    """Rebuild the FORWARD panel copy to ``end``, in a child process.

    The canonical builder (``alpha_agent.r57.panel.build_panel``) does the work;
    only the store and the window differ. The frozen R57 panel directory is
    never opened for writing, because the child's ``PAPER_TRADER_R57_RESEARCH_ROOT``
    points somewhere else.
    """
    script = REPO_ROOT / ENTRYPOINT
    if not script.exists():
        return {"ran": False, "state": "REFRESH_ENTRYPOINT_MISSING",
                "detail": "no %s in the checkout" % ENTRYPOINT}
    env = dict(os.environ)
    env[R57_ROOT_ENV] = str(panel_root())
    env.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        proc = subprocess.run(
            [sys.executable, str(script), "--end", str(end)],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True,
            timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"ran": True, "state": "REFRESH_TIMED_OUT",
                "timeout_seconds": timeout}
    except Exception as exc:                                # noqa: BLE001
        return {"ran": False, "state": "REFRESH_FAILED",
                "error": type(exc).__name__, "detail": str(exc)[:200]}
    return {"ran": True,
            "state": ("REFRESH_OK" if proc.returncode == 0
                      else "REFRESH_FAILED"),
            "returncode": proc.returncode,
            "requested_end": str(end),
            "stdout_tail": (proc.stdout or "")[-400:],
            "stderr_tail": (proc.stderr or "")[-400:],
            "panel_last_session": forward_panel_last_session()}


# --------------------------------------------------------------------------- #
# The boundary
# --------------------------------------------------------------------------- #
def _eligible_distance(a: str, b: str, cap: int = 400) -> Optional[int]:
    """How many ELIGIBLE sessions separate ``a`` from ``b`` (a < b)."""
    from paper_trader.engine import exchange_calendar as EC
    from datetime import date as _date
    try:
        cur, end = _date.fromisoformat(str(a)[:10]), _date.fromisoformat(str(b)[:10])
    except (TypeError, ValueError):
        return None
    if cur > end:
        return None
    n = 0
    while cur < end and n < cap:
        cur = cur + timedelta(days=1)
        if not EC.is_supported(cur):
            return None
        if not EC.is_non_session(cur):
            n += 1
    return n if cur == end else None


def next_boundary(registration: dict, *, now: str) -> dict:
    """The next cadence boundary this producer may still decide, and its window.

    Walks the registration's cadence grid on the exchange calendar from the
    registrar's first eligible observation session. A boundary already carrying
    a frozen decision is done; the FIRST boundary is governed by the book
    adopted at registration and needs none; a boundary whose window has shut
    without a decision is MISSED and reported as such. The first boundary with
    an OPEN window is the one this run may act on.
    """
    from paper_trader.api import canonical_forward_accrual as CFA
    from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
    from paper_trader.engine import forward_emission_window as EW

    cid = str(registration.get("challenger_id") or "")
    first = CFA.first_decision_session(registration)
    policy = CFA.emission_policy(registration)
    root = decision_root()
    missed, cur, seen = [], first, 0
    while cur and seen < 60:
        seen += 1
        frozen = PD.load_decision(cid, cur, root=root)
        cls = EW.classify(policy=policy, session=cur, now=now)
        state = cls.get("state")
        if frozen:
            pass                     # decided; move on
        elif cur == first:
            pass                     # governed by the adopted book, by design
        elif state == EW.WINDOW_CLOSED:
            missed.append({"decision_session": cur,
                           "closed_at": cls.get("closes_at")})
        else:
            return {"boundary": cur, "first_decision_session": first,
                    "emission_window": {k: cls.get(k) for k in
                                        ("state", "opens_at", "closes_at",
                                         "boundary")},
                    "window_state": state, "missed_boundaries": missed,
                    "already_frozen": False}
        nxt = CFA.cadence_boundary_after(
            registration, cadence_sessions=R58_CADENCE, after=cur, first=first)
        if not nxt or nxt <= cur:
            break
        cur = nxt
    return {"boundary": None, "first_decision_session": first,
            "missed_boundaries": missed, "already_frozen": True,
            "detail": "no boundary within the walked horizon is still open and "
                      "undecided"}


# --------------------------------------------------------------------------- #
# The score - computed by the ORIGINATING owner, never here
# --------------------------------------------------------------------------- #
def score_books(information_session: str) -> dict:
    """Every challenger's book at ``information_session``, from the R58 owner.

    This function opens the forward panel and calls
    ``alpha_agent.r58.challengers.build``. It contains no formula: the universe
    rule, the scores, the ranking and the top-N construction are all the
    originating owner's, asked with a session.
    """
    from paper_trader.alpha_agent.r58 import challengers as CH

    prev = os.environ.get(R57_ROOT_ENV)
    os.environ[R57_ROOT_ENV] = str(panel_root())
    try:
        from paper_trader.alpha_agent.r57 import panel as R57P
        price = R57P.load_panel()
        built = CH.build(price, str(information_session))
    finally:
        if prev is None:
            os.environ.pop(R57_ROOT_ENV, None)
        else:
            os.environ[R57_ROOT_ENV] = prev
    uni = built["universe"]
    books = {}
    for cid, (scores, meta) in (built.get("candidates") or {}).items():
        book = CH._book_from_scores(scores, uni)
        books[cid] = {"weights": dict(book.get("weights") or {}),
                      "n_held": book.get("n_held"),
                      "n_scored": book.get("n_scored"),
                      "input_evidence": meta}
    return {"asof": built.get("asof"), "universe_session": uni.get("session"),
            "n_eligible": uni.get("n"),
            "universe_identity_hash": stable_hash(uni.get("symbols")),
            "books": books}


# --------------------------------------------------------------------------- #
# The advance
# --------------------------------------------------------------------------- #
def advance(*, now: Optional[str] = None, dry_run: bool = False,
            allow_refresh: bool = True) -> dict:
    """Take every R58 cadence decision that is legally due right now.

    ``now`` is the ONLY clock this function consults, supplied by the caller so
    the whole contract is testable without waiting a month for a boundary.
    """
    ts = now or now_iso()
    today = str(ts)[:10]
    out = {
        "calculation_owner": CALCULATION_OWNER,
        "release": RELEASE,
        "now": ts,
        "today": today,
        "state_vocabulary": list(STATES),
        "cadence_sessions": R58_CADENCE,
        "horizon_sessions": R58_HORIZON,
        "freeze_lead_sessions": FREEZE_LEAD_SESSIONS,
        "dry_run": bool(dry_run),
        "backfilled": False,
        "paid_dollars": 0.0,
        "safety": dict(SAFETY),
        "challengers": [],
    }
    regs = registrations()
    if not regs:
        return {**out, "state": ST_NO_REGISTRATION, "n_registered": 0,
                "detail": "no adoptable R58 registration is held by the "
                          "canonical registrar"}
    out["n_registered"] = len(regs)
    out["policies"] = declare_policies(now=ts)

    # WHICH boundaries are live, asked BEFORE anything expensive happens.
    live, rows = [], []
    for reg in regs:
        cid = str(reg.get("challenger_id") or "")
        try:
            nb = next_boundary(reg, now=ts)
        except Exception as exc:                            # noqa: BLE001
            rows.append({"challenger_id": cid, "state": ST_BLOCKED,
                         "error": type(exc).__name__,
                         "detail": str(exc)[:200]})
            continue
        row = {"challenger_id": cid,
               "first_decision_session": nb.get("first_decision_session"),
               "next_boundary": nb.get("boundary"),
               "missed_boundaries": nb.get("missed_boundaries") or [],
               "emission_window": nb.get("emission_window")}
        if nb.get("boundary"):
            live.append((reg, nb, row))
        else:
            row["state"] = ST_ALREADY_FROZEN if nb.get("already_frozen") \
                else ST_NOT_A_BOUNDARY
            rows.append(row)
    if not live:
        out["challengers"] = rows
        return {**out, "state": _rollup(rows),
                "detail": "no R58 challenger has an open, undecided boundary"}

    # THE REFRESH, spent only when a boundary is actually within reach. The
    # cheapest correct thing this producer can do on 19 sessions out of 21 is
    # nothing at all, and that is what it does.
    panel_last = forward_panel_last_session()
    soonest = min(nb["boundary"] for _r, nb, _row in live)
    need_refresh = True
    if panel_last:
        gap = _eligible_distance(panel_last, soonest)
        need_refresh = gap is None or gap > 1
        if gap is not None and gap > FREEZE_LEAD_SESSIONS + 1:
            for _r, _nb, row in live:
                row["state"] = ST_BEFORE_LEAD
                row["panel_last_session"] = panel_last
                row["eligible_sessions_to_boundary"] = gap
                rows.append(row)
            out["challengers"] = rows
            out["forward_panel_last_session"] = panel_last
            return {**out, "state": ST_BEFORE_LEAD,
                    "detail": ("the soonest boundary is %s, %d eligible sessions "
                               "after the panel's newest session %s; a decision "
                               "is formed as late as its rule allows, not as "
                               "early as possible" % (soonest, gap, panel_last))}
    if need_refresh and allow_refresh and not dry_run:
        out["panel_refresh"] = refresh_forward_panel(end=today)
        panel_last = forward_panel_last_session()
    out["forward_panel_last_session"] = panel_last

    if not panel_last:
        for _r, _nb, row in live:
            row["state"] = ST_AWAITING_DATA
            row["blocked_on"] = "FORWARD_PANEL_ABSENT"
            row["blocked_owner"] = "alpha_agent.r57.panel.build_panel"
            rows.append(row)
        out["challengers"] = rows
        return {**out, "state": ST_AWAITING_DATA,
                "detail": "the forward panel holds no session; nothing is scored "
                          "from a panel this producer cannot read"}

    # THE INFORMATION SESSION. Strictly before the boundary, and the newest such
    # session the panel actually holds. A panel that has not reached it is a
    # WAIT, never a decision taken on older data than the rule allows without
    # saying so.
    results = []
    scored_cache = {}
    for reg, nb, row in live:
        cid = row["challenger_id"]
        boundary = nb["boundary"]
        if panel_last >= boundary:
            # The panel already holds the boundary session itself. Scoring on it
            # would form the decision from the very close it is entered at.
            row["state"] = ST_BLOCKED
            row["blocked_on"] = "PANEL_HOLDS_THE_DECISION_SESSION"
            row["detail"] = ("the forward panel holds %s, which is at or after "
                             "the boundary %s; a decision entered at that close "
                             "may not be formed from it"
                             % (panel_last, boundary))
            results.append(row)
            continue
        gap = _eligible_distance(panel_last, boundary)
        if gap is None or gap > FREEZE_LEAD_SESSIONS:
            row["state"] = ST_AWAITING_DATA
            row["blocked_on"] = "FORWARD_PANEL_BEHIND_THE_DECISION_BOUNDARY"
            row["blocked_owner"] = "norgatedata via alpha_agent.r57.panel"
            row["panel_last_session"] = panel_last
            row["eligible_sessions_to_boundary"] = gap
            row["detail"] = ("the newest panel session is %s and the boundary is "
                             "%s; the decision is not formed from a panel that "
                             "has not caught up" % (panel_last, boundary))
            results.append(row)
            continue
        info = panel_last
        row["information_session"] = info
        row["eligible_sessions_to_boundary"] = gap
        if info not in scored_cache:
            try:
                scored_cache[info] = score_books(info)
            except Exception as exc:                        # noqa: BLE001
                scored_cache[info] = {"error": type(exc).__name__,
                                      "detail": str(exc)[:220]}
        scored = scored_cache[info]
        if scored.get("error"):
            row["state"] = ST_BLOCKED
            row["blocked_on"] = "SCORER_FAILED"
            row["blocked_owner"] = "alpha_agent.r58.challengers.build"
            row["detail"] = scored.get("detail")
            results.append(row)
            continue
        book = (scored.get("books") or {}).get(cid) or {}
        weights = dict(book.get("weights") or {})
        row["n_held"] = book.get("n_held")
        row["n_scored"] = book.get("n_scored")
        row["universe_session"] = scored.get("universe_session")
        if not weights:
            row["state"] = ST_BLOCKED
            row["blocked_on"] = "EMPTY_BOOK"
            row["detail"] = ("the specification scored no eligible name at %s; "
                             "an empty book is refused, never frozen" % info)
            results.append(row)
            continue
        if dry_run:
            row["state"] = ST_FROZEN
            row["dry_run"] = True
            row["weights_hash"] = stable_hash(dict(sorted(weights.items())))
            results.append(row)
            continue
        res = _freeze(reg, boundary=boundary, information_session=info,
                      weights=weights, scored=scored, now=ts)
        row.update(res)
        results.append(row)

    out["challengers"] = rows + results
    return {**out, "state": _rollup(out["challengers"])}


def _freeze(registration: dict, *, boundary: str, information_session: str,
            weights: dict, scored: dict, now: str) -> dict:
    """Hand ONE book to the estate's decision-freezing owner."""
    from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD

    cid = str(registration.get("challenger_id") or "")
    root = decision_root()
    meta = forward_panel_meta() or {}
    res = PD.freeze_decision(
        challenger_id=cid,
        eligible_session=boundary,
        weights=weights,
        source_data_hash=str(meta.get("manifest_hash") or "UNKNOWN"),
        feature_state_hash=stable_hash({
            "universe_identity_hash": scored.get("universe_identity_hash"),
            "information_session": information_session,
            "n_eligible": scored.get("n_eligible")}),
        # The inputs are the panel and the owned event feeds AS OF the
        # information session's close, which is strictly before the boundary,
        # so the cutoff cannot be violated by construction. The instant is
        # stated rather than left absent so a reader can check it.
        feature_observed_at="%sT23:59:59+00:00" % str(information_session)[:10],
        decision_timestamp=now, now=now, root=root,
        context={
            "producer": CALCULATION_OWNER,
            "information_session": information_session,
            "decision_session": boundary,
            "entry_boundary": "the close of the decision session",
            "cadence_sessions": R58_CADENCE,
            "horizon_sessions": R58_HORIZON,
            "universe_session": scored.get("universe_session"),
            "universe_identity_hash": scored.get("universe_identity_hash"),
            "n_eligible": scored.get("n_eligible"),
            "forward_panel_manifest_hash": meta.get("manifest_hash"),
            "forward_panel_last_session": (meta.get("dates") or [None])[-1],
            "backfilled": False,
        })
    outcome = res.get("outcome")
    if outcome == PD.FROZEN:
        state = ST_FROZEN
    elif outcome == PD.ALREADY_FROZEN:
        state = ST_ALREADY_FROZEN
    elif outcome == PD.REFUSED_TOO_LATE:
        state = ST_MISSED
    else:
        state = ST_BLOCKED
    return {"state": state, "freeze_outcome": outcome,
            "frozen": bool(res.get("frozen")),
            "decision_path": res.get("path"),
            "decision_identity_hash": (res.get("decision") or {}).get(
                "decision_identity_hash"),
            "weights_hash": (res.get("decision") or {}).get("weights_hash"),
            "detail": res.get("detail")}


def _rollup(rows: list) -> str:
    """The run's state is the state of its most consequential row."""
    states = [r.get("state") for r in rows or []]
    for s in (ST_BLOCKED, ST_MISSED, ST_FROZEN, ST_AWAITING_DATA,
              ST_AWAITING_POLICY, ST_BEFORE_LEAD, ST_ALREADY_FROZEN,
              ST_NOT_A_BOUNDARY):
        if s in states:
            return s
    return ST_NOT_A_BOUNDARY


# --------------------------------------------------------------------------- #
# The governed activation record
# --------------------------------------------------------------------------- #
def activation_decision() -> dict:
    """WHY these four identities may continue rather than be superseded.

    Written as an artifact because it is a governed judgement, not a mechanical
    consequence, and a human owner must be able to read the evidence it rests on
    and reverse it in one step if they disagree.
    """
    rows = []
    for reg in registrations():
        cid = str(reg.get("challenger_id") or "")
        rec = frozen_record(cid) or {}
        construction = rec.get("construction") or {}
        rows.append({
            "challenger_id": cid,
            "registration_session": reg.get("registration_session"),
            "freeze_record_hash": reg.get("freeze_record_hash"),
            "frozen_construction_declares_cadence":
                construction.get("rebalance_cadence_sessions"),
            "frozen_construction_declares_horizon":
                construction.get("evaluation_horizon_sessions"),
            "frozen_inception_rule": rec.get("inception_rule"),
            "role": (rec.get("spec") or {}).get("role", "CANDIDATE"),
        })
    return {
        "schema": "r68_r58_producer_activation_decision/1",
        "decision_owner": CALCULATION_OWNER,
        "decided_at": now_iso(),
        "DECISION": "CONTINUE_THE_EXISTING_FOUR_IDENTITIES",
        "the_question": (
            "R67 held that attaching a cadence producer to an identity "
            "registered when no such producer existed CHANGES WHAT THAT "
            "IDENTITY MEANS, and is therefore a governed decision for a human "
            "owner rather than an engineering repair."),
        "why_continuation_is_faithful": [
            "The frozen construction of all four challengers declares "
            "rebalance_cadence_sessions = 21. A rebalance cadence is a "
            "statement that the book WILL be re-scored every 21 sessions. The "
            "producer fulfils that declaration; it does not add one.",
            "The registrar copied that construction verbatim into each "
            "registration, so the cadence is part of the registered "
            "specification and not an inference.",
            "The accrual owner's state at every boundary since registration "
            "has been AWAITING_NEW_GOVERNED_FREEZE, whose own definition is "
            "'the originating owner never froze a new decision'. That is a "
            "system waiting for this producer, not one declaring that these "
            "identities never rebalance.",
            "Each per-session decision carries the ORIGINAL adoption freeze's "
            "record_hash, so the registrar's integrity binding is unchanged "
            "and a book whose hash does not match is still refused.",
            "The declared emission boundary is PRIOR_SESSION_ONLY - the "
            "strictest rule in the vocabulary and byte-for-byte the one these "
            "registrations have been judged by since the day they were made. "
            "Nothing is loosened.",
            "No historical record is rewritten. The adoption freeze files are "
            "not opened for writing, the one emitted observation of 2026-09-10 "
            "stands, and no session before this producer existed is decided.",
        ],
        "what_would_have_required_a_NEW_identity": [
            "A change to any scored input, weight rule, universe rule, cadence "
            "or horizon. None is made: every score comes from "
            "alpha_agent.r58.challengers.build, unmodified.",
            "A looser emission boundary, which would let a decision be frozen "
            "with information the original rule excluded.",
            "Backfilling any boundary that passed while no producer existed. "
            "Those are reported MISSED and are never written.",
        ],
        "how_to_reverse_this_in_one_step": (
            "Remove the R58 entry from "
            "api.canonical_forward_accrual._PER_SESSION_DECISION_ROOTS. The "
            "per-session decisions stop resolving, the accrual owner returns to "
            "reporting AWAITING_NEW_GOVERNED_FREEZE at each boundary, and every "
            "frozen decision remains on disk as an unread immutable record. "
            "Nothing is deleted and nothing needs to be undone."),
        "registrations": rows,
        "safety": dict(SAFETY),
    }


def write_activation_decision() -> Path:
    return _atomic_write(forward_dir() / ACTIVATION_ARTIFACT,
                         activation_decision())


__all__ = ["CALCULATION_OWNER", "RELEASE", "CHALLENGER_IDS", "STATES",
           "PROGRESS_STATES", "DATA_WAIT_STATES", "MISSED_STATES",
           "FAILURE_STATES", "IDLE_STATES", "R58_CADENCE", "R58_HORIZON",
           "FREEZE_LEAD_SESSIONS", "forward_dir", "panel_root", "runs_dir",
           "decision_root", "registrations", "frozen_record",
           "declare_policies", "forward_panel_meta",
           "forward_panel_last_session", "refresh_forward_panel",
           "next_boundary", "score_books", "advance", "activation_decision",
           "write_activation_decision"]
