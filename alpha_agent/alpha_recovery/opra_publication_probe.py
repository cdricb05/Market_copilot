r"""alpha_agent.alpha_recovery.opra_publication_probe - WHEN does Databento's
HISTORICAL OPRA feed actually expose a session, measured on a real weekday.

THE QUESTION, AND WHY IT IS NOT ANSWERABLE FROM A CALENDAR
    ``next_open_challenger`` removes the need for a LIVE entitlement by entering
    at the next session's open. It does not remove every requirement: session
    ``t``'s 15:45 ET snapshot must be PUBLISHED by the historical vendor before
    session ``t+1`` opens, or the decision cannot be formed inside its window and
    the session is legitimately missed.

    That latency is a property of the vendor's pipeline. It is not in any
    contract this estate holds, it is not derivable from the market calendar, and
    guessing it would put a manufactured number underneath a forward evidence
    programme.

WHY THE EARLIER CLAIM WAS WITHDRAWN
    R62.3 recorded ``metadata.get_dataset_range`` returning an exact midnight-UTC
    boundary "during a live session" and read that as proof of a daily batch.
    The reading was taken on **Saturday 2026-09-12**. With the market shut,
    ``end = 2026-09-12T00:00:00Z`` means only "data through Friday", which is
    what any vendor returns on a Saturday - so the measurement demonstrated
    nothing about publication latency. The claim was withdrawn rather than
    softened. This module exists to replace it with a measurement taken while a
    weekday session is genuinely in flight and afterwards.

WHAT A POLL COSTS: NOTHING
    Two metadata endpoints, in that order, and no others:

        metadata.get_dataset_range     does the dataset claim to cover ``t``?
        metadata.get_billable_size     for the session's own one-day window and
                                       a handful of near-expiry OSI symbols, do
                                       any bytes exist?

    Both are free - they are the same endpoints the campaign's spending contract
    already uses as the gate BEFORE a download. ``timeseries.get_range`` is not
    reachable from this module at all, so a poll can never turn into a purchase,
    and the second call is made ONLY when the first says the data should be
    there, so a session that is not published costs one cheap call and stops.

WHAT THE ANSWER IS, AND WHAT IT IS NOT
    ``first_available_time`` is the first instant at which THIS PROBE ASKED and
    was told yes. It is an UPPER BOUND on the true publication instant, never the
    instant itself, and the gap to the previous negative poll is reported as the
    measurement's resolution so no reader can mistake a coarse poll schedule for
    a precise latency.

RESEARCH ONLY. Reads metadata; downloads nothing; buys nothing; writes one
append-only artifact under the campaign research root.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from paper_trader.engine import market_hours as MH

from . import now_iso, read_artifact, write_artifact
from . import databento_acquisition as DA
from . import options_acquisition as OA

CALCULATION_OWNER = "alpha_agent.alpha_recovery.opra_publication_probe"
ARTIFACT_NAME = "opra_publication_probe.json"

DATASET = OA.DATASET
SCHEMA = OA.SCHEMA

#: The only endpoints this module may reach. ``timeseries.get_range`` is absent
#: and there is no code path to it.
ENDPOINTS_USED = ("metadata.get_dataset_range", "metadata.get_billable_size")

#: How many strikes either side of the money the confirmation call names. A
#: handful is enough to prove records exist for the session; the probe is not
#: buying a surface, it is asking whether one could be bought.
CONFIRM_STRIKES = 3

#: The session ends at 16:00 ET. Publication cannot precede it, so it is the
#: natural zero for a latency, and the 15:45 snapshot instant is reported beside
#: it because that is the record the rule actually needs.
SESSION_CLOSE_ET = (16, 0)
SNAPSHOT_ET = OA.SNAPSHOT_ET

# Poll outcomes.
NOT_PUBLISHED = "NOT_PUBLISHED"
PUBLISHED = "PUBLISHED"
RANGE_COVERS_BUT_NO_BYTES = "RANGE_COVERS_BUT_NO_BYTES"
BLOCKED_CREDENTIAL = "BLOCKED_CREDENTIAL_ABSENT"
PROVIDER_ERROR = "PROVIDER_ERROR"
NOT_A_SESSION = "NOT_AN_ELIGIBLE_SESSION"
POLL_OUTCOMES = (NOT_PUBLISHED, PUBLISHED, RANGE_COVERS_BUT_NO_BYTES,
                 BLOCKED_CREDENTIAL, PROVIDER_ERROR, NOT_A_SESSION)

SAFETY = {
    "research_only": True,
    "downloads_market_data": False,
    "purchases_data": False,
    "paid_dollars_authorised": 0.0,
    "starts_trial_or_subscription": False,
    "creates_orders": False,
    "creates_fills": False,
    "promotes_model": False,
    "allocates_capital": False,
    "backfill_allowed": False,
    "records_are_append_only": True,
}


# --------------------------------------------------------------------------- #
# Instants
# --------------------------------------------------------------------------- #
def _d(value) -> Optional[date]:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _as_utc(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            d = _d(raw)
            if d is None:
                return None
            dt = datetime(d.year, d.month, d.day)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _et_instant(session, hhmm: tuple) -> Optional[str]:
    d = _d(session)
    if d is None:
        return None
    naive = datetime.combine(d, time(int(hhmm[0]), int(hhmm[1])))
    return MH.to_eastern(naive).astimezone(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# The confirmation symbols
# --------------------------------------------------------------------------- #
def confirm_symbols(session, underlying_level: Optional[float] = None) -> dict:
    """A few near-the-money OSI symbols on the expiry the frozen rule reads.

    The expiry choice, the band, the spacing and the symbol spelling all come
    from the acquisition owner - this module invents no symbology, because a
    symbol the venue does not recognise would answer "no bytes" for a session
    that is in fact published.
    """
    from . import live_snapshot_contract as LSC

    out = {"session": str(session)[:10], "dataset": DATASET, "schema": SCHEMA}
    nf = LSC.near_and_far(str(session)[:10])
    out["near_expiry"] = nf.get("near")
    if not nf.get("near"):
        return {**out, "symbols": [], "why": "no near expiry is live on this session"}
    level = underlying_level if underlying_level else _owned_level(session)
    if not level:
        return {**out, "symbols": [], "why": "no owned underlying level to centre a band on"}
    band = OA.band_for(float(level))
    mid = len(band) // 2
    lo = max(0, mid - CONFIRM_STRIKES // 2)
    strikes = band[lo:lo + CONFIRM_STRIKES]
    exp = date.fromisoformat(nf["near"])
    out["underlying_level"] = round(float(level), 2)
    out["strikes"] = strikes
    out["symbols"] = [OA.osi(exp, r, k) for k in strikes for r in ("C", "P")]
    return out


def _owned_level(session) -> Optional[float]:
    """An approximate SPY level from data already owned. A band 20 % wide needs
    nothing better, and asking the vendor for one would cost a call."""
    try:
        from . import options_surface as OS

        f = OS.features(surface=OS.surface_path())
        px = f["underlying_close"].to_numpy()
        for i in range(len(px) - 1, -1, -1):
            if px[i] and px[i] == px[i]:
                return float(px[i])
    except Exception:                                 # noqa: BLE001 - owned read
        return None
    return None


# --------------------------------------------------------------------------- #
# One poll
# --------------------------------------------------------------------------- #
def poll(session, *, client: Optional[DA.Client] = None,
         now: Optional[str] = None) -> dict:
    """Ask ONCE whether the vendor exposes ``session``. Free; downloads nothing.

    Stage two runs only when stage one says the dataset claims to cover the
    session, so an un-published session costs exactly one cheap metadata call.
    """
    ts = now or now_iso()
    s = str(session)[:10]
    out = {"session": s, "queried_at": ts, "dataset": DATASET, "schema": SCHEMA,
           "endpoints_used": list(ENDPOINTS_USED), "downloaded_bytes": 0,
           "paid_usd": 0.0}

    cred = DA.credential_state()
    if not cred["usable"]:
        return {**out, "outcome": BLOCKED_CREDENTIAL, "available": None,
                "detail": cred["remediation"]}

    cl = client or DA.Client()
    try:
        rng = cl.dataset_range(DATASET)
    except DA.DatabentoError as exc:
        return {**out, "outcome": PROVIDER_ERROR, "available": None,
                "detail": str(exc)[:300]}
    reported_end = ((rng or {}).get("schema") or {}).get(SCHEMA) or rng or {}
    reported_end = reported_end.get("end") or (rng or {}).get("end")
    avail_end = DA._available_end(rng, SCHEMA) or DA._available_end(rng)
    out["dataset_range_end_raw"] = reported_end
    out["dataset_available_end"] = avail_end
    out["range_covers_session"] = bool(avail_end and avail_end >= s)

    if not out["range_covers_session"]:
        return {**out, "outcome": NOT_PUBLISHED, "available": False,
                "detail": ("the dataset reports data through %s, which does not "
                           "reach %s" % (avail_end, s))}

    # Stage two: the range is a dataset-level claim; this asks whether the
    # session's OWN day holds bytes for symbols the frozen rule actually reads.
    sym = confirm_symbols(s)
    out["confirmation"] = {k: v for k, v in sym.items() if k != "symbols"}
    out["confirmation"]["n_symbols"] = len(sym.get("symbols") or [])
    if not sym.get("symbols"):
        return {**out, "outcome": RANGE_COVERS_BUT_NO_BYTES, "available": None,
                "detail": "no confirmation symbol set could be built: %s"
                          % sym.get("why")}
    nxt = (_d(s) + timedelta(days=1)).isoformat()     # Databento `end` is EXCLUSIVE
    try:
        size = cl.billable_size(sym["symbols"], s, nxt, dataset=DATASET,
                                schema=SCHEMA)
    except DA.DatabentoError as exc:
        # A window that runs past the served data answers 422 rather than zero.
        # That is a NEGATIVE, not a fault, and it must not be recorded as one.
        detail = str(exc)[:300]
        if "available_end" in detail or "422" in detail:
            return {**out, "outcome": NOT_PUBLISHED, "available": False,
                    "detail": "the venue refused the session's own window: %s"
                              % detail}
        return {**out, "outcome": PROVIDER_ERROR, "available": None,
                "detail": detail}
    out["billable_bytes"] = int(size)
    if int(size) <= 0:
        return {**out, "outcome": RANGE_COVERS_BUT_NO_BYTES, "available": False,
                "detail": ("the dataset range covers %s but the session's own "
                           "window holds no bytes for the near-expiry symbols" % s)}
    return {**out, "outcome": PUBLISHED, "available": True,
            "detail": "%d billable bytes exist for %d near-expiry symbols on %s"
                      % (int(size), len(sym["symbols"]), s)}


# --------------------------------------------------------------------------- #
# The append-only record
# --------------------------------------------------------------------------- #
def _load() -> dict:
    body = read_artifact(ARTIFACT_NAME)
    if not isinstance(body, dict) or "sessions" not in body:
        return {"sessions": {}}
    return body


def record(poll_result: dict, *, write: bool = True) -> dict:
    """Append ONE poll. Never rewrites a poll and never moves a first sighting.

    ``first_available_time`` is written once. A later poll cannot move it
    earlier (it did not happen earlier) and must not move it later (that would
    erase the sighting) - so the field is set exactly once and then left alone,
    which is what makes this record evidence rather than a status display.
    """
    body = _load()
    s = str(poll_result.get("session"))[:10]
    node = body["sessions"].setdefault(s, {"session": s, "polls": []})
    node["polls"].append(poll_result)
    node["polls"].sort(key=lambda p: str(p.get("queried_at") or ""))
    if not node.get("first_query_time"):
        node["first_query_time"] = poll_result.get("queried_at")
    if poll_result.get("available") and not node.get("first_available_time"):
        node["first_available_time"] = poll_result.get("queried_at")
    body["sessions"][s] = _summarise(node)
    body["calculation_owner"] = CALCULATION_OWNER
    body["dataset"] = DATASET
    body["schema"] = SCHEMA
    body["endpoints_used"] = list(ENDPOINTS_USED)
    body["downloads_performed"] = 0
    body["paid_usd"] = 0.0
    body["safety"] = dict(SAFETY)
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body["sessions"][s]


def _summarise(node: dict) -> dict:
    """Derive the reported fields from the polls. Nothing is stored that the
    polls do not say."""
    s = node["session"]
    polls = node.get("polls") or []
    first_seen = node.get("first_available_time")
    close_at = _et_instant(s, SESSION_CLOSE_ET)
    snap_at = _et_instant(s, SNAPSHOT_ET)
    out = dict(node)
    out.update({
        "session_close_at": close_at,
        "snapshot_at": snap_at,
        "polls_recorded": len(polls),
        "first_query_time": node.get("first_query_time"),
        "first_available_time": first_seen,
        "available": bool(first_seen),
        "publication_delay_seconds": None,
        "publication_delay_hours": None,
        "resolution_seconds": None,
        "measured_delay_is_an_upper_bound": True,
        "why_an_upper_bound": (
            "the reported delay runs to the first poll that saw the data, not "
            "to the instant the vendor published it; the true latency is at "
            "most this and may be shorter"),
    })
    if first_seen and close_at:
        a, b = _as_utc(first_seen), _as_utc(close_at)
        if a and b:
            out["publication_delay_seconds"] = (a - b).total_seconds()
            out["publication_delay_hours"] = round((a - b).total_seconds() / 3600.0, 3)
    # The gap to the last NEGATIVE poll is how precisely this was pinned down.
    if first_seen:
        prior = [p for p in polls
                 if p.get("available") is False
                 and str(p.get("queried_at") or "") < str(first_seen)]
        if prior:
            a, b = _as_utc(first_seen), _as_utc(prior[-1]["queried_at"])
            if a and b:
                out["resolution_seconds"] = (a - b).total_seconds()
                out["last_negative_poll_at"] = prior[-1]["queried_at"]
    # A session whose FIRST poll was already positive was never seen to be
    # absent, so the elapsed time says only when someone got round to asking.
    # Reporting it as a latency would manufacture a measurement out of a
    # scheduling accident, so the number is withheld rather than qualified.
    out["delay_is_bounded"] = bool(first_seen and out.get("resolution_seconds")
                                   is not None)
    if first_seen and out["resolution_seconds"] is None:
        out["publication_delay_seconds"] = None
        out["publication_delay_hours"] = None
        out["delay_unbounded_reason"] = (
            "the first poll for this session was already positive, so the "
            "session was never observed to be absent; the elapsed time since "
            "the close measures when this probe started asking, not when the "
            "vendor published. Poll from BEFORE the close to bound it.")
    out["state"] = (PUBLISHED if first_seen else
                    (NOT_PUBLISHED if polls else "NOT_PROBED"))
    return out


def session_state(session, *, next_open_at: Optional[str] = None) -> dict:
    """The reported answer for ONE session, in the operator's own vocabulary."""
    s = str(session)[:10]
    node = _load()["sessions"].get(s)
    if not node:
        return {"session": s, "state": "NOT_PROBED", "available": None,
                "first_query_time": None, "first_available_time": None,
                "publication_delay_seconds": None,
                "available_before_next_open": None,
                "next_open_at": next_open_at or _next_open_instant(s),
                "session_close_at": _et_instant(s, SESSION_CLOSE_ET),
                "polls_recorded": 0,
                "owner": CALCULATION_OWNER,
                "why": "no poll has been recorded for this session; a "
                       "publication latency is measured, never assumed"}
    out = dict(node)
    out["owner"] = CALCULATION_OWNER
    deadline = next_open_at or _next_open_instant(s)
    out["next_open_at"] = deadline
    if out.get("first_available_time") and deadline:
        a, b = _as_utc(out["first_available_time"]), _as_utc(deadline)
        out["available_before_next_open"] = bool(a and b and a < b)
    else:
        out["available_before_next_open"] = None
    return out


def _next_open_instant(session) -> Optional[str]:
    """The 09:30 ET open of the next eligible session - the deadline the whole
    next-open design has to beat. The calendar and the boundary both come from
    the challenger that declares them."""
    from . import next_open_challenger as NOC

    nxt = NOC.next_eligible_session(session, 1)
    if not nxt:
        return None
    return _et_instant(nxt, NOC.ENTRY_MARK_ET)


def report(session) -> dict:
    """Exactly the five fields the brief asks for, plus their honesty caveats."""
    st = session_state(session)
    return {
        "SESSION": st.get("session"),
        "FIRST_QUERY_TIME": st.get("first_query_time"),
        "FIRST_AVAILABLE_TIME": st.get("first_available_time"),
        "PUBLICATION_DELAY": (
            "at most %.2f h after the 16:00 ET close"
            % st["publication_delay_hours"]
            if st.get("publication_delay_hours") is not None else
            ("UNBOUNDED - the first poll was already positive"
             if st.get("delay_unbounded_reason") else None)),
        "AVAILABLE_BEFORE_NEXT_OPEN": (
            "YES" if st.get("available_before_next_open") is True else
            ("NO" if st.get("available_before_next_open") is False else None)),
        "state": st.get("state"),
        "polls_recorded": st.get("polls_recorded", 0),
        "measured_delay_is_an_upper_bound": True,
        "resolution_seconds": st.get("resolution_seconds"),
        "next_open_at": st.get("next_open_at"),
        "owner": CALCULATION_OWNER,
    }


def build(sessions=None, *, write: bool = True) -> dict:
    """The whole probe record, for an artifact or a read model."""
    body = _load()
    rows = {s: _summarise(n) for s, n in (body.get("sessions") or {}).items()}
    out = {
        "schema": "alpha_recovery_opra_publication_probe/1",
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": now_iso(),
        "question": "when does the HISTORICAL %s / %s feed expose a session's "
                    "15:45 ET snapshot, measured on a weekday" % (DATASET, SCHEMA),
        "withdrawn_claim": (
            "R62.3's 'measured during a live session' dataset-range reading was "
            "taken on Saturday 2026-09-12 and is WITHDRAWN; it demonstrated "
            "nothing about publication latency"),
        "endpoints_used": list(ENDPOINTS_USED),
        "downloads_performed": 0,
        "paid_usd": 0.0,
        "sessions": rows,
        "reports": {s: report(s) for s in (sessions or sorted(rows))},
        "safety": dict(SAFETY),
    }
    if write:
        write_artifact(ARTIFACT_NAME.replace(".json", "_report.json"), out)
    return out


__all__ = [
    "CALCULATION_OWNER", "ARTIFACT_NAME", "DATASET", "SCHEMA",
    "ENDPOINTS_USED", "POLL_OUTCOMES", "PUBLISHED", "NOT_PUBLISHED",
    "RANGE_COVERS_BUT_NO_BYTES", "BLOCKED_CREDENTIAL", "PROVIDER_ERROR",
    "SAFETY", "confirm_symbols", "poll", "record", "session_state", "report",
    "build",
]
