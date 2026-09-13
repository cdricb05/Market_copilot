r"""alpha_agent/alpha_recovery/next_open_runtime.py - THE daily advance owner
for ``REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1``.

ONE idempotent call that the canonical research runtime can own, so the
challenger's forward evidence does not depend on a person being at a keyboard
inside a particular nine-and-a-half-hour window.

    probe the vendor  ->  append the session  ->  freeze ONE decision

Each step is separately idempotent and separately refusable, and the call
reports which one it stopped at. A trigger that fires when there is nothing to
do is a no-op; a trigger that fires five times inside the window produces one
decision, because the freeze is first-write-wins in
:mod:`alpha_agent.alpha_recovery.prospective_decision`.

WHY THIS IS NOT THE CHALLENGER MODULE. :mod:`next_open_challenger` owns the
IDENTITY - the frozen specification, the session arithmetic, the state
vocabulary - and is read by the accrual owner. This module owns the ACT of
advancing a day, which needs a network, a budget and a clock. Keeping them
apart is what lets the identity be recomputed anywhere, by anyone, with no
credential.

WHAT IT MAY NOT DO. No order, no fill, no proposal, no promotion, no capital,
no registration, no operational-store write, and no backfill: a session whose
window shut without a decision is MISSED permanently. It never rebuilds the
discovery sample - the surface is APPENDED to, and on a key collision the
ORIGINAL row wins, so no row the sign was discovered on can move.

PAID DOLLARS = 0. The publication probe is two free metadata calls. The append
is bounded by ``BUDGET_USD`` and additionally refused by
:mod:`databento_acquisition` unless it fits the free credit already held.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from . import now_iso, stable_hash
from . import next_open_challenger as NOC
from . import opra_publication_probe as PP
from . import options_acquisition as OA
from . import options_surface as OS
from . import prospective_decision as PD
from . import reversed_skew as RS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.next_open_runtime"

#: The free-credit ceiling ONE session's append may price against. A single
#: session of one near-expiry band costs a fraction of a cent; this is a cap,
#: never a target, and the acquisition owner refuses anything that does not fit
#: the free credit regardless of what is written here.
BUDGET_USD = 0.25

#: The corrected spot series the surface is dated and marked against. These are
#: the SAME constants the one-off catch-up used, read from the acquisition
#: owner rather than retyped, so a daily append and that catch-up cannot drift.
SPOT_TAG = OA.SPOT_CORRECTED_TAG
SPOT_DATASET = OA.SPOT_DATASET_PRE_2024
SPOT_SCHEMA = OA.SPOT_SCHEMA_INTRADAY
SPOT_START = "2022-09-09"
OUT_TAG = OA.SPOT_CORRECTED_TAG

#: Outcome of the APPEND step alone.
APPEND_ALREADY_OWNED = "ALREADY_OWNED"
APPEND_NOT_PUBLISHED = "VENDOR_HAS_NOT_PUBLISHED_THE_SESSION"
APPEND_PRICED_NOT_EXECUTED = "PRICED_NOT_EXECUTED"
APPEND_REFUSED_BUDGET = "REFUSED_EXCEEDS_AUTHORISED_FREE_CREDIT"
APPEND_BLOCKED_CREDENTIAL = "BLOCKED_CREDENTIAL_ABSENT"
APPEND_APPENDED = "APPENDED"
APPEND_NO_ROWS = "BUILT_NO_ROWS"

#: Outcome of the WHOLE daily advance. Exactly one is returned per call.
ADV_FROZEN = "NEXT_OPEN_FROZEN"
ADV_ALREADY_FROZEN = "NEXT_OPEN_ALREADY_FROZEN"
ADV_AWAITING_INFORMATION = "NEXT_OPEN_AWAITING_INFORMATION_SESSION"
ADV_AWAITING_SOURCE = "NEXT_OPEN_AWAITING_SOURCE_PUBLICATION"
ADV_AWAITING_WINDOW = "NEXT_OPEN_AWAITING_DECISION_WINDOW"
ADV_MISSED = "NEXT_OPEN_MISSED"
ADV_NOTHING_DUE = "NEXT_OPEN_NOTHING_DUE"
ADV_BEFORE_INCEPTION = "NEXT_OPEN_BEFORE_FIRST_LEGAL_ENTRY"
ADV_BLOCKED = "NEXT_OPEN_BLOCKED"
ADVANCE_STATES = (ADV_FROZEN, ADV_ALREADY_FROZEN, ADV_AWAITING_INFORMATION,
                  ADV_AWAITING_SOURCE, ADV_AWAITING_WINDOW, ADV_MISSED,
                  ADV_NOTHING_DUE, ADV_BEFORE_INCEPTION, ADV_BLOCKED)

#: The states on which a scheduler has genuinely changed something.
PROGRESS_STATES = (ADV_FROZEN,)


# --------------------------------------------------------------------------- #
# 1. WHICH SESSION IS IN PLAY RIGHT NOW
# --------------------------------------------------------------------------- #
def current_information_session(now: Optional[str] = None,
                                *, lookback_days: int = 12) -> Optional[str]:
    """The most recent eligible session whose 15:45 ET snapshot now exists.

    Derived from the clock and the exchange calendar, never passed in, because
    a scheduler that had to be told which session it was would be a scheduler
    that could be told the wrong one. Walking BACK from today also means a
    runtime that was down for a weekend resumes on the right session instead of
    on the session it last saw.
    """
    ts = NOC._as_utc(now or now_iso())
    if ts is None:
        return None
    today = ts.date()
    for back in range(int(lookback_days) + 1):
        d = (today - timedelta(days=back)).isoformat()
        if not NOC.is_eligible_session(d):
            continue
        cutoff = NOC._as_utc(NOC._et_instant(d, NOC.INFORMATION_CUTOFF_ET))
        if cutoff is not None and ts >= cutoff:
            return d
    return None


def first_legal_information_session() -> Optional[str]:
    """The earliest session this challenger may READ, on its own inception.

    Strictly AFTER inception, because a registration cannot make itself
    observable on the session it was made on - which is the same rule the
    canonical registrar applies when it resolves a first eligible observation
    session, and it is derived here from the challenger's own frozen inception
    so the two cannot drift without a test noticing.
    """
    return NOC.next_eligible_session(NOC.INCEPTION)


def first_legal_entry_session() -> Optional[str]:
    """The earliest session this challenger may ENTER on.

    One eligible session after the earliest it may read, because it enters at
    the NEXT open. The 2026-09-11 snapshot is already owned and would otherwise
    let a decision be frozen for the 2026-09-14 open - a session whose
    information predates the registration, and one the canonical accrual grid
    does not contain, so the decision would be an orphan nothing ever scored.
    """
    first_info = first_legal_information_session()
    return NOC.entry_session_for(first_info) if first_info else None


def _surface_last_session(surface=None) -> Optional[str]:
    p = surface or OS.surface_path()
    try:
        f = OS.features(rebuild=True, surface=p)
    except Exception:                                       # noqa: BLE001
        return None
    if f is None or not len(f):
        return None
    return str(f["date"].iloc[-1])[:10]


def _near_leg_expiries(sessions: list) -> set:
    """The expiries that serve as the NEAR leg on some session in ``sessions``.

    ``skew`` - the only field this challenger scores - is computed on the near
    expiry alone, and an expiry qualifies as near only at ``MIN_T_YEARS_NEAR``
    or more to expiry. So the set is DERIVED from the frozen constant applied to
    the calendar, never typed in, and buying only these can only reduce spend.
    """
    cands = sorted({OA.third_friday(y, m)
                    for s in sessions
                    for y in (date.fromisoformat(s).year,
                              date.fromisoformat(s).year + 1)
                    for m in range(1, 13)})
    out = set()
    for s in sessions:
        d = date.fromisoformat(s)
        near = next((e for e in cands
                     if (e - d).days / 365.25 >= OS.MIN_T_YEARS_NEAR), None)
        if near is not None:
            out.add(near.isoformat())
    return out


# --------------------------------------------------------------------------- #
# 2. THE APPEND - bounded, priced first, and append-only by construction
# --------------------------------------------------------------------------- #
def append_information_session(information_session: str, *,
                               budget_usd: float = BUDGET_USD,
                               execute: bool = False,
                               client=None,
                               surface=None) -> dict:
    """Put ONE session's 15:45 ET snapshot into the owned surface, or say why not.

    Everything is DERIVED: the gap starts at the session after the one the
    surface already ends on, and the expiry scope is the near-leg set those gap
    sessions actually need. Nothing about a particular month is written down
    here, which is what makes this safe to run every day rather than once.
    """
    from . import databento_acquisition as DA

    info = str(information_session)[:10]
    out = {"calculation_owner": CALCULATION_OWNER,
           "information_session": info, "executed": bool(execute),
           "paid_dollars": 0.0, "budget_usd": float(budget_usd)}

    last = _surface_last_session(surface)
    out["surface_ends"] = last
    if last is not None and info <= last:
        return {**out, "state": APPEND_ALREADY_OWNED,
                "detail": "the owned surface already holds %s" % info}

    cred = DA.credential_state()
    if not cred.get("usable"):
        return {**out, "state": APPEND_BLOCKED_CREDENTIAL,
                "detail": "no usable Databento credential; nothing was spent"}

    client = client or DA.Client()
    rng = client.dataset_range(OA.DATASET)
    request_end = str(((rng.get("schema") or {}).get(OA.SCHEMA) or rng)
                      .get("end") or rng.get("end"))[:10]
    covered_through = DA._available_end(rng, OA.SCHEMA)
    out["venue_exclusive_end"] = request_end
    out["venue_covers_through"] = covered_through
    if not covered_through or str(covered_through)[:10] < info:
        return {**out, "state": APPEND_NOT_PUBLISHED,
                "detail": ("the venue serves through %s, which does not reach "
                           "%s" % (covered_through, info))}

    gap_start = NOC.next_eligible_session(last) if last else info
    gap_start = min(str(gap_start or info), info)
    gap = [s for s in _eligible_range(gap_start, info)]
    out["gap_start"] = gap_start
    out["gap_sessions"] = gap
    if not gap:
        return {**out, "state": APPEND_ALREADY_OWNED,
                "detail": "no eligible session lies between the surface end "
                          "and %s" % info}

    needed = _near_leg_expiries(gap)
    scope_end = max(needed) if needed else info
    out["near_leg_expiries"] = sorted(needed)

    plan = OA.plan(client, budget_usd=float(budget_usd),
                   window=(gap_start, scope_end), available_end=request_end)
    dropped = [r for r in plan["requests"] if r["expiry"] not in needed]
    plan["requests"] = [r for r in plan["requests"] if r["expiry"] in needed]
    band_usd = round(sum(r["cost_usd"] for r in plan["requests"]), 6)
    cap = float(budget_usd) * (1.0 - DA.BUDGET_SAFETY_MARGIN)
    plan["selection"]["estimated_spend_usd"] = band_usd
    plan["selection"]["fits_in_free_credit"] = band_usd <= cap
    out.update({"band_cost_usd": band_usd, "effective_cap_usd": round(cap, 6),
                "expiries_not_bought": [r["expiry"] for r in dropped],
                "n_requests": len(plan["requests"])})

    spot = OA.acquire_spot(client, SPOT_START, request_end, execute=False,
                           tag=SPOT_TAG, dataset=SPOT_DATASET,
                           schema=SPOT_SCHEMA)
    spot_reused = spot.get("state") == "REUSED"
    spot_usd = 0.0 if spot_reused else float(spot.get("cost_usd") or 0.0)
    if spot_reused:
        have = OA.session_closes(tag=SPOT_TAG, dataset=SPOT_DATASET,
                                 schema=SPOT_SCHEMA)
        covers = max(have) if have else None
        out["spot_reused_covers_through"] = covers
        if covers is None or str(covers)[:10] < info:
            # A reused file is only usable if it REACHES the session. The reuse
            # check is existence, not coverage, so a short cache is replaced
            # rather than discovered later as missing rows.
            stale = OA.spot_path(SPOT_TAG, SPOT_DATASET, SPOT_SCHEMA)
            out["stale_spot_replaced"] = str(stale)
            try:
                if execute and stale.exists():
                    stale.unlink()
            except OSError:
                pass
            spot = OA.acquire_spot(client, SPOT_START, request_end,
                                   execute=False, tag=SPOT_TAG,
                                   dataset=SPOT_DATASET, schema=SPOT_SCHEMA)
            spot_reused = False
            spot_usd = float(spot.get("cost_usd") or 0.0)
    total = round(band_usd + spot_usd, 6)
    out.update({"spot_cost_usd": spot_usd, "total_priced_usd": total,
                "spot_reused": spot_reused})

    if total > cap:
        return {**out, "state": APPEND_REFUSED_BUDGET,
                "detail": ("priced $%.6f against an effective cap of $%.6f; "
                           "nothing was downloaded" % (total, cap))}
    if not execute:
        return {**out, "state": APPEND_PRICED_NOT_EXECUTED,
                "detail": "priced only; nothing was downloaded"}

    got = DA.download(client, plan, out_root=OA.data_root(None), dry_run=False,
                      schema=OA.SCHEMA, dataset=OA.DATASET)
    out["bands"] = {"state": got.get("state"),
                    "n_written": len(got.get("written") or []),
                    "failed": got.get("failed")}
    if not spot_reused:
        s = OA.acquire_spot(client, SPOT_START, request_end, execute=True,
                            tag=SPOT_TAG, dataset=SPOT_DATASET,
                            schema=SPOT_SCHEMA)
        out["spot"] = {"state": s.get("state"), "path": str(s.get("path"))}
    out["spent_estimate_usd"] = round(
        float(got.get("spent_estimate_usd") or 0.0) + spot_usd, 6)

    # The gap-only build, then an APPEND that cannot alter an existing row.
    catchup_tag = "daily_%s" % info.replace("-", "")
    built = OA.build_surface(client, window=(gap_start, scope_end),
                             available_end=request_end, tag=None,
                             spot_tag=SPOT_TAG, spot_dataset=SPOT_DATASET,
                             spot_schema=SPOT_SCHEMA, out_tag=catchup_tag)
    out["built"] = {k: v for k, v in built.items()
                    if k in ("state", "path", "skipped")}
    if built.get("state") == "NO_ROWS":
        return {**out, "state": APPEND_NO_ROWS,
                "detail": "the bands on disk yielded no snapshot row"}
    out["extend"] = extend_surface(catchup_tag)
    out["surface_ends_after"] = _surface_last_session(surface)
    return {**out, "state": APPEND_APPENDED}


def _eligible_range(start: str, end: str) -> list:
    """Every eligible session from ``start`` to ``end`` inclusive."""
    out, cur, guard = [], str(start)[:10], 0
    while cur and cur <= str(end)[:10] and guard < 400:
        guard += 1
        if NOC.is_eligible_session(cur):
            out.append(cur)
        nxt = (date.fromisoformat(cur) + timedelta(days=1)).isoformat()
        cur = nxt
    return out


def extend_surface(catchup_tag: str) -> dict:
    """Append the freshly built rows to the frozen surface. Never rewrites one.

    The existing file is archived first, under a name that says what it covers,
    because it is the artifact the independent historical confirmation was
    computed on. On a key collision the ORIGINAL row wins, so this operation
    cannot alter a single date the discovery sample already held.
    """
    import pandas as pd

    frozen_p = OA.surface_path(OUT_TAG)
    catch_p = OA.surface_path(catchup_tag)
    frozen = pd.read_csv(frozen_p)
    catch = pd.read_csv(catch_p)
    before = sorted(frozen["date"].astype(str).unique())
    new_dates = sorted(set(catch["date"].astype(str)) - set(before))

    archive = frozen_p.with_name(
        frozen_p.name.replace(".csv.gz", "_through_%s.csv.gz"
                              % before[-1].replace("-", "")))
    if not archive.exists():
        archive.write_bytes(frozen_p.read_bytes())

    key = ["date", "expiration", "type", "strike"]
    combined = (pd.concat([frozen, catch], ignore_index=True)
                  .drop_duplicates(subset=key, keep="first")
                  .sort_values(key).reset_index(drop=True))
    combined.to_csv(frozen_p, index=False, compression="gzip")
    after = sorted(combined["date"].astype(str).unique())
    return {"archived_original": str(archive),
            "frozen_dates_before": len(before),
            "dates_after": len(after),
            "new_dates_appended": len(new_dates),
            "first": after[0], "last": after[-1],
            "discovery_rows_unchanged": len(frozen) == int(
                (combined["date"].astype(str) <= before[-1]).sum())}


# --------------------------------------------------------------------------- #
# 3. THE FREEZE - ONE decision, computed by the frozen rule, first-write-wins
# --------------------------------------------------------------------------- #
def freeze_for_entry(entry_session: str, *, now: Optional[str] = None,
                     observed_at: Optional[str] = None,
                     surface=None) -> dict:
    """Freeze the ONE immutable decision for ``entry_session``.

    The rule is READ from :mod:`reversed_skew` - the sign, the field and the
    z-score lookback are the sibling's frozen construction and are not restated
    here - and evaluated on the INFORMATION session only. The decision is keyed
    on the ENTRY session because that is the session its window, its mark and
    its horizon are all measured on.
    """
    import numpy as np
    from pathlib import Path

    ctx = NOC.decision_context(entry_session)
    info = ctx.get("information_session")
    out = {"calculation_owner": CALCULATION_OWNER, "decision_context": ctx,
           "entry_session": entry_session, "information_session": info}

    if not NOC.is_eligible_session(entry_session):
        return {**out, "frozen": False, "outcome": "REFUSED_NOT_A_SESSION"}
    if not info:
        return {**out, "frozen": False, "outcome": "REFUSED_NO_INFORMATION_SESSION"}

    src = NOC.source_available(info, surface=surface)
    out["source"] = src
    if not src.get("usable_for_a_freeze"):
        return {**out, "frozen": False,
                "outcome": NOC.REFUSED_SOURCE_NOT_PUBLISHED}

    sp = RS.spec()
    surface_p = surface or OS.surface_path()
    f = OS.features(rebuild=True, surface=surface_p)
    dates = [str(x)[:10] for x in f["date"]]
    if info not in dates:
        return {**out, "frozen": False,
                "outcome": NOC.REFUSED_SOURCE_NOT_PUBLISHED,
                "detail": "the owned surface holds no observation for %s" % info}
    i = dates.index(info)
    z = OS._z(f[sp["field"]])
    zi = z[i]
    if not np.isfinite(zi):
        return {**out, "frozen": False, "outcome": "REFUSED_ZSCORE_NOT_FINITE",
                "detail": ("the strictly-prior %d-observation window is "
                           "incomplete at %s" % (OS.ZSCORE_LOOKBACK, info))}
    position = float(sp["sign"]) * float(np.sign(zi))
    if position == 0.0:
        return {**out, "frozen": False, "outcome": "REFUSED_ZERO_POSITION"}

    observed = observed_at or ctx.get("information_cutoff_at")
    chk = NOC.check_information_instant(entry_session, observed)
    if chk.get("violates"):
        return {**out, "frozen": False, "outcome": NOC.REFUSED_STALE_INPUT,
                "detail": chk.get("reason")}

    src_path = Path(str(surface_p))
    source_data_hash = stable_hash({
        "surface": str(src_path), "bytes": src_path.stat().st_size,
        "observations": len(dates), "last_observation": dates[-1]})
    feature_state_hash = stable_hash({
        "field": sp["field"], "session": info,
        "value": float(f[sp["field"]].to_numpy()[i]),
        "z": float(zi), "lookback": OS.ZSCORE_LOOKBACK,
        "prior_window": dates[max(0, i - OS.ZSCORE_LOOKBACK):i]})

    res = PD.freeze_decision(
        challenger_id=NOC.CHALLENGER_ID, eligible_session=entry_session,
        weights={OA.UNDERLYING: position}, source_data_hash=source_data_hash,
        feature_state_hash=feature_state_hash, feature_observed_at=observed,
        now=now, context=ctx)
    return {**out, "frozen": bool(res.get("frozen")),
            "outcome": res.get("outcome"), "detail": res.get("detail"),
            "path": str(res.get("path") or ""),
            "z": float(zi), "position": position,
            "decision": res.get("decision")}


# --------------------------------------------------------------------------- #
# 4. THE DAILY ADVANCE - probe, append, freeze; idempotent end to end
# --------------------------------------------------------------------------- #
def advance_daily(*, now: Optional[str] = None,
                  information_session: Optional[str] = None,
                  probe: bool = True,
                  append: bool = True,
                  execute_append: bool = True,
                  budget_usd: float = BUDGET_USD,
                  client=None, surface=None) -> dict:
    """ONE call the runtime makes; exactly one ``state`` comes back.

    Safe at any hour and safe repeated. Outside the decision window it does the
    work that HAS to happen first - measuring publication and appending the
    session - so that when the window opens the only thing left is arithmetic.
    """
    ts = now or now_iso()
    info = str(information_session)[:10] if information_session else \
        current_information_session(ts)
    out = {"calculation_owner": CALCULATION_OWNER,
           "challenger_id": NOC.CHALLENGER_ID, "now": ts,
           "information_session": info, "paid_dollars": 0.0,
           "creates_orders": False, "creates_fills": False,
           "allocates_capital": False, "promotes_model": False,
           "backfill_allowed": False}
    if not info:
        return {**out, "state": ADV_NOTHING_DUE,
                "detail": "no eligible session has reached its 15:45 ET cutoff"}
    entry = NOC.entry_session_for(info)
    out["entry_session"] = entry
    if not entry:
        return {**out, "state": ADV_BLOCKED,
                "detail": "no eligible session follows %s" % info}

    # The prospective boundary. A decision frozen for an earlier entry would be
    # read from information that predates the registration and would sit on no
    # accrual grid, so it could never be scored - an orphan, not evidence.
    first_entry = first_legal_entry_session()
    out["first_legal_entry_session"] = first_entry
    if first_entry and entry < first_entry:
        return {**out, "state": ADV_BEFORE_INCEPTION,
                "detail": ("%s precedes the first entry session this "
                           "registration may act on (%s)" % (entry, first_entry))}

    if probe:
        try:
            res = PP.poll(info, now=ts, client=client)
            PP.record(res)
            out["publication"] = {
                "outcome": res.get("outcome"), "available": res.get("available")}
            out["publication_report"] = PP.report(info)
        except Exception as exc:                            # noqa: BLE001
            out["publication"] = {"outcome": "PROBE_ERROR",
                                  "detail": str(exc)[:200]}

    st = NOC.entry_state(entry, now=ts, surface=surface)
    out["entry_state"] = st.get("entry_state")
    out["source"] = st.get("source")

    if st["entry_state"] in (NOC.READY_FOR_ENTRY, NOC.AWAITING_MATURITY):
        return {**out, "state": ADV_ALREADY_FROZEN,
                "detail": "a decision for %s is already frozen" % entry}
    if st["entry_state"] == NOC.MISSED:
        return {**out, "state": ADV_MISSED, "backfill_refused": True,
                "detail": ("the %s open has passed with no decision; this "
                           "entry session is missed permanently" % entry)}

    if append and not (st.get("source") or {}).get("owned"):
        try:
            out["append"] = append_information_session(
                info, budget_usd=budget_usd, execute=execute_append,
                client=client, surface=surface)
        except Exception as exc:                            # noqa: BLE001
            out["append"] = {"state": "APPEND_ERROR",
                             "error": type(exc).__name__,
                             "detail": str(exc)[:220]}
        st = NOC.entry_state(entry, now=ts, surface=surface)
        out["entry_state"] = st.get("entry_state")
        out["source"] = st.get("source")

    if st["entry_state"] == NOC.AWAITING_INFORMATION_SESSION:
        return {**out, "state": ADV_AWAITING_INFORMATION}
    if st["entry_state"] == NOC.AWAITING_SOURCE_PUBLICATION:
        # Distinguish "the data is not here" from "the window is not open yet":
        # only the first is a reason to look again at the vendor.
        opens = NOC._as_utc(NOC._et_instant(entry, NOC.DECISION_WINDOW_OPENS_ET))
        nowdt = NOC._as_utc(ts)
        if (st.get("source") or {}).get("usable_for_a_freeze") and \
                opens and nowdt and nowdt < opens:
            return {**out, "state": ADV_AWAITING_WINDOW,
                    "window_opens_at": opens.isoformat(),
                    "detail": "the data is in hand; the window opens at %s"
                              % opens.isoformat()}
        return {**out, "state": ADV_AWAITING_SOURCE,
                "blocked_on": st.get("blocked_on")}

    if st["entry_state"] != NOC.DECISION_DUE_NOW:
        return {**out, "state": ADV_BLOCKED,
                "detail": "unhandled entry state %s" % st["entry_state"]}

    res = freeze_for_entry(entry, now=ts, surface=surface)
    out["freeze"] = {k: v for k, v in res.items()
                     if k in ("frozen", "outcome", "detail", "path", "z",
                              "position")}
    if not res.get("frozen"):
        return {**out, "state": ADV_BLOCKED,
                "detail": res.get("outcome")}
    return {**out, "state": ADV_FROZEN,
            "maturity_session": (res.get("decision_context") or {})
            .get("maturity_session")}


__all__ = [
    "CALCULATION_OWNER", "BUDGET_USD", "ADVANCE_STATES", "PROGRESS_STATES",
    "ADV_FROZEN", "ADV_ALREADY_FROZEN", "ADV_AWAITING_INFORMATION",
    "ADV_AWAITING_SOURCE", "ADV_AWAITING_WINDOW", "ADV_MISSED",
    "ADV_NOTHING_DUE", "ADV_BLOCKED",
    "APPEND_ALREADY_OWNED", "APPEND_NOT_PUBLISHED", "APPEND_APPENDED",
    "APPEND_PRICED_NOT_EXECUTED", "APPEND_REFUSED_BUDGET",
    "APPEND_BLOCKED_CREDENTIAL", "APPEND_NO_ROWS",
    "current_information_session", "append_information_session",
    "extend_surface", "freeze_for_entry", "advance_daily",
]
