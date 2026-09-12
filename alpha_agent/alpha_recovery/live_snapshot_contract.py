r"""alpha_agent.alpha_recovery.live_snapshot_contract - what a LIVE OPRA feed
must deliver so the FROZEN rule can evaluate its own session, and what that feed
owner is forbidden to do.

WHY THIS EXISTS AND WHY IT IS NOT AN ADAPTER
    The frozen construction forms its decision from the 15:45 ET option snapshot
    on session ``t`` and enters at the 16:00 ET mark on the SAME session. The
    historical API cannot serve that: ``metadata.get_dataset_range`` for
    ``OPRA.PILLAR`` reports an exact midnight-UTC boundary while the session is
    open, i.e. session ``t`` is published only after ``t`` ends. The fifteen
    minutes the rule needs are therefore unreachable from the owned path, and no
    change to this repository can reach them.

    The missing capability is an ENTITLEMENT, not code. So this module declares
    the contract instead of implementing a client: it names exactly what must
    arrive, in what shape, for which symbols, at which instant, and what the
    supplier may never do. It contains NO socket, NO client and NO credential.
    Writing an adapter now and pointing it at historical files would produce a
    same-session decision that was actually formed from data published after the
    session closed - the precise dishonesty the emission window exists to stop.

WHAT WAS MEASURED RATHER THAN ASSUMED
    The live gateway was asked directly, with authentication only and no
    subscription, so no market data was requested and nothing was billable. It
    answered that the credential is recognised and the LICENSE is absent. That
    distinction is the whole finding: nothing is broken and nothing is
    misconfigured; the account simply does not hold a live OPRA license.

RESEARCH ONLY. Declares; never acquires. No network, no order, no fill, no
promotion, no capital, no registration.
"""
from __future__ import annotations

from datetime import date, timedelta

from . import options_acquisition as OA
from . import options_surface as OS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.live_snapshot_contract"

# ------------------------------------------------------------------ entitlement
#: Measured by authenticating to the live gateway and closing WITHOUT
#: subscribing. Recorded verbatim so a later reader can tell a missing license
#: apart from a bad credential, a network fault or a protocol change.
ENTITLEMENT_PROBE = {
    "probed_on": "2026-09-12",
    "method": "CRAM authentication only; no subscription request was ever sent",
    "billable": False,
    "gateways": {
        "OPRA.PILLAR": {
            "host": "opra-pillar.lsg.databento.com:13000",
            "greeting": "lsg_version=0.9.4",
            "response": "success=0|error=A live data license is required to "
                        "access OPRA.PILLAR.",
            "entitled": False,
        },
        "EQUS.SUMMARY": {
            "host": "equs-summary.lsg.databento.com:13000",
            "greeting": "lsg_version=0.9.4",
            "response": "success=0|error=A live data license is required to "
                        "access EQUS.SUMMARY.",
            "entitled": False,
        },
    },
    "credential_was_rejected": False,
    "what_it_proves": (
        "the gateway completed the handshake and answered a LICENSE error, not "
        "an authentication error, so the key is recognised and only the live "
        "entitlement is missing. Both the option leg and the underlying leg are "
        "unlicensed, so a live snapshot needs BOTH."),
}

# ------------------------------------------------------------------- the feed
#: Taken FROM the acquisition owner. Naming them again here would create a
#: second spelling that could drift away from the surface the sign was found on.
REQUIRED_DATASET = OA.DATASET
MINIMUM_SCHEMA = OA.SCHEMA
SNAPSHOT_ET = OA.SNAPSHOT_ET

#: The columns ``options_acquisition.build_surface`` actually reads. Anything
#: else the feed carries is surplus and must be discarded rather than stored.
REQUIRED_FIELDS = ("ts_recv", "instrument_id",
                   "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00")

#: Why this schema and not a richer one. The numbers are ``metadata.get_cost``
#: and ``metadata.get_billable_size`` quotes for ONE session of the two-expiry
#: band - free calls, priced before anything was ever downloaded.
SCHEMA_CHOICE = {
    "minimum_sufficient": {
        "schema": "cbbo-1m",
        "why": "it IS the schema the frozen surface was built from, so the "
               "record structure, the null sentinel and the price scale are "
               "already the ones build_surface normalises",
        "bytes_one_session_116_symbols": 3_580_080,
        "usd_one_session_at_historical_rates": 0.006668,
    },
    "acceptable_superset": {
        "schema": "cbbo-1s",
        "why": "the same consolidated BBO at a finer cadence; the frozen rule "
               "takes the LAST record at or before the snapshot minute, so a "
               "finer grid answers the same question. 32x the data for no extra "
               "information.",
        "bytes_one_session_116_symbols": 114_967_440,
        "usd_one_session_at_historical_rates": 0.214144,
    },
    "rejected": {
        "cmbp-1": {
            "why": "every consolidated book update. 212x the bytes of cbbo-1m "
                   "and the extra updates are discarded by the snapshot rule.",
            "bytes_one_session_116_symbols": 758_460_400,
        },
        "trades": {
            "why": "most strikes in the band do not trade on most days, so a "
                   "trade feed cannot price a quote midpoint at a fixed instant",
        },
        "bbo-1m / mbp-1 / tbbo": {
            "why": "not offered on OPRA.PILLAR at all; the venue refused each "
                   "with dataset_schema_not_supported",
        },
    },
}

# -------------------------------------------------------------- the underlying
#: The one place where a live snapshot CANNOT simply repeat the historical
#: construction, and the resolution that leaves the signal untouched.
UNDERLYING_MARK = {
    "the_problem": (
        "build_surface drops any date with no underlying close, and the frozen "
        "spot is the 16:00 session close - fifteen minutes AFTER the declared "
        "information cutoff. A same-session row built the historical way would "
        "carry a mark the decision was not allowed to see."),
    "the_resolution": (
        "supply the underlying mark AT the 15:45 snapshot instant, from the "
        "same consolidated snapshot as the options, and record the instant "
        "alongside it so no reader mistakes it for a session close."),
    "why_it_cannot_change_the_signal": (
        "the frozen feature reads only implied volatility and FORWARD moneyness, "
        "and the forward comes from put-call parity inside the snapshot itself. "
        "Measured on the owned 503-session surface: shifting underlying_close by "
        "+3% leaves skew, atm_iv_near, atm_iv_far, term_slope and the "
        "60-observation z-score EXACTLY identical. Only rv21 and vrp move, and "
        "the frozen rule reads neither."),
    "forbidden": (
        "using the 16:00 close of the decision session as the mark. It is "
        "information after the declared cutoff, and the emission window exists "
        "to refuse exactly that."),
}

# --------------------------------------------------------------- prohibitions
#: The supplier delivers INFORMATION. Every decision remains where it already
#: lives, and this list is the boundary.
PROHIBITIONS = {
    "must_not_change_the_signal": (
        "no alternate schema mapping, no alternate snapshot minute, no "
        "alternate band, no alternate expiry choice, no re-derived implied "
        "volatility. It writes the same columns build_surface writes."),
    "must_not_choose_a_position": (
        "the sign, the horizon and the z-score are the research producer's; "
        "the feed owner never evaluates them."),
    "must_not_promote_anything": "it has no path to the registrar.",
    "must_not_allocate_capital": "it has no path to a book or a NAV.",
    "must_not_create_an_order_or_a_fill": "it has no path to an execution owner.",
    "must_not_backfill_a_missed_session": (
        "a session whose 15:45 snapshot was not captured inside its own window "
        "stays missing for ever. Capturing it later from the historical API "
        "would manufacture a decision the rule was never entitled to make."),
    "must_not_overwrite_history": (
        "append only. The discovery sample is the evidence a sign was chosen "
        "on; a rebuild would silently move it."),
}

#: Ordered, bounded, and all that remains once a license exists.
REMAINING_IMPLEMENTATION = (
    "1. Subscribe to REQUIRED_DATASET / MINIMUM_SCHEMA for symbols_for(session) "
    "a few minutes before the snapshot minute, take the last record per "
    "instrument at or before it, and disconnect.",
    "2. Map instrument_id -> OSI symbol from the gateway's own symbol mapping "
    "messages (the live feed supplies what symbology.resolve supplies "
    "historically).",
    "3. Write the snapshot rows through the EXISTING normalisation - the same "
    "parity forward, the same inversion, the same columns - appending one "
    "session to the owned surface.",
    "4. Supply the underlying mark per UNDERLYING_MARK.",
    "5. Run the existing freeze entrypoint inside the declared window.",
)

#: Steps 1-4 are the only new code. Nothing in step 5 changes: the producer, the
#: emission window, the resolver and the evidence kernel are already built and
#: tested, and none of them is touched by a license arriving.
IMPLEMENTATION_IS_BOUNDED_TO = ("a subscription client", "a symbol mapper",
                                "an append of one session", "an underlying mark")


# ----------------------------------------------------------------- the symbols
def expiries_for(session: str) -> list:
    """The monthly expiries the FROZEN construction has live on ``session``.

    The historical plan buys each expiry over the ``LOOKBACK_DAYS`` before it, so
    a date carries every monthly expiry that has not yet expired and is no more
    than that far away. Reproducing the same set is what makes a live snapshot
    the same observation rather than a similar one.
    """
    d = date.fromisoformat(session)
    out = set()
    y, m = d.year, d.month
    for _ in range(8):
        e = OA.third_friday(y, m)
        if d <= e <= d + timedelta(days=OA.LOOKBACK_DAYS):
            out.add(e)
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return sorted(out)


def near_and_far(session: str) -> dict:
    """Which of those expiries the feature owner will read.

    ``near`` is the first expiry far enough out to BE a near leg, which is why a
    live capture that bought only the front monthly would produce a NaN feature
    on exactly the sessions closest to an expiry.
    """
    d = date.fromisoformat(session)
    exps = expiries_for(session)
    near = None
    for e in exps:
        if (e - d).days / 365.25 >= OS.MIN_T_YEARS_NEAR:
            near = e
            break
    return {"session": session,
            "expiries": [e.isoformat() for e in exps],
            "near": near.isoformat() if near else None,
            "far": exps[-1].isoformat() if exps else None,
            "min_t_years_near": OS.MIN_T_YEARS_NEAR}


def symbols_for(session: str, underlying_level: float) -> dict:
    """``expiry -> the OSI symbols to subscribe to``.

    The band, the spacing and the symbol spelling all come from the acquisition
    owner. ``underlying_level`` only CENTRES a band 20 % wide, so an approximate
    level is sufficient - which is why the owned ES panel has always been enough
    to decide what to buy.
    """
    return {e.isoformat(): [OA.osi(e, r, k)
                            for k in OA.band_for(underlying_level)
                            for r in ("C", "P")]
            for e in expiries_for(session)}


# ------------------------------------------------------------------- the state
def blocking_state() -> dict:
    """Why the frozen rule cannot evaluate its own session today."""
    return {
        "state": "BLOCKED_ON_LIVE_DATA_ENTITLEMENT",
        "entitled": False,
        "historical_api_cannot_serve_the_window": (
            "OPRA.PILLAR publishes session t only after t ends, so the 15:45 "
            "snapshot is unavailable for the fifteen minutes the rule needs it"),
        "live_api_refuses": ENTITLEMENT_PROBE["gateways"]["OPRA.PILLAR"]["response"],
        "what_is_NOT_wrong": (
            "the rule, the sign, the horizon, the emission window, the signed "
            "evidence kernel and the decision producer are all implemented and "
            "tested. Only the information is missing."),
        "a_missed_session_stays_missed": True,
    }


def contract() -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "dataset": REQUIRED_DATASET,
        "minimum_schema": MINIMUM_SCHEMA,
        "snapshot_et": "%02d:%02d" % SNAPSHOT_ET,
        "required_fields": list(REQUIRED_FIELDS),
        "schema_choice": SCHEMA_CHOICE,
        "underlying_mark": UNDERLYING_MARK,
        "prohibitions": PROHIBITIONS,
        "remaining_implementation": list(REMAINING_IMPLEMENTATION),
        "bounded_to": list(IMPLEMENTATION_IS_BOUNDED_TO),
        "entitlement": ENTITLEMENT_PROBE,
        "state": blocking_state(),
    }
