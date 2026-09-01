r"""alpha_agent.r53.intraday_factory - the prospective INTRADAY challenger
framework: frozen specifications, a slot clock, an append-only chain-hashed
prediction ledger, forfeiture as first-class state, and an outcome-scoring
contract - built BEFORE the data feed exists, so the moment a current
intraday feed is owned the evidence clock starts instead of the engineering.

WHY THE LEDGER IS EMPTY TODAY, AND WHY THAT IS THE HONEST STATE
---------------------------------------------------------------
The canonical intraday-lane owner (:mod:`alpha_agent.r46.intraday`) was
re-probed LIVE in this release, during regular hours, with the operator's
venue key in the shell: Norgate serves daily bars by construction, the owned
venue plan answers HTTP 403 for current-session aggregates, and the acquired
minute panels (R38/R45) are frozen history. A 30-minute prediction stamped
against data that arrives after the horizon closes is not prospective - it is
the exact fabrication this estate's evidence discipline exists to prevent. So
this module freezes the SPECIFICATIONS and the machinery now, emits nothing,
and says so, loudly, in its own artifact.

WHAT IS REUSED (nothing here is a second implementation)
--------------------------------------------------------
* ledger mechanics: the canonical chain-hash primitives from
  ``api.paper_trading_desk`` - the same convention every desk and R46 ledger
  has used since Phase 27, pointed at the R53 research root.
* spec freezing: ``alpha_agent.r46.challengers.sha``-style spec hashing via
  the shared :func:`alpha_agent.r46.sha`.
* forfeiture semantics: mirrored from ``alpha_agent.r52.forfeiture`` - every
  missed window is recorded with ``backfill_refused: true`` and the append
  REFUSES anything else.
* the daily-horizon cells stay owned by the R46 ledger. This ledger holds
  ONLY sub-session horizons (30/120 minutes and same-session close), which
  the R46 intraday-lane owner explicitly declares outside the daily system's
  scope. One business concept, one owner, no overlap.

AUTHORITY BOUNDARY (Release 53)
-------------------------------
Nothing in this module carries expected-return authority over the production
portfolio. An intraday challenger's rows accrue in SHADOW; the canonical
promotion gates (``alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES``) decide
if a lane ever earns more, and a risk-only event can never become
expected-return authority by transiting this module.

RESEARCH ONLY. No orders, no fills, no promotion, no operational write.
"""
from __future__ import annotations

import datetime as _dt
from typing import Callable, Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, sha, write_json)

CALCULATION_OWNER = "alpha_agent.r53.intraday_factory"

# --------------------------------------------------------------------------- #
# Vocabulary
# --------------------------------------------------------------------------- #
TRUE_FORWARD = "TRUE_FORWARD"          # the only evidence class this ledger holds
STATUS_PENDING = "PENDING"
STATUS_SCORED = "SCORED"

LANE_AVAILABLE = "AVAILABLE_NOW"       # from alpha_agent.r46.intraday
LANE_BLOCKED = "DATA_BLOCKED"

EMIT_OK = "EMITTED"
EMIT_LANE_BLOCKED = "LANE_BLOCKED_STRUCTURAL"
EMIT_DUPLICATE = "DUPLICATE_SUPPRESSED"
EMIT_NOT_A_SLOT = "NOT_AN_EMISSION_SLOT"
EMIT_STALE_INPUT = "STALE_INPUT_REFUSED"

#: Sub-session horizons, minutes. ``CLOSE`` is the same-session close - the
#: one horizon the daily system does NOT own (its h1 cell enters at the NEXT
#: close, this one exits at the CURRENT session's close).
HORIZON_MINUTES = (30, 120)
HORIZON_CLOSE = "SESSION_CLOSE"

#: The intraday decision slots, US/Eastern wall-clock. Declared, not tuned:
#: one mid-morning slot after the open auction noise, one midday, one
#: afternoon slot that still leaves the 120-minute window inside the session.
EMISSION_SLOTS_ET = ("10:00", "12:00", "14:00")
#: Minutes after a slot during which an emission still belongs to it. A later
#: invocation records a forfeiture, never a backdated row.
SLOT_GRACE_MINUTES = 15
#: A bar older than this at emission cannot stamp a 30-minute signal.
MAX_INPUT_AGE_MINUTES = 20

#: Declared conservative cost assumption per side, basis points - the desk's
#: canonical equity rate reused verbatim (``api.paper_trading_desk``), NOT an
#: intraday-optimistic invention.
COST_BPS_PER_SIDE = 12.5

PREDICTION_LEDGER = "r53_intraday_predictions.json"
OUTCOME_LEDGER = "r53_intraday_outcomes.json"
FORFEITURE_LEDGER = "r53_intraday_forfeitures.json"
LEDGERS = (PREDICTION_LEDGER, OUTCOME_LEDGER, FORFEITURE_LEDGER)
FACTORY_ARTIFACT = "R53_INTRADAY_FACTORY.json"

#: Identity keys - emission and scoring are idempotent on these.
PREDICTION_IDENTITY_KEY = ("challenger_id", "challenger_version", "instrument",
                          "slot_utc", "horizon")
OUTCOME_IDENTITY_KEY = ("prediction_id", "horizon")
FORFEITURE_IDENTITY_KEY = ("challenger_id", "slot_utc")

PREDICTION_RECORD_FIELDS = (
    "prediction_id", "challenger_id", "challenger_version", "spec_hash",
    "instrument", "slot_utc", "emitted_at_utc", "data_timestamp_utc",
    "data_freshness_seconds", "horizon", "outcome_window_start_utc",
    "outcome_window_end_utc", "direction", "score", "entry_convention",
    "cost_bps_per_side", "evidence_class", "forward_evidence_type", "status",
)


# --------------------------------------------------------------------------- #
# The frozen intraday challenger specifications
# --------------------------------------------------------------------------- #
def _spec(**kw) -> dict:
    spec = {
        "challenger_version": "v1",
        "promotion_allowed": False,
        "research_shadow_only": True,
        "expected_return_state": "NOT_CALIBRATED",
        "parameters_were_searched": False,
        "cost_bps_per_side": COST_BPS_PER_SIDE,
        "entry_convention": "first observable print at or after the slot "
                            "instant; never a price the emitter already saw",
        "horizons": list(HORIZON_MINUTES) + [HORIZON_CLOSE],
    }
    spec.update(kw)
    return spec


#: Eight frozen specifications across six economically distinct families.
#: Every parameter is a canonical, literature-standard constant declared here
#: before any intraday bar was (or could be) read: no sweep, no screen, no
#: winner picked. The instrument set is the liquid-proxy layer (SPY/QQQ/IWM +
#: sector SPDRs) because a first intraday feed will serve the index/ETF layer
#: before it serves 500 single names; single-name variants are a NEW version
#: when the feed proves wider.
INTRADAY_SPECS = (
    _spec(
        challenger_id="r53i_open_gap_continuation",
        family="OPENING_GAP",
        thesis="an overnight gap large relative to recent daily volatility "
               "continues intraday: the open under-reacts to overnight "
               "information flow",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"gap_measure": "open vs prior close, in units of 20-day "
                                   "close-to-close volatility",
                    "min_abs_gap_sigma": 0.5,
                    "direction": "sign of the gap"},
        signal_owner="gap_continuation",
    ),
    _spec(
        challenger_id="r53i_open_gap_reversal",
        family="OPENING_GAP",
        thesis="a SMALL overnight gap is liquidity noise from the auction and "
               "mean-reverts; the two gap cells partition the gap domain so "
               "they can never both fire on one session",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"gap_measure": "open vs prior close, in units of 20-day "
                                   "close-to-close volatility",
                    "max_abs_gap_sigma": 0.5,
                    "direction": "opposite sign of the gap"},
        signal_owner="gap_reversal",
    ),
    _spec(
        challenger_id="r53i_intraday_momentum_30m",
        family="INTRADAY_TREND",
        thesis="the first half-hour return predicts the rest of the session "
               "in the same direction (Gao-Han-Li-Zhou 2018, 'market "
               "intraday momentum')",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"formation_minutes": 30,
                    "formation_anchor": "session open",
                    "direction": "sign of the formation return"},
        signal_owner="intraday_momentum",
    ),
    _spec(
        challenger_id="r53i_intraday_reversal_30m",
        family="SHORT_HORIZON_REVERSAL",
        thesis="a large 30-minute move without a volume shock is liquidity "
               "provision and pays back within the session",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"formation_minutes": 30,
                    "formation_anchor": "slot instant",
                    "min_abs_move_sigma": 1.0,
                    "volume_confirmation_max_ratio": 1.5,
                    "direction": "opposite sign of the formation return"},
        signal_owner="intraday_reversal",
    ),
    _spec(
        challenger_id="r53i_realized_vol_breakout",
        family="VOLATILITY_EXPANSION",
        thesis="a realized-volatility expansion versus the trailing session "
               "marks regime entry: range breakouts continue while the "
               "expansion persists",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"rv_window_minutes": 60,
                    "rv_reference_sessions": 5,
                    "min_expansion_ratio": 2.0,
                    "direction": "sign of the 60-minute return during the "
                                 "expansion"},
        signal_owner="vol_breakout",
    ),
    _spec(
        challenger_id="r53i_sector_vs_index_rs",
        family="CROSS_SECTIONAL_RELATIVE_STRENGTH",
        thesis="intraday sector relative strength versus the index "
               "persists over the following hours: sector rotation moves "
               "slower than the tape",
        instruments=("XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU",
                     "XLB"),
        parameters={"benchmark": "SPY",
                    "formation_minutes": 120,
                    "book": "long the strongest third, short the weakest "
                            "third, dollar-neutral"},
        signal_owner="sector_relative_strength",
    ),
    _spec(
        challenger_id="r53i_abnormal_volume_confirmation",
        family="VOLUME_LIQUIDITY",
        thesis="a price move on abnormal volume (versus the same clock "
               "window's trailing average) is information, not liquidity, "
               "and continues; the reversal cell holds the complement",
        instruments=("SPY", "QQQ", "IWM"),
        parameters={"volume_window_minutes": 30,
                    "reference_sessions": 20,
                    "min_volume_ratio": 2.0,
                    "min_abs_move_sigma": 0.5,
                    "direction": "sign of the confirmed move"},
        signal_owner="volume_confirmation",
    ),
    _spec(
        challenger_id="r53i_vix_equity_lead",
        family="CROSS_ASSET_LEAD_LAG",
        thesis="an intraday implied-volatility shock leads the equity tape: "
               "vol markets reprice risk faster than cash equities absorb it",
        instruments=("SPY",),
        parameters={"signal_instrument": "VIX (or VX front) intraday change",
                    "formation_minutes": 30,
                    "min_abs_change_pct": 3.0,
                    "direction": "opposite sign of the volatility shock"},
        signal_owner="vix_lead",
    ),
)

INTRADAY_FAMILIES = tuple(sorted({s["family"] for s in INTRADAY_SPECS}))


def spec_hash(spec: dict) -> str:
    """Everything that changes the challenger's economics, nothing else."""
    return sha({k: spec[k] for k in
                ("challenger_id", "challenger_version", "family", "thesis",
                 "instruments", "parameters", "signal_owner", "horizons",
                 "cost_bps_per_side", "entry_convention")})


def spec_by_id(challenger_id: str) -> Optional[dict]:
    for s in INTRADAY_SPECS:
        if s["challenger_id"] == challenger_id:
            return s
    return None


# --------------------------------------------------------------------------- #
# Ledger (canonical desk primitives; append-only, chain-hashed)
# --------------------------------------------------------------------------- #
def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def ledger_dir():
    d = research_dir() / "prospective_intraday"
    d.mkdir(parents=True, exist_ok=True)
    return d


def predictions() -> list:
    return _desk()._read_ledger(ledger_dir(), PREDICTION_LEDGER)


def outcomes() -> list:
    return _desk()._read_ledger(ledger_dir(), OUTCOME_LEDGER)


def forfeitures() -> list:
    return _desk()._read_ledger(ledger_dir(), FORFEITURE_LEDGER)


def verify() -> dict:
    desk = _desk()
    reports = [desk.verify_ledger(ledger_dir(), f) for f in LEDGERS]
    return {"all_intact": all(r["intact"] for r in reports), "ledgers": reports,
            "primitives": "api.paper_trading_desk chain-hash ledgers (canonical)"}


class LedgerRefusal(Exception):
    """Raised when a row may not enter a ledger. Never caught silently."""


def prediction_key(row: dict) -> tuple:
    return tuple(str(row.get(k)) for k in PREDICTION_IDENTITY_KEY)


def prediction_id(challenger_id: str, version: str, instrument: str,
                  slot_utc: str, horizon) -> str:
    return "r53i_" + sha({"c": challenger_id, "v": version, "i": instrument,
                          "s": slot_utc, "h": str(horizon)})[:20]


def validate_prediction(row: dict) -> None:
    missing = [f for f in PREDICTION_RECORD_FIELDS if f not in row]
    if missing:
        raise LedgerRefusal("prediction is missing required contract fields: "
                            + ", ".join(sorted(missing)))
    if row.get("forward_evidence_type") != TRUE_FORWARD:
        raise LedgerRefusal("this ledger holds TRUE_FORWARD rows only; got %r"
                            % row.get("forward_evidence_type"))
    if row.get("status") != STATUS_PENDING:
        raise LedgerRefusal("a prediction enters the ledger as %s; got %r"
                            % (STATUS_PENDING, row.get("status")))
    emitted = str(row.get("emitted_at_utc") or "")
    start = str(row.get("outcome_window_start_utc") or "")
    if not emitted or not start or not emitted < start:
        raise LedgerRefusal(
            "REFUSED - not TRUE_FORWARD: emitted_at_utc %r is not strictly "
            "before outcome_window_start_utc %r" % (emitted, start))
    data_ts = str(row.get("data_timestamp_utc") or "")
    if not data_ts or not data_ts <= emitted:
        raise LedgerRefusal("REFUSED - data_timestamp_utc %r is missing or "
                            "after emitted_at_utc %r" % (data_ts, emitted))
    fresh = row.get("data_freshness_seconds")
    if fresh is None or float(fresh) > MAX_INPUT_AGE_MINUTES * 60.0:
        raise LedgerRefusal(
            "REFUSED - stale input: data is %s seconds old at emission "
            "against a %d-minute ceiling; a stale bar cannot stamp an "
            "intraday signal" % (fresh, MAX_INPUT_AGE_MINUTES))


def append_predictions(rows: list) -> dict:
    """Append prediction rows. Idempotent on the identity key; the FIRST
    emission wins and a duplicate is skipped, never overwritten."""
    seen = {prediction_key(r) for r in predictions()}
    fresh, duplicates = [], []
    for row in rows:
        validate_prediction(row)
        k = prediction_key(row)
        if k in seen:
            duplicates.append({"key": list(k),
                               "prediction_id": row.get("prediction_id")})
            continue
        seen.add(k)
        fresh.append(row)
    appended = _desk()._append_ledger(ledger_dir(), PREDICTION_LEDGER,
                                      fresh) if fresh else []
    return {"n_offered": len(rows), "n_appended": len(appended),
            "appended": appended,
            "n_duplicates_skipped": len(duplicates), "duplicates": duplicates,
            "idempotent": True}


def append_forfeitures(rows: list) -> dict:
    """Record missed emission windows. Every row MUST carry
    ``backfill_refused: true`` - the append refuses anything else, exactly as
    the R52 forfeiture owner does for the daily lanes."""
    for row in rows:
        if row.get("backfill_refused") is not True:
            raise LedgerRefusal(
                "REFUSED - a forfeiture row must carry backfill_refused: "
                "true; a 'forfeiture' that leaves the door open to a "
                "backdated row is not a forfeiture")
        for f in ("challenger_id", "slot_utc", "reason"):
            if not row.get(f):
                raise LedgerRefusal("forfeiture is missing %r" % f)
    seen = {tuple(str(r.get(k)) for k in FORFEITURE_IDENTITY_KEY)
            for r in forfeitures()}
    fresh = []
    for row in rows:
        k = tuple(str(row.get(kk)) for kk in FORFEITURE_IDENTITY_KEY)
        if k in seen:
            continue
        seen.add(k)
        fresh.append(row)
    appended = _desk()._append_ledger(ledger_dir(), FORFEITURE_LEDGER,
                                      fresh) if fresh else []
    return {"n_offered": len(rows), "n_appended": len(appended),
            "idempotent": True}


def validate_outcome(row: dict) -> None:
    for f in ("prediction_id", "challenger_id", "horizon", "scored_at_utc",
              "entry_price", "exit_price", "realised_net_return",
              "forward_evidence_type", "maturity_state"):
        if f not in row:
            raise LedgerRefusal("outcome is missing required field %r" % f)
    if row.get("forward_evidence_type") != TRUE_FORWARD:
        raise LedgerRefusal("outcome rows score TRUE_FORWARD predictions only")
    if row.get("maturity_state") != "MATURED":
        raise LedgerRefusal(
            "REFUSED - only a MATURED outcome may enter this ledger; a "
            "mark-to-market reading is not an outcome and is never summed "
            "with one (the R46.5 rule)")


def append_outcomes(rows: list) -> dict:
    seen = {tuple(str(r.get(k)) for k in OUTCOME_IDENTITY_KEY)
            for r in outcomes()}
    fresh = []
    for row in rows:
        validate_outcome(row)
        k = tuple(str(row.get(kk)) for kk in OUTCOME_IDENTITY_KEY)
        if k in seen:
            continue
        seen.add(k)
        fresh.append(row)
    appended = _desk()._append_ledger(ledger_dir(), OUTCOME_LEDGER,
                                      fresh) if fresh else []
    return {"n_offered": len(rows), "n_appended": len(appended),
            "idempotent": True}


# --------------------------------------------------------------------------- #
# The slot clock
# --------------------------------------------------------------------------- #
def _et(now_utc: _dt.datetime) -> _dt.datetime:
    from zoneinfo import ZoneInfo
    return now_utc.astimezone(ZoneInfo("America/New_York"))


def slot_for(now_utc: _dt.datetime) -> Optional[dict]:
    """The emission slot ``now`` belongs to, or None outside every window.

    An instant belongs to a slot only within ``SLOT_GRACE_MINUTES`` after the
    slot time. Sunday-evening or overnight invocations belong to no slot; a
    slot whose grace passed is FORFEITED for that (challenger, slot), never
    emitted late.
    """
    et = _et(now_utc)
    if et.weekday() >= 5:
        return None
    for hhmm in EMISSION_SLOTS_ET:
        hh, mm = (int(x) for x in hhmm.split(":"))
        slot_et = et.replace(hour=hh, minute=mm, second=0, microsecond=0)
        delta = (et - slot_et).total_seconds() / 60.0
        if 0.0 <= delta <= SLOT_GRACE_MINUTES:
            slot_utc = slot_et.astimezone(_dt.timezone.utc)
            return {"slot_et": hhmm, "slot_date_et": et.date().isoformat(),
                    "slot_utc": slot_utc.isoformat().replace("+00:00", "Z"),
                    "minutes_into_grace": round(delta, 2)}
    return None


def missed_slots(now_utc: _dt.datetime, session_date_et: Optional[str] = None
                 ) -> list[dict]:
    """Every slot of the CURRENT Eastern session whose grace window has
    already passed. Used by the forfeiture sweep; never by emission."""
    et = _et(now_utc)
    if et.weekday() >= 5:
        return []
    out = []
    for hhmm in EMISSION_SLOTS_ET:
        hh, mm = (int(x) for x in hhmm.split(":"))
        slot_et = et.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if (et - slot_et).total_seconds() / 60.0 > SLOT_GRACE_MINUTES:
            slot_utc = slot_et.astimezone(_dt.timezone.utc)
            out.append({"slot_et": hhmm,
                        "slot_date_et": et.date().isoformat(),
                        "slot_utc": slot_utc.isoformat().replace("+00:00", "Z")})
    return out


# --------------------------------------------------------------------------- #
# The feed gate
# --------------------------------------------------------------------------- #
def lane_state(lane: Optional[dict] = None) -> dict:
    """The canonical intraday-lane verdict, read from its ONE owner's
    artifact (:mod:`alpha_agent.r46.intraday`). Injected in hermetic tests."""
    if lane is None:
        from ..r46 import campaign_dir as _r46_dir
        from ..r46 import intraday as _il
        lane = read_json(_r46_dir() / _il.ARTIFACT, default=None)
    if not lane:
        return {"state": LANE_BLOCKED, "exact_blocker":
                "the canonical intraday-lane artifact is absent; absence of "
                "evidence of a feed is absence of a feed"}
    return {"state": lane.get("state") or LANE_BLOCKED,
            "exact_blocker": lane.get("exact_blocker"),
            "probed_at_utc": lane.get("probed_at_utc"),
            "sources": lane.get("sources")}


# --------------------------------------------------------------------------- #
# Emission
# --------------------------------------------------------------------------- #
def build_prediction_rows(*, spec: dict, slot: dict, now_utc: _dt.datetime,
                          signals: list[dict],
                          session_close_utc: str) -> list[dict]:
    """Assemble complete TRUE_FORWARD rows from a challenger's signal output.

    ``signals`` rows: ``{instrument, direction (+1/-1/0), score,
    data_timestamp_utc}``. A zero direction emits nothing (FLAT is a
    position, not a prediction). Freshness is measured, not asserted.
    """
    emitted = now_utc.astimezone(_dt.timezone.utc)
    emitted_iso = emitted.isoformat().replace("+00:00", "Z")
    rows = []
    for sig in signals:
        direction = int(sig.get("direction") or 0)
        if direction == 0:
            continue
        data_ts = str(sig["data_timestamp_utc"])
        freshness = (emitted - _dt.datetime.fromisoformat(
            data_ts.replace("Z", "+00:00"))).total_seconds()
        for horizon in spec["horizons"]:
            start = emitted + _dt.timedelta(seconds=1)
            if horizon == HORIZON_CLOSE:
                end_iso = session_close_utc
            else:
                end = emitted + _dt.timedelta(minutes=int(horizon))
                end_iso = end.isoformat().replace("+00:00", "Z")
            if end_iso <= emitted_iso:
                continue          # a horizon past the close emits nothing
            rows.append({
                "prediction_id": prediction_id(
                    spec["challenger_id"], spec["challenger_version"],
                    sig["instrument"], slot["slot_utc"], horizon),
                "challenger_id": spec["challenger_id"],
                "challenger_version": spec["challenger_version"],
                "spec_hash": spec_hash(spec),
                "instrument": sig["instrument"],
                "slot_utc": slot["slot_utc"],
                "emitted_at_utc": emitted_iso,
                "data_timestamp_utc": data_ts,
                "data_freshness_seconds": round(freshness, 3),
                "horizon": horizon,
                "outcome_window_start_utc":
                    start.isoformat().replace("+00:00", "Z"),
                "outcome_window_end_utc": end_iso,
                "direction": direction,
                "score": sig.get("score"),
                "entry_convention": spec["entry_convention"],
                "cost_bps_per_side": spec["cost_bps_per_side"],
                "evidence_class": "PROSPECTIVE_INTRADAY",
                "forward_evidence_type": TRUE_FORWARD,
                "status": STATUS_PENDING,
                "expected_return": None,
                "expected_return_state": "NOT_CALIBRATED",
            })
    return rows


def emit_due(*, now_utc: Optional[_dt.datetime] = None,
             lane: Optional[dict] = None,
             signal_fn: Optional[Callable] = None,
             session_close_utc: Optional[str] = None,
             specs=None) -> dict:
    """ONE bounded emission attempt for the current slot.

    Order of refusals, each recorded rather than silent:
    1. the canonical lane owner says DATA_BLOCKED -> structural block, no
       forfeiture (a window that never existed cannot be missed);
    2. now is not inside any slot's grace window -> NOT_AN_EMISSION_SLOT;
    3. duplicate (challenger, instrument, slot, horizon) -> suppressed by the
       ledger; the FIRST emission wins;
    4. stale input -> refused row by row by the ledger validator.
    """
    now = (now_utc or _dt.datetime.now(_dt.timezone.utc)).astimezone(
        _dt.timezone.utc)
    ls = lane_state(lane)
    if ls["state"] != LANE_AVAILABLE:
        return {"state": EMIT_LANE_BLOCKED, "lane": ls,
                "n_appended": 0, "forfeitures_recorded": 0,
                "detail": "no current intraday feed exists; the factory "
                          "freezes specs and emits nothing. Structural "
                          "block, not an operational miss."}
    slot = slot_for(now)
    if slot is None:
        return {"state": EMIT_NOT_A_SLOT, "lane": ls, "n_appended": 0,
                "detail": "now is outside every emission slot's grace window"}
    if signal_fn is None or session_close_utc is None:
        return {"state": EMIT_STALE_INPUT, "lane": ls, "n_appended": 0,
                "detail": "no signal path was supplied; emitting without one "
                          "would fabricate a prediction"}
    rows: list[dict] = []
    for spec in (specs if specs is not None else INTRADAY_SPECS):
        signals = signal_fn(spec, slot, now) or []
        rows.extend(build_prediction_rows(
            spec=spec, slot=slot, now_utc=now, signals=signals,
            session_close_utc=session_close_utc))
    res = append_predictions(rows)
    return {"state": EMIT_OK, "lane": ls, "slot": slot, **res}


def sweep_forfeitures(*, now_utc: Optional[_dt.datetime] = None,
                      lane: Optional[dict] = None, specs=None) -> dict:
    """Record every slot of the current session whose window passed with no
    emission WHILE the lane was available. With the lane structurally
    blocked nothing is forfeited - a window that never existed cannot be
    missed - and that distinction is the R52 split between
    OPERATIONALLY_MISSED and structurally impossible."""
    now = (now_utc or _dt.datetime.now(_dt.timezone.utc)).astimezone(
        _dt.timezone.utc)
    ls = lane_state(lane)
    if ls["state"] != LANE_AVAILABLE:
        return {"state": EMIT_LANE_BLOCKED, "lane": ls, "n_appended": 0}
    emitted_slots = {(r.get("challenger_id"), r.get("slot_utc"))
                     for r in predictions()}
    rows = []
    now_iso = now.isoformat().replace("+00:00", "Z")
    for slot in missed_slots(now):
        for spec in (specs if specs is not None else INTRADAY_SPECS):
            if (spec["challenger_id"], slot["slot_utc"]) in emitted_slots:
                continue
            rows.append({
                "challenger_id": spec["challenger_id"],
                "slot_utc": slot["slot_utc"],
                "slot_et": slot["slot_et"],
                "recorded_at_utc": now_iso,
                "reason": "OPERATIONALLY_MISSED",
                "backfill_refused": True,
                "detail": "the emission window passed while the lane was "
                          "available and nothing was emitted; the loss is "
                          "permanent and recorded, never repaired",
            })
    res = append_forfeitures(rows)
    return {"state": "SWEPT", "lane": ls, **res}


# --------------------------------------------------------------------------- #
# Outcome scoring
# --------------------------------------------------------------------------- #
def score_due(*, now_utc: Optional[_dt.datetime] = None,
              mark_fn: Optional[Callable] = None) -> dict:
    """Score every pending prediction whose outcome window has closed.

    ``mark_fn(instrument, at_utc_iso)`` returns the first observable price at
    or after the instant, from the SAME feed the prediction was stamped on,
    or None. A prediction that cannot be marked stays pending and is
    reported; it is never guessed. Matured outcomes only - a mark-to-market
    reading of an open window is refused by the ledger contract."""
    now = (now_utc or _dt.datetime.now(_dt.timezone.utc)).astimezone(
        _dt.timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")
    if mark_fn is None:
        return {"state": "NO_MARK_PATH", "n_scored": 0,
                "detail": "no feed to mark against; nothing is invented"}
    scored_ids = {(str(o.get("prediction_id")), str(o.get("horizon")))
                  for o in outcomes()}
    rows, unmarkable = [], []
    for p in predictions():
        key = (str(p.get("prediction_id")), str(p.get("horizon")))
        if key in scored_ids:
            continue
        if str(p.get("outcome_window_end_utc")) > now_iso:
            continue                       # still open; never marked early
        entry = mark_fn(p["instrument"], p["outcome_window_start_utc"])
        exitp = mark_fn(p["instrument"], p["outcome_window_end_utc"])
        if entry is None or exitp is None or float(entry) <= 0:
            unmarkable.append(p.get("prediction_id"))
            continue
        gross = (float(exitp) / float(entry) - 1.0) * float(p["direction"])
        cost = 2.0 * float(p["cost_bps_per_side"]) / 1.0e4
        rows.append({
            "prediction_id": p["prediction_id"],
            "challenger_id": p["challenger_id"],
            "instrument": p["instrument"],
            "horizon": p["horizon"],
            "scored_at_utc": now_iso,
            "entry_price": float(entry), "exit_price": float(exitp),
            "gross_return": round(gross, 8),
            "realised_net_return": round(gross - cost, 8),
            "cost_return": round(cost, 8),
            "maturity_state": "MATURED",
            "forward_evidence_type": TRUE_FORWARD,
        })
    res = append_outcomes(rows)
    return {"state": "SCORED", "n_scored": res["n_appended"],
            "n_unmarkable_still_pending": len(unmarkable),
            "unmarkable": unmarkable[:20], **res}


# --------------------------------------------------------------------------- #
# The factory artifact
# --------------------------------------------------------------------------- #
def write_factory_artifact(lane: Optional[dict] = None) -> dict:
    ls = lane_state(lane)
    preds, outs, forf = predictions(), outcomes(), forfeitures()
    body = artifact_body(
        "r53_intraday_factory/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        state=("EMITTING" if ls["state"] == LANE_AVAILABLE
               else "SPECS_FROZEN_AWAITING_FEED"),
        lane=ls,
        n_specs=len(INTRADAY_SPECS),
        families=list(INTRADAY_FAMILIES),
        specs=[{"challenger_id": s["challenger_id"], "family": s["family"],
                "spec_hash": spec_hash(s), "instruments": list(s["instruments"]),
                "horizons": [str(h) for h in s["horizons"]],
                "signal_owner": s["signal_owner"]}
               for s in INTRADAY_SPECS],
        emission_slots_et=list(EMISSION_SLOTS_ET),
        slot_grace_minutes=SLOT_GRACE_MINUTES,
        max_input_age_minutes=MAX_INPUT_AGE_MINUTES,
        cost_bps_per_side=COST_BPS_PER_SIDE,
        predictions_emitted=len(preds),
        outcomes_scored=len(outs),
        forfeitures_recorded=len(forf),
        ledger_integrity=verify(),
        authority_boundary=(
            "SHADOW research only: no row here carries expected-return "
            "authority over the production portfolio; the canonical R46 "
            "promotion gates decide if any lane ever earns more"),
        daily_horizons_stay_with_r46=(
            "this ledger holds sub-session horizons only; the session-close-"
            "to-close and longer cells remain owned by the R46 ledger"),
        **safety_block(),
    )
    write_json(research_dir() / FACTORY_ARTIFACT, body)
    return body
