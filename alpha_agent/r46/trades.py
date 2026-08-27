"""alpha_agent.r46.trades - THE research paper-trade ledger.

A TRUE_FORWARD prediction becomes exactly one hypothetical research trade.
The trade is a RESEARCH object: it is not an order, not a holding, not an
operational position, and no operational code may consume it as one. It
exists so that "the model predicted correctly" and "the model made money"
stop being the same sentence.

Three append-only, chain-hashed ledgers - the same primitives every desk
ledger has used since Phase 27 - pointed at the Release-46 research root:

``r46_4_research_trades.json``
    one OPEN row per prediction, written the first time the prediction's
    entry close has actually printed on every leg. Keyed by ``prediction_id``;
    a prediction can never open twice. The entry marks on this row are never
    edited.

``r46_4_trade_marks.json``
    one row per (trade, session) for every session after entry, written from
    point-in-time marks by :mod:`alpha_agent.r46.pnl`. Keyed by
    ``(research_trade_id, session)``.

``r46_4_trade_closes.json``
    one row per trade at maturity, TAKEN from the judge's outcome row. Keyed
    by ``research_trade_id``.

State is DERIVED from these ledgers and the prediction / outcome ledgers,
never stored and never rewritten::

    SIGNAL_EMITTED -> TRADE_OPEN -> TRADE_MARKED -> TRADE_MATURED -> TRADE_CLOSED
                   \\-> DATA_BLOCKED            INVALIDATED

Funding follows the no-hindsight rule of section 42: a trade is funded by a
shadow allocation ONLY if that allocation was decided strictly BEFORE the
trade's entry session. A trade that entered before the first allocation
existed is tracked at unit economics and carries zero shadow capital - the
shadow portfolio cannot be credited with a position it never decided to hold.

No backdating: an open row is written when the entry has printed, stamped
with the sync instant; a mark for a past session written during a catch-up is
flagged ``catch_up`` and stamped with the instant it was actually written.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Callable, Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, sha, write_json
from . import clock as CK
from . import contract as C
from . import ledger as LG
from . import pnl as PN

CALCULATION_OWNER = "alpha_agent.r46.trades"

SHADOW_DIRNAME = "shadow_pnl"
OPENS_LEDGER = "r46_4_research_trades.json"
MARKS_LEDGER = "r46_4_trade_marks.json"
CLOSES_LEDGER = "r46_4_trade_closes.json"
LEDGERS = (OPENS_LEDGER, MARKS_LEDGER, CLOSES_LEDGER)

ARTIFACT = "R46_4_RESEARCH_TRADES.json"

#: The frozen state vocabulary.
SIGNAL_EMITTED = "SIGNAL_EMITTED"
TRADE_OPEN = "TRADE_OPEN"
TRADE_MARKED = "TRADE_MARKED"
TRADE_MATURED = "TRADE_MATURED"
TRADE_CLOSED = "TRADE_CLOSED"
DATA_BLOCKED = "DATA_BLOCKED"
INVALIDATED = "INVALIDATED"
STATES = (SIGNAL_EMITTED, TRADE_OPEN, TRADE_MARKED, TRADE_MATURED,
          TRADE_CLOSED, DATA_BLOCKED, INVALIDATED)

#: An entry that has not printed this many weekdays after it was expected is
#: a data problem, named as such rather than left as "emitted".
ENTRY_GRACE_WEEKDAYS = 5

#: The one NAV clock (section 9 of the governance doc: one authoritative NAV).
NAV_CALENDAR_INSTRUMENT = "SPY"

#: Books wider than this store a hash of their per-leg marks, not the marks.
WIDE_BOOK_LEGS = 12

FundingFn = Callable[[str, _dt.date, int], dict]


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def shadow_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = campaign_dir(campaign_id) / SHADOW_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def trade_id(prediction_id: str) -> str:
    return "r46t_" + sha({"prediction_id": str(prediction_id)})[:20]


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def opens(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(shadow_dir(campaign_id), OPENS_LEDGER)


def marks(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(shadow_dir(campaign_id), MARKS_LEDGER)


def closes(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(shadow_dir(campaign_id), CLOSES_LEDGER)


def verify(campaign_id: str = CAMPAIGN_ID) -> dict:
    desk = _desk()
    d = shadow_dir(campaign_id)
    reports = [desk.verify_ledger(d, f) for f in LEDGERS]
    return {"all_intact": all(r["intact"] for r in reports),
            "ledgers": reports}


def nav_calendar(series_fn=None, start: _dt.date = None,
                 end: _dt.date = None) -> list:
    """Realised sessions of the NAV clock instrument inside [start, end]."""
    from . import marketdata as MD
    sf = series_fn or MD.closes
    s = sf(NAV_CALENDAR_INSTRUMENT)
    if s is None or not len(s):
        return []
    out = []
    for ts in s.index:
        d = ts.date()
        if start is not None and d < start:
            continue
        if end is not None and d > end:
            break
        out.append(d)
    return out


# --------------------------------------------------------------------------- #
# Synchronise the ledgers with the prediction / outcome ledgers as of a session
# --------------------------------------------------------------------------- #
def sync(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID, registry: dict = None,
         series_fn=None, funding_fn: FundingFn = None,
         risk_free_annual: float = None, synced_at: _dt.datetime = None) -> dict:
    """Open, mark and close research trades through ``as_of``. Idempotent.

    ``funding_fn(challenger_id, entry_session, horizon)`` returns the shadow
    capital each policy had decided for this cell STRICTLY BEFORE the entry
    session, or an empty dict when no such decision exists.
    """
    from . import marketdata as MD
    from . import registry as RG
    sf = series_fn or MD.closes
    now = synced_at or CK.now_utc()
    reg = registry if registry is not None else RG.load(campaign_id)
    entries = {c["challenger_id"]: c for c in (reg.get("challengers") or ())}

    preds = LG.predictions(campaign_id)
    outs = {str(o.get("prediction_id")): o for o in LG.outcomes(campaign_id)}
    open_rows = {str(r.get("prediction_id")): r for r in opens(campaign_id)}
    mark_keys = {(str(r.get("research_trade_id")), str(r.get("session")))
                 for r in marks(campaign_id)}
    close_ids = {str(r.get("research_trade_id")) for r in closes(campaign_id)}
    calendar = nav_calendar(sf, end=as_of)
    rf = risk_free_annual
    if rf is None:
        rf = MD.risk_free_annual().get("annual")

    new_opens, new_marks, new_closes, blocked = [], [], [], []
    for p in preds:
        pid = str(p.get("prediction_id"))
        tid = trade_id(pid)
        cid = p.get("challenger_id")
        entry = entries.get(cid) or {}
        horizon = int(p.get("horizon") or 0)
        outcome = outs.get(pid)

        # ---- OPEN: the first time every leg's entry close has printed ------- #
        if pid not in open_rows:
            econ = PN.economics(p, as_of, outcome=None, registry_entry=entry,
                                series_fn=sf, risk_free_annual=rf)
            if econ.get("state") != "OPEN":
                if econ.get("state") == "ENTRY_NOT_PRINTED":
                    blocked.append({"prediction_id": pid, "state": _entry_state(
                        p, as_of), "missing": econ.get("missing")})
                continue
            entry_session = _dt.date.fromisoformat(econ["entry_session"])
            n_cells = max(1, len(entry.get("horizons") or [horizon]))
            funding = (funding_fn(cid, entry_session, horizon)
                       if funding_fn else {}) or {}
            row = _open_row(p, econ, entry, tid, n_cells, funding, now, as_of)
            new_opens.append(row)
            open_rows[pid] = row

        orow = open_rows[pid]
        entry_session = _dt.date.fromisoformat(str(orow["entry_session"]))

        # ---- CLOSE: the judge has spoken; take its number ----------------- #
        if outcome is not None and tid not in close_ids:
            econ = PN.economics(p, as_of, outcome=outcome, registry_entry=entry,
                                series_fn=sf, risk_free_annual=rf)
            new_closes.append(_close_row(p, econ, orow, outcome, now, as_of))
            close_ids.add(tid)

        # ---- MARK: every NAV session after entry, through as_of, before exit #
        exit_session = None
        if outcome is not None and outcome.get("maturity_date"):
            exit_session = _dt.date.fromisoformat(
                str(outcome.get("maturity_date"))[:10])
        for s in calendar:
            if s <= entry_session:
                continue
            if exit_session is not None and s >= exit_session:
                break
            if (tid, str(s)) in mark_keys:
                continue
            econ = PN.economics(p, s, outcome=None, registry_entry=entry,
                                series_fn=sf, risk_free_annual=rf)
            if econ.get("state") != "OPEN":
                continue
            new_marks.append(_mark_row(p, econ, orow, s, now, as_of))
            mark_keys.add((tid, str(s)))

    desk = _desk()
    d = shadow_dir(campaign_id)
    a_open = desk._append_ledger(d, OPENS_LEDGER, new_opens) if new_opens else []
    a_mark = desk._append_ledger(d, MARKS_LEDGER, new_marks) if new_marks else []
    a_close = desk._append_ledger(d, CLOSES_LEDGER, new_closes) \
        if new_closes else []
    return {
        "as_of": str(as_of),
        "synced_at_utc": CK.iso(now),
        "n_predictions": len(preds),
        "n_opened": len(a_open), "n_marked": len(a_mark),
        "n_closed": len(a_close),
        "n_entry_not_printed": len(blocked),
        "entry_not_printed": blocked[:40],
        "idempotent": True, "backdated": False,
        "calculation_owner": CALCULATION_OWNER,
    }


def _entry_state(p: dict, as_of: _dt.date) -> str:
    try:
        expected = _dt.date.fromisoformat(str(p.get("effective_as_of"))[:10])
    except ValueError:
        return INVALIDATED
    lag, d = 0, expected
    while d < as_of:
        d += _dt.timedelta(days=1)
        if d.weekday() not in CK.WEEKEND:
            lag += 1
    return DATA_BLOCKED if lag > ENTRY_GRACE_WEEKDAYS else SIGNAL_EMITTED


def _open_row(p: dict, econ: dict, entry: dict, tid: str, n_cells: int,
              funding: dict, now: _dt.datetime, as_of: _dt.date) -> dict:
    horizon = int(p.get("horizon") or 0)
    share = 1.0 / float(n_cells * max(1, horizon))
    cap = {}
    for pol, f in (funding or {}).items():
        cap[pol] = {
            "capital_usd": round(float(f.get("nav_at_decision") or 0.0)
                                 * float(f.get("weight") or 0.0) * share, 6),
            "strategy_weight": f.get("weight"),
            "decision_session": f.get("decision_session"),
            "nav_at_decision": f.get("nav_at_decision"),
        }
    funded = any(v["capital_usd"] > 0 for v in cap.values())
    return {
        "research_trade_id": tid,
        "prediction_id": p.get("prediction_id"),
        "challenger_id": p.get("challenger_id"),
        "challenger_version": p.get("challenger_version"),
        "challenger_spec_hash": p.get("challenger_spec_hash"),
        "horizon": horizon,
        "asset_class": econ.get("asset_class"),
        "economic_family": econ.get("economic_family"),
        "information_family": econ.get("information_family"),
        "dependence_cluster": econ.get("dependence_cluster"),
        "instrument": econ.get("instrument"),
        "instruments": econ.get("instruments"),
        "trade_structure": econ.get("trade_structure"),
        "direction": p.get("direction"),
        "n_legs": econ.get("n_legs"),
        "gross_exposure_per_unit_capital":
            econ.get("gross_notional_per_unit_capital"),
        "net_exposure_per_unit_capital":
            econ.get("net_notional_per_unit_capital"),
        "signal_timestamp_utc": p.get("data_cutoff_utc"),
        "decision_timestamp_utc": p.get("emitted_at_utc"),
        "entry_session_expected": str(p.get("effective_as_of")),
        "entry_session": econ["entry_session"],
        "entry_sessions_by_leg": econ.get("entry_sessions"),
        "entry_marks": econ["entry_marks"],
        "exit_session_expected": p.get("horizon_end_expected"),
        "cost_return": econ["cost_return"],
        "cost_breakdown_bps": econ.get("cost_breakdown"),
        "cost_return_at_2x": 2.0 * float(econ["cost_return"]),
        "cost_return_at_stress": (econ.get("cost_scenarios") or {}).get(
            PN.SCENARIO_STRESS, {}).get("total_return_units"),
        "cost_recognition": econ.get("cost_recognition"),
        "control": p.get("control"),
        "benchmark": p.get("benchmark"),
        "sizing_rule": econ.get("sizing_rule"),
        "weight_within_strategy": share,
        "n_cells_in_strategy": n_cells,
        "capital_by_policy": cap,
        "funded": funded,
        "funding_state": ("FUNDED_BY_PRIOR_ALLOCATION" if funded
                          else "UNFUNDED_NO_ALLOCATION_DECIDED_BEFORE_ENTRY"),
        "evidence_status": p.get("forward_evidence_type"),
        "pit_status": p.get("point_in_time_status"),
        "opened_as_of": str(as_of),
        "opened_at_utc": CK.iso(now),
        "opened_at_utc_precise": CK.iso_precise(now),
        "state_at_open": TRADE_OPEN,
        "research_only": True,
        "is_an_order": False, "is_a_holding": False,
        "calculation_owner": CALCULATION_OWNER,
        "economics_owner": PN.CALCULATION_OWNER,
    }


def _mark_row(p: dict, econ: dict, orow: dict, session: _dt.date,
              now: _dt.datetime, as_of: _dt.date) -> dict:
    cur = econ.get("current_marks") or {}
    by_leg = {l["instrument"]: l["mark_date"] for l in (econ.get("per_leg") or [])}
    wide = len(cur) > WIDE_BOOK_LEGS
    return {
        "research_trade_id": orow["research_trade_id"],
        "prediction_id": p.get("prediction_id"),
        "challenger_id": p.get("challenger_id"),
        "session": str(session),
        # A hundred-leg book stores a hash of its marks and the count, not the
        # marks: the ledger must stay readable after a year of daily rows.
        "marks": (None if wide else cur),
        "marks_hash": sha(cur),
        "n_legs_marked": len(cur),
        "mark_sessions_by_leg": (None if wide else by_leg),
        "n_legs_marked_on_session": sum(1 for d in by_leg.values()
                                        if d == str(session)),
        "sessions_held": econ.get("sessions_held"),
        "gross_return": econ.get("gross_return"),
        "cost_return": econ.get("cost_return"),
        "net_return": econ.get("net_return"),
        "net_return_at_2x": econ.get("net_return_at_2x"),
        "net_return_at_stress": econ.get("net_return_at_stress"),
        "control_return": econ.get("control_return"),
        "benchmark_return": econ.get("benchmark_return"),
        "residual_return": econ.get("residual_return"),
        "residual_alpha_vs_control": econ.get("residual_alpha_vs_control"),
        "unrealised": True,
        "catch_up": bool(session < as_of),
        "marked_as_of": str(as_of),
        "marked_at_utc": CK.iso(now),
        "point_in_time": "last bar on or before the session, per leg",
        "calculation_owner": CALCULATION_OWNER,
        "economics_owner": PN.CALCULATION_OWNER,
    }


def _close_row(p: dict, econ: dict, orow: dict, outcome: dict,
               now: _dt.datetime, as_of: _dt.date) -> dict:
    return {
        "research_trade_id": orow["research_trade_id"],
        "prediction_id": p.get("prediction_id"),
        "challenger_id": p.get("challenger_id"),
        "exit_session": econ.get("exit_session"),
        "exit_marks": econ.get("exit_marks"),
        "sessions_held": econ.get("sessions_held"),
        "gross_return": econ.get("gross_return"),
        "cost_return": econ.get("cost_return"),
        "net_return": econ.get("net_return"),
        "net_return_at_2x": econ.get("net_return_at_2x"),
        "net_return_at_stress": econ.get("net_return_at_stress"),
        "control_return": econ.get("control_return"),
        "benchmark_return": econ.get("benchmark_return"),
        "residual_return": econ.get("residual_return"),
        "residual_alpha_vs_control": econ.get("residual_alpha_vs_control"),
        "residual_alpha_at_2x": econ.get("residual_alpha_at_2x"),
        "turnover_per_unit_capital": econ.get("turnover_per_unit_capital"),
        "hit": outcome.get("hit"),
        "rank_ic": outcome.get("rank_ic"),
        "realised": True,
        "source_of_truth": econ.get("source_of_truth"),
        "reconciliation": econ.get("reconciliation"),
        "scored_at_utc": outcome.get("scored_at_utc"),
        "closed_as_of": str(as_of),
        "closed_at_utc": CK.iso(now),
        "calculation_owner": CALCULATION_OWNER,
        "economics_owner": PN.CALCULATION_OWNER,
    }


# --------------------------------------------------------------------------- #
# Derived state and the read model
# --------------------------------------------------------------------------- #
def states(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID) -> dict:
    """Every prediction's trade state, DERIVED - never stored."""
    preds = LG.predictions(campaign_id)
    outs = {str(o.get("prediction_id")): o for o in LG.outcomes(campaign_id)}
    open_rows = {str(r.get("prediction_id")): r for r in opens(campaign_id)}
    mark_rows: dict = {}
    for m in marks(campaign_id):
        mark_rows.setdefault(str(m.get("research_trade_id")), []).append(m)
    close_rows = {str(r.get("research_trade_id")): r
                  for r in closes(campaign_id)}
    out = []
    for p in preds:
        pid = str(p.get("prediction_id"))
        tid = trade_id(pid)
        orow = open_rows.get(pid)
        my_marks = sorted(mark_rows.get(tid, []), key=lambda m: m["session"])
        crow = close_rows.get(tid)
        if p.get("status") == C.STATUS_INVALIDATED:
            state = INVALIDATED
        elif crow is not None:
            state = TRADE_CLOSED
        elif pid in outs:
            state = TRADE_MATURED
        elif orow is not None and my_marks:
            state = TRADE_MARKED
        elif orow is not None:
            state = TRADE_OPEN
        else:
            state = _entry_state(p, as_of)
        last = my_marks[-1] if my_marks else None
        out.append({
            "research_trade_id": tid,
            "prediction_id": pid,
            "challenger_id": p.get("challenger_id"),
            "challenger_version": p.get("challenger_version"),
            "horizon": p.get("horizon"),
            "asset_class": p.get("asset_class"),
            "instrument": p.get("instrument"),
            "direction": p.get("direction"),
            "state": state,
            "entry_session_expected": p.get("effective_as_of"),
            "entry_session": (orow or {}).get("entry_session"),
            "exit_session_expected": p.get("horizon_end_expected"),
            "exit_session": (crow or {}).get("exit_session"),
            "funded": bool((orow or {}).get("funded")),
            "funding_state": (orow or {}).get("funding_state"),
            "capital_by_policy": (orow or {}).get("capital_by_policy") or {},
            "cost_return": (orow or {}).get("cost_return"),
            "last_mark_session": (last or {}).get("session"),
            "unrealised_net_return": (None if crow is not None or last is None
                                      else last.get("net_return")),
            "unrealised_gross_return": (None if crow is not None
                                        or last is None
                                        else last.get("gross_return")),
            "unrealised_residual_alpha": (None if crow is not None
                                          or last is None
                                          else last.get(
                                              "residual_alpha_vs_control")),
            "realised_net_return": (crow or {}).get("net_return"),
            "realised_gross_return": (crow or {}).get("gross_return"),
            "realised_residual_alpha": (crow or {}).get(
                "residual_alpha_vs_control"),
            "reconciliation": ((crow or {}).get("reconciliation") or {}).get(
                "state"),
            "evidence_status": p.get("forward_evidence_type"),
        })
    counts = {s: sum(1 for t in out if t["state"] == s) for s in STATES}
    return {"as_of": str(as_of), "n_trades": len(out), "counts": counts,
            "trades": out}


def snapshot(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID) -> dict:
    st = states(as_of, campaign_id)
    body = artifact_body(
        "r46_4_research_trades/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        state_vocabulary=list(STATES),
        n_trades=st["n_trades"],
        counts=st["counts"],
        n_open=(st["counts"][TRADE_OPEN] + st["counts"][TRADE_MARKED]
                + st["counts"][TRADE_MATURED]),
        n_closed=st["counts"][TRADE_CLOSED],
        n_signal_emitted=st["counts"][SIGNAL_EMITTED],
        n_funded=sum(1 for t in st["trades"] if t["funded"]),
        n_unfunded_open=sum(1 for t in st["trades"]
                            if not t["funded"] and t["state"] in
                            (TRADE_OPEN, TRADE_MARKED, TRADE_MATURED)),
        chain=verify(campaign_id),
        no_hindsight_funding_rule=(
            "a trade is funded only by an allocation decided strictly before "
            "its entry session; earlier trades carry zero shadow capital"),
        one_prediction_one_trade=True,
        idempotent=True,
        research_only=True,
        is_an_order=False, is_a_holding=False,
        trades=st["trades"],
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "STATES", "SIGNAL_EMITTED", "TRADE_OPEN",
           "TRADE_MARKED", "TRADE_MATURED", "TRADE_CLOSED", "DATA_BLOCKED",
           "INVALIDATED", "LEDGERS", "ARTIFACT", "shadow_dir", "trade_id",
           "opens", "marks", "closes", "verify", "nav_calendar", "sync",
           "states", "snapshot"]
