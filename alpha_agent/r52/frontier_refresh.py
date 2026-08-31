"""alpha_agent.r52.frontier_refresh - keep the R51 promotion frontier current.

:mod:`alpha_agent.r51.promotion_frontier` is a PURE calculation: every input
is injected, nothing is read from disk, nothing is written. Release 51 built
it once by hand; a frontier nobody rebuilds is the same class of defect as a
lane nobody calls (R46.6's finding, one layer up). This module is the
assembler the runtime calls after evidence legitimately changes:

* leaderboard / velocity / verdicts / continuation - read from the canonical
  R46 campaign artifacts the advance just rebuilt;
* sleeves - ``api.investability_registry.declared_sleeves()`` (a READ of the
  operational registry; the frontier module itself stays free of operational
  imports by design, so the injection happens here);
* unit economics - measured from owned reference data
  (``api.market_reference_data``), smallest owned unit per sleeve;
* NAV - the canonical forward-performance ledger's latest row (read-only).

It persists ONE refreshed frontier artifact and one promotion-packet state
file in the R52 runtime root, and records every state TRANSITION exactly
once. A sleeve that becomes PROMOTION_READY is surfaced prominently in the
health read model - and NOTHING here can approve it:
``model_approval_state`` and ``capital_eligible`` have no writer in this
package, and the audit proves it.
"""
from __future__ import annotations

import datetime as _dt

from . import artifact_body, read_json, runtime_dir, write_json
from ..r46 import CAMPAIGN_ID
from ..r46 import campaign_dir as r46_campaign_dir
from ..r46 import clock as CK
from ..r51 import promotion_frontier as PF

CALCULATION_OWNER = "alpha_agent.r52.frontier_refresh"

ARTIFACT = "R51_PROMOTION_FRONTIER.json"
PACKET_STATE_ARTIFACT = "promotion_packet_states.json"

LEADERBOARD_ARTIFACT = "R46_LEADERBOARD.json"
VELOCITY_ARTIFACT = "R46_EVIDENCE_VELOCITY.json"
VERDICTS_ARTIFACT = "R46_5_STRATEGY_VERDICTS.json"
CONTINUATION_ARTIFACT = "R46_6_1_ADOPTED_CONTINUATION.json"

NAME_CAP_FRACTION = 0.10


def _campaign_inputs(campaign_id: str = CAMPAIGN_ID) -> dict:
    d = r46_campaign_dir(campaign_id)
    return {
        "leaderboard": read_json(d / LEADERBOARD_ARTIFACT, default={}) or {},
        "velocity": read_json(d / VELOCITY_ARTIFACT, default={}) or {},
        "verdicts": read_json(d / VERDICTS_ARTIFACT, default={}) or {},
        "continuation": read_json(d / CONTINUATION_ARTIFACT, default={}) or {},
    }


def _declared_sleeves() -> list:
    try:
        from paper_trader.api import investability_registry as ir
        return list(ir.declared_sleeves())
    except Exception:                      # noqa: BLE001 - injected input probe
        return []


def _nav_usd():
    try:
        from paper_trader.api import paper_trading_desk as desk
        perf = desk.load_performance()
        rows = perf.get("current_rows") or perf.get("rows") or []
        if rows and rows[-1].get("nav") is not None:
            return float(rows[-1]["nav"])
    except Exception:                      # noqa: BLE001 - read-only probe
        pass
    return None


def _unit_economics(sleeves: list) -> dict:
    """Smallest owned unit per futures sleeve, from owned reference data."""
    try:
        from paper_trader.api import market_reference_data as mrd
    except Exception:                      # noqa: BLE001
        return {}
    econ = {}
    for rec in sleeves:
        sid = rec.get("sleeve_id")
        ids = rec.get("instrument_ids") or []
        if not sid or not ids:
            continue
        best = None
        for sym in ids:
            try:
                meta = mrd.futures_metadata(sym)
                point = meta.get("point_value")
                bars = mrd.daily_bars(sym)
                last = bars[-1] if bars else None
                mark = (last.get("close") if isinstance(last, dict)
                        else (last[4] if last and len(last) > 4 else None))
                if point is None or mark is None:
                    continue
                fx = 1.0
                cur = meta.get("currency") or rec.get("currency") or "USD"
                if cur != "USD":
                    fxq = mrd.fx_to_usd(cur)
                    if fxq.get("rate") is None:
                        continue
                    fx = float(fxq["rate"])
                notional = abs(float(mark) * float(point) * fx)
                margin = meta.get("initial_margin")
                if best is None or notional < best["smallest_unit_notional_usd"]:
                    best = {"smallest_unit_notional_usd": round(notional, 2),
                            "smallest_unit_symbol": sym,
                            "margin_usd": (round(float(margin) * fx, 2)
                                           if margin is not None else None)}
            except Exception:              # noqa: BLE001 - per-symbol probe
                continue
        if best:
            econ[sid] = best
    return econ


def refresh(now: _dt.datetime = None, *,
            campaign_id: str = CAMPAIGN_ID, write: bool = True,
            sleeves: list = None, unit_economics: dict = None,
            nav_usd=None) -> dict:
    """Rebuild the frontier on the evidence that now exists. Idempotent."""
    now = now or CK.now_utc()
    inputs = _campaign_inputs(campaign_id)
    sl = sleeves if sleeves is not None else _declared_sleeves()
    econ = unit_economics if unit_economics is not None else _unit_economics(sl)
    nav = nav_usd if nav_usd is not None else _nav_usd()

    frontier = PF.build(
        leaderboard=inputs["leaderboard"],
        velocity=inputs["velocity"],
        verdicts=inputs["verdicts"],
        continuation=inputs["continuation"],
        sleeves=sl,
        unit_economics=econ,
        nav_usd=(nav if nav is not None else 0.0),
        name_cap_fraction=NAME_CAP_FRACTION,
        as_of=str(CK.eastern_date(now)),
    )

    rows = list(frontier.get("rows") or ())
    states = {str(r.get("sleeve_id")): str(r.get("state")
                                           or "CONTINUE_OBSERVATION")
              for r in rows}
    ready = sorted(str(s) for s in (frontier.get("promotion_ready") or ()))
    transitions = _record_packet_states(states, now, write=write)

    body = artifact_body(
        "r52_promotion_frontier_refresh/1", CALCULATION_OWNER,
        refreshed_at_utc=CK.iso(now),
        nav_usd=nav,
        nav_source="api.paper_trading_desk.load_performance current_rows "
                   "(read-only)",
        name_cap_fraction=NAME_CAP_FRACTION,
        inputs_read={
            "leaderboard": LEADERBOARD_ARTIFACT,
            "velocity": VELOCITY_ARTIFACT,
            "verdicts": VERDICTS_ARTIFACT,
            "continuation": CONTINUATION_ARTIFACT,
            "sleeves": "api.investability_registry.declared_sleeves()",
            "unit_economics": "api.market_reference_data (owned)",
        },
        promotion_ready=ready,
        promotion_ready_count=len(ready),
        packet_state_transitions=transitions,
        automatic_promotion_performed=False,
        approval_writers_in_this_package=0,
        frontier=frontier,
    )
    if write:
        write_json(runtime_dir() / ARTIFACT, body)
    return body


def _record_packet_states(states: dict, now: _dt.datetime, *,
                          write: bool) -> list:
    """Record each sleeve's packet state; a TRANSITION is written once."""
    path = runtime_dir() / PACKET_STATE_ARTIFACT
    prior = read_json(path, default=None) or {}
    known = dict(prior.get("states") or {})
    history = list(prior.get("transitions") or [])
    transitions = []
    for sid, st in sorted(states.items()):
        old = known.get(sid)
        if old != st:
            row = {"sleeve_id": sid, "from": old, "to": st,
                   "at_utc": CK.iso(now)}
            transitions.append(row)
            history.append(row)
            known[sid] = st
    if write:
        write_json(path, artifact_body(
            "r52_promotion_packet_states/1", CALCULATION_OWNER,
            states=known,
            transitions=history,
            n_transitions_total=len(history),
            manual_approval_remains_mandatory=True))
    return transitions


def load() -> dict:
    return read_json(runtime_dir() / ARTIFACT, default={}) or {}


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "PACKET_STATE_ARTIFACT",
           "NAME_CAP_FRACTION", "refresh", "load"]
