"""alpha_agent.r45.replication - Track B. The frozen rule, four ways, no tuning.

The order matters and is declared in the contract. L1 comes first because it
is the only test on this list where the ONLY thing that changes is which
events are looked at: same instrument, same broker, same bars, same observed
spread, same code - just the 370 events Release 44's parameter search could
not reach, because it never scored them.

If a rule cannot survive that, no amount of new market data is going to save
it, and Release 45 says so before it spends a byte on anything else.
"""
from __future__ import annotations

import numpy as np

from . import bars as B
from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.replication"


# --------------------------------------------------------------------------- #
def verdict(card: dict, *, min_events: int = None) -> dict:
    """REPLICATES / DOES_NOT_REPLICATE / DATA_INSUFFICIENT, by the contract."""
    if not card or card.get("state") != "MEASURED":
        return {"replication_state": "DATA_INSUFFICIENT",
                "why": "no measurable events"}
    n = int(card.get("n_events") or 0)
    floor = int(min_events if min_events is not None
                else C.MIN_EVENTS_TO_JUDGE_REPLICATION)
    if n < floor:
        return {"replication_state": "DATA_INSUFFICIENT",
                "why": f"{n} events is below the declared floor of {floor}",
                "n_events": n}
    net = float(card.get("net_bps_per_event") or 0.0)
    t = card.get("net_t_cluster")
    t = float(t) if t is not None else None
    same_sign = net > 0.0
    ok = bool(same_sign and t is not None
              and t >= float(C.REPLICATION_NET_T_MIN))
    return {
        "replication_state": "REPLICATES" if ok else "DOES_NOT_REPLICATE",
        "n_events": n, "net_bps_per_event": net, "net_t_cluster": t,
        "same_sign_as_r44": same_sign,
        "bar": f"net > 0 and clustered t >= {C.REPLICATION_NET_T_MIN}",
    }


def _cell(symbol: str, stamps, *, zone: str = None, label: str = None,
          min_events: int = None) -> dict:
    ev = ES.event_book(symbol, stamps)
    if ev is None:
        return {"symbol": symbol, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "instrument_class": B.instrument_class(symbol),
                "replication_state": "DATA_INSUFFICIENT"}
    if zone:
        ev = ES.slice_zone(ev, zone)
    card = ES.score(ev, label=label or f"{symbol}_FROZEN")
    card.update(verdict(card, min_events=min_events))
    card["sleeve"] = B.sleeve(symbol)
    card["zone"] = zone or "ALL"
    return card


# --------------------------------------------------------------------------- #
# L1 - the events R44 never scored
# --------------------------------------------------------------------------- #
def lane_l1_gold_holdout(stamps=None) -> dict:
    """The frozen rule on XAUUSD, zone by zone, on bars the estate owns."""
    stamps = stamps if stamps is not None else ES.release_stamps()
    sym = C.FROZEN_RULE["instrument_of_origin"]
    ev = ES.event_book(sym, stamps)
    if ev is None:
        return {"lane": "L1_GOLD_HOLDOUT",
                "state": "HISTORICAL_DATA_UNAVAILABLE"}
    z = ES.zone_of(ev)
    cards = {}
    for zone in ("A", "B", "C", "BC", "ALL"):
        sub = ES.slice_zone(ev, zone)
        card = ES.score(sub, label=f"XAUUSD_FROZEN_ZONE_{zone}")
        card.update(verdict(card))
        card["zone"] = zone
        cards[zone] = card

    a, bc = cards["A"], cards["BC"]
    decay = {
        "gross_bps_zone_a": a["gross_bps_per_event"],
        "gross_bps_zone_bc": bc["gross_bps_per_event"],
        "gross_bps_lost": a["gross_bps_per_event"] - bc["gross_bps_per_event"],
        "gross_retained_fraction": (bc["gross_bps_per_event"]
                                    / a["gross_bps_per_event"]
                                    if a["gross_bps_per_event"] else None),
        "hit_rate_zone_a": a["hit_rate"], "hit_rate_zone_bc": bc["hit_rate"],
        "net_t_zone_a": a["net_t_cluster"],
        "net_t_zone_bc": bc["net_t_cluster"],
    }
    return {
        "lane": "L1_GOLD_HOLDOUT", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["L1_GOLD_HOLDOUT"],
        "instrument": sym, "instrument_class": B.instrument_class(sym),
        "cost_source": ev.attrs.get("cost_source"),
        "zone_ranges": {k: z[k] for k in ("a_range", "b_range", "c_range")},
        "n_events_total": int(len(ev)),
        "n_events_never_scored_by_r44": int(cards["BC"]["n_events"]),
        "cards": cards,
        "out_of_sample_decay": decay,
        "replication_state": bc["replication_state"],
        "what_changed_between_a_and_bc":
            "nothing except which events were looked at - same instrument, "
            "same broker, same bars, same observed spread, same code path",
    }


# --------------------------------------------------------------------------- #
# L2 / L3 / L4 - the same rule elsewhere
# --------------------------------------------------------------------------- #
def _lane(name: str, symbols, stamps, *, min_events: int = None) -> dict:
    cards = {}
    for sym in symbols:
        cards[sym] = _cell(sym, stamps, label=f"{sym}_FROZEN",
                           min_events=min_events)
    measured = [c for c in cards.values() if c.get("state") == "MEASURED"]
    reps = [c for c in measured if c.get("replication_state") == "REPLICATES"]
    if not measured:
        state = "DATA_INSUFFICIENT"
    elif reps:
        state = "REPLICATES"
    elif all(c.get("replication_state") == "DATA_INSUFFICIENT"
             for c in measured):
        state = "DATA_INSUFFICIENT"
    else:
        state = "DOES_NOT_REPLICATE"
    return {
        "lane": name, "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES[name],
        "cards": cards,
        "n_markets": len(cards), "n_measured": len(measured),
        "n_replicating": len(reps),
        "replicating_markets": [c["symbol"] for c in reps],
        "replication_state": state,
        "coverage": {s: B.coverage(s) for s in symbols},
    }


def lane_l2_listed_us(stamps=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    out = _lane("L2_LISTED_US", list(C.LISTED_MINUTE_INSTRUMENTS), stamps)
    out["instrument_class"] = "LISTED_ETF"
    out["this_is_not_a_futures_result"] = True
    out["structural_note"] = (
        "every release in the calendar prints BEFORE the US cash equity "
        "open, so every entry in this lane is a pre-market fill and its "
        "estimated spread is a pre-market spread")
    return out


def lane_l3_native_futures(stamps=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    out = _lane("L3_NATIVE_FUTURES", list(C.NATIVE_FUTURES_INSTRUMENTS),
                stamps)
    out["instrument_class"] = "NATIVE_FUTURES"
    out["this_is_the_lane_the_release_was_named_for"] = True
    return out


def lane_l4_owned_breadth(stamps=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    others = [s for s in C.OWNED_MINUTE_INSTRUMENTS
              if s != C.FROZEN_RULE["instrument_of_origin"]]
    out = _lane("L4_OWNED_BREADTH", others, stamps)
    out["cfd_symbols_may_not_be_called_a_futures_replication"] = [
        s for s in others
        if C.OWNED_MINUTE_INSTRUMENTS[s]["class"] == "CFD"]
    return out


# --------------------------------------------------------------------------- #
def run(stamps=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "B", "state": "HISTORICAL_DATA_UNAVAILABLE",
                "why": "no PIT release calendar"}
    identity = ES.identity_check()
    l1 = lane_l1_gold_holdout(stamps)
    l2 = lane_l2_listed_us(stamps)
    l3 = lane_l3_native_futures(stamps)
    l4 = lane_l4_owned_breadth(stamps)

    by_sleeve = {}
    for lane in (l2, l3, l4):
        for sym, card in lane["cards"].items():
            if card.get("state") != "MEASURED":
                continue
            by_sleeve.setdefault(card.get("sleeve", "UNKNOWN"), []).append({
                "symbol": sym, "class": card.get("instrument_class"),
                "n_events": card["n_events"],
                "net_bps_per_event": card["net_bps_per_event"],
                "net_t_cluster": card["net_t_cluster"],
                "replication_state": card["replication_state"],
            })
    gold = l1["cards"]["BC"]
    by_sleeve.setdefault("GOLD", []).insert(0, {
        "symbol": "XAUUSD", "class": "OTC_SPOT", "zone": "BC",
        "n_events": gold["n_events"],
        "net_bps_per_event": gold["net_bps_per_event"],
        "net_t_cluster": gold["net_t_cluster"],
        "replication_state": gold["replication_state"],
    })

    reps = sorted(
        [c for lane in (l1, l2, l3, l4)
         for c in ([lane["cards"]["BC"]] if lane["lane"] == "L1_GOLD_HOLDOUT"
                   else lane["cards"].values())
         if c.get("state") == "MEASURED"],
        key=lambda c: -(c.get("net_t_cluster") or -9))

    any_rep = any(c.get("replication_state") == "REPLICATES" for c in reps)
    all_insufficient = all(
        c.get("replication_state") == "DATA_INSUFFICIENT" for c in reps)
    return {
        "track": "B", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "frozen_rule": C.FROZEN_RULE,
        "identity_check": identity,
        "L1_GOLD_HOLDOUT": l1, "L2_LISTED_US": l2,
        "L3_NATIVE_FUTURES": l3, "L4_OWNED_BREADTH": l4,
        "by_sleeve": by_sleeve,
        "ranked": [{k: c.get(k) for k in
                    ("state", "symbol", "sleeve", "instrument_class", "zone",
                     "cost_source", "year_range", "max_event_loss_bps",
                     "largest_event_share_of_pnl",
                     "largest_year_share_of_pnl", "n_events",
                     "gross_bps_per_event", "gross_t", "cost_bps_per_event",
                     "net_bps_per_event", "net_t", "net_t_cluster",
                     "hit_rate", "replication_state", "resolution_degraded",
                     "fill_rate", "fill_rate_in_window",
                     "n_stamps_in_panel_window", "panel_window")}
                   for c in reps],
        "FROZEN_R44_RULE_NATIVE_REPLICATION_RESULT":
            "REPLICATES" if any_rep
            else ("DATA_INSUFFICIENT" if all_insufficient
                  else "DOES_NOT_REPLICATE"),
        "n_markets_tested": len(reps),
        "n_markets_replicating": sum(
            1 for c in reps if c.get("replication_state") == "REPLICATES"),
    }
