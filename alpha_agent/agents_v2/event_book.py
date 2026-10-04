r"""alpha_agent.agents_v2.event_book - the canonical EVENT-TIME book (R91).

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION. Everything here
simulates event windows on the certified futures panel and returns numbers.

THE DEFECT THIS EXISTS TO REMOVE (R90)
--------------------------------------
Every book the agent campaigns could stage was a cadence/horizon RANK book.
Information that arrives at irregular instants - a USDA release, a central-bank
decision, a hurricane entering the Gulf, an earthquake under a mining belt -
had to be converted into a weekly cross-sectional score, which destroyed the
economic design (SW_07 was refused in R90 for exactly this: "the harness
supports no event-time 1-2 session holding").

WHAT THIS BOOK IS
-----------------
One event = {event_id, cluster_id, event_timestamp, available_at,
weights{symbol: signed notional per unit capital}}. The book:

    ENTRY        NEXT_CLOSE_AFTER_AVAILABLE - the close of the first session
                 whose date is >= available_at's date, or the NEXT session
                 when available_at is after the entry cutoff (15:00 ET);
                 CLOSE_K_SESSIONS_BEFORE_EVENT - for pre-scheduled releases,
                 the close K sessions before the session the release lands
                 on; refused unless the schedule was available_at that close.
    HOLDING      ``hold`` sessions, one of ALLOWED_HOLDS (1, 2, 5); the window
                 is the preregistered parameter, never chosen from a result.
    EXIT         the close ``hold`` sessions after entry.
    COST         FUTURES_PER_MARKET_R38_PLUS_ROLL_V1 from the certified panel:
                 2 x |w| @ cost_per_side (open + close) plus the owner's
                 interior roll charge 2 x cost x |w| per flagged session.
    BENCHMARK    UNCONDITIONAL_SAME_WINDOW (default): the same weights' mean
                 ``hold``-session return over every session of the SAME
                 layer; or CASH (0). "Excess" is measured against it.
    CLUSTERING   events sharing a cluster_id are ONE episode; episodes whose
                 windows overlap are merged into ONE observation (equal-mean
                 of their returns). ``effective_observations`` counts merged
                 observations, never raw events. Duplicate event_ids count 0.
    D / V / L    by ENTRY date against r59.VALIDATION_START / LOCKBOX_START;
                 an event whose window crosses the next boundary is PURGED
                 (embargo). Measuring stage S resolves and computes ONLY the
                 events of layers up to S - a lockbox event is never touched
                 while D or V is being read.
    STATISTICS   alpha_agent.r57.engine.nw_tstat on non-overlapping
                 observations (lag 0 by construction), drawdowns from the
                 canonical owner alpha_agent.r61.drawdown; annualisation by
                 observations per year of the layer.
    PLACEBO      ``placebo_seed`` re-draws every episode's entry uniformly
                 within its OWN layer (the same rule on random days); the
                 runner then cost-matches it exactly as for rank books.

What it does NOT do: read revised future values as historical event values
(the caller passes available_at; this book trusts and records it), take any
"as of today" argument (there is none - forward observation has one door,
AgentPipeline.request_forward_observation), or report a layer it did not earn.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Callable, Optional

import numpy as np
import pandas as pd

from .. import r59
from ..r57 import engine as K
from ..r61 import drawdown as DD

CALCULATION_OWNER = "alpha_agent.agents_v2.event_book"
EVENT_BOOK_VERSION = "R91_EVENT_WINDOW_BOOK_V1"
BOOK_EVENT = "FUTURES_EVENT_WINDOW"
COST_MODEL = "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1"
ET = "America/New_York"

ENTRY_NEXT_CLOSE = "NEXT_CLOSE_AFTER_AVAILABLE"
ENTRY_BEFORE_EVENT = "CLOSE_K_SESSIONS_BEFORE_EVENT"
ENTRY_RULES = (ENTRY_NEXT_CLOSE, ENTRY_BEFORE_EVENT)
BENCH_CASH = "CASH"
BENCH_UNCONDITIONAL = "UNCONDITIONAL_SAME_WINDOW"
BENCHMARKS = (BENCH_CASH, BENCH_UNCONDITIONAL)
#: The holding windows a pre-registration may name. Fixed here so a window can
#: never be invented after a layer is read.
ALLOWED_HOLDS = (1, 2, 5)
DEFAULT_CUTOFF_ET = "15:00"
#: Event fields every record must carry.
REQUIRED_EVENT_KEYS = ("event_id", "cluster_id", "event_timestamp",
                       "available_at", "weights")


class EventBookRefusal(RuntimeError):
    """The event book could not be measured as asked."""


# --------------------------------------------------------------------------- #
# Calendar arithmetic
# --------------------------------------------------------------------------- #
def _dates64(dates) -> np.ndarray:
    return np.asarray(dates).astype("datetime64[D]")


def _to_et(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.tz_convert(ET)


def _cutoff(cutoff_et: str) -> tuple:
    hh, mm = (int(x) for x in str(cutoff_et).split(":"))
    return hh, mm


def effective_cutoff(event: dict, *, cutoff_et: str = DEFAULT_CUTOFF_ET,
                     leg_cutoff_et: Optional[dict] = None) -> str:
    """The cutoff that binds an event: the EARLIEST settlement among its legs.

    R91 skeptic finding D2: one 15:00 ET cutoff for every leg lets an event
    published at 14:00 ET be 'entered' at a copper settlement fixed at 13:00
    ET or a Nikkei close fixed in Asia - a price set before the news existed.
    ``leg_cutoff_et`` maps symbol -> "HH:MM" ET settlement; the event's
    cutoff is the minimum over its non-zero legs, never later than the rule's.
    """
    best = _cutoff(cutoff_et)
    for sym, w in (event.get("weights") or {}).items():
        if w in (None, 0, 0.0):
            continue
        c = (leg_cutoff_et or {}).get(str(sym))
        if c:
            best = min(best, _cutoff(c))
    return "%02d:%02d" % best


def entry_index(dates64: np.ndarray, event: dict, *, hold: int,
                entry_rule: str = ENTRY_NEXT_CLOSE, pre_event_sessions: int = 1,
                cutoff_et: str = DEFAULT_CUTOFF_ET,
                leg_cutoff_et: Optional[dict] = None) -> tuple:
    """``(t, reason)``: the entry session index or ``(None, why)``.

    Returns are earned over ``t+1 .. t+hold`` (the panel's own convention:
    decide at the close of t, enter at that close).
    """
    n = len(dates64)
    avail = _to_et(event["available_at"])
    if entry_rule == ENTRY_NEXT_CLOSE:
        day = np.datetime64(avail.date().isoformat(), "D")
        i = int(np.searchsorted(dates64, day, side="left"))
        hh, mm = _cutoff(effective_cutoff(event, cutoff_et=cutoff_et,
                                          leg_cutoff_et=leg_cutoff_et))
        after_cutoff = (avail.hour, avail.minute, avail.second) > (hh, mm, 0)
        if i < n and dates64[i] == day and not after_cutoff:
            t = i
        else:
            t = int(np.searchsorted(dates64, day, side="right"))
    elif entry_rule == ENTRY_BEFORE_EVENT:
        ev = _to_et(event["event_timestamp"])
        day = np.datetime64(ev.date().isoformat(), "D")
        i = int(np.searchsorted(dates64, day, side="left"))   # the release session
        t = i - int(pre_event_sessions)
        if t < 0:
            return None, "OUT_OF_PANEL"
        avail_day = np.datetime64(avail.date().isoformat(), "D")
        if avail_day > dates64[t]:
            return None, "SCHEDULE_NOT_YET_AVAILABLE_AT_ENTRY"
    else:
        raise EventBookRefusal("unknown entry rule %r (one of %s)"
                               % (entry_rule, ENTRY_RULES))
    if t < 0 or t + int(hold) >= n:
        return None, "OUT_OF_PANEL"
    return int(t), "OK"


def layer_of_entry(dates64: np.ndarray, t: int, hold: int) -> Optional[str]:
    """D / V / L by entry date; ``None`` when the window crosses the next
    boundary (embargo purge) or the entry precedes the discovery start."""
    d_entry = dates64[int(t)]
    d_exit = dates64[int(t) + int(hold)]
    d0 = np.datetime64(r59.DISCOVERY_START, "D")
    v0 = np.datetime64(r59.VALIDATION_START, "D")
    l0 = np.datetime64(r59.LOCKBOX_START, "D")
    if d_entry < d0:
        return None
    if d_entry < v0:
        return "D" if d_exit < v0 else None
    if d_entry < l0:
        return "V" if d_exit < l0 else None
    return "L"


def _layer_bounds(dates64: np.ndarray, layer: str) -> tuple:
    d0 = np.datetime64(r59.DISCOVERY_START, "D")
    v0 = np.datetime64(r59.VALIDATION_START, "D")
    l0 = np.datetime64(r59.LOCKBOX_START, "D")
    if layer == "D":
        return d0, v0
    if layer == "V":
        return v0, l0
    return l0, dates64[-1] + np.timedelta64(1, "D")


def _layer_years(dates64: np.ndarray, layer: str) -> float:
    a, b = _layer_bounds(dates64, layer)
    return max(1e-9, float((b - a) / np.timedelta64(1, "D")) / 365.25)


def _window_cum(ret: np.ndarray, t: int, hold: int) -> np.ndarray:
    """Compound return of every market over ``t+1 .. t+hold``; a missing
    session earns zero (the owner's ``_fwd`` rule)."""
    w = ret[:, t + 1:t + 1 + hold]
    r0 = np.where(np.isfinite(w), w, 0.0)
    return np.prod(1.0 + r0, axis=1) - 1.0


def _unconditional_window_mean(ret: np.ndarray, dates64: np.ndarray, layer: str,
                               hold: int) -> np.ndarray:
    """Per-market mean ``hold``-session window return over every entry session
    of ``layer`` whose window stays inside the layer. The event-time benchmark."""
    a, b = _layer_bounds(dates64, layer)
    n = len(dates64)
    lo = int(np.searchsorted(dates64, a, side="left"))
    hi = int(np.searchsorted(dates64, b, side="left"))
    acc = np.zeros(ret.shape[0])
    cnt = np.zeros(ret.shape[0])
    for t in range(lo, hi):
        if t + hold >= n or dates64[t + hold] >= b:
            break
        c = _window_cum(ret, t, hold)
        fin = np.isfinite(ret[:, t])
        acc += np.where(fin, c, 0.0)
        cnt += fin
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0)
    return out


# --------------------------------------------------------------------------- #
# Events -> observations
# --------------------------------------------------------------------------- #
def make_events_fn(events: list) -> Callable:
    """The plan callable for this book: ``events_fn(stage) -> list[event]``."""
    rows = list(events)

    def _fn(stage: str = "L") -> list:
        return rows

    _fn.__event_book__ = True
    return _fn


def permuted_events_fn(events_fn: Callable, *, seed: int) -> Callable:
    """The placebo: the same events, re-dated uniformly inside their own
    layer by the book (``placebo_seed``). Marked so ``run_stage`` passes the
    seed to the book instead of shuffling a numeric vector."""
    def _fn(stage: str = "L") -> list:
        return events_fn(stage)

    _fn.__event_book__ = True
    _fn.__placebo_seed__ = int(seed)
    return _fn


def _resolve(events: list, *, dates64, symbols: list, ret: np.ndarray,
             hold: int, stage: str, entry_rule: str, pre_event_sessions: int,
             cutoff_et: str, leg_cutoff_et: Optional[dict] = None) -> tuple:
    allowed = r59.STAGES[:r59.STAGES.index(stage) + 1]
    sym_index = {str(s): i for i, s in enumerate(symbols)}
    n_m = len(symbols)
    seen = set()
    dropped: Counter = Counter()
    resolved = []
    for ev in events:
        missing = [k for k in REQUIRED_EVENT_KEYS if k not in ev]
        if missing:
            raise EventBookRefusal("event missing %s: %r" % (missing, ev))
        eid = str(ev["event_id"])
        if eid in seen:
            dropped["DUPLICATE_EVENT_ID"] += 1
            continue
        seen.add(eid)
        w = np.zeros(n_m)
        for s, x in (ev.get("weights") or {}).items():
            j = sym_index.get(str(s))
            if j is not None and x is not None and np.isfinite(float(x)):
                w[j] = float(x)
        if not np.any(w != 0.0):
            dropped["NO_TRADABLE_LEG"] += 1
            continue
        t, why = entry_index(dates64, ev, hold=hold, entry_rule=entry_rule,
                             pre_event_sessions=pre_event_sessions,
                             cutoff_et=cutoff_et, leg_cutoff_et=leg_cutoff_et)
        if t is None:
            dropped[why] += 1
            continue
        lay = layer_of_entry(dates64, t, hold)
        if lay is None:
            dropped["BOUNDARY_PURGE_OR_PRE_DISCOVERY"] += 1
            continue
        if lay not in allowed:
            # LOCKBOX PROTECTION: nothing of a later layer is computed.
            dropped["BEYOND_STAGE_%s" % stage] += 1
            continue
        live = np.isfinite(ret[:, t])
        w = np.where(live, w, 0.0)
        if not np.any(w != 0.0):
            dropped["NOT_LIVE_AT_ENTRY"] += 1
            continue
        resolved.append({"event_id": eid, "cluster_id": str(ev["cluster_id"]),
                         "t": int(t), "layer": lay, "w": w,
                         "available_at": str(ev["available_at"]),
                         "event_timestamp": str(ev["event_timestamp"])})
    return resolved, dropped


def _placebo_redate(resolved: list, *, dates64, hold: int, seed: int) -> list:
    """Every EPISODE (cluster) is moved to a uniformly random entry inside its
    own layer; members keep their relative offsets. Weights are untouched."""
    rng = np.random.default_rng(int(seed))
    by_cluster: dict = {}
    for r in resolved:
        by_cluster.setdefault((r["layer"], r["cluster_id"]), []).append(r)
    out = []
    n = len(dates64)
    for (lay, _cid), members in by_cluster.items():
        a, b = _layer_bounds(dates64, lay)
        lo = int(np.searchsorted(dates64, a, side="left"))
        hi = int(np.searchsorted(dates64, b, side="left"))
        t0 = min(m["t"] for m in members)
        span = max(m["t"] for m in members) - t0
        hi_ok = hi - hold - span - 1
        if hi_ok <= lo:
            continue
        new0 = int(rng.integers(lo, hi_ok))
        for m in members:
            t = new0 + (m["t"] - t0)
            if t + hold >= n or layer_of_entry(dates64, t, hold) != lay:
                continue
            out.append({**m, "t": int(t)})
    return out


def _episodes(resolved: list, *, ret, roll, costs, dates64, hold: int,
              cost_mult: float, charge_rolls: bool, bench_mean: dict) -> list:
    """Per-event economics, then per-cluster means."""
    rows = []
    for r in resolved:
        t, w = r["t"], r["w"]
        gross = float(w @ _window_cum(ret, t, hold))
        open_close = 2.0 * float(np.abs(w) @ costs)
        rollc = 0.0
        if charge_rolls and hold >= 2:
            lo, hi = t + 2, min(t + hold + 1, ret.shape[1])
            interior = roll[:, lo:hi].astype(np.float64)
            rollc = float((interior * (2.0 * np.abs(w) * costs)[:, None]).sum())
        cost = cost_mult * (open_close + rollc)
        bench = float(w @ bench_mean[r["layer"]])
        rows.append({**r, "gross": gross, "cost": cost,
                     "rebalance_cost": cost_mult * open_close,
                     "roll_cost": cost_mult * rollc,
                     "net": gross - cost, "bench": bench,
                     "oneway_turnover": float(np.abs(w).sum()),
                     "n_positions": int((w != 0.0).sum()),
                     "t_exit": t + hold})
    by: dict = {}
    for r in rows:
        by.setdefault((r["layer"], r["cluster_id"]), []).append(r)
    episodes = []
    for (lay, cid), members in by.items():
        episodes.append({
            "layer": lay, "cluster_id": cid, "n_events": len(members),
            "event_ids": [m["event_id"] for m in members],
            "start": min(m["t"] for m in members) + 1,
            "end": max(m["t_exit"] for m in members),
            "entry_date": str(dates64[min(m["t"] for m in members)]),
            **{k: float(np.mean([m[k] for m in members]))
               for k in ("gross", "cost", "rebalance_cost", "roll_cost",
                         "net", "bench", "oneway_turnover", "n_positions")}})
    episodes.sort(key=lambda e: (e["start"], e["cluster_id"]))
    return episodes


def merge_overlaps(episodes: list) -> list:
    """Overlapping episode windows -> ONE observation (equal mean)."""
    obs = []
    for e in episodes:
        if obs and obs[-1]["layer"] == e["layer"] and e["start"] <= obs[-1]["end"]:
            cur = obs[-1]
            cur["members"].append(e)
            cur["end"] = max(cur["end"], e["end"])
            continue
        obs.append({"layer": e["layer"], "start": e["start"], "end": e["end"],
                    "members": [e]})
    out = []
    for o in obs:
        ms = o["members"]
        row = {"layer": o["layer"], "start": o["start"], "end": o["end"],
               "n_episodes": len(ms),
               "n_events": int(sum(m["n_events"] for m in ms)),
               "cluster_ids": [m["cluster_id"] for m in ms],
               "entry_date": ms[0]["entry_date"]}
        for k in ("gross", "cost", "rebalance_cost", "roll_cost", "net", "bench",
                  "oneway_turnover", "n_positions"):
            row[k] = float(np.mean([m[k] for m in ms]))
        out.append(row)
    return out


# --------------------------------------------------------------------------- #
# Layer statistics
# --------------------------------------------------------------------------- #
def layer_stats(obs: list, *, dates64, layer: str, hold: int,
                n_raw_events: int, n_episodes: int, benchmark: str,
                entry_rule: str) -> dict:
    if not obs:
        return {"periods": 0, "effective_observations": 0, "raw_events": n_raw_events,
                "episodes": n_episodes, "hold_sessions": int(hold),
                "benchmark": benchmark, "entry_rule": entry_rule}
    net = np.array([o["net"] for o in obs])
    gross = np.array([o["gross"] for o in obs])
    bench = np.array([o["bench"] for o in obs])
    cost = np.array([o["cost"] for o in obs])
    x_net, x_gross = net - bench, gross - bench
    n = len(obs)
    opy = n / _layer_years(dates64, layer)
    st = K.nw_tstat(x_net, lag=0)
    dd = DD.layer_drawdowns(strategy_returns=net, benchmark_returns=bench)
    sd = float(x_net.std())
    half = n // 2
    return {
        "periods": n, "effective_observations": n,
        "raw_events": int(n_raw_events), "episodes": int(n_episodes),
        "merged_overlapping_episodes": int(n_episodes - n),
        "observations_per_year": float(opy),
        "first": obs[0]["entry_date"], "last": obs[-1]["entry_date"],
        "ann_net_excess": float(x_net.mean() * opy),
        "ann_gross_excess": float(x_gross.mean() * opy),
        "ann_strat_net": float(net.mean() * opy),
        "ann_bench_net": float(bench.mean() * opy),
        "ann_cost_drag": float(cost.mean() * opy),
        "ann_rebalance_cost_drag": float(np.mean([o["rebalance_cost"] for o in obs]) * opy),
        "ann_roll_cost_drag": float(np.mean([o["roll_cost"] for o in obs]) * opy),
        "mean_event_net_excess": float(x_net.mean()),
        "mean_event_gross_excess": float(x_gross.mean()),
        "mean_oneway_turnover_per_period": float(np.mean([o["oneway_turnover"] for o in obs])),
        "ann_oneway_turnover": float(np.mean([o["oneway_turnover"] for o in obs]) * opy),
        "ann_vol_net": float(sd * math.sqrt(opy)) if n > 1 else None,
        "period_ir_ann": (float(x_net.mean()) / sd * math.sqrt(opy)
                          if n > 1 and sd > 0 else None),
        DD.STRATEGY_MAX_DRAWDOWN: dd[DD.STRATEGY_MAX_DRAWDOWN],
        DD.BENCHMARK_MAX_DRAWDOWN: dd[DD.BENCHMARK_MAX_DRAWDOWN],
        DD.EXCESS_MAX_DRAWDOWN: dd[DD.EXCESS_MAX_DRAWDOWN],
        "drawdown_owner": dd["drawdown_owner"],
        "strat_max_dd": dd[DD.STRATEGY_MAX_DRAWDOWN],
        "bench_max_dd": dd[DD.BENCHMARK_MAX_DRAWDOWN],
        "hit_rate": float((x_net > 0).mean()),
        "t_net_excess": st["t"], "p_one_sided": st["p_one_sided"],
        "halves_ann_net_excess": ([float(x_net[:half].mean() * opy),
                                   float(x_net[half:].mean() * opy)]
                                  if n >= 4 else None),
        "median_positions": float(np.median([o["n_positions"] for o in obs])),
        "mean_gross_notional": float(np.mean([o["oneway_turnover"] for o in obs])),
        "flat_decisions": 0,
        "hold_sessions": int(hold), "benchmark": benchmark, "entry_rule": entry_rule,
        "statistics_owner": "alpha_agent.r57.engine.nw_tstat (lag 0: observations "
                            "are non-overlapping by construction)",
    }


# --------------------------------------------------------------------------- #
# The book
# --------------------------------------------------------------------------- #
def run_event_window_book(layer: dict, events_or_fn, *, stage: str, hold: int,
                          label: str = "", cost_mult: float = 1.0,
                          charge_rolls: bool = True,
                          event_rule: Optional[dict] = None,
                          placebo_seed: Optional[int] = None) -> dict:
    """Measure every layer UP TO ``stage`` of one event book.

    ``layer``  the certified panel (``dates``, ``symbols``, ``ret``, ``roll``,
               ``cost_per_side``).
    ``events_or_fn``  a list of events or ``events_fn(stage) -> list``.
    ``event_rule``    {entry_rule, pre_event_sessions, benchmark, cutoff_et};
                      frozen in the pre-registered plan.
    """
    if stage not in r59.STAGES:
        raise EventBookRefusal("unknown stage %r" % (stage,))
    hold = int(hold)
    if hold not in ALLOWED_HOLDS:
        raise EventBookRefusal(
            "holding window %d is not one of the pre-registrable windows %s"
            % (hold, ALLOWED_HOLDS))
    rule = dict(event_rule or {})
    entry_rule = str(rule.get("entry_rule") or ENTRY_NEXT_CLOSE)
    if entry_rule not in ENTRY_RULES:
        raise EventBookRefusal("unknown entry rule %r" % (entry_rule,))
    benchmark = str(rule.get("benchmark") or BENCH_UNCONDITIONAL)
    if benchmark not in BENCHMARKS:
        raise EventBookRefusal("unknown benchmark %r" % (benchmark,))
    pre_k = int(rule.get("pre_event_sessions", 1))
    cutoff_et = str(rule.get("cutoff_et") or DEFAULT_CUTOFF_ET)
    leg_cutoff_et = dict(rule.get("leg_cutoff_et") or {})
    for key in ("as_of", "backdate", "effective_from", "inception", "start_date"):
        if key in rule:
            raise EventBookRefusal("%s is not an argument of this book" % key)

    dates64 = _dates64(layer["dates"])
    symbols = [str(s) for s in layer["symbols"]]
    ret = np.asarray(layer["ret"], dtype=np.float64)
    roll = np.asarray(layer.get("roll") if layer.get("roll") is not None
                      else np.zeros_like(ret, dtype=np.uint8))
    costs = np.asarray(layer["cost_per_side"], dtype=np.float64)
    events = events_or_fn(stage) if callable(events_or_fn) else list(events_or_fn)
    if callable(events_or_fn) and placebo_seed is None:
        placebo_seed = getattr(events_or_fn, "__placebo_seed__", None)

    resolved, dropped = _resolve(
        events, dates64=dates64, symbols=symbols, ret=ret, hold=hold, stage=stage,
        entry_rule=entry_rule, pre_event_sessions=pre_k, cutoff_et=cutoff_et,
        leg_cutoff_et=leg_cutoff_et)
    if placebo_seed is not None:
        resolved = _placebo_redate(resolved, dates64=dates64, hold=hold,
                                   seed=int(placebo_seed))
    allowed = r59.STAGES[:r59.STAGES.index(stage) + 1]
    if not resolved:
        raise EventBookRefusal("no event resolves inside layers %s: dropped %s"
                               % (list(allowed), dict(dropped)))
    bench_mean = {lay: (_unconditional_window_mean(ret, dates64, lay, hold)
                        if benchmark == BENCH_UNCONDITIONAL
                        else np.zeros(ret.shape[0]))
                  for lay in allowed}
    episodes = _episodes(resolved, ret=ret, roll=roll, costs=costs, dates64=dates64,
                         hold=hold, cost_mult=float(cost_mult),
                         charge_rolls=bool(charge_rolls), bench_mean=bench_mean)
    obs = merge_overlaps(episodes)
    layers = {}
    for lay in allowed:
        o = [x for x in obs if x["layer"] == lay]
        layers[lay] = layer_stats(
            o, dates64=dates64, layer=lay, hold=hold,
            n_raw_events=sum(1 for r in resolved if r["layer"] == lay),
            n_episodes=sum(1 for e in episodes if e["layer"] == lay),
            benchmark=benchmark, entry_rule=entry_rule)
    for name, row in layers.items():
        if "net_sharpe" in row and "ann_net_excess" in row:
            raise AssertionError("layer %s carries net_sharpe AND ann_net_excess" % name)
    return {
        "state": "MEASURED", "label": label or ("%s:%s" % (BOOK_EVENT, stage)),
        "book": BOOK_EVENT, "version": EVENT_BOOK_VERSION,
        "evaluator": ("alpha_agent.agents_v2.event_book.run_event_window_book"
                      " + alpha_agent.r57.engine.nw_tstat + alpha_agent.r61.drawdown"),
        "cost_model": COST_MODEL, "cost_mult": float(cost_mult),
        "charge_rolls": bool(charge_rolls), "hold": hold,
        "entry_rule": entry_rule, "pre_event_sessions": pre_k,
        "cutoff_et": cutoff_et, "leg_cutoff_et": leg_cutoff_et,
        "benchmark": benchmark,
        "stage_computed": stage, "layers_computed": list(allowed),
        "placebo_seed": placebo_seed,
        "n_events_in": len(events), "n_events_resolved": len(resolved),
        "n_episodes": len(episodes), "n_observations": len(obs),
        "dropped": dict(dropped), "layers": layers,
        "series": {"observations": [{k: v for k, v in o.items()} for o in obs],
                   "episodes": [{k: v for k, v in e.items()} for e in episodes]},
        "lockbox_protection": ("events whose entry falls in a layer after %s were "
                               "neither resolved nor computed" % stage),
    }


def unconditional_window_vol(layer: dict, weights: dict, *, hold: int,
                             layer_name: str) -> Optional[float]:
    """The FROZEN EX-ANTE per-observation volatility of ``weights`` over
    every ``hold``-session window of ``layer_name`` (R92). Reads the panel
    unconditionally - never an event-conditioned return - so it may be
    computed before pre-registration and frozen into the power sample."""
    dates64 = _dates64(layer["dates"])
    symbols = [str(s) for s in layer["symbols"]]
    ret = np.asarray(layer["ret"], dtype=np.float64)
    w = np.zeros(len(symbols))
    for s, x in (weights or {}).items():
        if s in symbols and x is not None and np.isfinite(float(x)):
            w[symbols.index(s)] = float(x)
    if not np.any(w != 0.0):
        return None
    a, b = _layer_bounds(dates64, layer_name)
    lo = int(np.searchsorted(dates64, a, side="left"))
    hi = int(np.searchsorted(dates64, b, side="left"))
    vals = []
    for t in range(lo, hi):
        if t + hold >= len(dates64) or dates64[t + hold] >= b:
            break
        live = np.isfinite(ret[:, t]) & (w != 0.0)
        if not live.any():
            continue
        vals.append(float(np.where(live, w, 0.0) @ _window_cum(ret, t, hold)))
    if len(vals) < 2:
        return None
    return float(np.std(np.asarray(vals), ddof=1))


def pre_measurement_sample(layer: dict, events: list, *, hold: int,
                           event_rule: Optional[dict] = None) -> dict:
    """Per-layer RAW events, CLUSTERS and MERGED independent observations of
    an event list, plus observations per year, WITHOUT reading any return
    (R92). This is the sample the power owner needs before a layer is read:
    resolution uses liveness only (``_resolve``), windows come from the entry
    index and the hold, and merging follows ``merge_overlaps``' rule."""
    rule = dict(event_rule or {})
    dates64 = _dates64(layer["dates"])
    symbols = [str(s) for s in layer["symbols"]]
    ret = np.asarray(layer["ret"], dtype=np.float64)
    resolved, dropped = _resolve(
        list(events), dates64=dates64, symbols=symbols, ret=ret, hold=int(hold),
        stage="L", entry_rule=str(rule.get("entry_rule") or ENTRY_NEXT_CLOSE),
        pre_event_sessions=int(rule.get("pre_event_sessions", 1)),
        cutoff_et=str(rule.get("cutoff_et") or DEFAULT_CUTOFF_ET),
        leg_cutoff_et=dict(rule.get("leg_cutoff_et") or {}))
    out = {"hold": int(hold), "dropped": dict(dropped), "layers": {}}
    for lay in r59.STAGES:
        rows = [r for r in resolved if r["layer"] == lay]
        by: dict = {}
        for r in rows:
            by.setdefault(r["cluster_id"], []).append(r)
        eps = sorted((min(m["t"] for m in ms) + 1,
                      max(m["t"] for m in ms) + int(hold)) for ms in by.values())
        merged = 0
        end = None
        for s, e in eps:
            if end is not None and s <= end:
                end = max(end, e)
                continue
            merged += 1
            end = e
        years = _layer_years(dates64, lay)
        out["layers"][lay] = {"raw_events": len(rows), "clusters": len(by),
                              "merged_observations": merged,
                              "layer_years": years,
                              "observations_per_year": merged / years}
    return out


def check_frozen_parameters(spec: dict, plan: dict) -> list:
    """Why a plan may NOT run: its event window or rule differs from what the
    pre-registration froze. Empty list = consistent."""
    params = (spec or {}).get("parameters") or {}
    kw = (plan or {}).get("book_kwargs") or {}
    problems = []
    if "holding_window_sessions" in params:
        if int(kw.get("hold", -1)) != int(params["holding_window_sessions"]):
            problems.append("holding window %s differs from the pre-registered %s"
                            % (kw.get("hold"), params["holding_window_sessions"]))
    frozen_rule = params.get("event_rule") or {}
    plan_rule = kw.get("event_rule") or {}

    def _canon(v):
        # A mapping is the same rule whatever its key order (the frozen copy
        # travels through JSON); scalars compare as strings.
        import json as _json
        return _json.dumps(v, sort_keys=True, default=str) if isinstance(v, (dict, list)) else str(v)

    for k in ("entry_rule", "pre_event_sessions", "benchmark", "cutoff_et",
              "leg_cutoff_et"):
        if k in frozen_rule and _canon(plan_rule.get(k)) != _canon(frozen_rule[k]):
            problems.append("%s %r differs from the pre-registered %r"
                            % (k, plan_rule.get(k), frozen_rule[k]))
    return problems
