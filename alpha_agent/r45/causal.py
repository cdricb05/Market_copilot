"""alpha_agent.r45.causal - Tracks C and D. Is it the release, or the clock?

Release 44 produced two pieces of evidence that its gold effect was causal: a
non-release-day placebo that lost money, and a timing sweep that peaked
exactly at the declared release minute and died within a minute either side.
Both were computed on the SAME zone A the rule's parameters were chosen on.

That is the flaw this module is built to expose. A maximum found by search
will always look locally peaked when you sweep around it, and a placebo will
always look weak next to a number that was selected for being large. So every
control here is run on zone A, on the never-scored holdout, and on the full
sample - and the three are reported side by side. If the causal signature
lives only where the search lived, it was a signature of the search.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.causal"

#: Declared, so the draw is reproducible and nobody has to trust a hash seed.
PLACEBO_SEED = 45_000_045
N_RANDOM_DATE_DRAWS = 200
TIMING_OFFSETS_MIN = (-60, -30, -15, -5, -2, -1, 0, 1, 2, 5, 15, 30, 60)


# --------------------------------------------------------------------------- #
# Placebos
# --------------------------------------------------------------------------- #
def _shifted_stamps(stamps: pd.DataFrame, days: int) -> pd.DataFrame:
    s = stamps.copy()
    s["stamp_utc"] = s["stamp_utc"] + pd.Timedelta(days=days)
    s["date"] = pd.to_datetime(s["date"]) + pd.Timedelta(days=days)
    real = set(pd.to_datetime(stamps["date"]).dt.date)
    return s[~s["date"].dt.date.isin(real)]


def placebo_shifted_days(symbol: str, stamps: pd.DataFrame, *, days: int,
                         zone: str = None) -> dict:
    """The SAME rule at the SAME clock time, a whole week away.

    Shifting by multiples of seven keeps time of day, day of week and
    seasonal position, and removes only the release.
    """
    s = _shifted_stamps(stamps, days)
    ev = ES.event_book(symbol, s)
    if ev is None:
        return {"state": "NO_EVENTS", "shift_days": days}
    if zone:
        ev = _restrict_to_zone_dates(ev, symbol, stamps, zone)
    card = ES.score(ev, label=f"PLACEBO_SHIFT_{days:+d}D")
    card.update({"placebo": True, "kind": "SHIFTED_CALENDAR",
                 "shift_days": days})
    return card


def placebo_random_dates(symbol: str, stamps: pd.DataFrame, *,
                         draws: int = N_RANDOM_DATE_DRAWS,
                         zone: str = None) -> dict:
    """Random business days at the same declared clock times.

    The rule is scored on ``draws`` synthetic calendars; the true result is
    then read as a percentile of that distribution, which is a far harder
    test than beating one shifted week.
    """
    rng = np.random.default_rng(PLACEBO_SEED)
    d = pd.to_datetime(stamps["date"])
    pool = pd.bdate_range(d.min(), d.max())
    real = set(d.dt.date)
    pool = pd.DatetimeIndex([x for x in pool if x.date() not in real])
    if len(pool) < 50:
        return {"state": "NO_EVENTS"}
    times = stamps["declared_time_et"].to_numpy()
    n = len(stamps)
    nets = []
    for _ in range(int(draws)):
        pick = pool[rng.integers(0, len(pool), size=n)]
        s = pd.DataFrame({
            "event": stamps["event"].to_numpy(),
            "date": pick,
            "declared_time_et": times,
        })
        loc = pd.DatetimeIndex(pick).tz_localize(ES.ET)
        hh = np.array([int(t.split(":")[0]) for t in times])
        mm = np.array([int(t.split(":")[1]) for t in times])
        s["stamp_utc"] = (loc + pd.to_timedelta(hh, unit="h")
                          + pd.to_timedelta(mm, unit="m")).tz_convert("UTC")
        s = s.sort_values("stamp_utc")
        ev = ES.event_book(symbol, s)
        if ev is None or len(ev) < 30:
            continue
        if zone:
            ev = _restrict_to_zone_dates(ev, symbol, stamps, zone)
            if ev is None or len(ev) < 30:
                continue
        _, _, net = ES.net_series(ev)
        nets.append(float(np.nanmean(net) * 1e4))
    if len(nets) < 20:
        return {"state": "NO_EVENTS", "n_draws_scored": len(nets)}
    arr = np.asarray(nets, dtype=float)
    return {"state": "MEASURED", "placebo": True, "kind": "RANDOM_DATES",
            "n_draws": int(arr.size), "seed": PLACEBO_SEED,
            "mean_bps": float(arr.mean()), "sd_bps": float(arr.std(ddof=1)),
            "p05_bps": float(np.percentile(arr, 5)),
            "p95_bps": float(np.percentile(arr, 95)),
            "max_bps": float(arr.max()), "min_bps": float(arr.min())}


def placebo_label_permutation(ev: pd.DataFrame, *, draws: int = 2000) -> dict:
    """Permute the SIGN the rule takes, keeping every price move intact.

    This asks whether the direction the rule chooses carries information, or
    whether the same event returns would have produced the same average under
    any sign assignment.
    """
    if ev is None or len(ev) < 30:
        return {"state": "NO_EVENTS"}
    rng = np.random.default_rng(PLACEBO_SEED + 1)
    gross, cost, net = ES.net_series(ev)
    truth = float(np.nanmean(net) * 1e4)
    fwd = ev["forward"].to_numpy(dtype=float)
    n = fwd.size
    sims = np.empty(int(draws), dtype=float)
    for i in range(int(draws)):
        sgn = rng.choice((-1.0, 1.0), size=n)
        sims[i] = float(np.nanmean(sgn * fwd - cost) * 1e4)
    p = float(np.mean(sims >= truth))
    return {"state": "MEASURED", "placebo": True, "kind": "SIGN_PERMUTATION",
            "n_draws": int(draws), "seed": PLACEBO_SEED + 1,
            "true_net_bps": truth, "permuted_mean_bps": float(sims.mean()),
            "permuted_p95_bps": float(np.percentile(sims, 95)),
            "p_value_one_sided": p}


def _restrict_to_zone_dates(ev, symbol, true_stamps, zone):
    """Keep placebo events inside the calendar span of the named true zone."""
    ref = ES.event_book(symbol, true_stamps)
    if ref is None:
        return ev
    z = ES.zone_of(ref)
    rng = {"A": z["a_range"], "B": z["b_range"], "C": z["c_range"]}.get(zone)
    if zone == "BC":
        rng = (z["b_range"][0], z["c_range"][1]) if z["b_range"] else None
    if not rng:
        return ev
    d = pd.to_datetime(ev["date"])
    out = ev[(d >= pd.Timestamp(rng[0])) & (d <= pd.Timestamp(rng[1]))].copy()
    out.attrs.update(ev.attrs)
    return out


# --------------------------------------------------------------------------- #
# Timing
# --------------------------------------------------------------------------- #
def timing_sweep(symbol: str, stamps: pd.DataFrame, *, zone: str = None,
                 offsets=TIMING_OFFSETS_MIN) -> dict:
    """Pretend the release happened N minutes early or late, and re-score.

    A real release-time effect must die away from the declared minute. A
    selected maximum will also look peaked - which is why this is run on the
    holdout too, where nothing was selected.
    """
    rows = []
    for off in offsets:
        ev = ES.event_book(symbol, stamps, offset_minutes=int(off))
        if ev is None:
            continue
        if zone:
            ev = ES.slice_zone(ev, zone)
        card = ES.score(ev, label=f"TIMING_OFFSET_{off:+d}")
        if card.get("state") != "MEASURED":
            continue
        rows.append({"offset_min": int(off), "n_events": card["n_events"],
                     "gross_bps_per_event": card["gross_bps_per_event"],
                     "net_bps_per_event": card["net_bps_per_event"],
                     "net_t": card["net_t"],
                     "net_t_cluster": card["net_t_cluster"]})
    if not rows:
        return {"state": "NO_EVENTS"}
    best = max(rows, key=lambda r: r["net_bps_per_event"])
    at_zero = next((r for r in rows if r["offset_min"] == 0), None)
    return {
        "state": "MEASURED", "zone": zone or "ALL", "rows": rows,
        "peak_offset_min": best["offset_min"],
        "peak_is_at_the_declared_minute": bool(best["offset_min"] == 0),
        "net_at_declared_minute": at_zero["net_bps_per_event"] if at_zero
        else None,
        "note": "the release minute is a DECLARED CONSTANT; if the effect "
                "survives a large timing error it was never about the "
                "release, and if it only peaks where the search looked it "
                "was never about the release either",
    }


# --------------------------------------------------------------------------- #
# Track D - event families
# --------------------------------------------------------------------------- #
def family_decomposition(symbol: str, stamps: pd.DataFrame, *,
                         zone: str = None, min_events: int = 25) -> dict:
    ev = ES.event_book(symbol, stamps)
    if ev is None:
        return {"state": "NO_EVENTS"}
    if zone:
        ev = ES.slice_zone(ev, zone)
    rows = []
    for name, grp in ev.groupby("event"):
        if len(grp) < int(min_events):
            continue
        g = grp.copy()
        g.attrs.update(ev.attrs)
        card = ES.score(g, label=f"{symbol}_{name}")
        if card.get("state") != "MEASURED":
            continue
        rows.append({"event_family": name, "n_events": card["n_events"],
                     "gross_bps_per_event": card["gross_bps_per_event"],
                     "net_bps_per_event": card["net_bps_per_event"],
                     "net_t_cluster": card["net_t_cluster"],
                     "hit_rate": card["hit_rate"],
                     "shock_bps_mean_abs": card["shock_bps_mean_abs"]})
    rows.sort(key=lambda r: -(r["net_t_cluster"] or -9))
    return {"state": "MEASURED", "symbol": symbol, "zone": zone or "ALL",
            "rows": rows, "n_families": len(rows),
            "warning": "a large t on a small subfamily is a search result, "
                       "not a finding; family selection is charged burden"}


def family_stability(symbol: str, stamps: pd.DataFrame) -> dict:
    """Does the family ranking on zone A survive into the holdout?"""
    a = family_decomposition(symbol, stamps, zone="A")
    bc = family_decomposition(symbol, stamps, zone="BC")
    if a.get("state") != "MEASURED" or bc.get("state") != "MEASURED":
        return {"state": "NO_EVENTS"}
    ra = {r["event_family"]: r for r in a["rows"]}
    rb = {r["event_family"]: r for r in bc["rows"]}
    both = sorted(set(ra) & set(rb))
    if len(both) < 3:
        return {"state": "NO_EVENTS", "n_common": len(both)}
    xa = np.array([ra[k]["net_bps_per_event"] for k in both])
    xb = np.array([rb[k]["net_bps_per_event"] for k in both])
    rank = float(pd.Series(xa).corr(pd.Series(xb), method="spearman"))
    return {
        "state": "MEASURED", "n_common_families": len(both),
        "rows": [{"event_family": k,
                  "zone_a_net_bps": ra[k]["net_bps_per_event"],
                  "zone_bc_net_bps": rb[k]["net_bps_per_event"],
                  "sign_agrees": bool(np.sign(ra[k]["net_bps_per_event"])
                                      == np.sign(rb[k]["net_bps_per_event"]))}
                 for k in both],
        "rank_correlation_a_vs_bc": rank,
        "n_signs_agreeing": int(sum(
            np.sign(xa) == np.sign(xb))),
        "reading": "if the families that worked in the search zone are not "
                   "the families that work afterwards, the decomposition was "
                   "noise being sorted",
    }


# --------------------------------------------------------------------------- #
# How big was the selection premium?
# --------------------------------------------------------------------------- #
#: R44's screen, restated exactly: 3 instruments x 2 delays x 5 holds x 2
#: rules = 60 cells. Nothing here is chosen by Release 45.
R44_SCREEN_INSTRUMENTS = ("EURUSD", "USDJPY", "XAUUSD")
R44_SCREEN_DELAYS = (1, 5)
R44_SCREEN_HOLDS = (5, 15, 30, 60, 120)
R44_SCREEN_RULES = ("REVERSAL", "CONTINUATION")


def selection_premium(stamps: pd.DataFrame, *, charge=None) -> dict:
    """Run R44's whole 60-cell screen on each zone and compare the winners.

    The obvious alternative to "R44 selected a maximum" is "the world changed
    after 2018". This separates them. If the effect were real and then
    stopped, the best of sixty cells on the later events would be visibly
    worse than the best of sixty on the earlier ones. If instead the best of
    sixty is about as flattering in BOTH halves - just at a different cell
    each time - then what R44 measured was the height of a maximum over
    sixty draws, and this number is its size.
    """
    out = {}
    for zone in ("A", "B", "C"):
        cells = []
        for sym in R44_SCREEN_INSTRUMENTS:
            for delay in R44_SCREEN_DELAYS:
                for hold in R44_SCREEN_HOLDS:
                    ev = ES.event_book(sym, stamps, entry_delay=delay,
                                       hold=hold)
                    if ev is None or len(ev) < 100:
                        continue
                    sub = ES.slice_zone(ev, zone)
                    if len(sub) < 60:
                        continue
                    for rule in R44_SCREEN_RULES:
                        card = ES.score(sub, rule=rule,
                                        label=f"{sym}_d{delay}_h{hold}_{rule}")
                        if card.get("state") != "MEASURED":
                            continue
                        cells.append({
                            "symbol": sym, "entry_delay_min": delay,
                            "hold_min": hold, "rule": rule,
                            "n_events": card["n_events"],
                            "gross_bps_per_event": card["gross_bps_per_event"],
                            "gross_t": card["gross_t"],
                            "net_bps_per_event": card["net_bps_per_event"],
                            "net_t": card["net_t"],
                            "net_t_cluster": card["net_t_cluster"],
                            "hit_rate": card["hit_rate"]})
        if not cells:
            out[zone] = {"state": "NO_EVENTS"}
            continue
        best = max(cells, key=lambda c: (c["net_t"] or -9))
        arr = np.array([c["net_t"] or 0.0 for c in cells], dtype=float)
        out[zone] = {
            "state": "MEASURED", "n_cells": len(cells),
            "best_cell": best,
            "median_net_t": float(np.median(arr)),
            "max_net_t": float(arr.max()),
            "n_cells_above_t_1_5": int((arr >= 1.5).sum()),
        }

    a, b = out.get("A", {}), out.get("B", {})
    same = None
    if a.get("state") == "MEASURED" and b.get("state") == "MEASURED":
        ka = {k: a["best_cell"][k] for k in
              ("symbol", "entry_delay_min", "hold_min", "rule")}
        kb = {k: b["best_cell"][k] for k in
              ("symbol", "entry_delay_min", "hold_min", "rule")}
        same = (ka == kb)
    if charge is not None:
        charge({"lane": "SELECTION_PREMIUM_DIAGNOSTIC",
                "screen": "R44_60_CELL", "zones": ["A", "B", "C"]},
               family="EVENT_FAMILY", lane="L5_CAUSAL",
               label="the height of a maximum over sixty draws")
    return {
        "state": "MEASURED", "by_zone": out,
        "winning_cell_is_the_same_in_a_and_b": same,
        "reading": (
            "if the best of sixty cells is comparably flattering in both "
            "halves but at a DIFFERENT cell, R44 measured the height of a "
            "maximum rather than an effect; if the later half's best is "
            "clearly worse, the effect was real and stopped"),
        "this_is_a_diagnostic_and_may_not_qualify_anything": True,
    }


def run(symbol: str = None, stamps=None, *, charge=None) -> dict:
    symbol = symbol or C.FROZEN_RULE["instrument_of_origin"]
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "C+D", "state": "HISTORICAL_DATA_UNAVAILABLE"}

    ev_all = ES.event_book(symbol, stamps)
    zones = ("A", "BC", "ALL")
    placebos, sweeps = {}, {}
    for z in zones:
        placebos[z] = {
            "shift_plus_7d": placebo_shifted_days(symbol, stamps, days=7,
                                                  zone=z),
            "shift_minus_7d": placebo_shifted_days(symbol, stamps, days=-7,
                                                   zone=z),
            "shift_plus_14d": placebo_shifted_days(symbol, stamps, days=14,
                                                   zone=z),
            "random_dates": placebo_random_dates(symbol, stamps, zone=z),
        }
        sweeps[z] = timing_sweep(symbol, stamps,
                                 zone=(None if z == "ALL" else z))

    perm = {z: placebo_label_permutation(
        ES.slice_zone(ev_all, z) if z != "ALL" else ev_all)
        for z in zones} if ev_all is not None else {}

    truth = {z: ES.score(ES.slice_zone(ev_all, z) if z != "ALL" else ev_all,
                         label=f"TRUTH_{z}")
             for z in zones} if ev_all is not None else {}

    verdicts = {}
    for z in zones:
        t = truth.get(z, {})
        pb = placebos[z]["shift_plus_7d"]
        rd = placebos[z]["random_dates"]
        beats_shift = (t.get("net_bps_per_event") is not None
                       and pb.get("net_bps_per_event") is not None
                       and t["net_bps_per_event"] > pb["net_bps_per_event"])
        beats_random = (t.get("net_bps_per_event") is not None
                        and rd.get("p95_bps") is not None
                        and t["net_bps_per_event"] > rd["p95_bps"])
        peaked = sweeps[z].get("peak_is_at_the_declared_minute")
        verdicts[z] = {
            "true_net_bps": t.get("net_bps_per_event"),
            "beats_shifted_placebo": bool(beats_shift),
            "beats_random_date_p95": bool(beats_random),
            "timing_peak_at_declared_minute": bool(peaked),
            "event_causality":
                "SUPPORTED" if (beats_shift and beats_random and peaked)
                else "NOT_SUPPORTED",
        }

    a_ok = verdicts.get("A", {}).get("event_causality") == "SUPPORTED"
    bc_ok = verdicts.get("BC", {}).get("event_causality") == "SUPPORTED"
    return {
        "track": "C+D", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "symbol": symbol,
        "truth_by_zone": truth,
        "placebos_by_zone": placebos,
        "timing_sweeps_by_zone": sweeps,
        "sign_permutation_by_zone": perm,
        "verdicts_by_zone": verdicts,
        "families_zone_a": family_decomposition(symbol, stamps, zone="A"),
        "families_zone_bc": family_decomposition(symbol, stamps, zone="BC"),
        "family_stability": family_stability(symbol, stamps),
        "selection_premium": selection_premium(stamps, charge=charge),
        "EVENT_CAUSALITY_RESULT":
            "SUPPORTED_OUT_OF_SAMPLE" if bc_ok
            else ("SUPPORTED_ONLY_WHERE_THE_SEARCH_LOOKED" if a_ok
                  else "NOT_SUPPORTED"),
        "the_point": "R44's causal evidence was computed on the zone its "
                     "parameters were chosen on. Running the identical "
                     "controls on the never-scored events is what tells you "
                     "whether that evidence was about the release or about "
                     "the search.",
    }
