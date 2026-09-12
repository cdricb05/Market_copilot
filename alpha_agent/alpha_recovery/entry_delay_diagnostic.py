r"""alpha_agent.alpha_recovery.entry_delay_diagnostic - does the frozen edge
actually REQUIRE same-session execution, and therefore a live OPRA feed?

WHY THIS EXISTS
    ``REVERSED_SPY_PUT_CALL_SKEW_H5`` forms its signal from the 15:45 ET option
    snapshot on session ``t`` and enters at the 16:00 ET close on the SAME
    session. Those fifteen minutes are the ONLY reason a live OPRA entitlement is
    needed at all: the historical API publishes session ``t`` after ``t`` ends -
    too late for ``t``'s own close, but in good time for the next session.

    A live OPRA subscription is a recurring cost. Before paying it, the estate
    should know what it is actually buying. So this module asks one economic
    question and no others: how much of the edge survives if the same
    information is acted on at the next session's open, or the next session's
    close?

WHAT THIS IS NOT
    It is NOT qualification evidence. It runs on the sample the sign was read
    off, so its LEVELS inherit the whole post-hoc discovery disclosure
    (``reversed_skew.DISCOVERY_DISCLOSURE``) and cannot qualify anything. It is
    NOT TRUE_FORWARD. It does NOT create, replace or modify a challenger - the
    frozen challenger is untouched and stays blocked on live OPRA.

    The RATIO is the part worth trusting. Baseline and variants share one
    sample, one sign, one z-score and one horizon, so whatever selection
    advantage inflates the baseline inflates the variants too, and the retention
    ratio largely divides it out. The ratio answers the purchase question; the
    levels do not.

WHAT IS HELD FIXED - every one of these is IMPORTED, never restated
    feature        ``reversed_skew.spec()["field"]``
    sign           ``reversed_skew.spec()["sign"]``
    horizon        ``reversed_skew.spec()["horizon"]``, and the cadence equals it
    z-score        ``options_surface._z`` (60 observations, strictly prior)
    cost ladder    ``options_surface.COST_LADDER_BPS``
    selection      ``options_surface.SELECTION_FRACTION``
    statistics     ``options_surface._stats`` (the same Newey-West t)

    ONLY the entry and exit marks move, and only to two boundaries DECLARED
    below. There is deliberately no grid, no sweep and no third variant, and
    none may be added: a delay that can be searched is a delay that was chosen.

THE SOURCE CONTROL, AND WHY IT IS NOT OPTIONAL
    The variants need an open price, and the surface carries only a close, so
    they read the licensed local Norgate book. That is a DIFFERENT price source
    from the surface's own ``underlying_close``. A same-day path is therefore
    also computed from Norgate closes, so the difference between the frozen
    baseline and a variant can be attributed to the DELAY rather than to the
    change of price source. Without it, a source discrepancy would be read as an
    economic finding.

RESEARCH ONLY. Reads two owned price books and writes one artifact. No order,
no fill, no promotion, no capital, no registration, no purchase.
"""
from __future__ import annotations

from . import MIN_EFFECTIVE_PERIODS, now_iso, write_artifact
from . import options_acquisition as OA
from . import options_surface as OS
from . import reversed_skew as RS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.entry_delay_diagnostic"
ARTIFACT_NAME = "entry_delay_diagnostic.json"

#: The ONLY entry boundaries this diagnostic is permitted to measure, declared
#: before it ran. ``lag`` is in SESSIONS from the signal session; ``book`` names
#: which daily mark is used for both entry and exit.
VARIANTS = {
    "frozen_same_day": {
        "lag": 0, "book": "surface_close",
        "needs_live_opra": True,
        "what_it_is": "the frozen construction itself, computed by "
                      "options_surface.path so the baseline cannot drift",
    },
    "control_same_day": {
        "lag": 0, "book": "close",
        "needs_live_opra": True,
        "what_it_is": "the same same-day boundary priced from the Norgate book; "
                      "isolates the price SOURCE from the delay",
    },
    "next_open": {
        "lag": 1, "book": "open",
        "needs_live_opra": False,
        "what_it_is": "A. enter at the next session's open. The 15:45 snapshot "
                      "of session t is published by the historical API once t "
                      "has ended, which is before the next open.",
    },
    "next_close": {
        "lag": 1, "book": "close",
        "needs_live_opra": False,
        "what_it_is": "B. enter at the next session's close.",
    },
}

#: Turnover is IDENTICAL across variants - same cadence, same decision count,
#: same gross exposure - so a delay adds no incremental turnover cost. What it
#: can add is a wider effective spread at the open, and the frozen ladder's top
#: rung already prices that at five times the canonical SPY rate. No new cost
#: number is invented here.
COST_TREATMENT = {
    "ladder_bps_per_side": list(OS.COST_LADDER_BPS),
    "incremental_turnover": 0.0,
    "why_zero": "the cadence, the decision count and the gross exposure are "
                "unchanged by a delay; only the mark moves",
    "spread_at_the_open": "covered by the existing %.1f bp/side stress rung, "
                          "which is %.0fx the canonical SPY rate"
                          % (OS.COST_STRESS_BPS,
                             OS.COST_STRESS_BPS / OS.COST_PRIMARY_BPS),
}

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# The owned price book
# --------------------------------------------------------------------------- #
def price_book(start: str = "2024-08-01", end: str = None) -> dict:
    """SPY daily open/close from the licensed local Norgate install.

    Lazy vendor import, matching ``alpha_agent.historical_backfill``: nothing is
    installed, upgraded or re-exported, and an absent Norgate is reported rather
    than worked around.
    """
    key = (start, end)
    if key in _CACHE:
        return _CACHE[key]
    try:
        # The vendor library greets on import with logger.warn(), which is a
        # DeprecationWarning under this Python and an ERROR under the repo's
        # warning policy. Suppressing it here keeps a vendor's logging style
        # from deciding whether a diagnostic can run.
        import warnings                                # noqa: PLC0415
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            import norgatedata                         # noqa: PLC0415 - lazy local vendor import
    except ImportError as exc:
        return {"state": "NORGATE_UNAVAILABLE", "why": str(exc)[:200]}
    try:
        df = norgatedata.price_timeseries(
            OA.UNDERLYING, start_date=start, end_date=end,
            format="pandas-dataframe")
    except Exception as exc:                          # noqa: BLE001 - vendor call
        return {"state": "NORGATE_ERROR",
                "why": "%s: %s" % (type(exc).__name__, str(exc)[:200])}
    idx = [str(x)[:10] for x in df.index]
    out = {"state": "OK", "calendar": idx,
           "position": {d: i for i, d in enumerate(idx)},
           "open": dict(zip(idx, df["Open"].tolist())),
           "close": dict(zip(idx, df["Close"].tolist())),
           "provenance": "norgatedata.price_timeseries (licensed local install)"}
    _CACHE[key] = out
    return out


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
def variant_path(variant: str, *, cost_bps: float, surface=None,
                 book: dict = None) -> dict:
    """Non-overlapping per-decision NET returns for one declared boundary.

    The signal is computed exactly once, by the owners: ``options_surface``
    supplies the feature and the z-score, ``reversed_skew`` supplies the sign and
    the horizon. This function chooses no position and evaluates no rule; it only
    decides WHICH MARK each already-chosen position is entered and exited at.
    """
    import numpy as np                                # noqa: PLC0415

    if variant not in VARIANTS:
        raise ValueError("undeclared variant %r; the declared set is %s"
                         % (variant, sorted(VARIANTS)))
    cfg = VARIANTS[variant]
    sp = RS.spec()
    surface = surface or OS.surface_path()
    if cfg["book"] == "surface_close":
        return OS.path(sp, cost_bps=cost_bps, surface=surface)

    book = book or price_book()
    if book.get("state") != "OK":
        return {"state": book.get("state"), "net": np.array([]),
                "gross": np.array([]), "dates": []}

    f = OS.features(surface=surface)
    dates = [str(x)[:10] for x in f["date"]]
    pos = float(sp["sign"]) * np.sign(OS._z(f[sp["field"]]))
    h = int(sp["horizon"])
    cal, posn = book["calendar"], book["position"]
    marks = book[cfg["book"]]
    lag = int(cfg["lag"])
    rate = float(cost_bps) * 1e-4

    net, gross, out_dates, skipped = [], [], [], 0
    t = 0
    while t + h < len(f):
        p, d = pos[t], dates[t]
        if np.isfinite(p) and d in posn:
            i, j = posn[d] + lag, posn[d] + lag + h
            if j < len(cal):
                pe, pxx = marks.get(cal[i]), marks.get(cal[j])
                if pe and pxx and np.isfinite(pe) and np.isfinite(pxx):
                    g = p * (pxx / pe - 1.0)
                    gross.append(g)
                    net.append(g - abs(p) * 2.0 * rate)
                    out_dates.append(d)
                else:
                    skipped += 1
            else:
                skipped += 1
        t += h
    return {"state": "OK", "net": np.array(net), "gross": np.array(gross),
            "dates": out_dates, "cadence": h, "skipped": skipped}


def measure_variant(variant: str, *, surface=None, book: dict = None) -> dict:
    """Every layer the purchase decision needs, at every rung of the ladder."""
    import numpy as np                                # noqa: PLC0415

    sp = RS.spec()
    h = int(sp["horizon"])
    out = {"variant": variant, "by_cost_bps_per_side": {}, **VARIANTS[variant]}
    for c in OS.COST_LADDER_BPS:
        p = variant_path(variant, cost_bps=c, surface=surface, book=book)
        net = p["net"]
        cut = int(round(len(net) * OS.SELECTION_FRACTION))
        ho = net[cut:]
        half = len(ho) // 2
        out["by_cost_bps_per_side"]["%.1f" % c] = {
            "all": OS._stats(net, h=h, label="ALL", cost_bps=c),
            "selection": OS._stats(net[:cut], h=h, label="SELECTION", cost_bps=c),
            "holdout": OS._stats(ho, h=h, label="HOLDOUT", cost_bps=c),
            "holdout_halves_ann_net": (
                [float(np.mean(ho[:half]) * OS.PPY / h),
                 float(np.mean(ho[half:]) * OS.PPY / h)] if half >= 5 else None),
        }
    p0 = variant_path(variant, cost_bps=0.0, surface=surface, book=book)
    from alpha_agent.r63 import sensitivity as S     # noqa: PLC0415
    st0 = S.nw_tstat(p0["gross"], 0) if len(p0["gross"]) else {"t": None}
    out["gross"] = {
        "ann_gross": (float(np.nanmean(p0["gross"]) * OS.PPY / h)
                      if len(p0["gross"]) else None),
        "t_gross": st0.get("t"),
        "reaches_t2_at_zero_cost": bool((st0.get("t") or 0) >= 2.0)}
    out["decisions"] = int(len(p0["net"]))
    try:
        prim = variant_path(variant, cost_bps=OS.COST_PRIMARY_BPS,
                            surface=surface, book=book)
        out["capital_applicability"] = OS._capital(sp, prim)
    except Exception as exc:                          # noqa: BLE001 - owner call
        out["capital_applicability"] = {
            "state": "DATA_HOLD",
            "why": "%s: %s" % (type(exc).__name__, str(exc)[:160])}
    return out


def _ann_net(m: dict) -> float:
    lay = (m.get("by_cost_bps_per_side") or {}).get("%.1f" % OS.COST_PRIMARY_BPS)
    return ((lay or {}).get("all") or {}).get("ann_net")


def retention(results: dict) -> dict:
    """How much of the edge each delayed boundary keeps.

    Reported against BOTH baselines on purpose. Against the frozen baseline it
    answers "what would we give up by not buying the feed"; against the source
    control it isolates the delay from the price source. Quoting only the
    flattering one would be a choice, so both are always present.
    """
    base_frozen = _ann_net(results.get("frozen_same_day") or {})
    base_ctrl = _ann_net(results.get("control_same_day") or {})
    out = {}
    for v in ("next_open", "next_close"):
        n = _ann_net(results.get(v) or {})
        out[v] = {
            "ann_net": n,
            "vs_frozen_same_day": (n / base_frozen
                                   if (n is not None and base_frozen) else None),
            "vs_source_control": (n / base_ctrl
                                  if (n is not None and base_ctrl) else None),
            "needs_live_opra": VARIANTS[v]["needs_live_opra"],
        }
    return out


def forward_evidence_arithmetic(monthly_usd: float = None) -> dict:
    """How long a LIVE forward evaluation would take, and what it would cost.

    The floor is the estate's own ``MIN_EFFECTIVE_PERIODS``; the cadence is the
    frozen horizon. Neither is chosen here. ``monthly_usd`` is left None unless a
    caller supplies an observed price, because an unverified price must not
    harden into a fact.
    """
    per_year = OS.PPY / float(RS.FROZEN_HORIZON)
    per_month = per_year / 12.0
    months = MIN_EFFECTIVE_PERIODS / per_month
    out = {
        "cadence_sessions": RS.FROZEN_HORIZON,
        "independent_decisions_per_year": per_year,
        "independent_decisions_per_month": per_month,
        "min_effective_periods": MIN_EFFECTIVE_PERIODS,
        "months_to_reach_the_floor": months,
        "why_this_is_a_floor_not_a_finish": (
            "reaching MIN_EFFECTIVE_PERIODS makes a measurement admissible, not "
            "conclusive; a t-statistic on %d observations still has to clear the "
            "family's gates" % MIN_EFFECTIVE_PERIODS),
    }
    if monthly_usd is not None:
        out["monthly_usd_observed"] = float(monthly_usd)
        out["usd_to_reach_the_floor"] = float(monthly_usd) * months
        out["usd_per_year"] = float(monthly_usd) * 12.0
    return out


def build(*, surface=None, write: bool = True, monthly_usd: float = None) -> dict:
    """Run every declared variant and report. Measures; recommends nothing."""
    book = price_book()
    results = {v: measure_variant(v, surface=surface, book=book) for v in VARIANTS}
    ret = retention(results)
    body = {
        "schema": "alpha_recovery_entry_delay_diagnostic/1",
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": now_iso(),
        "question": "does the frozen edge require SAME-SESSION execution, and "
                    "therefore a live OPRA entitlement?",
        "challenger_id": RS.CHALLENGER_ID,
        "challenger_unchanged": True,
        "is_qualification_evidence": False,
        "is_true_forward": False,
        "creates_a_challenger": False,
        "why_not_evidence": RS.DISCOVERY_DISCLOSURE,
        "what_the_ratio_is_for": (
            "baseline and variants share one sample, one sign, one z-score and "
            "one horizon, so the selection advantage that inflates the baseline "
            "inflates the variants too and largely divides out of the ratio. The "
            "RATIO informs the purchase question; the LEVELS do not."),
        "frozen_inputs": {
            "field": RS.spec()["field"], "sign": RS.spec()["sign"],
            "horizon": RS.FROZEN_HORIZON, "zscore_lookback": OS.ZSCORE_LOOKBACK,
            "selection_fraction": OS.SELECTION_FRACTION,
            "all_imported_not_restated": True},
        "variants_declared_in_advance": {k: v["what_it_is"]
                                         for k, v in VARIANTS.items()},
        "no_delay_was_searched": (
            "exactly two delayed boundaries exist in VARIANTS and both are the "
            "next session; no intermediate entry time was scanned"),
        "cost_treatment": COST_TREATMENT,
        "price_book": {"state": book.get("state"),
                       "provenance": book.get("provenance")},
        "results": results,
        "alpha_retention": ret,
        "forward_evidence": forward_evidence_arithmetic(monthly_usd),
        "safety": {"orders_created": 0, "fills_created": 0,
                   "capital_allocated": False, "promoted": False,
                   "registered": False, "purchased_usd": 0.0,
                   "backfilled": False},
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


def summary_lines(body: dict) -> list:
    """One readable block; the numbers come from ``body``, never recomputed."""
    out = []
    prim = "%.1f" % OS.COST_PRIMARY_BPS
    for v, m in (body.get("results") or {}).items():
        lay = ((m.get("by_cost_bps_per_side") or {}).get(prim) or {}).get("all") or {}
        g = m.get("gross") or {}
        out.append("%-18s dec=%-4s gross=%7s net=%7s t=%6s sharpe=%6s dd=%7s" % (
            v, m.get("decisions"),
            "%.2f%%" % (g["ann_gross"] * 100) if g.get("ann_gross") is not None else "n/a",
            "%.2f%%" % (lay["ann_net"] * 100) if lay.get("ann_net") is not None else "n/a",
            "%.2f" % lay["t_net"] if lay.get("t_net") is not None else "n/a",
            "%.2f" % lay["sharpe"] if lay.get("sharpe") is not None else "n/a",
            "%.1f%%" % (lay["max_dd"] * 100) if lay.get("max_dd") is not None else "n/a"))
    for v, r in (body.get("alpha_retention") or {}).items():
        a, b = r.get("vs_frozen_same_day"), r.get("vs_source_control")
        out.append("retention %-12s vs frozen %s   vs source control %s" % (
            v, "%.1f%%" % (a * 100) if a is not None else "n/a",
            "%.1f%%" % (b * 100) if b is not None else "n/a"))
    return out
