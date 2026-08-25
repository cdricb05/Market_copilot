"""alpha_agent.r45.rv - Track F. Is the dislocation RELATIVE rather than outright?

"The market went up and then came back" is a weak hypothesis: it needs the
whole market to be wrong about the level. "Two markets that price the same
macro news disagreed for two hours" is a much stronger one, because it only
needs one of them to be slow, and because the common direction - the part
that is genuinely new information - is hedged away rather than traded.

So each expression here fades only the part of the shock the hedge market
does NOT explain:

    residual_shock   = target_shock   - beta . hedge_shock
    residual_forward = target_forward - beta . hedge_forward
    position         = -sign(residual_shock)

The hedge ratio is a parameter and it is treated like one. It is estimated on
the FIT events only, never on the events it is applied to, and both legs pay
their own spread on both sides - a two-legged trade costs two legs.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from . import bars as B
from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.rv"

#: Economically motivated pairs. Each one has a reason, stated, and each one
#: is charged a trial. None of them was chosen by looking at a result.
EXPRESSIONS = (
    {"id": "RV01_GOLD_VS_DOLLAR", "target": "XAUUSD",
     "hedges": ("EURUSD", "USDJPY"),
     "why": "gold is quoted in dollars; the dollar factor is the largest "
            "single driver of an XAUUSD print reaction and it is not "
            "information about gold"},
    {"id": "RV02_GOLD_VS_RATES", "target": "XAUUSD",
     "hedges": ("BUNDTREUR",),
     "why": "gold is a zero-coupon real asset; the duration shock is the "
            "mechanism by which a macro release should move it at all"},
    {"id": "RV03_GOLD_VS_RATES_AND_DOLLAR", "target": "XAUUSD",
     "hedges": ("BUNDTREUR", "EURUSD"),
     "why": "both channels at once - what is left is gold-specific"},
    {"id": "RV04_EQUITY_VS_RATES", "target": "USA500IDXUSD",
     "hedges": ("BUNDTREUR",),
     "why": "an equity index is a duration asset with a cash-flow leg; the "
            "residual is the growth surprise the rates market did not price"},
    {"id": "RV05_EQUITY_CROSS_BORDER", "target": "USA500IDXUSD",
     "hedges": ("DEUIDXEUR",),
     "why": "a US macro release is about US growth; the European index "
            "carries the global component and not the US-specific one"},
    {"id": "RV06_OIL_VS_EQUITY", "target": "LIGHTCMDUSD",
     "hedges": ("USA500IDXUSD",),
     "why": "the demand channel is common; the residual is the inventory "
            "and supply component"},
    {"id": "RV07_FX_VS_RATES", "target": "EURUSD",
     "hedges": ("BUNDTREUR",),
     "why": "the rate differential is the textbook FX transmission channel"},
)

NATIVE_EXPRESSIONS = (
    {"id": "RV08_CURVE_5S10S", "target": "ZN=F", "hedges": ("ZF=F",),
     "why": "the classic curve trade: the 10-year against the 5-year, which "
            "removes the parallel level shock and keeps the slope"},
    {"id": "RV09_CURVE_2S10S", "target": "ZN=F", "hedges": ("ZT=F",),
     "why": "level-neutral, and the 2-year is where policy expectations "
            "actually live"},
    {"id": "RV10_GOLD_VS_TREASURIES", "target": "GC=F", "hedges": ("ZN=F",),
     "why": "the native version of RV02 - gold against the duration shock, "
            "both quoted by the exchange that lists them"},
    {"id": "RV11_EQUITY_VS_TREASURIES", "target": "ES=F", "hedges": ("ZN=F",),
     "why": "the native version of RV04"},
    {"id": "RV12_NQ_VS_ES", "target": "NQ=F", "hedges": ("ES=F",),
     "why": "duration within equities: the long-duration index against the "
            "broad one, conditional on a rate shock"},
)


# --------------------------------------------------------------------------- #
def aligned_books(symbols, stamps) -> dict:
    """Event books for several symbols, restricted to their common events."""
    books, keys = {}, None
    for s in symbols:
        ev = ES.event_book(s, stamps)
        if ev is None or len(ev) == 0:
            return None
        ev = ev.copy()
        ev["key"] = ev["stamp_utc"].astype(str)
        ev = ev.drop_duplicates(subset=["key"]).set_index("key")
        books[s] = ev
        keys = set(ev.index) if keys is None else (keys & set(ev.index))
    if not keys or len(keys) < 30:
        return None
    common = sorted(keys)
    return {s: books[s].loc[common] for s in symbols}


def _fit_beta(y: np.ndarray, X: np.ndarray) -> np.ndarray:
    A = np.column_stack([np.ones(len(y)), X])
    b, *_ = np.linalg.lstsq(A, y, rcond=None)
    return b[1:]


def rv_book(spec: dict, stamps, *, fit_zone: str = "A") -> dict:
    """Build the residual event book, with beta fitted on ``fit_zone`` only."""
    syms = [spec["target"], *spec["hedges"]]
    books = aligned_books(syms, stamps)
    if books is None:
        return {"id": spec["id"], "state": "DATA_INSUFFICIENT",
                "why": "fewer than 30 events common to every leg"}
    tgt = books[spec["target"]]
    hedges = [books[h] for h in spec["hedges"]]

    ref = ES.event_book(spec["target"], stamps)
    z = ES.zone_of(ref)
    a_end = pd.Timestamp(z["a_range"][1]) if z["a_range"] else None
    d = pd.to_datetime(tgt["date"])
    in_fit = (d <= a_end).to_numpy() if a_end is not None else \
        np.ones(len(tgt), dtype=bool)

    y_shock = tgt["shock"].to_numpy(dtype=float)
    X_shock = np.column_stack([h["shock"].to_numpy(dtype=float)
                               for h in hedges])
    if in_fit.sum() < 40:
        return {"id": spec["id"], "state": "DATA_INSUFFICIENT",
                "why": f"only {int(in_fit.sum())} fit events"}
    beta = _fit_beta(y_shock[in_fit], X_shock[in_fit])

    y_fwd = tgt["forward"].to_numpy(dtype=float)
    X_fwd = np.column_stack([h["forward"].to_numpy(dtype=float)
                             for h in hedges])
    res_shock = y_shock - X_shock @ beta
    res_fwd = y_fwd - X_fwd @ beta

    half_in = tgt["half_in_bps"].to_numpy(dtype=float)
    half_out = tgt["half_out_bps"].to_numpy(dtype=float)
    for bi, h in zip(beta, hedges):
        half_in = half_in + abs(float(bi)) * h["half_in_bps"].to_numpy(float)
        half_out = half_out + abs(float(bi)) * h["half_out_bps"].to_numpy(float)
    legs = 1.0 + float(np.abs(beta).sum())

    ev = pd.DataFrame({
        "event": tgt["event"].to_numpy(), "date": tgt["date"].to_numpy(),
        "stamp_utc": tgt["stamp_utc"].to_numpy(),
        "shock": res_shock, "forward": res_fwd,
        "half_in_bps": half_in, "half_out_bps": half_out,
    })
    ev.attrs.update({
        "symbol": spec["id"],
        "cost_source": tgt.attrs.get("cost_source"),
        "instrument_class": "RELATIVE_VALUE",
        "fill_rate": tgt.attrs.get("fill_rate"),
        "dropped_events": tgt.attrs.get("dropped_events"),
        "resolution_degraded": tgt.attrs.get("resolution_degraded"),
        "bar_tolerance_min": tgt.attrs.get("bar_tolerance_min"),
    })
    return {"id": spec["id"], "state": "MEASURED", "book": ev,
            "beta": [float(b) for b in beta], "legs_of_notional": legs,
            "in_fit": in_fit, "fit_zone": fit_zone,
            "n_fit_events": int(in_fit.sum()),
            "target": spec["target"], "hedges": list(spec["hedges"]),
            "hedge_r2_in_fit": float(
                1.0 - np.var(res_shock[in_fit]) / max(1e-24,
                                                      np.var(y_shock[in_fit])))}


def score_expression(spec: dict, stamps, *, cost_mult: float = 1.0) -> dict:
    b = rv_book(spec, stamps)
    if b.get("state") != "MEASURED":
        return {**{k: b.get(k) for k in ("id", "state", "why")},
                "why_it_matters": spec["why"]}
    ev, in_fit = b["book"], b["in_fit"]
    out = {"id": spec["id"], "state": "MEASURED",
           "target": b["target"], "hedges": b["hedges"],
           "why_it_matters": spec["why"], "beta": b["beta"],
           "legs_of_notional": b["legs_of_notional"],
           "hedge_r2_in_fit": b["hedge_r2_in_fit"],
           "n_fit_events": b["n_fit_events"],
           "beta_fitted_on": "the fit events only, never on the applied ones"}
    for name, mask in (("FIT_ZONE_A", in_fit), ("HOLDOUT_BC", ~in_fit),
                       ("ALL", np.ones(len(ev), dtype=bool))):
        sub = ev[mask].copy()
        sub.attrs.update(ev.attrs)
        card = ES.score(sub, cost_mult=cost_mult,
                        label=f"{spec['id']}_{name}")
        out[name] = card
    hold = out["HOLDOUT_BC"]
    out["holdout_positive"] = bool(
        hold.get("state") == "MEASURED"
        and (hold.get("net_bps_per_event") or 0) > 0
        and (hold.get("net_t_cluster") or 0)
        >= C.REPLICATION_NET_T_MIN)
    return out


# --------------------------------------------------------------------------- #
def run(stamps=None, *, charge=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "F", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    specs = list(EXPRESSIONS) + [
        s for s in NATIVE_EXPRESSIONS
        if B.panel(s["target"]) is not None
        and all(B.panel(h) is not None for h in s["hedges"])]

    rows, charged = [], []
    for spec in specs:
        card = score_expression(spec, stamps)
        rows.append(card)
        if charge is not None and card.get("state") == "MEASURED":
            charged.append(charge(
                {"expression": spec["id"], "target": spec["target"],
                 "hedges": list(spec["hedges"]),
                 "rule": C.FROZEN_RULE["rule"],
                 "entry_delay_min": C.FROZEN_RULE["entry_delay_min"],
                 "hold_min": C.FROZEN_RULE["hold_min"]},
                family="EVENT_RELATIVE_VALUE", lane="L8_RV",
                label=spec["id"]))

    measured = [r for r in rows if r.get("state") == "MEASURED"]
    ranked = sorted(measured,
                    key=lambda r: -((r.get("HOLDOUT_BC") or {}).get(
                        "net_t_cluster") or -9))
    survivors = [r for r in ranked if r.get("holdout_positive")]

    bh = None
    if measured:
        ts = {r["id"]: (r["HOLDOUT_BC"] or {}).get("net_t_cluster")
              for r in measured
              if (r["HOLDOUT_BC"] or {}).get("net_t_cluster") is not None}
        if ts:
            bh = EV.family_bh(ts)

    return {
        "track": "F", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["L8_RV"],
        "n_expressions": len(rows), "expressions": rows,
        "ranked_by_holdout_t": [
            {"id": r["id"], "target": r["target"], "hedges": r["hedges"],
             "beta": r["beta"], "hedge_r2_in_fit": r["hedge_r2_in_fit"],
             "fit_net_bps": (r["FIT_ZONE_A"] or {}).get("net_bps_per_event"),
             "fit_net_t": (r["FIT_ZONE_A"] or {}).get("net_t_cluster"),
             "holdout_n": (r["HOLDOUT_BC"] or {}).get("n_events"),
             "holdout_net_bps": (r["HOLDOUT_BC"] or {}).get(
                 "net_bps_per_event"),
             "holdout_net_t": (r["HOLDOUT_BC"] or {}).get("net_t_cluster"),
             "holdout_hit": (r["HOLDOUT_BC"] or {}).get("hit_rate")}
            for r in ranked],
        "n_holdout_survivors": len(survivors),
        "survivors": [r["id"] for r in survivors],
        "benjamini_hochberg_on_holdout": bh,
        "burden_charged": charged,
        "RELATIVE_VALUE_RESULT":
            "CANDIDATE_FOUND" if survivors else "NO_SURVIVING_EXPRESSION",
    }
