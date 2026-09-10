"""alpha_agent.alpha_recovery.forecast_products - what this estate can actually
predict today, in economic units.

Workstream 14, the campaign's answer to "produce actual economic forecasts where
evidence supports them". Four product families are assembled through the ONE
forecast contract and every economic field is either a VALUE carrying an
out-of-sample calibration record, or UNAVAILABLE carrying the reason:

    BROAD_MARKET_DIRECTION   SPY probability up/down, expected return,
                             uncertainty, downside and tail, per horizon
                             (from market_direction).
    EQUITY_CROSS_SECTION     per-name expected excess return and rank for the
                             incumbent's live book (from incumbent).
    EQUITY_BOOK_LEVEL        the expected 21-session EXCESS return, tracking
                             error, downside and tail of the top-25 book at
                             each rebalance cadence (from equity_challengers).
    MULTI_ASSET_SLEEVE       the expected 21-session return, volatility,
                             downside and tail of the chosen FX carry sleeve
                             (from cadence).

THE CALIBRATION TEST, stated before it was run
    A book-level expected return is licensed only if the estimate a forecaster
    could have formed on SELECTION periods survives contact with the LOCKBOX:

        1. the two layer means share a sign - the direction the selection
           period implied actually held;
        2. the lockbox mean lies inside a 95 % predictive interval around the
           selection mean, width 1.96 * sd_selection * sqrt(1/n_s + 1/n_l);
        3. the lockbox carries at least MIN_EFFECTIVE_PERIODS observations.

    All three, or the field is UNAVAILABLE. Passing licenses the FULL-sample
    out-of-sample mean as the point forecast, never a selection-period-only
    number, and the uncertainty travels with it: a licensed expected return
    whose t is below 2 is published WITH its t, because an honest forecast of a
    small edge is a forecast, and pretending it is significant would not be.

Nothing here allocates capital, registers a challenger or writes a live store.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from alpha_agent.r63 import LOCKBOX_START
from alpha_agent.r63 import sensitivity as S

from . import (INCUMBENT_MODEL_ID, MIN_EFFECTIVE_PERIODS, now_iso, read_artifact, write_artifact)
from . import forecast_contract as FC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.forecast_products"
ARTIFACT_NAME = "forecast_products.json"
PPY = 252.0
BLOCK_SESSIONS = 21
Z95 = 1.959963984540054

FAM_MARKET = "BROAD_MARKET_DIRECTION"
FAM_XS = "EQUITY_CROSS_SECTION"
FAM_BOOK = "EQUITY_BOOK_LEVEL"
FAM_SLEEVE = "MULTI_ASSET_SLEEVE"


# --------------------------------------------------------------------------- #
# The pre-stated calibration test
# --------------------------------------------------------------------------- #
def selection_lockbox_calibration(x: np.ndarray, is_lockbox: np.ndarray, *,
                                  periods_per_year: float, label: str) -> dict:
    """Does a SELECTION-period estimate predict the LOCKBOX realisation?

    The rule is fixed in this module's docstring and is applied unchanged to
    every book: sign agreement, a 95 % predictive interval, and a lockbox at
    least ``MIN_EFFECTIVE_PERIODS`` long.
    """
    x = np.asarray(x, dtype=float)
    is_lockbox = np.asarray(is_lockbox, dtype=bool)
    sel, lock = x[~is_lockbox], x[is_lockbox]
    out = {"label": label, "n_selection": int(len(sel)), "n_lockbox": int(len(lock)),
           "rule": ("sign agreement AND lockbox mean inside a 95 %% predictive interval around the "
                    "selection mean AND at least %d lockbox observations" % MIN_EFFECTIVE_PERIODS),
           "periods_per_year": periods_per_year}
    if len(sel) < 4 or len(lock) < 2:
        out.update({"calibrated": False, "reason": "too few periods in one layer"})
        return out
    m_s, m_l = float(sel.mean()), float(lock.mean())
    sd_s = float(sel.std(ddof=1))
    se = sd_s * math.sqrt(1.0 / len(sel) + 1.0 / len(lock)) if sd_s > 0 else float("nan")
    inside = bool(np.isfinite(se) and abs(m_l - m_s) <= Z95 * se)
    sign_ok = bool(np.sign(m_l) == np.sign(m_s) and m_s != 0.0)
    long_enough = bool(len(lock) >= MIN_EFFECTIVE_PERIODS)
    ok = bool(inside and sign_ok and long_enough)
    reasons = []
    if not sign_ok:
        reasons.append("the selection and lockbox means disagree in sign")
    if not inside:
        reasons.append("the lockbox mean is outside the 95 % predictive interval of the selection mean")
    if not long_enough:
        reasons.append("fewer than %d lockbox observations" % MIN_EFFECTIVE_PERIODS)
    out.update({
        "selection_ann": m_s * periods_per_year, "lockbox_ann": m_l * periods_per_year,
        "selection_sd_per_period": sd_s, "predictive_se_per_period": (se if np.isfinite(se) else None),
        "predictive_interval_ann": ([(m_s - Z95 * se) * periods_per_year,
                                     (m_s + Z95 * se) * periods_per_year] if np.isfinite(se) else None),
        "sign_agreement": sign_ok, "lockbox_inside_interval": inside,
        "lockbox_long_enough": long_enough,
        "calibrated": ok, "reason": None if ok else "; ".join(reasons)})
    return out


def _horizon_distribution(per_period: np.ndarray, *, periods_per_horizon: int) -> dict:
    """Empirical distribution of the compounded return over one forecast
    horizon, from non-overlapping blocks of the out-of-sample path."""
    x = np.asarray(per_period, dtype=float)
    x = x[np.isfinite(x)]
    k = max(1, int(periods_per_horizon))
    n = len(x) // k
    if n < 4:
        return {"n_blocks": int(n)}
    blocks = np.array([float(np.prod(1.0 + x[i * k:(i + 1) * k]) - 1.0) for i in range(n)])
    return {"n_blocks": int(n), "mean": float(blocks.mean()), "std": float(blocks.std(ddof=1)),
            "probability_negative": float((blocks < 0).mean()),
            "percentile_5": float(np.percentile(blocks, 5)),
            "percentile_95": float(np.percentile(blocks, 95)),
            "worst": float(blocks.min())}


def _record(cal: dict, method: str) -> dict:
    return FC.calibration_record(
        calibrated=bool(cal.get("calibrated")), method=method,
        oos_test={"selection_ann": cal.get("selection_ann"), "lockbox_ann": cal.get("lockbox_ann"),
                  "predictive_interval_ann": cal.get("predictive_interval_ann"),
                  "sign_agreement": cal.get("sign_agreement"),
                  "lockbox_inside_interval": cal.get("lockbox_inside_interval")},
        n_oos_periods=int(cal.get("n_lockbox") or 0),
        notes=cal.get("reason"))


# --------------------------------------------------------------------------- #
# The families
# --------------------------------------------------------------------------- #
def market_family() -> dict:
    md = read_artifact("market_direction.json") or {}
    prods = md.get("products") or {}
    rows = {}
    for h, p in sorted(prods.items(), key=lambda kv: int(kv[0])):
        pu = (p or {}).get("probability_up") or {}
        er = (p or {}).get("expected_return") or {}
        rows[str(h)] = {"product": p, "probability_up_state": pu.get("state"),
                        "expected_return_state": er.get("state"),
                        "verdict": ((md.get("horizons") or {}).get(str(h)) or {}).get("verdicts", {}).get("verdict"),
                        "valid": FC.validate(p)["valid"] if p else None}
    licensed = sorted(h for h, r in rows.items() if r["probability_up_state"] == FC.VALUE)
    return {"family": FAM_MARKET, "instrument": "SPY", "by_horizon": rows,
            "licensed_horizons": licensed, "n_licensed": len(licensed),
            "answer": ("no calibrated broad-market probability exists at any tested horizon; every "
                       "raw model probability is reported UNAVAILABLE"
                       if not licensed else
                       "calibrated at horizons %s" % ", ".join(licensed))}


def equity_cross_section_family() -> dict:
    inc = read_artifact("incumbent_baseline.json") or {}
    today = inc.get("predicts_today") or {}
    cal21 = ((inc.get("historical_oos") or {}).get("calibration") or {}).get("21") or {}
    return {"family": FAM_XS, "model_id": INCUMBENT_MODEL_ID,
            "as_of": today.get("market_date"), "n_ranked": today.get("n_target_weights"),
            "economic_units_licensed": bool(today.get("economic_units_licensed")),
            "calibration": {k: cal21.get(k) for k in
                            ("state", "calibrated", "monotonicity_selection", "monotonicity_lockbox",
                             "selection_lockbox_agreement", "reason", "method", "units")},
            "products_sample": today.get("forecast_products_sample"),
            "n_products": today.get("n_products"),
            "every_product_valid": today.get("every_product_valid"),
            "answer": ("the incumbent emits a RANK, not an expected return: the decile-to-return "
                       "mapping is not monotone on selection and confirmed on the lockbox, so every "
                       "per-name expected excess return is UNAVAILABLE"
                       if not today.get("economic_units_licensed") else
                       "per-name expected excess returns are licensed by the lockbox-tested decile map")}


def equity_book_family() -> dict:
    """Book-level forecasts for the top-25 book at each rebalance cadence: the
    quantity the desk can actually act on."""
    from . import equity_challengers as EC
    bk = EC._books()
    dates = bk["dates"]
    bench = EC.blocks(bk["benchmark"]["daily_net"], dates, first=bk["first"], last=bk["last"])
    rows = {}
    products = []
    for sp in EC.default_grid():
        if sp["leg"] != EC.LEG_BLEND:
            continue
        cid = sp["cell_id"]
        bl = EC.blocks(bk["arms"][cid]["daily_net"], dates, first=bk["first"], last=bk["last"])
        n = min(len(bl), len(bench))
        if n < 8:
            continue
        ex = bl["ret"].to_numpy()[:n] - bench["ret"].to_numpy()[:n]
        lock = (bl["layer"].to_numpy()[:n] == "LOCKBOX")
        cal = selection_lockbox_calibration(ex, lock, periods_per_year=PPY / BLOCK_SESSIONS,
                                            label=cid)
        rec = _record(cal, "selection-period mean excess return tested once on the lockbox")
        dist = _horizon_distribution(ex, periods_per_horizon=1)
        st = S.nw_tstat(ex, 0)
        ann = float(ex.mean() * PPY / BLOCK_SESSIONS)
        sd = float(ex.std(ddof=1))
        units = "fraction over %d sessions, excess over the equal-weight scored universe" % BLOCK_SESSIONS
        p = FC.sleeve_forecast(
            sleeve=cid, as_of=str(bl["end"].iloc[n - 1]), horizon_sessions=BLOCK_SESSIONS,
            model_id="alpha_recovery_equity_book_%s" % cid.replace("|", "_"),
            expected_return=(FC.expected_return(float(ex.mean()), calibration=rec, units=units)
                             if cal["calibrated"] else FC.unavailable(cal["reason"], units)),
            expected_risk=FC.field(sd, units="std of the %d-session excess return" % BLOCK_SESSIONS),
            downside=(FC.field(dist.get("probability_negative"),
                               units="probability the %d-session excess is negative" % BLOCK_SESSIONS)
                      if dist.get("probability_negative") is not None
                      else FC.unavailable("too few blocks")),
            confidence=FC.field(st["t"], units="Newey-West t of the mean excess") if st.get("t") is not None
            else FC.unavailable("no t"),
            capital_applicability={"universe": "PIT S&P 500 eligible names", "book": "long-only top-25 EW",
                                   "rebalance_sessions": sp["trade_every"],
                                   "is_the_operational_construction": bool(sp["is_reference"])},
            liquidity_cost_assumptions={"cost_bps_per_side": 12.5,
                                        "ann_oneway_turnover": float(bk["arms"][cid]["turnover"].sum()
                                                                     / n * PPY / BLOCK_SESSIONS)},
            evidence_maturity="HISTORICAL_OOS_ONLY",
            information=["FUNDAMENTAL_LEVELS", "FREE_CASH_FLOW", "MOMENTUM"],
            notes="ann_excess=%.4f t=%.2f tail5=%.4f" % (ann, st.get("t") or float("nan"),
                                                         dist.get("percentile_5") or float("nan")))
        products.append(p)
        rows[cid] = {"rebalance_sessions": sp["trade_every"], "ann_expected_excess": ann,
                     "calibration": cal, "horizon_distribution": dist,
                     "t_mean_excess": st.get("t"), "valid": FC.validate(p)["valid"],
                     "expected_return_state": p["expected_return"]["state"]}
    licensed = sorted(k for k, v in rows.items() if v["expected_return_state"] == FC.VALUE)
    return {"family": FAM_BOOK, "by_cadence": rows, "licensed": licensed, "n_licensed": len(licensed),
            "products": products,
            "every_product_valid": all(FC.validate(p)["valid"] for p in products) if products else None,
            "answer": ("%d of %d cadence books carry a lockbox-tested expected excess return"
                       % (len(licensed), len(rows)))}


def sleeve_family() -> dict:
    from . import cadence as CD
    cad = read_artifact(CD.ARTIFACT_NAME) or {}
    chosen = (cad.get("chosen_by_family") or {}).get("FX_CARRY_CADENCE") or {}
    cid = chosen.get("cell_id")
    if not cid:
        return {"family": FAM_SLEEVE, "state": "DATA_HOLD", "why": "no chosen sleeve"}
    s = CD.sleeve_series(cid)
    if s is None or len(s) == 0:
        return {"family": FAM_SLEEVE, "state": "DATA_HOLD", "why": "sleeve returns unavailable"}
    x = np.asarray(s.values, dtype=float)
    idx = pd.to_datetime(s.index)
    lock = np.array([str(d.date()) >= LOCKBOX_START for d in idx])
    cal = selection_lockbox_calibration(x, lock, periods_per_year=PPY, label=cid)
    rec = _record(cal, "selection-period mean net return tested once on the lockbox")
    dist = _horizon_distribution(x, periods_per_horizon=BLOCK_SESSIONS)
    st = S.nw_tstat(x, 0)
    units = "fraction over %d sessions, net of cost" % BLOCK_SESSIONS
    mean_h = dist.get("mean")
    cd_row = next((b for b in (cad.get("brief") or []) if b.get("cell_id") == cid), {})
    xd = read_artifact("cross_domain.json") or {}
    res = xd.get("result") or {}
    p = FC.sleeve_forecast(
        sleeve=cid, as_of=str(idx[-1].date()), horizon_sessions=BLOCK_SESSIONS,
        model_id="alpha_recovery_fx_carry_cadence_v1",
        expected_return=(FC.expected_return(mean_h, calibration=rec, units=units)
                         if cal["calibrated"] and mean_h is not None else
                         FC.unavailable(cal.get("reason") or "no calibrated mapping", units)),
        expected_risk=FC.field(dist.get("std"), units="std of the %d-session net return" % BLOCK_SESSIONS)
        if dist.get("std") is not None else FC.unavailable("too few blocks"),
        downside=(FC.field(dist.get("probability_negative"),
                           units="probability the %d-session net return is negative" % BLOCK_SESSIONS)
                  if dist.get("probability_negative") is not None else FC.unavailable("too few blocks")),
        confidence=FC.field(st["t"], units="Newey-West t of the mean net return")
        if st.get("t") is not None else FC.unavailable("no t"),
        capital_applicability={
            "book": "cross-sectional long-short dated-contract FX futures, risk-controlled",
            "rebalance_sessions": chosen.get("trade_every"),
            "adds_utility_to_the_incumbent_at_equal_risk": res.get("positive_incremental_utility_after_costs"),
            "incremental_ann_net_return_on_the_incumbent": res.get("incremental_ann_net_return"),
            "t_incremental": res.get("t_incremental")},
        liquidity_cost_assumptions={"per_market_cost_bps_per_side": "R41 dated-contract table",
                                    "ann_oneway_turnover": cd_row.get("augmented_turnover")},
        evidence_maturity="HISTORICAL_OOS_ONLY",
        information=["CARRY"],
        notes=cad.get("evidence_label"))
    return {"family": FAM_SLEEVE, "cell_id": cid, "calibration": cal,
            "horizon_distribution": dist, "t_mean": st.get("t"), "product": p,
            "expected_return_state": p["expected_return"]["state"],
            "valid": FC.validate(p)["valid"],
            "answer": ("the sleeve carries a lockbox-tested expected return"
                       if cal["calibrated"] else
                       "the sleeve's expected return is UNAVAILABLE: %s" % cal.get("reason"))}


# --------------------------------------------------------------------------- #
def build(*, write: bool = True) -> dict:
    fams = {FAM_MARKET: market_family(), FAM_XS: equity_cross_section_family(),
            FAM_BOOK: equity_book_family(), FAM_SLEEVE: sleeve_family()}
    licensed = []
    if fams[FAM_MARKET].get("n_licensed"):
        licensed.append(FAM_MARKET)
    if fams[FAM_XS].get("economic_units_licensed"):
        licensed.append(FAM_XS)
    if fams[FAM_BOOK].get("n_licensed"):
        licensed.append(FAM_BOOK)
    if fams[FAM_SLEEVE].get("expected_return_state") == FC.VALUE:
        licensed.append(FAM_SLEEVE)
    body = {"schema": "alpha_recovery_forecast_products/1", "calculation_owner": CALCULATION_OWNER,
            "generated_at_note": "the artifact hash excludes generated_at",
            "question": "what can this estate predict today, in economic units?",
            "contract": "alpha_agent.alpha_recovery.forecast_contract (a score is never an "
                        "expected return; an uncalibrated field is UNAVAILABLE with a reason)",
            "calibration_rule": ("sign agreement between the selection and lockbox means, the "
                                 "lockbox mean inside a 95 %% predictive interval around the "
                                 "selection mean, and at least %d lockbox observations"
                                 % MIN_EFFECTIVE_PERIODS),
            "families": fams, "licensed_families": licensed,
            "n_families": len(fams), "n_licensed_families": len(licensed),
            "answer": ("%d of %d product families carry a lockbox-tested economic forecast: %s"
                       % (len(licensed), len(fams), ", ".join(licensed) or "none")),
            "checked_at": now_iso(),
            "research_only": True}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
