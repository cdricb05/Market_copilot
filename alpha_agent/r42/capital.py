"""alpha_agent.r42.capital - Track E: the denominator is part of the claim.

This module is the ONE owner of the COMPLETE R42 economic equation. It
assembles, per day and per unit of LEG notional:

    funding cashflow
  + basis P&L (spot leg minus perp leg)
  - execution cost (fees + spread, both legs, on position change)
  - spot borrow (only when the position is short spot)
  = book P&L on traded notional

and then divides by the COMMITTED CAPITAL the book actually immobilises
and subtracts the risk-free rate that capital would otherwise have earned.

Why this matters: R41 scored a DELTA_NEUTRAL_BASIS stream against a ZERO
control - the convention reserved for self-financing RV books. A
cash-and-carry is the opposite of self-financing. Buying spot ties up 100%
of the notional in a non-interest-bearing coin; the perpetual leg ties up
margin in non-interest-bearing stablecoin. Scoring that against zero
silently credits the strategy with the entire risk-free rate it forgoes.

Nothing here modifies R41. Every stream produced carries a NEW R42
identity.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, R41_RESEARCH_ROOT, artifact_body, sha
from . import contract as C
from . import execution as EX
from . import legs as LG
from . import pnl_audit as PA
from . import write_artifact
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r42.capital"
ARTIFACT = "CAPITAL_EFFICIENCY_REPORT.json"

FRED_PANEL = (R41_RESEARCH_ROOT / "_data_fred" / "fred_daily_panel.csv")


# --------------------------------------------------------------------------- #
# Risk-free rate
# --------------------------------------------------------------------------- #
_RF_CACHE = None


def risk_free_daily(index: pd.DatetimeIndex) -> pd.Series:
    """Daily risk-free rate, calendar-day compounded, on the crypto clock.

    Crypto trades 7 days a week; the money-market series do not. The rate
    in force on a weekend is the last published rate, which is exactly
    what an overnight cash balance would have earned.
    """
    global _RF_CACHE
    if _RF_CACHE is None:
        df = pd.read_csv(FRED_PANEL, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True)
        cols = [c for c in C.RISK_FREE_SERIES_PREFERENCE if c in df.columns]
        s = None
        for c in cols:
            v = pd.to_numeric(df[c], errors="coerce")
            s = v if s is None else s.fillna(v)
        _RF_CACHE = (s.astype(float) / 100.0).sort_index()
    rf = _RF_CACHE.reindex(_RF_CACHE.index.union(index)).ffill() \
        .reindex(index)
    return (rf / 365.0).rename("rf_daily")


def risk_free_summary(index: pd.DatetimeIndex) -> dict:
    rf = risk_free_daily(index)
    return {"series_preference": list(C.RISK_FREE_SERIES_PREFERENCE),
            "source": C.RISK_FREE_SOURCE,
            "mean_annualised": float(rf.mean() * 365.0),
            "min_annualised": float(rf.min() * 365.0),
            "max_annualised": float(rf.max() * 365.0),
            "n_days": int(rf.notna().sum())}


# --------------------------------------------------------------------------- #
# The complete book
# --------------------------------------------------------------------------- #
def implementable_book(df: pd.DataFrame, signal: pd.Series, *,
                       capital_model: str = None,
                       execution_model: str = None,
                       cost_multiplier: float = 1.0,
                       borrow_annual: float = None,
                       buffer_earns_rf: bool = False,
                       charge_financing: bool = True) -> pd.DataFrame:
    """Assemble the complete equation for one position stream.

    ``signal`` is the position DECIDED on each date; the position actually
    HELD on date t is ``signal.shift(1)`` - the same convention R41 used,
    preserved exactly so comparisons are like-for-like.
    """
    capital_model = capital_model or C.PRIMARY_CAPITAL_MODEL
    execution_model = execution_model or C.PRIMARY_EXECUTION_MODEL
    K = float(C.CAPITAL_MODELS[capital_model]["denominator"])
    held = signal.shift(1)
    on = (held != 0) & held.notna()

    funding_pnl = held * df["funding"]
    basis_pnl = held * df["basis_ret"]
    gross = (funding_pnl + basis_pnl).fillna(0.0)

    pos_change = signal.diff().abs()
    fees = EX.cost_stream(pos_change, execution_model, cost_multiplier)

    if borrow_annual is None:
        borrow = pd.Series(0.0, index=df.index)
    else:
        borrow = (held < 0).astype(float) * (borrow_annual / 365.0)

    rf = risk_free_daily(df.index).fillna(0.0)
    # Capital immobilised while the book is on. The variation buffer may
    # optionally be modelled as remaining in an interest-bearing account.
    if buffer_earns_rf and capital_model == "CONSERVATIVE_COLLATERAL":
        idle = K - float(
            C.CAPITAL_MODELS["FULLY_FUNDED_COMMITTED"]["denominator"])
    else:
        idle = 0.0
    financing = on.astype(float) * rf * (K - idle) if charge_financing \
        else pd.Series(0.0, index=df.index)

    pnl_on_notional = gross - fees - borrow
    pnl_on_capital = pnl_on_notional / K
    # When flat, the capital sits in cash and earns rf; excess is zero.
    # ``charge_financing=False`` reproduces R41's ZERO control exactly.
    benchmark = (on.astype(float) * rf) if charge_financing \
        else pd.Series(0.0, index=df.index)
    excess = pnl_on_capital - benchmark

    return pd.DataFrame({
        "held": held, "on": on.astype(float),
        "funding_pnl": funding_pnl, "basis_pnl": basis_pnl, "gross": gross,
        "fees": fees, "borrow": borrow, "financing": financing,
        "pnl_on_notional": pnl_on_notional,
        "pnl_on_capital": pnl_on_capital,
        "rf_daily": rf, "benchmark": benchmark, "excess": excess,
    }, index=df.index)


def score(book: pd.DataFrame, zone, *, overlap: int = 1) -> dict:
    """Score the EXCESS stream with the canonical R41 evidence owner."""
    d = book.reindex(zone)
    card = EV.scorecard(d["pnl_on_capital"].to_numpy(),
                        np.zeros(len(d)),
                        d["benchmark"].to_numpy(),
                        periods_per_year=PA.R41_PPY, overlap=overlap)
    return card


def denominator_table(df: pd.DataFrame, signal: pd.Series, zone, *,
                      execution_model: str = None,
                      cost_multiplier: float = 1.0,
                      borrow_annual: float = None) -> dict:
    """The same book, reported on every declared denominator."""
    out = {}
    for name, spec in C.CAPITAL_MODELS.items():
        bk = implementable_book(df, signal, capital_model=name,
                                execution_model=execution_model,
                                cost_multiplier=cost_multiplier,
                                borrow_annual=borrow_annual)
        d = bk.reindex(zone)
        card = EV.scorecard(d["pnl_on_capital"].to_numpy(),
                            np.zeros(len(d)), d["benchmark"].to_numpy(),
                            periods_per_year=PA.R41_PPY, overlap=1)
        out[name] = {
            "denominator": spec["denominator"],
            "note": spec["note"],
            "return_on_capital_ann": float(
                np.nanmean(d["pnl_on_capital"]) * PA.R41_PPY),
            "risk_free_drag_ann": float(
                np.nanmean(d["benchmark"]) * PA.R41_PPY),
            "excess_over_rf_ann": card.get("excess_ann"),
            "excess_t_hac": card.get("excess_t_hac"),
            "sharpe": card.get("sharpe"),
            "vol_ann": card.get("vol_ann"),
            "max_drawdown": card.get("max_drawdown"),
            "is_primary": name == C.PRIMARY_CAPITAL_MODEL,
        }
    return out


def margin_utilisation(df: pd.DataFrame, signal: pd.Series,
                       capital_model: str = None) -> dict:
    capital_model = capital_model or C.PRIMARY_CAPITAL_MODEL
    K = float(C.CAPITAL_MODELS[capital_model]["denominator"])
    im = 0.20
    buf = K - 1.0 - im
    return {
        "capital_model": capital_model,
        "committed_capital_per_unit_leg_notional": K,
        "spot_cash": 1.0, "perp_initial_margin": im,
        "variation_buffer": round(buf, 4),
        "gross_exposure_per_unit_capital": round(2.0 / K, 4),
        "effective_leverage_on_capital": round(2.0 / K, 4),
        "margin_utilisation_at_entry": round(im / (im + buf), 4),
        "liquidation_buffer_fraction_of_perp_notional": round(im + buf, 4),
        "note": "no leverage is used: gross exposure 2.0 is carried on "
                "%.2f of capital, and the perpetual leg is over-"
                "collateralised by %.0f%% of its notional" % (K,
                                                              (im + buf) * 100),
    }


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def run() -> dict:
    df = PA.r41_panel("BTCUSDT")
    z = PA.r41_zones(df.index)
    r41_sig = df["signal"]
    clipped = LG.r41_signal_positive_clipped(df)
    pos_only = LG.positive_only_signal(df)

    variants = {
        "R41_AS_SCORED": {
            "signal": r41_sig, "execution": "R41_BASELINE",
            "charge_financing": False,
            "note": "R41 exactly: 5bps x 2 legs, zero control, one leg's "
                    "notional as the denominator"},
        "R41_RULE_FULL_ECONOMICS": {
            "signal": r41_sig, "execution": C.PRIMARY_EXECUTION_MODEL,
            "charge_financing": True,
            "note": "the SAME R41 positions, with the complete equation: "
                    "real fee schedule + spread, conservative committed "
                    "capital, risk-free control. Reverse leg still "
                    "included, which the borrow evidence forbids - shown "
                    "only to isolate the capital effect"},
        "R42_R41RULE_POSITIVE_CLIPPED": {
            "signal": clipped, "execution": C.PRIMARY_EXECUTION_MODEL,
            "charge_financing": True,
            "note": "R41 z-gate, reverse leg removed, full economics"},
        "R42_POSITIVE_ONLY_CASH_AND_CARRY": {
            "signal": pos_only, "execution": C.PRIMARY_EXECUTION_MODEL,
            "charge_financing": True,
            "note": "the predeclared implementability-first baseline, full "
                    "economics"},
    }

    results = {}
    for name, spec in variants.items():
        per_zone = {}
        for zn in ("A", "B", "C"):
            bk = implementable_book(
                df, spec["signal"],
                capital_model=("TRADED_NOTIONAL"
                               if name == "R41_AS_SCORED"
                               else C.PRIMARY_CAPITAL_MODEL),
                execution_model=spec["execution"],
                charge_financing=spec["charge_financing"])
            d = bk.reindex(z[zn])
            card = EV.scorecard(d["pnl_on_capital"].to_numpy(),
                                np.zeros(len(d)), d["benchmark"].to_numpy(),
                                periods_per_year=PA.R41_PPY, overlap=1)
            per_zone[zn] = {
                "range": z["%s_range" % zn.lower()],
                "n_days": int(len(d)),
                "gross_ann_on_notional": float(np.nanmean(d["gross"])
                                               * PA.R41_PPY),
                "fees_ann_on_notional": float(np.nanmean(d["fees"])
                                              * PA.R41_PPY),
                "return_on_capital_ann": float(np.nanmean(d["pnl_on_capital"])
                                               * PA.R41_PPY),
                "risk_free_drag_ann": float(np.nanmean(d["benchmark"])
                                            * PA.R41_PPY),
                "excess_ann": card.get("excess_ann"),
                "excess_t_hac": card.get("excess_t_hac"),
                "sharpe": card.get("sharpe"),
                "vol_ann": card.get("vol_ann"),
                "max_drawdown": card.get("max_drawdown"),
                "ess": (card.get("effective_sample") or {}).get("ess"),
                "positive": bool((card.get("excess_ann") or 0) > 0),
            }
        results[name] = {"note": spec["note"], "zones": per_zone}

    denoms = {zn: denominator_table(df, r41_sig, z[zn])
              for zn in ("B", "C")}

    body = artifact_body("r42_capital_efficiency/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "E - capital denominator / true return on capital",
        "risk_free": risk_free_summary(df.index),
        "control_rationale": C.CONTROL_RATIONALE,
        "capital_models": C.CAPITAL_MODELS,
        "primary_capital_model": C.PRIMARY_CAPITAL_MODEL,
        "margin_utilisation": margin_utilisation(df, r41_sig),
        "variants": results,
        "denominator_table": denoms,
        "authoritative_primary_roic": _primary(results),
        "r41_candidate_modified": False,
        "verdict": _verdict(results),
    })
    body["capital_efficiency_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _primary(results: dict) -> dict:
    v = results["R42_POSITIVE_ONLY_CASH_AND_CARRY"]["zones"]
    return {
        "candidate": "R42_POSITIVE_ONLY_CASH_AND_CARRY",
        "capital_model": C.PRIMARY_CAPITAL_MODEL,
        "execution_model": C.PRIMARY_EXECUTION_MODEL,
        "control": C.PRIMARY_CONTROL,
        "zone_b_roic_ann": v["B"]["return_on_capital_ann"],
        "zone_b_excess_over_rf_ann": v["B"]["excess_ann"],
        "zone_b_t": v["B"]["excess_t_hac"],
        "zone_c_roic_ann": v["C"]["return_on_capital_ann"],
        "zone_c_excess_over_rf_ann": v["C"]["excess_ann"],
        "zone_c_t": v["C"]["excess_t_hac"],
        "one_authoritative_number": True,
    }


def _verdict(results: dict) -> dict:
    r41 = results["R41_AS_SCORED"]["zones"]
    full = results["R41_RULE_FULL_ECONOMICS"]["zones"]
    prim = results["R42_POSITIVE_ONLY_CASH_AND_CARRY"]["zones"]
    kills = (full["C"]["excess_ann"] or 0) <= 0 \
        or (prim["C"]["excess_ann"] or 0) <= 0
    return {
        "state": ("R42_CAPITAL_EFFICIENCY_KILLS_EDGE" if kills
                  else "SURVIVES_CAPITAL_CORRECTION"),
        "zone_c_r41_as_scored_ann": r41["C"]["excess_ann"],
        "zone_c_full_economics_ann": full["C"]["excess_ann"],
        "zone_c_primary_ann": prim["C"]["excess_ann"],
        "zone_b_r41_as_scored_ann": r41["B"]["excess_ann"],
        "zone_b_full_economics_ann": full["B"]["excess_ann"],
        "zone_b_primary_ann": prim["B"]["excess_ann"],
        "note": "the capital correction is not a haircut on the return; it "
                "changes both the numerator (real fees and spread) and the "
                "denominator (committed capital) and introduces the "
                "risk-free control the book actually forgoes.",
    }
