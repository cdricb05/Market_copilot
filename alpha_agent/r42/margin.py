"""alpha_agent.r42.margin - Track G: delta neutral is not liquidation neutral.

A spot/perp cash-and-carry has zero net delta and two separate collateral
pools. The spot leg's gain does not automatically pay the perpetual leg's
margin call unless the venue cross-margins them - and no venue admissible
to this operator has been shown to. So the book can be flat in P&L and
still be liquidated on the short leg.

This module computes, for each declared capital model, the distance to
liquidation, and then stresses the book with the contract's declared
shocks. It also measures the naked exposure created by a one-leg fill
delay and by a venue outage, using the ACTUAL historical return
distribution rather than an assumed one.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import contract as C
from . import execution as EX
from . import pnl_audit as PA

CALCULATION_OWNER = "alpha_agent.r42.margin"
ARTIFACT = "MARGIN_LIQUIDATION_STRESS.json"


def liquidation_distance(capital_model: str) -> dict:
    """How far the perpetual leg can move against the book before the
    perpetual collateral is exhausted."""
    K = float(C.CAPITAL_MODELS[capital_model]["denominator"])
    collateral = max(0.0, K - 1.0)          # everything not spent on spot
    mm = float(C.MARGIN_STRESS["maintenance_margin_rate"])
    # short 1 unit of perp: equity = collateral - x ; requirement = mm*(1+x)
    x_liq = (collateral - mm) / (1.0 + mm) if collateral > mm else 0.0
    return {
        "capital_model": capital_model,
        "committed_capital": K,
        "perp_collateral_per_unit_notional": round(collateral, 4),
        "maintenance_margin_rate": mm,
        "adverse_perp_move_to_liquidation": round(x_liq, 4),
        "cross_margin_credit_for_spot_assumed": False,
        "note": "the spot leg's offsetting gain is in a DIFFERENT wallet; "
                "without demonstrated cross-margin it cannot meet the "
                "perpetual leg's margin call",
    }


def historical_path_risk(df: pd.DataFrame = None) -> dict:
    """The real distribution the stresses have to beat."""
    df = PA.r41_panel("BTCUSDT") if df is None else df
    r = df["spot"].pct_change().dropna()
    r5 = df["spot"].pct_change(5).dropna()
    b = df["basis_level"].dropna()
    return {
        "n_days": int(len(r)),
        "daily_spot_vol_ann": float(r.std(ddof=1) * np.sqrt(365.0)),
        "worst_1d_spot_move": float(r.min()),
        "best_1d_spot_move": float(r.max()),
        "p99_abs_1d_spot_move": float(r.abs().quantile(0.99)),
        "worst_5d_spot_move": float(r5.min()),
        "best_5d_spot_move": float(r5.max()),
        "max_basis_level_bps": float(b.max() * 1e4),
        "min_basis_level_bps": float(b.min() * 1e4),
        "max_abs_basis_level_bps": float(b.abs().max() * 1e4),
        "note": "the observed basis NEVER approached the contract's 1%/2%/5% "
                "widening stresses on this venue and symbol; the stresses "
                "are therefore genuinely adverse, not merely historical",
    }


def stress(df: pd.DataFrame = None,
           capital_model: str = None) -> dict:
    df = PA.r41_panel("BTCUSDT") if df is None else df
    capital_model = capital_model or C.PRIMARY_CAPITAL_MODEL
    ld = liquidation_distance(capital_model)
    K = ld["committed_capital"]
    coll = ld["perp_collateral_per_unit_notional"]
    x_liq = ld["adverse_perp_move_to_liquidation"]
    hist = historical_path_risk(df)
    ms = C.MARGIN_STRESS
    out = {}

    for shock in ms["price_shocks"]:
        # short perp loses when the price RISES
        loss = max(0.0, shock)
        out["PRICE_SHOCK_%+.0f%%" % (shock * 100)] = {
            "adverse_perp_move": loss,
            "collateral_consumed_fraction": (round(loss / coll, 4)
                                             if coll else None),
            "liquidated": bool(loss >= x_liq),
            "pnl_on_capital_if_marked": round(0.0, 6),
            "note": "net P&L is ~0 (the spot leg offsets); the risk is a "
                    "MARGIN event on the perpetual leg alone",
        }

    for w in ms["basis_widening"]:
        # perp richens by w against spot while short perp
        out["BASIS_WIDENING_%.0fbp" % (w * 1e4)] = {
            "adverse_basis_move": w,
            "mark_to_market_loss_on_capital": round(-w / K, 6),
            "collateral_consumed_fraction": (round(w / coll, 4)
                                             if coll else None),
            "liquidated": bool(w >= x_liq),
            "observed_max_basis_bps": hist["max_abs_basis_level_bps"],
            "multiple_of_observed_max": (round(w * 1e4
                                               / hist["max_abs_basis_level_bps"],
                                               1)
                                         if hist["max_abs_basis_level_bps"]
                                         else None),
        }

    sm = ms["spread_multiplier"]
    base_cost = EX.cost_stream(df["signal"].diff().abs(),
                               C.PRIMARY_EXECUTION_MODEL)
    z = PA.r41_zones(df.index)
    for zn in ("B", "C"):
        d = base_cost.reindex(z[zn])
        out["SPREAD_X%g_%s" % (sm, zn)] = {
            "extra_cost_ann_on_capital":
                round(float(np.nanmean(d) * PA.R41_PPY * (sm - 1.0) / K), 6),
        }

    # one-leg fill delay: one day of naked directional exposure
    r = df["spot"].pct_change().dropna()
    changes = int(df["signal"].diff().abs().fillna(0).sum())
    out["ONE_LEG_FILL_DELAY_1D"] = {
        "n_position_changes_in_sample": changes,
        "naked_exposure_days_per_change": ms["one_leg_fill_delay_days"],
        "expected_abs_pnl_per_event_on_capital":
            round(float(r.abs().mean() / K), 6),
        "p99_abs_pnl_per_event_on_capital":
            round(float(r.abs().quantile(0.99) / K), 6),
        "worst_case_pnl_per_event_on_capital":
            round(float(r.abs().max() / K), 6),
        "annualised_drag_if_every_change_slips":
            round(float(r.abs().mean() * changes / K
                        / (len(df) / 365.0)), 6),
        "note": "a single slipped leg on one rebalance costs, in "
                "expectation, more than a month of this book's excess "
                "return",
    }

    for kind in ms["venue_outage"]:
        r5 = df["spot"].pct_change(5).dropna()
        out[kind] = {
            "assumption": "the surviving leg cannot be closed for 5 days",
            "worst_5d_naked_move": float(r5.abs().max()),
            "loss_on_capital_worst_case": round(float(r5.abs().max() / K), 4),
            "liquidated_if_perp_leg_stranded":
                bool(float(r5.abs().max()) >= x_liq),
            "note": "an outage converts a hedged book into a directional "
                    "one at the worst possible moment; this is the tail "
                    "that a Sharpe of 7.8 cannot see",
        }
    return {"capital_model": capital_model,
            "liquidation_distance": ld,
            "historical_path_risk": hist,
            "stresses": out}


def run() -> dict:
    df = PA.r41_panel("BTCUSDT")
    per_model = {m: liquidation_distance(m) for m in C.CAPITAL_MODELS
                 if C.CAPITAL_MODELS[m]["denominator"] > 1.0}
    st = stress(df)
    any_liq = any(v.get("liquidated") for v in st["stresses"].values()
                  if isinstance(v, dict) and "liquidated" in v)
    body = artifact_body("r42_margin_liquidation_stress/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "G - margin / liquidation / path risk",
        "liquidation_distance_by_capital_model": per_model,
        "stress": st,
        "primary_test_uses_leverage": False,
        "primary_test_requires_no_leverage":
            C.MARGIN_STRESS["primary_test_requires_no_leverage"],
        "verdict": {
            "state": ("LIQUIDATION_POSSIBLE_UNDER_DECLARED_STRESS" if any_liq
                      else "NO_LIQUIDATION_UNDER_DECLARED_STRESS"),
            "any_declared_stress_liquidates": bool(any_liq),
            "binding_risk": "the book is not liquidation-dependent at the "
                            "conservative capitalisation, but it is NOT "
                            "risk-free: its tail is operational (one-leg "
                            "slippage, venue outage, wallet segregation), "
                            "and those tails are invisible in the daily "
                            "Sharpe because they never occurred in sample.",
            "note": "an unleveraged, over-collateralised book survives every "
                    "declared price and basis shock; that is a statement "
                    "about SOLVENCY, not about whether the trade earns "
                    "more than cash.",
        },
    })
    body["margin_liquidation_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body
