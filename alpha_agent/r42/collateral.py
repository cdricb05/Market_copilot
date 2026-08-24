"""alpha_agent.r42.collateral - Track O: the risk that has no Sharpe.

A crypto-neutral book still has to keep its capital somewhere. On every
venue tested that somewhere is a stablecoin balance and a spot inventory
held by the exchange itself. The strategy's measured volatility - 0.4 %/yr
on Zone C - describes none of that. This module prices the exposures the
daily P&L cannot see: a stablecoin depeg, a collateral haircut, a
withdrawal freeze and a counterparty impairment, each expressed as a
multiple of the edge it would consume.
"""
from __future__ import annotations

import numpy as np

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import capital as CAP
from . import contract as C
from . import legs as LG
from . import pnl_audit as PA

CALCULATION_OWNER = "alpha_agent.r42.collateral"
ARTIFACT = "COLLATERAL_COUNTERPARTY_STRESS.json"


def exposure_map() -> dict:
    K = float(C.CAPITAL_MODELS[C.PRIMARY_CAPITAL_MODEL]["denominator"])
    return {
        "capital_model": C.PRIMARY_CAPITAL_MODEL,
        "committed_capital": K,
        "spot_inventory_at_exchange": 1.0,
        "stablecoin_collateral": round(K - 1.0, 4),
        "share_of_capital_in_stablecoin": round((K - 1.0) / K, 4),
        "share_of_capital_at_single_counterparty": 1.0,
        "interest_earned_on_collateral": 0.0,
        "note": "100% of committed capital sits with ONE counterparty, in "
                "assets that pay nothing. Segregation, insurance and legal "
                "recourse are not demonstrated for any venue in the frozen "
                "universe.",
    }


def run() -> dict:
    df = PA.r41_panel("BTCUSDT")
    z = PA.r41_zones(df.index)
    bk = CAP.implementable_book(
        df, LG.positive_only_signal(df),
        capital_model=C.PRIMARY_CAPITAL_MODEL,
        execution_model=C.PRIMARY_EXECUTION_MODEL, charge_financing=True)
    d = bk.reindex(z["C"])
    edge_ann = float(np.nanmean(d["excess"]) * PA.R41_PPY)
    roc_ann = float(np.nanmean(d["pnl_on_capital"]) * PA.R41_PPY)
    exp = exposure_map()
    K = exp["committed_capital"]
    stable_share = exp["share_of_capital_in_stablecoin"]

    def years_of_edge(loss_fraction_of_capital):
        if roc_ann <= 0:
            return None
        return round(loss_fraction_of_capital / roc_ann, 2)

    stresses = {}
    for dp in C.COLLATERAL_STRESS["stablecoin_depeg"]:
        loss = dp * stable_share
        stresses["STABLECOIN_DEPEG_%.1f%%" % (dp * 100)] = {
            "loss_fraction_of_capital": round(loss, 6),
            "loss_ann_equivalent": round(loss, 6),
            "years_of_gross_carry_consumed": years_of_edge(loss),
            "wipes_out_annual_excess": bool(loss > abs(edge_ann)),
        }
    for h in C.COLLATERAL_STRESS["exchange_haircut"]:
        loss = h
        stresses["EXCHANGE_HAIRCUT_%.0f%%" % (h * 100)] = {
            "loss_fraction_of_capital": round(loss, 6),
            "years_of_gross_carry_consumed": years_of_edge(loss),
            "wipes_out_annual_excess": bool(loss > abs(edge_ann)),
        }
    for days in C.COLLATERAL_STRESS["withdrawal_freeze_days"]:
        # capital trapped: it keeps earning carry but cannot be redeployed,
        # and the risk-free alternative is forgone with certainty
        cost = (days / 365.0) * float(np.nanmean(d["rf_daily"]) * 365.0)
        stresses["WITHDRAWAL_FREEZE_%dD" % days] = {
            "forgone_risk_free_fraction_of_capital": round(cost, 6),
            "years_of_gross_carry_consumed": years_of_edge(cost),
            "note": "the freeze itself is survivable; it is the correlated "
                    "case - a freeze BECAUSE the venue is failing - that "
                    "is not",
        }
    for imp in C.COLLATERAL_STRESS["counterparty_impairment"]:
        loss = imp * 1.0
        stresses["COUNTERPARTY_IMPAIRMENT_%.0f%%" % (imp * 100)] = {
            "loss_fraction_of_capital": round(loss, 6),
            "years_of_gross_carry_consumed": years_of_edge(loss),
            "wipes_out_annual_excess": True,
        }

    body = artifact_body("r42_collateral_counterparty_stress/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "O - stablecoin / collateral / cash management",
        "exposure_map": exp,
        "zone_c_gross_return_on_capital_ann": roc_ann,
        "zone_c_excess_over_cash_ann": edge_ann,
        "zone_c_measured_vol_ann": float(np.nanstd(d["excess"], ddof=1)
                                         * np.sqrt(PA.R41_PPY)),
        "stresses": stresses,
        "opportunity_cost_of_collateral_ann":
            float(np.nanmean(d["benchmark"]) * PA.R41_PPY),
        "verdict": {
            "state": "NON_MARKET_TAIL_DOMINATES_THE_EDGE",
            "smallest_stress_that_exceeds_one_year_of_gross_carry":
                _smallest(stresses, roc_ann),
            "note": "a 0.5% stablecoin depeg - the mildest stress in the "
                    "frozen list - costs a meaningful fraction of a YEAR of "
                    "this book's gross carry, and a single counterparty "
                    "impairment costs decades of it. The strategy's 0.4%/yr "
                    "measured volatility is not a risk estimate; it is a "
                    "description of the days on which nothing happened.",
        },
    })
    body["collateral_stress_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _smallest(stresses: dict, roc: float):
    cands = [(k, v) for k, v in stresses.items()
             if (v.get("years_of_gross_carry_consumed") or 0) >= 1.0]
    if not cands:
        return None
    return min(cands, key=lambda kv: kv[1]["years_of_gross_carry_consumed"])[0]
