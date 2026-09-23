r"""Build PORTFOLIO_FEASIBILITY.json - the artifact the R66 director commissioned.

READ ONLY. Registers no hypothesis, tests nothing, computes no return, charges
no burden. Run:

    C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe ^
        research\agents\campaign_r66_hedged_rv\build_feasibility.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import feasibility as F                                          # noqa: E402

OUT = Path(__file__).resolve().parent / "PORTFOLIO_FEASIBILITY.json"

#: The structures measured. Chosen to span the mandate's named families, NOT to
#: be a candidate list - several of these are settled mechanisms and appear here
#: only so the feasibility question is answered for the family the operator
#: named. Feasibility is independent of whether a trade works.
STRUCTURES = [
    ("ZN", "ZB", "US_10s30s_CURVE", "C_YIELD_CURVE_RV"),
    ("ZT", "ZN", "US_2s10s_CURVE", "C_YIELD_CURVE_RV"),
    ("ZF", "ZN", "US_5s10s_CURVE", "C_YIELD_CURVE_RV"),
    ("ZN", "SR3", "US_SWAP_SPREAD_PROXY_r66_01", "A_TREASURY_VS_SWAP_PROXY"),
    ("6A", "GC", "AUD_vs_GOLD_TERMS_OF_TRADE", "E_FX_COMMODITY_RV"),
    ("6C", "CL", "CAD_vs_CRUDE_TERMS_OF_TRADE", "E_FX_COMMODITY_RV"),
    ("HO", "CL", "CRACK_HO_CL", "D_FUTURES_RV"),
    ("ZM", "ZS", "CRUSH_ZM_ZS", "D_FUTURES_RV"),
    ("ZC", "ZW", "FEED_ZC_ZW", "D_FUTURES_RV"),
    ("ZC", "ZM", "FEED_ZC_ZM", "D_FUTURES_RV"),
    ("LE", "HE", "LIVE_LE_HE", "D_FUTURES_RV"),
]


def book_state() -> dict:
    from paper_trader.api.portfolio_state import load_portfolio_state

    s = load_portfolio_state()
    cap, dates = s["capital"], s["dates"]
    return {"nav_usd": cap["nav"], "free_cash_usd": cap["cash"],
            "invested_usd": cap["invested_value"],
            "cash_fraction_of_nav": cap["cash"] / cap["nav"],
            "holdings_count": s["active_book"]["holdings_count"],
            "eligible_market_date": dates["eligible_market_date"],
            "initial_capital": cap["initial_capital"],
            "cumulative_return_pct": cap["cumulative_return"],
            "max_drawdown_pct": cap["max_drawdown"],
            "benchmark_cumulative_return_pct": cap["benchmark_cumulative_return"],
            "state": s["state"], "owner": "api.portfolio_state"}


def forward_portfolio() -> dict:
    """The prospective book AS A PORTFOLIO, which is what the director asked."""
    p = Path(r"D:\Stock_Prediction_app_data\canonical_forward_accrual"
             r"\accrual_projection.json")
    if not p.exists():
        return {"state": "ACCRUAL_PROJECTION_ABSENT"}
    d = json.loads(p.read_text(encoding="utf-8"))
    rows, by_class = [], {}
    for v in (d.get("by_identity") or {}).values():
        rows.append({"challenger_id": v.get("challenger_id"),
                     "asset_class": v.get("asset_class"),
                     "accrual_state": v.get("current_accrual_state"),
                     "predictions_emitted": v.get("predictions_emitted") or 0,
                     "pending_observations": v.get("pending_observations") or 0,
                     "matured_observations": v.get("matured_observations") or 0,
                     "forfeitures": v.get("forfeitures") or 0,
                     "horizon_sessions": v.get("horizon_sessions"),
                     "cadence_sessions": v.get("cadence_sessions"),
                     "last_emission_session": v.get("last_emission_session")})
        by_class[v.get("asset_class")] = by_class.get(v.get("asset_class"), 0) + 1
    return {
        "owner": d.get("owner"), "generated_at": d.get("generated_at"),
        "n_registered": d.get("n_registered"),
        "predictions_emitted_total": d.get("predictions_emitted_total"),
        "matured_observations_total": d.get("matured_observations_total"),
        "effective_independent_observations_total":
            d.get("effective_independent_observations_total"),
        "forfeitures_total": d.get("forfeitures_total"),
        "concentration_by_asset_class": by_class,
        "members": sorted(rows, key=lambda r: str(r["challenger_id"])),
        "diversification_read": (
            "Five of eight registrations are a single asset class (four "
            "US_EQUITY plus two US_ETF on the same underlying, SPY), so the "
            "prospective book is concentrated in US equity risk and is NOT a "
            "diversified portfolio. The only non-equity members are one "
            "FX_FUTURES carry and one MULTI_ASSET_FUTURES trend."),
        "capacity_read": (
            "Capacity is not binding and cannot be, because NOTHING HAS "
            "MATURED. matured_observations_total = 0 and "
            "effective_independent_observations_total = 0 across all eight "
            "registrations, so there is no forward evidence to allocate "
            "against - only five pending predictions. No capacity, "
            "correlation or diversification statistic computed on this book "
            "would be measuring anything yet."),
        "forfeiture_read": (
            "forfeitures_total = 0 is CORRECT under the accrual contract and "
            "is not a cleaned-up counter. The contract reserves forfeiture "
            "for a frozen decision whose emission window shut; a cadence "
            "boundary at which the owner never froze anything is "
            "AWAITING_NEW_GOVERNED_FREEZE, a fact about governance. R66 did "
            "not alter that definition."),
    }


def main() -> int:
    bk = book_state()
    nav, cash = bk["nav_usd"], bk["free_cash_usd"]

    results = []
    for lo, sh, label, family in STRUCTURES:
        r = F.structure_feasibility(lo, sh, nav=nav, free_cash=cash, label=label)
        r["mandate_family"] = family
        results.append(r)

    ok = [r for r in results if r.get("verdict")]
    counts = {}
    for r in ok:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1

    cash_policy = []
    for pct in (5, 10, 15, 20, 25, 30):
        c = nav * pct / 100.0
        cash_policy.append({
            "cash_pct_of_nav": pct, "cash_usd": round(c, 2),
            "pass_initial_margin": sum(
                1 for r in ok
                if r["committed_capital_initial_margin_usd"] <= c),
            "pass_margin_plus_variation_reserve": sum(
                1 for r in ok
                if r["cash_needed_with_variation_reserve_usd"] <= c),
            "pass_everything_incl_gross_notional": sum(
                1 for r in ok
                if r["cash_needed_with_variation_reserve_usd"] <= c
                and r["gross_notional_over_nav"] <= F.MAX_GROSS_NOTIONAL_OVER_NAV)})

    body = {
        "schema": "r66_portfolio_feasibility/1",
        "campaign_id": "R66_HEDGED_RELATIVE_VALUE",
        "calculation_owner": F.CALCULATION_OWNER,
        "commissioned_by": "quant-research-director, DIRECTOR_R66_RULING.json ruling 6",
        "read_only": True,
        "registers_no_hypothesis": True,
        "burden_charged": 0,
        "question": (
            "Is a multi-leg futures structure EXPRESSIBLE at this book's size "
            "at all? This is prior to, and independent of, whether any of "
            "these trades is profitable."),
        "operational_book": bk,
        "declared_assumptions": {
            "span_credit_state": F.SPAN_CREDIT_STATE,
            "span_credit_default_used_for_every_verdict": F.SPAN_CREDIT_DEFAULT,
            "span_credit_sensitivity_reported": list(F.SPAN_CREDIT_SENSITIVITY),
            "span_note": (
                "The estate owns no SPAN parameter file and no exchange "
                "spread-credit table. Every committed-capital figure is "
                "therefore an UPPER BOUND at 0% credit, and the sensitivity "
                "shows how far each verdict depends on an assumption that "
                "cannot be verified from owned data."),
            "margin_pit_state": "CURRENT_ONLY_NOT_POINT_IN_TIME",
            "margin_source": "R38 futures_market_registry metadata.margin, 105 markets",
            "hedge_ratio_method": (
                "equalises the two legs' daily dollar volatility over the "
                "trailing 504 sessions. A volatility is a risk characteristic, "
                "not a performance statistic, so nothing here reads any "
                "spread's P&L."),
            "hedge_error_tolerance": F.HEDGE_ERROR_TOLERANCE,
            "max_gross_notional_over_nav": F.MAX_GROSS_NOTIONAL_OVER_NAV,
            "variation_margin_reserve_multiple": F.VARIATION_MARGIN_RESERVE_MULTIPLE,
            "prices_as_of": "2026-08-21, the frozen R38 layer's last settle",
        },
        "verdict_counts": counts,
        "structures": results,
        "cash_policy_sensitivity": cash_policy,
        "findings": {
            "headline": (
                "ZERO of the eleven structures is implementable by this book "
                "today, and the constraint that stops almost all of them is "
                "NOT margin."),
            "two_constraints_that_fail_differently": (
                "MARGIN is a POLICY constraint - it depends on how much cash "
                "the book chooses to hold and could be relieved by selling "
                "equities. GROSS NOTIONAL is a STRUCTURAL constraint - it is "
                "set by the exchange's contract size against this book's NAV "
                "and NO cash policy changes it. That is why raising cash to "
                "25% of NAV lets nine of eleven post margin and still leaves "
                "one passing everything."),
            "granularity": (
                "The minimum faithful expression is rarely 1:1. US 2s10s needs "
                "5 ZT : 3 ZN - eight contracts and $1.35M of notional - and "
                "AUD/GOLD cannot be hedged within a 10% tolerance at any size "
                "up to twelve contracts, because one gold contract ($462,830) "
                "is too large to be neutralised by any whole number of AUD "
                "contracts."),
            "one_ZT_is_larger_than_the_whole_book": (
                "One ZT contract is $205,906 of notional against a $99,127 "
                "NAV - 2.08x. One ZN is 1.09x. The US Treasury relative-value "
                "programme the mandate asked for first is dimensionally "
                "incompatible with a $100k account, and it is not close."),
            "the_leverage_trap": (
                "Return on committed capital is not a portfolio return. The "
                "one structure that can fit carries $82,450 of gross notional "
                "against $4,565 of margin - about 18:1. A 1% return on gross "
                "notional is 0.83% of NAV and simultaneously 18% 'return on "
                "committed capital'. The second number is the first "
                "multiplied by the margin ratio, and quoting it as portfolio "
                "performance would be the undocumented leverage the mandate "
                "forbids."),
            "what_would_fix_it_and_whether_we_own_it": (
                "Micro futures are the instrument class designed for this "
                "problem and would cut notional roughly tenfold. The estate "
                "owns NO micro Treasury and NO micro FX data: the only micros "
                "in the 105-market registry are equity index (MES, MNQ, M2K, "
                "MYM) and crypto, and none of them reached the certified "
                "68-market dated-contract layer either."),
            "the_blocker_named_precisely": (
                "Not economics, not evidence, and not approval - INSTRUMENT "
                "GRANULARITY, plus the absence of owned micro-contract data."),
        },
        "forward_evidence_portfolio": forward_portfolio(),
        "what_this_does_not_say": [
            "It does not say these trades are unprofitable; profitability is a separate question ruled on elsewhere in this campaign.",
            "It does not say the book should hold more cash; it says what would follow if it did.",
            "It is not an allocation, a proposal, an approval or a promotion.",
        ],
    }
    OUT.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
    print("WROTE %s" % OUT)
    print("verdicts: %s" % counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
