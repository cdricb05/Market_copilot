r"""alpha_agent.r53.multi_horizon_view - the SHADOW multi-horizon capital
view: strategic and tactical signals for the SAME capital pool, on one page,
with their authority and evidence state attached - and NO naive averaging.

WHAT THIS IS (and is not)
-------------------------
It is a READ MODEL: it composes the canonical owners' current outputs per
instrument per horizon - the operational model's daily percentile (the only
signal with production authority), the frozen R46 challengers' current books
per horizon (SHADOW evidence accruing), and the R53 intraday factory's state
(specs frozen, feed blocked). It derives no new signal, owns no capital, and
feeds nothing into production. Horizons COMPETE for capital only through the
one canonical allocator, and only after promotion gates that this module
cannot touch.

CONFLICT HANDLING - RESEARCHED, NOT CHOSEN
------------------------------------------
The same instrument can carry conflicting horizon signals. The artifact
records the candidate aggregation architectures with their preconditions;
none is adopted, because adopting one today would be a parameter choice made
with zero matured intraday observations and single-digit matured daily
observations. The honest current rule stands: horizons do not average; each
accrues evidence in its own lane; the allocator sees only approved signals.
"""
from __future__ import annotations

from typing import Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53.multi_horizon_view"
ARTIFACT = "R53_MULTI_HORIZON_VIEW.json"

#: Aggregation architectures under research. Each names its preconditions;
#: none is adopted in R53 and the artifact says why.
AGGREGATION_CANDIDATES = [
    {"architecture": "HORIZON_SPECIFIC_SLEEVES",
     "sketch": "each horizon is a sleeve generating opportunities; the "
               "global allocator owns all capital (the Release-32 design "
               "rule verbatim - the architecture the estate already has)",
     "preconditions": "each horizon's sleeve passes the SAME promotion gates",
     "assessment": "the default: no new machinery, no naive averaging; "
                   "conflicts resolve as capital competition under risk and "
                   "cost, not as signal arithmetic"},
    {"architecture": "CONFIDENCE_WEIGHTED_COMBINATION",
     "sketch": "combine horizon signals per instrument, weighted by each "
               "lane's matured forward evidence",
     "preconditions": "calibrated per-lane confidence, which requires "
                      "matured samples per horizon (intraday: zero today)",
     "assessment": "premature; also R44 measured the combination frontier "
                   "and the answer did not depend on the weighting scheme"},
    {"architecture": "EXPECTED_HOLDING_PERIOD_MATCHING",
     "sketch": "route each signal to the turnover/cost budget its half-life "
               "affords; a 30-minute signal may only trade cost-free size",
     "preconditions": "measured signal half-lives; measured intraday costs",
     "assessment": "economically right and evidence-blocked: half-life "
                   "measurement needs the intraday feed"},
    {"architecture": "TACTICAL_OVERLAY_ON_STRATEGIC_CORE",
     "sketch": "strategic book owns capital; a bounded tactical overlay "
               "rents a small risk budget against it",
     "preconditions": "a risk-budget primitive in the canonical allocator "
                      "(the Track-A philosophy change), plus a qualified "
                      "tactical lane",
     "assessment": "the natural end-state IF an intraday lane ever "
                   "qualifies; requires the risk-budget primitive first"},
    {"architecture": "BAYESIAN_ENSEMBLE",
     "sketch": "posterior over returns given all horizon signals",
     "preconditions": "calibrated likelihoods per lane - far beyond current "
                      "evidence; every input is NOT_CALIBRATED today",
     "assessment": "declined for now: calibration theatre without data"},
]

#: The standing rule while no aggregation is adopted.
STANDING_RULE = (
    "horizons do not average. Each horizon accrues evidence in its own "
    "prospective lane; the canonical allocator sees only signals with "
    "operational approval; intraday/delayed movement stays RISK authority "
    "and events stay TRIGGER authority")


def _daily_equity_rows(limit: int = 10) -> tuple[list, Optional[str]]:
    """Current holdings with the operational model's percentile - the ONE
    production-authoritative signal."""
    try:
        from paper_trader.api import portfolio_state as psmod
        from paper_trader.api import universe_scoring as us
        ps = psmod.load_portfolio_state()
        scoring = us.load_universe_scoring()
    except Exception:  # noqa: BLE001 - read model degrades, never invents
        return [], None
    pct = {r.get("ticker"): r for r in (scoring or {}).get("rankings") or []}
    rows = []
    positions = sorted((p for p in (ps.get("positions") or [])
                        if p.get("instrument_type") == "CASH_EQUITY"),
                       key=lambda p: -(p.get("exposure_weight") or 0.0))
    for p in positions[:limit]:
        tk = p.get("instrument_id") or p.get("ticker")
        r = pct.get(tk) or {}
        rows.append({"instrument": tk,
                     "weight": p.get("exposure_weight"),
                     "percentile": r.get("percentile"),
                     "rank": r.get("rank")})
    return rows, (ps.get("dates") or {}).get("eligible_market_date")


def _tactical_futures_rows() -> list:
    """The frozen challengers' CURRENT books per horizon - SHADOW lanes."""
    from ..r46 import challengers as CH
    out = []
    for cid in ("r46_spx_trend_200d", "r52_eqidx_xs_rel_mom_12_1",
                "r46_vx_term_carry_5d", "r51_fx_xs_carry_cip",
                "r52_rates_copper_gold_lead", "r46_comdty_xs_mom_252",
                "r53_fut_xs_value_5y", "r53_comdty_xs_skew_12m"):
        spec = CH.spec_by_id(cid)
        if spec is None:
            continue
        try:
            book = CH.build(spec)
        except Exception:  # noqa: BLE001
            continue
        for leg in (book.get("legs") or [])[:12]:
            out.append({"challenger_id": cid,
                        "family": spec["family"],
                        "horizons_sessions": list(spec["horizons"]),
                        "instrument": leg.get("instrument"),
                        "side": leg.get("side"),
                        "score": leg.get("score"),
                        "evidence_state": "SHADOW_ACCRUING",
                        "authority": "NONE (research shadow)"})
    return out


def compose() -> dict:
    equity_rows, as_of = _daily_equity_rows()
    tactical = _tactical_futures_rows()
    intraday_state = read_json(
        research_dir() / "R53_INTRADAY_FACTORY.json", default=None) or {}

    # ONE instrument's full multi-horizon expression, as the release brief
    # asked for - built from whatever signals actually exist for it today.
    example = None
    if equity_rows:
        tk = equity_rows[0]["instrument"]
        example = {
            "instrument": tk,
            "horizons": {
                "20_sessions_strategic": {
                    "source": "operational model percentile",
                    "value": equity_rows[0].get("percentile"),
                    "authority": "PRODUCTION (approved model)"},
                "intraday_30m": {
                    "source": "r53 intraday factory",
                    "value": None,
                    "state": intraday_state.get("state") or
                             "SPECS_FROZEN_AWAITING_FEED",
                    "authority": "NONE (no feed, no rows)"},
            },
            "conflict_rule": STANDING_RULE,
        }

    body = artifact_body(
        "r53_multi_horizon_view/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        as_of=as_of,
        strategic_daily_rows=equity_rows,
        tactical_shadow_rows=tactical,
        intraday_factory_state={
            "state": intraday_state.get("state"),
            "n_specs": intraday_state.get("n_specs"),
            "families": intraday_state.get("families")},
        example_instrument_view=example,
        aggregation_candidates=AGGREGATION_CANDIDATES,
        aggregation_adopted=None,
        why_no_aggregation_adopted=(
            "zero matured intraday observations and single-digit matured "
            "daily-shadow observations cannot calibrate any combination "
            "rule; adopting one would be a swept parameter, the exact "
            "failure mode R45 measured"),
        standing_rule=STANDING_RULE,
        one_capital_pool=True,
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT, body)
    return body
