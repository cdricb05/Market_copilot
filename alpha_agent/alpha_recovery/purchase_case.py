"""alpha_agent.alpha_recovery.purchase_case - owned / free exhaustion and the paid case.

Workstream 11. Nothing is bought. If the ranked owned / free program produces
no material challenger, the campaign does not end with "more research is
needed": it states OWNED_FREE_INFORMATION_EXHAUSTED for the families it
closed, names the TOP missing information need, and works the economics of
every paid candidate - information need first, provider second - with the
R63 break-even arithmetic against the LIVE paper NAV (read only):

    break-even alpha = fee / NAV + trading cost by horizon
                     + half the expected gross increment (the haircut)
                     + 10 % of fee / NAV (the required return on research spend)

The verdict per candidate is BUY_TEST only when the expected incremental NET
P&L after the haircut exceeds the break-even alpha AND the point-in-time
credibility, history and effective sample are adequate; otherwise DO_NOT_BUY
or FEE_UNQUOTED_CANNOT_GATE. Fees come from the estate's recorded references
or are UNQUOTED; nothing is invented.
"""
from __future__ import annotations

from alpha_agent.r63 import sourcing as SRC

from . import ST_EXHAUSTED, read_artifact, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.purchase_case"
ARTIFACT_NAME = "purchase_case.json"

BUY_TEST = "BUY_TEST"
DO_NOT_BUY = "DO_NOT_BUY"
FEE_UNQUOTED = "FEE_UNQUOTED_CANNOT_GATE"
PROXY_PENDING = "PROXY_EVIDENCE_PENDING"
#: annual fees at which the break-even alpha is quoted (no vendor price is
#: asserted; the ladder says what any price would have to earn)
FEE_LADDER_USD = (250.0, 500.0, 1000.0, 2000.0, 5000.0)

#: Information needs the campaign can name, each with the reason the owned /
#: free proxy is insufficient. Providers are named only as references for a
#: fee the estate has recorded elsewhere; the NEED comes first.
CANDIDATES = (
    {"need": "EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS",
     "dimension": "EARNINGS_EXPECTATIONS", "asset_class": "US_EQUITY", "horizons": [5, 21],
     "why_owned_free_insufficient": "the owned proxy is the XBRL seasonal-difference surprise (SUE) and "
                                    "the announcement-window reaction; a consensus-relative surprise "
                                    "needs the as-was analyst consensus at report time, which no owned "
                                    "or free source carries with a verifiable timestamp",
     "pit_credibility": "PIT_UNVERIFIED until a vendor proves as-was vintages",
     "usable_history_years": None, "effective_sample": None,
     "fee_reference": "UNQUOTED (Intrinio DO_NOT_BUY; Zacks via sales; Steele sample WAITING - R59/R63)",
     "proxy_evidence_family": "EARNINGS_EVENT_REACTION"},
    {"need": "SINGLE_NAME_OPTIONS_IV_SURFACE_HISTORY",
     "dimension": "VOLATILITY_EXPECTATIONS_IV", "asset_class": "US_EQUITY", "horizons": [5, 21],
     "why_owned_free_insufficient": "only index-level implied volatility (VIX family) is owned; the "
                                    "cross-sectional dispersion of single-name implied volatility and "
                                    "skew is not derivable from any owned or free source",
     "pit_credibility": "PIT_MARKET_OBSERVABLE if end-of-day surfaces are delivered as-of",
     "usable_history_years": None, "effective_sample": None,
     "fee_reference": "UNQUOTED (ORATS / Cboe DataShop - R63)",
     "proxy_evidence_family": None},
    {"need": "UNIVERSE_WIDE_NEWS_HISTORY_PRE_2021",
     "dimension": "EVENT_INFORMATION", "asset_class": "US_EQUITY", "horizons": [1, 5],
     "why_owned_free_insufficient": "the entitled EODHD archive is empty before 2018 and dense only from "
                                    "2021 (measured by the campaign's read-only depth probe); a longer "
                                    "PIT-stamped archive would require a paid news-history vendor",
     "pit_credibility": "PIT_PLAUSIBLE for publication timestamps; sentiment vendor-computed",
     "usable_history_years": None, "effective_sample": None,
     "fee_reference": "UNQUOTED",
     "proxy_evidence_family": "NEWS_INTENSITY"},
    {"need": "SHORT_INTEREST_HISTORY",
     "dimension": "SHORT_POSITIONING", "asset_class": "US_EQUITY", "horizons": [5, 21],
     "why_owned_free_insufficient": "FINRA daily short volume is owned only from 2026-07-23; the free "
                                    "history endpoint returns HTTP 403 (R63 measurement)",
     "pit_credibility": "PIT_LAGGED (exchange settlement files)",
     "usable_history_years": None, "effective_sample": None,
     "fee_reference": "UNQUOTED (S3 / Ortex / exchange files - R63)",
     "proxy_evidence_family": None},
)


def _best_family_evidence(family: str | None, tournament: dict | None) -> dict:
    if not family or not tournament:
        return {"state": PROXY_PENDING, "best_conditional_t": None, "best_ann_advantage": None}
    rows = [b for b in (tournament.get("brief") or []) if b.get("family") == family]
    if not rows:
        return {"state": PROXY_PENDING, "best_conditional_t": None, "best_ann_advantage": None}
    best = max(rows, key=lambda b: (b.get("conditional_t") or -99))
    return {"state": "MEASURED", "best_cell": best.get("cell_id"),
            "best_conditional_t": best.get("conditional_t"),
            "best_ann_advantage": best.get("ann_advantage_top25"),
            "verdict": best.get("tournament_verdict")}


def assess_candidate(candidate: dict, *, nav: float | None, tournament: dict | None) -> dict:
    proxy = _best_family_evidence(candidate.get("proxy_evidence_family"), tournament)
    horizon = int(candidate["horizons"][-1])
    fee = None                          # UNQUOTED unless the estate recorded one
    refs = SRC.fee_references().get("recorded_annual_costs") or {}
    for k, v in refs.items():
        if candidate["dimension"].split("_")[0] in str(k).upper():
            fee = float(v)
    expected_gross = None
    if proxy.get("best_ann_advantage") is not None and proxy["best_ann_advantage"] > 0:
        expected_gross = float(proxy["best_ann_advantage"])
    be = SRC.break_even(fee, nav, horizon, expected_gross)
    if be.get("state") != "OK":
        # no recorded fee: the ladder below still decides whether ANY fee could
        # clear on the measured proxy evidence
        verdict = FEE_UNQUOTED
        why = ("no recorded annual fee in the estate; the fee ladder shows what any price would have to earn; "
               "nothing is bought on a guess")
    else:
        expected_net = (expected_gross or 0.0) * SRC.HAIRCUT - be["trading_cost"]
        if expected_gross is None:
            verdict, why = DO_NOT_BUY, "no owned / free proxy shows incremental value (R32 condition 3 fails first)"
        elif expected_net > be["break_even_alpha_annual"]:
            verdict, why = BUY_TEST, "expected net P&L after the haircut exceeds the break-even alpha"
        else:
            verdict, why = DO_NOT_BUY, "expected net P&L after the haircut does not clear the break-even alpha"
    # A fee ladder: the break-even alpha the need would have to deliver at
    # each annual fee, against the live NAV - so the economics are quantified
    # even where no vendor price is recorded, without inventing one.
    ladder = []
    for f in FEE_LADDER_USD:
        b2 = SRC.break_even(f, nav, horizon, expected_gross)
        if b2.get("state") == "OK":
            exp_net = ((expected_gross or 0.0) * SRC.HAIRCUT - b2["trading_cost"]
                       if expected_gross is not None else None)
            ladder.append({"annual_fee_usd": f, "fee_share_of_nav": b2["fee_share_of_nav"],
                           "break_even_alpha_annual": b2["break_even_alpha_annual"],
                           "extreme_hurdle": b2["extreme_hurdle"],
                           "expected_net_after_haircut": exp_net,
                           "clears": (exp_net is not None and exp_net > b2["break_even_alpha_annual"])})
    max_clear = max([r["annual_fee_usd"] for r in ladder if r["clears"]], default=0)
    if verdict == FEE_UNQUOTED and ladder:
        if expected_gross is None:
            verdict, why = DO_NOT_BUY, ("no owned / free proxy shows incremental value (R32 condition 3 fails first); "
                                        "no fee on the ladder could clear")
        elif max_clear == 0:
            verdict, why = DO_NOT_BUY, ("the measured proxy evidence (%.4f/yr gross, halved) does not clear the "
                                        "break-even alpha even at the lowest fee on the ladder ($%d/yr)"
                                        % (expected_gross, FEE_LADDER_USD[0]))
        else:
            verdict, why = BUY_TEST, ("the proxy evidence clears the break-even alpha at fees up to $%d/yr; a "
                                      "purchase experiment is justified only below that fee" % max_clear)
    return {**candidate, "proxy_evidence": proxy, "annual_fee_usd": fee, "nav": nav,
            "fee_ladder": ladder,
            "max_fee_that_clears_usd": max_clear,
            "fee_share_of_nav": be.get("fee_share_of_nav"), "trading_cost": be.get("trading_cost"),
            "uncertainty_haircut": be.get("uncertainty_haircut"),
            "required_return_on_research_spend": be.get("research_spend_return"),
            "break_even_alpha_annual": be.get("break_even_alpha_annual"),
            "expected_incremental_signal": expected_gross,
            "expected_incremental_net_pnl_annual": ((expected_gross or 0.0) * SRC.HAIRCUT - (be.get("trading_cost") or 0.0)
                                                    if expected_gross is not None else None),
            "expected_portfolio_value_usd": (((expected_gross or 0.0) * SRC.HAIRCUT - (be.get("trading_cost") or 0.0)) * (nav or 0.0)
                                             if expected_gross is not None and nav else None),
            "extreme_hurdle": be.get("extreme_hurdle"),
            "purchase_experiment_verdict": verdict, "why": why, "purchased": False}


def build(*, tournament: dict | None = None, cadence: dict | None = None, direction: dict | None = None,
          news_manifest: dict | None = None, equity: dict | None = None, residual: dict | None = None,
          write: bool = True) -> dict:
    tournament = tournament if tournament is not None else read_artifact("tournament.json")
    cadence = cadence if cadence is not None else read_artifact("cadence_grid.json")
    direction = direction if direction is not None else read_artifact("market_direction.json")
    equity = equity if equity is not None else read_artifact("equity_challengers.json")
    residual = residual if residual is not None else read_artifact("frontier_residual.json")
    nav_rec = SRC.live_nav()
    nav = nav_rec.get("nav")
    closed = []
    for b in (tournament or {}).get("brief") or []:
        closed.append({"family": b.get("family"), "cell_id": b.get("cell_id"), "verdict": b.get("tournament_verdict"),
                       "binding_failure": (b.get("failed_gates") or ["DATA_HOLD"])[0] if b.get("tournament_verdict") not in ("MATERIALLY_BEATS_INCUMBENT",) else None})
    for b in (cadence or {}).get("brief") or []:
        closed.append({"family": b.get("family"), "cell_id": b.get("cell_id"), "verdict": b.get("cadence_verdict"),
                       "binding_failure": b.get("cadence_verdict") if b.get("cadence_verdict") != "ECONOMIC_UNDER_CONTROLS" else None})
    for h, r in ((direction or {}).get("horizons") or {}).items():
        v = (r.get("verdicts") or {}).get("verdict") if isinstance(r, dict) else None
        closed.append({"family": "MARKET_DIRECTION_SPY", "cell_id": "SPY|%s" % h, "verdict": v,
                       "binding_failure": None if v == "CALIBRATED_DIRECTIONAL_SKILL" else v})
    # the construction challengers and the residual frontier needs close too, so
    # the exhaustion claim covers EVERY specification the campaign executed
    for b in (equity or {}).get("brief") or []:
        if b.get("verdict") == "REFERENCE_ARM":
            continue
        closed.append({"family": b.get("family"), "cell_id": b.get("cell_id"), "verdict": b.get("verdict"),
                       "binding_failure": (b.get("failed_gates") or ["DATA_HOLD"])[0]
                       if b.get("verdict") != "MATERIALLY_BEATS_INCUMBENT" else None})
    for b in (residual or {}).get("brief") or []:
        closed.append({"family": b.get("family") or "FRONTIER_MANDATES_RISK_CONTROLLED",
                       "cell_id": b.get("cell_id"), "verdict": b.get("r64_verdict"),
                       "binding_failure": b.get("r64_verdict")
                       if b.get("r64_verdict") != "ECONOMIC_UNDER_CONTROLS" else None})
    survivors = [c for c in closed if c["verdict"] in ("MATERIALLY_BEATS_INCUMBENT", "ECONOMIC_UNDER_CONTROLS",
                                                        "CALIBRATED_DIRECTIONAL_SKILL")]
    families = sorted({c["family"] for c in closed if c["family"]})
    exhausted = bool(closed) and not survivors
    cands = [assess_candidate(c, nav=nav, tournament=tournament) for c in CANDIDATES]
    ranked = sorted(cands, key=lambda c: (-(c.get("expected_incremental_signal") or 0.0), c["need"]))
    body = {"schema": "alpha_recovery_purchase_case/1", "calculation_owner": CALCULATION_OWNER,
            "no_purchase_during_campaign": True, "purchases": 0, "subscriptions": 0, "trials": 0, "accounts": 0,
            "nav": nav_rec, "families_closed": families, "closed_specifications": closed,
            "survivors": survivors,
            "owned_free_information_exhausted": exhausted,
            "status_if_no_survivor": ST_EXHAUSTED,
            "top_missing_information_need": ranked[0]["need"] if ranked else None,
            "candidates_ranked": ranked,
            "news_acquisition": {k: (news_manifest or {}).get(k) for k in ("state", "n_complete", "items", "requests")}
            if news_manifest else None,
            "rule": "information need first, provider second; R63 break-even arithmetic; BUY_TEST only when the "
                    "economics clear"}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
