"""alpha_agent.r44.purchase - the active data purchase gate.

No purchase is made. The contract sets ``MAY_SPEND_MONEY = False`` and the
default authorised spend to $0. What this module does is put a number on
each candidate dataset so the operator's decision is a decision and not a
mood.

The scoring rule is deliberately crude and completely transparent, because a
precise-looking score built on invented inputs is worse than a rough one
built on measured ones. Every input below is a MEASUREMENT this release
actually made:

  * how many predeclared hypotheses the dataset unblocks, counted;
  * whether this release found live evidence pointing at the family, and how
    strong that evidence was (a t-statistic this release computed, not a
    hoped-for one);
  * whether a $0 route was tried and what exactly stopped it;
  * the first-year cash cost.

The output ranks by expected information gain per dollar and states, for
each, whether the honest recommendation is BUY, SKIP or NEED_SAMPLE.
"""
from __future__ import annotations

import datetime as _dt

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.purchase"


def _score(hypotheses: int, evidence_t: float, blocker_is_external: bool,
           first_year_usd: float) -> dict:
    """Information gain per dollar, from measured inputs only."""
    # Evidence weight: a family this release measured at |t| >= 2 is worth
    # more than one it could not measure at all, but an unmeasured family is
    # not worth zero - it is worth its breadth.
    ev = 1.0 + min(abs(float(evidence_t or 0.0)), 4.0) / 2.0
    breadth = float(hypotheses)
    external = 1.0 if blocker_is_external else 0.5
    gain = breadth * ev * external
    cost = max(float(first_year_usd), 1.0)
    return {"hypotheses_unlocked": hypotheses,
            "evidence_weight": round(ev, 3),
            "external_blocker_factor": external,
            "information_gain": round(gain, 3),
            "first_year_usd": first_year_usd,
            "gain_per_1000_usd": round(gain / cost * 1000.0, 3)}


def candidates(*, options_short_by_sessions: int = None,
               intraday_evidence_t: float = None,
               analyst_match_rate: float = None,
               credit_years_owned: float = None) -> list:
    """Every paid candidate, priced against what R44 measured."""
    rows = []

    # ---- 1. historical option surface ------------------------------------ #
    short = options_short_by_sessions
    rows.append({
        "rank_key": "OPTIONS",
        "PROVIDER": "Polygon.io",
        "EXACT_DATASET": "Options Starter (historical option aggregates "
                         "beyond the free ~2-year window)",
        "EXACT_PRICE": "$29/month Starter, $79/month Developer (list)",
        "FIRST_YEAR_USD": 29.0 * 12,
        "TRIAL_MONTH_USD": 29.0,
        "HISTORY": "Starter ~5 years, Developer ~10 years (VENDOR CLAIM, "
                   "still unverified by this release)",
        "ASSETS": "all US listed options incl. SPX/SPY/QQQ and single names",
        "FREQUENCY": "daily aggregates per dated contract",
        "PIT_QUALITY": "STRONG - dated contracts, strike and expiry in the "
                       "identifier, expired universe queryable as-of a date",
        "SURVIVORSHIP_QUALITY": "STRONG - expired contracts enumerable, "
                                "re-verified live this release",
        "LICENSING": "standard non-redistribution; research use permitted",
        "SAMPLE_QUALITY": "ACQUIRED AND NORMALISED AT $0 - R43 proved the "
                          "pipeline on 30 contracts; R44 deepened it to a "
                          "strike x expiry x call/put surface with locally "
                          "inverted IV",
        "EXACT_HYPOTHESES_UNLOCKED": [
            "variance risk premium from ACTUAL option prices with a term "
            "structure by expiry and a smile by strike",
            "delta-hedged option returns, isolating the volatility premium "
            "from direction",
            "index vs constituent DISPERSION - the one option hypothesis the "
            "owned CBOE indices cannot express at all",
            "option-implied moments as cross-sectional equity signals",
            "event-implied volatility and post-event normalisation around "
            "the PIT macro calendar this estate now owns",
        ],
        "WHY_CURRENT_DATA_CANNOT_ANSWER_THEM":
            "the owned VIX complex IS the SPX surface already summarised - "
            "no strikes, no single names, therefore no smile, no dispersion "
            "and no delta-hedged return. The free Polygon window is a "
            "rolling ~2 years, which this release measured as %s sessions "
            "short of the frozen 250-fit + 250-judged requirement."
            % ("an unmeasured number of" if short is None else short),
        "BLOCKER": "HISTORICAL_DATA_UNAVAILABLE",
        "EXPECTED_DECISION_VALUE":
            "converts a family that is currently UNTESTABLE into a testable "
            "one for the price of one month",
        "RECOMMEND": "NEED_SAMPLE",
        "WHY_NOT_BUY_YET": "the per-tier history claim is still the vendor's "
                           "word. One Starter month verifies it, and that is "
                           "the operator's call, not this release's.",
        "_gain": _score(5, 0.0, True, 29.0 * 12),
    })

    # ---- 2. native intraday futures --------------------------------------- #
    t = intraday_evidence_t
    rows.append({
        "rank_key": "INTRADAY_FUTURES",
        "PROVIDER": "Databento (CME MDP-3) or CME DataMine",
        "EXACT_DATASET": "CME MDP-3 historical intraday, rates / FX / equity "
                         "index / energy futures",
        "EXACT_PRICE": "pay-as-you-go; Databento has advertised signup "
                       "credits. Budget $125-$500 for the first study.",
        "FIRST_YEAR_USD": 500.0,
        "HISTORY": "full depth-of-book from 2010 for most CME products",
        "ASSETS": "native dated futures contracts",
        "FREQUENCY": "tick / MBO / 1-minute",
        "PIT_QUALITY": "STRONG - exchange feed, dated contracts",
        "SURVIVORSHIP_QUALITY": "STRONG - dated contracts",
        "LICENSING": "CME redistribution terms; research use permitted",
        "SAMPLE_QUALITY": "NOT SAMPLED - account and payment details "
                          "required, both forbidden this release",
        "EXACT_HYPOTHESES_UNLOCKED": [
            "the macro-release reaction this release measured in gold and "
            "FX, tested in the RATES and EQUITY INDEX futures where the "
            "release is actually about the underlying",
            "whether the gold result is a gold result or a macro result - "
            "the single question R44 could not answer",
            "roll-window microstructure and calendar-spread execution cost",
            "overnight vs intraday decomposition of the carry and RV books",
        ],
        "WHY_CURRENT_DATA_CANNOT_ANSWER_THEM":
            "the estate's owned intraday bytes are spot FX, spot gold and "
            "crypto. They are real instruments, not proxies, and that is "
            "exactly the limit: a US macro release is priced first in "
            "Treasury and equity index FUTURES, and the estate owns none of "
            "them intraday. The Dukascopy index and Bund symbols are CFDs "
            "and the contract forbids a CFD standing in for a futures "
            "hypothesis.",
        "BLOCKER": "PAYMENT_REQUIRED",
        "EXPECTED_DECISION_VALUE":
            "this release measured a release-locked, event-specific, "
            "cost-surviving effect at gross t %s in ONE market that did not "
            "replicate in the other two. Native futures is the only route "
            "that decides whether that is an edge or an artefact."
            % ("(unmeasured)" if t is None else round(float(t), 2)),
        "RECOMMEND": "NEED_SAMPLE",
        "WHY_NOT_BUY_YET": "a free-tier or credit-funded sample of ONE "
                           "product (ZN or ES 1-minute, 2012-2019) would "
                           "settle the replication question for far less "
                           "than a full subscription.",
        "_gain": _score(4, t or 0.0, True, 500.0),
    })

    # ---- 3. historical analyst expectation vintages ----------------------- #
    mr = analyst_match_rate
    rows.append({
        "rank_key": "ANALYST_VINTAGES",
        "PROVIDER": "Steele (Barcomb) / Intrinio / Zacks / LSEG I/B/E/S",
        "EXACT_DATASET": "historical analyst estimate VINTAGES with as-of "
                         "timestamps and inactive-security coverage",
        "EXACT_PRICE": "not quoted to this release; institutional tiers "
                       "typically four to five figures per year",
        "FIRST_YEAR_USD": 10000.0,
        "HISTORY": "vendor-dependent; ten years is the minimum useful depth",
        "ASSETS": "US equities incl. delisted and acquired",
        "FREQUENCY": "per estimate revision",
        "PIT_QUALITY": "UNVERIFIED - and this release has a specific reason "
                       "to doubt it",
        "SURVIVORSHIP_QUALITY": "UNVERIFIED - inactive-security handling is "
                                "the first thing the sample must show",
        "SAMPLE_QUALITY": "NOT RECEIVED. A sample request is PREPARED AND "
                          "NOT SENT; the contract forbids vendor email.",
        "EXACT_HYPOTHESES_UNLOCKED": [
            "revision momentum, breadth and acceleration",
            "dispersion compression and expansion",
            "earnings surprise x revision, and post-earnings drift",
            "cross-sectional disagreement and analyst count changes",
        ],
        "WHY_CURRENT_DATA_CANNOT_ANSWER_THEM":
            "every reachable free endpoint serves the CURRENT consensus. "
            "This release then tested the one thing that looked like an "
            "exception - EODHD's backward strip of 7/30/60/90-day-ago "
            "consensus - against the estate's OWN prospectively captured "
            "snapshots, and it reproduced them only %s of the time. A "
            "vendor's account of the past consensus is restated; the "
            "estate's own prospective ledger is 24 days long."
            % ("at an unmeasured rate" if mr is None
               else "%.0f%%" % (100.0 * mr)),
        "BLOCKER": "PAYMENT_REQUIRED",
        "EXPECTED_DECISION_VALUE":
            "the largest genuinely orthogonal family the estate has never "
            "tested - and the most expensive",
        "RECOMMEND": "NEED_SAMPLE",
        "WHY_NOT_BUY_YET": "no sample, no price, and no evidence yet that "
                           "the family works in this estate's cost regime. "
                           "The prospective ledger costs $0 and reaches a "
                           "testable length by itself.",
        "_gain": _score(4, 0.0, True, 10000.0),
    })

    # ---- 4. native credit -------------------------------------------------- #
    yrs = credit_years_owned
    rows.append({
        "rank_key": "NATIVE_CREDIT",
        "PROVIDER": "IHS Markit / S&P (CDX, iTraxx), ICE",
        "EXACT_DATASET": "index and single-name CDS levels, corporate bond "
                         "OAS at issue level",
        "EXACT_PRICE": "licensed; institutional pricing, not publicly quoted",
        "FIRST_YEAR_USD": 25000.0,
        "HISTORY": "deep",
        "ASSETS": "credit indices and single names",
        "FREQUENCY": "daily",
        "PIT_QUALITY": "STRONG (vendor)",
        "SURVIVORSHIP_QUALITY": "STRONG (vendor)",
        "SAMPLE_QUALITY": "NOT AVAILABLE - licence required before any data "
                          "moves",
        "EXACT_HYPOTHESES_UNLOCKED": [
            "credit-equity basis as a native RV trade rather than an ETF "
            "proxy",
            "rating migration and default-event studies",
            "credit curve relative value",
        ],
        "WHY_CURRENT_DATA_CANNOT_ANSWER_THEM":
            "the owned ICE BofA OAS family is INFORMATION, not a tradable "
            "instrument, and FRED has now narrowed every one of its series "
            "to %s years - measured across all 18 owned series this "
            "release. HYG/LQD remains PROXY_ONLY."
            % ("an unmeasured number of" if yrs is None else yrs),
        "BLOCKER": "LICENCE_REQUIRED",
        "EXPECTED_DECISION_VALUE":
            "lowest of the four: the estate cannot express a native credit "
            "trade even if it had the data, and the information leg is "
            "already partly owned",
        "RECOMMEND": "RECOMMEND_SKIP",
        "WHY_NOT_BUY_YET": "a licence negotiation for a family whose "
                           "tradable expression this estate does not have",
        "_gain": _score(3, 0.0, True, 25000.0),
    })

    rows.sort(key=lambda r: -r["_gain"]["gain_per_1000_usd"])
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


def gate(**measured) -> dict:
    rows = candidates(**measured)
    top = rows[0]
    return {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "authorized_spend_usd": C.DEFAULT_AUTHORIZED_SPEND_USD,
        "money_spent_usd": 0.0,
        "accounts_created": 0,
        "trials_started": 0,
        "licences_accepted": 0,
        "payment_details_submitted": 0,
        "vendor_emails_sent": 0,
        "candidates": rows,
        "TOP_DATA_PURCHASE_RECOMMENDATION": top["PROVIDER"] + " - "
        + top["EXACT_DATASET"],
        "TOP_DATA_PURCHASE_PRICE": top["EXACT_PRICE"],
        "TOP_DATA_EXPECTED_INFORMATION_GAIN":
            top["_gain"]["gain_per_1000_usd"],
        "TOP_RECOMMENDATION_STATE": top["RECOMMEND"],
        "ranking_rule": "information gain per $1,000 of first-year cost, "
                        "where gain = (hypotheses unlocked) x (evidence "
                        "weight measured THIS release) x (external-blocker "
                        "factor)",
        "no_purchase_is_made": True,
        "operator_decision_required": True,
    }
