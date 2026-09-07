"""alpha_agent.r59.providers - the provider value scoreboard.

Section S asks one question: *are we extracting value from what we pay for?*

The scoreboard refuses the lazy answer in both directions. "No alpha qualified,
therefore the subscription is worthless" is wrong - a survivorship-safe vendor's
second job is to stop the estate believing a false positive, and R57 used
Norgate to retire twelve price families that a survivor-biased panel would have
blessed. "We use it every day, therefore it pays for itself" is equally wrong.

So each data class is scored on four measured facts: what coverage it actually
delivers, how many hypotheses in the research memory consumed it, what those
hypotheses concluded, and what research-ready information the estate owns but
has never turned into a feature. The last one is the actionable column.

Everything here is measured from the owned artifacts and the research memory.
Costs are recorded only where the operator has stated them; an unknown cost is
reported as unknown rather than guessed.
"""
from __future__ import annotations

from typing import Optional

from .. import r59
from . import form4 as F4
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.providers"

#: Stated by the operator in the Release-37 native-market-data gate. Any cost
#: not recorded there is left None rather than estimated.
KNOWN_COSTS_USD_YEAR = {
    ("NORGATE", "WORLD_FUTURES"): 270.0,
}


def _hyp_consuming(mem: M.ResearchMemory, needle: str) -> list:
    """Hypotheses whose input identity or asset scope consumed this substrate."""
    out = []
    for h in mem.list_hypotheses(limit=20000):
        blob = "%s %s %s" % (h.get("input_data_identity") or "",
                             h.get("title") or "", h.get("release") or "")
        if needle.lower() in blob.lower():
            out.append(h)
    return out


def _outcome_mix(rows: list) -> dict:
    mix: dict = {}
    for h in rows:
        mix[h.get("outcome") or "OPEN"] = mix.get(h.get("outcome") or "OPEN",
                                                  0) + 1
    return mix


def norgate_scoreboard(mem: M.ResearchMemory) -> dict:
    eq = r59.read_json(r59.EQUITY_PANEL_META) or {}
    fut = r59.read_json(r59.FUTURES_PANEL_META) or {}
    markets = fut.get("markets") or []
    by_class: dict = {}
    for m in markets:
        by_class[str(m.get("classification"))] = \
            by_class.get(str(m.get("classification")), 0) + 1

    eq_hyp = _hyp_consuming(mem, "sp500_pit_panel")
    eq_hyp += [h for h in mem.list_hypotheses(asset_class=r59.AC_US_EQUITY)
               if h.get("release") in ("R57", "R59")]
    fut_hyp = [h for h in mem.list_hypotheses(limit=20000)
               if h.get("asset_class") in
               (r59.AC_EQUITY_INDEX, r59.AC_RATES, r59.AC_COMMODITY,
                r59.AC_FX, r59.AC_VOLATILITY, r59.AC_CROSS_ASSET)]

    n_delisted = None
    fnd = r59.read_json(r59.FUNDAMENTAL_PANEL_META) or {}
    if fnd:
        n_delisted = fnd.get("n_joined_delisted")

    classes = [
        {
            "data_class": "US_EQUITY_PIT_PANEL",
            "coverage": {
                "symbols": eq.get("n_symbols"),
                "sessions": eq.get("n_dates"),
                "first": (eq.get("dates") or [None])[0],
                "last": (eq.get("dates") or [None])[-1],
                "membership": "point-in-time S&P 500 Current & Past",
                "delisted_names_joined_to_fundamentals": n_delisted,
                "price_basis": eq.get("price_basis"),
            },
            "experiments_unlocked": "every survivorship-safe US cross-sectional "
                                    "family the estate has ever run",
            "experiments_run": len(set(h["hypothesis_id"] for h in eq_hyp)),
            "outcomes": _outcome_mix(list({h["hypothesis_id"]: h
                                           for h in eq_hyp}.values())),
            "second_job": "R57's twelve price families were retired ON this "
                          "panel. On a survivor-only universe several would "
                          "have shown positive lockbox excess - the "
                          "subscription's measurable return here is a false "
                          "positive NOT adopted, which no PnL line records",
            "unused_research_ready": [
                "per-market volume and open interest are in the vendor feed but "
                "no cost model uses them; the flat 2bp/side futures cost is "
                "applied identically to ES and to the thinnest market in the "
                "panel",
                "intraday bars are entitled but the estate holds only daily "
                "closes in this panel",
            ],
        },
        {
            "data_class": "WORLD_FUTURES",
            "coverage": {
                "markets": fut.get("n_markets"),
                "sessions": fut.get("n_dates"),
                "first": fut.get("date_start"),
                "last": fut.get("date_end"),
                "by_classification": by_class,
                "contracts": "front and second continuous, with roll flags",
                "universe_rule": fut.get("universe_rule"),
            },
            "experiments_unlocked": "every non-equity scope the estate can "
                                    "research at all: equity index, rates, "
                                    "commodities, FX, volatility, cross-asset",
            "experiments_run": len(fut_hyp),
            "outcomes": _outcome_mix(fut_hyp),
            "second_job": "the 103-market panel is what makes a cross-sectional "
                          "non-equity claim possible; without it the estate has "
                          "no non-equity cross-section at all",
            "unused_research_ready": [
                "calendar-spread and inter-commodity relative value: the second "
                "contract is IN the panel (close_b) and only the carry slope "
                "uses it",
                "roll-date behaviour: roll flags are carried but no hypothesis "
                "conditions on them",
                "the 25-market interest-rate block spans several curves and no "
                "curve-relative-value family has been prosecuted per curve",
                "seasonality on the 25 agricultural markets, where the "
                "economic case is strongest and the estate has run none",
            ],
        },
    ]
    for c in classes:
        c["cost_usd_year"] = KNOWN_COSTS_USD_YEAR.get(
            ("NORGATE", c["data_class"]))
        c["cost_known"] = c["cost_usd_year"] is not None
    return {"provider": "NORGATE", "data_classes": classes}


def eodhd_scoreboard(mem: M.ResearchMemory) -> dict:
    """EODHD utilisation, measured from R58's inventory scan of the owned
    normalized store."""
    inv = r59.read_json(r59.R58_ROOT / "results"
                        / "r58_information_inventory.json") or {}
    fams = inv.get("families") or {}

    classes = []
    for fam, label, note in (
            ("MARKET_BAR", "PRICES",
             "the estate prices research from the Norgate panel, so the EODHD "
             "bar feed is an operational/freshness source rather than a "
             "research substrate"),
            ("FUNDAMENTAL_FACT", "FUNDAMENTALS",
             "EODHD contributes 61 of 14,418 owned fundamental records; the "
             "research substrate is SEC companyfacts, and the EODHD snapshot "
             "is survivor-biased so it cannot carry a historical claim"),
            ("CORPORATE_ACTION", "CORPORATE_ACTIONS",
             "operationally load-bearing (the MNST split repair) but only 5 "
             "distinct tickers are in the owned normalized scan - not a "
             "cross-section"),
            ("EARNINGS_EVENT", "EARNINGS_EVENTS",
             "15 distinct tickers owned; a PEAD cross-section needs the "
             "universe, and the publication time of day is unknown"),
            ("INSIDER_FILING", "INSIDER",
             "EODHD contributes 100 of 28,002 records and its availability "
             "date precision is flagged; the usable insider parse is the free "
             "SEC one"),
            ("NEWS_EVENT", "NEWS",
             "5,494 records across SEVEN tickers over 28 days - the feed is "
             "live but the collection is configured to a handful of names, so "
             "no cross-sectional news family is possible from what is stored"),
    ):
        row = fams.get(fam) or {}
        src = row.get("sources") or {}
        classes.append({
            "data_class": label,
            "record_family": fam,
            "coverage": {
                "records_scanned": row.get("records_scanned"),
                "eodhd_records": src.get("eodhd") or src.get("eodhd_analyst"),
                "distinct_tickers": row.get("distinct_tickers"),
                "owned_years": row.get("owned_years"),
                "available_at_populated_fraction":
                    row.get("available_at_populated_fraction"),
            },
            "classification": row.get("classification"),
            "current_usage": note,
            "cost_usd_year": None,
            "cost_known": False,
        })

    return {
        "provider": "EODHD",
        "data_classes": classes,
        "utilisation_gap": [
            "NEWS: the subscription delivers news for the full universe; the "
            "estate stores it for 7 tickers. This is a CONFIGURATION limit, "
            "not an entitlement limit - the single cheapest way to create a "
            "new information family from an existing subscription.",
            "EARNINGS: report dates are stored without a time of day, which "
            "collapses the before/after-close distinction a PEAD study needs. "
            "The feed carries more than the collector keeps.",
            "CORPORATE_ACTIONS: collected for a handful of names although the "
            "entitlement covers the universe; a universe-wide action history "
            "would remove the estate's dependence on ad-hoc split repairs.",
            "FUNDAMENTALS: the snapshot is survivor-biased and therefore "
            "unusable for a historical claim; it is not a gap that more "
            "collection fixes.",
        ],
        "verdict": "PARTIALLY_UTILISED - the binding constraint is collection "
                   "CONFIGURATION on news, earnings-time and corporate "
                   "actions, not entitlement and not price",
    }


def free_scoreboard(mem: M.ResearchMemory) -> dict:
    inv = r59.read_json(r59.R58_ROOT / "results"
                        / "r58_information_inventory.json") or {}
    fams = inv.get("families") or {}
    macro = fams.get("MACRO_OBSERVATION") or {}
    split = (macro.get("source_split") or {})
    cov = F4.coverage_report()

    return {
        "provider": "FREE_PUBLIC_SOURCES",
        "data_classes": [
            {
                "data_class": "SEC_COMPANYFACTS",
                "coverage": (fams.get("SEC_COMPANYFACTS_STORE") or {}),
                "current_usage": "the substrate for R58's 13 fundamental "
                                 "families and the only PIT fundamental "
                                 "history the estate owns",
                "unused_research_ready": "quarterly and YTD flow facts beyond "
                                         "the ladders R58 built",
                "cost_usd_year": 0.0,
            },
            {
                "data_class": "SEC_FORM4_INSIDER",
                "coverage": cov["owned_r46_parse"],
                "current_usage": "collected daily and parsed completely by "
                                 "R46; NOT readable by the canonical feature "
                                 "pipeline until R59 added a reader",
                "unused_research_ready": "R59 resolved the reader gap; the "
                                         "remaining limit is calendar time - "
                                         "27 business days cannot support a "
                                         "partitioned backtest",
                "cost_usd_year": 0.0,
            },
            {
                "data_class": "FRED_ALFRED_VINTAGES",
                "coverage": (split.get("fred_alfred") or {}),
                "current_usage": "macro conditioners and the credit lane",
                "unused_research_ready": "12 series carry true vintages; only "
                                         "a handful are used as conditioners "
                                         "and none as a cross-sectional "
                                         "exposure",
                "cost_usd_year": 0.0,
            },
            {
                "data_class": "BEA_BLS",
                "coverage": {"classification": "TIMESTAMP_INSUFFICIENT"},
                "current_usage": "NONE - available_at is null, so a "
                                 "point-in-time study cannot use it",
                "unused_research_ready": "blocked until the collector records "
                                         "the release date rather than the "
                                         "reference period",
                "cost_usd_year": 0.0,
            },
            {
                "data_class": "FINRA_SHORT_VOLUME",
                "coverage": (fams.get("SHORT_VOLUME") or {}),
                "current_usage": "one R58 prospective challenger",
                "unused_research_ready": "13,243 tickers daily; only a single "
                                         "pressure signal has been frozen "
                                         "against it",
                "cost_usd_year": 0.0,
            },
        ],
        "verdict": "The free tier supplies the estate's only PIT fundamental "
                   "history, its only insider flow and its only macro "
                   "vintages. Two of the five classes are limited by "
                   "collector behaviour rather than by the source.",
    }


def pending_scoreboard() -> dict:
    from . import steele as ST
    r = ST.readiness()
    return {
        "provider": "ANALYST_REVISIONS_PENDING",
        "state": r["state"],
        "cost_usd_year": None,
        "cost_known": False,
        "experiments_unlocked_if_bought": [h["id"]
                                           for h in r["unlocked_hypotheses"]],
        "experiments_run": 0,
        "blocker": "no sample received; no purchase permitted",
        "prior_evidence": r["prior_evidence"],
        "gate": "alpha_agent.r59.steele -> api.data_expansion",
    }


def scoreboard(mem: Optional[M.ResearchMemory] = None, *,
               write: bool = True) -> dict:
    mem = mem or M.open_memory()
    body = {
        "calculation_owner": CALCULATION_OWNER,
        "question": "are we extracting value from what we pay for?",
        "scoring_rule": "a provider's return is measured as coverage delivered, "
                        "hypotheses consumed, verdicts produced AND false "
                        "positives prevented - never as alpha found",
        "providers": [
            norgate_scoreboard(mem),
            eodhd_scoreboard(mem),
            free_scoreboard(mem),
            pending_scoreboard(),
        ],
    }
    for p in body["providers"]:
        for dc in p.get("data_classes") or []:
            mem.set_provider_usage(p["provider"], dc.get("data_class", "?"),
                                   coverage=dc.get("coverage"),
                                   detail={k: v for k, v in dc.items()
                                           if k != "coverage"})
    if write:
        path = r59.write_artifact("r59_provider_value_scoreboard.json", body)
        body["artifact_path"] = str(path)
    return body
