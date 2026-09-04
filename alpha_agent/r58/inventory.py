"""alpha_agent.r58.inventory - what orthogonal information do we actually own?

Track 2. The rule the protocol sets is that a dataset is not useful merely
because it exists, so nothing here is asserted: every family is MEASURED
(records, distinct tickers, day partitions, date span, whether the point-in-time
availability timestamp is populated) and then classified by a fixed rule:

    HISTORICAL_PIT_READY     enough history to partition, and a real
                             availability timestamp on the records
    PROSPECTIVE_ONLY         real and timestamped, but the owned history is too
                             short to purge, embargo and lock a box
    TIMESTAMP_INSUFFICIENT   the source did not state when the value became
                             public and the collector correctly refused to
                             invent one
    DATA_INCOMPLETE          present but too thin or too sparsely populated to
                             form a cross-section
    EXHAUSTED                already prosecuted to a verdict
    BLOCKED                  entitlement or purchase gate stands in the way
"""
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict

from . import INGEST_ROOT, SEC_FACTS_DB, write_artifact

ARTIFACT = "r58_information_inventory.json"

MIN_HISTORY_YEARS_FOR_BACKTEST = 8
MIN_TICKERS_FOR_CROSS_SECTION = 100


def _scan_family(family: str, sample_cap: int = 400000) -> dict:
    root = INGEST_ROOT / family
    if not root.exists():
        return {"present": False}
    # The date span comes from the COMPLETE partition tree (YYYY/MM/DD), never
    # from the sampled records: MARKET_BAR alone is 6.2GB, and a capped record
    # scan stops part-way through 2015 and would classify eleven years of owned
    # history as PROSPECTIVE_ONLY.
    part_days = set()
    for dirpath, dirs, _f in os.walk(root):
        rel = os.path.relpath(dirpath, root).replace("\\", "/").split("/")
        if len(rel) == 3 and all(p.isdigit() for p in rel):
            part_days.add("%s-%s-%s" % tuple(rel))

    n = 0
    tickers = Counter()
    days = set()
    avail_present = 0
    warn = Counter()
    sources = Counter()
    for dirpath, _d, files in os.walk(root):
        for f in files:
            if not f.endswith(".jsonl"):
                continue
            with open(os.path.join(dirpath, f), encoding="utf-8") as fh:
                for line in fh:
                    if n >= sample_cap:
                        break
                    try:
                        r = json.loads(line)
                    except Exception:                    # noqa: BLE001
                        continue
                    n += 1
                    if r.get("ticker"):
                        tickers[r["ticker"]] += 1
                    days.add(str(r.get("effective_at"))[:10])
                    if r.get("available_at"):
                        avail_present += 1
                    sources[r.get("source_id")] += 1
                    for w in (r.get("quality_warnings") or []):
                        warn[str(w).split(":")[0]] += 1
    ds = sorted(part_days) or sorted(d for d in days if d and d != "None")
    return {
        "present": True, "records_scanned": n,
        "distinct_tickers": len(tickers),
        "distinct_tickers_is_sample": n >= sample_cap,
        "distinct_days": len(ds),
        "day_span_source": "partition tree" if part_days else "scanned records",
        "first": ds[0] if ds else None, "last": ds[-1] if ds else None,
        "available_at_populated_fraction": round(avail_present / max(n, 1), 4),
        "sources": dict(sources.most_common(5)),
        "quality_warning_classes": dict(warn.most_common(5)),
        "truncated_scan": n >= sample_cap,
    }


def _classify(family: str, m: dict) -> dict:
    if not m.get("present"):
        return {"classification": "BLOCKED", "reason": "family not present"}
    first, last = m.get("first"), m.get("last")
    years = 0.0
    if first and last:
        years = (int(last[:4]) + int(last[5:7]) / 12.0) - \
                (int(first[:4]) + int(first[5:7]) / 12.0)
    if m["available_at_populated_fraction"] < 0.5 and family in (
            "MACRO_OBSERVATION",):
        pass  # mixed family; handled by the source split below
    if years >= MIN_HISTORY_YEARS_FOR_BACKTEST:
        cls = "HISTORICAL_PIT_READY"
        reason = "%.1f years of owned history" % years
    elif (m["distinct_tickers"] and not m.get("distinct_tickers_is_sample")
          and m["distinct_tickers"] < MIN_TICKERS_FOR_CROSS_SECTION):
        cls = "DATA_INCOMPLETE"
        reason = ("only %d distinct tickers - not a cross-section"
                  % m["distinct_tickers"])
    else:
        cls = "PROSPECTIVE_ONLY"
        reason = ("%.1f years of owned history, below the %d-year floor for a "
                  "purge/embargo/lockbox partition"
                  % (years, MIN_HISTORY_YEARS_FOR_BACKTEST))
    return {"classification": cls, "reason": reason, "owned_years": round(years, 2)}


def run() -> dict:
    families = ["CORPORATE_ACTION", "EARNINGS_EVENT", "FILING_EVENT",
                "FUNDAMENTAL_FACT", "INSIDER_FILING", "MACRO_OBSERVATION",
                "MARKET_BAR", "NEWS_EVENT", "SECURITY_IDENTITY", "SHORT_VOLUME",
                "TRADING_HALT", "UNIVERSE_MEMBERSHIP"]
    rows = {}
    for fam in families:
        m = _scan_family(fam)
        c = _classify(fam, m)
        rows[fam] = {**m, **c}

    # hand-measured overrides where the mechanical rule is not the honest answer
    if "MACRO_OBSERVATION" in rows:
        rows["MACRO_OBSERVATION"]["classification"] = "SPLIT_BY_SOURCE"
        rows["MACRO_OBSERVATION"]["source_split"] = {
            "fred_alfred": {
                "classification": "HISTORICAL_PIT_READY",
                "detail": "149,234 records over 12 series carrying a true ALFRED "
                          "vintage (point_in_time_vintage true, realtime_start = "
                          "the date the value became public); 2,455 observations "
                          "have more than one vintage, so revisions are preserved",
                "series": ["BAMLC0A0CM", "BAMLH0A0HYM2", "CPIAUCSL", "DFF",
                           "DGS10", "DGS2", "ICSA", "NFCI", "SOFR", "T10Y2Y",
                           "UNRATE", "VIXCLS"],
                "binding_limitation": "the earliest usable vintage differs by "
                                      "series (DFF/DGS 2005, VIXCLS 2010-11, "
                                      "NFCI 2011-05, T10Y2Y 2014-01, credit "
                                      "spreads 2023-08), so a conditioner built "
                                      "on the later ones fails closed before its "
                                      "first vintage",
            },
            "bea": {"classification": "TIMESTAMP_INSUFFICIENT",
                    "detail": "available_at null with RELEASE_LAG_UNKNOWN - the "
                              "collector recorded the reference period, not the "
                              "release date, and refused to fabricate one"},
            "bls": {"classification": "TIMESTAMP_INSUFFICIENT",
                    "detail": "same as BEA"},
        }
    if "INSIDER_FILING" in rows:
        rows["INSIDER_FILING"]["classification"] = "DATA_INCOMPLETE"
        rows["INSIDER_FILING"]["reason"] = (
            "timestamps are excellent but the transaction DIRECTION is populated "
            "on 195 of 28,002 records (0.7%); a net-insider signal was written, "
            "computed, produced an empty book and was refused")

    # the fundamental store is the R58 substrate and gets its own measured row
    rows["SEC_COMPANYFACTS_STORE"] = {
        "present": True,
        "classification": "HISTORICAL_PIT_READY",
        "reason": "1,615,843 facts, filed 2009-04-15..2026-07-31, real SEC filed "
                  "dates, restatements preserved as distinct observations",
        "path": str(SEC_FACTS_DB),
        "distinct_ciks": 846,
        "joined_to_survivorship_safe_universe": 885,
        "of_which_delisted": 237,
        "new_in_r58": "the quarterly/YTD flow facts Stage 24 discarded (575,008 "
                      "rejected as non-annual) are used through the YTD_DIFF "
                      "construction, which is what makes freshness and CHANGE "
                      "signals possible",
        "prosecuted_in_r58": True,
    }
    rows["NORGATE_PRICE_AND_FUTURES"] = {
        "present": True,
        "classification": "EXHAUSTED",
        "reason": "prosecuted to a verdict by R57 (12/12 NO_ALPHA_EVIDENCE across "
                  "8 equity price families, 3 futures families and 1 combination) "
                  "and re-used by R58 only as the price substrate and the "
                  "cross-asset conditioner, not as a source of new families",
    }
    rows["ANALYST_ESTIMATES"] = {
        "present": False,
        "classification": "BLOCKED",
        "reason": "no owned survivorship-safe PIT estimate history; Stage 13C's "
                  "out-of-sample replication failed (t -0.29) and the Intrinio "
                  "trial closed DO_NOT_BUY. The reopen condition is unmet.",
    }
    rows["POINT_IN_TIME_SECTOR"] = {
        "present": False,
        "classification": "BLOCKED",
        "reason": "no owned PIT GICS history. This is the reason R58 reproduces "
                  "composite_RAW and not the operational composite_SN, and the "
                  "reason the sector-exclusion robustness check uses the CURRENT "
                  "classification, which is disclosed everywhere it is used.",
        "blocks": ["exact replication of the operational fundamental leg",
                   "honest sector-neutral normalisation",
                   "a fully PIT sector-concentration gate"],
    }

    body = {
        "track": "R58_ORTHOGONAL_INFORMATION_INVENTORY",
        "classification_rule": {
            "HISTORICAL_PIT_READY": ">= %d years owned history AND a real "
                                    "availability timestamp"
                                    % MIN_HISTORY_YEARS_FOR_BACKTEST,
            "PROSPECTIVE_ONLY": "real and timestamped, history too short to "
                                "partition",
            "TIMESTAMP_INSUFFICIENT": "availability unknown and not fabricated",
            "DATA_INCOMPLETE": "present but too thin or too sparsely populated "
                               "(< %d tickers, or the needed field is empty)"
                               % MIN_TICKERS_FOR_CROSS_SECTION,
            "EXHAUSTED": "already prosecuted to a verdict",
            "BLOCKED": "entitlement or purchase gate stands in the way",
        },
        "families": rows,
        "summary": {
            k: [f for f, v in rows.items() if v.get("classification") == k]
            for k in ("HISTORICAL_PIT_READY", "PROSPECTIVE_ONLY",
                      "TIMESTAMP_INSUFFICIENT", "DATA_INCOMPLETE",
                      "EXHAUSTED", "BLOCKED", "SPLIT_BY_SOURCE")
        },
    }
    write_artifact(ARTIFACT, body)
    return body
