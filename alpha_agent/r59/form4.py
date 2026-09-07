"""alpha_agent.r59.form4 - the canonical insider-transaction normaliser.

R58 recorded this as the estate's highest-value owned-but-unused information:

    INSIDER_FILING ... timestamps are excellent but the transaction DIRECTION
    is populated on 195 of 28,002 records (0.7%); a net-insider signal was
    written, computed, produced an empty book and was refused

The brief expected a MISSING PARSER. Measurement says otherwise, and the
distinction matters because it changes the fix:

* ``alpha_agent.collectors.sec_edgar.parse_form4_xml`` already extracts
  ``transaction_code`` and ``acquired_disposed`` correctly. It is throttled -
  ``form4_xml_cap`` defaults to 8 filings per collection run - so the canonical
  normalized store fills with INDEX rows that carry no transaction detail.
* ``alpha_agent.r46.form4.parse_submission_text`` already produces the COMPLETE
  parse, direction included, and R46 has been running it daily. Its output sits
  in an R46-private research root that no feature pipeline reads.

So the estate does not need a parser. It needs a READER: one canonical way to
turn the owned R46 daily parse into insider-transaction records any engine can
consume, with the acceptance instant as the point-in-time key. That is this
module.

Point-in-time discipline: a transaction is admissible at instant T only if its
filing was ACCEPTED before T. The transaction date - which may be two business
days earlier - is never used as the observation instant, because using it would
let the research see a trade before the market could.

Read-only. It writes nothing outside the R59 research root and never mutates
the R46 store it reads.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from .. import r59

CALCULATION_OWNER = "alpha_agent.r59.form4"
SCHEMA = "r59_insider_transactions/1"

#: Only these codes are open-market decisions. Grants, option exercises, tax
#: withholding and gifts are recorded and excluded BY NAME - an insider who is
#: handed stock has not expressed a view.
INFORMATIVE_CODES = ("P", "S")


def _iter_day_files() -> Iterator[Path]:
    d = r59.FORM4_RAW_DIR
    if not d.exists():
        return iter(())
    return iter(sorted(d.glob("form4_rows_*.json")))


def _parse_accept(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def read_transactions(*, as_of_instant: Optional[datetime] = None,
                      informative_only: bool = False) -> dict:
    """Read every owned Form-4 transaction, direction included.

    ``as_of_instant`` enforces the point-in-time rule: a filing accepted at or
    after that instant is EXCLUDED and counted, so the caller can see how much
    of the store was withheld rather than silently losing it.
    """
    rows: list = []
    days: list = []
    withheld = 0
    unparsed = 0
    files = 0
    for p in _iter_day_files():
        files += 1
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        day = doc.get("day")
        days.append(day)
        for f in doc.get("filings") or []:
            if not f.get("parsed"):
                unparsed += 1
                continue
            acc = _parse_accept(f.get("accepted_at_utc"))
            if as_of_instant is not None and (acc is None
                                              or acc >= as_of_instant):
                withheld += 1
                continue
            ticker = f.get("issuer_ticker")
            for t in f.get("transactions") or []:
                code = t.get("transaction_code")
                if informative_only and code not in INFORMATIVE_CODES:
                    continue
                direction = t.get("direction")
                ad = t.get("acquired_disposed")
                rows.append({
                    "accession": f.get("accession"),
                    "observed_at_utc": f.get("accepted_at_utc"),
                    "day": day,
                    "issuer_cik": f.get("issuer_cik"),
                    "ticker": ticker,
                    "owner_count": f.get("n_owners"),
                    "transaction_date": t.get("transaction_date"),
                    "transaction_code": code,
                    "transaction_class": t.get("transaction_class"),
                    "acquired_disposed": ad,
                    "direction": direction,
                    "signed_shares": (float(t.get("shares") or 0.0)
                                      * (1.0 if ad == "A" else -1.0)),
                    "shares": t.get("shares"),
                    "price_per_share": t.get("price_per_share"),
                    "is_informative": bool(t.get("is_informative")),
                })
    days = sorted(d for d in days if d)
    with_dir = sum(1 for r in rows if r["acquired_disposed"] in ("A", "D"))
    tickers = {r["ticker"] for r in rows if r["ticker"]}
    return {
        "schema": SCHEMA,
        "calculation_owner": CALCULATION_OWNER,
        "source": str(r59.FORM4_RAW_DIR),
        "source_owner": "alpha_agent.r46.form4",
        "day_files_read": files,
        "days": days,
        "first_day": days[0] if days else None,
        "last_day": days[-1] if days else None,
        "filings_unparsed": unparsed,
        "filings_withheld_by_pit_rule": withheld,
        "transactions": rows,
        "n_transactions": len(rows),
        "n_with_direction": with_dir,
        "direction_populated_fraction": (round(with_dir / len(rows), 4)
                                         if rows else None),
        "distinct_tickers": len(tickers),
        "pit_key": "SEC acceptance instant (accepted_at_utc), never the "
                   "transaction date",
    }


def coverage_report() -> dict:
    """Measure the owned insider store against R58's refusal.

    Reports the canonical-store fraction R58 measured next to what the owned
    R46 parse actually contains, so the gap is a number rather than a claim.
    """
    tx = read_transactions()
    inv = r59.read_json(r59.R58_ROOT / "results"
                        / "r58_information_inventory.json") or {}
    r58_row = ((inv.get("families") or {}).get("INSIDER_FILING") or {})
    probe = ((r59.read_json(r59.R58_ROOT / "results"
                            / "r58_forward_challengers.json") or {})
             .get("insider_probe_result") or {})

    informative = [r for r in tx["transactions"]
                   if r["transaction_code"] in INFORMATIVE_CODES]
    buys = sum(1 for r in informative if r["direction"] == "BUY")
    sells = sum(1 for r in informative if r["direction"] == "SELL")
    inf_tickers = {r["ticker"] for r in informative if r["ticker"]}

    return {
        "calculation_owner": CALCULATION_OWNER,
        "gap_is_a_missing_parser": False,
        "gap_is_a_missing_reader": True,
        "canonical_normalized_store": {
            "measured_by": "alpha_agent.r58.inventory",
            "records": r58_row.get("records_scanned"),
            "distinct_tickers": r58_row.get("distinct_tickers"),
            "classification": r58_row.get("classification"),
            "reason": r58_row.get("reason"),
            "throttle": "alpha_agent.collectors.sec_edgar._collect_form4_"
                        "transactions caps XML fetches at form4_xml_cap "
                        "(default 8) per collection run, so the canonical "
                        "store fills with index rows carrying no direction",
        },
        "owned_r46_parse": {
            "parser": "alpha_agent.r46.form4.parse_submission_text",
            "day_files": tx["day_files_read"],
            "days": "%s..%s" % (tx["first_day"], tx["last_day"]),
            "transactions": tx["n_transactions"],
            "with_direction": tx["n_with_direction"],
            "direction_populated_fraction": tx["direction_populated_fraction"],
            "distinct_tickers": tx["distinct_tickers"],
            "informative_open_market": len(informative),
            "informative_buys": buys,
            "informative_sells": sells,
            "informative_distinct_tickers": len(inf_tickers),
        },
        "r58_probe_that_was_refused": {
            "scored": probe.get("scored"),
            "tickers_with_data": (probe.get("meta") or {}).get(
                "tickers_with_data"),
            "records_seen": (probe.get("meta") or {}).get("records"),
            "why": "the probe read the canonical normalized store, which "
                   "carries the filings but not the direction",
        },
        "research_readiness": {
            "state": "PROSPECTIVE_ONLY",
            "reason": "the owned parse spans %d business days; a "
                      "discovery/validation/lockbox partition needs years, so "
                      "this information can support a FORWARD challenger and "
                      "cannot support a historical backtest"
                      % len(tx["days"]),
            "honest_limit": "no historical insider backtest is possible from "
                            "owned data at any coverage level",
        },
    }


def normalise(*, write: bool = True) -> dict:
    """Produce the canonical insider-transaction artifact under the R59 root."""
    tx = read_transactions()
    body = {k: v for k, v in tx.items() if k != "transactions"}
    body["coverage"] = coverage_report()
    body["sample"] = tx["transactions"][:5]
    if write:
        p = r59.write_artifact("r59_insider_transactions.json", body,
                               subdir="data")
        body["artifact_path"] = str(p)
    return body
